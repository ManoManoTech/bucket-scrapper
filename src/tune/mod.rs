//! Auto-tuner search policy — ported from the `wip/auto-tuner` tag's
//! `src/tune/policy.rs` (commit 4b295ed) and adapted for the *live* control
//! loop. The original ran a fixed sample from scratch once per candidate; here
//! the same hill-climb drives live `bsctl` knobs on a continuous run, so the
//! policy stays pure (no I/O) and the mechanics live in the `bsctl autotune`
//! loop.
//!
//! Two adaptations from the original:
//! - **Knob set.** Dropped `line_buffer` (unsupported live). Collapsed the two
//!   download semaphores into one composite `download_concurrency`: on the
//!   non-chunked path `range_concurrency` is the real GET cap and `file_slots`
//!   (decoder slots) must keep pace, so the loop moves both together — matching
//!   the operator's "more downloaders" intent and avoiding the `min()` trap.
//! - **Saturation ceiling.** [`Trial::over_pressure`] lets the loop feed back a
//!   CPU/memory-pressure verdict; a probe that crosses the ceiling saturates
//!   its knob so the search stops climbing *before* the oversubscription cliff
//!   instead of only reverting after throughput collapses.

use std::collections::HashMap;

/// The knobs the tuner searches over. Both map onto live `bsctl` adjustments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Knobs {
    /// Concurrent download+decompress tasks. The loop sets `file_slots` and
    /// `range_concurrency` to this same value.
    pub download_concurrency: usize,
    /// Filter worker tasks.
    pub filter_workers: usize,
}

/// A completed trial: the knobs evaluated, the achieved scalar objective
/// (MB/s read throughput — higher is better), the dominant bottleneck label
/// observed, and whether the run crossed the saturation ceiling during it.
#[derive(Debug, Clone)]
pub struct Trial {
    pub knobs: Knobs,
    pub objective: f64,
    pub dominant: String,
    /// `true` if CPU/memory pressure exceeded the loop's ceiling during this
    /// trial — the knob just probed is then treated as saturated.
    pub over_pressure: bool,
}

/// What the policy wants the loop to do next.
#[derive(Debug, Clone, PartialEq)]
pub enum Proposal {
    /// Apply these knobs and measure a trial.
    Evaluate(Knobs),
    /// Stop; the loop settles on the global-best trial from the history.
    Done,
}

/// A search strategy. Pure w.r.t. its own state plus the trial history.
pub trait TunePolicy {
    /// Called **exactly once per newly-appended trial**: each call folds
    /// `history.last()` into the policy state, so calling it twice without
    /// appending a fresh trial re-folds the same result and corrupts the
    /// search (double-advances `best` / double-shrinks the step). The
    /// `bsctl autotune` loop upholds this by pushing one trial per proposal.
    fn propose(&mut self, history: &[Trial]) -> Proposal;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum KnobId {
    DownloadConcurrency,
    FilterWorkers,
}

/// Per-knob caps + search hyper-parameters.
#[derive(Debug, Clone)]
pub struct PolicyConfig {
    pub download_concurrency_cap: usize,
    pub filter_workers_cap: usize,
    /// A probe must beat the prior best by this fraction to count as an
    /// improvement; otherwise the knob's step shrinks. e.g. 0.03 = +3%.
    pub improve_threshold: f64,
}

/// Initial per-probe step fraction (+25%).
const INIT_STEP: f64 = 0.25;
/// A knob saturates once its step shrinks below this.
const MIN_STEP_FRAC: f64 = 0.10;

/// Hill-climb with backtracking, ceiling-aware. Faithful to the original:
///
/// - **Global best is the baseline.** Every move is proposed from the best
///   point seen, never from a degraded probe.
/// - **Revert on regression.** A probe that fails to beat the best by
///   `improve_threshold` is discarded; its knob's step halves.
/// - **Decaying steps.** Steps only shrink; a knob is saturated once its step
///   underflows `MIN_STEP_FRAC`, it hits its cap, or a probe crossed the
///   saturation ceiling.
/// - **Cross-knob CPU contention.** Over-saturating cores shows up as a
///   regression (revert) *and*, when a pressure ceiling is set, as an
///   `over_pressure` probe that saturates the knob outright.
pub struct HillClimbPolicy {
    cfg: PolicyConfig,
    max_trials: usize,
    seed: Knobs,
    steps: HashMap<KnobId, f64>,
    saturated: HashMap<KnobId, bool>,
    best: Option<(Knobs, f64, String)>,
    /// The knob we last probed and are awaiting the result for.
    pending: Option<KnobId>,
}

impl HillClimbPolicy {
    pub fn new(baseline: Knobs, cfg: PolicyConfig, max_trials: usize) -> Self {
        let mut steps = HashMap::new();
        steps.insert(KnobId::DownloadConcurrency, INIT_STEP);
        steps.insert(KnobId::FilterWorkers, INIT_STEP);
        Self {
            cfg,
            max_trials,
            seed: baseline,
            steps,
            saturated: HashMap::new(),
            best: None,
            pending: None,
        }
    }

    /// The best knobs found so far (valid after at least one absorbed trial).
    pub fn best_knobs(&self) -> Option<Knobs> {
        self.best.as_ref().map(|(k, _, _)| *k)
    }

    fn cap(&self, k: KnobId) -> usize {
        match k {
            KnobId::DownloadConcurrency => self.cfg.download_concurrency_cap,
            KnobId::FilterWorkers => self.cfg.filter_workers_cap,
        }
    }

    fn value(knobs: &Knobs, k: KnobId) -> usize {
        match k {
            KnobId::DownloadConcurrency => knobs.download_concurrency,
            KnobId::FilterWorkers => knobs.filter_workers,
        }
    }

    fn with_value(mut knobs: Knobs, k: KnobId, v: usize) -> Knobs {
        match k {
            KnobId::DownloadConcurrency => knobs.download_concurrency = v,
            KnobId::FilterWorkers => knobs.filter_workers = v,
        }
        knobs
    }

    fn is_saturated(&self, k: KnobId) -> bool {
        *self.saturated.get(&k).unwrap_or(&false)
    }

    fn can_increase(&self, k: KnobId) -> bool {
        self.best
            .as_ref()
            .is_some_and(|(best, _, _)| Self::value(best, k) < self.cap(k))
    }

    /// Map a bottleneck label to the knob that relieves it. `None` ⇒ the lid is
    /// somewhere these knobs can't move (a busy sink) → stop.
    fn knob_for(dominant: &str) -> Option<KnobId> {
        match dominant {
            "filter" => Some(KnobId::FilterWorkers),
            // Download-side lids: waiting on GETs, decoder CPU, or chunk
            // reassembly — all relieved (if there's headroom) by more
            // concurrent download+decompress tasks.
            "download" | "decompress" | "chunk_reassembly" => Some(KnobId::DownloadConcurrency),
            // sink_* / sink_busy / unknown — not tunable via these knobs.
            _ => None,
        }
    }

    fn bumped(&self, k: KnobId) -> usize {
        let best = &self.best.as_ref().expect("best set before bumping").0;
        let cur = Self::value(best, k);
        let frac = *self.steps.get(&k).unwrap_or(&INIT_STEP);
        let raw = ((cur as f64) * (1.0 + frac)).ceil() as usize;
        raw.max(cur + 1).min(self.cap(k))
    }

    fn absorb_latest(&mut self, latest: &Trial) {
        match &self.best {
            None => {
                self.best = Some((latest.knobs, latest.objective, latest.dominant.clone()));
            }
            Some((_, prev_best_obj, _)) => {
                let prev_best_obj = *prev_best_obj;
                // A probe that crossed the pressure ceiling never becomes
                // `best`: the ceiling is a hard safety limit, so the loop must
                // not *settle* above it even if throughput was momentarily
                // higher there. It still saturates the knob (below).
                if latest.objective > prev_best_obj && !latest.over_pressure {
                    self.best = Some((latest.knobs, latest.objective, latest.dominant.clone()));
                }
                let improved =
                    latest.objective > prev_best_obj * (1.0 + self.cfg.improve_threshold);
                if let Some(k) = self.pending {
                    // Crossing the pressure ceiling saturates the knob outright,
                    // regardless of the marginal objective — we won't chase
                    // throughput past the oversubscription point.
                    if latest.over_pressure {
                        self.saturated.insert(k, true);
                    } else if !improved {
                        let step = self.steps.entry(k).or_insert(INIT_STEP);
                        *step /= 2.0;
                        if *step < MIN_STEP_FRAC {
                            self.saturated.insert(k, true);
                        }
                    }
                }
            }
        }
    }

    fn pick_next_knob(&self) -> Option<KnobId> {
        let (_, _, dominant) = self.best.as_ref()?;
        let primary = Self::knob_for(dominant)?;
        // Only ever grow the *current bottleneck's* knob. Growing a different
        // knob can't relieve the current lid; the search moves between knobs
        // organically when an improving probe shifts `best.dominant`. So once
        // the bottleneck's knob is saturated or capped, we're done.
        (!self.is_saturated(primary) && self.can_increase(primary)).then_some(primary)
    }
}

impl TunePolicy for HillClimbPolicy {
    fn propose(&mut self, history: &[Trial]) -> Proposal {
        if history.len() >= self.max_trials {
            return Proposal::Done;
        }
        if history.is_empty() {
            self.pending = None;
            return Proposal::Evaluate(self.seed);
        }
        self.absorb_latest(history.last().expect("non-empty history"));
        match self.pick_next_knob() {
            None => Proposal::Done,
            Some(k) => {
                let best = self.best.as_ref().expect("best set").0;
                let next = Self::with_value(best, k, self.bumped(k));
                self.pending = Some(k);
                Proposal::Evaluate(next)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> PolicyConfig {
        PolicyConfig {
            download_concurrency_cap: 128,
            filter_workers_cap: 64,
            improve_threshold: 0.03,
        }
    }

    fn trial(k: Knobs, obj: f64, dominant: &str) -> Trial {
        Trial {
            knobs: k,
            objective: obj,
            dominant: dominant.into(),
            over_pressure: false,
        }
    }

    #[test]
    fn seed_emitted_first() {
        let seed = Knobs {
            download_concurrency: 16,
            filter_workers: 8,
        };
        let mut p = HillClimbPolicy::new(seed, cfg(), 20);
        assert_eq!(p.propose(&[]), Proposal::Evaluate(seed));
    }

    #[test]
    fn climbs_the_bottleneck_knob_then_reverts_on_regression() {
        let seed = Knobs {
            download_concurrency: 16,
            filter_workers: 8,
        };
        let mut p = HillClimbPolicy::new(seed, cfg(), 20);
        let mut hist = vec![];

        // Seed.
        let Proposal::Evaluate(k0) = p.propose(&hist) else {
            panic!()
        };
        hist.push(trial(k0, 100.0, "download"));

        // download-bound → grows download_concurrency (16 → 20).
        let Proposal::Evaluate(k1) = p.propose(&hist) else {
            panic!()
        };
        assert!(k1.download_concurrency > 16 && k1.filter_workers == 8);
        hist.push(trial(k1, 130.0, "download")); // improved → new best

        // still download-bound, keeps climbing.
        let Proposal::Evaluate(k2) = p.propose(&hist) else {
            panic!()
        };
        assert!(k2.download_concurrency > k1.download_concurrency);
        hist.push(trial(k2, 120.0, "download")); // worse than best(130) → regression

        // Regression: best stays k1, the knob's step halves. Next probe climbs
        // again from k1 but by a smaller increment.
        let Proposal::Evaluate(k3) = p.propose(&hist) else {
            panic!()
        };
        assert_eq!(p.best_knobs().unwrap(), k1);
        assert!(k3.download_concurrency > k1.download_concurrency);
        assert!(k3.download_concurrency < k2.download_concurrency); // smaller step
    }

    #[test]
    fn over_pressure_saturates_knob_and_stops() {
        let seed = Knobs {
            download_concurrency: 16,
            filter_workers: 8,
        };
        let mut p = HillClimbPolicy::new(seed, cfg(), 20);
        let mut hist = vec![];
        let Proposal::Evaluate(k0) = p.propose(&hist) else {
            panic!()
        };
        hist.push(trial(k0, 100.0, "download"));
        let Proposal::Evaluate(k1) = p.propose(&hist) else {
            panic!()
        };
        // This probe crossed the ceiling: even though throughput rose, the knob
        // saturates and — download-bound with no other growable lever — we stop.
        hist.push(Trial {
            knobs: k1,
            objective: 200.0,
            dominant: "download".into(),
            over_pressure: true,
        });
        // filter_workers isn't the download bottleneck's knob, so once
        // download_concurrency is saturated the search is done.
        assert_eq!(p.propose(&hist), Proposal::Done);
        // And critically, we settle on the seed — NOT the over-ceiling probe,
        // even though it scored higher. The ceiling is a hard limit.
        assert_eq!(p.best_knobs().unwrap(), k0);
    }

    #[test]
    fn sink_bound_is_not_tunable() {
        let seed = Knobs {
            download_concurrency: 16,
            filter_workers: 8,
        };
        let mut p = HillClimbPolicy::new(seed, cfg(), 20);
        let mut hist = vec![];
        let Proposal::Evaluate(k0) = p.propose(&hist) else {
            panic!()
        };
        hist.push(trial(k0, 100.0, "sink_void"));
        assert_eq!(p.propose(&hist), Proposal::Done);
    }

    #[test]
    fn stops_at_max_trials() {
        let seed = Knobs {
            download_concurrency: 16,
            filter_workers: 8,
        };
        let mut p = HillClimbPolicy::new(seed, cfg(), 1);
        let hist = vec![trial(seed, 100.0, "download")];
        assert_eq!(p.propose(&hist), Proposal::Done);
    }
}

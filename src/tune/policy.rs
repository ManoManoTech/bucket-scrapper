//! Search policy for the auto-tuner — the part most likely to be iterated on,
//! deliberately isolated from the trial-running mechanics in [`super`].
//!
//! The runner only knows "build config → run trial → record [`Trial`] → ask the
//! policy for the next [`Proposal`]". It is agnostic to *how* the next candidate
//! is chosen, so a different strategy (pattern search, model-based, Nelder-Mead,
//! …) is a drop-in replacement for [`HillClimbPolicy`] without touching the
//! runner.

use std::collections::HashMap;

/// The read-side knobs the tuner searches over. Maps directly onto the
/// corresponding [`crate::pipeline::StreamingDownloaderConfig`] fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Knobs {
    pub max_parallel: usize,
    pub filter_tasks: usize,
    pub line_buffer_size: usize,
}

/// A completed trial: the knobs evaluated, the achieved scalar objective
/// (bytes/sec — higher is better), and the dominant bottleneck label observed
/// over the run (see [`crate::progress::dominant_label`]).
#[derive(Debug, Clone)]
pub struct Trial {
    pub knobs: Knobs,
    pub objective: f64,
    pub dominant: &'static str,
}

/// What the policy wants the runner to do next.
#[derive(Debug, Clone, PartialEq)]
pub enum Proposal {
    /// Run a trial with these knobs.
    Evaluate(Knobs),
    /// Stop; the runner emits the global-best trial from the history.
    Done,
}

/// A search strategy. Pure with respect to its own state plus the trial
/// history; performs no I/O. Called exactly once per completed trial, so each
/// `propose` sees a history one entry longer than the previous call.
pub trait TunePolicy {
    fn propose(&mut self, history: &[Trial]) -> Proposal;
}

/// Which knob a given bottleneck label points at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum KnobId {
    MaxParallel,
    FilterTasks,
    LineBuffer,
}

/// Per-knob caps and the search hyper-parameters. Caps and the improvement
/// threshold come from the [`super::TuneConfig`]; the step seeds are policy
/// internals.
#[derive(Debug, Clone)]
pub struct PolicyConfig {
    pub max_parallel_cap: usize,
    pub filter_tasks_cap: usize,
    pub line_buffer_cap: usize,
    /// A probe must beat the prior best by this fraction to count as an
    /// improvement; otherwise the knob's step shrinks. e.g. 0.03 = +3%.
    pub improve_threshold: f64,
}

/// Initial step fraction for `max_parallel` / `filter_tasks` (+25% per probe).
const INIT_STEP_LINEAR: f64 = 0.25;
/// Initial step fraction for `line_buffer_size` (×2 per probe).
const INIT_STEP_BUFFER: f64 = 1.0;
/// A knob saturates once its step shrinks below this — further refinement
/// isn't worth a trial.
const MIN_STEP_FRAC: f64 = 0.10;

/// Hill-climb with backtracking. Avoids the naive "multiply forever, never
/// revert" failure mode:
///
/// - **Global best is the baseline.** Every move is proposed *from the best
///   point seen*, never from the last (possibly degraded) probe. A bad probe
///   never advances the baseline.
/// - **Revert on regression.** A probe that fails to beat the best by
///   `improve_threshold` is discarded; its knob's step is **halved** and we
///   keep proposing from the best point.
/// - **Decaying, not growing, steps.** Steps start modest and only ever
///   shrink, so there's no runaway escalation. A knob is **saturated** once its
///   step underflows `MIN_STEP_FRAC` or it hits its cap.
/// - **Cross-knob CPU contention needs no special case.** Because the objective
///   is end-to-end read throughput, over-saturating cores shows up as a
///   regression and triggers the revert above.
pub struct HillClimbPolicy {
    cfg: PolicyConfig,
    max_trials: usize,
    /// Baseline knobs, emitted as the first (seed) trial.
    seed: Knobs,
    /// Current step fraction per knob (shrinks on non-improvement).
    steps: HashMap<KnobId, f64>,
    /// Knobs that can no longer be usefully increased.
    saturated: HashMap<KnobId, bool>,
    /// Best `(knobs, objective, dominant)` seen so far.
    best: Option<(Knobs, f64, &'static str)>,
    /// The knob whose bump we proposed last and are now awaiting the result
    /// for. `None` for the seed trial.
    pending: Option<KnobId>,
}

impl HillClimbPolicy {
    pub fn new(baseline: Knobs, cfg: PolicyConfig, max_trials: usize) -> Self {
        let mut steps = HashMap::new();
        steps.insert(KnobId::MaxParallel, INIT_STEP_LINEAR);
        steps.insert(KnobId::FilterTasks, INIT_STEP_LINEAR);
        steps.insert(KnobId::LineBuffer, INIT_STEP_BUFFER);
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

    fn cap(&self, k: KnobId) -> usize {
        match k {
            KnobId::MaxParallel => self.cfg.max_parallel_cap,
            KnobId::FilterTasks => self.cfg.filter_tasks_cap,
            KnobId::LineBuffer => self.cfg.line_buffer_cap,
        }
    }

    fn value(knobs: &Knobs, k: KnobId) -> usize {
        match k {
            KnobId::MaxParallel => knobs.max_parallel,
            KnobId::FilterTasks => knobs.filter_tasks,
            KnobId::LineBuffer => knobs.line_buffer_size,
        }
    }

    fn with_value(mut knobs: Knobs, k: KnobId, v: usize) -> Knobs {
        match k {
            KnobId::MaxParallel => knobs.max_parallel = v,
            KnobId::FilterTasks => knobs.filter_tasks = v,
            KnobId::LineBuffer => knobs.line_buffer_size = v,
        }
        knobs
    }

    fn is_saturated(&self, k: KnobId) -> bool {
        *self.saturated.get(&k).unwrap_or(&false)
    }

    /// Can this knob be increased from the best point (not yet at its cap)?
    fn can_increase(&self, k: KnobId) -> bool {
        if let Some((best, _, _)) = &self.best {
            Self::value(best, k) < self.cap(k)
        } else {
            false
        }
    }

    /// Map a bottleneck label to the knob that relieves it.
    fn knob_for(dominant: &str) -> KnobId {
        match dominant {
            "filter" => KnobId::FilterTasks,
            "download" => KnobId::MaxParallel,
            // sink_void / starvation / anything else → line buffer is the
            // secondary smoothing knob.
            _ => KnobId::LineBuffer,
        }
    }

    /// Compute the bumped value for a knob from the current best point.
    fn bumped(&self, k: KnobId) -> usize {
        let best = self.best.as_ref().expect("best set before bumping").0;
        let cur = Self::value(&best, k);
        let frac = *self.steps.get(&k).unwrap_or(&INIT_STEP_LINEAR);
        let raw = ((cur as f64) * (1.0 + frac)).ceil() as usize;
        raw.max(cur + 1).min(self.cap(k))
    }

    /// Fold the most recent trial result into the policy state: advance the
    /// best point on improvement, or shrink/saturate the probed knob's step on
    /// regression.
    fn absorb_latest(&mut self, latest: &Trial) {
        match self.best {
            None => {
                // Seed trial result.
                self.best = Some((latest.knobs, latest.objective, latest.dominant));
            }
            Some((_, prev_best_obj, _)) => {
                if latest.objective > prev_best_obj {
                    self.best = Some((latest.knobs, latest.objective, latest.dominant));
                }
                let improved =
                    latest.objective > prev_best_obj * (1.0 + self.cfg.improve_threshold);
                if !improved {
                    if let Some(k) = self.pending {
                        let step = self.steps.entry(k).or_insert(INIT_STEP_LINEAR);
                        *step /= 2.0;
                        if *step < MIN_STEP_FRAC {
                            self.saturated.insert(k, true);
                        }
                    }
                }
            }
        }
    }

    /// Choose the next knob to probe given the best point's dominant label.
    /// Returns `None` (→ `Done`) when nothing useful remains.
    fn pick_next_knob(&self) -> Option<KnobId> {
        let (_, _, dominant) = self.best.as_ref()?;
        let primary = Self::knob_for(dominant);
        if !self.is_saturated(primary) && self.can_increase(primary) {
            return Some(primary);
        }
        // Primary exhausted. When we're download-bound and `max_parallel` can't
        // grow, we've hit the network/S3 ceiling — stop rather than fiddle with
        // CPU-side knobs that won't move a download lid.
        if *dominant == "download" {
            return None;
        }
        // Otherwise fall back to the line buffer as a last-resort smoothing knob.
        if primary != KnobId::LineBuffer
            && !self.is_saturated(KnobId::LineBuffer)
            && self.can_increase(KnobId::LineBuffer)
        {
            return Some(KnobId::LineBuffer);
        }
        None
    }
}

impl TunePolicy for HillClimbPolicy {
    fn propose(&mut self, history: &[Trial]) -> Proposal {
        if history.len() >= self.max_trials {
            return Proposal::Done;
        }

        // First call: emit the seed (baseline) trial.
        if history.is_empty() {
            self.pending = None;
            return Proposal::Evaluate(self.seed);
        }

        // Fold in the result of our previous proposal.
        let latest = history.last().expect("non-empty history");
        self.absorb_latest(latest);

        // Decide the next move from the best point.
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
            max_parallel_cap: 512,
            filter_tasks_cap: 64,
            line_buffer_cap: 65536,
            improve_threshold: 0.03,
        }
    }

    fn baseline() -> Knobs {
        Knobs {
            max_parallel: 16,
            filter_tasks: 8,
            line_buffer_size: 1000,
        }
    }

    /// Drive a policy to completion against a synthetic evaluator
    /// `eval(knobs) -> (objective, dominant)`. Returns the full history and the
    /// global-best trial — mirroring what the real runner does.
    fn drive(
        mut policy: HillClimbPolicy,
        mut eval: impl FnMut(&Knobs) -> (f64, &'static str),
    ) -> (Vec<Trial>, Trial) {
        let mut history: Vec<Trial> = Vec::new();
        loop {
            match policy.propose(&history) {
                Proposal::Done => break,
                Proposal::Evaluate(knobs) => {
                    let (objective, dominant) = eval(&knobs);
                    history.push(Trial {
                        knobs,
                        objective,
                        dominant,
                    });
                }
            }
        }
        let best = history
            .iter()
            .cloned()
            .max_by(|a, b| a.objective.partial_cmp(&b.objective).unwrap())
            .expect("at least the seed trial ran");
        (history, best)
    }

    #[test]
    fn improving_then_plateau_terminates_with_best() {
        // filter_tasks helps up to 32, then download becomes the lid and more
        // max_parallel helps up to 64, then everything plateaus.
        let policy = HillClimbPolicy::new(baseline(), cfg(), 50);
        let (history, best) = drive(policy, |k| {
            let f = (k.filter_tasks.min(32)) as f64; // saturates at 32
            let p = (k.max_parallel.min(64)) as f64; // saturates at 64
            let obj = f * 10.0 + p * 5.0;
            // Whichever side is still below its knee is the lid.
            let dominant = if k.filter_tasks < 32 {
                "filter"
            } else {
                "download"
            };
            (obj, dominant)
        });
        assert!(history.len() >= 3, "should probe several times");
        // Best must be at least as good as the seed.
        assert!(best.objective >= history[0].objective);
        // Best knobs never below baseline (we only ever increase).
        assert!(best.knobs.filter_tasks >= baseline().filter_tasks);
        assert!(best.knobs.max_parallel >= baseline().max_parallel);
    }

    #[test]
    fn regression_does_not_move_baseline() {
        // Any increase to filter_tasks REGRESSES throughput (CPU contention).
        // The policy must keep the baseline as the best and never adopt a worse
        // point.
        let policy = HillClimbPolicy::new(baseline(), cfg(), 50);
        let base_obj = 1000.0;
        let (history, best) = drive(policy, |k| {
            // Seed scores base_obj; any filter bump is strictly worse.
            if k.filter_tasks == baseline().filter_tasks
                && k.max_parallel == baseline().max_parallel
                && k.line_buffer_size == baseline().line_buffer_size
            {
                (base_obj, "filter")
            } else {
                // Worse the more we deviate; dominant stays "filter" so the
                // policy keeps trying filter (with shrinking steps) then gives
                // up.
                (base_obj - k.filter_tasks as f64, "filter")
            }
        });
        assert_eq!(
            best.knobs,
            baseline(),
            "baseline must remain best under pure regression"
        );
        assert_eq!(best.objective, base_obj);
        // It must terminate (saturate), not loop forever.
        assert!(history.len() < 50);
    }

    #[test]
    fn download_bound_stops_when_max_parallel_saturates() {
        // Always download-bound; max_parallel never helps. Policy should bump
        // max_parallel a few times (shrinking), then STOP — never touching
        // filter_tasks (download-bound terminal rule).
        let policy = HillClimbPolicy::new(baseline(), cfg(), 50);
        let (history, best) = drive(policy, |_k| (500.0, "download"));
        assert_eq!(
            best.knobs.filter_tasks,
            baseline().filter_tasks,
            "filter_tasks must be untouched when download-bound"
        );
        assert_eq!(
            best.knobs.line_buffer_size,
            baseline().line_buffer_size,
            "line buffer must be untouched when download-bound"
        );
        assert!(
            history.len() < 50,
            "must terminate via saturation, not max_trials"
        );
    }

    #[test]
    fn respects_caps() {
        let mut c = cfg();
        c.max_parallel_cap = 20; // baseline max_parallel is 16
        c.filter_tasks_cap = 8; // == baseline → can't increase at all
        let policy = HillClimbPolicy::new(baseline(), c, 50);
        let (history, _best) = drive(policy, |_k| (500.0, "download"));
        for t in &history {
            assert!(t.knobs.max_parallel <= 20, "cap must hold: {:?}", t.knobs);
            assert!(t.knobs.filter_tasks <= 8);
        }
    }

    #[test]
    fn max_trials_is_a_hard_ceiling() {
        // Strictly increasing objective forever would never plateau; max_trials
        // must still bound the run.
        let policy = HillClimbPolicy::new(baseline(), cfg(), 5);
        let (history, _best) = drive(policy, |k| {
            (k.filter_tasks as f64 + k.max_parallel as f64, "filter")
        });
        assert_eq!(history.len(), 5);
    }
}

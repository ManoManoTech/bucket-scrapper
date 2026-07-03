//! Host CPU-saturation signals for the control plane, chosen to avoid the
//! "100% CPU is misleading" trap (Brendan Gregg): `/proc/stat` %CPU counts
//! stall cycles as busy, so on a memory-bound workload it reads ~100% while
//! cores are actually waiting on memory and there may be headroom (or none).
//!
//! We surface, cheapest-first:
//! - **PSI** (Pressure Stall Information) — `cpu`/`memory.pressure` `some avg10`
//!   / `avg60`. The scheduler-level saturation signal done right: the fraction
//!   of wall-time at least one runnable task was stalled waiting for the
//!   resource. Normalized 0–100%, ~10s responsiveness, no privileges. Read
//!   from the cgroup-v2 file first (container-scoped) with a `/proc/pressure`
//!   fallback (host-scoped).
//! - **%CPU** from `/proc/stat` — familiar but weak; surfaced *beside* PSI so
//!   the divergence is visible (100% busy + low pressure ⇒ stalled, not
//!   saturated).
//!
//! IPC via perf counters would be the highest-fidelity "is that 100% real"
//! signal, but `perf_event_open` needs `perf_event_paranoid <= 1` /
//! `CAP_PERFMON`, routinely denied in containers — deliberately out of scope
//! here; PSI covers the saturation question without privileges.

use std::sync::atomic::{AtomicU64, Ordering};

/// PSI `some` averages for one resource (percentages, 0–100).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Pressure {
    pub some_avg10: f64,
    pub some_avg60: f64,
}

/// Read cgroup-v2 `cpu.pressure` (container-scoped), falling back to the
/// host-wide `/proc/pressure/cpu`. `None` when PSI is unavailable.
pub fn cpu_pressure() -> Option<Pressure> {
    read_pressure("/sys/fs/cgroup/cpu.pressure", "/proc/pressure/cpu")
}

/// Read cgroup-v2 `memory.pressure`, falling back to `/proc/pressure/memory`.
/// High memory pressure marks a bandwidth/allocation-bound regime — for the
/// zstd + per-line-allocation pipeline, the cue to shrink concurrency rather
/// than grow it.
pub fn memory_pressure() -> Option<Pressure> {
    read_pressure("/sys/fs/cgroup/memory.pressure", "/proc/pressure/memory")
}

fn read_pressure(cgroup_path: &str, proc_path: &str) -> Option<Pressure> {
    let content = std::fs::read_to_string(cgroup_path)
        .or_else(|_| std::fs::read_to_string(proc_path))
        .ok()?;
    parse_pressure(&content)
}

/// Parse the `some` line of a PSI file:
/// `some avg10=0.42 avg60=0.11 avg300=0.03 total=123456`.
fn parse_pressure(content: &str) -> Option<Pressure> {
    let some = content.lines().find(|l| l.starts_with("some "))?;
    let field = |key: &str| -> Option<f64> {
        some.split_whitespace()
            .find_map(|tok| tok.strip_prefix(key))
            .and_then(|v| v.parse().ok())
    };
    Some(Pressure {
        some_avg10: field("avg10=")?,
        some_avg60: field("avg60=")?,
    })
}

/// Sampling meter for `/proc/stat` CPU busy-fraction over an interval. The
/// value is meaningless from a single read (it needs two points), so a caller
/// samples it periodically and stores the latest.
pub struct CpuMeter {
    last: Option<(u64, u64)>, // (total_jiffies, idle_jiffies)
}

impl Default for CpuMeter {
    fn default() -> Self {
        Self::new()
    }
}

impl CpuMeter {
    pub fn new() -> Self {
        Self { last: None }
    }

    /// Busy % since the previous call, or `None` on the first call / when
    /// `/proc/stat` is unreadable / when no time elapsed.
    pub fn sample(&mut self) -> Option<f64> {
        let line = std::fs::read_to_string("/proc/stat").ok()?;
        let (total, idle) = parse_proc_stat_cpu(line.lines().next()?)?;
        let prev = self.last.replace((total, idle));
        let (ptotal, pidle) = prev?;
        busy_pct(ptotal, pidle, total, idle)
    }
}

/// Aggregate-CPU line: `cpu  user nice system idle iowait irq softirq steal …`.
/// Returns `(total_jiffies, idle_jiffies)` where idle includes iowait.
fn parse_proc_stat_cpu(line: &str) -> Option<(u64, u64)> {
    let mut it = line.split_whitespace();
    if it.next()? != "cpu" {
        return None;
    }
    let vals: Vec<u64> = it.filter_map(|t| t.parse().ok()).collect();
    if vals.len() < 5 {
        return None;
    }
    // Sum user..steal only. `guest`/`guest_nice` (fields 9–10) are already
    // included in `user`/`nice` by the kernel, so summing them too would
    // double-count guest time and inflate the denominator on VM hosts.
    let total: u64 = vals.iter().take(8).sum();
    let idle = vals[3] + vals[4]; // idle + iowait
    Some((total, idle))
}

/// Busy fraction over the delta between two `/proc/stat` reads.
fn busy_pct(ptotal: u64, pidle: u64, total: u64, idle: u64) -> Option<f64> {
    let dtotal = total.saturating_sub(ptotal);
    let didle = idle.saturating_sub(pidle);
    if dtotal == 0 {
        return None;
    }
    Some((dtotal.saturating_sub(didle) as f64) / dtotal as f64 * 100.0)
}

/// f64 stored as bits in an atomic; used by the control server's sampler to
/// publish the latest %CPU for `status` to read. `NaN` sentinel ⇒ "no reading".
pub fn store_f64(cell: &AtomicU64, v: Option<f64>) {
    cell.store(v.unwrap_or(f64::NAN).to_bits(), Ordering::Relaxed);
}

pub fn load_f64(cell: &AtomicU64) -> Option<f64> {
    let v = f64::from_bits(cell.load(Ordering::Relaxed));
    (!v.is_nan()).then_some(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pressure_reads_some_line() {
        let s = "some avg10=0.42 avg60=1.50 avg300=0.03 total=123456\n\
                 full avg10=0.10 avg60=0.20 avg300=0.00 total=999";
        let p = parse_pressure(s).unwrap();
        assert!((p.some_avg10 - 0.42).abs() < 1e-9);
        assert!((p.some_avg60 - 1.50).abs() < 1e-9);
    }

    #[test]
    fn parse_pressure_none_without_some_line() {
        assert!(parse_pressure("full avg10=0.10 avg60=0.20 total=1").is_none());
        assert!(parse_pressure("").is_none());
    }

    #[test]
    fn parse_proc_stat_totals_and_idle() {
        // cpu user=100 nice=0 system=50 idle=800 iowait=50 irq=0 softirq=0 ...
        let (total, idle) = parse_proc_stat_cpu("cpu  100 0 50 800 50 0 0 0 0 0").unwrap();
        assert_eq!(total, 1000);
        assert_eq!(idle, 850); // 800 + 50
    }

    #[test]
    fn parse_proc_stat_excludes_guest_double_count() {
        // guest=7, guest_nice=3 (fields 9–10) are already folded into user/nice
        // upstream — they must not be added again.
        let (total, idle) = parse_proc_stat_cpu("cpu 100 0 50 800 50 0 0 0 7 3").unwrap();
        assert_eq!(total, 1000); // user..steal only, not +10
        assert_eq!(idle, 850);
    }

    #[test]
    fn parse_proc_stat_rejects_non_cpu_line() {
        assert!(parse_proc_stat_cpu("cpu0 1 2 3 4 5").is_none());
        assert!(parse_proc_stat_cpu("intr 1 2 3").is_none());
    }

    #[test]
    fn busy_pct_computes_over_delta() {
        // total +1000, idle +850 → busy 150/1000 = 15%.
        assert!((busy_pct(1000, 850, 2000, 1700).unwrap() - 15.0).abs() < 1e-9);
        assert!(busy_pct(1000, 850, 1000, 850).is_none()); // no elapsed jiffies
    }

    #[test]
    fn f64_atomic_roundtrip_and_none_sentinel() {
        let c = AtomicU64::new(0);
        store_f64(&c, Some(3.5));
        assert_eq!(load_f64(&c), Some(3.5));
        store_f64(&c, None);
        assert_eq!(load_f64(&c), None);
    }
}

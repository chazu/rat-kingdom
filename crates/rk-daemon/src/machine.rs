//! The machine-aware admission signal (P3a): the scarce *physical* resources a
//! spawn competes for on this castle, sampled as one value and evaluated
//! against one set of floors.
//!
//! # Why this exists
//!
//! The 2026-08-18 drain probe's only true SILENT stall (probe note O12) was a
//! resource refusal nobody could see: 231 GB of accumulated build artifacts
//! left 8.5 GB free, tripping `[disk] min_free_gb`. Gate verifies failed with
//! disk-floor panics that read as ordinary test failures, and the continuous
//! drain simply STOPPED dispatching. Nothing named the cause.
//!
//! Two lessons are encoded here:
//!
//! 1. **Disk is a first-class admission input, not an afterthought.** Admission
//!    used to weigh only cost (the budget cap) and concurrency (the fleet-WIP
//!    ceiling) — both bookkeeping counters the daemon owns. Physical capacity
//!    was checked in exactly one place, deep inside `Supervisor::spawn`, so
//!    every *pre*-spawn decision (notably the drain's preflight) was blind to
//!    the one resource that had actually run out.
//! 2. **A refusal is an event, not a silence.** Whatever refuses must say so
//!    through the notification sinks. See [`crate::recovery`] for the
//!    announce/rate-cap machinery this feeds.
//!
//! # Stack neutrality
//!
//! Nothing here knows what is *stored* on the disk or what is *running* on the
//! CPU. The signal is free bytes and load average — properties of the machine,
//! not of any language, toolchain, or build system. Reclaiming disk (which does
//! need per-repo knowledge of what is regenerable) is deliberately somebody
//! else's job, driven by a per-repo glob list in policy; this module only ever
//! *measures*.

use std::path::Path;

/// Bytes per gibibyte — the unit `[disk] min_free_gb` is expressed in.
pub const BYTES_PER_GB: u64 = 1024 * 1024 * 1024;

/// Which scarce resource ran out. Kept as a small enum rather than a string so
/// the announce class, the obstacle `type`, and the rate-cap key for each kind
/// are derived in one place and cannot drift apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceKind {
    /// Free space under `RK_HOME` fell below `[disk] min_free_gb`.
    Disk,
    /// 1-minute load average per CPU rose above `[machine] max_load_per_cpu`.
    Load,
}

impl ResourceKind {
    /// Sink-routing class for the escalation notice, and the stable name an
    /// operator greps for. Also the rate-cap key, so a sustained disk breach
    /// cannot suppress a *separate* load breach (and vice versa).
    pub fn class(self) -> &'static str {
        match self {
            ResourceKind::Disk => "disk-floor",
            ResourceKind::Load => "load-floor",
        }
    }

    /// `type` field of the `Category::Obstacle` tuple `rk inbox` renders.
    pub fn obstacle_type(self) -> &'static str {
        match self {
            ResourceKind::Disk => "disk_pressure",
            ResourceKind::Load => "load_pressure",
        }
    }

    /// Obstacle tuple identity (the inbox row's subject).
    pub fn obstacle_identity(self) -> &'static str {
        match self {
            ResourceKind::Disk => "disk-pressure",
            ResourceKind::Load => "load-pressure",
        }
    }
}

/// One sample of this castle's physical capacity.
///
/// Sampled as a unit so a refusal for *either* reason can report *both*
/// numbers: the probe's post-mortem was slowed by having the disk figure
/// without the contemporaneous load figure, leaving it ambiguous whether the
/// stall was pressure or contention.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MachineSignal {
    /// Free bytes available to this (non-root) user under `RK_HOME`.
    pub free_disk_bytes: u64,
    /// 1-minute load average, or `None` where the platform declines to report
    /// one. Never fabricated as 0.0 — a missing reading must not look like an
    /// idle machine and silently satisfy a load floor.
    pub load_1m: Option<f64>,
    /// Usable parallelism, the denominator that makes load comparable across
    /// machines. `load_1m / cpus` is the portable "how oversubscribed am I"
    /// number; raw load average is meaningless without it.
    pub cpus: usize,
}

impl MachineSignal {
    /// Sample free disk under `path` plus this machine's load. Fails only if
    /// the disk reading fails — a load reading is best-effort, because losing
    /// it must degrade the signal rather than block admission entirely.
    pub fn sample(path: &Path) -> rk_core::Result<Self> {
        Ok(Self {
            free_disk_bytes: disk_free_bytes(path)?,
            load_1m: load_average_1m(),
            cpus: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
        })
    }

    /// Load per CPU, or `None` when the platform gave no load reading.
    pub fn load_per_cpu(&self) -> Option<f64> {
        self.load_1m.map(|l| l / self.cpus.max(1) as f64)
    }

    /// Free disk in GB, for operator-facing text.
    pub fn free_disk_gb(&self) -> f64 {
        self.free_disk_bytes as f64 / BYTES_PER_GB as f64
    }

    /// Both numbers in one operator-readable clause, appended to every refusal
    /// notice regardless of which floor tripped.
    pub fn summary(&self) -> String {
        match self.load_per_cpu() {
            Some(per_cpu) => format!(
                "{:.1} GB free, load {:.2}/cpu over {} cpus",
                self.free_disk_gb(),
                per_cpu,
                self.cpus
            ),
            None => format!("{:.1} GB free, load unavailable", self.free_disk_gb()),
        }
    }
}

/// The configured admission floors. Both dials are independently disable-able
/// with zero, and both default to disabled on a bare `Supervisor` so a test or
/// an embedding crate never inherits a machine-dependent refusal it did not ask
/// for.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct MachineFloors {
    /// `[disk] min_free_gb`. Zero disables.
    pub min_free_disk_gb: u64,
    /// `[machine] max_load_per_cpu`. Zero disables.
    pub max_load_per_cpu: f64,
}

impl MachineFloors {
    /// True when neither floor is armed, letting a caller skip sampling
    /// entirely — `statvfs` on every spawn of a fully-disabled guard is pure
    /// waste.
    pub fn disabled(&self) -> bool {
        self.min_free_disk_gb == 0 && self.max_load_per_cpu <= 0.0
    }

    /// Evaluate a sample. `None` = admit.
    ///
    /// Disk is checked first and wins ties: running the disk to zero takes the
    /// *daemon itself* down (the 2026-08-16 incident's "terminal state
    /// persistence failed: io"), whereas an overloaded machine only runs slow.
    /// A missing load reading admits rather than refuses, for the same reason
    /// [`MachineSignal::load_1m`] is an `Option` — an unmeasurable resource is
    /// not an exhausted one.
    pub fn evaluate(&self, signal: &MachineSignal) -> Option<ResourceRefusal> {
        if self.min_free_disk_gb > 0 {
            let floor_bytes = self.min_free_disk_gb.saturating_mul(BYTES_PER_GB);
            if signal.free_disk_bytes < floor_bytes {
                return Some(ResourceRefusal {
                    kind: ResourceKind::Disk,
                    signal: *signal,
                    detail: format!(
                        "only {:.1} GB free — below the configured floor of {} GB \
                         ([disk] min_free_gb)",
                        signal.free_disk_gb(),
                        self.min_free_disk_gb
                    ),
                    action: "free disk under RK_HOME, e.g. `rk prune --reap-git \
                             --reap-artifacts`, or raise [disk] min_free_gb"
                        .to_string(),
                });
            }
        }
        if self.max_load_per_cpu > 0.0 {
            if let Some(per_cpu) = signal.load_per_cpu() {
                if per_cpu > self.max_load_per_cpu {
                    return Some(ResourceRefusal {
                        kind: ResourceKind::Load,
                        signal: *signal,
                        detail: format!(
                            "load is {per_cpu:.2} per cpu ({} cpus) — above the configured \
                             ceiling of {:.2} ([machine] max_load_per_cpu)",
                            signal.cpus, self.max_load_per_cpu
                        ),
                        action: "wait for in-flight work to drain, lower [drain] max_wip, \
                                 or raise [machine] max_load_per_cpu"
                            .to_string(),
                    });
                }
            }
        }
        None
    }
}

/// A refusal to admit new work because the machine is out of a resource.
///
/// Carries everything the three consumers need — the error returned to the
/// caller, the obstacle tuple `rk inbox` renders, and the escalation notice the
/// sinks fan out — so all three describe the same event in the same words.
#[derive(Debug, Clone, PartialEq)]
pub struct ResourceRefusal {
    pub kind: ResourceKind,
    /// The sample that tripped the floor, reported in full.
    pub signal: MachineSignal,
    /// What was exceeded, in operator-facing terms.
    pub detail: String,
    /// What the operator can do about it.
    pub action: String,
}

impl ResourceRefusal {
    /// The message shared by the spawn error, the obstacle, and the notice.
    pub fn message(&self, home: &Path) -> String {
        format!(
            "{RESOURCE_REFUSAL_PREFIX}{} (under {}; {})",
            self.detail,
            home.display(),
            self.signal.summary()
        )
    }
}

/// Stable prefix on every [`ResourceRefusal::message`], so a caller can tell
/// "the machine is out of a resource, the work item itself is fine" from a
/// genuine spawn failure. Matched by name rather than by widening
/// [`rk_core::Error`] for one internal admission signal — the same convention
/// `FLEET_WIP_CAP_REFUSED` follows.
pub const RESOURCE_REFUSAL_PREFIX: &str = "spawn refused: ";

/// True when `error` is a machine-capacity refusal rather than a real failure.
/// A caller seeing this should put the work item back, not mark it broken.
pub fn is_resource_refusal(error: &rk_core::Error) -> bool {
    matches!(error, rk_core::Error::Other(msg) if msg.starts_with(RESOURCE_REFUSAL_PREFIX))
}

/// Bytes of free space available to the current (non-root) user on the
/// filesystem containing `path` — `f_bavail`, not the possibly-larger
/// root-only `f_bfree`, matching what `df` reports as "available" and the
/// number that actually bounds a new worktree checkout.
#[cfg(unix)]
pub fn disk_free_bytes(path: &Path) -> rk_core::Result<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let c_path = CString::new(path.as_os_str().as_bytes()).map_err(|e| {
        rk_core::Error::other(format!("disk floor check: invalid path {path:?}: {e}"))
    })?;
    // SAFETY: statvfs writes into a single stack-allocated struct owned for
    // the duration of this call; the C string it reads from outlives the call.
    unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c_path.as_ptr(), &mut stat) != 0 {
            return Err(rk_core::Error::other(format!(
                "disk floor check: statvfs({}) failed: {}",
                path.display(),
                std::io::Error::last_os_error()
            )));
        }
        Ok(stat.f_bavail as u64 * stat.f_frsize as u64)
    }
}

#[cfg(not(unix))]
pub fn disk_free_bytes(_path: &Path) -> rk_core::Result<u64> {
    Err(rk_core::Error::other(
        "disk floor check: unsupported platform",
    ))
}

/// 1-minute load average, or `None` where unavailable. Best-effort by design:
/// see [`MachineSignal::load_1m`].
#[cfg(unix)]
fn load_average_1m() -> Option<f64> {
    let mut avg = [0f64; 3];
    // SAFETY: getloadavg writes at most `avg.len()` doubles into the array we
    // own for the duration of the call.
    let n = unsafe { libc::getloadavg(avg.as_mut_ptr(), avg.len() as libc::c_int) };
    if n < 1 {
        return None;
    }
    // A negative reading is not physically meaningful; treat it as no reading
    // rather than as an idle machine.
    if avg[0] < 0.0 {
        return None;
    }
    Some(avg[0])
}

#[cfg(not(unix))]
fn load_average_1m() -> Option<f64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signal(free_gb: u64, load: Option<f64>, cpus: usize) -> MachineSignal {
        MachineSignal {
            free_disk_bytes: free_gb * BYTES_PER_GB,
            load_1m: load,
            cpus,
        }
    }

    #[test]
    fn zero_floors_are_disabled_and_admit_an_exhausted_machine() {
        let floors = MachineFloors::default();
        assert!(floors.disabled());
        assert_eq!(floors.evaluate(&signal(0, Some(999.0), 8)), None);
    }

    #[test]
    fn disk_below_floor_refuses() {
        let floors = MachineFloors {
            min_free_disk_gb: 10,
            max_load_per_cpu: 0.0,
        };
        let refusal = floors.evaluate(&signal(8, Some(0.1), 8)).expect("refusal");
        assert_eq!(refusal.kind, ResourceKind::Disk);
        assert!(refusal.detail.contains("min_free_gb"), "{}", refusal.detail);
    }

    #[test]
    fn disk_exactly_at_floor_admits() {
        // The floor is a minimum, not a strict inequality: at exactly the
        // configured headroom there is, by definition, enough headroom.
        let floors = MachineFloors {
            min_free_disk_gb: 10,
            max_load_per_cpu: 0.0,
        };
        assert_eq!(floors.evaluate(&signal(10, Some(0.1), 8)), None);
    }

    #[test]
    fn load_above_ceiling_refuses_and_is_normalised_per_cpu() {
        let floors = MachineFloors {
            min_free_disk_gb: 0,
            max_load_per_cpu: 2.0,
        };
        // 24.0 across 8 cpus = 3.0/cpu — over the ceiling.
        let refusal = floors
            .evaluate(&signal(500, Some(24.0), 8))
            .expect("refusal");
        assert_eq!(refusal.kind, ResourceKind::Load);
        // The SAME raw load on a bigger machine is under the ceiling, which is
        // the whole point of normalising: 24.0 across 16 cpus = 1.5/cpu.
        assert_eq!(floors.evaluate(&signal(500, Some(24.0), 16)), None);
    }

    #[test]
    fn missing_load_reading_admits_rather_than_refusing() {
        let floors = MachineFloors {
            min_free_disk_gb: 0,
            max_load_per_cpu: 0.5,
        };
        assert_eq!(floors.evaluate(&signal(500, None, 8)), None);
    }

    #[test]
    fn disk_wins_when_both_floors_are_breached() {
        // Disk exhaustion takes the daemon down; load only makes it slow.
        let floors = MachineFloors {
            min_free_disk_gb: 10,
            max_load_per_cpu: 1.0,
        };
        let refusal = floors.evaluate(&signal(1, Some(99.0), 8)).expect("refusal");
        assert_eq!(refusal.kind, ResourceKind::Disk);
    }

    #[test]
    fn a_refusal_reports_both_resources_not_just_the_one_that_tripped() {
        // O12's post-mortem cost: the disk number without the contemporaneous
        // load number left the stall ambiguous.
        let floors = MachineFloors {
            min_free_disk_gb: 10,
            max_load_per_cpu: 0.0,
        };
        let refusal = floors.evaluate(&signal(8, Some(16.0), 8)).expect("refusal");
        let msg = refusal.message(Path::new("/tmp/rk-home"));
        assert!(msg.contains("8.0 GB free"), "{msg}");
        assert!(msg.contains("2.00/cpu"), "{msg}");
        assert!(msg.contains("/tmp/rk-home"), "{msg}");
    }

    #[test]
    fn a_resource_refusal_is_distinguishable_from_a_real_spawn_failure() {
        // Drain reopens a ticket on the former and strands it on the latter,
        // so this predicate is load-bearing for backlog integrity.
        let floors = MachineFloors {
            min_free_disk_gb: 10,
            max_load_per_cpu: 0.0,
        };
        let refusal = floors.evaluate(&signal(1, Some(1.0), 8)).expect("refusal");
        let err = rk_core::Error::other(refusal.message(Path::new("/tmp/rk-home")));
        assert!(is_resource_refusal(&err));
        assert!(!is_resource_refusal(&rk_core::Error::other(
            "unknown harness profile: nope"
        )));
    }

    #[test]
    fn kinds_have_distinct_classes_so_one_breach_cannot_mask_the_other() {
        assert_ne!(ResourceKind::Disk.class(), ResourceKind::Load.class());
        assert_ne!(
            ResourceKind::Disk.obstacle_type(),
            ResourceKind::Load.obstacle_type()
        );
    }

    #[test]
    fn sampling_a_real_path_reads_disk_and_reports_cpus() {
        let signal = MachineSignal::sample(Path::new(".")).expect("sample");
        assert!(
            signal.free_disk_bytes > 0,
            "a live filesystem has free space"
        );
        assert!(signal.cpus >= 1);
    }
}

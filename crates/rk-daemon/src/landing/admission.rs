//! Capacity waiting and explicit operator recovery. Old verdicts are immutable.
use super::*;

pub(super) const ADMISSION_HOLD_IDENTITY: &str = "landing_admission_hold";
const ADMISSION_WAIT_IDENTITY: &str = "landing_admission_wait";
const ADMISSION_RECOVERY_IDENTITY: &str = "landing_admission_recovery";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct BootClock {
    boot: String,
    nanos: u64,
}

fn boot_clock() -> Option<BootClock> {
    #[cfg(target_os = "linux")]
    let (boot, clock) = (
        std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?,
        libc::CLOCK_BOOTTIME,
    );
    #[cfg(target_os = "macos")]
    let (boot, clock) = {
        let mut bytes = [0u8; 64];
        let mut length = bytes.len();
        // The kernel boot UUID distinguishes reboot from wall-clock adjustment.
        let result = unsafe {
            libc::sysctlbyname(
                c"kern.bootsessionuuid".as_ptr(),
                bytes.as_mut_ptr().cast(),
                &mut length,
                std::ptr::null_mut(),
                0,
            )
        };
        if result != 0 || length > bytes.len() {
            return None;
        }
        let boot = std::str::from_utf8(&bytes[..length])
            .ok()?
            .trim_end_matches('\0')
            .to_string();
        (boot, libc::CLOCK_MONOTONIC_RAW)
    };
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    return None;
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let mut stamp = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        if unsafe { libc::clock_gettime(clock, &mut stamp) } != 0 {
            return None;
        }
        let nanos = u64::try_from(stamp.tv_sec)
            .ok()?
            .checked_mul(1_000_000_000)?
            .checked_add(u64::try_from(stamp.tv_nsec).ok()?)?;
        Some(BootClock { boot, nanos })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct AdmissionWindow {
    #[serde(default)]
    clock: Option<BootClock>,
    #[serde(default)]
    deadline_nanos: u64,
    started_at: DateTime<Utc>,
    deadline: DateTime<Utc>,
    observed_at: DateTime<Utc>,
}

impl AdmissionWindow {
    pub(super) fn new(budget: Duration) -> Self {
        let now = Utc::now();
        let clock = boot_clock();
        let deadline_nanos = clock
            .as_ref()
            .map(|c| {
                c.nanos
                    .saturating_add(u64::try_from(budget.as_nanos()).unwrap_or(u64::MAX))
            })
            .unwrap_or(0);
        Self {
            clock,
            deadline_nanos,
            started_at: now,
            observed_at: now,
            deadline: now + chrono::Duration::from_std(budget).unwrap_or(chrono::Duration::MAX),
        }
    }

    fn charge(&mut self, elapsed: Duration, now: DateTime<Utc>) {
        let before = (self.deadline - self.observed_at)
            .to_std()
            .unwrap_or(Duration::ZERO);
        let left = before.saturating_sub(elapsed);
        // A backward wall-clock adjustment during an await cannot refund the
        // monotonic time already consumed. The original deadline never grows.
        self.deadline = self
            .deadline
            .min(now + chrono::Duration::from_std(left).unwrap_or(chrono::Duration::MAX));
        self.observed_at = self.observed_at.max(now);
    }

    fn remaining(&mut self) -> Duration {
        self.remaining_at(Utc::now(), boot_clock())
    }

    fn remaining_at(&mut self, now: DateTime<Utc>, clock: Option<BootClock>) -> Duration {
        let Some((anchor, current)) = self.clock.as_ref().zip(clock.as_ref()) else {
            return Duration::ZERO;
        };
        if anchor.boot != current.boot || current.nanos < anchor.nanos {
            return Duration::ZERO;
        }
        let monotonic = Duration::from_nanos(self.deadline_nanos.saturating_sub(current.nanos));
        if now < self.observed_at {
            self.deadline = self.deadline.min(self.observed_at);
            return Duration::ZERO;
        }
        self.observed_at = now;
        monotonic.min((self.deadline - now).to_std().unwrap_or(Duration::ZERO))
    }
}

fn invalid(message: impl Into<String>) -> rk_core::Error {
    rk_core::Error::other(message.into())
}

impl LandingPipeline {
    pub(super) fn charge_admission_elapsed(
        &self,
        entry: &mut LandingQueueEntry,
        elapsed: Duration,
    ) -> rk_core::Result<()> {
        if let Some(window) = &mut entry.admission {
            window.charge(elapsed, Utc::now());
        }
        self.queue.persist(entry, LandingEntryStatus::RunningGates)
    }

    pub(super) fn admission_remaining(
        &self,
        entry: &mut LandingQueueEntry,
        check: &str,
        candidate: &str,
    ) -> rk_core::Result<Duration> {
        let remaining = entry
            .admission
            .as_mut()
            .ok_or_else(|| invalid("missing admission window"))?
            .remaining();
        self.queue
            .persist(entry, LandingEntryStatus::RunningGates)?;
        self.space.out(
            Tuple::new(
                Category::Event,
                &entry.repo_name,
                ADMISSION_WAIT_IDENTITY,
                "daemon",
                json!({"entry": entry, "check": check, "candidate_sha": candidate,
                "state": "waiting", "executed": false, "remaining_ms": remaining.as_millis()}),
            )
            .with_lifecycle(Lifecycle::Furniture),
        )?;
        Ok(remaining.min(entry.admission.as_mut().unwrap().remaining()))
    }

    pub(super) fn record_admission_hold(
        &self,
        entry: &mut LandingQueueEntry,
        check: &str,
        candidate: &str,
        outcome: &rk_core::Result<Value>,
    ) -> rk_core::Result<bool> {
        let Ok(result) = outcome else {
            return Ok(false);
        };
        if result["executed"] != false || result["reason"] != "admission-timeout" {
            return Ok(false);
        }
        let prior = self
            .space
            .scan(
                &Pattern::category(Category::Event)
                    .scope(&entry.repo_name)
                    .identity(ADMISSION_HOLD_IDENTITY),
            )?
            .into_iter()
            .find(|t| {
                t.payload["entry"]["seq"] == entry.seq
                    && t.payload["entry"]["head_sha"] == entry.head_sha
                    && t.payload["entry"]["branch"] == entry.branch
                    && t.payload["entry"]["target"] == entry.target
                    && t.payload["candidate_sha"] == candidate
                    && t.payload["check"] == check
                    && t.payload["entry"]["admission_recovery"] == json!(entry.admission_recovery)
            });
        let hold = if let Some(prior) = prior {
            prior
        } else {
            let mut hold = Tuple::new(
                Category::Event,
                &entry.repo_name,
                ADMISSION_HOLD_IDENTITY,
                "daemon",
                Value::Null,
            )
            .with_lifecycle(Lifecycle::Furniture);
            entry.admission_hold = Some(hold.id.to_string());
            hold.payload = json!({"entry": entry, "check": check, "candidate_sha": candidate,
                "result": result, "state": "admission-held", "retry_eligible": !entry.gate_infra_retry_used && entry.batch_branches.is_empty()});
            self.space.out(hold.clone())?;
            hold
        };
        entry.admission_hold = Some(hold.id.to_string());
        self.queue
            .persist(entry, LandingEntryStatus::RunningGates)?;
        Ok(true)
    }

    fn admission_receipt(&self, hold: &str) -> rk_core::Result<Option<Tuple>> {
        Ok(self
            .space
            .scan(&Pattern::category(Category::Event).identity(ADMISSION_RECOVERY_IDENTITY))?
            .into_iter()
            .find(|t| t.payload["hold"].as_str() == Some(hold)))
    }

    fn receipt_entry(receipt: &Tuple) -> rk_core::Result<LandingQueueEntry> {
        serde_json::from_value(receipt.payload["entry"].clone())
            .map_err(|e| invalid(format!("invalid recovery receipt: {e}")))
    }

    fn receipt_matches(entry: &LandingQueueEntry, receipt: &Tuple) -> rk_core::Result<bool> {
        let authorized = Self::receipt_entry(receipt)?;
        Ok(entry.repo_name == authorized.repo_name
            && entry.repo_path == authorized.repo_path
            && entry.branch == authorized.branch
            && entry.head_sha == authorized.head_sha
            && entry.target == authorized.target
            && entry.task == authorized.task
            && entry.source_spawn == authorized.source_spawn
            && entry.candidate_sha == authorized.candidate_sha
            && entry.candidate_base == authorized.candidate_base
            && entry.candidate_ref == authorized.candidate_ref
            && entry
                .admission
                .as_ref()
                .zip(authorized.admission.as_ref())
                .is_some_and(|(actual, granted)| {
                    actual.started_at == granted.started_at
                        && actual.deadline <= granted.deadline
                        && actual.clock == granted.clock
                        && actual.deadline_nanos == granted.deadline_nanos
                })
            && entry.admission_recovery.as_deref() == Some(receipt.id.to_string().as_str()))
    }

    pub(super) fn recovery_supersedes(
        &self,
        entry: &LandingQueueEntry,
        marker: &Tuple,
    ) -> rk_core::Result<bool> {
        let Some(recovery) = &entry.admission_recovery else {
            return Ok(false);
        };
        let Some(receipt) = self.space.get(
            recovery
                .parse()
                .map_err(|_| invalid("invalid recovery id"))?,
        )?
        else {
            return Err(invalid("missing recovery receipt"));
        };
        if receipt.identity != ADMISSION_RECOVERY_IDENTITY
            || receipt.instance != "operator"
            || !Self::receipt_matches(entry, &receipt)?
        {
            return Err(invalid("landing entry disagrees with authorized recovery"));
        }
        Ok(receipt.payload["processed"] == marker.id.to_string()
            && marker.payload["admission_hold"] == receipt.payload["hold"]
            && marker.payload["outcome"] == "gate-held")
    }

    pub(super) fn validate_recovery_refs(&self, entry: &LandingQueueEntry) -> rk_core::Result<()> {
        let repo = rk_git::Repo::discover(Path::new(&entry.repo_path))?;
        if self.supervisor.repository_name(&repo)? != entry.repo_name
            || repo.rev_parse(&entry.branch)? != entry.head_sha
            || Some(repo.rev_parse(&entry.target)?) != entry.candidate_base
            || entry
                .candidate_ref
                .as_deref()
                .map(|r| repo.rev_parse(r))
                .transpose()?
                != entry.candidate_sha
            || entry.candidate_sha.is_none()
            || entry.candidate_base.is_none()
            || entry.candidate_ref.is_none()
        {
            return Err(invalid(
                "admission recovery repository/source/target/candidate fence changed",
            ));
        }
        if let Some(error) = self.invalid_source_identity(entry)? {
            return Err(invalid(error));
        }
        let ticket = self.tickets.resolve(&entry.task)?;
        if ticket.as_ref().is_some_and(|t| t.scope != entry.repo_name)
            || (entry.task.starts_with("TKT-") && ticket.is_none())
        {
            return Err(invalid(
                "admission recovery task does not belong to this repository",
            ));
        }
        Ok(())
    }

    /// Enqueue exactly one attempt per typed admission hold. Its immutable receipt
    /// is written first, so a restart can complete queue insertion without a grant.
    pub(crate) fn retry_admission(
        &self,
        repo_path: &Path,
        hold_id: &str,
        reason: &str,
    ) -> rk_core::Result<Value> {
        if reason.trim().is_empty() {
            return Err(invalid("admission recovery requires an operator reason"));
        }
        let _guard = self.enqueue_lock.lock().unwrap_or_else(|p| p.into_inner());
        let hold = self
            .space
            .get(
                hold_id
                    .parse()
                    .map_err(|_| invalid("invalid admission hold id"))?,
            )?
            .ok_or_else(|| {
                invalid("admission hold not found; legacy gate-held markers are not eligible")
            })?;
        if hold.instance != "daemon"
            || hold.identity != ADMISSION_HOLD_IDENTITY
            || hold.category != Category::Event
            || hold.payload["retry_eligible"] != true
            || hold.payload["result"]["executed"] != false
            || hold.payload["result"]["reason"] != "admission-timeout"
        {
            return Err(invalid("only a typed admission-only hold permits recovery"));
        }
        let mut entry: LandingQueueEntry = serde_json::from_value(hold.payload["entry"].clone())
            .map_err(|e| invalid(format!("invalid admission hold evidence: {e}")))?;
        if Path::new(&entry.repo_path).canonicalize()? != repo_path.canonicalize()?
            || hold.scope != entry.repo_name
            || entry.admission_hold.as_deref() != Some(hold_id)
            || entry.gate_infra_retry_used
            || !entry.batch_branches.is_empty()
        {
            return Err(invalid(
                "admission hold repository or retry identity mismatch",
            ));
        }
        if let Some(receipt) = self.admission_receipt(hold_id)? {
            return self.resume_admission_receipt(&receipt);
        }
        self.validate_recovery_refs(&entry)?;
        let marker = self
            .processed_marker(&entry)?
            .ok_or_else(|| invalid("admission hold is not terminal yet"))?;
        if marker.payload["admission_hold"] != hold_id || marker.payload["outcome"] != "gate-held" {
            return Err(invalid(
                "admission hold was superseded or already delivered",
            ));
        }
        let repo = rk_git::Repo::discover(repo_path)?;
        entry.admission = Some(AdmissionWindow::new(self.gate_config(&repo)?.gate_timeout));
        entry.admission_hold = None;
        entry.seq = 0;
        entry.rev = 0;
        entry.operator_fast_lane = true;
        entry.keep_branch = true;
        let mut receipt = Tuple::new(
            Category::Event,
            &entry.repo_name,
            ADMISSION_RECOVERY_IDENTITY,
            "operator",
            Value::Null,
        )
        .with_lifecycle(Lifecycle::Furniture);
        entry.admission_recovery = Some(receipt.id.to_string());
        receipt.payload = json!({"hold": hold_id, "processed": marker.id.to_string(), "reason": reason.trim(), "entry": entry});
        self.space.out(receipt.clone())?;
        self.resume_admission_receipt(&receipt)
    }

    fn resume_admission_receipt(&self, receipt: &Tuple) -> rk_core::Result<Value> {
        let entry = Self::receipt_entry(receipt)?;
        if let Some(marker) = self.processed_marker(&entry)? {
            if marker.payload["admission_recovery"] == receipt.id.to_string() {
                return Ok(
                    json!({"recovery": receipt.id.to_string(), "status": marker.payload["outcome"],
                    "merged": marker.payload["outcome"] == "landed", "already_processed": true}),
                );
            }
            if !self.recovery_supersedes(&entry, &marker)? {
                return Err(invalid(
                    "admission recovery was superseded by another terminal result",
                ));
            }
        } else {
            return Err(invalid(
                "admission recovery lost its original processed evidence",
            ));
        }
        if self.queue.contains_work_key(&entry)? {
            return Ok(
                json!({"recovery": receipt.id.to_string(), "status": "queued", "already_queued": true}),
            );
        }
        self.validate_recovery_refs(&entry)?;
        let seq = self.queue.enqueue(entry)?;
        Ok(json!({"recovery": receipt.id.to_string(), "status": "queued", "seq": seq}))
    }

    pub(super) fn resume_admission_recoveries(&self) -> rk_core::Result<()> {
        let _guard = self.enqueue_lock.lock().unwrap_or_else(|p| p.into_inner());
        for receipt in self
            .space
            .scan(&Pattern::category(Category::Event).identity(ADMISSION_RECOVERY_IDENTITY))?
        {
            // A changed fence is terminal for automatic repair; never overwrite it
            // or prevent unrelated keys from draining. The operator can inspect it.
            if let Err(error) = self.resume_admission_receipt(&receipt) {
                tracing::debug!(%error, recovery = %receipt.id, "admission receipt not resumable");
            }
        }
        Ok(())
    }
}

const QUARANTINE_IDENTITY: &str = "landing_queue_quarantine";
impl LandingPipeline {
    pub(super) fn invalid_source_identity(
        &self,
        entry: &LandingQueueEntry,
    ) -> rk_core::Result<Option<String>> {
        let Some(spawn) = entry.source_spawn else {
            return Ok(None);
        };
        let record = self
            .supervisor
            .lock_registry()
            .list_all()
            .into_iter()
            .find(|record| record.spawn_id() == spawn)
            .cloned();
        let Some(record) = record else {
            return Ok(Some(
                "landing source generation is absent from the authoritative registry".into(),
            ));
        };
        if record.repo_name != entry.repo_name {
            return Ok(Some(format!(
                "source generation belongs to {}, not queued repository {}",
                record.repo_name, entry.repo_name
            )));
        }
        if record.repo_root.canonicalize()? != Path::new(&entry.repo_path).canonicalize()? {
            return Ok(Some(
                "source generation repository root differs from queued repository".into(),
            ));
        }
        // Explicit operator/workflow landings may submit a chained reviewer
        // branch. Automatic completion admission remains worker-only.
        if !(record.role == "rat" || (entry.operator_fast_lane && record.role == "reviewer"))
            || record.branch.as_deref() != Some(entry.branch.as_str())
            || (!entry.operator_fast_lane && record.target_branch != entry.target)
        {
            return Ok(Some(
                "source generation role/branch/target differs from queued completion".into(),
            ));
        }
        let canonical = |raw: &str| -> rk_core::Result<Option<String>> {
            match self.tickets.resolve(raw)? {
                Some(t) if t.scope == entry.repo_name => Ok(Some(t.identity)),
                Some(_) => Ok(None),
                None if raw.starts_with("TKT-") => Ok(None),
                None => Ok(Some(raw.to_string())),
            }
        };
        let expected = canonical(record.task.as_deref().unwrap_or_default())?;
        let queued = canonical(&entry.task)?;
        // resolve_land_task permits an operator to bind an otherwise unbound
        // generation to a real ticket. An existing task is never overridden.
        let explicit_binding = entry.operator_fast_lane
            && record.task.is_none()
            && self
                .tickets
                .resolve(&entry.task)?
                .is_some_and(|ticket| ticket.scope == entry.repo_name);
        if !explicit_binding && (expected.is_none() || queued.is_none() || expected != queued) {
            return Ok(Some(
                "source generation task differs from queued repository/task".into(),
            ));
        }
        Ok(None)
    }

    /// A prior `landing_queue_quarantine` evidence tuple bound to this exact
    /// source/task/target/generation, if one already exists — the shared
    /// idempotency probe behind every quarantine route: a restart that
    /// re-discovers the same durably-invalid entry must return the same
    /// terminal verdict instead of writing a second piece of evidence.
    ///
    /// Matched on repo identity (the scan's own `scope`, structural — a
    /// cross-repo collision cannot reach this branch at all), queue `seq`,
    /// `source_spawn` generation, `branch`, `head_sha`, `target`, AND
    /// `task`: `seq` alone is a per-repo counter, not a global one, and a
    /// stale/superseded queue revision must never retire a DIFFERENT
    /// ticket's newer work that happens to reuse it — the `task` comparison
    /// is what stops that.
    fn find_quarantine(&self, entry: &LandingQueueEntry) -> rk_core::Result<Option<Tuple>> {
        let archived = self.space.scan(
            &Pattern::category(Category::Event)
                .scope(&entry.repo_name)
                .identity(QUARANTINE_IDENTITY),
        )?;
        Ok(archived.into_iter().find(|t| {
            t.payload["entry"]["seq"] == entry.seq
                && t.payload["entry"]["source_spawn"] == json!(entry.source_spawn)
                && t.payload["entry"]["branch"] == entry.branch
                && t.payload["entry"]["head_sha"] == entry.head_sha
                && t.payload["entry"]["target"] == entry.target
                && t.payload["entry"]["task"] == entry.task
        }))
    }

    pub(super) fn quarantine_invalid_source(
        &self,
        entry: &LandingQueueEntry,
    ) -> rk_core::Result<Option<LandingOutcome>> {
        if let Some(prior) = self.find_quarantine(entry)? {
            return Ok(Some(LandingOutcome::Quarantined(prior)));
        }
        let archived = self.space.scan(
            &Pattern::category(Category::Event)
                .scope(&entry.repo_name)
                .identity(QUARANTINE_IDENTITY),
        )?;
        if let Some(candidate) = &entry.candidate_sha {
            if let Some(prior) = archived.iter().find(|t| {
                t.payload["entry"]["candidate_sha"] == *candidate
                    || t.payload["affected_candidates"]
                        .as_array()
                        .is_some_and(|affected| {
                            affected.iter().any(|c| c["candidate_sha"] == *candidate)
                        })
            }) {
                return self.archive_quarantine(entry, format!("prepared candidate was quarantined by evidence {}; preserve it and resubmit valid sources through fresh gates", prior.id)).map(Some);
            }
        }
        let Some(reason) = self.invalid_source_identity(entry)? else {
            return Ok(None);
        };
        self.archive_quarantine(entry, reason).map(Some)
    }

    /// A landing target that is not a real local branch — persisted from a
    /// detached commit an operator or workflow mistakenly passed as `--base`,
    /// or a branch since deleted out from under a queued entry — can never
    /// receive `rk_git::Repo::prepare_merge`'s merge, which hard-errors
    /// "merge target does not exist" instead of returning an ordinary
    /// `PrepareOutcome`. Left unchecked that error propagates out of
    /// `process_entry` on every drain pass, so `run_cycle` logs "will retry
    /// next cycle" and hot-loops the same permanently-invalid entry forever
    /// without ever running a gate. Caught here — before `prepare_merge` is
    /// reached — and routed through the same durable quarantine record as an
    /// invalid source, so the entry leaves the active queue exactly once,
    /// keeps its original clocks/attempts/source/ticket in the evidence, and
    /// a restart replays the same verdict instead of re-quarantining.
    ///
    /// A transient git read failure is not proof the branch is absent
    /// (`branch_exists_checked`'s own contract), so it falls through to
    /// `Ok(None)` and lets the ordinary retry-next-cycle path run instead of
    /// misreporting an inconclusive check as a permanent hold.
    pub(super) fn quarantine_invalid_target(
        &self,
        entry: &LandingQueueEntry,
        git_repo: &rk_git::Repo,
    ) -> rk_core::Result<Option<LandingOutcome>> {
        if let Some(prior) = self.find_quarantine(entry)? {
            return Ok(Some(LandingOutcome::Quarantined(prior)));
        }
        if !matches!(git_repo.branch_exists_checked(&entry.target), Ok(false)) {
            return Ok(None);
        }
        let reason = format!(
            "landing target is not an existing branch: {} — a merge target must be a real \
             branch, not a bare commit or a branch that no longer exists",
            entry.target
        );
        self.archive_quarantine(entry, reason).map(Some)
    }

    pub(super) fn archive_quarantine(
        &self,
        entry: &LandingQueueEntry,
        reason: String,
    ) -> rk_core::Result<LandingOutcome> {
        // Snapshot shared-candidate membership BEFORE deleting any queue row.
        // A crash can leave only a valid-looking peer; that peer must still
        // recognize the contaminated object from this durable evidence.
        let mut affected = std::collections::BTreeMap::new();
        if let Some(sha) = &entry.candidate_sha {
            affected.insert(sha.clone(), entry.candidate_ref.clone());
        }
        for queued in self
            .queue
            .scan_current(&entry.repo_name, Some(&entry.target))?
        {
            if queued.payload["batch_branches"]
                .as_array()
                .is_some_and(|branches| branches.iter().any(|b| b == &entry.branch))
            {
                if let Some(sha) = queued.payload["candidate_sha"].as_str() {
                    affected.insert(
                        sha.to_string(),
                        queued.payload["candidate_ref"].as_str().map(str::to_string),
                    );
                }
            }
        }
        let affected_candidates: Vec<Value> = affected.into_iter().map(|(sha, candidate_ref)|
            json!({"candidate_sha": sha, "candidate_ref": candidate_ref})).collect();
        let source = entry.source_spawn.and_then(|spawn| {
            self.supervisor
                .lock_registry()
                .list_all()
                .into_iter()
                .find(|r| r.spawn_id() == spawn)
                .cloned()
        });
        let evidence = Tuple::new(
            Category::Event,
            &entry.repo_name,
            QUARANTINE_IDENTITY,
            "daemon",
            json!({"entry": entry, "source_generation": source, "reason": reason, "affected_candidates": affected_candidates,
                "state": "quarantined", "merged": false, "delivered": false}),
        )
        .with_lifecycle(Lifecycle::Furniture);
        self.space.out(evidence.clone())?;
        Ok(LandingOutcome::Quarantined(evidence))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn admission_window_charges_monotonic_wait_even_when_wall_clock_rolls_back_during_it() {
        let start = Utc::now();
        let deadline = start + chrono::Duration::seconds(60);
        let mut window = AdmissionWindow {
            started_at: start,
            observed_at: start,
            deadline,
            ..AdmissionWindow::new(Duration::from_secs(60))
        };
        window.charge(
            Duration::from_secs(40),
            start + chrono::Duration::seconds(20),
        );
        assert_eq!(window.deadline, start + chrono::Duration::seconds(40));
        assert_eq!((window.deadline - window.observed_at).num_seconds(), 20);
        window.observed_at = deadline + chrono::Duration::hours(1);
        let before = window.deadline;
        assert_eq!(window.remaining(), Duration::ZERO);
        assert!(
            window.deadline <= before,
            "rollback never increases a frozen deadline"
        );
    }
    #[test]
    fn admission_boot_clock_bounds_a_crash_during_wait_with_partial_wall_rollback() {
        let start = Utc::now();
        let mut window = AdmissionWindow::new(Duration::from_secs(60));
        window.started_at = start;
        window.observed_at = start;
        window.deadline = start + chrono::Duration::seconds(60);
        window.clock = Some(BootClock {
            boot: "boot-a".into(),
            nanos: 100_000_000_000,
        });
        window.deadline_nanos = 160_000_000_000;
        let mut restarted: AdmissionWindow = serde_json::from_value(json!(window)).unwrap();
        assert_eq!(
            restarted.remaining_at(
                start + chrono::Duration::seconds(20),
                Some(BootClock {
                    boot: "boot-a".into(),
                    nanos: 150_000_000_000,
                })
            ),
            Duration::from_secs(10)
        );
        assert_eq!(
            restarted.remaining_at(
                start + chrono::Duration::seconds(21),
                Some(BootClock {
                    boot: "boot-b".into(),
                    nanos: 151_000_000_000,
                })
            ),
            Duration::ZERO,
            "reboot cannot renew a prior boot's admission allowance"
        );
    }
}

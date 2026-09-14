//! Managed verification: resolved execution, per-repository admission, exact
//! proof identity, process ownership/cancellation, restart cleanup and evidence.
//! Workflow routing and landing policy compose this module without owning child
//! process lifetimes or maintaining another verification queue.

use chrono::{DateTime, Utc};
use rk_core::paths::Layout;
use rk_core::tuple::{Category, Lifecycle, Pattern, Tuple};
use rk_space::Space;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::task::JoinHandle;
use tracing::info;

pub(crate) struct CheckExecution<'a> {
    pub id: &'a str,
    pub repo: &'a str,
    pub agent: &'a str,
    pub dir: &'a Path,
    pub command: &'a str,
    pub resolved: &'a ResolvedRun,
    pub env: &'a [(String, String)],
    pub timeout: Duration,
    pub admission_timeout: Option<Duration>,
    pub previous_result: Option<&'a Value>,
    pub progress: Option<Arc<Mutex<RunProgress>>>,
}

#[derive(Default)]
pub(crate) struct VerificationResources {
    pub(crate) test_exec_lock: TestExecLock,
    pub(crate) admission: VerificationAdmission,
    pub(crate) host_admission: HostVerificationAdmission,
    pub(crate) runs: ManagedVerificationRuns,
    pub(crate) clock: SpanClock,
}

/// The single injectable wall-clock seam [`ManagedVerification::run`] reads
/// to stamp `RunProgress`'s real `queued_at`/`started_at`/`ended_at`
/// boundaries — production wiring is `Utc::now` ([`Default`]); a test
/// substitutes a clock it controls to model a delayed publish or a host
/// suspend deterministically, without actually sleeping the test host.
/// Follows the same narrow-injectable-seam idiom as `landing.rs`'s
/// `RetrySchedule`: nothing outside this one field reads through it, so a
/// frozen/advancing test clock here cannot distort unrelated behavior.
pub(crate) struct SpanClock {
    now: std::sync::Mutex<Box<dyn Fn() -> DateTime<Utc> + Send + Sync>>,
}

impl SpanClock {
    pub(crate) fn now(&self) -> DateTime<Utc> {
        (self.now.lock().unwrap())()
    }

    #[cfg(test)]
    pub(crate) fn from_fn(now: impl Fn() -> DateTime<Utc> + Send + Sync + 'static) -> Self {
        Self {
            now: std::sync::Mutex::new(Box::new(now)),
        }
    }

    /// Swap the wall-clock function on an already-constructed
    /// [`VerificationResources`] — for a test whose harness only builds one
    /// via a `Supervisor`/`WorkflowEngine` it doesn't otherwise control the
    /// construction of (`landing.rs`'s `test_pipeline`), rather than
    /// threading a clock through every production constructor just to make
    /// it reachable from a test.
    #[cfg(test)]
    pub(crate) fn set(&self, now: impl Fn() -> DateTime<Utc> + Send + Sync + 'static) {
        *self.now.lock().unwrap() = Box::new(now);
    }
}

impl Default for SpanClock {
    fn default() -> Self {
        Self {
            now: std::sync::Mutex::new(Box::new(Utc::now)),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ManagedVerification<'a> {
    layout: &'a Layout,
    space: &'a Space,
    resources: &'a VerificationResources,
    shared_cargo_target: bool,
}

impl<'a> ManagedVerification<'a> {
    /// Whether a check for `repo` goes through ANY bounded admission at all —
    /// the shared-`CARGO_TARGET_DIR` lock, this repo's own per-repo WIP
    /// limit, OR the aggregate host-wide cap (P3.1, TKT-vilug-hujok-bolis).
    /// `landing.rs`'s combined-candidate batching (`process_batch`) reads
    /// this to decide whether a repo is safe to batch multiple tickets'
    /// checks into one combined run: batching assumes the repo has no
    /// capacity contention to protect against. Before the aggregate cap
    /// existed, a repo with no per-repo override and no shared-target flag
    /// correctly read as "unbounded, safe to batch" — but a positive
    /// aggregate limit bounds this repo's checks too even with no per-repo
    /// override configured, so omitting it here would have silently kept
    /// batching a repo that is, in fact, now under host-wide admission.
    pub(crate) fn uses_capacity_admission(&self, repo: &str) -> bool {
        self.shared_cargo_target
            || self.resources.admission.limit_for(repo) > 0
            || self.resources.host_admission.limit() > 0
    }

    pub(crate) fn new(
        layout: &'a Layout,
        space: &'a Space,
        resources: &'a VerificationResources,
        shared_cargo_target: bool,
    ) -> Self {
        Self {
            layout,
            space,
            resources,
            shared_cargo_target,
        }
    }
    /// Run one repo-registered named check directly for `agent`, outside any
    /// workflow instance — the `verify.run` RPC's entry point into the same
    /// admission-controlled, env-stripped, exact-exit-provenance execution
    /// [`run`](Self::run) already gives landing gates and workflow `run` steps
    /// (TKT-01M0HNESEECWWFQF8X6VH1XSJ6). This is the managed alternative a
    /// rat's own completion-protocol verification step is meant to call
    /// instead of self-invoking a full suite directly, so the same bounded
    /// per-repo queue sees it. `dir` is both the check-registry root
    /// (`<dir>/.rk/checks.cue`) and the check's execution cwd — the caller's
    /// own worktree, or the repo's registered root checkout for the
    /// operator.
    ///
    /// Reuses a durable proof for an EXACT match on `(repo, candidate sha,
    /// check command, toolchain, environment policy)` instead of re-running
    /// — including a proof an unrelated landing gate already wrote for this
    /// exact candidate sha (`landing_gate_pass`, commit `3d47d08`), so a rat
    /// whose branch a landing gate already tested gets an instant, free hit.
    /// Proof reuse is gated on the worktree being clean: a dirty tree has no
    /// stable "candidate" identity a later caller could safely match against,
    /// so it is NEVER read from or written to the cache — always runs fresh,
    /// through the same admission queue as everything else. Never reuses a
    /// stale proof for a different prepared merge: the key binds the exact
    /// candidate sha, so a rebased or amended branch head simply misses.
    ///
    /// Bound to the requesting caller's lifecycle
    /// (TKT-01M0PA6C5WYRWS757R1SS2F2GR): registers itself with
    /// [`ManagedVerificationRuns::register`] for `generation` (the
    /// live agent generation this call belongs to, when the caller is a
    /// supervised agent — `None` for the operator) and `request_key` (this
    /// exact RPC call, for a caller-disconnect cancellation). Races its own
    /// `run_check_in` call against that registration's cancel signal: a
    /// cancellation from an agent interrupt/dismiss/terminal death or an RPC
    /// disconnect drops the run in flight — killing its managed child
    /// process group and releasing its admission permit immediately, via the
    /// same drop-based cleanup `run_check_in` already relies on for a
    /// timeout — and this records a durable cancellation outcome instead of
    /// ever writing a reusable proof for it.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn verify_repo_check(
        &self,
        agent: &str,
        dir: &Path,
        repo_name: &str,
        check_name: &str,
        generation: Option<rk_core::id::SpawnId>,
        request_key: &str,
        // The ticket this call's task-to-main span correlates on
        // (TKT-01M0QJXVF5QP858YXF82E9WRWQ) — `None` for the operator (no
        // ticket to correlate against), `Some` for a supervised agent's own
        // task, exactly the same string `AgentLaunched`/`FirstProgress`
        // already carry for that generation.
        task: Option<&str>,
    ) -> rk_core::Result<Value> {
        let check = self.find_check(&dir.display().to_string(), check_name)?;
        let resolved = ResolvedRun {
            command: check.command.clone(),
            cwd: check.cwd.clone(),
            // Same reasoning as the landing pipeline's own construction
            // (`LandingPipeline::run_gates_at`): read `verdict`/`exit` off
            // the result instead of `run_check_in`'s inline exit-gate `Err`
            // path, so a failing check is always a clean `Ok` result
            // carrying its exact exit code, never a propagated error.
            expect_exit: None,
            timeout: check
                .timeout
                .clone()
                .unwrap_or_else(|| DEFAULT_RUN_TIMEOUT.to_string()),
            on_timeout: OnTimeout::Fail,
            environment_policy: check.environment_policy,
            retry_on_fail: 0,
            shared_cargo_target: check.shared_cargo_target,
        };
        let exec_dir = match &resolved.cwd {
            Some(cwd) => dir.join(cwd),
            None => dir.to_path_buf(),
        };
        let timeout = parse_duration(&resolved.timeout)?;

        let candidate_sha = clean_candidate_sha(dir).await;
        if let Some(sha) = &candidate_sha {
            if let Some(cached) = self.lookup_verification_proof(repo_name, sha, &check) {
                if let Some(task) = task {
                    self.record_ad_hoc_verification_span(AdHocVerificationSpan {
                        task,
                        repo_name,
                        check_name,
                        candidate: sha,
                        queued_at: None,
                        started_at: None,
                        ended_at: None,
                        queue_wait_ms_monotonic: None,
                        duration_ms_monotonic: None,
                        proof_reused: true,
                        terminal_reason: "reused",
                    });
                }
                return Ok(cached);
            }
        }

        let progress = Arc::new(Mutex::new(RunProgress::default()));
        let (managed_id, mut cancel_rx) =
            self.resources.runs.register(agent, generation, request_key);
        let registration = ManagedRegistration {
            runs: &self.resources.runs,
            id: managed_id,
        };
        let run_id = format!("verify-run:{agent}");
        let run_fut = self.run(CheckExecution {
            admission_timeout: None,
            id: &run_id,
            repo: repo_name,
            agent,
            dir: &exec_dir,
            command: &resolved.command,
            resolved: &resolved,
            env: &[],
            timeout,
            previous_result: None,
            progress: Some(Arc::clone(&progress)),
        });
        tokio::pin!(run_fut);
        let outcome = tokio::select! {
            result = &mut run_fut => Ok(result),
            _ = cancel_rx.changed() => {
                let reason: Option<&'static str> = *cancel_rx.borrow();
                Err(reason.unwrap_or("cancelled"))
            }
        };
        drop(registration);

        let result = match outcome {
            Ok(result) => result?,
            Err(reason) => {
                // A cancellation is settled right here, at the moment it's
                // observed — `ended_at` is real, not reconstructed later —
                // but never `run()`'s own `Settled` (execution never finished
                // normally, so there is nothing frozen to read).
                let (queued_at, started_at, queue_wait_ms, duration_ms) = {
                    let p = progress.lock().unwrap();
                    (
                        p.queued_at_wall,
                        p.started_at_wall,
                        p.queue_wait_ms,
                        p.execution_started_at.map(|started| {
                            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
                        }),
                    )
                };
                let ended_at = started_at.map(|_| self.resources.clock.now());
                self.record_verification_cancellation(
                    repo_name,
                    agent,
                    generation,
                    request_key,
                    candidate_sha.as_deref(),
                    &check,
                    queue_wait_ms,
                    duration_ms,
                    reason,
                );
                if let Some(task) = task {
                    self.record_ad_hoc_verification_span(AdHocVerificationSpan {
                        task,
                        repo_name,
                        check_name,
                        candidate: candidate_sha.as_deref().unwrap_or("dirty"),
                        queued_at,
                        started_at,
                        ended_at,
                        queue_wait_ms_monotonic: queue_wait_ms,
                        duration_ms_monotonic: duration_ms,
                        proof_reused: false,
                        terminal_reason: reason,
                    });
                }
                return Err(rk_core::Error::other(format!(
                    "verification cancelled ({reason}) for repo `{repo_name}` check `{check_name}`"
                )));
            }
        };

        if let Some(sha) = &candidate_sha {
            if result.get("verdict").and_then(Value::as_str) == Some("pass") {
                self.record_verification_proof(repo_name, sha, &check, &result);
            }
        }

        if let Some(task) = task {
            let (queued_at, started_at, ended_at, queue_wait_ms, duration_ms) = {
                let p = progress.lock().unwrap();
                (
                    p.queued_at_wall,
                    p.started_at_wall,
                    p.settled.map(|s| s.ended_at_wall),
                    p.queue_wait_ms,
                    p.settled.map(|s| s.duration_ms),
                )
            };
            self.record_ad_hoc_verification_span(AdHocVerificationSpan {
                task,
                repo_name,
                check_name,
                candidate: candidate_sha.as_deref().unwrap_or("dirty"),
                queued_at,
                started_at,
                ended_at,
                queue_wait_ms_monotonic: queue_wait_ms,
                duration_ms_monotonic: duration_ms,
                proof_reused: false,
                terminal_reason: result
                    .get("verdict")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown"),
            });
        }

        Ok(result)
    }

    /// Record one `verify.run` call's `Phase::VerificationQueued` span — the
    /// managed alternative's own occurrence, alongside (never instead of)
    /// whatever a landing gate's per-check spans already recorded for the
    /// same task (`LandingPipeline::record_check_verification_span`).
    /// `lane` carries the check name, exactly like the landing gate's
    /// per-check spans, so both producers read the same way in
    /// `spans_for_task`. `attempt` is deliberately NOT this task's own
    /// small per-check ordinal (`10_000 +` a running count of this task's
    /// existing `verification`-phase spans): a landing gate's per-check
    /// attempts are small, deterministic plan positions
    /// (`LandingPipeline::run_gates_at`'s `check_attempt`, 1, 2, 3, ...),
    /// and an ad-hoc `verify.run` for the very same task can otherwise land
    /// on the exact same small number, silently losing one span to
    /// `record_phase_span`'s `(task, phase, attempt)` idempotency key even
    /// though they are for two different, unrelated occurrences. Pushing
    /// this producer's own numbering into a disjoint high range keeps the
    /// two producers from ever colliding without touching that shared key.
    fn record_ad_hoc_verification_span(&self, occurrence: AdHocVerificationSpan<'_>) {
        let AdHocVerificationSpan {
            task,
            repo_name,
            check_name,
            candidate,
            queued_at,
            started_at,
            ended_at,
            queue_wait_ms_monotonic,
            duration_ms_monotonic,
            proof_reused,
            terminal_reason,
        } = occurrence;
        const AD_HOC_ATTEMPT_BASE: u32 = 10_000;
        let existing = crate::span::spans_for_task(self.space, repo_name, task).unwrap_or_default();
        let ad_hoc_occurrences = u32::try_from(
            existing
                .iter()
                .filter(|s| s["phase"] == "verification" && s["lane"] == check_name)
                .count(),
        )
        .unwrap_or(0);
        let attempt = AD_HOC_ATTEMPT_BASE.saturating_add(ad_hoc_occurrences);
        let mut span = crate::span::PhaseSpan::from_observed(
            task,
            crate::span::Phase::VerificationQueued,
            queued_at,
            started_at,
            ended_at,
        )
        .attempt(attempt)
        .repo(repo_name)
        .candidate(candidate)
        .lane(check_name)
        .proof_kind("ad-hoc")
        .proof_reused(proof_reused)
        .terminal_reason(terminal_reason);
        if let Some(ms) = queue_wait_ms_monotonic {
            span = span.queue_wait_ms_monotonic(ms);
        }
        if let Some(ms) = duration_ms_monotonic {
            span = span.duration_ms_monotonic(ms);
        }
        let _ = crate::span::record_phase_span(self.space, repo_name, "daemon", &span);
    }

    /// Durable record of a managed verification run cancelled before it could
    /// settle (TKT-01M0PA6C5WYRWS757R1SS2F2GR) — never written for a run that
    /// actually completed, whatever its verdict; that's
    /// [`record_verification_admission_event`](Self::record_verification_admission_event)
    /// and [`record_verification_proof`](Self::record_verification_proof)'s
    /// job. `queue_wait_ms`/`duration_ms` are best-effort: `None` for either
    /// means the cancellation landed before `run_check_in` ever wrote to its
    /// progress cell (still queued behind the admission bound), not that the
    /// value is unknown for a run that did start.
    #[allow(clippy::too_many_arguments)]
    fn record_verification_cancellation(
        &self,
        repo_name: &str,
        agent: &str,
        generation: Option<rk_core::id::SpawnId>,
        request_key: &str,
        candidate_sha: Option<&str>,
        check: &rk_workflow::Check,
        queue_wait_ms: Option<u64>,
        duration_ms: Option<u64>,
        reason: &str,
    ) {
        let proof_key = candidate_sha.and_then(|sha| verification_proof_key(repo_name, sha, check));
        let _ = self.space.out(
            Tuple::new(
                Category::Event,
                repo_identity(self.layout, repo_name),
                VERIFICATION_CANCELLED_IDENTITY,
                "daemon",
                json!({
                    "agent": agent,
                    "generation": generation.map(|g| g.to_string()),
                    "request_key": request_key,
                    "proof_key": proof_key,
                    "command": check.command,
                    "queue_wait_ms": queue_wait_ms,
                    "duration_ms": duration_ms,
                    "reason": reason,
                }),
            )
            .with_lifecycle(Lifecycle::Furniture),
        );
    }

    /// Best-effort exact-key lookup: a durable proof this repo already wrote
    /// for `candidate_sha` under this check's exact command/toolchain/env
    /// policy, then — as a free secondary win — an unrelated landing gate's
    /// `landing_gate_pass` for the same candidate sha and check name. That
    /// secondary source is matched on the SAME `verification_proof_key`
    /// digest as the primary cache (`check_proof_keys`, a per-check digest
    /// `run_gates_at` stores on the event alongside the plain `checks` name
    /// list) — not merely on `check.name` appearing in that list, which said
    /// nothing about whether the command/toolchain/environment that
    /// actually ran still matches this caller's. An older `landing_gate_pass`
    /// event written before `check_proof_keys` existed carries none, so it
    /// simply misses here rather than false-matching.
    ///
    /// `pub(crate)`: also called from `landing.rs`'s `run_gates_at`
    /// (TKT-01M0QRZ7QT8CQD74GHRN81XFT5) so a landing gate can reuse a
    /// passing managed `rk verify` proof instead of always re-running the
    /// full suite in its own gate worktree — the reverse direction of the
    /// `landing_gate_pass` fallback this method already provides.
    pub(crate) fn lookup_verification_proof(
        &self,
        repo_name: &str,
        candidate_sha: &str,
        check: &rk_workflow::Check,
    ) -> Option<Value> {
        let key = verification_proof_key(repo_name, candidate_sha, check)?;
        if let Ok(tuples) = self.space.scan(
            &Pattern::category(Category::Event)
                .identity(VERIFICATION_PROOF_IDENTITY)
                .scope(repo_name),
        ) {
            if let Some(t) = tuples
                .iter()
                .find(|t| t.payload.get("key").and_then(Value::as_str) == Some(key.as_str()))
            {
                let mut result = t
                    .payload
                    .get("result")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                if let Value::Object(map) = &mut result {
                    map.insert("reused".into(), json!(true));
                    map.insert("reused_from".into(), json!("verification_proof"));
                }
                return Some(result);
            }
        }
        if let Ok(tuples) = self.space.scan(
            &Pattern::category(Category::Event)
                .identity("landing_gate_pass")
                .scope(repo_name),
        ) {
            if let Some(t) = tuples.iter().find(|t| {
                t.payload.get("candidate_sha").and_then(Value::as_str) == Some(candidate_sha)
                    && t.payload
                        .get("check_proof_keys")
                        .and_then(|v| v.get(check.name.as_str()))
                        .and_then(Value::as_str)
                        == Some(key.as_str())
            }) {
                return Some(json!({
                    "exit": 0,
                    "verdict": "pass",
                    "reused": true,
                    "reused_from": "landing_gate_pass",
                    "candidate_sha": candidate_sha,
                    "branch": t.payload.get("branch"),
                }));
            }
        }
        None
    }

    /// Record a durable proof of `result` (only ever called on a "pass"
    /// verdict) for later exact-key reuse by
    /// [`lookup_verification_proof`](Self::lookup_verification_proof).
    fn record_verification_proof(
        &self,
        repo_name: &str,
        candidate_sha: &str,
        check: &rk_workflow::Check,
        result: &Value,
    ) {
        let Some(key) = verification_proof_key(repo_name, candidate_sha, check) else {
            return;
        };
        let _ = self.space.out(
            Tuple::new(
                Category::Event,
                repo_name.to_string(),
                VERIFICATION_PROOF_IDENTITY,
                "daemon",
                json!({
                    "key": key,
                    "candidate_sha": candidate_sha,
                    "command": check.command,
                    "toolchain": check.toolchain,
                    "environment_policy": check.environment_policy.to_string(),
                    "result": result,
                }),
            )
            .with_lifecycle(Lifecycle::Furniture),
        );
    }

    /// Run one resolved check to completion in `dir`, with retry/timeout
    /// policy and durable gate-failure recording — everything downstream of
    /// "have a directory and a fully-resolved command". Split out of
    /// [`run_command`](Self::run_command) so this logic no longer requires
    /// `ctx.active_agent`: `run_command` resolves a directory from the active
    /// agent's worktree and calls this; a daemon-native caller with its own
    /// directory (a persistent gate worktree, no agent involved) can call it
    /// directly. `agent` is carried through only for RK_AGENT env attribution
    /// (under the `inherit` environment policy) and the `gate-failure`
    /// artifact's `agent` field — it need not name a live registered agent.
    /// `previous_result` is only `ctx.previous_result` threaded through so a
    /// failed `expectExit` can still lead with a prior gate's own verdict; a
    /// caller with no workflow context at all passes `None`.
    /// Crate-scoped: the T2 daemon-native landing consumer (another module in
    /// this crate) calls this directly with its persistent gate worktree; see
    /// docs/proposals/daemon-native-landing-pipeline.md T1->T2 interface.
    pub(crate) async fn run(self, request: CheckExecution<'_>) -> rk_core::Result<Value> {
        let CheckExecution {
            admission_timeout,
            id,
            repo,
            agent,
            dir,
            command,
            resolved,
            env,
            timeout,
            previous_result,
            progress,
        } = request;
        let identity = repo_identity(self.layout, repo);
        let repo = identity.as_str();
        // One capacity budget spans both locks. Named check execution gets
        // its own timeout only after all admission resources are acquired.
        let admission_limit = self.resources.admission.limit_for(repo);
        // Read once per call, same convention as `admission_limit` — the
        // aggregate cap is startup-config-only (TKT-vilug-hujok-bolis), never
        // live-reloaded mid-flight outside a test.
        let host_limit = self.resources.host_admission.limit();
        let admission_started = Instant::now();
        let admission_started_wall = self.resources.clock.now();
        if let Some(p) = &progress {
            p.lock().unwrap().queued_at_wall = Some(admission_started_wall);
        }
        let wait_budget = admission_timeout.unwrap_or(timeout);
        let acquire = async {
            let test_guard = if resolved.shared_cargo_target && self.shared_cargo_target {
                Some(self.resources.test_exec_lock.acquire(repo).await)
            } else {
                None
            };
            let admission = if admission_limit > 0 {
                self.resources
                    .admission
                    .acquire(repo, admission_limit)
                    .await
            } else {
                None
            };
            // Aggregate host permit, acquired LAST — strictly after the
            // shared-target lock and the per-repo permit above. A request
            // still queued behind either of those has not yet entered this
            // semaphore's own wait queue, so a saturated repo (or a held
            // shared-target lock) can never occupy a host slot, or block
            // ahead of, an eligible request from a different repo
            // (TKT-vilug-hujok-bolis: preventing cross-repo head-of-line
            // blocking). This does not implement check sharing — it is a
            // pure ordering property of two independent semaphores.
            let host_guard = self.resources.host_admission.acquire().await;
            (test_guard, admission, host_guard)
        };
        let (_test_exec_guard, admission, _host_guard) = match if wait_budget.is_zero() {
            None
        } else {
            tokio::time::timeout(wait_budget, acquire).await.ok()
        } {
            Some(guards) => guards,
            None => {
                let stderr = format!(
                    "run step: `{command}` did not acquire verification capacity for repo `{repo}` within {wait_budget:?} (shared CARGO_TARGET_DIR lock / verification admission; WIP limit {admission_limit}; host limit {host_limit})"
                );
                let queue_wait_ms =
                    u64::try_from(admission_started.elapsed().as_millis()).unwrap_or(u64::MAX);
                if let Some(progress) = &progress {
                    progress.lock().unwrap().queue_wait_ms = Some(queue_wait_ms);
                }
                self.record_gate_failure(
                    id,
                    repo,
                    agent,
                    command,
                    LOCK_TIMEOUT_EXIT,
                    "infra",
                    false,
                    None,
                    "",
                    false,
                    &stderr,
                    false,
                    &[],
                );
                self.record_verification_admission_event(VerificationAdmissionOutcome {
                    repo,
                    agent,
                    command,
                    queue_wait_ms: Some(queue_wait_ms),
                    duration: Duration::ZERO,
                    exit: LOCK_TIMEOUT_EXIT,
                    verdict: "infra",
                });
                if resolved.expect_exit.is_some()
                    || (admission_timeout.is_none()
                        && resolved.shared_cargo_target
                        && self.shared_cargo_target)
                {
                    return Err(rk_core::Error::other(stderr));
                }
                return Ok(json!({
                    "exit": LOCK_TIMEOUT_EXIT, "stdout": "", "stdout_truncated": false,
                    "stderr": stderr, "stderr_truncated": false, "timed_out": false,
                    "no_exit_code": true, "signal": null, "verdict": "infra",
                    "executed": false, "reason": "admission-timeout", "queue_wait_ms": queue_wait_ms,
                }));
            }
        };
        let admission_queue_wait_ms = (admission_limit > 0
            || _test_exec_guard.is_some()
            || host_limit > 0)
            .then(|| u64::try_from(admission_started.elapsed().as_millis()).unwrap_or(u64::MAX));
        let _admission_guard = admission.map(|(permit, _)| permit);
        let run_started = Instant::now();
        let run_started_wall = self.resources.clock.now();
        if let Some(progress) = &progress {
            let mut p = progress.lock().unwrap();
            p.queue_wait_ms = admission_queue_wait_ms;
            p.execution_started_at = Some(run_started);
            p.started_at_wall = Some(run_started_wall);
        }

        // Extra attempts on a non-"pass" verdict, for a check already
        // characterized as flaky for reasons outside the code under test
        // (TKT-01M02AMKD24WZVVMARJPXKYKSW). 0 retries is the historical
        // behaviour: exactly one attempt, no backoff, no history recorded.
        // `resolve_run` already rejects `retry_on_fail > MAX_RETRY_ON_FAIL`, so
        // this can never actually saturate; `saturating_add` is a second,
        // independent guarantee that this never panics or wraps even if that
        // guard is ever loosened.
        let attempts = resolved.retry_on_fail.saturating_add(1);
        let mut history: Vec<Value> = Vec::new();
        let mut settled: Option<SettledAttempt> = None;
        for attempt in 1..=attempts {
            let outcome = self
                .spawn_check_child(command, dir, resolved, agent, env, timeout)
                .await?;
            // A `TimedOut` outcome reaches here under either `onTimeout`
            // policy now — `collect_child_output` only reports it, it does not
            // decide the policy (TKT-01M02QT9KTDY2CN6YJEVP3VCF8). The captured
            // output is genuinely gone (the reader tasks are aborted with the
            // child), so stderr carries the explanation instead of a lie about
            // what the suite printed.
            let (
                mut exit,
                mut stdout,
                mut stdout_truncated,
                mut stderr,
                mut stderr_truncated,
                mut timed_out,
                mut no_exit_code,
                mut signal,
            ) = decode_run_outcome(outcome, command, resolved);
            // TKT-01M0CF9PG9NHHM0ZTFKDW6BVBV: under the shared
            // `CARGO_TARGET_DIR` (`[disk] shared_cargo_target`), a concurrent
            // `cargo build` in another worktree can prune a test binary
            // between this process resolving its path and execing it,
            // producing exactly this "could not execute process ... (never
            // executed) ... No such file or directory (os error 2)" text.
            // That's cross-process contention, not a real failure of the code
            // under test (docs/2026-08-19-tkt-hot-scan-target-dir-contention.md
            // option 2), so it gets exactly one free retry here — ahead of,
            // and independent from, the configured `retry_on_fail` flaky-retry
            // loop below, so it fires even when `retry_on_fail` is 0. Scoped
            // tightly to this exact signature so a real compile error or test
            // failure is never retried.
            if !timed_out && exit != 0 && is_cargo_target_contention_signature(&stdout, &stderr) {
                info!(
                    agent = %agent, command = %command,
                    "run step hit shared cargo target-dir contention signature, retrying once"
                );
                let retry_outcome = self
                    .spawn_check_child(command, dir, resolved, agent, env, timeout)
                    .await?;
                (
                    exit,
                    stdout,
                    stdout_truncated,
                    stderr,
                    stderr_truncated,
                    timed_out,
                    no_exit_code,
                    signal,
                ) = decode_run_outcome(retry_outcome, command, resolved);
            }
            // The routable four-way summary. `exit` alone cannot express it: a
            // suite may exit 124 on its own, and "did not finish" calls for a
            // different hand-off than "finished and said no". `infra` is its
            // own case, not folded into `fail`: `no_exit_code` means the
            // process never reported an exit code of its own — killed by a
            // signal, or any other runner-loss shape decoded the same way —
            // an infrastructure death (OOM killer, an external `kill -9`, the
            // runner losing the child) that says nothing about the code under
            // test, as opposed to a real exit code (however nonzero) the
            // suite chose itself. Classified on `no_exit_code` rather than
            // `signal.is_some()` so this still fires on a platform (or a
            // runner-loss shape) that cannot decode which signal it was.
            let verdict: &'static str = if timed_out {
                "timeout"
            } else if no_exit_code {
                "infra"
            } else if exit == 0 {
                "pass"
            } else {
                "fail"
            };
            // The default `onTimeout: "fail"` policy ends the run immediately
            // on a timeout — no retry, matching the historical behaviour —
            // but must still leave the same durable evidence a fail or
            // retry-exhausted verdict does below. Before this, a timeout on
            // this (default) path returned an `Err` straight out of
            // `spawn_check_child` and was never seen here, so
            // `record_gate_failure` never ran for it
            // (TKT-01M02QT9KTDY2CN6YJEVP3VCF8).
            if timed_out && resolved.on_timeout == OnTimeout::Fail {
                self.record_gate_failure(
                    id,
                    repo,
                    agent,
                    command,
                    exit,
                    verdict,
                    timed_out,
                    signal,
                    &stdout,
                    stdout_truncated,
                    &stderr,
                    stderr_truncated,
                    &history,
                );
                let duration = run_started.elapsed();
                if let Some(p) = &progress {
                    p.lock().unwrap().settled = Some(Settled {
                        duration_ms: u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
                        ended_at_wall: self.resources.clock.now(),
                    });
                }
                self.record_verification_admission_event(VerificationAdmissionOutcome {
                    repo,
                    agent,
                    command,
                    queue_wait_ms: admission_queue_wait_ms,
                    duration,
                    exit,
                    verdict,
                });
                return Err(rk_core::Error::other(stderr));
            }
            if verdict == "pass" || attempt == attempts {
                settled = Some(SettledAttempt {
                    exit,
                    stdout,
                    stdout_truncated,
                    stderr,
                    stderr_truncated,
                    timed_out,
                    no_exit_code,
                    signal,
                    verdict,
                });
                break;
            }
            info!(
                agent = %agent, exit, timed_out, verdict, attempt, attempts,
                command = %command, "run step attempt failed, retrying"
            );
            history.push(json!({
                "attempt": attempt,
                "exit": exit,
                "verdict": verdict,
                "timed_out": timed_out,
            }));
            tokio::time::sleep(RETRY_BACKOFF).await;
        }
        // `attempts >= 1`, and `settled` is always set on the final iteration
        // (attempt == attempts), so the loop never exits without it.
        let SettledAttempt {
            exit,
            stdout,
            stdout_truncated,
            stderr,
            stderr_truncated,
            timed_out,
            no_exit_code,
            signal,
            verdict,
        } = settled.expect("run step: attempt loop always settles by the final attempt");
        info!(agent = %agent, exit, timed_out, no_exit_code, signal = ?signal, verdict, command = %command, retries = history.len(), "run step completed");
        let mut result = json!({
            "exit": exit,
            "stdout": stdout,
            "stdout_truncated": stdout_truncated,
            "stderr": stderr,
            "stderr_truncated": stderr_truncated,
            "timed_out": timed_out,
            "no_exit_code": no_exit_code,
            "signal": signal,
            "verdict": verdict,
        });

        if !history.is_empty() {
            result["retries"] = json!(history);
        }

        let settled_duration = run_started.elapsed();
        if let Some(p) = &progress {
            p.lock().unwrap().settled = Some(Settled {
                duration_ms: u64::try_from(settled_duration.as_millis()).unwrap_or(u64::MAX),
                ended_at_wall: self.resources.clock.now(),
            });
        }
        self.record_verification_admission_event(VerificationAdmissionOutcome {
            repo,
            agent,
            command,
            queue_wait_ms: admission_queue_wait_ms,
            duration: settled_duration,
            exit,
            verdict,
        });

        // A non-"pass" verdict is a gate that said no (or never finished).
        // Persist a durable, bounded record of what it said BEFORE a following
        // step overwrites ctx.previous_result — otherwise the only trace left
        // once the workflow routes past this step is a composed one-line
        // instance error (TKT-01M02AMKD24WZVVMARJPXKYKSW).
        if verdict != "pass" {
            self.record_gate_failure(
                id,
                repo,
                agent,
                command,
                exit,
                verdict,
                timed_out,
                signal,
                &stdout,
                stdout_truncated,
                &stderr,
                stderr_truncated,
                &history,
            );
        }

        // Inline fail-closed gate: when the step (or named check) declares the
        // expected exit, enforce it here so `run` can gate on its own without a
        // trailing evaluate. When unset, the exit is left for a following
        // evaluate/when. A timed-out command reports 124, so this rejects it
        // exactly as it rejects a red suite — `onTimeout: "continue"` never
        // sneaks a too-slow check past a declared exit gate.
        if let Some(expected) = resolved.expect_exit {
            if exit != expected {
                // Carry the check's own words into the instance error, and —
                // when this check is an escalation running right after a failed
                // gate — LEAD with the gate's result. Without both, a failing
                // report check (empty payload, forbidden caller) replaces the
                // reason the workflow actually stopped.
                return Err(rk_core::Error::other(format!(
                    "{}run step: `{command}` exited {exit}, expected {expected}{}",
                    prior_gate_failure(previous_result),
                    check_failure_detail(&stdout, &stderr)
                )));
            }
        }
        Ok(result)
    }

    /// Build and run one attempt of a `run` step's child process, returning its
    /// captured outcome. Split out of [`run_command`](Self::run_command) so a
    /// retry can spawn a fresh `Command`/`Child` per attempt — both are
    /// single-use.
    #[allow(clippy::too_many_arguments)]
    async fn spawn_check_child(
        &self,
        command: &str,
        dir: &Path,
        resolved: &ResolvedRun,
        agent: &str,
        env: &[(String, String)],
        timeout: Duration,
    ) -> rk_core::Result<RunOutcome> {
        // Plain `sh -c`, with `pipefail` deliberately NOT forced on: a `run`
        // step's command — raw or a named check reference alike — is
        // workflow-author-owned shell, and existing workflows rely on `sh`'s
        // default (last-stage-wins) pipe semantics for assertion idioms like
        // `... | grep -q '<expected text>'` (examples/checks.cue's
        // landing-protected-paths does exactly this). Forcing `pipefail` over
        // those INVERTS them: `grep -q` exits as soon as it matches, so a
        // producer large enough to still be writing takes SIGPIPE (141), and
        // under `pipefail` the negated pipeline reports success precisely when
        // the protected path WAS touched. Size-dependent, silent, and fail-open
        // — strictly worse than the masking it would fix.
        //
        // What RK owes instead is that its OWN layer adds no masking: the only
        // consumer RK puts on a check is `collect_child_output`/`read_capped`
        // below, which reads both streams to EOF and succeeds independently of
        // the child, and the status it reports is THIS child's, verbatim.
        // An author who wants their pipeline unmasked says so in the declared
        // command (`bash -c 'set -o pipefail; ...'`, `${PIPESTATUS[0]}`) and
        // RK carries that through — pinned by
        // `collect_child_output_reports_a_failing_check_through_its_own_output_consumer`
        // and end-to-end by `run_step_fails_closed_on_a_failing_check_piped_to_a_successful_consumer`.
        let mut child_command = tokio::process::Command::new("sh");
        child_command
            .arg("-c")
            .arg(command)
            .current_dir(dir)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            // Deliberately NOT `.kill_on_drop(true)`: that would let
            // `abort_task`'s `JoinHandle::abort()` (the timeout/error paths
            // in `collect_child_output`) drop this `Child` — and with
            // `kill_on_drop` set, SIGKILL its pid — before
            // `ProcessGroupGuard` gets to snapshot the live descendant tree.
            // A descendant already reparented to init by then is invisible
            // to that snapshot's `ppid`-chain walk, exactly the bug this
            // check's own group-only kill used to have
            // (TKT-01M0PN2JSN24AHGQHFJ4XGAVKD). Tokio's own orphan reaper
            // still reaps this child once it exits regardless of
            // `kill_on_drop`, so nothing here leaks a zombie by omitting it —
            // `ProcessGroupGuard` is the sole, and sufficient, killer.
            //
            // Its own process group (mirroring rk-harness's launcher): lets
            // `ProcessGroupGuard` in `collect_child_output` reach every
            // descendant this check spawns (mise/cargo/rustc under `sh -c`),
            // not just the `sh` wrapper itself.
            .process_group(0);
        // Named checks routinely shell back into `rk` (escalation needs, rework
        // tickets), but the child inherits the DAEMON's environment — and the
        // daemon's PATH is whatever its first auto-starting client happened to
        // carry. Put the daemon's own binary directory first so a check always
        // resolves the same `rk` the daemon is running, regardless of who
        // started the daemon.
        if let Some(path) = check_child_path(std::env::current_exe().ok(), std::env::var_os("PATH"))
        {
            child_command.env("PATH", path);
        }
        if resolved.environment_policy == rk_workflow::CheckEnvironmentPolicy::StripRkSpawn {
            for name in rk_workflow::STRIPPED_RK_SPAWN_ENV {
                child_command.env_remove(name);
            }
        } else {
            // The child executes inside the active agent's worktree, and the
            // server treats a worktree cwd as that agent's authority domain: a
            // connection from there may claim only that agent as caller. An
            // escalation check that shells back into `rk` (need rows, rework
            // tickets) with no identity is therefore FORBIDDEN
            // deterministically — the silent-escalation defect
            // (TKT-01M00WPWEFZVPW3YBNX3825MBG). Give the child the same
            // credential set a harness gets so its writes are authorized and
            // attributed to the agent whose worktree it runs in.
            child_command.env("RK_HOME", self.layout.home().display().to_string());
            child_command.env("RK_AGENT", agent);
            if let Ok(token) = self.layout.agent_auth_token(agent) {
                child_command.env("RK_AUTH_TOKEN", token);
            }
        }
        for (name, value) in env {
            child_command.env(name, value);
        }
        // See rk_core::exec::close_extra_fds: a captured-output pipe
        // created elsewhere in the daemon (a concurrent `git` call, another
        // check, a harness launch) can be caught between `pipe()` and its
        // own close-on-exec setup by this exact spawn; without this, this
        // check's child could inherit it and keep that pipe's read side
        // from ever seeing EOF (TKT-bikuz-kumuz-zutit).
        rk_core::exec::close_extra_fds(child_command.as_std_mut());
        let child = child_command.spawn().map_err(|e| {
            rk_core::Error::other(format!("run step: failed to spawn `{command}`: {e}"))
        })?;
        // Durable counterpart to `group_guard` inside `collect_child_output`:
        // that guard only reaches this child for as long as ITS OWN task is
        // alive, which a daemon restart cannot guarantee (see
        // `ManagedChildMarker`'s doc comment). Held in this local, so it is
        // dropped — durable marker removed — on every exit from this
        // `.await` below, including this whole future being dropped out from
        // under it by `verify_repo_check`'s cancellation race.
        let _managed_marker = child
            .id()
            .map(|pid| ManagedChildMarker::create(self.layout, pid));

        collect_child_output(child, timeout, command).await
    }

    /// Persist a bounded, durable `(artifact, <repo>, gate-failure)` tuple for
    /// a failed (or timed-out) `run` step. Without this, the only trace of a
    /// gate's own verdict is `ctx.previous_result`, which the very next `run`
    /// step (an escalation check, a `landing-report-gate-failure`) overwrites
    /// — so once the workflow routes past this step, everything but a
    /// composed one-line instance error is gone (TKT-01M02AMKD24WZVVMARJPXKYKSW).
    /// Called unconditionally for a non-"pass" verdict, independent of whether
    /// this step also fails the instance via `expectExit`.
    #[allow(clippy::too_many_arguments)]
    fn record_gate_failure(
        &self,
        id: &str,
        repo: &str,
        agent: &str,
        command: &str,
        exit: i64,
        verdict: &str,
        timed_out: bool,
        signal: Option<i32>,
        stdout: &str,
        stdout_truncated: bool,
        stderr: &str,
        stderr_truncated: bool,
        history: &[Value],
    ) {
        let failing_tests = extract_failing_tests(stdout);
        let payload = json!({
            "instance": id,
            "agent": agent,
            "command": command,
            "exit": exit,
            "verdict": verdict,
            "timed_out": timed_out,
            "signal": signal,
            "stdout_tail": bounded_tail(stdout, GATE_EVIDENCE_LIMIT),
            "stdout_truncated": stdout_truncated,
            "stderr_tail": bounded_tail(stderr, GATE_EVIDENCE_LIMIT),
            "stderr_truncated": stderr_truncated,
            "failing_tests": failing_tests,
            "retries": history,
        });
        let _ = self.space.out(rk_core::tuple::Tuple::new(
            Category::Artifact,
            repo_identity(self.layout, repo),
            "gate-failure",
            "daemon",
            payload,
        ));
    }

    /// Durable queue-wait/execution timing for one check that engaged the
    /// bounded per-repo verification admission queue (TKT-01M0HNESEECWWFQF8X6VH1XSJ6)
    /// — a no-op when it didn't (`queue_wait_ms` is `None` exactly when
    /// admission control was disabled, matching [`run`](Self::run)(Self::run)'s
    /// own admission block). Written unconditionally otherwise, whatever the
    /// verdict: an operator diagnosing contention needs the failed/timed-out
    /// runs' timing as much as the passing ones'.
    ///
    /// Scoped by [`repo_identity`] — the SAME
    /// resolution [`run`](Self::run)(Self::run) used to key the
    /// admission permit itself (continuation of TKT-01M0P5NM51SKT5ABXRCDZD07J3)
    /// — rather than `repo_name_of`'s directory basename. Before this, the
    /// event's scope and the semaphore's key could disagree: a workflow `run`
    /// step's absolute repo path admitted against one bound while its event
    /// logged under a basename that happened to look identical to a landing
    /// gate's bare name, so the log read as unified even when execution was
    /// split across two independent semaphores.
    fn record_verification_admission_event(&self, outcome: VerificationAdmissionOutcome<'_>) {
        let Some(queue_wait_ms) = outcome.queue_wait_ms else {
            return;
        };
        let duration_ms = u64::try_from(outcome.duration.as_millis()).unwrap_or(u64::MAX);
        let _ = self.space.out(
            Tuple::new(
                Category::Event,
                repo_identity(self.layout, outcome.repo),
                VERIFICATION_ADMISSION_IDENTITY,
                "daemon",
                json!({
                    "agent": outcome.agent,
                    "command": outcome.command,
                    "queue_wait_ms": queue_wait_ms,
                    "duration_ms": duration_ms,
                    "exit": outcome.exit,
                    "verdict": outcome.verdict,
                }),
            )
            .with_lifecycle(Lifecycle::Furniture),
        );
    }

    /// Look up a named check in the repo's registry (`<repo>/.rk/checks.cue`).
    /// Fails closed: a missing registry, an unparseable one, or an unknown name
    /// all error rather than silently running nothing.
    pub(crate) fn find_check(&self, repo: &str, name: &str) -> rk_core::Result<rk_workflow::Check> {
        let file = std::path::PathBuf::from(repo)
            .join(".rk")
            .join("checks.cue");
        if !file.exists() {
            return Err(rk_core::Error::other(format!(
                "run step: check '{name}' referenced but no registry at {}",
                file.display()
            )));
        }
        let checks = rk_workflow::load_checks(&file)?;
        checks.into_iter().find(|c| c.name == name).ok_or_else(|| {
            rk_core::Error::other(format!(
                "run step: no check named '{name}' in {}",
                file.display()
            ))
        })
    }
}

/// The effective parameters of a `run` step after named-check resolution and
/// policy enforcement — a raw command or a repo-registered check collapse to the
/// same shape here.
/// Crate-scoped alongside [`ManagedVerification::run`]: a daemon-native
/// caller (the T2 landing pipeline) builds this input shape itself instead of
/// going through a workflow `run` step.
pub(crate) struct ResolvedRun {
    pub(crate) command: String,
    pub(crate) cwd: Option<String>,
    pub(crate) expect_exit: Option<i64>,
    pub(crate) timeout: String,
    pub(crate) on_timeout: OnTimeout,
    pub(crate) environment_policy: rk_workflow::CheckEnvironmentPolicy,
    /// Extra attempts on a non-"pass" verdict. Step-only, like `on_timeout` —
    /// never inherited from a named check.
    pub(crate) retry_on_fail: u32,
    /// Whether this run contends for the shared `CARGO_TARGET_DIR` and must
    /// be serialized in [`ManagedVerification::run`] against every other
    /// same-repo run/check that also sets this
    /// ([`Check::shared_cargo_target`](rk_workflow::Check::shared_cargo_target),
    /// TKT-01M0CFA1RX36SJ7DV4YWGHQ9BT). Always false for a raw `command` —
    /// only a repo-registered named check can opt in.
    pub(crate) shared_cargo_target: bool,
}

/// What a blown `run` wall-clock bound does to the instance (TKT-169).
///
/// The command is killed either way — `ProcessGroupGuard` owns that, and a
/// hung suite never survives its budget. The choice here is only whether the
/// kill is reported as an ERROR (which ends the run where it stands) or as a
/// RESULT the following steps get to route on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OnTimeout {
    /// Fail the instance immediately. The default, and the only behaviour before
    /// TKT-169.
    Fail,
    /// Report `{exit: 124, timed_out: true, verdict: "timeout"}` and keep going,
    /// so the workflow decides what too-slow means. Not a weakening: 124 is not
    /// 0, so every exit-based gate still rejects it.
    Continue,
}

impl OnTimeout {
    /// Parse the schema's `onTimeout` string. Fails closed on anything else: a
    /// typo must not quietly resolve to the permissive-looking arm (nor to the
    /// strict one, which would hide the typo until a timeout finally happened).
    pub(crate) fn parse(raw: &str) -> rk_core::Result<Self> {
        match raw {
            "fail" => Ok(Self::Fail),
            "continue" => Ok(Self::Continue),
            other => Err(rk_core::Error::other(format!(
                "run step: unknown onTimeout {other:?} (expected \"fail\" or \"continue\")"
            ))),
        }
    }
}

/// Exit code reported for a command killed by its wall-clock bound — the
/// `timeout(1)` convention, so shell-side readers already know it. A suite can
/// exit 124 on its own, which is exactly why the result also carries the
/// unambiguous `timed_out` / `verdict` fields for routing.
pub(crate) const TIMEOUT_EXIT: i64 = 124;

/// Exit code reported when a check never ran at all because it could not
/// acquire the shared-target-dir test-execution lock (`TestExecLock`) within
/// its own declared timeout. Distinct from [`TIMEOUT_EXIT`], which means the
/// command itself started and was killed — this means it never started.
pub(crate) const LOCK_TIMEOUT_EXIT: i64 = -2;

/// Durable `(Event, <repo>, "verification_admission")` timing record
/// (`Furniture`), written once per check that actually engaged the bounded
/// per-repo admission queue (TKT-01M0HNESEECWWFQF8X6VH1XSJ6) — landing gate,
/// workflow `run` step, or `verify.run` alike, whatever the verdict. Without
/// this the ticket's queue-wait/execution timing requirement has no durable
/// record: `landing_gate_pass` (commit `3d47d08`, "perf: reuse landing gate
/// proof during review") only covers a full green LANDING gate, not every
/// individual managed check.
pub(crate) const VERIFICATION_ADMISSION_IDENTITY: &str = "verification_admission";

/// Durable `(Event, <repo>, "verification_proof")` cache entry (`Furniture`)
/// recorded by [`ManagedVerification::verify_repo_check`] on a passing verdict for
/// a clean-worktree candidate. Looked up again on a later exact-key match
/// (same repo, candidate sha, check command, toolchain, environment policy —
/// TKT-01M0HNESEECWWFQF8X6VH1XSJ6's dedup requirement) to skip a redundant
/// re-run entirely, admission queue included.
pub(crate) const VERIFICATION_PROOF_IDENTITY: &str = "verification_proof";

/// Durable `(Event, <repo>, "verification_cancelled")` outcome recorded by
/// [`ManagedVerification::verify_repo_check`] when its requesting agent is
/// interrupted/dismissed/dies, or its RPC caller disconnects, before the run
/// settled (TKT-01M0PA6C5WYRWS757R1SS2F2GR). Never written for a run that
/// completes on its own, whatever its verdict — that's
/// [`VERIFICATION_ADMISSION_IDENTITY`]'s job — and a cancelled run never
/// writes [`VERIFICATION_PROOF_IDENTITY`] either, so it can never be reused.
pub(crate) const VERIFICATION_CANCELLED_IDENTITY: &str = "verification_cancelled";

/// Pause between a failed attempt and a `retryOnFail` retry. Fixed rather than
/// configurable: this exists to ride out a transient condition (machine load,
/// a build-lock hold), not to be tuned per workflow.
pub(crate) const RETRY_BACKOFF: Duration = Duration::from_secs(5);

/// Hard cap on `retryOnFail` — mirrors `#RunStep.retryOnFail` in schema.cue
/// (`int & >=0 & <=20`). Enforced again here, independent of the schema
/// bound, so `resolved.retry_on_fail + 1` can never approach u32::MAX
/// (TKT-01M02QT9KTDY2CN6YJEVP3VCF8): unbounded, that addition panics on
/// overflow in debug and, wrapped in release, would settle the attempt loop
/// with zero real attempts.
pub(crate) const MAX_RETRY_ON_FAIL: u32 = 20;

/// Daemon-side counterpart of the schema.cue `retryOnFail` bound. A raw
/// negative value never reaches here — `u32` deserialization already refuses
/// it when a workflow definition is loaded — but an over-cap value up to
/// `u32::MAX` is a valid `u32` and would otherwise reach
/// `resolved.retry_on_fail + 1` unbounded. Kept as a free function (no
/// `&self`) so it is unit-testable without standing up a full
/// `WorkflowEngine`.
pub(crate) fn validate_retry_on_fail(value: u32) -> rk_core::Result<()> {
    if value > MAX_RETRY_ON_FAIL {
        return Err(rk_core::Error::other(format!(
            "run step: retryOnFail {value} exceeds cap {MAX_RETRY_ON_FAIL}"
        )));
    }
    Ok(())
}

/// Bound on the stdout/stderr tail kept in a durable `gate-failure` artifact.
/// Generous enough to usually catch a cargo test summary's `failures:` list,
/// bounded so a runaway suite cannot blow up the tuplespace.
pub(crate) const GATE_EVIDENCE_LIMIT: usize = 8000;

/// The outcome of running a `run` step's command to completion or to its bound.
#[derive(Debug)]
pub(crate) enum RunOutcome {
    Completed {
        status: std::process::ExitStatus,
        stdout: Vec<u8>,
        stdout_truncated: bool,
        stderr: Vec<u8>,
        stderr_truncated: bool,
    },
    /// The wall-clock bound elapsed and the child was killed. Returned
    /// regardless of [`OnTimeout`] policy — `collect_child_output` only
    /// collects the outcome; `run_command` is what turns a `Fail`-policy
    /// timeout into an `Err`, and only after recording gate-failure evidence
    /// (TKT-01M02QT9KTDY2CN6YJEVP3VCF8).
    TimedOut,
}

/// A single `run_check_in` attempt once its outcome is fully decoded and
/// classified — the payload `settled` carries out of the retry loop.
pub(crate) struct SettledAttempt {
    exit: i64,
    stdout: String,
    stdout_truncated: bool,
    stderr: String,
    stderr_truncated: bool,
    timed_out: bool,
    no_exit_code: bool,
    signal: Option<i32>,
    verdict: &'static str,
}

/// Best-effort timing [`ManagedVerification::run`] reports into as it
/// goes, for a caller (`verify_repo_check`) racing the whole call against
/// cancellation. Written exactly once, right after the admission queue
/// settles — never updated again — so a cancellation landing before that
/// point sees both fields `None` ("still queued, never started"), and one
/// landing after sees both set ("ran for at least this long before it was
/// cancelled").
///
/// `queued_at_wall`/`started_at_wall` and [`Settled::ended_at_wall`] are the
/// real wall-clock boundaries, each stamped from [`SpanClock`] at the exact
/// statement that also takes the paired `Instant::now()` — never
/// reconstructed later. A caller building a span from a settled run reads
/// `settled` instead of calling `execution_started_at.elapsed()` itself: the
/// latter re-measures elapsed time at whatever later moment the caller
/// happens to get around to it (after a `.await` for further pipeline work),
/// which is exactly the delayed-publish drift TKT-hodij-lujak-kibon reported
/// (span.rs's `from_durations` doc). `settled` is written exactly once, at
/// the moment execution genuinely finishes inside [`ManagedVerification::run`],
/// so every later reader — however much real time has passed by the time it
/// gets around to reading it — sees the same true boundary.
#[derive(Default)]
pub(crate) struct RunProgress {
    pub(crate) queue_wait_ms: Option<u64>,
    pub(crate) execution_started_at: Option<Instant>,
    pub(crate) queued_at_wall: Option<DateTime<Utc>>,
    pub(crate) started_at_wall: Option<DateTime<Utc>>,
    pub(crate) settled: Option<Settled>,
}

/// The frozen outcome timing of a run that actually finished (as opposed to
/// one raced away by cancellation before it settled) — see [`RunProgress`]'s
/// doc for why this is captured once rather than re-derived per reader.
#[derive(Clone, Copy)]
pub(crate) struct Settled {
    pub(crate) duration_ms: u64,
    pub(crate) ended_at_wall: DateTime<Utc>,
}

impl RunProgress {
    /// Best-effort admission-queue wait this check settled with, once
    /// `run_check_in`'s admission queue has settled — `None` before that
    /// point, or when admission was disabled. Exposed for the landing pipeline's own
    /// durable edge events (`LandingPipeline::run_gates_at`), which race no
    /// cancellation and so has no other use for the rest of [`RunProgress`].
    pub(crate) fn queue_wait_ms(&self) -> Option<u64> {
        self.queue_wait_ms
    }
}

/// One check's admission-relevant outcome, bundled so
/// [`record_verification_admission_event`](ManagedVerification::record_verification_admission_event)
/// stays under the clippy `too_many_arguments` threshold without an
/// `#[allow]` (continuation of TKT-01M0P5NM51SKT5ABXRCDZD07J3) — see that
/// method for what each field means.
pub(crate) struct VerificationAdmissionOutcome<'a> {
    repo: &'a str,
    agent: &'a str,
    command: &'a str,
    queue_wait_ms: Option<u64>,
    duration: Duration,
    exit: i64,
    verdict: &'a str,
}

/// One `verify.run` call's `Phase::VerificationQueued` occurrence, bundled
/// so [`record_ad_hoc_verification_span`](ManagedVerification::record_ad_hoc_verification_span)
/// stays under the clippy `too_many_arguments` threshold without an
/// `#[allow]`, matching [`VerificationAdmissionOutcome`] above — see that
/// method for what each field means.
pub(crate) struct AdHocVerificationSpan<'a> {
    task: &'a str,
    repo_name: &'a str,
    check_name: &'a str,
    candidate: &'a str,
    queued_at: Option<DateTime<Utc>>,
    started_at: Option<DateTime<Utc>>,
    ended_at: Option<DateTime<Utc>>,
    queue_wait_ms_monotonic: Option<u64>,
    duration_ms_monotonic: Option<u64>,
    proof_reused: bool,
    terminal_reason: &'a str,
}

/// Decode a `spawn_check_child` outcome into the flat tuple `run_check_in`
/// tracks. Factored out so a retried outcome (the shared cargo target-dir
/// contention retry, and the initial attempt) decode identically.
///
/// The trailing `(bool, Option<i32>)` is `(no_exit_code, signal)`.
/// `no_exit_code` is true exactly when `status.code()` came back `None` —
/// the process never chose its own exit status at all, whether or not this
/// platform can also decode WHICH signal killed it. This is the classifier
/// `run_check_in` uses to tell a genuine "the suite said no" (a real exit
/// code, however nonzero) apart from "the process was killed out from under
/// the check" (OOM killer, an external `kill -9`, the runner itself dying,
/// or any other runner-loss shape reported this way) — an infrastructure
/// death, not a verdict on the branch. `signal` is carried alongside purely
/// as richer evidence when this platform can decode it (Unix); its absence
/// must never suppress the `no_exit_code` classification itself, or a
/// non-Unix runner (or a runner-loss shape with no decodable signal) would
/// silently fall back to treating an infrastructure death as an ordinary
/// "fail" and never get retried. A `TimedOut` outcome is never confused with
/// this: it is `collect_child_output` itself killing the child on the
/// wall-clock bound, reported as its own variant with no `ExitStatus` to
/// inspect at all.
pub(crate) fn decode_run_outcome(
    outcome: RunOutcome,
    command: &str,
    resolved: &ResolvedRun,
) -> (i64, String, bool, String, bool, bool, bool, Option<i32>) {
    match outcome {
        RunOutcome::Completed {
            status,
            stdout,
            stdout_truncated,
            stderr,
            stderr_truncated,
        } => {
            let no_exit_code = status.code().is_none();
            #[cfg(unix)]
            let signal = {
                use std::os::unix::process::ExitStatusExt;
                status.signal()
            };
            #[cfg(not(unix))]
            let signal: Option<i32> = None;
            (
                status.code().unwrap_or(-1) as i64,
                String::from_utf8_lossy(&stdout).into_owned(),
                stdout_truncated,
                String::from_utf8_lossy(&stderr).into_owned(),
                stderr_truncated,
                false,
                no_exit_code,
                signal,
            )
        }
        RunOutcome::TimedOut => (
            TIMEOUT_EXIT,
            String::new(),
            false,
            format!(
                "run step: `{command}` timed out after {} and was killed",
                resolved.timeout
            ),
            false,
            true,
            false,
            None,
        ),
    }
}

/// Matches only the shared `CARGO_TARGET_DIR` cross-process contention
/// signature (docs/2026-08-19-tkt-hot-scan-target-dir-contention.md): a
/// build artifact resolved by one process gets pruned by a concurrent
/// `cargo build` in another worktree before this process can exec it.
/// Deliberately narrow -- a real compile error or test failure must never
/// match this and get a free retry.
pub(crate) fn is_cargo_target_contention_signature(stdout: &str, stderr: &str) -> bool {
    let hits = |s: &str| {
        s.contains("could not execute process")
            && s.contains("(never executed)")
            && s.contains("No such file or directory (os error 2)")
    };
    hits(stdout) || hits(stderr)
}

/// A gate child's captured stream: bytes bounded to [`MAX_RUN_OUTPUT_BYTES`],
/// keeping the TAIL of the stream (where a suite's failure summary lives)
/// rather than the head, plus whether the raw stream actually exceeded that
/// bound.
pub(crate) struct CappedOutput {
    pub(crate) bytes: Vec<u8>,
    pub(crate) truncated: bool,
}

/// Stream a child's output to completion, never erroring on volume alone —
/// only a genuine read failure returns `Err`. A chatty-but-otherwise-healthy
/// suite must still run to its real exit code and route/retry normally
/// (gate-children-truncate-not-kill): exceeding the cap used to abort the
/// read (and, via the caller's `?`, the whole instance) instead of just
/// bounding what is kept.
pub(crate) async fn read_capped<R>(mut reader: R) -> rk_core::Result<CappedOutput>
where
    R: AsyncRead + Unpin,
{
    let mut output = Vec::new();
    let mut truncated = false;
    let mut chunk = [0_u8; 8192];
    loop {
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            return Ok(CappedOutput {
                bytes: output,
                truncated,
            });
        }
        output.extend_from_slice(&chunk[..read]);
        if output.len() > MAX_RUN_OUTPUT_BYTES {
            truncated = true;
            let excess = output.len() - MAX_RUN_OUTPUT_BYTES;
            output.drain(..excess);
        }
    }
}

pub(crate) async fn abort_task<T>(task: &mut JoinHandle<T>) {
    task.abort();
    let _ = task.await;
}

/// One row of the system-wide process table, used to walk a managed check
/// child's REAL descendant tree instead of trusting it to stay in the one
/// process group `spawn_check_child` put its leader in. `mise` (and
/// potentially any other check command) does not keep every descendant in
/// that leader's group — a live smoke test found `mise run verify` moving
/// its own task execution into a SECOND, freshly created group (a `shell`
/// child, and `cargo` under that), which a plain `kill(-leader_pid,
/// SIGKILL)` never reaches: killing the leader's group only removes the
/// leader, and the orphaned nested group survives, reparented to init
/// (TKT-01M0PN2JSN24AHGQHFJ4XGAVKD). Captured as `(pid, ppid, pgid)` so
/// descendants can be found by walking live `ppid` links and the exact
/// groups to signal collected from `pgid`. `stat` is the same STAT field
/// `pid_alive` test helpers already poll elsewhere in this file (a leading
/// `Z` or an empty read means gone/zombie, never "still doing work") — kept
/// here too so a supervisor liveness check can tell a merely-listed pid from
/// one actually alive without a second `ps` invocation. `comm` is that same
/// liveness check's OTHER need: which command a live descendant actually is,
/// so it can tell a real verifier/build descendant from an arbitrary blocked
/// subprocess (a bare `sleep`, a hung network read) — see
/// [`is_verifier_command`].
pub(crate) struct ProcessTableRow {
    pub(crate) pid: u32,
    pub(crate) ppid: u32,
    pub(crate) pgid: u32,
    pub(crate) stat: String,
    pub(crate) comm: String,
}

/// A snapshot of every process the OS reports right now, via `ps`(1) — the
/// same portable, no-extra-dependency approach `process_signature` already
/// uses elsewhere in this file. Best-effort: an empty result (a transient
/// `ps` failure, or none on a platform without it) simply means a caller
/// falls back to signalling only the root process group it already knew
/// about, same as before this tree-walk existed.
pub(crate) fn live_process_table() -> Vec<ProcessTableRow> {
    let mut cmd = std::process::Command::new("ps");
    cmd.args(["-Ao", "pid=,ppid=,pgid=,stat=,comm="]);
    rk_core::exec::close_extra_fds(&mut cmd);
    let Ok(output) = cmd.output() else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let pid = fields.next()?.parse().ok()?;
            let ppid = fields.next()?.parse().ok()?;
            let pgid = fields.next()?.parse().ok()?;
            let stat = fields.next()?.to_string();
            // `comm` is the last field but may itself be an absolute path
            // (macOS `ps` reports one for some binaries, a bare name for
            // others) — collected as everything remaining on the line, not
            // just one more `split_whitespace` token, since a path could in
            // principle contain no further whitespace anyway; normalized to
            // its basename by `is_verifier_command`.
            let comm = fields.next()?.to_string();
            Some(ProcessTableRow {
                pid,
                ppid,
                pgid,
                stat,
                comm,
            })
        })
        .collect()
}

/// Whether `row` is a real, running process right now — not exited and not a
/// zombie awaiting reap (a leading `Z` STAT), which `kill(pid, 0)` alone
/// cannot distinguish from "still doing work" (the same gotcha
/// `rk-harness/src/fake.rs`'s `still_running` helper documents).
pub(crate) fn row_alive(row: &ProcessTableRow) -> bool {
    !(row.stat.is_empty() || row.stat.starts_with('Z'))
}

/// Command names recognized as genuine verifier/build work — matched against
/// `ps`'s `comm` (basename only; `ps` itself may report a full path).
/// Deliberately a static allowlist, not a denylist: an unrecognized live
/// descendant is NOT evidence of anything by this function, on purpose. A
/// live regression test found the opposite policy (any live descendant at
/// all counts) excusing a genuinely wedged fake harness whose script's LAST
/// command forked a plain `sleep` — indistinguishable from a real compiler
/// descendant by process-tree PRESENCE alone. Naming the command is the
/// cheapest signal that actually tells the two apart. `rk` covers both an
/// agent's own `rk verify` CLI call and any other `rk` subcommand it might
/// shell out to; `mise` covers the `mise run <check>`/`mise verify` task
/// runner this repo's own checks are declared through; `cargo`/`rustc` cover
/// a rat directly running `cargo test`/`cargo build` without going through
/// either. Extend this list, don't loosen the policy, if a legitimate
/// descendant is missed.
pub(crate) fn is_verifier_command(comm: &str) -> bool {
    let name = comm.rsplit('/').next().unwrap_or(comm);
    matches!(name, "rk" | "mise" | "cargo" | "rustc")
}

/// Liveness evidence for one harness generation's own OS process, gathered
/// directly from the process table rather than inferred from the daemon's
/// event stream: whether `root` itself is still a real running process, and
/// how many of its live descendants are RECOGNIZED verifier/build work (see
/// [`is_verifier_command`]) — e.g. a `cargo test`/compiler the rat's own
/// shell tool-use launched, or an `rk verify` CLI call blocked on the
/// daemon's RPC, neither of which the harness event stream has any
/// visibility into on its own. Reuses the exact `ppid`-walk
/// [`descendant_process_groups`] already does for check-process teardown
/// (TKT-01M0PN2JSN24AHGQHFJ4XGAVKD), just counting live pids instead of
/// collecting groups to signal.
pub(crate) struct ProcessLiveness {
    pub(crate) child_alive: bool,
    /// Count of live descendants whose OWN command is recognized as
    /// verifier/build work. An arbitrary live descendant that is NOT
    /// recognized (a bare `sleep`, a hung shell, a blocked network client)
    /// is deliberately excluded — see [`is_verifier_command`].
    pub(crate) live_verifier_descendants: usize,
}

pub(crate) fn process_liveness(root: u32) -> ProcessLiveness {
    process_liveness_with_snapshots(root, live_process_table)
}

/// A successful `ps` invocation can omit a live descendant under fork/exec
/// contention. Corroborate negative evidence once before the silent-worker
/// sweep acts on it. Keep snapshots separate: joining rows across them could
/// invent an ancestry chain that never existed. There is no retry on positive
/// evidence, no sleep, and never more than two reads for one observation.
fn process_liveness_with_snapshots(
    root: u32,
    mut snapshot: impl FnMut() -> Vec<ProcessTableRow>,
) -> ProcessLiveness {
    let first = process_liveness_in_table(root, &snapshot());
    if first.child_alive && first.live_verifier_descendants > 0 {
        first
    } else {
        process_liveness_in_table(root, &snapshot())
    }
}

fn process_liveness_in_table(root: u32, table: &[ProcessTableRow]) -> ProcessLiveness {
    let child_alive = table.iter().any(|row| row.pid == root && row_alive(row));
    let mut children: HashMap<u32, Vec<u32>> = HashMap::new();
    for row in table {
        children.entry(row.ppid).or_default().push(row.pid);
    }
    let mut visited = HashSet::from([root]);
    let mut queue = VecDeque::from([root]);
    let mut live_verifier_descendants = 0usize;
    while let Some(pid) = queue.pop_front() {
        for &child in children.get(&pid).into_iter().flatten() {
            if visited.insert(child) {
                queue.push_back(child);
                if let Some(row) = table.iter().find(|row| row.pid == child) {
                    if row_alive(row) && is_verifier_command(&row.comm) {
                        live_verifier_descendants += 1;
                    }
                }
            }
        }
    }
    ProcessLiveness {
        child_alive,
        live_verifier_descendants,
    }
}

/// Every distinct process-group id live under `root` right now: `root`'s own
/// group plus every descendant's, found by walking `ppid` links in `table`
/// breadth-first from `root` — so a descendant that `setsid`/`setpgid`ed
/// itself into a group of its own (exactly what a `mise` task does) is still
/// found and its group still collected, not just the leader's. `root` is
/// treated as its own group even if `table` (a racy snapshot) no longer
/// contains it, which is correct for every caller here: they only ever name
/// a process-group LEADER (`.process_group(0)` makes a spawned check's own
/// pid equal its pgid).
pub(crate) fn descendant_process_groups(root: u32, table: &[ProcessTableRow]) -> Vec<i32> {
    let mut children: HashMap<u32, Vec<u32>> = HashMap::new();
    for row in table {
        children.entry(row.ppid).or_default().push(row.pid);
    }
    let mut groups = HashSet::new();
    groups.insert(root as i32);
    let mut visited = HashSet::from([root]);
    let mut queue = VecDeque::from([root]);
    while let Some(pid) = queue.pop_front() {
        if let Some(row) = table.iter().find(|row| row.pid == pid) {
            groups.insert(row.pgid as i32);
        }
        for &child in children.get(&pid).into_iter().flatten() {
            if visited.insert(child) {
                queue.push_back(child);
            }
        }
    }
    groups.into_iter().collect()
}

/// Guarantees a gate child's WHOLE process TREE dies, not just the single
/// process group `spawn_check_child` put its leader in.
/// `spawn_check_child` puts the child in its own group via
/// `.process_group(0)` (mirroring rk-harness's launcher) and deliberately
/// does NOT set `.kill_on_drop(true)` — this guard is the SOLE killer, by
/// design: it walks the live descendant tree from that pid
/// ([`descendant_process_groups`]) and sends the negative-pid signal to
/// EVERY group found, reaching a `mise`/`cargo`/`rustc` descendant even if it
/// moved itself into a group of its own. That walk needs the leader and its
/// descendants to still be alive and correctly parented when it runs — a
/// `kill_on_drop` racing ahead of it (as `abort_task` used to trigger on the
/// timeout/error paths below) would reparent a nested descendant to init
/// first, breaking the `ppid`-chain discovery that finds its group at all
/// (TKT-01M0PN2JSN24AHGQHFJ4XGAVKD). Disarmed only on the clean-completion
/// path — every other exit from `collect_child_output` (reader/wait join
/// failure, timeout, or this function's future simply being dropped out
/// from under it) drops the guard still armed and kills whatever the check
/// left running.
pub(crate) struct ProcessGroupGuard(Option<u32>);

impl ProcessGroupGuard {
    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        if let Some(pid) = self.0 {
            let table = live_process_table();
            for pgid in descendant_process_groups(pid, &table) {
                // SAFETY: plain kill(2) on a process group either this
                // process created directly (`.process_group(0)`) or a live
                // descendant of it created for itself — discovered via the
                // OS's own real `ppid` links moments before this signal, so
                // never a group this daemon has no relation to.
                unsafe {
                    libc::kill(-pgid, libc::SIGKILL);
                }
            }
        }
    }
}

/// Durable counterpart to [`ProcessGroupGuard`]: while that guard reaches a
/// check's whole process group for as long as ITS OWNING PROCESS is alive,
/// nothing previously reached it across a daemon restart. A check child is
/// spawned by a connection-handling task that `server.rs`'s accept loop
/// starts with a bare, detached `tokio::spawn` — not the `background_tasks`
/// `JoinSet` `Daemon::run`'s own doc comment explains exists specifically so
/// aborting the outer future tears down every task it owns. So a same-
/// process simulated crash (`handle.abort()`, the technique
/// `live_landing_restart.rs` and `managed_verification_cancel_e2e.rs` both
/// use) never reaches that task, and a real `SIGKILL` reaches the task but
/// not this child either — it lives in ITS OWN process group precisely so an
/// operator `interrupt`'s SIGINT does not land on it, which equally means no
/// signal the OS delivers to the dying daemon process ever reaches it. Either
/// way the child is orphaned, not killed, unless something explicit reaps it.
///
/// This guard is that "something": written the moment the child's pid is
/// known (mirroring `ProcessGroupGuard`'s own `child.id()` capture),
/// removed on every exit from `spawn_check_child`'s scope including this
/// future being dropped out from under it (the managed-verification
/// cancellation race in `verify_repo_check`) — so a run that finishes,
/// times out, errors, or gets cancelled all leave nothing durable behind.
/// [`reap_stale_managed_children`] is the other half: called once at the
/// START of every `Daemon::run`, before this directory is trusted again, so
/// anything still here at that point can only be an actual orphan left by a
/// daemon generation that is provably gone — not a false positive, since
/// this daemon does not even accept connections yet when it sweeps.
///
/// The marker's content, not just its filename, is what makes reaping safe:
/// a bare pid is not a stable identity across the (however unlikely, however
/// long the gap between a dead generation and the next daemon start) window
/// in which the OS can recycle it for an unrelated process. The file also
/// carries [`process_signature`] as recorded the moment this daemon spawned
/// the child, so the reap sweep can tell "the exact process I spawned,
/// simply still running" apart from "a stranger now squatting its old pid"
/// and refuse to signal the latter. That signature deliberately survives the
/// child later exec'ing into a different command — see `process_signature`.
pub(crate) struct ManagedChildMarker {
    layout: Layout,
    pid: u32,
}

impl ManagedChildMarker {
    pub(crate) fn create(layout: &Layout, pid: u32) -> Self {
        let dir = layout.managed_children_dir();
        let _ = std::fs::create_dir_all(&dir);
        // Best-effort: if the child has already exited in the (negligible)
        // window between `spawn()` and this call, there is no signature to
        // record and therefore nothing safe to reap later — skip the write
        // rather than persist an unverifiable marker. Written atomically
        // (temp file + rename, same directory/filesystem) so a daemon that
        // dies mid-write never leaves a torn, unparseable marker behind for
        // the reap sweep to trip over.
        if let Some(signature) = process_signature(pid) {
            let path = dir.join(pid.to_string());
            let tmp = dir.join(format!("{pid}.tmp-{}", std::process::id()));
            if std::fs::write(&tmp, signature).is_ok() {
                let _ = std::fs::rename(&tmp, &path);
            }
        }
        Self {
            layout: layout.clone(),
            pid,
        }
    }
}

impl Drop for ManagedChildMarker {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(
            self.layout
                .managed_children_dir()
                .join(self.pid.to_string()),
        );
    }
}

/// A best-effort process identity: `pid`'s start time, as `ps` reports it
/// right now. PID plus start time — not `comm` — is deliberate: start time
/// is immutable for the life of a process, but `comm` is not. `sh -c
/// '<single simple command>'` (exactly what `spawn_check_child` runs for
/// every named check and raw `run` step) can have `sh` tail-call-exec
/// directly into that command at any point after `spawn()` returns —
/// same pid, same start time, but `comm` flips from `sh` to whatever the
/// command was. A `comm`-inclusive signature would then stop matching for a
/// process the daemon is still watching, and the reap sweep's fail-closed
/// fence would wrongly treat a genuine orphan as a stranger squatting a
/// recycled pid and leave it running forever. PID + start time alone is not
/// a cryptographic identity — just enough to distinguish "the same process
/// this daemon spawned" from "the OS reused this pid for something this
/// daemon never spawned", which is all [`reap_stale_managed_children`]
/// needs: two distinct processes are vanishingly unlikely to share an exact
/// start second, whereas the SAME process obviously reports the same one
/// every time it's asked, exec or no exec. `None` covers both "no process is
/// live at this pid at all" and "`ps` itself failed" — both must be treated
/// identically by every caller (nothing to compare against, so no confident
/// answer either way).
pub(crate) fn process_signature(pid: u32) -> Option<String> {
    let mut cmd = std::process::Command::new("ps");
    cmd.args(["-o", "lstart=", "-p", &pid.to_string()]);
    rk_core::exec::close_extra_fds(&mut cmd);
    let output = cmd.output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

/// Kill and clear every marker [`ManagedChildMarker`] left behind by a
/// daemon generation that is gone by the time this one starts — see that
/// type's doc comment for why a daemon restart cannot rely on anything else
/// (task-tree teardown, signal delivery) to reach these children. Called
/// once from `Daemon::run`, right after `on_daemon_started` and before the
/// accept loop can serve a single request — so before any NEW managed check
/// can possibly exist — meaning every marker this sees genuinely predates
/// this process.
///
/// Fails CLOSED on identity: a marker is only ever signalled when the pid it
/// names is CURRENTLY live AND its [`process_signature`] still matches what
/// was recorded at spawn time. A pid with no live process at all is simply
/// stale (the child already exited on its own) — nothing to kill. A pid that
/// IS live but whose signature has changed means the OS has handed that pid
/// to a process this daemon never spawned; sending it a signal on the
/// strength of a recycled number alone would be exactly the bug this check
/// exists to prevent, so that case is left strictly alone. Every marker is
/// removed once considered either way, so a later restart never re-examines
/// the same stale entry.
pub(crate) fn reap_stale_managed_children(layout: &Layout) {
    let dir = layout.managed_children_dir();
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return;
    };
    for entry in entries.flatten() {
        if let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() {
            let recorded = std::fs::read_to_string(entry.path()).unwrap_or_default();
            let recorded = recorded.trim();
            if !recorded.is_empty() && process_signature(pid).as_deref() == Some(recorded) {
                // The pid's CURRENT identity was just confirmed, still, to
                // match the process this daemon itself spawned as a
                // process-group leader (`spawn_check_child` always spawns
                // via `.process_group(0)`) — safe to walk its live
                // descendant tree and signal every group found, same as
                // `ProcessGroupGuard` does for a check cancelled while this
                // daemon generation is still alive.
                let table = live_process_table();
                for pgid in descendant_process_groups(pid, &table) {
                    // SAFETY: plain kill(2) on a process group either the
                    // identity-confirmed pid above or one of its live
                    // descendants created for itself.
                    unsafe {
                        libc::kill(-pgid, libc::SIGKILL);
                    }
                }
            }
        }
        let _ = std::fs::remove_file(entry.path());
    }
}

pub(crate) async fn collect_child_output(
    mut child: tokio::process::Child,
    timeout: Duration,
    command: &str,
) -> rk_core::Result<RunOutcome> {
    let mut group_guard = ProcessGroupGuard(child.id());
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| rk_core::Error::other("run step: child stdout was not piped"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| rk_core::Error::other("run step: child stderr was not piped"))?;

    // Put the child in a task whose cancellation/drop semantics own the
    // immediate process. Join failure and timeout both abort this task,
    // dropping (detaching, not killing — no `kill_on_drop` here) the child;
    // `group_guard` above is the only thing that actually signals it and
    // whatever tree it left behind, and it needs the process still alive
    // and correctly parented to find that tree at all.
    let mut wait_task = tokio::spawn(async move { child.wait().await });
    let mut stdout_task = tokio::spawn(read_capped(stdout));
    let mut stderr_task = tokio::spawn(read_capped(stderr));
    let mut status = None;
    let mut stdout = None;
    let mut stderr = None;
    let sleep = tokio::time::sleep(timeout);
    tokio::pin!(sleep);

    while status.is_none() || stdout.is_none() || stderr.is_none() {
        tokio::select! {
            result = &mut wait_task, if status.is_none() => {
                match result {
                    Ok(Ok(exit)) => status = Some(exit),
                    Ok(Err(error)) => {
                        if stdout.is_none() {
                            abort_task(&mut stdout_task).await;
                        }
                        if stderr.is_none() {
                            abort_task(&mut stderr_task).await;
                        }
                        return Err(rk_core::Error::other(format!(
                            "run step: `{command}` failed: {error}"
                        )));
                    }
                    Err(error) => {
                        if stdout.is_none() {
                            abort_task(&mut stdout_task).await;
                        }
                        if stderr.is_none() {
                            abort_task(&mut stderr_task).await;
                        }
                        return Err(rk_core::Error::other(format!(
                            "run step: `{command}` wait task failed: {error}"
                        )));
                    }
                }
            }
            result = &mut stdout_task, if stdout.is_none() => {
                match result {
                    Ok(Ok(bytes)) => stdout = Some(bytes),
                    Ok(Err(error)) => {
                        if status.is_none() {
                            abort_task(&mut wait_task).await;
                        }
                        if stderr.is_none() {
                            abort_task(&mut stderr_task).await;
                        }
                        return Err(error);
                    }
                    Err(error) => {
                        if status.is_none() {
                            abort_task(&mut wait_task).await;
                        }
                        if stderr.is_none() {
                            abort_task(&mut stderr_task).await;
                        }
                        return Err(rk_core::Error::other(format!(
                            "run step: stdout task failed: {error}"
                        )));
                    }
                }
            }
            result = &mut stderr_task, if stderr.is_none() => {
                match result {
                    Ok(Ok(bytes)) => stderr = Some(bytes),
                    Ok(Err(error)) => {
                        if status.is_none() {
                            abort_task(&mut wait_task).await;
                        }
                        if stdout.is_none() {
                            abort_task(&mut stdout_task).await;
                        }
                        return Err(error);
                    }
                    Err(error) => {
                        if status.is_none() {
                            abort_task(&mut wait_task).await;
                        }
                        if stdout.is_none() {
                            abort_task(&mut stdout_task).await;
                        }
                        return Err(rk_core::Error::other(format!(
                            "run step: stderr task failed: {error}"
                        )));
                    }
                }
            }
            _ = &mut sleep => {
                // The child dies here unconditionally: aborting the wait task
                // detaches it, and this function returning (below) drops
                // `group_guard`, whose own tree-walk kill is what actually
                // signals it. What an `OnTimeout::Fail` policy does with
                // this — error out, but only after the
                // caller has had the chance to persist gate-failure evidence —
                // is the caller's decision, not this function's; it just
                // reports the outcome (TKT-01M02QT9KTDY2CN6YJEVP3VCF8).
                if status.is_none() {
                    abort_task(&mut wait_task).await;
                }
                if stdout.is_none() {
                    abort_task(&mut stdout_task).await;
                }
                if stderr.is_none() {
                    abort_task(&mut stderr_task).await;
                }
                return Ok(RunOutcome::TimedOut);
            }
        }
    }

    // The child exited on its own — the group is (or will imminently be)
    // empty either way, and killing it here would race a legitimately
    // finished process; nothing left for `group_guard` to clean up.
    group_guard.disarm();
    let stdout = stdout.expect("stdout completed with all child tasks");
    let stderr = stderr.expect("stderr completed with all child tasks");
    Ok(RunOutcome::Completed {
        status: status.expect("status completed with all child tasks"),
        stdout: stdout.bytes,
        stdout_truncated: stdout.truncated,
        stderr: stderr.bytes,
        stderr_truncated: stderr.truncated,
    })
}

/// PATH for a run-step child: the daemon executable's directory first, then
/// the daemon's inherited PATH. `None` only when the exe location is unknown
/// and there is no inherited PATH to preserve.
pub(crate) fn check_child_path(
    exe: Option<std::path::PathBuf>,
    inherited: Option<std::ffi::OsString>,
) -> Option<std::ffi::OsString> {
    let exe_dir = exe.and_then(|p| p.parent().map(std::path::Path::to_path_buf));
    let mut parts: Vec<std::path::PathBuf> = exe_dir.into_iter().collect();
    if let Some(inherited) = &inherited {
        parts.extend(std::env::split_paths(inherited));
    }
    if parts.is_empty() {
        return None;
    }
    std::env::join_paths(parts).ok().or(inherited)
}

/// When a run step fails immediately after a failed (or timed-out) run step —
/// the escalation-check-after-red-gate shape — the instance error must open
/// with the gate's own result. The escalation's failure is secondary; the gate
/// verdict is why the workflow stopped.
pub(crate) fn prior_gate_failure(previous: Option<&Value>) -> String {
    let Some(previous) = previous else {
        return String::new();
    };
    let verdict = previous["verdict"].as_str().unwrap_or("");
    if verdict != "fail" && verdict != "timeout" {
        return String::new();
    }
    format!(
        "gate failed first: verdict {verdict}, exit {}{}; escalation also failed: ",
        previous["exit"].as_i64().unwrap_or(-1),
        check_failure_detail(
            previous["stdout"].as_str().unwrap_or(""),
            previous["stderr"].as_str().unwrap_or("")
        )
    )
}

/// The last `limit` characters of `text`, trimmed. Shared by the instance-error
/// composer ([`check_failure_detail`], 400 chars) and the durable gate-failure
/// artifact ([`record_gate_failure`](ManagedVerification::record_gate_failure),
/// [`GATE_EVIDENCE_LIMIT`]) — the artifact keeps a longer tail because it is
/// the only copy that outlives the next workflow step.
pub(crate) fn bounded_tail(text: &str, limit: usize) -> String {
    let trimmed = text.trim();
    let start = trimmed
        .char_indices()
        .rev()
        .take(limit)
        .last()
        .map(|(i, _)| i)
        .unwrap_or(0);
    trimmed[start..].to_string()
}

/// Bounded stdout/stderr tails for a failed check's instance error, so the
/// operator sees what the check said, not just that it said no.
pub(crate) fn check_failure_detail(stdout: &str, stderr: &str) -> String {
    let mut detail = String::new();
    for (label, text) in [("stderr", stderr), ("stdout", stdout)] {
        let tail = bounded_tail(text, 400);
        if !tail.is_empty() {
            detail.push_str(&format!("; {label}: {tail}"));
        }
    }
    detail
}

/// Pull failing test names out of a `cargo test` (or compatible) stdout, so a
/// durable gate-failure artifact names what broke instead of just recording
/// that something did. Matches the one line format both the per-binary
/// `failures:` summary and the individual `---- name stdout ----` headers
/// disagree on but every runner prints consistently: `test <name> ... FAILED`.
/// Deliberately NOT parsed from the `failures:` summary block — with
/// `cargo test --workspace` running many binaries, several such blocks can
/// appear and only the last is anywhere near the tail of a bounded capture, so
/// scanning every `... FAILED` line is the only method robust to truncation.
/// Bounded and deduplicated, order preserved; a suite with more than
/// `MAX_FAILING_TESTS` distinct failures is summarized rather than listed in
/// full — the point is to name the failures a human or a peer rat acts on
/// first, not to reproduce the whole log.
pub(crate) const MAX_FAILING_TESTS: usize = 50;

pub(crate) fn extract_failing_tests(stdout: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut names = Vec::new();
    for line in stdout.lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("test ") else {
            continue;
        };
        let Some(name) = rest.strip_suffix(" ... FAILED") else {
            continue;
        };
        if seen.insert(name.to_string()) {
            names.push(name.to_string());
            if names.len() >= MAX_FAILING_TESTS {
                break;
            }
        }
    }
    names
}

/// Best-effort candidate identity for verification proof reuse
/// (TKT-01M0HNESEECWWFQF8X6VH1XSJ6): `Some(sha)` only when `dir` is a git
/// worktree with a clean working tree (no uncommitted changes) — otherwise
/// there is no stable "candidate" a later caller could safely match a cached
/// proof against, so proof reuse must be skipped entirely (never read from,
/// never written to). Deliberately shells out scoped to `dir` itself rather
/// than going through `rk_git::Repo` (whose `discover`/`rev_parse` resolve to
/// the repo's common root, not necessarily this specific linked worktree) —
/// `git -C <dir>` is exactly the worktree under test.
pub(crate) async fn clean_candidate_sha(dir: &Path) -> Option<String> {
    let mut status_cmd = tokio::process::Command::new("git");
    status_cmd
        .arg("-C")
        .arg(dir)
        .args(["status", "--porcelain"]);
    rk_core::exec::close_extra_fds(status_cmd.as_std_mut());
    let status = status_cmd.output().await.ok()?;
    if !status.status.success() || !status.stdout.is_empty() {
        return None;
    }
    let mut head_cmd = tokio::process::Command::new("git");
    head_cmd.arg("-C").arg(dir).args(["rev-parse", "HEAD"]);
    rk_core::exec::close_extra_fds(head_cmd.as_std_mut());
    let head = head_cmd.output().await.ok()?;
    if !head.status.success() {
        return None;
    }
    let sha = String::from_utf8(head.stdout).ok()?;
    let sha = sha.trim();
    if sha.is_empty() {
        None
    } else {
        Some(sha.to_string())
    }
}

/// The exact-match dedup key the ticket asks for: repository, tested
/// candidate sha, check name, command, toolchain, and environment-policy
/// digest, all folded into one canonical sha256 digest
/// ([`rk_core::action::canonical_digest`]). `None` only on a (practically
/// unreachable) serialization failure — callers treat that as "cannot cache
/// this", never as a false hit.
///
/// `check.name` is part of the digest (not just `command`/`toolchain`/
/// `environment_policy`): two differently-named checks that happen to share
/// identical command text are still two distinct checks as far as a caller
/// asking "did check X pass for this candidate" is concerned, and the
/// ticket's own identity list names "check name" explicitly.
pub(crate) fn verification_proof_key(
    repo_name: &str,
    candidate: &str,
    check: &rk_workflow::Check,
) -> Option<String> {
    rk_core::action::canonical_digest(&json!({
        "repo": repo_name,
        "candidate": candidate,
        "check": check.name,
        "command": check.command,
        "toolchain": check.toolchain,
        "environment_policy": check.environment_policy.to_string(),
    }))
    .ok()
}

/// Crate-scoped alongside [`ManagedVerification::run`]: the T2 landing
/// pipeline resolves a named check's `timeout` string into a bound itself,
/// the same way a `run` step does.
pub(crate) fn parse_duration(s: &str) -> rk_core::Result<Duration> {
    let s = s.trim();
    let invalid = || rk_core::Error::other(format!("invalid duration: {s}"));
    // Split on the last *char*, not the last byte: a multibyte suffix (e.g.
    // "5m²", "10µ") would make byte-index split_at panic on a non-boundary.
    // The unit chars (s/m/h) are single-byte ASCII, so trimming one byte off
    // the end when they match is always a valid boundary.
    let (value, mult) = match s.chars().last() {
        Some('s') => (&s[..s.len() - 1], 1u64),
        Some('m') => (&s[..s.len() - 1], 60),
        Some('h') => (&s[..s.len() - 1], 3600),
        _ => (s, 1),
    };
    let n = value.parse::<u64>().map_err(|_| invalid())?;
    // checked_mul: a huge value like "9223372036854775807m" would otherwise
    // panic in debug builds and silently wrap in release.
    n.checked_mul(mult)
        .map(Duration::from_secs)
        .ok_or_else(invalid)
}

/// Keep a noisy or compromised check from turning the daemon into an
/// unbounded stdout/stderr buffer. The cap applies independently to each
/// stream; exceeding it fails the run and kills the child.
pub(crate) const MAX_RUN_OUTPUT_BYTES: usize = 256 * 1024;

/// Mirrors rk-workflow's `RunStep` timeout default; a referencing `run` step
/// left at this value defers to a named check's own timeout (TKT-30).
pub(crate) const DEFAULT_RUN_TIMEOUT: &str = "10m";

pub(crate) fn repo_identity(layout: &Layout, repo: &str) -> String {
    let path = std::path::Path::new(repo);
    if path.is_absolute() {
        if let Ok(registry) = crate::repos::RepoRegistry::load(&layout.home().join("repos.json")) {
            if let Some(record) = registry.get_by_path(path) {
                return record.name.clone();
            }
        }
    }
    repo.to_string()
}

/// Serializes the *test-execution* phase of a repo-registered check against
/// every other same-repo check that opts in
/// ([`rk_workflow::Check::shared_cargo_target`], TKT-01M0CFA1RX36SJ7DV4YWGHQ9BT).
///
/// Only relevant when `[disk] shared_cargo_target` points every spawned
/// agent's `CARGO_TARGET_DIR` at one shared `<RK_HOME>/cargo-target-cache/<repo>`
/// directory (TKT-01M04D1QDBNCF0T0D0EHRVNJV5). Cargo's own target-dir lock
/// only covers the *build* phase of a single `cargo test`/`cargo build`
/// invocation — it is released as soon as that invocation's build finishes,
/// before the invocation execs the test binaries it just resolved paths for.
/// A second, concurrent invocation against the same shared dir can acquire
/// cargo's lock in that gap, recompile, and garbage-collect a test binary the
/// first invocation is about to exec, producing `could not execute process
/// ... (never executed) ... No such file or directory`. Fully serializing
/// every opted-in check's entire run (build + exec together, not just the
/// exec sliver) closes the gap: as long as no other check touches the shared
/// dir while one is mid-flight, nothing it resolved a path for can be pruned
/// out from under it.
///
/// Keyed per repo only (the target dir is shared per repo, not per branch/
/// worktree/target) — distinct from [`MergeQueue`], which keys on
/// `(repo_root, target)` for a different resource (the git ref). One process
/// (the daemon) holds this, so a plain per-key async `Mutex` is enough; no
/// cross-process `flock` is needed even though the *contended resource*
/// (the shared target dir) is filesystem state, because it is only ever
/// touched by checks this same daemon spawns.
#[derive(Default)]
pub(crate) struct TestExecLock {
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl TestExecLock {
    /// Acquire the lock for `repo`. The returned guard is held for one
    /// check's entire run (every retry attempt); the next waiter proceeds
    /// only once it drops. Unbounded here — [`ManagedVerification::run`]
    /// wraps the await in a `tokio::time::timeout` bounded by the check's own
    /// declared timeout, so a caller never waits past that budget even though
    /// this method alone cannot starve (every holder is itself bounded by its
    /// own check timeout, so the queue always drains).
    pub(crate) async fn acquire(&self, repo: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.locks.lock().unwrap();
            Arc::clone(
                locks
                    .entry(repo.to_string())
                    .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
            )
        };
        lock.lock_owned().await
    }
}

/// Bounded per-repository admission queue for daemon-managed verification
/// runs (TKT-01M0HNESEECWWFQF8X6VH1XSJ6): the ONE gate `run_check_in` sends
/// every managed check through, whether it was dispatched by a
/// landing gate, a workflow `run` step, or the `verify.run` RPC an
/// agent/reviewer's own completion check calls into instead of self-invoking
/// a full suite. Keyed per repo, exactly like [`TestExecLock`] — the two are
/// independent resources (this bounds CPU/wall-clock contention across
/// concurrent full-suite runs; `TestExecLock` serializes a shared-disk build
/// hazard down to 1), so checks using a shared Cargo directory acquire
/// both, in the order [`ManagedVerification::run`] declares them.
///
/// Backed by `tokio::sync::Semaphore`, which grants permits in acquire order
/// (FIFO) — the fairness property the ticket asks to be provable. A repo's
/// semaphore is created lazily, sized to its configured limit at that moment;
/// changing the configured limit at runtime does not resize an
/// already-created semaphore (matches this codebase's existing
/// `TestExecLock`/`shared_cargo_target` precedent of reading config once at
/// daemon startup, not live-reloading mid-flight).
///
/// RESTART RECOVERY IS AUTOMATIC: every field here is in-memory only, with no
/// durable counterpart. A daemon restart drops this struct along with every
/// outstanding `OwnedSemaphorePermit` it had handed out — there is no state
/// to leak or to recover, because there is no state that survives the
/// process. The next daemon simply starts every repo's semaphore fresh, full
/// of permits. (Contrast a durable lease record, which WOULD need explicit
/// restart-recovery logic to avoid permanently stranding a permit whose
/// holder died with the old process — deliberately not built, since it would
/// only add a way to leak what the in-memory design cannot.)
#[derive(Default)]
pub(crate) struct VerificationAdmission {
    semaphores: Mutex<HashMap<String, Arc<tokio::sync::Semaphore>>>,
    /// Fleet-wide default WIP limit; `0` disables admission control (no
    /// semaphore is ever created, so an unconfigured repo pays zero overhead
    /// beyond the lookup itself).
    default_limit: AtomicU64,
    /// Per-repo overrides, keyed by repo name — same convention as
    /// `rk_core::config::PolicyConfig::verification_admission_limit_by_repo`.
    overrides: Mutex<HashMap<String, u32>>,
}

impl VerificationAdmission {
    pub(crate) fn set_limits(&self, default_limit: u32, overrides: HashMap<String, u32>) {
        self.default_limit
            .store(u64::from(default_limit), Ordering::Relaxed);
        *self.overrides.lock().unwrap() = overrides;
    }

    /// The configured WIP limit for `repo` — its own override if set, else
    /// the fleet-wide default. `0` means admission control is off for this
    /// repo.
    pub(crate) fn limit_for(&self, repo: &str) -> u32 {
        self.overrides
            .lock()
            .unwrap()
            .get(repo)
            .copied()
            .unwrap_or(self.default_limit.load(Ordering::Relaxed) as u32)
    }

    /// Repos with an explicit per-repo override — a starting point for
    /// capacity reporting (`Supervisor::capacity_summary`), which unions this
    /// with any repo that currently has live agents.
    pub(crate) fn overridden_repos(&self, out: &mut std::collections::BTreeSet<String>) {
        out.extend(self.overrides.lock().unwrap().keys().cloned());
    }

    /// How many of `repo`'s configured permits are currently checked out, for
    /// reporting only (`Supervisor::capacity_summary`) — never consulted for
    /// admission itself. `0` whenever the limit is `0` (disabled) or no check
    /// has ever run for `repo` (no semaphore created yet).
    pub(crate) fn in_flight(&self, repo: &str) -> u32 {
        let limit = self.limit_for(repo);
        if limit == 0 {
            return 0;
        }
        match self.semaphores.lock().unwrap().get(repo) {
            Some(sem) => limit.saturating_sub(sem.available_permits() as u32),
            None => 0,
        }
    }

    /// Acquire one admission permit for `repo`, waiting in FIFO order behind
    /// any earlier waiter. Returns the held permit together with how long
    /// this call waited for it — the queue-wait half of the ticket's durable
    /// timing requirement (the caller times execution itself). `None` when
    /// admission control is disabled for `repo` (limit 0): every caller must
    /// treat that as "proceed unbounded", matching pre-existing behaviour.
    pub(crate) async fn acquire(
        &self,
        repo: &str,
        limit: u32,
    ) -> Option<(tokio::sync::OwnedSemaphorePermit, std::time::Duration)> {
        if limit == 0 {
            return None;
        }
        let sem = {
            let mut semaphores = self.semaphores.lock().unwrap();
            Arc::clone(
                semaphores
                    .entry(repo.to_string())
                    .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(limit as usize))),
            )
        };
        let started = std::time::Instant::now();
        // A semaphore is only ever closed by `close()`, which nothing here
        // calls — this can never actually return `Err`.
        let permit = sem
            .acquire_owned()
            .await
            .expect("verification admission semaphore is never closed");
        Some((permit, started.elapsed()))
    }
}

/// Optional aggregate concurrency ceiling for daemon-managed verification
/// runs ACROSS EVERY REPOSITORY this daemon serves (P3.1,
/// TKT-vilug-hujok-bolis) — layered ABOVE, never instead of, each
/// repository's own [`VerificationAdmission`] bound. `0` (the default)
/// disables it entirely: no semaphore is ever created, so a daemon that
/// hasn't opted in pays nothing beyond one mutex lock per managed run, and
/// behaves exactly as it did before this existed.
///
/// Backed by a single `tokio::sync::Semaphore` sized to the configured
/// limit — no per-repo dimension, unlike [`VerificationAdmission`]. Like
/// that struct, this is IN-MEMORY ONLY: a daemon restart drops it along with
/// every outstanding permit, and the next daemon starts fresh, full of
/// permits — there is no durable lease to recover or strand across a
/// restart. The real OS child processes a restart must still clean up are
/// reached the same way they always were: [`ManagedChildMarker`] /
/// `reap_stale_managed_children`, unaffected by this struct's own lifetime.
///
/// [`ManagedVerification::run`] acquires this STRICTLY AFTER the per-repo
/// admission (and the shared-`CARGO_TARGET_DIR` lock, when applicable): a
/// request still queued behind its own saturated repo has not yet entered
/// this semaphore's wait queue, and so can never occupy — or queue ahead
/// of — a host slot an eligible request from a DIFFERENT repo could
/// otherwise use immediately. Ordinary acquire-order is what prevents the
/// cross-repo head-of-line blocking the ticket requires be prevented; no
/// separate coordination or check-sharing is needed for it.
#[derive(Default)]
pub(crate) struct HostVerificationAdmission {
    semaphore: Mutex<Option<Arc<tokio::sync::Semaphore>>>,
    limit: AtomicU64,
    /// Requests currently blocked in [`acquire`](Self::acquire)'s own
    /// await — SPECIFICALLY waiting for the aggregate host permit, never a
    /// broader "waiting for any managed admission" count. A request still
    /// queued behind its own per-repo `VerificationAdmission` semaphore, or
    /// behind the shared-`CARGO_TARGET_DIR` `TestExecLock`, has not called
    /// [`acquire`](Self::acquire) yet (see [`ManagedVerification::run`]'s
    /// acquire order) and so is not counted here at all — it shows up only
    /// implicitly, as elapsed queue-wait time once it settles. Distinct from
    /// `executing` (checked-out permits): a request that already holds its
    /// own repo's permit but is still queued here for the host-wide one is
    /// "waiting", not yet "executing".
    waiting: AtomicU64,
}

impl HostVerificationAdmission {
    /// Set `[policy] verification_admission_aggregate_limit`. Applied once by
    /// `Daemon::new` from config — same pattern, and same
    /// restart-required-to-change contract, as
    /// [`VerificationAdmission::set_limits`]. Replaces any existing
    /// semaphore outright: safe in production (called exactly once, before
    /// the daemon serves its first request) and otherwise only ever called
    /// again by a test.
    pub(crate) fn set_limit(&self, limit: u32) {
        self.limit.store(u64::from(limit), Ordering::Relaxed);
        *self.semaphore.lock().unwrap() =
            (limit > 0).then(|| Arc::new(tokio::sync::Semaphore::new(limit as usize)));
    }

    /// The configured aggregate ceiling. `0` means disabled.
    pub(crate) fn limit(&self) -> u32 {
        self.limit.load(Ordering::Relaxed) as u32
    }

    /// Host permits currently checked out, for reporting only
    /// (`Supervisor::host_verification_capacity_summary`) — never consulted
    /// for admission itself. `0` whenever the limit is `0` (disabled).
    pub(crate) fn executing(&self) -> u32 {
        let limit = self.limit();
        if limit == 0 {
            return 0;
        }
        match self.semaphore.lock().unwrap().as_ref() {
            Some(sem) => limit.saturating_sub(sem.available_permits() as u32),
            None => 0,
        }
    }

    /// Requests currently waiting for a host permit, for reporting only.
    pub(crate) fn waiting(&self) -> u32 {
        self.waiting.load(Ordering::Relaxed) as u32
    }

    /// Acquire one host-wide permit, or `None` immediately when the
    /// aggregate cap is disabled (limit `0`) — every caller must treat that
    /// as "proceed unbounded", matching [`VerificationAdmission::acquire`]'s
    /// own convention. The `waiting` counter is incremented only around the
    /// actual await and decremented by a drop guard rather than inline code
    /// after it, so a caller that cancels this future mid-wait (the overall
    /// admission `tokio::time::timeout`, or `verify_repo_check`'s own
    /// cancellation race) can never leak the count.
    pub(crate) async fn acquire(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        let sem = self.semaphore.lock().unwrap().clone()?;
        struct WaitGuard<'a>(&'a AtomicU64);
        impl Drop for WaitGuard<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::Relaxed);
            }
        }
        self.waiting.fetch_add(1, Ordering::Relaxed);
        let _wait_guard = WaitGuard(&self.waiting);
        // A semaphore is only ever closed by `close()`, which nothing here
        // calls — this can never actually return `Err`.
        Some(
            sem.acquire_owned()
                .await
                .expect("host verification admission semaphore is never closed"),
        )
    }
}

/// One in-flight `verify.run`-mediated verification execution
/// (TKT-01M0PA6C5WYRWS757R1SS2F2GR): a live post-deploy probe found that
/// interrupting the requesting agent, or killing the RPC client blocked on
/// `verify.run`, left the daemon-owned check process running under the
/// daemon alone, still occupying its repo's admission slot. Registered by
/// [`crate::managed_verification::ManagedVerification::verify_repo_check`] for the
/// lifetime of exactly one call; `cancel` is the signal that call races its
/// own execution against, so sending on it drops that execution's future —
/// and with it, via the existing `ProcessGroupGuard`-on-drop discipline in
/// `crate::workflow_exec`, SIGKILLs the check's entire live descendant
/// process tree — not just its own leader group, which a check command
/// (`mise run <task>`) can itself move part of its work out of.
struct ManagedVerificationRun {
    generation: Option<rk_core::id::SpawnId>,
    agent: String,
    request_key: String,
    cancel: tokio::sync::watch::Sender<Option<&'static str>>,
}

/// Registry of in-flight [`ManagedVerificationRun`]s, keyed by an opaque
/// monotonic id. In-memory only, exactly like [`VerificationAdmission`]: a
/// daemon restart drops every entry, and a fresh daemon's own registry
/// starts genuinely empty, so nothing about a dead generation's bookkeeping
/// can ever block a new one's forward progress. The OS-level check child
/// each entry corresponds to is a SEPARATE concern this in-memory registry
/// cannot reach across a restart on its own (it lives in its own process
/// group, reached only via the `cancel` signal above while this process is
/// still alive) — durably marked and reaped instead by
/// [`ManagedChildMarker`] /
/// `reap_stale_managed_children`, which every `Daemon::run` runs before its
/// accept loop can serve a single request.
#[derive(Default)]
pub(crate) struct ManagedVerificationRuns {
    next_id: AtomicU64,
    runs: Mutex<HashMap<u64, ManagedVerificationRun>>,
}

struct ManagedRegistration<'a> {
    runs: &'a ManagedVerificationRuns,
    id: u64,
}

impl Drop for ManagedRegistration<'_> {
    fn drop(&mut self) {
        self.runs.unregister(self.id);
    }
}

impl ManagedVerificationRuns {
    pub(crate) fn register(
        &self,
        agent: &str,
        generation: Option<rk_core::id::SpawnId>,
        request_key: &str,
    ) -> (u64, tokio::sync::watch::Receiver<Option<&'static str>>) {
        let (cancel, rx) = tokio::sync::watch::channel(None);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.runs.lock().unwrap().insert(
            id,
            ManagedVerificationRun {
                generation,
                agent: agent.to_string(),
                request_key: request_key.to_string(),
                cancel,
            },
        );
        (id, rx)
    }

    pub(crate) fn unregister(&self, id: u64) {
        self.runs.lock().unwrap().remove(&id);
    }

    /// Cancel every run belonging to `agent`, fenced to `generation` when
    /// given: a namesake that has since taken over the name (a fresh
    /// generation after a dismiss+respawn) is never touched by a signal meant
    /// for its predecessor — the exact "never affects ... a newer
    /// generation/namesake" guarantee the ticket asks for.
    pub(crate) fn cancel_agent(
        &self,
        agent: &str,
        generation: Option<rk_core::id::SpawnId>,
        reason: &'static str,
    ) {
        for run in self.runs.lock().unwrap().values() {
            if run.agent == agent && (generation.is_none() || run.generation == generation) {
                let _ = run.cancel.send(Some(reason));
            }
        }
    }

    /// Cancel the one run correlated with `request_key` — an RPC connection
    /// dying mid-call. Never touches a sibling call from the same agent on a
    /// different connection, since each call mints its own key.
    pub(crate) fn cancel_request(&self, request_key: &str, reason: &'static str) {
        for run in self.runs.lock().unwrap().values() {
            if run.request_key == request_key {
                let _ = run.cancel.send(Some(reason));
            }
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn liveness_corroborates_only_negative_snapshots_once() {
        fn row(pid: u32, ppid: u32, comm: &str, stat: &str) -> ProcessTableRow {
            ProcessTableRow {
                pid,
                ppid,
                pgid: 10,
                stat: stat.into(),
                comm: comm.into(),
            }
        }
        let live = || vec![row(10, 1, "sh", "S"), row(11, 10, "cargo", "S")];
        let missed = || vec![row(10, 1, "sh", "S")];
        let mut calls = 0;
        let result = process_liveness_with_snapshots(10, || {
            calls += 1;
            if calls == 1 {
                missed()
            } else {
                live()
            }
        });
        assert!(result.child_alive);
        assert_eq!(result.live_verifier_descendants, 1);
        assert_eq!(calls, 2);

        calls = 0;
        let result = process_liveness_with_snapshots(10, || {
            calls += 1;
            live()
        });
        assert_eq!(result.live_verifier_descendants, 1);
        assert_eq!(
            calls, 1,
            "positive evidence does not add another process scan"
        );

        calls = 0;
        let result = process_liveness_with_snapshots(10, || {
            calls += 1;
            missed()
        });
        assert_eq!(result.live_verifier_descendants, 0);
        assert_eq!(
            calls, 2,
            "persistent absence must remain negative and bounded"
        );
    }

    #[test]
    fn liveness_retry_does_not_join_snapshots_or_excuse_unrecognized_children() {
        let row = |pid, ppid, comm: &str, stat: &str| ProcessTableRow {
            pid,
            ppid,
            pgid: 10,
            stat: stat.into(),
            comm: comm.into(),
        };
        let mut calls = 0;
        let result = process_liveness_with_snapshots(10, || {
            calls += 1;
            if calls == 1 {
                vec![row(10, 1, "sh", "S"), row(11, 10, "sh", "S")]
            } else {
                // The old ancestor is missing; an unrelated cargo, a zombie,
                // and a bare sleep are all insufficient liveness evidence.
                vec![
                    row(10, 1, "sh", "S"),
                    row(12, 11, "cargo", "S"),
                    row(13, 10, "rustc", "Z"),
                    row(14, 10, "sleep", "S"),
                ]
            }
        });
        assert!(result.child_alive);
        assert_eq!(result.live_verifier_descendants, 0);
        assert_eq!(calls, 2);
        let gone = process_liveness_with_snapshots(10, Vec::new);
        assert!(!gone.child_alive);
        assert_eq!(gone.live_verifier_descendants, 0);
    }
    use super::*;

    #[tokio::test]
    async fn dropping_the_request_releases_its_registration_child_and_admission() {
        let home = tempfile::tempdir().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        layout.ensure().unwrap();
        std::fs::create_dir(dir.path().join(".rk")).unwrap();
        std::fs::write(
            dir.path().join(".rk/checks.cue"),
            r#"checks: [{name: "verify",
            command: "echo $$ > child.pid; exec sleep 60", timeout: "2m",
            environmentPolicy: "strip_rk_spawn", sharedCargoTarget: false}]"#,
        )
        .unwrap();
        let space = Space::open_in_memory().unwrap();
        let resources = VerificationResources::default();
        resources.admission.set_limits(1, HashMap::new());
        let verifier = ManagedVerification::new(&layout, &space, &resources, false);
        let mut request = Box::pin(verifier.verify_repo_check(
            "operator",
            dir.path(),
            "repo",
            "verify",
            None,
            "abandoned-request",
            None,
        ));
        let pid: i32 = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    _ = &mut request => panic!("check unexpectedly completed"),
                    _ = tokio::time::sleep(Duration::from_millis(20)) => {
                        if let Ok(pid) = std::fs::read_to_string(dir.path().join("child.pid")) {
                            if let Ok(pid) = pid.trim().parse() { break pid; }
                        }
                    }
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(resources.runs.runs.lock().unwrap().len(), 1);
        drop(request);
        assert!(resources.runs.runs.lock().unwrap().is_empty());
        tokio::time::timeout(Duration::from_secs(5), async {
            while unsafe { libc::kill(pid, 0) == 0 } {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("abandoned request must kill its child");
        let permit = tokio::time::timeout(
            Duration::from_millis(100),
            resources.admission.acquire("repo", 1),
        )
        .await
        .expect("abandoned request must release admission");
        assert!(permit.is_some());
        assert!(space
            .scan(&Pattern::category(Category::Event).identity(VERIFICATION_PROOF_IDENTITY))
            .unwrap()
            .is_empty());
    }
    #[tokio::test]
    async fn ordinary_shared_lock_admission_expiry_without_expect_exit_is_an_error() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        let space = Space::open_in_memory().unwrap();
        let resources = VerificationResources::default();
        let verifier = ManagedVerification::new(&layout, &space, &resources, true);
        let _held = resources.test_exec_lock.acquire("repo").await;
        let resolved = ResolvedRun {
            command: "touch should-not-execute".into(),
            cwd: None,
            expect_exit: None,
            timeout: "50ms".into(),
            on_timeout: OnTimeout::Fail,
            environment_policy: Default::default(),
            retry_on_fail: 0,
            shared_cargo_target: true,
        };
        let result = verifier
            .run(CheckExecution {
                id: "ordinary-workflow",
                repo: "repo",
                agent: "rat",
                dir: home.path(),
                command: &resolved.command,
                resolved: &resolved,
                env: &[],
                timeout: Duration::from_millis(50),
                admission_timeout: None,
                previous_result: None,
                progress: None,
            })
            .await;
        assert!(
            result.is_err(),
            "a workflow must not advance after a check that never ran"
        );
        assert!(!home.path().join("should-not-execute").exists());
    }

    /// End-to-end regression for TKT-hodij-lujak-kibon through the real
    /// `verify_repo_check`/`run` producer path, driven by an injectable
    /// [`SpanClock`] rather than an actual sleep. The fake clock only ever
    /// hands out three timestamps — the true `queued_at`/`started_at`/
    /// `ended_at` `run()` reads at its three real capture points — and
    /// panics on a fourth read. The OLD code (`PhaseSpan::from_durations`
    /// anchored on a `Utc::now()` called back in `verify_repo_check` after
    /// `run()` already returned) would have consumed that fourth,
    /// deliberately-corrupt value as `ended_at`; this proves the fixed
    /// pipeline never reads the clock again downstream of `run()` settling,
    /// so the recorded span carries the real settle-time boundaries no
    /// matter how much further pipeline work (or a host suspend) happens
    /// afterward.
    #[tokio::test]
    async fn a_span_recorded_after_further_pipeline_work_still_carries_the_real_settle_time() {
        let home = tempfile::tempdir().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        layout.ensure().unwrap();
        std::fs::create_dir(dir.path().join(".rk")).unwrap();
        std::fs::write(
            dir.path().join(".rk/checks.cue"),
            r#"checks: [{name: "verify", command: "true", timeout: "2m",
            environmentPolicy: "strip_rk_spawn", sharedCargoTarget: false}]"#,
        )
        .unwrap();
        let space = Space::open_in_memory().unwrap();

        let true_queued_at = Utc::now();
        let true_started_at = true_queued_at + chrono::Duration::milliseconds(5);
        let true_ended_at = true_started_at + chrono::Duration::milliseconds(50);
        // Never legitimately read: proof that nothing downstream of `run()`
        // settling calls the clock again to build the span.
        let corrupt_if_reread = true_ended_at + chrono::Duration::minutes(40);
        let remaining = Arc::new(Mutex::new(
            vec![
                true_queued_at,
                true_started_at,
                true_ended_at,
                corrupt_if_reread,
            ]
            .into_iter(),
        ));
        let resources = VerificationResources {
            clock: SpanClock::from_fn(move || {
                remaining
                    .lock()
                    .unwrap()
                    .next()
                    .expect("span clock read more times than a settled run should need")
            }),
            ..Default::default()
        };
        let verifier = ManagedVerification::new(&layout, &space, &resources, false);
        verifier
            .verify_repo_check(
                "operator",
                dir.path(),
                "repo",
                "verify",
                None,
                "req-delayed-publish",
                Some("TKT-delayed-publish"),
            )
            .await
            .unwrap();

        let spans = crate::span::spans_for_task(&space, "repo", "TKT-delayed-publish").unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0]["timestamp_provenance"], "observed");
        assert_eq!(spans[0]["duration_semantic"], "additive");
        assert_eq!(
            spans[0]["queued_at"],
            serde_json::to_value(true_queued_at).unwrap()
        );
        assert_eq!(
            spans[0]["started_at"],
            serde_json::to_value(true_started_at).unwrap()
        );
        assert_eq!(
            spans[0]["ended_at"],
            serde_json::to_value(true_ended_at).unwrap(),
            "ended_at must be the real settle time, never a later clock read"
        );
    }

    /// A check that genuinely waits behind another one at a repo's admission
    /// bound gets real, observed `queued_at`/`started_at`/`ended_at` —
    /// `started_at` lands meaningfully after `queued_at` (the true admission
    /// wait), not folded into a single instantaneous "now" the way an
    /// `ended_at`-only reconstruction would.
    #[tokio::test]
    async fn a_genuinely_queued_check_records_observed_boundaries_with_a_real_admission_wait() {
        let home = tempfile::tempdir().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        layout.ensure().unwrap();
        std::fs::create_dir(dir.path().join(".rk")).unwrap();
        std::fs::write(
            dir.path().join(".rk/checks.cue"),
            r#"checks: [{name: "verify", command: "sleep 0.2", timeout: "2m",
            environmentPolicy: "strip_rk_spawn", sharedCargoTarget: false}]"#,
        )
        .unwrap();
        let space = Space::open_in_memory().unwrap();
        let resources = VerificationResources::default();
        resources.admission.set_limits(1, HashMap::new());
        let verifier = ManagedVerification::new(&layout, &space, &resources, false);

        // The first check occupies the repo's one admission slot; the second
        // must genuinely queue behind it.
        let first = verifier.verify_repo_check(
            "operator",
            dir.path(),
            "repo",
            "verify",
            None,
            "req-first",
            Some("TKT-queued"),
        );
        let second = verifier.verify_repo_check(
            "operator",
            dir.path(),
            "repo",
            "verify",
            None,
            "req-second",
            Some("TKT-queued"),
        );
        let (first_result, second_result) = tokio::join!(first, second);
        first_result.unwrap();
        second_result.unwrap();

        let spans = crate::span::spans_for_task(&space, "repo", "TKT-queued").unwrap();
        assert_eq!(spans.len(), 2);
        // Whichever ran second (later `started_at`) must show a real
        // admission wait, and every observed span's boundaries must be in
        // true chronological order.
        for span in &spans {
            assert_eq!(span["timestamp_provenance"], "observed");
            let queued_at = parse_span_time(&span["queued_at"]);
            let started_at = parse_span_time(&span["started_at"]);
            let ended_at = parse_span_time(&span["ended_at"]);
            assert!(queued_at <= started_at, "{span}");
            assert!(started_at <= ended_at, "{span}");
        }
        let waited_ms: Vec<i64> = spans
            .iter()
            .map(|s| s["queue_wait_ms"].as_i64().unwrap())
            .collect();
        assert!(
            waited_ms.iter().any(|&ms| ms > 0),
            "one of the two checks must have genuinely queued behind the other: {waited_ms:?}"
        );
    }

    fn parse_span_time(v: &Value) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(v.as_str().unwrap())
            .unwrap()
            .with_timezone(&Utc)
    }
}

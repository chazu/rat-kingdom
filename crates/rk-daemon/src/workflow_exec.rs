//! Workflow execution: sequential step machine over the supervisor and the
//! tuplespace. Definitions come from rk-workflow (cue CLI); this module owns
//! instances, context threading, and step semantics.

use crate::agents::AgentState;
use crate::recovery::{RateCap, RecoveryAction, RecoveryAnnouncer};
use crate::supervisor::{SpawnParams, Supervisor, FLEET_WIP_CAP_REFUSED};
use crate::tickets::Tickets;
use chrono::{DateTime, Utc};
use rk_core::id::prefixed_id;
use rk_core::notify::{EscalationNotice, Severity, SinkRegistry};
use rk_core::paths::Layout;
use rk_core::tuple::{Category, Pattern, Tuple, DEFAULT_TRAIL_TTL, SYSTEM_SCOPE};
use rk_space::Space;
use rk_workflow::{
    resolve::{resolve, resolve_fields},
    AgentProfile, DismissAllStep, ForEachStep, RunStep, Step, SubWorkflowStep, TicketQuery,
    TierRouting, WaitAllStep, Workflow,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// A boxed future for hand-rolled async recursion (nested `when` / `repeat`).
type StepFuture<'a> = Pin<Box<dyn Future<Output = rk_core::Result<Flow>> + Send + 'a>>;

/// Mirrors rk-workflow's `RunStep` timeout default; a referencing `run` step
/// left at this value defers to a named check's own timeout (TKT-30).
const DEFAULT_RUN_TIMEOUT: &str = "10m";

/// Keep a noisy or compromised check from turning the daemon into an
/// unbounded stdout/stderr buffer. The cap applies independently to each
/// stream; exceeding it fails the run and kills the child.
const MAX_RUN_OUTPUT_BYTES: usize = 256 * 1024;

/// Hard ceiling on `sub_workflow` nesting depth — the depth analog of the
/// `repeat` max cap (rk-workflow `#RepeatStep.max`). A top-level `run` is depth
/// 0; each nested `sub_workflow` is one deeper. A workflow cycle (A→B→A…) hits
/// this cap and fails closed rather than recursing until it exhausts the stack.
const MAX_SUBWORKFLOW_DEPTH: usize = 8;

/// How often a blocking `wait`/`wait_all` comes up for air to check whether the
/// rat it is waiting on is still capable of reporting (TKT-147). Short enough
/// that a crash surfaces in seconds instead of at the step's (typically
/// hours-long) timeout, long enough to cost nothing: the read itself blocks in
/// the tuplespace for the whole slice, so this is a wake-up cadence, not a spin
/// — one indexed query and one registry lookup every few seconds per open wait.
const LIVENESS_POLL: Duration = Duration::from_secs(5);
/// Poll cadence for [`WorkflowEngine::await_fleet_capacity`]. Deliberately
/// much shorter than [`LIVENESS_POLL`]: a fleet slot is an in-memory count
/// (one cheap registry scan), not a tuplespace read, and a `spawn` step should
/// notice a freed slot promptly rather than sit out most of a 5s window after
/// it opens.
const FLEET_CAPACITY_POLL: Duration = Duration::from_millis(250);

/// Whether a spawn attempt failed because the fleet-WIP ceiling had no free
/// slot at the moment `Supervisor::spawn` atomically checked (as opposed to a
/// genuine spawn failure) — a `Step::Spawn` retries on this rather than
/// failing the step.
pub(crate) fn is_fleet_wip_refusal(error: &rk_core::Error) -> bool {
    matches!(error, rk_core::Error::Other(msg) if msg == FLEET_WIP_CAP_REFUSED)
}

static PERSIST_SEQ: AtomicU64 = AtomicU64::new(0);

/// Where a live instance snapshot lives; every mutation rewrites its file here.
const INSTANCE_DIR: &str = "workflow-instances";

/// Where a pruned terminal instance is offloaded to. The same JSON in a
/// different directory: archiving PRESERVES the run — `rk workflow status` and
/// `rk workflow list --archived` still read it, `rk workflow unarchive` puts it
/// back — it just stops the run counting as something awaiting a human
/// (TKT-177).
const INSTANCE_ARCHIVE_DIR: &str = "workflow-instances-archive";

/// The effective parameters of a `run` step after named-check resolution and
/// policy enforcement — a raw command or a repo-registered check collapse to the
/// same shape here.
/// Crate-scoped alongside [`WorkflowEngine::run_check_in`]: a daemon-native
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
    /// be serialized in [`WorkflowEngine::run_check_in`] against every other
    /// same-repo run/check that also sets this
    /// ([`Check::shared_cargo_target`](rk_workflow::Check::shared_cargo_target),
    /// TKT-01M0CFA1RX36SJ7DV4YWGHQ9BT). Always false for a raw `command` —
    /// only a repo-registered named check can opt in.
    pub(crate) shared_cargo_target: bool,
}

/// What a blown `run` wall-clock bound does to the instance (TKT-169).
///
/// The command is killed either way — `kill_on_drop` owns that, and a hung suite
/// never survives its budget. The choice here is only whether the kill is
/// reported as an ERROR (which ends the run where it stands) or as a RESULT the
/// following steps get to route on.
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
    fn parse(raw: &str) -> rk_core::Result<Self> {
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
const TIMEOUT_EXIT: i64 = 124;

/// Exit code reported when a check never ran at all because it could not
/// acquire the shared-target-dir test-execution lock (`TestExecLock`) within
/// its own declared timeout. Distinct from [`TIMEOUT_EXIT`], which means the
/// command itself started and was killed — this means it never started.
const LOCK_TIMEOUT_EXIT: i64 = -2;

/// Pause between a failed attempt and a `retryOnFail` retry. Fixed rather than
/// configurable: this exists to ride out a transient condition (machine load,
/// a build-lock hold), not to be tuned per workflow.
const RETRY_BACKOFF: Duration = Duration::from_secs(5);

/// Hard cap on `retryOnFail` — mirrors `#RunStep.retryOnFail` in schema.cue
/// (`int & >=0 & <=20`). Enforced again here, independent of the schema
/// bound, so `resolved.retry_on_fail + 1` can never approach u32::MAX
/// (TKT-01M02QT9KTDY2CN6YJEVP3VCF8): unbounded, that addition panics on
/// overflow in debug and, wrapped in release, would settle the attempt loop
/// with zero real attempts.
const MAX_RETRY_ON_FAIL: u32 = 20;

/// Daemon-side counterpart of the schema.cue `retryOnFail` bound. A raw
/// negative value never reaches here — `u32` deserialization already refuses
/// it when a workflow definition is loaded — but an over-cap value up to
/// `u32::MAX` is a valid `u32` and would otherwise reach
/// `resolved.retry_on_fail + 1` unbounded. Kept as a free function (no
/// `&self`) so it is unit-testable without standing up a full
/// `WorkflowEngine`.
fn validate_retry_on_fail(value: u32) -> rk_core::Result<()> {
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
const GATE_EVIDENCE_LIMIT: usize = 8000;

/// The outcome of running a `run` step's command to completion or to its bound.
#[derive(Debug)]
enum RunOutcome {
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

/// Decode a `spawn_check_child` outcome into the flat tuple `run_check_in`
/// tracks. Factored out so a retried outcome (the shared cargo target-dir
/// contention retry, and the initial attempt) decode identically.
fn decode_run_outcome(
    outcome: RunOutcome,
    command: &str,
    resolved: &ResolvedRun,
) -> (i64, String, bool, String, bool, bool) {
    match outcome {
        RunOutcome::Completed {
            status,
            stdout,
            stdout_truncated,
            stderr,
            stderr_truncated,
        } => (
            status.code().unwrap_or(-1) as i64,
            String::from_utf8_lossy(&stdout).into_owned(),
            stdout_truncated,
            String::from_utf8_lossy(&stderr).into_owned(),
            stderr_truncated,
            false,
        ),
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
        ),
    }
}

/// Matches only the shared `CARGO_TARGET_DIR` cross-process contention
/// signature (docs/2026-08-19-tkt-hot-scan-target-dir-contention.md): a
/// build artifact resolved by one process gets pruned by a concurrent
/// `cargo build` in another worktree before this process can exec it.
/// Deliberately narrow -- a real compile error or test failure must never
/// match this and get a free retry.
fn is_cargo_target_contention_signature(stdout: &str, stderr: &str) -> bool {
    let hits = |s: &str| {
        s.contains("could not execute process")
            && s.contains("(never executed)")
            && s.contains("No such file or directory (os error 2)")
    };
    hits(stdout) || hits(stderr)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Instance {
    pub id: String,
    pub workflow: String,
    pub repo: String,
    /// Stable user-facing coordinator session that owns this workflow, when
    /// explicitly supplied. Legacy and ad-hoc runs remain unowned.
    #[serde(default)]
    pub coordinator: Option<String>,
    /// Schedule name that launched this run. `None` for manual, reactor, and
    /// legacy snapshots. Persisted so per-schedule single-flight survives restart
    /// without conflating two schedules that intentionally run identical work.
    #[serde(default)]
    pub schedule: Option<String>,
    pub status: InstanceStatus,
    /// Monotonic observable-state revision for coordinator consumers. Older
    /// snapshots deserialize as zero and enter the same sequence on their next
    /// mutation.
    #[serde(default)]
    pub revision: u64,
    pub current_step: usize,
    pub total_steps: usize,
    pub context: WorkflowContext,
    #[serde(default)]
    pub error: Option<String>,
    /// Set while the instance is blocked awaiting a human decision at an
    /// approval gate; cleared once the decision (or timeout) arrives. This is
    /// the precise "parked at a gate" signal `rk inbox` reports — a `Running`
    /// status alone can't distinguish a parked gate from active execution.
    #[serde(default)]
    pub awaiting: Option<String>,
    /// Per-instance budget cap in USD from the workflow's `budget:` field.
    /// Once this instance's summed agent cost reaches it, further dispatch
    /// (single spawn or fan-out) is refused. `None`/0 = unlimited.
    #[serde(default)]
    pub instance_max_usd: Option<f64>,
    /// The definition name/path `run` was invoked with, used to relocate and
    /// reload the workflow when resuming after a restart (TKT-52). Persisted so
    /// a rehydrated instance can re-`load` the exact same steps.
    #[serde(default)]
    pub definition: String,
    /// SHA-256 of the definition bytes used to start this instance. A resumed
    /// workflow refuses to execute a changed definition after restart.
    #[serde(default)]
    pub definition_digest: String,
    /// Operator-granted capability for this exact workflow run. Set only when
    /// the configured workflow name resolved directly from the managed global
    /// workflow directory; repo-local name shadowing cannot set it.
    #[serde(default)]
    pub automated_landing_authorized: bool,
    /// The original `_input` params this instance launched with, replayed at
    /// reload so a resumed workflow validates and interpolates identically to
    /// its first run (TKT-52).
    #[serde(default)]
    pub params: HashMap<String, Value>,
    /// Sub-workflow nesting depth: 0 for a top-level `run`, incremented for each
    /// enclosing `sub_workflow` step (TKT-57). Bounded by
    /// [`MAX_SUBWORKFLOW_DEPTH`] — the depth analog of the `repeat` max cap — so
    /// a workflow cycle fails closed instead of recursing without end.
    #[serde(default)]
    pub depth: usize,
    pub started_at: chrono::DateTime<chrono::Utc>,
    #[serde(default)]
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    /// When this instance was pruned out of the live store (`None` = live).
    /// Set by [`WorkflowEngine::archive`] and cleared by
    /// [`WorkflowEngine::unarchive`] — nothing else writes it, so it doubles as
    /// the "is this row archived?" flag every view keys on.
    #[serde(default)]
    pub archived_at: Option<chrono::DateTime<chrono::Utc>>,
    /// The `#Trigger` name that launched this instance, when it was launched by
    /// the reactor. `None` for manual, scheduled, and legacy runs. This is what
    /// [`WorkflowEngine::live_count_for_trigger`] counts against a trigger's
    /// `maxInFlight` cap — admission control the reactor enforces per trigger,
    /// not per workflow definition (two triggers can share one `run`).
    #[serde(default)]
    pub trigger: Option<String>,
    /// This instance's own override of the stale-`Running`-instance hard
    /// timeout (strategic review B8), resolved from the workflow's
    /// `staleTimeout:` field at launch. `None` defers to the sweep's
    /// configured `default_timeout_secs`. Resolved once at launch (like
    /// [`instance_max_usd`](Self::instance_max_usd)) rather than re-parsed
    /// from `definition` on every sweep pass.
    #[serde(default)]
    pub stale_timeout_secs: Option<u64>,
}

impl Instance {
    /// This instance's [`work_key`] — the identity of the work it was launched
    /// to perform, as opposed to the identity of the run that performed it.
    pub fn work_key(&self) -> String {
        work_key(&self.repo, &self.workflow, &self.params)
    }
}

/// The identity of the WORK a run was launched to perform — its repo, workflow
/// name, and the exact params it was given — as a stable digest.
///
/// Two instances share a `work_key` exactly when launching one would be a retry
/// of the other. That is the whole basis of TKT-187: `rk inbox` retires a
/// workflow failure once a later run of the SAME work has completed, without
/// ever inspecting the failure's error text.
///
/// **Derived, not stored — deliberately.** It could have been a field written
/// at launch, but the branch-shaped inbox rows already settled this argument
/// (`inbox.rs`: the dropped-land row re-asks git rather than waiting for
/// something to write a "resolved" record). A derived answer is correct against
/// current data, needs no migration, and works on instances that were persisted
/// before the feature existed; a written one needs a writer that fires at
/// exactly the right moment and cannot be recomputed when it does not.
///
/// **`definition_digest` is deliberately EXCLUDED.** Editing the workflow file
/// is the single most common repair for a workflow that failed, and folding the
/// digest in would mean that repair prevents the retry from ever clearing the
/// failure it fixed — exactly backwards.
///
/// Params are canonicalized through a `BTreeMap` before hashing so key order in
/// the caller's `HashMap` cannot change the digest. serde_json's own `Map` is a
/// `BTreeMap` unless the `preserve_order` feature is enabled (it is not here),
/// so nested objects serialize in sorted key order for free.
///
/// Returns the empty string when the params cannot be serialized at all. Empty
/// is the "matches nothing" key by contract — an instance whose work identity
/// is unknowable must neither retire another failure nor be retired by one —
/// which is why this fails closed instead of hashing a placeholder that every
/// such instance would collide on.
pub fn work_key(repo: &str, workflow: &str, params: &HashMap<String, Value>) -> String {
    let canonical: std::collections::BTreeMap<&str, &Value> =
        params.iter().map(|(k, v)| (k.as_str(), v)).collect();
    let Ok(params_json) = serde_json::to_string(&canonical) else {
        return String::new();
    };
    // Length-prefixed, not merely delimited: a separator alone would let a repo
    // path containing the delimiter be re-cut into a different (repo, workflow)
    // pair that hashes identically, and a false match here retires a real
    // failure. Prefixing makes the encoding injective.
    let material = format!(
        "{}:{repo}|{}:{workflow}|{params_json}",
        repo.len(),
        workflow.len()
    );
    hex::encode(Sha256::digest(material.as_bytes()))
}

/// When a terminal instance settled: its `completed_at`, falling back to
/// `started_at` for snapshots written before that field was populated. This is
/// what an `rk prune --before` window is measured against, and what orders the
/// attempts within one [`work_key`] when `rk inbox` decides whether a failure
/// has since been made good.
pub(crate) fn settled_at(instance: &Instance) -> DateTime<Utc> {
    instance.completed_at.unwrap_or(instance.started_at)
}

/// What one prune pass selects.
///
/// Both forms refuse a `Running` instance: an in-flight workflow is not
/// settled, and hiding it would destroy the only signal that it is still going.
#[derive(Debug, Clone)]
pub enum Selection {
    /// Every terminal instance that settled strictly before this cutoff — the
    /// windowed sweep `rk prune` and `rk workflow prune --before` perform.
    Before(DateTime<Utc>),
    /// Exactly these ids — the targeted clear behind one `rk inbox` row.
    Ids(Vec<String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceStatus {
    Running,
    Completed,
    Failed,
}

impl InstanceStatus {
    fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkflowContext {
    pub active_agent: Option<String>,
    /// The generation of `active_agent` captured at the moment `spawn` minted
    /// it. This is the sequential counterpart to `FannedAgent::spawn`: a
    /// `dismiss` step resolves `active_agent` by name only, and a name is
    /// recycled once its holder is archived (TKT-146), so a `dismiss` that
    /// runs after a same-named respawn (a namesake spawned between this
    /// step's `wait` and its `dismiss`) must refuse to act on it rather than
    /// silently tearing down a stranger. `None` for a context that predates
    /// this field (deserialized from a durable snapshot written before the
    /// migration); preserves the old unchecked behaviour.
    #[serde(default)]
    pub active_agent_spawn: Option<rk_core::id::SpawnId>,
    pub active_branch: Option<String>,
    pub previous_result: Option<Value>,
    /// Values lifted from the space by `read` steps, keyed by `read.into`.
    /// Consumed by `when` steps and by `{{ctx.var.<name>}}` interpolation.
    #[serde(default)]
    pub vars: HashMap<String, Value>,
    /// Agents spawned by the most recent fan-out (`for_each`), awaiting a
    /// `wait_all` join. This is the fan-out counterpart to `active_agent`:
    /// sequential steps keep using `active_agent`; fan-out steps use this list
    /// so the single-active-agent path stays untouched.
    ///
    /// `None` and `Some(vec![])` mean different things, which is why this is an
    /// `Option` and not a bare `Vec` (TKT-170). `None` is "no `for_each` has run
    /// here" — a `wait_all` in that state is an authoring error and fails the
    /// instance. `Some(vec![])` is "a `for_each` ran and its query matched
    /// nothing" — a quiet night, which joins and dismisses as a no-op so the
    /// steps after the fan-out still run and the instance completes. Cleared
    /// back to `None` by `dismiss_all`, which spends the set.
    #[serde(default)]
    pub fanout: Option<Vec<FannedAgent>>,
    /// The agents whose `harness_result` produced the current
    /// `previous_result`: one for a `wait`, the whole fan-out for a `wait_all`,
    /// empty for every other source (a `dismiss` outcome, a `run` exit, an
    /// approval decision, a sub-workflow's return).
    ///
    /// This is the provenance an `evaluate` needs to assert that the result it
    /// is about to judge came from a rat that actually ran (TKT-147). Without
    /// it the gate would have to guess from `active_agent`, which lingers past
    /// the step that set it.
    #[serde(default)]
    pub awaited: Vec<String>,
    /// Set only by an approval gate that received `{approved: true}`. This is
    /// the capability checked by destructive `land`/`open_pr` steps; a reviewer
    /// payload or an arbitrary CUE `when` branch cannot forge it.
    #[serde(default)]
    pub approval_granted: bool,
    /// Durable child instance owned by the currently executing `sub_workflow`
    /// step. Written before the child snapshot so a restart can recreate a child
    /// that was not installed yet, or rejoin the exact child that was.
    #[serde(default)]
    pub active_subworkflow: Option<String>,
}

/// A gate child's captured stream: bytes bounded to [`MAX_RUN_OUTPUT_BYTES`],
/// keeping the TAIL of the stream (where a suite's failure summary lives)
/// rather than the head, plus whether the raw stream actually exceeded that
/// bound.
struct CappedOutput {
    bytes: Vec<u8>,
    truncated: bool,
}

/// Stream a child's output to completion, never erroring on volume alone —
/// only a genuine read failure returns `Err`. A chatty-but-otherwise-healthy
/// suite must still run to its real exit code and route/retry normally
/// (gate-children-truncate-not-kill): exceeding the cap used to abort the
/// read (and, via the caller's `?`, the whole instance) instead of just
/// bounding what is kept.
async fn read_capped<R>(mut reader: R) -> rk_core::Result<CappedOutput>
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

async fn abort_task<T>(task: &mut JoinHandle<T>) {
    task.abort();
    let _ = task.await;
}

/// Guarantees a gate child's WHOLE process group dies, not just the `sh -c`
/// wrapper `kill_on_drop` reaches. `spawn_check_child` puts the child in its
/// own group via `.process_group(0)` (mirroring rk-harness's launcher); this
/// guard sends the negative-pid signal that actually reaches everything in
/// it. Disarmed only on the clean-completion path — every other exit from
/// `collect_child_output` (reader/wait join failure, timeout, or this
/// function's future simply being dropped out from under it) drops the guard
/// still armed and kills whatever the check left running, so a `mise`/
/// `cargo`/`rustc` grandchild can no longer outlive its `sh -c` parent.
struct ProcessGroupGuard(Option<u32>);

impl ProcessGroupGuard {
    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        if let Some(pid) = self.0 {
            // SAFETY: plain kill(2) on a process group we created ourselves
            // via `.process_group(0)` — the negative pid targets the group,
            // not the single process.
            unsafe {
                libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
            }
        }
    }
}

async fn collect_child_output(
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
    // dropping the kill_on_drop child; `group_guard` above is what reaches
    // any grandchildren it left behind.
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
                // drops the `kill_on_drop` child. What an `OnTimeout::Fail`
                // policy does with this — error out, but only after the
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

fn definition_inside_roots(candidate: &Path, repo: &str, global_root: &Path) -> Option<PathBuf> {
    let candidate = candidate.canonicalize().ok()?;
    if !candidate.is_file() {
        return None;
    }
    let repo_root = PathBuf::from(repo)
        .join(".rk")
        .join("workflows")
        .canonicalize()
        .ok();
    let global_root = global_root.canonicalize().ok();
    [repo_root, global_root]
        .into_iter()
        .flatten()
        .any(|root| candidate.starts_with(root))
        .then_some(candidate)
}

fn resolve_worktree_cwd(
    worktree: &Path,
    requested: Option<&str>,
    ctx: &WorkflowContext,
) -> rk_core::Result<PathBuf> {
    let root = worktree.canonicalize().map_err(|e| {
        rk_core::Error::other(format!(
            "run step: cannot resolve worktree '{}': {e}",
            worktree.display()
        ))
    })?;
    let relative = requested.map(|value| interpolate(value, ctx));
    let candidate = match relative {
        None => root.clone(),
        Some(value) => {
            let path = Path::new(&value);
            if path.is_absolute() {
                return Err(rk_core::Error::other(
                    "run step: cwd must be relative to the agent worktree",
                ));
            }
            root.join(path)
        }
    };
    let candidate = candidate.canonicalize().map_err(|e| {
        rk_core::Error::other(format!(
            "run step: cannot resolve cwd '{}': {e}",
            candidate.display()
        ))
    })?;
    if !candidate.starts_with(&root) {
        return Err(rk_core::Error::other(
            "run step: cwd escapes the agent worktree",
        ));
    }
    if !candidate.is_dir() {
        return Err(rk_core::Error::other(format!(
            "run step: cwd '{}' is not a directory",
            candidate.display()
        )));
    }
    Ok(candidate)
}

/// One agent in a fan-out set: its name, its branch, and the ticket it drains.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FannedAgent {
    pub agent: String,
    pub branch: Option<String>,
    pub ticket: Option<String>,
    /// This generation's join key, captured at fan-out time. `dismiss_all`
    /// verifies it against the live registry row before acting — see
    /// `Supervisor::dismiss_checked` — so a fanned dismiss can never tear down
    /// a different generation that came to hold `agent`'s name later.
    /// `None` for a fan-out built before this migration; preserves the old
    /// unchecked behaviour rather than refusing to dismiss.
    #[serde(default)]
    pub spawn: Option<rk_core::id::SpawnId>,
}

/// Control-flow signal threaded out of a step (or nested step sequence).
enum Flow {
    /// Continue with the next step in sequence.
    Next,
    /// Continue and join a completed sub-workflow result into this step's
    /// durable completion snapshot.
    NextWithSubworkflowResult(Value),
    /// A nested control-flow block joined one child. Its link remains durable
    /// until the enclosing top-level cursor advances.
    NextAfterNestedSubworkflow,
    /// Exit the nearest enclosing `repeat` (or end the workflow at top level).
    Break,
}

pub struct WorkflowEngine {
    layout: Layout,
    supervisor: Arc<Supervisor>,
    space: Space,
    tickets: Arc<Tickets>,
    global_agents: HashMap<String, AgentProfile>,
    /// Global cost-tier routing; a workflow's own `tiers:` table shadows it.
    tier_routing: TierRouting,
    default_harness: String,
    /// When set, a `run` step may only invoke a repo-registered named check; a
    /// raw inline command is refused fail-closed (TKT-30, `[policy]`).
    require_named_checks: bool,
    /// Whether the supervisor's self-healing respawn sweep is armed. A crashed
    /// rat may still come back when it is, so a `wait` on one keeps blocking
    /// until the sweep gives up; with the sweep disarmed a crash is final and
    /// the `wait` fails immediately (TKT-147).
    respawn_enabled: bool,
    /// Whether `finalize` runs the guaranteed-cleanup safety net
    /// (`Supervisor::dismiss_orphaned_instance_agents`) over every agent a
    /// terminalizing workflow instance spawned. A separate switch from the
    /// periodic `[worktree_sweep]` timer — see
    /// `rk_core::config::WorktreeSweepConfig::finalize_cleanup_enabled`.
    finalize_cleanup_enabled: bool,
    require_approval_for_landing: bool,
    automated_landing_workflows: Vec<String>,
    allowed_target_branches: Vec<String>,
    /// Fleet-wide concurrent-agent ceiling shared with the continuous-drain
    /// autoscaler (`[drain] max_wip`): a `spawn` step waits for a free slot
    /// under the same cap a drain refill respects, so workflow-spawned agents
    /// (e.g. steward reviewers) cannot unboundedly outrun it. Zero (the
    /// default, and drain's own "disabled" value) means no ceiling — matches
    /// pre-admission-control behaviour.
    fleet_wip_cap: usize,
    instances: Mutex<HashMap<String, Instance>>,
    /// Pruned terminal instances, kept for history. Held apart from `instances`
    /// rather than flagged inside it so every existing reader — `list`, the
    /// inbox sweep, the step machine — stays untouched and simply stops seeing
    /// an archived run.
    ///
    /// LOCK ORDER: `instances` before `archived`, always. The two are taken
    /// together only in [`archive`](WorkflowEngine::archive),
    /// [`unarchive`](WorkflowEngine::unarchive), and their read-side helpers.
    archived: Mutex<HashMap<String, Instance>>,
}

impl WorkflowEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        layout: Layout,
        supervisor: Arc<Supervisor>,
        space: Space,
        tickets: Arc<Tickets>,
        global_agents: HashMap<String, AgentProfile>,
        tier_routing: TierRouting,
        default_harness: String,
        require_named_checks: bool,
        respawn_enabled: bool,
        require_approval_for_landing: bool,
        automated_landing_workflows: Vec<String>,
        allowed_target_branches: Vec<String>,
        fleet_wip_cap: usize,
        finalize_cleanup_enabled: bool,
    ) -> Self {
        Self {
            layout,
            supervisor,
            space,
            tickets,
            global_agents,
            tier_routing,
            default_harness,
            require_named_checks,
            respawn_enabled,
            finalize_cleanup_enabled,
            require_approval_for_landing,
            automated_landing_workflows,
            allowed_target_branches,
            fleet_wip_cap,
            instances: Mutex::new(HashMap::new()),
            archived: Mutex::new(HashMap::new()),
        }
    }

    /// Resolve `<name>` to a definition file: `<repo>/.rk/workflows/<name>.cue`
    /// wins over `~/.rat-kingdom/workflows/<name>.cue`. Direct `.cue` paths are
    /// accepted only when they stay inside one of those two roots.
    pub fn find_definition(&self, name: &str, repo: &str) -> rk_core::Result<PathBuf> {
        let as_path = PathBuf::from(name);
        if as_path.extension().map(|e| e == "cue").unwrap_or(false) && as_path.exists() {
            return definition_inside_roots(&as_path, repo, &self.layout.workflows_dir())
                .ok_or_else(|| {
                    rk_core::Error::other(format!(
                        "workflow definition path '{}' is outside the registered workflow roots",
                        as_path.display()
                    ))
                });
        }
        let repo_local = PathBuf::from(repo)
            .join(".rk")
            .join("workflows")
            .join(format!("{name}.cue"));
        if repo_local.exists() {
            return definition_inside_roots(&repo_local, repo, &self.layout.workflows_dir())
                .ok_or_else(|| {
                    rk_core::Error::other(format!(
                        "repo-local workflow '{}' is outside the registered workflow root",
                        repo_local.display()
                    ))
                });
        }
        let global = self.layout.workflows_dir().join(format!("{name}.cue"));
        if global.exists() {
            return definition_inside_roots(&global, repo, &self.layout.workflows_dir())
                .ok_or_else(|| {
                    rk_core::Error::other(format!(
                        "global workflow '{}' is outside the registered workflow root",
                        global.display()
                    ))
                });
        }
        Err(rk_core::Error::other(format!(
            "no workflow named '{name}' (looked in {} and {})",
            repo_local.display(),
            global.display()
        )))
    }

    /// Whether this exact definition carries the operator's configured
    /// unattended-landing authority. Name membership alone is insufficient:
    /// repo-local definitions shadow global ones during normal resolution, so
    /// trusting only the name would let a repository replace `steward.cue` and
    /// inherit a capability intended for an operator-managed definition.
    fn is_automated_landing_definition(&self, file: &Path, workflow: &str) -> bool {
        if !self
            .automated_landing_workflows
            .iter()
            .any(|trusted| trusted == workflow)
        {
            return false;
        }
        let Ok(managed_dir) = std::fs::canonicalize(self.layout.workflows_dir()) else {
            return false;
        };
        let Ok(definition) = std::fs::canonicalize(file) else {
            return false;
        };
        definition.parent() == Some(managed_dir.as_path())
    }

    pub fn definitions(&self, repo: &str) -> Vec<String> {
        let mut names: Vec<String> = rk_workflow::definitions(&self.layout.workflows_dir())
            .into_iter()
            .chain(rk_workflow::definitions(
                &PathBuf::from(repo).join(".rk").join("workflows"),
            ))
            .filter_map(|p| p.file_stem().map(|s| s.to_string_lossy().to_string()))
            .collect();
        names.sort();
        names.dedup();
        names
    }

    /// Load, validate, and launch a workflow. Returns the instance snapshot;
    /// execution continues in a background task.
    pub fn run(
        self: &Arc<Self>,
        name: &str,
        repo: &str,
        params: HashMap<String, Value>,
    ) -> rk_core::Result<Instance> {
        self.run_owned(name, repo, params, None)
    }

    /// Launch a workflow with an explicit coordinator-session owner.
    pub fn run_owned(
        self: &Arc<Self>,
        name: &str,
        repo: &str,
        params: HashMap<String, Value>,
        coordinator: Option<String>,
    ) -> rk_core::Result<Instance> {
        self.run_owned_with_id(prefixed_id("wf"), name, repo, params, coordinator)
    }

    /// Launch a workflow using a caller-supplied durable instance id. If that id
    /// already exists, return its snapshot instead of dispatching a second copy.
    /// Factory approvals use this to bind approval to a workflow instance before
    /// launch and make retries/concurrent execute calls single-flight.
    pub fn run_owned_with_id(
        self: &Arc<Self>,
        instance_id: String,
        name: &str,
        repo: &str,
        params: HashMap<String, Value>,
        coordinator: Option<String>,
    ) -> rk_core::Result<Instance> {
        self.run_owned_with_id_and_schedule(
            instance_id,
            name,
            repo,
            params,
            coordinator,
            None,
            None,
        )
    }

    /// Launch a workflow the reactor fired from `trigger`, tagging the instance
    /// so [`live_count_for_trigger`](Self::live_count_for_trigger) can enforce
    /// that trigger's `maxInFlight` admission cap. Otherwise identical to
    /// [`run_owned_with_id`](Self::run_owned_with_id).
    pub fn run_owned_with_id_from_trigger(
        self: &Arc<Self>,
        instance_id: String,
        trigger: &str,
        name: &str,
        repo: &str,
        params: HashMap<String, Value>,
    ) -> rk_core::Result<Instance> {
        self.run_owned_with_id_and_schedule(
            instance_id,
            name,
            repo,
            params,
            None,
            None,
            Some(trigger.to_string()),
        )
    }

    pub fn run_scheduled(
        self: &Arc<Self>,
        schedule: &str,
        name: &str,
        repo: &str,
        params: HashMap<String, Value>,
    ) -> rk_core::Result<Instance> {
        self.run_owned_with_id_and_schedule(
            prefixed_id("wf"),
            name,
            repo,
            params,
            None,
            Some(schedule.to_string()),
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn run_owned_with_id_and_schedule(
        self: &Arc<Self>,
        instance_id: String,
        name: &str,
        repo: &str,
        params: HashMap<String, Value>,
        coordinator: Option<String>,
        schedule: Option<String>,
        trigger: Option<String>,
    ) -> rk_core::Result<Instance> {
        let file = self.find_definition(name, repo)?;
        let definition_digest = definition_digest(&file)?;
        let workflow = rk_workflow::load(&file, &params)?;
        let automated_landing_authorized =
            self.is_automated_landing_definition(&file, &workflow.name);
        let stale_timeout_secs = resolve_stale_timeout_secs(&workflow)?;

        let instance = Instance {
            id: instance_id,
            workflow: workflow.name.clone(),
            repo: repo.to_string(),
            coordinator,
            schedule,
            status: InstanceStatus::Running,
            revision: 0,
            current_step: 0,
            total_steps: workflow.steps.len(),
            context: WorkflowContext::default(),
            error: None,
            awaiting: None,
            instance_max_usd: workflow.budget.map(|b| b.max_usd),
            definition: name.to_string(),
            definition_digest,
            automated_landing_authorized,
            params,
            depth: 0,
            started_at: chrono::Utc::now(),
            completed_at: None,
            archived_at: None,
            trigger,
            stale_timeout_secs,
        };
        if let Some(existing) = self.store_if_absent(instance.clone())? {
            return Ok(existing);
        }
        self.spawn_execution(instance.id.clone(), workflow, repo.to_string());
        Ok(instance)
    }

    /// Drive an instance's steps to completion on a background task, then record
    /// the terminal status and emit the completion/failure event. Shared by a
    /// fresh `run` and a post-restart `resume`, so both paths finalize
    /// identically.
    fn spawn_execution(self: &Arc<Self>, id: String, workflow: Workflow, repo: String) {
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            let result = engine.execute(&id, workflow, &repo).await;
            // The instance record carries the workflow name for the event; read
            // it back rather than threading it through the moved `workflow`.
            let workflow_name = engine.status(&id).map(|i| i.workflow).unwrap_or_default();
            if let Err(error) = engine.finalize(&id, &repo, &workflow_name, result).await {
                warn!(instance = %id, %error, "workflow terminal state was not persisted");
            }
        });
    }

    /// Record an instance's terminal status, broadcast its completion event,
    /// and run the guaranteed-cleanup safety net over every agent this
    /// instance spawned (TKT-01M04N6W4X47KMXDA6MH0WPH8H): a `finally`-style
    /// sweep, not per-arm CUE `dismiss`/`dismiss_all` steps, so a workflow
    /// that errors out (or completes) before reaching its own cleanup step
    /// still reclaims every spawned agent's worktree. See
    /// [`Supervisor::dismiss_orphaned_instance_agents`].
    async fn finalize(
        &self,
        id: &str,
        repo: &str,
        workflow_name: &str,
        result: rk_core::Result<()>,
    ) -> rk_core::Result<()> {
        let (status, error) = match result {
            Ok(()) => (InstanceStatus::Completed, None),
            Err(e) => (InstanceStatus::Failed, Some(e.to_string())),
        };
        // Guarded like `timeout_stale_instance`: only write a terminal status
        // if the instance is still `Running` under the lock at the moment of
        // the write. Without this, a `finalize` from a genuinely still-running
        // `execute()` future can race the B8 stale-timeout sweep and
        // unconditionally overwrite the `Failed` it already persisted with
        // this call's `Completed` — silently reviving a workflow the sweep
        // had correctly declared wedged.
        let mut already_terminal = false;
        let transition = self.try_update_with_reason(id, "terminal", |i| {
            if i.status.is_terminal() {
                already_terminal = true;
                return;
            }
            i.status = status;
            i.error = error.clone();
            i.completed_at = Some(chrono::Utc::now());
        });
        if already_terminal {
            // The race resolved itself before this write: something else
            // (the stale-timeout sweep, or a duplicate finalize) already
            // persisted a terminal status. That status wins; this is not a
            // recovery failure, so do not escalate.
            info!(instance = %id, status = ?status, "finalize: instance already terminal, not overwriting");
            return Ok(());
        }
        match &transition {
            Err(persist_error) => self.fail_recovery_in_memory(
                id,
                format!("terminal state persistence failed: {persist_error}"),
            ),
            Ok(false) => self.fail_recovery_in_memory(
                id,
                "terminal state transition did not update an instance".into(),
            ),
            Ok(true) => {}
        }
        require_persisted_transition(transition, id, "terminal state")?;
        let final_status = if status == InstanceStatus::Completed {
            "workflow_complete"
        } else {
            "workflow_failed"
        };
        info!(instance = %id, status = ?status, "workflow finished");
        let _ = self.space.out(rk_core::tuple::Tuple::new(
            Category::Event,
            repo_name_of(repo),
            final_status,
            "daemon".to_string(),
            json!({"instance": id, "workflow": workflow_name, "error": error}),
        ));
        // Best-effort: the instance's own terminal state is already durably
        // persisted above regardless of whether every spawned agent could be
        // swept, so a dismiss failure here is logged, never propagated. Gated
        // by `finalize_cleanup_enabled` (defaults off for bare/test daemons):
        // see the field doc for why this must not run unconditionally.
        if self.finalize_cleanup_enabled {
            let swept = self.supervisor.dismiss_orphaned_instance_agents(id).await;
            if !swept.is_empty() {
                let failed = swept.iter().filter(|(_, ok)| !ok).count();
                info!(
                    instance = %id,
                    count = swept.len(),
                    failed,
                    "finalize-time cleanup sweep dismissed agents left behind by their own workflow steps"
                );
            }
        }
        Ok(())
    }

    /// Guarded terminal transition for [`stale_timeout_sweep_once`](Self::stale_timeout_sweep_once):
    /// mutates `instance.id` from `Running` to `Failed` ONLY IF it is still
    /// `Running` under the lock at the moment of the write, so a genuine
    /// completion racing the sweep between its read (in `stale_timeout_sweep_once`)
    /// and this write always wins — the instance is never overwritten out from
    /// under its own (still-live) execute() future. `Ok(false)` means that race
    /// resolved itself (or the instance is already gone); that is NOT an error.
    /// [`finalize`](Self::finalize) carries the mirror-image guard (only
    /// writes a terminal status if the instance is still `Running`), so
    /// whichever of the two writes the terminal status first wins and the
    /// other becomes a no-op rather than an overwrite. When it does
    /// transition, this performs the same terminal-state event +
    /// guaranteed-cleanup agent sweep `finalize` does — the ticket's
    /// "mark failed, finalize" — deliberately not calling `finalize` itself,
    /// which would call [`require_persisted_transition`] and turn the benign
    /// race outcome into a hard error.
    async fn timeout_stale_instance(
        &self,
        instance: &Instance,
        timeout_secs: u64,
    ) -> rk_core::Result<bool> {
        let id = &instance.id;
        let error_text = format!(
            "stale-instance timeout: Running past {timeout_secs}s wall-clock with no completion (strategic review B8) — likely a wedged execution future that skipped finalize"
        );
        let transition = self.try_update_with_reason(id, "terminal", |i| {
            if i.status != InstanceStatus::Running {
                return;
            }
            i.status = InstanceStatus::Failed;
            i.error = Some(error_text.clone());
            i.completed_at = Some(chrono::Utc::now());
        });
        if let Err(persist_error) = &transition {
            self.fail_recovery_in_memory(
                id,
                format!("stale-instance timeout persistence failed: {persist_error}"),
            );
        }
        if !transition? {
            return Ok(false);
        }
        info!(instance = %id, "workflow instance marked failed by the stale-Running hard timeout");
        let _ = self.space.out(rk_core::tuple::Tuple::new(
            Category::Event,
            repo_name_of(&instance.repo),
            "workflow_failed",
            "daemon".to_string(),
            json!({"instance": id, "workflow": instance.workflow, "error": error_text}),
        ));
        if self.finalize_cleanup_enabled {
            let swept = self.supervisor.dismiss_orphaned_instance_agents(id).await;
            if !swept.is_empty() {
                let failed = swept.iter().filter(|(_, ok)| !ok).count();
                info!(
                    instance = %id,
                    count = swept.len(),
                    failed,
                    "stale-instance timeout: cleanup sweep dismissed agents left behind"
                );
            }
        }
        Ok(true)
    }

    /// One pass of the stale-`Running`-instance hard timeout sweep (strategic
    /// review B8). A panic in an instance's execution future skips
    /// [`finalize`](Self::finalize), so the instance would otherwise stay
    /// `Running` forever with no live task backing it — this sweep is the only
    /// thing that ever notices. Every `Running` instance older than its
    /// effective timeout (the workflow's own `staleTimeout:` override, else
    /// `default_timeout`) is marked failed, finalized, and escalated through
    /// the B2 [`RecoveryAnnouncer`]. Returns the number of instances timed out.
    pub async fn stale_timeout_sweep_once(
        &self,
        now: DateTime<Utc>,
        default_timeout: Duration,
        announcer: &RecoveryAnnouncer,
        sinks: &SinkRegistry,
        cap: RateCap,
    ) -> usize {
        let stale: Vec<Instance> = self
            .list()
            .into_iter()
            .filter(|i| i.status == InstanceStatus::Running)
            .filter(|i| {
                let timeout = i
                    .stale_timeout_secs
                    .map(Duration::from_secs)
                    .unwrap_or(default_timeout);
                now.signed_duration_since(i.started_at)
                    .to_std()
                    .map(|elapsed| elapsed > timeout)
                    .unwrap_or(false)
            })
            .collect();
        let mut timed_out = 0usize;
        for instance in stale {
            let effective_secs = instance
                .stale_timeout_secs
                .unwrap_or(default_timeout.as_secs());
            match self.timeout_stale_instance(&instance, effective_secs).await {
                Ok(true) => {
                    timed_out += 1;
                    let notice = EscalationNotice::new(
                        "pending",
                        "instance-timeout",
                        Severity::Critical,
                        repo_name_of(&instance.repo),
                        format!("{} ({})", instance.workflow, instance.id),
                        format!(
                            "workflow instance {} (workflow `{}`) stayed Running past its {effective_secs}s hard timeout with no completion. Marked failed and finalized automatically.",
                            instance.id, instance.workflow
                        ),
                    )
                    .with_ref("instance", instance.id.clone())
                    .with_ref("workflow", instance.workflow.clone())
                    .with_ref("repo", instance.repo.clone());
                    if let Err(error) = announcer.announce(
                        &self.space,
                        sinks,
                        RecoveryAction {
                            kind: "instance-timeout".into(),
                            instance: "daemon".into(),
                            notice,
                        },
                        cap,
                    ) {
                        warn!(instance = %instance.id, %error, "stale-instance timeout: escalation announce failed");
                    }
                }
                Ok(false) => {}
                Err(error) => warn!(
                    instance = %instance.id,
                    %error,
                    "stale-instance timeout: failed to persist terminal transition"
                ),
            }
        }
        timed_out
    }

    /// Load persisted instances into memory on daemon startup (TKT-52).
    ///
    /// Every mutation writes each instance to
    /// `<home>/workflow-instances/<id>.json`; this restores that durable state
    /// before reactor or scheduler dispatch can mint a duplicate stable id.
    /// Completed and failed instances are loaded for history. Top-level
    /// `Running` instances are returned to the caller but are not started here.
    /// The daemon must call [`resume_rehydrated`](Self::resume_rehydrated) only
    /// after event consumers are listening. Calling this method again replaces
    /// the in-memory snapshots with the same durable state.
    pub fn rehydrate(self: &Arc<Self>) -> Vec<Instance> {
        for instance in self.read_instance_dir(&self.instances_dir()) {
            self.lock().insert(instance.id.clone(), instance.clone());
        }
        // The pruned side of the store: terminal runs an operator cleared off
        // the board. Loaded for history only, never resumed. An id present in
        // BOTH stores is the archive/persist crash window — the live copy wins,
        // so a crash mid-prune silently no-ops instead of losing a run.
        for instance in self.read_instance_dir(&self.archive_dir()) {
            if self.lock().contains_key(&instance.id) {
                continue;
            }
            self.lock_archived().insert(instance.id.clone(), instance);
        }
        let blocked = self.fail_legacy_unlinked_subworkflows();
        // Only top-level (depth 0) instances resume standalone. A linked nested
        // child is re-driven by its parent's resumed `sub_workflow` step, which
        // rejoins the same durable child id. Resuming it here as well would
        // execute the same interrupted step twice.
        let resumable: Vec<Instance> = self
            .lock()
            .values()
            .filter(|instance| {
                instance.status == InstanceStatus::Running
                    && instance.depth == 0
                    && !blocked.contains(&instance.id)
            })
            .cloned()
            .collect();
        if !resumable.is_empty() {
            info!(
                count = resumable.len(),
                "resuming in-flight workflow instances after restart"
            );
        }
        resumable
    }

    /// Snapshots written before `active_subworkflow` cannot prove which parent
    /// owns a running nested child. Continuing the parent would repeat the step
    /// and duplicate the child's side effects, while resuming both independently
    /// would race them. Fail the orphan and every exact current-step parent match
    /// closed so an operator can inspect and retry deliberately.
    fn fail_legacy_unlinked_subworkflows(&self) -> HashSet<String> {
        let snapshots = self.list();
        let linked: HashSet<&str> = snapshots
            .iter()
            .filter(|instance| instance.status == InstanceStatus::Running)
            .filter_map(|instance| instance.context.active_subworkflow.as_deref())
            .collect();
        let orphans: Vec<Instance> = snapshots
            .iter()
            .filter(|instance| instance.depth > 0 && !linked.contains(instance.id.as_str()))
            .cloned()
            .collect();
        let mut blocked = HashSet::new();

        for child in orphans {
            let parents: Vec<String> = snapshots
                .iter()
                .filter(|parent| {
                    parent.status == InstanceStatus::Running
                        && parent.depth + 1 == child.depth
                        && parent.repo == child.repo
                        && parent.started_at <= child.started_at
                        && self.current_step_contains_subworkflow(parent, &child.definition)
                })
                .map(|parent| parent.id.clone())
                .collect();
            let child_id = child.id.clone();
            for parent_id in parents {
                blocked.insert(parent_id.clone());
                if let Err(error) = self.try_update_with_reason(
                    &parent_id,
                    "legacy_sub_workflow_ambiguous",
                    |instance| {
                        instance.status = InstanceStatus::Failed;
                        instance.error = Some(format!(
                            "restart refused to repeat sub_workflow child {child_id} without durable parent linkage"
                        ));
                        instance.completed_at = Some(Utc::now());
                    },
                ) {
                    warn!(parent = %parent_id, child = %child_id, %error, "could not persist legacy sub-workflow parent failure; suppressing resume in this process");
                    self.fail_recovery_in_memory(
                        &parent_id,
                        format!("legacy parent failure persistence failed: {error}"),
                    );
                }
            }
            if child.status == InstanceStatus::Running {
                if let Err(error) = self.try_update_with_reason(
                    &child_id,
                    "legacy_sub_workflow_orphaned",
                    |instance| {
                        instance.status = InstanceStatus::Failed;
                        instance.error = Some(
                            "restart refused an unlinked legacy sub_workflow child; retry its parent deliberately"
                                .into(),
                        );
                        instance.completed_at = Some(Utc::now());
                    },
                ) {
                    warn!(child = %child_id, %error, "could not persist legacy sub-workflow child failure");
                    self.fail_recovery_in_memory(
                        &child_id,
                        format!("legacy child failure persistence failed: {error}"),
                    );
                }
            }
        }
        blocked
    }

    fn current_step_contains_subworkflow(&self, instance: &Instance, child: &str) -> bool {
        self.find_definition(&instance.definition, &instance.repo)
            .and_then(|file| rk_workflow::load(&file, &instance.params))
            .ok()
            .and_then(|workflow| workflow.steps.get(instance.current_step).cloned())
            .is_some_and(|step| step_contains_subworkflow(&step, child))
    }

    /// Resume the top-level running snapshots returned by [`rehydrate`](Self::rehydrate).
    ///
    /// This is deliberately separate from loading durable ids so startup can
    /// close the duplicate-dispatch window before resumed workflows emit events.
    pub fn resume_rehydrated(self: &Arc<Self>, resumable: Vec<Instance>) {
        for instance in resumable {
            self.resume(instance);
        }
    }

    /// Read every `<id>.json` snapshot in one instance directory. A file that
    /// no longer parses is reported as a `workflow_persistence_corrupt`
    /// obstacle and skipped, so one bad snapshot cannot stop the rest of the
    /// store loading. A directory that does not exist yet is simply empty.
    fn read_instance_dir(&self, dir: &Path) -> Vec<Instance> {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return Vec::new();
        };
        let mut loaded = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            match std::fs::read(&path)
                .ok()
                .and_then(|data| serde_json::from_slice::<Instance>(&data).ok())
            {
                Some(instance) => loaded.push(instance),
                None => {
                    let error =
                        format!("unreadable persisted workflow instance: {}", path.display());
                    warn!(path = %path.display(), "{error}");
                    self.record_persistence_failure(&path, error);
                }
            }
        }
        loaded
    }

    /// Resume one rehydrated `Running` instance: reload its definition with the
    /// original params and continue execution from the persisted step cursor. A
    /// definition that no longer loads (deleted, or now invalid) fails the
    /// instance cleanly — surfaced in `rk inbox` — rather than leaving it wedged
    /// `Running` forever.
    fn resume(self: &Arc<Self>, instance: Instance) {
        let id = instance.id.clone();
        let loaded = match self
            .find_definition(&instance.definition, &instance.repo)
            .and_then(|file| {
                let digest = definition_digest(&file)?;
                if !instance.definition_digest.is_empty() && instance.definition_digest != digest {
                    return Err(rk_core::Error::other(format!(
                        "definition digest changed (persisted {}, current {})",
                        instance.definition_digest, digest
                    )));
                }
                Ok((rk_workflow::load(&file, &instance.params)?, digest))
            }) {
            Ok(loaded) => loaded,
            Err(e) => {
                warn!(instance = %id, error = %e, "cannot resume workflow; failing instance");
                self.update(&id, |i| {
                    i.status = InstanceStatus::Failed;
                    i.error = Some(format!("resume failed: could not reload definition: {e}"));
                    i.awaiting = None;
                    i.completed_at = Some(chrono::Utc::now());
                });
                return;
            }
        };
        let (workflow, current_digest) = loaded;
        // Backfill the digest for instances written before this field existed;
        // subsequent snapshots then carry the restart guard.
        if instance.definition_digest.is_empty() {
            self.update(&id, |i| i.definition_digest = current_digest.clone());
        }
        // A stale `awaiting` flag from before the restart is cleared here; the
        // resumed gate re-sets it if it parks again.
        self.update(&id, |i| i.awaiting = None);
        info!(instance = %id, from_step = instance.current_step, "resuming workflow after restart");
        self.spawn_execution(id, workflow, instance.repo);
    }

    /// Run the top-level step list once. `current_step` is the resume cursor:
    /// the count of top-level steps that have fully COMPLETED, i.e. the index of
    /// the next step to run. Steps already completed before a restart are
    /// skipped; the step that was in flight when the daemon stopped re-runs
    /// (at-least-once for the interrupted step). Steps nested inside
    /// `when`/`repeat` execute in place without advancing the cursor (they are
    /// bounded by the `repeat` cap), so a resume inside a loop re-enters the
    /// whole enclosing top-level step.
    async fn execute(&self, id: &str, workflow: Workflow, repo: &str) -> rk_core::Result<()> {
        let start = self.lock().get(id).map(|i| i.current_step).unwrap_or(0);
        for (index, step) in workflow.steps.iter().enumerate() {
            if index < start {
                // Already completed on a prior run; do not re-execute it.
                continue;
            }
            let flow = self
                .run_step(id, step, repo, &workflow.agents, &workflow.tiers)
                .await?;
            let (subworkflow_result, clear_subworkflow) = match flow {
                Flow::Break => {
                    // A top-level break ends the workflow (nothing to loop out of).
                    break;
                }
                Flow::Next => (None, false),
                Flow::NextWithSubworkflowResult(result) => (Some(result), true),
                Flow::NextAfterNestedSubworkflow => (None, true),
            };
            // Advance only AFTER the step completes, so a restart resumes at the
            // interrupted step and never re-runs a finished one.
            if !self.update_with_reason(id, "step_advanced", |instance| {
                complete_top_level_step(instance, index, clear_subworkflow, subworkflow_result);
            }) {
                return Err(rk_core::Error::other(format!(
                    "could not durably advance workflow {id} after step {index}"
                )));
            }
        }
        Ok(())
    }

    /// Run a sequence of steps, short-circuiting on the first `Break`.
    fn run_steps<'a>(
        &'a self,
        id: &'a str,
        steps: &'a [Step],
        repo: &'a str,
        agents: &'a HashMap<String, AgentProfile>,
        tiers: &'a TierRouting,
    ) -> StepFuture<'a> {
        Box::pin(async move {
            let mut joined_subworkflow = false;
            for step in steps {
                match self.run_step(id, step, repo, agents, tiers).await? {
                    Flow::Break => return Ok(Flow::Break),
                    Flow::Next => {}
                    Flow::NextWithSubworkflowResult(result) => {
                        if joined_subworkflow {
                            return Err(rk_core::Error::other(
                                "multiple nested sub_workflow executions in one top-level step are refused because they cannot be replayed safely",
                            ));
                        }
                        let result_for_snapshot = result.clone();
                        self.try_update_with_reason(
                            id,
                            "nested_sub_workflow_joined",
                            |instance| {
                                join_nested_subworkflow_result(instance, result_for_snapshot);
                            },
                        )?;
                        joined_subworkflow = true;
                    }
                    Flow::NextAfterNestedSubworkflow => {
                        if joined_subworkflow {
                            return Err(rk_core::Error::other(
                                "multiple nested sub_workflow executions in one top-level step are refused because they cannot be replayed safely",
                            ));
                        }
                        joined_subworkflow = true;
                    }
                }
            }
            if joined_subworkflow {
                Ok(Flow::NextAfterNestedSubworkflow)
            } else {
                Ok(Flow::Next)
            }
        })
    }

    /// Execute a single step (recursing for `when`/`repeat`). `tiers` is the
    /// workflow's own tier-routing table, chained over the global one at fan-out.
    fn run_step<'a>(
        &'a self,
        id: &'a str,
        step: &'a Step,
        repo: &'a str,
        agents: &'a HashMap<String, AgentProfile>,
        tiers: &'a TierRouting,
    ) -> StepFuture<'a> {
        Box::pin(async move {
            let ctx = self.context(id);
            match step {
                Step::Spawn(spawn) => {
                    // Best-effort pre-wait: cheap and avoids constructing spawn
                    // params / paying repo discovery just to be refused, but it is
                    // NOT the authoritative gate — `spawn_agent` re-checks
                    // atomically against the live registry, and the loop below
                    // retries if a concurrent admitter (a drain refill, or another
                    // workflow spawn step) wins the race for the slot this saw free.
                    self.await_fleet_capacity(id).await;
                    let resolved =
                        resolve(spawn, agents, &self.global_agents, &self.default_harness)?;
                    let title = interpolate(&spawn.task.title, &ctx);
                    let prompt = spawn
                        .task
                        .description
                        .as_ref()
                        .map(|d| interpolate(d, &ctx));
                    let params = SpawnParams {
                        repo: repo.to_string(),
                        task: title,
                        prompt,
                        role: spawn.role.clone(),
                        coordination: spawn.coordination.clone(),
                        harness: Some(resolved.harness),
                        parent: None,
                        base: spawn.branch.clone().or(ctx.active_branch.clone()),
                        model: resolved.model,
                        permission_mode: resolved.permission_mode,
                        attach: false,
                        workflow_instance: Some(id.to_string()),
                        coordinator: self.coordinator(id),
                        instance_max_usd: self.instance_budget(id),
                    };
                    let record = loop {
                        match self.spawn_agent(params.clone(), self.fleet_wip_cap).await {
                            Ok(record) => break record,
                            Err(e) if is_fleet_wip_refusal(&e) => {
                                self.update(id, |i| i.awaiting = Some("fleet_wip".to_string()));
                                tokio::time::sleep(FLEET_CAPACITY_POLL).await;
                            }
                            Err(e) => return Err(e),
                        }
                    };
                    self.update(id, |i| {
                        i.awaiting = None;
                        i.context.active_agent = Some(record.name.clone());
                        i.context.active_agent_spawn = Some(record.spawn_id());
                        i.context.active_branch = record.branch.clone();
                    });
                }
                Step::Wait(wait) => {
                    let agent = ctx
                        .active_agent
                        .clone()
                        .ok_or_else(|| rk_core::Error::other("wait step with no active agent"))?;
                    let deadline = tokio::time::Instant::now() + parse_duration(&wait.timeout)?;
                    let payload = self
                        .await_result(id, &agent, deadline, "wait", &wait.timeout)
                        .await?;
                    self.update(id, |i| {
                        i.context.previous_result = Some(payload.clone());
                        i.context.awaited = vec![agent.clone()];
                    });
                }
                Step::Evaluate(eval) => {
                    // Before judging the result, assert it came from a rat that
                    // actually ran (TKT-147). `expect`/`anyOf` unify against
                    // whatever landed in previousResult and cannot tell a real
                    // verdict from a crashed rat's leftovers, so a gate alone
                    // would pass a silent no-op as a clean run.
                    for agent in &ctx.awaited {
                        if let Some(why) = self.liveness_failure(agent) {
                            return Err(rk_core::Error::other(format!("evaluate failed: {why}")));
                        }
                    }
                    let actual = ctx.previous_result.clone().unwrap_or(Value::Null);
                    // Pass if the result unifies with `expect` OR any `anyOf`
                    // alternative — a disjunction single-`expect` unification (an
                    // AND over fields) cannot express. Short-circuits on the
                    // first match.
                    let mut passed = rk_workflow::unify_concrete(&eval.expect, &actual)?;
                    for alt in &eval.any_of {
                        if passed {
                            break;
                        }
                        passed = rk_workflow::unify_concrete(alt, &actual)?;
                    }
                    if !passed {
                        return Err(rk_core::Error::other(format!(
                            "evaluate failed: expect {} (anyOf {:?}) did not unify with {}",
                            eval.expect, eval.any_of, actual
                        )));
                    }
                }
                Step::Dismiss(dismiss) => {
                    let agent = ctx.active_agent.clone().ok_or_else(|| {
                        rk_core::Error::other("dismiss step with no active agent")
                    })?;
                    let expected_spawn = ctx.active_agent_spawn;
                    let outcome = self
                        .supervisor
                        .dismiss_checked(&agent, expected_spawn, dismiss.no_merge)
                        .await?;
                    self.update(id, |i| {
                        i.context.previous_result = Some(outcome.clone());
                        i.context.awaited = Vec::new();
                        i.context.active_agent = None;
                        i.context.active_agent_spawn = None;
                    });
                }
                Step::Gate(gate) => match gate.gate_type.as_str() {
                    "timer" => {
                        let duration = gate
                            .duration
                            .as_deref()
                            .ok_or_else(|| rk_core::Error::other("timer gate missing duration"))?;
                        tokio::time::sleep(parse_duration(duration)?).await;
                    }
                    "approval" => {
                        // Block until a human decision for THIS instance arrives
                        // (via `rk approve`/`rk reject`, which write a
                        // `workflow_approval` event) or the timeout elapses.
                        let timeout = parse_duration(gate.timeout.as_deref().unwrap_or("24h"))?;
                        // Scope the wait to this instance. The `read` that lifts
                        // the decision behind this gate derives its predicate
                        // from the SAME constructor (`fromInstance: true`), so
                        // the two cannot drift apart — which is what let the
                        // read take a peer's decision in TKT-172.
                        let pattern = Pattern::for_workflow_instance(
                            Category::Event,
                            "workflow_approval",
                            id,
                        );
                        // Flag the instance as parked so `rk inbox` can surface
                        // it with the `rk approve`/`rk reject` resolving command.
                        self.update_with_reason(id, "approval_parked", |i| {
                            i.awaiting = Some("approval".to_string())
                        });
                        let read = self.space.rd(&pattern, timeout).await;
                        self.update_with_reason(id, "approval_resolved", |i| i.awaiting = None);
                        let decision = match read.map_err(|e| {
                            rk_core::Error::other(format!("approval gate failed: {e}"))
                        })? {
                            Some(tuple) => tuple.payload,
                            None => {
                                // Fail closed: no human response means no merge.
                                // Record the synthetic decision as a
                                // workflow_approval event too, so a following
                                // `read`/`when` routes the timeout down the same
                                // clean reject path as an explicit rejection —
                                // rather than the read blocking on a tuple that
                                // never arrives.
                                let payload = json!({
                                    "instance": id,
                                    "approved": false,
                                    "by": "system",
                                    "reason": format!("no approval within {}", gate.timeout.as_deref().unwrap_or("24h")),
                                });
                                let _ = self.space.out(rk_core::tuple::Tuple::new(
                                    Category::Event,
                                    repo_name_of(repo),
                                    "workflow_approval",
                                    "system".to_string(),
                                    payload.clone(),
                                ));
                                payload
                            }
                        };
                        let approval_granted =
                            decision.get("approved").and_then(Value::as_bool) == Some(true);
                        self.update(id, |i| {
                            i.context.previous_result = Some(decision);
                            i.context.awaited = Vec::new();
                            i.context.approval_granted = approval_granted;
                        });
                    }
                    other => {
                        return Err(rk_core::Error::other(format!("unknown gate type: {other}")));
                    }
                },
                Step::Read(read) => {
                    let category = Category::from_str(&read.category)?;
                    let scope = read.scope.clone().unwrap_or_else(|| repo_name_of(repo));
                    // Bind the read, or it is satisfied by a stranger. Bare
                    // (category, scope, identity) is NOT an identity: two
                    // instances of one workflow on one repo share it by
                    // construction, and "newest wins" then routes an instance on
                    // a tuple written for its peer. Two discriminators cure it,
                    // by which key the wanted tuple actually carries:
                    //
                    // - `fromAgent` (TKT-161) — what an agent THIS instance
                    //   spawned wrote. The reactor fires `steward` per rat
                    //   completion, so concurrent reviewers write
                    //   `artifact/<repo>/review` at the same time and an unbound
                    //   read can hand a steward the OTHER steward's verdict to
                    //   land on. Cured by the agent's name plus its generation
                    //   floor (`for_agent_since`), since a name keys a
                    //   generation and not a rat.
                    // - `fromInstance` (TKT-172) — what was written FOR this
                    //   run. The `workflow_approval` event behind an approval
                    //   gate is the case: two gated instances on one repo, one
                    //   approved and one rejected, and an unbound read routes
                    //   both on whichever decision landed last. Cured by the
                    //   instance id (`for_workflow_instance`) — the same
                    //   predicate the gate itself waits on, so gate and read
                    //   cannot disagree about whose decision this is. No
                    //   generation floor: an instance id is never reused.
                    // - `forCommit` (steward Phase 2 verdict cache) — what was
                    //   written for a specific branch tip, regardless of who
                    //   wrote it or which run it belongs to. Deliberately the
                    //   OPPOSITE scoping of the other two: it exists to find a
                    //   PRIOR run's verdict, not this run's own.
                    //
                    // All four of `search`/`fromAgent`/`fromInstance`/`forCommit`
                    // write the one `payload_search` slot, so at most one may be
                    // set.
                    let bindings = read.from_agent as u8
                        + read.from_instance as u8
                        + read.search.is_some() as u8
                        + read.for_commit.is_some() as u8;
                    if bindings > 1 {
                        return Err(rk_core::Error::other(
                            "read step sets more than one of \
                             `fromAgent`/`fromInstance`/`forCommit`/`search`; they claim the \
                             same payload predicate — keep one",
                        ));
                    }
                    let mut pattern = if read.from_agent {
                        let agent = ctx.active_agent.clone().ok_or_else(|| {
                            rk_core::Error::other(
                                "read step has `fromAgent` but no active agent; only a step \
                                 after a `spawn` can bind a read to its author",
                            )
                        })?;
                        Pattern::for_agent_since(
                            category,
                            read.identity.clone(),
                            &agent,
                            self.generation_floor(id, &agent),
                        )
                    } else if read.from_instance {
                        Pattern::for_workflow_instance(category, read.identity.clone(), id)
                    } else if let Some(sha) = read.for_commit.as_deref() {
                        if sha.is_empty() {
                            return Err(rk_core::Error::other(
                                "read step has `forCommit` set to an empty sha; a cache lookup \
                                 needs a real commit to key on — guard the step at CUE load \
                                 time when the sha may be absent",
                            ));
                        }
                        let branch = read.for_branch.as_deref().unwrap_or_default();
                        if branch.is_empty() {
                            return Err(rk_core::Error::other(
                                "read step has `forCommit` but no (or an empty) `forBranch`; a \
                                 sha alone is not exclusive to one branch — two branches cut \
                                 from the same point share a tip commit, so this cache lookup \
                                 needs the branch bound too, or guard the step at CUE load time \
                                 when the branch may be absent",
                            ));
                        }
                        Pattern::for_commit(category, read.identity.clone(), branch, sha)
                    } else {
                        let mut pattern =
                            Pattern::category(category).identity(read.identity.clone());
                        pattern.payload_search = read.search.clone();
                        pattern
                    };
                    pattern.scope = Some(scope);
                    // Newest match wins (scan is oldest-first, so pop the tail);
                    // fall back to a blocking read if none is present yet.
                    let tuple = match self
                        .space
                        .scan(&pattern)
                        .map_err(|e| rk_core::Error::other(format!("read scan failed: {e}")))?
                        .pop()
                    {
                        Some(t) => Some(t),
                        None => self
                            .space
                            .rd(&pattern, parse_duration(&read.timeout)?)
                            .await
                            .map_err(|e| rk_core::Error::other(format!("read failed: {e}")))?,
                    };
                    // `onTimeout: "continue"` (steward Phase 2 verdict cache) lets
                    // a bounded, non-blocking probe come back empty without
                    // ending the run — the following `when` routes on "nothing
                    // cached yet" instead. Every read before the cache used the
                    // fail-closed default, unchanged here.
                    let continue_on_miss = match read.on_timeout.as_str() {
                        "fail" => false,
                        "continue" => true,
                        other => {
                            return Err(rk_core::Error::other(format!(
                                "read step: unknown onTimeout {other:?} (expected \"fail\" or \
                                 \"continue\")"
                            )));
                        }
                    };
                    let value = match tuple {
                        Some(tuple) => match &read.field {
                            Some(field) => tuple.payload.get(field).cloned().unwrap_or(Value::Null),
                            None => tuple.payload.clone(),
                        },
                        None if continue_on_miss => Value::Null,
                        None => {
                            // Name the binding in the failure: a bound read that
                            // matched nothing is otherwise indistinguishable from
                            // a tuple that was never written. Under `fromAgent`
                            // the usual cause is an agent that left its own name
                            // out of the payload; under `fromInstance` it is a
                            // decision recorded without this run's id.
                            let bound_to = match (read.from_agent, ctx.active_agent.as_deref()) {
                                (true, Some(agent)) => format!(" written by {agent}"),
                                _ if read.from_instance => format!(" naming instance {id}"),
                                _ if read.for_commit.is_some() => {
                                    format!(
                                        " naming branch {:?} at commit {:?}",
                                        read.for_branch.as_deref(),
                                        read.for_commit.as_deref()
                                    )
                                }
                                _ => String::new(),
                            };
                            return Err(rk_core::Error::other(format!(
                                "read timed out after {} for {} tuple '{}'{bound_to}",
                                read.timeout, read.category, read.identity
                            )));
                        }
                    };
                    self.update(id, |i| {
                        i.context.vars.insert(read.into.clone(), value.clone());
                    });
                }
                Step::When(when) => {
                    let key = ctx
                        .vars
                        .get(&when.var)
                        .map(value_as_key)
                        .unwrap_or_default();
                    let branch = when.cases.get(&key).unwrap_or(&when.default);
                    return self.run_steps(id, branch, repo, agents, tiers).await;
                }
                Step::Repeat(repeat) => {
                    let mut joined_subworkflow = false;
                    for _ in 0..repeat.max {
                        match self
                            .run_steps(id, &repeat.steps, repo, agents, tiers)
                            .await?
                        {
                            Flow::Break => break,
                            Flow::Next => {}
                            Flow::NextAfterNestedSubworkflow => {
                                if joined_subworkflow {
                                    return Err(rk_core::Error::other(
                                        "repeat attempted more than one nested sub_workflow execution in a top-level step; refusing unsafe replay",
                                    ));
                                }
                                joined_subworkflow = true;
                            }
                            Flow::NextWithSubworkflowResult(_) => unreachable!(
                                "run_steps converts a direct nested sub_workflow result"
                            ),
                        }
                    }
                    if joined_subworkflow {
                        return Ok(Flow::NextAfterNestedSubworkflow);
                    }
                }
                Step::Break => return Ok(Flow::Break),
                Step::Stop(stop) => {
                    return Err(rk_core::Error::other(format!(
                        "workflow stopped: {}",
                        stop.reason.as_deref().unwrap_or("stop step reached")
                    )));
                }
                Step::ForEach(fe) => {
                    let fanout = self.fan_out(id, agents, tiers, repo, fe).await?;
                    // Recorded even when the query matched nothing: an empty set
                    // is still a set, and it is what tells the following
                    // `wait_all` that a fan-out ran (TKT-170).
                    self.update(id, |i| i.context.fanout = Some(fanout));
                }
                Step::WaitAll(wait_all) => {
                    let summary = self.join(id, ctx.fanout.as_deref(), wait_all).await?;
                    let awaited: Vec<String> = ctx
                        .fanout
                        .iter()
                        .flatten()
                        .map(|fa| fa.agent.clone())
                        .collect();
                    self.update(id, |i| {
                        i.context.previous_result = Some(summary.clone());
                        i.context.awaited = awaited.clone();
                    });
                }
                Step::DismissAll(dismiss_all) => {
                    let summary = self
                        .dismiss_fanout(
                            ctx.fanout.as_deref(),
                            dismiss_all,
                            ctx.previous_result.as_ref(),
                        )
                        .await?;
                    self.update(id, |i| {
                        i.context.previous_result = Some(summary.clone());
                        i.context.awaited = Vec::new();
                        // The fan-out set is spent once its branches are merged.
                        // Back to `None`, not an empty set: a later `wait_all`
                        // with no `for_each` of its own is an authoring error
                        // again, not a quiet night.
                        i.context.fanout = None;
                    });
                }
                Step::Run(run) => {
                    let result = self.run_command(id, &ctx, run, repo).await?;
                    // Optionally lift a field of the result into a ctx var so a
                    // following `when` can ROUTE on how the check went, not just
                    // fail on it (TKT-169). Same (field, into) semantics as a
                    // `read` step, including "a field the result does not carry
                    // lifts as null" — which `value_as_key` renders as the empty
                    // string, so it falls to the `when`'s `default` arm rather
                    // than silently matching a case.
                    let lifted = run.into.as_ref().map(|into| {
                        let value = match &run.field {
                            Some(field) => result.get(field).cloned().unwrap_or(Value::Null),
                            None => result.clone(),
                        };
                        (into.clone(), value)
                    });
                    self.update(id, |i| {
                        i.context.previous_result = Some(result.clone());
                        i.context.awaited = Vec::new();
                        if let Some((name, value)) = &lifted {
                            i.context.vars.insert(name.clone(), value.clone());
                        }
                    });
                }
                Step::Land(land) => {
                    let automated = self
                        .status(id)
                        .is_some_and(|instance| instance.automated_landing_authorized);
                    if self.require_approval_for_landing && !ctx.approval_granted && !automated {
                        return Err(rk_core::Error::other(
                            "land step requires a prior approved human gate or a trusted automated workflow",
                        ));
                    }
                    let branch = interpolate(&land.branch, &ctx);
                    let target = interpolate(&land.target, &ctx);
                    if branch.is_empty() {
                        return Err(rk_core::Error::other(
                            "land step: branch resolved to empty (no branch to land — did an \
                             earlier step set {{ctx.activeBranch}}?)",
                        ));
                    }
                    if target.is_empty() {
                        return Err(rk_core::Error::other("land step: target resolved to empty"));
                    }
                    self.require_allowed_target(&target, repo, automated)?;
                    let result = self
                        .supervisor
                        .land(
                            std::path::Path::new(repo),
                            &branch,
                            &target,
                            land.keep_branch,
                        )
                        .await?;
                    self.update(id, |i| {
                        i.context.previous_result = Some(result.clone());
                        i.context.awaited = Vec::new();
                    });
                }
                Step::OpenPr(open_pr) => {
                    if self.require_approval_for_landing && !ctx.approval_granted {
                        return Err(rk_core::Error::other(
                            "open_pr step requires a prior approved human gate",
                        ));
                    }
                    let branch = interpolate(&open_pr.branch, &ctx);
                    let target = interpolate(&open_pr.target, &ctx);
                    if branch.is_empty() {
                        return Err(rk_core::Error::other(
                            "open_pr step: branch resolved to empty (no branch to open a PR for — \
                             did an earlier step set {{ctx.activeBranch}}?)",
                        ));
                    }
                    if target.is_empty() {
                        return Err(rk_core::Error::other(
                            "open_pr step: target resolved to empty",
                        ));
                    }
                    self.require_allowed_target(&target, repo, false)?;
                    let result = self
                        .supervisor
                        .open_pr(std::path::Path::new(repo), &branch, &target)
                        .await?;
                    self.update(id, |i| {
                        i.context.previous_result = Some(result.clone());
                        i.context.awaited = Vec::new();
                    });
                }
                Step::SubWorkflow(sub) => {
                    let result = self.run_sub_workflow(id, sub, repo, &ctx).await?;
                    return Ok(Flow::NextWithSubworkflowResult(result));
                }
            }
            Ok(Flow::Next)
        })
    }

    /// Run another workflow inline as a step — composition (TKT-57). Resolves and
    /// loads the named definition exactly like a top-level `run` (params
    /// templated from the parent's ctx, then coerced to the child's declared
    /// types), executes it to completion on THIS task (the parent step blocks on
    /// it), and returns the child's final `ctx.previous_result` so the caller can
    /// join it into the parent's context for a following `evaluate`/`when`.
    ///
    /// The child gets its own persisted [`Instance`] and its own
    /// `workflow_complete`/`workflow_failed` event via [`finalize`], so it shows
    /// up in `rk workflow list`/`status` and (on failure) `rk inbox` just like a
    /// directly-run workflow — the one difference being that its result flows
    /// back to a parent. Its budget/agents come from its own definition, so
    /// running B as a sub-step behaves like running B directly.
    ///
    /// Nesting is bounded by [`MAX_SUBWORKFLOW_DEPTH`]: a child one deeper than
    /// its parent, refused fail-closed past the cap. This is the depth analog of
    /// the `repeat` max cap and is what keeps a workflow cycle (A→B→A…) finite.
    /// A child failure is propagated as this step's error (fail-closed).
    async fn run_sub_workflow(
        &self,
        parent_id: &str,
        sub: &SubWorkflowStep,
        repo: &str,
        ctx: &WorkflowContext,
    ) -> rk_core::Result<Value> {
        let parent_depth = self.lock().get(parent_id).map(|i| i.depth).unwrap_or(0);
        let depth = parent_depth + 1;
        if depth > MAX_SUBWORKFLOW_DEPTH {
            return Err(rk_core::Error::other(format!(
                "sub_workflow nesting too deep (depth {depth} > cap {MAX_SUBWORKFLOW_DEPTH}): \
                 refusing to run '{}' — a workflow cycle? (depth guard, the analog of the \
                 repeat max cap)",
                sub.workflow
            )));
        }
        // Repo defaults to the parent's; a child may target another registered
        // repo/path when set.
        let child_repo = sub.repo.clone().unwrap_or_else(|| repo.to_string());
        // Interpolate each param against the parent's ctx, then hand them to the
        // loader as strings — coerced to the child's declared `#Param` types
        // exactly like reactor-templated params (a single `--param k=v` is a
        // string too). Forward a parent param with CUE interpolation in the def.
        let params: HashMap<String, Value> = sub
            .params
            .iter()
            .map(|(k, v)| (k.clone(), Value::String(interpolate(v, ctx))))
            .collect();
        let file = self.find_definition(&sub.workflow, &child_repo)?;
        let definition_digest = definition_digest(&file)?;
        let workflow = rk_workflow::load(&file, &params)?;
        let workflow_name = workflow.name.clone();
        let automated_landing_authorized =
            self.is_automated_landing_definition(&file, &workflow_name);
        let child_id = if let Some(existing) = ctx.active_subworkflow.clone() {
            existing
        } else {
            let child_id = prefixed_id("wf");
            let linked = self.update_with_reason(parent_id, "sub_workflow_linked", |parent| {
                parent.context.active_subworkflow = Some(child_id.clone());
            });
            if !linked {
                return Err(rk_core::Error::other(format!(
                    "could not durably link sub_workflow '{}' to parent {parent_id}",
                    sub.workflow
                )));
            }
            child_id
        };
        let child = Instance {
            id: child_id.clone(),
            workflow: workflow_name.clone(),
            repo: child_repo.clone(),
            coordinator: self.status(parent_id).and_then(|i| i.coordinator),
            schedule: None,
            status: InstanceStatus::Running,
            revision: 0,
            current_step: 0,
            total_steps: workflow.steps.len(),
            context: WorkflowContext::default(),
            error: None,
            awaiting: None,
            instance_max_usd: workflow.budget.map(|b| b.max_usd),
            definition: sub.workflow.clone(),
            definition_digest: definition_digest.clone(),
            automated_landing_authorized,
            params: params.clone(),
            depth,
            started_at: chrono::Utc::now(),
            completed_at: None,
            archived_at: None,
            trigger: None,
            stale_timeout_secs: resolve_stale_timeout_secs(&workflow)?,
        };
        if let Some(existing) = self.store_if_absent(child)? {
            if existing.workflow != workflow_name
                || existing.repo != child_repo
                || existing.definition != sub.workflow
                || existing.definition_digest != definition_digest
                || existing.params != params
                || existing.depth != depth
            {
                if existing.status == InstanceStatus::Running {
                    let mismatch = format!(
                        "linked sub_workflow instance {child_id} does not match '{}'",
                        sub.workflow
                    );
                    if let Err(error) = self.try_update_with_reason(
                        &child_id,
                        "sub_workflow_link_mismatch",
                        |instance| {
                            instance.status = InstanceStatus::Failed;
                            instance.error = Some(mismatch.clone());
                            instance.completed_at = Some(Utc::now());
                        },
                    ) {
                        self.fail_recovery_in_memory(
                            &child_id,
                            format!("linked child mismatch persistence failed: {error}"),
                        );
                        return Err(rk_core::Error::other(format!(
                            "linked sub_workflow instance {child_id} does not match '{}'; failed to persist its fail-closed state: {error}",
                            sub.workflow
                        )));
                    }
                }
                return Err(rk_core::Error::other(format!(
                    "linked sub_workflow instance {child_id} does not match '{}'",
                    sub.workflow
                )));
            }
            match existing.status {
                InstanceStatus::Completed => {
                    return Ok(existing.context.previous_result.unwrap_or(Value::Null));
                }
                InstanceStatus::Failed => {
                    return Err(rk_core::Error::other(format!(
                        "sub_workflow '{}' (instance {child_id}) failed: {}",
                        sub.workflow,
                        existing
                            .error
                            .unwrap_or_else(|| "unknown child failure".into())
                    )));
                }
                InstanceStatus::Running => {}
            }
        }
        info!(parent = %parent_id, child = %child_id, workflow = %workflow_name, depth, "running sub-workflow inline");
        // Execute the child on this task so the parent step joins on it. finalize
        // records the terminal status and emits the child's own completion event,
        // identical to a top-level run.
        match self.execute(&child_id, workflow, &child_repo).await {
            Ok(()) => {
                self.finalize(&child_id, &child_repo, &workflow_name, Ok(()))
                    .await?;
                // The child's final result is this sub_workflow's return value.
                Ok(self
                    .status(&child_id)
                    .and_then(|i| i.context.previous_result)
                    .unwrap_or(Value::Null))
            }
            Err(e) => {
                let msg = e.to_string();
                if let Err(finalize_error) = self
                    .finalize(
                        &child_id,
                        &child_repo,
                        &workflow_name,
                        Err(rk_core::Error::other(msg.clone())),
                    )
                    .await
                {
                    return Err(rk_core::Error::other(format!(
                        "sub_workflow '{}' (instance {child_id}) failed: {msg}; its terminal state also failed to persist: {finalize_error}",
                        sub.workflow
                    )));
                }
                Err(rk_core::Error::other(format!(
                    "sub_workflow '{}' (instance {child_id}) failed: {msg}",
                    sub.workflow
                )))
            }
        }
    }

    /// Enumerate the matching tickets and spawn one agent per ticket in
    /// parallel, returning the fan-out set. The task title defaults to the
    /// ticket id, so the supervisor owns each ticket's status lifecycle exactly
    /// as it does for any ticket-dispatched rat (→ `done` on a clean finish,
    /// → `closed` on merge).
    ///
    /// Each ticket is atomically claimed (`open` → `in_progress`) via
    /// `tickets.claim` *before* its agent spawns, so two concurrent drains
    /// never grab the same ticket — the loser simply skips it (TKT-6). Claiming
    /// before the spawn (rather than after) keeps this write strictly ahead of
    /// the supervisor's fire-and-forget `done`, so it no longer races
    /// completion the way an unordered post-spawn `in_progress` write would.
    async fn fan_out(
        &self,
        id: &str,
        agents: &HashMap<String, AgentProfile>,
        tiers: &TierRouting,
        repo: &str,
        fe: &ForEachStep,
    ) -> rk_core::Result<Vec<FannedAgent>> {
        // Freeze list (R6). The exclusion binds *automated* dispatch, so it is
        // keyed on whether this instance was fired by the scheduler
        // (`Instance.schedule` is `Some` only via `run_scheduled`) rather than
        // on the workflow's name: it is the nightly cadence that regrows frozen
        // mass unattended, not the fan-out shape. An operator running the same
        // definition by hand (`rk workflow run backlog-drain`) is a deliberate
        // act and still fans out over everything ready.
        let scheduled = self.status(id).is_some_and(|i| i.schedule.is_some());
        let items = self.query_tickets(&fe.query, repo, scheduled)?;
        if items.is_empty() {
            // Normal, not a fault: a nightly drain over an empty ready queue is
            // a quiet night. The empty set is still recorded, and the following
            // wait_all/dismiss_all no-op over it (TKT-170).
            info!(instance = %id, "for_each matched no tickets; nothing to fan out");
        }
        // The workflow's own tier rules shadow the global ones for this fan-out.
        let routing = tiers.chained(&self.tier_routing);
        let ctx = self.context(id);
        // The per-instance cap is static for the run; spent is recomputed live
        // in the supervisor per spawn, so later fan-out spawns are refused once
        // earlier ones have burned the instance past its cap.
        let instance_cap = self.instance_budget(id);
        let mut fanned = Vec::with_capacity(items.len());
        for item in items {
            // Atomically claim the ticket before spawning. If a concurrent drain
            // already claimed it, we lose the race and skip it, so one ticket is
            // never dispatched to two rats.
            if !self.tickets.claim(&item.id).await? {
                info!(instance = %id, ticket = %item.id, "ticket already claimed; skipping");
                continue;
            }
            // Route this ticket to a cost tier from its labels/priority. The tier
            // is an agent profile that resolves just below inline overrides.
            let tier = routing.route(&item.labels, Some(&item.priority));
            if let Some(tier) = tier {
                info!(instance = %id, ticket = %item.id, tier, "routed ticket to cost tier");
            }
            let resolved = resolve_fields(
                fe.agent.as_deref(),
                tier,
                fe.harness.as_deref(),
                fe.model.as_deref(),
                fe.permission_mode.as_deref(),
                agents,
                &self.global_agents,
                &self.default_harness,
            )?;
            let title = interpolate_item(&fe.task.title, &item, &ctx);
            let prompt = fe
                .task
                .description
                .as_ref()
                .map(|d| interpolate_item(d, &item, &ctx));
            let params = SpawnParams {
                repo: repo.to_string(),
                task: title,
                prompt,
                role: fe.role.clone(),
                harness: Some(resolved.harness),
                parent: None,
                // Each rat gets its own branch off the base; fan-out never
                // chains onto ctx.active_branch (that would serialize them).
                base: fe.branch.clone(),
                model: resolved.model,
                permission_mode: resolved.permission_mode,
                attach: false,
                workflow_instance: Some(id.to_string()),
                coordinator: self.coordinator(id),
                instance_max_usd: instance_cap,
                coordination: None,
            };
            // Route through the same fleet-WIP admission/retry path as
            // `Step::Spawn` (TKT-01M036NWE1EW5B1PWSHK0MKX8E rework 2): a
            // refusal here retries under poll rather than erroring the whole
            // fan-out, and the ticket claimed above simply sits `in_progress`
            // across the wait — it is not released and cannot be double-claimed
            // by a concurrent drain in the meantime.
            self.await_fleet_capacity(id).await;
            let record = loop {
                match self.spawn_agent(params.clone(), self.fleet_wip_cap).await {
                    Ok(record) => break record,
                    Err(e) if is_fleet_wip_refusal(&e) => {
                        self.update(id, |i| i.awaiting = Some("fleet_wip".to_string()));
                        tokio::time::sleep(FLEET_CAPACITY_POLL).await;
                    }
                    Err(e) => return Err(e),
                }
            };
            self.update(id, |i| i.awaiting = None);
            fanned.push(FannedAgent {
                agent: record.name.clone(),
                branch: record.branch.clone(),
                ticket: Some(item.id),
                spawn: record.spawn,
            });
        }
        Ok(fanned)
    }

    /// The predicate a `wait`/`wait_all` blocks on: THIS generation of `agent`
    /// reporting its `harness_result`.
    ///
    /// The agent name alone is not enough. `harness_result` events are durable
    /// and outlive the rat they name forever, so a bare `"agent":"<name>"`
    /// search matches a PREDECESSOR of the same name and satisfies the wait in
    /// milliseconds. That is TKT-146: TKT-136 briefly let an archived name be
    /// reused, the wait returned a two-day-old namesake's tuple, the following
    /// `evaluate` judged a stranger's work, and the `dismiss` behind it killed
    /// a rat one second into its task (SIGTERM, so `code None`, no session,
    /// zero tokens). Whole workflows reported success having done nothing.
    ///
    /// `reserve_name` no longer recycles names, so the collision should not
    /// arise — but a `wait` that can be satisfied by a tuple predating the rat
    /// it waits on is wrong on its own terms. Bounding the read below the
    /// agent record's `created_at` makes the predicate generation-exact and
    /// keeps it correct however the naming policy moves.
    ///
    /// TKT-159: the bound is now unconditional. It previously degraded to an
    /// UNBOUNDED read when the agent's registry record was unreachable, which
    /// left the exact defect this method exists to prevent live on that path.
    /// [`generation_floor`](Self::generation_floor) now always yields a valid
    /// bound, so there is no case in which a `wait` can match a namesake.
    ///
    /// TKT-160: the generation floor is necessary but NOT sufficient. It
    /// separates generations; it does not separate the TURNS within one, and a
    /// harness reports a result per turn — so this read used to be satisfied by
    /// a mid-flight "tests still running" turn milliseconds after the rat
    /// started. That is fixed on the producer side (a generation now publishes
    /// exactly one `harness_result`, the one it finished on — see
    /// `Supervisor::claim_completion`), because `wait` is not the only reader:
    /// the reactor's steward trigger and the ticket auto-close read the same
    /// event. Do not reintroduce a per-turn `harness_result`.
    ///
    /// Generation-identity migration (consumer B1,
    /// `docs/2026-08-17-tkt-c1-generation-identity.md`): every `harness_result`
    /// now carries `spawn` (`Supervisor::route_completion`), so a reachable
    /// registry record with a minted `SpawnId` keys the read on
    /// [`Pattern::for_spawn`] instead — an equality predicate that cannot match
    /// a namesake regardless of timing, no floor required. Falls back to the
    /// name+floor predicate only for a record with no minted id (unreachable,
    /// or written before this migration).
    fn result_pattern(&self, id: &str, agent: &str) -> Pattern {
        if let Some(spawn) = self.supervisor.status(agent).and_then(|r| r.spawn) {
            return Pattern::for_spawn(Category::Event, "harness_result", spawn);
        }
        Pattern::for_agent_since(
            Category::Event,
            "harness_result",
            agent,
            self.generation_floor(id, agent),
        )
    }

    /// The instant a waited-on agent's `harness_result` provably postdates.
    ///
    /// The agent record's own `created_at` is the exact answer. When no record
    /// is reachable — it was removed, or the registry file was replaced under a
    /// resumed instance — fall back to when THIS workflow instance started:
    /// every agent a `wait`/`wait_all` blocks on was spawned by this instance
    /// (`ctx.active_agent` is only set by `spawn`, `ctx.fanout` only by
    /// `for_each`), so its result cannot predate the instance. That makes the
    /// fallback a sound lower bound rather than no bound at all — never too
    /// tight to miss the real tuple, and still tight enough to exclude every
    /// namesake predecessor from before the run.
    fn generation_floor(&self, id: &str, agent: &str) -> DateTime<Utc> {
        let record_created_at = self.supervisor.status(agent).map(|r| r.created_at);
        if record_created_at.is_none() {
            warn!(
                agent,
                instance = id,
                "no registry record for waited-on agent; falling back to the instance start"
            );
        }
        let instance_started_at = self.lock().get(id).map(|i| i.started_at);
        generation_floor_of(record_created_at, instance_started_at, Utc::now())
    }

    /// The liveness assertion under every result a workflow acts on (TKT-147):
    /// `Some(diagnostic)` when `agent` did NOT reach a verdict of its own
    /// through the harness, so nothing attributed to it can be trusted.
    ///
    /// A workflow's gates unify against whatever landed in `previous_result`
    /// and have no notion of "this rat never really ran". That is how TKT-146
    /// stayed silent: a rat was SIGTERMed one second in — no session, zero
    /// tokens, `process exited (code None) without completing` — yet the chain
    /// evaluated clean and reported `Completed`, and nightly-self-improve
    /// looked green for two runs while grooming nothing. Fixing *why* that rat
    /// was killed did not teach the chain to notice, so any future path that
    /// kills or crashes a rat could still be reported as a clean run. A silent
    /// no-op is the worst failure mode for a self-driving loop; this makes it
    /// a failure that lands in `rk inbox`.
    ///
    /// Two ways to fail the assertion:
    ///  - the record is still live, so whatever we are holding cannot have come
    ///    from it (`harness_result` is emitted only *after* the record goes
    ///    terminal, so a running agent has not produced one);
    ///  - the record is terminal but crashed out of its run
    ///    ([`crashed_without_reporting`]), so no result of its own exists.
    ///
    /// A missing record degrades to a pass with a warning, exactly like
    /// [`result_pattern`](Self::result_pattern): a read-side check must not be
    /// the thing that fails a live workflow.
    ///
    /// [`crashed_without_reporting`]: crate::agents::AgentRecord::crashed_without_reporting
    fn liveness_failure(&self, agent: &str) -> Option<String> {
        let Some(record) = self.supervisor.status(agent) else {
            warn!(agent, "no record for waited-on agent; liveness unchecked");
            return None;
        };
        if record.state.is_live() {
            return Some(format!(
                "agent {agent} is still {:?}: a result attributed to it cannot have come from it \
                 (it reports only once it finishes)",
                record.state
            ));
        }
        if record.crashed_without_reporting() {
            return Some(format!(
                "agent {agent} never reported a result of its own — it ended {:?} after burning \
                 {} tokens, with the harness never reporting: {}. Whatever is in \
                 ctx.previousResult did not come from this rat; treating it as its work would \
                 report a no-op as success (`rk log {agent}`)",
                record.state,
                record.usage.total(),
                record
                    .result
                    .as_deref()
                    .unwrap_or("no result recorded")
                    .trim(),
            ));
        }
        None
    }

    /// Whether a `wait` on `agent` can no longer be satisfied: it left the
    /// fleet without reporting and nothing will bring it back, so blocking to
    /// the step's timeout only delays a failure that is already certain.
    ///
    /// Deliberately narrow. `Orphaned` is excluded even though it is terminal:
    /// its worktree/branch/session are preserved precisely so `rk respawn` (or
    /// the sweep) can pick it up, and an operator who does that inside the
    /// step's timeout heals the run. A crashed (`Failed`) agent is likewise
    /// still revivable while the self-healing sweep is armed and has not yet
    /// hit its crash-loop cap.
    fn abandoned(&self, agent: &str) -> Option<String> {
        let record = self.supervisor.status(agent)?;
        if record.state == AgentState::Orphaned || !record.crashed_without_reporting() {
            return None;
        }
        if record.state == AgentState::Failed
            && self.respawn_enabled
            && !self.supervisor.respawn_exhausted(agent)
        {
            return None; // the self-healing sweep may still bring it back
        }
        Some(format!(
            "agent {agent} left the fleet without reporting ({:?}: {}) — no harness_result will \
             ever arrive, so this wait can only fail (`rk log {agent}`, then `rk respawn {agent}`)",
            record.state,
            record
                .result
                .as_deref()
                .unwrap_or("no result recorded")
                .trim(),
        ))
    }

    /// Block for `timeout` on `agent`'s own `harness_result`, giving up early if
    /// the agent crashes out of its run in the meantime (TKT-147). Returns the
    /// result payload, or an error naming why no result is coming.
    async fn await_result(
        &self,
        id: &str,
        agent: &str,
        deadline: tokio::time::Instant,
        step: &str,
        timeout: &str,
    ) -> rk_core::Result<Value> {
        let pattern = self.result_pattern(id, agent);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(rk_core::Error::other(format!(
                    "{step} timed out after {timeout} waiting on agent {agent}"
                )));
            }
            let slice = remaining.min(LIVENESS_POLL);
            if let Some(tuple) = self
                .space
                .rd(&pattern, slice)
                .await
                .map_err(|e| rk_core::Error::other(format!("{step} failed: {e}")))?
            {
                // The result is this generation's by construction (the pattern
                // is floored at the record's own created_at); the liveness gate
                // is the belt to that braces, covering the degraded unbounded
                // read and any future path that lands a foreign result here.
                if let Some(why) = self.liveness_failure(agent) {
                    return Err(rk_core::Error::other(format!("{step} failed: {why}")));
                }
                return Ok(tuple.payload);
            }
            if let Some(why) = self.abandoned(agent) {
                return Err(rk_core::Error::other(format!("{step} failed: {why}")));
            }
        }
    }

    /// Block until every fanned-out agent has emitted its `harness_result`,
    /// then aggregate into `{count, ok, errors, all_ok, results}`. All agents
    /// share one deadline: the step times out if any is still running when it
    /// elapses.
    ///
    /// `fanout` is `None` only when no `for_each` ran before this step — an
    /// authoring error, and the one case that fails here. An *empty* fan-out is
    /// not: a `for_each` whose query matched no tickets is a quiet night, and it
    /// joins to the vacuous aggregate (`count: 0, all_ok: true`) so the rest of
    /// the instance runs and the night completes instead of landing in
    /// `rk inbox` as a failure with nothing to look at (TKT-170).
    async fn join(
        &self,
        id: &str,
        fanout: Option<&[FannedAgent]>,
        wait_all: &WaitAllStep,
    ) -> rk_core::Result<Value> {
        let fanout = fanout.ok_or_else(|| {
            rk_core::Error::other(
                "wait_all step with no preceding for_each: there is no fan-out to join",
            )
        })?;
        if fanout.is_empty() {
            info!(instance = %id, "wait_all over an empty fan-out; nothing to join");
        }
        let deadline = tokio::time::Instant::now() + parse_duration(&wait_all.timeout)?;
        let mut results = Vec::with_capacity(fanout.len());
        for fa in fanout {
            // Same generation-exact predicate and same liveness gate as `wait`:
            // one crashed rat fails the join rather than being counted as a
            // clean member of the batch.
            results.push(
                self.await_result(id, &fa.agent, deadline, "wait_all", &wait_all.timeout)
                    .await?,
            );
        }
        let ok = results
            .iter()
            .filter(|r| r.get("is_error").and_then(Value::as_bool) == Some(false))
            .count();
        let count = results.len();
        Ok(json!({
            "count": count,
            "ok": ok,
            "errors": count - ok,
            "all_ok": ok == count,
            "results": results,
        }))
    }

    /// Dismiss every agent in the fan-out set in parallel — the fan-out
    /// counterpart to a single `dismiss` over `active_agent`. Each agent is
    /// merged (unless `no_merge`) and cleaned up concurrently, then the caller
    /// clears the fan-out set. Aggregates into `{count, merged, parked, errors,
    /// all_merged, results}`. A hard dismiss failure (e.g. a git error — a
    /// merge *conflict* is a clean `merged: false`, not an error) fails the
    /// step, symmetric to how `wait_all` fails on a timeout.
    ///
    /// When `dismiss_all.only_clean` is set, this reads the preceding
    /// `wait_all` aggregate (`previous_result`) and merges *only* the branches
    /// of rats that finished clean (`is_error: false`), parking every failed
    /// rat's branch with `no_merge` for review instead of failing the whole
    /// batch. A branch parked because its rat failed is counted in `parked`
    /// (distinct from a `merged: false` merge *conflict*), and `all_merged`
    /// stays `merged == count`, so a following `evaluate {all_merged: true}`
    /// still surfaces the failure in `rk inbox` — but only after the clean
    /// branches have already merged. `only_clean` requires a preceding
    /// `wait_all` (its per-agent results supply the clean/failed signal); it
    /// fails the step if none is present rather than silently merging all.
    ///
    /// As with [`join`](Self::join), only a missing `for_each` (`None`) fails
    /// here; an empty fan-out merges nothing and aggregates to `count: 0,
    /// all_merged: true` (TKT-170). The `only_clean` check still runs first, so
    /// a `dismiss_all` that wants a `wait_all` it never got is caught on a quiet
    /// night too, rather than lying dormant until a night with tickets in it.
    async fn dismiss_fanout(
        &self,
        fanout: Option<&[FannedAgent]>,
        dismiss_all: &DismissAllStep,
        previous_result: Option<&Value>,
    ) -> rk_core::Result<Value> {
        let fanout = fanout.ok_or_else(|| {
            rk_core::Error::other(
                "dismiss_all step with no preceding for_each: there is no fan-out to merge",
            )
        })?;
        // With only_clean, the per-agent no_merge is driven by the preceding
        // wait_all's results: an agent is parked (no_merge=true) unless its
        // harness_result reported is_error:false. Without a preceding wait_all
        // there is no clean/failed signal, so the flag is meaningless — fail
        // rather than silently merge everything.
        let clean = if dismiss_all.only_clean {
            let agg = previous_result.ok_or_else(|| {
                rk_core::Error::other(
                    "dismiss_all onlyClean requires a preceding wait_all: no aggregate in \
                     ctx.previous_result to determine which rats finished clean",
                )
            })?;
            let results = agg
                .get("results")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    rk_core::Error::other(
                    "dismiss_all onlyClean requires a preceding wait_all: ctx.previous_result has \
                     no `results` array (is the previous step a wait_all?)",
                )
                })?;
            let clean: std::collections::HashSet<String> = results
                .iter()
                .filter(|r| r.get("is_error").and_then(Value::as_bool) == Some(false))
                .filter_map(|r| r.get("agent").and_then(Value::as_str).map(str::to_string))
                .collect();
            Some(clean)
        } else {
            None
        };
        if fanout.is_empty() {
            info!("dismiss_all over an empty fan-out; nothing to merge");
        }
        // Dismiss all branches concurrently: each dismissal kills its child and
        // merges its branch independently, so serializing them would waste the
        // whole point of a fan-out.
        let mut set = tokio::task::JoinSet::new();
        for fa in fanout {
            let supervisor = Arc::clone(&self.supervisor);
            let agent = fa.agent.clone();
            let spawn = fa.spawn;
            // Base no_merge from the step, plus: under only_clean, park (don't
            // merge) any agent not in the clean set.
            let parked = clean
                .as_ref()
                .is_some_and(|clean| !clean.contains(&fa.agent));
            let no_merge = dismiss_all.no_merge || parked;
            set.spawn(async move {
                let outcome = supervisor.dismiss_checked(&agent, spawn, no_merge).await;
                (agent, parked, outcome)
            });
        }
        let count = fanout.len();
        let mut results = Vec::with_capacity(count);
        let mut merged = 0usize;
        let mut parked = 0usize;
        let mut failures = Vec::new();
        while let Some(joined) = set.join_next().await {
            let (agent, was_parked, outcome) = joined
                .map_err(|e| rk_core::Error::other(format!("dismiss_all task join error: {e}")))?;
            match outcome {
                Ok(value) => {
                    if value.get("merged").and_then(Value::as_bool) == Some(true) {
                        merged += 1;
                    } else if was_parked {
                        // Held back because the rat failed, not because the
                        // branch would not merge — track it separately so a
                        // following evaluate/report can tell the two apart.
                        parked += 1;
                    }
                    results.push(value);
                }
                Err(e) => {
                    failures.push(format!("{agent}: {e}"));
                    results.push(json!({"agent": agent, "error": e.to_string()}));
                }
            }
        }
        if !failures.is_empty() {
            return Err(rk_core::Error::other(format!(
                "dismiss_all failed for {} of {count} agents: {}",
                failures.len(),
                failures.join("; ")
            )));
        }
        Ok(json!({
            "count": count,
            "merged": merged,
            "parked": parked,
            "errors": count - merged,
            "all_merged": merged == count,
            "results": results,
        }))
    }

    /// Execute a `run` step's command in the active agent's worktree — the
    /// deterministic quality gate. Where `evaluate` unifies only against the
    /// harness's self-reported output (it takes the rat's word), this runs the
    /// repo's real checks and captures `{exit, stdout, stderr}` into a value
    /// for `ctx.previous_result`, so a following `evaluate {expect: {exit: 0}}`
    /// (or a `when`) gates the merge on a verdict the runner cannot forge.
    ///
    /// Fail-closed: a spawn failure, a timeout (the child is killed on drop),
    /// or an `expect_exit` mismatch all return an `Err` that fails the instance
    /// rather than letting a red — or hung — suite slip through.
    async fn run_command(
        &self,
        id: &str,
        ctx: &WorkflowContext,
        run: &RunStep,
        repo: &str,
    ) -> rk_core::Result<Value> {
        let agent = ctx
            .active_agent
            .clone()
            .ok_or_else(|| rk_core::Error::other("run step with no active agent"))?;
        let record = self.supervisor.status(&agent).ok_or_else(|| {
            rk_core::Error::other(format!("run step: no record for agent {agent}"))
        })?;
        let worktree = record.worktree.ok_or_else(|| {
            rk_core::Error::other(format!("run step: agent {agent} has no worktree"))
        })?;
        // Resolve the effective command, cwd, expect_exit, and timeout from
        // either a repo-registered named check or a raw inline command — the
        // latter gated fail-closed by the require_named_checks policy (TKT-30).
        let resolved = self.resolve_run(run, repo)?;
        // Resolve cwd relative to the worktree root; interpolation is allowed,
        // but absolute paths, `..`, and symlinks that leave the worktree are not.
        let dir = resolve_worktree_cwd(&worktree, resolved.cwd.as_deref(), ctx)?;
        let command = interpolate(&resolved.command, ctx);
        let timeout = parse_duration(&resolved.timeout)?;
        for name in run.env.keys() {
            if !valid_check_env_name(name) {
                return Err(rk_core::Error::other(format!(
                    "run step: environment name '{name}' is not allowed; use RK_CHECK_*"
                )));
            }
        }
        let env: Vec<(String, String)> = run
            .env
            .iter()
            .map(|(name, value)| (name.clone(), interpolate(value, ctx)))
            .collect();

        self.run_check_in(
            id,
            repo,
            &agent,
            &dir,
            &command,
            &resolved,
            &env,
            timeout,
            ctx.previous_result.as_ref(),
        )
        .await
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
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn run_check_in(
        &self,
        id: &str,
        repo: &str,
        agent: &str,
        dir: &Path,
        command: &str,
        resolved: &ResolvedRun,
        env: &[(String, String)],
        timeout: Duration,
        previous_result: Option<&Value>,
    ) -> rk_core::Result<Value> {
        // Serialize this check's entire run (every retry attempt) against
        // every other same-repo check also opted into `sharedCargoTarget`,
        // when `[disk] shared_cargo_target` actually has agents sharing one
        // CARGO_TARGET_DIR per repo (TKT-01M0CFA1RX36SJ7DV4YWGHQ9BT). Held
        // for the rest of this function's scope — including every early
        // return below — and dropped on exit, so the next queued check only
        // ever proceeds once this one is fully done touching the shared dir.
        // Bounded by this check's own timeout: if the queue is deep enough
        // that a check cannot even START within its own declared budget,
        // that is as good as it failing outright — fail closed rather than
        // let the wait grow unbounded.
        let _test_exec_guard = if resolved.shared_cargo_target
            && self.supervisor.shared_cargo_target_enabled()
        {
            match tokio::time::timeout(timeout, self.supervisor.acquire_test_exec_lock(repo)).await
            {
                Ok(guard) => Some(guard),
                Err(_) => {
                    let stderr = format!(
                        "run step: `{command}` did not acquire the shared CARGO_TARGET_DIR \
                         test-execution lock for repo `{repo}` within {timeout:?} — queued \
                         behind other same-repo checks that also set sharedCargoTarget"
                    );
                    self.record_gate_failure(
                        id,
                        repo,
                        agent,
                        command,
                        LOCK_TIMEOUT_EXIT,
                        "fail",
                        false,
                        "",
                        false,
                        &stderr,
                        false,
                        &[],
                    );
                    return Err(rk_core::Error::other(stderr));
                }
            }
        } else {
            None
        };

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
        let mut settled: Option<(i64, String, bool, String, bool, bool, &'static str)> = None;
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
                ) = decode_run_outcome(retry_outcome, command, resolved);
            }
            // The routable three-way summary. `exit` alone cannot express it:
            // a suite may exit 124 on its own, and "did not finish" calls for
            // a different hand-off than "finished and said no".
            let verdict: &'static str = if timed_out {
                "timeout"
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
                    &stdout,
                    stdout_truncated,
                    &stderr,
                    stderr_truncated,
                    &history,
                );
                return Err(rk_core::Error::other(stderr));
            }
            if verdict == "pass" || attempt == attempts {
                settled = Some((
                    exit,
                    stdout,
                    stdout_truncated,
                    stderr,
                    stderr_truncated,
                    timed_out,
                    verdict,
                ));
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
        let (exit, stdout, stdout_truncated, stderr, stderr_truncated, timed_out, verdict) =
            settled.expect("run step: attempt loop always settles by the final attempt");
        info!(agent = %agent, exit, timed_out, verdict, command = %command, retries = history.len(), "run step completed");
        let mut result = json!({
            "exit": exit,
            "stdout": stdout,
            "stdout_truncated": stdout_truncated,
            "stderr": stderr,
            "stderr_truncated": stderr_truncated,
            "timed_out": timed_out,
            "verdict": verdict,
        });

        if !history.is_empty() {
            result["retries"] = json!(history);
        }

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
        let mut child_command = tokio::process::Command::new("sh");
        child_command
            .arg("-c")
            .arg(command)
            .current_dir(dir)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            // Kill the suite if the timeout below drops the wait future, so a
            // hung check leaves no orphan behind.
            .kill_on_drop(true)
            // Its own process group (mirroring rk-harness's launcher): lets
            // `ProcessGroupGuard` in `collect_child_output` reach every
            // descendant this check spawns (mise/cargo/rustc under `sh -c`),
            // not just the `sh` wrapper `kill_on_drop` kills on its own.
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
            for name in [
                "RK_AGENT",
                "RK_TASK",
                "RK_REPO",
                "RK_ROLE",
                "RK_HOME",
                "RK_BRANCH",
                "RK_WORKTREE",
                "RK_AUTH_TOKEN",
            ] {
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
        let child = child_command.spawn().map_err(|e| {
            rk_core::Error::other(format!("run step: failed to spawn `{command}`: {e}"))
        })?;

        collect_child_output(child, timeout, command).await
    }

    /// Persist a bounded, durable `(artifact, <repo>, gate-failure)` tuple for
    /// a failed (or timed-out) `run` step. Without this, the only trace of a
    /// gate's own verdict is `ctx.previous_result`, which the very next `run`
    /// step (an escalation check, a `steward-report-gate-failure`) overwrites
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
            "stdout_tail": bounded_tail(stdout, GATE_EVIDENCE_LIMIT),
            "stdout_truncated": stdout_truncated,
            "stderr_tail": bounded_tail(stderr, GATE_EVIDENCE_LIMIT),
            "stderr_truncated": stderr_truncated,
            "failing_tests": failing_tests,
            "retries": history,
        });
        let _ = self.space.out(rk_core::tuple::Tuple::new(
            Category::Artifact,
            repo_name_of(repo),
            "gate-failure",
            "daemon",
            payload,
        ));
    }

    fn require_allowed_target(
        &self,
        target: &str,
        repo: &str,
        automated: bool,
    ) -> rk_core::Result<()> {
        if automated {
            // `Repo::discover` shells out to git; `run_step` runs this
            // synchronously inside its own async future, so keep the
            // subprocess off the worker thread the same way
            // `Supervisor::diff_summary_for` does. The flavor check (rather
            // than an unconditional `block_in_place`) is needed because
            // `#[tokio::test]` defaults to a current-thread runtime, where
            // `block_in_place` panics.
            let on_multithread = tokio::runtime::Handle::try_current()
                .map(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
                .unwrap_or(false);
            let repo_path = Path::new(repo);
            let discovered = if on_multithread {
                tokio::task::block_in_place(|| rk_git::Repo::discover(repo_path))
            } else {
                rk_git::Repo::discover(repo_path)
            };
            if let Ok(git_repo) = discovered {
                let registry_path = self.layout.home().join("repos.json");
                if let Ok(registry) = crate::repos::RepoRegistry::load(&registry_path) {
                    if let Some(approved) = registry
                        .get_by_path(git_repo.root())
                        .and_then(|record| record.activated_policy.as_ref())
                    {
                        let policy_target = approved.policy.delivery.target.as_str();
                        if policy_target == "agent-base" || policy_target == target {
                            return Ok(());
                        }
                        return Err(rk_core::Error::other(format!(
                            "workflow target '{target}' does not match activated repository policy target '{policy_target}'"
                        )));
                    }
                }
            }
        }
        if self
            .allowed_target_branches
            .iter()
            .any(|allowed| allowed == target)
        {
            return Ok(());
        }
        Err(rk_core::Error::other(format!(
            "workflow target '{target}' is not authorized by the activated repository policy or policy.allowed_target_branches"
        )))
    }

    /// Resolve a `run` step to its effective command, cwd, exit gate, and
    /// timeout — enforcing the named-check policy (TKT-30).
    ///
    /// A step names EITHER a raw `command` OR a repo-registered `check`, never
    /// both and never neither. A `check` is looked up in `<repo>/.rk/checks.cue`
    /// (the repo owner's allowlist); its command/cwd/expectExit/timeout supply
    /// the defaults, with the step's own `cwd`/`expectExit`/`timeout` (when set)
    /// taking precedence. A raw `command` is refused fail-closed when the
    /// `require_named_checks` policy is on, so a compromised workflow definition
    /// cannot run arbitrary shell — only the checks the repo registered.
    fn resolve_run(&self, run: &RunStep, repo: &str) -> rk_core::Result<ResolvedRun> {
        // Parsed before either arm so an unknown value is rejected even for a
        // step that would never have timed out — an authoring error should
        // surface on the first run, not on the first slow day.
        //
        // `on_timeout` is deliberately step-only and never inherited from a
        // named check: a check owns WHAT to run and how long to allow, but what
        // a blown budget MEANS is the workflow's routing decision, and the
        // routing (`into`/`when`) lives in the workflow too.
        let on_timeout = OnTimeout::parse(&run.on_timeout)?;
        // Defense-in-depth alongside the schema.cue bound (`retryOnFail: int &
        // >=0 & <=20`): fail closed rather than let an over-cap value reach
        // `resolved.retry_on_fail + 1` unbounded (TKT-01M02QT9KTDY2CN6YJEVP3VCF8).
        validate_retry_on_fail(run.retry_on_fail)?;
        match (&run.command, &run.check) {
            (Some(_), Some(_)) => Err(rk_core::Error::other(
                "run step: set exactly one of `command` or `check`, not both",
            )),
            (None, None) => Err(rk_core::Error::other(
                "run step: set one of `command` (raw) or `check` (named)",
            )),
            (Some(command), None) => {
                if self.require_named_checks {
                    return Err(rk_core::Error::other(
                        "run step: raw `command` refused by policy (require_named_checks); \
                         reference a named `check` registered in <repo>/.rk/checks.cue",
                    ));
                }
                Ok(ResolvedRun {
                    command: command.clone(),
                    cwd: run.cwd.clone(),
                    expect_exit: run.expect_exit,
                    timeout: run.timeout.clone(),
                    on_timeout,
                    environment_policy: rk_workflow::CheckEnvironmentPolicy::Inherit,
                    retry_on_fail: run.retry_on_fail,
                    // A raw command is an unvetted workflow-def string, never
                    // the repo's own registered check — it never opts into
                    // the shared target-dir lock.
                    shared_cargo_target: false,
                })
            }
            (None, Some(name)) => {
                let check = self.find_check(repo, name)?;
                // Step-level overrides win over the check's own defaults; the
                // step's timeout only overrides when it is non-default (a check
                // gets to set its own bound without every referencing step
                // having to restate it).
                let timeout = if run.timeout == DEFAULT_RUN_TIMEOUT {
                    check
                        .timeout
                        .clone()
                        .unwrap_or_else(|| DEFAULT_RUN_TIMEOUT.to_string())
                } else {
                    run.timeout.clone()
                };
                Ok(ResolvedRun {
                    command: check.command,
                    cwd: run.cwd.clone().or(check.cwd),
                    expect_exit: run.expect_exit.or(check.expect_exit),
                    timeout,
                    on_timeout,
                    environment_policy: check.environment_policy,
                    retry_on_fail: run.retry_on_fail,
                    shared_cargo_target: check.shared_cargo_target,
                })
            }
        }
    }

    /// Look up a named check in the repo's registry (`<repo>/.rk/checks.cue`).
    /// Fails closed: a missing registry, an unparseable one, or an unknown name
    /// all error rather than silently running nothing.
    fn find_check(&self, repo: &str, name: &str) -> rk_core::Result<rk_workflow::Check> {
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

    /// Resolve a fan-out ticket query to a bounded list of items in the
    /// workflow's own repo scope. `status: "ready"` uses dependency-aware
    /// readiness; any other value is a literal status filter.
    ///
    /// `exclude_frozen` drops tickets tagged to a frozen subsystem (R6). It is
    /// applied *before* `limit`, so a run of frozen tickets at the head of the
    /// queue cannot silently eat the fan-out budget and turn a busy night into
    /// a no-op — the limit bounds work dispatched, not tickets inspected.
    fn query_tickets(
        &self,
        query: &TicketQuery,
        repo: &str,
        exclude_frozen: bool,
    ) -> rk_core::Result<Vec<TicketItem>> {
        let scope = Some(repo_name_of(repo));
        let tuples = if query.status == "ready" {
            self.tickets.ready(scope)?
        } else {
            self.tickets.list(scope, Some(query.status.clone()), None)?
        };
        Ok(tuples
            .into_iter()
            .filter(|t| {
                if !exclude_frozen {
                    return true;
                }
                let frozen = rk_core::freeze::blocks_automated_dispatch(&string_array(
                    &t.payload, "labels",
                ));
                if frozen {
                    info!(ticket = %t.identity, "scheduled fan-out skipped ticket tagged to a frozen subsystem");
                }
                !frozen
            })
            .take(query.limit)
            .map(|t| TicketItem {
                id: t.identity.clone(),
                title: field(&t.payload, "title"),
                body: field(&t.payload, "body"),
                priority: field(&t.payload, "priority"),
                labels: string_array(&t.payload, "labels"),
            })
            .collect())
    }

    pub fn list(&self) -> Vec<Instance> {
        let mut all: Vec<Instance> = self.lock().values().cloned().collect();
        all.sort_by_key(|i| i.started_at);
        all
    }

    /// How many `Running` instances a `#Trigger` named `trigger` currently has
    /// in flight — the count the reactor checks against that trigger's
    /// `maxInFlight` cap. Reads the live (rehydrated-on-restart) instance
    /// store directly, so it is correct immediately after a daemon restart
    /// without depending on the reactor's own ephemeral fire markers.
    pub fn live_count_for_trigger(&self, trigger: &str) -> usize {
        self.lock()
            .values()
            .filter(|i| {
                i.status == InstanceStatus::Running && i.trigger.as_deref() == Some(trigger)
            })
            .count()
    }

    /// Pruned instances only, oldest first.
    pub fn list_archived(&self) -> Vec<Instance> {
        let mut all: Vec<Instance> = self.lock_archived().values().cloned().collect();
        all.sort_by_key(|i| i.started_at);
        all
    }

    /// Live + archived, oldest first — the full run history. An id in both
    /// stores (the crash window) yields the live copy only.
    pub fn list_all(&self) -> Vec<Instance> {
        let live = self.lock();
        let mut all: Vec<Instance> = live.values().cloned().collect();
        all.extend(
            self.lock_archived()
                .values()
                .filter(|i| !live.contains_key(&i.id))
                .cloned(),
        );
        drop(live);
        all.sort_by_key(|i| i.started_at);
        all
    }

    pub fn status(&self, id: &str) -> Option<Instance> {
        self.lock().get(id).cloned()
    }

    /// Live snapshot for `id`, falling back to the archived one.
    ///
    /// Read-only callers (`rk workflow status`/`timeline`) use this so a pruned
    /// run's history stays readable. Every mutation path deliberately keeps
    /// using [`status`](WorkflowEngine::status), so an archived instance reads
    /// as "no such instance" until it is explicitly unarchived.
    pub fn status_any(&self, id: &str) -> Option<Instance> {
        self.status(id)
            .or_else(|| self.lock_archived().get(id).cloned())
    }

    /// Terminal instances this selection would archive, oldest first.
    ///
    /// [`Selection::Ids`] is strict: an unknown id, or one still `Running`, is
    /// an error, so a targeted `rk workflow prune <id>` never silently
    /// no-ops. [`Selection::Before`] is lenient by construction — it only ever
    /// names rows it found.
    pub fn archivable(&self, selection: &Selection) -> rk_core::Result<Vec<Instance>> {
        let instances = self.lock();
        let mut eligible: Vec<Instance> = match selection {
            Selection::Before(cutoff) => instances
                .values()
                .filter(|i| i.status != InstanceStatus::Running && settled_at(i) < *cutoff)
                .cloned()
                .collect(),
            Selection::Ids(ids) => {
                let mut picked = Vec::new();
                for id in ids {
                    let Some(instance) = instances.get(id) else {
                        // Lock order: `instances` is already held; `archived`
                        // is only ever taken after it.
                        let already = self.lock_archived().contains_key(id);
                        return Err(rk_core::Error::other(if already {
                            format!("workflow instance {id} is already archived")
                        } else {
                            format!("no such workflow instance: {id}")
                        }));
                    };
                    if instance.status == InstanceStatus::Running {
                        return Err(rk_core::Error::other(format!(
                            "workflow instance {id} is still running (step {}/{}) — \
                             let it settle, or reject its gate, before pruning it",
                            instance.current_step, instance.total_steps
                        )));
                    }
                    picked.push(instance.clone());
                }
                picked
            }
        };
        drop(instances);
        eligible.sort_by_key(|i| i.started_at);
        eligible.dedup_by(|a, b| a.id == b.id);
        Ok(eligible)
    }

    /// Move every [`archivable`](WorkflowEngine::archivable) instance into the
    /// archive store, returning them as archived (with `archived_at` stamped).
    ///
    /// Every archive file is written BEFORE any live file is removed: a crash
    /// part-way leaves those instances in both stores, which
    /// [`rehydrate`](WorkflowEngine::rehydrate) resolves in favour of the live
    /// copy — the pass no-ops rather than losing a run, and re-running it is
    /// idempotent.
    pub fn archive(&self, selection: &Selection) -> rk_core::Result<Vec<Instance>> {
        let now = Utc::now();
        let mut moved: Vec<Instance> = self.archivable(selection)?;
        if moved.is_empty() {
            return Ok(Vec::new());
        }
        let archive_dir = self.archive_dir();
        for instance in &mut moved {
            instance.archived_at = Some(now);
        }
        let live_dir = self.instances_dir();
        {
            // Reserve every stable id and serialize the archive files themselves
            // across both maps. Otherwise overlapping first-time prune requests
            // can both observe no archive file; one failed writer may then remove
            // the other request's committed snapshot during rollback.
            let mut live = self.lock();
            let mut archived = self.lock_archived();
            let mut originals = Vec::with_capacity(moved.len());
            for instance in &moved {
                let current = live.get(&instance.id).ok_or_else(|| {
                    rk_core::Error::other(format!(
                        "workflow instance {} changed while it was being archived",
                        instance.id
                    ))
                })?;
                if current.revision != instance.revision
                    || current.status == InstanceStatus::Running
                {
                    return Err(rk_core::Error::other(format!(
                        "workflow instance {} changed while it was being archived",
                        instance.id
                    )));
                }
                originals.push(current.clone());
            }

            // Archive copies are durable before any live snapshot is removed.
            // Holding both map locks makes this a single-writer transition for
            // every selected stable id, including the atomic writer's rollback.
            for instance in &moved {
                self.persist_to(&archive_dir, instance)?;
            }

            for original in &originals {
                let path = live_dir.join(format!("{}.json", original.id));
                if let Err(remove_error) = remove_snapshot_durably(&path) {
                    let rollback_errors: Vec<String> = originals
                        .iter()
                        .filter_map(|snapshot| {
                            self.persist_to(&live_dir, snapshot)
                                .err()
                                .map(|error| format!("restore {} failed: {error}", snapshot.id))
                        })
                        .collect();
                    return Err(rk_core::Error::other(if rollback_errors.is_empty() {
                        format!("could not durably remove live workflow snapshot: {remove_error}")
                    } else {
                        format!(
                            "could not durably remove live workflow snapshot: {remove_error}; rollback failed: {}",
                            rollback_errors.join("; ")
                        )
                    }));
                }
            }
            for instance in &moved {
                live.remove(&instance.id);
                archived.insert(instance.id.clone(), instance.clone());
            }
        }
        info!(count = moved.len(), "archived terminal workflow instances");
        Ok(moved)
    }

    /// Restore one archived instance to the live store — the undo for
    /// [`archive`](WorkflowEngine::archive). `Ok(None)` means no such archived
    /// instance; an id a live instance already holds is a real collision, not a
    /// no-op, and errors.
    pub fn unarchive(&self, id: &str) -> rk_core::Result<Option<Instance>> {
        let mut live = self.lock();
        let mut archived = self.lock_archived();
        if live.contains_key(id) {
            return Err(rk_core::Error::other(format!(
                "cannot unarchive {id}: a live instance already holds that id"
            )));
        }
        let Some(mut instance) = archived.get(id).cloned() else {
            return Ok(None);
        };
        let archived_snapshot = instance.clone();
        instance.archived_at = None;
        // Live file first: a crash before the archive file is removed leaves
        // the instance in both stores, where the live copy wins — never in
        // neither.
        let live_dir = self.instances_dir();
        let archive_dir = self.archive_dir();
        let live_path = live_dir.join(format!("{id}.json"));
        let archive_path = archive_dir.join(format!("{id}.json"));
        self.persist_to(&live_dir, &instance)?;
        if let Err(remove_error) = remove_snapshot_durably(&archive_path) {
            if let Err(rollback_error) = self.persist_to(&archive_dir, &archived_snapshot) {
                return Err(rk_core::Error::other(format!(
                    "could not durably remove archived workflow snapshot: {remove_error}; rollback failed: {rollback_error}; live recovery copy retained"
                )));
            }
            if let Err(rollback_error) = remove_snapshot_durably(&live_path) {
                return Err(rk_core::Error::other(format!(
                    "could not durably remove archived workflow snapshot: {remove_error}; rollback failed: {rollback_error}"
                )));
            }
            return Err(remove_error);
        }
        live.insert(id.to_string(), instance.clone());
        archived.remove(id);
        info!(instance = id, "unarchived workflow instance");
        Ok(Some(instance))
    }

    /// The instance plus its labelled step trace, for `rk workflow timeline`:
    /// every step of the definition rendered as a row so the CLI can mark
    /// done/current/pending against the persisted `current_step` cursor.
    /// `None` rows = the definition no longer loads (file moved or deleted
    /// since launch); the CLI then falls back to bare step numbers.
    pub fn timeline(&self, id: &str) -> Option<(Instance, Option<Vec<TimelineRow>>)> {
        let instance = self.status_any(id)?;
        let rows = self
            .find_definition(&instance.definition, &instance.repo)
            .ok()
            .and_then(|file| rk_workflow::load(&file, &instance.params).ok())
            .map(|workflow| timeline_rows(&workflow.steps));
        Some((instance, rows))
    }

    /// Record a human approval decision for a parked instance. Writes a
    /// `workflow_approval` event that an approval gate blocked on this instance
    /// is waiting to read. Idempotent from the caller's view: the first
    /// decision to reach the blocked gate wins.
    pub fn approve(
        &self,
        instance_id: &str,
        approved: bool,
        by: &str,
        reason: Option<String>,
    ) -> rk_core::Result<()> {
        let instance = self.status(instance_id).ok_or_else(|| {
            rk_core::Error::other(format!("no such workflow instance: {instance_id}"))
        })?;
        let payload = json!({
            "instance": instance_id,
            "step": instance.current_step,
            "approved": approved,
            "by": by,
            "reason": reason,
        });
        self.space.out(rk_core::tuple::Tuple::new(
            Category::Event,
            repo_name_of(&instance.repo),
            "workflow_approval",
            by.to_string(),
            payload,
        ))?;
        info!(instance = %instance_id, approved, by = %by, "workflow approval recorded");
        Ok(())
    }

    fn context(&self, id: &str) -> WorkflowContext {
        self.lock()
            .get(id)
            .map(|i| i.context.clone())
            .unwrap_or_default()
    }

    /// `fleet_wip_cap` is the ceiling this spawn must be atomically admitted
    /// against (0 = none, used by `for_each` fan-out — see its call site).
    /// The caller must be prepared to retry on
    /// [`is_fleet_wip_refusal`]: a refusal means the fleet was already full
    /// at the moment [`Supervisor::spawn`](crate::supervisor::Supervisor::spawn)
    /// checked, atomically with reserving the slot — not that this step
    /// failed.
    async fn spawn_agent(
        &self,
        params: SpawnParams,
        fleet_wip_cap: usize,
    ) -> rk_core::Result<crate::agents::AgentRecord> {
        self.supervisor.spawn_async(params, fleet_wip_cap).await
    }

    /// Every live agent fleet-wide, regardless of what spawned it — the same
    /// tally [`Drain::run_cycle_at`](crate::drain::Drain::run_cycle_at) counts
    /// against `[drain] max_wip`. Sharing this count is what makes the fleet
    /// WIP ceiling bidirectional: a drain refill already skips spawning once
    /// workflow-spawned agents fill the cap, and a workflow `spawn` step now
    /// waits its turn under the exact same number instead of dispatching
    /// unbounded.
    ///
    /// This is a snapshot, not a reservation — used only to decide whether
    /// [`await_fleet_capacity`](Self::await_fleet_capacity) should short-circuit
    /// or start polling. The authoritative, TOCTOU-safe check is
    /// `Registry::try_reserve_wip`, taken atomically inside
    /// [`Supervisor::spawn`](crate::supervisor::Supervisor::spawn).
    fn live_fleet_count(&self) -> usize {
        self.supervisor
            .list()
            .iter()
            .filter(|r| r.state.is_live())
            .count()
    }

    /// Best-effort wait for the fleet-wide WIP ceiling to look free before a
    /// `spawn` step even tries — cheap, and avoids paying for spawn-param
    /// construction and repo discovery just to be refused. `fleet_wip_cap ==
    /// 0` (the default, and drain's own "disabled" value) means no ceiling —
    /// returns immediately, matching pre-admission-control behaviour. Polls
    /// rather than occupying a thread: this instance's execution runs in its
    /// own task, so a wait here never blocks any other instance's steps or
    /// the daemon's RPC loop.
    ///
    /// NOT authoritative: this snapshot can go stale between here and the
    /// actual spawn attempt (another admitter can claim the slot in between),
    /// which is why the spawn call itself re-checks atomically and the caller
    /// loops on [`is_fleet_wip_refusal`] rather than trusting this alone.
    async fn await_fleet_capacity(&self, id: &str) {
        if self.fleet_wip_cap == 0 || self.live_fleet_count() < self.fleet_wip_cap {
            return;
        }
        self.update(id, |i| i.awaiting = Some("fleet_wip".to_string()));
        while self.live_fleet_count() >= self.fleet_wip_cap {
            tokio::time::sleep(FLEET_CAPACITY_POLL).await;
        }
        self.update(id, |i| i.awaiting = None);
    }

    /// This instance's per-run budget cap (from the workflow's `budget:`), used
    /// as the dispatch preflight ceiling on every spawn it makes.
    pub(crate) fn instance_budget(&self, id: &str) -> Option<f64> {
        self.lock().get(id).and_then(|i| i.instance_max_usd)
    }

    pub(crate) fn coordinator(&self, id: &str) -> Option<String> {
        self.lock().get(id).and_then(|i| i.coordinator.clone())
    }

    fn store_if_absent(&self, instance: Instance) -> rk_core::Result<Option<Instance>> {
        let mut instances = self.lock();
        if let Some(existing) = instances.get(&instance.id) {
            return Ok(Some(existing.clone()));
        }
        if let Some(existing) = self.lock_archived().get(&instance.id) {
            return Ok(Some(existing.clone()));
        }
        instances.insert(instance.id.clone(), instance.clone());
        if let Err(error) = self.persist(&instance) {
            instances.remove(&instance.id);
            return Err(error);
        }
        self.emit_state_event(&instance, "started");
        Ok(None)
    }

    fn update<F: FnOnce(&mut Instance)>(&self, id: &str, mutate: F) {
        self.update_with_reason(id, "state_changed", mutate);
    }

    fn update_with_reason<F: FnOnce(&mut Instance)>(
        &self,
        id: &str,
        reason: &str,
        mutate: F,
    ) -> bool {
        match self.try_update_with_reason(id, reason, mutate) {
            Ok(changed) => changed,
            Err(error) => {
                warn!(instance = %id, %error, "failed to persist workflow state; skipping coordinator event");
                false
            }
        }
    }

    fn try_update_with_reason<F: FnOnce(&mut Instance)>(
        &self,
        id: &str,
        reason: &str,
        mutate: F,
    ) -> rk_core::Result<bool> {
        let mut instances = self.lock();
        if let Some(instance) = instances.get_mut(id) {
            let before = instance.clone();
            mutate(instance);
            if *instance == before {
                return Ok(false);
            }
            instance.revision = instance.revision.saturating_add(1);
            let snapshot = instance.clone();
            if let Err(error) = self.persist(&snapshot) {
                *instance = before;
                return Err(error);
            }
            self.emit_state_event(&snapshot, reason);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn fail_recovery_in_memory(&self, id: &str, detail: String) {
        if let Some(instance) = self.lock().get_mut(id) {
            mark_recovery_failure_in_memory(instance, &detail);
        }
    }

    /// Publish a compact, durable coordinator transition after the current
    /// workflow snapshot has been persisted. The snapshot remains the recovery
    /// source if the event write fails; callers never infer a false state from
    /// a notification that was not durably accepted.
    fn emit_state_event(&self, instance: &Instance, reason: &str) {
        let payload = json!({
            "instance": instance.id,
            "workflow": instance.workflow,
            "repo": repo_name_of(&instance.repo),
            "coordinator": instance.coordinator,
            "revision": instance.revision,
            "reason": reason,
            "route": if instance.status.is_terminal() { "terminal" } else { "rollup" },
            "severity": if instance.status.is_terminal() { "info" } else { "debug" },
            "summary": format!("workflow {:?} at step {}/{}", instance.status, instance.current_step, instance.total_steps),
            "status": instance.status,
            "current_step": instance.current_step,
            "total_steps": instance.total_steps,
            "awaiting": instance.awaiting,
            "active_agent": instance.context.active_agent,
            "active_branch": instance.context.active_branch,
            "awaited": instance.context.awaited,
            "error": instance.error.as_deref().map(|error| error.chars().take(512).collect::<String>()),
        });
        if let Err(error) = self.space.out_coordinator(
            Tuple::new(
                Category::Event,
                repo_name_of(&instance.repo),
                "workflow_state_changed",
                "daemon",
                payload,
            )
            .with_lifecycle(rk_core::tuple::Lifecycle::Furniture),
        ) {
            warn!(
                instance = %instance.id,
                error = %error,
                "failed to emit workflow coordinator state event"
            );
        }
    }

    fn instances_dir(&self) -> PathBuf {
        self.layout.home().join(INSTANCE_DIR)
    }

    fn archive_dir(&self) -> PathBuf {
        self.layout.home().join(INSTANCE_ARCHIVE_DIR)
    }

    fn persist(&self, instance: &Instance) -> rk_core::Result<()> {
        self.persist_to(&self.instances_dir(), instance)
    }

    fn persist_to(&self, dir: &Path, instance: &Instance) -> rk_core::Result<()> {
        let path = dir.join(format!("{}.json", instance.id));
        let data = serde_json::to_vec_pretty(instance)?;
        persist_bytes_atomically(&path, &data)
    }

    fn record_persistence_failure(&self, path: &Path, error: String) {
        let _ = self.space.out(
            rk_core::tuple::Tuple::new(
                Category::Obstacle,
                SYSTEM_SCOPE,
                "workflow_persistence_corrupt",
                "daemon",
                json!({"path": path, "error": error}),
            )
            .into_trail(DEFAULT_TRAIL_TTL),
        );
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Instance>> {
        match self.instances.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    /// Guard on the archive store. Never taken before [`lock`](Self::lock) —
    /// see the lock-order note on `WorkflowEngine::archived`.
    fn lock_archived(&self) -> std::sync::MutexGuard<'_, HashMap<String, Instance>> {
        match self.archived.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }
}

fn complete_top_level_step(
    instance: &mut Instance,
    index: usize,
    clear_subworkflow: bool,
    subworkflow_result: Option<Value>,
) {
    instance.current_step = index + 1;
    if let Some(result) = subworkflow_result {
        instance.context.previous_result = Some(result);
        instance.context.awaited = Vec::new();
    }
    if clear_subworkflow {
        instance.context.active_subworkflow = None;
    }
}

fn join_nested_subworkflow_result(instance: &mut Instance, result: Value) {
    instance.context.previous_result = Some(result);
    instance.context.awaited = Vec::new();
    // Keep active_subworkflow until the enclosing top-level cursor advances.
    // That durable link is what lets a restart rejoin the completed child.
}

fn require_persisted_transition(
    result: rk_core::Result<bool>,
    id: &str,
    transition: &str,
) -> rk_core::Result<()> {
    match result {
        Ok(true) => Ok(()),
        Ok(false) => Err(rk_core::Error::other(format!(
            "workflow {id} {transition} was not persisted"
        ))),
        Err(error) => Err(rk_core::Error::other(format!(
            "workflow {id} {transition} was not persisted: {error}"
        ))),
    }
}

fn mark_recovery_failure_in_memory(instance: &mut Instance, detail: &str) {
    instance.status = InstanceStatus::Failed;
    instance.error = Some(format!(
        "fail-closed recovery state was not durably recorded: {detail}"
    ));
    instance.completed_at = Some(Utc::now());
}

/// Replace one workflow snapshot durably.
///
/// The temporary file is synced before rename, then the parent directory is
/// synced so both file contents and the directory entry survive a crash.
fn persist_bytes_atomically(path: &Path, data: &[u8]) -> rk_core::Result<()> {
    persist_bytes_atomically_with_sync(path, data, &mut sync_directory)
}

fn remove_snapshot_durably(path: &Path) -> rk_core::Result<()> {
    remove_snapshot_durably_with_sync(path, &mut sync_directory)
}

fn remove_snapshot_durably_with_sync<F>(path: &Path, sync: &mut F) -> rk_core::Result<()>
where
    F: FnMut(&Path) -> rk_core::Result<()>,
{
    let dir = path
        .parent()
        .ok_or_else(|| rk_core::Error::other("workflow snapshot path has no parent"))?;
    std::fs::remove_file(path)?;
    sync(dir)
}

fn persist_bytes_atomically_with_sync<F>(
    path: &Path,
    data: &[u8],
    sync: &mut F,
) -> rk_core::Result<()>
where
    F: FnMut(&Path) -> rk_core::Result<()>,
{
    let dir = path
        .parent()
        .ok_or_else(|| rk_core::Error::other("workflow snapshot path has no parent"))?;
    let dir_was_missing = !dir.exists();
    std::fs::create_dir_all(dir)?;
    if dir_was_missing {
        let parent = dir
            .parent()
            .ok_or_else(|| rk_core::Error::other("workflow snapshot directory has no parent"))?;
        sync(parent)?;
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| rk_core::Error::other("workflow snapshot path has no file name"))?;
    let sequence = PERSIST_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp = dir.join(format!("{file_name}.tmp-{}-{sequence}", std::process::id()));
    let backup = dir.join(format!(
        "{file_name}.backup-{}-{sequence}",
        std::process::id()
    ));
    let result = (|| -> rk_core::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp)?;
        file.write_all(data)?;
        file.sync_all()?;
        let had_previous = path.exists();
        if had_previous {
            std::fs::hard_link(path, &backup)?;
            sync(dir)?;
        }
        std::fs::rename(&tmp, path)?;
        if let Err(commit_error) = sync(dir) {
            let rollback = if had_previous {
                restore_snapshot_from_backup_with(path, &backup, sync, &mut |from, to| {
                    std::fs::rename(from, to).map_err(rk_core::Error::from)
                })
            } else {
                std::fs::remove_file(path)
                    .map_err(rk_core::Error::from)
                    .and_then(|()| sync(dir))
            };
            if let Err(rollback_error) = rollback {
                return Err(rk_core::Error::other(format!(
                    "workflow snapshot commit failed: {commit_error}; rollback failed: {rollback_error}; recovery backup retained at {}",
                    backup.display()
                )));
            }
            if had_previous {
                let _ = std::fs::remove_file(&backup);
                let _ = sync(dir);
            }
            return Err(commit_error);
        }
        if had_previous {
            let _ = std::fs::remove_file(&backup);
            let _ = sync(dir);
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

fn restore_snapshot_from_backup_with<F, R>(
    path: &Path,
    backup: &Path,
    sync: &mut F,
    replace: &mut R,
) -> rk_core::Result<()>
where
    F: FnMut(&Path) -> rk_core::Result<()>,
    R: FnMut(&Path, &Path) -> rk_core::Result<()>,
{
    let dir = path
        .parent()
        .ok_or_else(|| rk_core::Error::other("workflow snapshot path has no parent"))?;
    let backup_name = backup
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| rk_core::Error::other("workflow backup path has no file name"))?;
    let restore = backup.with_file_name(format!("{backup_name}.restore"));
    std::fs::hard_link(backup, &restore)?;
    if let Err(error) = replace(&restore, path) {
        let _ = std::fs::remove_file(&restore);
        return Err(error);
    }
    sync(dir)
}

#[cfg(unix)]
fn sync_directory(dir: &Path) -> rk_core::Result<()> {
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_dir: &Path) -> rk_core::Result<()> {
    Ok(())
}

/// A ticket flattened into the fields a fan-out task template can bind, plus the
/// `labels`/`priority` a tier-routing rule keys on.
struct TicketItem {
    id: String,
    title: String,
    body: String,
    priority: String,
    labels: Vec<String>,
}

fn field(payload: &Value, key: &str) -> String {
    payload
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn string_array(payload: &Value, key: &str) -> Vec<String> {
    payload
        .get(key)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Interpolate a fan-out task template: `{{ctx.*}}` first, then the per-ticket
/// `{{item.id}}` / `{{item.title}}` / `{{item.body}}` placeholders.
fn interpolate_item(text: &str, item: &TicketItem, ctx: &WorkflowContext) -> String {
    interpolate(text, ctx)
        .replace("{{item.id}}", &item.id)
        .replace("{{item.title}}", &item.title)
        .replace("{{item.body}}", &item.body)
}

/// Keep named-check inputs in a data-only namespace. In particular, a workflow
/// must not be able to replace PATH/BASH_ENV/loader hooks or forge RK_AGENT.
fn valid_check_env_name(name: &str) -> bool {
    let Some(suffix) = name.strip_prefix("RK_CHECK_") else {
        return false;
    };
    !suffix.is_empty()
        && suffix
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

/// PATH for a run-step child: the daemon executable's directory first, then
/// the daemon's inherited PATH. `None` only when the exe location is unknown
/// and there is no inherited PATH to preserve.
fn check_child_path(
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
fn prior_gate_failure(previous: Option<&Value>) -> String {
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
/// artifact ([`record_gate_failure`](WorkflowEngine::record_gate_failure),
/// [`GATE_EVIDENCE_LIMIT`]) — the artifact keeps a longer tail because it is
/// the only copy that outlives the next workflow step.
fn bounded_tail(text: &str, limit: usize) -> String {
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
fn check_failure_detail(stdout: &str, stderr: &str) -> String {
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
const MAX_FAILING_TESTS: usize = 50;

fn extract_failing_tests(stdout: &str) -> Vec<String> {
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

/// Replace `{{ctx.*}}` placeholders in workflow strings at execution time.
fn interpolate(text: &str, ctx: &WorkflowContext) -> String {
    let previous = ctx
        .previous_result
        .as_ref()
        .map(|v| {
            v["result"]
                .as_str()
                .map(String::from)
                .unwrap_or_else(|| v.to_string())
        })
        .unwrap_or_default();
    let mut out = text
        .replace(
            "{{ctx.activeAgent}}",
            ctx.active_agent.as_deref().unwrap_or(""),
        )
        .replace(
            "{{ctx.activeBranch}}",
            ctx.active_branch.as_deref().unwrap_or(""),
        )
        .replace("{{ctx.previousResult}}", &previous);
    // `read`-lifted variables: {{ctx.var.<name>}}.
    for (name, value) in &ctx.vars {
        out = out.replace(&format!("{{{{ctx.var.{name}}}}}"), &value_as_key(value));
    }
    out
}

/// Render a ctx variable as a plain string for `when`-case matching and
/// interpolation: strings pass through, null becomes empty, anything else is
/// its compact JSON form.
fn value_as_key(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn definition_digest(path: &Path) -> rk_core::Result<String> {
    let data = std::fs::read(path)?;
    Ok(hex::encode(Sha256::digest(data)))
}

/// Pick the lower bound for a generation-exact `harness_result` read, given
/// what is still known about the waited-on agent. Pure so the choice is
/// testable without a live supervisor; see
/// [`WorkflowEngine::generation_floor`].
///
/// Ordered by how tight a bound each source gives, and every arm returns SOME
/// instant — there is deliberately no "unbounded" result. That is the TKT-159
/// fix: this decision used to fall through to no bound at all when the agent
/// record was missing, which reinstated the TKT-146 defect (a `wait` satisfied
/// by a namesake predecessor's two-day-old tuple) on exactly that path.
fn generation_floor_of(
    record_created_at: Option<DateTime<Utc>>,
    instance_started_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> DateTime<Utc> {
    // 1. The agent record's own birth: exact, and the normal case.
    // 2. The instance's start: every waited-on agent was spawned by this
    //    instance, so its result cannot predate the run. Looser but sound.
    // 3. `now`: nothing is known. Cannot admit an older namesake's tuple, which
    //    is the failure mode that kills a live rat. The cost is a wait that
    //    times out if the result already landed — fail toward waiting, never
    //    toward a stranger's record.
    record_created_at.or(instance_started_at).unwrap_or(now)
}

fn repo_name_of(repo: &str) -> String {
    PathBuf::from(repo)
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "repo".into())
}

/// Crate-scoped alongside [`WorkflowEngine::run_check_in`]: the T2 landing
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

/// Resolve a workflow's `staleTimeout:` override (strategic review B8) into
/// seconds at launch time, once, rather than re-parsing `definition` on every
/// sweep pass. `None` when the workflow declares no override — the sweep then
/// falls back to its configured `default_timeout_secs`. A malformed override
/// fails the launch immediately (via `?` at the call site) rather than being
/// silently ignored until the sweep would have needed it 12 hours later.
fn resolve_stale_timeout_secs(workflow: &Workflow) -> rk_core::Result<Option<u64>> {
    workflow
        .stale_timeout
        .as_deref()
        .map(|s| parse_duration(s).map(|d| d.as_secs()))
        .transpose()
}

/// One row of an instance's rendered step trace. `index` is the TOP-LEVEL
/// step index the row belongs to — the executor's `current_step` cursor only
/// counts top-level steps, so nested rows (a `when` case body, a `repeat`
/// body) carry their parent's index and a deeper `depth` for indentation.
#[derive(Debug, Clone, Serialize)]
pub struct TimelineRow {
    pub index: usize,
    pub depth: usize,
    pub label: String,
}

/// Flatten a workflow's steps into labelled timeline rows, recursing into
/// `when`/`repeat` bodies with increased depth.
fn timeline_rows(steps: &[Step]) -> Vec<TimelineRow> {
    let mut rows = Vec::new();
    for (index, step) in steps.iter().enumerate() {
        flatten_step(&mut rows, index, 0, step);
    }
    rows
}

fn flatten_step(rows: &mut Vec<TimelineRow>, index: usize, depth: usize, step: &Step) {
    rows.push(TimelineRow {
        index,
        depth,
        label: step_label(step),
    });
    match step {
        Step::When(when) => {
            // HashMap order is nondeterministic; sort so the trace is stable.
            let mut cases: Vec<_> = when.cases.iter().collect();
            cases.sort_by(|a, b| a.0.cmp(b.0));
            for (value, body) in cases {
                rows.push(TimelineRow {
                    index,
                    depth: depth + 1,
                    label: format!("case {value}:"),
                });
                for s in body {
                    flatten_step(rows, index, depth + 2, s);
                }
            }
            if !when.default.is_empty() {
                rows.push(TimelineRow {
                    index,
                    depth: depth + 1,
                    label: "default:".into(),
                });
                for s in &when.default {
                    flatten_step(rows, index, depth + 2, s);
                }
            }
        }
        Step::Repeat(repeat) => {
            for s in &repeat.steps {
                flatten_step(rows, index, depth + 1, s);
            }
        }
        _ => {}
    }
}

fn step_contains_subworkflow(step: &Step, child: &str) -> bool {
    match step {
        Step::SubWorkflow(sub) => sub.workflow == child,
        Step::When(when) => when
            .cases
            .values()
            .flat_map(|steps| steps.iter())
            .chain(when.default.iter())
            .any(|step| step_contains_subworkflow(step, child)),
        Step::Repeat(repeat) => repeat
            .steps
            .iter()
            .any(|step| step_contains_subworkflow(step, child)),
        _ => false,
    }
}

/// Short human label for one step, mirroring the CUE field names an operator
/// wrote in the definition.
fn step_label(step: &Step) -> String {
    match step {
        Step::Spawn(s) => format!("spawn {} — \"{}\"", s.role, s.task.title),
        Step::Wait(w) => format!("wait for result ({})", w.timeout),
        Step::Evaluate(e) => {
            if e.any_of.is_empty() {
                format!("evaluate expect {}", e.expect)
            } else {
                format!("evaluate expect {} (+{} anyOf)", e.expect, e.any_of.len())
            }
        }
        Step::Dismiss(d) => {
            if d.no_merge {
                "dismiss (no merge)".into()
            } else {
                "dismiss (merge)".into()
            }
        }
        Step::Gate(g) => match (&g.duration, &g.timeout) {
            (Some(d), _) => format!("gate {} ({d})", g.gate_type),
            (None, Some(t)) => format!("gate {} (timeout {t})", g.gate_type),
            (None, None) => format!("gate {}", g.gate_type),
        },
        Step::Read(r) => {
            let field = r
                .field
                .as_deref()
                .map(|f| format!(".{f}"))
                .unwrap_or_default();
            format!("read {}/{}{} → {}", r.category, r.identity, field, r.into)
        }
        Step::When(w) => format!("when {}", w.var),
        Step::Repeat(r) => format!("repeat ×{}", r.max),
        Step::Break => "break".into(),
        Step::Stop(s) => match &s.reason {
            Some(reason) => format!("stop — {reason}"),
            None => "stop".into(),
        },
        Step::ForEach(f) => format!(
            "for_each {} tickets (≤{}) → spawn {}",
            f.query.status, f.query.limit, f.role
        ),
        Step::WaitAll(w) => format!("wait_all ({})", w.timeout),
        Step::DismissAll(d) => {
            let mut label = String::from("dismiss_all");
            if d.no_merge {
                label.push_str(" (no merge)");
            } else if d.only_clean {
                label.push_str(" (only clean)");
            }
            label
        }
        Step::Run(r) => match (&r.check, &r.command) {
            (Some(check), _) => format!("run check:{check}"),
            (None, Some(command)) => format!("run `{command}`"),
            (None, None) => "run".into(),
        },
        Step::Land(l) => format!("land {} → {}", l.branch, l.target),
        Step::OpenPr(p) => format!("open_pr {} → {}", p.branch, p.target),
        Step::SubWorkflow(s) => format!("sub_workflow {}", s.workflow),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rk_core::id::RecordId;
    use rk_workflow::DismissStep;

    #[test]
    fn atomic_persist_replaces_file_without_temporary_artifacts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("instance.json");

        persist_bytes_atomically(&path, b"first").unwrap();
        persist_bytes_atomically(&path, b"second").unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), b"second");
        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(entries, vec![std::ffi::OsString::from("instance.json")]);
    }

    #[test]
    fn failed_directory_sync_after_rename_leaves_no_rehydratable_initial_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rejected.json");
        let mut syncs = 0;

        let result = persist_bytes_atomically_with_sync(&path, b"running", &mut |_| {
            syncs += 1;
            if syncs == 1 {
                Err(rk_core::Error::other("injected directory sync failure"))
            } else {
                Ok(())
            }
        });

        assert!(result.is_err());
        assert!(
            !path.exists(),
            "a caller-rejected initial snapshot must not be resumed after restart"
        );
    }

    #[test]
    fn failed_directory_sync_after_replacement_restores_previous_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("instance.json");
        std::fs::write(&path, b"previous").unwrap();
        let mut syncs = 0;

        let result = persist_bytes_atomically_with_sync(&path, b"replacement", &mut |_| {
            syncs += 1;
            if syncs == 2 {
                Err(rk_core::Error::other("injected replacement sync failure"))
            } else {
                Ok(())
            }
        });

        assert!(result.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"previous");
    }

    #[test]
    fn failed_rollback_sync_preserves_backup_and_reports_indeterminate_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("instance.json");
        std::fs::write(&path, b"previous").unwrap();
        let mut syncs = 0;

        let error = persist_bytes_atomically_with_sync(&path, b"replacement", &mut |_| {
            syncs += 1;
            if syncs >= 2 {
                Err(rk_core::Error::other(format!(
                    "injected sync failure {syncs}"
                )))
            } else {
                Ok(())
            }
        })
        .unwrap_err();

        assert!(error.to_string().contains("rollback"));
        assert_eq!(std::fs::read(&path).unwrap(), b"previous");
        assert!(
            std::fs::read_dir(dir.path()).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .contains(".backup-")
            }),
            "a failed rollback durability sync must retain the old snapshot backup"
        );
    }

    #[test]
    fn snapshot_removal_requires_parent_directory_sync() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("instance.json");
        std::fs::write(&path, b"snapshot").unwrap();
        let mut synced = false;

        let error = remove_snapshot_durably_with_sync(&path, &mut |parent| {
            synced = true;
            assert_eq!(parent, dir.path());
            Err(rk_core::Error::other("injected removal sync failure"))
        })
        .unwrap_err();

        assert!(synced);
        assert!(error.to_string().contains("injected removal sync failure"));
        assert!(!path.exists());
    }

    #[test]
    fn subworkflow_completion_advances_cursor_and_clears_link_in_one_snapshot() {
        let mut instance = Instance {
            id: "parent".into(),
            workflow: "parent".into(),
            repo: "/repo".into(),
            coordinator: None,
            schedule: None,
            status: InstanceStatus::Running,
            revision: 0,
            current_step: 0,
            total_steps: 1,
            context: WorkflowContext {
                active_subworkflow: Some("child".into()),
                ..Default::default()
            },
            error: None,
            awaiting: None,
            instance_max_usd: None,
            definition: "parent".into(),
            definition_digest: String::new(),
            automated_landing_authorized: false,
            params: HashMap::new(),
            depth: 0,
            started_at: Utc::now(),
            completed_at: None,
            archived_at: None,
            trigger: None,
            stale_timeout_secs: None,
        };

        complete_top_level_step(&mut instance, 0, true, Some(json!({"joined": true})));

        assert_eq!(instance.current_step, 1);
        assert_eq!(instance.context.active_subworkflow, None);
        assert_eq!(
            instance.context.previous_result,
            Some(json!({"joined": true}))
        );
    }

    #[test]
    fn nested_subworkflow_result_keeps_link_until_top_level_cursor_advances() {
        let mut instance = Instance {
            id: "parent".into(),
            workflow: "parent".into(),
            repo: "/repo".into(),
            coordinator: None,
            schedule: None,
            status: InstanceStatus::Running,
            revision: 0,
            current_step: 0,
            total_steps: 1,
            context: WorkflowContext {
                active_subworkflow: Some("child".into()),
                ..Default::default()
            },
            error: None,
            awaiting: None,
            instance_max_usd: None,
            definition: "parent".into(),
            definition_digest: String::new(),
            automated_landing_authorized: false,
            params: HashMap::new(),
            depth: 0,
            started_at: Utc::now(),
            completed_at: None,
            archived_at: None,
            trigger: None,
            stale_timeout_secs: None,
        };

        join_nested_subworkflow_result(&mut instance, json!({"joined": true}));
        assert_eq!(instance.current_step, 0);
        assert_eq!(
            instance.context.active_subworkflow.as_deref(),
            Some("child")
        );

        complete_top_level_step(&mut instance, 0, true, None);
        assert_eq!(instance.current_step, 1);
        assert_eq!(instance.context.active_subworkflow, None);
        assert_eq!(
            instance.context.previous_result,
            Some(json!({"joined": true}))
        );
    }

    #[test]
    fn terminal_persistence_failure_is_returned_to_the_joining_parent() {
        let error = require_persisted_transition(
            Err(rk_core::Error::other(
                "injected terminal persistence failure",
            )),
            "child",
            "terminal state",
        )
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("injected terminal persistence failure"));
        assert!(error.to_string().contains("child"));
    }

    #[test]
    fn failed_backup_restore_keeps_both_canonical_and_recovery_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("instance.json");
        let backup = dir.path().join("instance.json.backup");
        std::fs::write(&path, b"replacement").unwrap();
        std::fs::write(&backup, b"previous").unwrap();

        let error =
            restore_snapshot_from_backup_with(&path, &backup, &mut |_| Ok(()), &mut |_, _| {
                Err(rk_core::Error::other("injected restore failure"))
            })
            .unwrap_err();

        assert!(error.to_string().contains("injected restore failure"));
        assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
        assert_eq!(std::fs::read(&backup).unwrap(), b"previous");
    }

    #[test]
    fn recovery_persistence_failure_marks_the_in_memory_instance_non_resumable() {
        let mut instance = Instance {
            id: "child".into(),
            workflow: "child".into(),
            repo: "/repo".into(),
            coordinator: None,
            schedule: None,
            status: InstanceStatus::Running,
            revision: 0,
            current_step: 0,
            total_steps: 1,
            context: WorkflowContext::default(),
            error: None,
            awaiting: None,
            instance_max_usd: None,
            definition: "child".into(),
            definition_digest: String::new(),
            automated_landing_authorized: false,
            params: HashMap::new(),
            depth: 1,
            started_at: Utc::now(),
            completed_at: None,
            archived_at: None,
            trigger: None,
            stale_timeout_secs: None,
        };

        mark_recovery_failure_in_memory(&mut instance, "injected recovery persistence failure");

        assert_eq!(instance.status, InstanceStatus::Failed);
        assert!(instance
            .error
            .as_deref()
            .unwrap()
            .contains("not durably recorded"));
        assert!(instance.completed_at.is_some());
    }

    #[test]
    fn new_snapshot_directory_must_be_synced_before_installing_a_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("new-instances").join("instance.json");

        let result = persist_bytes_atomically_with_sync(&path, b"running", &mut |_| {
            Err(rk_core::Error::other("injected parent sync failure"))
        });

        assert!(result.is_err());
        assert!(!path.exists());
    }

    #[test]
    fn interpolate_replaces_ctx_placeholders() {
        let ctx = WorkflowContext {
            active_agent: Some("Whisker".into()),
            active_branch: Some("rat/whisker/t1".into()),
            previous_result: Some(json!({"result": "looks good", "is_error": false})),
            ..Default::default()
        };
        let text = "Review {{ctx.activeBranch}} by {{ctx.activeAgent}}: {{ctx.previousResult}}";
        assert_eq!(
            interpolate(text, &ctx),
            "Review rat/whisker/t1 by Whisker: looks good"
        );
    }

    /// The steward escalation checks (`rk out need`, `rk ticket new`) run in
    /// the daemon's inherited environment, which may not contain the rk binary
    /// directory at all — the daemon is auto-started by whatever client first
    /// connects. The child PATH must therefore always lead with the daemon's
    /// own executable directory so checks resolve the daemon's rk.
    #[test]
    fn check_child_path_leads_with_the_daemon_exe_dir() {
        let path = check_child_path(
            Some("/opt/rk/bin/rk".into()),
            Some(std::ffi::OsString::from("/usr/bin:/bin")),
        )
        .unwrap();
        let parts: Vec<_> = std::env::split_paths(&path).collect();
        assert_eq!(
            parts,
            vec![
                std::path::PathBuf::from("/opt/rk/bin"),
                "/usr/bin".into(),
                "/bin".into()
            ]
        );

        // No exe location: preserve the inherited PATH untouched.
        let inherited = std::ffi::OsString::from("/usr/bin");
        assert_eq!(
            check_child_path(None, Some(inherited.clone())),
            Some(inherited)
        );
        // Nothing known at all: leave the child env alone.
        assert_eq!(check_child_path(None, None), None);
    }

    /// The escalation-after-red-gate error must LEAD with the gate result and
    /// contain both failures — a dead report check must never replace the
    /// reason the workflow stopped.
    #[test]
    fn prior_gate_failure_leads_the_composed_error() {
        let gate = json!({
            "exit": 1,
            "verdict": "fail",
            "stdout": "",
            "stderr": "test blew up",
            "timed_out": false,
        });
        let prefix = prior_gate_failure(Some(&gate));
        assert_eq!(
            prefix,
            "gate failed first: verdict fail, exit 1; stderr: test blew up; escalation also failed: "
        );
        // A timeout is a gate failure too.
        let timed = json!({"exit": 124, "verdict": "timeout", "stdout": "", "stderr": ""});
        assert!(prior_gate_failure(Some(&timed)).starts_with("gate failed first: verdict timeout"));
        // A passing prior step (the REWORK arm's green gate) adds nothing —
        // the escalation's own failure stands alone.
        let green = json!({"exit": 0, "verdict": "pass", "stdout": "ok", "stderr": ""});
        assert_eq!(prior_gate_failure(Some(&green)), "");
        assert_eq!(prior_gate_failure(None), "");
        // Non-run prior results (harness output) have no verdict: no prefix.
        assert_eq!(prior_gate_failure(Some(&json!({"result": "done"}))), "");
    }

    /// A failing check's error must carry the check's own words — an exit code
    /// alone masked `jq: command not found` behind "exited 1, expected 0" for
    /// every steward escalation failure.
    #[test]
    fn check_failure_detail_surfaces_bounded_output() {
        let detail = check_failure_detail("payload rejected", "sh: jq: command not found\n");
        assert_eq!(
            detail,
            "; stderr: sh: jq: command not found; stdout: payload rejected"
        );
        assert_eq!(check_failure_detail("", ""), "");
        // Long output is tail-bounded, keeping the end where errors live.
        let long = format!("{}THE END", "x".repeat(2000));
        let detail = check_failure_detail("", &long);
        assert!(detail.len() < 450, "detail stays bounded: {}", detail.len());
        assert!(detail.ends_with("THE END"));
    }

    /// A `cargo test --workspace` failure names its failing tests via
    /// `test <name> ... FAILED` lines, possibly repeated across several
    /// per-binary `failures:` summaries. The gate-failure artifact must
    /// recover every distinct name, deduplicated, in first-seen order.
    #[test]
    fn extract_failing_tests_finds_cargo_style_failures() {
        let stdout = "\
running 3 tests
test workflow_run::cue_workflow_runs_end_to_end_with_agent_resolution ... FAILED
test workflow_run::run_step_green_check_gates_and_merges ... FAILED
test workflow_run::run_step_red_check_fails_closed_and_holds_branch ... ok

failures:
    workflow_run::cue_workflow_runs_end_to_end_with_agent_resolution
    workflow_run::run_step_green_check_gates_and_merges

test result: FAILED. 1 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out
";
        let names = extract_failing_tests(stdout);
        assert_eq!(
            names,
            vec![
                "workflow_run::cue_workflow_runs_end_to_end_with_agent_resolution",
                "workflow_run::run_step_green_check_gates_and_merges",
            ]
        );
    }

    #[test]
    fn extract_failing_tests_ignores_passing_tests_and_dedupes() {
        let stdout = "\
test a::ok_test ... ok
test a::flaky ... FAILED
test a::flaky ... FAILED
";
        assert_eq!(extract_failing_tests(stdout), vec!["a::flaky"]);
        assert_eq!(extract_failing_tests(""), Vec::<String>::new());
        assert_eq!(
            extract_failing_tests("no test lines here at all"),
            Vec::<String>::new()
        );
    }

    #[test]
    fn extract_failing_tests_is_bounded() {
        let stdout: String = (0..MAX_FAILING_TESTS + 20)
            .map(|i| format!("test suite::t{i} ... FAILED\n"))
            .collect();
        assert_eq!(extract_failing_tests(&stdout).len(), MAX_FAILING_TESTS);
    }

    #[test]
    fn bounded_tail_keeps_the_end_and_respects_char_boundaries() {
        assert_eq!(bounded_tail("hello", 100), "hello");
        assert_eq!(bounded_tail("  padded  ", 100), "padded");
        let long = format!("{}END", "x".repeat(5000));
        let tail = bounded_tail(&long, GATE_EVIDENCE_LIMIT);
        assert!(tail.ends_with("END"));
        assert!(tail.chars().count() <= GATE_EVIDENCE_LIMIT);
    }

    #[test]
    fn named_check_inputs_cannot_replace_process_authority() {
        for allowed in ["RK_CHECK_TASK", "RK_CHECK_DIFF_LIMIT_2"] {
            assert!(valid_check_env_name(allowed), "{allowed}");
        }
        for rejected in [
            "PATH",
            "BASH_ENV",
            "RK_AGENT",
            "RK_CHECK_",
            "RK_CHECK_lower",
            "RK_CHECK_BAD-NAME",
        ] {
            assert!(!valid_check_env_name(rejected), "{rejected}");
        }
    }

    #[test]
    fn interpolate_replaces_read_vars() {
        let ctx = WorkflowContext {
            vars: HashMap::from([
                ("verdict".to_string(), json!("REWORK")),
                ("rounds".to_string(), json!(3)),
            ]),
            ..Default::default()
        };
        let text = "verdict={{ctx.var.verdict}} rounds={{ctx.var.rounds}}";
        assert_eq!(interpolate(text, &ctx), "verdict=REWORK rounds=3");
    }

    /// TKT-159 regression. Reverting the fallback — i.e. leaving the wait
    /// unbounded when the agent record is gone — makes this read admit a
    /// namesake predecessor's `harness_result` again, which is the TKT-146
    /// kill-a-live-rat defect. Every arm must yield a floor that excludes any
    /// tuple written before the run started.
    #[test]
    fn generation_floor_is_never_unbounded() {
        let started_at = Utc::now();
        let spawned_at = started_at + chrono::Duration::seconds(5);
        // A namesake's durable tuple from a previous night — the input that made
        // TKT-146 fire, and which the 24 duplicated name generations supply today.
        let predecessor = RecordId::floor_at(started_at - chrono::Duration::days(2));

        // Record present: the exact, tightest bound.
        assert_eq!(
            generation_floor_of(Some(spawned_at), Some(started_at), Utc::now()),
            spawned_at,
        );
        // Record gone: fall back to the instance start, which the agent this
        // instance spawned provably postdates.
        assert_eq!(
            generation_floor_of(None, Some(started_at), Utc::now()),
            started_at,
        );
        // Neither survives: `now`, the most conservative bound.
        let now = Utc::now();
        assert_eq!(generation_floor_of(None, None, now), now);

        // The property that actually matters on every arm.
        for floor in [
            generation_floor_of(Some(spawned_at), Some(started_at), now),
            generation_floor_of(None, Some(started_at), now),
            generation_floor_of(None, None, now),
        ] {
            assert!(
                predecessor <= RecordId::floor_at(floor),
                "floor {floor} would admit a predecessor's tuple",
            );
        }
    }

    /// The fallback must never be so tight that it misses the tuple the wait is
    /// actually for: a result written after the instance started still matches.
    #[test]
    fn instance_start_fallback_still_admits_this_generations_result() {
        let started_at = Utc::now();
        let floor = generation_floor_of(None, Some(started_at), Utc::now());
        let pattern = Pattern::for_agent_since(Category::Event, "harness_result", "Whisker", floor);
        let mut mine = rk_core::tuple::Tuple::new(
            Category::Event,
            "myrepo",
            "harness_result",
            "castle",
            json!({"agent": "Whisker", "is_error": false}),
        );
        mine.id = RecordId::floor_at(started_at + chrono::Duration::seconds(90));
        assert!(pattern.matches(&mine));
    }

    #[test]
    fn value_as_key_renders_variants() {
        assert_eq!(value_as_key(&json!("APPROVE")), "APPROVE");
        assert_eq!(value_as_key(&Value::Null), "");
        assert_eq!(value_as_key(&json!(42)), "42");
    }

    #[test]
    fn interpolate_item_binds_ticket_fields() {
        let ctx = WorkflowContext::default();
        let item = TicketItem {
            id: "TKT-7".into(),
            title: "add caching".into(),
            body: "cache the API layer".into(),
            priority: "normal".into(),
            labels: vec![],
        };
        let text = "Work {{item.id}}: {{item.title}}\n\n{{item.body}}";
        assert_eq!(
            interpolate_item(text, &item, &ctx),
            "Work TKT-7: add caching\n\ncache the API layer"
        );
    }

    #[test]
    fn parse_duration_handles_units_and_bare_numbers() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("24h").unwrap(), Duration::from_secs(86_400));
        // Bare number with no unit is treated as seconds.
        assert_eq!(parse_duration("45").unwrap(), Duration::from_secs(45));
        // Surrounding whitespace is trimmed.
        assert_eq!(parse_duration("  10m ").unwrap(), Duration::from_secs(600));
    }

    #[test]
    fn parse_duration_rejects_multibyte_suffix_without_panicking() {
        // Non-boundary byte split used to panic here; must return Err instead.
        assert!(parse_duration("5m²").is_err());
        assert!(parse_duration("10µ").is_err());
        assert!(parse_duration("²").is_err());
    }

    #[test]
    fn parse_duration_rejects_overflow() {
        // u64::MAX minutes would overflow the seconds multiplication.
        assert!(parse_duration("9223372036854775807m").is_err());
        assert!(parse_duration("18446744073709551615h").is_err());
    }

    #[test]
    fn parse_duration_rejects_empty_and_garbage() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("   ").is_err());
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("m").is_err());
    }

    // TKT-01M02QT9KTDY2CN6YJEVP3VCF8: `retry_on_fail` is a `u32`, so a
    // negative-in-source value never reaches this check — it is already
    // refused by deserialization before a `RunStep` exists. This guard is
    // for the range deserialization does NOT cover: an in-bounds-for-u32,
    // over-cap value (including u32::MAX), which would otherwise reach
    // `resolved.retry_on_fail + 1` in `run_command` unbounded.
    #[test]
    fn validate_retry_on_fail_accepts_zero_and_cap() {
        assert!(validate_retry_on_fail(0).is_ok());
        assert!(validate_retry_on_fail(MAX_RETRY_ON_FAIL).is_ok());
    }

    #[test]
    fn validate_retry_on_fail_rejects_over_cap() {
        assert!(validate_retry_on_fail(MAX_RETRY_ON_FAIL + 1).is_err());
        assert!(validate_retry_on_fail(u32::MAX).is_err());
    }

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), json!(v)))
            .collect()
    }

    /// The whole TKT-187 auto-clear rests on this key meaning "the same work",
    /// so the things that must and must not move it are pinned here rather than
    /// left to the inbox tests that consume it.
    #[test]
    fn work_key_identifies_the_work_not_the_run() {
        let base = work_key("/dev/repo", "steward", &params(&[("ticket", "TKT-1")]));

        // A retry is a different RUN of the same WORK: nothing about the run —
        // its id, its start time, its outcome — is an input here, so re-deriving
        // from the same three fields must land on the same key.
        assert_eq!(
            base,
            work_key("/dev/repo", "steward", &params(&[("ticket", "TKT-1")]))
        );

        // Param insertion order is a HashMap accident, not a difference in work.
        let mut reordered = HashMap::new();
        reordered.insert("b".to_string(), json!("2"));
        reordered.insert("a".to_string(), json!("1"));
        let mut forward = HashMap::new();
        forward.insert("a".to_string(), json!("1"));
        forward.insert("b".to_string(), json!("2"));
        assert_eq!(
            work_key("/dev/repo", "steward", &forward),
            work_key("/dev/repo", "steward", &reordered)
        );

        // Each of the three inputs genuinely separates work.
        assert_ne!(
            base,
            work_key("/dev/other", "steward", &params(&[("ticket", "TKT-1")]))
        );
        assert_ne!(
            base,
            work_key("/dev/repo", "reactor", &params(&[("ticket", "TKT-1")]))
        );
        assert_ne!(
            base,
            work_key("/dev/repo", "steward", &params(&[("ticket", "TKT-2")]))
        );
        assert_ne!(base, work_key("/dev/repo", "steward", &HashMap::new()));
    }

    /// The length prefixes are load-bearing, not cosmetic: without them a repo
    /// path ending in the delimiter could be re-cut into a different
    /// (repo, workflow) pair with identical material, and a false match here
    /// retires a real failure from the operator's inbox.
    #[test]
    fn work_key_cannot_be_re_cut_across_its_fields() {
        assert_ne!(
            work_key("/dev/repo|x", "steward", &HashMap::new()),
            work_key("/dev/repo", "x|steward", &HashMap::new())
        );
        assert_ne!(
            work_key("a", "bc", &HashMap::new()),
            work_key("ab", "c", &HashMap::new())
        );
    }

    /// Editing the workflow file is the commonest repair for a workflow that
    /// failed. If the definition digest were folded into the key, that repair
    /// would guarantee the retry could never clear the failure it fixed — so the
    /// exclusion is asserted, not merely commented.
    #[test]
    fn work_key_ignores_the_definition_digest() {
        let mut before = Instance {
            id: "wf-a".into(),
            workflow: "steward".into(),
            repo: "/dev/repo".into(),
            coordinator: None,
            schedule: None,
            status: InstanceStatus::Failed,
            revision: 0,
            current_step: 0,
            total_steps: 1,
            context: WorkflowContext::default(),
            error: None,
            awaiting: None,
            instance_max_usd: None,
            definition: "steward".into(),
            definition_digest: "aaaa".into(),
            automated_landing_authorized: false,
            params: params(&[("ticket", "TKT-1")]),
            depth: 0,
            started_at: Utc::now(),
            completed_at: None,
            archived_at: None,
            trigger: None,
            stale_timeout_secs: None,
        };
        let original = before.work_key();
        before.definition_digest = "bbbb".into();
        assert_eq!(original, before.work_key());
        // Nor does the run's own identity or outcome move it.
        before.id = "wf-b".into();
        before.status = InstanceStatus::Completed;
        assert_eq!(original, before.work_key());
    }

    #[test]
    fn run_cwd_cannot_escape_the_worktree() {
        let temp = tempfile::tempdir().unwrap();
        let worktree = temp.path().join("worktree");
        let nested = worktree.join("src");
        std::fs::create_dir_all(&nested).unwrap();
        let ctx = WorkflowContext::default();

        assert_eq!(
            resolve_worktree_cwd(&worktree, None, &ctx).unwrap(),
            worktree.canonicalize().unwrap()
        );
        assert_eq!(
            resolve_worktree_cwd(&worktree, Some("src"), &ctx).unwrap(),
            nested.canonicalize().unwrap()
        );
        assert!(resolve_worktree_cwd(&worktree, Some("../"), &ctx).is_err());
        assert!(
            resolve_worktree_cwd(&worktree, Some(temp.path().to_str().unwrap()), &ctx).is_err()
        );
    }

    #[tokio::test]
    async fn run_output_cap_truncates_but_does_not_kill_a_noisy_child() {
        // Emits well over MAX_RUN_OUTPUT_BYTES then exits cleanly on its own —
        // the cap must bound what is kept, not turn a healthy, verbose,
        // exit-0 suite into an instance failure.
        let command = "yes noisy | head -c 300000";
        let child = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(command)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let outcome = collect_child_output(child, Duration::from_secs(10), command)
            .await
            .unwrap();
        match outcome {
            RunOutcome::Completed {
                status,
                stdout,
                stdout_truncated,
                ..
            } => {
                assert!(status.success(), "expected the pipeline to exit 0");
                assert!(stdout_truncated, "300000 bytes must trip the cap");
                assert!(stdout.len() <= MAX_RUN_OUTPUT_BYTES);
            }
            RunOutcome::TimedOut => panic!("output volume alone must never time out the run"),
        }
    }

    #[tokio::test]
    async fn timeout_kills_the_whole_process_group_not_just_the_wrapper() {
        // `kill_on_drop` alone only reaches the `sh -c` wrapper's own pid; a
        // grandchild it backgrounds (mise/cargo/rustc in the real case) is
        // untouched unless the whole process group is signalled.
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("grandchild.pid");
        let command = format!("sleep 600 & echo $! > {}; wait", pid_file.display());
        let mut cmd = tokio::process::Command::new("sh");
        cmd.arg("-c")
            .arg(&command)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .process_group(0);
        let child = cmd.spawn().unwrap();

        let outcome = collect_child_output(child, Duration::from_millis(300), &command)
            .await
            .unwrap();
        assert!(matches!(outcome, RunOutcome::TimedOut));

        // Give the group-kill signal a moment to land, then confirm the
        // backgrounded grandchild is actually dead, not merely orphaned.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let pid_text = std::fs::read_to_string(&pid_file).unwrap();
        let grandchild_pid: i32 = pid_text.trim().parse().unwrap();
        // SAFETY: signal 0 only probes liveness/permission; it affects nothing.
        let alive = unsafe { libc::kill(grandchild_pid, 0) == 0 };
        assert!(!alive, "grandchild `sleep` survived the gate timeout");
    }

    #[test]
    fn timeline_rows_flatten_and_label_steps() {
        let steps: Vec<Step> = serde_json::from_value(serde_json::json!([
            {"type": "spawn", "task": {"title": "fix the bug"}},
            {"type": "wait", "timeout": "30m"},
            {"type": "gate", "gateType": "approval", "timeout": "24h"},
            {"type": "read", "category": "event", "identity": "workflow_approval",
             "field": "approved", "into": "verdict"},
            {"type": "when", "var": "verdict",
             "cases": {"true": [{"type": "dismiss"}]},
             "default": [{"type": "dismiss", "noMerge": true}, {"type": "stop", "reason": "rejected"}]},
        ]))
        .unwrap();

        let rows = timeline_rows(&steps);
        let rendered: Vec<(usize, usize, &str)> = rows
            .iter()
            .map(|r| (r.index, r.depth, r.label.as_str()))
            .collect();
        assert_eq!(
            rendered,
            vec![
                (0, 0, "spawn rat — \"fix the bug\""),
                (1, 0, "wait for result (30m)"),
                (2, 0, "gate approval (timeout 24h)"),
                (3, 0, "read event/workflow_approval.approved → verdict"),
                (4, 0, "when verdict"),
                (4, 1, "case true:"),
                (4, 2, "dismiss (merge)"),
                (4, 1, "default:"),
                (4, 2, "dismiss (no merge)"),
                (4, 2, "stop — rejected"),
            ]
        );
    }

    #[test]
    fn timeline_rows_nest_repeat_bodies() {
        let steps: Vec<Step> = serde_json::from_value(serde_json::json!([
            {"type": "repeat", "max": 3, "steps": [
                {"type": "run", "command": "cargo test"},
                {"type": "break"},
            ]},
            {"type": "land", "branch": "{{ctx.activeBranch}}", "target": "main"},
        ]))
        .unwrap();

        let rows = timeline_rows(&steps);
        let rendered: Vec<(usize, usize, &str)> = rows
            .iter()
            .map(|r| (r.index, r.depth, r.label.as_str()))
            .collect();
        assert_eq!(
            rendered,
            vec![
                (0, 0, "repeat ×3"),
                (0, 1, "run `cargo test`"),
                (0, 1, "break"),
                (1, 0, "land {{ctx.activeBranch}} → main"),
            ]
        );
    }

    /// A minimal engine with an in-memory space/registry, enough to exercise
    /// [`WorkflowEngine::run_check_in`] directly without a live daemon or a
    /// spawned agent (mirrors `supervisor::respawn_tests::supervisor`).
    fn test_engine(home: &Path) -> WorkflowEngine {
        let layout = Layout::at(home);
        let space = Space::open_in_memory().unwrap();
        let tickets = Arc::new(Tickets::new(space.clone(), "castle".into()));
        let supervisor = Arc::new(
            Supervisor::new(
                layout.clone(),
                "castle".into(),
                "fake".into(),
                rk_ledger::Budget::default(),
                rk_ledger::FleetBudget::default(),
                space.clone(),
                tickets.clone(),
            )
            .unwrap(),
        );
        WorkflowEngine::new(
            layout,
            supervisor,
            space,
            tickets,
            HashMap::new(),
            TierRouting::default(),
            "fake".into(),
            false,
            true,
            false,
            Vec::new(),
            Vec::new(),
            0,
            false,
        )
    }

    /// T1: `run_check_in` was extracted from `run_command` precisely so a
    /// caller with its own directory (a future daemon-native gate worktree,
    /// no agent involved) does not need `ctx.active_agent` at all. Prove the
    /// extraction preserved behavior exactly by calling it twice with the
    /// same resolved check and env but two independently-built directories —
    /// one shaped like today's agent worktree, one a bare directory with no
    /// agent behind it — and asserting byte-identical outcomes.
    #[tokio::test]
    async fn run_check_in_is_identical_via_agent_worktree_and_bare_directory() {
        let home = tempfile::tempdir().unwrap();
        let engine = test_engine(home.path());

        let agent_worktree = tempfile::tempdir().unwrap();
        std::fs::write(agent_worktree.path().join("marker.txt"), "hello\n").unwrap();
        let bare_gate_dir = tempfile::tempdir().unwrap();
        std::fs::write(bare_gate_dir.path().join("marker.txt"), "hello\n").unwrap();

        let resolved = ResolvedRun {
            command: "cat marker.txt && printf '%s' \"$RK_CHECK_MARK\"".into(),
            cwd: None,
            expect_exit: Some(0),
            timeout: "5s".into(),
            on_timeout: OnTimeout::Fail,
            environment_policy: rk_workflow::CheckEnvironmentPolicy::StripRkSpawn,
            retry_on_fail: 0,
            shared_cargo_target: false,
        };
        let env = vec![("RK_CHECK_MARK".to_string(), "gate".to_string())];
        let timeout = Duration::from_secs(5);

        let via_agent_path = engine
            .run_check_in(
                "inst-agent",
                "/repo",
                "Whisker",
                agent_worktree.path(),
                &resolved.command,
                &resolved,
                &env,
                timeout,
                None,
            )
            .await
            .unwrap();
        let via_bare_dir = engine
            .run_check_in(
                "inst-daemon",
                "/repo",
                "daemon",
                bare_gate_dir.path(),
                &resolved.command,
                &resolved,
                &env,
                timeout,
                None,
            )
            .await
            .unwrap();

        assert_eq!(via_agent_path["exit"], json!(0));
        assert_eq!(via_agent_path["verdict"], json!("pass"));
        assert_eq!(via_agent_path["stdout"], json!("hello\ngate"));
        assert_eq!(via_agent_path["exit"], via_bare_dir["exit"]);
        assert_eq!(via_agent_path["verdict"], via_bare_dir["verdict"]);
        assert_eq!(via_agent_path["stdout"], via_bare_dir["stdout"]);
        assert_eq!(
            via_agent_path["stdout_truncated"],
            via_bare_dir["stdout_truncated"]
        );
    }

    /// The same non-"pass" path (retry exhaustion + `record_gate_failure`)
    /// runs identically for a bare directory as it does for an agent
    /// worktree: a failing command still produces a `fail` verdict and an
    /// `Err` on the declared `expectExit`, with the durable gate-failure
    /// artifact written regardless of whether an agent was ever involved.
    #[tokio::test]
    async fn run_check_in_records_gate_failure_for_a_bare_directory() {
        let home = tempfile::tempdir().unwrap();
        let engine = test_engine(home.path());
        let space = engine.space.clone();
        let bare_gate_dir = tempfile::tempdir().unwrap();

        let resolved = ResolvedRun {
            command: "echo boom 1>&2; exit 3".into(),
            cwd: None,
            expect_exit: Some(0),
            timeout: "5s".into(),
            on_timeout: OnTimeout::Fail,
            environment_policy: rk_workflow::CheckEnvironmentPolicy::StripRkSpawn,
            retry_on_fail: 0,
            shared_cargo_target: false,
        };
        let timeout = Duration::from_secs(5);

        let err = engine
            .run_check_in(
                "inst-daemon-fail",
                "/repo/daemon-gate",
                "daemon",
                bare_gate_dir.path(),
                &resolved.command,
                &resolved,
                &[],
                timeout,
                None,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("exited 3"));

        let failures = space
            .scan(&Pattern::category(Category::Artifact).identity("gate-failure"))
            .unwrap();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].payload["agent"], json!("daemon"));
        assert_eq!(failures[0].payload["verdict"], json!("fail"));
        assert_eq!(failures[0].payload["exit"], json!(3));
    }

    /// TKT-01M0CF9PG9NHHM0ZTFKDW6BVBV: a shared `CARGO_TARGET_DIR`
    /// cross-process contention failure (see
    /// docs/2026-08-19-tkt-hot-scan-target-dir-contention.md) gets exactly
    /// one free retry, ahead of and independent from `retry_on_fail`, so it
    /// fires even at the historical default of 0.
    #[tokio::test]
    async fn run_check_in_retries_once_on_cargo_target_contention_signature_then_passes() {
        let home = tempfile::tempdir().unwrap();
        let engine = test_engine(home.path());
        let bare_gate_dir = tempfile::tempdir().unwrap();

        let resolved = ResolvedRun {
            command: "if [ -f retried ]; then exit 0; else touch retried; \
                      echo 'could not execute process `/tmp/hot_scan-deadbeef` (never executed)' 1>&2; \
                      echo 'Caused by: No such file or directory (os error 2)' 1>&2; \
                      exit 101; fi"
                .into(),
            cwd: None,
            expect_exit: Some(0),
            timeout: "5s".into(),
            on_timeout: OnTimeout::Fail,
            environment_policy: rk_workflow::CheckEnvironmentPolicy::StripRkSpawn,
            retry_on_fail: 0,
        };
        let timeout = Duration::from_secs(5);

        let result = engine
            .run_check_in(
                "inst-contention-retry",
                "/repo/daemon-gate",
                "daemon",
                bare_gate_dir.path(),
                &resolved.command,
                &resolved,
                &[],
                timeout,
                None,
            )
            .await
            .unwrap();

        assert_eq!(result["verdict"], json!("pass"));
        assert_eq!(result["exit"], json!(0));
        assert!(
            result.get("retries").is_none(),
            "the contention retry must not populate the flaky `retry_on_fail` history: {result:?}"
        );
    }

    /// The contention signature retries exactly once — a second consecutive
    /// hit still records a gate failure rather than retrying forever.
    #[tokio::test]
    async fn run_check_in_retries_exactly_once_on_contention_signature_then_records_gate_failure() {
        let home = tempfile::tempdir().unwrap();
        let engine = test_engine(home.path());
        let space = engine.space.clone();
        let bare_gate_dir = tempfile::tempdir().unwrap();

        let resolved = ResolvedRun {
            command: "n=$(cat count 2>/dev/null || echo 0); n=$((n+1)); echo $n > count; \
                      echo 'could not execute process `/tmp/hot_scan-deadbeef` (never executed): No such file or directory (os error 2)' 1>&2; \
                      exit 101"
                .into(),
            cwd: None,
            expect_exit: Some(0),
            timeout: "5s".into(),
            on_timeout: OnTimeout::Fail,
            environment_policy: rk_workflow::CheckEnvironmentPolicy::StripRkSpawn,
            retry_on_fail: 0,
        };
        let timeout = Duration::from_secs(5);

        let err = engine
            .run_check_in(
                "inst-contention-exhausted",
                "/repo/daemon-gate",
                "daemon",
                bare_gate_dir.path(),
                &resolved.command,
                &resolved,
                &[],
                timeout,
                None,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("exited 101"));

        let count: u32 = std::fs::read_to_string(bare_gate_dir.path().join("count"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert_eq!(count, 2, "expected exactly one retry (2 total executions)");

        let failures = space
            .scan(&Pattern::category(Category::Artifact).identity("gate-failure"))
            .unwrap();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].payload["exit"], json!(101));
    }

    /// A real failure — no contention signature in its output — must never
    /// get the free retry; it fails on the first attempt like today.
    #[tokio::test]
    async fn run_check_in_does_not_retry_a_genuine_failure_that_lacks_the_contention_signature() {
        let home = tempfile::tempdir().unwrap();
        let engine = test_engine(home.path());
        let bare_gate_dir = tempfile::tempdir().unwrap();

        let resolved = ResolvedRun {
            command: "n=$(cat count 2>/dev/null || echo 0); n=$((n+1)); echo $n > count; \
                      echo 'assertion failed: left == right' 1>&2; \
                      exit 101"
                .into(),
            cwd: None,
            expect_exit: Some(0),
            timeout: "5s".into(),
            on_timeout: OnTimeout::Fail,
            environment_policy: rk_workflow::CheckEnvironmentPolicy::StripRkSpawn,
            retry_on_fail: 0,
        };
        let timeout = Duration::from_secs(5);

        let err = engine
            .run_check_in(
                "inst-genuine-fail",
                "/repo/daemon-gate",
                "daemon",
                bare_gate_dir.path(),
                &resolved.command,
                &resolved,
                &[],
                timeout,
                None,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("exited 101"));

        let count: u32 = std::fs::read_to_string(bare_gate_dir.path().join("count"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert_eq!(
            count, 1,
            "a real failure must not get the contention free retry"
        );
    }

    /// TKT-146, closed for the SEQUENTIAL path
    /// (`docs/2026-08-17-tkt-c1-generation-identity.md`): a `dismiss` step
    /// must not act on whoever currently holds `ctx.activeAgent`'s name if
    /// that is a different generation than the one this instance's own
    /// `spawn` step captured. Mirrors
    /// `dismiss_checked_refuses_a_namesake_that_is_not_the_expected_generation`
    /// in `supervisor.rs`, but drives it through `Step::Dismiss` itself so the
    /// wiring is under test, not just the guard it calls: the exact TKT-146
    /// shape is spawn -> wait -> [a namesake respawns] -> dismiss, and the
    /// dismiss must refuse rather than tear down the new namesake.
    #[tokio::test]
    async fn dismiss_step_refuses_a_namesake_that_respawned_between_wait_and_dismiss() {
        let home = tempfile::tempdir().unwrap();
        let engine = test_engine(home.path());
        let now = Utc::now();

        let waited_generation = rk_core::id::SpawnId::new();
        let mut record = crate::agents::AgentRecord {
            name: "Nibble".into(),
            spawn: Some(waited_generation),
            role: "rat".into(),
            coordination: None,
            harness: "fake".into(),
            permission_mode: None,
            model: None,
            repo_root: PathBuf::from("/repo"),
            repo_name: "repo".into(),
            task: Some("t".into()),
            branch: Some("rat/nibble/t".into()),
            worktree: Some(PathBuf::from("/repo")),
            target_branch: "main".into(),
            parent: None,
            workflow_instance: None,
            coordinator: None,
            session_id: None,
            attach_target: None,
            pid: None,
            merge_commit: None,
            state: AgentState::Running,
            crashed: false,
            stderr_tail: None,
            result: None,
            progress: None,
            usage: rk_harness::TokenUsage::default(),
            cost_usd: 0.0,
            created_at: now,
            updated_at: now,
            archived_at: None,
        };
        // The workflow's own `spawn` step ran and its `wait` completed against
        // this generation.
        engine
            .supervisor
            .lock_registry()
            .insert(record.clone())
            .unwrap();

        let id = "inst-namesake-dismiss";
        let instance = Instance {
            id: id.into(),
            workflow: "wf".into(),
            repo: "/repo".into(),
            coordinator: None,
            schedule: None,
            status: InstanceStatus::Running,
            revision: 0,
            current_step: 0,
            total_steps: 1,
            context: WorkflowContext {
                active_agent: Some("Nibble".into()),
                active_agent_spawn: Some(waited_generation),
                ..Default::default()
            },
            error: None,
            awaiting: None,
            instance_max_usd: None,
            definition: "wf".into(),
            definition_digest: String::new(),
            automated_landing_authorized: false,
            params: HashMap::new(),
            depth: 0,
            started_at: now,
            completed_at: None,
            archived_at: None,
            trigger: None,
            stale_timeout_secs: None,
        };
        engine.store_if_absent(instance).unwrap();

        // A namesake respawns between this instance's `wait` and its
        // `dismiss`: a different generation now holds "Nibble".
        let respawned_generation = rk_core::id::SpawnId::new();
        record.spawn = Some(respawned_generation);
        record.created_at = Utc::now();
        engine.supervisor.lock_registry().insert(record).unwrap();

        let outcome = engine
            .run_step(
                id,
                &Step::Dismiss(DismissStep::default()),
                "/repo",
                &HashMap::new(),
                &TierRouting::default(),
            )
            .await;

        let error = match outcome {
            Err(e) => e,
            Ok(_) => panic!("must refuse to dismiss a different generation"),
        };
        assert!(
            error.to_string().contains("dismiss target mismatch"),
            "unexpected error: {error}"
        );

        // The new namesake must be untouched: still live, still that generation.
        let live = engine
            .supervisor
            .lock_registry()
            .get("Nibble")
            .cloned()
            .unwrap();
        assert_eq!(live.spawn, Some(respawned_generation));
        assert_eq!(live.state, AgentState::Running);
    }

    // --- B8: stale-`Running`-instance hard timeout sweep ---

    struct RecordingSink(std::sync::Arc<Mutex<Vec<String>>>);

    impl rk_core::notify::NotificationSink for RecordingSink {
        fn kind(&self) -> &str {
            "recorder"
        }
        fn deliver(&self, notice: &EscalationNotice) -> rk_core::Result<()> {
            self.0
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(notice.tuple_id.clone());
            Ok(())
        }
    }

    fn recording_sinks() -> (SinkRegistry, std::sync::Arc<Mutex<Vec<String>>>) {
        let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
        let mut registry = SinkRegistry::new();
        registry.register(
            rk_core::config::SinkConfig::of_kind("recorder"),
            Box::new(RecordingSink(seen.clone())),
        );
        (registry, seen)
    }

    fn wedged_instance(id: &str, started_at: DateTime<Utc>) -> Instance {
        Instance {
            id: id.into(),
            workflow: "steward".into(),
            repo: "/repo".into(),
            coordinator: None,
            schedule: None,
            status: InstanceStatus::Running,
            revision: 0,
            current_step: 1,
            total_steps: 3,
            context: WorkflowContext::default(),
            error: None,
            awaiting: None,
            instance_max_usd: None,
            definition: "steward".into(),
            definition_digest: String::new(),
            automated_landing_authorized: false,
            params: HashMap::new(),
            depth: 0,
            started_at,
            completed_at: None,
            archived_at: None,
            trigger: None,
            stale_timeout_secs: None,
        }
    }

    #[test]
    fn resolve_stale_timeout_secs_parses_the_override_and_defaults_to_none() {
        let mut workflow = Workflow {
            name: "wf".into(),
            description: String::new(),
            params: HashMap::new(),
            agents: HashMap::new(),
            tiers: TierRouting::default(),
            budget: None,
            stale_timeout: None,
            steps: Vec::new(),
            aspects: Vec::new(),
        };
        assert_eq!(resolve_stale_timeout_secs(&workflow).unwrap(), None);

        workflow.stale_timeout = Some("24h".into());
        assert_eq!(
            resolve_stale_timeout_secs(&workflow).unwrap(),
            Some(24 * 3600)
        );

        workflow.stale_timeout = Some("not-a-duration".into());
        assert!(resolve_stale_timeout_secs(&workflow).is_err());
    }

    /// Acceptance criterion: an artificially wedged instance (`Running`,
    /// `started_at` far past the default timeout) transitions to `failed` with
    /// an escalation notice.
    #[tokio::test]
    async fn stale_timeout_sweep_fails_a_wedged_instance_and_announces() {
        let home = tempfile::tempdir().unwrap();
        let engine = test_engine(home.path());
        let started_at = Utc::now() - chrono::Duration::hours(13);
        engine
            .store_if_absent(wedged_instance("wf-wedged", started_at))
            .unwrap();

        let (sinks, recorder) = recording_sinks();
        let announcer = RecoveryAnnouncer::new();
        let timed_out = engine
            .stale_timeout_sweep_once(
                Utc::now(),
                Duration::from_secs(12 * 3600),
                &announcer,
                &sinks,
                RateCap::unlimited(),
            )
            .await;

        assert_eq!(timed_out, 1);
        let after = engine.status("wf-wedged").unwrap();
        assert_eq!(after.status, InstanceStatus::Failed);
        assert!(after.error.unwrap().contains("stale-instance timeout"));
        assert_eq!(recorder.lock().unwrap().len(), 1);

        let events = engine
            .space
            .scan(&Pattern::category(Category::Event).identity("workflow_failed"))
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload["instance"], json!("wf-wedged"));
    }

    /// Acceptance criterion: a long-running workflow with an explicit
    /// `staleTimeout:` override is untouched by the default-timeout sweep.
    #[tokio::test]
    async fn stale_timeout_sweep_leaves_an_overridden_instance_running() {
        let home = tempfile::tempdir().unwrap();
        let engine = test_engine(home.path());
        // Past the 12h default, but within its own 24h override.
        let started_at = Utc::now() - chrono::Duration::hours(13);
        let mut instance = wedged_instance("wf-overridden", started_at);
        instance.stale_timeout_secs = Some(24 * 3600);
        engine.store_if_absent(instance).unwrap();

        let (sinks, recorder) = recording_sinks();
        let announcer = RecoveryAnnouncer::new();
        let timed_out = engine
            .stale_timeout_sweep_once(
                Utc::now(),
                Duration::from_secs(12 * 3600),
                &announcer,
                &sinks,
                RateCap::unlimited(),
            )
            .await;

        assert_eq!(timed_out, 0);
        assert_eq!(
            engine.status("wf-overridden").unwrap().status,
            InstanceStatus::Running
        );
        assert!(recorder.lock().unwrap().is_empty());
    }

    /// The sweep must never race a genuine completion out from under it: the
    /// guarded transition is a no-op on anything that is not `Running` at the
    /// moment the lock is held, however old its `started_at` is.
    #[tokio::test]
    async fn timeout_stale_instance_is_a_guarded_no_op_once_already_terminal() {
        let home = tempfile::tempdir().unwrap();
        let engine = test_engine(home.path());
        let started_at = Utc::now() - chrono::Duration::hours(13);
        let mut instance = wedged_instance("wf-already-done", started_at);
        instance.status = InstanceStatus::Completed;
        instance.completed_at = Some(Utc::now());
        engine.store_if_absent(instance.clone()).unwrap();

        let changed = engine
            .timeout_stale_instance(&instance, 12 * 3600)
            .await
            .unwrap();

        assert!(!changed);
        assert_eq!(
            engine.status("wf-already-done").unwrap().status,
            InstanceStatus::Completed
        );
    }

    /// The mirror-image race: the stale-timeout sweep wins first and persists
    /// `Failed`, but the `execute()` future it declared wedged was not
    /// actually dead — it finishes a moment later and its `spawn_execution`
    /// task calls `finalize` with `Ok(())`. `finalize`'s terminal write must
    /// be a no-op here, or the sweep's `Failed` verdict would be silently
    /// overwritten with `Completed`.
    #[tokio::test]
    async fn finalize_does_not_overwrite_a_sweep_that_already_failed_the_instance() {
        let home = tempfile::tempdir().unwrap();
        let engine = test_engine(home.path());
        let started_at = Utc::now() - chrono::Duration::hours(13);
        let instance = wedged_instance("wf-race", started_at);
        engine.store_if_absent(instance.clone()).unwrap();

        // The sweep wins the race first and marks the instance Failed.
        let timed_out = engine
            .timeout_stale_instance(&instance, 12 * 3600)
            .await
            .unwrap();
        assert!(timed_out);
        assert_eq!(
            engine.status("wf-race").unwrap().status,
            InstanceStatus::Failed
        );

        // The "still-running" execute() future the sweep declared wedged
        // finishes anyway and its spawn_execution task calls finalize.
        engine
            .finalize("wf-race", "/repo", "steward", Ok(()))
            .await
            .unwrap();

        // The sweep's Failed verdict must survive, not be overwritten by
        // finalize's Completed.
        let after = engine.status("wf-race").unwrap();
        assert_eq!(after.status, InstanceStatus::Failed);
        assert!(after.error.unwrap().contains("stale-instance timeout"));
    }
}

//! Workflow execution: sequential step machine over the supervisor and the
//! tuplespace. Definitions come from rk-workflow (cue CLI); this module owns
//! instances, context threading, and step semantics.

use crate::agents::AgentState;
use crate::supervisor::{SpawnParams, Supervisor};
use crate::tickets::Tickets;
use chrono::{DateTime, Utc};
use rk_core::id::prefixed_id;
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
use std::collections::HashMap;
use std::future::Future;
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
struct ResolvedRun {
    command: String,
    cwd: Option<String>,
    expect_exit: Option<i64>,
    timeout: String,
    on_timeout: OnTimeout,
}

/// What a blown `run` wall-clock bound does to the instance (TKT-169).
///
/// The command is killed either way — `kill_on_drop` owns that, and a hung suite
/// never survives its budget. The choice here is only whether the kill is
/// reported as an ERROR (which ends the run where it stands) or as a RESULT the
/// following steps get to route on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OnTimeout {
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

/// The outcome of running a `run` step's command to completion or to its bound.
#[derive(Debug)]
enum RunOutcome {
    Completed {
        status: std::process::ExitStatus,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    },
    /// The wall-clock bound elapsed and the child was killed. Only ever returned
    /// under [`OnTimeout::Continue`]; under `Fail` a timeout is an `Err`.
    TimedOut,
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
}

async fn read_capped<R>(mut reader: R) -> rk_core::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut output = Vec::new();
    let mut chunk = [0_u8; 8192];
    loop {
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            return Ok(output);
        }
        if output.len().saturating_add(read) > MAX_RUN_OUTPUT_BYTES {
            return Err(rk_core::Error::other(format!(
                "run step output exceeds {MAX_RUN_OUTPUT_BYTES} bytes"
            )));
        }
        output.extend_from_slice(&chunk[..read]);
    }
}

async fn abort_task<T>(task: &mut JoinHandle<T>) {
    task.abort();
    let _ = task.await;
}

async fn collect_child_output(
    mut child: tokio::process::Child,
    timeout: Duration,
    command: &str,
    timeout_text: &str,
    on_timeout: OnTimeout,
) -> rk_core::Result<RunOutcome> {
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| rk_core::Error::other("run step: child stdout was not piped"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| rk_core::Error::other("run step: child stderr was not piped"))?;

    // Put the child in a task whose cancellation/drop semantics own the
    // process. Reader overflow, join failure, and timeout all abort this task,
    // dropping the kill_on_drop child and preventing orphaned checks.
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
                // The child dies here regardless of `on_timeout`: aborting the
                // wait task drops the `kill_on_drop` child. The only choice is
                // whether the caller learns about it as an error or a result.
                if status.is_none() {
                    abort_task(&mut wait_task).await;
                }
                if stdout.is_none() {
                    abort_task(&mut stdout_task).await;
                }
                if stderr.is_none() {
                    abort_task(&mut stderr_task).await;
                }
                return match on_timeout {
                    OnTimeout::Fail => Err(rk_core::Error::other(format!(
                        "run step: `{command}` timed out after {timeout_text}"
                    ))),
                    OnTimeout::Continue => Ok(RunOutcome::TimedOut),
                };
            }
        }
    }

    Ok(RunOutcome::Completed {
        status: status.expect("status completed with all child tasks"),
        stdout: stdout.expect("stdout completed with all child tasks"),
        stderr: stderr.expect("stderr completed with all child tasks"),
    })
}

fn definition_inside_roots(
    candidate: &Path,
    repo: &str,
    global_root: &Path,
) -> Option<PathBuf> {
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
}

/// Control-flow signal threaded out of a step (or nested step sequence).
enum Flow {
    /// Continue with the next step in sequence.
    Next,
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
    require_approval_for_landing: bool,
    allowed_target_branches: Vec<String>,
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
        allowed_target_branches: Vec<String>,
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
            require_approval_for_landing,
            allowed_target_branches,
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
        let file = self.find_definition(name, repo)?;
        let definition_digest = definition_digest(&file)?;
        let workflow = rk_workflow::load(&file, &params)?;

        let instance = Instance {
            id: prefixed_id("wf"),
            workflow: workflow.name.clone(),
            repo: repo.to_string(),
            coordinator,
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
            params,
            depth: 0,
            started_at: chrono::Utc::now(),
            completed_at: None,
            archived_at: None,
        };
        self.store(instance.clone());
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
            engine.finalize(&id, &repo, &workflow_name, result);
        });
    }

    /// Record an instance's terminal status and broadcast its completion event.
    fn finalize(&self, id: &str, repo: &str, workflow_name: &str, result: rk_core::Result<()>) {
        let (status, error) = match result {
            Ok(()) => (InstanceStatus::Completed, None),
            Err(e) => (InstanceStatus::Failed, Some(e.to_string())),
        };
        let updated = self.update_with_reason(id, "terminal", |i| {
            i.status = status;
            i.error = error.clone();
            i.completed_at = Some(chrono::Utc::now());
        });
        if !updated {
            warn!(instance = %id, "workflow terminal state was not persisted; skipping completion event");
            return;
        }
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
    }

    /// Load persisted instances from disk on daemon startup (TKT-52).
    ///
    /// Every mutation already writes each instance to
    /// `<home>/workflow-instances/<id>.json`; this is the missing read side.
    /// Completed and failed instances are restored for history — so
    /// `rk workflow status`/`list` and `rk approve` survive a restart — while
    /// `Running` instances are additionally *resumed*: re-executed from their
    /// persisted step cursor so a crash or restart mid-run no longer silently
    /// drops an in-flight workflow (a parked approval gate, a fan-out waiting on
    /// `wait_all`). Idempotent: instances already in memory are overwritten by
    /// their on-disk snapshot, so calling it twice is harmless.
    pub fn rehydrate(self: &Arc<Self>) {
        let mut resumable = Vec::new();
        for instance in self.read_instance_dir(&self.instances_dir()) {
            // Only top-level (depth 0) instances resume standalone. A nested
            // sub-workflow child (depth > 0) is re-driven by its parent's
            // resumed `sub_workflow` step (which re-runs the interrupted step and
            // launches a fresh child), so resuming it independently here would
            // double-run its agents. It is still loaded into memory for history.
            let running = instance.status == InstanceStatus::Running && instance.depth == 0;
            self.lock().insert(instance.id.clone(), instance.clone());
            if running {
                resumable.push(instance);
            }
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
        if !resumable.is_empty() {
            info!(
                count = resumable.len(),
                "resuming in-flight workflow instances after restart"
            );
        }
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
                if !instance.definition_digest.is_empty()
                    && instance.definition_digest != digest
                {
                    return Err(rk_core::Error::other(format!(
                        "definition digest changed (persisted {}, current {})",
                        instance.definition_digest, digest
                    )));
                }
                Ok((rk_workflow::load(&file, &instance.params)?, digest))
            })
        {
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
            if let Flow::Break = self
                .run_step(id, step, repo, &workflow.agents, &workflow.tiers)
                .await?
            {
                // A top-level break ends the workflow (nothing to loop out of).
                break;
            }
            // Advance only AFTER the step completes, so a restart resumes at the
            // interrupted step and never re-runs a finished one.
            self.update_with_reason(id, "step_advanced", |i| i.current_step = index + 1);
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
            for step in steps {
                if let Flow::Break = self.run_step(id, step, repo, agents, tiers).await? {
                    return Ok(Flow::Break);
                }
            }
            Ok(Flow::Next)
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
                    let resolved =
                        resolve(spawn, agents, &self.global_agents, &self.default_harness)?;
                    let title = interpolate(&spawn.task.title, &ctx);
                    let prompt = spawn
                        .task
                        .description
                        .as_ref()
                        .map(|d| interpolate(d, &ctx));
                    let record = self.spawn_agent(SpawnParams {
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
                    })
                    .await?;
                    self.update(id, |i| {
                        i.context.active_agent = Some(record.name.clone());
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
                    let outcome = self.supervisor.dismiss(&agent, dismiss.no_merge).await?;
                    self.update(id, |i| {
                        i.context.previous_result = Some(outcome.clone());
                        i.context.awaited = Vec::new();
                        i.context.active_agent = None;
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
                        let approval_granted = decision
                            .get("approved")
                            .and_then(Value::as_bool)
                            == Some(true);
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
                    //
                    // All three of `search`/`fromAgent`/`fromInstance` write the
                    // one `payload_search` slot, so at most one may be set.
                    let bindings = read.from_agent as u8
                        + read.from_instance as u8
                        + read.search.is_some() as u8;
                    if bindings > 1 {
                        return Err(rk_core::Error::other(
                            "read step sets more than one of `fromAgent`/`fromInstance`/`search`; \
                             they claim the same payload predicate — keep one",
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
                    let tuple = tuple.ok_or_else(|| {
                        // Name the binding in the failure: a bound read that
                        // matched nothing is otherwise indistinguishable from a
                        // tuple that was never written. Under `fromAgent` the
                        // usual cause is an agent that left its own name out of
                        // the payload; under `fromInstance` it is a decision
                        // recorded without this run's id.
                        let bound_to = match (read.from_agent, ctx.active_agent.as_deref()) {
                            (true, Some(agent)) => format!(" written by {agent}"),
                            _ if read.from_instance => format!(" naming instance {id}"),
                            _ => String::new(),
                        };
                        rk_core::Error::other(format!(
                            "read timed out after {} for {} tuple '{}'{bound_to}",
                            read.timeout, read.category, read.identity
                        ))
                    })?;
                    let value = match &read.field {
                        Some(field) => tuple.payload.get(field).cloned().unwrap_or(Value::Null),
                        None => tuple.payload.clone(),
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
                    for _ in 0..repeat.max {
                        if let Flow::Break = self
                            .run_steps(id, &repeat.steps, repo, agents, tiers)
                            .await?
                        {
                            break;
                        }
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
                    let result = self.run_command(&ctx, run, repo).await?;
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
                    if self.require_approval_for_landing && !ctx.approval_granted {
                        return Err(rk_core::Error::other(
                            "land step requires a prior approved human gate",
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
                    self.require_allowed_target(&target)?;
                    let result = self
                        .supervisor
                        .land(std::path::Path::new(repo), &branch, &target, land.keep_branch)
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
                    self.require_allowed_target(&target)?;
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
                    self.update(id, |i| {
                        i.context.previous_result = Some(result.clone());
                        i.context.awaited = Vec::new();
                    });
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
        let child = Instance {
            id: prefixed_id("wf"),
            workflow: workflow_name.clone(),
            repo: child_repo.clone(),
            coordinator: self.status(parent_id).and_then(|i| i.coordinator),
            status: InstanceStatus::Running,
            revision: 0,
            current_step: 0,
            total_steps: workflow.steps.len(),
            context: WorkflowContext::default(),
            error: None,
            awaiting: None,
            instance_max_usd: workflow.budget.map(|b| b.max_usd),
            definition: sub.workflow.clone(),
            definition_digest,
            params,
            depth,
            started_at: chrono::Utc::now(),
            completed_at: None,
            archived_at: None,
        };
        let child_id = child.id.clone();
        self.store(child);
        info!(parent = %parent_id, child = %child_id, workflow = %workflow_name, depth, "running sub-workflow inline");
        // Execute the child on this task so the parent step joins on it. finalize
        // records the terminal status and emits the child's own completion event,
        // identical to a top-level run.
        match self.execute(&child_id, workflow, &child_repo).await {
            Ok(()) => {
                self.finalize(&child_id, &child_repo, &workflow_name, Ok(()));
                // The child's final result is this sub_workflow's return value.
                Ok(self
                    .status(&child_id)
                    .and_then(|i| i.context.previous_result)
                    .unwrap_or(Value::Null))
            }
            Err(e) => {
                let msg = e.to_string();
                self.finalize(
                    &child_id,
                    &child_repo,
                    &workflow_name,
                    Err(rk_core::Error::other(msg.clone())),
                );
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
        let items = self.query_tickets(&fe.query, repo)?;
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
            let record = self.spawn_agent(SpawnParams {
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
            })
            .await?;
            fanned.push(FannedAgent {
                agent: record.name.clone(),
                branch: record.branch.clone(),
                ticket: Some(item.id),
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
    fn result_pattern(&self, id: &str, agent: &str) -> Pattern {
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
            // Base no_merge from the step, plus: under only_clean, park (don't
            // merge) any agent not in the clean set.
            let parked = clean
                .as_ref()
                .is_some_and(|clean| !clean.contains(&fa.agent));
            let no_merge = dismiss_all.no_merge || parked;
            set.spawn(async move {
                let outcome = supervisor.dismiss(&agent, no_merge).await;
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

        let child = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&command)
            .current_dir(&dir)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            // Kill the suite if the timeout below drops the wait future, so a
            // hung check leaves no orphan behind.
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| {
                rk_core::Error::other(format!("run step: failed to spawn `{command}`: {e}"))
            })?;

        let outcome = collect_child_output(
            child,
            timeout,
            &command,
            &resolved.timeout,
            resolved.on_timeout,
        )
        .await?;
        // A `TimedOut` outcome only reaches here under `onTimeout: "continue"`;
        // otherwise the timeout already returned an error above. The captured
        // output is genuinely gone in that case (the reader tasks are aborted
        // with the child), so stderr carries the explanation instead of a lie
        // about what the suite printed.
        let (exit, stdout, stderr, timed_out) = match outcome {
            RunOutcome::Completed {
                status,
                stdout,
                stderr,
            } => (
                status.code().unwrap_or(-1) as i64,
                String::from_utf8_lossy(&stdout).into_owned(),
                String::from_utf8_lossy(&stderr).into_owned(),
                false,
            ),
            RunOutcome::TimedOut => (
                TIMEOUT_EXIT,
                String::new(),
                format!(
                    "run step: `{command}` timed out after {} and was killed",
                    resolved.timeout
                ),
                true,
            ),
        };
        // The routable three-way summary. `exit` alone cannot express it: a
        // suite may exit 124 on its own, and "did not finish" calls for a
        // different hand-off than "finished and said no".
        let verdict = if timed_out {
            "timeout"
        } else if exit == 0 {
            "pass"
        } else {
            "fail"
        };
        info!(agent = %agent, exit, timed_out, verdict, command = %command, "run step completed");
        let result = json!({
            "exit": exit,
            "stdout": stdout,
            "stderr": stderr,
            "timed_out": timed_out,
            "verdict": verdict,
        });

        // Inline fail-closed gate: when the step (or named check) declares the
        // expected exit, enforce it here so `run` can gate on its own without a
        // trailing evaluate. When unset, the exit is left for a following
        // evaluate/when. A timed-out command reports 124, so this rejects it
        // exactly as it rejects a red suite — `onTimeout: "continue"` never
        // sneaks a too-slow check past a declared exit gate.
        if let Some(expected) = resolved.expect_exit {
            if exit != expected {
                return Err(rk_core::Error::other(format!(
                    "run step: `{command}` exited {exit}, expected {expected}"
                )));
            }
        }
        Ok(result)
    }

    fn require_allowed_target(&self, target: &str) -> rk_core::Result<()> {
        if self
            .allowed_target_branches
            .iter()
            .any(|allowed| allowed == target)
        {
            return Ok(());
        }
        Err(rk_core::Error::other(format!(
            "workflow target '{target}' is not in policy.allowed_target_branches"
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
    fn query_tickets(&self, query: &TicketQuery, repo: &str) -> rk_core::Result<Vec<TicketItem>> {
        let scope = Some(repo_name_of(repo));
        let tuples = if query.status == "ready" {
            self.tickets.ready(scope)?
        } else {
            self.tickets.list(scope, Some(query.status.clone()), None)?
        };
        Ok(tuples
            .into_iter()
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
            self.persist_to(&archive_dir, instance)?;
        }
        let live_dir = self.instances_dir();
        for instance in &moved {
            self.lock().remove(&instance.id);
            self.lock_archived()
                .insert(instance.id.clone(), instance.clone());
            let _ = std::fs::remove_file(live_dir.join(format!("{}.json", instance.id)));
        }
        info!(count = moved.len(), "archived terminal workflow instances");
        Ok(moved)
    }

    /// Restore one archived instance to the live store — the undo for
    /// [`archive`](WorkflowEngine::archive). `Ok(None)` means no such archived
    /// instance; an id a live instance already holds is a real collision, not a
    /// no-op, and errors.
    pub fn unarchive(&self, id: &str) -> rk_core::Result<Option<Instance>> {
        if self.lock().contains_key(id) {
            return Err(rk_core::Error::other(format!(
                "cannot unarchive {id}: a live instance already holds that id"
            )));
        }
        let Some(mut instance) = self.lock_archived().get(id).cloned() else {
            return Ok(None);
        };
        instance.archived_at = None;
        // Live file first: a crash before the archive file is removed leaves
        // the instance in both stores, where the live copy wins — never in
        // neither.
        self.persist_to(&self.instances_dir(), &instance)?;
        self.lock().insert(id.to_string(), instance.clone());
        self.lock_archived().remove(id);
        let _ = std::fs::remove_file(self.archive_dir().join(format!("{id}.json")));
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

    async fn spawn_agent(
        &self,
        params: SpawnParams,
    ) -> rk_core::Result<crate::agents::AgentRecord> {
        self.supervisor.spawn_async(params).await
    }

    /// This instance's per-run budget cap (from the workflow's `budget:`), used
    /// as the dispatch preflight ceiling on every spawn it makes.
    pub(crate) fn instance_budget(&self, id: &str) -> Option<f64> {
        self.lock().get(id).and_then(|i| i.instance_max_usd)
    }

    pub(crate) fn coordinator(&self, id: &str) -> Option<String> {
        self.lock().get(id).and_then(|i| i.coordinator.clone())
    }

    fn store(&self, instance: Instance) {
        let mut instances = self.lock();
        instances.insert(instance.id.clone(), instance.clone());
        if let Err(error) = self.persist(&instance) {
            warn!(instance = %instance.id, %error, "failed to persist initial workflow state; skipping coordinator event");
            return;
        }
        self.emit_state_event(&instance, "started");
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
        let mut instances = self.lock();
        if let Some(instance) = instances.get_mut(id) {
            let before = instance.clone();
            mutate(instance);
            if *instance == before {
                return false;
            }
            instance.revision = instance.revision.saturating_add(1);
            let snapshot = instance.clone();
            if let Err(error) = self.persist(&snapshot) {
                *instance = before;
                warn!(instance = %id, %error, "failed to persist workflow state; skipping coordinator event");
                return false;
            }
            self.emit_state_event(&snapshot, reason);
            true
        } else {
            false
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
        std::fs::create_dir_all(dir)?;
        let path = dir.join(format!("{}.json", instance.id));
        let data = serde_json::to_vec_pretty(instance)?;
        let sequence = PERSIST_SEQ.fetch_add(1, Ordering::Relaxed);
        let tmp = dir.join(format!("{}.json.tmp-{}-{sequence}", instance.id, std::process::id()));
        std::fs::write(&tmp, data)?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
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

fn parse_duration(s: &str) -> rk_core::Result<Duration> {
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
            params: params(&[("ticket", "TKT-1")]),
            depth: 0,
            started_at: Utc::now(),
            completed_at: None,
            archived_at: None,
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
        assert!(resolve_worktree_cwd(&worktree, Some(temp.path().to_str().unwrap()), &ctx).is_err());
    }

    #[tokio::test]
    async fn run_output_cap_kills_a_noisy_child() {
        let child = tokio::process::Command::new("sh")
            .arg("-c")
            .arg("yes noisy")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let error = collect_child_output(
            child,
            Duration::from_secs(2),
            "yes noisy",
            "2s",
            OnTimeout::Fail,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("output exceeds"));
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
}

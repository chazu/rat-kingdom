//! Workflow execution: sequential step machine over the supervisor and the
//! tuplespace. Definitions come from rk-workflow (cue CLI); this module owns
//! instances, context threading, and step semantics.

use crate::agents::AgentState;
#[cfg(test)]
use crate::managed_verification::*;
pub(crate) use crate::managed_verification::{
    parse_duration, reap_stale_managed_children, validate_retry_on_fail, verification_proof_key,
    OnTimeout, ResolvedRun, RunProgress, DEFAULT_RUN_TIMEOUT,
};
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
use std::collections::HashMap;
use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{info, warn};

/// A boxed future for hand-rolled async recursion (nested `when` / `repeat`).
type StepFuture<'a> = Pin<Box<dyn Future<Output = rk_core::Result<Flow>> + Send + 'a>>;

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

/// Whether a spawn attempt failed because the fleet-WIP ceiling, or this
/// repository's own implementation/review capacity lane
/// (TKT-01M0P2KM83Y4MD5QYETR3JCKF2), had no free slot at the moment
/// `Supervisor::spawn` atomically checked (as opposed to a genuine spawn
/// failure) — a `Step::Spawn` retries on this rather than failing the step.
/// All three refusal reasons are treated identically for retry purposes: the
/// distinct error strings exist only so a caller that DOES want to
/// distinguish them (for observability) can.
pub(crate) fn is_fleet_wip_refusal(error: &rk_core::Error) -> bool {
    matches!(error, rk_core::Error::Other(msg) if msg == FLEET_WIP_CAP_REFUSED
        || msg == crate::supervisor::IMPLEMENTATION_LANE_REFUSED
        || msg == crate::supervisor::REVIEW_LANE_REFUSED)
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
    /// Runtime-owned identity of an exact review request. Persisted with the
    /// workflow instance so a daemon restart or a stale installed workflow
    /// definition cannot strip the binding before the reviewer spawn.
    #[serde(default)]
    pub review: Option<rk_core::review::ReviewContext>,
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
    /// Fleet-wide concurrent-agent ceiling shared with the continuous-drain
    /// autoscaler (`[drain] max_wip`): a `spawn` step waits for a free slot
    /// under the same cap a drain refill respects, so workflow-spawned agents
    /// (e.g. landing reviewers) cannot unboundedly outrun it. Zero (the
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
    /// Workflow reads and events share the registered scope exported to its
    /// agents as RK_REPO. Basenames remain a fallback for unregistered repos.
    fn repo_scope(&self, repo: &str) -> String {
        crate::repos::RepoRegistry::load(&self.layout.home().join("repos.json"))
            .ok()
            .and_then(|registry| {
                registry
                    .get_by_path(Path::new(repo))
                    .map(|record| record.name.clone())
            })
            .unwrap_or_else(|| repo_name_of(repo))
    }

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
            fleet_wip_cap,
            instances: Mutex::new(HashMap::new()),
            archived: Mutex::new(HashMap::new()),
        }
    }

    /// Resolve `<name>` to a definition file: `<repo>/.rk/workflows/<name>.cue`
    /// wins over `~/.rat-kingdom/workflows/<name>.cue`. Direct `.cue` paths are
    /// accepted only when they stay inside one of those two roots.
    pub fn find_definition(&self, name: &str, repo: &str) -> rk_core::Result<PathBuf> {
        let name = rk_core::landing_names::canonical(name);
        if name == "candidate-review" {
            // Preserve repository-local customization across the rename. New
            // installs use the canonical file; existing installs keep working
            // until their definitions are migrated, including after restart.
            for root in [
                PathBuf::from(repo).join(".rk/workflows"),
                self.layout.workflows_dir(),
            ] {
                for candidate_name in [name, rk_core::landing_names::LEGACY_REVIEW_WORKFLOW] {
                    let candidate = root.join(format!("{candidate_name}.cue"));
                    if candidate.exists() {
                        return definition_inside_roots(
                            &candidate,
                            repo,
                            &self.layout.workflows_dir(),
                        )
                        .ok_or_else(|| {
                            rk_core::Error::other("review definition is outside workflow roots")
                        });
                    }
                }
            }
        }
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
            None,
        )
    }

    /// Launch a review workflow with daemon-owned correlation metadata. The
    /// binding is persisted independently of the CUE definition so an older
    /// installed copy cannot turn a correctly emitted verdict into no-verdict.
    pub(crate) fn run_review_owned_with_id(
        self: &Arc<Self>,
        instance_id: String,
        name: &str,
        repo: &str,
        params: HashMap<String, Value>,
        review: rk_core::review::ReviewContext,
    ) -> rk_core::Result<Instance> {
        self.run_owned_with_id_and_schedule(
            instance_id,
            name,
            repo,
            params,
            None,
            None,
            None,
            Some(review),
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
            None,
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
        review: Option<rk_core::review::ReviewContext>,
    ) -> rk_core::Result<Instance> {
        let file = self.find_definition(name, repo)?;
        let definition_digest = definition_digest(&file)?;
        let workflow = rk_workflow::load(&file, &params)?;
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
            context: WorkflowContext {
                review,
                ..WorkflowContext::default()
            },
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
            self.repo_scope(repo),
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
            let swept = self.sweep_instance_agents(id).await;
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

    /// Every agent this instance owns, terminal or still live, released in
    /// one pass: [`Supervisor::dismiss_orphaned_instance_agents`] only ever
    /// touches an already-terminal (`Completed`/`Failed`) record — by
    /// design, so an ordinary completion never races a still-working agent
    /// — but that means it structurally cannot release an agent that is
    /// STILL `Running`/`Paused`/`Spawning` when its owning instance goes
    /// terminal, e.g. a reviewer stuck reconnecting through a transport
    /// outage (the 2026-08-21 incident this closes: a Codex reviewer stayed
    /// `Running` and reconnecting well after its owning candidate-review
    /// workflow had already timed out, holding fleet capacity
    /// indefinitely). [`Supervisor::dismiss_live_instance_agents`] is the
    /// live-state counterpart; running both here means a workflow's own
    /// terminal transition (ordinary completion, the B8 stale-timeout
    /// sweep, or this module's error path) always durably releases every
    /// agent it owns, regardless of that agent's own liveness state at the
    /// moment — "transport-unhealthy does not count as healthy liveness
    /// past the workflow ceiling".
    async fn sweep_instance_agents(&self, id: &str) -> Vec<(String, bool)> {
        let mut swept = self.supervisor.dismiss_orphaned_instance_agents(id).await;
        swept.extend(self.supervisor.dismiss_live_instance_agents(id).await);
        swept
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
            self.repo_scope(&instance.repo),
            "workflow_failed",
            "daemon".to_string(),
            json!({"instance": id, "workflow": instance.workflow, "error": error_text}),
        ));
        if self.finalize_cleanup_enabled {
            let swept = self.sweep_instance_agents(id).await;
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
                        self.repo_scope(&instance.repo),
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
        self.fail_children_owned_by_terminal_parents();
        // Only top-level (depth 0) instances resume standalone. A linked nested
        // child is re-driven by its parent's resumed `sub_workflow` step, which
        // rejoins the same durable child id. Resuming it here as well would
        // execute the same interrupted step twice.
        let resumable: Vec<Instance> = self
            .lock()
            .values()
            .filter(|instance| instance.status == InstanceStatus::Running && instance.depth == 0)
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

    /// Exact persisted parent-to-child links remain authoritative across a
    /// restart. A terminal parent can no longer rejoin its still-running child,
    /// so fail that child instead of leaving an unresumable workflow alive.
    /// This deliberately does not infer ownership for unlinked snapshots.
    fn fail_children_owned_by_terminal_parents(&self) {
        let links: Vec<(String, String)> = self
            .list()
            .into_iter()
            .filter(|parent| parent.status.is_terminal())
            .filter_map(|parent| {
                parent
                    .context
                    .active_subworkflow
                    .map(|child| (parent.id, child))
            })
            .collect();

        for (parent_id, child_id) in links {
            if !self
                .status(&child_id)
                .is_some_and(|child| child.status == InstanceStatus::Running)
            {
                continue;
            }
            if let Err(error) = self.try_update_with_reason(
                &child_id,
                "sub_workflow_parent_terminal",
                |child| {
                    child.status = InstanceStatus::Failed;
                    child.error = Some(format!(
                        "sub_workflow parent {parent_id} became terminal before rejoining this child"
                    ));
                    child.completed_at = Some(Utc::now());
                },
            ) {
                warn!(parent = %parent_id, child = %child_id, %error, "could not persist linked sub-workflow child failure");
                self.fail_recovery_in_memory(
                    &child_id,
                    format!("linked child failure persistence failed: {error}"),
                );
            }
        }
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
                    let routing = tiers.chained(&self.tier_routing);
                    let resolved = resolve(
                        spawn,
                        &routing,
                        agents,
                        &self.global_agents,
                        &self.default_harness,
                    )?;
                    let title = interpolate(&spawn.task.title, &ctx);
                    let prompt = spawn
                        .task
                        .description
                        .as_ref()
                        .map(|d| interpolate(d, &ctx));
                    let review = match (&ctx.review, &spawn.review) {
                        (Some(runtime), Some(declared)) if runtime != declared => {
                            return Err(rk_core::Error::other(format!(
                                "review spawn binding disagrees with runtime context: runtime \
                                 {runtime:?}, workflow {declared:?}"
                            )));
                        }
                        (Some(runtime), _) => Some(runtime.clone()),
                        (None, declared) => declared.clone(),
                    };
                    let params = SpawnParams {
                        repo: repo.to_string(),
                        task: title,
                        prompt,
                        role: spawn.role.clone(),
                        coordination: spawn.coordination.clone(),
                        harness: Some(resolved.harness),
                        parent: None,
                        base: spawn.branch.clone().or(ctx.active_branch.clone()),
                        review,
                        model: resolved.model,
                        permission_mode: resolved.permission_mode,
                        attach: false,
                        workflow_instance: Some(id.to_string()),
                        coordinator: self.coordinator(id),
                        instance_max_usd: self.instance_budget(id),
                        profile: None,
                        resolved_profile: None,
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
                        .await_result(
                            &agent,
                            ctx.active_agent_spawn,
                            deadline,
                            "wait",
                            &wait.timeout,
                        )
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
                    let expected_spawn = ctx.active_agent_spawn.ok_or_else(|| {
                        rk_core::Error::other("dismiss step has no exact active-agent spawn")
                    })?;
                    let landing = self.supervisor.status(&agent).and_then(|record| {
                        record
                            .branch
                            .map(|branch| (record.repo_root, branch, record.target_branch))
                    });
                    let dismissed = self
                        .supervisor
                        .dismiss_checked(&agent, expected_spawn, true)
                        .await?;
                    let outcome = if dismiss.no_merge {
                        dismissed
                    } else {
                        let (repo_root, branch, target) = landing.ok_or_else(|| {
                            rk_core::Error::other(
                                "dismiss-and-land step resolved no branch to submit",
                            )
                        })?;
                        self.supervisor
                            .land(&repo_root, &branch, &target, false, None)
                            .await?
                    };
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
                                    self.repo_scope(repo),
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
                    let scope = read.scope.clone().unwrap_or_else(|| self.repo_scope(repo));
                    // Bind the read, or it is satisfied by a stranger. Bare
                    // (category, scope, identity) is NOT an identity: two
                    // instances of one workflow on one repo share it by
                    // construction, and "newest wins" then routes an instance on
                    // a tuple written for its peer. Two discriminators cure it,
                    // by which key the wanted tuple actually carries:
                    //
                    // - `fromAgent` (TKT-161) — what an agent THIS instance
                    //   spawned wrote. The reactor fires `landing` per rat
                    //   completion, so concurrent reviewers write
                    //   `artifact/<repo>/review` at the same time and an unbound
                    //   read can hand a landing the OTHER landing's verdict to
                    //   land on. Cured by the rat generation's exact spawn id.
                    // - `fromInstance` (TKT-172) — what was written FOR this
                    //   run. The `workflow_approval` event behind an approval
                    //   gate is the case: two gated instances on one repo, one
                    //   approved and one rejected, and an unbound read routes
                    //   both on whichever decision landed last. Cured by the
                    //   instance id (`for_workflow_instance`) — the same
                    //   predicate the gate itself waits on, so gate and read
                    //   cannot disagree about whose decision this is.
                    // - `forCommit` (landing Phase 2 verdict cache) — what was
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
                        if ctx.active_agent.is_none() {
                            return Err(rk_core::Error::other(
                                "read step has `fromAgent` but no active agent; only a step \
                                 after a `spawn` can bind a read to its author",
                            ));
                        }
                        let spawn = ctx.active_agent_spawn.ok_or_else(|| {
                            rk_core::Error::other(
                                "read step has `fromAgent` but no exact active agent generation",
                            )
                        })?;
                        Pattern::for_spawn(category, read.identity.clone(), spawn)
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
                    // `onTimeout: "continue"` (landing Phase 2 verdict cache) lets
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
                    self.require_allowed_target(&target, repo)?;
                    let result = self
                        .supervisor
                        .land(
                            std::path::Path::new(repo),
                            &branch,
                            &target,
                            land.keep_branch,
                            None,
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
                    self.require_allowed_target(&target, repo)?;
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
                review: None,
                model: resolved.model,
                permission_mode: resolved.permission_mode,
                attach: false,
                workflow_instance: Some(id.to_string()),
                coordinator: self.coordinator(id),
                instance_max_usd: instance_cap,
                profile: None,
                resolved_profile: None,
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
    /// The tuple is keyed by the rat generation's minted spawn id, so neither
    /// a predecessor nor a newer namesake can satisfy the read. Missing exact
    /// identity fails closed.
    ///
    /// TKT-160: generation identity separates generations, not the TURNS within
    /// one, and a
    /// harness reports a result per turn — so this read used to be satisfied by
    /// a mid-flight "tests still running" turn milliseconds after the rat
    /// started. That is fixed on the producer side (a generation now publishes
    /// exactly one `harness_result`, the one it finished on — see
    /// `Supervisor::claim_completion`), because `wait` is not the only reader:
    /// the reactor's landing trigger and the ticket auto-close read the same
    /// event. Do not reintroduce a per-turn `harness_result`.
    ///
    fn result_pattern(
        &self,
        agent: &str,
        spawn: Option<rk_core::id::SpawnId>,
    ) -> rk_core::Result<Pattern> {
        let spawn = spawn.ok_or_else(|| {
            rk_core::Error::other(format!(
                "agent {agent} has no exact generation in the workflow snapshot"
            ))
        })?;
        Ok(Pattern::for_spawn(Category::Event, "harness_result", spawn))
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
    ///
    /// A pre-work transport-outage episode (`AgentRecord::transport_outage`)
    /// is a THIRD, separate case: `is_auto_respawn_candidate`
    /// (`crates/rk-daemon/src/supervisor.rs`) deliberately excludes it from
    /// the ordinary `RespawnState` tracking the check below reads, so
    /// `respawn_exhausted` never saw this generation and reads it as "not
    /// exhausted" by default — which would keep this wait blocked for the
    /// full step timeout even after the outage's own retry ceiling
    /// (`TransportOutageState::ceiling_hit`) has already fired, on a
    /// non-retryable class that was never going to be retried at all. Read
    /// that ceiling directly instead of falling through to a check that was
    /// never tracking this episode.
    fn abandoned(&self, agent: &str) -> Option<String> {
        let record = self.supervisor.status(agent)?;
        if record.state == AgentState::Orphaned || !record.crashed_without_reporting() {
            return None;
        }
        if let Some(outage) = &record.transport_outage {
            if !outage.ceiling_hit {
                return None; // the transport-retry sweep may still bring it back
            }
        } else if record.state == AgentState::Failed
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
        agent: &str,
        spawn: Option<rk_core::id::SpawnId>,
        deadline: tokio::time::Instant,
        step: &str,
        timeout: &str,
    ) -> rk_core::Result<Value> {
        let pattern = self.result_pattern(agent, spawn)?;
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
                // The result is this generation's by construction: the
                // pattern binds its exact spawn id. The liveness gate also
                // rejects any future path that lands a foreign result here.
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
                self.await_result(&fa.agent, fa.spawn, deadline, "wait_all", &wait_all.timeout)
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
    /// cleaned up and, unless `no_merge`, its preserved branch is separately
    /// submitted to the landing queue. The caller then clears the fan-out set.
    /// Aggregates into `{count, merged, parked, errors, all_merged, results}`.
    /// A hard cleanup or landing failure fails the step, symmetric to how
    /// `wait_all` fails on a timeout.
    ///
    /// When `dismiss_all.only_clean` is set, this reads the preceding
    /// `wait_all` aggregate (`previous_result`) and lands *only* the branches
    /// of rats that finished clean (`is_error: false`), parking every failed
    /// rat's branch for review instead of failing the whole
    /// batch. A branch parked because its rat failed is counted in `parked`
    /// (distinct from a `merged: false` landing result), and `all_merged`
    /// stays `merged == count`, so a following `evaluate {all_merged: true}`
    /// still surfaces the failure in `rk inbox` — but only after the clean
    /// branches have already landed. `only_clean` requires a preceding
    /// `wait_all` (its per-agent results supply the clean/failed signal); it
    /// fails the step if none is present rather than silently merging all.
    ///
    /// As with [`join`](Self::join), only a missing `for_each` (`None`) fails
    /// here; an empty fan-out lands nothing and aggregates to `count: 0,
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
                "dismiss_all step with no preceding for_each: there is no fan-out to clean up",
            )
        })?;
        // With only_clean, the per-agent no_merge is driven by the preceding
        // wait_all's results: an agent is parked (no_merge=true) unless its
        // harness_result reported is_error:false. Without a preceding wait_all
        // there is no clean/failed signal, so the flag is meaningless — fail
        // rather than silently land everything.
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
            info!("dismiss_all over an empty fan-out; nothing to clean up or land");
        }
        // Clean up all agents concurrently, then submit each eligible branch
        // independently; the landing pipeline serializes same-target updates.
        let mut set = tokio::task::JoinSet::new();
        for fa in fanout {
            let supervisor = Arc::clone(&self.supervisor);
            let agent = fa.agent.clone();
            let spawn = fa.spawn.ok_or_else(|| {
                rk_core::Error::other(format!(
                    "dismiss_all member {} has no exact spawn id",
                    fa.agent
                ))
            })?;
            let landing = supervisor.status(&agent).and_then(|record| {
                record
                    .branch
                    .map(|branch| (record.repo_root, branch, record.target_branch))
            });
            // Base no_merge from the step, plus: under only_clean, park (don't
            // land) any agent not in the clean set.
            let parked = clean
                .as_ref()
                .is_some_and(|clean| !clean.contains(&fa.agent));
            let no_merge = dismiss_all.no_merge || parked;
            set.spawn(async move {
                let outcome = match supervisor.dismiss_checked(&agent, spawn, true).await {
                    Ok(dismissed) if no_merge => Ok(dismissed),
                    Ok(_) => match landing {
                        Some((repo_root, branch, target)) => {
                            supervisor
                                .land(&repo_root, &branch, &target, false, None)
                                .await
                        }
                        None => Err(rk_core::Error::other(
                            "dismiss_all could not resolve a branch to submit",
                        )),
                    },
                    Err(error) => Err(error),
                };
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

        self.verification()
            .run(crate::managed_verification::CheckExecution {
                admission_timeout: None,
                id,
                repo,
                agent: &agent,
                dir: &dir,
                command: &command,
                resolved: &resolved,
                env: &env,
                timeout,
                previous_result: ctx.previous_result.as_ref(),
                progress: None,
            })
            .await
    }

    pub(crate) fn verification(&self) -> crate::managed_verification::ManagedVerification<'_> {
        crate::managed_verification::ManagedVerification::new(
            &self.layout,
            &self.space,
            self.supervisor.verification_resources(),
            self.supervisor.shared_cargo_target_enabled(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn verify_repo_check(
        &self,
        agent: &str,
        dir: &Path,
        repo_name: &str,
        check_name: &str,
        generation: Option<rk_core::id::SpawnId>,
        request_key: &str,
        task: Option<&str>,
    ) -> rk_core::Result<Value> {
        self.verification()
            .verify_repo_check(
                agent,
                dir,
                repo_name,
                check_name,
                generation,
                request_key,
                task,
            )
            .await
    }

    pub(crate) fn lookup_verification_proof(
        &self,
        repo_name: &str,
        candidate_sha: &str,
        check: &rk_workflow::Check,
    ) -> Option<Value> {
        self.verification()
            .lookup_verification_proof(repo_name, candidate_sha, check)
    }

    fn require_allowed_target(&self, target: &str, repo: &str) -> rk_core::Result<()> {
        // `Repo::discover` shells out to git; `run_step` runs this
        // synchronously inside its own async future, so keep the subprocess
        // off a multi-thread worker. Current-thread tests cannot use
        // `block_in_place`, hence the runtime flavor check.
        let on_multithread = tokio::runtime::Handle::try_current()
            .map(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
            .unwrap_or(false);
        let repo_path = Path::new(repo);
        let git_repo = if on_multithread {
            tokio::task::block_in_place(|| rk_git::Repo::discover(repo_path))
        } else {
            rk_git::Repo::discover(repo_path)
        }?;
        let registry = crate::repos::RepoRegistry::load(&self.layout.home().join("repos.json"))?;
        let record = registry.get_by_path(git_repo.root()).ok_or_else(|| {
            rk_core::Error::other(format!(
                "repository '{}' is not registered; run `rk repo add` and activate .rk/repo.cue",
                git_repo.root().display()
            ))
        })?;
        let approved = record.activated_policy.as_ref().ok_or_else(|| {
            rk_core::Error::other(format!(
                "repository '{0}' has no activated .rk/repo.cue policy; run `rk repo onboard start {0}` before workflow landing",
                record.name,
            ))
        })?;
        let policy_target = approved.policy.delivery.target.as_str();
        if policy_target == "agent-base" || policy_target == target {
            return Ok(());
        }
        Err(rk_core::Error::other(format!(
            "workflow target '{target}' does not match activated repository policy target '{policy_target}'"
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
                let check = self.verification().find_check(repo, name)?;
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
        let scope = Some(self.repo_scope(repo));
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
            self.repo_scope(&instance.repo),
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
            "repo": self.repo_scope(&instance.repo),
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
                self.repo_scope(&instance.repo),
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

pub(crate) fn repo_name_of(repo: &str) -> String {
    PathBuf::from(repo)
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "repo".into())
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
                "dismiss + land".into()
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

    /// The landing escalation checks (`rk out need`, `rk ticket new`) run in
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
    /// every landing escalation failure.
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
        let base = work_key("/dev/repo", "landing", &params(&[("ticket", "TKT-1")]));

        // A retry is a different RUN of the same WORK: nothing about the run —
        // its id, its start time, its outcome — is an input here, so re-deriving
        // from the same three fields must land on the same key.
        assert_eq!(
            base,
            work_key("/dev/repo", "landing", &params(&[("ticket", "TKT-1")]))
        );

        // Param insertion order is a HashMap accident, not a difference in work.
        let mut reordered = HashMap::new();
        reordered.insert("b".to_string(), json!("2"));
        reordered.insert("a".to_string(), json!("1"));
        let mut forward = HashMap::new();
        forward.insert("a".to_string(), json!("1"));
        forward.insert("b".to_string(), json!("2"));
        assert_eq!(
            work_key("/dev/repo", "landing", &forward),
            work_key("/dev/repo", "landing", &reordered)
        );

        // Each of the three inputs genuinely separates work.
        assert_ne!(
            base,
            work_key("/dev/other", "landing", &params(&[("ticket", "TKT-1")]))
        );
        assert_ne!(
            base,
            work_key("/dev/repo", "reactor", &params(&[("ticket", "TKT-1")]))
        );
        assert_ne!(
            base,
            work_key("/dev/repo", "landing", &params(&[("ticket", "TKT-2")]))
        );
        assert_ne!(base, work_key("/dev/repo", "landing", &HashMap::new()));
    }

    /// The length prefixes are load-bearing, not cosmetic: without them a repo
    /// path ending in the delimiter could be re-cut into a different
    /// (repo, workflow) pair with identical material, and a false match here
    /// retires a real failure from the operator's inbox.
    #[test]
    fn work_key_cannot_be_re_cut_across_its_fields() {
        assert_ne!(
            work_key("/dev/repo|x", "landing", &HashMap::new()),
            work_key("/dev/repo", "x|landing", &HashMap::new())
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
            workflow: "landing".into(),
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
            definition: "landing".into(),
            definition_digest: "aaaa".into(),
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

    /// TKT-01M0H5JNZQKZ35V87Q4H4N3EPH: RK reports a check command's OWN exit
    /// status, unchanged by RK's own successful output consumer.
    ///
    /// RK does pipe every check: `spawn_check_child` hands the child's stdout
    /// and stderr to `read_capped`, a consumer that succeeds (and truncates)
    /// entirely independently of whether the check passed. That consumer is
    /// the one masking layer RK actually owns, and this pins that it never
    /// launders a failure into a pass — including when the check floods it
    /// past `MAX_RUN_OUTPUT_BYTES`, where output is still bounded and the
    /// exit code is still 3.
    ///
    /// Expectations here are ABSOLUTE, deliberately not compared against a
    /// reference `sh -c` run of the same command: a reference oracle agrees
    /// with RK on the masked case by construction (both report 0 for
    /// `exit 3 | cat`) and so asserts nothing about masking at all.
    #[tokio::test]
    async fn collect_child_output_reports_a_failing_check_through_its_own_output_consumer() {
        // (declared command, expected exit code, expected stdout truncation)
        let cases: [(&str, Option<i32>, bool); 7] = [
            ("exit 3", Some(3), false),
            ("true", Some(0), false),
            // A failing check whose output is piped to a SUCCESSFUL consumer —
            // here RK's own `read_capped`, which reads to EOF and returns Ok
            // no matter what the child did. 3 must reach the gate.
            ("echo noisy; exit 3", Some(3), false),
            // ...and the same with the volume that makes RK's consumer visibly
            // do work: >MAX_RUN_OUTPUT_BYTES of stdout. Bounded output is
            // retained (truncated) AND the failure still surfaces as 3.
            ("seq 1 60000; exit 3", Some(3), true),
            // The DECLARED command's own pipe masks its failing stage behind
            // `cat` — POSIX sh's documented last-stage-wins. RK reports that
            // verbatim: the repo author's shell semantics are theirs to choose,
            // and silently forcing `pipefail` over them inverts real checks
            // (`! producer | grep -q pat` starts passing when the pattern
            // MATCHES, once the producer is big enough to take SIGPIPE).
            ("exit 3 | cat", Some(0), false),
            // ...so the unmasking has to be the author's, and RK's `sh -c`
            // wrap must not defeat it when they ask. Both forms the completion
            // protocol tells an agent to use reach the gate as 3, not cat's 0.
            ("bash -c 'set -o pipefail; exit 3 | cat'", Some(3), false),
            (
                "bash -c 'exit 3 | cat; exit ${PIPESTATUS[0]}'",
                Some(3),
                false,
            ),
        ];
        for (command, expected_code, expected_truncated) in cases {
            let child = tokio::process::Command::new("sh")
                .arg("-c")
                .arg(command)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            let outcome = collect_child_output(child, Duration::from_secs(30), command)
                .await
                .unwrap();
            match outcome {
                RunOutcome::Completed {
                    status,
                    stdout,
                    stdout_truncated,
                    ..
                } => {
                    assert_eq!(
                        status.code(),
                        expected_code,
                        "`{command}`: RK must report the check's own exit status, not one \
                         laundered by its own output consumer"
                    );
                    assert_eq!(
                        stdout_truncated, expected_truncated,
                        "`{command}`: output bounding must be unchanged by the exit-status path"
                    );
                    assert!(
                        stdout.len() <= MAX_RUN_OUTPUT_BYTES,
                        "`{command}`: captured stdout must stay bounded"
                    );
                }
                RunOutcome::TimedOut => panic!("`{command}` must not time out"),
            }
        }
    }

    #[tokio::test]
    async fn signal_death_classifies_as_infra_on_no_exit_code_not_on_decoded_signal() {
        // A real child killed by a signal: `status.code()` comes back `None`.
        // `decode_run_outcome`'s classifier is `no_exit_code` — `status.code().is_none()`
        // — deliberately independent of whether this platform can ALSO name
        // which signal it was, so a runner-loss shape with no decodable
        // signal (or a non-Unix target) still gets classified as an
        // infrastructure death rather than silently falling back to an
        // ordinary "fail" that would never earn a retry
        // (TKT-01M0FXGQMA10JYCV9QCGEAK4TT).
        let command = "kill -9 $$";
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
        assert!(
            matches!(&outcome, RunOutcome::Completed { status, .. } if status.code().is_none()),
            "a self-`kill -9` must complete with no exit code: {outcome:?}"
        );
        let resolved = ResolvedRun {
            command: command.into(),
            cwd: None,
            expect_exit: None,
            timeout: "10s".into(),
            on_timeout: OnTimeout::Fail,
            environment_policy: rk_workflow::CheckEnvironmentPolicy::Inherit,
            retry_on_fail: 0,
            shared_cargo_target: false,
        };
        let (exit, _, _, _, _, timed_out, no_exit_code, _signal) =
            decode_run_outcome(outcome, command, &resolved);
        assert!(!timed_out);
        assert!(
            no_exit_code,
            "the infra classifier must fire on the absent exit code alone, exit reported as {exit}"
        );
    }

    #[tokio::test]
    async fn timeout_kills_the_whole_process_group_not_just_the_wrapper() {
        // No `.kill_on_drop(true)` — mirroring `spawn_check_child` exactly:
        // the wrapper's own pid alone would never reach a grandchild it
        // backgrounds (mise/cargo/rustc in the real case) anyway; only the
        // whole process group being signalled does, which `ProcessGroupGuard`
        // alone is responsible for.
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("grandchild.pid");
        let command = format!("sleep 600 & echo $! > {}; wait", pid_file.display());
        let mut cmd = tokio::process::Command::new("sh");
        cmd.arg("-c")
            .arg(&command)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
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

    /// The exact real-world gap (TKT-01M0PN2JSN24AHGQHFJ4XGAVKD): a check
    /// command that moves part of ITS OWN work into a second, freshly
    /// created process group — precisely what a live smoke test observed
    /// `mise run verify` do (the leader stayed in `spawn_check_child`'s
    /// group; a nested shell/cargo pair ended up in a NEW group of their
    /// own). The old `kill(-leader_pid, SIGKILL)` reached only the leader's
    /// group; the nested one survived, reparented to init. Modeled here with
    /// `perl`'s `setpgrp(0, 0)` (portable, no `setsid`(1) binary required —
    /// absent on macOS) instead of a real `mise`, since what's under test is
    /// `ProcessGroupGuard` walking the live descendant tree, not `mise`
    /// itself.
    #[tokio::test]
    async fn timeout_kills_a_nested_process_group_the_check_command_moved_itself_into() {
        let temp = tempfile::tempdir().unwrap();
        let nested_pid_file = temp.path().join("nested.pid");
        let script = temp.path().join("detach.pl");
        std::fs::write(
            &script,
            "setpgrp(0, 0);\n\
             open(my $fh, '>', $ARGV[0]) or die $!;\n\
             print $fh $$;\n\
             close $fh;\n\
             sleep 300;\n",
        )
        .unwrap();
        let command = format!(
            "perl {} {} & wait",
            script.display(),
            nested_pid_file.display()
        );
        let mut cmd = tokio::process::Command::new("sh");
        cmd.arg("-c")
            .arg(&command)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .process_group(0);
        let child = cmd.spawn().unwrap();

        let outcome = collect_child_output(child, Duration::from_millis(300), &command)
            .await
            .unwrap();
        assert!(matches!(outcome, RunOutcome::TimedOut));

        // Give the perl child a moment to actually call `setpgrp` and write
        // its own pid before asserting anything — this races the guard's
        // kill signal against perl's own startup, not just against the
        // liveness check below.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !nested_pid_file.exists() && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let pid_text = std::fs::read_to_string(&nested_pid_file).unwrap();
        let nested_pid: i32 = pid_text.trim().parse().unwrap();

        // Give the group-kill signal(s) a moment to land, then confirm the
        // process that detached into its own group is actually dead, not
        // merely orphaned under init.
        tokio::time::sleep(Duration::from_millis(300)).await;
        // SAFETY: signal 0 only probes liveness/permission; it affects nothing.
        let alive = unsafe { libc::kill(nested_pid, 0) == 0 };
        assert!(
            !alive,
            "a descendant that moved itself into a new process group survived the gate timeout"
        );
    }

    /// Polls `ps` STAT rather than raw `kill(pid, 0)`: these sleepers are
    /// spawned as DIRECT children of the test process, so a signal-0
    /// liveness check keeps reporting "alive" for a zombie — killed but not
    /// yet reaped by its parent (this process, which never calls `wait` on
    /// it) — exactly the gotcha `rk-harness/src/fake.rs`'s own `still_running`
    /// helper documents. `None`/empty STAT or a leading `Z` both mean gone.
    fn pid_alive(pid: u32) -> bool {
        let output = std::process::Command::new("ps")
            .args(["-o", "stat=", "-p", &pid.to_string()])
            .output()
            .expect("failed to invoke `ps`");
        if !output.status.success() {
            return false;
        }
        let stat = String::from_utf8_lossy(&output.stdout).trim().to_string();
        !(stat.is_empty() || stat.starts_with('Z'))
    }

    /// A real process, its own process-group leader (mirroring exactly how
    /// `spawn_check_child` spawns a check), that just sleeps — standing in
    /// for a managed check child in every test below.
    ///
    /// Deliberately `sleep 300 & wait`, not a bare `sleep 300`: `sh -c
    /// '<single simple command>'` is exactly the shape `sh`'s tail-call
    /// optimization targets — with nothing left to do after the command,
    /// `sh` `exec`s directly into it instead of forking, which keeps the
    /// pid but silently changes `comm` from `sh` to `sleep`. That exec can
    /// land at any point after `spawn()` returns, racing
    /// `ManagedChildMarker::create`'s `process_signature` capture right
    /// after it: if the marker was written before the exec but the reap
    /// sweep's re-check runs after, `reap_stale_managed_children`'s
    /// identity fence sees a changed `comm` and — correctly, by design —
    /// refuses to signal what looks like a different process (TKT-
    /// 01M0PN18QNWPKKARFV4KRWGP6S). Backgrounding the sleep and `wait`ing
    /// on it gives `sh` more work to do after spawning it, which suppresses
    /// the tail-call exec: the leader's `comm` stays `sh` for its entire
    /// life, so its recorded signature can never drift out from under it.
    /// Mirrors `spawn_sleeper_with_nested_group`'s already-stable `& wait`
    /// shape below.
    fn spawn_sleeper() -> tokio::process::Child {
        tokio::process::Command::new("sh")
            .arg("-c")
            .arg("sleep 300 & wait")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap()
    }

    /// Like [`spawn_sleeper`], but the leader also backgrounds a `perl`
    /// descendant that moves ITSELF into a second, freshly created process
    /// group before sleeping (`setpgrp(0, 0)` — portable, no `setsid`(1)
    /// binary required, which macOS lacks) — modeling exactly what a live
    /// smoke test found `mise run <task>` do under a real managed check
    /// leader (TKT-01M0PN2JSN24AHGQHFJ4XGAVKD). Returns the leader `Child`
    /// together with the nested descendant's own pid, only once that
    /// descendant has actually reported it.
    async fn spawn_sleeper_with_nested_group(dir: &Path) -> (tokio::process::Child, u32) {
        let nested_pid_file = dir.join("nested.pid");
        let script = dir.join("detach.pl");
        std::fs::write(
            &script,
            "setpgrp(0, 0);\n\
             open(my $fh, '>', $ARGV[0]) or die $!;\n\
             print $fh $$;\n\
             close $fh;\n\
             sleep 300;\n",
        )
        .unwrap();
        let command = format!(
            "perl '{}' '{}' & wait",
            script.display(),
            nested_pid_file.display()
        );
        let child = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&command)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap();
        let nested_pid: u32 = loop {
            if let Ok(text) = std::fs::read_to_string(&nested_pid_file) {
                if let Ok(pid) = text.trim().parse() {
                    break pid;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        (child, nested_pid)
    }

    /// The nested-group counterpart to the identity-fenced reap test below
    /// (TKT-01M0PN2JSN24AHGQHFJ4XGAVKD): the daemon only ever marks the
    /// LEADER's pid (`ManagedChildMarker::create` is called once, from
    /// `spawn_check_child`, on the pid `.process_group(0)` made a leader) —
    /// never anything it later forks. A daemon generation that dies with a
    /// check still running whose command (`mise run <task>` in production)
    /// had already moved part of its own work into a SECOND process group
    /// must still have that nested group reaped, found by walking the
    /// leader's live descendant tree at reap time, not just the leader's own
    /// group.
    #[tokio::test]
    async fn reap_stale_managed_children_kills_a_nested_process_group_the_orphaned_child_moved_itself_into(
    ) {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        let script_dir = tempfile::tempdir().unwrap();
        let (mut child, nested_pid) = spawn_sleeper_with_nested_group(script_dir.path()).await;
        let pid = child.id().unwrap();
        assert!(pid_alive(pid), "the leader must be alive before the test");
        assert!(
            pid_alive(nested_pid),
            "the nested descendant must be alive before the test"
        );
        assert_ne!(
            pid, nested_pid,
            "the nested descendant must be a genuinely different process from the leader"
        );

        // Simulate the marker surviving its own generation's death, exactly
        // like the test below — only the LEADER's pid is ever marked.
        std::mem::forget(ManagedChildMarker::create(&layout, pid));
        assert!(
            layout.managed_children_dir().join(pid.to_string()).exists(),
            "the marker must exist before reaping"
        );

        reap_stale_managed_children(&layout);

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        for target in [pid, nested_pid] {
            while pid_alive(target) {
                assert!(
                    std::time::Instant::now() < deadline,
                    "pid {target} survived reap_stale_managed_children's nested-group walk"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        assert!(
            !layout.managed_children_dir().join(pid.to_string()).exists(),
            "the marker must be removed once considered, win or lose"
        );
        // Reap the OS zombie: this test process is its direct parent.
        let _ = child.wait().await;
    }

    /// The reap half of item (3) (TKT-01M0PBNGGZTNQPXB16214V4D7M): a marker
    /// left behind with NO owning `ManagedChildMarker` guard still alive to
    /// remove it (`std::mem::forget`, modeling a daemon generation that died
    /// before its own drop path could ever run) names a process that is
    /// STILL the exact one this "daemon" spawned — same pid, same
    /// `process_signature`. `reap_stale_managed_children` must kill it.
    #[tokio::test]
    async fn reap_stale_managed_children_kills_a_genuinely_orphaned_child_whose_signature_still_matches(
    ) {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        let mut child = spawn_sleeper();
        let pid = child.id().unwrap();
        assert!(pid_alive(pid), "the sleeper must be alive before the test");

        // Simulate the marker surviving its own generation's death: create
        // it, then `forget` it so its `Drop` (which would otherwise remove
        // the file the instant this scope ends) never runs.
        std::mem::forget(ManagedChildMarker::create(&layout, pid));
        assert!(
            layout.managed_children_dir().join(pid.to_string()).exists(),
            "the marker must exist before reaping"
        );

        reap_stale_managed_children(&layout);

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while pid_alive(pid) {
            assert!(
                std::time::Instant::now() < deadline,
                "the genuinely orphaned sleeper survived reap_stale_managed_children"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            !layout.managed_children_dir().join(pid.to_string()).exists(),
            "the marker must be removed once considered, win or lose"
        );
        // Reap the OS zombie: this test process is its direct parent.
        let _ = child.wait().await;
    }

    /// The fail-closed half: a marker recording a WRONG signature for a
    /// CURRENTLY LIVE pid — modeling the OS having recycled that exact pid
    /// for a process this daemon never spawned in the gap between a dead
    /// generation and this one starting. `reap_stale_managed_children` must
    /// never signal it: killing on the strength of a bare, reused pid number
    /// alone is exactly the bug this identity check exists to prevent.
    #[tokio::test]
    async fn reap_stale_managed_children_never_signals_a_pid_reused_by_an_unrelated_process() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        // A REAL, currently-alive, unrelated process standing in for "the OS
        // reused this pid" — this test never spawned it as a managed check,
        // and no `ManagedChildMarker` for it was ever created.
        let mut decoy = spawn_sleeper();
        let decoy_pid = decoy.id().unwrap();
        assert!(
            pid_alive(decoy_pid),
            "the decoy must be alive before the test"
        );

        // Hand-write a marker for that pid carrying a start time that cannot
        // possibly match the decoy's real one — the exact shape a stale
        // marker from a LONG-dead generation would have once its originally
        // recorded process has exited and the pid later got recycled onto
        // this unrelated decoy with a different start time.
        let dir = layout.managed_children_dir();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(decoy_pid.to_string()), "Thu Jan  1 00:00:00 1970").unwrap();

        reap_stale_managed_children(&layout);

        // Give a wrongly-issued kill every chance to land before asserting
        // survival — this must NOT be a race the assertion wins by luck.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            pid_alive(decoy_pid),
            "reap_stale_managed_children signalled a pid it never spawned, on a mismatched \
             signature alone"
        );
        assert!(
            !dir.join(decoy_pid.to_string()).exists(),
            "the stale/mismatched marker must still be cleared so it is never re-examined"
        );

        // Cleanup: this decoy is never reaped by design, so kill it by hand
        // and wait on it ourselves, same as the test above. Signal the whole
        // group (`-decoy_pid`, valid because `spawn_sleeper` makes it its own
        // leader), not just the leader pid: `spawn_sleeper` backgrounds its
        // actual `sleep` under `sh -c 'sleep 300 & wait'` to keep the
        // leader's identity stable (see that function's doc comment), so a
        // leader-only kill would leave that backgrounded sleep orphaned for
        // the rest of its 300s.
        let _ = std::process::Command::new("kill")
            .args(["-9", &format!("-{decoy_pid}")])
            .status();
        let _ = decoy.wait().await;
    }

    /// Read `pid`'s current `comm` via `ps`, or `None` if the process is
    /// gone or `ps` itself failed.
    fn comm_of(pid: u32) -> Option<String> {
        std::process::Command::new("ps")
            .args(["-o", "comm=", "-p", &pid.to_string()])
            .output()
            .ok()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .filter(|c| !c.is_empty())
    }

    /// Poll `pid`'s `comm` until it stops reading `sh`. Used only AFTER the
    /// barrier below has been released, to confirm the resulting exec has
    /// actually landed — never to establish ordering by itself, since
    /// polling after the fact only proves an exec eventually happened, not
    /// that it happened after some earlier event the test cares about.
    async fn wait_for_comm_to_leave_sh(pid: u32) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if matches!(comm_of(pid), Some(c) if !c.contains("sh")) {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "comm never flipped away from sh — the tail-call exec did not land"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Spawn `sh -c '<script>'` gated on `barrier` NOT existing: the leader
    /// polls for `barrier` in a loop (never the last statement in the
    /// script, so sh has trailing work and never tail-call-execs while
    /// waiting) and only reaches `exec sleep 300` — an EXPLICIT, so
    /// deterministic-once-reached, tail-call — once the caller creates
    /// `barrier`. This is what turns "was the marker captured before or
    /// after the exec" from a race against sh's own unspecified tail-call
    /// timing into something the TEST controls outright: the leader is
    /// PROVABLY still `sh`, not just probably, for as long as `barrier` is
    /// absent.
    fn spawn_sh_blocked_until_barrier(barrier: &Path) -> tokio::process::Child {
        let command = format!(
            "while [ ! -f '{}' ]; do sleep 0.02; done; exec sleep 300",
            barrier.display()
        );
        tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&command)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap()
    }

    /// TKT-01M0PR64A3H2S9W7KR54Q68VN9: `process_signature`'s identity is PID
    /// plus start time, deliberately excluding `comm`, because `comm` is not
    /// stable across exec — `sh -c '<single simple command>'` (exactly what
    /// `spawn_check_child` runs for every named check and raw `run` step)
    /// can have `sh` tail-call-exec directly into that command at any point
    /// after `spawn()` returns, same pid, same start time, but `comm` flips.
    /// A barrier (see `spawn_sh_blocked_until_barrier`) proves `before` is
    /// captured while the leader is STILL, provably, `sh` — not merely
    /// likely to still be `sh` — before the exec that flips `comm` is even
    /// reachable, so this cannot pass by accident on a build that still
    /// includes `comm` in the signature.
    #[tokio::test]
    async fn process_signature_survives_a_tail_call_exec_that_changes_comm() {
        let script_dir = tempfile::tempdir().unwrap();
        let barrier = script_dir.path().join("release");
        let mut child = spawn_sh_blocked_until_barrier(&barrier);
        let pid = child.id().unwrap();
        assert!(pid_alive(pid), "the leader must be alive before the test");
        assert_eq!(
            comm_of(pid).as_deref(),
            Some("sh"),
            "the leader must still be sh, blocked on the absent barrier, before `before` is \
             captured — otherwise this proves nothing about ordering"
        );

        let before = process_signature(pid).expect("the leader must be alive right after spawn");

        // Only now can the leader reach `exec sleep 300` — `before` is
        // provably pre-exec.
        std::fs::write(&barrier, "go").unwrap();
        wait_for_comm_to_leave_sh(pid).await;

        let after = process_signature(pid).expect("the process must still be alive after the exec");
        assert_eq!(
            before, after,
            "process_signature must be unchanged by an exec that only changes comm"
        );

        let _ = std::process::Command::new("kill")
            .args(["-9", &pid.to_string()])
            .status();
        let _ = child.wait().await;
    }

    /// The production regression this ticket exists for
    /// (TKT-01M0PR64A3H2S9W7KR54Q68VN9): `spawn_check_child` calls
    /// `ManagedChildMarker::create` immediately after `spawn()`, so the
    /// signature it records can land BEFORE `sh` tail-call-execs into the
    /// check command — same pid, `comm` now different from what was
    /// recorded. A `comm`-inclusive identity would make
    /// `reap_stale_managed_children`'s fail-closed fence refuse to touch
    /// this process ever again across a daemon restart, defeating the
    /// orphan-reap feature (TKT-01M0PBNGGZTNQPXB16214V4D7M) for exactly the
    /// single-bare-command class of check most likely to hit it.
    ///
    /// The barrier (see `spawn_sh_blocked_until_barrier`) makes the ordering
    /// this test depends on PROVABLE rather than merely likely: the marker
    /// is written while the leader is confirmed still `sh` — the exec is
    /// physically unreachable until the barrier file is created, which
    /// happens strictly after `ManagedChildMarker::create` returns. Without
    /// that guarantee, a fast-enough tail-call exec could land before the
    /// marker is even written, in which case the OLD comm-inclusive
    /// implementation would ALSO pass this test — proving nothing about the
    /// fix.
    #[tokio::test]
    async fn reap_stale_managed_children_kills_a_genuinely_orphaned_child_that_tail_call_execd_after_the_marker_was_written(
    ) {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        let script_dir = tempfile::tempdir().unwrap();
        let barrier = script_dir.path().join("release");
        let mut child = spawn_sh_blocked_until_barrier(&barrier);
        let pid = child.id().unwrap();
        assert!(pid_alive(pid), "the child must be alive before the test");
        assert_eq!(
            comm_of(pid).as_deref(),
            Some("sh"),
            "the leader must still be sh, blocked on the absent barrier, when the marker is \
             written — this is what makes the exec happen strictly AFTER the marker, not a race"
        );

        // Marker written while the leader is provably still `sh`, mirroring
        // spawn_check_child's real ordering (marker written immediately
        // after spawn) with the ordering made airtight instead of merely
        // likely.
        std::mem::forget(ManagedChildMarker::create(&layout, pid));
        assert!(
            layout.managed_children_dir().join(pid.to_string()).exists(),
            "the marker must exist before reaping"
        );

        // Only now can the leader reach `exec sleep 300` — strictly after
        // the marker was written.
        std::fs::write(&barrier, "go").unwrap();
        wait_for_comm_to_leave_sh(pid).await;

        reap_stale_managed_children(&layout);

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while pid_alive(pid) {
            assert!(
                std::time::Instant::now() < deadline,
                "the exec'd orphan survived reap_stale_managed_children — comm drift broke the \
                 identity fence"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            !layout.managed_children_dir().join(pid.to_string()).exists(),
            "the marker must be removed once considered, win or lose"
        );
        // Reap the OS zombie: this test process is its direct parent.
        let _ = child.wait().await;
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
                (4, 2, "dismiss + land"),
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

    #[test]
    fn review_rename_preserves_local_customization_and_old_resume_names() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let engine = test_engine(home.path());
        let global = home.path().join("workflows");
        let local = repo.path().join(".rk/workflows");
        std::fs::create_dir_all(&global).unwrap();
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(
            global.join("candidate-review.cue"),
            "workflow: {name: \"candidate-review\", steps: []}",
        )
        .unwrap();
        let legacy_name = rk_core::landing_names::LEGACY_REVIEW_WORKFLOW;
        let legacy = local.join(format!("{legacy_name}.cue"));
        let source = include_str!("../../../examples/workflows/candidate-review.cue")
            .replace("candidate-review", legacy_name)
            .replace("review-only: spawn", "local customization: spawn");
        std::fs::write(&legacy, source).unwrap();
        let selected = engine
            .find_definition("candidate-review", repo.path().to_str().unwrap())
            .unwrap();
        assert_eq!(selected, legacy.canonicalize().unwrap());
        let inputs = HashMap::from([
            ("branch".into(), json!("candidate")),
            ("reviewAttempt".into(), json!("review-test")),
        ]);
        let definition = rk_workflow::load(&selected, &inputs).unwrap();
        assert_eq!(definition.name, "candidate-review");
        assert!(definition.description.starts_with("local customization"));
        let migrated = local.join("candidate-review.cue");
        std::fs::rename(&legacy, &migrated).unwrap();
        assert_eq!(
            engine
                .find_definition(legacy_name, repo.path().to_str().unwrap())
                .unwrap(),
            migrated.canonicalize().unwrap()
        );
    }

    /// A minimal engine with an in-memory space/registry, enough to exercise
    /// [`crate::managed_verification::ManagedVerification::run`] directly without a live daemon or a
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
            0,
            false,
        )
    }

    /// [`test_engine`] with `finalize_cleanup_enabled: true` — needed by any
    /// test asserting on the guaranteed-cleanup sweep itself
    /// ([`WorkflowEngine::sweep_instance_agents`]), which `test_engine`
    /// deliberately leaves off so its other (unrelated) tests never race a
    /// background dismiss.
    fn test_engine_with_cleanup(home: &Path) -> WorkflowEngine {
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
            0,
            true,
        )
    }

    /// A minimal `AgentRecord` owned by `instance`, in `state` — shared by
    /// the still-live-at-ceiling cleanup tests below so each only has to
    /// vary the one field it is testing.
    /// A bare, real, discoverable git repo — [`Supervisor::dismiss_inner`]
    /// (behind `dismiss_live_instance_agents`/`dismiss_orphaned_instance_agents`)
    /// unconditionally calls `Repo::discover` on the agent record's
    /// `repo_root`, so a still-live-agent dismiss test needs a real repo, not
    /// the placeholder `/repo` path other tests in this file use for records
    /// that are never actually dismissed.
    fn init_bare_repo(dir: &Path) {
        let git = |args: &[&str]| {
            let status = std::process::Command::new("git")
                .args(args)
                .current_dir(dir)
                .status()
                .unwrap();
            assert!(status.success(), "git {args:?} failed");
        };
        git(&["init", "-q", "-b", "main"]);
        git(&["config", "user.email", "r@x"]);
        git(&["config", "user.name", "R"]);
        std::fs::write(dir.join("f"), "0\n").unwrap();
        git(&["add", "."]);
        git(&["commit", "-q", "-m", "init"]);
    }

    /// A minimal `AgentRecord` owned by `instance`, in `state`, rooted at
    /// `repo_root` (a real repo — see [`init_bare_repo`]) with no worktree
    /// to reclaim, so a dismiss exercises only the state transition these
    /// tests check, not worktree removal.
    fn owned_agent_record(
        name: &str,
        instance: &str,
        state: AgentState,
        repo_root: &Path,
    ) -> crate::agents::AgentRecord {
        let now = Utc::now();
        crate::agents::AgentRecord {
            name: name.into(),
            spawn: Some(rk_core::id::SpawnId::new()),
            role: "reviewer".into(),
            coordination: None,
            harness: "fake".into(),
            permission_mode: None,
            model: None,
            repo_root: repo_root.to_path_buf(),
            repo_name: "repo".into(),
            task: Some("t".into()),
            branch: Some(format!("rat/{name}/t")),
            fork_point: None,
            worktree: None,
            target_branch: "main".into(),
            parent: None,
            workflow_instance: Some(instance.into()),
            review: None,
            coordinator: None,
            session_id: None,
            attach_target: None,
            pid: None,
            merge_commit: None,
            state,
            crashed: false,
            stderr_tail: None,
            result: None,
            progress: None,
            usage: rk_harness::TokenUsage::default(),
            cost_usd: 0.0,
            created_at: now,
            updated_at: now,
            archived_at: None,
            liveness: Default::default(),
            transport_outage: None,
            recovery: None,
            recovery_receipt: None,
            current_attempt: None,
        }
    }

    /// Root-cause regression for the parent transport-outage incident
    /// (2026-08-21, TKT-01M0HDFZNVPHE0JV382VSBCQD0): a Codex reviewer stayed
    /// `Running` and reconnecting well after its owning candidate-review
    /// workflow had already timed out, holding fleet capacity indefinitely.
    /// Before [`WorkflowEngine::sweep_instance_agents`] existed, the
    /// finalize-time/stale-timeout cleanup sweep called ONLY
    /// `dismiss_orphaned_instance_agents`, which filters to
    /// `Completed`/`Failed` by design and therefore could never touch a
    /// still-`Running` (transport-reconnecting) agent — the instance would
    /// go `Failed` and the agent would simply be left running forever.
    /// Proves the B8 stale-instance-timeout path (a workflow ceiling) now
    /// releases such an agent instead of orphaning it.
    #[tokio::test]
    async fn stale_instance_timeout_releases_a_still_live_owned_agent() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_bare_repo(repo_dir.path());
        let engine = test_engine_with_cleanup(home.path());
        let id = "inst-ceiling-timeout";

        engine
            .supervisor
            .lock_registry()
            .insert(owned_agent_record(
                "Scurry",
                id,
                AgentState::Running,
                repo_dir.path(),
            ))
            .unwrap();
        let started_at = Utc::now() - chrono::Duration::hours(13);
        engine
            .store_if_absent(wedged_instance(id, started_at))
            .unwrap();

        let (sinks, _recorder) = recording_sinks();
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
        assert_eq!(engine.status(id).unwrap().status, InstanceStatus::Failed);

        let released = engine
            .supervisor
            .lock_registry()
            .get("Scurry")
            .cloned()
            .unwrap();
        assert_eq!(
            released.state,
            AgentState::Dismissed,
            "a still-live reviewer must be released when its owning workflow times out, not \
             left reconnecting forever"
        );
    }

    /// Companion to the timeout case above: the SAME still-live release must
    /// happen on the ordinary `finalize()` path (an instance whose
    /// `execute()` future returns an error — a `wait` step timing out
    /// because the agent's own liveness never resolves — not just the B8
    /// stale-Running sweep), and it must never double-dismiss an agent that
    /// [`dismiss_orphaned_instance_agents`] already reclaimed because it was
    /// already terminal.
    #[tokio::test]
    async fn finalize_releases_a_still_live_owned_agent_without_double_dismissing_a_terminal_one() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_bare_repo(repo_dir.path());
        let engine = test_engine_with_cleanup(home.path());
        let id = "inst-finalize-cleanup";

        engine
            .supervisor
            .lock_registry()
            .insert(owned_agent_record(
                "Scurry",
                id,
                AgentState::Running,
                repo_dir.path(),
            ))
            .unwrap();
        engine
            .supervisor
            .lock_registry()
            .insert(owned_agent_record(
                "Nibble",
                id,
                AgentState::Failed,
                repo_dir.path(),
            ))
            .unwrap();
        engine
            .store_if_absent(wedged_instance(id, Utc::now()))
            .unwrap();

        engine
            .finalize(
                id,
                "/repo",
                "wf",
                Err(rk_core::Error::other("wait timed out")),
            )
            .await
            .unwrap();

        let reviewer = engine
            .supervisor
            .lock_registry()
            .get("Scurry")
            .cloned()
            .unwrap();
        assert_eq!(
            reviewer.state,
            AgentState::Dismissed,
            "still-live agent must be released"
        );
        let crashed = engine
            .supervisor
            .lock_registry()
            .get("Nibble")
            .cloned()
            .unwrap();
        assert_eq!(
            crashed.state,
            AgentState::Dismissed,
            "already-terminal agent still reclaimed"
        );
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
            .verification()
            .run(crate::managed_verification::CheckExecution {
                admission_timeout: None,
                id: "inst-agent",
                repo: "/repo",
                agent: "Whisker",
                dir: agent_worktree.path(),
                command: &resolved.command,
                resolved: &resolved,
                env: &env,
                timeout,
                previous_result: None,
                progress: None,
            })
            .await
            .unwrap();
        let via_bare_dir = engine
            .verification()
            .run(crate::managed_verification::CheckExecution {
                admission_timeout: None,
                id: "inst-daemon",
                repo: "/repo",
                agent: "daemon",
                dir: bare_gate_dir.path(),
                command: &resolved.command,
                resolved: &resolved,
                env: &env,
                timeout,
                previous_result: None,
                progress: None,
            })
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
            .verification()
            .run(crate::managed_verification::CheckExecution {
                admission_timeout: None,
                id: "inst-daemon-fail",
                repo: "/repo/daemon-gate",
                agent: "daemon",
                dir: bare_gate_dir.path(),
                command: &resolved.command,
                resolved: &resolved,
                env: &[],
                timeout,
                previous_result: None,
                progress: None,
            })
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
            shared_cargo_target: false,
        };
        let timeout = Duration::from_secs(5);

        let result = engine
            .verification()
            .run(crate::managed_verification::CheckExecution {
                admission_timeout: None,
                id: "inst-contention-retry",
                repo: "/repo/daemon-gate",
                agent: "daemon",
                dir: bare_gate_dir.path(),
                command: &resolved.command,
                resolved: &resolved,
                env: &[],
                timeout,
                previous_result: None,
                progress: None,
            })
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
            shared_cargo_target: false,
        };
        let timeout = Duration::from_secs(5);

        let err = engine
            .verification()
            .run(crate::managed_verification::CheckExecution {
                admission_timeout: None,
                id: "inst-contention-exhausted",
                repo: "/repo/daemon-gate",
                agent: "daemon",
                dir: bare_gate_dir.path(),
                command: &resolved.command,
                resolved: &resolved,
                env: &[],
                timeout,
                previous_result: None,
                progress: None,
            })
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
            shared_cargo_target: false,
        };
        let timeout = Duration::from_secs(5);

        let err = engine
            .verification()
            .run(crate::managed_verification::CheckExecution {
                admission_timeout: None,
                id: "inst-genuine-fail",
                repo: "/repo/daemon-gate",
                agent: "daemon",
                dir: bare_gate_dir.path(),
                command: &resolved.command,
                resolved: &resolved,
                env: &[],
                timeout,
                previous_result: None,
                progress: None,
            })
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
            fork_point: None,
            worktree: Some(PathBuf::from("/repo")),
            target_branch: "main".into(),
            parent: None,
            workflow_instance: None,
            review: None,
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
            liveness: Default::default(),
            transport_outage: None,
            recovery: None,
            recovery_receipt: None,
            current_attempt: None,
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
            workflow: "landing".into(),
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
            definition: "landing".into(),
            definition_digest: String::new(),
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
            .finalize("wf-race", "/repo", "landing", Ok(()))
            .await
            .unwrap();

        // The sweep's Failed verdict must survive, not be overwritten by
        // finalize's Completed.
        let after = engine.status("wf-race").unwrap();
        assert_eq!(after.status, InstanceStatus::Failed);
        assert!(after.error.unwrap().contains("stale-instance timeout"));
    }

    // --- Verification admission queue (TKT-01M0HNESEECWWFQF8X6VH1XSJ6):
    // properties that need a full `WorkflowEngine` (`find_check`,
    // `run_check_in`, `verify_repo_check`) to exercise, as opposed to the
    // FIFO/bound/independence/restart properties of `VerificationAdmission`
    // alone, covered in `supervisor::verification_admission_tests`. ---

    fn git(dir: &Path, args: &[&str]) {
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// A clean, committed git worktree — the precondition `clean_candidate_sha`
    /// requires before verification-proof reuse ever engages.
    fn init_clean_repo(dir: &Path) {
        git(dir, &["init", "-b", "main"]);
        git(dir, &["config", "user.email", "r@x"]);
        git(dir, &["config", "user.name", "R"]);
        std::fs::write(dir.join("readme"), "hello\n").unwrap();
        git(dir, &["add", "."]);
        git(dir, &["commit", "-m", "init"]);
    }

    /// Write a single named check to `<dir>/.rk/checks.cue`. `command` must
    /// avoid embedded double quotes and `\(` (the CUE string-interpolation
    /// escape) — every command built by the tests below sticks to single
    /// quotes for paths, so this is always safe.
    fn write_check(dir: &Path, name: &str, command: &str, shared_cargo_target: bool) {
        std::fs::create_dir_all(dir.join(".rk")).unwrap();
        let cue = format!(
            "checks: [{{name: \"{name}\", command: \"{command}\", timeout: \"5s\", environmentPolicy: \"strip_rk_spawn\", sharedCargoTarget: {shared_cargo_target}}}]",
        );
        std::fs::write(dir.join(".rk").join("checks.cue"), cue).unwrap();
    }

    /// `verify_repo_check` — the `verify.run`-mediated path a rat's `rk
    /// verify` ultimately calls — must surface the check's EXACT child exit
    /// code, not a masked/rounded one: `run_check_in`'s inline `expectExit`
    /// gate is deliberately unset for this caller (see its doc comment), so
    /// a failing check comes back as a clean `Ok` result carrying `exit`
    /// verbatim.
    #[tokio::test]
    async fn verify_repo_check_returns_the_exact_child_exit_code() {
        let home = tempfile::tempdir().unwrap();
        let engine = test_engine(home.path());
        let repo_dir = tempfile::tempdir().unwrap();
        write_check(repo_dir.path(), "verify", "exit 37", false);

        let result = engine
            .verify_repo_check(
                "agent",
                repo_dir.path(),
                "repo-exact-exit",
                "verify",
                None,
                "test-request",
                None,
            )
            .await
            .unwrap();

        assert_eq!(result["exit"], json!(37));
        assert_eq!(result["verdict"], json!("fail"));
    }

    /// TKT-01M0QJXVF5QP858YXF82E9WRWQ: the ad-hoc `verify.run` path (a rat's
    /// own completion-protocol check, outside any landing gate) previously
    /// recorded no task-to-main span at all — only a caller carrying a
    /// ticket (`task: Some(...)`) gets one; the operator (`task: None`, no
    /// ticket to correlate against) gets none, on purpose.
    #[tokio::test]
    async fn verify_repo_check_records_a_span_only_for_a_ticketed_caller() {
        let home = tempfile::tempdir().unwrap();
        let engine = test_engine(home.path());
        let repo_dir = tempfile::tempdir().unwrap();
        write_check(repo_dir.path(), "verify", "true", false);

        // The operator path: no ticket, no span.
        engine
            .verify_repo_check(
                "operator",
                repo_dir.path(),
                "ad-hoc-repo",
                "verify",
                None,
                "operator-request",
                None,
            )
            .await
            .unwrap();
        assert!(
            crate::span::spans_for_task(&engine.space, "ad-hoc-repo", "TKT-ad-hoc")
                .unwrap()
                .is_empty()
        );

        // A ticketed rat's own `rk verify` gets a span, correlated on its
        // ticket, carrying the check name as `lane` and `proof_kind: "ad-hoc"`
        // — distinct from a landing gate's own per-check spans.
        let result = engine
            .verify_repo_check(
                "some-rat",
                repo_dir.path(),
                "ad-hoc-repo",
                "verify",
                None,
                "rat-request",
                Some("TKT-ad-hoc"),
            )
            .await
            .unwrap();
        assert_eq!(result["verdict"], json!("pass"));

        let spans =
            crate::span::spans_for_task(&engine.space, "ad-hoc-repo", "TKT-ad-hoc").unwrap();
        assert_eq!(spans.len(), 1, "{spans:?}");
        assert_eq!(spans[0]["phase"], "verification");
        assert_eq!(spans[0]["lane"], "verify");
        assert_eq!(spans[0]["proof_kind"], "ad-hoc");
        assert_eq!(spans[0]["proof_reused"], false);
        assert_eq!(spans[0]["terminal_reason"], "pass");
        // Deliberately far from a landing gate's small per-check plan
        // positions (1, 2, 3, ...) so the two producers never collide on
        // `record_phase_span`'s `(task, phase, attempt)` idempotency key.
        assert!(spans[0]["attempt"].as_u64().unwrap() >= 10_000);

        // A second ad-hoc call for the SAME ticket and check must not
        // collide with (silently drop) the first — a rat re-running its own
        // `rk verify` is a genuinely new occurrence, not a replay.
        engine
            .verify_repo_check(
                "some-rat",
                repo_dir.path(),
                "ad-hoc-repo",
                "verify",
                None,
                "rat-request-2",
                Some("TKT-ad-hoc"),
            )
            .await
            .unwrap();
        let spans =
            crate::span::spans_for_task(&engine.space, "ad-hoc-repo", "TKT-ad-hoc").unwrap();
        assert_eq!(spans.len(), 2, "{spans:?}");
        let attempts: std::collections::BTreeSet<u64> = spans
            .iter()
            .map(|s| s["attempt"].as_u64().unwrap())
            .collect();
        assert_eq!(attempts.len(), 2, "{spans:?}");
    }

    /// TKT-01M0PA6C5WYRWS757R1SS2F2GR: `Supervisor::interrupt`/`dismiss`/the
    /// harness-exit handler all funnel into `cancel_managed_verification_for_agent`.
    /// This is the live regression: a REAL `sh -c` child process (not a mock,
    /// not a cancellation token in isolation) reports its own pid, cancelling
    /// its run kills that exact OS process group, and a second call queued
    /// behind the same repo's admission bound (limit 1) starts and completes
    /// promptly afterward instead of waiting for its own timeout — proving
    /// the permit was released as part of cancellation, not merely eventually
    /// reclaimed.
    #[tokio::test]
    async fn cancelling_a_managed_verification_run_kills_its_real_process_group_and_frees_the_queued_follower(
    ) {
        let home = tempfile::tempdir().unwrap();
        let engine = test_engine(home.path());
        engine
            .supervisor
            .set_verification_admission_limits(1, HashMap::new());

        let repo_dir = tempfile::tempdir().unwrap();
        let pid_file = repo_dir.path().join("child.pid");
        write_check(
            repo_dir.path(),
            "verify",
            &format!("echo $$ > '{}'; sleep 30", pid_file.display()),
            true,
        );

        let generation = rk_core::id::SpawnId::new();
        let first = engine.verify_repo_check(
            "Whisker",
            repo_dir.path(),
            "cancel-test-repo",
            "verify",
            Some(generation),
            "req-1",
            None,
        );
        tokio::pin!(first);

        // Poll until the child has actually started and reported its own
        // pid — proof there is a real process to kill, not just a scheduled
        // task.
        let child_pid: i32 = loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(20)) => {
                    if let Ok(text) = std::fs::read_to_string(&pid_file) {
                        if let Ok(pid) = text.trim().parse() {
                            break pid;
                        }
                    }
                }
                _ = &mut first => panic!("the check must not settle on its own before it's cancelled"),
            }
        };

        // A second call for the SAME repo, still bound by the admission
        // limit of 1: it must not even start while the first holds the
        // permit.
        let second_dir = tempfile::tempdir().unwrap();
        let second_marker = second_dir.path().join("ran");
        write_check(
            second_dir.path(),
            "verify",
            &format!("touch '{}'", second_marker.display()),
            true,
        );
        let second = engine.verify_repo_check(
            "Nibble",
            second_dir.path(),
            "cancel-test-repo",
            "verify",
            None,
            "req-2",
            None,
        );
        tokio::pin!(second);
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            _ = &mut second => panic!("the follower must stay queued behind the first run's admission permit"),
        }
        assert!(
            !second_marker.exists(),
            "the queued follower must not have started yet"
        );

        // Cancel exactly like `Supervisor::interrupt` does.
        engine.supervisor.cancel_managed_verification_for_agent(
            "Whisker",
            Some(generation),
            "agent_interrupt",
        );

        let error = first
            .await
            .expect_err("a cancelled run must return an error, not a verdict");
        assert!(
            error.to_string().contains("cancelled"),
            "unexpected error: {error}"
        );

        // The real process must actually be gone shortly after — proof the
        // managed child's process GROUP was killed, not just abandoned.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            // Signal 0: existence check only, no signal actually delivered.
            let alive = unsafe { libc::kill(child_pid, 0) == 0 };
            if !alive {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "child process {child_pid} is still alive after cancellation"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // The queued follower must now proceed promptly — the admission
        // permit was released as part of cancellation, not left held until
        // its own timeout.
        tokio::time::timeout(Duration::from_secs(5), &mut second)
            .await
            .expect("the queued follower must start promptly once the first run is cancelled")
            .expect("the follower's own check must pass");
        assert!(second_marker.exists());
    }

    /// TKT-01M0PN2JSN24AHGQHFJ4XGAVKD's exact real-world gap, exercised
    /// through the REAL production cancellation path (`verify_repo_check`'s
    /// `tokio::select!` against `cancel_managed_verification_for_agent`), not
    /// `collect_child_output` called directly: a check command that moves
    /// part of its own work into a SECOND process group (`mise run <task>`
    /// in production; modeled here with `perl`'s `setpgrp(0, 0)`, portable
    /// and needing no `setsid`(1) binary — absent on macOS) must have that
    /// nested group killed too, not just its leader's. Also proves the two
    /// other properties this fix must not regress: the admission permit is
    /// still released promptly (the queued follower still starts), and the
    /// tree-walk kill touches ONLY this check's own descendants — a
    /// completely unrelated process, its own group, spawned independently
    /// of any managed check, survives the cancellation untouched.
    #[tokio::test]
    async fn cancelling_a_managed_verification_run_kills_its_nested_process_group_frees_the_queued_follower_and_spares_unrelated_processes(
    ) {
        let home = tempfile::tempdir().unwrap();
        let engine = test_engine(home.path());
        engine
            .supervisor
            .set_verification_admission_limits(1, HashMap::new());

        // An UNRELATED process, its own independent group, with no
        // connection whatsoever to the managed check machinery under test —
        // the negative control for "no unrelated process/group is killed".
        let unrelated_dir = tempfile::tempdir().unwrap();
        let unrelated_pid_file = unrelated_dir.path().join("unrelated.pid");
        let mut unrelated_cmd = tokio::process::Command::new("sh");
        unrelated_cmd
            .arg("-c")
            .arg(format!(
                "echo $$ > '{}'; sleep 30",
                unrelated_pid_file.display()
            ))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .process_group(0);
        let mut unrelated_child = unrelated_cmd.spawn().unwrap();
        let unrelated_pid: i32 = {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                if let Ok(text) = std::fs::read_to_string(&unrelated_pid_file) {
                    if let Ok(pid) = text.trim().parse() {
                        break pid;
                    }
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "unrelated sibling process never reported its own pid"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };

        let repo_dir = tempfile::tempdir().unwrap();
        let pid_file = repo_dir.path().join("child.pid");
        let nested_pid_file = repo_dir.path().join("nested.pid");
        let script = repo_dir.path().join("detach.pl");
        std::fs::write(
            &script,
            "setpgrp(0, 0);\n\
             open(my $fh, '>', $ARGV[0]) or die $!;\n\
             print $fh $$;\n\
             close $fh;\n\
             sleep 300;\n",
        )
        .unwrap();
        write_check(
            repo_dir.path(),
            "verify",
            &format!(
                "echo $$ > '{}'; perl '{}' '{}' & wait",
                pid_file.display(),
                script.display(),
                nested_pid_file.display()
            ),
            true,
        );

        let generation = rk_core::id::SpawnId::new();
        let first = engine.verify_repo_check(
            "Whisker",
            repo_dir.path(),
            "nested-cancel-test-repo",
            "verify",
            Some(generation),
            "req-1",
            None,
        );
        tokio::pin!(first);

        // Poll until BOTH the leader and its self-detached nested descendant
        // have reported their own pids — proof there is a genuine two-group
        // tree to kill, not just a leader that hasn't forked its nested
        // group yet.
        let child_pid: i32 = loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(20)) => {
                    if let Ok(text) = std::fs::read_to_string(&pid_file) {
                        if let Ok(pid) = text.trim().parse() {
                            break pid;
                        }
                    }
                }
                _ = &mut first => panic!("the check must not settle on its own before it's cancelled"),
            }
        };
        let nested_pid: i32 = loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(20)) => {
                    if let Ok(text) = std::fs::read_to_string(&nested_pid_file) {
                        if let Ok(pid) = text.trim().parse() {
                            break pid;
                        }
                    }
                }
                _ = &mut first => panic!("the check must not settle on its own before it's cancelled"),
            }
        };
        assert_ne!(
            child_pid, nested_pid,
            "the nested descendant must be a genuinely different process from the leader"
        );

        // A second call for the SAME repo, still bound by the admission
        // limit of 1: it must not even start while the first holds the
        // permit.
        let second_dir = tempfile::tempdir().unwrap();
        let second_marker = second_dir.path().join("ran");
        write_check(
            second_dir.path(),
            "verify",
            &format!("touch '{}'", second_marker.display()),
            true,
        );
        let second = engine.verify_repo_check(
            "Nibble",
            second_dir.path(),
            "nested-cancel-test-repo",
            "verify",
            None,
            "req-2",
            None,
        );
        tokio::pin!(second);
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            _ = &mut second => panic!("the follower must stay queued behind the first run's admission permit"),
        }
        assert!(
            !second_marker.exists(),
            "the queued follower must not have started yet"
        );

        // Cancel exactly like `Supervisor::interrupt` does.
        engine.supervisor.cancel_managed_verification_for_agent(
            "Whisker",
            Some(generation),
            "agent_interrupt",
        );

        let error = first
            .await
            .expect_err("a cancelled run must return an error, not a verdict");
        assert!(
            error.to_string().contains("cancelled"),
            "unexpected error: {error}"
        );

        // Both the leader AND the nested descendant it moved into a group of
        // its own must actually be gone shortly after — proof the tree-walk
        // reached the nested group, not just the leader's own.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        for pid in [child_pid, nested_pid] {
            loop {
                // Signal 0: existence check only, no signal actually delivered.
                let alive = unsafe { libc::kill(pid, 0) == 0 };
                if !alive {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "process {pid} is still alive after cancellation"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }

        // The queued follower must now proceed promptly — the admission
        // permit was released as part of cancellation, not left held until
        // its own timeout.
        tokio::time::timeout(Duration::from_secs(5), &mut second)
            .await
            .expect("the queued follower must start promptly once the first run is cancelled")
            .expect("the follower's own check must pass");
        assert!(second_marker.exists());

        // The negative control: a process with no relation to the cancelled
        // check's tree must be completely unaffected.
        let unrelated_alive = unsafe { libc::kill(unrelated_pid, 0) == 0 };
        assert!(
            unrelated_alive,
            "an unrelated process must survive a cancellation it had nothing to do with"
        );
        let _ = unrelated_child.start_kill();
        let _ = unrelated_child.wait().await;
    }

    /// The ticket's explicit shared-bound goal: a landing gate (which calls
    /// `run_check_in` directly with `entry.repo_name`, per
    /// `LandingPipeline::run_gates_at`) and `verify.run` (which calls
    /// `verify_repo_check`, itself a `run_check_in` wrapper) for the SAME
    /// bare repo name must contend for one admission permit, not two. Proven
    /// by running both concurrently at limit 1 and showing neither ever
    /// observes the other's in-flight marker file: whichever starts second
    /// cannot even spawn its child process until the first's `run_check_in`
    /// call — marker file removal included — has fully returned and released
    /// the permit.
    #[tokio::test]
    async fn landing_gate_and_verify_run_share_one_admission_bound_for_the_same_repo_name() {
        assert_cross_caller_admission(true).await;
    }

    #[tokio::test]
    async fn non_cargo_checks_share_the_configured_admission_bound() {
        assert_cross_caller_admission(false).await;
    }

    async fn assert_cross_caller_admission(shared_cargo_target: bool) {
        let home = tempfile::tempdir().unwrap();
        let engine = test_engine(home.path());
        engine
            .supervisor
            .set_verification_admission_limits(1, HashMap::new());
        let shared = tempfile::tempdir().unwrap();
        let shared_path = shared.path().display().to_string();
        let repo_name = "acme-shared-bound";

        // The "landing gate" side: exactly `LandingPipeline::run_gates_at`'s
        // call shape (`run_check_in` direct, no `verify_repo_check`
        // indirection), holding the marker for 300ms.
        let landing_resolved = ResolvedRun {
            command: format!(
                "touch '{shared_path}/landing-marker'; sleep 0.3; \
                 if [ -f '{shared_path}/verify-marker' ]; then echo yes; else echo no; fi \
                 > '{shared_path}/landing-saw'; rm -f '{shared_path}/landing-marker'"
            ),
            cwd: None,
            expect_exit: None,
            timeout: "5s".into(),
            on_timeout: OnTimeout::Fail,
            environment_policy: rk_workflow::CheckEnvironmentPolicy::StripRkSpawn,
            retry_on_fail: 0,
            shared_cargo_target,
        };
        let landing_dir = tempfile::tempdir().unwrap();

        // The `verify.run` side: through `verify_repo_check`/`find_check`,
        // starting shortly after the landing side so it would see the
        // landing marker if the two were NOT serialized against each other.
        let verify_dir = tempfile::tempdir().unwrap();
        write_check(
            verify_dir.path(),
            "verify",
            &format!(
                "sleep 0.05; touch '{shared_path}/verify-marker'; \
                 if [ -f '{shared_path}/landing-marker' ]; then echo yes; else echo no; fi \
                 > '{shared_path}/verify-saw'; rm -f '{shared_path}/verify-marker'"
            ),
            shared_cargo_target,
        );

        let landing = engine
            .verification()
            .run(crate::managed_verification::CheckExecution {
                admission_timeout: None,
                id: "inst-landing",
                repo: repo_name,
                agent: "daemon",
                dir: landing_dir.path(),
                command: &landing_resolved.command,
                resolved: &landing_resolved,
                env: &[],
                timeout: Duration::from_secs(5),
                previous_result: None,
                progress: None,
            });
        let verify = engine.verify_repo_check(
            "agent",
            verify_dir.path(),
            repo_name,
            "verify",
            None,
            "test-request",
            None,
        );
        let (landing_result, verify_result) = tokio::join!(landing, verify);
        landing_result.unwrap();
        verify_result.unwrap();

        let landing_saw =
            std::fs::read_to_string(shared.path().join("landing-saw")).unwrap_or_default();
        let verify_saw =
            std::fs::read_to_string(shared.path().join("verify-saw")).unwrap_or_default();
        assert_eq!(
            landing_saw.trim(),
            "no",
            "the landing gate must never observe verify.run's marker: a shared bound means \
             verify.run cannot even start its child process until the landing gate's whole \
             run_check_in call (marker removal included) has returned"
        );
        assert_eq!(
            verify_saw.trim(),
            "no",
            "verify.run must never observe the landing gate's marker for the same reason"
        );
    }

    /// The continuation's required cross-shape proof
    /// (TKT-01M0P5NM51SKT5ABXRCDZD07J3): a workflow `run` step / reactor
    /// dispatch reaches `run_check_in` with the repo's absolute,
    /// already-canonicalized checkout PATH, while a landing gate /
    /// `verify.run` reaches it with the repo's bare registered NAME. Register
    /// one repo under both identities and show the two shapes contend for the
    /// SAME admission permit, not two — via
    /// `Supervisor::verification_repo_identity`'s path-to-registered-name
    /// resolution — using the identical marker-file technique as
    /// `landing_gate_and_verify_run_share_one_admission_bound_for_the_same_repo_name`.
    #[tokio::test]
    async fn workflow_path_shape_and_landing_name_shape_share_one_admission_bound_for_the_same_registered_repo(
    ) {
        let home = tempfile::tempdir().unwrap();
        let engine = test_engine(home.path());
        engine
            .supervisor
            .set_verification_admission_limits(1, HashMap::new());

        let repo_dir = tempfile::tempdir().unwrap();
        let repo_name = "acme-cross-shape";
        {
            let mut registry =
                crate::repos::RepoRegistry::load(&home.path().join("repos.json")).unwrap();
            registry
                .add(crate::repos::RepoRecord {
                    name: repo_name.into(),
                    path: repo_dir.path().to_path_buf(),
                    created_at: Utc::now(),
                    host: None,
                    activated_policy: None,
                })
                .unwrap();
        }
        let repo_path = repo_dir.path().display().to_string();

        let shared = tempfile::tempdir().unwrap();
        let shared_path = shared.path().display().to_string();

        // The "workflow run step / reactor dispatch" side: `run_check_in`
        // called with the repo's absolute PATH, exactly matching what
        // `instance.repo`/`record.path` carry.
        let path_resolved = ResolvedRun {
            command: format!(
                "touch '{shared_path}/path-marker'; sleep 0.3; \
                 if [ -f '{shared_path}/name-marker' ]; then echo yes; else echo no; fi \
                 > '{shared_path}/path-saw'; rm -f '{shared_path}/path-marker'"
            ),
            cwd: None,
            expect_exit: None,
            timeout: "5s".into(),
            on_timeout: OnTimeout::Fail,
            environment_policy: rk_workflow::CheckEnvironmentPolicy::StripRkSpawn,
            retry_on_fail: 0,
            shared_cargo_target: true,
        };
        let path_dir = tempfile::tempdir().unwrap();

        // The "landing gate / verify.run" side: through `verify_repo_check`,
        // called with the repo's bare registered NAME, starting shortly
        // after the path side so it would see the path side's marker if the
        // two were NOT serialized against each other.
        let name_dir = tempfile::tempdir().unwrap();
        write_check(
            name_dir.path(),
            "verify",
            &format!(
                "sleep 0.05; touch '{shared_path}/name-marker'; \
                 if [ -f '{shared_path}/path-marker' ]; then echo yes; else echo no; fi \
                 > '{shared_path}/name-saw'; rm -f '{shared_path}/name-marker'"
            ),
            true,
        );

        let path_side = engine
            .verification()
            .run(crate::managed_verification::CheckExecution {
                admission_timeout: None,
                id: "inst-path",
                repo: &repo_path,
                agent: "daemon",
                dir: path_dir.path(),
                command: &path_resolved.command,
                resolved: &path_resolved,
                env: &[],
                timeout: Duration::from_secs(5),
                previous_result: None,
                progress: None,
            });
        let name_side = engine.verify_repo_check(
            "agent",
            name_dir.path(),
            repo_name,
            "verify",
            None,
            "test-request",
            None,
        );
        let (path_result, name_result) = tokio::join!(path_side, name_side);
        path_result.unwrap();
        name_result.unwrap();

        let path_saw = std::fs::read_to_string(shared.path().join("path-saw")).unwrap_or_default();
        let name_saw = std::fs::read_to_string(shared.path().join("name-saw")).unwrap_or_default();
        assert_eq!(
            path_saw.trim(),
            "no",
            "the path-shaped caller must never observe the name-shaped caller's marker: a \
             shared bound means the name side cannot even start its child process until the \
             path side's whole run_check_in call (marker removal included) has returned"
        );
        assert_eq!(
            name_saw.trim(),
            "no",
            "the name-shaped caller must never observe the path-shaped caller's marker for the \
             same reason"
        );
    }

    /// A durable verification proof is reused for an EXACT match (same repo,
    /// same clean candidate sha, same check) instead of re-running the
    /// check — but the moment the worktree goes dirty, the cache is never
    /// consulted again: `clean_candidate_sha` has no stable identity to key
    /// on, so every call after that runs fresh, however many prior clean
    /// proofs exist.
    #[tokio::test]
    async fn verify_repo_check_reuses_an_exact_proof_but_never_for_a_dirty_worktree() {
        let home = tempfile::tempdir().unwrap();
        let engine = test_engine(home.path());
        let repo_dir = tempfile::tempdir().unwrap();
        init_clean_repo(repo_dir.path());
        let counter = tempfile::tempdir().unwrap();
        let counter_file = counter.path().join("count").display().to_string();
        write_check(
            repo_dir.path(),
            "verify",
            &format!(
                "n=$(cat '{counter_file}' 2>/dev/null || echo 0); n=$((n+1)); \
                 echo $n > '{counter_file}'; exit 0"
            ),
            false,
        );
        // `.rk/checks.cue` itself must be committed, or the worktree is
        // "dirty" (untracked) before the test even starts.
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "add check"]);

        let first = engine
            .verify_repo_check(
                "agent",
                repo_dir.path(),
                "repo-proof-reuse",
                "verify",
                None,
                "test-request",
                None,
            )
            .await
            .unwrap();
        assert_eq!(first["exit"], json!(0));
        assert!(
            first.get("reused").is_none(),
            "the first run has no prior proof to reuse: {first:?}"
        );
        assert_eq!(std::fs::read_to_string(&counter_file).unwrap().trim(), "1");

        // Same clean candidate sha, same check: an exact-match reuse, no
        // second execution.
        let second = engine
            .verify_repo_check(
                "agent",
                repo_dir.path(),
                "repo-proof-reuse",
                "verify",
                None,
                "test-request",
                None,
            )
            .await
            .unwrap();
        assert_eq!(second["reused"], json!(true));
        assert_eq!(second["reused_from"], json!("verification_proof"));
        assert_eq!(
            std::fs::read_to_string(&counter_file).unwrap().trim(),
            "1",
            "an exact-match proof must never re-run the check"
        );

        // Dirty the worktree: candidate_sha now resolves to None, so the
        // cache (despite holding a valid proof for the old clean sha) is
        // never consulted — a fresh run every time.
        std::fs::write(repo_dir.path().join("scratch.txt"), "uncommitted\n").unwrap();
        let third = engine
            .verify_repo_check(
                "agent",
                repo_dir.path(),
                "repo-proof-reuse",
                "verify",
                None,
                "test-request",
                None,
            )
            .await
            .unwrap();
        assert!(
            third.get("reused").is_none(),
            "a dirty worktree must never reuse a proof: {third:?}"
        );
        assert_eq!(
            std::fs::read_to_string(&counter_file).unwrap().trim(),
            "2",
            "a dirty worktree must run the check fresh"
        );
    }
}

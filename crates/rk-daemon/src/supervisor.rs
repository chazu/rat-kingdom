//! The supervisor: spawn rats into worktrees, pump their harness events into
//! the registry and tuplespace, route completions up the spawn tree, and
//! merge their work on dismissal.

use crate::agents::{AgentProgress, AgentRecord, AgentState, Registry};
use crate::onboarding_sessions::{onboarding_branch, onboarding_worktree, ONBOARDER_ROLE};
use crate::read_only_roles::{is_read_only_role, DIAGNOSTICIAN_ROLE};
use chrono::{DateTime, Utc};
use rk_core::config::SupervisorConfig;
use rk_core::paths::Layout;
use rk_core::prime::{render, PrimeContext, VerificationCheck, MAX_INJECTED_FACTS};
use rk_core::tuple::{Category, Pattern, Tuple, DEFAULT_TRAIL_TTL, SYSTEM_SCOPE};
use rk_git::Repo;
use rk_harness::{make_harness, HarnessEvent, LaunchSpec, SessionControl, TokenUsage};
use rk_ledger::pricing::PricingTable;
use rk_ledger::{Budget, BudgetAction, BudgetScope, DispatchCheck, FleetBudget};
use rk_space::Space;
use rk_workflow::{AgentProfile, Coordination, DeliveryMode};
use serde::Deserialize;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

const MIN_PROGRESS_INTERVAL: chrono::Duration = chrono::Duration::seconds(5);

/// Error text [`Supervisor::spawn`] returns when `fleet_wip_cap` refused
/// admission. Matched by name (not a dedicated [`rk_core::Error`] variant, to
/// avoid widening a shared enum for one internal admission-control signal) by
/// callers that must distinguish "no free slot right now, try again" from a
/// genuine spawn failure — see
/// [`WorkflowEngine::await_fleet_capacity`](crate::workflow_exec::WorkflowEngine::await_fleet_capacity).
pub(crate) const FLEET_WIP_CAP_REFUSED: &str =
    "fleet WIP cap reached: no free slot to admit this spawn";

// Review-tiering diff_class thresholds (Phase 0 of the steward remediation).
// The steward trigger reads `diff_class` off the completion payload to decide
// whether a diff is worth an LLM reviewer's judgment at all; these bounds are
// deliberately conservative — a diff outside them defaults toward "large",
// never away from it, so a threshold bug can only ADD a review, never skip
// one it shouldn't.
const DIFF_TRIVIAL_MAX_FILES: usize = 2;
const DIFF_TRIVIAL_MAX_LINES: u64 = 40;
const DIFF_SMALL_MAX_FILES: usize = 10;
const DIFF_SMALL_MAX_LINES: u64 = 400;

/// The completion-payload fields a reactive steward tiers its review on: the
/// branch tip this generation produced, its size vs. the recorded target, and
/// a precomputed bucket. See [`Supervisor::diff_summary`].
struct DiffSummary {
    head_sha: String,
    diff_files: usize,
    diff_lines: u64,
    diff_class: &'static str,
}

impl DiffSummary {
    /// The fail-closed default: no sha, no stats, and `"large"` — the same
    /// tier a real oversized diff gets, so an unreadable repo or a branchless
    /// completion routes through the full reviewer flow rather than skipping
    /// it.
    fn fallback() -> Self {
        Self {
            head_sha: String::new(),
            diff_files: 0,
            diff_lines: 0,
            diff_class: "large",
        }
    }
}

/// Bucket a diff by size and shape. `doc-only` requires at least one changed
/// file (an empty diff is `trivial`, not `doc-only`) with every path either a
/// markdown file or under a `docs/` directory at any depth.
fn classify_diff(files: &[String], lines: u64) -> &'static str {
    if !files.is_empty() && files.iter().all(|f| is_doc_path(f)) {
        "doc-only"
    } else if files.len() <= DIFF_TRIVIAL_MAX_FILES && lines <= DIFF_TRIVIAL_MAX_LINES {
        "trivial"
    } else if files.len() <= DIFF_SMALL_MAX_FILES && lines <= DIFF_SMALL_MAX_LINES {
        "small"
    } else {
        "large"
    }
}

fn is_doc_path(path: &str) -> bool {
    path.ends_with(".md") || path.split('/').any(|segment| segment == "docs")
}

#[derive(Debug, Default)]
struct BranchDelivery {
    target: String,
    remote: String,
    remote_branch: Option<String>,
    delivered: bool,
    merged: bool,
    merge_commit: Option<String>,
    pushed: bool,
    pr_opened: bool,
    pr_url: Option<String>,
    branch_deleted: bool,
    detail: String,
}

fn default_permission_mode(harness: &str) -> &'static str {
    match harness {
        // Workers are unattended. A mode that still asks about Bash commands
        // strands git/rk operations because no human is attached to approve.
        "claude" => "bypassPermissions",
        // A rat's coordination contract includes `rk done`, tuple writes, and
        // ticket operations. Codex's workspace-write sandbox blocks the Unix
        // socket outside the worktree, while jcode exposes no narrower enforced
        // sandbox. Record the full authority both harnesses actually need.
        "codex" | "jcode" => "danger-full-access",
        _ => "workspace-write",
    }
}

#[derive(Debug, Clone, PartialEq)]
struct EffectiveAgentConfig {
    harness: String,
    model: Option<String>,
    permission_mode: String,
}

#[derive(Debug, Clone)]
pub(crate) struct AgentDefaults {
    harness: String,
    profile: AgentProfile,
}

impl AgentDefaults {
    pub(crate) fn new(harness: String, profile: AgentProfile) -> Self {
        Self { harness, profile }
    }
}

fn effective_agent_config(
    default_harness: &str,
    default_agent: &AgentProfile,
    params: &SpawnParams,
) -> rk_core::Result<EffectiveAgentConfig> {
    // A read-only role is an assessment boundary: global worker defaults must
    // never widen it. Explicit harness/model selection remains available, while
    // the permission mode is forced by role.
    if is_read_only_role(&params.role) {
        let harness = params
            .harness
            .clone()
            .unwrap_or_else(|| default_harness.to_string());
        return Ok(EffectiveAgentConfig {
            permission_mode: permission_mode(&params.role, &harness)?,
            harness,
            model: params.model.clone(),
        });
    }

    let harness = params
        .harness
        .clone()
        .or_else(|| default_agent.harness.clone())
        .unwrap_or_else(|| default_harness.to_string());
    let mode = params
        .permission_mode
        .clone()
        .or_else(|| default_agent.permission_mode.clone())
        .unwrap_or_else(|| default_permission_mode(&harness).into());
    validate_permission_mode(&harness, &mode)?;
    Ok(EffectiveAgentConfig {
        harness,
        model: params
            .model
            .clone()
            .or_else(|| default_agent.model.clone()),
        permission_mode: mode,
    })
}

fn respawn_permission_mode(record: &AgentRecord) -> rk_core::Result<String> {
    record
        .permission_mode
        .clone()
        .map(Ok)
        .unwrap_or_else(|| permission_mode(&record.role, &record.harness))
}

fn validate_permission_mode(harness: &str, permission_mode: &str) -> rk_core::Result<()> {
    if !matches!(harness, "codex" | "jcode") {
        return Ok(());
    }

    match permission_mode {
        "danger-full-access" | "bypassPermissions" => Ok(()),
        "read-only" | "workspace-write" => Err(rk_core::Error::other(
            format!(
                "{harness} agents need danger-full-access to reach the rk daemon socket; \
             use --permission-mode danger-full-access (or omit the override)",
            ),
        )),
        other => Err(rk_core::Error::other(format!(
            "unsupported {harness} permission mode '{other}': use danger-full-access"
        ))),
    }
}

/// Roles are an authority input, not open-ended prompt decoration. Keep the
/// accepted vocabulary explicit so a typo cannot silently receive the default
/// worker prompt and capability set.
pub fn validate_role(role: &str) -> rk_core::Result<()> {
    if matches!(
        role,
        "rat" | "reviewer" | "foreman" | "verifier" | ONBOARDER_ROLE | DIAGNOSTICIAN_ROLE
    ) {
        Ok(())
    } else {
        Err(rk_core::Error::other(format!(
            "unknown agent role {role:?}; expected rat, reviewer, foreman, verifier, \
             onboarder, or diagnostician"
        )))
    }
}

/// Read-only roles are assessment-only. Their filesystem boundary is enforced
/// by the harness rather than by prompt prose, and callers cannot override it.
fn permission_mode(role: &str, harness: &str) -> rk_core::Result<String> {
    if !is_read_only_role(role) {
        return Ok(default_permission_mode(harness).into());
    }
    crate::read_only_roles::permission_mode(harness)
}

fn uses_harness_terminal_completion(role: &str, harness: &str) -> bool {
    role == ONBOARDER_ROLE && harness == "jcode"
}

fn is_reporting_boundary(record: &AgentRecord) -> bool {
    record
        .coordination
        .as_ref()
        .and_then(|coordination| coordination.reports_to.as_deref())
        == Some("coordinator")
        || (record.workflow_instance.is_some()
            && matches!(record.role.as_str(), "foreman" | "steward"))
}

struct SpawnJournal<'a> {
    params: &'a SpawnParams,
    repo: &'a Repo,
    repo_name: &'a str,
    name: String,
    branch: String,
    worktree: PathBuf,
    target_branch: String,
    harness: String,
    model: Option<String>,
    permission_mode: String,
}

fn spawning_record(journal: SpawnJournal<'_>) -> AgentRecord {
    let now = Utc::now();
    AgentRecord {
        name: journal.name,
        role: journal.params.role.clone(),
        coordination: journal.params.coordination.clone(),
        harness: journal.harness,
        permission_mode: Some(journal.permission_mode),
        model: journal.model,
        repo_root: journal.repo.root().to_path_buf(),
        repo_name: journal.repo_name.to_string(),
        task: Some(journal.params.task.clone()),
        branch: Some(journal.branch),
        worktree: Some(journal.worktree),
        target_branch: journal.target_branch,
        parent: journal.params.parent.clone(),
        workflow_instance: journal.params.workflow_instance.clone(),
        coordinator: journal.params.coordinator.clone(),
        session_id: None,
        attach_target: None,
        pid: None,
        merge_commit: None,
        state: AgentState::Spawning,
        crashed: false,
        stderr_tail: None,
        result: None,
        progress: None,
        usage: TokenUsage::default(),
        cost_usd: 0.0,
        created_at: now,
        updated_at: now,
        archived_at: None,
    }
}

async fn blocking_io<T, F>(operation: &'static str, f: F) -> rk_core::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> rk_core::Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| rk_core::Error::other(format!("{operation} task failed: {e}")))?
}

#[derive(Debug, Clone, Deserialize)]
pub struct SpawnParams {
    /// Path to the repository (or any path inside it).
    pub repo: String,
    /// Task identifier.
    pub task: String,
    /// Task description / initial prompt body.
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default = "default_role")]
    pub role: String,
    /// Optional explicit reporting-boundary metadata.
    #[serde(default)]
    pub coordination: Option<Coordination>,
    /// Harness kind; falls back to the daemon's configured default.
    #[serde(default)]
    pub harness: Option<String>,
    /// Spawning agent (structural parent for completion routing).
    #[serde(default)]
    pub parent: Option<String>,
    /// Base/merge-target branch; defaults to the repo's current branch.
    #[serde(default)]
    pub base: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub permission_mode: Option<String>,
    /// Run in a herdr pane (interactive, human-attachable) instead of
    /// headless. Completion comes from the rat's own `rk done` tuple.
    #[serde(default)]
    pub attach: bool,
    /// Workflow instance dispatching this spawn (None = not from a workflow).
    /// Recorded on the agent so its cost sums into the instance's rollup.
    #[serde(default)]
    pub workflow_instance: Option<String>,
    /// Coordinator session inherited from the workflow owner, when any.
    #[serde(default)]
    pub coordinator: Option<String>,
    /// Per-instance USD cap for `workflow_instance`, from the workflow's
    /// `budget:` field. Enforced as a dispatch preflight: once this instance's
    /// summed cost reaches it, further spawns are refused. `None`/0 = unlimited.
    #[serde(default)]
    pub instance_max_usd: Option<f64>,
}

fn default_role() -> String {
    "rat".into()
}

/// Render one budget scope's rollup for `rk cost --fleet`: spend vs cap, the
/// remaining headroom, and an "ok"/"warn"/"exceeded"/"unlimited" status. A
/// `repo` label is attached for per-repo rows and omitted for the fleet total.
fn scope_json(spent: f64, cap: f64, warn_at: f64, repo: Option<String>) -> serde_json::Value {
    let warn_frac = if warn_at > 0.0 { warn_at } else { 0.8 };
    let status = if cap <= 0.0 {
        "unlimited"
    } else if spent >= cap {
        "exceeded"
    } else if spent >= cap * warn_frac {
        "warn"
    } else {
        "ok"
    };
    let mut obj = json!({
        "spent_usd": spent,
        "cap_usd": cap,
        "remaining_usd": if cap > 0.0 { (cap - spent).max(0.0) } else { 0.0 },
        "status": status,
    });
    if let Some(repo) = repo {
        obj["repo"] = json!(repo);
    }
    obj
}

pub struct Supervisor {
    layout: Layout,
    castle: String,
    default_harness: String,
    default_agent: AgentProfile,
    registry: Mutex<Registry>,
    /// Live control handles (not persisted; gone after restart).
    controls: Mutex<HashMap<String, SessionControl>>,
    space: Space,
    /// Shared with the server so ticket-lifecycle writes serialize on one lock.
    tickets: Arc<crate::tickets::Tickets>,
    pricing: PricingTable,
    budget: Budget,
    /// Hierarchical fleet/repo caps enforced as a pre-dispatch guard.
    fleet_budget: FleetBudget,
    /// Agents already warned about budget (avoid repeat warnings).
    budget_warned: Mutex<std::collections::HashSet<String>>,
    /// Fleet/repo scopes already warned at dispatch (avoid repeat obstacles).
    fleet_warned: Mutex<std::collections::HashSet<String>>,
    /// Per-agent liveness-sweep bookkeeping (burn-rate deltas + flag episodes).
    sweep_state: Mutex<HashMap<String, SweepState>>,
    /// Per-agent self-healing-respawn bookkeeping (attempt count + backoff
    /// clock + whether the cap has already been escalated). In-memory: a daemon
    /// restart is a fresh episode, so attempt counts reset with it.
    respawn_state: Mutex<HashMap<String, RespawnState>>,
    /// Per-agent completion-routing state, so one agent generation emits
    /// exactly one durable `harness_result` — the one for the turn it actually
    /// finished on (TKT-160). In-memory: a daemon restart loses the withheld
    /// turn, which is the same blindness a restart already imposes on the
    /// harness event stream it came from.
    completions: Mutex<HashMap<String, CompletionState>>,
    /// Bounded per-agent transcript (assistant text / tool calls / retries),
    /// so the operator can `rk log` a run instead of being blind to it.
    log: crate::agent_log::AgentLog,
    /// Serializes concurrent land/dismiss merges to the same target branch so
    /// unattended auto-merges never interleave and lose a branch (TKT-51).
    merge_queue: MergeQueue,
    /// `[disk] min_free_gb` (0 = disabled), applied by `Daemon::new` from
    /// config. Defaults to 0 here — a bare `Supervisor` constructed directly
    /// by a test or another crate stays disk-guard-free unless it opts in via
    /// [`set_min_free_disk_gb`](Supervisor::set_min_free_disk_gb), so this
    /// safety feature cannot spuriously fail spawns on a tight CI disk.
    min_free_disk_gb: AtomicU64,
}

/// How far one agent generation has got through reporting its completion.
///
/// A harness returns control once per TURN, not once per task: a re-armed
/// monitor, a background test suite finishing, or a task notification all end a
/// turn and produce a `Completed` event while the rat is still mid-task. Every
/// one of those used to be published as a durable `harness_result`, and every
/// reader of that event — the workflow `wait`, the reactor's steward trigger,
/// the ticket auto-close — takes the OLDEST match, i.e. the mid-flight one.
/// This is the bookkeeping that reduces a generation's turns to the single turn
/// it finished on. See [`Supervisor::claim_completion`].
#[derive(Debug, Clone)]
struct CompletionState {
    /// Which generation of the name this state describes (the record's
    /// `created_at`). A later generation resets it.
    generation: DateTime<Utc>,
    /// This generation's `harness_result` has been published; later turns of
    /// the same generation must not publish a second one.
    routed: bool,
    /// A clean turn result is being held back because nothing yet proves the
    /// session is over. Superseded by each later turn, and flushed if the
    /// process exits (see the `Exited` arm of [`Supervisor::handle_event`]).
    withheld: bool,
}

/// What [`Supervisor::claim_completion`] decided about the turn that just ended.
#[derive(Debug, Clone, Copy)]
struct TurnClaim {
    /// Publish this turn as the generation's `harness_result`.
    publish: bool,
    /// This generation positively declared completion, normally through
    /// `rk done`; restricted one-shot jcode onboarding uses its native terminal
    /// event instead. Carried onto the published event so a reader can tell an
    /// agent that finished from one that merely stopped producing turns.
    declared_done: bool,
}

/// One agent's rolling state across supervisor sweeps.
#[derive(Debug, Clone)]
struct SweepState {
    /// Cost at the previous sweep, for the burn-rate delta.
    last_cost_usd: f64,
    /// When the previous sweep observed this agent (burn-rate denominator).
    last_observed: DateTime<Utc>,
    /// When the current STUCK/RUNAWAY episode was first flagged (soft-steered).
    /// `None` = not currently flagged. The kill escalation measures from here.
    flagged_at: Option<DateTime<Utc>>,
}

/// What a sweep decided to do about one agent, computed under the sweep-state
/// lock and then acted on after releasing it (steer/kill spawn async tasks).
enum SweepAction {
    /// Healthy, or still within the grace window — leave it alone.
    None,
    /// First detection this episode: obstacle tuple + soft steer.
    Soft { kind: &'static str, detail: String },
    /// Still flagged past the grace window: obstacle tuple + kill.
    Hard { kind: &'static str, detail: String },
}

/// One crashed agent's rolling self-healing-respawn state across sweeps.
#[derive(Debug, Clone)]
struct RespawnState {
    /// How many auto-respawns have fired for this agent this daemon lifetime.
    attempts: u32,
    /// When the last auto-respawn fired — the exponential-backoff clock.
    last_attempt: DateTime<Utc>,
    /// Set once the attempt cap has been hit and a `need` escalated, so the
    /// sweep does not re-escalate the same exhausted agent every cycle.
    escalated: bool,
}

/// What the respawn sweep decided to do about one crashed agent, computed under
/// the respawn-state lock and acted on after it is released (respawn launches).
enum RespawnDecision {
    /// Within the backoff window (or nothing to do) — leave it for next sweep.
    Wait,
    /// Backoff elapsed and attempts remain — relaunch it in its worktree.
    Respawn,
    /// Attempt cap exhausted — escalate a `need` for a human, once.
    Escalate,
}

/// Serializes merges to the same target branch — the land / merge queue.
///
/// Both the steward's `land` step and `dismiss`/`dismiss_all` merge branches
/// into `main` (or any base) concurrently and unattended. Without
/// serialization two auto-merges racing on the same target interleave: each
/// merges in its own detached worktree captured from the target ref *before*
/// the other advanced it, so the compare-and-swap in [`Repo::merge_branch`]
/// bounces the loser to a silent `merged: false` and its branch is left
/// unmerged (the root cause of the "done ticket never in main" gap). A
/// per-`(repo, target)` FIFO lock makes every land/dismiss to a given target
/// take its turn on the *freshly-updated* target, so each merge either applies
/// cleanly or is a genuine conflict — never a lost race. Merges to distinct
/// targets keep separate locks and still run concurrently.
#[derive(Default)]
struct MergeQueue {
    /// One async mutex per active target key; entries are created on demand.
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl MergeQueue {
    fn key(repo_root: &std::path::Path, target: &str) -> String {
        // NUL can't appear in a path or ref name, so it's an unambiguous joiner.
        format!("{}\u{0}{}", repo_root.display(), target)
    }

    /// Acquire the serialization lock for `(repo_root, target)`. The returned
    /// guard is held for the duration of one merge; the next waiter proceeds
    /// only once it drops. tokio's `Mutex` is FIFO, so callers land in arrival
    /// order — the "queue" in merge queue.
    async fn acquire(
        &self,
        repo_root: &std::path::Path,
        target: &str,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        let key = Self::key(repo_root, target);
        // Clone the Arc out under the std mutex, then release it *before*
        // awaiting the async lock — never hold a std guard across an await.
        let lock = {
            let mut locks = self.locks.lock().unwrap();
            Arc::clone(
                locks
                    .entry(key)
                    .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
            )
        };
        lock.lock_owned().await
    }
}

/// Which of an archived record's leftovers `rk prune` should reclaim, beyond
/// the record itself. Named fields rather than two positional `bool`s, because
/// silently swapping them is exactly the bug worth designing out.
///
/// Two switches and not one, because they answer different questions. A branch
/// can still hold the only copy of a rat's work, so `git` defers to whether it
/// has landed; a transcript only narrates work that lives elsewhere, so `logs`
/// has nothing to defer to. Folding them together would mean either deleting
/// the transcript of the very rat whose branch was kept back for a closer look,
/// or never reaping the transcript of a rat that had no branch at all.
#[derive(Debug, Clone, Copy, Default)]
pub struct Reap {
    /// The worktree and local branch — merged branches only.
    pub git: bool,
    /// The generation's transcript file under `agent-logs/`. One-way.
    pub logs: bool,
}

impl Supervisor {
    pub fn new(
        layout: Layout,
        castle: String,
        default_harness: String,
        budget: Budget,
        fleet_budget: FleetBudget,
        space: Space,
        tickets: Arc<crate::tickets::Tickets>,
    ) -> rk_core::Result<Self> {
        Self::new_with_agent_defaults(
            layout,
            castle,
            AgentDefaults::new(default_harness, AgentProfile::default()),
            budget,
            fleet_budget,
            space,
            tickets,
        )
    }

    pub(crate) fn new_with_agent_defaults(
        layout: Layout,
        castle: String,
        defaults: AgentDefaults,
        budget: Budget,
        fleet_budget: FleetBudget,
        space: Space,
        tickets: Arc<crate::tickets::Tickets>,
    ) -> rk_core::Result<Self> {
        let registry = Registry::load(&layout.home().join("agents.json"))?;
        let mut pricing = PricingTable::vendored();
        // User/runtime overrides, LiteLLM-shaped.
        let overrides = layout.home().join("pricing.json");
        if let Ok(json) = std::fs::read_to_string(&overrides) {
            match pricing.merge_pricing_json(&json) {
                Ok(n) => tracing::info!(entries = n, "merged pricing overrides"),
                Err(e) => warn!(error = %e, "invalid pricing.json ignored"),
            }
        }
        let log = crate::agent_log::AgentLog::new(&layout);
        Ok(Self {
            layout,
            castle,
            default_harness: defaults.harness,
            default_agent: defaults.profile,
            registry: Mutex::new(registry),
            controls: Mutex::new(HashMap::new()),
            space,
            tickets,
            pricing,
            budget,
            fleet_budget,
            budget_warned: Mutex::new(std::collections::HashSet::new()),
            fleet_warned: Mutex::new(std::collections::HashSet::new()),
            sweep_state: Mutex::new(HashMap::new()),
            respawn_state: Mutex::new(HashMap::new()),
            completions: Mutex::new(HashMap::new()),
            log,
            merge_queue: MergeQueue::default(),
            min_free_disk_gb: AtomicU64::new(0),
        })
    }

    /// Set the `[disk] min_free_gb` floor (0 = disabled). Applied by
    /// `Daemon::new` from config; exposed on `&self` (not `&mut self`) since
    /// the supervisor is shared behind an `Arc` from construction onward.
    pub fn set_min_free_disk_gb(&self, gb: u64) {
        self.min_free_disk_gb.store(gb, Ordering::Relaxed);
    }

    /// The per-agent transcript store (for `agent.log` reads and `--follow`).
    pub fn log(&self) -> &crate::agent_log::AgentLog {
        &self.log
    }

    /// Every transcript generation of `name`, oldest first, each carrying the
    /// exclusive upper bound (the next generation's `created_at`) that isolates
    /// it inside a legacy name-keyed log file.
    ///
    /// Empty when no record — live or archived — carries the name. Callers
    /// reading a transcript anyway should fall back to
    /// [`Generation::unrecorded`](crate::agent_log::Generation::unrecorded).
    pub fn log_generations(&self, name: &str) -> Vec<crate::agent_log::Generation> {
        let starts = self.lock_registry().generations_of(name);
        starts
            .iter()
            .enumerate()
            .map(|(i, &start)| {
                crate::agent_log::Generation::of(name, start, starts.get(i + 1).copied())
            })
            .collect()
    }

    /// Called once the daemon has WON the socket bind — never earlier. A
    /// Daemon that loses the bind race must not touch shared registry state.
    pub fn on_daemon_started(&self) {
        match self.lock_registry().orphan_live_agents() {
            Ok(orphaned) if !orphaned.is_empty() => {
                warn!(?orphaned, "orphaned live agents from previous daemon run");
            }
            Ok(_) => {}
            Err(e) => warn!(error = %e, "failed to orphan stale agents"),
        }
    }

    fn mark_spawn_failed(&self, name: &str, error: &rk_core::Error) {
        let detail = error.to_string();
        if let Err(e) = self.lock_registry().update(name, |record| {
            record.state = AgentState::Failed;
            record.result = Some(detail.clone());
            record.pid = None;
        }) {
            warn!(agent = name, error = %e, "failed to record spawn failure");
        }
    }

    /// Async callers must not run Git discovery/worktree setup or harness
    /// launch on a Tokio worker. The synchronous method remains for already
    /// blocking supervisors and tests.
    ///
    /// `fleet_wip_cap`: see [`spawn`](Self::spawn).
    pub async fn spawn_async(
        self: &Arc<Self>,
        params: SpawnParams,
        fleet_wip_cap: usize,
    ) -> rk_core::Result<AgentRecord> {
        let supervisor = Arc::clone(self);
        let handle = tokio::runtime::Handle::current();
        blocking_io("agent spawn", move || {
            let _entered = handle.enter();
            supervisor.spawn(params, fleet_wip_cap)
        })
        .await
    }

    /// `fleet_wip_cap` is the fleet-wide concurrent-agent ceiling this call
    /// must be atomically admitted against before any other spawn work
    /// begins (worktree, branch, harness launch) — `0` means this caller
    /// does not enforce one (manual/operator spawns, sub-spawns), matching
    /// pre-admission-control behaviour. The continuous-drain autoscaler and a
    /// workflow `spawn` step both pass the same `[drain] max_wip` value here,
    /// which is what makes the ceiling bidirectional: both share one
    /// admission path (see [`Registry::try_reserve_wip`]) so neither can
    /// TOCTOU-race the other onto the same free slot.
    pub fn spawn(
        self: &Arc<Self>,
        params: SpawnParams,
        fleet_wip_cap: usize,
    ) -> rk_core::Result<AgentRecord> {
        validate_role(&params.role)?;
        let repo = Repo::discover(std::path::Path::new(&params.repo))?;
        let repo_name = repo.name();
        let repo_policy = self.repository_policy(&repo);

        // Hierarchical fleet/repo budget guard: the wallet kill-switch. Once the
        // fleet-wide (or per-repo) cost sum reaches its cap we refuse the spawn
        // here — before any worktree/branch/name is allocated — so a runaway
        // autoscaler or nightly drain stops dispatching instead of draining the
        // account. Single spawns and workflow fan-out both funnel through here.
        // A workflow's per-instance cap is enforced in the same preflight.
        self.check_dispatch_budget(
            &repo_name,
            params.workflow_instance.as_deref(),
            params.instance_max_usd,
        )?;
        self.check_disk_floor(&repo_name)?;
        let target_branch = match &params.base {
            Some(b) => b.clone(),
            None => repo_policy.delivery_target(&repo.current_branch()?),
        };
        let instruction_base = self.instruction_base(&params.role, &target_branch, &repo);

        // Resolve the harness before journaling so an unknown adapter never
        // leaves a durable failed row. After this point every side effect has a
        // registry record to explain it, including a worktree or launch failure.
        let effective =
            effective_agent_config(&self.default_harness, &self.default_agent, &params)?;
        let harness = make_harness(&effective.harness)?;
        if params.attach && uses_harness_terminal_completion(&params.role, &effective.harness) {
            return Err(rk_core::Error::other(
                "jcode onboarding is headless-only: its restricted tool surface cannot run \
                 `rk done`, so Rat Kingdom completes the assessment from jcode's one-shot \
                 terminal event",
            ));
        }

        // Atomically admit one fleet-WIP slot and reserve the name in the
        // SAME registry-lock critical section: the free-slot check and the
        // reservation must not be two separate lock acquisitions, or two
        // concurrent callers (a drain refill and a workflow `spawn` step, or
        // two of either) can each observe the same free slot before either's
        // spawn lands in the registry. Name reservation stays claimed against
        // concurrent spawns until the journal row is inserted; picking
        // without reserving let two near-simultaneous spawns grab the same
        // name and collide on the worktree path.
        let name = {
            let mut reg = self.lock_registry();
            if !reg.try_reserve_wip(fleet_wip_cap) {
                return Err(rk_core::Error::other(FLEET_WIP_CAP_REFUSED));
            }
            reg.reserve_name()
        };
        let (branch, worktree) = if params.role == ONBOARDER_ROLE {
            if !params.task.starts_with("onb-")
                || !params
                    .task
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
            {
                let mut reg = self.lock_registry();
                reg.release_name(&name);
                reg.release_wip(fleet_wip_cap);
                return Err(rk_core::Error::other(
                    "onboarder task must be a stable onb- session id",
                ));
            }
            (
                onboarding_branch(&params.task),
                onboarding_worktree(&self.layout.worktrees_dir(), &repo_name, &params.task),
            )
        } else {
            (
                repo_policy.branch_name(&name, &params.task, &repo_name, &params.role),
                self.layout.worktrees_dir().join(repo_policy.worktree_path(
                    &name,
                    &params.task,
                    &repo_name,
                    &params.role,
                )),
            )
        };
        let spawning = spawning_record(SpawnJournal {
            params: &params,
            repo: &repo,
            repo_name: &repo_name,
            name: name.clone(),
            branch: branch.clone(),
            worktree: worktree.clone(),
            target_branch: target_branch.clone(),
            harness: effective.harness.clone(),
            model: effective.model.clone(),
            permission_mode: effective.permission_mode.clone(),
        });
        if let Err(e) = self.lock_registry().insert(spawning) {
            let mut reg = self.lock_registry();
            reg.release_name(&name);
            reg.release_wip(fleet_wip_cap);
            return Err(e);
        }
        // The reservation's job is done: this spawn now has a live registry
        // row (state `Spawning`), which itself counts toward the fleet-WIP
        // ceiling from here on — worktree creation and harness launch (both
        // potentially slow) proceed without holding the reservation, and any
        // failure from here is recorded on that row via `mark_spawn_failed`,
        // which naturally frees its slot by leaving the live count.
        self.lock_registry().release_wip(fleet_wip_cap);
        if let Err(e) = repo.create_worktree(&worktree, &branch, &target_branch) {
            self.mark_spawn_failed(&name, &e);
            return Err(e);
        }

        let prime_ctx = PrimeContext {
            agent: name.clone(),
            repo: repo_name.clone(),
            task: Some(params.task.clone()),
            branch: Some(branch.clone()),
            base: Some(instruction_base.clone()),
            parent: params.parent.clone(),
            facts: self.scan_facts(&repo_name),
            conventions: self.scan_conventions(&repo_name),
            verification_checks: self.scan_verification_checks(&worktree),
            harness_terminal_completion: uses_harness_terminal_completion(
                &params.role,
                &effective.harness,
            ),
        };
        let prompt = params
            .prompt
            .clone()
            .unwrap_or_else(|| format!("Work on task {}. Begin now.", params.task));

        let mut env = self.agent_env(
            &name,
            &params.role,
            &repo_name,
            &params.task,
            Some(&branch),
            &instruction_base,
            &worktree,
            params.workflow_instance.as_deref(),
        );
        if let Some(parent) = &params.parent {
            env.insert("RK_PARENT".into(), parent.clone());
        }

        let spec = LaunchSpec {
            prompt,
            system_prompt: Some(render(&params.role, &prime_ctx)),
            cwd: worktree.clone(),
            env,
            // Ordinary workers must run unattended; onboarding's restricted
            // mode was enforced during effective config resolution above.
            // Persist this exact value so respawn cannot silently narrow it.
            permission_mode: Some(effective.permission_mode),
            model: effective.model,
            resume_session: None,
        };

        if params.attach {
            return self.spawn_attached(
                params,
                repo,
                repo_name,
                name,
                branch,
                worktree,
                target_branch,
                effective.harness,
                spec,
            );
        }

        let session = match harness.launch(&spec) {
            Ok(s) => s,
            Err(e) => {
                let _ = repo.remove_worktree(&worktree);
                let _ = repo.delete_branch(&branch);
                self.mark_spawn_failed(&name, &e);
                return Err(e);
            }
        };

        let record = self
            .lock_registry()
            .update(&name, |record| {
                record.state = AgentState::Running;
                record.pid = session.pid;
            })?
            .ok_or_else(|| rk_core::Error::other("spawn journal row vanished"))?;
        self.lock_controls()
            .insert(name.clone(), session.control.clone());

        self.emit_event(
            &repo_name,
            "agent_spawned",
            json!({
                "agent": name,
                "task": params.task,
                "role": params.role,
                "parent": params.parent,
                "workflow_instance": params.workflow_instance,
            }),
        );
        self.emit_coordinator_event(
            &record,
            "agent_lifecycle",
            json!({
                "route": "rollup",
                "severity": "info",
                "change": "started",
                "summary": format!("{} started", record.name),
                "coordinator": record.coordinator,
                "workflow_instance": record.workflow_instance,
                "agent": record.name,
                "generation": record.created_at,
            }),
        );
        let supervisor = Arc::clone(self);
        let mut events = session.events;
        let generation = record.created_at;
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                supervisor.handle_event(&name, generation, event);
            }
        });

        Ok(record)
    }

    /// Attach-mode spawn: the harness runs interactively in a herdr pane.
    /// The daemon still owns the worktree/branch/registry; completion arrives
    /// via the rat's own `rk done` tuple, and humans can attach any time.
    #[allow(clippy::too_many_arguments)]
    fn spawn_attached(
        self: &Arc<Self>,
        params: SpawnParams,
        repo: Repo,
        repo_name: String,
        name: String,
        branch: String,
        worktree: std::path::PathBuf,
        target_branch: String,
        harness_kind: String,
        spec: LaunchSpec,
    ) -> rk_core::Result<AgentRecord> {
        if !rk_mux::HerdrMux::available() {
            let _ = repo.remove_worktree(&worktree);
            let _ = repo.delete_branch(&branch);
            let error = rk_core::Error::other(
                "--attach needs a running herdr server (https://herdr.dev); \
                 spawn headless or start herdr first",
            );
            self.mark_spawn_failed(&name, &error);
            return Err(error);
        }
        let argv = match rk_mux::interactive_argv(
            &harness_kind,
            spec.system_prompt.as_deref(),
            spec.model.as_deref(),
            spec.permission_mode.as_deref(),
        ) {
            Ok(argv) => argv,
            Err(e) => {
                self.mark_spawn_failed(&name, &e);
                return Err(e);
            }
        };
        let mut attach_env = spec.env.clone();
        if harness_kind == "jcode" {
            attach_env.insert("JCODE_SWARM_ENABLED".into(), "0".into());
            attach_env.insert("JCODE_AUTO_POKE".into(), "0".into());
        }
        let target = match rk_mux::HerdrMux::start_agent(&name, &worktree, &attach_env, &argv) {
            Ok(t) => t,
            Err(e) => {
                let _ = repo.remove_worktree(&worktree);
                let _ = repo.delete_branch(&branch);
                self.mark_spawn_failed(&name, &e);
                return Err(e);
            }
        };

        let record = self
            .lock_registry()
            .update(&name, |record| {
                record.state = AgentState::Running;
                record.attach_target = Some(target.clone());
                record.target_branch = target_branch.clone();
            })?
            .ok_or_else(|| rk_core::Error::other("spawn journal row vanished"))?;
        self.emit_event(
            &repo_name,
            "agent_spawned",
            json!({
                "agent": name,
                "task": params.task,
                "role": params.role,
                "attached": true,
                "workflow_instance": params.workflow_instance,
            }),
        );
        self.emit_coordinator_event(
            &record,
            "agent_lifecycle",
            json!({
                "route": "rollup",
                "severity": "info",
                "change": "started",
                "summary": format!("{} started", record.name),
                "coordinator": record.coordinator,
                "workflow_instance": record.workflow_instance,
                "agent": record.name,
                "generation": record.created_at,
            }),
        );

        // Deliver the initial prompt once herdr reports the TUI idle (its
        // integration hook, not a sleep); fall back to sending anyway.
        {
            let target = target.clone();
            let prompt = if harness_kind == "jcode" {
                match &spec.system_prompt {
                    Some(system) => format!("{system}\n\n---\n\n{}", spec.prompt),
                    None => spec.prompt.clone(),
                }
            } else {
                spec.prompt.clone()
            };
            tokio::task::spawn_blocking(move || {
                let _ = std::process::Command::new("herdr")
                    .args([
                        "agent",
                        "wait",
                        &target,
                        "--status",
                        "idle",
                        "--timeout",
                        "30000",
                    ])
                    .output();
                if let Err(e) = rk_mux::HerdrMux::send(&target, &prompt) {
                    warn!(error = %e, "failed to deliver prompt to herdr pane");
                }
            });
        }

        self.watch_attached_completion(&record);

        Ok(record)
    }

    /// Reinstallable attach watcher. The pane can outlive the daemon, so
    /// restart recovery must be able to wire this durable signal back up.
    fn watch_attached_completion(self: &Arc<Self>, record: &AgentRecord) {
            let supervisor = Arc::clone(self);
        let agent = record.name.clone();
            let space = self.space.clone();
            // Bound the read to this generation of the name. `task_done` events
            // are durable and outlive the rat they name, so an unbounded name
        // search matches a predecessor's completion.
            let since = record.created_at;
            tokio::spawn(async move {
                let pattern = Pattern::for_agent_since(Category::Event, "task_done", &agent, since);
                match space
                    .rd(&pattern, std::time::Duration::from_secs(24 * 3600))
                    .await
                {
                    Ok(Some(tuple)) => {
                        let diff = supervisor.diff_summary_for(&agent);
                        let updated = supervisor.lock_registry().update(&agent, |r| {
                            r.state = AgentState::Completed;
                            r.result = tuple.payload["summary"]
                                .as_str()
                                .map(String::from)
                                .or(Some("done".into()));
                        });
                        if let Ok(Some(record)) = updated {
                            supervisor.route_completion(&record, false, true, diff);
                            rk_mux::HerdrMux::notify(
                                &format!("{agent} finished"),
                                record.result.as_deref().unwrap_or(""),
                            );
                        }
                    }
                Ok(None) => warn!(agent = %agent, "attach-mode completion watch timed out"),
                    Err(e) => warn!(error = %e, "completion watch failed"),
                }
            });
        }

    /// Resume an orphaned/failed agent in its preserved worktree.
    pub fn respawn(self: &Arc<Self>, name: &str) -> rk_core::Result<AgentRecord> {
        self.respawn_mode(name, false)
    }

    /// Resume an onboarding agent in either durable presentation mode. Unlike
    /// ordinary `agent.respawn`, this may reattach to a herdr pane that
    /// survived the daemon or recreate one in the preserved worktree.
    pub fn respawn_onboarding(
        self: &Arc<Self>,
        name: &str,
        attach: bool,
    ) -> rk_core::Result<AgentRecord> {
        let role = self
            .status(name)
            .map(|record| record.role)
            .ok_or_else(|| rk_core::Error::other(format!("no such agent: {name}")))?;
        if role != ONBOARDER_ROLE {
            return Err(rk_core::Error::other(format!(
                "onboarding session agent {name} has downgraded role {role:?}"
            )));
        }
        self.respawn_mode(name, attach)
    }

    pub async fn respawn_onboarding_async(
        self: &Arc<Self>,
        name: String,
        attach: bool,
    ) -> rk_core::Result<AgentRecord> {
        let supervisor = Arc::clone(self);
        let handle = tokio::runtime::Handle::current();
        blocking_io("onboarding agent resume", move || {
            let _entered = handle.enter();
            supervisor.respawn_onboarding(&name, attach)
        })
        .await
    }

    fn respawn_mode(self: &Arc<Self>, name: &str, attach: bool) -> rk_core::Result<AgentRecord> {
        let record = self
            .lock_registry()
            .get(name)
            .cloned()
            .ok_or_else(|| rk_core::Error::other(format!("no such agent: {name}")))?;
        if record.state.is_live() {
            return Err(rk_core::Error::other(format!("{name} is still running")));
        }
        validate_role(&record.role)?;
        if attach && uses_harness_terminal_completion(&record.role, &record.harness) {
            return Err(rk_core::Error::other(
                "jcode onboarding is headless-only: resume without `--attach`",
            ));
        }
        let (Some(worktree), Some(task)) = (record.worktree.clone(), record.task.clone()) else {
            return Err(rk_core::Error::other("record lacks worktree/task"));
        };

        let harness = make_harness(&record.harness)?;
        let resume = if harness.caps().resume {
            record.session_id.clone()
        } else {
            None
        };
        let repo = Repo::discover(&record.repo_root)?;
        let instruction_base =
            self.instruction_base(&record.role, &record.target_branch, &repo);

        let env = self.agent_env(
            &record.name,
            &record.role,
            &record.repo_name,
            &task,
            record.branch.as_deref(),
            &instruction_base,
            &worktree,
            record.workflow_instance.as_deref(),
        );

        let prime_ctx = PrimeContext {
            agent: record.name.clone(),
            repo: record.repo_name.clone(),
            task: record.task.clone(),
            branch: record.branch.clone(),
            base: Some(instruction_base),
            parent: record.parent.clone(),
            facts: self.scan_facts(&record.repo_name),
            conventions: self.scan_conventions(&record.repo_name),
            verification_checks: self.scan_verification_checks(&worktree),
            harness_terminal_completion: uses_harness_terminal_completion(
                &record.role,
                &record.harness,
            ),
        };
        let resume_prompt = if uses_harness_terminal_completion(&record.role, &record.harness) {
            format!(
                "Resume onboarding session {task}. Reassess the repository read-only, \
                 preserve the existing onboarding branch and session record, then return \
                 the final findings and stop. Do not edit or commit files and do not try \
                 to run `rk done`; the terminal response completes this assessment."
            )
        } else if record.role == ONBOARDER_ROLE {
            format!(
                "Resume onboarding session {task}. Reassess the repository read-only, \
                 preserve the existing onboarding branch and session record, and finish \
                 with `rk done` after reporting findings. Do not edit or commit files."
            )
        } else {
            format!(
                "You are resuming task {task} after an interruption. Check `git log` and \
                 `git status` in your worktree to see where you left off, then continue. \
                 Finish with `rk done` as usual."
            )
        };
        let spec = LaunchSpec {
            prompt: resume_prompt,
            system_prompt: Some(render(&record.role, &prime_ctx)),
            cwd: worktree.clone(),
            env,
            permission_mode: Some(respawn_permission_mode(&record)?),
            model: record.model.clone(),
            resume_session: resume,
        };
        if attach {
            return self.respawn_attached(record, spec);
        }
        let session = harness.launch(&spec)?;

        let updated = self
            .lock_registry()
            .update(name, |r| {
                r.state = AgentState::Running;
                r.pid = session.pid;
                r.result = None;
                // A fresh attempt: the previous crash no longer describes this
                // record, so a workflow waiting on it stops treating it as
                // abandoned (TKT-147).
                r.crashed = false;
                // The prior generation's stderr tail describes a run that is
                // now gone; a retry that fails silently, with no stderr of its
                // own, must not publish that stale diagnosis as if it were
                // current.
                r.stderr_tail = None;
            })?
            .ok_or_else(|| rk_core::Error::other("record vanished"))?;
        self.lock_controls()
            .insert(name.to_string(), session.control.clone());

        self.emit_event(
            &updated.repo_name,
            "agent_respawned",
            json!({
                "agent": name,
                "task": updated.task,
                "workflow_instance": updated.workflow_instance,
            }),
        );
        self.emit_coordinator_event(
            &updated,
            "agent_lifecycle",
            json!({
                "route": "rollup",
                "severity": "info",
                "change": "respawned",
                "summary": format!("{} respawned", updated.name),
                "coordinator": updated.coordinator,
                "workflow_instance": updated.workflow_instance,
                "agent": updated.name,
                "generation": updated.created_at,
            }),
        );

        // The interrupted run's completion bookkeeping does not carry over: its
        // withheld turn is stale, and (for a manually respawned Completed
        // record) its `routed` flag would gag the resumed run (TKT-160).
        self.forget_completion(name);

        let supervisor = Arc::clone(self);
        let owned = name.to_string();
        let mut events = session.events;
        // A respawn continues the SAME generation — the record (and its
        // `created_at`) is reused — so the second run appends to the transcript
        // the first run started, which is what an operator expects.
        let generation = updated.created_at;
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                supervisor.handle_event(&owned, generation, event);
            }
        });
        Ok(updated)
    }

    fn respawn_attached(
        self: &Arc<Self>,
        record: AgentRecord,
        spec: LaunchSpec,
    ) -> rk_core::Result<AgentRecord> {
        if !rk_mux::HerdrMux::available() {
            return Err(rk_core::Error::other(
                "--attach needs a running herdr server (https://herdr.dev)",
            ));
        }
        let existing = record
            .attach_target
            .as_deref()
            .filter(|target| rk_mux::HerdrMux::agent_status(target).is_some())
            .map(String::from);
        let (target, created) = if let Some(target) = existing {
            (target, false)
        } else {
            let argv = rk_mux::interactive_argv(
                &record.harness,
                spec.system_prompt.as_deref(),
                spec.model.as_deref(),
                spec.permission_mode.as_deref(),
            )?;
            let mut attach_env = spec.env.clone();
            if record.harness == "jcode" {
                attach_env.insert("JCODE_SWARM_ENABLED".into(), "0".into());
                attach_env.insert("JCODE_AUTO_POKE".into(), "0".into());
            }
            (
                rk_mux::HerdrMux::start_agent(&record.name, &spec.cwd, &attach_env, &argv)?,
                true,
            )
        };
        let updated = self
            .lock_registry()
            .update(&record.name, |current| {
                current.state = AgentState::Running;
                current.pid = None;
                current.attach_target = Some(target.clone());
                current.result = None;
                current.crashed = false;
                // See the ordinary respawn path above: a stale stderr tail
                // from the previous generation must not survive a retry.
                current.stderr_tail = None;
            })?
            .ok_or_else(|| rk_core::Error::other("record vanished"))?;

        if created {
            let target = target.clone();
            let prompt = if record.harness == "jcode" {
                match spec.system_prompt {
                    Some(system) => format!("{system}\n\n---\n\n{}", spec.prompt),
                    None => spec.prompt,
                }
            } else {
                spec.prompt
            };
            tokio::task::spawn_blocking(move || {
                let _ = std::process::Command::new("herdr")
                    .args([
                        "agent",
                        "wait",
                        &target,
                        "--status",
                        "idle",
                        "--timeout",
                        "30000",
                    ])
                    .output();
                if let Err(e) = rk_mux::HerdrMux::send(&target, &prompt) {
                    warn!(error = %e, "failed to deliver resume prompt to herdr pane");
                }
            });
        }

        self.emit_event(
            &updated.repo_name,
            "agent_respawned",
            json!({
                "agent": updated.name,
                "task": updated.task,
                "attached": true,
                "workflow_instance": updated.workflow_instance,
            }),
        );
        self.forget_completion(&updated.name);
        self.watch_attached_completion(&updated);
        Ok(updated)
    }

    /// `generation` is the agent record's `created_at`, captured once when the
    /// event loop is wired up: transcript writes are keyed on the generation, not
    /// the name, so a line can never land in a namesake's file.
    fn handle_event(self: &Arc<Self>, name: &str, generation: DateTime<Utc>, event: HarnessEvent) {
        match event {
            HarnessEvent::Started { session_id } => {
                let _ = self.lock_registry().update(name, |r| {
                    r.session_id = session_id.clone();
                });
            }
            HarnessEvent::Usage { usage } => {
                let updated = self.lock_registry().update(name, |r| {
                    r.usage.add(&usage);
                    // Incremental cost for harnesses that don't self-report
                    // USD; an authoritative Completed cost overwrites later.
                    if let Some(model) = &r.model {
                        if let Some(price) = self.pricing.lookup(model) {
                            r.cost_usd += price.cost(&usage);
                        }
                    }
                });
                if let Ok(Some(record)) = updated {
                    self.enforce_budget(&record);
                }
            }
            HarnessEvent::Completed {
                result,
                is_error,
                usage,
                cost_usd,
                session_id,
            } => {
                let diff = self.diff_summary_for(name);
                let updated = self.lock_registry().update(name, |r| {
                    r.state = if is_error {
                        AgentState::Failed
                    } else {
                        AgentState::Completed
                    };
                    r.result = Some(result.clone());
                    if usage.total() > 0 {
                        r.usage = usage;
                    }
                    if let Some(cost) = cost_usd {
                        r.cost_usd = cost;
                    }
                    if session_id.is_some() {
                        r.session_id = session_id.clone();
                    }
                });
                if let Ok(Some(record)) = updated {
                    let claim = self.claim_completion(
                        name,
                        generation,
                        is_error,
                        uses_harness_terminal_completion(&record.role, &record.harness),
                    );
                    if claim.publish {
                        info!(agent = name, is_error, "agent completed");
                        self.route_completion(&record, is_error, claim.declared_done, diff);
                    } else {
                        info!(
                            agent = name,
                            "harness returned control without a `rk done`; holding the turn \
                             result back rather than publishing it as the completion"
                        );
                    }
                }
            }
            HarnessEvent::Exited { code } => {
                let diff = self.diff_summary_for(name);
                self.lock_controls().remove(name);
                let updated = self.lock_registry().update(name, |r| {
                    r.pid = None;
                    // Exit without a Completed event = crash/kill.
                    if r.state.is_live() {
                        r.state = AgentState::Failed;
                        // The one place that knows the harness never reported a
                        // verdict for this generation, so no `harness_result`
                        // exists or ever will. Recorded as data rather than
                        // left to be inferred from the result string, because a
                        // workflow `wait`/`evaluate` has to be able to tell a
                        // rat that produced nothing from one that ran (TKT-147).
                        r.crashed = true;
                        let base = format!("process exited (code {code:?}) without completing");
                        // A starved/misconfigured harness (rate limit, queueing,
                        // auth refresh, model unavailable) can produce zero
                        // protocol output and die silently — stderr is the only
                        // trace of why, so fold its tail into the published
                        // result rather than leaving this message as the whole
                        // story wherever `result` is read (inbox, harness_result).
                        r.result = Some(match r.stderr_snippet() {
                            Some(snippet) => format!("{base} — stderr: {snippet}"),
                            None => base,
                        });
                    }
                });
                // The process is gone, so no further turn can follow: a turn
                // result held back for want of a `rk done` is now provably this
                // generation's last word, and must be published. Harnesses that
                // end with the run (codex, the test fake) take this path
                // for every agent; a Claude session — which stays alive between
                // turns to receive steers — normally reports at its `rk done`
                // and only lands here when it is killed mid-task.
                //
                // Anything flushed here publishes as a FAILURE, whatever the
                // exit status (TKT-175). Reaching this flush is itself the
                // proof: a turn result is withheld for exactly one reason —
                // the generation had not written its `task_done` — so the text
                // being published is a mid-flight turn ("the test suite is
                // still running"), and by the fleet's own completion protocol a
                // rat that never declared itself done did not finish its task.
                // The honest publication is `is_error: true`.
                //
                // TKT-173 shipped the narrower half of this, keyed on whether
                // the process was KILLED (`status.code()` is `None` for a
                // signal-terminated child, and both the budget hard-stop and the
                // sweep's hard escalation SIGTERM the process group). That was
                // the loudest case: the mid-flight turn had already set the
                // record's state to `Completed`, so the kill did not read as a
                // crash either — TKT-147's `crashed` is only set over a LIVE
                // state — and a rat stopped by the budget reported
                // `is_error: false` to the workflow waiting on it.
                //
                // What survived was the same lie told more quietly: a rat that
                // exited 0 mid-task — every codex run, whose harness ends
                // with the run rather than staying alive between turns — also
                // published `is_error: false`, and every workflow gating on
                // `expect {is_error: false}` accepted an unfinished task. How
                // the process ended is not the question; whether the rat said it
                // was finished is.
                //
                // The turn's TEXT is still published as-is, so `rk inbox` shows
                // what the rat had got to; only the verdict changes. A rat that
                // did declare done never reaches here at all — `claim_completion`
                // published its turn when it ended — which is what keeps this
                // from collapsing into "every fake-harness agent fails".
                if let Ok(Some(record)) = updated {
                    if self.flush_withheld_completion(name, generation) {
                        info!(
                            agent = name,
                            killed = code != Some(0),
                            exit_code = ?code,
                            "agent ended without ever running `rk done`; publishing its last \
                             turn result as a failure"
                        );
                        self.route_completion(&record, true, false, diff);
                    }
                }
            }
            // Formerly dropped on the floor; now persisted as the agent's
            // transcript so the operator can `rk log` a run without --attach.
            HarnessEvent::AssistantText { text } => {
                self.log
                    .append(name, generation, crate::agent_log::LogEvent::Text { text });
            }
            HarnessEvent::ToolUse { name: tool } => {
                self.log.append(
                    name,
                    generation,
                    crate::agent_log::LogEvent::Tool { name: tool },
                );
            }
            HarnessEvent::Retry { attempt, error } => {
                self.log.append(
                    name,
                    generation,
                    crate::agent_log::LogEvent::Retry { attempt, error },
                );
            }
            HarnessEvent::Stderr { text } => {
                self.log.append(
                    name,
                    generation,
                    crate::agent_log::LogEvent::Stderr { text: text.clone() },
                );
                let _ = self.lock_registry().update(name, |r| {
                    crate::agents::append_stderr_tail(&mut r.stderr_tail, &text);
                });
            }
        }
    }

    /// Graduated budget policy: warn once at the threshold (obstacle tuple +
    /// steer when possible), hard-stop at the cap.
    fn enforce_budget(self: &Arc<Self>, record: &AgentRecord) {
        match self.budget.check(record.cost_usd, record.usage.total()) {
            BudgetAction::Ok => {}
            BudgetAction::Warn => {
                if !self.mark_budget_warned(&record.name) {
                    return; // already warned
                }
                warn!(agent = %record.name, cost = record.cost_usd, "budget warning threshold crossed");
                self.emit_obstacle_for_budget(record, "warning");
                let control = self.lock_controls().get(&record.name).cloned();
                let name = record.name.clone();
                if let Some(control) = control {
                    tokio::spawn(async move {
                        let _ = control
                            .steer(&format!(
                                "BUDGET WARNING for {name}: you are approaching your \
                                 token/cost cap. Wrap up: commit what you have and run \
                                 `rk done` now."
                            ))
                            .await;
                    });
                }
            }
            BudgetAction::Stop => {
                warn!(agent = %record.name, cost = record.cost_usd, tokens = record.usage.total(), "budget cap hit — stopping agent");
                self.emit_obstacle_for_budget(record, "exceeded");
                let control = self.lock_controls().remove(&record.name);
                if let Some(control) = control {
                    tokio::spawn(async move {
                        let _ = control.kill().await;
                    });
                }
            }
        }
    }

    /// Returns true if this call newly marked the agent (first warning).
    fn mark_budget_warned(&self, name: &str) -> bool {
        match self.budget_warned.lock() {
            Ok(mut set) => set.insert(name.to_string()),
            Err(p) => p.into_inner().insert(name.to_string()),
        }
    }

    /// Active fleet conventions binding on a rat spawned into `repo`: the text
    /// of every `Convention` tuple in the `system` scope (fleet-wide norms) and
    /// the repo's own scope (repo-local norms). Composed into the rat's prompt
    /// as a "Standing conventions" section (stigmergy P6) so a quorum-promoted
    /// norm actually changes what the rat does. Scan/parse failures degrade to
    /// no conventions — priming must never fail on a convention read.
    ///
    /// Conventions carrying the same name (the text before the first `:`) are
    /// SUPERSEDED newest-wins within a scope. Convention tuples are Furniture:
    /// they cannot be edited or destructively taken, so refreshing a stale norm
    /// works by minting a newer tuple under the same name (quorum re-promotion
    /// or an operator refresh). Without this, a refreshed norm would inject
    /// BOTH texts and hand every rat a contradiction — exactly the drift that
    /// let `prove-your-tools-on-entry` keep ordering entry STOPs after the
    /// shipped prompt policy moved on (docs/proposals/prompts/0019).
    fn scan_conventions(&self, repo: &str) -> Vec<String> {
        let mut texts = Vec::new();
        for scope in [SYSTEM_SCOPE, repo] {
            let pattern = Pattern::category(Category::Convention).scope(scope);
            match self.space.scan(&pattern) {
                Ok(tuples) => {
                    texts.extend(supersede_conventions_newest_wins(
                        tuples
                            .iter()
                            .filter_map(|t| {
                                t.payload
                                    .get("text")
                                    .and_then(|v| v.as_str())
                                    .map(|text| (t.id.to_string(), text.to_string()))
                            })
                            .collect(),
                    ));
                }
                Err(e) => warn!(error = %e, scope, "failed to scan conventions for priming"),
            }
        }
        texts
    }

    /// Repo-owned named verification checks for a rat's worktree. A malformed
    /// or absent registry must not make priming fail: the worker gets the
    /// universal "do not guess" guidance and the workflow gate still fails
    /// closed when it explicitly references a bad check.
    fn scan_verification_checks(&self, worktree: &std::path::Path) -> Vec<VerificationCheck> {
        let file = worktree.join(".rk").join("checks.cue");
        if !file.exists() {
            return Vec::new();
        }
        match rk_workflow::load_checks(&file) {
            Ok(checks) => checks
                .into_iter()
                .map(|check| VerificationCheck {
                    name: check.name,
                    command: check.command,
                    cwd: check.cwd,
                    expect_exit: check.expect_exit,
                    timeout: check.timeout,
                    environment_policy: Some(check.environment_policy.to_string()),
                    toolchain: check.toolchain,
                })
                .collect(),
            Err(e) => {
                warn!(error = %e, path = %file.display(), "failed to load verification checks for priming");
                Vec::new()
            }
        }
    }

    /// Recent facts for a rat spawned into repo: newest facts from the system
    /// and repo scopes, interleaved so one scope cannot crowd the other out,
    /// then bounded to the prompt cap. Scan failures degrade to no facts —
    /// priming must never fail on a knowledge read.
    fn scan_facts(&self, repo: &str) -> Vec<String> {
        let mut scoped = Vec::new();
        for scope in [SYSTEM_SCOPE, repo] {
            let pattern = Pattern::category(Category::Fact).scope(scope);
            match self.space.scan_newest_limited(&pattern, MAX_INJECTED_FACTS) {
                Ok(tuples) => scoped.push(
                    tuples
                        .into_iter()
                        .map(|t| {
                            let payload = serde_json::to_string(&t.payload)
                                .unwrap_or_else(|_| "null".to_string());
                            format!(
                                "{} {} (reported by {}): {}",
                                t.id, t.identity, t.instance, payload
                            )
                        })
                        .collect::<Vec<_>>(),
                ),
                Err(e) => {
                    warn!(error = %e, scope, "failed to scan facts for priming");
                    scoped.push(Vec::new());
                }
            }
        }

        let mut facts = Vec::new();
        let mut offset = 0;
        while facts.len() < MAX_INJECTED_FACTS {
            let mut added = false;
            for entries in &scoped {
                if let Some(fact) = entries.get(offset) {
                    facts.push(fact.clone());
                    added = true;
                    if facts.len() == MAX_INJECTED_FACTS {
                        break;
                    }
                }
            }
            if !added {
                break;
            }
            offset += 1;
        }
        facts
    }

    fn emit_obstacle_for_budget(&self, record: &AgentRecord, kind: &str) {
        let tuple = Tuple::new(
            Category::Obstacle,
            record.repo_name.clone(),
            record.name.clone(),
            self.castle.clone(),
            json!({
                "type": format!("budget_{kind}"),
                "agent": record.name,
                "workflow_instance": record.workflow_instance,
                "task": record.task,
                "cost_usd": record.cost_usd,
                "tokens": record.usage.total(),
            }),
        );
        if let Err(e) = self.space.out(tuple.into_trail(DEFAULT_TRAIL_TTL)) {
            warn!(error = %e, "failed to emit budget obstacle");
        }
    }

    /// Cost rollups behind the dispatch preflight, for one `repo` and (when
    /// given) one workflow `instance`.
    ///
    /// The **fleet/repo** tallies count only *live* (`Spawning`/`Running`)
    /// agents (`AgentState::is_live`): a record that has left the live fleet —
    /// `Completed`, `Failed`, `Dismissed`, or `Orphaned` — lingers in the
    /// registry (for respawn, `rk log`, history) but its spend drops off. That
    /// keeps `fleet_max_usd`/`repo_max_usd` standing guardrails on the *current
    /// live/concurrent* fleet, not cumulative lifetime ceilings that would
    /// refuse all spawns once lifetime spend crossed the cap. TKT-39 dropped
    /// only `Dismissed`, but steward-landed rats linger as `Completed` (the
    /// steward lands via a separate reviewer branch and never dismisses the
    /// original ticket-rat), so their spend still accumulated and could
    /// silently block continuous-drain and every other spawn (TKT-40).
    ///
    /// The **instance** tally is a different knob (TKT-32): a workflow's
    /// `budget:` caps the *cumulative* spend of one finite run, so a completed
    /// sequential step must still count against that run's total. It therefore
    /// counts every non-dismissed agent under the instance — an instance ends
    /// with the run, so lingering `Completed` spend can never block an
    /// unrelated future spawn the way it does for the fleet/repo caps.
    fn cost_rollup(&self, repo: &str, instance: Option<&str>) -> (f64, f64, f64) {
        let reg = self.lock_registry();
        let mut fleet = 0.0;
        let mut repo_total = 0.0;
        let mut instance_total = 0.0;
        // `list_all`, not `list`: archiving a record must never move a budget
        // number. It cannot change the fleet/repo tallies (only live agents
        // count, and archiving never touches a live record), but the instance
        // tally is cumulative over a run, so an archived Completed step still
        // has to count against its workflow's `budget:`.
        for a in reg.list_all() {
            if a.state.is_live() {
                fleet += a.cost_usd;
                if a.repo_name == repo {
                    repo_total += a.cost_usd;
                }
            }
            if instance.is_some()
                && a.workflow_instance.as_deref() == instance
                && a.state != AgentState::Dismissed
            {
                instance_total += a.cost_usd;
            }
        }
        (fleet, repo_total, instance_total)
    }

    /// Preflight fleet/repo/instance budget guard run before every spawn.
    /// Returns `Err` (refusing dispatch) once a cap is hit; posts an obstacle on
    /// both the warn band and the hard cap so it surfaces in `rk inbox`. When
    /// `instance`/`instance_cap` are set, the workflow's per-instance cap is
    /// enforced alongside the global fleet/repo caps.
    fn check_dispatch_budget(
        &self,
        repo: &str,
        instance: Option<&str>,
        instance_cap: Option<f64>,
    ) -> rk_core::Result<()> {
        let (fleet_spent, repo_spent, instance_spent) = self.cost_rollup(repo, instance);
        // Only fold in the instance scope when this spawn carries an instance id
        // and a positive cap; otherwise the check stays fleet/repo-only.
        let instance_arg = match (instance, instance_cap) {
            (Some(_), Some(cap)) if cap > 0.0 => Some((instance_spent, cap)),
            _ => None,
        };
        let check = self
            .fleet_budget
                .check_dispatch_scoped(fleet_spent, repo_spent, instance_arg);
        match check.action {
            BudgetAction::Ok => Ok(()),
            BudgetAction::Warn => {
                if let Some(scope) = check.scope {
                    if self.mark_fleet_warned(scope, repo, instance) {
                        warn!(
                            scope = scope.as_str(),
                            spent = check.spent_usd,
                            cap = check.cap_usd,
                            "budget warning threshold crossed"
                        );
                        self.emit_dispatch_obstacle(repo, scope, "warning", &check, instance);
                    }
                }
                Ok(())
            }
            BudgetAction::Stop => {
                let scope = check.scope.unwrap_or(BudgetScope::Fleet);
                warn!(
                    scope = scope.as_str(),
                    spent = check.spent_usd,
                    cap = check.cap_usd,
                    "budget cap hit — refusing dispatch"
                );
                self.emit_dispatch_obstacle(repo, scope, "exceeded", &check, instance);
                Err(rk_core::Error::other(format!(
                    "{} budget cap hit: ${:.4} spent >= ${:.4} cap — dispatch refused",
                    scope.as_str(),
                    check.spent_usd,
                    check.cap_usd
                )))
            }
        }
    }

    /// Disk-pressure preflight guard (`[disk] min_free_gb`): refuse a spawn
    /// before it creates a new worktree if free space under `RK_HOME` is
    /// already below the configured floor, instead of running the disk to
    /// zero and failing deep inside an io path. Root-caused by the
    /// 2026-08-16 incident: 104 leaked agent worktrees (298 GB) drove the
    /// disk to 97% full, and the daemon started failing writes with
    /// "terminal state persistence failed: io" rather than refusing new work
    /// up front. Zero (the default for a bare `Supervisor`; `[disk]
    /// min_free_gb` for a real daemon) disables the guard. Mirrors
    /// [`check_dispatch_budget`](Self::check_dispatch_budget)'s placement —
    /// both single spawns and workflow fan-out funnel through here before any
    /// worktree/branch/name is allocated.
    fn check_disk_floor(&self, repo: &str) -> rk_core::Result<()> {
        let floor_gb = self.min_free_disk_gb.load(Ordering::Relaxed);
        if floor_gb == 0 {
            return Ok(());
        }
        let floor_bytes = floor_gb.saturating_mul(BYTES_PER_GB);
        let available = disk_free_bytes(self.layout.home())?;
        if available >= floor_bytes {
            return Ok(());
        }
        let available_gb = available as f64 / BYTES_PER_GB as f64;
        warn!(
            available_gb,
            floor_gb, "disk floor breached — refusing spawn"
        );
        self.emit_disk_pressure_obstacle(repo, available, floor_bytes);
        Err(rk_core::Error::other(format!(
            "refusing to spawn: only {available_gb:.1} GB free under {} — below the \
             configured floor of {floor_gb} GB ([disk] min_free_gb)",
            self.layout.home().display()
        )))
    }

    /// Companion to [`emit_dispatch_obstacle`](Self::emit_dispatch_obstacle):
    /// same `Category::Obstacle` shape, surfaced by `rk inbox`, but for a
    /// disk-pressure refusal rather than a budget one.
    fn emit_disk_pressure_obstacle(&self, repo: &str, available_bytes: u64, floor_bytes: u64) {
        let tuple = Tuple::new(
            Category::Obstacle,
            repo.to_string(),
            "disk-pressure".to_string(),
            self.castle.clone(),
            json!({
                "type": "disk_pressure",
                "available_bytes": available_bytes,
                "floor_bytes": floor_bytes,
                "path": self.layout.home().display().to_string(),
            }),
        );
        if let Err(e) = self.space.out(tuple.into_trail(DEFAULT_TRAIL_TTL)) {
            warn!(error = %e, "failed to emit disk pressure obstacle");
        }
    }

    /// Read-only preflight: would a spawn into `repo` be refused by the
    /// fleet/repo budget cap right now? Lets an autoscaler (the continuous-drain
    /// controller) skip claiming a ticket it could not dispatch, rather than
    /// claim-then-orphan it in `in_progress`. Side-effect free — unlike
    /// [`check_dispatch_budget`](Self::check_dispatch_budget), it emits no
    /// obstacle, so polling it every drain cycle does not spam `rk inbox`. The
    /// authoritative guard still runs inside `spawn`; this only avoids the claim.
    ///
    /// Preflight has no workflow instance in hand, so it checks only the
    /// fleet/repo scopes (`check_dispatch`); the per-instance cap (TKT-32) is
    /// still enforced by the authoritative guard in `spawn`.
    pub fn would_exceed_budget(&self, repo: &str) -> bool {
        let (fleet_spent, repo_spent, _instance_spent) = self.cost_rollup(repo, None);
        matches!(
            self.fleet_budget
                .check_dispatch(fleet_spent, repo_spent)
                .action,
            BudgetAction::Stop
        )
    }

    /// Returns true the first time a given scope is warned, so a warn obstacle
    /// is not re-posted on every subsequent dispatch in the band. The instance
    /// scope is keyed by instance id so each workflow run warns independently.
    fn mark_fleet_warned(&self, scope: BudgetScope, repo: &str, instance: Option<&str>) -> bool {
        let key = match scope {
            BudgetScope::Fleet => "__fleet__".to_string(),
            BudgetScope::Repo => format!("__repo__:{repo}"),
            BudgetScope::Instance => format!("__instance__:{}", instance.unwrap_or("")),
        };
        match self.fleet_warned.lock() {
            Ok(mut set) => set.insert(key),
            Err(p) => p.into_inner().insert(key),
        }
    }

    fn emit_dispatch_obstacle(
        &self,
        repo: &str,
        scope: BudgetScope,
        kind: &str,
        check: &DispatchCheck,
        instance: Option<&str>,
    ) {
        let mut payload = json!({
            "type": format!("budget_{}_{kind}", scope.as_str()),
            "scope": scope.as_str(),
            "spent_usd": check.spent_usd,
            "cap_usd": check.cap_usd,
        });
        // Name the offending instance on an instance-scoped obstacle so the
        // operator can tell which workflow run hit its cap.
        if scope == BudgetScope::Instance {
            if let Some(id) = instance {
                payload["instance"] = json!(id);
            }
        }
        let tuple = Tuple::new(
            Category::Obstacle,
            repo.to_string(),
            format!("budget-{}", scope.as_str()),
            self.castle.clone(),
            payload,
        );
        if let Err(e) = self.space.out(tuple.into_trail(DEFAULT_TRAIL_TTL)) {
            warn!(error = %e, "failed to emit dispatch budget obstacle");
        }
    }

    /// Fleet + per-repo cost rollup against the configured caps, for
    /// `rk cost --fleet`. Read-only; mirrors the denominator `check_dispatch`
    /// enforces on, matching `cost_rollup`: the fleet/repo totals count only
    /// live (`Spawning`/`Running`) agents — completed/failed/dismissed/orphaned
    /// records drop off — while per-instance spend stays cumulative (every
    /// non-dismissed agent under the instance), since a workflow's `budget:` is
    /// a lifetime cap on one finite run (TKT-40).
    pub fn fleet_rollup(&self) -> serde_json::Value {
        use std::collections::BTreeMap;
        let mut fleet_spent = 0.0;
        let mut per_repo: BTreeMap<String, f64> = BTreeMap::new();
        let mut per_instance: BTreeMap<String, f64> = BTreeMap::new();
        {
            let reg = self.lock_registry();
            // Full history (live + archived), matching `cost_rollup`: archiving
            // is a UI operation, never a budget one.
            for a in reg.list_all() {
                if a.state.is_live() {
                    fleet_spent += a.cost_usd;
                    *per_repo.entry(a.repo_name.clone()).or_default() += a.cost_usd;
                }
                // Per-instance spend is cumulative over the run (TKT-32): count
                // every non-dismissed agent, including completed sequential
                // steps, so it mirrors the instance-cap denominator.
                if a.state != AgentState::Dismissed {
                    if let Some(inst) = &a.workflow_instance {
                        *per_instance.entry(inst.clone()).or_default() += a.cost_usd;
                    }
                }
            }
        }
        let fb = &self.fleet_budget;
        let repos: Vec<serde_json::Value> = per_repo
            .into_iter()
            .map(|(repo, spent)| scope_json(spent, fb.repo_max_usd, fb.warn_at, Some(repo)))
            .collect();
        // Per-instance spend is reported cap-less: an instance's cap lives on
        // its workflow definition, not in the fleet config, so this rollup shows
        // current burn per running workflow (the cap is enforced at dispatch).
        let instances: Vec<serde_json::Value> = per_instance
            .into_iter()
            .map(|(instance, spent)| json!({"instance": instance, "spent_usd": spent}))
            .collect();
        json!({
            "fleet": scope_json(fleet_spent, fb.fleet_max_usd, fb.warn_at, None),
            "repos": repos,
            "instances": instances,
        })
    }

    /// One liveness/burn-rate sweep over the live, headless (event-pumped) rats.
    ///
    /// Budget checks fire only on Usage events, so a rat hung mid-tool-call
    /// emitting nothing never trips them. This out-of-band pass compares each
    /// rat's `updated_at` (bumped on every event via `Registry::update`) to now
    /// — silence past `stuck_after_secs` is STUCK — and tracks cost across
    /// sweeps — sustained USD/min above `burn_usd_per_min` is RUNNING AWAY.
    ///
    /// The response is graduated and mirrors budget enforcement: the first sweep
    /// to flag an agent posts an obstacle tuple and soft-steers it ("still
    /// working? wrap up"); only if it is STILL flagged after `kill_grace_secs`
    /// does the next sweep escalate to a kill. A steer that revives the rat (any
    /// new event bumps `updated_at`) clears the flag before it can be killed.
    ///
    /// Attach-mode rats and any without a live control handle are skipped: their
    /// liveness isn't tracked through the event pump, so silence proves nothing.
    pub fn sweep(&self, cfg: &SupervisorConfig) {
        let now = Utc::now();
        let live: Vec<AgentRecord> = self
            .lock_registry()
            .list()
            .into_iter()
            .filter(|r| r.state.is_live())
            .cloned()
            .collect();
        let live_names: std::collections::HashSet<&str> =
            live.iter().map(|r| r.name.as_str()).collect();

        for record in &live {
            // Only headless rats we actively control are event-pumped, so only
            // for them does `updated_at` silence mean anything — and only them
            // can we steer/kill through this path.
            if !self.lock_controls().contains_key(&record.name) {
                continue;
            }
            let action = self.decide_sweep(record, now, cfg);
            match action {
                SweepAction::None => {}
                SweepAction::Soft { kind, detail } => {
                    warn!(agent = %record.name, kind, %detail, "supervisor sweep flagged agent");
                    self.emit_sweep_obstacle(record, kind, &detail);
                    self.steer_flagged(record, kind);
                }
                SweepAction::Hard { kind, detail } => {
                    warn!(agent = %record.name, kind, %detail, "supervisor sweep killing agent after grace");
                    self.emit_sweep_obstacle(
                        record,
                        kind,
                        &format!("{detail} — killed after grace"),
                    );
                    let control = self.lock_controls().remove(&record.name);
                    if let Some(control) = control {
                        tokio::spawn(async move {
                            let _ = control.kill().await;
                        });
                    }
                }
            }
        }

        // Drop bookkeeping for agents that are no longer live so a later respawn
        // starts from a clean episode.
        self.lock_sweep_state()
            .retain(|name, _| live_names.contains(name.as_str()));
    }

    /// Update this agent's rolling sweep state and decide what to do about it.
    /// All state mutation happens here under the one lock; the caller acts on
    /// the returned decision after the lock is released.
    fn decide_sweep(
        &self,
        record: &AgentRecord,
        now: DateTime<Utc>,
        cfg: &SupervisorConfig,
    ) -> SweepAction {
        let mut state = self.lock_sweep_state();
        let st = state.entry(record.name.clone()).or_insert(SweepState {
            last_cost_usd: record.cost_usd,
            last_observed: now,
            flagged_at: None,
        });

        // Burn rate (USD/min) since the previous sweep of this agent.
        let dt_min = (now - st.last_observed).num_milliseconds() as f64 / 60_000.0;
        let burn = if dt_min > 0.0 {
            (record.cost_usd - st.last_cost_usd) / dt_min
        } else {
            0.0
        };
        st.last_cost_usd = record.cost_usd;
        st.last_observed = now;

        let idle_secs = (now - record.updated_at).num_seconds().max(0) as u64;
        let stuck = cfg.stuck_after_secs > 0 && idle_secs >= cfg.stuck_after_secs;
        let running_away = cfg.burn_usd_per_min > 0.0 && burn >= cfg.burn_usd_per_min;

        if !stuck && !running_away {
            // Recovered (or never flagged): clear any open episode.
            st.flagged_at = None;
            return SweepAction::None;
        }

        // Stuck takes precedence in the message; both post an obstacle whose
        // `type` a reactor #Trigger can match ("stuck" / "runaway").
        let (kind, detail): (&'static str, String) = if stuck {
            (
                "stuck",
                format!("no events for {idle_secs}s while still running"),
            )
        } else {
            (
                "runaway",
                format!("sustained burn ${burn:.2}/min with no completion"),
            )
        };

        match st.flagged_at {
            None => {
                st.flagged_at = Some(now);
                SweepAction::Soft { kind, detail }
            }
            Some(flagged) => {
                let elapsed = (now - flagged).num_seconds().max(0) as u64;
                if elapsed >= cfg.kill_grace_secs {
                    SweepAction::Hard { kind, detail }
                } else {
                    SweepAction::None
                }
            }
        }
    }

    fn steer_flagged(&self, record: &AgentRecord, kind: &str) {
        let control = self.lock_controls().get(&record.name).cloned();
        if let Some(control) = control {
            let name = record.name.clone();
            let nudge = if kind == "stuck" {
                format!(
                    "SUPERVISOR CHECK for {name}: you have gone quiet — still working? \
                     If you are stuck, record it with `rk obstacle`, then either make \
                     progress or wrap up: commit what you have and run `rk done` now."
                )
            } else {
                format!(
                    "SUPERVISOR CHECK for {name}: you are burning cost fast with no \
                     completion. Wrap up: commit what you have and run `rk done` now."
                )
            };
            tokio::spawn(async move {
                let _ = control.steer(&nudge).await;
            });
        }
    }

    fn emit_sweep_obstacle(&self, record: &AgentRecord, kind: &str, detail: &str) {
        let tuple = Tuple::new(
            Category::Obstacle,
            record.repo_name.clone(),
            record.name.clone(),
            self.castle.clone(),
            json!({
                "type": kind,
                "agent": record.name,
                "task": record.task,
                "detail": detail,
                "cost_usd": record.cost_usd,
                "tokens": record.usage.total(),
            }),
        );
        if let Err(e) = self.space.out(tuple.into_trail(DEFAULT_TRAIL_TTL)) {
            warn!(error = %e, "failed to emit sweep obstacle");
        }
    }

    /// Self-healing respawn sweep: auto-`respawn` agents that crashed out of
    /// their run — `Orphaned` (a daemon restart killed the process, worktree
    /// preserved) or `Failed` (the harness died non-zero) — so a transient
    /// crash stops being a manual `rk respawn` chore.
    ///
    /// Bounded by a crash-loop backoff so a genuinely-broken task cannot
    /// respawn-loop forever: each agent is respawned up to
    /// `respawn_max_attempts` times, the retries spaced by exponential backoff
    /// (`respawn_backoff_secs * 2^(attempt-1)`); once the cap is hit the sweep
    /// escalates a `need` (surfaced by `rk inbox`) for a human and stops.
    ///
    /// Guardrail: an agent whose branch already merged is never respawned — its
    /// work already landed, so a respawn would redo merged work. It is dropped
    /// from tracking instead.
    ///
    /// Shares the liveness-sweep loop (TKT-15): the server calls this right
    /// after `sweep()` on the same tick.
    pub fn respawn_sweep(self: &Arc<Self>, cfg: &SupervisorConfig) {
        if !cfg.respawn_enabled || cfg.respawn_max_attempts == 0 {
            return;
        }
        let now = Utc::now();
        // Candidates: crashed but not dismissed and not cleanly completed. A
        // `Completed` rat ran `rk done` — a clean finish we must not relaunch.
        let candidates: Vec<AgentRecord> = self
            .lock_registry()
            .list()
            .into_iter()
            .filter(|r| matches!(r.state, AgentState::Orphaned | AgentState::Failed))
            .cloned()
            .collect();

        for record in &candidates {
            // Guardrail: never auto-respawn an agent whose branch already merged
            // (or was deleted) — its work is done; a respawn would redo it.
            if self.branch_already_merged(record) {
                self.lock_respawn_state().remove(&record.name);
                continue;
            }
            match self.decide_respawn(record, now, cfg) {
                RespawnDecision::Wait => {}
                RespawnDecision::Respawn => {
                    // Respect the wallet: an operator who set a fleet/repo cap
                    // does not want auto-respawn to blow past it. Skip this
                    // cycle without counting the attempt if we're over the cap.
                    if self.would_exceed_budget(&record.repo_name) {
                        warn!(agent = %record.name, "skipping auto-respawn: over budget cap");
                        continue;
                    }
                    let attempt = self.record_respawn_attempt(&record.name, now);
                    info!(
                        agent = %record.name,
                        attempt,
                        max = cfg.respawn_max_attempts,
                        "self-healing sweep respawning crashed agent"
                    );
                    if let Err(e) = self.respawn(&record.name) {
                        warn!(agent = %record.name, error = %e, "auto-respawn failed");
                    }
                }
                RespawnDecision::Escalate => {
                    self.escalate_respawn_cap(record, cfg);
                }
            }
        }

        // Forget bookkeeping for agents that reached a terminal-clean state
        // (Completed/Dismissed) or vanished; a Running/Failed/Orphaned agent
        // keeps its counter so the Running->Failed cycle stays bounded.
        let keep: std::collections::HashSet<String> = self
            .lock_registry()
            .list()
            .into_iter()
            .filter(|r| !matches!(r.state, AgentState::Completed | AgentState::Dismissed))
            .map(|r| r.name.clone())
            .collect();
        self.lock_respawn_state()
            .retain(|name, _| keep.contains(name.as_str()));
    }

    /// Decide what to do about one crashed agent. Reads (does not mutate) the
    /// respawn-state so the caller can act — the attempt is recorded separately
    /// via `record_respawn_attempt` only if the respawn actually launches.
    fn decide_respawn(
        &self,
        record: &AgentRecord,
        now: DateTime<Utc>,
        cfg: &SupervisorConfig,
    ) -> RespawnDecision {
        let state = self.lock_respawn_state();
        let st = state.get(&record.name);
        let attempts = st.map(|s| s.attempts).unwrap_or(0);

        if attempts >= cfg.respawn_max_attempts {
            // Cap hit: escalate once, then stay quiet until a human intervenes.
            return if st.map(|s| s.escalated).unwrap_or(false) {
                RespawnDecision::Wait
            } else {
                RespawnDecision::Escalate
            };
        }

        // First attempt fires immediately; every retry waits out an exponential
        // backoff measured from the previous attempt.
        match st {
            None => RespawnDecision::Respawn,
            Some(st) => {
                let backoff = cfg
                    .respawn_backoff_secs
                    .saturating_mul(1u64 << (attempts.saturating_sub(1)).min(16));
                let waited = (now - st.last_attempt).num_seconds().max(0) as u64;
                if waited >= backoff {
                    RespawnDecision::Respawn
                } else {
                    RespawnDecision::Wait
                }
            }
        }
    }

    /// Record that an auto-respawn just fired for `name`, bumping its attempt
    /// count and resetting the backoff clock. Returns the new attempt number.
    fn record_respawn_attempt(&self, name: &str, now: DateTime<Utc>) -> u32 {
        let mut state = self.lock_respawn_state();
        let st = state.entry(name.to_string()).or_insert(RespawnState {
            attempts: 0,
            last_attempt: now,
            escalated: false,
        });
        st.attempts += 1;
        st.last_attempt = now;
        st.attempts
    }

    /// Whether the self-healing respawn sweep has already given up on `name`:
    /// its crash-loop cap was hit and escalated to a human, so no further
    /// auto-respawn will fire and the agent will not come back on its own.
    ///
    /// Read by the workflow engine (TKT-147) to tell a crashed rat that may yet
    /// be revived from one that is gone for good — a `wait` must keep blocking
    /// for the former and must fail fast on the latter.
    pub fn respawn_exhausted(&self, name: &str) -> bool {
        self.lock_respawn_state()
            .get(name)
            .map(|st| st.escalated)
            .unwrap_or(false)
    }

    /// The merged-branch guardrail: true if this agent's work already landed
    /// (so a respawn would redo merged work) or its branch is gone (nothing to
    /// resume). "Merged" here is precise — the branch is *strictly behind* its
    /// target: contained in it yet the target has advanced past it. That
    /// deliberately excludes a branch that merely equals its target (an agent
    /// that crashed before committing anything: tip == base), which is exactly
    /// the transient crash we most want to auto-respawn — a plain
    /// "is-ancestor" test would mis-skip it. An unmerged branch (commits not in
    /// target) is not an ancestor, so it respawns. Fail-safe: an unresolvable
    /// repo reads as "not merged" so we never wrongly skip a recoverable agent.
    fn branch_already_merged(&self, record: &AgentRecord) -> bool {
        let Some(branch) = record.branch.as_deref() else {
            return false;
        };
        let Ok(repo) = Repo::discover(&record.repo_root) else {
            return false;
        };
        if !repo.branch_exists(branch) {
            return true; // gone: the worktree can't be resumed onto it.
        }
        let target = &record.target_branch;
        repo.is_ancestor(branch, target) && !repo.is_ancestor(target, branch)
    }

    /// Escalate an exhausted crash-loop to a human: emit a `need` tuple (which
    /// `rk inbox` surfaces) and mark the agent escalated so we do it only once.
    fn escalate_respawn_cap(&self, record: &AgentRecord, cfg: &SupervisorConfig) {
        {
            let mut state = self.lock_respawn_state();
            if let Some(st) = state.get_mut(&record.name) {
                st.escalated = true;
            }
        }
        warn!(
            agent = %record.name,
            attempts = cfg.respawn_max_attempts,
            "auto-respawn cap exhausted — escalating a need for a human"
        );
        let tuple = Tuple::new(
            Category::Need,
            record.repo_name.clone(),
            record.name.clone(),
            self.castle.clone(),
            json!({
                "type": "respawn_exhausted",
                "agent": record.name,
                "task": record.task,
                "attempts": cfg.respawn_max_attempts,
                "text": format!(
                    "agent {} crashed and exhausted {} auto-respawn attempts; \
                     needs a human — investigate then `rk respawn {}`",
                    record.name, cfg.respawn_max_attempts, record.name
                ),
            }),
        );
        if let Err(e) = self.space.out(tuple.into_trail(DEFAULT_TRAIL_TTL)) {
            warn!(error = %e, "failed to emit respawn-exhausted need");
        }
    }

    /// Decide whether the turn that just ended is the one this generation
    /// finishes on, and claim the right to publish it.
    /// [`TurnClaim::publish`] = publish now.
    ///
    /// A harness returns control once per TURN. A background test suite
    /// finishing, a re-armed monitor, or a task notification each end a turn
    /// mid-task, and the `Completed` event they produce is indistinguishable
    /// from the real thing: `is_error` is `false` on both, and no other field
    /// says "still working". Publishing every one of them is TKT-160 — twelve
    /// agent generations in the live fleet emitted more than one durable
    /// `harness_result`, and because a `LIMIT 1` read returns the OLDEST match,
    /// every reader keyed on the agent name gets a MID-FLIGHT turn: a workflow
    /// `wait` unblocks on "the full cargo test pass is still running", the
    /// `evaluate` behind it judges that text, and a steward reviewer whose
    /// APPROVE lands in a later turn is read as having no verdict at all.
    ///
    /// Three things prove a turn is the last one, and this is where the first
    /// two are applied:
    ///
    /// 1. **The agent said so.** `rk done` writes exactly one `task_done` per
    ///    generation — the one signal in the system that a harness cannot
    ///    duplicate, because the rat writes it rather than the harness. Every
    ///    spawned role is primed with `rk done` as its mandatory final step.
    /// 2. **A restricted one-shot harness ended its request.** Jcode onboarding
    ///    has no Bash tool and its headless `done` event is terminal by native
    ///    contract.
    /// 3. **The process is gone.** Handled at `Exited`; see
    ///    [`Self::flush_withheld_completion`].
    ///
    /// A failed turn is terminal on its own: the session ended in an error, so
    /// there is no later turn to prefer, and holding it back would only turn a
    /// fast, legible failure into a `wait` timeout.
    fn claim_completion(
        &self,
        name: &str,
        generation: DateTime<Utc>,
        is_error: bool,
        harness_terminal: bool,
    ) -> TurnClaim {
        // Asked unconditionally rather than short-circuited behind `is_error`,
        // because the answer is published as `declared_done` and a failed turn
        // has one too: a rat can run `rk done` and then have a later turn error
        // out. Costs one indexed scan on a path that runs once per turn.
        // Jcode onboarding runs one headless request with no Bash tool. Its
        // native `done` event is therefore the only safe positive completion
        // signal and, unlike an interactive turn boundary, ends the process.
        let declared_done = harness_terminal || self.declared_done(name, generation);
        let terminal = is_error || declared_done;
        let mut completions = self.lock_completions();
        let state = completions
            .entry(name.to_string())
            .or_insert_with(|| CompletionState {
                generation,
                routed: false,
                withheld: false,
            });
        if state.generation != generation {
            *state = CompletionState {
                generation,
                routed: false,
                withheld: false,
            };
        }
        if state.routed {
            // Already published for this generation. A rat that keeps talking
            // after its `rk done` does not get to report twice.
            return TurnClaim {
                publish: false,
                declared_done,
            };
        }
        if terminal {
            state.routed = true;
            state.withheld = false;
        } else {
            // Superseded by whatever turn comes next, or flushed at `Exited`.
            state.withheld = true;
        }
        TurnClaim {
            publish: terminal,
            declared_done,
        }
    }

    /// Whether this generation of `name` has written its `rk done` tuple.
    ///
    /// Bounded to the generation via [`Pattern::for_agent_since`]: `task_done`
    /// is durable and a name is an identity key that outlives the rat wearing
    /// it, so an unbounded name search here would read a predecessor's `rk done`
    /// as this rat's (TKT-146/TKT-159).
    ///
    /// Fails OPEN — an unreadable space means "publish", which is the behaviour
    /// that predates this gate. Withholding on a storage error would strand
    /// every workflow waiting on the agent until its step timeout.
    fn declared_done(&self, name: &str, generation: DateTime<Utc>) -> bool {
        let pattern = Pattern::for_agent_since(Category::Event, "task_done", name, generation);
        match self.space.scan(&pattern) {
            Ok(tuples) => !tuples.is_empty(),
            Err(e) => {
                warn!(error = %e, agent = name, "task_done lookup failed; publishing the turn result anyway");
                true
            }
        }
    }

    /// Claim the right to publish a turn result that was held back, now that the
    /// process has exited and no further turn can follow. `true` = publish.
    ///
    /// A `true` here is also the proof that the generation never declared itself
    /// done — withholding is the only way a result reaches this path — which is
    /// why the caller publishes it with `declared_done: false` and, since
    /// TKT-175, as `is_error: true` however the process ended.
    fn flush_withheld_completion(&self, name: &str, generation: DateTime<Utc>) -> bool {
        let mut completions = self.lock_completions();
        let Some(state) = completions.get_mut(name) else {
            return false;
        };
        if state.generation != generation || state.routed || !state.withheld {
            return false;
        }
        state.routed = true;
        state.withheld = false;
        true
    }

    /// Forget a generation's completion bookkeeping.
    ///
    /// Called on a deliberate teardown (`dismiss`), where the held-back turn
    /// result must NOT surface as a late completion — nothing is waiting on it,
    /// and a stray `harness_result` carrying `"role":"rat"` would re-fire the
    /// reactor's steward on a branch that was just merged. Also called on
    /// `respawn`, which continues the SAME generation in a fresh process: the
    /// crashed run's withheld turn is stale, and its `routed` flag would
    /// otherwise gag the resumed run.
    fn forget_completion(&self, name: &str) {
        self.lock_completions().remove(name);
    }

    /// Diff summary for the completion payload (Phase 0 review tiering): the
    /// branch tip this generation produced, its size vs. the recorded target,
    /// and the precomputed `diff_class` bucket a reactor trigger gates a
    /// reviewer spawn on.
    ///
    /// Computed with a direct, synchronous `git` call rather than routed
    /// through `blocking_io`/`spawn_blocking` — the same tradeoff
    /// [`Self::branch_already_merged`] already makes for a cheap read on this
    /// path. Any failure (unresolvable repo, branch gone, bad rev) fails
    /// closed to [`DiffSummary::fallback`]: a bug here can only ADD a
    /// reviewer spawn downstream, never skip one.
    fn diff_summary(&self, record: &AgentRecord) -> DiffSummary {
        let fallback = DiffSummary::fallback();
        let Some(branch) = record.branch.as_deref() else {
            return fallback;
        };
        let Ok(repo) = Repo::discover(&record.repo_root) else {
            return fallback;
        };
        let Ok(head_sha) = repo.rev_parse(branch) else {
            return fallback;
        };
        let Ok(stat) = repo.diff_stat(&record.target_branch, branch) else {
            return DiffSummary {
                head_sha,
                ..fallback
            };
        };
        DiffSummary {
            head_sha,
            diff_files: stat.files.len(),
            diff_lines: stat.lines,
            diff_class: classify_diff(&stat.files, stat.lines),
        }
    }

    /// The diff summary for a completion, computed BEFORE the registry state
    /// flips to a terminal state. Ordering matters: consumers (tests, tooling,
    /// the reactor) observe `state == completed` via RPC and then expect the
    /// `harness_result` event to already be visible, so the git subprocesses
    /// behind [`Self::diff_summary`] must run before the flip, not between
    /// the flip and the emit — that gap is a race two integration tests
    /// caught on this branch's first gate runs.
    ///
    /// Runs the git subprocesses via `block_in_place` when the multi-thread
    /// runtime is available: `handle_event` executes on a tokio worker, and a
    /// slow git under machine load blocking workers directly is exactly what
    /// wedged the whole daemon socket (TKT-01M04D394PQ8VS5N3V441D1MDD).
    /// `block_in_place` hands this worker's queue to another thread for the
    /// duration; the git calls themselves are additionally subprocess-bounded
    /// in rk-git, so the block is never unbounded either way.
    fn diff_summary_for(&self, name: &str) -> DiffSummary {
        let record = self.lock_registry().get(name).cloned();
        let Some(record) = record else {
            return DiffSummary::fallback();
        };
        let on_multithread = tokio::runtime::Handle::try_current()
            .map(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
            .unwrap_or(false);
        if on_multithread {
            tokio::task::block_in_place(|| self.diff_summary(&record))
        } else {
            self.diff_summary(&record)
        }
    }

    /// Route a completion up the spawn tree: the structural parent gets a
    /// directed message; the repo scope gets the event either way.
    ///
    /// Reached exactly once per agent generation — see
    /// [`Self::claim_completion`] for what "once" means and why it matters.
    /// `diff` is precomputed via [`Self::diff_summary_for`] before the caller
    /// flips the agent's registry state — see that method for why.
    fn route_completion(
        &self,
        record: &AgentRecord,
        is_error: bool,
        declared_done: bool,
        diff: DiffSummary,
    ) {
        self.emit_event(
            &record.repo_name,
            "harness_result",
            json!({
                "agent": record.name,
                // The completed agent's role ("rat", "reviewer", ...). Carried so
                // a reactor trigger can scope reactively — e.g. the steward fires
                // on `"role":"rat"` completions only, which also breaks its own
                // re-entrancy: the reviewer it spawns completes as "reviewer" and
                // never re-triggers the steward on the branch it just reviewed.
                "role": record.role,
                "task": record.task,
                "branch": record.branch,
                // The actual base this agent was forked from. Steward carries
                // this daemon-authored value through as its delivery target so
                // feature-branch work does not silently reroute to `main`.
                "target": record.target_branch,
                "parent": record.parent,
                "is_error": is_error,
                // Review-tiering inputs (Phase 0, TKT-01M036N1RT74H6NPRH5FMM8A6T):
                // the branch tip this generation produced and a precomputed size
                // bucket, so a reactor trigger can decide whether the diff is
                // worth an LLM reviewer's judgment without needing an agent
                // worktree of its own first. See `diff_summary`.
                "head_sha": diff.head_sha,
                "diff_files": diff.diff_files,
                "diff_lines": diff.diff_lines,
                "diff_class": diff.diff_class,
                // Whether the agent itself declared the task finished (`rk done`)
                // in this generation, as opposed to merely stopping — killed by
                // the budget hard-stop, swept, or exiting mid-task (TKT-173).
                // Since TKT-175 every undeclared generation is also
                // `is_error: true`, so this no longer carries a fact `is_error`
                // lacks on the exit path. It still discriminates on the OTHER
                // path: a turn that errored out is undeclared too, so a reader
                // that wants "the rat said it was done" — rather than "nothing
                // went wrong" — reads this instead of inferring it from prose.
                "declared_done": declared_done,
                "result": record.result,
                "cost_usd": record.cost_usd,
                "tokens": record.usage.total(),
            }),
        );
        let boundary = is_reporting_boundary(record);
        self.emit_coordinator_event(
            record,
            "agent_lifecycle",
            json!({
                "route": if boundary { "terminal" } else { "rollup" },
                "severity": if is_error { "error" } else { "info" },
                "change": if is_error { "failed" } else { "completed" },
                "summary": record.result,
                "coordinator": record.coordinator,
                "workflow_instance": record.workflow_instance,
                "agent": record.name,
                "generation": record.created_at,
                "declared_done": declared_done,
            }),
        );
        if let Some(parent) = &record.parent {
            let tuple = Tuple::new(
                Category::Message,
                record.repo_name.clone(),
                parent.clone(),
                self.castle.clone(),
                json!({
                    "type": "child_completed",
                    "child": record.name,
                    "task": record.task,
                    "is_error": is_error,
                    "result": record.result,
                }),
            );
            if let Err(e) = self.space.out(tuple) {
                warn!(error = %e, "failed to notify parent");
            }
        }
        // A rat dispatched from a ticket closes that ticket's loop: a clean
        // finish marks it done (which unblocks any dependents), an error leaves
        // it in_progress for inspection. Fire-and-forget so completion routing
        // is never held up by the ticket's serialization lock.
        //
        // Since TKT-173 a rat KILLED mid-task reports an error, so it no longer
        // closes the ticket it never finished — and no dependent is unblocked
        // behind it. A rat that exits without `rk done` still closes its ticket;
        // TKT-175 is where that gets revisited.
        if !is_error {
            if let Some(task) = record.task.clone() {
                if task.starts_with(crate::tickets::ID_PREFIX) {
                    let tickets = self.tickets.clone();
                    tokio::spawn(async move {
                        if let Err(e) = tickets.set_status(&task, "done").await {
                            warn!(ticket = %task, error = %e, "failed to mark ticket done");
                        }
                    });
                }
            }
        }
    }

    pub async fn steer(&self, name: &str, message: &str) -> rk_core::Result<()> {
        let control = self.lock_controls().get(name).cloned();
        if let Some(control) = control {
            return control.steer(message).await;
        }
        // Attach-mode rats steer through their herdr pane.
        let target = self
            .lock_registry()
            .get(name)
            .and_then(|r| r.attach_target.clone());
        if let Some(target) = target {
            let message = message.to_string();
            return tokio::task::spawn_blocking(move || rk_mux::HerdrMux::send(&target, &message))
                .await
                .map_err(|e| rk_core::Error::other(e.to_string()))?;
        }
        Err(rk_core::Error::other(format!("{name} has no live session")))
    }

    pub async fn interrupt(&self, name: &str) -> rk_core::Result<()> {
        let control = self
            .lock_controls()
            .get(name)
            .cloned()
            .ok_or_else(|| rk_core::Error::other(format!("{name} has no live session")))?;
        control.interrupt().await
    }

    /// Resolve the activated repository policy, translating legacy registry
    /// fields into the same defaults. The registry is operator-owned and
    /// content-bound; live `.rk/repo.cue` edits do not take effect until the
    /// repository is re-added or an onboarding activation updates the record.
    ///
    /// `pub(crate)` so [`crate::landing::LandingPipeline`] can resolve a
    /// candidate's `landing` gate policy (protected paths, diff-scope
    /// budget, gate/review timeouts) the same way `deliver_branch` resolves
    /// `delivery` — one activated policy, one lookup.
    pub(crate) fn repository_policy(&self, repo: &Repo) -> rk_workflow::RepositoryPolicy {
        let path = self.layout.home().join("repos.json");
        crate::repos::RepoRegistry::load(&path)
            .ok()
            .and_then(|registry| {
                registry
                    .get_by_path(repo.root())
                    .map(|record| record.effective_policy())
            })
            .unwrap_or_default()
    }

    /// Execute one repository policy against a source branch. Every delivery
    /// mode returns the same `delivered` truth so workflows do not accidentally
    /// treat a local merge with a failed push as complete.
    async fn deliver_branch(
        &self,
        repo: &Repo,
        repo_name: &str,
        branch: &str,
        agent_base: &str,
        keep_branch: bool,
    ) -> rk_core::Result<BranchDelivery> {
        let policy = self.repository_policy(repo);
        let target = policy.delivery_target(agent_base);
        let remote = policy.delivery.remote.clone();
        let remote_branch = policy.remote_branch(branch, &target, repo_name);
        let mut delivery = BranchDelivery {
            target: target.clone(),
            remote: remote.clone(),
            ..BranchDelivery::default()
        };

        match policy.delivery.mode {
            DeliveryMode::Merge | DeliveryMode::MergePush => {
                let outcome = {
                    let _merge_guard = self.merge_queue.acquire(repo.root(), &target).await;
                    let repo = repo.clone();
                    let branch = branch.to_string();
                    let target = target.clone();
                    blocking_io("repository policy merge", move || {
                        repo.merge_branch(&branch, &target)
                    })
                    .await?
                };
                delivery.merged = outcome.merged;
                delivery.merge_commit = outcome.commit;
                delivery.detail = outcome.detail;
                if delivery.merged && policy.delivery.mode == DeliveryMode::MergePush {
                    let repo = repo.clone();
                    let target_to_push = target.clone();
                    let remote_to_push = remote.clone();
                    match blocking_io("repository policy target push", move || {
                        repo.push_branch_as(&target_to_push, &target_to_push, &remote_to_push)
                    })
                    .await
                    {
                        Ok(output) => {
                            delivery.pushed = true;
                            delivery.detail = format!(
                                "{}; pushed {target} to {remote}: {}",
                                delivery.detail,
                                output.trim()
                            );
                        }
                        Err(error) => {
                            delivery.detail = format!(
                                "{}; local merge succeeded but push to {remote}/{target} failed: {error}",
                                delivery.detail
                            );
                        }
                    }
                }
                delivery.delivered = delivery.merged
                    && (policy.delivery.mode == DeliveryMode::Merge || delivery.pushed);
            }
            DeliveryMode::PushBranch => {
                let repo = repo.clone();
                let branch_to_push = branch.to_string();
                let remote_branch_to_push = remote_branch.clone();
                let remote_to_push = remote.clone();
                match blocking_io("repository policy branch push", move || {
                    repo.push_branch_as(
                        &branch_to_push,
                        &remote_branch_to_push,
                        &remote_to_push,
                    )
                })
                .await
                {
                    Ok(output) => {
                        delivery.pushed = true;
                        delivery.delivered = true;
                        delivery.remote_branch = Some(remote_branch.clone());
                        delivery.detail = format!(
                            "pushed {branch} to {remote}/{remote_branch}: {}",
                            output.trim()
                        );
                    }
                    Err(error) => {
                        delivery.remote_branch = Some(remote_branch.clone());
                        delivery.detail = format!(
                            "push failed for {branch} -> {remote}/{remote_branch}: {error}"
                        );
                    }
                }
            }
            DeliveryMode::Pr => {
                let repo = repo.clone();
                let branch_for_pr = branch.to_string();
                let remote_branch_for_pr = remote_branch.clone();
                let target_for_pr = target.clone();
                let remote_for_pr = remote.clone();
                let outcome = blocking_io("repository policy pull request", move || {
                    Ok(repo.open_pull_request_as(
                        &branch_for_pr,
                        &remote_branch_for_pr,
                        &target_for_pr,
                        &remote_for_pr,
                    ))
                })
                .await?;
                delivery.remote_branch = Some(remote_branch);
                delivery.pushed = outcome.opened;
                delivery.pr_opened = outcome.opened;
                delivery.pr_url = outcome.url;
                delivery.delivered = outcome.opened;
                delivery.detail = outcome.detail;
            }
        }

        let delete_source = delivery.delivered
            && policy.delivery.delete_source
            && !keep_branch
            && policy.delivery.mode != DeliveryMode::Pr;
        if delete_source {
            let repo = repo.clone();
            let source = branch.to_string();
            match blocking_io("repository policy branch deletion", move || {
                repo.delete_branch(&source)
            })
            .await
            {
                Ok(()) => delivery.branch_deleted = true,
                Err(error) => warn!(
                    branch,
                    error = %error,
                    "delivery succeeded but source branch could not be deleted"
                ),
            }
        }
        Ok(delivery)
    }

    /// Dismiss: stop the session if live, then reconcile the branch with its
    /// target per the repo's merge mode — a `Direct` merge into the target (and
    /// branch delete on success), or a `Pr` push + opened pull/merge request
    /// that leaves the branch standing for review. Always removes the worktree.
    pub async fn dismiss(&self, name: &str, no_merge: bool) -> rk_core::Result<serde_json::Value> {
        self.dismiss_inner(name, no_merge, false).await
    }

    /// Same as [`dismiss`](Self::dismiss), except when `park_if_dirty` is set:
    /// a worktree carrying uncommitted changes is left standing (and reported
    /// via an `obstacle` tuple) instead of force-removed. Used by
    /// [`dismiss_orphaned_instance_agents`](Self::dismiss_orphaned_instance_agents),
    /// the unattended finalize-time sweep, which must apply the same
    /// dirty-worktree guard [`reap_git`](Self::reap_git) already applies —
    /// unlike an explicit operator/workflow `dismiss`, nobody looked at this
    /// worktree's contents before deciding to tear it down.
    async fn dismiss_inner(
        &self,
        name: &str,
        no_merge: bool,
        park_if_dirty: bool,
    ) -> rk_core::Result<serde_json::Value> {
        let record = self
            .lock_registry()
            .get(name)
            .cloned()
            .ok_or_else(|| rk_core::Error::other(format!("no such agent: {name}")))?;

        // Drop any held-back turn result BEFORE the kill: the `Exited` this
        // provokes must not publish a late `harness_result` for an agent the
        // caller is deliberately tearing down (TKT-160).
        self.forget_completion(name);
        let control = self.lock_controls().remove(name);
        if let Some(control) = control {
            let _ = control.kill().await;
            // Give the child a moment to exit cleanly before touching git.
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }
        if let Some(target) = record.attach_target.clone() {
            let _ = tokio::task::spawn_blocking(move || rk_mux::HerdrMux::close(&target)).await;
        }

        let repo_path = record.repo_root.clone();
        let repo =
            blocking_io("dismiss repo discovery", move || Repo::discover(&repo_path)).await?;
        let policy = self.repository_policy(&repo);
        let mut delivery = BranchDelivery {
            target: record.target_branch.clone(),
            remote: policy.delivery.remote,
            detail: "no delivery requested".into(),
            ..BranchDelivery::default()
        };

        if let Some(worktree) = &record.worktree {
            if worktree.exists() {
                let dirty = if park_if_dirty {
                    let worktree_check = worktree.clone();
                    blocking_io("dismiss dirty-worktree check", move || {
                        Repo::worktree_is_dirty(&worktree_check)
                    })
                    .await?
                } else {
                    false
                };
                if dirty {
                    self.emit_parked_dirty_worktree_obstacle(&record, worktree);
                } else {
                    let repo = repo.clone();
                    let worktree = worktree.clone();
                    blocking_io("dismiss worktree cleanup", move || {
                        repo.remove_worktree(&worktree)
                    })
                    .await?;
                }
            }
        }
        if let Some(branch) = &record.branch {
            if no_merge {
                delivery.detail = format!("branch {branch} preserved (--no-merge)");
            } else {
                delivery = self
                    .deliver_branch(
                        &repo,
                        &record.repo_name,
                        branch,
                        &record.target_branch,
                        false,
                    )
                    .await?;
            }
        }

        self.lock_registry().update(name, |r| {
            r.state = AgentState::Dismissed;
            r.pid = None;
            // Record the landed merge commit as the `rk revert` anchor; a
            // no-merge or PR-mode dismiss leaves any prior record untouched.
            if delivery.merge_commit.is_some() {
                r.merge_commit = delivery.merge_commit.clone();
            }
        })?;
        // A merged ticket-rat closes its ticket for good.
        if delivery.merged {
            if let Some(task) = &record.task {
                if task.starts_with(crate::tickets::ID_PREFIX) {
                    if let Err(e) = self.tickets.set_status(task, "closed").await {
                        warn!(ticket = %task, error = %e, "failed to close ticket on dismiss");
                    }
                }
            }
        }
        self.emit_event(
            &record.repo_name,
            "agent_dismissed",
            json!({
                "agent": name,
                "workflow_instance": record.workflow_instance,
                "delivered": delivery.delivered,
                "target": &delivery.target,
                "remote": &delivery.remote,
                "remote_branch": &delivery.remote_branch,
                "merged": delivery.merged,
                "merge_commit": &delivery.merge_commit,
                "pushed": delivery.pushed,
                "pr_opened": delivery.pr_opened,
                "pr_url": &delivery.pr_url,
                "branch_deleted": delivery.branch_deleted,
                "detail": &delivery.detail,
                "parent": &record.parent,
            }),
        );
        if is_reporting_boundary(&record) {
            self.emit_coordinator_event(
                &record,
                "agent_lifecycle",
                json!({
                    "route": "terminal",
                    "severity": "info",
                    "change": "dismissed",
                    "summary": format!("{} dismissed", record.name),
                    "coordinator": record.coordinator,
                    "workflow_instance": record.workflow_instance,
                    "agent": record.name,
                    "generation": record.created_at,
                }),
            );
        }
        // A PR-mode dismiss hands the branch off for review rather than merging;
        // surface that as its own event so the inbox / steward can pick it up.
        if delivery.pr_opened {
            self.emit_event(
                &record.repo_name,
                "pull_request_opened",
                json!({
                    "agent": name,
                    "branch": &record.branch,
                    "target": &delivery.target,
                    "remote": &delivery.remote,
                    "remote_branch": &delivery.remote_branch,
                    "url": &delivery.pr_url,
                    "detail": &delivery.detail,
                    "parent": &record.parent,
                }),
            );
        }
        Ok(json!({
            "agent": name,
            "delivered": delivery.delivered,
            "target": delivery.target,
            "remote": delivery.remote,
            "remote_branch": delivery.remote_branch,
            "merged": delivery.merged,
            "merge_commit": delivery.merge_commit,
            "pushed": delivery.pushed,
            "pr_opened": delivery.pr_opened,
            "pr_url": delivery.pr_url,
            "branch_deleted": delivery.branch_deleted,
            "detail": delivery.detail,
        }))
    }

    /// Companion to [`dismiss_inner`](Self::dismiss_inner)'s `park_if_dirty`
    /// guard: name the agent and worktree an unattended sweep declined to
    /// force-remove, so an operator (or `rk inbox`) can see why the worktree
    /// is still sitting there instead of the salvage window silently closing
    /// unnoticed.
    fn emit_parked_dirty_worktree_obstacle(
        &self,
        record: &AgentRecord,
        worktree: &std::path::Path,
    ) {
        let tuple = Tuple::new(
            Category::Obstacle,
            record.repo_name.clone(),
            record.name.clone(),
            self.castle.clone(),
            json!({
                "type": "worktree_parked_dirty",
                "agent": record.name,
                "task": record.task,
                "worktree": worktree.display().to_string(),
                "text": format!(
                    "{} finalized with uncommitted changes in its worktree — {} left standing, not force-removed",
                    record.name,
                    worktree.display()
                ),
            }),
        );
        if let Err(e) = self.space.out(tuple.into_trail(DEFAULT_TRAIL_TTL)) {
            warn!(error = %e, "failed to emit parked-dirty-worktree obstacle");
        }
    }

    /// Guaranteed-cleanup safety net for a terminalizing workflow instance
    /// (`WorkflowEngine::finalize`): find every LIVE agent this instance
    /// spawned that itself already reached a terminal agent state
    /// (`Completed`/`Failed`) without ever going through `dismiss` — e.g.
    /// because the workflow's own step sequence errored out before reaching
    /// its `dismiss`/`dismiss_all` step, the exact steward/workflow failure
    /// path that leaked 104 worktrees over the 2026-08-16 incident — and
    /// dismiss each one now, so its worktree is reclaimed even when the
    /// per-arm CUE steps that were supposed to do it never ran.
    ///
    /// Always dismisses with `no_merge: true`: this is a cleanup guarantee,
    /// not a normal completion path, and the workflow that spawned these
    /// agents already decided (by failing, or by completing without an
    /// explicit dismiss) not to route them through its own merge logic — a
    /// REWORK/STOP verdict deliberately holds a branch unmerged, and this
    /// sweep must not second-guess that by merging it anyway. The branch
    /// itself survives (only the worktree is reclaimed), so the work is never
    /// lost, only left for a human/ticket to land deliberately.
    ///
    /// Also dismisses with `park_if_dirty: true` — nobody has looked at these
    /// worktrees before this unattended sweep tears them down, so a worktree
    /// still carrying uncommitted edits is left standing (with an `obstacle`
    /// tuple naming it) rather than force-removed. This is the same
    /// dirty-worktree guard [`reap_git`](Self::reap_git) applies to the
    /// periodic sweep; the salvage window a budget-killed agent's uncommitted
    /// work depends on must not close just because the terminalization path
    /// is different from the periodic one.
    ///
    /// Best-effort: a single agent's dismissal failing is logged and does not
    /// stop the sweep or the instance's own terminal-state persistence, which
    /// must succeed regardless of whether every spawned agent could be swept.
    pub async fn dismiss_orphaned_instance_agents(&self, instance: &str) -> Vec<(String, bool)> {
        let names: Vec<String> = {
            let reg = self.lock_registry();
            reg.list()
                .into_iter()
                .filter(|a| {
                    a.workflow_instance.as_deref() == Some(instance)
                        && matches!(a.state, AgentState::Completed | AgentState::Failed)
                })
                .map(|a| a.name.clone())
                .collect()
        };
        let mut results = Vec::with_capacity(names.len());
        for name in names {
            match self.dismiss_inner(&name, true, true).await {
                Ok(_) => results.push((name, true)),
                Err(error) => {
                    warn!(agent = %name, instance, %error, "finalize-time cleanup sweep could not dismiss agent");
                    results.push((name, false));
                }
            }
        }
        results
    }

    /// Revert a dismissed agent's landed merge — the undo for an unattended
    /// auto-merge that turned out bad (steward/drain landed it, then main
    /// broke). Revert-merges the merge commit recorded on the agent's record
    /// at dismiss time (CAS-safe, through the same per-target merge queue as
    /// land/dismiss), reopens the agent's ticket (`open`, or `blocked` with
    /// `block` to hold it out of the auto-dispatch backlog), and emits a
    /// `fact` tuple recording what was undone. A revert conflict or a target
    /// moved mid-revert is a clean `reverted: false`, mirroring merge; an
    /// agent that never merged (no-merge, PR-mode, or already reverted) is an
    /// error.
    pub async fn revert(&self, name: &str, block: bool) -> rk_core::Result<serde_json::Value> {
        let record = self
            .lock_registry()
            .get(name)
            .cloned()
            .ok_or_else(|| rk_core::Error::other(format!("no such agent: {name}")))?;
        let Some(commit) = record.merge_commit.clone() else {
            return Err(rk_core::Error::other(format!(
                "{name} has no recorded merge commit to revert \
                 (never merged, PR-mode, or already reverted)"
            )));
        };

        let repo_path = record.repo_root.clone();
        let repo = blocking_io("revert repo discovery", move || Repo::discover(&repo_path)).await?;
        // Same per-target merge queue as land/dismiss: the revert takes its
        // turn so it never races a concurrent auto-merge into this target.
        let outcome = {
            let _merge_guard = self
                .merge_queue
                .acquire(repo.root(), &record.target_branch)
                .await;
            let repo = repo.clone();
            let target = record.target_branch.clone();
            let commit = commit.clone();
            blocking_io("revert merge", move || repo.revert_merge(&commit, &target)).await?
        };
        let reverted = outcome.merged;

        let mut ticket_status: Option<&str> = None;
        if reverted {
            // Clear the anchor so a second `rk revert` errors instead of
            // reverting the revert.
            self.lock_registry().update(name, |r| {
                r.merge_commit = None;
            })?;
            // Reopen the ticket the bad merge closed, so the work is durably
            // back on the backlog rather than falsely done.
            if let Some(task) = &record.task {
                if task.starts_with(crate::tickets::ID_PREFIX) {
                    let status = if block { "blocked" } else { "open" };
                    match self.tickets.reopen(task, status).await {
                        Ok(_) => ticket_status = Some(status),
                        Err(e) => {
                            warn!(ticket = %task, error = %e, "failed to reopen ticket on revert");
                        }
                    }
                }
            }
            let fact = Tuple::new(
                Category::Fact,
                record.repo_name.clone(),
                format!("merge-reverted-{name}"),
                self.castle.clone(),
                json!({
                    "agent": name,
                    "branch": &record.branch,
                    "target": &record.target_branch,
                    "task": &record.task,
                    "merge_commit": &commit,
                    "revert_commit": &outcome.commit,
                    "ticket_status": ticket_status,
                    "detail": &outcome.detail,
                }),
            );
            if let Err(e) = self.space.out(fact) {
                warn!(error = %e, "failed to emit merge-reverted fact tuple");
            }
        }
        info!(agent = name, reverted, merge_commit = %commit, "revert");
        Ok(json!({
            "agent": name,
            "reverted": reverted,
            "merge_commit": commit,
            "revert_commit": outcome.commit,
            "target": record.target_branch,
            "task": record.task,
            "ticket_status": ticket_status,
            "detail": outcome.detail,
        }))
    }

    /// Land a NAMED branch into a target — the explicit `{branch, target}`
    /// counterpart to [`dismiss`](Self::dismiss), which reconciles an agent's
    /// branch with its own base. Names neither an agent nor a worktree.
    ///
    /// Routes on the repo's merge mode, exactly like `dismiss`:
    /// - **Direct** — merge through a detached worktree (CAS-safe, touching no
    ///   live checkout). A merge conflict or a target that moved mid-merge is a
    ///   clean `merged: false`, not an error, so a workflow can gate on the
    ///   result (`evaluate {expect: {merged: true}}`) and retry rather than
    ///   fail. On success the source branch is deleted unless `keep_branch`;
    ///   deletion is best-effort (a protected or still-checked-out branch is
    ///   left in place and reported `branch_deleted: false`) so it never masks
    ///   the merge that already succeeded.
    /// - **Pr** — push the branch and open a pull/merge request against the
    ///   target, leaving the branch standing (`keep_branch` is implied) and
    ///   `merged: false`. A push failure is a clean `pr_opened: false`.
    pub async fn land(
        &self,
        repo_root: &std::path::Path,
        branch: &str,
        target: &str,
        keep_branch: bool,
    ) -> rk_core::Result<serde_json::Value> {
        let repo_path = repo_root.to_path_buf();
        let repo = blocking_io("land repo discovery", move || Repo::discover(&repo_path)).await?;
        let repo_name = repo.name();
        let delivery = self
            .deliver_branch(&repo, &repo_name, branch, target, keep_branch)
            .await?;
        let result = json!({
            "branch": branch,
            "target": delivery.target,
            "remote": delivery.remote,
            "remote_branch": delivery.remote_branch,
            "delivered": delivery.delivered,
            "merged": delivery.merged,
            "merge_commit": delivery.merge_commit,
            "pushed": delivery.pushed,
            "pr_opened": delivery.pr_opened,
            "pr_url": delivery.pr_url,
            "detail": delivery.detail,
            "branch_deleted": delivery.branch_deleted,
        });
        self.emit_event(&repo_name, "branch_landed", result.clone());
        // Surface an opened PR as its own event, mirroring dismiss.
        if delivery.pr_opened {
            self.emit_event(
                &repo_name,
                "pull_request_opened",
                json!({
                    "branch": branch,
                    "target": result.get("target"),
                    "remote": result.get("remote"),
                    "remote_branch": result.get("remote_branch"),
                    "url": result.get("pr_url"),
                    "detail": result.get("detail"),
                }),
            );
        }
        info!(
            branch,
            target = %delivery.target,
            delivered = delivery.delivered,
            merged = delivery.merged,
            pushed = delivery.pushed,
            pr_opened = delivery.pr_opened,
            branch_deleted = delivery.branch_deleted,
            "land"
        );
        Ok(result)
    }

    /// Open a pull/merge request for a NAMED branch against a target — the PR
    /// counterpart to [`land`](Self::land). Where `land` routes on the repo's
    /// merge mode (so it only opens a PR when the repo is registered PR-mode),
    /// `open_pr` ALWAYS pushes the branch and opens a pull/merge request,
    /// regardless of policy. This lets a workflow choose the review-by-PR
    /// outcome explicitly (e.g. `pr-on-approve.cue`) even in a Direct-merge repo.
    ///
    /// The branch is never merged or deleted; it is pushed and left standing for
    /// review. A push/auth failure is a clean `pr_opened: false` (never an
    /// error), mirroring `land`'s `merged: false`, so a workflow can gate on the
    /// result rather than fail. The remote comes from the repo's registered
    /// policy (defaulting to `origin`); only the merge *mode* is ignored.
    pub async fn open_pr(
        &self,
        repo_root: &std::path::Path,
        branch: &str,
        target: &str,
    ) -> rk_core::Result<serde_json::Value> {
        let repo_path = repo_root.to_path_buf();
        let repo = blocking_io("pull request repo discovery", move || {
            Repo::discover(&repo_path)
        })
        .await?;
        let repo_name = repo.name();
        let policy = self.repository_policy(&repo);
        let remote = policy.delivery.remote.clone();
        let remote_branch = policy.remote_branch(branch, target, &repo_name);
        let repo_for_pr = repo.clone();
        let branch_name = branch.to_string();
        let remote_branch_name = remote_branch.clone();
        let target_name = target.to_string();
        let outcome = blocking_io("pull request", move || {
            Ok(repo_for_pr.open_pull_request_as(
                &branch_name,
                &remote_branch_name,
                &target_name,
                &remote,
            ))
        })
        .await?;
        let result = json!({
            "branch": branch,
            "target": target,
            "remote": policy.delivery.remote,
            "remote_branch": remote_branch,
            "delivered": outcome.opened,
            "merged": false,
            "pushed": outcome.opened,
            "pr_opened": outcome.opened,
            "pr_url": outcome.url,
            "detail": outcome.detail,
        });
        // Surface an opened PR as its own event, exactly as `land`/`dismiss` do,
        // so the inbox / steward can pick up the hand-off.
        if outcome.opened {
            self.emit_event(
                &repo_name,
                "pull_request_opened",
                json!({
                    "branch": branch,
                    "target": target,
                    "remote": result.get("remote"),
                    "remote_branch": result.get("remote_branch"),
                    "url": result.get("pr_url"),
                    "detail": result.get("detail"),
                }),
            );
        }
        info!(branch, target, pr_opened = outcome.opened, "open_pr");
        Ok(result)
    }

    /// The live registry — archived records excluded. This is the default view
    /// every operator surface (`rk list`, `rk top`, `rk inbox`) and every sweep
    /// reads, so archiving a record retires it from all of them at once.
    pub fn list(&self) -> Vec<AgentRecord> {
        self.lock_registry().list().into_iter().cloned().collect()
    }

    /// Archived records only.
    pub fn list_archived(&self) -> Vec<AgentRecord> {
        self.lock_registry()
            .list_archived()
            .into_iter()
            .cloned()
            .collect()
    }

    /// Live + archived: the full lifetime history, for cost/usage reporting.
    pub fn list_all(&self) -> Vec<AgentRecord> {
        self.lock_registry()
            .list_all()
            .into_iter()
            .cloned()
            .collect()
    }

    /// Read-only status lookup, falling back to the archive so an archived
    /// rat's history stays inspectable with `rk status`.
    pub fn status(&self, name: &str) -> Option<AgentRecord> {
        self.lock_registry().get_any(name).cloned()
    }

    /// Recover the supervisor side of a session whose session journal did not
    /// yet persist its linked agent. Spawn journals the agent before worktree
    /// creation, so matching the dedicated role + stable session task closes
    /// the crash window without allocating a duplicate branch/worktree.
    pub fn onboarding_agent(&self, session: &str) -> Option<AgentRecord> {
        self.lock_registry()
            .list()
            .into_iter()
            .filter(|record| {
                record.role == ONBOARDER_ROLE
                    && record.task.as_deref() == Some(session)
                    && record.state != AgentState::Dismissed
            })
            .max_by_key(|record| record.created_at)
            .cloned()
    }

    /// Reconcile an attach-mode record against herdr. Losing the terminal that
    /// ran `rk attach` changes nothing; losing the pane itself turns the
    /// durable record into an orphan that `repo onboard resume` can recover.
    pub fn reconcile_attached(&self, name: &str) -> rk_core::Result<Option<AgentRecord>> {
        let Some(record) = self.lock_registry().get(name).cloned() else {
            return Ok(None);
        };
        let missing = record.state == AgentState::Running
            && record
                .attach_target
                .as_deref()
                .is_some_and(|target| rk_mux::HerdrMux::agent_status(target).is_none());
        if !missing {
            return Ok(Some(record));
        }
        self.lock_registry().update(name, |current| {
            current.state = AgentState::Orphaned;
            current.pid = None;
        })
    }

    /// Resolve which non-dismissed supervised agent(s) own a local client
    /// process.
    ///
    /// Agent-supplied environment is not authority: a harness can clear
    /// `RK_AGENT` and `RK_AUTH_TOKEN`, and all harnesses run as the same Unix
    /// user that owns `RK_HOME/auth.token`. The server therefore binds a
    /// connection to kernel-observed process ancestry, process group, and
    /// worktree cwd before it considers the request's claimed caller.
    ///
    /// The worktree check is also what covers attach-mode agents launched by
    /// herdr, whose root pid is not owned by the daemon. Matching every observed
    /// owner (rather than picking the first) makes an ambiguous cross-worktree
    /// process fail closed at the server boundary.
    pub(crate) fn supervised_agents_for_peer(&self, peer_pid: u32) -> HashSet<String> {
        let agents: Vec<_> = self
            .lock_registry()
            .list()
            .into_iter()
            // A harness may keep running after it reports Completed, and an
            // attach-mode pane deliberately does. Its worktree stays an agent
            // authority domain until dismissal tears that session down.
            .filter(|record| record.state != AgentState::Dismissed)
            .map(|record| {
                let worktree = record.worktree.as_ref().map(|worktree| {
                    std::fs::canonicalize(worktree).unwrap_or_else(|_| worktree.clone())
                });
                (record.name.clone(), record.pid, worktree)
            })
            .collect();

        let mut owners = HashSet::new();
        let mut seen = HashSet::new();
        let mut pid = Some(peer_pid);
        // A normal harness tree is only a few processes deep. The cap keeps a
        // corrupted platform response from turning authentication into a loop.
        for _ in 0..64 {
            let Some(current) = pid.filter(|current| *current > 1 && seen.insert(*current)) else {
                break;
            };
            let Some(info) = process_info(current) else {
                break;
            };
            for (name, root_pid, worktree) in &agents {
                let root_matches = root_pid
                    .is_some_and(|root| root == current || Some(root) == info.process_group);
                let worktree_matches = worktree
                    .as_ref()
                    .zip(info.cwd.as_ref())
                    .is_some_and(|(worktree, cwd)| cwd.starts_with(worktree));
                if root_matches || worktree_matches {
                    owners.insert(name.clone());
                }
            }
            pid = info.parent;
        }
        owners
    }

    /// Persist a bounded semantic checkpoint for the authenticated agent's
    /// current generation and publish a compact coordinator event.
    pub fn record_progress(
        &self,
        name: &str,
        summary: String,
        next: Option<String>,
        status: String,
    ) -> rk_core::Result<AgentRecord> {
        let summary = summary.trim().chars().take(512).collect::<String>();
        if summary.is_empty() {
            return Err(rk_core::Error::other("progress summary cannot be empty"));
        }
        let next = next.map(|value| value.trim().chars().take(512).collect());
        let status = status.trim().to_ascii_lowercase();
        if !matches!(status.as_str(), "working" | "blocked" | "complete") {
            return Err(rk_core::Error::other(
                "progress status must be working, blocked, or complete",
            ));
        }
        let now = Utc::now();
        let current = self
            .status(name)
            .ok_or_else(|| rk_core::Error::other(format!("no such agent: {name}")))?;
        if !current.state.is_live() {
            return Err(rk_core::Error::other(format!("agent {name} is not live")));
        }
        if let Some(progress) = &current.progress {
            if now - progress.updated_at < MIN_PROGRESS_INTERVAL {
                return Ok(current);
            }
        }
        let mut registry = self.lock_registry();
        let updated = registry
            .update(name, |record| {
                if !record.state.is_live() {
                    return;
                }
                let revision = record
                    .progress
                    .as_ref()
                    .map(|progress| progress.revision.saturating_add(1))
                    .unwrap_or(1);
                record.progress = Some(AgentProgress {
                    summary: summary.clone(),
                    next: next.clone(),
                    status: status.clone(),
                    revision,
                    updated_at: now,
                });
            })?
            .ok_or_else(|| rk_core::Error::other(format!("no such agent: {name}")))?;
        if !updated.state.is_live() {
            return Err(rk_core::Error::other(format!("agent {name} is not live")));
        }
        drop(registry);
        self.emit_coordinator_event(
            &updated,
            "middle_rat_progress",
            json!({
                "route": if status == "blocked" { "escalate" } else { "rollup" },
                "severity": if status == "blocked" { "warning" } else { "info" },
                "coordinator": updated.coordinator,
                "workflow_instance": updated.workflow_instance,
                "agent": updated.name,
                "generation": updated.created_at,
                "revision": updated.progress.as_ref().map(|p| p.revision),
                "summary": summary,
                "next": next,
                "status": status,
            }),
        );
        Ok(updated)
    }

    /// Promote an authenticated middle-rat's obstacle/need into the protected
    /// coordinator journal. Leaf-rat detail remains in the tuplespace and is
    /// owned by its reporting boundary.
    pub fn publish_coordination_attention(
        &self,
        caller: &str,
        category: &str,
        scope: &str,
        identity: &str,
        payload: &serde_json::Value,
    ) {
        let Some(agent) = self.status(caller) else {
            return;
        };
        if !is_reporting_boundary(&agent) {
            return;
        }
        let summary = payload
            .get("text")
            .or_else(|| payload.get("summary"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or(identity)
            .chars()
            .take(512)
            .collect::<String>();
        self.emit_coordinator_event(
            &agent,
            "coordination_attention",
            json!({
                "route": "escalate",
                "severity": "warning",
                "category": category,
                "scope": scope,
                "identity": identity,
                "coordinator": agent.coordinator,
                "workflow_instance": agent.workflow_instance,
                "agent": agent.name,
                "generation": agent.created_at,
                "summary": summary,
            }),
        );
    }

    /// Whether an authenticated agent is a foreman allowed to manage a child
    /// subtree. This is deliberately role-scoped: ordinary rats remain
    /// workers, even though they have authenticated daemon access for their
    /// own tuples.
    pub fn is_foreman(&self, name: &str) -> bool {
        self.status(name)
            .is_some_and(|record| record.role == "foreman" && record.state.is_live())
    }

    pub fn is_reporting_boundary(&self, name: &str) -> bool {
        self.status(name)
            .is_some_and(|record| is_reporting_boundary(&record))
    }

    /// Validate the structural edge a foreman is attempting to manage.
    /// Foremen may control only their direct children, never an arbitrary rat
    /// selected by name. Workflow ownership and generation remain on the child
    /// record, so respawn/dismiss preserve the same boundary.
    pub fn authorize_child(&self, foreman: &str, child: &str) -> rk_core::Result<()> {
        if !self.is_foreman(foreman) {
            return Err(rk_core::Error::other(
                "only a foreman may manage worker agents",
            ));
        }
        let record = self
            .status(child)
            .ok_or_else(|| rk_core::Error::other(format!("no such agent: {child}")))?;
        if record.parent.as_deref() != Some(foreman) {
            return Err(rk_core::Error::other(format!(
                "foreman {foreman} may manage only its direct children; {child} is owned by {}",
                record.parent.as_deref().unwrap_or("the operator")
            )));
        }
        Ok(())
    }

    /// Normalize a foreman's child spawn. The caller is the source of truth
    /// for parentage, workflow ownership, and the shared integration branch;
    /// accepting any of those fields from an agent would let it escape its
    /// supervision subtree or merge directly into the repository target.
    pub fn prepare_foreman_spawn(
        &self,
        foreman: &str,
        mut params: SpawnParams,
    ) -> rk_core::Result<SpawnParams> {
        let record = self
            .status(foreman)
            .ok_or_else(|| rk_core::Error::other(format!("no such agent: {foreman}")))?;
        if record.role != "foreman" {
            return Err(rk_core::Error::other(
                "only a foreman may spawn worker agents",
            ));
        }
        if record.branch.is_none() {
            return Err(rk_core::Error::other(
                "foreman has no integration branch for a worker spawn",
            ));
        }
        if params
            .parent
            .as_deref()
            .is_some_and(|parent| parent != foreman)
        {
            return Err(rk_core::Error::other(
                "a foreman child spawn cannot name another parent",
            ));
        }
        if params
            .workflow_instance
            .as_deref()
            .is_some_and(|instance| Some(instance) != record.workflow_instance.as_deref())
        {
            return Err(rk_core::Error::other(
                "a foreman child must remain in its parent's workflow instance",
            ));
        }
        if params
            .base
            .as_deref()
            .is_some_and(|base| Some(base) != record.branch.as_deref())
        {
            return Err(rk_core::Error::other(
                "a foreman child must target the foreman's integration branch",
            ));
        }
        params.parent = Some(foreman.to_string());
        params.workflow_instance = record.workflow_instance.clone();
        params.base = record.branch.clone();
        Ok(params)
    }

    /// Move settled terminal records (`Completed`/`Failed`/`Dismissed`) last
    /// touched before `cutoff` into the archive store, so they stop inflating
    /// the default views. Live and `Orphaned` records are never touched.
    ///
    /// With `dry_run` the registry is not mutated at all — the reply lists what
    /// *would* move, so an operator can preview before committing. `reap`
    /// additionally reclaims the leftovers each archived agent scattered
    /// outside its record (see [`Reap`]).
    pub fn archive_agents(
        &self,
        cutoff: DateTime<Utc>,
        dry_run: bool,
        reap: Reap,
    ) -> rk_core::Result<serde_json::Value> {
        if dry_run {
            let eligible: Vec<AgentRecord> = self
                .lock_registry()
                .archivable(cutoff)
                .into_iter()
                .cloned()
                .collect();
            return Ok(json!({
                "dry_run": true,
                "count": eligible.len(),
                "agents": eligible,
                "reaped": [],
                "reaped_logs": [],
            }));
        }
        let archived = self.lock_registry().archive(cutoff)?;
        let done =
            |rows: &[serde_json::Value]| rows.iter().filter(|r| r["reaped"] == json!(true)).count();
        let reaped: Vec<serde_json::Value> = if reap.git {
            archived.iter().map(|r| self.reap_git(r)).collect()
        } else {
            Vec::new()
        };
        let reaped_logs: Vec<serde_json::Value> = if reap.logs {
            archived.iter().map(|r| self.reap_log(r)).collect()
        } else {
            Vec::new()
        };
        info!(
            count = archived.len(),
            cutoff = %cutoff,
            reaped = done(&reaped),
            reaped_logs = done(&reaped_logs),
            "archived terminal agent records"
        );
        Ok(json!({
            "dry_run": false,
            "count": archived.len(),
            "agents": archived,
            "reaped": reaped,
            "reaped_logs": reaped_logs,
        }))
    }

    /// Restore an archived record to the live registry (the undo for
    /// [`archive_agents`](Supervisor::archive_agents)).
    pub fn unarchive_agent(&self, name: &str) -> rk_core::Result<serde_json::Value> {
        let restored = self
            .lock_registry()
            .unarchive(name)?
            .ok_or_else(|| rk_core::Error::other(format!("no archived agent: {name}")))?;
        info!(agent = name, "unarchived agent record");
        Ok(json!({ "agent": restored }))
    }

    /// Reclaim one archived agent's git leftovers — its worktree and local
    /// branch — but ONLY when the branch has already landed in its target (or
    /// is already gone) AND the worktree itself carries no uncommitted
    /// changes. An unmerged branch still holds the only copy of that rat's
    /// work; uncommitted edits sitting in the worktree never made it onto any
    /// commit, so a merged branch cannot vouch for them either — either
    /// condition leaves the worktree standing and reported as skipped, never
    /// force-deleted.
    ///
    /// Best-effort by construction: every failure becomes a `reaped: false` row
    /// with a reason rather than failing the archive that triggered it.
    fn reap_git(&self, record: &AgentRecord) -> serde_json::Value {
        let row = |reaped: bool, reason: String| json!({"agent": record.name, "branch": record.branch, "reaped": reaped, "reason": reason});
        let Some(branch) = record.branch.as_deref() else {
            return row(false, "no branch recorded".into());
        };
        let repo = match Repo::discover(&record.repo_root) {
            Ok(r) => r,
            Err(e) => return row(false, format!("repo unavailable: {e}")),
        };
        if !repo.branch_merged_or_gone(branch, &record.target_branch) {
            return row(
                false,
                format!(
                    "branch {branch} is not merged into {} — left standing",
                    record.target_branch
                ),
            );
        }
        if let Some(worktree) = &record.worktree {
            if worktree.exists() {
                match Repo::worktree_is_dirty(worktree) {
                    Ok(true) => {
                        return row(
                            false,
                            "worktree has uncommitted changes — left standing".into(),
                        )
                    }
                    Ok(false) => {}
                    Err(e) => return row(false, format!("could not check worktree status: {e}")),
                }
            }
        }
        let mut detail = Vec::new();
        if let Some(worktree) = &record.worktree {
            if worktree.exists() {
                match repo.remove_worktree(worktree) {
                    Ok(()) => detail.push("worktree removed".to_string()),
                    Err(e) => return row(false, format!("worktree removal failed: {e}")),
                }
            }
        }
        if repo.branch_exists(branch) {
            match repo.delete_branch(branch) {
                Ok(()) => detail.push(format!("branch {branch} deleted")),
                Err(e) => return row(false, format!("branch delete failed: {e}")),
            }
        } else {
            detail.push(format!("branch {branch} already gone"));
        }
        row(true, detail.join("; "))
    }

    /// Reclaim one archived agent's transcript: the `agent-logs/` file its own
    /// generation wrote, and nothing else. Each file is a bounded ring, but the
    /// count grows once per rat forever, so this is the sweep that keeps the
    /// directory finite.
    ///
    /// Unlike [`reap_git`](Supervisor::reap_git) there is no "has it settled
    /// yet" question to ask first — the branch holds the work, the transcript
    /// only narrates it — so every record that actually archives loses its own
    /// file. That makes this strictly one-way: `rk unarchive` restores the
    /// record but cannot bring the transcript back.
    ///
    /// Best-effort in the same shape as `reap_git`: a failure is a
    /// `reaped: false` row with a reason, never a failed archive.
    fn reap_log(&self, record: &AgentRecord) -> serde_json::Value {
        let row = |reaped: bool, reason: String| json!({"agent": record.name, "reaped": reaped, "reason": reason});
        match self.log.delete_for(&record.name, record.created_at) {
            Ok(true) => row(true, "transcript deleted".into()),
            Ok(false) => row(false, "no transcript on disk".into()),
            Err(e) => row(false, format!("transcript delete failed: {e}")),
        }
    }

    /// Standard spawn environment. Prepends the running `rk` binary's
    /// directory to PATH so the sugar commands work inside agent sessions.
    #[allow(clippy::too_many_arguments)]
    fn agent_env(
        &self,
        name: &str,
        role: &str,
        repo_name: &str,
        task: &str,
        branch: Option<&str>,
        base: &str,
        worktree: &std::path::Path,
        workflow_instance: Option<&str>,
    ) -> HashMap<String, String> {
        let mut env = HashMap::new();
        env.insert("RK_HOME".into(), self.layout.home().display().to_string());
        env.insert("RK_AGENT".into(), name.to_string());
        if let Ok(token) = self.layout.agent_auth_token(name) {
            env.insert("RK_AUTH_TOKEN".into(), token);
        }
        // So `rk prime` inside the rat renders this rat's own role automatically.
        env.insert("RK_ROLE".into(), role.to_string());
        env.insert("RK_REPO".into(), repo_name.to_string());
        env.insert("RK_TASK".into(), task.to_string());
        if let Some(branch) = branch {
            env.insert("RK_BRANCH".into(), branch.to_string());
        }
        env.insert("RK_BASE".into(), base.to_string());
        env.insert("RK_WORKTREE".into(), worktree.display().to_string());
        if let Some(instance) = workflow_instance {
            env.insert("RK_WORKFLOW_INSTANCE".into(), instance.to_string());
        }
        if let Ok(exe) = std::env::current_exe() {
            if let Some(dir) = exe.parent() {
                let path = std::env::var("PATH").unwrap_or_default();
                env.insert("PATH".into(), format!("{}:{path}", dir.display()));
            }
        }
        env
    }

    fn emit_event(&self, scope: &str, identity: &str, payload: serde_json::Value) {
        let tuple = Tuple::new(
            Category::Event,
            scope.to_string(),
            identity.to_string(),
            self.castle.clone(),
            payload,
        );
        if let Err(e) = self.space.out(tuple) {
            warn!(error = %e, identity, "failed to emit event tuple");
        }
    }

    fn emit_coordinator_event(
        &self,
        agent: &AgentRecord,
        identity: &str,
        payload: serde_json::Value,
    ) {
        let tuple = Tuple::new(
            Category::Event,
            agent.repo_name.clone(),
            identity.to_string(),
            self.castle.clone(),
            payload,
        );
        if let Err(e) = self.space.out_coordinator(tuple) {
            warn!(error = %e, identity, agent = %agent.name, "failed to emit coordinator event");
        }
    }

    fn lock_registry(&self) -> std::sync::MutexGuard<'_, Registry> {
        match self.registry.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    /// The branch a worker must compare against in its instructions is not
    /// always the branch its worktree was cut from. Reviewers are deliberately
    /// chained onto the completed work, so their worktree base is that rat
    /// branch while their comparison base is the predecessor's merge target.
    fn instruction_base(&self, role: &str, worktree_base: &str, repo: &Repo) -> String {
        if role != "reviewer" {
            return worktree_base.to_string();
        }
        let predecessor_target = {
            let registry = self.lock_registry();
            registry
                .list_all()
                .into_iter()
                .rev()
                .find(|record| record.branch.as_deref() == Some(worktree_base))
                .map(|record| record.target_branch.clone())
        };
        predecessor_target.unwrap_or_else(|| {
            repo.current_branch()
                .unwrap_or_else(|_| worktree_base.to_string())
        })
    }

    fn lock_controls(&self) -> std::sync::MutexGuard<'_, HashMap<String, SessionControl>> {
        match self.controls.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    fn lock_sweep_state(&self) -> std::sync::MutexGuard<'_, HashMap<String, SweepState>> {
        match self.sweep_state.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    fn lock_completions(&self) -> std::sync::MutexGuard<'_, HashMap<String, CompletionState>> {
        match self.completions.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    fn lock_respawn_state(&self) -> std::sync::MutexGuard<'_, HashMap<String, RespawnState>> {
        match self.respawn_state.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }
}

struct ProcessInfo {
    parent: Option<u32>,
    process_group: Option<u32>,
    cwd: Option<PathBuf>,
}

#[cfg(target_os = "macos")]
fn process_info(pid: u32) -> Option<ProcessInfo> {
    use std::ffi::CStr;
    use std::mem::{size_of, zeroed};

    // SAFETY: proc_pidinfo initializes exactly the requested libc structures.
    // Both calls are read-only process metadata queries for a same-user peer.
    unsafe {
        let mut bsd: libc::proc_bsdinfo = zeroed();
        let bsd_size = size_of::<libc::proc_bsdinfo>() as i32;
        if libc::proc_pidinfo(
            pid as i32,
            libc::PROC_PIDTBSDINFO,
            0,
            &mut bsd as *mut _ as *mut libc::c_void,
            bsd_size,
        ) != bsd_size
        {
            return None;
        }

        let mut vnode: libc::proc_vnodepathinfo = zeroed();
        let vnode_size = size_of::<libc::proc_vnodepathinfo>() as i32;
        let cwd = if libc::proc_pidinfo(
            pid as i32,
            libc::PROC_PIDVNODEPATHINFO,
            0,
            &mut vnode as *mut _ as *mut libc::c_void,
            vnode_size,
        ) == vnode_size
        {
            let path = vnode.pvi_cdir.vip_path.as_ptr() as *const libc::c_char;
            CStr::from_ptr(path)
                .to_str()
                .ok()
                .filter(|path| !path.is_empty())
                .map(PathBuf::from)
        } else {
            None
        };

        Some(ProcessInfo {
            parent: (bsd.pbi_ppid > 0).then_some(bsd.pbi_ppid),
            process_group: (bsd.pbi_pgid > 0).then_some(bsd.pbi_pgid),
            cwd,
        })
    }
}

#[cfg(target_os = "linux")]
fn process_info(pid: u32) -> Option<ProcessInfo> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The comm field is parenthesized and may contain spaces or `)`, so split
    // after its final close paren. Remaining fields begin state, ppid, pgrp.
    let fields: Vec<_> = stat
        .get(stat.rfind(')')? + 1..)?
        .split_whitespace()
        .collect();
    let parent = fields.get(1)?.parse::<u32>().ok().filter(|pid| *pid > 0);
    let process_group = fields.get(2)?.parse::<u32>().ok().filter(|pid| *pid > 0);
    let cwd = std::fs::read_link(format!("/proc/{pid}/cwd")).ok();
    Some(ProcessInfo {
        parent,
        process_group,
        cwd,
    })
}

const BYTES_PER_GB: u64 = 1024 * 1024 * 1024;

/// Bytes of free space available to the current (non-root) user on the
/// filesystem containing `path` — `f_bavail`, not the possibly-larger
/// root-only `f_bfree`, matching what `df` reports as "available" and the
/// number that actually bounds a new worktree checkout.
#[cfg(unix)]
fn disk_free_bytes(path: &std::path::Path) -> rk_core::Result<u64> {
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

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn process_info(pid: u32) -> Option<ProcessInfo> {
    // Process-group ownership still covers headless harnesses on other Unix
    // targets. Attach-mode ancestry/cwd support is implemented on macOS/Linux,
    // the two deployment targets with a stable peer-pid API in this project.
    let process_group = unsafe { libc::getpgid(pid as libc::pid_t) };
    Some(ProcessInfo {
        parent: None,
        process_group: (process_group > 0).then_some(process_group as u32),
        cwd: None,
    })
}

/// Newest-wins supersession for convention texts within one scope. `entries`
/// are `(tuple_id, text)` pairs; tuple ids are ULIDs, so lexicographic order
/// is creation order. The name key is the text up to the first `:` (trimmed);
/// a text without a name prefix stands alone under its full text. Output keeps
/// first-seen name order so prompt rendering stays stable across refreshes.
fn supersede_conventions_newest_wins(entries: Vec<(String, String)>) -> Vec<String> {
    let mut order: Vec<String> = Vec::new();
    let mut best: HashMap<String, (String, String)> = HashMap::new();
    for (id, text) in entries {
        let key = text
            .split_once(':')
            .map(|(name, _)| name.trim().to_string())
            .unwrap_or_else(|| text.clone());
        match best.get(&key) {
            Some((existing_id, _)) if *existing_id >= id => {}
            existing => {
                if existing.is_none() {
                    order.push(key.clone());
                }
                best.insert(key, (id, text));
            }
        }
    }
    order
        .into_iter()
        .filter_map(|key| best.remove(&key).map(|(_, text)| text))
        .collect()
}

#[cfg(test)]
mod convention_supersession_tests {
    use super::supersede_conventions_newest_wins;

    /// A refreshed norm must replace its predecessor in the injected prompt,
    /// never coexist with it: Furniture convention tuples cannot be edited or
    /// taken, so a stale `prove-your-tools-on-entry` alongside its refresh
    /// would hand every rat two contradictory entry instructions.
    #[test]
    fn newer_same_name_convention_supersedes_older() {
        let injected = supersede_conventions_newest_wins(vec![
            (
                "01KYQRN6MWP7Q5CP7CD4891PYB".into(),
                "prove-your-tools-on-entry: STOP immediately".into(),
            ),
            (
                "01M02ZZZZZZZZZZZZZZZZZZZZZ".into(),
                "prove-your-tools-on-entry: report and proceed".into(),
            ),
            ("01KY00000000000000000000AA".into(), "unrelated: keep me".into()),
        ]);
        assert_eq!(
            injected,
            vec![
                "prove-your-tools-on-entry: report and proceed".to_string(),
                "unrelated: keep me".to_string(),
            ]
        );
    }

    #[test]
    fn order_is_stable_and_unnamed_texts_stand_alone() {
        let injected = supersede_conventions_newest_wins(vec![
            ("01A".into(), "no name prefix here".into()),
            ("01B".into(), "b-norm: first".into()),
            // Arrival order does not matter — ids decide.
            ("01C".into(), "b-norm: second".into()),
            ("01B2".into(), "no name prefix here".into()),
            ("01D".into(), "another bare text".into()),
        ]);
        assert_eq!(
            injected,
            vec![
                "no name prefix here".to_string(),
                "b-norm: second".to_string(),
                "another bare text".to_string(),
            ]
        );
    }
}

#[cfg(test)]
mod respawn_tests {
    use super::*;
    use rk_ledger::{Budget, FleetBudget};
    use std::path::Path;
    use std::process::Command;

    fn git(dir: &Path, args: &[&str]) {
        let out = Command::new("git")
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

    fn init_repo(dir: &Path) {
        git(dir, &["init", "-b", "main"]);
        git(dir, &["config", "user.email", "r@x"]);
        git(dir, &["config", "user.name", "R"]);
        std::fs::write(dir.join("f"), "0\n").unwrap();
        git(dir, &["add", "."]);
        git(dir, &["commit", "-m", "init"]);
    }

    fn supervisor(home: &Path) -> Arc<Supervisor> {
        let layout = Layout::at(home);
        let tickets = Arc::new(crate::tickets::Tickets::new(
            Space::open_in_memory().unwrap(),
            "castle".into(),
        ));
        Arc::new(
            Supervisor::new(
                layout,
                "castle".into(),
                "fake".into(),
                Budget::default(),
                FleetBudget::default(),
                Space::open_in_memory().unwrap(),
                tickets,
            )
            .unwrap(),
        )
    }

    #[test]
    fn autonomous_harnesses_default_to_socket_capable_permission_modes() {
        assert_eq!(default_permission_mode("codex"), "danger-full-access");
        assert_eq!(default_permission_mode("jcode"), "danger-full-access");
        assert_eq!(default_permission_mode("claude"), "bypassPermissions");
    }

    #[test]
    fn agent_env_exports_resolved_base() {
        let home = tempfile::tempdir().unwrap();
        let sup = supervisor(home.path());
        let env = sup.agent_env(
            "Nibble",
            "reviewer",
            "repo",
            "review",
            Some("rat/nibble/review"),
            "integration",
            Path::new("/tmp/review-worktree"),
            None,
        );

        assert_eq!(env.get("RK_BASE").map(String::as_str), Some("integration"));
    }

    #[test]
    fn reviewer_instructions_compare_with_the_predecessors_merge_target() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        git(repo_dir.path(), &["branch", "integration"]);
        let repo = Repo::discover(repo_dir.path()).unwrap();
        let sup = supervisor(home.path());
        let params = SpawnParams {
            repo: repo_dir.path().display().to_string(),
            task: "task".into(),
            prompt: None,
            role: "rat".into(),
            coordination: None,
            harness: Some("fake".into()),
            parent: None,
            base: Some("integration".into()),
            model: None,
            permission_mode: None,
            attach: false,
            workflow_instance: None,
            coordinator: None,
            instance_max_usd: None,
        };
        let record = spawning_record(SpawnJournal {
            params: &params,
            repo: &repo,
            repo_name: "repo",
            name: "Nibble".into(),
            branch: "rat/nibble/task".into(),
            worktree: repo_dir.path().join("worktree"),
            target_branch: "integration".into(),
            harness: "fake".into(),
            model: None,
            permission_mode: "workspace-write".into(),
        });
        sup.lock_registry().insert(record).unwrap();

        assert_eq!(
            sup.instruction_base("reviewer", "rat/nibble/task", &repo),
            "integration"
        );
        assert_eq!(
            sup.instruction_base("rat", "rat/nibble/task", &repo),
            "rat/nibble/task"
        );
    }

    #[test]
    fn jcode_global_defaults_apply_to_direct_spawn_and_survive_respawn() {
        let profile = AgentProfile {
            harness: Some("jcode".into()),
            model: Some("gpt-test".into()),
            permission_mode: Some("danger-full-access".into()),
        };
        let mut params = SpawnParams {
            repo: "/tmp/repo".into(),
            task: "task".into(),
            prompt: None,
            role: "rat".into(),
            coordination: None,
            harness: None,
            parent: None,
            base: None,
            model: None,
            permission_mode: None,
            attach: false,
            workflow_instance: None,
            coordinator: None,
            instance_max_usd: None,
        };

        let worker = effective_agent_config("claude", &profile, &params).unwrap();
        assert_eq!(worker.harness, "jcode");
        assert_eq!(worker.model.as_deref(), Some("gpt-test"));
        assert_eq!(worker.permission_mode, "danger-full-access");

        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let repo = Repo::discover(repo_dir.path()).unwrap();
        let record = spawning_record(SpawnJournal {
            params: &params,
            repo: &repo,
            repo_name: "repo",
            name: "Nibble".into(),
            branch: "rat/nibble/task".into(),
            worktree: repo_dir.path().join("worktree"),
            target_branch: "main".into(),
            harness: worker.harness,
            model: worker.model,
            permission_mode: worker.permission_mode,
        });
        assert_eq!(record.model.as_deref(), Some("gpt-test"));
        assert_eq!(
            record.permission_mode.as_deref(),
            Some("danger-full-access")
        );
        assert_eq!(
            respawn_permission_mode(&record).unwrap(),
            "danger-full-access"
        );

        params.model = Some("gpt-direct".into());
        params.permission_mode = Some("bypassPermissions".into());
        let direct_override = effective_agent_config("claude", &profile, &params).unwrap();
        assert_eq!(direct_override.harness, "jcode");
        assert_eq!(direct_override.model.as_deref(), Some("gpt-direct"));
        assert_eq!(direct_override.permission_mode, "bypassPermissions");

        params.role = ONBOARDER_ROLE.into();
        params.model = None;
        params.permission_mode = None;
        let onboarder = effective_agent_config("claude", &profile, &params).unwrap();
        assert_eq!(onboarder.harness, "claude");
        assert_eq!(onboarder.model, None);
        assert_eq!(onboarder.permission_mode, "plan");
    }

    #[test]
    fn full_access_harnesses_reject_modes_that_block_or_misstate_rk_socket_access() {
        for harness in ["codex", "jcode"] {
            assert!(validate_permission_mode(harness, "danger-full-access").is_ok());
            assert!(validate_permission_mode(harness, "bypassPermissions").is_ok());
            for mode in ["read-only", "workspace-write"] {
                let error = validate_permission_mode(harness, mode)
                    .expect_err("a restricted or unenforceable mode must fail before spawn");
                assert!(error.to_string().contains("rk daemon socket"));
            }
        }
        assert!(validate_permission_mode("fake", "workspace-write").is_ok());
    }

    #[test]
    fn roles_and_onboarder_sandbox_are_explicit() {
        for role in ["rat", "reviewer", "foreman", "verifier", "onboarder"] {
            validate_role(role).unwrap();
        }
        assert!(validate_role("onbaorder").is_err());
        assert!(validate_role("").is_err());

        assert_eq!(permission_mode("onboarder", "codex").unwrap(), "read-only");
        assert_eq!(permission_mode("onboarder", "claude").unwrap(), "plan");
        assert_eq!(permission_mode("onboarder", "jcode").unwrap(), "read-only");
        assert!(
            permission_mode("onboarder", "unknown").is_err(),
            "a harness without an enforced read-only mode must fail closed"
        );
        assert_eq!(
            permission_mode("rat", "codex").unwrap(),
            "danger-full-access"
        );
    }

    #[test]
    fn only_jcode_onboarders_use_native_terminal_completion() {
        assert!(uses_harness_terminal_completion("onboarder", "jcode"));
        assert!(!uses_harness_terminal_completion("rat", "jcode"));
        assert!(!uses_harness_terminal_completion("onboarder", "codex"));

        let dir = tempfile::tempdir().unwrap();
        let supervisor = supervisor(dir.path());
        let generation = Utc::now();
        let claim = supervisor.claim_completion("Jade", generation, false, true);
        assert!(claim.publish);
        assert!(claim.declared_done);
        assert!(
            !supervisor
                .claim_completion("Jade", generation, false, true)
                .publish
        );

        let ordinary = supervisor.claim_completion("Whisker", generation, false, false);
        assert!(!ordinary.publish);
        assert!(!ordinary.declared_done);
    }

    fn record(repo: &Path, branch: Option<&str>) -> AgentRecord {
        let now = Utc::now();
        AgentRecord {
            name: "Nibble".into(),
            role: "rat".into(),
            coordination: None,
            harness: "fake".into(),
            permission_mode: None,
            model: None,
            repo_root: repo.to_path_buf(),
            repo_name: "repo".into(),
            task: Some("t".into()),
            branch: branch.map(String::from),
            worktree: Some(repo.to_path_buf()),
            target_branch: "main".into(),
            parent: None,
            workflow_instance: None,
            coordinator: None,
            session_id: None,
            attach_target: None,
            pid: None,
            merge_commit: None,
            state: AgentState::Failed,
            crashed: false,
            stderr_tail: None,
            result: None,
            progress: None,
            usage: TokenUsage::default(),
            cost_usd: 0.0,
            created_at: now,
            updated_at: now,
            archived_at: None,
        }
    }

    /// The merged-branch guardrail is precise: a branch whose work landed (and
    /// whose target advanced past it) is skipped; a crashed-before-committing
    /// branch (tip == base) and an unmerged-work branch are both respawnable.
    #[test]
    fn guardrail_skips_only_genuinely_merged_branches() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let sup = supervisor(home.path());
        let p = repo.path();

        // (a) A branch that made a commit, then merged into a target that then
        // advanced past it => genuinely merged => skip.
        git(p, &["checkout", "-b", "merged", "main"]);
        std::fs::write(p.join("f"), "merged\n").unwrap();
        git(p, &["commit", "-am", "work"]);
        git(p, &["checkout", "main"]);
        std::fs::write(p.join("g"), "other\n").unwrap();
        git(p, &["add", "."]);
        git(p, &["commit", "-m", "other-main"]);
        git(p, &["merge", "--no-ff", "-m", "merge", "merged"]);
        assert!(
            sup.branch_already_merged(&record(p, Some("merged"))),
            "a branch merged into an advanced target must be skipped"
        );

        // (b) A branch cut from main with NO commits (crashed before work):
        // tip == base, not strictly behind => respawnable.
        git(p, &["checkout", "-b", "nowork", "main"]);
        git(p, &["checkout", "main"]);
        assert!(
            !sup.branch_already_merged(&record(p, Some("nowork"))),
            "a no-commit branch (crashed early) must be respawnable"
        );

        // (c) A branch with commits NOT in the target => unmerged work => respawn.
        git(p, &["checkout", "-b", "unmerged", "main"]);
        std::fs::write(p.join("h"), "wip\n").unwrap();
        git(p, &["add", "."]);
        git(p, &["commit", "-m", "wip"]);
        git(p, &["checkout", "main"]);
        assert!(
            !sup.branch_already_merged(&record(p, Some("unmerged"))),
            "unmerged work must be respawnable"
        );

        // (d) A branch that no longer exists => nothing to resume => skip.
        assert!(
            sup.branch_already_merged(&record(p, Some("ghost"))),
            "a vanished branch must be skipped"
        );

        // (e) No branch recorded => not merged (fail-safe, respawn preflight handles it).
        assert!(!sup.branch_already_merged(&record(p, None)));
    }

    #[test]
    fn classify_diff_buckets_by_size_and_shape() {
        assert_eq!(classify_diff(&[], 0), "trivial", "an empty diff is trivial, not doc-only");
        assert_eq!(
            classify_diff(&["README.md".into(), "docs/guide.md".into()], 500),
            "doc-only",
            "doc-only overrides size: every path is markdown or under docs/"
        );
        assert_eq!(
            classify_diff(&["README.md".into(), "src/lib.rs".into()], 5),
            "trivial",
            "a mixed diff is not doc-only even when tiny"
        );
        assert_eq!(classify_diff(&["a".into(), "b".into()], 40), "trivial");
        assert_eq!(
            classify_diff(&["a".into(), "b".into(), "c".into()], 40),
            "small",
            "3 files exceeds the trivial file cap even under the line cap"
        );
        assert_eq!(classify_diff(&["a".into()], 41), "small", "41 lines exceeds the trivial line cap");
        assert_eq!(
            classify_diff(&vec!["f".to_string(); 10], 400),
            "small",
            "at the small boundary"
        );
        assert_eq!(
            classify_diff(&vec!["f".to_string(); 11], 400),
            "large",
            "11 files exceeds the small file cap"
        );
        assert_eq!(classify_diff(&["a".into()], 401), "large", "401 lines exceeds the small line cap");
    }

    #[test]
    fn diff_summary_classifies_a_real_branch() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let sup = supervisor(home.path());
        let p = repo.path();

        git(p, &["checkout", "-b", "trivial", "main"]);
        std::fs::write(p.join("g"), "one line\n").unwrap();
        git(p, &["add", "."]);
        git(p, &["commit", "-m", "small change"]);
        git(p, &["checkout", "main"]);

        let expected_sha = Repo::discover(p).unwrap().rev_parse("trivial").unwrap();
        let summary = sup.diff_summary(&record(p, Some("trivial")));
        assert_eq!(summary.head_sha, expected_sha);
        assert_eq!(summary.diff_files, 1);
        assert_eq!(summary.diff_lines, 1);
        assert_eq!(summary.diff_class, "trivial");
    }

    #[test]
    fn diff_summary_fails_closed_to_large_when_unresolvable() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let sup = supervisor(home.path());
        let p = repo.path();

        // No branch recorded at all (e.g. an attach-mode completion).
        let summary = sup.diff_summary(&record(p, None));
        assert_eq!(summary.diff_class, "large");
        assert_eq!(summary.head_sha, "");

        // A branch that no longer exists.
        let summary = sup.diff_summary(&record(p, Some("ghost")));
        assert_eq!(summary.diff_class, "large");
    }

    /// Backoff + cap: immediate first attempt, exponential wait between retries,
    /// escalate-once at the cap.
    #[test]
    fn decide_respawn_backs_off_then_caps() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let sup = supervisor(home.path());
        let rec = record(repo.path(), Some("main"));
        let now = Utc::now();
        let cfg = SupervisorConfig {
            respawn_enabled: true,
            respawn_max_attempts: 3,
            respawn_backoff_secs: 10,
            ..SupervisorConfig::default()
        };

        // No prior state => first attempt fires immediately.
        assert!(matches!(
            sup.decide_respawn(&rec, now, &cfg),
            RespawnDecision::Respawn
        ));

        // After attempt 1, backoff = 10 * 2^0 = 10s: too soon => Wait.
        sup.record_respawn_attempt(&rec.name, now);
        assert!(matches!(
            sup.decide_respawn(&rec, now, &cfg),
            RespawnDecision::Wait
        ));
        // Past the 10s backoff => Respawn.
        assert!(matches!(
            sup.decide_respawn(&rec, now + chrono::Duration::seconds(11), &cfg),
            RespawnDecision::Respawn
        ));

        // Attempt 2: backoff doubles to 20s. 15s is still too soon.
        sup.record_respawn_attempt(&rec.name, now);
        assert!(matches!(
            sup.decide_respawn(&rec, now + chrono::Duration::seconds(15), &cfg),
            RespawnDecision::Wait
        ));

        // Reach the cap (attempts == max) => escalate once, then stay quiet.
        sup.record_respawn_attempt(&rec.name, now); // attempts now 3 == max
        assert!(matches!(
            sup.decide_respawn(&rec, now + chrono::Duration::seconds(999), &cfg),
            RespawnDecision::Escalate
        ));
        sup.lock_respawn_state()
            .get_mut(&rec.name)
            .unwrap()
            .escalated = true;
        assert!(matches!(
            sup.decide_respawn(&rec, now + chrono::Duration::seconds(999), &cfg),
            RespawnDecision::Wait
        ));
    }

    fn spawn_params(repo: &Path, task: &str) -> SpawnParams {
        SpawnParams {
            repo: repo.display().to_string(),
            task: task.into(),
            prompt: None,
            role: "rat".into(),
            coordination: None,
            harness: Some("fake".into()),
            parent: None,
            base: None,
            model: None,
            permission_mode: None,
            attach: false,
            workflow_instance: None,
            coordinator: None,
            instance_max_usd: None,
        }
    }

    /// The fleet-WIP admission check and the reservation must happen in one
    /// atomic step (TOCTOU regression for the review finding on
    /// workflow_exec.rs's `await_fleet_capacity`): five spawns fired
    /// genuinely concurrently (a multi-thread runtime, not the default
    /// single-threaded `#[tokio::test]`, so this is real OS-thread
    /// contention on the registry lock, not cooperative interleaving)
    /// against `fleet_wip_cap = 2` must admit EXACTLY two, never more — no
    /// window where two concurrent callers can each observe the same free
    /// slot before either's spawn lands in the registry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fleet_wip_admission_is_atomic_under_concurrent_spawns() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let sup = supervisor(home.path());

        let handles: Vec<_> = (0..5)
            .map(|i| {
                let sup = Arc::clone(&sup);
                let params = spawn_params(repo.path(), &format!("concurrent-{i}"));
                tokio::spawn(async move { sup.spawn_async(params, 2).await })
            })
            .collect();
        let mut results = Vec::with_capacity(handles.len());
        for h in handles {
            results.push(h.await.unwrap());
        }

        let admitted = results.iter().filter(|r| r.is_ok()).count();
        let refused = results
            .iter()
            .filter(|r| {
                matches!(r, Err(e) if e.to_string() == FLEET_WIP_CAP_REFUSED)
            })
            .count();
        assert_eq!(admitted, 2, "cap of 2 must admit exactly 2: {results:?}");
        assert_eq!(refused, 3, "the other 3 must be refused cleanly: {results:?}");
        // A refused attempt never reaches `reserve_name`/`insert`, so exactly
        // one registry row must exist per ADMITTED spawn — checked as a total
        // count rather than a live-state filter because the fake harness's
        // default script can race to `Completed` before this assertion runs,
        // which a live-only count would flag as a false cap violation.
        assert_eq!(
            sup.list().len(),
            2,
            "exactly the 2 admitted spawns should have a registry row"
        );
    }

    /// A spawn refused (or otherwise failed) BEFORE its registry row goes
    /// live must not leak its fleet-WIP reservation: with `fleet_wip_cap =
    /// 1`, a failing spawn (an onboarder task that fails validation before
    /// `insert`) followed by a real one must let the second succeed —
    /// leaking here would wedge the ceiling closed forever.
    #[tokio::test]
    async fn failed_spawn_before_insert_releases_its_fleet_wip_reservation() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let sup = supervisor(home.path());

        let mut bad = spawn_params(repo.path(), "not-a-valid-onboarder-task");
        bad.role = crate::onboarding_sessions::ONBOARDER_ROLE.into();
        let failure = sup.spawn_async(bad, 1).await;
        assert!(failure.is_err(), "invalid onboarder task must fail validation");

        let good = spawn_params(repo.path(), "concurrent-after-failure");
        let record = sup
            .spawn_async(good, 1)
            .await
            .expect("the failed attempt's reservation must have been released");
        assert!(record.state.is_live());
    }
}

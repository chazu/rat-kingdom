//! The supervisor: spawn rats into worktrees, pump their harness events into
//! the registry and tuplespace, route completions up the spawn tree, and
//! preserve their work on dismissal, and route delivery through the landing
//! pipeline.

use crate::agents::{AgentProgress, AgentRecord, AgentState, Registry};
use crate::onboarding_sessions::{onboarding_branch, onboarding_worktree, ONBOARDER_ROLE};
use crate::read_only_roles::{forces_read_only_harness, DIAGNOSTICIAN_ROLE, GROOMER_ROLE};
use chrono::{DateTime, Utc};
use rk_core::config::SupervisorConfig;
use rk_core::notify::{EscalationNotice, Severity, SinkRegistry};
use rk_core::paths::Layout;
use rk_core::prime::{render, PrimeContext, VerificationCheck, MAX_INJECTED_FACTS};
use rk_core::tuple::{Category, Lifecycle, Pattern, Tuple, DEFAULT_TRAIL_TTL, SYSTEM_SCOPE};
use rk_git::Repo;
use rk_harness::{
    make_harness, ControlEnvelope, HarnessEvent, LaunchSpec, SessionControl, TokenUsage,
};
use rk_ledger::pricing::PricingTable;
use rk_ledger::{Budget, BudgetAction, BudgetScope, DispatchCheck, FleetBudget};
use rk_space::Space;
use rk_workflow::{AgentProfile, Coordination, DeliveryMode};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tracing::{debug, info, warn};

const MIN_PROGRESS_INTERVAL: chrono::Duration = chrono::Duration::seconds(5);

/// Harness transport `Retry` events observed since the last real
/// forward-progress event (assistant text, tool use, or nonzero usage) at or
/// past which the stuck sweep treats the run as a reconnect loop rather than
/// liveness — see [`LivenessEvidence`].
const RECONNECT_LOOP_THRESHOLD: u32 = 3;

/// Fingerprint of one bounded-output event, so the stuck sweep can tell
/// "another event of this kind arrived, but it said exactly the same thing"
/// (a wedged process re-emitting stale output) from genuine new content,
/// without keeping the raw text around a second time (it already lives in
/// `agent_log`/`stderr_tail`).
fn output_fingerprint(kind: &str, text: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    kind.hash(&mut hasher);
    text.hash(&mut hasher);
    hasher.finish()
}

/// Error text [`Supervisor::spawn`] returns when `fleet_wip_cap` refused
/// admission. Matched by name (not a dedicated [`rk_core::Error`] variant, to
/// avoid widening a shared enum for one internal admission-control signal) by
/// callers that must distinguish "no free slot right now, try again" from a
/// genuine spawn failure — see
/// [`WorkflowEngine::await_fleet_capacity`](crate::workflow_exec::WorkflowEngine::await_fleet_capacity).
pub(crate) const FLEET_WIP_CAP_REFUSED: &str =
    "fleet WIP cap reached: no free slot to admit this spawn";

/// Error text [`Supervisor::spawn`] returns when a repository's implementation
/// lane (`Lane::Implementation` — every role except `"reviewer"`) had no free
/// slot (TKT-01M0P2KM83Y4MD5QYETR3JCKF2). A distinct string from
/// [`FLEET_WIP_CAP_REFUSED`] purely for observability (so a caller can tell
/// which ceiling refused); [`crate::workflow_exec::is_fleet_wip_refusal`]
/// treats both identically for retry purposes.
pub(crate) const IMPLEMENTATION_LANE_REFUSED: &str =
    "implementation lane at capacity for this repository: no free slot to admit this spawn";

/// Same as [`IMPLEMENTATION_LANE_REFUSED`], for `Lane::Review`
/// (`role == "reviewer"`).
pub(crate) const REVIEW_LANE_REFUSED: &str =
    "review lane at capacity for this repository: no free slot to admit this spawn";

/// Fixed prefix of the error [`Supervisor::spawn`] returns when the
/// castle-wide circuit breaker for the requested harness provider is
/// currently open (TKT-01M0HND8M25GYN1ZTRET3S5769) — a distinct string
/// (not [`FLEET_WIP_CAP_REFUSED`] or a lane-capacity refusal) so a caller
/// can tell an outage refusal apart from ordinary admission pressure. The
/// provider name is appended by [`transport_breaker_open_refused`] for
/// observability; match on this prefix rather than the full message.
pub(crate) const TRANSPORT_BREAKER_OPEN_REFUSED_PREFIX: &str =
    "transport circuit breaker open for provider";

pub(crate) fn transport_breaker_open_refused(provider: &str) -> String {
    format!(
        "{TRANSPORT_BREAKER_OPEN_REFUSED_PREFIX} {provider}: refusing new launches until it \
         recovers (see `rk inbox` for the open episode)"
    )
}

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

/// `[budget] reviewer_max_usd` built-in default (`BudgetConfig::default()`
/// mirrors this): above the observed cost of a legitimate deep review, but a
/// hard ceiling on the $27+ uncapped outliers production surfaced.
const DEFAULT_REVIEWER_MAX_USD: f64 = 30.0;

/// Durable evidence that a `task_done` arrived for a generation the budget,
/// stuck, or runaway machinery had already terminalized (`Stopped`/`Failed`)
/// before the completion could be applied — the loser of a same-tick race
/// between a hard-stop's CAS and [`Supervisor::reconcile_task_done`]'s own
/// CAS to `Completed`. Recorded instead of silently dropped so an operator
/// has an explicit recovery action rather than discovering a stranded,
/// already-verified branch by hand (2026-08-21 Cinder-11 incident,
/// TKT-01M0J5KT4TCH03W48MR9T7EJ27).
const LATE_TASK_DONE_EVIDENCE_IDENTITY: &str = "late_task_done_evidence";

/// Barrier names for [`Supervisor::reconcile_task_done`]'s CAS-then-publish
/// sequence. See `crate::fault` for why a barrier and not a sleep, and
/// `landing.rs`'s `BARRIER_CEILING_PRE/POST_MARKER` for the sibling pattern
/// this mirrors — there a late review verdict races a ceiling settlement;
/// here a late `task_done` races a budget/stuck/runaway hard stop.
const BARRIER_TASK_DONE_PRE_ROUTE: &str = "task-done-pre-route";
const BARRIER_TASK_DONE_POST_ROUTE: &str = "task-done-post-route";

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
pub(crate) fn classify_diff(files: &[String], lines: u64) -> &'static str {
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
    /// True iff a `Merge`/`MergePush` delivery's source branch carried zero
    /// file/line changes over `target` at the point it diverged
    /// (`Repo::diff_stat(target, branch)` before the merge moves `target`).
    /// A dismiss whose branch is content-free this way did not deliver its
    /// ticket's work even though the merge itself reports `merged: true` —
    /// most often a duplicate rat dispatched onto a ticket whose real branch
    /// already landed (TKT-01M0C663BZ86SMA2PVMFP5QJ8D). Left `false` (never
    /// checked) for `PushBranch`/`Pr` deliveries, which don't set `merged`.
    content_free: bool,
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
    // A read-only-harness role is an assessment (or, for the groomer, an
    // evidence-closure) boundary: global worker defaults must never widen it.
    // Explicit harness/model selection remains available, while the
    // permission mode is forced by role.
    if forces_read_only_harness(&params.role) {
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

    // A dispatch-time routed profile (cost tier and/or an explicitly named
    // profile, already layered over the global default by the server) stands in
    // for `[agents.default]` here. Deliberately *below* the read-only-role
    // return above: cost routing must never be able to hand an assessment role
    // a different harness, and so a different authority boundary.
    let default_agent = params.resolved_profile.as_ref().unwrap_or(default_agent);
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
        model: params.model.clone().or_else(|| default_agent.model.clone()),
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
        "read-only" | "workspace-write" => Err(rk_core::Error::other(format!(
            "{harness} agents need danger-full-access to reach the rk daemon socket; \
             use --permission-mode danger-full-access (or omit the override)",
        ))),
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
        "rat"
            | "reviewer"
            | "foreman"
            | "verifier"
            | ONBOARDER_ROLE
            | DIAGNOSTICIAN_ROLE
            | GROOMER_ROLE
    ) {
        Ok(())
    } else {
        Err(rk_core::Error::other(format!(
            "unknown agent role {role:?}; expected rat, reviewer, foreman, verifier, \
             onboarder, diagnostician, or groomer"
        )))
    }
}

/// Read-only roles are assessment-only. Their filesystem boundary is enforced
/// by the harness rather than by prompt prose, and callers cannot override it.
fn permission_mode(role: &str, harness: &str) -> rk_core::Result<String> {
    if !forces_read_only_harness(role) {
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
    fork_point: String,
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
        spawn: Some(rk_core::id::SpawnId::new()),
        role: journal.params.role.clone(),
        coordination: journal.params.coordination.clone(),
        harness: journal.harness,
        permission_mode: Some(journal.permission_mode),
        model: journal.model,
        repo_root: journal.repo.root().to_path_buf(),
        repo_name: journal.repo_name.to_string(),
        task: Some(journal.params.task.clone()),
        branch: Some(journal.branch),
        fork_point: Some(journal.fork_point),
        worktree: Some(journal.worktree),
        target_branch: journal.target_branch,
        parent: journal.params.parent.clone(),
        workflow_instance: journal.params.workflow_instance.clone(),
        review: journal.params.review.clone(),
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
        liveness: crate::agents::LivenessObservation::default(),
        transport_outage: None,
        recovery: None,
        recovery_receipt: None,
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

/// Free-function core of [`Supervisor::repository_policy`] — split out so the
/// ticket-delivery gate can resolve a policy from an owned `home` path inside
/// a `'static` spawned task, without borrowing a `Supervisor`.
fn resolve_repository_policy(home: &std::path::Path, repo: &Repo) -> rk_workflow::RepositoryPolicy {
    let path = home.join("repos.json");
    crate::repos::RepoRegistry::load(&path)
        .ok()
        .and_then(|registry| {
            registry
                .get_by_path(repo.root())
                .map(|record| record.effective_policy())
        })
        .unwrap_or_default()
}

/// Why `branch` has not been delivered under `policy`'s mode, or `None` if it
/// has (or the mode isn't gated). `gate_push_branch` controls whether
/// `push-branch` is enforced — see [`Supervisor::require_ticket_delivered`]
/// for why the automatic completion path leaves it `false`.
fn ticket_undelivered_reason(
    policy: &rk_workflow::RepositoryPolicy,
    repo: &Repo,
    branch: &str,
    target: &str,
    fork_point: Option<&str>,
    gate_push_branch: bool,
) -> Option<String> {
    match policy.delivery.mode {
        DeliveryMode::Merge | DeliveryMode::MergePush => {
            // Requires a *verified* merge, not `branch_merged_or_gone`'s
            // "gone counts as delivered" — a deleted-but-never-merged branch
            // must not read as done (TKT-18/46/147).
            if repo.branch_verified_merged(branch, target) {
                None
            } else {
                Some(format!(
                    "branch '{branch}' has not merged into '{target}' yet (delivery mode: {:?})",
                    policy.delivery.mode
                ))
            }
        }
        DeliveryMode::PushBranch if gate_push_branch => {
            let carries_work =
                fork_point.is_some_and(|fork| repo.branch_has_commits_since(branch, fork));
            if carries_work
                && repo.remote_branch_merged_or_gone(branch, target, &policy.delivery.remote)
            {
                None
            } else {
                Some(format!(
                    "branch '{branch}' has not landed on '{}/{target}' yet (delivery mode: push-branch)",
                    policy.delivery.remote
                ))
            }
        }
        DeliveryMode::PushBranch | DeliveryMode::Pr => None,
    }
}

/// Whether `branch` (bound for `target` in the repo rooted at `repo_root`)
/// has been delivered per that repo's activated delivery-mode policy. Runs
/// the git reads on a blocking-pool thread — see [`blocking_io`] — since this
/// is called from spawned tasks, never the hot event-handling path.
async fn ticket_delivered(
    home: PathBuf,
    repo_root: PathBuf,
    branch: String,
    target: String,
    fork_point: Option<String>,
    gate_push_branch: bool,
) -> rk_core::Result<()> {
    blocking_io("ticket delivery gate", move || {
        let repo = Repo::discover(&repo_root).map_err(|e| {
            // Unresolvable repo: fail CLOSED, unlike
            // Supervisor::branch_already_merged's fail-safe "not merged" —
            // that guard only ever skips a respawn (safe to under-trigger),
            // while this one gates `done`, and open-failing it would let a
            // ticket close without ever having checked delivery.
            rk_core::Error::other(format!(
                "repo at {} is unresolvable, cannot verify delivery: {e}",
                repo_root.display()
            ))
        })?;
        let policy = resolve_repository_policy(&home, &repo);
        match ticket_undelivered_reason(
            &policy,
            &repo,
            &branch,
            &target,
            fork_point.as_deref(),
            gate_push_branch,
        ) {
            None => Ok(()),
            Some(reason) => Err(rk_core::Error::other(reason)),
        }
    })
    .await
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
    /// Exact work identity when this spawn is a reviewer for a machine-routed
    /// verdict. Ordinary reviewers and non-review roles leave this unset.
    #[serde(default)]
    pub review: Option<rk_core::review::ReviewContext>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub permission_mode: Option<String>,
    /// Explicit agent-profile name (`[agents.<name>]`, or a workflow `agents:`
    /// entry) for this dispatch. Naming one is a deliberate override: on the
    /// operator path it *replaces* cost-tier routing rather than layering
    /// under it, so `--profile` is how you opt a hand-dispatched ticket out of
    /// the fleet's cost rules. See `Daemon::route_spawn_profile`.
    #[serde(default)]
    pub profile: Option<String>,
    /// The profile the daemon resolved for this dispatch — a cost tier and/or
    /// named profile already layered over global `[agents.default]`. It stands
    /// in for the supervisor's global default profile for this spawn only, so
    /// explicit `harness`/`model`/`permission_mode` above still win field-wise.
    ///
    /// Server-side output of `Daemon::route_spawn_profile`, never accepted from
    /// a client (hence `serde(skip)`): a caller must not be able to hand itself
    /// a profile the routing table would not have given it.
    #[serde(skip)]
    pub resolved_profile: Option<AgentProfile>,
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
    /// Identity of the process session currently behind `controls[name]`, one
    /// fresh [`rk_core::id::SpawnId`] per `harness.launch` call. Deliberately
    /// NOT `AgentRecord.spawn`/`created_at`: a manual `rk respawn` of a
    /// `Completed` record intentionally reuses that record's generation
    /// (`respawn_mode`'s comment on `let generation = updated.created_at;`),
    /// so a generation-keyed check cannot tell the predecessor process from
    /// its respawned successor. This map can, because a respawn overwrites
    /// the entry with a new token — giving
    /// [`kill_lingering_after_done`](Self::kill_lingering_after_done) a key
    /// that actually changes across a respawn instead of a stale check with
    /// stale data.
    session_tokens: Mutex<HashMap<String, rk_core::id::SpawnId>>,
    space: Space,
    /// Shared with the server so ticket-lifecycle writes serialize on one lock.
    tickets: Arc<crate::tickets::Tickets>,
    pricing: PricingTable,
    budget: Budget,
    /// Hierarchical fleet/repo caps enforced as a pre-dispatch guard.
    fleet_budget: FleetBudget,
    /// Per-agent USD cap for `role == "reviewer"` (`[budget] reviewer_max_usd`,
    /// default $30), checked INSTEAD OF `budget` for that role. Stored as raw
    /// `f64` bits in an `AtomicU64` (same reason as
    /// [`max_load_per_cpu_bits`](Self::max_load_per_cpu_bits)): applied once
    /// from config via [`set_reviewer_max_usd`](Supervisor::set_reviewer_max_usd)
    /// after construction, read on the same hot path as the worker cap.
    /// Reviewers were observed uncapped in production (one hit $27, above the
    /// worker cap) — set above the cost of a legitimate deep review so a
    /// thorough review is never cut off mid-verdict.
    reviewer_max_usd_bits: AtomicU64,
    /// Agents already warned about budget (avoid repeat warnings).
    budget_warned: Mutex<std::collections::HashSet<String>>,
    /// Cost/usage rollup the budget machinery had computed for an agent at
    /// the moment it decided to hard-stop it, keyed by name. A SIGTERM'd
    /// harness can still flush a terminal `Completed` event of its own after
    /// the kill — and that self-reported figure can reflect only the partial
    /// final turn, not everything already spent. Without a floor, that lower
    /// number silently overwrites the correct one in the terminal record
    /// (the `budget_exceeded` obstacle keeps the true figure; the archived
    /// agent record does not) — a control-loop sensor error, since tier
    /// routing and cost analytics read the terminal record. Consumed (and
    /// removed) the first time this generation reaches a terminal event.
    budget_stop_floor: Mutex<HashMap<String, (f64, TokenUsage)>>,
    /// Fleet/repo scopes already warned at dispatch (avoid repeat obstacles).
    fleet_warned: Mutex<std::collections::HashSet<String>>,
    /// Per-agent liveness-sweep bookkeeping (burn-rate deltas + flag episodes).
    sweep_state: Mutex<HashMap<String, SweepState>>,
    /// Per-agent self-healing-respawn bookkeeping (attempt count + backoff
    /// clock + whether the cap has already been escalated). In-memory: a daemon
    /// restart is a fresh episode, so attempt counts reset with it.
    respawn_state: Mutex<HashMap<String, RespawnState>>,
    /// Castle-wide rolling rate cap + durable announce for auto-respawns
    /// (strategic review B2/B3): shared across every agent so the cap groups
    /// on the `"respawn"` kind fleet-wide, not per agent. In-memory, like
    /// `respawn_state` — a daemon restart is a fresh rolling window.
    recovery_announcer: crate::recovery::RecoveryAnnouncer,
    /// Per-agent completion-routing state, so one agent generation emits
    /// exactly one durable `harness_result` — the one for the turn it actually
    /// finished on (TKT-160). In-memory: a daemon restart loses the withheld
    /// turn, which is the same blindness a restart already imposes on the
    /// harness event stream it came from.
    completions: Mutex<HashMap<String, CompletionState>>,
    /// Bounded per-agent transcript (assistant text / tool calls / retries),
    /// so the operator can `rk log` a run instead of being blind to it.
    log: crate::agent_log::AgentLog,
    /// Serializes direct delivery, exact-candidate advancement, and reverts to
    /// the same target branch so concurrent ref updates cannot lose work.
    merge_queue: MergeQueue,
    /// Installed by `Daemon::landing` after both sides exist. Weak avoids an
    /// Arc cycle (`LandingPipeline` already owns its `Supervisor`). In a live
    /// daemon, merge-mode `land` fails closed if this seam is absent.
    landing_pipeline: Mutex<Option<Weak<crate::landing::LandingPipeline>>>,
    /// Serializes a repo-registered check's test-execution phase against
    /// every other same-repo check opted into `sharedCargoTarget` (TKT-01M0CFA1RX36SJ7DV4YWGHQ9BT).
    /// Only ever contended when [`shared_cargo_target`](Self::shared_cargo_target)
    /// is also on — see [`TestExecLock`] for why.
    test_exec_lock: TestExecLock,
    /// Bounded per-repo admission queue for daemon-managed verification runs
    /// (`WorkflowEngine::run_check_in`, gated to `sharedCargoTarget` checks —
    /// TKT-01M0HNESEECWWFQF8X6VH1XSJ6). Distinct from [`test_exec_lock`](Self::test_exec_lock):
    /// that lock serializes a shared-disk hazard down to exactly 1 concurrent
    /// runner; this queue bounds CPU/wall-clock contention and its limit is a
    /// configurable policy value that may be raised above 1. See
    /// [`VerificationAdmission`].
    verification_admission: VerificationAdmission,
    /// In-flight `verify.run`-mediated verification executions, tracked so
    /// their requesting agent's interrupt/dismiss/terminal death, or their
    /// RPC caller's disconnect, can cancel the exact managed child process
    /// group instead of leaving it orphaned under the daemon. See
    /// [`ManagedVerificationRuns`].
    managed_verification: ManagedVerificationRuns,
    /// `[policy] implementation_admission_limit` / `_by_repo` — the
    /// implementation lane's configured limits (TKT-01M0P2KM83Y4MD5QYETR3JCKF2).
    /// See [`LaneLimits`] and [`crate::agents::Lane::Implementation`].
    implementation_admission_limits: LaneLimits,
    /// `[policy] review_admission_limit` / `_by_repo` — the review lane's
    /// configured limits, independent of `implementation_admission_limits` so
    /// a saturated implementation lane can never starve it. See
    /// [`crate::agents::Lane::Review`].
    review_admission_limits: LaneLimits,
    /// `[disk] min_free_gb` (0 = disabled), applied by `Daemon::new` from
    /// config. Defaults to 0 here — a bare `Supervisor` constructed directly
    /// by a test or another crate stays disk-guard-free unless it opts in via
    /// [`set_min_free_disk_gb`](Supervisor::set_min_free_disk_gb), so this
    /// safety feature cannot spuriously fail spawns on a tight CI disk.
    min_free_disk_gb: AtomicU64,
    /// `[machine] max_load_per_cpu` (0 = disabled), the CPU half of the
    /// scarce-resource signal. Stored as the raw bits of an `f64` in an
    /// `AtomicU64` (there is no stable `AtomicF64`) for the same reason
    /// [`min_free_disk_gb`](Self::min_free_disk_gb) is atomic: it is read on
    /// the hot admission path and set once from config, so a lock would be
    /// pure contention. Mirrors that field's default-disabled stance.
    max_load_per_cpu_bits: AtomicU64,
    /// `[disk] shared_cargo_target` (default false here), applied by
    /// `Daemon::new` from config. Mirrors [`min_free_disk_gb`](Self::min_free_disk_gb):
    /// a bare `Supervisor` built by a test stays on each worktree's own
    /// `target/` unless it opts in via
    /// [`set_shared_cargo_target`](Supervisor::set_shared_cargo_target).
    shared_cargo_target: AtomicBool,
    /// Set by `daemon.pause_dispatch` (`rk daemon rollover`'s drain step);
    /// gates admission in [`spawn`](Self::spawn) only — it does not touch
    /// agents already running. In-memory: a fresh daemon process always
    /// starts unpaused, so a rollover can never leave a *future* daemon stuck.
    dispatch_paused: AtomicBool,
    /// `[supervisor] done_kill_grace_secs` (seam 7): how long a harness
    /// process gets to exit on its own after a clean `rk done` before
    /// [`schedule_done_kill`](Supervisor::schedule_done_kill) SIGKILLs it.
    /// An `AtomicU64`, not a config struct held on `Supervisor`, because it
    /// must be read in real time from the event-handling path (mirrors
    /// `min_free_disk_gb` above) rather than only on a periodic sweep tick.
    done_kill_grace_secs: AtomicU64,
    /// `[supervisor] transport_breaker_trip_threshold`: consecutive
    /// castle-wide pre-work transport failures for one provider that trip
    /// the circuit breaker. Same reasoning as `done_kill_grace_secs` —
    /// `record_transport_outage` needs it in real time from the
    /// event-handling path, not just on the periodic sweep tick that
    /// otherwise carries a fresh `SupervisorConfig` on every call.
    transport_breaker_trip_threshold: AtomicU64,
    /// `[supervisor] transport_breaker_cooldown_secs`: same reasoning as
    /// `transport_breaker_trip_threshold` immediately above, but for
    /// [`spawn`](Self::spawn)'s admission check — a NEW launch for a
    /// tripped provider must be refused on the hot spawn path in real time,
    /// not just retried on `transport_retry_sweep`'s periodic tick.
    transport_breaker_cooldown_secs: AtomicU64,
    /// Push channels for automated recovery actions this supervisor
    /// announces (kill-at-`rk done` today). Set once by `Daemon::new`'s
    /// config-loading path via [`set_sinks`](Supervisor::set_sinks) —
    /// bare/test constructors default to an empty registry, so the durable
    /// announce tuple still gets written but nothing fans out.
    sinks: Mutex<rk_core::notify::SinkRegistry>,
    /// Rate-cap bookkeeping shared across this supervisor's recovery
    /// announcements (`RecoveryAnnouncer` state must persist across calls to
    /// cap correctly — see `recovery.rs`).
    announcer: crate::recovery::RecoveryAnnouncer,
    /// Castle-wide, per-provider circuit breaker for pre-work harness
    /// transport outages (TKT-01M0HND8M25GYN1ZTRET3S5769). Durable — see
    /// [`crate::transport_breaker::TransportBreakers`] — unlike
    /// `respawn_state`: a daemon restart must not silently re-open a breaker
    /// that was protecting a genuinely down provider.
    transport_breakers: Mutex<crate::transport_breaker::TransportBreakers>,
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
    /// When the current RUNAWAY episode was first flagged (soft-steered).
    /// `None` = not currently flagged. The kill escalation measures from
    /// here. In-memory only (a daemon restart is a fresh burn-rate episode —
    /// unlike the STUCK axis, whose ceiling is persisted on the record
    /// itself; see `AgentRecord::liveness::ceiling_started_at` and
    /// `Supervisor::update_stuck_ceiling`).
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

/// Evidence [`Supervisor::gather_liveness_evidence`] found for one generation
/// already silent past the stuck bar. A RECOGNIZED live verifier/build
/// descendant (`rk`, `mise`, `cargo`, `rustc` — see
/// [`crate::workflow_exec::is_verifier_command`]) or genuinely advancing
/// bounded output [`proves_alive`](Self::proves_alive). `child_alive` alone
/// does not: a live regression test caught an EARLIER, unclassified version
/// of this check (any live descendant at all counts) excusing a genuinely
/// wedged fake harness whose script's LAST command forked a plain `sleep` —
/// indistinguishable from a real compiler descendant by process-tree
/// PRESENCE alone, which is exactly why the descendant's own command is now
/// checked, not just that one exists. A reconnect loop vetoes the whole
/// thing even when output looks like it is changing (a transport retry
/// commonly logs its own error to stderr on every attempt).
#[derive(Debug, Clone, Copy, Default)]
struct LivenessEvidence {
    child_alive: bool,
    live_verifier_descendants: usize,
    output_progressed: bool,
    reconnect_loop: bool,
}

impl LivenessEvidence {
    fn proves_alive(&self) -> bool {
        !self.reconnect_loop && (self.live_verifier_descendants > 0 || self.output_progressed)
    }
}

fn describe_stuck(idle_secs: u64, evidence: Option<&LivenessEvidence>) -> String {
    match evidence {
        Some(e) => format!(
            "no events for {idle_secs}s while still running (child_alive={}, \
             live_verifier_descendants={}, output_progressed={}, reconnect_loop={})",
            e.child_alive, e.live_verifier_descendants, e.output_progressed, e.reconnect_loop
        ),
        None => format!("no events for {idle_secs}s while still running"),
    }
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

fn is_auto_respawn_candidate(record: &AgentRecord) -> bool {
    matches!(record.state, AgentState::Orphaned | AgentState::Failed)
}

/// Deterministic per-attempt jitter (seconds, in `[0, jitter_window_secs)`)
/// added to a pre-work transport-outage retry's backoff. Derived from the
/// agent name and attempt number rather than true randomness so the
/// schedule is stable and restart-safe without persisting a seed: the same
/// generation retrying the same attempt always waits the same jittered
/// window, but different agents (or different attempts) spread out instead
/// of all retrying in lockstep on a provider-wide outage.
fn transport_retry_jitter_secs(name: &str, attempts: u32, jitter_window_secs: u64) -> u64 {
    if jitter_window_secs == 0 {
        return 0;
    }
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut hasher);
    attempts.hash(&mut hasher);
    hasher.finish() % jitter_window_secs
}

/// Serializes merges to the same target branch — the land / merge queue.
///
/// Landing and revert operations can update `main` (or any base) concurrently
/// and unattended. Without serialization two updates racing on the same target
/// interleave: each
/// merges in its own detached worktree captured from the target ref *before*
/// the other advanced it, so the compare-and-swap in [`Repo::merge_branch`]
/// bounces the loser to a silent `merged: false` and its branch is left
/// unmerged (the root cause of the "done ticket never in main" gap). A
/// per-`(repo, target)` FIFO lock makes every update to a given target
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
struct TestExecLock {
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl TestExecLock {
    /// Acquire the lock for `repo`. The returned guard is held for one
    /// check's entire run (every retry attempt); the next waiter proceeds
    /// only once it drops. Unbounded here — [`WorkflowEngine::run_check_in`]
    /// wraps the await in a `tokio::time::timeout` bounded by the check's own
    /// declared timeout, so a caller never waits past that budget even though
    /// this method alone cannot starve (every holder is itself bounded by its
    /// own check timeout, so the queue always drains).
    async fn acquire(&self, repo: &str) -> tokio::sync::OwnedMutexGuard<()> {
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
/// every `sharedCargoTarget` check through, whether it was dispatched by a
/// landing gate, a workflow `run` step, or the `verify.run` RPC an
/// agent/reviewer's own completion check calls into instead of self-invoking
/// a full suite. Keyed per repo, exactly like [`TestExecLock`] — the two are
/// independent resources (this bounds CPU/wall-clock contention across
/// concurrent full-suite runs; `TestExecLock` serializes a shared-disk build
/// hazard down to 1), so a check that opts into `sharedCargoTarget` acquires
/// BOTH, in the order [`WorkflowEngine::run_check_in`] declares them.
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
struct VerificationAdmission {
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
    fn set_limits(&self, default_limit: u32, overrides: HashMap<String, u32>) {
        self.default_limit
            .store(u64::from(default_limit), Ordering::Relaxed);
        *self.overrides.lock().unwrap() = overrides;
    }

    /// The configured WIP limit for `repo` — its own override if set, else
    /// the fleet-wide default. `0` means admission control is off for this
    /// repo.
    fn limit_for(&self, repo: &str) -> u32 {
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
    fn overridden_repos(&self, out: &mut std::collections::BTreeSet<String>) {
        out.extend(self.overrides.lock().unwrap().keys().cloned());
    }

    /// How many of `repo`'s configured permits are currently checked out, for
    /// reporting only (`Supervisor::capacity_summary`) — never consulted for
    /// admission itself. `0` whenever the limit is `0` (disabled) or no check
    /// has ever run for `repo` (no semaphore created yet).
    fn in_flight(&self, repo: &str) -> u32 {
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
    async fn acquire(
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

/// One in-flight `verify.run`-mediated verification execution
/// (TKT-01M0PA6C5WYRWS757R1SS2F2GR): a live post-deploy probe found that
/// interrupting the requesting agent, or killing the RPC client blocked on
/// `verify.run`, left the daemon-owned check process running under the
/// daemon alone, still occupying its repo's admission slot. Registered by
/// [`crate::workflow_exec::WorkflowEngine::verify_repo_check`] for the
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
/// `crate::workflow_exec::ManagedChildMarker` /
/// `reap_stale_managed_children`, which every `Daemon::run` runs before its
/// accept loop can serve a single request.
#[derive(Default)]
struct ManagedVerificationRuns {
    next_id: AtomicU64,
    runs: Mutex<HashMap<u64, ManagedVerificationRun>>,
}

impl ManagedVerificationRuns {
    fn register(
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

    fn unregister(&self, id: u64) {
        self.runs.lock().unwrap().remove(&id);
    }

    /// Cancel every run belonging to `agent`, fenced to `generation` when
    /// given: a namesake that has since taken over the name (a fresh
    /// generation after a dismiss+respawn) is never touched by a signal meant
    /// for its predecessor — the exact "never affects ... a newer
    /// generation/namesake" guarantee the ticket asks for.
    fn cancel_agent(
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
    fn cancel_request(&self, request_key: &str, reason: &'static str) {
        for run in self.runs.lock().unwrap().values() {
            if run.request_key == request_key {
                let _ = run.cancel.send(Some(reason));
            }
        }
    }
}

/// Fleet-wide default + per-repo overrides for one capacity lane's limit
/// (TKT-01M0P2KM83Y4MD5QYETR3JCKF2) — the config-side counterpart to
/// [`crate::agents::Lane`]. Deliberately holds only the configured NUMBER, not
/// any occupancy state: unlike [`VerificationAdmission`] (which bounds check
/// runs that have no `AgentRecord` of their own), an implementation/review
/// lane's occupancy is the durable `Registry` itself
/// ([`Registry::live_or_reserved_lane_wip`](crate::agents::Registry::live_or_reserved_lane_wip)),
/// so this struct needs no restart-recovery story beyond "re-read the config".
/// Same lock-free-on-the-hot-path shape as `VerificationAdmission`'s own
/// `default_limit`/`overrides`.
#[derive(Default)]
struct LaneLimits {
    default_limit: AtomicU64,
    overrides: Mutex<HashMap<String, u32>>,
}

impl LaneLimits {
    fn set(&self, default_limit: u32, overrides: HashMap<String, u32>) {
        self.default_limit
            .store(u64::from(default_limit), Ordering::Relaxed);
        *self.overrides.lock().unwrap() = overrides;
    }

    /// The configured limit for `repo` — its own override if set, else the
    /// fleet-wide default. `0` means this lane is unbounded for `repo`.
    fn limit_for(&self, repo: &str) -> u32 {
        self.overrides
            .lock()
            .unwrap()
            .get(repo)
            .copied()
            .unwrap_or(self.default_limit.load(Ordering::Relaxed) as u32)
    }

    /// Repos with an explicit per-repo override — see
    /// [`VerificationAdmission::overridden_repos`], same reporting-only role.
    fn overridden_repos(&self, out: &mut std::collections::BTreeSet<String>) {
        out.extend(self.overrides.lock().unwrap().keys().cloned());
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
#[derive(Debug, Clone, Default)]
pub struct Reap {
    /// The worktree and local branch — merged branches only.
    pub git: bool,
    /// The generation's transcript file under `agent-logs/`. One-way.
    pub logs: bool,
    /// Master switch for build-artifact reclaim. Whether anything actually
    /// gets deleted for a given record is then decided per-repo: its own
    /// activated `.rk/repo.cue` `reap.artifactPaths` if it declares one, else
    /// the operator-set [`artifact_paths_by_repo`](Self::artifact_paths_by_repo)/
    /// [`artifact_paths`](Self::artifact_paths) fallback below — see
    /// [`Supervisor::reap_artifacts`]. A repo that has configured neither
    /// stays a no-op even with this on, so leaving it on by default is safe.
    pub artifacts: bool,
    /// Fleet-wide fallback build-artifact paths (relative to a worktree
    /// root), used only for a repo whose activated policy declares no
    /// `reap.artifactPaths` of its own. STACK NEUTRALITY: empty by default —
    /// see [`Supervisor::reap_artifacts`].
    pub artifact_paths: Vec<String>,
    /// Per-repo override of `artifact_paths`, keyed by repo name — same
    /// fallback role, consulted before the fleet-wide default.
    pub artifact_paths_by_repo: HashMap<String, Vec<String>>,
}

impl Reap {
    /// The fallback paths to reap for one record's repo — its per-repo
    /// override if one is configured, else the fleet-wide default. Only
    /// consulted when the repo's own activated policy names nothing; see
    /// [`Supervisor::reap_artifacts`].
    fn artifact_paths_for(&self, repo: &str) -> &[String] {
        self.artifact_paths_by_repo
            .get(repo)
            .map(Vec::as_slice)
            .unwrap_or(&self.artifact_paths)
    }
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
        let transport_breakers = crate::transport_breaker::TransportBreakers::load(
            &layout.home().join("transport_breaker.json"),
        )?;
        Ok(Self {
            layout,
            castle,
            default_harness: defaults.harness,
            default_agent: defaults.profile,
            registry: Mutex::new(registry),
            controls: Mutex::new(HashMap::new()),
            session_tokens: Mutex::new(HashMap::new()),
            space,
            tickets,
            pricing,
            budget,
            fleet_budget,
            reviewer_max_usd_bits: AtomicU64::new(DEFAULT_REVIEWER_MAX_USD.to_bits()),
            budget_warned: Mutex::new(std::collections::HashSet::new()),
            budget_stop_floor: Mutex::new(HashMap::new()),
            fleet_warned: Mutex::new(std::collections::HashSet::new()),
            sweep_state: Mutex::new(HashMap::new()),
            respawn_state: Mutex::new(HashMap::new()),
            recovery_announcer: crate::recovery::RecoveryAnnouncer::new(),
            completions: Mutex::new(HashMap::new()),
            log,
            merge_queue: MergeQueue::default(),
            landing_pipeline: Mutex::new(None),
            test_exec_lock: TestExecLock::default(),
            verification_admission: VerificationAdmission::default(),
            managed_verification: ManagedVerificationRuns::default(),
            implementation_admission_limits: LaneLimits::default(),
            review_admission_limits: LaneLimits::default(),
            min_free_disk_gb: AtomicU64::new(0),
            max_load_per_cpu_bits: AtomicU64::new(0f64.to_bits()),
            shared_cargo_target: AtomicBool::new(false),
            dispatch_paused: AtomicBool::new(false),
            done_kill_grace_secs: AtomicU64::new(
                rk_core::config::SupervisorConfig::default().done_kill_grace_secs,
            ),
            transport_breaker_trip_threshold: AtomicU64::new(
                rk_core::config::SupervisorConfig::default().transport_breaker_trip_threshold
                    as u64,
            ),
            transport_breaker_cooldown_secs: AtomicU64::new(
                rk_core::config::SupervisorConfig::default().transport_breaker_cooldown_secs,
            ),
            sinks: Mutex::new(rk_core::notify::SinkRegistry::default()),
            announcer: crate::recovery::RecoveryAnnouncer::new(),
            transport_breakers: Mutex::new(transport_breakers),
        })
    }

    /// Set the `[disk] min_free_gb` floor (0 = disabled). Applied by
    /// `Daemon::new` from config; exposed on `&self` (not `&mut self`) since
    /// the supervisor is shared behind an `Arc` from construction onward.
    pub fn set_min_free_disk_gb(&self, gb: u64) {
        self.min_free_disk_gb.store(gb, Ordering::Relaxed);
    }

    /// Set the `[machine] max_load_per_cpu` ceiling (0 = disabled). Applied by
    /// `Daemon::new` from config, same pattern as
    /// [`set_min_free_disk_gb`](Supervisor::set_min_free_disk_gb).
    pub fn set_max_load_per_cpu(&self, per_cpu: f64) {
        self.max_load_per_cpu_bits
            .store(per_cpu.to_bits(), Ordering::Relaxed);
    }

    /// Set `[budget] reviewer_max_usd` (0 = unlimited; built-in default
    /// [`DEFAULT_REVIEWER_MAX_USD`] applies until this is called). Applied by
    /// `Daemon::new` from config, same pattern as
    /// [`set_min_free_disk_gb`](Supervisor::set_min_free_disk_gb).
    pub fn set_reviewer_max_usd(&self, usd: f64) {
        self.reviewer_max_usd_bits
            .store(usd.to_bits(), Ordering::Relaxed);
    }

    /// The budget checked for `record.role == "reviewer"` — same graduated
    /// warn→stop shape as the ordinary worker `budget`, but a distinct cap
    /// (see [`enforce_budget`](Self::enforce_budget)) and no token cap: the
    /// $27 outlier that motivated this was a cost blowout, not a token one.
    fn reviewer_budget(&self) -> Budget {
        Budget {
            max_usd: f64::from_bits(self.reviewer_max_usd_bits.load(Ordering::Relaxed)),
            max_tokens: 0,
            warn_at: self.budget.warn_at,
        }
    }

    /// The budget governing `record`'s role: the reviewer cap for
    /// `role == "reviewer"`, the ordinary worker cap for everything else.
    fn budget_for(&self, record: &AgentRecord) -> Budget {
        if record.role == "reviewer" {
            self.reviewer_budget()
        } else {
            self.budget
        }
    }

    /// The currently configured physical-capacity floors, as one value.
    pub fn machine_floors(&self) -> crate::machine::MachineFloors {
        crate::machine::MachineFloors {
            min_free_disk_gb: self.min_free_disk_gb.load(Ordering::Relaxed),
            max_load_per_cpu: f64::from_bits(self.max_load_per_cpu_bits.load(Ordering::Relaxed)),
        }
    }

    /// Set `[disk] shared_cargo_target`. Applied by `Daemon::new` from
    /// config, same pattern as
    /// [`set_min_free_disk_gb`](Supervisor::set_min_free_disk_gb).
    pub fn set_shared_cargo_target(&self, enabled: bool) {
        self.shared_cargo_target.store(enabled, Ordering::Relaxed);
    }

    /// Whether spawned agents currently share one `CARGO_TARGET_DIR` per
    /// repo — the precondition for [`TestExecLock`] contention to be
    /// possible at all. [`WorkflowEngine::run_check_in`] reads this before
    /// bothering to acquire the lock, so the lock has zero effect (not even
    /// mutex overhead beyond the check) when the flag is off.
    pub(crate) fn shared_cargo_target_enabled(&self) -> bool {
        self.shared_cargo_target.load(Ordering::Relaxed)
    }

    /// Acquire the shared-target-dir test-execution lock for `repo`. See
    /// [`TestExecLock`] for what this serializes and why.
    pub(crate) async fn acquire_test_exec_lock(
        &self,
        repo: &str,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        self.test_exec_lock.acquire(repo).await
    }

    /// Set `[policy] verification_admission_limit` / `_by_repo`. Applied by
    /// `Daemon::new` from config, same pattern as
    /// [`set_min_free_disk_gb`](Supervisor::set_min_free_disk_gb).
    pub fn set_verification_admission_limits(
        &self,
        default_limit: u32,
        overrides: HashMap<String, u32>,
    ) {
        self.verification_admission
            .set_limits(default_limit, overrides);
    }

    /// The configured verification admission WIP limit for `repo` (0 =
    /// disabled). Exposed so a caller can decide whether to bother measuring
    /// queue-wait/execution timing at all before calling
    /// [`acquire_verification_admission`](Self::acquire_verification_admission).
    pub(crate) fn verification_admission_limit_for(&self, repo: &str) -> u32 {
        self.verification_admission
            .limit_for(&self.verification_repo_identity(repo))
    }

    /// Acquire one bounded per-repo verification admission permit for `repo`.
    /// See [`VerificationAdmission`] for what this bounds, the FIFO fairness
    /// guarantee, and why a daemon restart can never leak one.
    pub(crate) async fn acquire_verification_admission(
        &self,
        repo: &str,
        limit: u32,
    ) -> Option<(tokio::sync::OwnedSemaphorePermit, std::time::Duration)> {
        self.verification_admission
            .acquire(&self.verification_repo_identity(repo), limit)
            .await
    }

    /// Register one managed `verify.run` execution for cancellation binding.
    /// Returns an opaque id (for
    /// [`unregister_managed_verification`](Self::unregister_managed_verification))
    /// and the receiver half the execution races itself against — see
    /// [`ManagedVerificationRuns`].
    pub(crate) fn register_managed_verification(
        &self,
        agent: &str,
        generation: Option<rk_core::id::SpawnId>,
        request_key: &str,
    ) -> (u64, tokio::sync::watch::Receiver<Option<&'static str>>) {
        self.managed_verification
            .register(agent, generation, request_key)
    }

    /// Drop a managed run's registration once its call has returned (whatever
    /// the outcome) — must be called exactly once per
    /// [`register_managed_verification`](Self::register_managed_verification),
    /// or a settled call would remain a live cancellation target forever.
    pub(crate) fn unregister_managed_verification(&self, id: u64) {
        self.managed_verification.unregister(id);
    }

    /// Cancel every managed verification run belonging to `agent`, fenced to
    /// `generation` when the caller has one (an agent record's current
    /// [`AgentRecord::spawn_id`]) so a namesake's later generation is never
    /// touched.
    pub(crate) fn cancel_managed_verification_for_agent(
        &self,
        agent: &str,
        generation: Option<rk_core::id::SpawnId>,
        reason: &'static str,
    ) {
        self.managed_verification
            .cancel_agent(agent, generation, reason);
    }

    /// Cancel the one managed verification run correlated with
    /// `request_key` — the RPC-disconnect half of the binding.
    pub(crate) fn cancel_managed_verification_request(
        &self,
        request_key: &str,
        reason: &'static str,
    ) {
        self.managed_verification
            .cancel_request(request_key, reason);
    }

    /// Set `[policy] implementation_admission_limit` / `_by_repo`. Applied by
    /// `Daemon::new` from config, same pattern as
    /// [`set_verification_admission_limits`](Supervisor::set_verification_admission_limits).
    pub fn set_implementation_admission_limits(
        &self,
        default_limit: u32,
        overrides: HashMap<String, u32>,
    ) {
        self.implementation_admission_limits
            .set(default_limit, overrides);
    }

    /// Set `[policy] review_admission_limit` / `_by_repo`. Same pattern as
    /// [`set_implementation_admission_limits`](Supervisor::set_implementation_admission_limits).
    pub fn set_review_admission_limits(&self, default_limit: u32, overrides: HashMap<String, u32>) {
        self.review_admission_limits.set(default_limit, overrides);
    }

    /// The configured limit for `lane` in `repo` (0 = unbounded). See
    /// [`crate::agents::Lane`].
    pub(crate) fn lane_admission_limit_for(&self, repo: &str, lane: crate::agents::Lane) -> usize {
        (match lane {
            crate::agents::Lane::Implementation => {
                self.implementation_admission_limits.limit_for(repo)
            }
            crate::agents::Lane::Review => self.review_admission_limits.limit_for(repo),
        }) as usize
    }

    /// Configured capacity, current occupancy, and whether admission is
    /// currently exhausted, per repository, across all three lanes
    /// (TKT-01M0P2KM83Y4MD5QYETR3JCKF2) — what `rk top`/`rk status`/`rk digest`
    /// surface so an operator can see the configured ceiling and the reason a
    /// request would wait, not just the raw agent list. Reports every repo
    /// that either has an explicit per-repo override configured on any lane,
    /// or currently has a live agent — a repo with neither is fleet-default
    /// (usually unbounded) and has nothing occupying it, so omitting it keeps
    /// this proportional to what's actually interesting.
    pub fn capacity_summary(&self) -> serde_json::Value {
        let mut repos: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        self.implementation_admission_limits
            .overridden_repos(&mut repos);
        self.review_admission_limits.overridden_repos(&mut repos);
        self.verification_admission.overridden_repos(&mut repos);
        for record in self.list() {
            if record.state.is_live() {
                repos.insert(record.repo_name.clone());
            }
        }
        let reg = self.lock_registry();
        let mut out = serde_json::Map::new();
        for repo in repos {
            let impl_limit = self.implementation_admission_limits.limit_for(&repo);
            let impl_occupied =
                reg.live_or_reserved_lane_wip(&repo, crate::agents::Lane::Implementation) as u32;
            let (impl_waiting, impl_oldest_wait_secs) =
                reg.lane_wait_stats(&repo, crate::agents::Lane::Implementation);
            let review_limit = self.review_admission_limits.limit_for(&repo);
            let review_occupied =
                reg.live_or_reserved_lane_wip(&repo, crate::agents::Lane::Review) as u32;
            let (review_waiting, review_oldest_wait_secs) =
                reg.lane_wait_stats(&repo, crate::agents::Lane::Review);
            let verify_limit = self
                .verification_admission
                .limit_for(&self.verification_repo_identity(&repo));
            let verify_in_flight = self
                .verification_admission
                .in_flight(&self.verification_repo_identity(&repo));
            out.insert(
                repo,
                json!({
                    // `waiting_reason` fires on raw occupancy OR a non-empty
                    // durable queue: `try_reserve_lane_wip` refuses a NEW
                    // arrival whenever anyone else is queued ahead of it, even
                    // in the brief window right after a slot frees but before
                    // the queue's head has retried to claim it — that freed
                    // slot is already logically spoken for, so occupancy
                    // alone would under-report "full" during exactly that
                    // window.
                    "implementation": {
                        "limit": impl_limit,
                        "occupied": impl_occupied,
                        "waiting_count": impl_waiting,
                        "oldest_wait_secs": impl_oldest_wait_secs,
                        "waiting_reason":
                            (impl_limit != 0 && (impl_occupied >= impl_limit || impl_waiting > 0))
                                .then_some("implementation_lane_full"),
                    },
                    "review": {
                        "limit": review_limit,
                        "occupied": review_occupied,
                        "waiting_count": review_waiting,
                        "oldest_wait_secs": review_oldest_wait_secs,
                        "waiting_reason": (review_limit != 0
                            && (review_occupied >= review_limit || review_waiting > 0))
                            .then_some("review_lane_full"),
                    },
                    // The verification lane's own admission (`VerificationAdmission`)
                    // is a `tokio::sync::Semaphore`, independently proven FIFO
                    // (`acquire_verification_admission_grants_permits_in_fifo_order`)
                    // — it does not track queue depth/age the way the durable
                    // `Registry` wait-queue does for the other two lanes, so only
                    // `waiting_reason` is available here.
                    "verification": {
                        "limit": verify_limit,
                        "in_flight": verify_in_flight,
                        "waiting_reason": (verify_limit != 0 && verify_in_flight >= verify_limit)
                            .then_some("verification_lane_full"),
                    },
                }),
            );
        }
        serde_json::Value::Object(out)
    }

    /// Normalize `repo` — whatever shape reached
    /// [`WorkflowEngine::run_check_in`](crate::workflow_exec::WorkflowEngine::run_check_in)
    /// — to the one stable identity every [`VerificationAdmission`] bound,
    /// and the durable event recording it, must agree on (continuation of
    /// TKT-01M0HNESEECWWFQF8X6VH1XSJ6). Four call paths reach `run_check_in`
    /// with two different shapes: a workflow `run` step or reactor dispatch
    /// passes the repo's absolute, already-canonicalized checkout PATH
    /// (`instance.repo` / the registry's own `record.path`); a landing gate
    /// or `verify.run` passes its already-registered bare NAME
    /// (`entry.repo_name` / `VerifyRunParams.repo`). An absolute path is
    /// resolved here to its registered NAME via the repo registry —
    /// deliberately NOT to its directory basename (contrast
    /// `crate::workflow_exec::repo_name_of`): two independently registered
    /// repos can share a directory basename, so keying on basename would
    /// collide two distinct admission bounds into one. A path with no
    /// registry match, or a string that was already a bare name, is returned
    /// unchanged — a bare name is already the stable identity
    /// landing/`verify.run` use, and an unregistered path has no name to
    /// resolve to.
    pub(crate) fn verification_repo_identity(&self, repo: &str) -> String {
        let path = std::path::Path::new(repo);
        if path.is_absolute() {
            if let Ok(registry) =
                crate::repos::RepoRegistry::load(&self.layout.home().join("repos.json"))
            {
                if let Some(record) = registry.get_by_path(path) {
                    return record.name.clone();
                }
            }
        }
        repo.to_string()
    }

    /// Pause or resume new-agent admission ([`spawn`](Self::spawn)). Used by
    /// `rk daemon rollover` to stop the drain autoscaler, scheduler, and
    /// `agent.spawn`/`workflow.run` from growing the live-agent count while
    /// it waits the fleet down to park it for a daemon restart — all three
    /// dispatch sources funnel through the same `spawn` admission path, so
    /// one flag here covers all of them.
    pub fn set_dispatch_paused(&self, paused: bool) {
        self.dispatch_paused.store(paused, Ordering::Relaxed);
    }

    pub fn dispatch_paused(&self) -> bool {
        self.dispatch_paused.load(Ordering::Relaxed)
    }

    /// Wire the `[[notify.sinks]]` fan-out for this supervisor's automated
    /// recovery announcements. Applied by `Daemon::new` from config, same
    /// pattern as [`set_min_free_disk_gb`](Supervisor::set_min_free_disk_gb)
    /// — bare/test constructors never call this, so their announcements
    /// still write the durable tuple but push to no channel.
    pub fn set_sinks(&self, sinks: rk_core::notify::SinkRegistry) {
        *self.sinks.lock().unwrap_or_else(|p| p.into_inner()) = sinks;
    }

    /// Set `[supervisor] done_kill_grace_secs`. Applied by `Daemon::new` from
    /// config, same pattern as
    /// [`set_min_free_disk_gb`](Supervisor::set_min_free_disk_gb).
    pub fn set_done_kill_grace_secs(&self, secs: u64) {
        self.done_kill_grace_secs.store(secs, Ordering::Relaxed);
    }

    /// `[supervisor] transport_breaker_trip_threshold`, applied by
    /// `Daemon::new`/`set_sweep_config` from config — see the field doc.
    pub fn set_transport_breaker_trip_threshold(&self, threshold: u32) {
        self.transport_breaker_trip_threshold
            .store(threshold as u64, Ordering::Relaxed);
    }

    /// `[supervisor] transport_breaker_cooldown_secs`, applied by
    /// `Daemon::new`/`set_sweep_config` from config — see the field doc.
    pub fn set_transport_breaker_cooldown_secs(&self, secs: u64) {
        self.transport_breaker_cooldown_secs
            .store(secs, Ordering::Relaxed);
    }

    /// The per-agent transcript store (for `agent.log` reads and `--follow`).
    pub fn log(&self) -> &crate::agent_log::AgentLog {
        &self.log
    }

    /// Every transcript generation of `name`, oldest first, each carrying its
    /// [`rk_core::id::SpawnId`] (E3, docs/2026-08-17-tkt-c1-generation-identity.md)
    /// and the exclusive upper bound (the next generation's `created_at`) that
    /// isolates it inside a legacy name-keyed log file.
    ///
    /// Empty when no record — live or archived — carries the name. Callers
    /// reading a transcript anyway should fall back to
    /// [`Generation::unrecorded`](crate::agent_log::Generation::unrecorded).
    pub fn log_generations(&self, name: &str) -> Vec<crate::agent_log::Generation> {
        let records: Vec<AgentRecord> = self
            .lock_registry()
            .records_of(name)
            .into_iter()
            .cloned()
            .collect();
        records
            .iter()
            .enumerate()
            .map(|(i, record)| {
                crate::agent_log::Generation::of(
                    name,
                    record.spawn_id(),
                    record.created_at,
                    records.get(i + 1).map(|r| r.created_at),
                )
            })
            .collect()
    }

    /// Resolve an exact `SpawnId` typed at the `rk log` prompt to the
    /// generation it names — the E4 "exact form" alongside the existing
    /// name(+ordinal) resolution. Searches live and archived records (no name
    /// needed up front, since a bare `SpawnId` already disambiguates).
    pub fn find_generation_by_spawn(
        &self,
        spawn: rk_core::id::SpawnId,
    ) -> Option<crate::agent_log::Generation> {
        let name = self
            .list_all()
            .into_iter()
            .find(|r| r.spawn_id() == spawn)?
            .name;
        self.log_generations(&name)
            .into_iter()
            .find(|g| g.spawn == Some(spawn))
    }

    /// The transcript file of `name`'s most recent generation — live or
    /// archived. `None` when no record carries the name, which a lifecycle
    /// hook dispatch (agent_completed/failed/dismissed) treats as "no
    /// transcript to ship," not an error: an agent that never narrated still
    /// completes.
    pub fn latest_transcript_path(&self, name: &str) -> Option<std::path::PathBuf> {
        let generation = self.log_generations(name).into_iter().last()?;
        self.log.transcript_path(&generation)
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

    /// Best-effort `AgentLaunched` phase span for a generation that has just
    /// reached `Running`. The span covers the launch itself: `started_at` is
    /// the journal row's `created_at` (stamped before worktree creation and
    /// harness launch), `ended_at` is now, so `duration_ms` is the wall-clock
    /// cost of standing the agent up. Called from both launch tails —
    /// headless [`spawn`](Self::spawn) and [`spawn_attached`](Self::spawn_attached),
    /// which is spawn's early-return branch — so an attached rat is not a
    /// hole in the timeline.
    ///
    /// Attempt is left at the default `1`: a respawn of the same task dedups
    /// onto the first launch, matching the `Completed` producer's own shape.
    /// Telemetry never fails a spawn that already landed, hence the ignored
    /// result.
    fn record_agent_launched_span(&self, record: &AgentRecord) {
        let Some(task) = &record.task else {
            return;
        };
        let _ = crate::span::record_phase_span(
            &self.space,
            &record.repo_name,
            &self.castle,
            &crate::span::PhaseSpan::new(task, crate::span::Phase::AgentLaunched)
                .repo(&record.repo_name)
                .target(&record.target_branch)
                .started_at(record.created_at)
                .ended_at(Utc::now()),
        );
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
        if self.dispatch_paused.load(Ordering::Relaxed) {
            return Err(rk_core::Error::other(
                "dispatch is paused for a daemon rollover; try again shortly",
            ));
        }
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
        // Capture before creating the branch. Unlike a later merge-base read,
        // this remains the original fork even after a forge fast-forwards the
        // target to the branch tip.
        let fork_point = repo.rev_parse(&target_branch)?;

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

        // Castle-wide circuit-breaker admission: a NEW launch for a provider
        // whose breaker is currently open is refused here, before any
        // WIP/lane slot or budget is touched — `transport_retry_sweep` is
        // the only path that may relaunch THIS provider's own crashed
        // generations while it recovers, but an unrelated fresh spawn must
        // not queue up behind (or worse, race past) that recovery either.
        // Checked purely in memory (no registry lock), matching the other
        // early preflight gates above it.
        if self.lock_transport_breakers().is_open(
            &effective.harness,
            Utc::now(),
            self.transport_breaker_cooldown_secs.load(Ordering::Relaxed),
        ) {
            return Err(rk_core::Error::other(transport_breaker_open_refused(
                &effective.harness,
            )));
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
        // Independently of `fleet_wip_cap` above, also admit against this
        // repository's own capacity lane (TKT-01M0P2KM83Y4MD5QYETR3JCKF2) —
        // `Lane::Implementation` for every role but `"reviewer"`,
        // `Lane::Review` for reviewers — in the SAME critical section, so a
        // repo-scoped burst on one lane can never race past this repo's own
        // cap the way the fleet-wide ceiling (with no repo dimension) can be
        // exhausted by a single repository today.
        let lane = crate::agents::Lane::for_role(&params.role);
        let lane_cap = self.lane_admission_limit_for(&repo_name, lane);
        let lane_refused = match lane {
            crate::agents::Lane::Implementation => IMPLEMENTATION_LANE_REFUSED,
            crate::agents::Lane::Review => REVIEW_LANE_REFUSED,
        };
        // Stable across THIS caller's own retries (a workflow fan-out step
        // polling `is_fleet_wip_refusal` every 250ms, or drain reopening then
        // reclaiming the same ticket) so the durable wait queue holds this
        // logical request's place in line instead of minting a fresh entry
        // per attempt — see `Registry::try_reserve_lane_wip`/`LaneWaiter`.
        let lane_wait_key = match &params.workflow_instance {
            Some(instance) => format!("workflow:{instance}:{}", params.task),
            None => format!("{}:{}", params.role, params.task),
        };
        let name = {
            let mut reg = self.lock_registry();
            if !reg.try_reserve_wip(fleet_wip_cap) {
                return Err(rk_core::Error::other(FLEET_WIP_CAP_REFUSED));
            }
            if !reg.try_reserve_lane_wip(&repo_name, lane, lane_cap, &lane_wait_key) {
                reg.release_wip(fleet_wip_cap);
                return Err(rk_core::Error::other(lane_refused));
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
                reg.release_lane_wip(&repo_name, lane, lane_cap);
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
            fork_point,
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
            reg.release_lane_wip(&repo_name, lane, lane_cap);
            return Err(e);
        }
        // The reservation's job is done: this spawn now has a live registry
        // row (state `Spawning`), which itself counts toward the fleet-WIP
        // ceiling (and this repo's lane ceiling) from here on — worktree
        // creation and harness launch (both potentially slow) proceed without
        // holding the reservation, and any failure from here is recorded on
        // that row via `mark_spawn_failed`, which naturally frees its slot by
        // leaving the live count (for both ceilings alike, since
        // `live_or_reserved_lane_wip` filters on `state.is_live()` the same
        // way `live_or_reserved_wip` does).
        {
            let mut reg = self.lock_registry();
            reg.release_wip(fleet_wip_cap);
            reg.release_lane_wip(&repo_name, lane, lane_cap);
        }
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
            review: params.review.clone(),
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
            params.review.as_ref(),
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
        let session_token = self.track_session(&name, session.control.clone());

        self.record_agent_launched_span(&record);
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
        let spawn = record.spawn_id();
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                supervisor.handle_event(&name, generation, spawn, session_token, event);
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
        self.record_agent_launched_span(&record);
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
        // Generation-identity migration (C1/S3a, docs/2026-08-17-tkt-c1-
        // generation-identity.md): key on the minted `SpawnId` when this
        // generation has one — an equality predicate no namesake can satisfy —
        // falling back to the name+floor test for a record written before the
        // migration.
        let spawn = record.spawn;
        tokio::spawn(async move {
            let pattern = match spawn {
                Some(spawn) => Pattern::for_spawn(Category::Event, "task_done", spawn),
                None => Pattern::for_agent_since(Category::Event, "task_done", &agent, since),
            };
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
        let instruction_base = self.instruction_base(&record.role, &record.target_branch, &repo);

        let env = self.agent_env(
            &record.name,
            &record.role,
            &record.repo_name,
            &task,
            record.branch.as_deref(),
            &instruction_base,
            &worktree,
            record.workflow_instance.as_deref(),
            record.review.as_ref(),
        );

        let prime_ctx = PrimeContext {
            agent: record.name.clone(),
            repo: record.repo_name.clone(),
            task: record.task.clone(),
            branch: record.branch.clone(),
            base: Some(instruction_base),
            review: record.review.clone(),
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
        // Overwrites whatever token the predecessor session registered, so a
        // grace timer armed for that session can no longer match this one
        // (see `session_tokens` on `Supervisor`).
        let session_token = self.track_session(name, session.control.clone());

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
        // `created_at`/`spawn`) is reused — so the second run appends to the
        // transcript the first run started, which is what an operator expects.
        let generation = updated.created_at;
        let spawn = updated.spawn_id();
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                supervisor.handle_event(&owned, generation, spawn, session_token, event);
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
    /// event loop is wired up: completion-routing bookkeeping (`CompletionState`)
    /// is keyed on it. `spawn` is that same record's `SpawnId`
    /// (`AgentRecord::spawn_id`) — the key transcript writes use instead
    /// (docs/2026-08-17-tkt-c1-generation-identity.md, E-series), so a line can
    /// never land in a namesake's file. `session` is this specific process
    /// launch's token (see `session_tokens` on `Supervisor`) — unlike
    /// `generation`/`spawn`, it changes across a respawn even though all three
    /// share the same record.
    fn handle_event(
        self: &Arc<Self>,
        name: &str,
        generation: DateTime<Utc>,
        spawn: rk_core::id::SpawnId,
        session: rk_core::id::SpawnId,
        event: HarnessEvent,
    ) {
        // A harness that speaks again has resumed the turn it paused on.
        // `Completed`/`Exited` are excluded because they decide their own
        // state below (a fresh pause, a completion, or a death).
        if !matches!(
            event,
            HarnessEvent::Completed { .. } | HarnessEvent::Exited { .. }
        ) {
            self.resume_if_paused(name);
        }
        match event {
            HarnessEvent::Started { session_id } => {
                let had_outage = self
                    .lock_registry()
                    .get(name)
                    .is_some_and(|r| r.transport_outage.is_some());
                let updated = self.lock_registry().update(name, |r| {
                    r.session_id = session_id.clone();
                    // A `Started` handshake is proof of life: whatever
                    // pre-work transport-outage episode was in progress for
                    // this generation is over, and the castle-wide breaker
                    // for its provider gets the same proof (see below).
                    r.transport_outage = None;
                    // A post-commit recovery that was CONTINUED (not
                    // abandoned) reaches proof-of-life here too: clear it so
                    // this name goes back to behaving like an ordinary
                    // generation — eligible for `detect_post_commit_outage`
                    // and `respawn_sweep` again on a later, unrelated crash.
                    // An abandoned recovery must stay stamped forever (that
                    // exclusion is deliberate, see `abandon_recovery`), and a
                    // still-unacknowledged one can only belong to some other,
                    // stale generation's launch racing this handler — never
                    // clear either of those.
                    let continued = r.recovery.as_ref().is_some_and(|rec| {
                        rec.ack.as_ref().is_some_and(|ack| {
                            !matches!(ack.outcome, crate::agents::RecoveryOutcome::Abandoned)
                        })
                    });
                    if continued {
                        // The record itself stops existing, but the
                        // at-most-once contract on its `ack` must not: park
                        // a durable, generation-scoped tombstone so a
                        // continuation/abandonment call arriving AFTER this
                        // resumed harness has already spoken can still
                        // replay the same outcome (or be refused for a
                        // different key) instead of seeing "no pending
                        // recovery". Overwrites (never merges) any prior
                        // tombstone, so a later continuation-then-Started
                        // cycle on this same generation supersedes it.
                        if let Some(rec) = r.recovery.take() {
                            if let Some(ack) = rec.ack {
                                r.recovery_receipt = Some(crate::agents::RecoveryReceipt {
                                    spawn: rec.spawn,
                                    ack,
                                });
                            }
                        }
                    }
                });
                if had_outage {
                    if let Ok(Some(record)) = updated {
                        self.lock_transport_breakers()
                            .record_success(&record.harness);
                    }
                }
            }
            HarnessEvent::Usage { usage } => {
                let real_usage = usage.total() > 0;
                let updated = self.lock_registry().update(name, |r| {
                    r.usage.add(&usage);
                    // Incremental cost for harnesses that don't self-report
                    // USD; an authoritative Completed cost overwrites later.
                    if let Some(model) = &r.model {
                        if let Some(price) = self.pricing.lookup(model) {
                            r.cost_usd += price.cost(&usage);
                        }
                    }
                    // Nonzero usage means the model actually answered — proof
                    // this generation is not stuck in a transport reconnect
                    // loop (see `LivenessEvidence::reconnect_loop`).
                    if real_usage {
                        r.liveness.reconnect_events = 0;
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
                // The claim is decided BEFORE the state write, because it is
                // what the state write depends on: a clean turn that nothing
                // proves is the last one is a PAUSE, not a completion. The
                // record is read first only to supply `claim_completion`'s
                // identity arguments — a name with no record cannot be updated
                // either, so nothing is claimed for one.
                let Some(pre) = self.status(name) else {
                    warn!(agent = name, "completion event for an unknown agent");
                    return;
                };
                // A deliberate stop wins races with a final harness event.
                // SIGINT/SIGTERM can prompt an adapter to flush a `Completed`
                // event before its process exits; accepting that event as a
                // normal completion would erase the terminal cause and could
                // make an unfinished run look successful.
                if pre.state == AgentState::Stopped {
                    let _ = self.lock_registry().update(name, |r| {
                        if usage.total() > 0 {
                            r.usage = usage;
                        }
                        if let Some(cost) = cost_usd {
                            r.cost_usd = cost;
                        }
                        self.apply_budget_stop_floor(&r.name, &mut r.cost_usd, &mut r.usage);
                        if session_id.is_some() {
                            r.session_id = session_id.clone();
                        }
                    });
                    return;
                }
                let claim = self.claim_completion(
                    name,
                    generation,
                    pre.spawn,
                    is_error,
                    uses_harness_terminal_completion(&pre.role, &pre.harness),
                );
                let updated = self.lock_registry().update(name, |r| {
                    r.state = if is_error {
                        AgentState::Failed
                    } else if claim.publish {
                        AgentState::Completed
                    } else {
                        // Turn boundary without a `rk done`, process still
                        // alive: awaiting resume. Held live so drain keeps its
                        // slot, the reopen sweep keeps its ticket, and no
                        // reaper mistakes an interruption for a finish.
                        AgentState::Paused
                    };
                    r.result = Some(result.clone());
                    if usage.total() > 0 {
                        r.usage = usage;
                    }
                    if let Some(cost) = cost_usd {
                        r.cost_usd = cost;
                    }
                    self.apply_budget_stop_floor(&r.name, &mut r.cost_usd, &mut r.usage);
                    if session_id.is_some() {
                        r.session_id = session_id.clone();
                    }
                });
                if let Ok(Some(record)) = updated {
                    if claim.publish {
                        info!(agent = name, is_error, "agent completed");
                        self.route_completion(&record, is_error, claim.declared_done, diff);
                        // Seam 7: only a positively-declared, clean `rk done`
                        // arms the post-completion kill grace — a turn that
                        // merely errored out (`is_error`) leaves the record
                        // `Failed`, not `Completed`, and stays reachable by
                        // the respawn sweep instead.
                        if !is_error && claim.declared_done {
                            self.schedule_done_kill(name.to_string(), generation, session);
                        }
                    } else {
                        info!(
                            agent = name,
                            state = ?record.state,
                            "harness returned control without a `rk done`; holding the turn \
                             result back rather than publishing it as the completion, and \
                             parking the agent as awaiting-resume rather than finished"
                        );
                    }
                }
            }
            HarnessEvent::Exited { code } => {
                let diff = self.diff_summary_for(name);
                self.lock_controls().remove(name);
                // The harness process behind this generation is provably
                // gone — clean exit, crash, or kill alike. Any `verify.run`
                // execution it still has in flight will never be read by a
                // caller that no longer exists, so its managed child must not
                // keep running under the daemon alone
                // (TKT-01M0PA6C5WYRWS757R1SS2F2GR).
                self.cancel_managed_verification_for_agent(
                    name,
                    Some(spawn),
                    "agent_terminal_death",
                );
                let updated = self.lock_registry().update(name, |r| {
                    r.pid = None;
                    // A paused agent is live, but it is not mid-turn: its
                    // harness DID report, the result was merely withheld for
                    // want of a `rk done`. So it terminalizes here like any
                    // other live record — the process is gone, nothing will
                    // resume it — but it must not be marked `crashed`, and its
                    // withheld turn text must survive: that text is exactly
                    // what `flush_withheld_completion` is about to publish.
                    let paused = r.state == AgentState::Paused;
                    // Exit without a Completed event = crash/kill.
                    if r.state.is_live() {
                        r.state = AgentState::Failed;
                        // ...except for a PAUSED record, which is live but not
                        // mid-turn. Its harness did report; the result was
                        // merely withheld for want of a `rk done`. It still
                        // terminalizes (the process is gone, nothing will
                        // resume it), but the two crash markers below are both
                        // false for it: a verdict WAS reported, and the
                        // withheld turn text is exactly what
                        // `flush_withheld_completion` is about to publish, so
                        // overwriting `result` here would destroy it.
                        if !paused {
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
                    }
                    // Consumes the floor even when a `Completed` event already
                    // did (a harmless no-op then), so a budget-killed agent
                    // that never reports a `Completed` at all still lands its
                    // true cost/usage on the terminal record.
                    self.apply_budget_stop_floor(&r.name, &mut r.cost_usd, &mut r.usage);
                });
                // Fenced to the session that actually died: a late `Exited`
                // from a SUPERSEDED session (e.g. a continuation already
                // resumed this name under a fresh session token before this
                // event was processed) must never write recovery state over
                // whatever the active session already established
                // (TKT-01M0HNDJ7AS9F1A3W22FRCC63N — "a stale or late
                // generation cannot overwrite the active generation").
                if self.lock_session_tokens().get(name) == Some(&session) {
                    if let Ok(Some(record)) = &updated {
                        self.detect_post_commit_outage(name, record);
                    }
                }
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
                self.record_output_progress(name, "text", &text, true);
                self.log
                    .append(name, spawn, crate::agent_log::LogEvent::Text { text });
            }
            HarnessEvent::ToolUse { name: tool } => {
                self.record_output_progress(name, "tool", &tool, true);
                self.log
                    .append(name, spawn, crate::agent_log::LogEvent::Tool { name: tool });
            }
            HarnessEvent::Retry { attempt, error } => {
                self.record_reconnect_event(name);
                self.log.append(
                    name,
                    spawn,
                    crate::agent_log::LogEvent::Retry { attempt, error },
                );
            }
            HarnessEvent::Stderr { text } => {
                self.log.append(
                    name,
                    spawn,
                    crate::agent_log::LogEvent::Stderr { text: text.clone() },
                );
                // stderr already unconditionally hits the registry (via the
                // bumping `update`, for `stderr_tail`) regardless of this
                // feature, so folding the fingerprint in here costs nothing
                // extra — unlike `record_output_progress`, which throttles
                // specifically because `AssistantText`/`ToolUse` otherwise
                // never touch the registry at all. Deliberately does NOT
                // reset `reconnect_events`: a transport-retry loop typically
                // logs its own error to stderr on every attempt, and that
                // chatter must not mask the loop it is reporting
                // (`LivenessEvidence::reconnect_loop` vetoes stale-but-
                // changing output for exactly this reason).
                let fingerprint = output_fingerprint("stderr", &text);
                let session = self.lock_session_tokens().get(name).copied();
                let _ = self.lock_registry().update(name, |r| {
                    crate::agents::append_stderr_tail(&mut r.stderr_tail, &text);
                    if r.liveness.session != session {
                        r.liveness = crate::agents::LivenessObservation {
                            session,
                            ..Default::default()
                        };
                    }
                    if r.liveness.output_fingerprint != fingerprint {
                        r.liveness.output_fingerprint = fingerprint;
                        r.liveness.output_changed_at = Some(Utc::now());
                    }
                });
            }
            HarnessEvent::ControlDelivered { envelope } => {
                if envelope.durable {
                    if let Some(record) = self.lock_registry().get(name) {
                        if let Err(error) = crate::steer::acknowledge(
                            &self.space,
                            &record.repo_name,
                            &envelope,
                            &self.castle,
                        ) {
                            warn!(
                                agent = name,
                                message_id = %envelope.message_id,
                                %error,
                                "failed to persist steer acknowledgement"
                            );
                        }
                    }
                }
            }
            HarnessEvent::TransportFailure { outcome } => {
                self.record_transport_outage(name, &outcome);
            }
        }
    }

    /// Record one bounded-output liveness event (`AssistantText`/`ToolUse`)
    /// for the stuck sweep, throttled like [`record_progress`](Self::record_progress)
    /// — these are the chattiest events in the harness pump and deliberately
    /// never otherwise touch the registry (see [`resume_if_paused`](Self::resume_if_paused)'s
    /// doc comment), so this only actually persists once per
    /// [`MIN_PROGRESS_INTERVAL`] even though it is called on every one. A
    /// stale (respawned-over) session's evidence is discarded, not merged,
    /// the moment a fresh one is observed — see
    /// [`LivenessObservation::session`](crate::agents::LivenessObservation::session).
    fn record_output_progress(&self, name: &str, kind: &str, text: &str, resets_reconnect: bool) {
        let now = Utc::now();
        let session = self.lock_session_tokens().get(name).copied();
        let fingerprint = output_fingerprint(kind, text);
        let _ = self.lock_registry().update_quiet(name, |r| {
            let mut changed = false;
            if r.liveness.session != session {
                r.liveness = crate::agents::LivenessObservation {
                    session,
                    ..Default::default()
                };
                changed = true;
            }
            let fresh = r
                .liveness
                .output_changed_at
                .is_some_and(|at| now - at < MIN_PROGRESS_INTERVAL);
            if !fresh && r.liveness.output_fingerprint != fingerprint {
                r.liveness.output_fingerprint = fingerprint;
                r.liveness.output_changed_at = Some(now);
                changed = true;
            }
            if resets_reconnect && r.liveness.reconnect_events != 0 {
                r.liveness.reconnect_events = 0;
                changed = true;
            }
            changed
        });
    }

    /// Record a harness transport `Retry` event for the stuck sweep. Never
    /// throttled (unlike [`record_output_progress`](Self::record_output_progress)):
    /// a retry storm is exactly what
    /// [`LivenessEvidence::reconnect_loop`](LivenessEvidence::reconnect_loop)
    /// needs an accurate count of, and retries are inherently rate-limited by
    /// the harness's own backoff, never per-token chatty.
    fn record_reconnect_event(&self, name: &str) {
        let session = self.lock_session_tokens().get(name).copied();
        let _ = self.lock_registry().update_quiet(name, |r| {
            if r.liveness.session != session {
                r.liveness = crate::agents::LivenessObservation {
                    session,
                    ..Default::default()
                };
            }
            r.liveness.reconnect_events = r.liveness.reconnect_events.saturating_add(1);
            true
        });
    }

    /// Lift a [`Paused`](AgentState::Paused) record back to `Running` — the
    /// harness has produced output again, so the turn it parked on has resumed.
    ///
    /// Called for every harness event except `Completed`/`Exited`, which decide
    /// their own state. Reads before it writes: `Registry::update` persists
    /// synchronously, and this runs on chatty per-token event paths
    /// (`AssistantText`, `ToolUse`) that deliberately do not touch the registry
    /// at all — an unconditional update would turn each of them into a disk
    /// write.
    fn resume_if_paused(&self, name: &str) {
        let paused = self
            .lock_registry()
            .get(name)
            .is_some_and(|r| r.state == AgentState::Paused);
        if !paused {
            return;
        }
        let updated = self.lock_registry().update(name, |r| {
            // Re-checked under the write lock: another event may have
            // terminalized the record between the read above and here.
            if r.state == AgentState::Paused {
                r.state = AgentState::Running;
            }
        });
        if matches!(&updated, Ok(Some(r)) if r.state == AgentState::Running) {
            info!(agent = name, "paused agent resumed");
        }
    }

    /// Graduated budget policy: warn once at the threshold (obstacle tuple +
    /// steer when possible), hard-stop at the cap.
    fn enforce_budget(self: &Arc<Self>, record: &AgentRecord) {
        let budget = self.budget_for(record);
        match budget.check(record.cost_usd, record.usage.total()) {
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
                // `Usage` can arrive more than once after the cap is crossed.
                // The durable state is the idempotency guard: one transition,
                // one escalation, one kill.
                if record.state == AgentState::Stopped {
                    return;
                }
                warn!(agent = %record.name, cost = record.cost_usd, tokens = record.usage.total(), "budget cap hit — stopping agent");
                let detail = self.budget_stop_detail(record);
                let mut newly_stopped = false;
                let stopped = self.lock_registry().update(&record.name, |r| {
                    if r.state.is_live() {
                        newly_stopped = true;
                        r.state = AgentState::Stopped;
                        r.crashed = false;
                        r.result = Some(detail.clone());
                    }
                });
                if !newly_stopped
                    || !matches!(stopped, Ok(Some(ref r)) if r.state == AgentState::Stopped)
                {
                    return;
                }
                self.emit_obstacle_for_budget(record, "exceeded");
                self.note_budget_stop_floor(record);
                self.announce_budget_stop(record, &detail);
                let control = self.lock_controls().remove(&record.name);
                if let Some(control) = control {
                    tokio::spawn(async move {
                        let _ = control.kill().await;
                    });
                }
            }
        }
    }

    fn budget_stop_detail(&self, record: &AgentRecord) -> String {
        let budget = self.budget_for(record);
        format!(
            "budget stop: spent ${:.2} / ${:.2} cap; {} / {} token cap",
            record.cost_usd,
            budget.max_usd,
            record.usage.total(),
            budget.max_tokens
        )
    }

    fn announce_budget_stop(&self, record: &AgentRecord, detail: &str) {
        let notice = EscalationNotice::new(
            "placeholder",
            "budget-stop",
            Severity::Critical,
            record.repo_name.clone(),
            record.name.clone(),
            detail.to_string(),
        )
        .with_action(format!(
            "Review the cap and work on branch {}; explicitly `rk respawn {}` only if continuation is warranted",
            record.branch.as_deref().unwrap_or("-"),
            record.name
        ));
        if let Err(e) = self.announcer.announce(
            &self.space,
            &self.sinks.lock().unwrap_or_else(|p| p.into_inner()),
            crate::recovery::RecoveryAction {
                kind: "budget-stop".into(),
                instance: "supervisor".into(),
                notice,
            },
            crate::recovery::RateCap::unlimited(),
        ) {
            warn!(agent = %record.name, error = %e, "failed to announce budget stop");
        }
    }

    /// Returns true if this call newly marked the agent (first warning).
    fn mark_budget_warned(&self, name: &str) -> bool {
        match self.budget_warned.lock() {
            Ok(mut set) => set.insert(name.to_string()),
            Err(p) => p.into_inner().insert(name.to_string()),
        }
    }

    /// Snapshot the cost/usage rollup that just justified a budget hard-stop,
    /// so a terminal event arriving after the kill can't report a lower
    /// figure than what actually triggered it.
    fn note_budget_stop_floor(&self, record: &AgentRecord) {
        let floor = (record.cost_usd, record.usage);
        match self.budget_stop_floor.lock() {
            Ok(mut map) => map.insert(record.name.clone(), floor),
            Err(p) => p.into_inner().insert(record.name.clone(), floor),
        };
    }

    /// Consume (remove) the budget-stop floor for `name`, if one was set.
    fn take_budget_stop_floor(&self, name: &str) -> Option<(f64, TokenUsage)> {
        match self.budget_stop_floor.lock() {
            Ok(mut map) => map.remove(name),
            Err(p) => p.into_inner().remove(name),
        }
    }

    /// Raise `cost_usd`/`usage` to at least the budget-stop floor for `name`,
    /// if this generation was ever hard-stopped by the budget machinery.
    /// Applied at every terminal transition so whichever event reaches the
    /// record last — a harness `Completed` or a bare process `Exited` — can
    /// only ever push the recorded spend up to the true figure, never down.
    fn apply_budget_stop_floor(&self, name: &str, cost_usd: &mut f64, usage: &mut TokenUsage) {
        if let Some((floor_cost, floor_usage)) = self.take_budget_stop_floor(name) {
            if floor_cost > *cost_usd {
                *cost_usd = floor_cost;
            }
            if floor_usage.total() > usage.total() {
                *usage = floor_usage;
            }
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
        match self.refusing_resource(repo)? {
            Some(refusal) => Err(rk_core::Error::other(refusal.message(self.layout.home()))),
            None => Ok(()),
        }
    }

    /// Sample this castle's physical capacity and, if a floor is breached,
    /// make the refusal **loud** before returning it: an obstacle tuple for
    /// `rk inbox` and an escalation through the notification sinks.
    ///
    /// P3a (probe note O12): this is the one place a resource refusal is
    /// minted, so there is exactly one code path to audit for silence. Every
    /// caller — the authoritative guard in `spawn`, and the drain's
    /// pre-claim preflight — gets the announcement for free, which is the
    /// whole point: the probe's silent stall happened because the *preflight*
    /// path never reached the announcing code.
    ///
    /// Announcing from a preflight polled every drain cycle is safe precisely
    /// because of the rate cap: `RateCap::per_hour(1)` per resource kind, so a
    /// sustained breach announces once an hour rather than once a cycle. The
    /// cap holds-and-raises rather than silencing (see `recovery.rs`), which
    /// is the same "silence is earned later, not shipped now" stance
    /// `respawn_sweep` takes.
    fn refusing_resource(
        &self,
        repo: &str,
    ) -> rk_core::Result<Option<crate::machine::ResourceRefusal>> {
        let floors = self.machine_floors();
        if floors.disabled() {
            return Ok(None);
        }
        let signal = crate::machine::MachineSignal::sample(self.layout.home())?;
        let Some(refusal) = floors.evaluate(&signal) else {
            return Ok(None);
        };
        warn!(
            kind = refusal.kind.class(),
            detail = %refusal.detail,
            "machine floor breached — refusing spawn"
        );
        self.emit_resource_pressure_obstacle(repo, &refusal, &floors);
        let notice = EscalationNotice::new(
            "placeholder",
            refusal.kind.class(),
            Severity::Warn,
            repo.to_string(),
            self.layout.home().display().to_string(),
            refusal.message(self.layout.home()),
        )
        .with_action(refusal.action.clone());
        if let Err(e) = self.announcer.announce(
            &self.space,
            &self.sinks.lock().unwrap_or_else(|p| p.into_inner()),
            crate::recovery::RecoveryAction {
                // Keyed per resource kind so a sustained disk breach's rate cap
                // cannot swallow a distinct, newly-arrived load breach.
                kind: refusal.kind.class().into(),
                instance: "supervisor".into(),
                notice,
            },
            crate::recovery::RateCap::per_hour(1),
        ) {
            warn!(error = %e, "failed to announce resource refusal");
        }
        Ok(Some(refusal))
    }

    /// Read-only-ish preflight for the continuous-drain autoscaler: is the
    /// machine currently out of a resource? Lets drain skip *before* claiming a
    /// ticket, rather than claiming it and stranding it `in_progress` when the
    /// authoritative guard in `spawn` refuses (the generic error arm leaves the
    /// claim standing — one ticket lost per cycle, which is how O12's stall
    /// quietly ate the backlog).
    ///
    /// Deliberately NOT side-effect free, unlike
    /// [`would_exceed_budget`](Self::would_exceed_budget): a resource refusal
    /// must escalate wherever it is decided. The rate cap, not silence, is what
    /// keeps a per-cycle poll from spamming.
    pub fn would_refuse_for_resources(
        &self,
        repo: &str,
    ) -> Option<crate::machine::ResourceRefusal> {
        match self.refusing_resource(repo) {
            Ok(refusal) => refusal,
            // A sampling failure (e.g. statvfs on a vanished path) must not
            // wedge dispatch: the authoritative guard inside `spawn` re-runs
            // this and will surface the error there.
            Err(e) => {
                warn!(error = %e, "machine signal unavailable; leaving admission to spawn");
                None
            }
        }
    }

    /// Companion to [`emit_dispatch_obstacle`](Self::emit_dispatch_obstacle):
    /// same `Category::Obstacle` shape, surfaced by `rk inbox`, but for a
    /// physical-resource refusal rather than a budget one.
    ///
    /// Reports the WHOLE signal, not just the resource that tripped: the
    /// probe's post-mortem was slowed by having a disk figure with no
    /// contemporaneous load figure beside it.
    fn emit_resource_pressure_obstacle(
        &self,
        repo: &str,
        refusal: &crate::machine::ResourceRefusal,
        floors: &crate::machine::MachineFloors,
    ) {
        let signal = &refusal.signal;
        let tuple = Tuple::new(
            Category::Obstacle,
            repo.to_string(),
            refusal.kind.obstacle_identity().to_string(),
            self.castle.clone(),
            json!({
                "type": refusal.kind.obstacle_type(),
                "detail": refusal.detail,
                "action": refusal.action,
                "path": self.layout.home().display().to_string(),
                // Storage half of the signal. `available_bytes`/`floor_bytes`
                // keep their original names — `rk inbox` and the operator's
                // muscle memory both already know them.
                "available_bytes": signal.free_disk_bytes,
                "floor_bytes": floors.min_free_disk_gb.saturating_mul(crate::machine::BYTES_PER_GB),
                // CPU half.
                "load_1m": signal.load_1m,
                "load_per_cpu": signal.load_per_cpu(),
                "cpus": signal.cpus,
                "max_load_per_cpu": floors.max_load_per_cpu,
            }),
        );
        if let Err(e) = self.space.out(tuple.into_trail(DEFAULT_TRAIL_TTL)) {
            warn!(error = %e, "failed to emit resource pressure obstacle");
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
        // Unaffected by everything below: liveness evidence excuses SILENCE,
        // never cost.
        let dt_min = (now - st.last_observed).num_milliseconds() as f64 / 60_000.0;
        let burn = if dt_min > 0.0 {
            (record.cost_usd - st.last_cost_usd) / dt_min
        } else {
            0.0
        };
        st.last_cost_usd = record.cost_usd;
        st.last_observed = now;
        let running_away = cfg.burn_usd_per_min > 0.0 && burn >= cfg.burn_usd_per_min;

        // The silence bar is UNCHANGED from before this feature — evidence
        // gathered below can only EXCUSE a generation already past it, never
        // move the bar itself or flag one earlier.
        let idle_secs = (now - record.updated_at).num_seconds().max(0) as u64;
        let silent = cfg.stuck_after_secs > 0 && idle_secs >= cfg.stuck_after_secs;
        let evidence = silent.then(|| self.gather_liveness_evidence(record, now, cfg));
        let alive = evidence
            .as_ref()
            .is_some_and(LivenessEvidence::proves_alive);
        // An operator's own `rk progress` check-in is an auditable override,
        // not a required babysitting path: it is read here exactly like any
        // other excusing signal (never mandatory for a healthy check to
        // survive) and never touched by the event-pump seams above.
        let window = chrono::Duration::seconds(cfg.stuck_after_secs.max(1) as i64);
        let operator_overrode = silent
            && record
                .progress
                .as_ref()
                .is_some_and(|p| now - p.updated_at < window);
        let stuck = silent && !alive && !operator_overrode;

        // The stuck episode's ceiling is persisted on the record itself
        // (`AgentRecord::liveness::ceiling_started_at`), independent of this
        // in-memory `SweepState` — restart-safe by construction, since it is
        // read back from the very same `agents.json` a fresh daemon loads.
        // Kept deliberately separate from `st.flagged_at` (which still
        // governs ONLY the runaway axis below) so a stuck episode's clock can
        // never leak into, or be reset by, an unrelated burn-rate episode.
        let stuck_ceiling = self.update_stuck_ceiling(&record.name, stuck, now);

        if !stuck && !running_away {
            st.flagged_at = None;
            return SweepAction::None;
        }

        // Stuck takes precedence in the message; both post an obstacle whose
        // `type` a reactor #Trigger can match ("stuck" / "runaway").
        let (kind, detail): (&'static str, String) = if stuck {
            ("stuck", describe_stuck(idle_secs, evidence.as_ref()))
        } else {
            (
                "runaway",
                format!("sustained burn ${burn:.2}/min with no completion"),
            )
        };

        let flagged_since = if stuck {
            stuck_ceiling
        } else {
            match st.flagged_at {
                None => {
                    st.flagged_at = Some(now);
                    None
                }
                some => some,
            }
        };

        match flagged_since {
            None => SweepAction::Soft { kind, detail },
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

    /// Gather this generation's liveness evidence: whether its harness's own
    /// process still has a live verifier descendant underneath it (a `cargo
    /// test`/compiler its own tool-use launched, or an `rk verify` CLI call
    /// blocked on the daemon — see [`crate::workflow_exec::process_liveness`]),
    /// and whether its bounded output (assistant text, tool use, stderr) has
    /// genuinely advanced within the same window the silence bar itself
    /// uses. Only called once a generation is already silent past that bar
    /// (see [`decide_sweep`](Self::decide_sweep)) — this can only excuse it,
    /// never flag one earlier.
    fn gather_liveness_evidence(
        &self,
        record: &AgentRecord,
        now: DateTime<Utc>,
        cfg: &SupervisorConfig,
    ) -> LivenessEvidence {
        let session = self.lock_session_tokens().get(&record.name).copied();
        let session_matches = record.liveness.session == session;

        let process = record
            .pid
            .map(crate::workflow_exec::process_liveness)
            .unwrap_or(crate::workflow_exec::ProcessLiveness {
                child_alive: false,
                live_verifier_descendants: 0,
            });

        let window = chrono::Duration::seconds(cfg.stuck_after_secs.max(1) as i64);
        // A stale (respawned-over) session's stored evidence describes a
        // predecessor process, never proof of THIS generation's liveness.
        let output_progressed = session_matches
            && record
                .liveness
                .output_changed_at
                .is_some_and(|at| now - at < window);
        let reconnect_loop =
            session_matches && record.liveness.reconnect_events >= RECONNECT_LOOP_THRESHOLD;

        LivenessEvidence {
            child_alive: process.child_alive,
            live_verifier_descendants: process.live_verifier_descendants,
            output_progressed,
            reconnect_loop,
        }
    }

    /// Read-modify-write the persisted stuck-episode ceiling for `name`'s
    /// CURRENT session (a stale, respawned-over session's ceiling is
    /// discarded, never inherited — same rule as
    /// [`gather_liveness_evidence`](Self::gather_liveness_evidence)). Returns
    /// what the ceiling held BEFORE this write: `None` means this sweep is
    /// the first to observe the episode (matches `SweepAction::Soft`'s
    /// "just flagged" moment, whether that is because it is genuinely new or
    /// because a fresh daemon generation is seeing it for the first time);
    /// `Some(started)` is the elapsed-time anchor `kill_grace_secs` measures
    /// from, unchanged by a restart in between.
    fn update_stuck_ceiling(
        &self,
        name: &str,
        stuck: bool,
        now: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        let session = self.lock_session_tokens().get(name).copied();
        let mut previous = None;
        let _ = self.lock_registry().update_quiet(name, |r| {
            if r.liveness.session != session {
                r.liveness = crate::agents::LivenessObservation {
                    session,
                    ..Default::default()
                };
            }
            previous = r.liveness.ceiling_started_at;
            if stuck {
                if r.liveness.ceiling_started_at.is_none() {
                    r.liveness.ceiling_started_at = Some(now);
                    return true;
                }
                false
            } else if r.liveness.ceiling_started_at.is_some() {
                r.liveness.ceiling_started_at = None;
                true
            } else {
                false
            }
        });
        previous
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
    ///
    /// Every fired (or held) respawn announces through `sinks` via the
    /// castle-wide [`recovery::RecoveryAnnouncer`](crate::recovery), gated by
    /// `cfg.respawn_rate_cap_per_hour` — the fleet-wide counterpart to the
    /// per-agent `respawn_max_attempts` bound above. A respawn past the cap is
    /// HELD (not launched) and escalated at raised severity instead; it is
    /// retried on a later tick once the rolling window has room again.
    pub fn respawn_sweep(self: &Arc<Self>, cfg: &SupervisorConfig, sinks: &SinkRegistry) {
        if !cfg.respawn_enabled || cfg.respawn_max_attempts == 0 {
            return;
        }
        let now = Utc::now();
        // Candidates: crashed but not dismissed and not cleanly completed. A
        // `Completed` rat ran `rk done` — a clean finish we must not relaunch.
        // `transport_outage.is_none()` excludes a generation mid pre-work
        // transport-outage episode: it is ALSO `is_auto_respawn_candidate`
        // (Failed/Orphaned), but it is never a `RespawnState` entry, so
        // `decide_respawn` would see `None` and fire an immediate
        // `RespawnDecision::Respawn` — bypassing `transport_retry_sweep`'s
        // backoff, jitter, and castle-wide circuit breaker entirely.
        // `transport_retry_sweep` (which DOES gate on the breaker) owns these
        // records exclusively. `recovery.is_some()` excludes a generation
        // parked with a post-commit `RecoveryRecord` the same way: it is a
        // deliberate continuation decision (`continue_recovery`/
        // `abandon_recovery`), never a bare relaunch, and an abandoned one
        // must stay excluded forever so its WIP slot is never silently
        // reclaimed (TKT-01M0HNDJ7AS9F1A3W22FRCC63N).
        let candidates: Vec<AgentRecord> = self
            .lock_registry()
            .list()
            .into_iter()
            .filter(|r| {
                is_auto_respawn_candidate(r) && r.transport_outage.is_none() && r.recovery.is_none()
            })
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
                    let notice = EscalationNotice::new(
                        "placeholder",
                        "respawn",
                        Severity::Warn,
                        record.repo_name.clone(),
                        record.name.clone(),
                        format!(
                            "self-healing sweep auto-respawning crashed agent {} (task: {})",
                            record.name,
                            record.task.as_deref().unwrap_or("-")
                        ),
                    );
                    let outcome = self.recovery_announcer.announce(
                        &self.space,
                        sinks,
                        crate::recovery::RecoveryAction {
                            kind: "respawn".into(),
                            instance: "supervisor".into(),
                            notice,
                        },
                        crate::recovery::RateCap::per_hour(cfg.respawn_rate_cap_per_hour),
                    );
                    match outcome {
                        Ok(outcome) if outcome.held() => {
                            warn!(
                                agent = %record.name,
                                cap = cfg.respawn_rate_cap_per_hour,
                                "auto-respawn HELD: castle-wide respawn rate cap hit"
                            );
                        }
                        Ok(_) => {
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
                        Err(e) => {
                            warn!(agent = %record.name, error = %e, "failed to announce auto-respawn; skipping this tick");
                        }
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
    /// Not delegated to [`Repo::branch_merged_or_gone`] (which must stay
    /// FF-tolerant for its other callers): this call site's target only ever
    /// advances via `rk`'s own `--no-ff` `merge_branch`, so the strict check
    /// is safe here specifically.
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

    /// Detect a transport outage discovered AFTER a generation had already
    /// committed work (TKT-01M0HNDJ7AS9F1A3W22FRCC63N) — as opposed to
    /// `record_transport_outage`, which only ever fires before a harness's
    /// `Started` handshake. Builds and persists a `RecoveryRecord` so an
    /// operator/policy continuation decision (`continue_recovery`/
    /// `abandon_recovery`) can be made — and safely retried — arbitrarily
    /// long after this detecting daemon process is gone.
    ///
    /// A no-op unless ALL of: no pre-work outage is already in progress (that
    /// path owns the record instead), the generation has a branch with at
    /// least one commit past its fork point (this is what makes it
    /// "post-commit" rather than an ordinary crashed launch), and the
    /// generation's stderr classifies as a known transport signal. Anything
    /// else falls through to the ordinary crash/respawn handling the `Exited`
    /// arm already does, unchanged.
    fn detect_post_commit_outage(self: &Arc<Self>, name: &str, record: &AgentRecord) {
        if record.transport_outage.is_some() || record.recovery.is_some() {
            return;
        }
        let (Some(branch), Some(worktree), Some(fork_point)) = (
            record.branch.as_deref(),
            record.worktree.as_deref(),
            record.fork_point.as_deref(),
        ) else {
            return;
        };
        let Some(outcome) = record.stderr_tail.as_deref().and_then(|tail| {
            let lines: Vec<String> = tail.lines().map(str::to_string).collect();
            rk_harness::transport::classify(&record.harness, &lines)
        }) else {
            return;
        };
        let Ok(repo) = Repo::discover(worktree) else {
            return;
        };
        if !repo.branch_has_commits_since(branch, fork_point) {
            return; // no committed work yet — ordinary crash handling applies
        }
        let Ok(head) = repo.rev_parse("HEAD") else {
            return;
        };
        let budget = self.budget_for(record);
        let budget_remaining_usd =
            (budget.max_usd > 0.0).then(|| (budget.max_usd - record.cost_usd).max(0.0));
        let recovery = crate::agents::RecoveryRecord {
            ticket: record.task.clone(),
            branch: branch.to_string(),
            head,
            session_id: record.session_id.clone(),
            spawn: record.spawn_id(),
            liveness: record.liveness.clone(),
            budget_remaining_usd,
            provider: record.harness.clone(),
            class: outcome.class,
            evidence: outcome.evidence.clone(),
            detected_at: Utc::now(),
            ack: None,
        };
        let _ = self.lock_registry().update(name, |r| {
            r.recovery = Some(recovery.clone());
        });
        info!(
            agent = name,
            class = ?outcome.class,
            branch,
            "post-commit transport outage detected; durable recovery record written"
        );
        self.announce_pending_recovery(name, record, &recovery);
    }

    /// Surface a freshly-parked [`RecoveryRecord`] the same way every other
    /// automated recovery source in this file already does — through
    /// [`crate::recovery::RecoveryAnnouncer`], which writes the durable
    /// `recovery_action` event that `rk inbox` renders as a `recovery-action`
    /// row and fans it out through the configured `[[notify.sinks]]`.
    ///
    /// Without this an operator could only discover a parked generation by
    /// reading raw `agents.json`/`agent.status` JSON for every agent, which is
    /// exactly the polling this seam exists to remove. The suggested action
    /// names the two continuation commands, because the inbox row's own action
    /// is always `rk inbox ack <id>` (see `inbox::recovery_action_rows`) — the
    /// row body is the only place the real remedy can live.
    ///
    /// Announce-only: unlike the auto-respawn and transport-retry sites, a
    /// rate-cap hold does NOT suppress anything here. Detection already
    /// happened and the record is already persisted; there is no side effect
    /// left to withhold, and dropping the record because the castle is noisy
    /// would lose committed work. A held announce is logged and the parked
    /// record still stands, ready for `continue_recovery`/`abandon_recovery`.
    fn announce_pending_recovery(
        &self,
        name: &str,
        record: &AgentRecord,
        recovery: &crate::agents::RecoveryRecord,
    ) {
        let short_head: String = recovery.head.chars().take(12).collect();
        let notice = EscalationNotice::new(
            "placeholder",
            "post_commit_recovery",
            Severity::Warn,
            record.repo_name.clone(),
            name.to_string(),
            format!(
                "{name} lost its harness transport AFTER committing work on {} (head {}) — \
                 the generation is parked awaiting a continuation decision and will NOT be \
                 auto-respawned. Resume it with `rk continue-recovery {name}` (add \
                 `--harness <kind>` to route to a configured alternate harness instead), or \
                 `rk abandon-recovery {name}` to leave it terminal.",
                recovery.branch, short_head,
            ),
        )
        .with_action(format!("rk continue-recovery {name}"))
        .with_ref("agent", name)
        .with_ref("task", recovery.ticket.clone().unwrap_or_default())
        .with_ref("branch", recovery.branch.clone())
        .with_ref("head", recovery.head.clone())
        .with_ref("provider", recovery.provider.clone())
        .with_ref("class", format!("{:?}", recovery.class))
        .with_ref("evidence", recovery.evidence.clone());
        let announced = self.recovery_announcer.announce(
            &self.space,
            &self.sinks.lock().unwrap_or_else(|p| p.into_inner()),
            crate::recovery::RecoveryAction {
                kind: "post_commit_recovery".into(),
                instance: "supervisor".into(),
                notice,
            },
            // 20/hour, matching the kill-process-group site: generous enough
            // that a genuine multi-agent outage episode is fully visible,
            // tight enough that a castle-wide provider failure cannot turn
            // this into a notification storm.
            crate::recovery::RateCap::per_hour(20),
        );
        match announced {
            Ok(outcome) if outcome.held() => warn!(
                agent = name,
                "post-commit recovery announce HELD by the rate cap; the durable recovery \
                 record still stands — find it with `rk status` or the held escalation itself"
            ),
            Ok(_) => {}
            Err(e) => warn!(
                agent = name,
                error = %e,
                "failed to announce a parked post-commit recovery; the durable record still stands"
            ),
        }
    }

    /// Resume (or route to a configured alternate harness for) a generation
    /// parked with a post-commit `RecoveryRecord`. `action_id` is an opaque
    /// idempotency key: calling this again before the FIRST call's effects
    /// are acknowledged is safe to retry (replay before acknowledgement);
    /// the SAME key after acknowledgement replays the same recorded outcome
    /// instead of acting twice; a DIFFERENT key after acknowledgement is
    /// refused — acknowledgement makes continuation at-most-once.
    ///
    /// `target_harness = None` resumes the SAME provider/session in the same
    /// worktree; `Some(harness)` routes to a configured alternate harness in
    /// the same worktree instead (no session to resume — a fresh turn
    /// against the same preserved head). Either way this refuses to launch
    /// a second live owner: a record that is already live, or whose
    /// generation has moved on since the recovery record was minted, errors
    /// instead of double-spawning.
    pub fn continue_recovery(
        self: &Arc<Self>,
        name: &str,
        action_id: &str,
        target_harness: Option<&str>,
    ) -> rk_core::Result<crate::agents::RecoveryOutcome> {
        let record = self
            .lock_registry()
            .get(name)
            .cloned()
            .ok_or_else(|| rk_core::Error::other(format!("no such agent: {name}")))?;
        let recovery = match record.recovery.clone() {
            Some(recovery) => recovery,
            None => return Self::replay_receipt_or_refuse(name, action_id, &record),
        };
        if let Some(ack) = &recovery.ack {
            return Self::replay_or_refuse(name, action_id, ack);
        }
        if recovery.stale(record.spawn_id()) {
            return Err(rk_core::Error::other(format!(
                "{name}'s recovery record is stale: a newer generation is active"
            )));
        }
        // No second live owner: a process that already resumed (or never
        // left) this name must refuse a concurrent continuation instead of
        // double-launching.
        if record.state.is_live() || record.pid.is_some() {
            return Err(rk_core::Error::other(format!(
                "{name} already has a live owner — refusing to continue"
            )));
        }
        validate_role(&record.role)?;
        let (Some(worktree), Some(task)) = (record.worktree.clone(), record.task.clone()) else {
            return Err(rk_core::Error::other(format!("{name} lacks worktree/task")));
        };

        let harness_kind = target_harness.unwrap_or(record.harness.as_str());
        let same_provider = harness_kind == record.harness;
        let harness = make_harness(harness_kind)?;
        // A session from one provider can never resume under a different
        // one — continuing under an alternate harness is always a fresh
        // turn against the preserved head, never a resume.
        let resume = if same_provider && harness.caps().resume {
            recovery.session_id.clone()
        } else {
            None
        };
        let repo = Repo::discover(&record.repo_root)?;
        let instruction_base = self.instruction_base(&record.role, &record.target_branch, &repo);
        let env = self.agent_env(
            &record.name,
            &record.role,
            &record.repo_name,
            &task,
            record.branch.as_deref(),
            &instruction_base,
            &worktree,
            record.workflow_instance.as_deref(),
            record.review.as_ref(),
        );
        let prime_ctx = PrimeContext {
            agent: record.name.clone(),
            repo: record.repo_name.clone(),
            task: record.task.clone(),
            branch: record.branch.clone(),
            base: Some(instruction_base),
            review: record.review.clone(),
            parent: record.parent.clone(),
            facts: self.scan_facts(&record.repo_name),
            conventions: self.scan_conventions(&record.repo_name),
            verification_checks: self.scan_verification_checks(&worktree),
            harness_terminal_completion: uses_harness_terminal_completion(
                &record.role,
                harness_kind,
            ),
        };
        let resume_prompt = if same_provider {
            format!(
                "Resuming task {task} after a transport outage that interrupted you AFTER \
                 work was committed (branch {}, head {}). Check `git log` and `git status` in \
                 your worktree to see exactly what landed, re-run any check that was \
                 interrupted mid-flight, then continue. Finish with `rk done` as usual.",
                recovery.branch, recovery.head,
            )
        } else {
            format!(
                "You are continuing task {task} in the same worktree after a transport \
                 outage took down a prior harness AFTER work was committed (branch {}, head \
                 {}). That prior session cannot be resumed from here — treat this as a fresh \
                 turn: check `git log` and `git status` to see exactly what already landed, \
                 re-run any check that was interrupted mid-flight, then continue. Finish with \
                 `rk done` as usual.",
                recovery.branch, recovery.head,
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
        let session = harness.launch(&spec)?;

        // The continuation reuses this generation's identity — same
        // convention an ordinary respawn already follows (the record, and
        // its `created_at`/`spawn`, are reused rather than reallocated) —
        // which is exactly the "preserve retry generation" continuity the
        // ticket asks for.
        let new_spawn = record.spawn_id();
        let outcome = if same_provider {
            crate::agents::RecoveryOutcome::ResumedSameProvider { new_spawn }
        } else {
            crate::agents::RecoveryOutcome::ContinuedAlternateProvider {
                harness: harness_kind.to_string(),
                new_spawn,
            }
        };
        let action_id_owned = action_id.to_string();
        let outcome_for_ack = outcome.clone();
        let updated = self
            .lock_registry()
            .update(name, |r| {
                r.harness = harness_kind.to_string();
                r.state = AgentState::Running;
                r.pid = session.pid;
                r.result = None;
                r.crashed = false;
                r.stderr_tail = None;
                if let Some(rec) = r.recovery.as_mut() {
                    rec.ack = Some(crate::agents::RecoveryAck {
                        action_id: action_id_owned,
                        outcome: outcome_for_ack,
                        acknowledged_at: Utc::now(),
                    });
                }
            })?
            .ok_or_else(|| rk_core::Error::other("record vanished"))?;

        let session_token = self.track_session(name, session.control.clone());
        self.forget_completion(name);

        let supervisor = Arc::clone(self);
        let owned = name.to_string();
        let mut events = session.events;
        let generation = updated.created_at;
        let spawn = updated.spawn_id();
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                supervisor.handle_event(&owned, generation, spawn, session_token, event);
            }
        });

        Ok(outcome)
    }

    /// Explicitly decline to continue a parked post-commit recovery — a
    /// non-retryable class, an exhausted ceiling, or an operator's own
    /// choice. Same `action_id` at-most-once contract as
    /// [`continue_recovery`](Self::continue_recovery). The generation stays
    /// terminal (`Failed`, no live process): `respawn_sweep` excludes any
    /// record with `recovery.is_some()` (same exclusion pattern as
    /// `transport_outage`), so its WIP slot — already released the moment
    /// `Exited` cleared `pid` — is never silently reclaimed by a later
    /// auto-respawn.
    pub fn abandon_recovery(
        self: &Arc<Self>,
        name: &str,
        action_id: &str,
    ) -> rk_core::Result<crate::agents::RecoveryOutcome> {
        let record = self
            .lock_registry()
            .get(name)
            .cloned()
            .ok_or_else(|| rk_core::Error::other(format!("no such agent: {name}")))?;
        let recovery = match record.recovery.clone() {
            Some(recovery) => recovery,
            None => return Self::replay_receipt_or_refuse(name, action_id, &record),
        };
        if let Some(ack) = &recovery.ack {
            return Self::replay_or_refuse(name, action_id, ack);
        }
        let action_id_owned = action_id.to_string();
        self.lock_registry().update(name, |r| {
            if let Some(rec) = r.recovery.as_mut() {
                rec.ack = Some(crate::agents::RecoveryAck {
                    action_id: action_id_owned,
                    outcome: crate::agents::RecoveryOutcome::Abandoned,
                    acknowledged_at: Utc::now(),
                });
            }
        })?;
        Ok(crate::agents::RecoveryOutcome::Abandoned)
    }

    /// Shared acknowledged-ack handling for `continue_recovery`/
    /// `abandon_recovery`: the same `action_id` replays the recorded
    /// outcome (duplicate-call safety), a different one is refused (the
    /// at-most-once half of the contract).
    fn replay_or_refuse(
        name: &str,
        action_id: &str,
        ack: &crate::agents::RecoveryAck,
    ) -> rk_core::Result<crate::agents::RecoveryOutcome> {
        if ack.action_id == action_id {
            Ok(ack.outcome.clone())
        } else {
            Err(rk_core::Error::other(format!(
                "{name}'s recovery was already acknowledged by a different action ({} != \
                 {action_id})",
                ack.action_id
            )))
        }
    }

    /// Fallback for `continue_recovery`/`abandon_recovery` when `recovery`
    /// has already been cleared by `Started` proof-of-life: consult the
    /// durable [`crate::agents::RecoveryReceipt`] tombstone
    /// (`AgentRecord::recovery_receipt`) left behind for this exact
    /// generation instead of reporting "no pending recovery" outright. A
    /// receipt from a DIFFERENT generation (fenced by `spawn`, same
    /// discipline as `RecoveryRecord::stale`) is not this generation's to
    /// replay — reported the same as no receipt at all.
    fn replay_receipt_or_refuse(
        name: &str,
        action_id: &str,
        record: &AgentRecord,
    ) -> rk_core::Result<crate::agents::RecoveryOutcome> {
        match &record.recovery_receipt {
            Some(receipt) if receipt.spawn == record.spawn_id() => {
                Self::replay_or_refuse(name, action_id, &receipt.ack)
            }
            _ => Err(rk_core::Error::other(format!(
                "{name} has no pending recovery"
            ))),
        }
    }

    /// Record one pre-work harness transport failure: bump (or start) this
    /// generation's durable retry-schedule episode on its `AgentRecord`, and
    /// feed the castle-wide per-provider circuit breaker. Called from
    /// `handle_event` the moment a `TransportFailure` event arrives — the
    /// `Exited` event that always follows it drives the ordinary
    /// live->Failed state transition (and so the ordinary WIP release)
    /// exactly as any other crashed launch does; this only adds the typed,
    /// durable retry bookkeeping on top.
    fn record_transport_outage(&self, name: &str, outcome: &rk_harness::TransportOutcome) {
        let now = Utc::now();
        let _ = self.lock_registry().update(name, |r| {
            let attempts = r
                .transport_outage
                .as_ref()
                .map(|o| o.attempts + 1)
                .unwrap_or(1);
            r.transport_outage = Some(crate::agents::TransportOutageState {
                provider: outcome.provider.clone(),
                class: outcome.class,
                retryable: outcome.retryable,
                attempts,
                last_failure_at: now,
                evidence: outcome.evidence.clone(),
                ceiling_hit: false,
                circuit_refused: false,
            });
        });
        // Cheap, safe default: a breaker that never received the threshold
        // config yet (bare/test supervisor) still counts failures — it just
        // never trips, since `record_failure`'s own zero-threshold guard
        // matches `TransportBreakers::is_open`'s zero-cooldown guard. Real
        // config is applied by `Daemon::new` before any traffic flows.
        self.lock_transport_breakers().record_failure(
            &outcome.provider,
            self.transport_breaker_trip_threshold
                .load(Ordering::Relaxed) as u32,
            now,
        );
    }

    /// What the pre-work transport-outage retry sweep decided to do about
    /// one agent whose generation is mid-episode. Mirrors
    /// [`RespawnDecision`], with the addition of the castle-wide breaker.
    fn decide_transport_retry(
        &self,
        record: &AgentRecord,
        now: DateTime<Utc>,
        cfg: &SupervisorConfig,
    ) -> RespawnDecision {
        let Some(outage) = &record.transport_outage else {
            return RespawnDecision::Wait;
        };
        if outage.ceiling_hit {
            return RespawnDecision::Wait;
        }
        if !outage.retryable || outage.attempts >= cfg.transport_retry_max_attempts {
            return RespawnDecision::Escalate;
        }
        if self.lock_transport_breakers().is_open(
            &outage.provider,
            now,
            cfg.transport_breaker_cooldown_secs,
        ) {
            return RespawnDecision::Wait;
        }
        let backoff = cfg
            .transport_retry_backoff_secs
            .saturating_mul(1u64 << (outage.attempts.saturating_sub(1)).min(16));
        let jitter = transport_retry_jitter_secs(
            &record.name,
            outage.attempts,
            cfg.transport_retry_jitter_secs,
        );
        let waited = (now - outage.last_failure_at).num_seconds().max(0) as u64;
        if waited >= backoff.saturating_add(jitter) {
            RespawnDecision::Respawn
        } else {
            RespawnDecision::Wait
        }
    }

    /// Periodic pass over agents mid pre-work-transport-outage episode:
    /// bounded, jittered retry; castle-wide circuit-breaker refusal; ceiling
    /// escalation. Rides the same tick as [`Self::respawn_sweep`] but is
    /// entirely independent of it — a `transport_outage` record is never a
    /// `RespawnState` entry, so the two sweeps never double-count or
    /// double-launch the same generation.
    ///
    /// Restart-safe by construction: every input this reads (`transport_outage`
    /// on the record, the durable breaker file) survives a daemon restart, so
    /// resuming after one continues the exact same schedule — same attempt
    /// count, same backoff clock — rather than granting a fresh budget or
    /// relaunching a generation that already exhausted its ceiling.
    pub fn transport_retry_sweep(self: &Arc<Self>, cfg: &SupervisorConfig, sinks: &SinkRegistry) {
        if cfg.transport_retry_max_attempts == 0 {
            return;
        }
        let now = Utc::now();
        let candidates: Vec<AgentRecord> = self
            .lock_registry()
            .list()
            .into_iter()
            .filter(|r| r.transport_outage.is_some() && is_auto_respawn_candidate(r))
            .cloned()
            .collect();

        for record in &candidates {
            if self.branch_already_merged(record) {
                continue;
            }
            match self.decide_transport_retry(record, now, cfg) {
                RespawnDecision::Wait => {}
                RespawnDecision::Respawn => {
                    if self.would_exceed_budget(&record.repo_name) {
                        warn!(agent = %record.name, "skipping transport-outage retry: over budget cap");
                        continue;
                    }
                    let notice = EscalationNotice::new(
                        "placeholder",
                        "transport_retry",
                        Severity::Warn,
                        record.repo_name.clone(),
                        record.name.clone(),
                        format!(
                            "pre-work transport-outage sweep retrying {} (task: {})",
                            record.name,
                            record.task.as_deref().unwrap_or("-")
                        ),
                    );
                    let outcome = self.recovery_announcer.announce(
                        &self.space,
                        sinks,
                        crate::recovery::RecoveryAction {
                            kind: "transport_retry".into(),
                            instance: "supervisor".into(),
                            notice,
                        },
                        crate::recovery::RateCap::per_hour(cfg.respawn_rate_cap_per_hour),
                    );
                    match outcome {
                        Ok(outcome) if outcome.held() => {
                            warn!(
                                agent = %record.name,
                                "transport-outage retry HELD: castle-wide respawn rate cap hit"
                            );
                        }
                        Ok(_) => {
                            if let Err(e) = self.respawn(&record.name) {
                                warn!(agent = %record.name, error = %e, "transport-outage retry failed");
                            }
                        }
                        Err(e) => {
                            warn!(agent = %record.name, error = %e, "failed to announce transport-outage retry; skipping this tick");
                        }
                    }
                }
                RespawnDecision::Escalate => {
                    self.escalate_transport_outage(record, cfg);
                }
            }
        }
    }

    /// Escalate an exhausted (or non-retryable) transport-outage episode to
    /// a human: emit a `need` and mark it escalated so this fires exactly
    /// once. The record stays `Failed` (or `Orphaned`) — never respawned
    /// again — which is what releases its WIP slot for good; nothing further
    /// is needed here for that.
    fn escalate_transport_outage(&self, record: &AgentRecord, cfg: &SupervisorConfig) {
        let _ = self.lock_registry().update(&record.name, |r| {
            if let Some(outage) = &mut r.transport_outage {
                outage.ceiling_hit = true;
            }
        });
        let Some(outage) = &record.transport_outage else {
            return;
        };
        warn!(
            agent = %record.name,
            provider = %outage.provider,
            class = ?outage.class,
            attempts = outage.attempts,
            "pre-work transport outage exhausted its retry ceiling — escalating a need for a human"
        );
        let tuple = Tuple::new(
            Category::Need,
            record.repo_name.clone(),
            record.name.clone(),
            self.castle.clone(),
            json!({
                "type": "transport_outage_exhausted",
                "agent": record.name,
                "task": record.task,
                "provider": outage.provider,
                "class": outage.class,
                "retryable": outage.retryable,
                "attempts": outage.attempts,
                "max_attempts": cfg.transport_retry_max_attempts,
                "evidence": outage.evidence,
                "text": format!(
                    "agent {} could not reach its {} harness ({:?} transport failure) after {} attempt(s); \
                     needs a human — investigate then `rk respawn {}`",
                    record.name, outage.provider, outage.class, outage.attempts, record.name
                ),
            }),
        );
        if let Err(e) = self.space.out(tuple.into_trail(DEFAULT_TRAIL_TTL)) {
            warn!(error = %e, "failed to emit transport-outage-exhausted need");
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
        spawn: Option<rk_core::id::SpawnId>,
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
        let declared_done = harness_terminal || self.declared_done(name, generation, spawn);
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
    /// Generation-identity migration (C1, docs/2026-08-17-tkt-c1-generation-
    /// identity.md): `rk done` now stamps `spawn` into the `task_done` payload
    /// (C2/C6) whenever this generation was minted one, so a record carrying a
    /// real `SpawnId` keys the read on [`Pattern::for_spawn`] — an equality
    /// predicate no namesake can satisfy, no floor required. Falls back to the
    /// name+floor predicate for a record with no minted id (unreachable, or
    /// written before this migration).
    ///
    /// The durable `task_done` tuple this generation wrote via `rk done`, if
    /// any — the shared lookup behind [`Self::declared_done`] (bool) and
    /// [`Self::reconcile_task_done`] (needs the tuple itself, for its
    /// `summary` payload). Same generation-scoping as `declared_done`: keyed
    /// on `spawn` when minted, else the name+floor fallback.
    fn find_task_done(
        &self,
        name: &str,
        generation: DateTime<Utc>,
        spawn: Option<rk_core::id::SpawnId>,
    ) -> rk_core::Result<Option<Tuple>> {
        let pattern = match spawn {
            Some(spawn) => Pattern::for_spawn(Category::Event, "task_done", spawn),
            None => Pattern::for_agent_since(Category::Event, "task_done", name, generation),
        };
        Ok(self.space.scan(&pattern)?.into_iter().next())
    }

    /// Fails OPEN — an unreadable space means "publish", which is the behaviour
    /// that predates this gate. Withholding on a storage error would strand
    /// every workflow waiting on the agent until its step timeout.
    fn declared_done(
        &self,
        name: &str,
        generation: DateTime<Utc>,
        spawn: Option<rk_core::id::SpawnId>,
    ) -> bool {
        match self.find_task_done(name, generation, spawn) {
            Ok(found) => {
                let found = found.is_some();
                // Deliberately logged on every call, not just the negative
                // case: TKT-01M0BWWY15SH2KCQ99WKPGN9N7 saw this scan come back
                // empty for a generation whose `rk_done` had, by construction,
                // already round-tripped the daemon (the fixture's `space.out`
                // RPC cannot return before the write lands, and the harness
                // script cannot print its result line before that RPC
                // returns) — so a genuine miss here is either a spawn/pattern
                // mismatch or a store inconsistency, and the only way to tell
                // which is to see `spawn` and `found` together at the moment
                // it happened, not reconstruct it after the fact.
                debug!(
                    agent = name,
                    spawn = spawn.map(|s| s.to_string()),
                    found,
                    "declared_done scan"
                );
                found
            }
            Err(e) => {
                warn!(error = %e, agent = name, "task_done lookup failed; publishing the turn result anyway");
                true
            }
        }
    }

    /// Live + restart-safe reconciliation of `task_done` against terminal
    /// state — the fix for the 2026-08-21 Cinder-11 incident
    /// (TKT-01M0J5KT4TCH03W48MR9T7EJ27): a harness reports its own turn
    /// completion asynchronously (`HarnessEvent::Completed`), and if a
    /// concurrent budget/stuck/runaway sweep kills the process first, that
    /// event can simply never arrive — the generation's `rk done` still
    /// durably wrote its `task_done` tuple, but for a headless (non-attach)
    /// generation nothing was ever independently listening for it. Before
    /// this, only [`Self::flush_withheld_completion`] reacted to a process
    /// death, and only for a generation that had an EARLIER withheld turn to
    /// flush — a short generation that finishes on its first turn has none,
    /// so its already-accepted `task_done` was simply orphaned.
    ///
    /// Scans every non-terminal-in-the-good-sense record (excludes
    /// `Completed`, whose work here is already done, and `Dismissed`, whose
    /// completion bookkeeping was deliberately forgotten by
    /// [`Self::forget_completion`] and must not be resurrected) for a durable
    /// `task_done`. When one is found:
    ///
    ///  - **Still eligible** (`Spawning`/`Running`/`Paused`/`Orphaned` — the
    ///    last covers a daemon restart landing between the `task_done` write
    ///    and this reconcile pass, since [`crate::agents::Registry::orphan_live_agents`]
    ///    converts every live record to `Orphaned` at startup): CASes the
    ///    record to `Completed` under `lock_registry`'s mutex — the SAME lock
    ///    [`Self::enforce_budget`]'s hard-stop CAS uses — so whichever of the
    ///    two actually runs its `Registry::update` closure first durably
    ///    decides the outcome; there is no timing window to race, only lock
    ///    order. Routed exactly once via [`Self::claim_completion`]'s
    ///    existing dedup, shared with the harness-event path, so a
    ///    `HarnessEvent::Completed` racing this reconcile pass for the same
    ///    generation still only ever publishes once.
    ///  - **Already terminal for some other reason** (`Stopped`, `Failed`):
    ///    the stop (or crash) won durably first. Retained as evidence via
    ///    [`Self::retain_late_task_done_evidence`] instead of publishing —
    ///    the terminal state itself is never mutated, matching the
    ///    already-shipped early return in the harness-event path
    ///    (`handle_event`'s `pre.state == AgentState::Stopped` check) that a
    ///    deliberate stop wins races with a final harness event.
    ///
    /// Restart-safe by construction: every check here re-derives its answer
    /// from durable state (the registry snapshot on disk, the `task_done`
    /// tuple in the space) rather than in-memory bookkeeping a crash could
    /// lose, and every write (the `Completed` CAS, the evidence tuple) is
    /// itself durable and idempotent — [`Self::claim_completion`] refuses a
    /// second publish for the same generation, and
    /// [`Self::retain_late_task_done_evidence`] scans for its own prior
    /// artifact before writing another — so a daemon killed mid-pass and
    /// restarted just re-derives the same outcome on its next tick. Meant to
    /// run on the same event-feed + interval cadence as
    /// [`crate::landing::Landing::reconcile_late_review_evidence`] — see that
    /// sibling's doc comment for why (the same restart-safety argument for
    /// the same class of race: a late verdict there, a late `task_done`
    /// here).
    pub(crate) async fn reconcile_task_done(&self) -> rk_core::Result<usize> {
        let mut settled = 0;
        for record in self.list() {
            if matches!(record.state, AgentState::Completed | AgentState::Dismissed) {
                continue;
            }
            let Some(task_done) =
                self.find_task_done(&record.name, record.created_at, record.spawn)?
            else {
                continue;
            };
            let claim =
                self.claim_completion(&record.name, record.created_at, record.spawn, false, false);
            if !claim.publish {
                // Already routed — either the harness's own `Completed`
                // event won the race, or an earlier reconcile pass already
                // settled (or fenced) this generation.
                continue;
            }
            let diff = self.diff_summary_for(&record.name);
            let summary = task_done
                .payload
                .get("summary")
                .and_then(Value::as_str)
                .map(String::from);
            // The claim is spent as of the call above: whatever this CAS
            // decides, no other path will ever get to publish (or fence) a
            // completion for this generation again. See `crate::fault` for
            // why a barrier and not a sleep — a daemon killed here, before
            // the CAS lands, must converge to the same outcome on restart.
            crate::fault::barrier(&self.layout, BARRIER_TASK_DONE_PRE_ROUTE).await;
            let updated = self.lock_registry().update(&record.name, |r| {
                if r.state.is_live() || r.state == AgentState::Orphaned {
                    r.state = AgentState::Completed;
                    if let Some(s) = &summary {
                        r.result = Some(s.clone());
                    }
                }
            });
            match updated {
                Ok(Some(r)) if r.state == AgentState::Completed => {
                    info!(agent = %r.name, "task_done reconciled: generation completed");
                    self.route_completion(&r, false, claim.declared_done, diff);
                    crate::fault::barrier(&self.layout, BARRIER_TASK_DONE_POST_ROUTE).await;
                    settled += 1;
                }
                Ok(Some(r)) => {
                    // Lost the CAS: something else (a budget/stuck/runaway
                    // hard stop, or a plain crash) durably terminalized this
                    // generation first. The claim above is already spent, so
                    // this is the ONLY chance to record that the agent did,
                    // in fact, finish — retain it as evidence rather than
                    // silently dropping it.
                    if self
                        .retain_late_task_done_evidence(&r, &task_done)?
                        .is_some()
                    {
                        settled += 1;
                    }
                }
                Ok(None) | Err(_) => {}
            }
        }
        Ok(settled)
    }

    /// Whether a [`LATE_TASK_DONE_EVIDENCE_IDENTITY`] artifact already exists
    /// for this exact generation, so a repeat reconcile pass (the periodic
    /// tick, a restart) never duplicates it.
    fn late_task_done_evidence_exists(
        &self,
        repo_name: &str,
        name: &str,
        generation: DateTime<Utc>,
    ) -> rk_core::Result<bool> {
        let pattern = Pattern::category(Category::Artifact)
            .identity(LATE_TASK_DONE_EVIDENCE_IDENTITY)
            .scope(repo_name);
        let generation = generation.to_rfc3339();
        Ok(self.space.scan(&pattern)?.into_iter().any(|t| {
            t.payload.get("agent").and_then(Value::as_str) == Some(name)
                && t.payload.get("generation").and_then(Value::as_str) == Some(generation.as_str())
        }))
    }

    /// Retain a `task_done` that arrived for `record` after it was already
    /// terminalized for some reason other than a clean completion (a budget
    /// hard stop, a stuck/runaway kill, a plain crash) as durable evidence
    /// with an explicit recovery action — never by mutating `record`'s
    /// terminal state, which by construction has already been decided, and
    /// for a `Stopped` record has already been announced and its process
    /// already killed. Idempotent per generation via
    /// [`Self::late_task_done_evidence_exists`].
    fn retain_late_task_done_evidence(
        &self,
        record: &AgentRecord,
        task_done: &Tuple,
    ) -> rk_core::Result<Option<Tuple>> {
        if self.late_task_done_evidence_exists(
            &record.repo_name,
            &record.name,
            record.created_at,
        )? {
            return Ok(None);
        }
        let diff = self.diff_summary_for(&record.name);
        let recovery_action = match &record.branch {
            Some(branch) => format!(
                "rk land {branch} --repo {} --target {}  (verify the branch first; or `rk respawn {}` to resume the generation)",
                record.repo_name, record.target_branch, record.name
            ),
            None => format!(
                "rk respawn {} to resume the generation (no branch was recorded to land directly)",
                record.name
            ),
        };
        let evidence = Tuple::new(
            Category::Artifact,
            record.repo_name.clone(),
            LATE_TASK_DONE_EVIDENCE_IDENTITY,
            "daemon",
            json!({
                "agent": record.name,
                "generation": record.created_at.to_rfc3339(),
                "task": record.task,
                "branch": record.branch,
                "target": record.target_branch,
                "head_sha": diff.head_sha,
                "terminal_state": format!("{:?}", record.state),
                "terminal_reason": record.result,
                "declared_summary": task_done.payload.get("summary"),
                "recovery_action": recovery_action,
                "retained_at": Utc::now().to_rfc3339(),
            }),
        )
        .with_lifecycle(Lifecycle::Furniture);
        self.space.out(evidence.clone())?;
        warn!(
            agent = %record.name, state = ?record.state,
            "a task_done arrived after this generation was already terminalized; \
             retained as evidence, terminal state left unchanged"
        );
        Ok(Some(evidence))
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
                // Generation join key (docs/2026-08-17-tkt-c1-generation-identity.md,
                // consumer C3): the producer side of B1/C1's dual-key read. A
                // namesake predecessor's `harness_result` never carries this
                // generation's id, so a spawn-keyed reader cannot match it —
                // unlike "agent", which only a name+floor test disambiguates.
                "spawn": record.spawn_id().to_string(),
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
        if let Some(task) = &record.task {
            let _ = crate::span::record_phase_span(
                &self.space,
                &record.repo_name,
                &self.castle,
                &crate::span::PhaseSpan::new(task, crate::span::Phase::Completed)
                    .repo(&record.repo_name)
                    .target(&record.target_branch)
                    .candidate(&diff.head_sha)
                    .terminal_reason(if declared_done {
                        "declared-done"
                    } else if is_error {
                        "error"
                    } else {
                        "stopped"
                    }),
            );
        }
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
                    let home = self.layout.home().to_path_buf();
                    let repo_root = record.repo_root.clone();
                    let branch = record.branch.clone();
                    let target = record.target_branch.clone();
                    let fork_point = record.fork_point.clone();
                    tokio::spawn(async move {
                        // Bind `done` to delivery per delivery-mode policy
                        // (TKT-01M08HB566GFBZVMDKZ8DT1ES0 / strategic-review
                        // C3): a merge/merge-push ticket must not read `done`
                        // while its branch is still unmerged — the class
                        // behind TKT-18/46/147, where an approved-looking
                        // ticket's code never reached main. `push-branch` is
                        // deliberately NOT gated here: its delivery (the
                        // push) hasn't even been attempted yet at this point
                        // in the lifecycle (that happens later, on dismiss),
                        // and unlike merge-mode there is no later merge-driven
                        // transition to `closed` to fall back on — gating it
                        // here would strand the ticket at `in_progress`
                        // forever. `pr` mode is deferred per the ticket. A
                        // ticket with no branch (e.g. a grooming-only pass)
                        // has nothing to gate on and proceeds as before.
                        let gate = match branch {
                            Some(branch) => {
                                ticket_delivered(home, repo_root, branch, target, fork_point, false)
                                    .await
                            }
                            None => Ok(()),
                        };
                        match gate {
                            Ok(()) => {
                                if let Err(e) = tickets.set_status(&task, "done").await {
                                    warn!(ticket = %task, error = %e, "failed to mark ticket done");
                                }
                            }
                            Err(e) => {
                                info!(
                                    ticket = %task,
                                    reason = %e,
                                    "completion left ticket in_progress: not yet delivered per its repo's delivery-mode policy"
                                );
                            }
                        }
                    });
                }
            }
        }
    }

    /// Seam 7 (strategic-review B5): arm a one-shot grace timer the moment a
    /// generation's clean `rk done` publishes. `sweep()` only ever looks at
    /// `is_live()` agents, so once this record is `Completed` its process —
    /// interactive harnesses stay alive between turns — is otherwise never
    /// checked again; nothing else kills it. `generation` pins this timer to
    /// the record it fired on; `session` (B5 rework) pins it to the exact
    /// process launch — a manual `rk respawn` during the grace window keeps
    /// `generation` (see `respawn_mode`'s comment on why) but gets a fresh
    /// `session` token, so it cannot be shot out from under it by a stale
    /// timer armed for the process it replaced.
    fn schedule_done_kill(
        self: &Arc<Self>,
        name: String,
        generation: DateTime<Utc>,
        session: rk_core::id::SpawnId,
    ) {
        let this = Arc::clone(self);
        let grace_secs = self.done_kill_grace_secs.load(Ordering::Relaxed).max(1);
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(grace_secs)).await;
            this.kill_lingering_after_done(&name, generation, session, grace_secs)
                .await;
        });
    }

    /// The grace timer's payoff: if a process is STILL tracked under `name`
    /// for the SAME generation AND session that armed the timer, it did not
    /// exit on its own within the grace window — SIGKILL its whole process
    /// group. A clean exit within the window already removed the control
    /// handle (see the `Exited` arm of [`handle_event`](Supervisor::handle_event)),
    /// so that path is a no-op here, matching the "clean-exit path
    /// unaffected" acceptance criterion. The `session` check (not `generation`
    /// alone) is what makes a respawn during the grace window safe: a respawn
    /// reuses the record's generation but registers a new session token, so a
    /// timer armed for the predecessor no longer matches.
    async fn kill_lingering_after_done(
        self: &Arc<Self>,
        name: &str,
        generation: DateTime<Utc>,
        session: rk_core::id::SpawnId,
        grace_secs: u64,
    ) {
        let record = match self.lock_registry().get(name) {
            Some(r) if r.created_at == generation => r.clone(),
            _ => return, // dismissed, or already gone — not our process
        };
        if self.lock_session_tokens().get(name) != Some(&session) {
            return; // respawned since — a new session now owns this name
        }
        if !self.lock_controls().contains_key(name) {
            return; // exited on its own within the grace window
        }
        let notice = rk_core::notify::EscalationNotice::new(
            name,
            "kill-process-group",
            rk_core::notify::Severity::Warn,
            record.repo_name.clone(),
            name,
            format!(
                "{name}'s harness process was still running {grace_secs}s after a clean \
                 `rk done` — SIGKILLing its process group"
            ),
        )
        .with_ref("task", record.task.clone().unwrap_or_default());
        let outcome = self.announcer.announce(
            &self.space,
            &self.sinks.lock().unwrap_or_else(|p| p.into_inner()),
            crate::recovery::RecoveryAction {
                kind: "kill-process-group".to_string(),
                instance: "supervisor".to_string(),
                notice,
            },
            // 20/hour: generous enough that a genuine multi-agent lingering
            // episode is fully visible, tight enough that a pathological
            // fleet-wide loop cannot turn this into a notification storm.
            crate::recovery::RateCap::per_hour(20),
        );
        match outcome {
            Ok(outcome) if !outcome.held() => {
                let control = self.lock_controls().remove(name);
                if let Some(control) = control {
                    warn!(
                        agent = name,
                        grace_secs, "harness still running past grace after `rk done` — SIGKILLing"
                    );
                    let _ = control.hard_kill().await;
                }
            }
            // Rate-held: the escalation still went out (at raised severity,
            // explaining why) — the action itself must NOT proceed. Leave
            // the control handle in place so a later sweep/dismiss can still
            // reach this process normally.
            Ok(_) => {}
            Err(e) => warn!(agent = name, error = %e, "failed to announce done-kill"),
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

    /// Return the generation of the process currently behind this agent's
    /// control handle. This is deliberately separate from `AgentRecord`'s
    /// task generation: a respawn reuses that record while launching a new
    /// process, and trusted control audit must name the process that received
    /// the envelope.
    pub fn session_generation(&self, name: &str) -> Option<rk_core::id::SpawnId> {
        self.lock_session_tokens().get(name).copied()
    }

    /// Deliver a durable control envelope to a live harness. The old string
    /// method remains for daemon-internal nudges; operator/RPC steering must
    /// use this typed path so the adapter can acknowledge the exact message.
    pub async fn steer_envelope(
        &self,
        name: &str,
        envelope: &ControlEnvelope,
    ) -> rk_core::Result<()> {
        let control = self.lock_controls().get(name).cloned();
        if let Some(control) = control {
            return control.steer_envelope(envelope).await;
        }
        if self
            .lock_registry()
            .get(name)
            .and_then(|record| record.attach_target.clone())
            .is_some()
        {
            return Err(rk_core::Error::other(
                "attached harness has no authenticated control envelope channel",
            ));
        }
        Err(rk_core::Error::other(format!("{name} has no live session")))
    }

    pub async fn interrupt(&self, name: &str) -> rk_core::Result<()> {
        let control = self
            .lock_controls()
            .get(name)
            .cloned()
            .ok_or_else(|| rk_core::Error::other(format!("{name} has no live session")))?;
        let prior = self
            .status(name)
            .ok_or_else(|| rk_core::Error::other(format!("no such agent: {name}")))?;
        if !prior.state.is_live() {
            return Err(rk_core::Error::other(format!("{name} is not running")));
        }
        self.lock_registry().update(name, |r| {
            if r.state.is_live() {
                r.state = AgentState::Stopped;
                r.crashed = false;
                r.result = Some("interrupted deliberately by operator".into());
            }
        })?;
        if let Err(error) = control.interrupt().await {
            // If signal delivery itself failed, restore the exact live state
            // the operator observed. A concurrently-arrived terminal event is
            // left alone by the state guard.
            let _ = self.lock_registry().update(name, |r| {
                if r.state == AgentState::Stopped {
                    r.state = prior.state;
                    r.result = prior.result.clone();
                }
            });
            return Err(error);
        }
        // The interrupted agent may have a `verify.run` execution of its own
        // in flight (its completion check calling into the daemon-managed
        // check runner) — that RPC caller was just stopped, so its managed
        // child process must not keep running under the daemon alone
        // (TKT-01M0PA6C5WYRWS757R1SS2F2GR). Fenced to the generation this
        // interrupt actually observed, so a respawn racing in right behind it
        // is never touched.
        self.cancel_managed_verification_for_agent(name, Some(prior.spawn_id()), "agent_interrupt");
        Ok(())
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
        resolve_repository_policy(self.layout.home(), repo)
    }

    pub(crate) fn set_landing_pipeline(&self, pipeline: &Arc<crate::landing::LandingPipeline>) {
        *self
            .landing_pipeline
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(Arc::downgrade(pipeline));
    }

    fn landing_pipeline(&self) -> Option<Arc<crate::landing::LandingPipeline>> {
        self.landing_pipeline
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .and_then(Weak::upgrade)
    }

    fn task_for_branch(&self, repo_root: &std::path::Path, branch: &str) -> Option<String> {
        self.lock_registry()
            .list_all()
            .into_iter()
            .filter(|r| r.repo_root == repo_root && r.branch.as_deref() == Some(branch))
            .max_by_key(|r| r.created_at)
            .and_then(|r| r.task.clone())
    }

    /// Resolve the task identity a `land` submission delivers against.
    ///
    /// `task_for_branch` only ever finds a task if some agent record was
    /// spawned onto exactly this branch — a recovery branch built by hand
    /// (e.g. resubmitting escalated work after a reviewer died) carries no
    /// such record, so that lookup silently returns `None` and the landing
    /// pipeline enqueues with `task: ""`: no ticket/spec for the reviewer,
    /// no delivery record, no close. An explicit `--task` closes that gap,
    /// but only after validation — this must never let branch text or an
    /// operator typo bind a delivery to the wrong ticket:
    ///   - the ticket must exist;
    ///   - its scope must match the repo being landed into;
    ///   - if an agent record ALSO resolves a task for this branch, it must
    ///     agree with the explicit one (fail closed on disagreement rather
    ///     than silently letting the explicit value override real evidence).
    ///
    /// With no explicit task, behavior is unchanged: fall back to whatever
    /// `task_for_branch` finds (possibly `None`, for untracked work).
    fn resolve_land_task(
        &self,
        repo_name: &str,
        repo_root: &std::path::Path,
        branch: &str,
        explicit: Option<String>,
    ) -> rk_core::Result<Option<String>> {
        let Some(task) = explicit else {
            return Ok(self.task_for_branch(repo_root, branch));
        };
        let task = task.trim().to_string();
        if task.is_empty() {
            return Err(rk_core::Error::other("--task must not be empty"));
        }
        let ticket = self
            .tickets
            .get(&task)?
            .ok_or_else(|| rk_core::Error::other(format!("no such ticket: {task}")))?;
        if ticket.scope != repo_name {
            return Err(rk_core::Error::other(format!(
                "task {task} is scoped to '{}', not '{repo_name}' — refusing to bind this \
                 landing to a ticket from another repo",
                ticket.scope
            )));
        }
        if let Some(found) = self.task_for_branch(repo_root, branch) {
            if found != task {
                return Err(rk_core::Error::other(format!(
                    "task {task} disagrees with {found}, which an agent record already binds to \
                     branch {branch} — refusing to override real evidence with --task"
                )));
            }
        }
        Ok(Some(task))
    }

    fn record_merge_for_branch(
        &self,
        repo_root: &std::path::Path,
        branch: &str,
        result: &serde_json::Value,
    ) {
        let Some(commit) = result
            .get("merge_commit")
            .and_then(serde_json::Value::as_str)
        else {
            return;
        };
        let name = self
            .lock_registry()
            .list_all()
            .into_iter()
            .filter(|r| r.repo_root == repo_root && r.branch.as_deref() == Some(branch))
            .max_by_key(|r| r.created_at)
            .map(|r| r.name.clone());
        if let Some(name) = name {
            let _ = self.lock_registry().update(&name, |record| {
                record.merge_commit = Some(commit.to_string())
            });
        }
    }

    /// The most recent agent generation (live or archived) dispatched against
    /// ticket `task`, if any — used to resolve the branch/repo a ticket's
    /// `done` transition is checked against (TKT-01M08HB566GFBZVMDKZ8DT1ES0 /
    /// strategic-review C3).
    fn latest_task_record(&self, task: &str) -> Option<AgentRecord> {
        self.lock_registry()
            .list_all()
            .into_iter()
            .filter(|r| r.task.as_deref() == Some(task))
            .max_by_key(|r| r.created_at)
            .cloned()
    }

    fn recorded_fork_point(&self, repo_root: &std::path::Path, branch: &str) -> Option<String> {
        self.lock_registry()
            .list_all()
            .into_iter()
            .filter(|r| r.repo_root == repo_root && r.branch.as_deref() == Some(branch))
            .max_by_key(|r| r.created_at)
            .and_then(|r| r.fork_point.clone())
    }

    /// Refuse an explicit `done` (steward/operator `rk ticket update --status
    /// done`) on a ticket whose branch has not actually landed per its repo's
    /// delivery-mode policy — closing the "approved but never merged" class
    /// (TKT-18/46/147) at the point a human can act on the refusal instead of
    /// silently believing the work shipped. `merge`/`merge-push` require the
    /// branch to already be merged into (or gone from) its target;
    /// `push-branch` requires the same against the remote-tracking ref (the
    /// manual path can afford checking push-branch too — refusing an explicit
    /// request never stalls anything, unlike gating the automatic completion
    /// path before delivery has even been attempted, see the ticket-done
    /// block in [`Self::route_completion`]). `pr` is intentionally left
    /// unchecked:
    /// its binding is deferred until a forge ingest source exists (see
    /// strategic-review C4). A ticket with no dispatched branch (or an
    /// unresolvable repo) has nothing to gate on and is left unaffected.
    pub(crate) async fn require_ticket_delivered(&self, task: &str) -> rk_core::Result<()> {
        // The durable delivery record wins outright (P1b). It is written by
        // the landing pipeline at land time and survives the branch deletion
        // that landing performs, so a ticket that genuinely shipped still
        // reads delivered here — where the branch-ref fallback below would
        // see a missing ref and refuse. Only fall through to git when no
        // record exists (a ticket landed before this field, or delivered by a
        // path that predates the pipeline).
        if self
            .tickets
            .delivery(task)
            .map(|d| d.is_some())
            .unwrap_or(false)
        {
            return Ok(());
        }
        let Some(record) = self.latest_task_record(task) else {
            return Ok(());
        };
        let Some(branch) = record.branch.clone() else {
            return Ok(());
        };
        ticket_delivered(
            self.layout.home().to_path_buf(),
            record.repo_root.clone(),
            branch,
            record.target_branch.clone(),
            record.fork_point.clone(),
            true,
        )
        .await
        .map_err(|e| rk_core::Error::other(format!("ticket {task} cannot be marked done: {e}")))
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
                    // Measured BEFORE the merge moves `target`: `diff_stat`'s
                    // `target...branch` symmetric range is exactly "what did
                    // `branch` add since it diverged from `target`", so a
                    // branch that forked after its ticket's real work already
                    // landed reads as empty here regardless of what the merge
                    // itself reports.
                    let stat = {
                        let repo = repo.clone();
                        let branch = branch.to_string();
                        let target = target.clone();
                        blocking_io("repository policy pre-merge diff stat", move || {
                            repo.diff_stat(&target, &branch)
                        })
                        .await?
                    };
                    delivery.content_free = stat.files.is_empty() && stat.lines == 0;
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
                    repo.push_branch_as(&branch_to_push, &remote_branch_to_push, &remote_to_push)
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

    /// Dismiss: stop the session if live and remove its worktree. The branch
    /// is always preserved. Agent lifecycle and code delivery are deliberately
    /// separate acts; callers that want delivery must invoke `land`, which
    /// enters the gated landing queue.
    pub async fn dismiss(&self, name: &str, no_merge: bool) -> rk_core::Result<serde_json::Value> {
        self.dismiss_inner(name, no_merge, false, None).await
    }

    /// Same as [`dismiss`](Self::dismiss), but for a caller that captured a
    /// specific generation's [`rk_core::id::SpawnId`] earlier (a fan-out
    /// member, at the moment it was spawned) and must not act on whoever
    /// currently holds `name` if that is no longer the same generation.
    ///
    /// This is the fan-out half of the generation-identity migration
    /// (`docs/2026-08-17-tkt-c1-generation-identity.md`, consumers B3/B4):
    /// `dismiss_all` used to resolve purely by name, so a fanned-out
    /// `dismiss` could — if the name it captured were ever reused before the
    /// dismiss ran — tear down a different, unrelated live rat instead of the
    /// one it fanned out. `expected_spawn: None` (a pre-migration
    /// `FannedAgent` with no `spawn`) preserves the old, unchecked behaviour.
    pub async fn dismiss_checked(
        &self,
        name: &str,
        expected_spawn: Option<rk_core::id::SpawnId>,
        no_merge: bool,
    ) -> rk_core::Result<serde_json::Value> {
        self.dismiss_inner(name, no_merge, false, expected_spawn)
            .await
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
        expected_spawn: Option<rk_core::id::SpawnId>,
    ) -> rk_core::Result<serde_json::Value> {
        let record = self
            .lock_registry()
            .get(name)
            .cloned()
            .ok_or_else(|| rk_core::Error::other(format!("no such agent: {name}")))?;
        if let Some(expected) = expected_spawn {
            let actual = record.spawn_id();
            if actual != expected {
                return Err(rk_core::Error::other(format!(
                    "dismiss target mismatch: {name} is now a different generation than the \
                     one this caller fanned out (expected spawn {expected}, found {actual}); \
                     refusing to act on a stranger"
                )));
            }
        }

        // Drop any held-back turn result BEFORE the kill: the `Exited` this
        // provokes must not publish a late `harness_result` for an agent the
        // caller is deliberately tearing down (TKT-160).
        self.forget_completion(name);
        // Same reasoning as `interrupt`: this generation is being torn down,
        // so any `verify.run` execution it has in flight must not keep its
        // managed child running under the daemon alone
        // (TKT-01M0PA6C5WYRWS757R1SS2F2GR).
        self.cancel_managed_verification_for_agent(name, Some(record.spawn_id()), "agent_dismiss");
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
            remote: policy.delivery.remote.clone(),
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
                delivery.detail = format!(
                    "branch {branch} preserved; dismissal never lands code — submit it with the gated land command"
                );
            }
        }

        self.lock_registry().update(name, |r| {
            r.state = AgentState::Dismissed;
            r.pid = None;
        })?;
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
    /// Passes the legacy `no_merge: true` spelling for clarity: this is a cleanup guarantee,
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
        let names: Vec<(String, rk_core::id::SpawnId)> = {
            let reg = self.lock_registry();
            reg.list()
                .into_iter()
                .filter(|a| {
                    a.workflow_instance.as_deref() == Some(instance)
                        && matches!(a.state, AgentState::Completed | AgentState::Failed)
                })
                .map(|a| (a.name.clone(), a.spawn_id()))
                .collect()
        };
        let mut results = Vec::with_capacity(names.len());
        for (name, spawn) in names {
            match self.dismiss_inner(&name, true, true, Some(spawn)).await {
                Ok(_) => results.push((name, true)),
                Err(error) => {
                    warn!(agent = %name, instance, %error, "finalize-time cleanup sweep could not dismiss agent");
                    results.push((name, false));
                }
            }
        }
        results
    }

    /// Companion to [`dismiss_orphaned_instance_agents`](Self::dismiss_orphaned_instance_agents)
    /// for the case that one deliberately excludes: a workflow instance
    /// whose owning wait already gave up (a ceiling, or an explicit
    /// cancellation) while the agent it was waiting on is STILL live
    /// (`AgentState::is_live`) — e.g. a reviewer stuck reconnecting through a
    /// transport outage (the 2026-08-21 incident: Codex reviewer Scurry-11
    /// stayed `Running` and reconnecting well after its owning
    /// steward-review workflow had already timed out). Leaving it running
    /// holds fleet capacity indefinitely for a wait nothing is listening to
    /// any more, so this tears the process down the same way an explicit
    /// `dismiss` would (`dismiss_inner` with `park_if_dirty: true`, same
    /// dirty-worktree guard the terminal-only sweep above applies) rather
    /// than leaving it to reconnect forever.
    ///
    /// Best-effort, same contract as the terminal-only sweep: a single
    /// agent's dismissal failing is logged and does not stop the others.
    pub async fn dismiss_live_instance_agents(&self, instance: &str) -> Vec<(String, bool)> {
        let names: Vec<(String, rk_core::id::SpawnId)> = {
            let reg = self.lock_registry();
            reg.list()
                .into_iter()
                .filter(|a| a.workflow_instance.as_deref() == Some(instance) && a.state.is_live())
                .map(|a| (a.name.clone(), a.spawn_id()))
                .collect()
        };
        let mut results = Vec::with_capacity(names.len());
        for (name, spawn) in names {
            match self.dismiss_inner(&name, true, true, Some(spawn)).await {
                Ok(_) => results.push((name, true)),
                Err(error) => {
                    warn!(agent = %name, instance, %error, "review-ceiling sweep could not dismiss a still-live agent");
                    results.push((name, false));
                }
            }
        }
        results
    }

    /// Revert an agent branch's recorded landing — the undo for an unattended
    /// delivery that turned out bad (steward/drain landed it, then main
    /// broke). Revert-merges the merge commit recorded on the agent's record
    /// by the landing path (serialized through the same per-target ref lock),
    /// reopens the agent's ticket (`open`, or `blocked` with
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
        // Same per-target ref lock as landing: the revert takes its
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
                    // Clear the durable delivery record first (P1b). Reopening
                    // the status alone is not enough: the record is what every
                    // "is it delivered" question now reads, so a reverted merge
                    // that left it standing would keep claiming the work
                    // shipped while the commit is no longer in the target.
                    match self.tickets.clear_delivery(task, status).await {
                        Ok(true) => ticket_status = Some(status),
                        Ok(false) => match self.tickets.reopen(task, status).await {
                            Ok(_) => ticket_status = Some(status),
                            Err(e) => {
                                warn!(ticket = %task, error = %e, "failed to reopen ticket on revert");
                            }
                        },
                        Err(e) => {
                            warn!(ticket = %task, error = %e, "failed to clear delivery on revert");
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
    /// delivery operation. Names neither an agent nor a worktree.
    ///
    /// Routes on the repo's merge mode:
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
    pub(crate) async fn land_prepared(
        &self,
        repo_root: &std::path::Path,
        branch: &str,
        target: &str,
        keep_branch: bool,
        candidate: &rk_git::PreparedMerge,
    ) -> rk_core::Result<serde_json::Value> {
        let repo_path = repo_root.to_path_buf();
        let repo = blocking_io("prepared land repo discovery", move || {
            Repo::discover(&repo_path)
        })
        .await?;
        let repo_name = repo.name();
        let policy = self.repository_policy(&repo);
        let effective_target = policy.delivery_target(target);
        if effective_target != target {
            return Err(rk_core::Error::other(format!(
                "prepared landing target mismatch: candidate was built for {target}, policy resolved {effective_target}"
            )));
        }
        if !matches!(
            policy.delivery.mode,
            DeliveryMode::Merge | DeliveryMode::MergePush
        ) {
            return Err(rk_core::Error::other(format!(
                "prepared landing requires merge or merge-push delivery mode, found {:?}",
                policy.delivery.mode
            )));
        }

        let advance = {
            let _merge_guard = self.merge_queue.acquire(repo.root(), target).await;
            let repo = repo.clone();
            let target = target.to_string();
            let commit = candidate.commit.clone();
            let base = candidate.base.clone();
            blocking_io("prepared landing compare-and-swap", move || {
                repo.advance_target_to(&target, &commit, &base)
            })
            .await?
        };
        match advance {
            rk_git::AdvanceOutcome::Advanced { .. } => {}
            rk_git::AdvanceOutcome::Stale { expected, actual } => {
                return Ok(json!({
                    "branch": branch,
                    "target": target,
                    "delivered": false,
                    "merged": false,
                    "stale": true,
                    "tested_sha": candidate.commit,
                    "expected_target_sha": expected,
                    "actual_target_sha": actual,
                    "detail": "target moved after gates; candidate must be rebuilt and retested",
                }));
            }
            // `target` is checked out (root or a linked worktree, e.g. an
            // agent's own worktree on its own branch) and refused the
            // fast-forward — a genuinely dirty checkout, not a contended
            // race. Nothing landed, the ref never moved, and the candidate
            // is still parked under its ref: fail closed and let the caller
            // (the landing pipeline) raise a durable human recovery gate
            // rather than silently retrying against the same dirty worktree.
            rk_git::AdvanceOutcome::Blocked {
                expected,
                path,
                detail,
            } => {
                return Ok(json!({
                    "branch": branch,
                    "target": target,
                    "delivered": false,
                    "merged": false,
                    "blocked": true,
                    "tested_sha": candidate.commit,
                    "expected_target_sha": expected,
                    "worktree_path": path.display().to_string(),
                    "detail": format!(
                        "{target} is checked out at {} and refused a fast-forward: {detail}",
                        path.display()
                    ),
                }));
            }
        }

        let mut delivery = BranchDelivery {
            target: target.to_string(),
            remote: policy.delivery.remote.clone(),
            merged: true,
            merge_commit: Some(candidate.commit.clone()),
            content_free: candidate.is_empty(),
            detail: format!("advanced {target} to pre-tested merge {}", candidate.commit),
            ..BranchDelivery::default()
        };
        repo.discard_candidate(&candidate.candidate_ref)?;
        if policy.delivery.mode == DeliveryMode::MergePush {
            let repo = repo.clone();
            let target_to_push = target.to_string();
            let remote_to_push = delivery.remote.clone();
            match blocking_io("prepared landing target push", move || {
                repo.push_branch_as(&target_to_push, &target_to_push, &remote_to_push)
            })
            .await
            {
                Ok(output) => {
                    delivery.pushed = true;
                    delivery.detail = format!(
                        "{}; pushed {target} to {}: {}",
                        delivery.detail,
                        delivery.remote,
                        output.trim()
                    );
                }
                Err(error) => {
                    delivery.detail = format!(
                        "{}; local merge succeeded but push to {}/{} failed: {error}",
                        delivery.detail, delivery.remote, target
                    );
                }
            }
        }
        delivery.delivered = policy.delivery.mode == DeliveryMode::Merge || delivery.pushed;
        if delivery.delivered && policy.delivery.delete_source && !keep_branch {
            let repo = repo.clone();
            let source = branch.to_string();
            match blocking_io("prepared landing branch deletion", move || {
                repo.delete_branch(&source)
            })
            .await
            {
                Ok(()) => delivery.branch_deleted = true,
                Err(error) => warn!(
                    branch,
                    error = %error,
                    "prepared delivery succeeded but source branch could not be deleted"
                ),
            }
        }
        let result = json!({
            "branch": branch,
            "target": delivery.target,
            "remote": delivery.remote,
            "delivered": delivery.delivered,
            "merged": delivery.merged,
            "merge_commit": delivery.merge_commit,
            "tested_sha": candidate.commit,
            "content_free": delivery.content_free,
            "pushed": delivery.pushed,
            "pr_opened": false,
            "detail": delivery.detail,
            "branch_deleted": delivery.branch_deleted,
            "stale": false,
        });
        self.emit_event(&repo_name, "branch_landed", result.clone());
        info!(
            branch,
            target,
            tested_sha = %candidate.commit,
            delivered = delivery.delivered,
            pushed = delivery.pushed,
            "landed exact pre-tested merge"
        );
        Ok(result)
    }

    pub async fn land(
        &self,
        repo_root: &std::path::Path,
        branch: &str,
        target: &str,
        keep_branch: bool,
        task: Option<String>,
    ) -> rk_core::Result<serde_json::Value> {
        let repo_path = repo_root.to_path_buf();
        let repo = blocking_io("land repo discovery", move || Repo::discover(&repo_path)).await?;
        let repo_name = repo.name();
        let fork_point = self.recorded_fork_point(repo.root(), branch);
        let head_sha = repo.rev_parse(branch).ok();
        let policy = self.repository_policy(&repo);
        if matches!(
            policy.delivery.mode,
            DeliveryMode::Merge | DeliveryMode::MergePush
        ) {
            if let Some(pipeline) = self.landing_pipeline() {
                let task = self.resolve_land_task(&repo_name, repo.root(), branch, task)?;
                let result = pipeline
                    .submit_manual(repo.root(), branch, target, keep_branch, task)
                    .await?;
                self.record_merge_for_branch(repo.root(), branch, &result);
                return Ok(result);
            }
            if !cfg!(test) {
                return Err(rk_core::Error::other(
                    "merge-mode landing pipeline is unavailable; refusing an untested direct merge",
                ));
            }
        }
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
            // Surfaced so the landing pipeline can refuse to write a delivery
            // record for a branch that added nothing over its target: an empty
            // merge is not a delivery, and closing a ticket on one is exactly
            // the duplicate-no-op-merge failure TKT-01M0C663BZ86SMA2PVMFP5QJ8D
            // describes. `dismiss` already gates its ticket close on this;
            // `land` withheld it from callers.
            "content_free": delivery.content_free,
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
                    "fork_point": fork_point,
                    "head_sha": head_sha,
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

    /// Deliberate operator escape hatch for an emergency ungated landing.
    /// Normal callers must use `land`, which enqueues. This path requires a
    /// human reason and emits both an audit event and an inbox-visible need.
    pub async fn land_force(
        &self,
        repo_root: &std::path::Path,
        branch: &str,
        target: &str,
        keep_branch: bool,
        reason: &str,
    ) -> rk_core::Result<serde_json::Value> {
        if reason.trim().is_empty() {
            return Err(rk_core::Error::other(
                "forced landing requires a non-empty --reason",
            ));
        }
        let repo_path = repo_root.to_path_buf();
        let repo = blocking_io("forced land repo discovery", move || {
            Repo::discover(&repo_path)
        })
        .await?;
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
            "content_free": delivery.content_free,
            "pushed": delivery.pushed,
            "pr_opened": delivery.pr_opened,
            "pr_url": delivery.pr_url,
            "detail": delivery.detail,
            "branch_deleted": delivery.branch_deleted,
            "forced": true,
            "reason": reason,
        });
        self.record_merge_for_branch(repo.root(), branch, &result);
        self.emit_event(&repo_name, "branch_landed", result.clone());
        self.emit_event(
            &repo_name,
            "forced_landing",
            json!({
                "branch": branch,
                "target": target,
                "reason": reason,
                "merge_commit": result.get("merge_commit"),
                "text": format!("operator forced ungated landing of {branch} onto {target}: {reason}"),
            }),
        );
        let _ = self.space.out(Tuple::new(
            Category::Need,
            repo_name.clone(),
            "forced_landing",
            "daemon",
            json!({
                "agent": "operator",
                "branch": branch,
                "target": target,
                "reason": reason,
                "text": format!("UNGATED landing was forced: {branch} -> {target}: {reason}"),
            }),
        ));
        warn!(repo = %repo_name, branch, target, reason, "operator forced ungated landing");
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
        let fork_point = self.recorded_fork_point(repo.root(), branch);
        let head_sha = repo.rev_parse(branch).ok();
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
                    "fork_point": fork_point,
                    "head_sha": head_sha,
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
        // First checkpoint only: `revision == 1` is the one check-in that is a
        // phase boundary — time-to-first-progress, measured from this
        // generation's `created_at` to the check-in itself. Every later
        // revision is ordinary progress, not a new phase, so it records
        // nothing (the span key would dedup it onto this one anyway).
        // Best-effort: the checkpoint above is already durable and must not be
        // undone by a telemetry failure.
        if updated.progress.as_ref().map(|p| p.revision) == Some(1) {
            if let Some(task) = &updated.task {
                let _ = crate::span::record_phase_span(
                    &self.space,
                    &updated.repo_name,
                    &self.castle,
                    &crate::span::PhaseSpan::new(task, crate::span::Phase::FirstProgress)
                        .repo(&updated.repo_name)
                        .target(&updated.target_branch)
                        .started_at(updated.created_at)
                        .ended_at(now)
                        .terminal_reason(status.as_str()),
                );
            }
        }
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

    pub fn is_groomer(&self, name: &str) -> bool {
        self.status(name)
            .is_some_and(|record| record.role == GROOMER_ROLE && record.state.is_live())
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
        let reaped_artifacts: Vec<serde_json::Value> = if reap.artifacts {
            archived
                .iter()
                .map(|r| self.reap_artifacts(r, reap.artifact_paths_for(&r.repo_name)))
                .collect()
        } else {
            Vec::new()
        };
        info!(
            count = archived.len(),
            cutoff = %cutoff,
            reaped = done(&reaped),
            reaped_logs = done(&reaped_logs),
            reaped_artifacts = done(&reaped_artifacts),
            "archived terminal agent records"
        );
        Ok(json!({
            "dry_run": false,
            "count": archived.len(),
            "agents": archived,
            "reaped": reaped,
            "reaped_logs": reaped_logs,
            "reaped_artifacts": reaped_artifacts,
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

    /// Reclaim EVERY still-live terminal record's regenerable build
    /// artifacts right now, with no age cutoff — the immediate counterpart to
    /// the artifact-reap [`archive_agents`] also performs on records that
    /// have already crossed its `after_days` cutoff. O12 (2026-08-18 drain
    /// probe): shipping artifact reap only inside `archive_agents` meant a
    /// newly terminal agent's `target/` stood untouched for up to
    /// `after_days` (default 3) before the first sweep even looked at it —
    /// exactly the silent-231GB accumulation this exists to close. A record
    /// that later actually archives just gets reaped again here first,
    /// idempotently (`reap_artifacts` no-ops once nothing named remains).
    ///
    /// [`archive_agents`]: Self::archive_agents
    pub fn reap_terminal_artifacts(&self, reap: &Reap) -> Vec<serde_json::Value> {
        if !reap.artifacts {
            return Vec::new();
        }
        self.lock_registry()
            .terminal()
            .into_iter()
            .map(|r| self.reap_artifacts(r, reap.artifact_paths_for(&r.repo_name)))
            .collect()
    }

    /// Reclaim one archived agent's regenerable build artifacts — e.g.
    /// `target` — from its worktree, regardless of merge state. Unlike
    /// [`reap_git`](Self::reap_git) there is no merged-or-gone gate: an
    /// unmerged branch's build output is exactly as regenerable as a merged
    /// one's, and only the resolved paths are ever removed, so the source
    /// tree, git history, and any uncommitted edits elsewhere in the worktree
    /// stay completely untouched. Reachable via [`archive_agents`] (archived
    /// records) and [`reap_terminal_artifacts`](Self::reap_terminal_artifacts)
    /// (any still-live terminal record) — both require the record to already
    /// be terminal (Completed/Failed/Dismissed); a live agent's worktree is
    /// never a candidate.
    ///
    /// STACK NEUTRALITY: which paths get removed is resolved per-repo, not
    /// hardcoded here — the record's own repo, if registered, uses its
    /// activated `.rk/repo.cue` `reap.artifactPaths`
    /// ([`rk_workflow::ReapPolicy`]) whenever it declares one; only a repo
    /// that declares NOTHING falls back to `fallback_paths` (the caller's
    /// operator-set [`Reap::artifact_paths`]/`artifact_paths_by_repo`, empty
    /// by default). The daemon itself never assumes what any language's
    /// build directory is called.
    ///
    /// Each resolved entry is a literal worktree-relative path (not a shell
    /// glob); entries that are empty, absolute, contain a `..` segment, or
    /// resolve to the worktree root itself (`.`, `./`, or equivalent — every
    /// segment empty or `.`) are skipped defensively rather than resolved,
    /// since nothing about this reap should ever be able to reach outside —
    /// or BE — the worktree. `.rk/repo.cue` policy loading already rejects
    /// these at activation time; the check is repeated here as the last line
    /// of defense against the operator-set fallback, which is not
    /// schema-validated the same way.
    ///
    /// Best-effort in the same shape as `reap_git`/`reap_log`: a failure is a
    /// `reaped: false` row with a reason, never a failed archive.
    fn reap_artifacts(&self, record: &AgentRecord, fallback_paths: &[String]) -> serde_json::Value {
        let row = |reaped: bool, reason: String| json!({"agent": record.name, "reaped": reaped, "reason": reason});
        let Some(worktree) = &record.worktree else {
            return row(false, "no worktree recorded".into());
        };
        if !worktree.exists() {
            return row(false, "worktree already gone".into());
        }
        let policy_paths = Repo::discover(&record.repo_root)
            .ok()
            .map(|repo| self.repository_policy(&repo).reap.artifact_paths)
            .unwrap_or_default();
        let paths: &[String] = if !policy_paths.is_empty() {
            &policy_paths
        } else {
            fallback_paths
        };
        let mut removed = Vec::new();
        for rel in paths {
            let resolves_to_root = rel.split('/').all(|seg| seg.is_empty() || seg == ".");
            if rel.is_empty()
                || rel.starts_with('/')
                || rel.split('/').any(|seg| seg == "..")
                || resolves_to_root
            {
                warn!(
                    agent = %record.name,
                    path = %rel,
                    "worktree artifact sweep: skipping unsafe path"
                );
                continue;
            }
            let target = worktree.join(rel);
            if !target.exists() {
                continue;
            }
            let result = if target.is_dir() {
                std::fs::remove_dir_all(&target)
            } else {
                std::fs::remove_file(&target)
            };
            match result {
                Ok(()) => removed.push(rel.clone()),
                Err(e) => return row(false, format!("failed to remove {rel}: {e}")),
            }
        }
        if removed.is_empty() {
            row(true, "no matching artifact paths present".into())
        } else {
            row(true, format!("removed: {}", removed.join(", ")))
        }
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
        match self.log.delete_for(&record.name, record.spawn_id()) {
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
        review: Option<&rk_core::review::ReviewContext>,
    ) -> HashMap<String, String> {
        let mut env = HashMap::new();
        env.insert("RK_HOME".into(), self.layout.home().display().to_string());
        env.insert("RK_AGENT".into(), name.to_string());
        // Generation-identity migration (C6, docs/2026-08-17-tkt-c1-generation-identity.md):
        // RK_AGENT stays the display label a rat and every `rk` sugar command
        // read; RK_SPAWN is the join key `rk done`/`rk out` stamp into their
        // payloads so a reader can key on `Pattern::for_spawn` instead of a
        // name+floor test. Absent only if the registry row vanished between
        // insert and here, which never happens on the live spawn path.
        if let Some(spawn) = self.status(name).and_then(|r| r.spawn) {
            env.insert("RK_SPAWN".into(), spawn.to_string());
        }
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
        if self.shared_cargo_target.load(Ordering::Relaxed) {
            env.insert(
                "CARGO_TARGET_DIR".into(),
                self.layout
                    .cargo_target_cache_dir(repo_name)
                    .display()
                    .to_string(),
            );
        }
        if let Some(instance) = workflow_instance {
            env.insert("RK_WORKFLOW_INSTANCE".into(), instance.to_string());
        }
        if let Some(review) = review {
            for (name, value) in review.env_pairs() {
                env.insert(name.into(), value.into());
            }
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

    pub(crate) fn lock_registry(&self) -> std::sync::MutexGuard<'_, Registry> {
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

    fn lock_session_tokens(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<String, rk_core::id::SpawnId>> {
        match self.session_tokens.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    /// Register a freshly launched session's control handle under `name`,
    /// stamping a fresh per-session token alongside it — the key
    /// [`kill_lingering_after_done`](Self::kill_lingering_after_done) needs to
    /// tell a respawned session apart from the one a grace timer was armed
    /// for, since both share the same `AgentRecord` generation.
    fn track_session(&self, name: &str, control: SessionControl) -> rk_core::id::SpawnId {
        let token = rk_core::id::SpawnId::new();
        self.lock_controls().insert(name.to_string(), control);
        self.lock_session_tokens().insert(name.to_string(), token);
        // A daemon restart can leave a durable steer request without its
        // delivery acknowledgement. Replay it exactly once per new live
        // session; an existing ack makes `pending` omit it permanently.
        if let (Some(record), Ok(handle)) = (
            self.lock_registry().get(name),
            tokio::runtime::Handle::try_current(),
        ) {
            if let Ok(pending) =
                crate::steer::pending(&self.space, &record.repo_name, name, &self.castle)
            {
                let control = self.lock_controls().get(name).cloned();
                if let Some(control) = control {
                    for envelope in pending {
                        let envelope = envelope.for_resume_generation(token.to_string());
                        let control = control.clone();
                        handle.spawn(async move {
                            if let Err(error) = control.steer_envelope(&envelope).await {
                                warn!(
                                    message_id = %envelope.message_id,
                                    %error,
                                    "failed to replay pending steer"
                                );
                            }
                        });
                    }
                }
            }
        }
        token
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

    fn lock_transport_breakers(
        &self,
    ) -> std::sync::MutexGuard<'_, crate::transport_breaker::TransportBreakers> {
        match self.transport_breakers.lock() {
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
            (
                "01KY00000000000000000000AA".into(),
                "unrelated: keep me".into(),
            ),
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
        let review = rk_core::review::ReviewContext {
            branch: "rat/worker/task".into(),
            head_sha: "abc123".into(),
            target: "integration".into(),
            task: "TKT-123".into(),
            attempt: "landing-review-123".into(),
        };
        let env = sup.agent_env(
            "Nibble",
            "reviewer",
            "repo",
            "review",
            Some("rat/nibble/review"),
            "integration",
            Path::new("/tmp/review-worktree"),
            None,
            Some(&review),
        );

        assert_eq!(env.get("RK_BASE").map(String::as_str), Some("integration"));
        for (name, expected) in review.env_pairs() {
            assert_eq!(env.get(name).map(String::as_str), Some(expected), "{name}");
        }
    }

    #[test]
    fn agent_env_omits_shared_target_dir_by_default() {
        let home = tempfile::tempdir().unwrap();
        let sup = supervisor(home.path());
        let env = sup.agent_env(
            "Nibble",
            "rat",
            "repo",
            "task",
            Some("rat/nibble/task"),
            "main",
            Path::new("/tmp/nibble-worktree"),
            None,
            None,
        );

        assert!(!env.contains_key("CARGO_TARGET_DIR"));
    }

    #[test]
    fn agent_env_shares_cargo_target_dir_per_repo_when_enabled() {
        let home = tempfile::tempdir().unwrap();
        let sup = supervisor(home.path());
        sup.set_shared_cargo_target(true);
        let env = sup.agent_env(
            "Nibble",
            "rat",
            "repo",
            "task",
            Some("rat/nibble/task"),
            "main",
            Path::new("/tmp/nibble-worktree"),
            None,
            None,
        );

        assert_eq!(
            env.get("CARGO_TARGET_DIR").map(String::as_str),
            Some(
                Layout::at(home.path())
                    .cargo_target_cache_dir("repo")
                    .display()
                    .to_string()
            )
            .as_deref(),
        );
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
            review: None,
            model: None,
            permission_mode: None,
            attach: false,
            workflow_instance: None,
            coordinator: None,
            instance_max_usd: None,
            profile: None,
            resolved_profile: None,
        };
        let record = spawning_record(SpawnJournal {
            params: &params,
            repo: &repo,
            repo_name: "repo",
            name: "Nibble".into(),
            branch: "rat/nibble/task".into(),
            fork_point: "base".into(),
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
            review: None,
            model: None,
            permission_mode: None,
            attach: false,
            workflow_instance: None,
            coordinator: None,
            instance_max_usd: None,
            profile: None,
            resolved_profile: None,
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
            fork_point: "base".into(),
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
        for role in [
            "rat",
            "reviewer",
            "foreman",
            "verifier",
            "onboarder",
            "diagnostician",
            "groomer",
        ] {
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
        assert_eq!(permission_mode("groomer", "codex").unwrap(), "read-only");
    }

    #[test]
    fn only_jcode_onboarders_use_native_terminal_completion() {
        assert!(uses_harness_terminal_completion("onboarder", "jcode"));
        assert!(!uses_harness_terminal_completion("rat", "jcode"));
        assert!(!uses_harness_terminal_completion("onboarder", "codex"));

        let dir = tempfile::tempdir().unwrap();
        let supervisor = supervisor(dir.path());
        let generation = Utc::now();
        let claim = supervisor.claim_completion("Jade", generation, None, false, true);
        assert!(claim.publish);
        assert!(claim.declared_done);
        assert!(
            !supervisor
                .claim_completion("Jade", generation, None, false, true)
                .publish
        );

        let ordinary = supervisor.claim_completion("Whisker", generation, None, false, false);
        assert!(!ordinary.publish);
        assert!(!ordinary.declared_done);
    }

    /// Probe O6/O8, RAT path: a rat whose harness returns control at a turn
    /// boundary without an `rk done` is PAUSED, not `Completed` — and paused is
    /// live, so it keeps its drain slot and its ticket.
    ///
    /// The two acceptance criteria are asserted through the exact predicate
    /// each consumer uses: `drain.rs` counts WIP over `state.is_live()`, and
    /// `Server::ticket_reopen_sweep_once` skips a ticket whose assignee
    /// `state.is_live()`. Before this state existed both read `Completed` —
    /// the slot was freed and the ticket recycled onto a duplicate rat while
    /// the original was still working.
    #[test]
    fn a_turn_that_ends_without_rk_done_pauses_the_rat_rather_than_completing_it() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let sup = supervisor(home.path());
        let mut rec = record(repo.path(), None);
        rec.state = AgentState::Running;
        let generation = rec.created_at;
        let spawn = rec.spawn_id();
        sup.lock_registry().insert(rec).unwrap();

        sup.handle_event(
            "Nibble",
            generation,
            spawn,
            spawn,
            HarnessEvent::Completed {
                result: "still waiting on the check to finish".into(),
                is_error: false,
                usage: TokenUsage::default(),
                cost_usd: None,
                session_id: None,
            },
        );

        let paused = sup.status("Nibble").unwrap();
        assert_eq!(
            paused.state,
            AgentState::Paused,
            "a clean turn with no `rk done` is a pause, not a completion"
        );
        assert!(
            paused.state.is_live(),
            "paused must be live: drain WIP and the ticket-reopen sweep both key on is_live()"
        );
        assert!(
            !paused.state.is_archivable(),
            "a paused agent is still working; archiving it would hide live work"
        );
        assert!(
            !paused.crashed_without_reporting(),
            "a paused agent has not left the fleet, so a workflow `wait` must keep waiting on it"
        );
        assert_eq!(
            paused.result.as_deref(),
            Some("still waiting on the check to finish"),
            "the withheld turn text is kept for the eventual flush"
        );

        // The next harness event proves the turn resumed.
        sup.handle_event(
            "Nibble",
            generation,
            spawn,
            spawn,
            HarnessEvent::ToolUse {
                name: "Bash".into(),
            },
        );
        assert_eq!(sup.status("Nibble").unwrap().state, AgentState::Running);
    }

    /// Probe O6/O8, REVIEWER path: a reviewer that pauses must not fail its
    /// review workflow, and a genuinely dead one must still terminalize.
    ///
    /// `WorkflowExec::abandoned` is the gate that fails a waiting step early;
    /// it fires on `crashed_without_reporting()`. While paused that is false,
    /// so the review keeps waiting rather than hard-failing a reviewer that was
    /// proceeding correctly. Once the process actually exits, the record
    /// terminalizes promptly to `Failed` — but WITHOUT the crash markers, since
    /// the harness did report: the withheld turn text survives for
    /// `flush_withheld_completion` to publish instead of being overwritten by
    /// the "exited without completing" placeholder.
    #[test]
    fn a_paused_reviewer_survives_its_wait_and_a_dead_one_terminalizes_promptly() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let sup = supervisor(home.path());
        let mut rec = record(repo.path(), None);
        rec.role = "reviewer".into();
        rec.state = AgentState::Running;
        rec.usage.output = 4_000;
        let generation = rec.created_at;
        let spawn = rec.spawn_id();
        sup.lock_registry().insert(rec).unwrap();

        sup.handle_event(
            "Nibble",
            generation,
            spawn,
            spawn,
            HarnessEvent::Completed {
                result: "reading the diff; verdict still pending".into(),
                is_error: false,
                usage: TokenUsage::default(),
                cost_usd: None,
                session_id: None,
            },
        );
        let paused = sup.status("Nibble").unwrap();
        assert_eq!(paused.state, AgentState::Paused);
        assert!(
            !paused.crashed_without_reporting(),
            "a paused reviewer has not abandoned its wait; the review must not be failed for it"
        );

        // Now the process really is gone.
        sup.handle_event(
            "Nibble",
            generation,
            spawn,
            spawn,
            HarnessEvent::Exited { code: Some(0) },
        );
        let dead = sup.status("Nibble").unwrap();
        assert_eq!(
            dead.state,
            AgentState::Failed,
            "a genuinely dead agent must reach a terminal state promptly, not linger paused"
        );
        assert!(
            !dead.state.is_live(),
            "a dead agent must release its drain slot"
        );
        assert!(
            !dead.crashed,
            "the harness DID report for a paused record; `crashed` means it never did"
        );
        assert_eq!(
            dead.result.as_deref(),
            Some("reading the diff; verdict still pending"),
            "the withheld turn text is what gets published; it must not be overwritten"
        );
    }

    fn record(repo: &Path, branch: Option<&str>) -> AgentRecord {
        let now = Utc::now();
        AgentRecord {
            name: "Nibble".into(),
            spawn: None,
            role: "rat".into(),
            coordination: None,
            harness: "fake".into(),
            permission_mode: None,
            model: None,
            repo_root: repo.to_path_buf(),
            repo_name: "repo".into(),
            task: Some("t".into()),
            branch: branch.map(String::from),
            fork_point: None,
            worktree: Some(repo.to_path_buf()),
            target_branch: "main".into(),
            parent: None,
            workflow_instance: None,
            review: None,
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
            liveness: Default::default(),
            transport_outage: None,
            recovery: None,
            recovery_receipt: None,
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

    /// Commit-count awareness at the done-gate call site
    /// (TKT-01M0CTC4DPFV7Q2642AZH354BV): a branch that never diverged from
    /// its target trivially satisfies `is_ancestor`, so a naive check would
    /// let `rk done` through for a rat that committed nothing. The
    /// merge-mode gate (`branch_verified_merged`) refuses it: rk's own
    /// merges are always `--no-ff`, so there is no legitimate
    /// fast-forward case to protect here. The push-branch gate
    /// (`remote_branch_merged_or_gone`) is deliberately NOT fixed the same
    /// way — a forge merge is very often a fast-forward, indistinguishable
    /// from "never diverged" from ref state alone; see
    /// `Repo::remote_branch_merged_or_gone`'s doc comment. That gap is
    /// tracked as a follow-up rather than fixed here unsafely.
    #[test]
    fn ticket_undelivered_reason_refuses_an_empty_branch() {
        let repo_dir = tempfile::tempdir().unwrap();
        let p = repo_dir.path();
        init_repo(p);
        git(p, &["checkout", "-b", "nowork", "main"]);
        git(p, &["checkout", "main"]);
        let repo = Repo::discover(p).unwrap();
        let fork_point = repo.rev_parse("main").unwrap();

        let merge_policy = rk_workflow::RepositoryPolicy::default();
        assert_eq!(merge_policy.delivery.mode, DeliveryMode::Merge);
        assert!(
            ticket_undelivered_reason(&merge_policy, &repo, "nowork", "main", None, true).is_some(),
            "an empty branch must not read as delivered under merge mode"
        );

        let mut push_policy = merge_policy.clone();
        push_policy.delivery.mode = DeliveryMode::PushBranch;
        assert!(
            ticket_undelivered_reason(
                &push_policy,
                &repo,
                "nowork",
                "main",
                Some(&fork_point),
                true,
            )
            .is_some(),
            "a missing remote ref must not make a never-diverged branch read delivered"
        );

        // A branch that actually made a commit still hasn't merged yet, so it
        // is refused too — confirming the fix doesn't also refuse real,
        // pending work as "empty".
        git(p, &["checkout", "-b", "work", "main"]);
        std::fs::write(p.join("g"), "1\n").unwrap();
        git(p, &["add", "g"]);
        git(p, &["commit", "-m", "work"]);
        git(p, &["checkout", "main"]);
        let repo = Repo::discover(p).unwrap();
        assert!(
            ticket_undelivered_reason(&merge_policy, &repo, "work", "main", None, true).is_some(),
            "unmerged real work is also not yet delivered"
        );

        // Once genuinely merged (target advances past the branch), the gate
        // clears.
        git(p, &["merge", "--no-ff", "-m", "merge", "work"]);
        let repo = Repo::discover(p).unwrap();
        assert!(
            ticket_undelivered_reason(&merge_policy, &repo, "work", "main", None, true).is_none(),
            "a genuinely merged branch must clear the done-gate"
        );
    }

    /// Lifecycle cleanup is never delivery: even a content-free duplicate
    /// whose branch already matches the target must not close a ticket merely
    /// because the agent was dismissed.
    #[tokio::test]
    async fn dismiss_never_closes_a_ticket_for_a_content_free_duplicate_branch() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let sup = supervisor(home.path());
        let p = repo_dir.path();

        let ticket = sup
            .tickets
            .create(crate::tickets::NewTicket {
                title: "test".into(),
                body: None,
                scope: None,
                parent: None,
                priority: "normal".into(),
                labels: vec![],
                depends_on: vec![],
                created_by: None,
                coalesce_key: None,
            })
            .await
            .unwrap();
        let task_id = ticket.identity.clone();
        sup.tickets
            .set_status(&task_id, "in_progress")
            .await
            .unwrap();

        // A duplicate branch cut from main with no commits — already
        // ancestor-equivalent of target, exactly the trivial no-op-merge case.
        git(p, &["checkout", "-b", "dup", "main"]);
        git(p, &["checkout", "main"]);

        let mut rec = record(p, Some("dup"));
        rec.name = "Duplicate".into();
        rec.task = Some(task_id.clone());
        rec.worktree = None; // nothing to tear down for this test
        sup.lock_registry().insert(rec).unwrap();

        sup.dismiss("Duplicate", false).await.unwrap();

        let ticket = sup.tickets.get(&task_id).unwrap().unwrap();
        assert_ne!(
            ticket.payload["status"],
            json!("closed"),
            "a content-free duplicate merge must not close the ticket"
        );
    }

    /// The TKT-146 scenario, closed structurally
    /// (`docs/2026-08-17-tkt-c1-generation-identity.md`, consumers B3/B4): a
    /// fan-out's dismiss must not act on whoever currently holds the captured
    /// name if that is a different generation than the one fanned out over —
    /// the shape that let a `dismiss_all` SIGTERM a live rat one second into
    /// its task because a namesake predecessor satisfied the read behind it.
    #[tokio::test]
    async fn dismiss_checked_refuses_a_namesake_that_is_not_the_expected_generation() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let sup = supervisor(home.path());

        let fanned_out_spawn = rk_core::id::SpawnId::new();
        let mut live = record(repo.path(), None);
        live.name = "Nibble".into();
        // A DIFFERENT generation now holds the name "Nibble" than the one this
        // caller's fan-out captured.
        live.spawn = Some(rk_core::id::SpawnId::new());
        assert_ne!(live.spawn, Some(fanned_out_spawn));
        sup.lock_registry().insert(live).unwrap();

        let outcome = sup
            .dismiss_checked("Nibble", Some(fanned_out_spawn), true)
            .await;
        let error = outcome.expect_err("must refuse to dismiss a different generation");
        assert!(
            error.to_string().contains("dismiss target mismatch"),
            "unexpected error: {error}"
        );
    }

    /// The companion case: when the live record IS the expected generation,
    /// `dismiss_checked` must behave exactly like `dismiss` — the guard is a
    /// pure precondition, not an extra restriction on the happy path.
    #[tokio::test]
    async fn dismiss_checked_proceeds_when_the_generation_matches() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let sup = supervisor(home.path());

        let spawn = rk_core::id::SpawnId::new();
        let mut live = record(repo.path(), None);
        live.name = "Nibble".into();
        live.spawn = Some(spawn);
        // No worktree/branch to reconcile — isolates the assertion to the
        // generation guard itself, not the rest of dismiss's git plumbing.
        live.worktree = None;
        sup.lock_registry().insert(live).unwrap();

        let outcome = sup.dismiss_checked("Nibble", Some(spawn), true).await;
        assert!(
            outcome.is_ok(),
            "the expected generation must not be refused: {outcome:?}"
        );
    }

    /// C1 (docs/2026-08-17-tkt-c1-generation-identity.md): `declared_done`
    /// keys on the minted `SpawnId` once one exists, so a namesake
    /// predecessor's `task_done` — durable, and unbounded by name alone —
    /// cannot satisfy the current generation's completion gate. Companion to
    /// `dismiss_checked_refuses_a_namesake_that_is_not_the_expected_generation`,
    /// same defect class (TKT-146), the producer side of the read instead of
    /// the fan-out side.
    #[test]
    fn declared_done_keys_on_spawn_and_rejects_a_namesake_predecessors_tuple() {
        let home = tempfile::tempdir().unwrap();
        let sup = supervisor(home.path());
        let generation = Utc::now();

        let predecessor_spawn = rk_core::id::SpawnId::new();
        let mine_spawn = rk_core::id::SpawnId::new();

        sup.space
            .out(Tuple::new(
                Category::Event,
                "repo",
                "task_done",
                "castle",
                json!({"agent": "Nibble", "spawn": predecessor_spawn.to_string()}),
            ))
            .unwrap();
        assert!(
            !sup.declared_done("Nibble", generation, Some(mine_spawn)),
            "a namesake predecessor's task_done must not satisfy this generation's gate"
        );

        sup.space
            .out(Tuple::new(
                Category::Event,
                "repo",
                "task_done",
                "castle",
                json!({"agent": "Nibble", "spawn": mine_spawn.to_string()}),
            ))
            .unwrap();
        assert!(
            sup.declared_done("Nibble", generation, Some(mine_spawn)),
            "this generation's own task_done must satisfy the gate"
        );

        // No minted id (pre-migration record): falls back to the name+floor
        // predicate, unaffected by either spawn-keyed tuple above.
        assert!(
            sup.declared_done("Nibble", generation, None),
            "the name+floor fallback must still see a task_done written after the floor"
        );
    }

    #[test]
    fn classify_diff_buckets_by_size_and_shape() {
        assert_eq!(
            classify_diff(&[], 0),
            "trivial",
            "an empty diff is trivial, not doc-only"
        );
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
        assert_eq!(
            classify_diff(&["a".into()], 41),
            "small",
            "41 lines exceeds the trivial line cap"
        );
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
        assert_eq!(
            classify_diff(&["a".into()], 401),
            "large",
            "401 lines exceeds the small line cap"
        );
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

    /// `TransportBreakers::is_open` used to be consulted only by
    /// `transport_retry_sweep` — an ordinary NEW spawn for a provider whose
    /// castle-wide breaker is open sailed straight through admission. This
    /// proves `spawn` now refuses it up front, before any WIP/lane slot or
    /// registry row is created (so nothing needs releasing on the refusal
    /// path — no reservation was ever made).
    #[test]
    fn spawn_refuses_admission_while_the_providers_breaker_is_open() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let sup = supervisor(home.path());

        sup.lock_transport_breakers()
            .record_failure("fake", 1, Utc::now());

        let err = sup
            .spawn(spawn_params(repo.path(), "TKT-breaker"), 0)
            .expect_err("a tripped provider breaker must refuse the spawn");
        assert!(
            err.to_string()
                .contains(TRANSPORT_BREAKER_OPEN_REFUSED_PREFIX),
            "unexpected refusal message: {err}"
        );
        assert!(err.to_string().contains("fake"));
        assert!(
            sup.lock_registry().list().is_empty(),
            "a refused spawn must not create any registry row (no WIP/budget consumed)"
        );
    }

    /// Regression for the race the two sweeps used to have: a record mid
    /// pre-work transport-outage episode is `Failed` (ordinary state) AND
    /// `is_auto_respawn_candidate` (also ordinary), but it is deliberately
    /// never a `RespawnState` entry — see `record_transport_outage`. Before
    /// `respawn_sweep` excluded `transport_outage.is_some()` records,
    /// `decide_respawn` saw `None` for that name and returned an immediate
    /// `RespawnDecision::Respawn`, bypassing `transport_retry_sweep`'s
    /// backoff, jitter, and castle-wide circuit breaker entirely. This test
    /// configures `respawn_sweep` to fire on its very first tick (no
    /// backoff, cap 1) and proves it still leaves the record untouched.
    #[test]
    fn respawn_sweep_excludes_transport_outage_records() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let sup = supervisor(home.path());

        let mut rec = record(repo.path(), Some("main"));
        rec.transport_outage = Some(crate::agents::TransportOutageState {
            provider: "claude".into(),
            class: rk_harness::TransportClass::Unavailable,
            retryable: true,
            attempts: 1,
            last_failure_at: Utc::now(),
            evidence: "503 Service Unavailable".into(),
            ceiling_hit: false,
            circuit_refused: false,
        });
        sup.lock_registry().insert(rec.clone()).unwrap();

        let cfg = SupervisorConfig {
            respawn_enabled: true,
            respawn_max_attempts: 1,
            respawn_backoff_secs: 0,
            ..SupervisorConfig::default()
        };
        let sinks = rk_core::notify::SinkRegistry::default();
        sup.respawn_sweep(&cfg, &sinks);

        assert!(
            sup.lock_respawn_state().get(&rec.name).is_none(),
            "a transport-outage record must never become a RespawnState candidate \
             for the generic crash-loop sweep"
        );
        let after = sup.status(&rec.name).unwrap();
        assert_eq!(
            after.state,
            AgentState::Failed,
            "respawn_sweep must not relaunch a transport-outage record — that is \
             transport_retry_sweep's job, gated by the breaker"
        );
        assert!(after.pid.is_none());
    }

    /// A `Running` record with committed work (a branch with at least one
    /// commit past its fork point) and live-verifier evidence in its
    /// `liveness` snapshot, whose harness dies with transport-shaped
    /// stderr — fault-injected through the SAME `handle_event` path a real
    /// process death drives, not by calling the detector directly.
    fn committed_work_record_with_live_verifier_evidence(
        repo: &Path,
        fork_point: String,
        branch: &str,
    ) -> AgentRecord {
        let mut rec = record(repo, Some(branch));
        rec.fork_point = Some(fork_point);
        rec.state = AgentState::Running;
        rec.session_id = Some("provider-sess-1".into());
        // Live verifier evidence: a local check (`cargo test`/`rk verify`)
        // was still advancing right up to the moment the harness died.
        rec.liveness = crate::agents::LivenessObservation {
            session: Some(rec.spawn_id()),
            output_fingerprint: 777,
            output_changed_at: Some(Utc::now()),
            reconnect_events: 1,
            ceiling_started_at: None,
        };
        rec.stderr_tail = Some("fatal: connection refused while contacting api\n".into());
        rec
    }

    /// End-to-end fault injection for TKT-01M0HNDJ7AS9F1A3W22FRCC63N: proves
    /// the durable recovery record survives a simulated daemon restart (two
    /// `Supervisor`s over the same home directory) with its liveness
    /// evidence intact, and that continuation is at-most-once — a replayed
    /// `action_id` returns the SAME outcome instead of double-launching, and
    /// a different `action_id` after acknowledgement is refused outright.
    #[tokio::test]
    async fn post_commit_transport_outage_recovers_across_a_daemon_restart_with_at_most_once_continuation(
    ) {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let p = repo.path();
        let fork_point = Repo::discover(p).unwrap().rev_parse("HEAD").unwrap();

        git(p, &["checkout", "-b", "rat/nibble/tkt-1", "main"]);
        std::fs::write(p.join("work"), "done\n").unwrap();
        git(p, &["add", "."]);
        git(p, &["commit", "-m", "committed work"]);
        git(p, &["checkout", "main"]);

        let rec =
            committed_work_record_with_live_verifier_evidence(p, fork_point, "rat/nibble/tkt-1");
        let name = rec.name.clone();
        let spawn = rec.spawn_id();
        let generation = rec.created_at;

        let sup1 = supervisor(home.path());
        sup1.lock_registry().insert(rec).unwrap();

        // A late `Exited` from a SUPERSEDED session must not write recovery
        // state: the session tracked for `name` is a DIFFERENT token than
        // the one this event names.
        let superseding = rk_core::id::SpawnId::new();
        let stale_session = rk_core::id::SpawnId::new();
        sup1.lock_session_tokens().insert(name.clone(), superseding);
        sup1.handle_event(
            &name,
            generation,
            spawn,
            stale_session,
            HarnessEvent::Exited { code: Some(1) },
        );
        assert!(
            sup1.status(&name).unwrap().recovery.is_none(),
            "a late Exited from a superseded session must not overwrite the active \
             generation's recovery state"
        );

        // The real fault: the CURRENT session's harness dies post-commit
        // with transport-shaped stderr.
        sup1.lock_registry()
            .update(&name, |r| {
                r.state = AgentState::Running; // undo the no-op stale attempt above
                r.pid = Some(4242);
            })
            .unwrap();
        let session = rk_core::id::SpawnId::new();
        sup1.lock_session_tokens().insert(name.clone(), session);
        sup1.handle_event(
            &name,
            generation,
            spawn,
            session,
            HarnessEvent::Exited { code: Some(1) },
        );

        let detected = sup1.status(&name).unwrap();
        let recovery = detected
            .recovery
            .as_ref()
            .expect("post-commit transport outage must be detected and recorded");
        assert_eq!(recovery.branch, "rat/nibble/tkt-1");
        assert_eq!(recovery.class, rk_harness::TransportClass::Unavailable);
        assert_eq!(
            recovery.liveness.output_fingerprint, 777,
            "live-verifier liveness evidence must be preserved verbatim"
        );
        assert_eq!(recovery.liveness.reconnect_events, 1);
        assert!(recovery.ack.is_none());
        assert_eq!(detected.state, AgentState::Failed);
        assert!(
            detected.pid.is_none(),
            "WIP must already be released at Exited"
        );

        // Simulated daemon restart: a FRESH Supervisor over the SAME home
        // directory must see exactly what the dead process recorded.
        drop(sup1);
        let sup2 = supervisor(home.path());
        let after_restart = sup2.status(&name).unwrap();
        assert_eq!(
            after_restart.recovery.as_ref().map(|r| &r.head),
            Some(&recovery.head),
            "the recovery record must survive a daemon restart intact"
        );

        // Continuation: same provider ("fake"), no target override.
        let outcome = sup2
            .continue_recovery(&name, "action-1", None)
            .expect("continuation must succeed for a fresh, unacknowledged recovery");
        assert!(
            matches!(
                outcome,
                crate::agents::RecoveryOutcome::ResumedSameProvider { .. }
            ),
            "same-provider continuation must resume, not route to an alternate: {outcome:?}"
        );
        assert_eq!(sup2.status(&name).unwrap().state, AgentState::Running);

        // Duplicate replay: the SAME action_id must return the SAME
        // outcome without erroring (no second live owner is ever attempted).
        let replayed = sup2
            .continue_recovery(&name, "action-1", None)
            .expect("a replayed action_id must be safe to retry");
        assert_eq!(
            replayed, outcome,
            "a replay must return the identical recorded outcome"
        );

        // A DIFFERENT action_id after acknowledgement must be refused —
        // acknowledgement makes continuation at-most-once.
        let conflict = sup2.continue_recovery(&name, "action-2", None);
        assert!(
            conflict.is_err(),
            "a different action_id after acknowledgement must be refused, not re-acted"
        );
    }

    /// `target_harness` routes continuation to a configured alternate
    /// harness in the SAME worktree rather than resuming the dead
    /// provider's session — no session id is portable across providers, so
    /// this must be a fresh turn, not a resume.
    #[tokio::test]
    async fn continue_recovery_routes_to_a_configured_alternate_harness() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let p = repo.path();
        let fork_point = Repo::discover(p).unwrap().rev_parse("HEAD").unwrap();

        git(p, &["checkout", "-b", "rat/nibble/tkt-2", "main"]);
        std::fs::write(p.join("work"), "done\n").unwrap();
        git(p, &["add", "."]);
        git(p, &["commit", "-m", "committed work"]);
        git(p, &["checkout", "main"]);

        // The dying provider is recorded as "claude" (never actually
        // launched here); the configured alternate is "fake" — the only
        // kind guaranteed to launch in a sandboxed test.
        let mut rec =
            committed_work_record_with_live_verifier_evidence(p, fork_point, "rat/nibble/tkt-2");
        rec.harness = "claude".into();
        let name = rec.name.clone();
        let spawn = rec.spawn_id();
        let generation = rec.created_at;

        let sup = supervisor(home.path());
        sup.lock_registry().insert(rec).unwrap();
        let session = rk_core::id::SpawnId::new();
        sup.lock_session_tokens().insert(name.clone(), session);
        sup.handle_event(
            &name,
            generation,
            spawn,
            session,
            HarnessEvent::Exited { code: Some(1) },
        );
        assert!(sup.status(&name).unwrap().recovery.is_some());

        let outcome = sup
            .continue_recovery(&name, "alt-action-1", Some("fake"))
            .expect("alternate-provider continuation must succeed");
        match &outcome {
            crate::agents::RecoveryOutcome::ContinuedAlternateProvider { harness, .. } => {
                assert_eq!(harness, "fake");
            }
            other => panic!("expected ContinuedAlternateProvider, got {other:?}"),
        }
        assert_eq!(
            sup.status(&name).unwrap().harness,
            "fake",
            "the record's harness must reflect the alternate it actually continued under"
        );
    }

    /// Terminal-failure path: an operator/policy decision to NOT continue a
    /// parked recovery must release its WIP slot cleanly and permanently —
    /// `respawn_sweep` must never resurrect it, even configured to fire on
    /// its very first tick with no backoff (mirrors
    /// `respawn_sweep_excludes_transport_outage_records`).
    #[test]
    fn abandoned_recovery_stays_excluded_from_respawn_sweep_forever() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let p = repo.path();
        let fork_point = Repo::discover(p).unwrap().rev_parse("HEAD").unwrap();
        git(p, &["checkout", "-b", "rat/nibble/tkt-3", "main"]);
        std::fs::write(p.join("work"), "done\n").unwrap();
        git(p, &["add", "."]);
        git(p, &["commit", "-m", "committed work"]);
        git(p, &["checkout", "main"]);

        let mut rec =
            committed_work_record_with_live_verifier_evidence(p, fork_point, "rat/nibble/tkt-3");
        rec.state = AgentState::Failed;
        rec.pid = None;
        rec.recovery = Some(crate::agents::RecoveryRecord {
            ticket: rec.task.clone(),
            branch: "rat/nibble/tkt-3".into(),
            head: "deadbeef".repeat(5),
            session_id: rec.session_id.clone(),
            spawn: rec.spawn_id(),
            liveness: rec.liveness.clone(),
            budget_remaining_usd: None,
            provider: "fake".into(),
            class: rk_harness::TransportClass::Unavailable,
            evidence: "connection refused".into(),
            detected_at: Utc::now(),
            ack: None,
        });
        let name = rec.name.clone();

        let sup = supervisor(home.path());
        sup.lock_registry().insert(rec).unwrap();

        let outcome = sup
            .abandon_recovery(&name, "give-up-1")
            .expect("abandoning a fresh recovery must succeed");
        assert_eq!(outcome, crate::agents::RecoveryOutcome::Abandoned);

        // Duplicate replay of the SAME action_id must return the same
        // terminal outcome, not error.
        assert_eq!(
            sup.abandon_recovery(&name, "give-up-1").unwrap(),
            crate::agents::RecoveryOutcome::Abandoned
        );
        // A different action_id after acknowledgement must be refused.
        assert!(sup.abandon_recovery(&name, "give-up-2").is_err());

        let cfg = SupervisorConfig {
            respawn_enabled: true,
            respawn_max_attempts: 1,
            respawn_backoff_secs: 0,
            ..SupervisorConfig::default()
        };
        let sinks = rk_core::notify::SinkRegistry::default();
        sup.respawn_sweep(&cfg, &sinks);

        let after = sup.status(&name).unwrap();
        assert_eq!(
            after.state,
            AgentState::Failed,
            "an abandoned recovery must never be auto-respawned"
        );
        assert!(after.pid.is_none(), "WIP must stay released permanently");
    }

    /// Regression for the gap Emmental-12 flagged in review (artifact
    /// 01M0RRQA5CBNZ0BFQ88A56V8YZ): unlike `abandon_recovery`, a CONTINUED
    /// recovery must not stay stamped on the record forever, or a name that
    /// resumed once can never have its NEXT post-commit outage detected
    /// again. Builds the record exactly as `continue_recovery` leaves it —
    /// `recovery.ack` set to a non-`Abandoned` outcome — then fires the same
    /// `Started` handshake `handle_event` would see from the relaunched
    /// harness, and proves a SECOND, later transport-shaped death on the
    /// same name is detected fresh rather than swallowed by the stale
    /// record.
    #[test]
    fn continued_recovery_clears_on_proof_of_life_and_reenables_detection() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let p = repo.path();
        let fork_point = Repo::discover(p).unwrap().rev_parse("HEAD").unwrap();
        git(p, &["checkout", "-b", "rat/nibble/tkt-4", "main"]);
        std::fs::write(p.join("work"), "done\n").unwrap();
        git(p, &["add", "."]);
        git(p, &["commit", "-m", "committed work"]);
        git(p, &["checkout", "main"]);

        let mut rec =
            committed_work_record_with_live_verifier_evidence(p, fork_point, "rat/nibble/tkt-4");
        let name = rec.name.clone();
        let spawn = rec.spawn_id();
        let generation = rec.created_at;
        rec.recovery = Some(crate::agents::RecoveryRecord {
            ticket: rec.task.clone(),
            branch: "rat/nibble/tkt-4".into(),
            head: "deadbeef".repeat(5),
            session_id: rec.session_id.clone(),
            spawn,
            liveness: rec.liveness.clone(),
            budget_remaining_usd: None,
            provider: "fake".into(),
            class: rk_harness::TransportClass::Unavailable,
            evidence: "connection refused".into(),
            detected_at: Utc::now(),
            ack: Some(crate::agents::RecoveryAck {
                action_id: "action-1".into(),
                outcome: crate::agents::RecoveryOutcome::ResumedSameProvider { new_spawn: spawn },
                acknowledged_at: Utc::now(),
            }),
        });
        // `continue_recovery` clears `stderr_tail` before relaunching; a
        // fresh Started handshake must not see the outage evidence that
        // parked the FIRST recovery.
        rec.stderr_tail = None;

        let sup = supervisor(home.path());
        sup.lock_registry().insert(rec).unwrap();
        let session = rk_core::id::SpawnId::new();
        sup.lock_session_tokens().insert(name.clone(), session);

        sup.handle_event(
            &name,
            generation,
            spawn,
            session,
            HarnessEvent::Started {
                session_id: Some("resumed-sess-1".into()),
            },
        );
        assert!(
            sup.status(&name).unwrap().recovery.is_none(),
            "a CONTINUED recovery must clear on its next proof-of-life, or this name is \
             stranded forever"
        );

        // A second, later transport-shaped death on the SAME name.
        sup.lock_registry()
            .update(&name, |r| {
                r.state = AgentState::Running;
                r.pid = Some(4343);
                r.stderr_tail = Some("fatal: connection refused while contacting api\n".into());
            })
            .unwrap();
        sup.handle_event(
            &name,
            generation,
            spawn,
            session,
            HarnessEvent::Exited { code: Some(1) },
        );
        let redetected = sup.status(&name).unwrap();
        assert!(
            redetected
                .recovery
                .as_ref()
                .is_some_and(|r| r.ack.is_none()),
            "a second, unrelated post-commit outage on the same name must be detected \
             fresh, not blocked by the first (already-continued) recovery: {:?}",
            redetected.recovery
        );
    }

    /// Companion to `continued_recovery_clears_on_proof_of_life_and_reenables_detection`:
    /// the same proof-of-life clear must also restore ORDINARY auto-respawn
    /// eligibility — `respawn_sweep`'s `recovery.is_none()` filter must not
    /// exclude a name forever just because it once continued a recovery.
    /// `#[tokio::test]` because a `Respawn` decision drives `self.respawn`,
    /// which launches the fake harness for real.
    #[tokio::test]
    async fn continued_recovery_clears_on_proof_of_life_and_reenables_ordinary_respawn() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());

        let mut rec = record(repo.path(), Some("main"));
        rec.state = AgentState::Running;
        rec.pid = Some(4343);
        let name = rec.name.clone();
        let spawn = rec.spawn_id();
        let generation = rec.created_at;
        rec.recovery = Some(crate::agents::RecoveryRecord {
            ticket: rec.task.clone(),
            branch: "rat/nibble/tkt-5".into(),
            head: "deadbeef".repeat(5),
            session_id: None,
            spawn,
            liveness: rec.liveness.clone(),
            budget_remaining_usd: None,
            provider: "fake".into(),
            class: rk_harness::TransportClass::Unavailable,
            evidence: "connection refused".into(),
            detected_at: Utc::now(),
            ack: Some(crate::agents::RecoveryAck {
                action_id: "action-1".into(),
                outcome: crate::agents::RecoveryOutcome::ResumedSameProvider { new_spawn: spawn },
                acknowledged_at: Utc::now(),
            }),
        });

        let sup = supervisor(home.path());
        sup.lock_registry().insert(rec).unwrap();
        let session = rk_core::id::SpawnId::new();
        sup.lock_session_tokens().insert(name.clone(), session);

        sup.handle_event(
            &name,
            generation,
            spawn,
            session,
            HarnessEvent::Started {
                session_id: Some("resumed-sess-2".into()),
            },
        );
        assert!(sup.status(&name).unwrap().recovery.is_none());

        // An ORDINARY crash later — no transport-shaped stderr — must be
        // treated like any other crash: eligible for the auto-respawn
        // sweep, not permanently excluded by the first (already-continued)
        // recovery.
        sup.handle_event(
            &name,
            generation,
            spawn,
            session,
            HarnessEvent::Exited { code: Some(1) },
        );
        let after_crash = sup.status(&name).unwrap();
        assert_eq!(after_crash.state, AgentState::Failed);
        assert!(after_crash.recovery.is_none());

        let cfg = SupervisorConfig {
            respawn_enabled: true,
            respawn_max_attempts: 1,
            respawn_backoff_secs: 0,
            ..SupervisorConfig::default()
        };
        let sinks = rk_core::notify::SinkRegistry::default();
        sup.respawn_sweep(&cfg, &sinks);

        assert!(
            sup.lock_respawn_state().get(&name).is_some(),
            "a name that once continued a recovery must be an ordinary auto-respawn \
             candidate again after its next crash, not excluded forever"
        );
    }

    /// TKT-01M0S28V7XQ17F0C3SDNGC4PQA: the at-most-once contract on a
    /// recovery ack must outlive the `RecoveryRecord` it was made against.
    /// Builds a record exactly as `continue_recovery` leaves one after a
    /// same-provider resume (`recovery.ack = Some(ResumedSameProvider)`),
    /// fires the `Started` handshake that clears it
    /// (`continued_recovery_clears_on_proof_of_life_and_reenables_detection`
    /// already proves the record disappears), then proves a caller replaying
    /// the SAME `action_id` after that point still gets the recorded
    /// outcome back, and a DIFFERENT `action_id` is still refused — not
    /// silently treated as "no pending recovery, nothing to conflict with".
    #[test]
    fn continue_recovery_replays_ack_after_started_clears_the_record_same_provider() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());

        let mut rec = record(repo.path(), Some("main"));
        let name = rec.name.clone();
        let spawn = rec.spawn_id();
        let generation = rec.created_at;
        let outcome = crate::agents::RecoveryOutcome::ResumedSameProvider { new_spawn: spawn };
        rec.recovery = Some(crate::agents::RecoveryRecord {
            ticket: rec.task.clone(),
            branch: "rat/nibble/tkt-6".into(),
            head: "deadbeef".repeat(5),
            session_id: None,
            spawn,
            liveness: rec.liveness.clone(),
            budget_remaining_usd: None,
            provider: "fake".into(),
            class: rk_harness::TransportClass::Unavailable,
            evidence: "connection refused".into(),
            detected_at: Utc::now(),
            ack: Some(crate::agents::RecoveryAck {
                action_id: "action-1".into(),
                outcome: outcome.clone(),
                acknowledged_at: Utc::now(),
            }),
        });

        let sup = supervisor(home.path());
        sup.lock_registry().insert(rec).unwrap();
        let session = rk_core::id::SpawnId::new();
        sup.lock_session_tokens().insert(name.clone(), session);

        sup.handle_event(
            &name,
            generation,
            spawn,
            session,
            HarnessEvent::Started {
                session_id: Some("resumed-sess-6".into()),
            },
        );
        assert!(
            sup.status(&name).unwrap().recovery.is_none(),
            "Started proof-of-life must still clear the active record"
        );

        let replayed = sup
            .continue_recovery(&name, "action-1", None)
            .expect("the SAME action_id must still replay after Started clears the record");
        assert_eq!(
            replayed, outcome,
            "a post-clear replay must return the identical recorded outcome"
        );

        let conflict = sup.continue_recovery(&name, "action-2", None);
        assert!(
            conflict.is_err(),
            "a DIFFERENT action_id after Started clears the record must still be refused, \
             not treated as a fresh, unacknowledged recovery"
        );
    }

    /// Companion to the same-provider case above for
    /// `ContinuedAlternateProvider`: an alternate-harness continuation's ack
    /// must survive the same `Started` clear the same way.
    #[test]
    fn continue_recovery_replays_ack_after_started_clears_the_record_alternate_provider() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());

        let mut rec = record(repo.path(), Some("main"));
        rec.harness = "codex".into();
        let name = rec.name.clone();
        let spawn = rec.spawn_id();
        let generation = rec.created_at;
        let outcome = crate::agents::RecoveryOutcome::ContinuedAlternateProvider {
            harness: "fake".into(),
            new_spawn: spawn,
        };
        rec.recovery = Some(crate::agents::RecoveryRecord {
            ticket: rec.task.clone(),
            branch: "rat/nibble/tkt-7".into(),
            head: "deadbeef".repeat(5),
            session_id: None,
            spawn,
            liveness: rec.liveness.clone(),
            budget_remaining_usd: None,
            provider: "codex".into(),
            class: rk_harness::TransportClass::Unavailable,
            evidence: "connection refused".into(),
            detected_at: Utc::now(),
            ack: Some(crate::agents::RecoveryAck {
                action_id: "alt-action-1".into(),
                outcome: outcome.clone(),
                acknowledged_at: Utc::now(),
            }),
        });

        let sup = supervisor(home.path());
        sup.lock_registry().insert(rec).unwrap();
        let session = rk_core::id::SpawnId::new();
        sup.lock_session_tokens().insert(name.clone(), session);

        sup.handle_event(
            &name,
            generation,
            spawn,
            session,
            HarnessEvent::Started {
                session_id: Some("resumed-sess-7".into()),
            },
        );
        assert!(sup.status(&name).unwrap().recovery.is_none());

        let replayed = sup
            .continue_recovery(&name, "alt-action-1", Some("fake"))
            .expect("the SAME action_id must still replay after Started clears the record");
        assert_eq!(replayed, outcome);

        let conflict = sup.continue_recovery(&name, "alt-action-2", Some("fake"));
        assert!(
            conflict.is_err(),
            "a DIFFERENT action_id after Started clears the record must still be refused"
        );
    }

    /// The `action_id`/refusal half of the contract must also survive a
    /// daemon restart between the `Started` clear and the duplicate call —
    /// same discipline `RecoveryRecord::ack` already had before it was
    /// cleared, now proven for the tombstone that replaces it.
    #[test]
    fn recovery_receipt_survives_started_clear_across_a_daemon_restart() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());

        let mut rec = record(repo.path(), Some("main"));
        let name = rec.name.clone();
        let spawn = rec.spawn_id();
        let generation = rec.created_at;
        let outcome = crate::agents::RecoveryOutcome::ResumedSameProvider { new_spawn: spawn };
        rec.recovery = Some(crate::agents::RecoveryRecord {
            ticket: rec.task.clone(),
            branch: "rat/nibble/tkt-8".into(),
            head: "deadbeef".repeat(5),
            session_id: None,
            spawn,
            liveness: rec.liveness.clone(),
            budget_remaining_usd: None,
            provider: "fake".into(),
            class: rk_harness::TransportClass::Unavailable,
            evidence: "connection refused".into(),
            detected_at: Utc::now(),
            ack: Some(crate::agents::RecoveryAck {
                action_id: "restart-action-1".into(),
                outcome: outcome.clone(),
                acknowledged_at: Utc::now(),
            }),
        });

        let sup1 = supervisor(home.path());
        sup1.lock_registry().insert(rec).unwrap();
        let session = rk_core::id::SpawnId::new();
        sup1.lock_session_tokens().insert(name.clone(), session);
        sup1.handle_event(
            &name,
            generation,
            spawn,
            session,
            HarnessEvent::Started {
                session_id: Some("resumed-sess-8".into()),
            },
        );
        assert!(sup1.status(&name).unwrap().recovery.is_none());

        // Simulated daemon restart: a FRESH Supervisor over the SAME home
        // must see exactly what the dead process recorded — the tombstone
        // included, since `recovery` itself is already gone.
        drop(sup1);
        let sup2 = supervisor(home.path());
        assert!(sup2.status(&name).unwrap().recovery.is_none());

        let replayed = sup2
            .continue_recovery(&name, "restart-action-1", None)
            .expect("the SAME action_id must replay after a restart too");
        assert_eq!(replayed, outcome);

        let conflict = sup2.continue_recovery(&name, "restart-action-2", None);
        assert!(
            conflict.is_err(),
            "a DIFFERENT action_id must still be refused after a restart"
        );
    }

    /// Extends `continued_recovery_clears_on_proof_of_life_and_reenables_detection`:
    /// a later, unrelated post-commit outage on the SAME generation must be
    /// not just *detected* fresh but genuinely *continuable* fresh — the
    /// tombstone left by the FIRST episode's ack must not leak into the
    /// second episode's own (freshly unacknowledged) `RecoveryRecord`, which
    /// takes priority over the tombstone by construction (`continue_recovery`
    /// only ever consults the receipt when `recovery` is `None`).
    #[tokio::test]
    async fn later_recovery_on_same_generation_supersedes_the_receipt_and_can_be_freshly_acknowledged(
    ) {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let p = repo.path();
        let fork_point = Repo::discover(p).unwrap().rev_parse("HEAD").unwrap();
        git(p, &["checkout", "-b", "rat/nibble/tkt-9", "main"]);
        std::fs::write(p.join("work"), "done\n").unwrap();
        git(p, &["add", "."]);
        git(p, &["commit", "-m", "committed work"]);
        git(p, &["checkout", "main"]);

        let mut rec =
            committed_work_record_with_live_verifier_evidence(p, fork_point, "rat/nibble/tkt-9");
        let name = rec.name.clone();
        let spawn = rec.spawn_id();
        let generation = rec.created_at;
        rec.recovery = Some(crate::agents::RecoveryRecord {
            ticket: rec.task.clone(),
            branch: "rat/nibble/tkt-9".into(),
            head: "deadbeef".repeat(5),
            session_id: rec.session_id.clone(),
            spawn,
            liveness: rec.liveness.clone(),
            budget_remaining_usd: None,
            provider: "fake".into(),
            class: rk_harness::TransportClass::Unavailable,
            evidence: "connection refused".into(),
            detected_at: Utc::now(),
            ack: Some(crate::agents::RecoveryAck {
                action_id: "episode-1-action".into(),
                outcome: crate::agents::RecoveryOutcome::ResumedSameProvider { new_spawn: spawn },
                acknowledged_at: Utc::now(),
            }),
        });
        rec.stderr_tail = None;

        let sup = supervisor(home.path());
        sup.lock_registry().insert(rec).unwrap();
        let session = rk_core::id::SpawnId::new();
        sup.lock_session_tokens().insert(name.clone(), session);

        sup.handle_event(
            &name,
            generation,
            spawn,
            session,
            HarnessEvent::Started {
                session_id: Some("resumed-sess-9".into()),
            },
        );
        assert!(sup.status(&name).unwrap().recovery.is_none());

        // A second, later transport-shaped death on the SAME generation.
        sup.lock_registry()
            .update(&name, |r| {
                r.state = AgentState::Running;
                r.pid = Some(5252);
                r.stderr_tail = Some("fatal: connection refused while contacting api\n".into());
            })
            .unwrap();
        sup.handle_event(
            &name,
            generation,
            spawn,
            session,
            HarnessEvent::Exited { code: Some(1) },
        );
        assert!(
            sup.status(&name)
                .unwrap()
                .recovery
                .as_ref()
                .is_some_and(|r| r.ack.is_none()),
            "the second episode must park fresh and unacknowledged"
        );

        // A fresh action_id, DIFFERENT from episode 1's, must be freely
        // acknowledgeable — the stale tombstone must not refuse it.
        let outcome = sup
            .continue_recovery(&name, "episode-2-action", None)
            .expect(
                "a later, unrelated recovery on the same generation must supersede the old \
                 receipt and be freshly acknowledgeable",
            );
        assert!(matches!(
            outcome,
            crate::agents::RecoveryOutcome::ResumedSameProvider { .. }
        ));
        assert_eq!(sup.status(&name).unwrap().state, AgentState::Running);

        // The NEW ack now governs replay, not the stale one: episode 1's
        // action_id must no longer replay episode 1's outcome.
        let conflict = sup.continue_recovery(&name, "episode-1-action", None);
        assert!(
            conflict.is_err(),
            "episode 1's action_id must not resurrect after episode 2 has its own ack"
        );
    }

    #[test]
    fn deliberate_stops_are_not_auto_respawn_candidates_but_crashes_are() {
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());

        let mut stopped = record(repo.path(), Some("main"));
        stopped.state = AgentState::Stopped;
        stopped.result = Some("interrupted deliberately by operator".into());
        stopped.crashed = false;
        assert!(!is_auto_respawn_candidate(&stopped));
        assert_ne!(stopped.state, AgentState::Failed);

        let mut crashed = stopped.clone();
        crashed.state = AgentState::Failed;
        crashed.crashed = true;
        assert!(is_auto_respawn_candidate(&crashed));

        let mut orphaned = stopped;
        orphaned.state = AgentState::Orphaned;
        assert!(is_auto_respawn_candidate(&orphaned));
    }

    #[test]
    fn budget_stop_is_terminal_and_announced_exactly_once() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let tickets = Arc::new(crate::tickets::Tickets::new(
            Space::open_in_memory().unwrap(),
            "castle".into(),
        ));
        let sup = Arc::new(
            Supervisor::new(
                Layout::at(home.path()),
                "castle".into(),
                "fake".into(),
                Budget {
                    max_usd: 20.0,
                    max_tokens: 100_000,
                    warn_at: 0.8,
                },
                FleetBudget::default(),
                Space::open_in_memory().unwrap(),
                tickets,
            )
            .unwrap(),
        );
        let mut rec = record(repo.path(), Some("main"));
        rec.state = AgentState::Running;
        rec.cost_usd = 20.25;
        rec.usage.input = 100_001;
        sup.lock_registry().insert(rec.clone()).unwrap();

        sup.enforce_budget(&rec);
        // A duplicate over-cap usage observation must not emit or kill twice.
        sup.enforce_budget(&rec);

        let stopped = sup.status(&rec.name).unwrap();
        assert_eq!(stopped.state, AgentState::Stopped);
        assert!(!stopped.crashed);
        assert!(stopped.state.is_archivable());
        let actions = sup
            .space
            .scan(
                &Pattern::category(Category::Event)
                    .identity(crate::recovery::RECOVERY_ACTION_IDENTITY),
            )
            .unwrap();
        assert_eq!(actions.len(), 1, "budget stop must escalate exactly once");
        let notice = &actions[0].payload["notice"];
        assert_eq!(notice["class"], json!("budget-stop"));
        let text = notice["text"].as_str().unwrap();
        assert!(text.contains("$20.25 / $20.00 cap"));
        assert!(text.contains("100001 / 100000 token cap"));
        assert!(!is_auto_respawn_candidate(&stopped));
    }

    /// A reviewer is checked against the distinct reviewer cap, NOT the
    /// ordinary worker `budget` — proven by a reviewer whose spend sits well
    /// under a generous worker cap but over the (much tighter, here overridden)
    /// reviewer cap still getting stopped, and a `rat` at the identical spend
    /// surviving untouched.
    #[test]
    fn reviewer_role_is_checked_against_the_reviewer_cap_not_the_worker_cap() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let tickets = Arc::new(crate::tickets::Tickets::new(
            Space::open_in_memory().unwrap(),
            "castle".into(),
        ));
        let sup = Arc::new(
            Supervisor::new(
                Layout::at(home.path()),
                "castle".into(),
                "fake".into(),
                Budget {
                    max_usd: 1000.0,
                    max_tokens: 0,
                    warn_at: 0.8,
                },
                FleetBudget::default(),
                Space::open_in_memory().unwrap(),
                tickets,
            )
            .unwrap(),
        );
        sup.set_reviewer_max_usd(10.0);

        let mut reviewer = record(repo.path(), Some("main"));
        reviewer.role = "reviewer".into();
        reviewer.state = AgentState::Running;
        reviewer.cost_usd = 10.5;
        sup.lock_registry().insert(reviewer.clone()).unwrap();
        sup.enforce_budget(&reviewer);
        let stopped = sup.status(&reviewer.name).unwrap();
        assert_eq!(
            stopped.state,
            AgentState::Stopped,
            "reviewer over the $10 reviewer cap must stop despite the $1000 worker cap"
        );

        let mut worker = record(repo.path(), Some("main"));
        worker.name = format!("{}-rat", worker.name);
        worker.role = "rat".into();
        worker.state = AgentState::Running;
        worker.cost_usd = 10.5;
        sup.lock_registry().insert(worker.clone()).unwrap();
        sup.enforce_budget(&worker);
        let untouched = sup.status(&worker.name).unwrap();
        assert_eq!(
            untouched.state,
            AgentState::Running,
            "a rat at the same spend is judged against the $1000 worker cap, unaffected \
             by the reviewer cap"
        );
    }

    /// `BudgetConfig::default().reviewer_max_usd` (rk-core) and
    /// `DEFAULT_REVIEWER_MAX_USD` (this module's built-in fallback before
    /// `Daemon::new` applies config) must agree, or a bare `Supervisor` built
    /// by a test/another crate silently runs a different cap than production.
    #[test]
    fn built_in_reviewer_cap_matches_config_default() {
        assert_eq!(
            DEFAULT_REVIEWER_MAX_USD,
            rk_core::config::BudgetConfig::default().reviewer_max_usd
        );
    }

    /// Graduated warning fires for a reviewer approaching its OWN cap even
    /// when nowhere near the (unrelated) worker cap.
    #[test]
    fn reviewer_budget_warns_at_its_own_threshold() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let tickets = Arc::new(crate::tickets::Tickets::new(
            Space::open_in_memory().unwrap(),
            "castle".into(),
        ));
        let sup = Arc::new(
            Supervisor::new(
                Layout::at(home.path()),
                "castle".into(),
                "fake".into(),
                Budget::default(),
                FleetBudget::default(),
                Space::open_in_memory().unwrap(),
                tickets,
            )
            .unwrap(),
        );
        // Default $30 reviewer cap, default 0.8 warn fraction => warns at $24.
        let mut reviewer = record(repo.path(), Some("main"));
        reviewer.role = "reviewer".into();
        reviewer.state = AgentState::Running;
        reviewer.cost_usd = 25.0;
        sup.lock_registry().insert(reviewer.clone()).unwrap();
        sup.enforce_budget(&reviewer);
        let after = sup.status(&reviewer.name).unwrap();
        assert_eq!(
            after.state,
            AgentState::Running,
            "a warn crossing must not stop the reviewer"
        );
        let obstacles = sup
            .space
            .scan(&Pattern::category(Category::Obstacle))
            .unwrap();
        assert!(
            obstacles
                .iter()
                .any(|t| t.payload["type"] == json!("budget_warning")),
            "expected a budget_warning obstacle once the reviewer crossed 80% of its cap"
        );
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
            review: None,
            model: None,
            permission_mode: None,
            attach: false,
            workflow_instance: None,
            coordinator: None,
            instance_max_usd: None,
            profile: None,
            resolved_profile: None,
        }
    }

    /// The launch producer wires into the task-to-main span substrate
    /// (`crate::span`): a spawn that reaches `Running` records exactly one
    /// `AgentLaunched` span, carrying the wall-clock cost of standing the
    /// agent up, and a relaunch of the same task dedups onto it (idempotent
    /// on `(task, phase, attempt)`).
    #[tokio::test]
    async fn spawn_records_an_agent_launched_phase_span_exactly_once() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let sup = supervisor(home.path());

        let launched = sup
            .spawn_async(spawn_params(repo.path(), "TKT-launch"), 0)
            .await
            .unwrap();

        // Filtered by phase, not counted wholesale: the fake harness can run
        // to completion mid-test, and the `Completed` producer then records
        // its own span against the same task.
        let launch_spans = |sup: &Supervisor| -> Vec<serde_json::Value> {
            crate::span::spans_for_task(&sup.space, &launched.repo_name, "TKT-launch")
                .unwrap()
                .into_iter()
                .filter(|s| s["phase"] == "agent_launched")
                .collect()
        };

        let spans = launch_spans(&sup);
        assert_eq!(spans.len(), 1, "exactly one launch span: {spans:?}");
        assert_eq!(spans[0]["repo"], launched.repo_name);
        assert_eq!(spans[0]["target"], launched.target_branch);
        assert!(
            spans[0]["duration_ms"].as_i64().is_some_and(|ms| ms >= 0),
            "launch span carries a stand-up duration: {:?}",
            spans[0]
        );

        // A relaunch of the same task is a second GENERATION, not a second
        // launch phase — the span key must absorb it, exactly as a restart or
        // a replayed event is absorbed.
        sup.spawn_async(spawn_params(repo.path(), "TKT-launch"), 0)
            .await
            .unwrap();
        let spans = launch_spans(&sup);
        assert_eq!(spans.len(), 1, "relaunch does not double-count: {spans:?}");
    }

    /// The check-in producer wires into the same substrate: the FIRST
    /// checkpoint (`revision == 1`) records exactly one `FirstProgress` span
    /// measuring launch-to-first-check-in, and a later checkpoint records no
    /// second one — every revision after the first is ordinary progress, not
    /// a new phase.
    #[tokio::test]
    async fn record_progress_records_a_phase_span_only_on_the_first_checkpoint() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let sup = supervisor(home.path());

        let mut live = record(repo.path(), Some("rat/nibble/t"));
        live.state = AgentState::Running;
        live.created_at = Utc::now() - chrono::Duration::seconds(30);
        sup.lock_registry().insert(live).unwrap();

        let first = sup
            .record_progress(
                "Nibble",
                "reading the ticket".into(),
                None,
                "working".into(),
            )
            .unwrap();
        assert_eq!(first.progress.as_ref().unwrap().revision, 1);

        let spans = crate::span::spans_for_task(&sup.space, "repo", "t").unwrap();
        assert_eq!(spans.len(), 1, "exactly one first-progress span: {spans:?}");
        assert_eq!(spans[0]["phase"], "first_progress");
        assert_eq!(spans[0]["terminal_reason"], "working");
        assert!(
            spans[0]["duration_ms"]
                .as_i64()
                .is_some_and(|ms| ms >= 30_000),
            "span measures this generation's launch to its first check-in: {:?}",
            spans[0]
        );

        // Backdate the checkpoint past MIN_PROGRESS_INTERVAL so a SECOND
        // check-in is genuinely accepted rather than rate-limited away before
        // it can reach the span producer at all.
        sup.lock_registry()
            .update("Nibble", |record| {
                if let Some(progress) = &mut record.progress {
                    progress.updated_at =
                        Utc::now() - MIN_PROGRESS_INTERVAL - chrono::Duration::seconds(1);
                }
            })
            .unwrap();
        let second = sup
            .record_progress("Nibble", "still working".into(), None, "working".into())
            .unwrap();
        assert_eq!(second.progress.as_ref().unwrap().revision, 2);

        let spans = crate::span::spans_for_task(&sup.space, "repo", "t").unwrap();
        assert_eq!(
            spans.len(),
            1,
            "a later checkpoint is not a new phase: {spans:?}"
        );
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
            .filter(|r| matches!(r, Err(e) if e.to_string() == FLEET_WIP_CAP_REFUSED))
            .count();
        assert_eq!(admitted, 2, "cap of 2 must admit exactly 2: {results:?}");
        assert_eq!(
            refused, 3,
            "the other 3 must be refused cleanly: {results:?}"
        );
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
        assert!(
            failure.is_err(),
            "invalid onboarder task must fail validation"
        );

        let good = spawn_params(repo.path(), "concurrent-after-failure");
        let record = sup
            .spawn_async(good, 1)
            .await
            .expect("the failed attempt's reservation must have been released");
        assert!(record.state.is_live());
    }

    /// The implementation lane (TKT-01M0P2KM83Y4MD5QYETR3JCKF2) is scoped per
    /// repository, unlike the fleet-wide `[drain] max_wip` ceiling tested
    /// above: saturating one repo's lane must refuse further spawns for THAT
    /// repo (proven with the same atomic-concurrency shape as
    /// `fleet_wip_admission_is_atomic_under_concurrent_spawns`) while a
    /// second, unconfigured repo's lane is entirely unaffected — one
    /// repository cannot consume another's capacity.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn implementation_lane_is_scoped_per_repo() {
        let home = tempfile::tempdir().unwrap();
        let repo_a = tempfile::tempdir().unwrap();
        let repo_b = tempfile::tempdir().unwrap();
        init_repo(repo_a.path());
        init_repo(repo_b.path());
        let repo_a_name = Repo::discover(repo_a.path()).unwrap().name();
        let sup = supervisor(home.path());
        sup.set_implementation_admission_limits(0, HashMap::from([(repo_a_name, 1)]));

        // fleet_wip_cap passed as 0 (disabled) throughout, isolating the new
        // per-repo lane ceiling as the only thing under test.
        let handles: Vec<_> = (0..3)
            .map(|i| {
                let sup = Arc::clone(&sup);
                let params = spawn_params(repo_a.path(), &format!("a-{i}"));
                tokio::spawn(async move { sup.spawn_async(params, 0).await })
            })
            .collect();
        let mut results = Vec::with_capacity(handles.len());
        for h in handles {
            results.push(h.await.unwrap());
        }
        let admitted = results.iter().filter(|r| r.is_ok()).count();
        let refused = results
            .iter()
            .filter(|r| matches!(r, Err(e) if e.to_string() == IMPLEMENTATION_LANE_REFUSED))
            .count();
        assert_eq!(
            admitted, 1,
            "repo-a's lane cap of 1 must admit exactly 1: {results:?}"
        );
        assert_eq!(
            refused, 2,
            "the other 2 concurrent attempts against repo-a must be refused cleanly, not \
             double-launched: {results:?}"
        );

        let unrelated = sup.spawn_async(spawn_params(repo_b.path(), "b-0"), 0).await;
        assert!(
            unrelated.is_ok(),
            "repo-b's unconfigured (unbounded) lane must not be affected by repo-a's \
             saturation: {unrelated:?}"
        );
    }

    /// A saturated implementation lane must never starve the review lane for
    /// the SAME repository — they are independent counters
    /// (TKT-01M0P2KM83Y4MD5QYETR3JCKF2's core "implementation admission
    /// cannot starve either lane" requirement).
    #[tokio::test]
    async fn implementation_lane_saturation_does_not_starve_the_review_lane() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let repo_name = Repo::discover(repo.path()).unwrap().name();
        let sup = supervisor(home.path());
        sup.set_implementation_admission_limits(0, HashMap::from([(repo_name.clone(), 1)]));
        sup.set_review_admission_limits(0, HashMap::from([(repo_name, 1)]));

        let occupying = sup
            .spawn_async(spawn_params(repo.path(), "impl-occupying"), 0)
            .await
            .unwrap();
        assert!(occupying.state.is_live());
        let overflow = sup
            .spawn_async(spawn_params(repo.path(), "impl-overflow"), 0)
            .await;
        assert!(matches!(
            &overflow,
            Err(e) if e.to_string() == IMPLEMENTATION_LANE_REFUSED
        ));

        let mut reviewer_params = spawn_params(repo.path(), "reviewer-task");
        reviewer_params.role = "reviewer".into();
        let reviewer = sup.spawn_async(reviewer_params, 0).await;
        assert!(
            reviewer.is_ok(),
            "a fully-saturated implementation lane must not starve the review lane for the \
             same repo: {reviewer:?}"
        );
    }

    /// Implementation-lane occupancy is durable and idempotent across a
    /// daemon restart WITHOUT any dedicated recovery procedure: it is
    /// recomputed from `AgentRecord.state` in `agents.json` (persisted on
    /// every insert), the exact same mechanism the pre-existing fleet-wide
    /// WIP ceiling already relies on — so a live row a predecessor process
    /// inserted still occupies its repo's lane for a freshly constructed
    /// `Supervisor` reading the same `home`.
    #[tokio::test]
    async fn implementation_lane_occupancy_survives_a_restart() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let repo_name = Repo::discover(repo.path()).unwrap().name();

        let before_restart = supervisor(home.path());
        before_restart
            .set_implementation_admission_limits(0, HashMap::from([(repo_name.clone(), 1)]));
        let mut live = record(repo.path(), Some("main"));
        live.repo_name = repo_name.clone();
        live.state = AgentState::Running;
        before_restart.lock_registry().insert(live).unwrap();
        drop(before_restart);

        let after_restart = supervisor(home.path());
        after_restart.set_implementation_admission_limits(0, HashMap::from([(repo_name, 1)]));
        let refused = after_restart
            .spawn_async(spawn_params(repo.path(), "post-restart"), 0)
            .await;
        assert!(
            matches!(&refused, Err(e) if e.to_string() == IMPLEMENTATION_LANE_REFUSED),
            "the predecessor's live record, persisted in agents.json, must still occupy the \
             lane after a restart: {refused:?}"
        );
    }

    /// Real FIFO admission order (TKT-01M0P2KM83Y4MD5QYETR3JCKF2), not just
    /// FIFO reporting: with the lane saturated, the FIRST request refused
    /// must be admitted before a SECOND, distinct request that was refused
    /// later — even if the second one happens to retry first once capacity
    /// frees. A caller that "retries faster" must not be able to jump the
    /// queue ahead of one that was refused earlier.
    #[tokio::test]
    async fn implementation_lane_admits_the_longest_waiting_request_first() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let repo_name = Repo::discover(repo.path()).unwrap().name();
        let sup = supervisor(home.path());
        sup.set_implementation_admission_limits(0, HashMap::from([(repo_name, 1)]));

        let occupying = sup
            .spawn_async(spawn_params(repo.path(), "occupying"), 0)
            .await
            .unwrap();
        assert!(occupying.state.is_live());

        // Two DISTINCT logical requests, both refused while the lane is full —
        // "first-in-line" strictly before "second-in-line" is queued.
        let first_params = spawn_params(repo.path(), "first-in-line");
        let first_refusal = sup.spawn_async(first_params.clone(), 0).await;
        assert!(matches!(
            &first_refusal,
            Err(e) if e.to_string() == IMPLEMENTATION_LANE_REFUSED
        ));

        let second_params = spawn_params(repo.path(), "second-in-line");
        let second_refusal = sup.spawn_async(second_params.clone(), 0).await;
        assert!(matches!(
            &second_refusal,
            Err(e) if e.to_string() == IMPLEMENTATION_LANE_REFUSED
        ));

        // Free the only occupied slot.
        sup.lock_registry()
            .update(&occupying.name, |r| r.state = AgentState::Completed)
            .unwrap();

        // The second waiter retries FIRST (simulating a faster poller) but
        // must still be refused: it is not at the head of the durable queue.
        let second_retry = sup.spawn_async(second_params.clone(), 0).await;
        assert!(
            matches!(&second_retry, Err(e) if e.to_string() == IMPLEMENTATION_LANE_REFUSED),
            "the second waiter must not be admitted ahead of the first, even though it \
             retried first: {second_retry:?}"
        );

        // The first waiter's retry is admitted — it was at the head of the line.
        let first_retry = sup.spawn_async(first_params, 0).await;
        assert!(
            first_retry.is_ok(),
            "the longest-waiting request must be admitted once a slot frees: {first_retry:?}"
        );

        // With the first now occupying the lane's only slot again, the second
        // waiter is STILL refused — proving the first retry didn't leave the
        // second's queue position stale/skipped.
        let second_again = sup.spawn_async(second_params, 0).await;
        assert!(matches!(
            &second_again,
            Err(e) if e.to_string() == IMPLEMENTATION_LANE_REFUSED
        ));
    }

    /// The durable wait-queue's ordering survives a daemon restart: a waiter
    /// queued by a predecessor process is still honored ahead of a brand-new
    /// request that only shows up after the restart, because the queue entry
    /// itself is read back from `lane_waiters.json`, not reconstructed from
    /// in-memory state that a restart would have dropped.
    #[tokio::test]
    async fn implementation_lane_wait_order_survives_a_restart() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let repo_name = Repo::discover(repo.path()).unwrap().name();

        let before_restart = supervisor(home.path());
        before_restart
            .set_implementation_admission_limits(0, HashMap::from([(repo_name.clone(), 1)]));
        let occupying = before_restart
            .spawn_async(spawn_params(repo.path(), "occupying"), 0)
            .await
            .unwrap();
        let queued_params = spawn_params(repo.path(), "queued-before-restart");
        let refusal = before_restart.spawn_async(queued_params.clone(), 0).await;
        assert!(matches!(
            &refusal,
            Err(e) if e.to_string() == IMPLEMENTATION_LANE_REFUSED
        ));
        // Free the slot but do NOT let the queued waiter retry before the
        // restart — the durable record must carry the ordering across it.
        before_restart
            .lock_registry()
            .update(&occupying.name, |r| r.state = AgentState::Completed)
            .unwrap();
        drop(before_restart);

        let after_restart = supervisor(home.path());
        after_restart.set_implementation_admission_limits(0, HashMap::from([(repo_name, 1)]));

        // A brand-new request, never queued anywhere, must still lose to the
        // waiter the predecessor process recorded before it died.
        let newcomer = after_restart
            .spawn_async(spawn_params(repo.path(), "newcomer-after-restart"), 0)
            .await;
        assert!(
            matches!(&newcomer, Err(e) if e.to_string() == IMPLEMENTATION_LANE_REFUSED),
            "a request with no wait history must not jump ahead of a waiter the predecessor \
             process durably queued: {newcomer:?}"
        );

        let queued_retry = after_restart.spawn_async(queued_params, 0).await;
        assert!(
            queued_retry.is_ok(),
            "the pre-restart waiter must be admitted once it retries post-restart: \
             {queued_retry:?}"
        );
    }

    /// If `lane_waiters.json` cannot be written, admission must fail CLOSED
    /// rather than silently proceed as if the durable queue had been updated
    /// — proceeding anyway would leave an on-disk phantom waiter that
    /// outlives its own (in-memory-only) admission, wrongly blocking every
    /// real waiter behind it. Forces the failure by making the home
    /// directory unwritable, so the atomic rename `persist_lane_waiters`
    /// depends on cannot create its temp file.
    #[tokio::test]
    async fn implementation_lane_refuses_admission_rather_than_silently_lose_durable_queue_order() {
        use std::os::unix::fs::PermissionsExt;

        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let repo_name = Repo::discover(repo.path()).unwrap().name();
        let sup = supervisor(home.path());
        sup.set_implementation_admission_limits(0, HashMap::from([(repo_name, 1)]));

        let occupying = sup
            .spawn_async(spawn_params(repo.path(), "occupying"), 0)
            .await
            .unwrap();
        let waiter_params = spawn_params(repo.path(), "waiter");
        let refusal = sup.spawn_async(waiter_params.clone(), 0).await;
        assert!(matches!(
            &refusal,
            Err(e) if e.to_string() == IMPLEMENTATION_LANE_REFUSED
        ));

        sup.lock_registry()
            .update(&occupying.name, |r| r.state = AgentState::Completed)
            .unwrap();

        // Make the home directory unwritable: `lane_waiters.json` already
        // exists (from the refusal above), but the atomic-write discipline
        // still needs to create a fresh `.tmp` file beside it, which this
        // blocks.
        let original_mode = std::fs::metadata(home.path()).unwrap().permissions().mode();
        std::fs::set_permissions(home.path(), std::fs::Permissions::from_mode(0o555)).unwrap();

        let retry_while_undurable = sup.spawn_async(waiter_params.clone(), 0).await;

        // Restore permissions BEFORE any assertion can panic and unwind past
        // this point — tempdir's own Drop cleanup must be able to delete
        // files inside `home`, and a failed assertion must not leak a
        // read-only directory on disk.
        std::fs::set_permissions(home.path(), std::fs::Permissions::from_mode(original_mode))
            .unwrap();

        assert!(
            matches!(&retry_while_undurable, Err(e) if e.to_string() == IMPLEMENTATION_LANE_REFUSED),
            "admission must fail closed when it cannot durably persist the queue-clear, not \
             silently admit while the on-disk record goes stale: {retry_while_undurable:?}"
        );

        // Once writes succeed again, the exact same retry is admitted —
        // proving the earlier refusal was purely about durability, not a
        // corrupted in-memory queue state.
        let retry_once_durable = sup.spawn_async(waiter_params, 0).await;
        assert!(
            retry_once_durable.is_ok(),
            "the same request must succeed once persistence recovers: {retry_once_durable:?}"
        );
    }
}

/// Acceptance properties for the bounded per-repo verification admission
/// queue (TKT-01M0HNESEECWWFQF8X6VH1XSJ6) that are properties of
/// [`VerificationAdmission`]/[`Supervisor`] alone — FIFO fairness, the
/// configured bound, cross-repo independence, and restart recovery — as
/// opposed to the [`crate::workflow_exec`] properties (exact exit
/// provenance, the landing-gate/`verify.run` shared bound, proof reuse) that
/// need a full [`crate::workflow_exec::WorkflowEngine`] to exercise.
#[cfg(test)]
mod verification_admission_tests {
    use super::*;
    use rk_ledger::{Budget, FleetBudget};
    use std::path::Path;
    use std::time::Duration;

    fn sup(home: &Path) -> Arc<Supervisor> {
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

    /// A configured limit of N never admits an (N+1)th concurrent holder:
    /// the (N+1)th `acquire` stays unresolved until an earlier permit is
    /// dropped.
    #[tokio::test]
    async fn acquire_verification_admission_bounds_concurrency_to_the_configured_limit() {
        let home = tempfile::tempdir().unwrap();
        let s = sup(home.path());
        s.set_verification_admission_limits(1, HashMap::new());

        let (first, _wait) = s.acquire_verification_admission("repo-a", 1).await.unwrap();

        let s2 = s.clone();
        let mut second = Box::pin(s2.acquire_verification_admission("repo-a", 1));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut second)
                .await
                .is_err(),
            "a second acquire must block while the configured limit's only permit is held"
        );

        drop(first);
        let _ = tokio::time::timeout(Duration::from_millis(200), second)
            .await
            .expect("releasing the held permit must unblock the waiter")
            .unwrap();
    }

    /// FIFO fairness (documented on `VerificationAdmission`, backed by
    /// `tokio::sync::Semaphore`): permits are granted in acquire order, not
    /// arbitrary scheduler order.
    #[tokio::test]
    async fn acquire_verification_admission_grants_permits_in_fifo_order() {
        let home = tempfile::tempdir().unwrap();
        let s = sup(home.path());
        s.set_verification_admission_limits(1, HashMap::new());

        let (first, _wait) = s
            .acquire_verification_admission("repo-fifo", 1)
            .await
            .unwrap();
        let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));

        let s_b = s.clone();
        let order_b = order.clone();
        let task_b = tokio::spawn(async move {
            let (_permit, _wait) = s_b
                .acquire_verification_admission("repo-fifo", 1)
                .await
                .unwrap();
            order_b.lock().unwrap().push("B");
        });
        // Give B time to actually reach its `.await` and join the semaphore's
        // wait queue before C is spawned, so acquire order is deterministic.
        tokio::time::sleep(Duration::from_millis(30)).await;

        let s_c = s.clone();
        let order_c = order.clone();
        let task_c = tokio::spawn(async move {
            let (_permit, _wait) = s_c
                .acquire_verification_admission("repo-fifo", 1)
                .await
                .unwrap();
            order_c.lock().unwrap().push("C");
        });
        tokio::time::sleep(Duration::from_millis(30)).await;

        drop(first);
        task_b.await.unwrap();
        task_c.await.unwrap();

        assert_eq!(*order.lock().unwrap(), vec!["B", "C"]);
    }

    /// Two different repos never share a bound: repo-a's outstanding permit
    /// must never block repo-b's acquire, even at the same configured limit.
    #[tokio::test]
    async fn independent_repos_do_not_serialize_against_each_other() {
        let home = tempfile::tempdir().unwrap();
        let s = sup(home.path());
        s.set_verification_admission_limits(1, HashMap::new());

        let (_a, _wait) = s.acquire_verification_admission("repo-a", 1).await.unwrap();
        let _ = tokio::time::timeout(
            Duration::from_millis(100),
            s.acquire_verification_admission("repo-b", 1),
        )
        .await
        .expect(
            "a different repo's admission queue must not be blocked by repo-a's outstanding permit",
        )
        .unwrap();
    }

    /// Restart recovery is automatic because there is nothing durable to
    /// recover: a fresh `Supervisor` (standing in for the next daemon
    /// process) never inherits a permit an earlier instance handed out and
    /// never released — even one deliberately leaked here to stand in for a
    /// holder that died mid-check.
    #[tokio::test]
    async fn a_fresh_supervisor_never_inherits_a_predecessors_leaked_permit() {
        let home = tempfile::tempdir().unwrap();
        let before_restart = sup(home.path());
        before_restart.set_verification_admission_limits(1, HashMap::new());
        let (leaked, _wait) = before_restart
            .acquire_verification_admission("repo-restart", 1)
            .await
            .unwrap();
        // Simulate the old process dying while still holding the permit: it
        // is never dropped/released, just abandoned along with `before_restart`.
        std::mem::forget(leaked);
        drop(before_restart);

        let after_restart = sup(home.path());
        after_restart.set_verification_admission_limits(1, HashMap::new());
        let _ = tokio::time::timeout(
            Duration::from_millis(100),
            after_restart.acquire_verification_admission("repo-restart", 1),
        )
        .await
        .expect(
            "a fresh Supervisor must start with a full complement of permits, unaffected by a predecessor's leaked one",
        )
        .unwrap();
    }

    /// Register one repo under `home/repos.json`, standing in for
    /// `handle_repo_add`'s already-canonicalized-path effect.
    fn register_repo(home: &Path, name: &str, path: &Path) {
        let mut registry = crate::repos::RepoRegistry::load(&home.join("repos.json")).unwrap();
        registry
            .add(crate::repos::RepoRecord {
                name: name.into(),
                path: path.to_path_buf(),
                created_at: Utc::now(),
                merge_mode: Default::default(),
                remote: None,
                host: None,
                activated_policy: None,
            })
            .unwrap();
    }

    /// The resolution [`Supervisor::verification_admission_limit_for`],
    /// [`Supervisor::acquire_verification_admission`], and
    /// `record_verification_admission_event` (`crate::workflow_exec`) all go
    /// through (continuation of TKT-01M0P5NM51SKT5ABXRCDZD07J3): a registered
    /// repo's absolute path resolves to its registered NAME, matching what a
    /// landing gate/`verify.run` already pass directly.
    #[tokio::test]
    async fn verification_repo_identity_resolves_a_registered_path_to_its_name() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        register_repo(home.path(), "acme", repo_dir.path());
        let s = sup(home.path());

        assert_eq!(
            s.verification_repo_identity(&repo_dir.path().display().to_string()),
            "acme"
        );
    }

    /// An absolute path with no registry match, and a string that was already
    /// a bare name, both pass through unchanged — there is nothing to
    /// resolve an unregistered path to, and a bare name is already the
    /// stable identity landing/`verify.run` use.
    #[tokio::test]
    async fn verification_repo_identity_leaves_unregistered_paths_and_bare_names_unchanged() {
        let home = tempfile::tempdir().unwrap();
        let s = sup(home.path());

        assert_eq!(
            s.verification_repo_identity("some-bare-name"),
            "some-bare-name"
        );
        assert_eq!(
            s.verification_repo_identity("/no/such/registered/repo"),
            "/no/such/registered/repo"
        );
    }

    /// The acceptance property basename-keying would have broken: two
    /// INDEPENDENTLY registered repos that merely happen to share a
    /// directory basename must never collide into one admission bound.
    #[tokio::test]
    async fn verification_repo_identity_does_not_collide_two_repos_sharing_a_directory_basename() {
        let home = tempfile::tempdir().unwrap();
        let parent_a = tempfile::tempdir().unwrap();
        let parent_b = tempfile::tempdir().unwrap();
        let repo_a = parent_a.path().join("backend");
        let repo_b = parent_b.path().join("backend");
        std::fs::create_dir_all(&repo_a).unwrap();
        std::fs::create_dir_all(&repo_b).unwrap();
        register_repo(home.path(), "team-a-backend", &repo_a);
        register_repo(home.path(), "team-b-backend", &repo_b);
        let s = sup(home.path());

        assert_eq!(
            s.verification_repo_identity(&repo_a.display().to_string()),
            "team-a-backend"
        );
        assert_eq!(
            s.verification_repo_identity(&repo_b.display().to_string()),
            "team-b-backend"
        );

        s.set_verification_admission_limits(1, HashMap::new());
        let (_a, _wait) = s
            .acquire_verification_admission(&repo_a.display().to_string(), 1)
            .await
            .unwrap();
        let _ = tokio::time::timeout(
            Duration::from_millis(100),
            s.acquire_verification_admission(&repo_b.display().to_string(), 1),
        )
        .await
        .expect("two repos sharing a directory basename must not share an admission bound")
        .unwrap();
    }
}

/// TKT-01M0HNF2HR9Y0PY44RHY4Q245P: durable liveness evidence for the stuck
/// sweep, replacing "silence past `stuck_after_secs` is stuck" with "silence
/// past `stuck_after_secs` AND no live verifier descendant, no advancing
/// bounded output, no operator override, and not a reconnect loop is stuck".
/// Every test here calls `decide_sweep` directly with an explicit `now`, so
/// elapsed time is simulated rather than slept — no test in this module
/// actually waits on a wall-clock grace window.
#[cfg(test)]
mod stuck_liveness_tests {
    use super::*;
    use rk_ledger::{Budget, FleetBudget};
    use std::path::Path;

    fn sup(home: &Path) -> Arc<Supervisor> {
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

    fn cfg(stuck_after_secs: u64, kill_grace_secs: u64) -> SupervisorConfig {
        SupervisorConfig {
            enabled: true,
            stuck_after_secs,
            kill_grace_secs,
            burn_usd_per_min: 0.0,
            ..SupervisorConfig::default()
        }
    }

    fn record(name: &str, pid: Option<u32>, updated_at: DateTime<Utc>) -> AgentRecord {
        let now = Utc::now();
        AgentRecord {
            name: name.to_string(),
            spawn: Some(rk_core::id::SpawnId::new()),
            role: "worker".into(),
            coordination: None,
            harness: "fake".into(),
            permission_mode: None,
            model: None,
            repo_root: PathBuf::from("/tmp"),
            repo_name: "repo".into(),
            task: Some("task".into()),
            branch: None,
            fork_point: None,
            worktree: None,
            target_branch: "main".into(),
            parent: None,
            workflow_instance: None,
            review: None,
            coordinator: None,
            session_id: None,
            attach_target: None,
            pid,
            merge_commit: None,
            state: AgentState::Running,
            crashed: false,
            stderr_tail: None,
            result: None,
            progress: None,
            usage: TokenUsage::default(),
            cost_usd: 0.0,
            created_at: now,
            updated_at,
            archived_at: None,
            liveness: crate::agents::LivenessObservation::default(),
            transport_outage: None,
            recovery: None,
            recovery_receipt: None,
        }
    }

    /// Kills and reaps the whole process GROUP of a real test process spawned
    /// with `.process_group(0)`, on drop — including on a panicking `assert!`
    /// unwinding past the point a manual cleanup call would otherwise sit.
    /// Two things this fixes at once, learned from a live regression this
    /// module caused against a concurrent managed `rk verify` run
    /// (TKT-01M0HNF2HR9Y0PY44RHY4Q245P): a `child.kill()` on just the LEADER
    /// pid never reaches a `sleep 300 &`-backgrounded descendant, which the
    /// leader's own process group DOES reach; and that orphaned descendant,
    /// left alive with this TEST PROCESS's inherited stdout/stderr pipes
    /// still open (a plain `spawn()` never redirects them), holds a fd the
    /// daemon's own verifier reads from open for the length of its sleep —
    /// the verifier hangs waiting for an EOF that will not come until it
    /// does. Every process this module spawns is created via
    /// [`kill_tree`](self::kill_tree) for exactly this reason.
    struct KillTree(std::process::Child);

    impl Drop for KillTree {
        fn drop(&mut self) {
            let pid = self.0.id() as i32;
            // SAFETY: signals the process group this exact test spawned via
            // `.process_group(0)` moments earlier, immediately before
            // reaping it below — never a pid this test does not own.
            unsafe {
                libc::kill(-pid, libc::SIGKILL);
            }
            let _ = self.0.wait();
        }
    }

    /// Spawn `command` as its own process-group leader with stdio fully
    /// redirected away from this test process's own pipes, wrapped for
    /// guaranteed group-kill-and-reap on drop. See [`KillTree`].
    fn kill_tree(command: &str, arg: &str) -> KillTree {
        use std::os::unix::process::CommandExt;
        use std::process::Stdio;
        let child = std::process::Command::new(command)
            .arg("-c")
            .arg(arg)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap();
        KillTree(child)
    }

    /// A real process with a live child underneath it (`sleep 300 & wait`,
    /// mirroring `spawn_check_child`'s own leader shape) — standing in for a
    /// `cargo test`/compiler a rat's own tool-use launched, or an `rk verify`
    /// CLI call still blocked on the daemon. Silence has run WAY past
    /// `stuck_after_secs` (the "long" in "long silent-but-live verifier"),
    /// but the live descendant alone must still excuse it — the event stream
    /// itself never has to say a word.
    #[tokio::test]
    async fn long_silent_but_live_verifier_descendant_prevents_kill() {
        let home = tempfile::tempdir().unwrap();
        let s = sup(home.path());
        // Use a real long-lived executable whose basename is `cargo`, so the
        // process-table classifier sees recognized build work without running
        // Cargo recursively from this test. On macOS `ps ... comm=` reports
        // the invoked executable path, including this copied basename.
        let tools = tempfile::tempdir().unwrap();
        let fake_cargo = tools.path().join("cargo");
        std::fs::copy("/bin/sleep", &fake_cargo).unwrap();
        let script = format!("\"{}\" 300 & wait", fake_cargo.display());
        let leader = kill_tree("sh", &script);
        let pid = leader.0.id();
        // The shell has not necessarily forked the fake cargo yet the instant
        // `spawn()` returns — wait for the descendant to actually show up in
        // the process table before asserting on it, rather than racing a
        // fixed sleep against however fast `sh` itself schedules.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while crate::workflow_exec::process_liveness(pid).live_verifier_descendants == 0
            && std::time::Instant::now() < deadline
        {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            crate::workflow_exec::process_liveness(pid).live_verifier_descendants > 0,
            "the backgrounded fake cargo must be recognized before this test proceeds"
        );

        let base = Utc::now() - chrono::Duration::seconds(600);
        let r = record("verifier-1", Some(pid), base);
        s.lock_registry().insert(r.clone()).unwrap();

        let action = s.decide_sweep(&r, base + chrono::Duration::seconds(590), &cfg(60, 30));
        assert!(
            matches!(action, SweepAction::None),
            "a live verifier descendant must excuse silence, however long"
        );
        drop(leader);
    }

    /// Bounded output (assistant text / tool use) that keeps genuinely
    /// changing is liveness on its own, with no live descendant and no
    /// process at all (`pid: None`) — the harness event pump proves it, not
    /// the OS.
    #[tokio::test]
    async fn advancing_bounded_output_prevents_kill_with_no_process_at_all() {
        let home = tempfile::tempdir().unwrap();
        let s = sup(home.path());
        let base = Utc::now() - chrono::Duration::seconds(120);
        let now = base + chrono::Duration::seconds(90);
        let mut r = record("chatty-1", None, base);
        r.liveness.output_changed_at = Some(now - chrono::Duration::seconds(5));
        s.lock_registry().insert(r.clone()).unwrap();

        let action = s.decide_sweep(&r, now, &cfg(60, 30));
        assert!(
            matches!(action, SweepAction::None),
            "recently-advancing bounded output must excuse silence"
        );
    }

    /// No descendant, no output, no lease, no override: a process that is
    /// genuinely alive by `kill(pid, 0)` but has done nothing at all is
    /// exactly what this feature must still catch — proof this is COUNTING
    /// live verifier descendants, not just checking the top-level pid.
    #[tokio::test]
    async fn truly_wedged_child_is_killed_after_grace() {
        let home = tempfile::tempdir().unwrap();
        let s = sup(home.path());
        let child = kill_tree("sh", "sleep 300");
        let pid = child.0.id();

        let base = Utc::now() - chrono::Duration::seconds(120);
        let r = record("wedged-1", Some(pid), base);
        s.lock_registry().insert(r.clone()).unwrap();
        let c = cfg(60, 30);

        let flagged = base + chrono::Duration::seconds(70);
        let first = s.decide_sweep(&r, flagged, &c);
        assert!(
            matches!(first, SweepAction::Soft { kind: "stuck", .. }),
            "a bare alive-but-idle process must still flag stuck"
        );

        let still_within_grace = s.decide_sweep(&r, flagged + chrono::Duration::seconds(29), &c);
        assert!(matches!(still_within_grace, SweepAction::None));

        let past_grace = s.decide_sweep(&r, flagged + chrono::Duration::seconds(31), &c);
        assert!(
            matches!(past_grace, SweepAction::Hard { kind: "stuck", .. }),
            "capacity must be reclaimed once genuinely wedged, same as before this feature"
        );

        drop(child);
    }

    /// A harness transport reconnect loop must NOT count as liveness even
    /// though it is producing bounded-output-shaped chatter (a retry's own
    /// error text, commonly logged to stderr on every attempt) — the
    /// reconnect counter vetoes the whole evidence bundle.
    #[tokio::test]
    async fn transport_reconnect_loop_is_killed_after_grace() {
        let home = tempfile::tempdir().unwrap();
        let s = sup(home.path());
        let base = Utc::now() - chrono::Duration::seconds(120);
        let mut r = record("reconnecting-1", None, base);
        r.liveness.reconnect_events = RECONNECT_LOOP_THRESHOLD;
        s.lock_registry().insert(r.clone()).unwrap();
        let c = cfg(60, 30);

        let flagged = base + chrono::Duration::seconds(70);
        // Fresh "output" arrives inside the same sweep as the reconnect
        // burst — without the veto this alone would read as advancing
        // output and excuse it.
        r.liveness.output_changed_at = Some(flagged - chrono::Duration::seconds(1));
        let first = s.decide_sweep(&r, flagged, &c);
        assert!(
            matches!(first, SweepAction::Soft { kind: "stuck", .. }),
            "reconnect-loop chatter must not be read as liveness"
        );

        let past_grace = s.decide_sweep(&r, flagged + chrono::Duration::seconds(31), &c);
        assert!(matches!(
            past_grace,
            SweepAction::Hard { kind: "stuck", .. }
        ));
    }

    /// The kill-grace ceiling is persisted on the record itself
    /// (`AgentRecord::liveness::ceiling_started_at`), so a fresh `Supervisor`
    /// over the SAME on-disk registry (a daemon restart) must resume the
    /// SAME clock, not grant a new grace window from its own construction
    /// time.
    #[tokio::test]
    async fn stuck_ceiling_survives_a_restart_without_resetting() {
        let home = tempfile::tempdir().unwrap();
        let sup1 = sup(home.path());
        let base = Utc::now() - chrono::Duration::seconds(120);
        let r = record("restart-1", None, base);
        sup1.lock_registry().insert(r.clone()).unwrap();
        let c = cfg(60, 30);

        let flagged = base + chrono::Duration::seconds(70);
        let first = sup1.decide_sweep(&r, flagged, &c);
        assert!(matches!(first, SweepAction::Soft { kind: "stuck", .. }));

        // Simulate a daemon restart: a brand-new Supervisor over the same
        // home directory, with an entirely empty in-memory sweep/session
        // state, reloading the registry (and its `liveness`) from disk.
        let sup2 = sup(home.path());
        let reloaded = sup2.lock_registry().get("restart-1").unwrap().clone();
        assert_eq!(
            reloaded.liveness.ceiling_started_at,
            Some(flagged),
            "the ceiling must reload exactly as the predecessor persisted it"
        );

        let still_within_original_grace =
            sup2.decide_sweep(&reloaded, flagged + chrono::Duration::seconds(29), &c);
        assert!(
            matches!(still_within_original_grace, SweepAction::None),
            "must not escalate before the ORIGINAL ceiling's grace elapses"
        );

        let past_original_grace =
            sup2.decide_sweep(&reloaded, flagged + chrono::Duration::seconds(31), &c);
        assert!(
            matches!(past_original_grace, SweepAction::Hard { kind: "stuck", .. }),
            "the post-restart sweep must escalate on the PRE-restart clock, \
             proving the ceiling was never reset by the restart"
        );
    }
}

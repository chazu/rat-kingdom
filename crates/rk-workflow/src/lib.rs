//! Workflow definitions: CUE files loaded through the `cue` CLI (full CUE
//! semantics, zero build-time deps), aspect weaving, and agent/model
//! resolution. Pure definition layer — execution lives in the daemon.

pub mod resolve;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const SCHEMA: &str = include_str!("schema.cue");
const TRIGGER_SCHEMA: &str = include_str!("triggers-schema.cue");
const SCHEDULE_SCHEMA: &str = include_str!("schedules-schema.cue");
const CHECK_SCHEMA: &str = include_str!("checks-schema.cue");
const REPOSITORY_POLICY_SCHEMA: &str = include_str!("repository-policy-schema.cue");

/// How a completed branch is delivered for one registered repository.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeliveryMode {
    /// Merge the source into its target locally.
    #[default]
    Merge,
    /// Merge locally, then push the resulting target branch.
    MergePush,
    /// Push the source branch without merging it.
    PushBranch,
    /// Push the source branch and open a pull/merge request.
    Pr,
}

/// Per-repository branch and worktree naming templates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkPolicy {
    #[serde(default = "default_branch_template")]
    pub branch: String,
    #[serde(default = "default_worktree_template")]
    pub worktree: String,
}

impl Default for WorkPolicy {
    fn default() -> Self {
        Self {
            branch: default_branch_template(),
            worktree: default_worktree_template(),
        }
    }
}

/// Per-repository destination and remote-delivery behavior.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryPolicy {
    #[serde(default = "default_delivery_target")]
    pub target: String,
    #[serde(default)]
    pub mode: DeliveryMode,
    #[serde(default = "default_remote")]
    pub remote: String,
    #[serde(default = "default_remote_branch", rename = "remoteBranch")]
    pub remote_branch: String,
    #[serde(default = "default_true", rename = "deleteSource")]
    pub delete_source: bool,
}

impl Default for DeliveryPolicy {
    fn default() -> Self {
        Self {
            target: default_delivery_target(),
            mode: DeliveryMode::default(),
            remote: default_remote(),
            remote_branch: default_remote_branch(),
            delete_source: true,
        }
    }
}

/// Versioned repository behavior activated into the daemon's repo registry.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryPolicy {
    #[serde(default)]
    pub work: WorkPolicy,
    #[serde(default)]
    pub delivery: DeliveryPolicy,
}

impl RepositoryPolicy {
    /// Render the local agent branch using git-ref-safe placeholder values.
    pub fn branch_name(&self, agent: &str, task: &str, repo: &str, role: &str) -> String {
        render_work_template(
            &self.work.branch,
            &slug(agent),
            &slug(task),
            &slug(repo),
            &slug(role),
        )
    }

    /// Render the path relative to Rat Kingdom's worktree root. The policy
    /// loader has already rejected absolute and parent-traversing templates.
    pub fn worktree_path(&self, agent: &str, task: &str, repo: &str, role: &str) -> PathBuf {
        PathBuf::from(render_work_template(
            &self.work.worktree,
            &safe_path_segment(agent),
            &safe_path_segment(task),
            &safe_path_segment(repo),
            &safe_path_segment(role),
        ))
    }

    /// Resolve `agent-base` to the branch the completed worker was actually
    /// based on; a fixed policy target ignores that runtime base.
    pub fn delivery_target(&self, agent_base: &str) -> String {
        if self.delivery.target == "agent-base" {
            agent_base.to_string()
        } else {
            self.delivery.target.clone()
        }
    }

    /// Render the configured remote branch. `branch` and `target` are already
    /// validated git refs and intentionally retain their slash hierarchy.
    pub fn remote_branch(&self, branch: &str, target: &str, repo: &str) -> String {
        self.delivery
            .remote_branch
            .replace("{{branch}}", branch)
            .replace("{{target}}", target)
            .replace("{{repo}}", &slug(repo))
    }
}

fn render_work_template(
    template: &str,
    agent: &str,
    task: &str,
    repo: &str,
    role: &str,
) -> String {
    template
        .replace("{{agent}}", agent)
        .replace("{{task}}", task)
        .replace("{{repo}}", repo)
        .replace("{{role}}", role)
}

fn slug(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

fn safe_path_segment(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('.')
        .to_string()
}

fn default_branch_template() -> String {
    "rat/{{agent}}/{{task}}".into()
}

fn default_worktree_template() -> String {
    "{{repo}}/{{agent}}".into()
}

fn default_delivery_target() -> String {
    "agent-base".into()
}

fn default_remote() -> String {
    "origin".into()
}

fn default_remote_branch() -> String {
    "{{branch}}".into()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Workflow {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub params: HashMap<String, Param>,
    #[serde(default)]
    pub agents: HashMap<String, AgentProfile>,
    /// Per-workflow cost-tier routing, taking precedence over the global
    /// `[tiers]` table for this workflow's fan-out spawns.
    #[serde(default)]
    pub tiers: TierRouting,
    /// Optional per-instance budget cap: once this instance's summed agent cost
    /// reaches it, further dispatch is refused. `None` = unlimited.
    #[serde(default)]
    pub budget: Option<WorkflowBudget>,
    pub steps: Vec<Step>,
    #[serde(default)]
    pub aspects: Vec<Aspect>,
}

/// Per-workflow-instance budget cap (the `budget:` field). A USD ceiling on the
/// summed cost of every agent one instance spawns; enforced as a dispatch
/// preflight, mirroring the fleet/repo caps but scoped to a single run.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct WorkflowBudget {
    pub max_usd: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Param {
    #[serde(rename = "type")]
    pub param_type: String,
    #[serde(default = "default_true")]
    pub required: bool,
    #[serde(default)]
    pub default: Option<Value>,
}

fn default_true() -> bool {
    true
}

/// Which harness/model runs an agent; every field optional (see resolve).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentProfile {
    #[serde(default)]
    pub harness: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub permission_mode: Option<String>,
}

/// One cost-tier routing rule: when a ticket's metadata satisfies the (AND'd)
/// predicate, its spawn resolves against the named tier — an agent profile like
/// any other (`[agents.<tier>]` global, or a workflow `agents:` entry). An empty
/// predicate (`priority` and `label` both unset) is an unconditional catch-all,
/// useful as a trailing fallback rule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TierRule {
    /// Match when the ticket's `priority` equals this (unset = any priority).
    #[serde(default)]
    pub priority: Option<String>,
    /// Match when the ticket's `labels` contain this (unset = any labels).
    #[serde(default)]
    pub label: Option<String>,
    /// Agent-profile name to resolve the spawn against when this rule matches.
    pub tier: String,
}

/// An ordered cost-tier routing table mapping ticket labels/priority to a tier
/// (an agent profile). Cheap tiers for bounded/mechanical tickets, premium tiers
/// for hard ones — so a fixed budget runs a wider fleet. First matching rule
/// wins; earlier rules (e.g. a per-workflow table) shadow later ones (global).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TierRouting {
    #[serde(default)]
    pub rules: Vec<TierRule>,
}

impl TierRouting {
    /// The tier name for a ticket, or `None` when no rule matches (resolution
    /// then falls through to the ordinary profile layers, unchanged).
    pub fn route(&self, labels: &[String], priority: Option<&str>) -> Option<&str> {
        self.rules
            .iter()
            .find(|r| {
                r.priority.as_deref().is_none_or(|p| priority == Some(p))
                    && r.label
                        .as_deref()
                        .is_none_or(|l| labels.iter().any(|x| x == l))
            })
            .map(|r| r.tier.as_str())
    }

    /// This table's rules followed by `fallback`'s — so a per-workflow table's
    /// rules take precedence over the global ones without a deep merge.
    pub fn chained(&self, fallback: &TierRouting) -> TierRouting {
        TierRouting {
            rules: self.rules.iter().chain(&fallback.rules).cloned().collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Step {
    Spawn(SpawnStep),
    Wait(WaitStep),
    Evaluate(EvaluateStep),
    Dismiss(DismissStep),
    Gate(GateStep),
    /// Lift the newest matching tuple's payload (or one field) into a ctx var.
    Read(ReadStep),
    /// Route on a ctx var: run the matching case's nested steps, else `default`.
    When(WhenStep),
    /// Bounded loop: run `steps` up to `max` times; `break` exits early.
    Repeat(RepeatStep),
    /// Exit the nearest enclosing `repeat`.
    Break,
    /// Abort the whole instance (failed) with an optional reason.
    Stop(StopStep),
    /// Dynamic fan-out: spawn one agent per matching ticket, in parallel.
    ForEach(ForEachStep),
    /// Parallel join: block until every fanned-out agent has completed.
    WaitAll(WaitAllStep),
    /// Parallel dismiss: merge/cleanup every fanned-out agent, clear the set.
    DismissAll(DismissAllStep),
    /// Run a command (the repo's real test/lint suite) in the active agent's
    /// worktree, capturing `{exit, stdout, stderr}` into `ctx.previousResult`.
    Run(RunStep),
    /// Merge a NAMED branch into a NAMED target directly — "land" the work.
    Land(LandStep),
    /// Open a pull/merge request for a NAMED branch — the PR counterpart to
    /// `land`, always opening a PR regardless of the repo's merge mode.
    OpenPr(OpenPrStep),
    /// Run another named workflow inline as a step, joining its result into
    /// `ctx.previousResult` — workflow composition.
    SubWorkflow(SubWorkflowStep),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpawnStep {
    #[serde(default = "default_role")]
    pub role: String,
    #[serde(default)]
    pub coordination: Option<Coordination>,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub harness: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub permission_mode: Option<String>,
    pub task: TaskDef,
    #[serde(default)]
    pub branch: Option<String>,
}

/// Dispatch metadata identifying an agent as a reporting boundary. The
/// supervision tree remains authoritative for safety; this metadata controls
/// which summaries are visible in coordinator views.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Coordination {
    #[serde(default)]
    pub reports_to: Option<String>,
    #[serde(default = "default_descendant_policy")]
    pub descendant_policy: String,
}

fn default_descendant_policy() -> String {
    "rollup".into()
}

fn default_role() -> String {
    "rat".into()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskDef {
    pub title: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WaitStep {
    #[serde(default = "default_wait_timeout")]
    pub timeout: String,
}

fn default_wait_timeout() -> String {
    "10m".into()
}

/// Fan out one agent per matching ticket, all spawned in parallel. Populates
/// the workflow's fan-out set, which a following [`WaitAllStep`] then joins on.
/// Every agent-selection field (`agent`/`harness`/`model`/`permission_mode`)
/// mirrors [`SpawnStep`] and resolves the same way. The `task` template binds
/// per-ticket placeholders `{{item.id}}`, `{{item.title}}`, `{{item.body}}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForEachStep {
    pub query: TicketQuery,
    #[serde(default = "default_role")]
    pub role: String,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub harness: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub permission_mode: Option<String>,
    pub task: TaskDef,
    #[serde(default)]
    pub branch: Option<String>,
}

/// Which tickets a fan-out enumerates. `status: "ready"` (the default) means
/// open tickets whose dependencies are all satisfied; any other value filters
/// by that literal ticket status. Scope is always the workflow's own repo.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TicketQuery {
    #[serde(default = "default_query_status")]
    pub status: String,
    #[serde(default = "default_query_limit")]
    pub limit: usize,
}

fn default_query_status() -> String {
    "ready".into()
}

fn default_query_limit() -> usize {
    5
}

/// Join step: block until every agent spawned by the preceding fan-out has
/// emitted its `harness_result`, aggregating them into `ctx.previousResult`
/// (`{count, ok, errors, all_ok, results}`) for a following `evaluate`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WaitAllStep {
    #[serde(default = "default_wait_all_timeout")]
    pub timeout: String,
}

fn default_wait_all_timeout() -> String {
    "45m".into()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvaluateStep {
    pub expect: Value,
    /// Alternative accepted outcomes. A single `expect` unifies as an AND over
    /// its fields, so it cannot express "one shape OR another". Listing shapes
    /// here makes the step pass if the result unifies with `expect` OR with any
    /// entry — e.g. accepting a PR-mode `land`'s `{pr_opened: true}` alongside a
    /// Direct-merge `land`'s `{merged: true}`.
    #[serde(default, rename = "anyOf")]
    pub any_of: Vec<Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DismissStep {
    #[serde(default, rename = "noMerge")]
    pub no_merge: bool,
}

/// Dismiss every agent in the fan-out set in parallel — the fan-out counterpart
/// to [`DismissStep`] over the single `active_agent`. Merges each parked branch
/// (unless `no_merge`), then clears the fan-out set. Aggregates the per-agent
/// outcomes into `ctx.previousResult` (`{count, merged, errors, all_merged,
/// results}`) for a following `evaluate`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DismissAllStep {
    #[serde(default, rename = "noMerge")]
    pub no_merge: bool,
    /// Best-effort merge: when set, merge only the branches of rats that
    /// finished clean (`is_error: false` in the preceding `wait_all`) and park
    /// the rest with `no_merge`, instead of failing the batch on the first
    /// error. Requires a preceding `wait_all` in the same instance — its
    /// per-agent results supply the clean/failed signal. Default `false` =
    /// atomic-batch (every branch merged unconditionally).
    #[serde(default, rename = "onlyClean")]
    pub only_clean: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GateStep {
    #[serde(rename = "gateType")]
    pub gate_type: String,
    /// Timer gates: how long to sleep. Absent for approval gates.
    #[serde(default)]
    pub duration: Option<String>,
    /// Approval gates: how long to wait for a human decision before failing
    /// closed (not-approved). Absent for timer gates.
    #[serde(default)]
    pub timeout: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadStep {
    /// Tuple category to match (rendered as its snake_case name in CUE).
    pub category: String,
    /// Tuple identity to match.
    pub identity: String,
    /// Scope to match; defaults to the workflow's repo name at runtime.
    #[serde(default)]
    pub scope: Option<String>,
    /// Optional substring the serialized payload must contain.
    #[serde(default)]
    pub search: Option<String>,
    /// Bind the read to the tuple THIS instance's active agent wrote, instead of
    /// the newest tuple any author left in the scope (TKT-161).
    ///
    /// `(category, scope, identity)` alone is not an identity: two instances of
    /// the same workflow running on one repo — which is the *designed* steady
    /// state, since the reactor fires `steward` per rat completion — have their
    /// reviewers writing `artifact/<repo>/review` concurrently. "Newest wins"
    /// then hands one instance the other's verdict, and the `when` behind it
    /// routes a land on a stranger's review. Setting this narrows the match to
    /// `"agent":"<ctx.activeAgent>"` above that agent's own generation floor,
    /// the same bound `wait`/`wait_all` carry (see [`rk_core::tuple::Pattern::
    /// for_agent_since`]).
    ///
    /// Mutually exclusive with [`ReadStep::search`], which owns the same
    /// predicate slot. Fails closed: no active agent, or an agent whose tuple
    /// never carries its name, times the step out rather than routing on a
    /// tuple it cannot attribute.
    #[serde(default, rename = "fromAgent")]
    pub from_agent: bool,
    /// Bind the read to the tuple that names THIS workflow instance in its
    /// payload, instead of the newest tuple any instance left in the scope
    /// (TKT-172).
    ///
    /// The sibling of [`ReadStep::from_agent`], for tuples keyed by the run
    /// rather than by an agent — `workflow_approval` above all. An approval
    /// `gate` already waits on `"instance":"<id>"`, but the `read` that lifts
    /// the decision behind it did not, and `(event, <repo>, workflow_approval)`
    /// is shared by every gated instance on the repo. Two instances parked on
    /// one repo would then both route on whichever decision landed last: approve
    /// A and reject B, and either B merges on A's approval or A is held on B's
    /// rejection. The fail-closed timeout decision makes it worse, since a
    /// timing-out instance synthesises an `{approved: false}` its live peer can
    /// pick up.
    ///
    /// Mutually exclusive with [`ReadStep::search`] and [`ReadStep::from_agent`],
    /// which own the same single predicate slot. Fails closed: a decision tuple
    /// that does not name this instance times the step out rather than routing.
    #[serde(default, rename = "fromInstance")]
    pub from_instance: bool,
    /// JSON payload field to lift; whole payload if unset.
    #[serde(default)]
    pub field: Option<String>,
    /// ctx variable name to store the value under.
    pub into: String,
    #[serde(default = "default_read_timeout")]
    pub timeout: String,
}

fn default_read_timeout() -> String {
    "5m".into()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WhenStep {
    /// ctx variable to switch on (as set by a prior `read`).
    pub var: String,
    /// Value -> nested steps. String values match by equality.
    #[serde(default)]
    pub cases: HashMap<String, Vec<Step>>,
    /// Steps run when the value matches no case.
    #[serde(default)]
    pub default: Vec<Step>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepeatStep {
    /// Hard iteration cap; the body runs at most this many times.
    pub max: u32,
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StopStep {
    #[serde(default)]
    pub reason: Option<String>,
}

/// Run a command in the active agent's worktree — the deterministic quality
/// gate. Where `evaluate` only unifies against the harness's *self-reported*
/// output, a `run` step executes the repo's real checks and captures the
/// verdict the runner cannot forge. The `{exit, stdout, stderr, timed_out,
/// verdict}` result lands in `ctx.previousResult` so a following
/// `evaluate {expect: {exit: 0}}` (or a `when`) can gate the merge; a red check
/// fails the instance fail-closed.
///
/// Two fields make a SLOW check routable rather than fatal (TKT-169): set
/// `on_timeout: "continue"` and the blown wall-clock bound becomes a result
/// (`verdict: "timeout"`) instead of an error, and `into`/`field` lift that
/// verdict into a ctx var a following `when` can branch on. Together they let a
/// workflow distinguish "the suite says no" from "the suite did not finish in
/// the budget" — two conditions that both have to block a merge but call for
/// very different operator hand-offs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunStep {
    /// Raw command line, executed verbatim via `sh -c` in the worktree. Only as
    /// trusted as the workflow definition that carries it, so it is gated behind
    /// the `[policy] require_named_checks` flag: when that flag is set, a `run`
    /// step MUST reference a named `check` instead and a raw `command` is refused
    /// fail-closed. Mutually exclusive with `check`.
    #[serde(default)]
    pub command: Option<String>,
    /// Name of a repo-registered check (`<repo>/.rk/checks.cue`) to run instead
    /// of a raw `command`. A named check is the repo owner's own allowlist entry,
    /// so it runs regardless of the `require_named_checks` policy — the whole
    /// point is that a compromised workflow def can invoke only these, never
    /// arbitrary shell. Mutually exclusive with `command`.
    #[serde(default)]
    pub check: Option<String>,
    /// Working directory relative to the worktree root; the root if unset. For a
    /// named check, this overrides the check's own `cwd` when set.
    #[serde(default)]
    pub cwd: Option<String>,
    /// Workflow-supplied data passed to a repository-owned named check without
    /// changing the check's executable command. Names are restricted at
    /// execution time to the `RK_CHECK_*` namespace so an untrusted workflow
    /// cannot replace `PATH`, loader hooks, or the supervised agent identity.
    /// Values support the same runtime context interpolation as commands.
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// If set, the step itself fails the instance when the actual exit code
    /// differs — a fail-closed inline gate. If unset, the exit is only
    /// captured for a following `evaluate`/`when` to route on. For a named check,
    /// this overrides the check's own `expectExit` when set.
    #[serde(default, rename = "expectExit")]
    pub expect_exit: Option<i64>,
    /// Hard wall-clock bound; a suite still running when it elapses is killed.
    /// For a named check, the check's own timeout applies unless the step sets
    /// one explicitly (a non-default value). What the kill does to the instance
    /// is [`RunStep::on_timeout`].
    #[serde(default = "default_run_timeout")]
    pub timeout: String,
    /// What a blown [`RunStep::timeout`] does to the instance: `"fail"` (the
    /// default, and the only behaviour before TKT-169) makes it an error that
    /// kills the run mid-flight; `"continue"` makes it a RESULT the workflow can
    /// route on (`{exit: 124, timed_out: true, verdict: "timeout"}`).
    ///
    /// `"continue"` does not weaken a gate — 124 is not 0, so a following
    /// `evaluate {expect: {exit: 0}}` (or an `expect_exit: 0`) still rejects a
    /// timed-out suite exactly like a red one. It only lets the workflow say so
    /// deliberately: escalate to a human, hold the branch, and finish, instead
    /// of dying with a bare "timed out" and skipping every step that would have
    /// explained it. Held as a string (like [`GateStep::gate_type`]) and parsed
    /// fail-closed at execution: an unknown value is an error, never a silent
    /// "fail".
    #[serde(default = "default_on_timeout", rename = "onTimeout")]
    pub on_timeout: String,
    /// JSON field of this step's result to lift into `ctx.vars[into]`; the whole
    /// result object when unset. Mirrors [`ReadStep::field`].
    #[serde(default)]
    pub field: Option<String>,
    /// ctx variable name to store the lifted value under, so a following `when`
    /// can route on it. `None` = lift nothing (every run step before TKT-169).
    /// Mirrors [`ReadStep::into`], which is required there because a `read`
    /// exists only to lift; a `run` lifts only when asked.
    #[serde(default)]
    pub into: Option<String>,
    /// Extra attempts on a non-"pass" verdict, for a check already characterized
    /// as flaky for reasons outside the code under test (machine load, not a red
    /// suite). 0 (default) preserves prior behaviour. Deliberately step-only,
    /// like [`RunStep::on_timeout`]: retry policy is a workflow routing
    /// decision, not a property of the command itself.
    #[serde(default, rename = "retryOnFail")]
    pub retry_on_fail: u32,
}

fn default_run_timeout() -> String {
    "10m".into()
}

fn default_on_timeout() -> String {
    "fail".into()
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckEnvironmentPolicy {
    #[default]
    Inherit,
    StripRkSpawn,
}

impl std::fmt::Display for CheckEnvironmentPolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Inherit => "inherit",
            Self::StripRkSpawn => "strip_rk_spawn",
        })
    }
}

/// A repo-registered named check: the per-repo allowlist entry a workflow `run`
/// step invokes by name instead of carrying a raw shell command. The registry
/// lives in `<repo>/.rk/checks.cue` and is owned by the repo, NOT by the (possibly
/// untrusted) workflow definition — so with the `require_named_checks` policy on,
/// a compromised workflow def can only ever run the checks listed here, never
/// arbitrary shell (TKT-30). The command is still executed via `sh -c`, but the
/// text is fixed by the repo owner.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Check {
    /// Stable name a `run` step references via `check: "<name>"`.
    pub name: String,
    /// Command line, executed via `sh -c` in the worktree.
    pub command: String,
    /// Working directory relative to the worktree root; the root if unset.
    #[serde(default)]
    pub cwd: Option<String>,
    /// Inline fail-closed exit gate, as on a raw `run` step.
    #[serde(default, rename = "expectExit")]
    pub expect_exit: Option<i64>,
    /// Hard wall-clock bound; unset falls back to the run step's own timeout.
    #[serde(default)]
    pub timeout: Option<String>,
    /// Environment inherited by the check process. Repositories whose test
    /// clients read ambient rat identity can explicitly strip the spawn
    /// contract rather than relying on an operator to remember shell hygiene.
    #[serde(default, rename = "environmentPolicy")]
    pub environment_policy: CheckEnvironmentPolicy,
    /// Repository-owned description of the runner/toolchain used by this
    /// command. Optional for legacy registries; required for onboarding-created
    /// checks so the verification report can preserve it.
    #[serde(default)]
    pub toolchain: Option<String>,
}

/// Load and validate every `#Check` in one repo's `checks.cue` registry.
pub fn load_checks(file: &Path) -> rk_core::Result<Vec<Check>> {
    let source = std::fs::read_to_string(file)
        .map_err(|e| rk_core::Error::other(format!("read {}: {e}", file.display())))?;
    load_checks_str(&source)
}

/// Load named checks from source text (see [`load_checks`]).
pub fn load_checks_str(source: &str) -> rk_core::Result<Vec<Check>> {
    let source = schema_with_source(CHECK_SCHEMA, source);
    let json = cue_export_stdin(&source, "checks")?;
    let checks: Vec<Check> = serde_json::from_str(&json)
        .map_err(|e| rk_core::Error::other(format!("checks JSON did not match schema: {e}")))?;
    Ok(checks)
}

/// Load and validate a versioned `.rk/repo.cue` policy.
pub fn load_repository_policy(file: &Path) -> rk_core::Result<RepositoryPolicy> {
    load_repository_policy_with_digest(file).map(|(policy, _)| policy)
}

/// Load and digest the same immutable byte snapshot, avoiding a policy/digest
/// mismatch if the file changes while registration or activation is reading it.
pub fn load_repository_policy_with_digest(
    file: &Path,
) -> rk_core::Result<(RepositoryPolicy, String)> {
    let source = std::fs::read_to_string(file)
        .map_err(|e| rk_core::Error::other(format!("read {}: {e}", file.display())))?;
    let digest = hex::encode(Sha256::digest(source.as_bytes()));
    Ok((load_repository_policy_str(&source)?, digest))
}

/// SHA-256 digest of the exact policy bytes used as the activation identity.
pub fn repository_policy_digest(file: &Path) -> rk_core::Result<String> {
    let bytes = std::fs::read(file)
        .map_err(|e| rk_core::Error::other(format!("read {}: {e}", file.display())))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

/// Load a repository policy from CUE source and enforce template safety rules
/// that are clearer to express once the schema has produced concrete strings.
pub fn load_repository_policy_str(source: &str) -> rk_core::Result<RepositoryPolicy> {
    let source = schema_with_source(REPOSITORY_POLICY_SCHEMA, source);
    let json = cue_export_stdin(&source, "repo")?;
    let policy: RepositoryPolicy = serde_json::from_str(&json).map_err(|e| {
        rk_core::Error::other(format!("repository policy JSON did not match schema: {e}"))
    })?;
    validate_repository_policy(&policy)?;
    Ok(policy)
}

fn validate_repository_policy(policy: &RepositoryPolicy) -> rk_core::Result<()> {
    validate_template(
        "repo.work.branch",
        &policy.work.branch,
        &["agent", "task", "repo", "role"],
    )?;
    validate_template(
        "repo.work.worktree",
        &policy.work.worktree,
        &["agent", "task", "repo", "role"],
    )?;
    if !policy.work.branch.contains("{{agent}}") {
        return Err(rk_core::Error::other(
            "repo.work.branch must contain {{agent}} so concurrent workers are unique",
        ));
    }
    if !policy.work.worktree.contains("{{agent}}") {
        return Err(rk_core::Error::other(
            "repo.work.worktree must contain {{agent}} so concurrent workers are unique",
        ));
    }
    validate_branch_value(
        "repo.work.branch",
        &policy.branch_name("sample-agent", "sample-task", "sample-repo", "rat"),
    )?;
    let worktree = Path::new(&policy.work.worktree);
    if worktree.is_absolute()
        || worktree.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
    {
        return Err(rk_core::Error::other(
            "repo.work.worktree must stay below Rat Kingdom's worktrees directory",
        ));
    }
    validate_template(
        "repo.delivery.remoteBranch",
        &policy.delivery.remote_branch,
        &["branch", "target", "repo"],
    )?;
    if policy.delivery.target != "agent-base" {
        validate_branch_value("repo.delivery.target", &policy.delivery.target)?;
    }
    validate_branch_value(
        "repo.delivery.remoteBranch",
        &policy.remote_branch("rat/sample-agent/sample-task", "main", "sample-repo"),
    )?;
    if matches!(
        policy.delivery.mode,
        DeliveryMode::PushBranch | DeliveryMode::Pr
    ) && !policy.delivery.remote_branch.contains("{{branch}}")
    {
        return Err(rk_core::Error::other(
            "repo.delivery.remoteBranch must contain {{branch}} for push-branch or pr mode",
        ));
    }
    for (name, value) in [
        ("repo.delivery.target", policy.delivery.target.as_str()),
        ("repo.delivery.remote", policy.delivery.remote.as_str()),
        (
            "repo.delivery.remoteBranch",
            policy.delivery.remote_branch.as_str(),
        ),
    ] {
        if value.trim().is_empty() || value.contains('\0') || value.contains(char::is_whitespace) {
            return Err(rk_core::Error::other(format!(
                "{name} must be a non-empty whitespace-free value"
            )));
        }
    }
    if policy.delivery.remote.starts_with('-') {
        return Err(rk_core::Error::other(
            "repo.delivery.remote must not begin with '-'",
        ));
    }
    Ok(())
}

fn validate_branch_value(name: &str, value: &str) -> rk_core::Result<()> {
    let output = Command::new("git")
        .args(["check-ref-format", "--branch", value])
        .output()
        .map_err(|error| {
            rk_core::Error::other(format!("git is required to validate {name}: {error}"))
        })?;
    if output.status.success() {
        return Ok(());
    }
    Err(rk_core::Error::other(format!(
        "{name} renders an invalid git branch {value:?}: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    )))
}

fn validate_template(name: &str, template: &str, allowed: &[&str]) -> rk_core::Result<()> {
    if template.trim().is_empty() || template.contains('\0') {
        return Err(rk_core::Error::other(format!(
            "{name} must be a non-empty template"
        )));
    }
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else {
            return Err(rk_core::Error::other(format!(
                "{name} contains an unclosed placeholder"
            )));
        };
        let placeholder = &after[..end];
        if !allowed.contains(&placeholder) {
            return Err(rk_core::Error::other(format!(
                "{name} contains unsupported placeholder {{{{{placeholder}}}}}"
            )));
        }
        rest = &after[end + 2..];
    }
    if rest.contains("}}") {
        return Err(rk_core::Error::other(format!(
            "{name} contains an unmatched placeholder terminator"
        )));
    }
    Ok(())
}

/// Merge a NAMED branch into a NAMED target — the explicit `{branch, target}`
/// counterpart to `dismiss`. Where `dismiss` merges the single active agent's
/// branch into *its own base*, `land` names both the source `branch` and the
/// merge `target`, so an APPROVE verdict can land reviewed work straight onto
/// (e.g.) `main` without a human doing the final merge. This closes the last
/// manual hop when a reviewer is chained off a work branch: its dismiss can only
/// merge into that base, never main.
///
/// Both fields interpolate `{{ctx.*}}` placeholders, so
/// `branch: "{{ctx.activeBranch}}"` lands the branch the workflow is holding.
/// The merge is CAS-safe (rk-git's `merge_branch` runs in a detached worktree
/// and advances the target ref only if it did not move), so it disturbs no live
/// checkout and fails safe under concurrency: a merge conflict or a moved target
/// is a clean `{merged: false}` in `ctx.previousResult`, NOT a hard error — gate
/// on it with a following `evaluate {expect: {merged: true}}` or a `when`. On a
/// successful merge the source branch is deleted unless `keep_branch` (a
/// protected or still-checked-out branch is left in place, reported not
/// deleted).
///
/// SAFETY: `land` merges with no review of its own. Reach it only through an
/// APPROVE `when`-branch or after an approval gate — never as an unconditional
/// step — or unreviewed work lands. A hard policy restriction (and merge-queue
/// serialization) is deferred to the policy engine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LandStep {
    /// Branch to merge; interpolated. Often `{{ctx.activeBranch}}`.
    pub branch: String,
    /// Branch to merge it into; interpolated. E.g. `"main"`.
    pub target: String,
    /// Keep the source branch after a successful merge instead of deleting it.
    #[serde(default, rename = "keepBranch")]
    pub keep_branch: bool,
}

/// Open a pull/merge request for a named branch against a named target — the
/// PR counterpart to [`LandStep`]. Unlike `land`, which routes on the repo's
/// registered merge mode, `open_pr` always pushes the branch and opens a PR
/// regardless of repo policy, so a workflow can choose the review-by-PR outcome
/// explicitly. The branch is left standing; the result is a clean
/// `{pr_opened: false}` on a push failure, never an error.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenPrStep {
    /// Branch to open a PR for; interpolated. Often `{{ctx.activeBranch}}`.
    pub branch: String,
    /// Branch the PR targets; interpolated. E.g. `"main"`.
    pub target: String,
}

/// Run another workflow inline as a step of the current one — composition. The
/// named workflow is resolved and launched exactly like a top-level `run`
/// (`<repo>/.rk/workflows/<name>.cue` over the global dir), executed to
/// completion synchronously, and its final `ctx.previous_result` joins back into
/// the parent's `ctx.previous_result` for a following `evaluate`/`when`. This is
/// how a macro like "decompose the backlog, then drain it" becomes one step onto
/// the existing `backlog-drain` definition instead of a copy of its steps.
///
/// `params` values are templated with the parent's `{{ctx.*}}` placeholders at
/// run time, then coerced to the child's declared `#Param` types (exactly like
/// reactor-templated params). Nesting is bounded at runtime by a hard depth cap
/// — the depth analog of the `repeat` max cap — so a workflow cycle fails closed
/// rather than recursing without end.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubWorkflowStep {
    /// Workflow definition name (or path to a `.cue` file) to run inline.
    pub workflow: String,
    /// Repo/path to run the child in; the parent's repo when unset.
    #[serde(default)]
    pub repo: Option<String>,
    /// Params for the child, each a template string interpolated against the
    /// parent's ctx before being coerced to the child's declared param type.
    #[serde(default)]
    pub params: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Aspect {
    #[serde(rename = "match")]
    pub matcher: AspectMatch,
    #[serde(default)]
    pub before: Vec<Step>,
    #[serde(default)]
    pub after: Vec<Step>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AspectMatch {
    #[serde(default, rename = "type")]
    pub step_type: Option<String>,
    #[serde(default)]
    pub role: Option<String>,
}

/// Load one workflow file: evaluate it as a CUE package together with the
/// embedded schema and generated `_input` values, export JSON, weave aspects.
pub fn load(file: &Path, inputs: &HashMap<String, Value>) -> rk_core::Result<Workflow> {
    let source = std::fs::read_to_string(file)
        .map_err(|e| rk_core::Error::other(format!("read {}: {e}", file.display())))?;
    load_str(&source, inputs)
}

/// Load from source text (see [`load`]).
///
/// Two-pass: params are exported first (they never reference `_input`) so
/// declared defaults can be merged into the inputs before the full export —
/// otherwise `_input.<param-with-default>` would be an unresolved reference.
pub fn load_str(source: &str, inputs: &HashMap<String, Value>) -> rk_core::Result<Workflow> {
    let dir = tempfile_dir()?;
    std::fs::write(dir.join("schema.cue"), SCHEMA)?;
    std::fs::write(dir.join("workflow.cue"), ensure_package(source))?;
    // Pass 1 sees an empty, open `_input`: params never reference `_input`, so
    // this exports them cleanly, and — crucially — leaves every `_input.<x>` in
    // the steps *incomplete* rather than concrete. A concrete raw string input
    // (e.g. count="3" for an `int` field) would otherwise conflict here, before
    // we ever get the chance to coerce it.
    std::fs::write(dir.join("input.cue"), render_inputs(&HashMap::new())?)?;

    // Pass 1: declared params → required-check + defaults.
    let params_json = cue_export(&dir, "workflow.params")?;
    let params: HashMap<String, Param> = serde_json::from_str(&params_json)
        .map_err(|e| rk_core::Error::other(format!("workflow params malformed: {e}")))?;
    let mut effective = inputs.clone();
    for (name, param) in &params {
        if !effective.contains_key(name) {
            match &param.default {
                Some(default) => {
                    effective.insert(name.clone(), default.clone());
                }
                None if param.required => {
                    std::fs::remove_dir_all(&dir).ok();
                    return Err(rk_core::Error::other(format!(
                        "missing required workflow param: {name} (pass --param {name}=...)"
                    )));
                }
                None => continue,
            }
        }
        // Coerce the supplied (or defaulted) value to the declared type so a
        // stringly-encoded --param / trigger value becomes real JSON before it
        // is rendered into `_input`, and a mistyped --param-file value is
        // rejected here rather than as an opaque CUE unification error.
        let raw = &effective[name];
        match coerce_param(name, &param.param_type, raw) {
            Ok(coerced) => {
                effective.insert(name.clone(), coerced);
            }
            Err(e) => {
                std::fs::remove_dir_all(&dir).ok();
                return Err(e);
            }
        }
    }
    std::fs::write(dir.join("input.cue"), render_inputs(&effective)?)?;

    // Pass 2: the full workflow with all inputs resolvable.
    let json = cue_export(&dir, "workflow")?;
    let mut workflow: Workflow = serde_json::from_str(&json)
        .map_err(|e| rk_core::Error::other(format!("workflow JSON did not match schema: {e}")))?;
    workflow.steps = expand_aspects(workflow.steps, &workflow.aspects);
    std::fs::remove_dir_all(&dir).ok();
    Ok(workflow)
}

/// A reactor trigger: a match predicate over a landing tuple plus the workflow
/// to run when it matches. Loaded from `#Trigger` CUE definitions, validated
/// against the embedded trigger schema exactly as workflows are.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Trigger {
    pub name: String,
    #[serde(rename = "match")]
    pub matcher: TriggerMatch,
    /// Workflow definition name to launch on a match.
    pub run: String,
    /// Registered repo name to run in; falls back to the tuple scope / the
    /// trigger file's own repo at dispatch time.
    #[serde(default)]
    pub repo: Option<String>,
    /// Workflow params, each templated from the matched tuple's fields/payload.
    #[serde(default)]
    pub params: HashMap<String, String>,
    /// Tuple authors this trigger never fires for.
    #[serde(default)]
    pub exclude: Vec<String>,
    /// Per-trigger fire cap within the reactor window; unset uses the config
    /// default.
    #[serde(default, rename = "maxFires")]
    pub max_fires: Option<u32>,
}

/// The tuple predicate half of a [`Trigger`]. Every set field must match (AND);
/// unset fields match anything.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TriggerMatch {
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub identity: Option<String>,
    #[serde(default)]
    pub instance: Option<String>,
    /// Substring the serialized payload must contain.
    #[serde(default)]
    pub search: Option<String>,
}

/// Load and validate every `#Trigger` in one CUE file.
pub fn load_triggers(file: &Path) -> rk_core::Result<Vec<Trigger>> {
    let source = std::fs::read_to_string(file)
        .map_err(|e| rk_core::Error::other(format!("read {}: {e}", file.display())))?;
    load_triggers_str(&source)
}

/// Load triggers from source text (see [`load_triggers`]).
pub fn load_triggers_str(source: &str) -> rk_core::Result<Vec<Trigger>> {
    let source = schema_with_source(TRIGGER_SCHEMA, source);
    let json = cue_export_stdin(&source, "triggers")?;
    let triggers: Vec<Trigger> = serde_json::from_str(&json)
        .map_err(|e| rk_core::Error::other(format!("triggers JSON did not match schema: {e}")))?;
    Ok(triggers)
}

/// A scheduled workflow: a cron cadence plus the workflow to launch on it. The
/// time-axis counterpart to a [`Trigger`] — where a trigger fires on a matching
/// tuple, a schedule fires on a clock. Loaded from `#Schedule` CUE definitions,
/// validated against the embedded schedule schema exactly as triggers are.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schedule {
    pub name: String,
    /// A 5-field cron expression or `@macro`, evaluated in UTC. Its full syntax
    /// is parsed and validated by the daemon scheduler, not this loader.
    pub cron: String,
    /// Workflow definition name to launch on cadence.
    pub run: String,
    /// Registered repo to run in; a repo-local schedule file defaults it to that
    /// repo. A global schedule with no repo cannot resolve and is skipped.
    #[serde(default)]
    pub repo: Option<String>,
    /// Static params passed to the workflow verbatim (each a string value).
    #[serde(default)]
    pub params: HashMap<String, String>,
}

/// Load and validate every `#Schedule` in one CUE file.
pub fn load_schedules(file: &Path) -> rk_core::Result<Vec<Schedule>> {
    let source = std::fs::read_to_string(file)
        .map_err(|e| rk_core::Error::other(format!("read {}: {e}", file.display())))?;
    load_schedules_str(&source)
}

/// Load schedules from source text (see [`load_schedules`]).
pub fn load_schedules_str(source: &str) -> rk_core::Result<Vec<Schedule>> {
    let source = schema_with_source(SCHEDULE_SCHEMA, source);
    let json = cue_export_stdin(&source, "schedules")?;
    let schedules: Vec<Schedule> = serde_json::from_str(&json)
        .map_err(|e| rk_core::Error::other(format!("schedules JSON did not match schema: {e}")))?;
    Ok(schedules)
}

/// Validate a workflow's syntax and schema without resolving its runtime
/// parameters. The definition and embedded schema are sent to `cue` on stdin,
/// so inspection does not create a temporary package or write beside the
/// repository being assessed.
pub fn validate_workflow_str(source: &str) -> rk_core::Result<()> {
    let base = schema_with_source(SCHEMA, source);
    let params_source = format!("{base}\n_input: [string]: _\n");
    let params_json = cue_export_stdin(&params_source, "workflow.params")?;
    let params: HashMap<String, Param> = serde_json::from_str(&params_json)
        .map_err(|e| rk_core::Error::other(format!("workflow params malformed: {e}")))?;

    let mut keys = params.keys().collect::<Vec<_>>();
    keys.sort();
    let mut input = String::from("\n_input: {\n");
    for key in keys {
        let param = &params[key];
        let value = param
            .default
            .clone()
            .unwrap_or_else(|| match param.param_type.as_str() {
                "string" => Value::String(String::new()),
                "int" => Value::Number(1.into()),
                "number" => Value::Number(1.into()),
                "bool" => Value::Bool(false),
                "list" => Value::Array(Vec::new()),
                _ => Value::Null,
            });
        input.push_str(&format!("\t{key}: {value}\n"));
    }
    input.push_str("}\n");
    let json = cue_export_stdin(&(base + &input), "workflow")?;
    serde_json::from_str::<Workflow>(&json)
        .map_err(|e| rk_core::Error::other(format!("workflow JSON did not match schema: {e}")))?;
    Ok(())
}

/// List workflow definitions in a directory (files named `<name>.cue`).
pub fn definitions(dir: &Path) -> Vec<PathBuf> {
    let mut defs: Vec<PathBuf> = std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().map(|x| x == "cue").unwrap_or(false))
                .collect()
        })
        .unwrap_or_default();
    defs.sort();
    defs
}

/// CUE-unify `expect` against `actual` and require a concrete result.
/// This is the evaluate-step engine: full CUE semantics via the CLI.
pub fn unify_concrete(expect: &Value, actual: &Value) -> rk_core::Result<bool> {
    let dir = tempfile_dir()?;
    let source =
        format!("package check\nresult: expect & actual\nexpect: {expect}\nactual: {actual}\n");
    std::fs::write(dir.join("check.cue"), source)?;
    let out = Command::new("cue")
        .args(["eval", "-c", "-e", "result"])
        .current_dir(&dir)
        .output()
        .map_err(|e| rk_core::Error::other(format!("cue CLI not runnable: {e}")))?;
    std::fs::remove_dir_all(&dir).ok();
    Ok(out.status.success())
}

fn ensure_package(source: &str) -> String {
    if source.trim_start().starts_with("package ") || source.contains("\npackage ") {
        source.to_string()
    } else {
        format!("package workflow\n\n{source}")
    }
}

/// Coerce a supplied workflow-param value to its declared `#Param` type.
///
/// A value already of the right JSON shape passes through unchanged, so bulk
/// `--param-file` inputs keep their native types. A JSON string is parsed into
/// the target type, which is how single `--param k=v` flags (always strings)
/// and reactor-templated params acquire a non-string type. Anything else is a
/// type error reported against the param name.
fn coerce_param(name: &str, ty: &str, value: &Value) -> rk_core::Result<Value> {
    let mismatch = |want: &str| {
        rk_core::Error::other(format!(
            "workflow param {name}: expected {want}, got {value}"
        ))
    };
    match ty {
        "string" => match value {
            Value::String(_) => Ok(value.clone()),
            // A number/bool passed for a string param is stringified for
            // convenience (e.g. a --param-file entry reused across types).
            Value::Number(_) | Value::Bool(_) => Ok(Value::String(value.to_string())),
            _ => Err(mismatch("string")),
        },
        "int" => match value {
            Value::Number(n) if n.is_i64() || n.is_u64() => Ok(value.clone()),
            Value::Number(_) => Err(mismatch("int (got a fractional number)")),
            Value::String(s) => s
                .trim()
                .parse::<i64>()
                .map(|i| Value::Number(i.into()))
                .map_err(|_| mismatch("int")),
            _ => Err(mismatch("int")),
        },
        "number" => match value {
            Value::Number(_) => Ok(value.clone()),
            Value::String(s) => s
                .trim()
                .parse::<f64>()
                .ok()
                .and_then(serde_json::Number::from_f64)
                .map(Value::Number)
                .ok_or_else(|| mismatch("number")),
            _ => Err(mismatch("number")),
        },
        "bool" => match value {
            Value::Bool(_) => Ok(value.clone()),
            Value::String(s) => match s.trim() {
                "true" => Ok(Value::Bool(true)),
                "false" => Ok(Value::Bool(false)),
                _ => Err(mismatch("bool (\"true\" or \"false\")")),
            },
            _ => Err(mismatch("bool")),
        },
        "list" => match value {
            Value::Array(_) => Ok(value.clone()),
            Value::String(s) => match serde_json::from_str::<Value>(s) {
                Ok(v @ Value::Array(_)) => Ok(v),
                _ => Err(mismatch("list (a JSON array)")),
            },
            _ => Err(mismatch("list")),
        },
        // Unreachable while the CUE schema constrains #Param.type, but keep the
        // Rust side total rather than silently accepting an unknown type.
        other => Err(rk_core::Error::other(format!(
            "workflow param {name}: unknown declared type {other:?}"
        ))),
    }
}

fn render_inputs(inputs: &HashMap<String, Value>) -> rk_core::Result<String> {
    let mut out = String::from("package workflow\n\n_input: {\n");
    for (key, value) in inputs {
        out.push_str(&format!("\t{key}: {value}\n"));
    }
    out.push_str("}\n");
    Ok(out)
}

fn cue_export(dir: &Path, expr: &str) -> rk_core::Result<String> {
    let out = Command::new("cue")
        .args(["export", ".", "-e", expr, "--out", "json"])
        .current_dir(dir)
        .output()
        .map_err(|e| rk_core::Error::other(format!("cue CLI not runnable: {e}")))?;
    if !out.status.success() {
        return Err(rk_core::Error::other(format!(
            "cue export failed:\n{}",
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

/// Merge a user definition into one embedded-schema package. A repository file
/// may declare the expected package itself; remove that declaration because the
/// embedded schema already carries it.
fn schema_with_source(schema: &str, source: &str) -> String {
    let mut removed_package = false;
    let source = source
        .lines()
        .filter(|line| {
            if !removed_package && line.trim_start().starts_with("package ") {
                removed_package = true;
                false
            } else {
                true
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!("{schema}\n\n// repository definition\n{source}\n")
}

fn cue_export_stdin(source: &str, expr: &str) -> rk_core::Result<String> {
    let mut child = Command::new("cue")
        .args(["export", "-", "-e", expr, "--out", "json"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| rk_core::Error::other(format!("cue CLI not runnable: {e}")))?;
    child
        .stdin
        .take()
        .expect("cue stdin is piped")
        .write_all(source.as_bytes())?;
    let out = child.wait_with_output()?;
    if !out.status.success() {
        return Err(rk_core::Error::other(format!(
            "cue export failed:\n{}",
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

fn tempfile_dir() -> rk_core::Result<PathBuf> {
    let dir = std::env::temp_dir().join(format!(
        "rk-cue-{}-{}",
        std::process::id(),
        rk_core::id::RecordId::new()
    ));
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// The predecessor's aspect semantics, verbatim: per aspect in declaration order, splice
/// `before`/`after` around every matching step; first aspect is innermost.
pub fn expand_aspects(mut steps: Vec<Step>, aspects: &[Aspect]) -> Vec<Step> {
    for aspect in aspects {
        let mut expanded = Vec::with_capacity(steps.len());
        for step in steps {
            if step_matches(&step, &aspect.matcher) {
                expanded.extend(aspect.before.iter().cloned());
                expanded.push(step);
                expanded.extend(aspect.after.iter().cloned());
            } else {
                expanded.push(step);
            }
        }
        steps = expanded;
    }
    steps
}

fn step_matches(step: &Step, matcher: &AspectMatch) -> bool {
    if let Some(step_type) = &matcher.step_type {
        let actual = match step {
            Step::Spawn(_) => "spawn",
            Step::Wait(_) => "wait",
            Step::Evaluate(_) => "evaluate",
            Step::Dismiss(_) => "dismiss",
            Step::Gate(_) => "gate",
            Step::Read(_) => "read",
            Step::When(_) => "when",
            Step::Repeat(_) => "repeat",
            Step::Break => "break",
            Step::Stop(_) => "stop",
            Step::ForEach(_) => "for_each",
            Step::WaitAll(_) => "wait_all",
            Step::DismissAll(_) => "dismiss_all",
            Step::Run(_) => "run",
            Step::Land(_) => "land",
            Step::OpenPr(_) => "open_pr",
            Step::SubWorkflow(_) => "sub_workflow",
        };
        if actual != step_type {
            return false;
        }
    }
    if let Some(role) = &matcher.role {
        match step {
            Step::Spawn(spawn) if &spawn.role == role => {}
            Step::ForEach(fe) if &fe.role == role => {}
            _ => return false,
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rule(priority: Option<&str>, label: Option<&str>, tier: &str) -> TierRule {
        TierRule {
            priority: priority.map(String::from),
            label: label.map(String::from),
            tier: tier.into(),
        }
    }

    #[test]
    fn tier_routing_first_match_wins() {
        let routing = TierRouting {
            rules: vec![
                rule(None, Some("mechanical"), "cheap"),
                rule(Some("high"), None, "premium"),
                rule(None, None, "normal"), // catch-all fallback
            ],
        };
        // Label match takes the first rule even though priority would match the
        // second — first match wins.
        assert_eq!(
            routing.route(&["mechanical".into()], Some("high")),
            Some("cheap")
        );
        assert_eq!(routing.route(&[], Some("high")), Some("premium"));
        // No label/priority match falls to the catch-all.
        assert_eq!(routing.route(&[], Some("low")), Some("normal"));
    }

    #[test]
    fn tier_routing_no_match_is_none() {
        let routing = TierRouting {
            rules: vec![rule(Some("high"), None, "premium")],
        };
        assert_eq!(routing.route(&["x".into()], Some("low")), None);
        assert_eq!(TierRouting::default().route(&[], Some("high")), None);
    }

    #[test]
    fn tier_routing_chained_prefers_own_rules() {
        let global = TierRouting {
            rules: vec![rule(Some("high"), None, "global-premium")],
        };
        let wf = TierRouting {
            rules: vec![rule(Some("high"), None, "wf-premium")],
        };
        // The workflow's rule shadows the global one for the same predicate.
        assert_eq!(
            wf.chained(&global).route(&[], Some("high")),
            Some("wf-premium")
        );
        // Global rules still apply where the workflow has none.
        let wf_empty = TierRouting::default();
        assert_eq!(
            wf_empty.chained(&global).route(&[], Some("high")),
            Some("global-premium")
        );
    }

    const SAMPLE: &str = r#"
workflow: {
    name:        "code-review"
    description: "worker implements, reviewer validates"
    params: {
        taskId: {type: "string", required: true}
        timeout: {type: "string", required: false, default: "5m"}
    }
    agents: {
        default: {harness: "claude", model: "sonnet"}
        cheap:   {harness: "codex", model: "gpt-5.5-codex"}
    }
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId}},
        {type: "wait", timeout: "15m"},
        {type: "evaluate", expect: {is_error: false}},
        {type: "spawn", role: "reviewer", agent: "cheap", model: "o4-mini", task: {title: "Review: " + _input.taskId}},
        {type: "wait"},
        {type: "dismiss"},
    ]
    aspects: [
        {match: {type: "spawn", role: "rat"}, after: [{type: "gate", gateType: "timer", duration: "1s"}]},
    ]
}
"#;

    fn inputs() -> HashMap<String, Value> {
        HashMap::from([("taskId".to_string(), json!(".rk-42"))])
    }

    #[test]
    fn loads_via_cue_with_input_interpolation_and_aspects() {
        let wf = load_str(SAMPLE, &inputs()).unwrap();
        assert_eq!(wf.name, "code-review");
        // _input interpolated by CUE itself.
        let Step::Spawn(first) = &wf.steps[0] else {
            panic!("first step should be spawn");
        };
        assert_eq!(first.task.title, ".rk-42");
        // Aspect wove a timer gate after the rat spawn (and only there):
        // spawn(rat), gate, wait, evaluate, spawn(reviewer), wait, dismiss.
        assert_eq!(wf.steps.len(), 7);
        assert!(matches!(&wf.steps[1], Step::Gate(g) if g.duration.as_deref() == Some("1s")));
        assert!(matches!(&wf.steps[4], Step::Spawn(s) if s.role == "reviewer"));
        // Workflow agent profiles parsed.
        assert_eq!(wf.agents["default"].model.as_deref(), Some("sonnet"));
    }

    #[test]
    fn approval_gate_loads_with_default_timeout() {
        let source = r#"
workflow: {
    name: "gated"
    steps: [
        {type: "spawn", task: {title: "t"}},
        {type: "gate", gateType: "approval"},
        {type: "gate", gateType: "approval", timeout: "1h"},
        {type: "gate", gateType: "timer", duration: "5s"},
    ]
}
"#;
        let wf = load_str(source, &HashMap::new()).unwrap();
        // Approval gate with no explicit timeout picks up the schema default.
        assert!(
            matches!(&wf.steps[1], Step::Gate(g) if g.gate_type == "approval" && g.timeout.as_deref() == Some("24h") && g.duration.is_none())
        );
        assert!(
            matches!(&wf.steps[2], Step::Gate(g) if g.gate_type == "approval" && g.timeout.as_deref() == Some("1h"))
        );
        assert!(
            matches!(&wf.steps[3], Step::Gate(g) if g.gate_type == "timer" && g.duration.as_deref() == Some("5s"))
        );
    }

    #[test]
    fn schema_violations_are_cue_errors() {
        let bad = r#"workflow: {name: "Bad Name!", steps: [{type: "spawn", task: {title: "x"}}]}"#;
        let err = load_str(bad, &HashMap::new()).unwrap_err();
        assert!(err.to_string().contains("cue export failed"), "{err}");
    }

    #[test]
    fn jcode_is_an_allowed_agent_profile_harness() {
        let source = r#"
workflow: {
    name: "jcode-worker"
    agents: {default: {harness: "jcode", model: "gpt-test"}}
    steps: [{type: "spawn", task: {title: "work"}}]
}
"#;
        let workflow = load_str(source, &HashMap::new()).unwrap();
        assert_eq!(
            workflow.agents["default"].harness.as_deref(),
            Some("jcode")
        );
    }

    #[test]
    fn missing_required_param_is_rejected() {
        let err = load_str(SAMPLE, &HashMap::new()).unwrap_err();
        assert!(err.to_string().contains("taskId"), "{err}");
    }

    const TYPED_PARAMS: &str = r#"
workflow: {
    name: "typed"
    params: {
        count:   {type: "int"}
        ratio:   {type: "number"}
        enabled: {type: "bool"}
        tags:    {type: "list"}
        label:   {type: "string", required: false, default: "x"}
    }
    steps: [
        {type: "spawn", task: {title: "n=\(_input.count)"}},
        {type: "repeat", max: _input.count, steps: [{type: "stop"}]},
    ]
}
"#;

    #[test]
    fn stringly_params_coerce_to_declared_types() {
        // Exactly what the CLI sends for `--param count=3 --param enabled=true
        // --param ratio=0.5 --param tags=[1,2]`: every value is a JSON string.
        let inputs = HashMap::from([
            ("count".to_string(), json!("3")),
            ("ratio".to_string(), json!("0.5")),
            ("enabled".to_string(), json!("true")),
            ("tags".to_string(), json!("[1,2]")),
        ]);
        let wf = load_str(TYPED_PARAMS, &inputs).unwrap();
        // The int reached CUE as a real int, so `repeat.max: _input.count`
        // (which requires an int) unified instead of erroring on a string.
        let Step::Repeat(r) = &wf.steps[1] else {
            panic!("second step should be repeat");
        };
        assert_eq!(r.max, 3);
        // int interpolated into the title as a bare `3`, not the string "3".
        let Step::Spawn(s) = &wf.steps[0] else {
            panic!("first step should be spawn");
        };
        assert_eq!(s.task.title, "n=3");
    }

    #[test]
    fn param_file_native_types_pass_through() {
        // What --param-file supplies: already-typed JSON, not strings.
        let inputs = HashMap::from([
            ("count".to_string(), json!(7)),
            ("ratio".to_string(), json!(1.5)),
            ("enabled".to_string(), json!(false)),
            ("tags".to_string(), json!(["a", "b"])),
        ]);
        let wf = load_str(TYPED_PARAMS, &inputs).unwrap();
        let Step::Repeat(r) = &wf.steps[1] else {
            panic!("second step should be repeat");
        };
        assert_eq!(r.max, 7);
    }

    #[test]
    fn mistyped_param_is_a_clear_error() {
        let inputs = HashMap::from([
            ("count".to_string(), json!("not-a-number")),
            ("ratio".to_string(), json!("0.5")),
            ("enabled".to_string(), json!("true")),
            ("tags".to_string(), json!("[]")),
        ]);
        let err = load_str(TYPED_PARAMS, &inputs).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("count") && msg.contains("int"), "{msg}");
    }

    #[test]
    fn fractional_value_rejected_for_int_param() {
        let inputs = HashMap::from([
            ("count".to_string(), json!(2.5)),
            ("ratio".to_string(), json!("0.5")),
            ("enabled".to_string(), json!("true")),
            ("tags".to_string(), json!("[]")),
        ]);
        let err = load_str(TYPED_PARAMS, &inputs).unwrap_err();
        assert!(err.to_string().contains("int"), "{err}");
    }

    const CONTROL_FLOW: &str = r#"
workflow: {
    name: "route"
    agents: {default: {harness: "fake"}}
    steps: [
        {type: "spawn", role: "rat", task: {title: "t"}},
        {type: "wait"},
        {type: "read", category: "artifact", identity: "review", field: "recommendation", into: "verdict"},
        {
            type: "repeat"
            max:  3
            steps: [
                {type: "spawn", role: "reviewer", task: {title: "r"}},
                {type: "wait"},
                {type: "read", category: "artifact", identity: "review", field: "recommendation", into: "verdict"},
                {
                    type: "when"
                    var:  "verdict"
                    cases: {
                        "APPROVE": [{type: "dismiss"}, {type: "break"}]
                        "STOP": [{type: "dismiss", noMerge: true}, {type: "stop", reason: "reviewer STOP"}]
                    }
                    default: [{type: "dismiss", noMerge: true}]
                },
            ]
        },
    ]
}
"#;

    #[test]
    fn loads_read_when_repeat_break_stop() {
        let wf = load_str(CONTROL_FLOW, &HashMap::new()).unwrap();
        assert_eq!(wf.steps.len(), 4);
        let Step::Read(read) = &wf.steps[2] else {
            panic!("step 2 should be read");
        };
        assert_eq!(read.category, "artifact");
        assert_eq!(read.field.as_deref(), Some("recommendation"));
        assert_eq!(read.into, "verdict");
        // read timeout defaulted.
        assert_eq!(read.timeout, "5m");

        let Step::Repeat(repeat) = &wf.steps[3] else {
            panic!("step 3 should be repeat");
        };
        assert_eq!(repeat.max, 3);
        assert_eq!(repeat.steps.len(), 4);
        let Step::When(when) = &repeat.steps[3] else {
            panic!("nested step 3 should be when");
        };
        assert_eq!(when.var, "verdict");
        // APPROVE case ends in a break; STOP case ends in a stop.
        assert!(matches!(when.cases["APPROVE"].last().unwrap(), Step::Break));
        assert!(matches!(
            when.cases["STOP"].last().unwrap(),
            Step::Stop(s) if s.reason.as_deref() == Some("reviewer STOP")
        ));
        assert!(matches!(when.default.first().unwrap(), Step::Dismiss(_)));
    }

    #[test]
    fn loads_dismiss_all() {
        let source = r#"
workflow: {
    name: "drain-merge"
    steps: [
        {type: "for_each", query: {status: "ready", limit: 3}, task: {title: "{{item.id}}"}},
        {type: "wait_all"},
        {type: "dismiss_all"},
        {type: "dismiss_all", noMerge: true},
        {type: "dismiss_all", onlyClean: true},
    ]
}
"#;
        let wf = load_str(source, &HashMap::new()).unwrap();
        assert_eq!(wf.steps.len(), 5);
        // Default dismiss_all merges (no_merge defaults false, only_clean off).
        assert!(matches!(&wf.steps[2], Step::DismissAll(d) if !d.no_merge && !d.only_clean));
        // noMerge parked variant.
        assert!(matches!(&wf.steps[3], Step::DismissAll(d) if d.no_merge));
        // onlyClean best-effort variant (opt-in; still merges by default).
        assert!(matches!(&wf.steps[4], Step::DismissAll(d) if d.only_clean && !d.no_merge));
    }

    #[test]
    fn loads_run_step() {
        let source = r#"
workflow: {
    name: "gated-check"
    steps: [
        {type: "spawn", role: "rat", task: {title: "T"}},
        {type: "wait"},
        {type: "run", command: "cargo test", timeout: "5m"},
        {type: "run", command: "cargo clippy", cwd: "crates/x", expectExit: 0},
    ]
}
"#;
        let wf = load_str(source, &HashMap::new()).unwrap();
        assert_eq!(wf.steps.len(), 4);
        // Plain run: command captured, cwd/expectExit unset, timeout parsed.
        assert!(matches!(
            &wf.steps[2],
            Step::Run(r)
                if r.command.as_deref() == Some("cargo test")
                    && r.check.is_none()
                    && r.cwd.is_none()
                    && r.expect_exit.is_none()
                    && r.timeout == "5m"
        ));
        // Run with cwd + inline expectExit gate; timeout defaults to 10m.
        assert!(matches!(
            &wf.steps[3],
            Step::Run(r)
                if r.cwd.as_deref() == Some("crates/x")
                    && r.expect_exit == Some(0)
                    && r.timeout == "10m"
        ));
    }

    #[test]
    fn loads_land_step() {
        let source = r#"
workflow: {
    name: "land-on-approve"
    steps: [
        {type: "spawn", role: "rat", task: {title: "T"}},
        {type: "wait"},
        {type: "land", branch: "{{ctx.activeBranch}}", target: "main"},
        {type: "land", branch: "rat/x/feat", target: "release", keepBranch: true},
    ]
}
"#;
        let wf = load_str(source, &HashMap::new()).unwrap();
        assert_eq!(wf.steps.len(), 4);
        // Default land deletes the source branch (keep_branch defaults false).
        assert!(matches!(
            &wf.steps[2],
            Step::Land(l)
                if l.branch == "{{ctx.activeBranch}}" && l.target == "main" && !l.keep_branch
        ));
        // keepBranch preserves the merged source branch.
        assert!(matches!(
            &wf.steps[3],
            Step::Land(l) if l.branch == "rat/x/feat" && l.target == "release" && l.keep_branch
        ));
    }

    #[test]
    fn loads_sub_workflow_step() {
        let source = r#"
workflow: {
    name: "decompose-then-drain"
    params: {limit: {type: "int", required: false, default: 5}}
    steps: [
        {type: "spawn", role: "rat", task: {title: "decompose"}},
        {type: "wait"},
        {type: "dismiss", noMerge: true},
        {type: "sub_workflow", workflow: "backlog-drain", params: {limit: "\(_input.limit)"}},
        {type: "sub_workflow", workflow: "groom", repo: "other-repo"},
    ]
}
"#;
        let wf = load_str(source, &HashMap::new()).unwrap();
        assert_eq!(wf.steps.len(), 5);
        // Param forwarded via CUE interpolation lands as a string, ready for the
        // child's own #Param coercion; repo defaults to the parent's (None).
        let Step::SubWorkflow(sub) = &wf.steps[3] else {
            panic!("step 3 should be sub_workflow");
        };
        assert_eq!(sub.workflow, "backlog-drain");
        assert_eq!(sub.repo, None);
        assert_eq!(sub.params["limit"], "5");
        // Explicit repo override, no params.
        let Step::SubWorkflow(sub) = &wf.steps[4] else {
            panic!("step 4 should be sub_workflow");
        };
        assert_eq!(sub.workflow, "groom");
        assert_eq!(sub.repo.as_deref(), Some("other-repo"));
        assert!(sub.params.is_empty());
    }

    #[test]
    fn repeat_max_over_cap_is_rejected() {
        let bad = r#"
workflow: {
    name: "loopy"
    steps: [{type: "repeat", max: 101, steps: [{type: "gate", gateType: "timer", duration: "1s"}]}]
}
"#;
        let err = load_str(bad, &HashMap::new()).unwrap_err();
        assert!(err.to_string().contains("cue export failed"), "{err}");
    }

    /// TKT-01M02QT9KTDY2CN6YJEVP3VCF8: an oversized `retryOnFail` must be
    /// rejected at the schema boundary, not reach `resolved.retry_on_fail + 1`
    /// unbounded in the daemon.
    #[test]
    fn retry_on_fail_over_cap_is_rejected() {
        let bad = r#"
workflow: {
    name: "flaky"
    steps: [{type: "run", command: "cargo test", retryOnFail: 21}]
}
"#;
        let err = load_str(bad, &HashMap::new()).unwrap_err();
        assert!(err.to_string().contains("cue export failed"), "{err}");
    }

    /// TKT-01M02QT9KTDY2CN6YJEVP3VCF8: a negative `retryOnFail` must be
    /// rejected at the schema boundary too, not just by `u32` deserialization
    /// (which would otherwise be the only thing standing between an authored
    /// negative and undefined behaviour further down the pipeline).
    #[test]
    fn retry_on_fail_negative_is_rejected() {
        let bad = r#"
workflow: {
    name: "flaky"
    steps: [{type: "run", command: "cargo test", retryOnFail: -1}]
}
"#;
        let err = load_str(bad, &HashMap::new()).unwrap_err();
        assert!(err.to_string().contains("cue export failed"), "{err}");
    }

    #[test]
    fn retry_on_fail_at_cap_is_accepted() {
        let source = r#"
workflow: {
    name: "flaky"
    steps: [{type: "run", command: "cargo test", retryOnFail: 20}]
}
"#;
        let wf = load_str(source, &HashMap::new()).unwrap();
        assert!(matches!(&wf.steps[0], Step::Run(r) if r.retry_on_fail == 20));
    }

    #[test]
    fn unify_concrete_accepts_and_rejects() {
        assert!(unify_concrete(
            &json!({"is_error": false}),
            &json!({"is_error": false, "extra": 1})
        )
        .unwrap());
        assert!(!unify_concrete(&json!({"is_error": false}), &json!({"is_error": true})).unwrap());
        // Constraint-style expectations work (full CUE semantics).
        assert!(unify_concrete(&json!({}), &json!({"anything": "goes"})).unwrap());
    }

    #[test]
    fn aspects_apply_in_declaration_order_first_innermost() {
        let steps = vec![Step::Spawn(SpawnStep {
            role: "rat".into(),
            coordination: None,
            agent: None,
            harness: None,
            model: None,
            permission_mode: None,
            task: TaskDef {
                title: "t".into(),
                description: None,
            },
            branch: None,
        })];
        let gate = |d: &str| {
            Step::Gate(GateStep {
                gate_type: "timer".into(),
                duration: Some(d.into()),
                timeout: None,
            })
        };
        let aspects = vec![
            Aspect {
                matcher: AspectMatch {
                    step_type: Some("spawn".into()),
                    role: None,
                },
                before: vec![gate("inner-before")],
                after: vec![gate("inner-after")],
            },
            Aspect {
                matcher: AspectMatch {
                    step_type: Some("spawn".into()),
                    role: None,
                },
                before: vec![gate("outer-before")],
                after: vec![],
            },
        ];
        let woven = expand_aspects(steps, &aspects);
        // Second aspect wraps the result of the first: outer-before lands
        // before the spawn but after inner-before was already spliced.
        let durations: Vec<&str> = woven
            .iter()
            .map(|s| match s {
                Step::Gate(g) => g.duration.as_deref().unwrap_or("?"),
                Step::Spawn(_) => "SPAWN",
                _ => "?",
            })
            .collect();
        assert_eq!(
            durations,
            vec!["inner-before", "outer-before", "SPAWN", "inner-after"]
        );
    }

    const TRIGGERS: &str = r#"
triggers: [
    {
        name: "endorse-quorum"
        match: {category: "endorsement", scope: "system"}
        run:  "promote-convention"
        params: {suggestion: "{{tuple.payload.suggestion}}"}
        maxFires: 5
    },
    {
        name: "drain-on-ticket"
        match: {category: "event", identity: "ticket_created"}
        run:  "backlog-drain"
        exclude: ["daemon"]
    },
]
"#;

    #[test]
    fn loads_triggers_via_cue() {
        let triggers = load_triggers_str(TRIGGERS).unwrap();
        assert_eq!(triggers.len(), 2);
        let first = &triggers[0];
        assert_eq!(first.name, "endorse-quorum");
        assert_eq!(first.matcher.category.as_deref(), Some("endorsement"));
        assert_eq!(first.matcher.scope.as_deref(), Some("system"));
        assert_eq!(first.run, "promote-convention");
        assert_eq!(first.params["suggestion"], "{{tuple.payload.suggestion}}");
        assert_eq!(first.max_fires, Some(5));
        assert_eq!(triggers[1].exclude, vec!["daemon".to_string()]);
    }

    #[test]
    fn trigger_maxfires_over_cap_is_a_cue_error() {
        let bad = r#"triggers: [{name: "x", match: {category: "need"}, run: "w", maxFires: 101}]"#;
        let err = load_triggers_str(bad).unwrap_err();
        assert!(err.to_string().contains("cue export failed"), "{err}");
    }

    #[test]
    fn trigger_bad_name_is_a_cue_error() {
        let bad = r#"triggers: [{name: "Bad Name", match: {category: "need"}, run: "w"}]"#;
        let err = load_triggers_str(bad).unwrap_err();
        assert!(err.to_string().contains("cue export failed"), "{err}");
    }

    const SCHEDULES: &str = r#"
schedules: [
    {
        name: "nightly-drain"
        cron: "0 3 * * *"
        run:  "backlog-drain"
        repo: "rat-kingdom"
        params: {limit: "5"}
    },
    {
        name: "hourly-groom"
        cron: "@hourly"
        run:  "groom"
    },
]
"#;

    #[test]
    fn loads_schedules_via_cue() {
        let schedules = load_schedules_str(SCHEDULES).unwrap();
        assert_eq!(schedules.len(), 2);
        let first = &schedules[0];
        assert_eq!(first.name, "nightly-drain");
        assert_eq!(first.cron, "0 3 * * *");
        assert_eq!(first.run, "backlog-drain");
        assert_eq!(first.repo.as_deref(), Some("rat-kingdom"));
        assert_eq!(first.params["limit"], "5");
        // A macro cron and an omitted repo both load.
        assert_eq!(schedules[1].cron, "@hourly");
        assert_eq!(schedules[1].repo, None);
        assert!(schedules[1].params.is_empty());
    }

    #[test]
    fn schedule_bad_name_is_a_cue_error() {
        let bad = r#"schedules: [{name: "Bad Name", cron: "* * * * *", run: "w"}]"#;
        let err = load_schedules_str(bad).unwrap_err();
        assert!(err.to_string().contains("cue export failed"), "{err}");
    }

    #[test]
    fn schedule_empty_cron_is_a_cue_error() {
        let bad = r#"schedules: [{name: "x", cron: "", run: "w"}]"#;
        let err = load_schedules_str(bad).unwrap_err();
        assert!(err.to_string().contains("cue export failed"), "{err}");
    }

    #[test]
    fn loads_named_checks() {
        let source = r#"
checks: [
    {name: "test", command: "cargo test"},
    {
        name: "clippy", command: "cargo clippy", cwd: "crates/x",
        expectExit: 0, timeout: "5m",
        environmentPolicy: "strip_rk_spawn",
        toolchain: "mise rust@1.95.0",
    },
]
"#;
        let checks = load_checks_str(source).unwrap();
        assert_eq!(checks.len(), 2);
        assert_eq!(checks[0].name, "test");
        assert_eq!(checks[0].command, "cargo test");
        assert!(checks[0].cwd.is_none());
        assert!(checks[0].expect_exit.is_none());
        assert!(checks[0].timeout.is_none());
        assert_eq!(checks[1].cwd.as_deref(), Some("crates/x"));
        assert_eq!(checks[1].expect_exit, Some(0));
        assert_eq!(checks[1].timeout.as_deref(), Some("5m"));
        assert_eq!(
            checks[1].environment_policy,
            CheckEnvironmentPolicy::StripRkSpawn
        );
        assert_eq!(checks[1].toolchain.as_deref(), Some("mise rust@1.95.0"));
    }

    #[test]
    fn repository_policy_loads_versioned_work_and_delivery_behavior() {
        let policy = load_repository_policy_str(
            r#"
            repo: {
                work: {
                    branch: "agents/{{task}}/{{agent}}"
                    worktree: "{{repo}}/{{task}}/{{agent}}"
                }
                delivery: {
                    target: "agent-base"
                    mode: "merge-push"
                    remote: "upstream"
                    remoteBranch: "review/{{branch}}"
                    deleteSource: false
                }
            }
            "#,
        )
        .unwrap();

        assert_eq!(policy.work.branch, "agents/{{task}}/{{agent}}");
        assert_eq!(policy.work.worktree, "{{repo}}/{{task}}/{{agent}}");
        assert_eq!(policy.delivery.target, "agent-base");
        assert_eq!(policy.delivery.mode, DeliveryMode::MergePush);
        assert_eq!(policy.delivery.remote, "upstream");
        assert_eq!(policy.delivery.remote_branch, "review/{{branch}}");
        assert!(!policy.delivery.delete_source);
    }

    #[test]
    fn repository_policy_defaults_preserve_existing_behavior() {
        let policy = load_repository_policy_str("repo: {}").unwrap();
        assert_eq!(policy, RepositoryPolicy::default());
    }

    #[test]
    fn repository_policy_rejects_unsafe_or_non_unique_worktree_templates() {
        for source in [
            r#"repo: {work: {worktree: "../outside/{{agent}}"}}"#,
            r#"repo: {work: {worktree: "shared"}}"#,
            r#"repo: {work: {branch: "rat/{{unknown}}/{{agent}}"}}"#,
            r#"repo: {work: {branch: "rat//{{agent}}"}}"#,
            r#"repo: {delivery: {target: "bad..target"}}"#,
            r#"repo: {delivery: {remote: "--upload-pack=oops"}}"#,
        ] {
            assert!(load_repository_policy_str(source).is_err(), "{source}");
        }
    }

    #[test]
    fn check_bad_name_is_a_cue_error() {
        let bad = r#"checks: [{name: "Bad Name", command: "x"}]"#;
        let err = load_checks_str(bad).unwrap_err();
        assert!(err.to_string().contains("cue export failed"), "{err}");
    }

    #[test]
    fn check_missing_command_is_a_cue_error() {
        let bad = r#"checks: [{name: "x"}]"#;
        let err = load_checks_str(bad).unwrap_err();
        assert!(err.to_string().contains("cue export failed"), "{err}");
    }

    #[test]
    fn check_unknown_environment_policy_is_a_cue_error() {
        let bad =
            r#"checks: [{name: "x", command: "true", environmentPolicy: "ambient_magic"}]"#;
        let err = load_checks_str(bad).unwrap_err();
        assert!(err.to_string().contains("environmentPolicy"), "{err}");
    }
}

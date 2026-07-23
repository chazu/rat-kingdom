//! Workflow definitions: CUE files loaded through the `cue` CLI (full CUE
//! semantics, zero build-time deps), aspect weaving, and agent/model
//! resolution. Pure definition layer — execution lives in the daemon.

pub mod resolve;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

const SCHEMA: &str = include_str!("schema.cue");
const TRIGGER_SCHEMA: &str = include_str!("triggers-schema.cue");
const SCHEDULE_SCHEMA: &str = include_str!("schedules-schema.cue");
const CHECK_SCHEMA: &str = include_str!("checks-schema.cue");

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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpawnStep {
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
/// verdict the runner cannot forge. The `{exit, stdout, stderr}` result lands
/// in `ctx.previousResult` so a following `evaluate {expect: {exit: 0}}` (or a
/// `when`) can gate the merge; a red check fails the instance fail-closed.
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
    /// If set, the step itself fails the instance when the actual exit code
    /// differs — a fail-closed inline gate. If unset, the exit is only
    /// captured for a following `evaluate`/`when` to route on. For a named check,
    /// this overrides the check's own `expectExit` when set.
    #[serde(default, rename = "expectExit")]
    pub expect_exit: Option<i64>,
    /// Hard wall-clock bound; a suite still running when it elapses is killed
    /// and the step fails closed. For a named check, the check's own timeout
    /// applies unless the step sets one explicitly (a non-default value).
    #[serde(default = "default_run_timeout")]
    pub timeout: String,
}

fn default_run_timeout() -> String {
    "10m".into()
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
}

/// Load and validate every `#Check` in one repo's `checks.cue` registry.
pub fn load_checks(file: &Path) -> rk_core::Result<Vec<Check>> {
    let source = std::fs::read_to_string(file)
        .map_err(|e| rk_core::Error::other(format!("read {}: {e}", file.display())))?;
    load_checks_str(&source)
}

/// Load named checks from source text (see [`load_checks`]).
pub fn load_checks_str(source: &str) -> rk_core::Result<Vec<Check>> {
    let dir = tempfile_dir()?;
    std::fs::write(dir.join("schema.cue"), CHECK_SCHEMA)?;
    std::fs::write(dir.join("checks.cue"), ensure_checks_package(source))?;
    let json = cue_export(&dir, "checks")?;
    let checks: Vec<Check> = serde_json::from_str(&json)
        .map_err(|e| rk_core::Error::other(format!("checks JSON did not match schema: {e}")))?;
    std::fs::remove_dir_all(&dir).ok();
    Ok(checks)
}

fn ensure_checks_package(source: &str) -> String {
    if source.trim_start().starts_with("package ") || source.contains("\npackage ") {
        source.to_string()
    } else {
        format!("package checks\n\n{source}")
    }
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
    std::fs::write(dir.join("input.cue"), render_inputs(inputs)?)?;

    // Pass 1: declared params → required-check + defaults.
    let params_json = cue_export(&dir, "workflow.params")?;
    let params: HashMap<String, Param> = serde_json::from_str(&params_json)
        .map_err(|e| rk_core::Error::other(format!("workflow params malformed: {e}")))?;
    let mut effective = inputs.clone();
    for (name, param) in &params {
        if effective.contains_key(name) {
            continue;
        }
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
            None => {}
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
    let dir = tempfile_dir()?;
    std::fs::write(dir.join("schema.cue"), TRIGGER_SCHEMA)?;
    std::fs::write(dir.join("triggers.cue"), ensure_triggers_package(source))?;
    let json = cue_export(&dir, "triggers")?;
    let triggers: Vec<Trigger> = serde_json::from_str(&json)
        .map_err(|e| rk_core::Error::other(format!("triggers JSON did not match schema: {e}")))?;
    std::fs::remove_dir_all(&dir).ok();
    Ok(triggers)
}

fn ensure_triggers_package(source: &str) -> String {
    if source.trim_start().starts_with("package ") || source.contains("\npackage ") {
        source.to_string()
    } else {
        format!("package triggers\n\n{source}")
    }
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
    let dir = tempfile_dir()?;
    std::fs::write(dir.join("schema.cue"), SCHEDULE_SCHEMA)?;
    std::fs::write(dir.join("schedules.cue"), ensure_schedules_package(source))?;
    let json = cue_export(&dir, "schedules")?;
    let schedules: Vec<Schedule> = serde_json::from_str(&json)
        .map_err(|e| rk_core::Error::other(format!("schedules JSON did not match schema: {e}")))?;
    std::fs::remove_dir_all(&dir).ok();
    Ok(schedules)
}

fn ensure_schedules_package(source: &str) -> String {
    if source.trim_start().starts_with("package ") || source.contains("\npackage ") {
        source.to_string()
    } else {
        format!("package schedules\n\n{source}")
    }
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
    fn missing_required_param_is_rejected() {
        let err = load_str(SAMPLE, &HashMap::new()).unwrap_err();
        assert!(err.to_string().contains("taskId"), "{err}");
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
    {name: "clippy", command: "cargo clippy", cwd: "crates/x", expectExit: 0, timeout: "5m"},
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
}

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Workflow {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub params: HashMap<String, Param>,
    #[serde(default)]
    pub agents: HashMap<String, AgentProfile>,
    pub steps: Vec<Step>,
    #[serde(default)]
    pub aspects: Vec<Aspect>,
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
    ]
}
"#;
        let wf = load_str(source, &HashMap::new()).unwrap();
        assert_eq!(wf.steps.len(), 4);
        // Default dismiss_all merges (no_merge defaults false).
        assert!(matches!(&wf.steps[2], Step::DismissAll(d) if !d.no_merge));
        // noMerge parked variant.
        assert!(matches!(&wf.steps[3], Step::DismissAll(d) if d.no_merge));
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
}

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvaluateStep {
    pub expect: Value,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DismissStep {
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
        };
        if actual != step_type {
            return false;
        }
    }
    if let Some(role) = &matcher.role {
        match step {
            Step::Spawn(spawn) if &spawn.role == role => {}
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
}

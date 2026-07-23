//! The shipped example workflows must always load — they are the onboarding
//! surface, and a schema drift that breaks them should fail CI, not users.

use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;

fn examples_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("examples")
        .join("workflows")
}

#[test]
fn all_shipped_examples_load() {
    let dir = examples_dir();
    let defs = rk_workflow::definitions(&dir);
    assert!(
        defs.len() >= 2,
        "expected shipped examples in {}",
        dir.display()
    );
    // Cover every required-without-default param across the shipped set so a new
    // example that adds one is caught here rather than by a user.
    let inputs = HashMap::from([
        ("taskId".to_string(), json!("example-task")),
        ("description".to_string(), json!("example description")),
        ("question".to_string(), json!("How does the tuplespace work?")),
    ]);
    for def in defs {
        let workflow = rk_workflow::load(&def, &inputs)
            .unwrap_or_else(|e| panic!("{} failed to load: {e}", def.display()));
        assert!(!workflow.steps.is_empty(), "{}", def.display());
    }
}

#[test]
fn shipped_example_triggers_load() {
    let file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("examples")
        .join("triggers.cue");
    let triggers = rk_workflow::load_triggers(&file)
        .unwrap_or_else(|e| panic!("{} failed to load: {e}", file.display()));
    assert!(!triggers.is_empty(), "example triggers should not be empty");
    // Every example trigger names a workflow that ships in examples/workflows.
    let workflows: Vec<String> = rk_workflow::definitions(&examples_dir())
        .iter()
        .filter_map(|p| p.file_stem().map(|s| s.to_string_lossy().to_string()))
        .collect();
    for t in &triggers {
        assert!(
            workflows.contains(&t.run),
            "trigger '{}' runs unknown workflow '{}'",
            t.name,
            t.run
        );
    }
}

#[test]
fn reviewer_drives_rework_loads_and_routes() {
    use rk_workflow::Step;
    let inputs = HashMap::from([
        ("taskId".to_string(), json!("fix-login")),
        ("maxRounds".to_string(), json!(4)),
    ]);
    let workflow =
        rk_workflow::load(&examples_dir().join("reviewer-drives-rework.cue"), &inputs).unwrap();

    // Top level: spawn, wait, evaluate, dismiss, repeat.
    let Step::Repeat(repeat) = workflow.steps.last().unwrap() else {
        panic!("last step should be a repeat loop");
    };
    // The `max` came from _input.maxRounds — a real, bounded cap.
    assert_eq!(repeat.max, 4);

    // The loop body ends in a `when` routing on the read verdict.
    let read = repeat
        .steps
        .iter()
        .find_map(|s| match s {
            Step::Read(r) => Some(r),
            _ => None,
        })
        .expect("a read step lifting the verdict");
    assert_eq!(read.into, "verdict");
    assert_eq!(read.field.as_deref(), Some("recommendation"));

    let Step::When(when) = repeat.steps.last().unwrap() else {
        panic!("loop body should end in a when");
    };
    assert_eq!(when.var, "verdict");
    // APPROVE merges then breaks; STOP aborts; REWORK loops back.
    assert!(matches!(when.cases["APPROVE"].last().unwrap(), Step::Break));
    assert!(when.cases["STOP"]
        .iter()
        .any(|s| matches!(s, Step::Stop(_))));
    assert!(when.cases["REWORK"]
        .iter()
        .any(|s| matches!(s, Step::Spawn(sp) if sp.role == "rat")));
}

#[test]
fn steward_loads_and_routes() {
    use rk_workflow::Step;
    // Trigger passes {taskId, branch, repo}; the rest default. Loads with just
    // taskId, proving every other param is defaulted (the reactor supplies the
    // real branch/repo at fire time).
    let inputs = HashMap::from([("taskId".to_string(), json!("fix-login"))]);
    let workflow = rk_workflow::load(&examples_dir().join("steward.cue"), &inputs).unwrap();

    // The reviewer chains onto the completed branch (spawn.branch set), so the
    // gates below run against that work — not a fresh branch off HEAD.
    let Step::Spawn(spawn) = &workflow.steps[0] else {
        panic!("steward must start by spawning the reviewer");
    };
    assert_eq!(spawn.role, "reviewer");
    assert!(
        spawn.branch.is_some(),
        "reviewer must chain onto the branch param"
    );

    // Two fail-closed gates precede the merge decision: a protected-path POLICY
    // gate and the repo's real RUN gate, each a `run` + `evaluate {exit: 0}`.
    let runs: Vec<&rk_workflow::RunStep> = workflow
        .steps
        .iter()
        .filter_map(|s| match s {
            Step::Run(r) => Some(r),
            _ => None,
        })
        .collect();
    assert!(
        runs.iter()
            .any(|r| r.command.contains("git diff --name-only")),
        "a protected-path policy gate must run before merge"
    );
    let gate_evaluates = workflow
        .steps
        .iter()
        .filter(|s| matches!(s, Step::Evaluate(e) if e.expect.get("exit").is_some()))
        .count();
    assert!(
        gate_evaluates >= 2,
        "both the policy and run gates must fail closed on non-zero exit"
    );

    // The verdict is lifted, then routed on.
    let read = workflow
        .steps
        .iter()
        .find_map(|s| match s {
            Step::Read(r) => Some(r),
            _ => None,
        })
        .expect("a read step lifting the verdict");
    assert_eq!(read.into, "verdict");
    assert_eq!(read.field.as_deref(), Some("recommendation"));

    let Step::When(when) = workflow.steps.last().unwrap() else {
        panic!("steward should end in a when routing on the verdict");
    };
    assert_eq!(when.var, "verdict");
    // APPROVE is the ONLY path that lands (auto-merge).
    assert!(
        when.cases["APPROVE"]
            .iter()
            .any(|s| matches!(s, Step::Land(_))),
        "APPROVE must land the branch"
    );
    assert!(
        !when.cases["REWORK"]
            .iter()
            .any(|s| matches!(s, Step::Land(_))),
        "REWORK must never land"
    );
    // REWORK files a durable ticket rather than looping a rework rat here.
    assert!(
        when.cases["REWORK"]
            .iter()
            .any(|s| matches!(s, Step::Run(r) if r.command.contains("rk ticket new"))),
        "REWORK must file a follow-up ticket"
    );
    // STOP escalates to the operator via a need tuple and holds the branch.
    assert!(
        when.cases["STOP"]
            .iter()
            .any(|s| matches!(s, Step::Run(r) if r.command.contains("rk out need"))),
        "STOP must escalate via a need tuple"
    );
    // Unknown verdict: escalate and fail loudly.
    assert!(
        !when.default.is_empty(),
        "unknown verdicts must route to a default arm"
    );
    assert!(when.default.iter().any(|s| matches!(s, Step::Stop(_))));
}

#[test]
fn code_review_resolves_reviewer_profile() {
    let inputs = HashMap::from([("taskId".to_string(), json!("t1"))]);
    let workflow = rk_workflow::load(&examples_dir().join("code-review.cue"), &inputs).unwrap();
    // The reviewer spawn resolves through the named profile to the cheap model.
    let reviewer = workflow
        .steps
        .iter()
        .find_map(|s| match s {
            rk_workflow::Step::Spawn(sp) if sp.role == "reviewer" => Some(sp),
            _ => None,
        })
        .expect("reviewer spawn step");
    let resolved =
        rk_workflow::resolve::resolve(reviewer, &workflow.agents, &HashMap::new(), "fake").unwrap();
    assert_eq!(resolved.harness, "claude");
    assert_eq!(resolved.model.as_deref(), Some("haiku"));
}

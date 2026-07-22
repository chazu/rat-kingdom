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
    let inputs = HashMap::from([
        ("taskId".to_string(), json!("example-task")),
        ("description".to_string(), json!("example description")),
    ]);
    for def in defs {
        let workflow = rk_workflow::load(&def, &inputs)
            .unwrap_or_else(|e| panic!("{} failed to load: {e}", def.display()));
        assert!(!workflow.steps.is_empty(), "{}", def.display());
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

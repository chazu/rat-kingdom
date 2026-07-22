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

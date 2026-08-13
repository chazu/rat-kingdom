use std::collections::BTreeMap;

use chrono::{TimeZone, Utc};
use rk_core::action::{
    canonical_digest, canonical_json_bytes, ActionKind, ActionProposal, ActionRisk, ActionScope,
    FactoryAction, RepoScope, WorkflowRunAction,
};
use serde_json::{json, Value};

fn params(entries: &[(&str, Value)]) -> BTreeMap<String, Value> {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_owned(), value.clone()))
        .collect()
}

fn workflow_run(params: BTreeMap<String, Value>) -> WorkflowRunAction {
    WorkflowRunAction {
        name: "verify".into(),
        repo: "rat-kingdom".into(),
        repo_identity: "repo-01".into(),
        repo_path: "/srv/repos/rat-kingdom".into(),
        params,
        coordinator: Some("factory".into()),
    }
}

fn proposal(requester: &str) -> ActionProposal {
    let action = FactoryAction::WorkflowRun(workflow_run(params(&[("release", json!(true))])));
    ActionProposal {
        schema: 1,
        id: "proposal-01".into(),
        digest: String::new(),
        kind: ActionKind::WorkflowRun,
        risk: action.risk(),
        scope: ActionScope {
            repo: RepoScope {
                identity: "repo-01".into(),
                path: "/srv/repos/rat-kingdom".into(),
            },
        },
        requester: requester.into(),
        action,
        created_at: Utc.with_ymd_and_hms(2026, 8, 13, 10, 0, 0).unwrap(),
        expires_at: Utc.with_ymd_and_hms(2026, 8, 13, 10, 5, 0).unwrap(),
        status: "proposed".into(),
    }
}

#[test]
fn test_workflow_run_digest_is_stable_across_map_insertion_order() {
    let first = workflow_run(params(&[("beta", json!(2)), ("alpha", json!(1))]));
    let second = workflow_run(params(&[("alpha", json!(1)), ("beta", json!(2))]));

    assert_eq!(
        canonical_digest(&first).unwrap(),
        canonical_digest(&second).unwrap()
    );
}

#[test]
fn test_digest_changes_when_repo_identity_or_path_changes() {
    let baseline = workflow_run(BTreeMap::new());
    let mut changed_identity = baseline.clone();
    changed_identity.repo_identity = "repo-02".into();
    let mut changed_path = baseline.clone();
    changed_path.repo_path = "/srv/repos/other".into();

    let baseline_digest = canonical_digest(&baseline).unwrap();
    assert_ne!(
        baseline_digest,
        canonical_digest(&changed_identity).unwrap()
    );
    assert_ne!(baseline_digest, canonical_digest(&changed_path).unwrap());
}

#[test]
fn test_digest_covers_authenticated_requester() {
    assert_ne!(
        canonical_digest(&proposal("operator-a")).unwrap(),
        canonical_digest(&proposal("operator-b")).unwrap()
    );
}

#[test]
fn test_digest_covers_action_kind_and_schema() {
    let baseline = json!({
        "schema": 1,
        "kind": ActionKind::WorkflowRun,
        "proposal": proposal("operator-a")
    });
    let changed_kind = json!({
        "schema": 1,
        "kind": "workflow.run.v2",
        "proposal": proposal("operator-a")
    });
    let changed_schema = json!({
        "schema": 2,
        "kind": ActionKind::WorkflowRun,
        "proposal": proposal("operator-a")
    });

    let baseline_digest = canonical_digest(&baseline).unwrap();
    assert_ne!(baseline_digest, canonical_digest(&changed_kind).unwrap());
    assert_ne!(baseline_digest, canonical_digest(&changed_schema).unwrap());
}

#[test]
fn test_workflow_run_is_mutation_risk() {
    let action = FactoryAction::WorkflowRun(workflow_run(BTreeMap::new()));
    assert_eq!(action.risk(), ActionRisk::Mutation);
}

#[test]
fn test_canonical_json_has_sorted_object_keys() {
    let value = json!({"params": {"zeta": 1, "alpha": 2}, "action": "workflow.run"});
    let json = String::from_utf8(canonical_json_bytes(&value).unwrap()).unwrap();

    assert_eq!(
        json,
        r#"{"action":"workflow.run","params":{"alpha":2,"zeta":1}}"#
    );
}

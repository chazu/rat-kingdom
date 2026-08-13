use std::collections::BTreeMap;

use chrono::{TimeZone, Utc};
use rk_core::action::{
    action_digest, canonical_digest, canonical_json_bytes, ActionKind, ActionProposal, ActionRisk,
    ActionScope, FactoryAction, RepoScope, WorkflowRunAction,
};
use serde::Serialize;
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
        nonce: "nonce-01".into(),
        created_at: Utc.with_ymd_and_hms(2026, 8, 13, 10, 0, 0).unwrap(),
        expires_at: Utc.with_ymd_and_hms(2026, 8, 13, 10, 5, 0).unwrap(),
        status: "proposed".into(),
    }
}

#[derive(Serialize)]
struct FloatPayload {
    value: f64,
}

#[derive(Serialize)]
struct NestedFloatPayload {
    values: Vec<FloatPayload>,
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

#[test]
fn test_canonical_json_rejects_nan_before_value_serialization() {
    let err = canonical_json_bytes(&FloatPayload { value: f64::NAN }).unwrap_err();

    assert!(
        err.to_string().contains("non-finite float"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_canonical_json_rejects_infinities_recursively_before_value_serialization() {
    for value in [f64::INFINITY, f64::NEG_INFINITY] {
        let err = canonical_json_bytes(&NestedFloatPayload {
            values: vec![FloatPayload { value }],
        })
        .unwrap_err();

        assert!(
            err.to_string().contains("non-finite float"),
            "unexpected error for {value}: {err}"
        );
    }
}

#[test]
fn test_action_digest_payload_excludes_proposal_digest_and_status() {
    let proposal = proposal("operator-a");
    let baseline = proposal.digest_payload();
    let mut changed_status = proposal.clone();
    changed_status.status = "approved".into();
    let mut changed_digest = proposal.clone();
    changed_digest.digest = "tampered".into();

    assert_eq!(
        action_digest(&baseline).unwrap(),
        action_digest(&changed_status.digest_payload()).unwrap()
    );
    assert_eq!(
        action_digest(&baseline).unwrap(),
        action_digest(&changed_digest.digest_payload()).unwrap()
    );
}

#[test]
fn test_action_digest_payload_binds_nonce_and_expiry() {
    let proposal = proposal("operator-a");
    let baseline = proposal.digest_payload();
    let mut changed_nonce_proposal = proposal.clone();
    changed_nonce_proposal.nonce = "nonce-02".into();
    let changed_nonce = changed_nonce_proposal.digest_payload();
    let mut changed_expiry_proposal = proposal.clone();
    changed_expiry_proposal.expires_at = Utc.with_ymd_and_hms(2026, 8, 13, 10, 10, 0).unwrap();
    let changed_expiry = changed_expiry_proposal.digest_payload();

    let baseline_digest = action_digest(&baseline).unwrap();
    assert_ne!(baseline_digest, action_digest(&changed_nonce).unwrap());
    assert_ne!(baseline_digest, action_digest(&changed_expiry).unwrap());
}

use std::collections::BTreeMap;

use chrono::{TimeZone, Utc};
use rk_core::action::{
    action_digest, canonical_digest, canonical_json_bytes, ActionKind, ActionProposal, ActionRisk,
    ActionScope, FactoryAction, RepoScope, TicketGraphApplyAction, WorkflowRunAction,
};
use rk_core::product_to_code::contracts::{
    AcceptanceCriterion, InitiativeContract, TicketGraph, TicketGraphEdge, TicketGraphNode,
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

fn ticket_graph_apply() -> TicketGraphApplyAction {
    let initiative = InitiativeContract {
        id: "INIT-product-to-code".into(),
        title: "Product to code".into(),
        scope: "offline-contracts".into(),
        acceptance_criteria: vec![
            AcceptanceCriterion {
                id: "AC-1".into(),
                text: "Contracts exist".into(),
                browser_acceptance_applicable: false,
            },
            AcceptanceCriterion {
                id: "AC-2".into(),
                text: "Tests exist".into(),
                browser_acceptance_applicable: false,
            },
        ],
        browser_acceptance_applicable: false,
    };
    let graph = TicketGraph {
        id: "GRAPH-product-to-code".into(),
        initiative_id: initiative.id.clone(),
        nodes: vec![
            TicketGraphNode {
                id: "NODE-contracts".into(),
                title: "Add contracts".into(),
                description: "Add typed contracts".into(),
                acceptance_criterion_ids: vec!["AC-1".into()],
                feature_set_ids: Vec::new(),
                browser_acceptance_applicable: false,
                browser_acceptance_criterion_ids: Vec::new(),
            },
            TicketGraphNode {
                id: "NODE-tests".into(),
                title: "Add tests".into(),
                description: "Add regression tests".into(),
                acceptance_criterion_ids: vec!["AC-2".into()],
                feature_set_ids: Vec::new(),
                browser_acceptance_applicable: false,
                browser_acceptance_criterion_ids: Vec::new(),
            },
        ],
        edges: vec![TicketGraphEdge {
            from: "NODE-contracts".into(),
            to: "NODE-tests".into(),
            relationship: "depends_on".into(),
        }],
    };
    let acceptance_ids = initiative
        .acceptance_criteria
        .iter()
        .map(|criterion| criterion.id.clone())
        .collect::<Vec<_>>();
    let apply_plan = graph.apply_plan("rat-kingdom", &acceptance_ids).unwrap();
    TicketGraphApplyAction {
        repo: "rat-kingdom".into(),
        repo_identity: "repo-01".into(),
        repo_path: "/srv/repos/rat-kingdom".into(),
        graph,
        initiative,
        apply_plan,
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
fn test_ticket_graph_apply_kind_risk_and_digest_are_canonical() {
    let action = FactoryAction::TicketGraphApply(ticket_graph_apply());
    let same = FactoryAction::TicketGraphApply(ticket_graph_apply());
    let mut changed = ticket_graph_apply();
    changed.apply_plan.topological_order.reverse();

    assert_eq!(action.kind(), ActionKind::TicketGraphApply);
    assert_eq!(action.risk(), ActionRisk::Mutation);
    assert_eq!(
        canonical_digest(&action).unwrap(),
        canonical_digest(&same).unwrap()
    );
    assert_ne!(
        canonical_digest(&action).unwrap(),
        canonical_digest(&FactoryAction::TicketGraphApply(changed)).unwrap()
    );
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

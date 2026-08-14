use chrono::{TimeZone, Utc};
use rk_core::config::ReactorConfig;
use rk_core::paths::Layout;
use rk_core::sdlc::{
    CiSignal, ConfiguredSourceName, Correlation, SignalEnvelope, SignalKind, SignalPayload,
    SignalSourcePrincipal,
};
use rk_core::tuple::{Category, Pattern, Tuple};
use rk_daemon::reactor::{Reactor, REACTOR_INSTANCE};
use rk_daemon::supervisor::Supervisor;
use rk_daemon::tickets::Tickets;
use rk_daemon::workflow_exec::WorkflowEngine;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

fn build_reactor(layout: &Layout, space: rk_space::Space) -> Arc<Reactor> {
    let tickets = Arc::new(Tickets::new(space.clone(), "test-castle".into()));
    let supervisor = Arc::new(
        Supervisor::new(
            layout.clone(),
            "test-castle".into(),
            "fake".into(),
            rk_ledger::Budget::default(),
            rk_ledger::FleetBudget::default(),
            space.clone(),
            tickets.clone(),
        )
        .unwrap(),
    );
    let engine = Arc::new(WorkflowEngine::new(
        layout.clone(),
        supervisor.clone(),
        space.clone(),
        tickets.clone(),
        Default::default(),
        Default::default(),
        "fake".into(),
        false,
        false,
        false,
        Vec::new(),
        vec!["main".into(), "master".into()],
    ));
    Arc::new(Reactor::new(
        space,
        engine,
        tickets,
        Some(supervisor),
        layout.clone(),
        ReactorConfig::default(),
    ))
}

fn source() -> ConfiguredSourceName {
    ConfiguredSourceName::new("local-ci").unwrap()
}

fn principal() -> SignalSourcePrincipal {
    SignalSourcePrincipal::for_source(&source())
}

fn ci(delivery_id: &str, kind: SignalKind, status: &str, seq: i64) -> SignalEnvelope {
    SignalEnvelope {
        kind,
        source: source(),
        delivery_id: delivery_id.into(),
        occurred_at: Utc.timestamp_opt(seq, 0).unwrap(),
        observed_at: Utc.timestamp_opt(seq, 0).unwrap(),
        correlation: Correlation {
            repo: Some("repo".into()),
            branch: Some("main".into()),
            workflow: Some("test".into()),
            job: Some("unit".into()),
            commit_sha: Some("abc123".into()),
            ..Default::default()
        },
        summary: format!("CI {status}"),
        refs: vec![],
        attributes: BTreeMap::new(),
        payload: SignalPayload::Ci(CiSignal {
            status: status.into(),
            conclusion: None,
        }),
    }
}

fn run_after(space: &rk_space::Space, layout: &Layout) {
    build_reactor(layout, space.clone()).run_cycle().unwrap();
}

fn diagnostics(space: &rk_space::Space) -> Vec<rk_core::tuple::Tuple> {
    space
        .scan(&Pattern::category(Category::Need).identity("sdlc_ci_diagnostic"))
        .unwrap()
}

fn recoveries(space: &rk_space::Space) -> Vec<rk_core::tuple::Tuple> {
    space
        .scan(&Pattern::category(Category::Fact).identity("sdlc_ci_recovered"))
        .unwrap()
}

#[test]
fn test_ci_failed_transition_enqueues_one_diagnostic_reactor_tuple() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();

    let receipt = space
        .accept_sdlc_signal(ci("d1", SignalKind::CiFailed, "failure", 1), principal())
        .unwrap();
    assert!(receipt.transition_emitted);
    run_after(&space, &layout);

    let diagnostics = diagnostics(&space);
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(diagnostics[0].instance, REACTOR_INSTANCE);
    assert_eq!(diagnostics[0].payload["family"], "ci");
}

#[test]
fn test_duplicate_ci_occurrence_does_not_enqueue_second_reaction() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();

    space
        .accept_sdlc_signal(ci("d1", SignalKind::CiFailed, "failure", 1), principal())
        .unwrap();
    run_after(&space, &layout);
    space
        .accept_sdlc_signal(ci("d1", SignalKind::CiFailed, "failure", 1), principal())
        .unwrap();
    run_after(&space, &layout);

    assert_eq!(diagnostics(&space).len(), 1);
}

#[test]
fn test_ci_failed_to_failed_new_delivery_same_state_does_not_enqueue_second_reaction() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();

    space
        .accept_sdlc_signal(ci("d1", SignalKind::CiFailed, "failure", 1), principal())
        .unwrap();
    run_after(&space, &layout);
    let receipt = space
        .accept_sdlc_signal(ci("d2", SignalKind::CiFailed, "failure", 2), principal())
        .unwrap();
    assert!(!receipt.transition_emitted);
    run_after(&space, &layout);

    assert_eq!(diagnostics(&space).len(), 1);
}

#[test]
fn test_ci_recovered_resets_failure_state_for_future_failures() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();

    space
        .accept_sdlc_signal(ci("d1", SignalKind::CiFailed, "failure", 1), principal())
        .unwrap();
    run_after(&space, &layout);
    space
        .accept_sdlc_signal(ci("d2", SignalKind::CiRecovered, "success", 2), principal())
        .unwrap();
    run_after(&space, &layout);
    space
        .accept_sdlc_signal(ci("d3", SignalKind::CiFailed, "failure", 3), principal())
        .unwrap();
    run_after(&space, &layout);

    assert_eq!(diagnostics(&space).len(), 2);
    assert_eq!(recoveries(&space).len(), 1);
}

#[test]
fn test_ci_recovered_without_prior_failure_does_not_enqueue_diagnostic() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();

    space
        .accept_sdlc_signal(ci("d1", SignalKind::CiRecovered, "success", 1), principal())
        .unwrap();
    run_after(&space, &layout);

    assert!(diagnostics(&space).is_empty());
}

#[test]
fn test_ci_recovery_kind_acknowledges_even_if_projected_status_is_failed() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();
    let subject = "repo:main:test:unit:abc123";

    space
        .out(Tuple::new(
            Category::Fact,
            "ci",
            "sdlc:current:local-ci:repo:main:test:unit:abc123",
            "source:local-ci",
            serde_json::json!({
                "source": "local-ci",
                "family": "ci",
                "subject": subject,
                "current": {"status": "failed", "conclusion": "failure"}
            }),
        ))
        .unwrap();
    space
        .out(Tuple::new(
            Category::Need,
            "ci",
            "sdlc_ci_diagnostic",
            REACTOR_INSTANCE,
            serde_json::json!({"family": "ci", "subject": subject}),
        ))
        .unwrap();
    space
        .out(Tuple::new(
            Category::Event,
            "ci",
            "sdlc:transition:local-ci:ci:repo:main:test:unit:abc123",
            "source:local-ci",
            serde_json::json!({
                "source": "local-ci",
                "delivery_id": "recovery-with-failed-status",
                "family": "ci",
                "subject": subject,
                "kind": "ci_recovered"
            }),
        ))
        .unwrap();

    run_after(&space, &layout);

    assert_eq!(diagnostics(&space).len(), 1);
    assert_eq!(recoveries(&space).len(), 1);
}

#[test]
fn test_ci_diagnostic_reaction_uses_phase2_proposal_path_for_mutation() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();

    space
        .accept_sdlc_signal(ci("d1", SignalKind::CiFailed, "failure", 1), principal())
        .unwrap();
    run_after(&space, &layout);

    let payload = diagnostics(&space).remove(0).payload;
    assert_eq!(payload["proposal_path"], "phase2_advisory_proposal");
    assert_eq!(payload["phase2"]["requires_approval"], true);
    assert_eq!(payload["phase2"]["effect"], "diagnostic_only");
}

#[test]
fn test_ci_diagnostic_reaction_has_no_mutating_action_fields() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();

    space
        .accept_sdlc_signal(ci("d1", SignalKind::CiFailed, "failure", 1), principal())
        .unwrap();
    run_after(&space, &layout);

    fn assert_no_mutating_fields(value: &Value) {
        match value {
            Value::Object(map) => {
                for (key, child) in map {
                    assert!(
                        !matches!(
                            key.as_str(),
                            "action" | "command" | "cmd" | "shell" | "exec" | "deploy" | "rollback"
                        ),
                        "mutating field present: {key}"
                    );
                    assert_no_mutating_fields(child);
                }
            }
            Value::Array(values) => {
                for child in values {
                    assert_no_mutating_fields(child);
                }
            }
            _ => {}
        }
    }

    assert_no_mutating_fields(&diagnostics(&space).remove(0).payload);
}

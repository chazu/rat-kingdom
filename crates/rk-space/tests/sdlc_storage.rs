use chrono::{TimeZone, Utc};
use rk_core::sdlc::{
    CiSignal, ConfiguredSourceName, Correlation, DeploymentSignal, ProductionAlertSignal,
    SignalEnvelope, SignalKind, SignalPayload, SignalSourcePrincipal,
};
use rk_core::tuple::{Category, Pattern};
use rk_space::Space;
use serde_json::Value;

fn ts(n: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(1_800_000_000 + n, 0).unwrap()
}

fn source() -> ConfiguredSourceName {
    ConfiguredSourceName::new("github").unwrap()
}

fn principal() -> SignalSourcePrincipal {
    SignalSourcePrincipal::for_source(&source())
}

fn ci(delivery_id: &str, status: &str, observed: i64) -> SignalEnvelope {
    SignalEnvelope {
        kind: if status == "success" {
            SignalKind::CiRecovered
        } else {
            SignalKind::CiFailed
        },
        source: source(),
        delivery_id: delivery_id.into(),
        occurred_at: ts(observed - 1),
        observed_at: ts(observed),
        correlation: Correlation {
            repo: Some("rat-kingdom".into()),
            branch: Some("main".into()),
            workflow: Some("ci".into()),
            job: Some("test".into()),
            commit_sha: Some("abc123".into()),
            ..Default::default()
        },
        summary: format!("ci {status}"),
        refs: vec![],
        attributes: Default::default(),
        payload: SignalPayload::Ci(CiSignal {
            status: status.into(),
            conclusion: None,
        }),
    }
}

fn deployment(delivery_id: &str, service: &str, version: &str, observed: i64) -> SignalEnvelope {
    SignalEnvelope {
        kind: SignalKind::DeploymentSucceeded,
        source: source(),
        delivery_id: delivery_id.into(),
        occurred_at: ts(observed - 1),
        observed_at: ts(observed),
        correlation: Correlation {
            environment: Some("prod".into()),
            service: Some(service.into()),
            ..Default::default()
        },
        summary: format!("deploy {service} {version}"),
        refs: vec![],
        attributes: Default::default(),
        payload: SignalPayload::Deployment(DeploymentSignal {
            environment: "prod".into(),
            service: service.into(),
            version: Some(version.into()),
        }),
    }
}

fn alert(delivery_id: &str, state: &str, observed: i64) -> SignalEnvelope {
    SignalEnvelope {
        kind: if state == "resolved" {
            SignalKind::ProductionAlertResolved
        } else {
            SignalKind::ProductionAlertFiring
        },
        source: source(),
        delivery_id: delivery_id.into(),
        occurred_at: ts(observed - 1),
        observed_at: ts(observed),
        correlation: Correlation {
            environment: Some("prod".into()),
            service: Some("api".into()),
            alert_key: Some("latency".into()),
            ..Default::default()
        },
        summary: format!("alert {state}"),
        refs: vec![],
        attributes: Default::default(),
        payload: SignalPayload::ProductionAlert(ProductionAlertSignal {
            environment: "prod".into(),
            service: "api".into(),
            alert_key: "latency".into(),
            severity: Some("page".into()),
            state: state.into(),
        }),
    }
}

#[test]
fn test_accept_signal_persists_receipt_after_store_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("space.sqlite");
    let space = Space::open(&path).unwrap();
    let first = space
        .accept_sdlc_signal(ci("d1", "failure", 1), principal())
        .unwrap();

    drop(space);
    let reopened = Space::open(&path).unwrap();
    let stored = reopened.get_sdlc_receipt(&source(), "d1").unwrap().unwrap();
    assert_eq!(stored, first);
}

#[test]
fn test_duplicate_occurrence_source_delivery_id_returns_existing_receipt() {
    let space = Space::open_in_memory().unwrap();
    let first = space
        .accept_sdlc_signal(ci("d1", "failure", 1), principal())
        .unwrap();
    let duplicate = space
        .accept_sdlc_signal(ci("d1", "success", 2), principal())
        .unwrap();
    assert_eq!(duplicate, first);
    assert_eq!(
        space
            .scan(&Pattern::category(Category::Event))
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn test_same_semantic_state_new_delivery_updates_last_seen_without_transition() {
    let space = Space::open_in_memory().unwrap();
    let first = space
        .accept_sdlc_signal(ci("d1", "failure", 1), principal())
        .unwrap();
    let second = space
        .accept_sdlc_signal(ci("d2", "failure", 2), principal())
        .unwrap();
    assert!(first.transition_emitted);
    assert!(!second.transition_emitted);

    let facts = space
        .current_sdlc_facts(Some("github"), Some("rat-kingdom"), None)
        .unwrap();
    assert_eq!(facts.len(), 1);
    assert_eq!(facts[0].payload["last_delivery_id"], "d2");
    assert_eq!(facts[0].payload["first_delivery_id"], "d1");
    let transitions = space
        .scan(
            &Pattern::category(Category::Event)
                .identity("sdlc:transition:github:rat-kingdom:ci:main:ci:test"),
        )
        .unwrap();
    assert_eq!(transitions.len(), 1);
}

#[test]
fn test_transaction_rolls_back_when_tuple_projection_fails() {
    let space = Space::open_in_memory().unwrap();
    space.enable_sdlc_rollback_injection_for_tests(true);
    assert!(space
        .accept_sdlc_signal(ci("d1", "failure", 1), principal())
        .is_err());
    assert!(space.get_sdlc_receipt(&source(), "d1").unwrap().is_none());
    assert!(space
        .current_sdlc_facts(Some("github"), Some("rat-kingdom"), None)
        .unwrap()
        .is_empty());
    assert!(space
        .scan(&Pattern::category(Category::Event))
        .unwrap()
        .is_empty());
    assert!(space
        .scan(&Pattern::category(Category::Fact))
        .unwrap()
        .is_empty());
}

#[test]
fn test_current_state_snapshot_tracks_latest_ci_transition() {
    let space = Space::open_in_memory().unwrap();
    space
        .accept_sdlc_signal(ci("d1", "failure", 1), principal())
        .unwrap();
    let receipt = space
        .accept_sdlc_signal(ci("d2", "success", 2), principal())
        .unwrap();
    assert!(receipt.transition_emitted);
    let transitions = space
        .scan(
            &Pattern::category(Category::Event)
                .identity("sdlc:transition:github:rat-kingdom:ci:main:ci:test"),
        )
        .unwrap();
    assert_eq!(transitions.len(), 2);
    let facts = space
        .current_sdlc_facts(Some("github"), Some("rat-kingdom"), None)
        .unwrap();
    assert_eq!(facts[0].payload["family"], "ci");
    assert_eq!(facts[0].payload["current"]["status"], "success");
}

#[test]
fn test_current_state_snapshot_tracks_latest_alert_transition() {
    let space = Space::open_in_memory().unwrap();
    space
        .accept_sdlc_signal(alert("a1", "firing", 1), principal())
        .unwrap();
    space
        .accept_sdlc_signal(alert("a2", "resolved", 2), principal())
        .unwrap();
    let facts = space
        .current_sdlc_facts(
            Some("github"),
            Some("prod"),
            Some("production_alert:api:latency"),
        )
        .unwrap();
    assert_eq!(facts.len(), 1);
    assert_eq!(facts[0].payload["family"], "production_alert");
    assert_eq!(facts[0].payload["current"]["state"], "resolved");
}

#[test]
fn test_deployment_provenance_current_fact_is_replaced_by_newer_deployment() {
    let space = Space::open_in_memory().unwrap();
    space
        .accept_sdlc_signal(deployment("p1", "api", "v1", 1), principal())
        .unwrap();
    space
        .accept_sdlc_signal(deployment("p2", "api", "v2", 2), principal())
        .unwrap();
    space
        .accept_sdlc_signal(deployment("p3", "web", "v1", 3), principal())
        .unwrap();

    let facts = space
        .current_sdlc_facts(Some("github"), Some("prod"), Some("deployment:api"))
        .unwrap();
    assert_eq!(facts.len(), 1);
    assert_eq!(
        facts[0].payload["current"]["version"],
        Value::String("v2".into())
    );
    assert_eq!(facts[0].payload["last_delivery_id"], "p2");
}

#[test]
fn test_receipt_lists_projected_event_and_fact_tuple_ids() {
    let space = Space::open_in_memory().unwrap();
    let receipt = space
        .accept_sdlc_signal(ci("d1", "failure", 1), principal())
        .unwrap();
    assert!(space
        .get(receipt.projected_event_id.parse().unwrap())
        .unwrap()
        .is_some());
    assert_eq!(receipt.projected_fact_ids.len(), 1);
    assert!(receipt
        .projected_fact_ids
        .iter()
        .all(|id| space.get(id.parse().unwrap()).unwrap().is_some()));
}

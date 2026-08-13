use chrono::{TimeZone, Utc};
use rk_core::sdlc::{
    CiSignal, ConfiguredSourceName, Correlation, DeploymentSignal, ProductionAlertSignal,
    SignalEnvelope, SignalKind, SignalPayload, SignalSourcePrincipal,
};
use rk_core::tuple::{Category, Pattern};
use rk_space::Space;
use serde_json::Value;
use std::sync::{Arc, Barrier};
use std::thread;

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
        .current_sdlc_facts(Some("github"), Some("ci"), None)
        .unwrap();
    assert_eq!(facts.len(), 1);
    assert_eq!(facts[0].payload["last_delivery_id"], "d2");
    assert_eq!(facts[0].payload["first_delivery_id"], "d1");
    let transitions = space
        .scan(
            &Pattern::category(Category::Event)
                .identity("sdlc:transition:github:ci:rat-kingdom:main:ci:test:abc123"),
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
        .current_sdlc_facts(Some("github"), Some("ci"), None)
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
                .identity("sdlc:transition:github:ci:rat-kingdom:main:ci:test:abc123"),
        )
        .unwrap();
    assert_eq!(transitions.len(), 2);
    let facts = space
        .current_sdlc_facts(Some("github"), Some("ci"), None)
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
            Some("production_alert"),
            Some("prod:api:latency"),
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
        .current_sdlc_facts(Some("github"), Some("deployment"), Some("prod:api"))
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

#[test]
fn test_state_key_contract_uses_literal_scope_and_exact_subject() {
    let space = Space::open_in_memory().unwrap();
    space
        .accept_sdlc_signal(ci("ci-key", "failure", 10), principal())
        .unwrap();
    space
        .accept_sdlc_signal(deployment("deploy-key", "api", "v1", 11), principal())
        .unwrap();
    space
        .accept_sdlc_signal(alert("alert-key", "firing", 12), principal())
        .unwrap();

    let ci_facts = space
        .current_sdlc_facts(
            Some("github"),
            Some("ci"),
            Some("rat-kingdom:main:ci:test:abc123"),
        )
        .unwrap();
    assert_eq!(ci_facts.len(), 1);
    assert_eq!(ci_facts[0].scope, "ci");
    assert_eq!(
        ci_facts[0].payload["subject"],
        "rat-kingdom:main:ci:test:abc123"
    );

    let deployment_facts = space
        .current_sdlc_facts(Some("github"), Some("deployment"), Some("prod:api"))
        .unwrap();
    assert_eq!(deployment_facts.len(), 1);
    assert_eq!(deployment_facts[0].scope, "deployment");
    assert_eq!(deployment_facts[0].payload["subject"], "prod:api");

    let alert_facts = space
        .current_sdlc_facts(
            Some("github"),
            Some("production_alert"),
            Some("prod:api:latency"),
        )
        .unwrap();
    assert_eq!(alert_facts.len(), 1);
    assert_eq!(alert_facts[0].scope, "production_alert");
    assert_eq!(alert_facts[0].payload["subject"], "prod:api:latency");
}

#[test]
fn test_projected_tuple_ids_are_deterministic_across_databases_and_replay() {
    let first_dir = tempfile::tempdir().unwrap();
    let second_dir = tempfile::tempdir().unwrap();
    let first_path = first_dir.path().join("space.sqlite");
    let second_path = second_dir.path().join("space.sqlite");

    let first = Space::open(&first_path).unwrap();
    let first_receipt = first
        .accept_sdlc_signal(ci("deterministic", "failure", 20), principal())
        .unwrap();
    drop(first);
    let replayed_receipt = Space::open(&first_path)
        .unwrap()
        .accept_sdlc_signal(ci("deterministic", "failure", 20), principal())
        .unwrap();
    let second_receipt = Space::open(&second_path)
        .unwrap()
        .accept_sdlc_signal(ci("deterministic", "failure", 20), principal())
        .unwrap();

    assert_eq!(
        replayed_receipt.projected_event_id,
        first_receipt.projected_event_id
    );
    assert_eq!(
        replayed_receipt.projected_fact_ids,
        first_receipt.projected_fact_ids
    );
    assert_eq!(
        second_receipt.projected_event_id,
        first_receipt.projected_event_id
    );
    assert_eq!(
        second_receipt.projected_fact_ids,
        first_receipt.projected_fact_ids
    );
}

#[test]
fn test_duplicate_delivery_race_independent_opens_returns_original_without_extra_tuples() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("space.sqlite");
    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();

    for _ in 0..2 {
        let path = path.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let space = Space::open(&path).unwrap();
            space
                .accept_sdlc_signal(ci("race", "failure", 30), principal())
                .unwrap()
        }));
    }

    let receipts = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(receipts[0], receipts[1]);

    let reopened = Space::open(&path).unwrap();
    assert_eq!(
        reopened
            .scan(&Pattern::category(Category::Event))
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        reopened
            .scan(&Pattern::category(Category::Fact))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn test_secret_like_raw_telemetry_summary_is_rejected_before_storage() {
    let space = Space::open_in_memory().unwrap();
    let mut envelope = ci("secret-summary", "failure", 40);
    envelope.summary = "raw telemetry includes bearer token".into();

    assert!(space.accept_sdlc_signal(envelope, principal()).is_err());
    assert!(space
        .get_sdlc_receipt(&source(), "secret-summary")
        .unwrap()
        .is_none());
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
fn test_pre_sdlc_database_migrates_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("space.sqlite");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE tuples (
                id TEXT PRIMARY KEY,
                category TEXT NOT NULL,
                scope TEXT NOT NULL,
                identity TEXT NOT NULL,
                instance TEXT NOT NULL,
                lifecycle TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT,
                strength REAL
            );",
        )
        .unwrap();
    }

    let space = Space::open(&path).unwrap();
    let receipt = space
        .accept_sdlc_signal(ci("migrated", "failure", 50), principal())
        .unwrap();
    assert_eq!(
        space.get_sdlc_receipt(&source(), "migrated").unwrap(),
        Some(receipt)
    );
}

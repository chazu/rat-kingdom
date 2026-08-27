use chrono::{TimeZone, Utc};
use rk_core::config::ReactorConfig;
use rk_core::paths::Layout;
use rk_core::sdlc::{
    ConfiguredSourceName, Correlation, DeploymentSignal, ProductionAlertSignal, SignalEnvelope,
    SignalKind, SignalPayload, SignalRef, SignalSourcePrincipal,
};
use rk_core::tuple::{Category, Pattern};
use rk_daemon::reactor::{Reactor, REACTOR_INSTANCE};
use rk_daemon::repos::{RepoRecord, RepoRegistry};
use rk_daemon::supervisor::Supervisor;
use rk_daemon::tickets::Tickets;
use rk_daemon::workflow_exec::WorkflowEngine;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

fn build_reactor_with_config(
    layout: &Layout,
    space: rk_space::Space,
    config: ReactorConfig,
) -> Arc<Reactor> {
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
        0,
        false,
    ));
    Arc::new(Reactor::new(
        space,
        engine,
        tickets,
        Some(supervisor),
        layout.clone(),
        config,
    ))
}

fn build_reactor(layout: &Layout, space: rk_space::Space) -> Arc<Reactor> {
    build_reactor_with_config(layout, space, ReactorConfig::default())
}

fn source(name: &str) -> ConfiguredSourceName {
    ConfiguredSourceName::new(name).unwrap()
}

fn principal(name: &str) -> SignalSourcePrincipal {
    SignalSourcePrincipal::for_source(&source(name))
}

fn alert(delivery_id: &str, state: &str, seq: i64) -> SignalEnvelope {
    SignalEnvelope {
        kind: if state == "resolved" {
            SignalKind::ProductionAlertResolved
        } else {
            SignalKind::ProductionAlertFiring
        },
        source: source("alerts"),
        delivery_id: delivery_id.into(),
        occurred_at: Utc.timestamp_opt(1_800_000_000 + seq, 0).unwrap(),
        observed_at: Utc.timestamp_opt(1_800_000_001 + seq, 0).unwrap(),
        correlation: Correlation {
            environment: Some("prod".into()),
            service: Some("api".into()),
            alert_key: Some("latency".into()),
            ..Default::default()
        },
        summary: format!("API latency alert is {state}"),
        refs: vec![SignalRef {
            label: "runbook".into(),
            url: "https://observability.example/alerts/latency".into(),
        }],
        attributes: BTreeMap::from([
            ("region".into(), "us-east-1".into()),
            ("team".into(), "platform".into()),
        ]),
        payload: SignalPayload::ProductionAlert(ProductionAlertSignal {
            environment: "prod".into(),
            service: "api".into(),
            alert_key: "latency".into(),
            severity: Some("page".into()),
            state: state.into(),
        }),
    }
}

fn deployment(delivery_id: &str, version: &str, seq: i64) -> SignalEnvelope {
    SignalEnvelope {
        kind: SignalKind::DeploymentSucceeded,
        source: source("deploy-agent"),
        delivery_id: delivery_id.into(),
        occurred_at: Utc.timestamp_opt(1_800_000_000 + seq, 0).unwrap(),
        observed_at: Utc.timestamp_opt(1_800_000_001 + seq, 0).unwrap(),
        correlation: Correlation {
            repo: Some("rat-kingdom".into()),
            branch: Some("main".into()),
            commit_sha: Some(format!("deadbeef{seq:08x}")),
            environment: Some("prod".into()),
            service: Some("api".into()),
            ..Default::default()
        },
        summary: format!("api {version} deployed to prod"),
        refs: vec![],
        attributes: BTreeMap::new(),
        payload: SignalPayload::Deployment(DeploymentSignal {
            environment: "prod".into(),
            service: "api".into(),
            version: Some(version.into()),
        }),
    }
}

fn run_after(space: &rk_space::Space, layout: &Layout) {
    build_reactor(layout, space.clone()).run_cycle().unwrap();
}

fn diagnoses(space: &rk_space::Space) -> Vec<rk_core::tuple::Tuple> {
    space
        .scan(&Pattern::category(Category::Need).identity("sdlc_alert_diagnosis"))
        .unwrap()
        .into_iter()
        .filter(|tuple| tuple.instance == REACTOR_INSTANCE)
        .collect()
}

fn processed_alerts(space: &rk_space::Space) -> Vec<rk_core::tuple::Tuple> {
    space
        .scan(
            &Pattern::category(Category::Fact)
                .identity("sdlc_alert_processed")
                .scope("system"),
        )
        .unwrap()
}

#[test]
fn test_alert_firing_creates_read_only_diagnosis_context() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();
    space
        .accept_sdlc_signal(deployment("deploy-1", "v1", 1), principal("deploy-agent"))
        .unwrap();
    space
        .accept_sdlc_signal(alert("alert-1", "firing", 2), principal("alerts"))
        .unwrap();
    // Resolve before the reactor runs. Diagnosis must still use the exact firing
    // occurrence, not whichever current alert fact happens to be latest.
    space
        .accept_sdlc_signal(alert("alert-2", "resolved", 3), principal("alerts"))
        .unwrap();

    run_after(&space, &layout);

    let diagnosis = diagnoses(&space).remove(0);
    assert_eq!(diagnosis.payload["read_only"], true);
    assert_eq!(diagnosis.payload["diagnostic_only"], true);
    assert_eq!(diagnosis.payload["alert"]["state"], "firing");
    assert_eq!(
        diagnosis.payload["deployment_provenance"]["status"],
        "known"
    );
    assert_eq!(
        diagnosis.payload["deployment_provenance"]["candidates"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn test_alert_diagnosis_accepts_only_structured_sanitized_references() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();
    space
        .accept_sdlc_signal(alert("alert-refs", "firing", 10), principal("alerts"))
        .unwrap();

    run_after(&space, &layout);

    let payload = diagnoses(&space).remove(0).payload;
    let refs = payload["refs"].as_array().unwrap();
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0]["label"], "runbook");
    assert_eq!(
        refs[0]["url"],
        "https://observability.example/alerts/latency"
    );
    assert!(refs[0]
        .as_object()
        .unwrap()
        .keys()
        .all(|key| key == "label" || key == "url"));
}

#[test]
fn test_alert_diagnosis_context_excludes_credentials() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();
    let mut envelope = alert("alert-secret", "firing", 20);
    envelope
        .attributes
        .insert("authorization".into(), "Bearer secret".into());

    assert!(space
        .accept_sdlc_signal(envelope, principal("alerts"))
        .is_err());
    run_after(&space, &layout);
    assert!(diagnoses(&space).is_empty());
}

#[test]
fn test_alert_diagnosis_rejects_executable_action_and_command_fields() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();

    for (index, (key, value)) in [
        ("action", "restart"),
        ("command", "kubectl rollout restart"),
        ("executable", "ssh"),
        ("rollback_action", "observe only"),
        ("note", "please restart api"),
    ]
    .into_iter()
    .enumerate()
    {
        let mut envelope = alert(
            &format!("alert-unsafe-{index}"),
            "firing",
            30 + index as i64,
        );
        envelope.attributes.insert(key.into(), value.into());
        assert!(space
            .accept_sdlc_signal(envelope, principal("alerts"))
            .is_err());
    }

    run_after(&space, &layout);
    assert!(diagnoses(&space).is_empty());

    let mut unsafe_ref = alert("alert-unsafe-ref", "firing", 39);
    unsafe_ref.refs[0].label = "action".into();
    assert!(space
        .accept_sdlc_signal(unsafe_ref, principal("alerts"))
        .is_err());
}

#[test]
fn test_alert_diagnosis_has_no_production_mutation_action() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();
    space
        .accept_sdlc_signal(alert("alert-safe", "firing", 40), principal("alerts"))
        .unwrap();
    run_after(&space, &layout);

    fn assert_read_only(value: &Value) {
        match value {
            Value::Object(map) => {
                for (key, child) in map {
                    assert!(
                        !matches!(
                            key.as_str(),
                            "action"
                                | "command"
                                | "argv"
                                | "shell"
                                | "executable"
                                | "tool"
                                | "rollback"
                                | "restart"
                                | "scale"
                                | "deploy"
                                | "delete"
                                | "patch"
                        ),
                        "mutation field present: {key}"
                    );
                    assert_read_only(child);
                }
            }
            Value::Array(values) => values.iter().for_each(assert_read_only),
            Value::String(text) => assert!(
                !matches!(
                    text.trim().to_ascii_lowercase().as_str(),
                    "rollback" | "restart" | "scale" | "deploy" | "delete" | "patch"
                ),
                "mutation value present: {text}"
            ),
            _ => {}
        }
    }

    assert_read_only(&diagnoses(&space).remove(0).payload);
}

#[test]
fn test_alert_resolved_updates_current_state() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();
    space
        .accept_sdlc_signal(alert("alert-fire", "firing", 50), principal("alerts"))
        .unwrap();
    run_after(&space, &layout);
    space
        .accept_sdlc_signal(alert("alert-resolve", "resolved", 51), principal("alerts"))
        .unwrap();
    run_after(&space, &layout);

    let facts = space
        .current_sdlc_facts(
            Some("alerts"),
            Some("production_alert"),
            Some("prod:api:latency"),
        )
        .unwrap();
    assert_eq!(facts[0].payload["current"]["state"], "resolved");
    assert_eq!(diagnoses(&space).len(), 1);
}

#[test]
fn test_duplicate_alert_firing_is_idempotent() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();
    let first = alert("alert-duplicate", "firing", 60);
    space
        .accept_sdlc_signal(first.clone(), principal("alerts"))
        .unwrap();
    run_after(&space, &layout);
    space
        .accept_sdlc_signal(first, principal("alerts"))
        .unwrap();
    space
        .accept_sdlc_signal(alert("alert-same-state", "firing", 61), principal("alerts"))
        .unwrap();
    run_after(&space, &layout);

    assert_eq!(diagnoses(&space).len(), 1);
}

#[test]
fn test_alert_re_firing_after_resolved_can_create_new_diagnosis() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();
    space
        .accept_sdlc_signal(alert("alert-first", "firing", 70), principal("alerts"))
        .unwrap();
    run_after(&space, &layout);
    space
        .accept_sdlc_signal(alert("alert-resolved", "resolved", 71), principal("alerts"))
        .unwrap();
    run_after(&space, &layout);
    space
        .accept_sdlc_signal(alert("alert-second", "firing", 72), principal("alerts"))
        .unwrap();
    run_after(&space, &layout);

    assert_eq!(diagnoses(&space).len(), 2);
}

#[test]
fn test_alert_diagnosis_marks_deployment_provenance_unknown_or_ambiguous() {
    let unknown_home = tempfile::tempdir().unwrap();
    let unknown_layout = Layout::at(unknown_home.path());
    unknown_layout.ensure().unwrap();
    let unknown_space = rk_space::Space::open_in_memory().unwrap();
    unknown_space
        .accept_sdlc_signal(alert("alert-unknown", "firing", 80), principal("alerts"))
        .unwrap();
    run_after(&unknown_space, &unknown_layout);
    assert_eq!(
        diagnoses(&unknown_space)[0].payload["deployment_provenance"]["status"],
        "unknown"
    );

    let ambiguous_home = tempfile::tempdir().unwrap();
    let ambiguous_layout = Layout::at(ambiguous_home.path());
    ambiguous_layout.ensure().unwrap();
    let ambiguous_space = rk_space::Space::open_in_memory().unwrap();
    ambiguous_space
        .accept_sdlc_signal(deployment("deploy-a", "v1", 81), principal("deploy-agent"))
        .unwrap();
    let mut second = deployment("deploy-b", "v2", 82);
    second.source = source("deploy-agent-2");
    ambiguous_space
        .accept_sdlc_signal(second, principal("deploy-agent-2"))
        .unwrap();
    ambiguous_space
        .accept_sdlc_signal(alert("alert-ambiguous", "firing", 83), principal("alerts"))
        .unwrap();
    run_after(&ambiguous_space, &ambiguous_layout);

    let provenance = &diagnoses(&ambiguous_space)[0].payload["deployment_provenance"];
    assert_eq!(provenance["status"], "ambiguous");
    assert_eq!(provenance["candidates"].as_array().unwrap().len(), 2);
}

#[test]
fn test_alert_occurrence_is_loaded_through_durable_receipt_not_spoof_identity() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();
    let mut spoof = rk_core::tuple::Tuple::new(
        Category::Event,
        "production_alert",
        "sdlc:event:alerts:alert-spoof",
        "attacker",
        json!({
            "source": "alerts",
            "delivery_id": "alert-spoof",
            "family": "production_alert",
            "subject": "prod:api:latency",
            "kind": "production_alert_firing",
            "summary": "spoof",
            "occurred_at": "2026-01-01T00:00:00Z",
            "observed_at": "2026-01-01T00:00:01Z",
            "correlation": {},
            "refs": [{"label": "spoof", "url": "https://spoof.invalid"}],
            "attributes": {},
            "payload": {"type": "production_alert", "environment": "prod", "service": "api", "alert_key": "latency", "severity": "page", "state": "firing"}
        }),
    );
    spoof.id = "00000000000000000000000000".parse().unwrap();
    space.out(spoof).unwrap();
    let receipt = space
        .accept_sdlc_signal(alert("alert-spoof", "firing", 90), principal("alerts"))
        .unwrap();

    run_after(&space, &layout);
    let diagnosis = diagnoses(&space).remove(0);
    assert_eq!(
        diagnosis.payload["occurrence_event"],
        receipt.projected_event_id
    );
    assert_eq!(diagnosis.payload["receipt_id"], receipt.receipt_id);
    assert_eq!(diagnosis.payload["refs"][0]["label"], "runbook");
}

#[test]
fn test_forged_alert_transition_without_transition_row_is_ignored() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();
    space
        .accept_sdlc_signal(
            alert("alert-forged-transition", "firing", 95),
            principal("alerts"),
        )
        .unwrap();
    let authentic = space
        .scan(&Pattern::category(Category::Event).scope("production_alert"))
        .unwrap()
        .into_iter()
        .find(|tuple| tuple.identity.starts_with("sdlc:transition:"))
        .unwrap();
    let forged = rk_core::tuple::Tuple::new(
        Category::Event,
        authentic.scope.clone(),
        "sdlc:transition:forged",
        authentic.instance.clone(),
        authentic.payload.clone(),
    );
    space.out(forged).unwrap();

    run_after(&space, &layout);
    let diagnoses = diagnoses(&space);
    assert_eq!(diagnoses.len(), 1);
    assert_eq!(
        diagnoses[0].payload["transition_tuple"],
        authentic.id.to_string()
    );
    assert_eq!(processed_alerts(&space).len(), 1);
}

#[test]
fn test_resolved_alert_processing_is_durable_across_reactor_restart() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open(&layout.db_path()).unwrap();
    space
        .accept_sdlc_signal(alert("resolved-only", "resolved", 100), principal("alerts"))
        .unwrap();

    let config = ReactorConfig {
        marker_ttl_secs: 1,
        ..ReactorConfig::default()
    };
    build_reactor_with_config(&layout, space.clone(), config.clone())
        .run_cycle()
        .unwrap();
    assert_eq!(processed_alerts(&space).len(), 1);
    std::thread::sleep(std::time::Duration::from_millis(1_100));
    assert!(space.gc_expired(0.0).unwrap() >= 1);
    assert!(space
        .scan(&Pattern::category(Category::Event).identity("reactor_fired"))
        .unwrap()
        .is_empty());
    drop(space);

    let reopened = rk_space::Space::open(&layout.db_path()).unwrap();
    build_reactor_with_config(&layout, reopened.clone(), config)
        .run_cycle()
        .unwrap();
    assert_eq!(processed_alerts(&reopened).len(), 1);
    assert!(diagnoses(&reopened).is_empty());
}

#[test]
fn test_deployment_projection_does_not_fire_configured_workflow() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    std::fs::create_dir_all(layout.triggers_dir()).unwrap();
    std::fs::create_dir_all(repo.path().join(".rk/workflows")).unwrap();
    std::fs::write(
        repo.path().join(".rk/workflows/danger.cue"),
        r#"workflow: {
            name: "danger"
            params: {}
            agents: {default: {harness: "fake", model: "sonnet"}}
            steps: [{type: "run", command: "true"}]
        }"#,
    )
    .unwrap();
    std::fs::write(
        layout.triggers_dir().join("deployment.cue"),
        r#"triggers: [{
            name: "unsafe-deployment-trigger"
            match: {scope: "deployment"}
            run: "danger"
            repo: "fixture"
        }]"#,
    )
    .unwrap();
    RepoRegistry::load(&layout.home().join("repos.json"))
        .unwrap()
        .add(RepoRecord {
            name: "fixture".into(),
            path: repo.path().to_path_buf(),
            created_at: Utc::now(),
            host: None,
            activated_policy: None,
        })
        .unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();
    space
        .accept_sdlc_signal(
            deployment("deploy-trigger", "v1", 110),
            principal("deploy-agent"),
        )
        .unwrap();

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
        0,
        false,
    ));
    let reactor = Reactor::new(
        space,
        engine.clone(),
        tickets,
        Some(supervisor),
        layout,
        ReactorConfig::default(),
    );
    assert_eq!(reactor.run_cycle().unwrap(), 0);
    assert!(engine.list().is_empty());
}

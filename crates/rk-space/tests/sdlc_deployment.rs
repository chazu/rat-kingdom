use chrono::{TimeZone, Utc};
use rk_core::id::RecordId;
use rk_core::sdlc::{
    ConfiguredSourceName, Correlation, DeploymentSignal, SignalEnvelope, SignalKind, SignalPayload,
    SignalRef, SignalSourcePrincipal,
};
use rk_core::tuple::{Category, Pattern};
use rk_space::Space;
use serde_json::json;
use std::collections::BTreeMap;

fn ts(n: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(1_800_000_000 + n, 0).unwrap()
}

fn source() -> ConfiguredSourceName {
    ConfiguredSourceName::new("deploy-agent").unwrap()
}

fn principal() -> SignalSourcePrincipal {
    SignalSourcePrincipal::for_source(&source())
}

fn deployment(
    delivery_id: &str,
    environment: &str,
    service: &str,
    version: &str,
    occurred: i64,
) -> SignalEnvelope {
    SignalEnvelope {
        kind: SignalKind::DeploymentSucceeded,
        source: source(),
        delivery_id: delivery_id.into(),
        occurred_at: ts(occurred),
        observed_at: ts(occurred + 1),
        correlation: Correlation {
            repo: Some("rat-kingdom".into()),
            branch: Some("main".into()),
            commit_sha: Some(format!("deadbeef{occurred:08x}")),
            environment: Some(environment.into()),
            service: Some(service.into()),
            ..Default::default()
        },
        summary: format!("deployed {service} {version} to {environment}"),
        refs: vec![SignalRef {
            label: "deployment".into(),
            url: format!("https://deployments.example/{environment}/{service}/{version}"),
        }],
        attributes: BTreeMap::from([("artifact".into(), format!("{service}:{version}"))]),
        payload: SignalPayload::Deployment(DeploymentSignal {
            environment: environment.into(),
            service: service.into(),
            version: Some(version.into()),
        }),
    }
}

fn current_fact(space: &Space, environment: &str, service: &str) -> rk_core::tuple::Tuple {
    let subject = format!("{environment}:{service}");
    let mut facts = space
        .current_sdlc_facts(Some("deploy-agent"), Some("deployment"), Some(&subject))
        .unwrap();
    assert_eq!(facts.len(), 1);
    facts.remove(0)
}

#[test]
fn test_deployment_succeeded_projects_current_provenance_fact() {
    let space = Space::open_in_memory().unwrap();
    let envelope = deployment("deploy-1", "prod", "api", "v1", 10);
    let receipt = space
        .accept_sdlc_signal(envelope.clone(), principal())
        .unwrap();

    let fact = current_fact(&space, "prod", "api");
    assert_eq!(fact.payload["source"], "deploy-agent");
    assert_eq!(fact.payload["family"], "deployment");
    assert_eq!(fact.payload["subject"], "prod:api");
    assert_eq!(fact.payload["receipt_id"], receipt.receipt_id);
    assert_eq!(
        fact.payload["occurred_at"],
        envelope.occurred_at.to_rfc3339()
    );
    assert_eq!(
        fact.payload["observed_at"],
        envelope.observed_at.to_rfc3339()
    );
    assert_eq!(fact.payload["current"]["environment"], "prod");
    assert_eq!(fact.payload["current"]["service"], "api");
    assert_eq!(fact.payload["current"]["version"], "v1");
    assert_eq!(
        fact.payload["current"]["commit_sha"],
        envelope.correlation.commit_sha.unwrap()
    );
}

#[test]
fn test_newer_deployment_replaces_current_fact_for_same_service_environment() {
    let space = Space::open_in_memory().unwrap();
    space
        .accept_sdlc_signal(
            deployment("deploy-new", "prod", "api", "v2", 20),
            principal(),
        )
        .unwrap();
    let delayed_old = space
        .accept_sdlc_signal(
            deployment("deploy-old", "prod", "api", "v1", 10),
            principal(),
        )
        .unwrap();

    let fact = current_fact(&space, "prod", "api");
    assert_eq!(fact.payload["current"]["version"], "v2");
    assert_eq!(fact.payload["last_delivery_id"], "deploy-new");
    assert!(!delayed_old.transition_emitted);
}

#[test]
fn test_deployment_for_different_environment_keeps_separate_fact() {
    let space = Space::open_in_memory().unwrap();
    space
        .accept_sdlc_signal(
            deployment("deploy-prod", "prod", "api", "v2", 20),
            principal(),
        )
        .unwrap();
    space
        .accept_sdlc_signal(
            deployment("deploy-stage", "stage", "api", "v3", 30),
            principal(),
        )
        .unwrap();

    assert_eq!(
        current_fact(&space, "prod", "api").payload["current"]["version"],
        "v2"
    );
    assert_eq!(
        current_fact(&space, "stage", "api").payload["current"]["version"],
        "v3"
    );
}

#[test]
fn test_deployment_fact_contains_receipt_and_sanitized_refs() {
    let space = Space::open_in_memory().unwrap();
    let envelope = deployment("deploy-refs", "prod", "api", "v4", 40);
    let receipt = space
        .accept_sdlc_signal(envelope.clone(), principal())
        .unwrap();

    let fact = current_fact(&space, "prod", "api");
    assert_eq!(fact.payload["receipt_id"], receipt.receipt_id);
    assert_eq!(
        fact.payload["refs"],
        serde_json::to_value(envelope.refs).unwrap()
    );
    assert_eq!(fact.payload["current"]["repo"], "rat-kingdom");
    assert_eq!(fact.payload["current"]["branch"], "main");
}

#[test]
fn test_deployment_fact_rejects_credential_attributes() {
    let space = Space::open_in_memory().unwrap();
    let mut envelope = deployment("deploy-secret", "prod", "api", "v5", 50);
    envelope
        .attributes
        .insert("authorization".into(), "Bearer redacted".into());

    assert!(space.accept_sdlc_signal(envelope, principal()).is_err());
    assert!(space
        .get_sdlc_receipt(&source(), "deploy-secret")
        .unwrap()
        .is_none());
    assert!(space
        .current_sdlc_facts(Some("deploy-agent"), Some("deployment"), None)
        .unwrap()
        .is_empty());
}

#[test]
fn test_deployment_projection_does_not_enqueue_mutation() {
    let space = Space::open_in_memory().unwrap();
    space
        .accept_sdlc_signal(
            deployment("deploy-safe", "prod", "api", "v6", 60),
            principal(),
        )
        .unwrap();

    assert!(space
        .scan(&Pattern::category(Category::Need))
        .unwrap()
        .is_empty());
}

#[test]
fn test_legacy_deployment_ordering_is_backfilled_before_stale_ingest() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("space.sqlite");
    let fact_id = RecordId::new().to_string();
    let occurred_at = ts(20).to_rfc3339();
    let observed_at = ts(21).to_rfc3339();
    let payload = json!({
        "source": "deploy-agent",
        "family": "deployment",
        "subject": "prod:api",
        "semantic_state_digest": "legacy-v2",
        "first_delivery_id": "legacy-v2",
        "last_delivery_id": "legacy-v2",
        "first_seen_at": observed_at,
        "last_seen_at": observed_at,
        "occurred_at": occurred_at,
        "observed_at": observed_at,
        "current": {
            "environment": "prod",
            "service": "api",
            "version": "v2",
            "commit_sha": "deadbeef00000020",
            "repo": "rat-kingdom",
            "branch": "main"
        }
    });
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
            );
            CREATE TABLE sdlc_current_state (
                source TEXT NOT NULL,
                scope TEXT NOT NULL,
                subject TEXT NOT NULL,
                semantic_state_digest TEXT NOT NULL,
                first_delivery_id TEXT NOT NULL,
                last_delivery_id TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                fact_tuple_id TEXT NOT NULL UNIQUE,
                PRIMARY KEY (source, scope, subject)
            );",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tuples
             (id, category, scope, identity, instance, lifecycle, payload, created_at, expires_at, strength)
             VALUES (?1, 'fact', 'deployment', 'sdlc:current:deploy-agent:prod:api',
                     'source:deploy-agent', 'furniture', ?2, ?3, NULL, NULL)",
            rusqlite::params![fact_id, payload.to_string(), observed_at],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sdlc_current_state
             (source, scope, subject, semantic_state_digest, first_delivery_id, last_delivery_id,
              first_seen_at, last_seen_at, fact_tuple_id)
             VALUES ('deploy-agent', 'deployment', 'prod:api', 'legacy-v2', 'legacy-v2',
                     'legacy-v2', ?1, ?1, ?2)",
            rusqlite::params![observed_at, fact_id],
        )
        .unwrap();
    }

    let space = Space::open(&path).unwrap();
    let receipt = space
        .accept_sdlc_signal(
            deployment("delayed-v1", "prod", "api", "v1", 10),
            principal(),
        )
        .unwrap();
    assert!(!receipt.transition_emitted);
    assert_eq!(
        current_fact(&space, "prod", "api").payload["current"]["version"],
        "v2"
    );
}

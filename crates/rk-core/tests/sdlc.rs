use std::collections::BTreeMap;

use chrono::{DateTime, TimeZone, Utc};
use rk_core::sdlc::{
    CiSignal, ConfiguredSourceName, Correlation, DeploymentSignal, OccurrenceId,
    ProductionAlertSignal, SemanticStateDigest, SignalEnvelope, SignalKind, SignalLimits,
    SignalReceipt, SignalRef, SignalSourcePrincipal, SourceToken,
};

fn ts(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0).single().unwrap()
}

fn base_correlation() -> Correlation {
    Correlation {
        repo: Some("rat-kingdom".into()),
        branch: Some("main".into()),
        workflow: Some("ci".into()),
        job: Some("test".into()),
        commit_sha: Some("abc123".into()),
        environment: None,
        service: None,
        alert_key: None,
    }
}

fn base_envelope(kind: SignalKind) -> SignalEnvelope {
    SignalEnvelope {
        kind,
        source: ConfiguredSourceName::new("local-ci").unwrap(),
        delivery_id: "delivery-1".into(),
        occurred_at: ts(10),
        observed_at: ts(11),
        correlation: base_correlation(),
        summary: "CI failed".into(),
        refs: vec![SignalRef {
            label: "build".into(),
            url: "https://example.invalid/build/1".into(),
        }],
        attributes: BTreeMap::from([("status".into(), "failed".into())]),
        payload: CiSignal {
            status: "failed".into(),
            conclusion: Some("test failure".into()),
        }
        .into(),
    }
}

#[test]
fn test_signal_envelope_round_trips_known_kinds() {
    let kinds = [
        SignalKind::CiFailed,
        SignalKind::CiRecovered,
        SignalKind::DeploymentSucceeded,
        SignalKind::ProductionAlertFiring,
        SignalKind::ProductionAlertResolved,
    ];

    for kind in kinds {
        let mut envelope = base_envelope(kind.clone());
        match kind {
            SignalKind::DeploymentSucceeded => {
                envelope.correlation = Correlation {
                    environment: Some("prod".into()),
                    service: Some("api".into()),
                    repo: Some("rat-kingdom".into()),
                    ..Correlation::default()
                };
                envelope.payload = DeploymentSignal {
                    environment: "prod".into(),
                    service: "api".into(),
                    version: Some("v1".into()),
                }
                .into();
            }
            SignalKind::ProductionAlertFiring | SignalKind::ProductionAlertResolved => {
                envelope.correlation = Correlation {
                    environment: Some("prod".into()),
                    service: Some("api".into()),
                    alert_key: Some("latency".into()),
                    ..Correlation::default()
                };
                envelope.payload = ProductionAlertSignal {
                    environment: "prod".into(),
                    service: "api".into(),
                    alert_key: "latency".into(),
                    severity: Some("page".into()),
                    state: "firing".into(),
                }
                .into();
            }
            _ => {}
        }

        let json = serde_json::to_string(&envelope).unwrap();
        assert_eq!(
            serde_json::from_str::<SignalEnvelope>(&json).unwrap(),
            envelope
        );
        envelope.validate(&SignalLimits::default()).unwrap();
    }
}

#[test]
fn test_correlation_rejects_empty_identity_for_transition_signals() {
    let mut envelope = base_envelope(SignalKind::CiFailed);
    envelope.correlation.job = Some("   ".into());

    assert!(envelope.validate(&SignalLimits::default()).is_err());
}

#[test]
fn test_signal_limits_reject_raw_telemetry_shape() {
    let mut envelope = base_envelope(SignalKind::CiFailed);
    envelope
        .attributes
        .insert("Authorization".into(), "Bearer secret".into());
    assert!(envelope.validate(&SignalLimits::default()).is_err());

    let mut envelope = base_envelope(SignalKind::CiFailed);
    envelope.refs.push(SignalRef {
        label: "raw_headers".into(),
        url: "https://example.invalid/headers".into(),
    });
    assert!(envelope.validate(&SignalLimits::default()).is_err());
}

#[test]
fn test_source_principal_is_configured_source_not_inline_text() {
    let source = ConfiguredSourceName::new("local-ci").unwrap();
    assert_eq!(
        SignalSourcePrincipal::for_source(&source).as_str(),
        "source:local-ci"
    );
    assert!(SignalSourcePrincipal::from_inline("source:local-ci").is_err());
}

#[test]
fn test_source_token_derives_source_principal_name() {
    let source = ConfiguredSourceName::new("local-ci").unwrap();
    let token = SourceToken::derive(&source, b"local secret");

    assert_eq!(token.source_name(), &source);
    assert_eq!(
        token.verify(&source, b"local secret").unwrap().as_str(),
        "source:local-ci"
    );
    assert!(token.verify(&source, b"wrong secret").is_err());
}

#[test]
fn test_occurrence_identity_is_source_and_delivery_id() {
    let source = ConfiguredSourceName::new("local-ci").unwrap();
    assert_eq!(
        OccurrenceId::new(source.clone(), "delivery-1")
            .unwrap()
            .to_string(),
        "local-ci:delivery-1"
    );
    assert!(OccurrenceId::new(source, "  ").is_err());
}

#[test]
fn test_semantic_state_digest_ignores_attribute_order() {
    let mut left = base_envelope(SignalKind::CiFailed);
    left.attributes = BTreeMap::from([("b".into(), "2".into()), ("a".into(), "1".into())]);

    let mut right = base_envelope(SignalKind::CiFailed);
    right.attributes = BTreeMap::from([("a".into(), "1".into()), ("b".into(), "2".into())]);

    assert_eq!(
        SemanticStateDigest::for_envelope(&left).unwrap(),
        SemanticStateDigest::for_envelope(&right).unwrap()
    );
}

#[test]
fn test_semantic_state_digest_changes_when_state_identity_changes() {
    let left = base_envelope(SignalKind::CiFailed);
    let mut right = base_envelope(SignalKind::CiFailed);
    right.correlation.commit_sha = Some("def456".into());

    assert_ne!(
        SemanticStateDigest::for_envelope(&left).unwrap(),
        SemanticStateDigest::for_envelope(&right).unwrap()
    );
}

#[test]
fn test_signal_receipt_contains_digest_principal_delivery_and_tuple_ids() {
    let envelope = base_envelope(SignalKind::CiFailed);
    let digest = SemanticStateDigest::for_envelope(&envelope).unwrap();
    let receipt = SignalReceipt::accepted(
        "receipt-1".into(),
        SignalSourcePrincipal::for_source(&envelope.source),
        envelope.delivery_id.clone(),
        ts(12),
        digest.clone(),
        "event-1".into(),
        vec!["fact-1".into()],
        true,
    );

    assert_eq!(receipt.source.as_str(), "source:local-ci");
    assert_eq!(receipt.delivery_id, "delivery-1");
    assert_eq!(receipt.semantic_state_digest, digest);
    assert_eq!(receipt.projected_event_id, "event-1");
    assert_eq!(receipt.projected_fact_ids, vec!["fact-1"]);
    assert!(receipt.transition_emitted);
}

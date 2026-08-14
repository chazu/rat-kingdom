use std::collections::BTreeMap;

use chrono::{DateTime, TimeZone, Utc};
use rk_core::sdlc::{
    CiSignal, ConfiguredSourceName, Correlation, DeploymentSignal, OccurrenceId,
    ProductionAlertSignal, SemanticStateDigest, SignalEnvelope, SignalKind, SignalLimits,
    SignalReceipt, SignalRef, SignalSourcePrincipal, SourceToken,
};

fn assert_invalid(envelope: &SignalEnvelope) {
    assert!(
        envelope.validate(&SignalLimits::default()).is_err(),
        "envelope should be rejected: {envelope:?}"
    );
}

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
            SignalKind::CiRecovered => {
                envelope.payload = CiSignal {
                    status: "success".into(),
                    conclusion: Some("success".into()),
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
fn test_ci_recovered_rejects_failure_payload_as_inconsistent() {
    let envelope = base_envelope(SignalKind::CiRecovered);

    let error = envelope.validate(&SignalLimits::default()).unwrap_err();
    assert!(error.to_string().contains("kind") && error.to_string().contains("payload"));
}

#[test]
fn test_url_refs_reject_encoded_sensitive_query_keys_and_userinfo() {
    for url in [
        "https://example.invalid/build?api%5Fkey=redacted",
        "https://example.invalid/build?Api%5fKey=redacted",
        "https://example.invalid/build?token%3Dredacted",
        "https://user:redacted@example.invalid/build",
        "https://USER:ReDaCtEd@example.invalid/build",
    ] {
        let mut envelope = base_envelope(SignalKind::CiFailed);
        envelope.refs = vec![SignalRef {
            label: "build".into(),
            url: url.into(),
        }];

        assert_invalid(&envelope);
    }
}

#[test]
fn test_url_refs_allow_benign_percent_encoding_without_raw_telemetry() {
    let mut envelope = base_envelope(SignalKind::CiFailed);
    envelope.refs = vec![SignalRef {
        label: "build".into(),
        url: "https://example.invalid/build?job%5Fid=unit%2Dtests&branch=main".into(),
    }];

    envelope.validate(&SignalLimits::default()).unwrap();
}

#[test]
fn test_url_refs_reject_repeatedly_encoded_sensitive_query_keys() {
    for url in [
        "https://example.invalid/build?%2574%256f%256b%2565%256e=redacted",
        "https://example.invalid/build?%25252561%25252570%25252569%2525255f%2525256b%25252565%25252579=redacted",
    ] {
        let mut envelope = base_envelope(SignalKind::CiFailed);
        envelope.refs = vec![SignalRef {
            label: "build".into(),
            url: url.into(),
        }];

        assert_invalid(&envelope);
    }
}

fn repeat_percent_encode(mut value: String, passes: usize) -> String {
    for _ in 0..passes {
        value = value
            .bytes()
            .map(|byte| format!("%{byte:02x}"))
            .collect::<Vec<_>>()
            .join("");
    }
    value
}

#[test]
fn test_url_refs_reject_sensitive_query_keys_encoded_at_decode_limit() {
    let encoded_token = repeat_percent_encode("token".into(), 8);
    let mut envelope = base_envelope(SignalKind::CiFailed);
    envelope.refs = vec![SignalRef {
        label: "build".into(),
        url: format!("https://example.invalid/build?{encoded_token}=super-secret-value"),
    }];

    assert_invalid(&envelope);
}

#[test]
fn test_url_refs_reject_sensitive_query_keys_encoded_beyond_decode_limit() {
    let encoded_token = repeat_percent_encode("token".into(), 9);
    let mut envelope = base_envelope(SignalKind::CiFailed);
    envelope.refs = vec![SignalRef {
        label: "build".into(),
        url: format!("https://example.invalid/build?{encoded_token}=super-secret-value"),
    }];

    let err = envelope.validate(&SignalLimits::default()).unwrap_err();
    let rendered = err.to_string();
    assert!(rendered.contains("ref"));
    assert!(!rendered.contains(&encoded_token));
    assert!(!rendered.contains("super-secret-value"));
    assert!(rendered.contains("<redacted>"));
}

#[test]
fn test_url_refs_reject_overly_long_encoded_input_instead_of_partially_decoding() {
    let encoded_token = repeat_percent_encode("token".into(), 8);
    let long_encoded = format!("{}{}", "%41".repeat(4096), encoded_token);
    let mut envelope = base_envelope(SignalKind::CiFailed);
    envelope.refs = vec![SignalRef {
        label: "build".into(),
        url: format!("https://example.invalid/build?{long_encoded}=super-secret-value"),
    }];

    let err = envelope.validate(&SignalLimits::default()).unwrap_err();
    let rendered = err.to_string();
    assert!(rendered.contains("ref"));
    assert!(!rendered.contains(&long_encoded));
    assert!(!rendered.contains("super-secret-value"));
    assert!(rendered.contains("<redacted>"));
}

#[test]
fn test_url_ref_errors_do_not_echo_secret_bearing_urls_or_values() {
    let secret_url = "https://example.invalid/build?token=super-secret-value";
    let mut envelope = base_envelope(SignalKind::CiFailed);
    envelope.refs = vec![SignalRef {
        label: "build".into(),
        url: secret_url.into(),
    }];

    let err = envelope.validate(&SignalLimits::default()).unwrap_err();
    let rendered = err.to_string();
    assert!(!rendered.contains(secret_url));
    assert!(!rendered.contains("super-secret-value"));
    assert!(rendered.contains("ref"));
}

#[test]
fn test_url_refs_reject_scheme_relative_and_username_only_userinfo() {
    for url in [
        "//user:secret@example.invalid/build",
        "//user@example.invalid/build",
        "https://user@example.invalid/build",
    ] {
        let mut envelope = base_envelope(SignalKind::CiFailed);
        envelope.refs = vec![SignalRef {
            label: "build".into(),
            url: url.into(),
        }];

        assert_invalid(&envelope);
    }
}

#[test]
fn test_url_refs_allow_sensitive_substrings_outside_parameter_name_boundaries() {
    for url in [
        "https://example.invalid/build?deployment_tokenizer=ok",
        "https://example.invalid/build?myapi_keychain=ok",
        "https://example.invalid/build?note=token-api_key-text",
        "https://example.invalid/api/token/status?job=ci",
    ] {
        let mut envelope = base_envelope(SignalKind::CiFailed);
        envelope.refs = vec![SignalRef {
            label: "build".into(),
            url: url.into(),
        }];

        envelope.validate(&SignalLimits::default()).unwrap();
    }
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

#[test]
fn test_serde_rejects_forged_configured_source_and_principal() {
    let mut envelope_json = serde_json::to_value(base_envelope(SignalKind::CiFailed)).unwrap();
    envelope_json["source"] = serde_json::json!("source:forged");

    assert!(serde_json::from_value::<SignalEnvelope>(envelope_json).is_err());

    let receipt_json = serde_json::json!({
        "receipt_id": "receipt-1",
        "source": "source:forged",
        "delivery_id": "delivery-1",
        "accepted_at": ts(12),
        "semantic_state_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "projected_event_id": "event-1",
        "projected_fact_ids": ["fact-1"],
        "transition_emitted": true
    });

    assert!(serde_json::from_value::<SignalReceipt>(receipt_json).is_err());
}

#[test]
fn test_signal_kind_must_match_payload_family() {
    let mut envelope = base_envelope(SignalKind::DeploymentSucceeded);
    envelope.correlation = Correlation {
        environment: Some("prod".into()),
        service: Some("api".into()),
        ..Correlation::default()
    };

    assert_invalid(&envelope);
}

#[test]
fn test_state_bearing_payload_fields_are_required_and_consistent() {
    let mut ci = base_envelope(SignalKind::CiFailed);
    if let rk_core::sdlc::SignalPayload::Ci(payload) = &mut ci.payload {
        payload.status = "   ".into();
    }
    assert_invalid(&ci);

    let mut alert = base_envelope(SignalKind::ProductionAlertFiring);
    alert.correlation = Correlation {
        environment: Some("prod".into()),
        service: Some("api".into()),
        alert_key: Some("latency".into()),
        ..Correlation::default()
    };
    alert.payload = ProductionAlertSignal {
        environment: "prod".into(),
        service: "worker".into(),
        alert_key: "latency".into(),
        severity: Some("page".into()),
        state: "firing".into(),
    }
    .into();
    assert_invalid(&alert);
}

#[test]
fn test_production_alert_diagnosis_rejects_camel_case_mutation_fields() {
    let mut alert = base_envelope(SignalKind::ProductionAlertFiring);
    alert.correlation = Correlation {
        environment: Some("prod".into()),
        service: Some("api".into()),
        alert_key: Some("latency".into()),
        ..Correlation::default()
    };
    alert.payload = ProductionAlertSignal {
        environment: "prod".into(),
        service: "api".into(),
        alert_key: "latency".into(),
        severity: Some("page".into()),
        state: "firing".into(),
    }
    .into();

    for key in ["rollbackAction", "recommendedAction", "restartCommand"] {
        let mut unsafe_alert = alert.clone();
        unsafe_alert
            .attributes
            .insert(key.into(), "observe only".into());
        assert_invalid(&unsafe_alert);
    }

    let mut unsafe_value = alert;
    unsafe_value
        .attributes
        .insert("note".into(), "terraformApply".into());
    assert_invalid(&unsafe_value);
}

#[test]
fn test_secret_values_and_token_bearing_urls_are_rejected() {
    let mut attribute_value = base_envelope(SignalKind::CiFailed);
    attribute_value
        .attributes
        .insert("note".into(), "Bearer abc123".into());
    assert_invalid(&attribute_value);

    let mut token_url = base_envelope(SignalKind::CiFailed);
    token_url.refs = vec![SignalRef {
        label: "build".into(),
        url: "https://example.invalid/build?api_key=abc123".into(),
    }];
    assert_invalid(&token_url);

    let mut password_url = base_envelope(SignalKind::CiFailed);
    password_url.refs = vec![SignalRef {
        label: "build".into(),
        url: "https://user:password@example.invalid/build".into(),
    }];
    assert_invalid(&password_url);
}

#[test]
fn test_deployment_digest_ignores_optional_repo_metadata_only() {
    let mut left = base_envelope(SignalKind::DeploymentSucceeded);
    left.correlation = Correlation {
        repo: Some("rat-kingdom".into()),
        environment: Some("prod".into()),
        service: Some("api".into()),
        ..Correlation::default()
    };
    left.payload = DeploymentSignal {
        environment: "prod".into(),
        service: "api".into(),
        version: Some("v1".into()),
    }
    .into();

    let mut repo_changed = left.clone();
    repo_changed.correlation.repo = Some("renamed-repo".into());

    let mut state_changed = left.clone();
    state_changed.payload = DeploymentSignal {
        environment: "prod".into(),
        service: "api".into(),
        version: Some("v2".into()),
    }
    .into();

    assert_eq!(
        SemanticStateDigest::for_envelope(&left).unwrap(),
        SemanticStateDigest::for_envelope(&repo_changed).unwrap()
    );
    assert_ne!(
        SemanticStateDigest::for_envelope(&left).unwrap(),
        SemanticStateDigest::for_envelope(&state_changed).unwrap()
    );
}

use rk_core::factory::{
    outcome_events::{FactoryMetricPayload, FactoryOutcomeEvent, StructuredOutcomeInput},
    outcome_facts::{OutcomeFactBuilder, OutcomeFactSource},
    OutcomeEvidenceKind, OutcomeStatus,
};

fn base(
    source: OutcomeEvidenceKind,
    source_id: &str,
    payload: FactoryMetricPayload,
) -> StructuredOutcomeInput {
    StructuredOutcomeInput {
        repo: "repo-a".into(),
        source_family: source,
        source_id: source_id.into(),
        source_version: Some("v1".into()),
        archived: false,
        archive_reason: None,
        observed_at_ms: 10,
        task_class: None,
        workflow: None,
        harness: None,
        model: None,
        agent_id: None,
        workflow_instance_id: None,
        ticket_id: None,
        phase3_outcome_id: None,
        phase4_signal_id: None,
        recurrence_key: None,
        coalesce_key: None,
        payload,
        decoy_prose: "LOG says accepted, rework requested, CI failed, revert commit, urgent approval, duplicate bug".into(),
    }
}

#[test]
fn normalizes_run_dimensions_from_agent_record_and_instance() {
    let mut input = base(
        OutcomeEvidenceKind::AgentRecord,
        "agent-1",
        FactoryMetricPayload::Run { count: 1 },
    );
    input.workflow = Some("wf-build".into());
    input.harness = Some("cargo-test".into());
    input.model = Some("claude-api:fable".into());

    let facts = OutcomeFactBuilder::from_structured_inputs([input], []).build();

    assert_eq!(facts[0].group_key.workflow.as_deref(), Some("wf-build"));
    assert_eq!(facts[0].group_key.harness.as_deref(), Some("cargo-test"));
    assert_eq!(
        facts[0].group_key.model.as_deref(),
        Some("claude-api:fable")
    );
    assert_eq!(facts[0].status, OutcomeStatus::Unknown);
}

#[test]
fn task_class_requires_phase3_explicit_contract_ticket_or_outcome() {
    let mut explicit = base(
        OutcomeEvidenceKind::Phase3Contract,
        "contract-1",
        FactoryMetricPayload::TaskClass,
    );
    explicit.task_class = Some("bugfix".into());
    let prose_only = base(
        OutcomeEvidenceKind::AgentRecord,
        "agent-mentions-bugfix",
        FactoryMetricPayload::TaskClass,
    );

    let facts = OutcomeFactBuilder::from_structured_inputs([explicit, prose_only], []).build();

    let explicit_fact = facts
        .iter()
        .find(|fact| fact.source.source_id == "contract-1")
        .unwrap();
    let prose_fact = facts
        .iter()
        .find(|fact| fact.source.source_id == "agent-mentions-bugfix")
        .unwrap();

    assert_eq!(
        explicit_fact.group_key.task_class.as_deref(),
        Some("bugfix")
    );
    assert!(prose_fact.group_key.task_class.is_none());
    assert!(prose_fact
        .source
        .warnings
        .iter()
        .any(|w| w.contains("task_class_unobserved")));
}

#[test]
fn normalizes_accepted_only_from_phase3_verified_delivery_or_land() {
    let accepted = base(
        OutcomeEvidenceKind::Phase3VerifiedDelivery,
        "delivery-1",
        FactoryMetricPayload::Accepted {
            verified_delivery: true,
            landed: false,
        },
    );
    let green_ci = base(
        OutcomeEvidenceKind::Phase4CiSignal,
        "ci-green",
        FactoryMetricPayload::Ci {
            failed: false,
            recovered: false,
        },
    );

    let facts = OutcomeFactBuilder::from_structured_inputs([accepted, green_ci], []).build();

    assert!(facts.iter().any(|f| f.status == OutcomeStatus::Accepted));
    assert!(!facts
        .iter()
        .any(|f| f.status == OutcomeStatus::Accepted && f.source.source_id == "ci-green"));
}

#[test]
fn normalizes_reworked_only_from_structured_reviewer_transition() {
    let rework = base(
        OutcomeEvidenceKind::StructuredReviewerRework,
        "review-1",
        FactoryMetricPayload::Reworked { requested: true },
    );
    let comment = base(
        OutcomeEvidenceKind::AgentRecord,
        "comment-1",
        FactoryMetricPayload::Reworked { requested: true },
    );

    let facts = OutcomeFactBuilder::from_structured_inputs([rework, comment], []).build();

    assert!(facts
        .iter()
        .any(|f| f.status == OutcomeStatus::Reworked && f.source.source_id == "review-1"));
    assert!(!facts
        .iter()
        .any(|f| f.status == OutcomeStatus::Reworked && f.source.source_id == "comment-1"));
}

#[test]
fn normalizes_ci_failed_and_recovered_from_phase4_signals() {
    let failed = base(
        OutcomeEvidenceKind::Phase4CiSignal,
        "ci-failed",
        FactoryMetricPayload::Ci {
            failed: true,
            recovered: false,
        },
    );
    let recovered = base(
        OutcomeEvidenceKind::Phase4CiSignal,
        "ci-recovered",
        FactoryMetricPayload::Ci {
            failed: false,
            recovered: true,
        },
    );

    let facts = OutcomeFactBuilder::from_structured_inputs([failed, recovered], []).build();

    assert!(facts.iter().any(|f| f.status == OutcomeStatus::CiFailed));
    assert!(facts.iter().any(|f| f.status == OutcomeStatus::CiRecovered));
}

#[test]
fn normalizes_revert_only_from_structured_revert_handler() {
    let revert = base(
        OutcomeEvidenceKind::StructuredRevert,
        "revert-1",
        FactoryMetricPayload::Reverted { reverted: true },
    );
    let commit_msg = base(
        OutcomeEvidenceKind::AgentRecord,
        "commit-msg",
        FactoryMetricPayload::Reverted { reverted: true },
    );

    let facts = OutcomeFactBuilder::from_structured_inputs([revert, commit_msg], []).build();

    assert!(facts
        .iter()
        .any(|f| f.status == OutcomeStatus::Reverted && f.source.source_id == "revert-1"));
    assert!(!facts
        .iter()
        .any(|f| f.status == OutcomeStatus::Reverted && f.source.source_id == "commit-msg"));
}

#[test]
fn counts_human_intervention_only_from_gate_approval_decision_events() {
    let gate = base(
        OutcomeEvidenceKind::HumanGateDecision,
        "gate-1",
        FactoryMetricPayload::HumanIntervention { count: 1 },
    );
    let mention = base(
        OutcomeEvidenceKind::AgentRecord,
        "mention-1",
        FactoryMetricPayload::HumanIntervention { count: 1 },
    );

    let facts = OutcomeFactBuilder::from_structured_inputs([gate, mention], []).build();

    assert_eq!(facts.iter().map(|f| f.human_interventions).sum::<u32>(), 1);
}

#[test]
fn uses_only_explicit_recurrence_or_coalesce_key() {
    let mut recurrent = base(
        OutcomeEvidenceKind::RecurrenceKey,
        "rec-1",
        FactoryMetricPayload::Recurrence,
    );
    recurrent.recurrence_key = Some("same-defect".into());
    let similar_text = base(
        OutcomeEvidenceKind::AgentRecord,
        "similar-prose",
        FactoryMetricPayload::Recurrence,
    );

    let facts = OutcomeFactBuilder::from_structured_inputs([recurrent, similar_text], []).build();

    assert_eq!(facts.iter().map(|f| f.recurrence_count).sum::<u32>(), 1);
}

#[test]
fn missing_source_family_is_unobserved_with_availability_counts() {
    let input = base(
        OutcomeEvidenceKind::AgentRecord,
        "agent-1",
        FactoryMetricPayload::Run { count: 1 },
    );

    let facts = OutcomeFactBuilder::from_structured_inputs(
        [input],
        [OutcomeFactSource::unavailable(
            OutcomeEvidenceKind::Phase4CiSignal,
        )],
    )
    .build();

    assert!(facts.iter().any(|f| f.status == OutcomeStatus::Unobserved));
    let unobserved = facts
        .iter()
        .find(|f| f.source.kind == OutcomeEvidenceKind::Phase4CiSignal)
        .unwrap();
    assert!(!unobserved.availability.available);
    assert_eq!(unobserved.source_counts.event_count, 0);
}

#[test]
fn fact_ids_are_deterministic_across_input_order() {
    let a = base(
        OutcomeEvidenceKind::Phase4CiSignal,
        "a",
        FactoryMetricPayload::Ci {
            failed: true,
            recovered: false,
        },
    );
    let b = base(
        OutcomeEvidenceKind::Phase3VerifiedDelivery,
        "b",
        FactoryMetricPayload::Accepted {
            verified_delivery: true,
            landed: false,
        },
    );

    let forward: Vec<_> = OutcomeFactBuilder::from_structured_inputs([a.clone(), b.clone()], [])
        .build()
        .into_iter()
        .map(|f| f.fact_id)
        .collect();
    let reverse: Vec<_> = OutcomeFactBuilder::from_structured_inputs([b, a], [])
        .build()
        .into_iter()
        .map(|f| f.fact_id)
        .collect();

    assert_eq!(forward, reverse);
}

#[test]
fn archived_source_marks_fact_archived_with_source_family() {
    let mut archived = base(
        OutcomeEvidenceKind::AgentRecord,
        "archived-agent",
        FactoryMetricPayload::Run { count: 1 },
    );
    archived.archived = true;
    archived.archive_reason = Some("history".into());

    let facts = OutcomeFactBuilder::from_structured_inputs([archived], [])
        .include_archived(true)
        .build();

    assert!(facts[0].archived);
    assert_eq!(
        facts[0].archive_source_family,
        Some(OutcomeEvidenceKind::AgentRecord)
    );
    assert_eq!(facts[0].source_counts.archived_source_count, 1);
}

#[test]
fn event_ids_are_stable_for_canonical_structured_fields() {
    let event = FactoryOutcomeEvent::from(base(
        OutcomeEvidenceKind::AgentRecord,
        "agent-1",
        FactoryMetricPayload::Run { count: 1 },
    ));
    let same = FactoryOutcomeEvent::from(base(
        OutcomeEvidenceKind::AgentRecord,
        "agent-1",
        FactoryMetricPayload::Run { count: 1 },
    ));

    assert_eq!(event.event_id, same.event_id);
}

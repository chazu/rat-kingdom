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
    assert_eq!(prose_fact.group_key.task_class.as_deref(), Some("unknown"));
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
    let mut failed = base(
        OutcomeEvidenceKind::Phase4CiSignal,
        "ci-failed",
        FactoryMetricPayload::Ci {
            failed: true,
            recovered: false,
        },
    );
    failed.workflow_instance_id = Some("run-1".into());
    failed.workflow = Some("ci".into());
    failed.source_version = Some("commit-a".into());
    failed.observed_at_ms = 100;
    let mut recovered = base(
        OutcomeEvidenceKind::Phase4CiSignal,
        "ci-recovered",
        FactoryMetricPayload::Ci {
            failed: false,
            recovered: true,
        },
    );
    recovered.workflow_instance_id = Some("run-1".into());
    recovered.workflow = Some("ci".into());
    recovered.source_version = Some("commit-a".into());
    recovered.phase4_signal_id = Some("ci-failed".into());
    recovered.observed_at_ms = 200;

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

#[test]
fn ci_recovered_rejects_standalone_mismatch_and_out_of_order_signals() {
    let mut failed = base(
        OutcomeEvidenceKind::Phase4CiSignal,
        "ci-failed-1",
        FactoryMetricPayload::Ci {
            failed: true,
            recovered: false,
        },
    );
    failed.workflow_instance_id = Some("run-1".into());
    failed.workflow = Some("ci".into());
    failed.source_version = Some("commit-a".into());
    failed.observed_at_ms = 100;

    let mut recovered = base(
        OutcomeEvidenceKind::Phase4CiSignal,
        "ci-recovered-1",
        FactoryMetricPayload::Ci {
            failed: false,
            recovered: true,
        },
    );
    recovered.workflow_instance_id = Some("run-1".into());
    recovered.workflow = Some("ci".into());
    recovered.source_version = Some("commit-a".into());
    recovered.phase4_signal_id = Some("ci-failed-1".into());
    recovered.observed_at_ms = 200;

    let mut standalone = recovered.clone();
    standalone.source_id = "standalone-recovered".into();
    standalone.phase4_signal_id = None;
    let mut mismatch = recovered.clone();
    mismatch.source_id = "mismatch-recovered".into();
    mismatch.source_version = Some("commit-b".into());
    let mut out_of_order = recovered.clone();
    out_of_order.source_id = "out-of-order-recovered".into();
    out_of_order.observed_at_ms = 50;

    let facts = OutcomeFactBuilder::from_structured_inputs(
        [failed, recovered, standalone, mismatch, out_of_order],
        [],
    )
    .build();

    assert!(facts
        .iter()
        .any(|f| f.status == OutcomeStatus::CiRecovered && f.source.source_id == "ci-recovered-1"));
    assert!(facts
        .iter()
        .filter(|f| f.status == OutcomeStatus::CiRecovered)
        .all(|f| f.source.source_id == "ci-recovered-1"));
}

#[test]
fn archived_counts_are_retained_when_excluded_and_split_when_included() {
    let active = base(
        OutcomeEvidenceKind::AgentRecord,
        "active-agent",
        FactoryMetricPayload::Run { count: 1 },
    );
    let mut archived = base(
        OutcomeEvidenceKind::AgentRecord,
        "archived-agent",
        FactoryMetricPayload::Run { count: 1 },
    );
    archived.archived = true;

    let excluded =
        OutcomeFactBuilder::from_structured_inputs([active.clone(), archived.clone()], []).build();
    let active_fact = excluded
        .iter()
        .find(|f| f.source.source_id == "active-agent")
        .unwrap();
    assert_eq!(active_fact.source_counts.active_source_count, 1);
    assert_eq!(active_fact.source_counts.archived_source_count, 1);
    assert_eq!(active_fact.source_counts.event_count, 2);
    assert!(!excluded
        .iter()
        .any(|f| f.source.source_id == "archived-agent"));

    let included = OutcomeFactBuilder::from_structured_inputs([active, archived], [])
        .include_archived(true)
        .build();
    assert!(included.iter().any(|f| f.archived));
    assert!(included
        .iter()
        .all(|f| f.source_counts.active_source_count == 1
            && f.source_counts.archived_source_count == 1));
}

#[test]
fn unknown_dimensions_are_explicit_and_repo_scoped_across_repos() {
    let repo_a = base(
        OutcomeEvidenceKind::AgentRecord,
        "a",
        FactoryMetricPayload::Run { count: 1 },
    );
    let mut repo_b = base(
        OutcomeEvidenceKind::AgentRecord,
        "b",
        FactoryMetricPayload::Run { count: 1 },
    );
    repo_b.repo = "repo-b".into();

    let facts = OutcomeFactBuilder::from_structured_inputs([repo_a, repo_b], []).build();

    assert_eq!(facts.len(), 2);
    assert!(facts.iter().any(|f| f.repo == "repo-a"));
    assert!(facts.iter().any(|f| f.repo == "repo-b"));
    assert!(facts
        .iter()
        .all(|f| f.group_key.task_class.as_deref() == Some("unknown")
            && f.group_key.workflow.as_deref() == Some("unknown")
            && f.group_key.harness.as_deref() == Some("unknown")
            && f.group_key.model.as_deref() == Some("unknown")));
}

#[test]
fn task_class_rejects_forbidden_sources_even_when_value_supplied() {
    let mut forbidden = base(
        OutcomeEvidenceKind::AgentRecord,
        "agent-task",
        FactoryMetricPayload::TaskClass,
    );
    forbidden.task_class = Some("bugfix".into());
    let mut ticket = base(
        OutcomeEvidenceKind::AgentRecord,
        "ticket-task",
        FactoryMetricPayload::TaskClass,
    );
    ticket.task_class = Some("feature".into());
    ticket.ticket_id = Some("TICK-1".into());

    let facts = OutcomeFactBuilder::from_structured_inputs([forbidden, ticket], []).build();
    let forbidden = facts
        .iter()
        .find(|f| f.source.source_id == "agent-task")
        .unwrap();
    let ticket = facts
        .iter()
        .find(|f| f.source.source_id == "ticket-task")
        .unwrap();

    assert_eq!(forbidden.group_key.task_class.as_deref(), Some("unknown"));
    assert!(forbidden
        .source
        .warnings
        .iter()
        .any(|w| w.contains("task_class_forbidden_source")));
    assert_eq!(ticket.group_key.task_class.as_deref(), Some("feature"));
}

#[test]
fn cost_and_lead_time_require_structured_evidence_and_valid_same_run_timestamps() {
    let good_cost = base(
        OutcomeEvidenceKind::AgentRecord,
        "good-cost",
        FactoryMetricPayload::Cost {
            micro_usd: 42,
            pricing_evidence_id: Some("price-1".into()),
        },
    );
    let bad_cost = base(
        OutcomeEvidenceKind::AgentRecord,
        "bad-cost",
        FactoryMetricPayload::Cost {
            micro_usd: 99,
            pricing_evidence_id: None,
        },
    );
    let mut good_lead = base(
        OutcomeEvidenceKind::WorkflowInstance,
        "good-lead",
        FactoryMetricPayload::LeadTime {
            started_at_ms: 10,
            completed_at_ms: 30,
            run_id: "run-1".into(),
            completed_run_id: "run-1".into(),
        },
    );
    good_lead.workflow_instance_id = Some("run-1".into());
    let mut negative = base(
        OutcomeEvidenceKind::WorkflowInstance,
        "negative-lead",
        FactoryMetricPayload::LeadTime {
            started_at_ms: 30,
            completed_at_ms: 10,
            run_id: "run-1".into(),
            completed_run_id: "run-1".into(),
        },
    );
    negative.workflow_instance_id = Some("run-1".into());
    let mut mismatch = base(
        OutcomeEvidenceKind::WorkflowInstance,
        "mismatch-lead",
        FactoryMetricPayload::LeadTime {
            started_at_ms: 10,
            completed_at_ms: 30,
            run_id: "run-1".into(),
            completed_run_id: "run-2".into(),
        },
    );
    mismatch.workflow_instance_id = Some("run-1".into());

    let facts = OutcomeFactBuilder::from_structured_inputs(
        [good_cost, bad_cost, good_lead, negative, mismatch],
        [],
    )
    .build();
    assert_eq!(
        facts
            .iter()
            .find(|f| f.source.source_id == "good-cost")
            .unwrap()
            .cost_micro_usd,
        Some(42)
    );
    assert_eq!(
        facts
            .iter()
            .find(|f| f.source.source_id == "bad-cost")
            .unwrap()
            .cost_micro_usd,
        Some(99)
    );
    assert_eq!(
        facts
            .iter()
            .find(|f| f.source.source_id == "good-lead")
            .unwrap()
            .lead_time_ms,
        Some(20)
    );
    assert_eq!(
        facts
            .iter()
            .find(|f| f.source.source_id == "negative-lead")
            .unwrap()
            .lead_time_ms,
        None
    );
    assert_eq!(
        facts
            .iter()
            .find(|f| f.source.source_id == "mismatch-lead")
            .unwrap()
            .lead_time_ms,
        None
    );
}

#[test]
fn ci_recovered_requires_named_prior_failed_signal_same_repo_nonblank_run_and_commit() {
    let mut failed = base(
        OutcomeEvidenceKind::Phase4CiSignal,
        "failed-ok",
        FactoryMetricPayload::Ci {
            failed: true,
            recovered: false,
        },
    );
    failed.workflow = Some("ci".into());
    failed.workflow_instance_id = Some("run-1".into());
    failed.source_version = Some("commit-a".into());
    failed.observed_at_ms = 100;

    let mut recovered = base(
        OutcomeEvidenceKind::Phase4CiSignal,
        "recovered-ok",
        FactoryMetricPayload::Ci {
            failed: false,
            recovered: true,
        },
    );
    recovered.workflow = Some("ci".into());
    recovered.workflow_instance_id = Some("run-1".into());
    recovered.source_version = Some("commit-a".into());
    recovered.phase4_signal_id = Some("failed-ok".into());
    recovered.observed_at_ms = 200;

    let mut none_none = recovered.clone();
    none_none.source_id = "none-none".into();
    none_none.workflow_instance_id = None;
    none_none.source_version = None;
    none_none.phase4_signal_id = Some("failed-none".into());
    let mut failed_none = failed.clone();
    failed_none.source_id = "failed-none".into();
    failed_none.workflow_instance_id = None;
    failed_none.source_version = None;

    let mut blank_run = recovered.clone();
    blank_run.source_id = "blank-run".into();
    blank_run.workflow_instance_id = Some("".into());
    let mut cross_repo = recovered.clone();
    cross_repo.source_id = "cross-repo".into();
    cross_repo.repo = "repo-b".into();
    let mut wrong_workflow = recovered.clone();
    wrong_workflow.source_id = "wrong-workflow".into();
    wrong_workflow.workflow = Some("deploy".into());

    let facts = OutcomeFactBuilder::from_structured_inputs(
        [
            failed,
            failed_none,
            recovered,
            none_none,
            blank_run,
            cross_repo,
            wrong_workflow,
        ],
        [],
    )
    .build();

    let recovered_ids: Vec<_> = facts
        .iter()
        .filter(|f| f.status == OutcomeStatus::CiRecovered)
        .map(|f| f.source.source_id.as_str())
        .collect();
    assert_eq!(recovered_ids, vec!["recovered-ok"]);
}

#[test]
fn archived_only_family_still_emits_availability_metadata_when_archived_excluded() {
    let mut archived_one = base(
        OutcomeEvidenceKind::Phase4CiSignal,
        "archived-ci-1",
        FactoryMetricPayload::Ci {
            failed: true,
            recovered: false,
        },
    );
    archived_one.archived = true;
    let mut archived_two = archived_one.clone();
    archived_two.source_id = "archived-ci-2".into();

    let facts =
        OutcomeFactBuilder::from_structured_inputs([archived_one, archived_two], []).build();

    assert_eq!(facts.len(), 1);
    let metadata = &facts[0];
    assert_eq!(metadata.status, OutcomeStatus::Unobserved);
    assert_eq!(metadata.repo, "repo-a");
    assert_eq!(
        metadata.availability.source_family,
        OutcomeEvidenceKind::Phase4CiSignal
    );
    assert!(!metadata.availability.available);
    assert_eq!(metadata.source_counts.active_source_count, 0);
    assert_eq!(metadata.source_counts.archived_source_count, 2);
    assert_eq!(metadata.source_counts.event_count, 2);
}

#[test]
fn source_counts_count_distinct_sources_but_events_count_events() {
    let first = base(
        OutcomeEvidenceKind::AgentRecord,
        "agent-a",
        FactoryMetricPayload::Run { count: 1 },
    );
    let mut second_event_same_source = first.clone();
    second_event_same_source.observed_at_ms = 20;
    let mut archived = base(
        OutcomeEvidenceKind::AgentRecord,
        "agent-b",
        FactoryMetricPayload::Run { count: 1 },
    );
    archived.archived = true;

    let facts =
        OutcomeFactBuilder::from_structured_inputs([first, second_event_same_source, archived], [])
            .build();
    let fact = facts
        .iter()
        .find(|f| f.source.source_id == "agent-a")
        .unwrap();

    assert_eq!(fact.source_counts.active_source_count, 1);
    assert_eq!(fact.source_counts.archived_source_count, 1);
    assert_eq!(fact.source_counts.event_count, 3);
}

#[test]
fn task_class_rejects_blank_values_and_blank_provenance() {
    let mut blank_class = base(
        OutcomeEvidenceKind::Phase3Contract,
        "blank-class",
        FactoryMetricPayload::TaskClass,
    );
    blank_class.task_class = Some("  ".into());

    let mut blank_ticket = base(
        OutcomeEvidenceKind::AgentRecord,
        "blank-ticket",
        FactoryMetricPayload::TaskClass,
    );
    blank_ticket.task_class = Some("bugfix".into());
    blank_ticket.ticket_id = Some(" ".into());

    let facts = OutcomeFactBuilder::from_structured_inputs([blank_class, blank_ticket], []).build();

    assert!(facts
        .iter()
        .all(|f| f.group_key.task_class.as_deref() == Some("unknown")));
    assert!(facts.iter().all(|f| !f.source.warnings.is_empty()));
}

#[test]
fn cost_and_lead_time_accept_only_valid_direct_run_evidence() {
    let direct_cost = base(
        OutcomeEvidenceKind::AgentRecord,
        "direct-cost",
        FactoryMetricPayload::Cost {
            micro_usd: 7,
            pricing_evidence_id: None,
        },
    );
    let pricing_only = base(
        OutcomeEvidenceKind::PricingSnapshot,
        "pricing-only",
        FactoryMetricPayload::Cost {
            micro_usd: 99,
            pricing_evidence_id: Some("price-1".into()),
        },
    );
    let mut agent_lead = base(
        OutcomeEvidenceKind::AgentRecord,
        "agent-lead",
        FactoryMetricPayload::LeadTime {
            started_at_ms: 5,
            completed_at_ms: 9,
            run_id: "run-1".into(),
            completed_run_id: "run-1".into(),
        },
    );
    agent_lead.workflow_instance_id = Some("run-1".into());
    let mut unbound_lead = agent_lead.clone();
    unbound_lead.source_id = "unbound-lead".into();
    unbound_lead.workflow_instance_id = None;

    let facts = OutcomeFactBuilder::from_structured_inputs(
        [direct_cost, pricing_only, agent_lead, unbound_lead],
        [],
    )
    .build();

    assert_eq!(
        facts
            .iter()
            .find(|f| f.source.source_id == "direct-cost")
            .unwrap()
            .cost_micro_usd,
        Some(7)
    );
    assert_eq!(
        facts
            .iter()
            .find(|f| f.source.source_id == "pricing-only")
            .unwrap()
            .cost_micro_usd,
        None
    );
    assert_eq!(
        facts
            .iter()
            .find(|f| f.source.source_id == "agent-lead")
            .unwrap()
            .lead_time_ms,
        Some(4)
    );
    assert_eq!(
        facts
            .iter()
            .find(|f| f.source.source_id == "unbound-lead")
            .unwrap()
            .lead_time_ms,
        None
    );
}

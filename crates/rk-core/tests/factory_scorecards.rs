use rk_core::factory::{
    outcome_events::{FactoryMetricPayload, StructuredOutcomeInput},
    outcome_facts::{OutcomeFactBuilder, OutcomeFactSource},
    scorecards::{aggregate_scorecards, ScorecardProjection, ScorecardQuery},
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
        task_class: Some("bugfix".into()),
        workflow: Some("wf-a".into()),
        harness: Some("harness-a".into()),
        model: Some("model-a".into()),
        agent_id: None,
        workflow_instance_id: None,
        ticket_id: Some("ticket-a".into()),
        phase3_outcome_id: None,
        phase4_signal_id: None,
        recurrence_key: None,
        coalesce_key: None,
        payload,
        decoy_prose: "accepted rework ci failed reverted approval duplicate expensive slow".into(),
    }
}

fn facts(inputs: Vec<StructuredOutcomeInput>) -> Vec<rk_core::factory::outcome_facts::OutcomeFact> {
    OutcomeFactBuilder::from_structured_inputs(
        inputs,
        [OutcomeFactSource::unavailable(
            OutcomeEvidenceKind::PricingSnapshot,
        )],
    )
    .include_archived(true)
    .build()
}

fn facts_active_only(
    inputs: Vec<StructuredOutcomeInput>,
) -> Vec<rk_core::factory::outcome_facts::OutcomeFact> {
    OutcomeFactBuilder::from_structured_inputs(inputs, std::iter::empty::<OutcomeFactSource>())
        .include_archived(false)
        .build()
}

#[test]
fn groups_metrics_by_composite_task_class_workflow_harness_model() {
    let mut other = base(
        OutcomeEvidenceKind::AgentRecord,
        "run-b",
        FactoryMetricPayload::Run { count: 1 },
    );
    other.harness = Some("harness-b".into());
    let rows = aggregate_scorecards(
        &facts(vec![
            base(
                OutcomeEvidenceKind::AgentRecord,
                "run-a",
                FactoryMetricPayload::Run { count: 1 },
            ),
            other,
        ]),
        ScorecardQuery::default(),
    );

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].group_key.task_class, "bugfix");
    assert_eq!(rows[0].group_key.workflow, "wf-a");
    assert_eq!(rows[0].group_key.harness, "harness-a");
    assert_eq!(rows[0].group_key.model, "model-a");
    assert_eq!(rows[0].metrics.runs, 1);
    assert_eq!(rows[1].group_key.harness, "harness-b");
    assert_eq!(rows[2].metrics.unobserved, 1);
}

#[test]
fn can_project_composite_rows_without_losing_source_counts() {
    let rows = aggregate_scorecards(
        &facts(vec![base(
            OutcomeEvidenceKind::AgentRecord,
            "run-a",
            FactoryMetricPayload::Run { count: 1 },
        )]),
        ScorecardQuery {
            projections: vec![ScorecardProjection::TaskClassWorkflow],
            include_archived: true,
        },
    );

    assert!(rows
        .iter()
        .any(|row| row.projection == ScorecardProjection::Composite && !row.projected));
    let projected = rows
        .iter()
        .find(|row| row.projection == ScorecardProjection::TaskClassWorkflow)
        .unwrap();
    assert!(projected.projected);
    assert_eq!(projected.group_key.task_class, "bugfix");
    assert_eq!(projected.group_key.workflow, "wf-a");
    assert_eq!(projected.group_key.harness, "*");
    assert_eq!(
        projected
            .source_counts
            .by_family
            .get(&OutcomeEvidenceKind::AgentRecord)
            .unwrap()
            .active_source_count,
        1
    );
    assert!(
        !projected
            .availability
            .by_family
            .get(&OutcomeEvidenceKind::PricingSnapshot)
            .unwrap()
            .available
    );
}

#[test]
fn counts_runs_accepted_reworked_ci_failed_ci_recovered_reverted_unknown_and_unobserved() {
    let mut failed = base(
        OutcomeEvidenceKind::Phase4CiSignal,
        "ci-failed",
        FactoryMetricPayload::Ci {
            failed: true,
            recovered: false,
        },
    );
    failed.workflow_instance_id = Some("run-1".into());
    let mut recovered = base(
        OutcomeEvidenceKind::Phase4CiSignal,
        "ci-recovered",
        FactoryMetricPayload::Ci {
            failed: false,
            recovered: true,
        },
    );
    recovered.workflow_instance_id = Some("run-1".into());
    recovered.phase4_signal_id = Some("ci-failed".into());
    recovered.observed_at_ms = 20;
    let rows = aggregate_scorecards(
        &facts(vec![
            base(
                OutcomeEvidenceKind::AgentRecord,
                "run",
                FactoryMetricPayload::Run { count: 1 },
            ),
            base(
                OutcomeEvidenceKind::Phase3VerifiedDelivery,
                "accepted",
                FactoryMetricPayload::Accepted {
                    verified_delivery: true,
                    landed: false,
                },
            ),
            base(
                OutcomeEvidenceKind::StructuredReviewerRework,
                "rework",
                FactoryMetricPayload::Reworked { requested: true },
            ),
            failed,
            recovered,
            base(
                OutcomeEvidenceKind::StructuredRevert,
                "revert",
                FactoryMetricPayload::Reverted { reverted: true },
            ),
            base(
                OutcomeEvidenceKind::AgentRecord,
                "unknown",
                FactoryMetricPayload::Unknown,
            ),
        ]),
        ScorecardQuery::default(),
    );
    let row = &rows[0];
    assert_eq!(row.metrics.runs, 1);
    assert_eq!(row.metrics.accepted, 1);
    assert_eq!(row.metrics.reworked, 1);
    assert_eq!(row.metrics.ci_failed, 1);
    assert_eq!(row.metrics.ci_recovered, 1);
    assert_eq!(row.metrics.reverted, 1);
    assert_eq!(row.metrics.unknown, 0);
    assert_eq!(rows.last().unwrap().metrics.unobserved, 1);
    assert_eq!(row.status_counts.get(&OutcomeStatus::Accepted), Some(&1));
}

#[test]
fn counts_only_explicit_run_facts_as_runs_without_triple_counting_agent_metrics() {
    let rows = aggregate_scorecards(
        &facts(vec![
            base(
                OutcomeEvidenceKind::AgentRecord,
                "run",
                FactoryMetricPayload::Run { count: 1 },
            ),
            base(
                OutcomeEvidenceKind::AgentRecord,
                "cost",
                FactoryMetricPayload::Cost {
                    micro_usd: 41,
                    pricing_evidence_id: Some("agent-cost".into()),
                },
            ),
            {
                let mut input = base(
                    OutcomeEvidenceKind::AgentRecord,
                    "lead",
                    FactoryMetricPayload::LeadTime {
                        started_at_ms: 10,
                        completed_at_ms: 30,
                        run_id: "wf-1".into(),
                        completed_run_id: "wf-1".into(),
                    },
                );
                input.workflow_instance_id = Some("wf-1".into());
                input
            },
            base(
                OutcomeEvidenceKind::AgentRecord,
                "unknown",
                FactoryMetricPayload::Unknown,
            ),
        ]),
        ScorecardQuery::default(),
    );

    let row = &rows[0];
    assert_eq!(row.metrics.runs, 1);
    assert_eq!(row.metrics.active_runs, 1);
    assert_eq!(row.metrics.archived_runs, 0);
    assert_eq!(row.metrics.cost_sample_size, 1);
    assert_eq!(row.metrics.lead_time_sample_size, 1);
    assert_eq!(row.metrics.unknown, 0);
    assert_eq!(row.sample_size, 1);
    assert!(row.status_counts.get(&OutcomeStatus::Unknown).is_none());
}

#[test]
fn deduplicates_requested_projections_and_prefers_available_sources_deterministically() {
    let rows = aggregate_scorecards(
        &facts(vec![base(
            OutcomeEvidenceKind::AgentRecord,
            "run-a",
            FactoryMetricPayload::Run { count: 1 },
        )]),
        ScorecardQuery {
            projections: vec![ScorecardProjection::All, ScorecardProjection::All],
            include_archived: false,
        },
    );

    assert_eq!(
        rows.iter()
            .filter(|row| row.projection == ScorecardProjection::All)
            .count(),
        1
    );
    assert!(
        rows.iter()
            .find(|row| row.projection == ScorecardProjection::All)
            .unwrap()
            .availability
            .by_family
            .get(&OutcomeEvidenceKind::AgentRecord)
            .unwrap()
            .available
    );
}

#[test]
fn projected_source_counts_union_distinct_source_ids_by_group() {
    let mut second = base(
        OutcomeEvidenceKind::AgentRecord,
        "source-b",
        FactoryMetricPayload::Run { count: 1 },
    );
    second.harness = Some("harness-b".into());

    let rows = aggregate_scorecards(
        &facts(vec![
            base(
                OutcomeEvidenceKind::AgentRecord,
                "source-a",
                FactoryMetricPayload::Run { count: 1 },
            ),
            second,
        ]),
        ScorecardQuery {
            projections: vec![ScorecardProjection::All],
            include_archived: false,
        },
    );

    let all = rows
        .iter()
        .find(|row| row.projection == ScorecardProjection::All)
        .unwrap();
    let counts = all
        .source_counts
        .by_family
        .get(&OutcomeEvidenceKind::AgentRecord)
        .unwrap();
    assert_eq!(counts.active_source_count, 2);
    assert_eq!(counts.archived_source_count, 0);
    assert_eq!(counts.event_count, 2);
}

#[test]
fn projected_recurrence_does_not_merge_same_key_across_distinct_composites() {
    let mut first = base(
        OutcomeEvidenceKind::RecurrenceKey,
        "rec-a",
        FactoryMetricPayload::Recurrence,
    );
    first.recurrence_key = Some("same".into());
    let mut second = base(
        OutcomeEvidenceKind::RecurrenceKey,
        "rec-b",
        FactoryMetricPayload::Recurrence,
    );
    second.recurrence_key = Some("same".into());
    second.harness = Some("other-harness".into());

    let rows = aggregate_scorecards(
        &facts(vec![first, second]),
        ScorecardQuery {
            projections: vec![ScorecardProjection::TaskClassWorkflow],
            include_archived: false,
        },
    );

    let projected = rows
        .iter()
        .find(|row| row.projection == ScorecardProjection::TaskClassWorkflow)
        .unwrap();
    assert_eq!(projected.metrics.recurrence_count, 0);
    assert_eq!(projected.metrics.distinct_recurrence_keys, 0);
    assert_eq!(projected.metrics.recurrence_sample_size, 2);
}

#[test]
fn archived_only_metadata_preserves_source_counts_when_archived_excluded() {
    let mut archived = base(
        OutcomeEvidenceKind::AgentRecord,
        "archived-only-source",
        FactoryMetricPayload::Run { count: 1 },
    );
    archived.archived = true;

    let built = facts_active_only(vec![archived]);
    let rows = aggregate_scorecards(
        &built,
        ScorecardQuery {
            include_archived: false,
            projections: vec![],
        },
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].metrics.runs, 0);
    assert_eq!(rows[0].metrics.archived_runs, 0);
    assert_eq!(rows[0].metrics.unobserved, 1);
    let counts = rows[0]
        .source_counts
        .by_family
        .get(&OutcomeEvidenceKind::AgentRecord)
        .unwrap();
    assert_eq!(counts.active_source_count, 0);
    assert_eq!(counts.archived_source_count, 1);
    assert_eq!(counts.event_count, 1);
}

#[test]
fn source_counts_do_not_collapse_same_source_id_across_repos_or_projections() {
    let first = base(
        OutcomeEvidenceKind::AgentRecord,
        "same-source-id",
        FactoryMetricPayload::Run { count: 1 },
    );
    let mut second = base(
        OutcomeEvidenceKind::AgentRecord,
        "same-source-id",
        FactoryMetricPayload::Run { count: 1 },
    );
    second.repo = "repo-b".into();
    second.harness = Some("harness-b".into());

    let rows = aggregate_scorecards(
        &facts(vec![first, second]),
        ScorecardQuery {
            include_archived: false,
            projections: vec![ScorecardProjection::All, ScorecardProjection::TaskClassWorkflow],
        },
    );

    let all = rows
        .iter()
        .find(|row| row.projection == ScorecardProjection::All)
        .unwrap();
    let all_counts = all
        .source_counts
        .by_family
        .get(&OutcomeEvidenceKind::AgentRecord)
        .unwrap();
    assert_eq!(all_counts.active_source_count, 2);
    assert_eq!(all_counts.event_count, 2);

    let projected = rows
        .iter()
        .find(|row| row.projection == ScorecardProjection::TaskClassWorkflow)
        .unwrap();
    let projected_counts = projected
        .source_counts
        .by_family
        .get(&OutcomeEvidenceKind::AgentRecord)
        .unwrap();
    assert_eq!(projected_counts.active_source_count, 2);
    assert_eq!(projected_counts.event_count, 2);
}

#[test]
fn cost_and_nearest_rank_arithmetic_are_overflow_safe() {
    let mut expensive_a = base(
        OutcomeEvidenceKind::AgentRecord,
        "cost-a",
        FactoryMetricPayload::Cost {
            micro_usd: u64::MAX,
            pricing_evidence_id: Some("pricing".into()),
        },
    );
    expensive_a.observed_at_ms = 1;
    let mut expensive_b = base(
        OutcomeEvidenceKind::AgentRecord,
        "cost-b",
        FactoryMetricPayload::Cost {
            micro_usd: u64::MAX,
            pricing_evidence_id: Some("pricing".into()),
        },
    );
    expensive_b.observed_at_ms = 2;
    let rows = aggregate_scorecards(
        &facts(vec![expensive_a, expensive_b]),
        ScorecardQuery::default(),
    );

    assert_eq!(rows[0].metrics.total_cost_micro_usd, u64::MAX);
    assert_eq!(rows[0].metrics.average_cost_micro_usd, Some(u64::MAX));
}

#[test]
fn separates_active_and_archived_history_counts() {
    let mut archived = base(
        OutcomeEvidenceKind::AgentRecord,
        "archived",
        FactoryMetricPayload::Run { count: 1 },
    );
    archived.archived = true;
    let built = facts(vec![
        base(
            OutcomeEvidenceKind::AgentRecord,
            "active",
            FactoryMetricPayload::Run { count: 1 },
        ),
        archived,
    ]);
    let active_only = aggregate_scorecards(&built, ScorecardQuery::default());
    let with_archived = aggregate_scorecards(
        &built,
        ScorecardQuery {
            include_archived: true,
            projections: vec![],
        },
    );

    assert_eq!(active_only[0].metrics.runs, 1);
    assert_eq!(active_only[0].metrics.active_runs, 1);
    assert_eq!(active_only[0].metrics.archived_runs, 0);
    assert_eq!(
        active_only[0]
            .source_counts
            .by_family
            .get(&OutcomeEvidenceKind::AgentRecord)
            .unwrap()
            .archived_source_count,
        1
    );
    assert_eq!(with_archived[0].metrics.runs, 2);
    assert_eq!(with_archived[0].metrics.archived_runs, 1);
}

#[test]
fn aggregates_micro_usd_with_integer_rounding_and_reversed_input_byte_equivalence() {
    let a = base(
        OutcomeEvidenceKind::PricingSnapshot,
        "cost-a",
        FactoryMetricPayload::Cost {
            micro_usd: 100,
            pricing_evidence_id: Some("p".into()),
        },
    );
    let b = base(
        OutcomeEvidenceKind::AgentRecord,
        "cost-b",
        FactoryMetricPayload::Cost {
            micro_usd: 101,
            pricing_evidence_id: Some("p".into()),
        },
    );
    let forward = aggregate_scorecards(
        &facts(vec![a.clone(), b.clone()]),
        ScorecardQuery::default(),
    );
    let reverse = aggregate_scorecards(&facts(vec![b, a]), ScorecardQuery::default());

    assert_eq!(forward[0].metrics.total_cost_micro_usd, 101);
    assert_eq!(forward[0].metrics.average_cost_micro_usd, Some(101));
    assert_eq!(forward[0].metrics.cost_sample_size, 1);
    assert_eq!(
        forward[0]
            .evidence_counts
            .by_kind
            .get(&OutcomeEvidenceKind::PricingSnapshot),
        Some(&1)
    );
    assert_eq!(
        serde_json::to_vec(&forward).unwrap(),
        serde_json::to_vec(&reverse).unwrap()
    );
}

#[test]
fn computes_lead_time_median_and_p95_nearest_rank() {
    let mut inputs = Vec::new();
    for (idx, ms) in [10, 20, 30, 40, 50].into_iter().enumerate() {
        let run_id = format!("r{idx}");
        let mut input = base(
            OutcomeEvidenceKind::WorkflowInstance,
            &format!("lead-{idx}"),
            FactoryMetricPayload::LeadTime {
                started_at_ms: 0,
                completed_at_ms: ms,
                run_id: run_id.clone(),
                completed_run_id: run_id.clone(),
            },
        );
        input.workflow_instance_id = Some(run_id);
        inputs.push(input);
    }
    let rows = aggregate_scorecards(&facts(inputs), ScorecardQuery::default());
    assert_eq!(rows[0].metrics.median_lead_time_ms, Some(30));
    assert_eq!(rows[0].metrics.p95_lead_time_ms, Some(50));
    assert_eq!(rows[0].metrics.lead_time_sample_size, 5);
}

#[test]
fn computes_nearest_rank_for_one_and_two_lead_time_samples() {
    let one = {
        let mut input = base(
            OutcomeEvidenceKind::WorkflowInstance,
            "lead-one",
            FactoryMetricPayload::LeadTime {
                started_at_ms: 0,
                completed_at_ms: 10,
                run_id: "one".into(),
                completed_run_id: "one".into(),
            },
        );
        input.workflow_instance_id = Some("one".into());
        input
    };
    let mut two_a = one.clone();
    two_a.source_id = "lead-two-a".into();
    two_a.workflow_instance_id = Some("two-a".into());
    if let FactoryMetricPayload::LeadTime {
        run_id,
        completed_run_id,
        ..
    } = &mut two_a.payload
    {
        *run_id = "two-a".into();
        *completed_run_id = "two-a".into();
    }
    let mut two_b = one.clone();
    two_b.source_id = "lead-two-b".into();
    two_b.workflow_instance_id = Some("two-b".into());
    if let FactoryMetricPayload::LeadTime {
        completed_at_ms,
        run_id,
        completed_run_id,
        ..
    } = &mut two_b.payload
    {
        *completed_at_ms = 20;
        *run_id = "two-b".into();
        *completed_run_id = "two-b".into();
    }

    let one_row = aggregate_scorecards(&facts(vec![one]), ScorecardQuery::default());
    assert_eq!(one_row[0].metrics.median_lead_time_ms, Some(10));
    assert_eq!(one_row[0].metrics.p95_lead_time_ms, Some(10));

    let two_rows = aggregate_scorecards(&facts(vec![two_a, two_b]), ScorecardQuery::default());
    assert_eq!(two_rows[0].metrics.median_lead_time_ms, Some(10));
    assert_eq!(two_rows[0].metrics.p95_lead_time_ms, Some(20));
}

#[test]
fn counts_human_interventions_from_explicit_events_only() {
    let rows = aggregate_scorecards(
        &facts(vec![
            base(
                OutcomeEvidenceKind::HumanGateDecision,
                "gate",
                FactoryMetricPayload::HumanIntervention { count: 2 },
            ),
            base(
                OutcomeEvidenceKind::AgentRecord,
                "prose",
                FactoryMetricPayload::HumanIntervention { count: 7 },
            ),
        ]),
        ScorecardQuery::default(),
    );
    assert_eq!(rows[0].metrics.human_interventions, 2);
    assert_eq!(rows[0].metrics.intervention_sample_size, 1);
}

#[test]
fn counts_recurrence_only_from_repeated_explicit_keys() {
    let mut a = base(
        OutcomeEvidenceKind::RecurrenceKey,
        "rec-a",
        FactoryMetricPayload::Recurrence,
    );
    a.recurrence_key = Some("same".into());
    let mut b = base(
        OutcomeEvidenceKind::RecurrenceKey,
        "rec-b",
        FactoryMetricPayload::Recurrence,
    );
    b.coalesce_key = Some("same".into());
    let mut c = base(
        OutcomeEvidenceKind::RecurrenceKey,
        "rec-c",
        FactoryMetricPayload::Recurrence,
    );
    c.recurrence_key = Some("single".into());
    let rows = aggregate_scorecards(&facts(vec![a, b, c]), ScorecardQuery::default());
    assert_eq!(rows[0].metrics.recurrence_count, 2);
    assert_eq!(rows[0].metrics.distinct_recurrence_keys, 1);
    assert_eq!(rows[0].metrics.recurrence_sample_size, 3);
}

#[test]
fn includes_evidence_and_source_counts_by_family() {
    let rows = aggregate_scorecards(
        &facts(vec![
            base(
                OutcomeEvidenceKind::AgentRecord,
                "agent",
                FactoryMetricPayload::Run { count: 1 },
            ),
            {
                let mut input = base(
                    OutcomeEvidenceKind::WorkflowInstance,
                    "instance",
                    FactoryMetricPayload::LeadTime {
                        started_at_ms: 0,
                        completed_at_ms: 1,
                        run_id: "r".into(),
                        completed_run_id: "r".into(),
                    },
                );
                input.workflow_instance_id = Some("r".into());
                input
            },
            base(
                OutcomeEvidenceKind::Phase3VerifiedDelivery,
                "phase3",
                FactoryMetricPayload::Accepted {
                    verified_delivery: true,
                    landed: false,
                },
            ),
            base(
                OutcomeEvidenceKind::Phase4CiSignal,
                "phase4",
                FactoryMetricPayload::Ci {
                    failed: true,
                    recovered: false,
                },
            ),
            base(
                OutcomeEvidenceKind::StructuredReviewerRework,
                "review",
                FactoryMetricPayload::Reworked { requested: true },
            ),
            base(
                OutcomeEvidenceKind::StructuredRevert,
                "revert",
                FactoryMetricPayload::Reverted { reverted: true },
            ),
            base(
                OutcomeEvidenceKind::HumanGateDecision,
                "gate",
                FactoryMetricPayload::HumanIntervention { count: 1 },
            ),
            base(
                OutcomeEvidenceKind::PricingSnapshot,
                "pricing",
                FactoryMetricPayload::Unknown,
            ),
        ]),
        ScorecardQuery::default(),
    );
    let row = &rows[0];
    for kind in [
        OutcomeEvidenceKind::AgentRecord,
        OutcomeEvidenceKind::WorkflowInstance,
        OutcomeEvidenceKind::Phase3VerifiedDelivery,
        OutcomeEvidenceKind::Phase4CiSignal,
        OutcomeEvidenceKind::StructuredReviewerRework,
        OutcomeEvidenceKind::StructuredRevert,
        OutcomeEvidenceKind::HumanGateDecision,
        OutcomeEvidenceKind::PricingSnapshot,
    ] {
        assert_eq!(row.evidence_counts.by_kind.get(&kind), Some(&1));
        let expected_events = if kind == OutcomeEvidenceKind::PricingSnapshot {
            2
        } else {
            1
        };
        assert_eq!(
            row.source_counts.by_family.get(&kind).unwrap().event_count,
            expected_events
        );
    }
}

#[test]
fn sorts_scorecard_rows_by_composite_key_projection_and_metric() {
    let mut b = base(
        OutcomeEvidenceKind::AgentRecord,
        "b",
        FactoryMetricPayload::Run { count: 1 },
    );
    b.task_class = Some("b-task".into());
    let mut a = base(
        OutcomeEvidenceKind::AgentRecord,
        "a",
        FactoryMetricPayload::Run { count: 1 },
    );
    a.task_class = Some("a-task".into());
    let rows = aggregate_scorecards(
        &facts(vec![b, a]),
        ScorecardQuery {
            include_archived: true,
            projections: vec![ScorecardProjection::All],
        },
    );
    let sort_keys: Vec<_> = rows
        .iter()
        .map(|row| {
            (
                row.group_key.task_class.clone(),
                row.projection.clone(),
                row.metric_sort_key.clone(),
            )
        })
        .collect();
    let mut sorted = sort_keys.clone();
    sorted.sort();
    assert_eq!(sort_keys, sorted);
}

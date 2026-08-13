use rk_core::factory::{
    recommendations::{
        evaluate_recommendation_report, evaluate_recommendations, RecommendationId,
        RecommendationThresholds,
    },
    scorecards::{
        FactoryScorecard, MetricAvailability, ScorecardGroupKey, ScorecardMetrics,
        ScorecardProjection, ScorecardSourceCounts,
    },
    OutcomeEvidenceKind, SourceAvailability, SourceCounts,
};
use std::collections::BTreeMap;

fn row(task_class: &str, workflow: &str) -> FactoryScorecard {
    FactoryScorecard {
        group_key: ScorecardGroupKey {
            task_class: task_class.into(),
            workflow: workflow.into(),
            harness: "harness-a".into(),
            model: "model-a".into(),
        },
        projection: ScorecardProjection::Composite,
        projected: false,
        metrics: ScorecardMetrics::default(),
        status_counts: BTreeMap::new(),
        evidence_counts: Default::default(),
        source_counts: ScorecardSourceCounts {
            by_family: BTreeMap::new(),
        },
        availability: MetricAvailability {
            by_family: BTreeMap::new(),
        },
        sample_size: 0,
        metric_sort_key: String::new(),
    }
}

fn available(
    mut row: FactoryScorecard,
    family: OutcomeEvidenceKind,
    active: u32,
    events: u32,
) -> FactoryScorecard {
    row.availability.by_family.insert(
        family,
        SourceAvailability {
            source_family: family,
            available: true,
        },
    );
    row.source_counts.by_family.insert(
        family,
        SourceCounts {
            active_source_count: active,
            archived_source_count: 0,
            event_count: events,
        },
    );
    row
}

fn unavailable(mut row: FactoryScorecard, family: OutcomeEvidenceKind) -> FactoryScorecard {
    row.availability.by_family.insert(
        family,
        SourceAvailability {
            source_family: family,
            available: false,
        },
    );
    row.source_counts
        .by_family
        .insert(family, SourceCounts::default());
    row
}

fn runs(mut row: FactoryScorecard, runs: u32, accepted: u32) -> FactoryScorecard {
    row.metrics.runs = runs;
    row.metrics.active_runs = runs;
    row.metrics.accepted = accepted;
    row.sample_size = runs;
    available(row, OutcomeEvidenceKind::AgentRecord, runs, runs)
}

fn ids(rows: &[FactoryScorecard]) -> Vec<RecommendationId> {
    evaluate_recommendations(rows)
        .into_iter()
        .map(|r| r.id)
        .collect()
}

#[test]
fn emits_all_rule_families_in_deterministic_order_with_thresholds_evidence_and_counts() {
    let mut target = runs(row("bugfix", "wf-a"), 20, 10);
    target.metrics.reworked = 7;
    target.metrics.ci_failed = 7;
    target.metrics.ci_recovered = 1;
    target.metrics.reverted = 3;
    target.metrics.average_cost_micro_usd = Some(2_500_000);
    target.metrics.cost_sample_size = 6;
    target.metrics.median_lead_time_ms = Some(8 * 60 * 60 * 1000);
    target.metrics.p95_lead_time_ms = Some(20 * 60 * 60 * 1000);
    target.metrics.lead_time_sample_size = 6;
    target.metrics.human_interventions = 8;
    target.metrics.intervention_sample_size = 8;
    target.metrics.recurrence_count = 5;
    target.metrics.distinct_recurrence_keys = 5;
    target.metrics.recurrence_sample_size = 10;
    target = available(target, OutcomeEvidenceKind::Phase3VerifiedDelivery, 20, 20);
    target = available(target, OutcomeEvidenceKind::StructuredReviewerRework, 7, 7);
    target = available(target, OutcomeEvidenceKind::Phase4CiSignal, 7, 8);
    target = available(target, OutcomeEvidenceKind::StructuredRevert, 3, 3);
    target = available(target, OutcomeEvidenceKind::PricingSnapshot, 6, 6);
    target = available(target, OutcomeEvidenceKind::HumanGateDecision, 8, 8);
    target = available(target, OutcomeEvidenceKind::RecurrenceKey, 5, 10);

    let mut peer = runs(row("bugfix", "wf-a"), 20, 18);
    peer = available(peer, OutcomeEvidenceKind::Phase3VerifiedDelivery, 20, 20);

    let recommendations = evaluate_recommendations(&[target, peer]);

    assert_eq!(
        recommendations.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![
            RecommendationId::LowAcceptance,
            RecommendationId::HighRework,
            RecommendationId::CiInstability,
            RecommendationId::Reverts,
            RecommendationId::HighCost,
            RecommendationId::SlowLeadTime,
            RecommendationId::HumanIntervention,
            RecommendationId::Recurrence,
        ]
    );
    let first = &recommendations[0];
    assert_eq!(
        first.thresholds.min_runs,
        RecommendationThresholds::default().min_runs
    );
    assert_eq!(first.evidence.numerator, 10);
    assert_eq!(first.evidence.denominator, 20);
    assert_eq!(first.source_counts.active_source_count, 20);
}

#[test]
fn payload_is_advisory_and_excludes_mutation_language_and_fields() {
    let mut target = runs(row("bugfix", "wf-a"), 20, 10);
    target = available(target, OutcomeEvidenceKind::Phase3VerifiedDelivery, 20, 20);
    let mut peer = runs(row("bugfix", "wf-a"), 20, 19);
    peer = available(peer, OutcomeEvidenceKind::Phase3VerifiedDelivery, 20, 20);

    let payload = serde_json::to_string(&evaluate_recommendations(&[target, peer])).unwrap();

    assert!(payload.contains("advisory"));
    for forbidden in [
        "command", "patch", "routing", "policy", "config", "workflow", "ticket", "approval",
        "dispatch", "must", "apply", "execute",
    ] {
        assert!(
            !payload.to_lowercase().contains(forbidden),
            "forbidden token {forbidden} in {payload}"
        );
    }
}

#[test]
fn low_acceptance_compares_only_same_task_class_and_workflow_peers_with_required_samples() {
    let mut target = runs(row("bugfix", "wf-a"), 20, 12);
    target = available(target, OutcomeEvidenceKind::Phase3VerifiedDelivery, 20, 20);
    let mut same = runs(row("bugfix", "wf-a"), 20, 19);
    same = available(same, OutcomeEvidenceKind::Phase3VerifiedDelivery, 20, 20);
    let other_workflow = runs(row("bugfix", "wf-b"), 20, 20);
    let other_class = runs(row("feature", "wf-a"), 20, 20);
    let low_sample_same = runs(row("bugfix", "wf-a"), 3, 3);

    assert_eq!(
        ids(&[target, same, other_workflow, other_class, low_sample_same]),
        vec![RecommendationId::LowAcceptance]
    );
}

#[test]
fn low_acceptance_without_peer_produces_no_recommendation() {
    let mut target = runs(row("bugfix", "wf-a"), 20, 1);
    target = available(target, OutcomeEvidenceKind::Phase3VerifiedDelivery, 20, 20);

    assert!(evaluate_recommendations(&[target]).is_empty());
}

#[test]
fn unavailable_metrics_warn_and_suppress_without_advice() {
    let mut target = runs(row("bugfix", "wf-a"), 20, 20);
    target.metrics.average_cost_micro_usd = Some(9_000_000);
    target.metrics.cost_sample_size = 10;
    target = unavailable(target, OutcomeEvidenceKind::PricingSnapshot);

    let report = evaluate_recommendation_report(&[target]);

    assert!(report.recommendations.is_empty());
    assert_eq!(report.suppressions[0].id, RecommendationId::HighCost);
    assert_eq!(report.suppressions[0].reason, "metric_unavailable");
    assert_eq!(report.warnings[0], "metric_unavailable: high_cost");
}

#[test]
fn observed_but_low_sample_metrics_are_suppressed() {
    let mut target = runs(row("bugfix", "wf-a"), 20, 20);
    target.metrics.average_cost_micro_usd = Some(9_000_000);
    target.metrics.cost_sample_size = 2;
    target = available(target, OutcomeEvidenceKind::PricingSnapshot, 2, 2);

    let report = evaluate_recommendation_report(&[target]);

    assert!(report.recommendations.is_empty());
    assert_eq!(report.suppressions[0].reason, "low_sample");
}

#[test]
fn exact_denominators_are_used_for_rate_rules() {
    let mut target = runs(row("bugfix", "wf-a"), 20, 20);
    target.metrics.reworked = 5;
    target.metrics.ci_failed = 4;
    target.metrics.ci_recovered = 1;
    target.metrics.reverted = 1;
    target.metrics.human_interventions = 4;
    target.metrics.intervention_sample_size = 10;
    target.metrics.recurrence_count = 4;
    target.metrics.recurrence_sample_size = 10;
    target = available(target, OutcomeEvidenceKind::StructuredReviewerRework, 5, 5);
    target = available(target, OutcomeEvidenceKind::Phase4CiSignal, 4, 5);
    target = available(target, OutcomeEvidenceKind::StructuredRevert, 1, 1);
    target = available(target, OutcomeEvidenceKind::HumanGateDecision, 4, 10);
    target = available(target, OutcomeEvidenceKind::RecurrenceKey, 4, 10);

    let recommendations = evaluate_recommendations(&[target]);

    assert!(recommendations
        .iter()
        .any(|r| r.id == RecommendationId::HighRework && r.evidence.denominator == 20));
    assert!(recommendations
        .iter()
        .any(|r| r.id == RecommendationId::CiInstability && r.evidence.denominator == 4));
    assert!(recommendations
        .iter()
        .any(|r| r.id == RecommendationId::Reverts && r.evidence.denominator == 20));
    assert!(recommendations
        .iter()
        .any(|r| r.id == RecommendationId::HumanIntervention && r.evidence.denominator == 10));
    assert!(recommendations
        .iter()
        .any(|r| r.id == RecommendationId::Recurrence && r.evidence.denominator == 10));
}

#[test]
fn threshold_comparisons_do_not_overflow() {
    let mut target = runs(row("bugfix", "wf-a"), u32::MAX, u32::MAX);
    target.metrics.reworked = u32::MAX;
    target = available(
        target,
        OutcomeEvidenceKind::StructuredReviewerRework,
        u32::MAX,
        u32::MAX,
    );

    assert_eq!(ids(&[target]), vec![RecommendationId::HighRework]);
}

#[test]
fn projected_rows_are_ignored_so_evaluation_is_over_scorecard_observations_only() {
    let mut projected = runs(row("bugfix", "wf-a"), 20, 20);
    projected.projected = true;
    projected.projection = ScorecardProjection::TaskClass;
    projected.metrics.reworked = 20;
    projected = available(
        projected,
        OutcomeEvidenceKind::StructuredReviewerRework,
        20,
        20,
    );

    assert!(evaluate_recommendations(&[projected]).is_empty());
}

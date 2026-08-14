use rk_core::factory::{
    recommendations::{
        evaluate_recommendation_report, evaluate_recommendations, RecommendationRule,
        RecommendationSeverity, SuppressionReason,
    },
    scorecards::{
        FactoryScorecard, MetricAvailability, ScorecardGroupKey, ScorecardMetrics,
        ScorecardProjection, ScorecardSourceCounts,
    },
    OutcomeEvidenceKind, SourceAvailability, SourceCounts,
};
use std::collections::BTreeMap;

fn row(task_class: &str, workflow: &str, harness: &str, model: &str) -> FactoryScorecard {
    FactoryScorecard {
        group_key: ScorecardGroupKey {
            task_class: task_class.into(),
            workflow: workflow.into(),
            harness: harness.into(),
            model: model.into(),
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
    match family {
        OutcomeEvidenceKind::Phase3VerifiedDelivery => row.metrics.accepted_sample_size = events,
        OutcomeEvidenceKind::StructuredReviewerRework => row.metrics.rework_sample_size = events,
        OutcomeEvidenceKind::Phase4CiSignal => row.metrics.ci_sample_size = events,
        OutcomeEvidenceKind::StructuredRevert => row.metrics.revert_sample_size = events,
        OutcomeEvidenceKind::HumanGateDecision => row.metrics.intervention_sample_size = events,
        _ => {}
    }
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

fn observed(mut row: FactoryScorecard) -> FactoryScorecard {
    for family in [
        OutcomeEvidenceKind::Phase3VerifiedDelivery,
        OutcomeEvidenceKind::StructuredReviewerRework,
        OutcomeEvidenceKind::Phase4CiSignal,
        OutcomeEvidenceKind::StructuredRevert,
        OutcomeEvidenceKind::PricingSnapshot,
        OutcomeEvidenceKind::AgentRecord,
        OutcomeEvidenceKind::HumanGateDecision,
        OutcomeEvidenceKind::RecurrenceKey,
    ] {
        row = available(row, family, 10, 10);
    }
    row
}

#[test]
fn plan_contract_exposes_advisory_recommendation_metadata_without_mutation_fields_or_language() {
    let mut target = runs(row("bugfix", "wf-a", "harness-a", "model-a"), 10, 5);
    target = available(target, OutcomeEvidenceKind::Phase3VerifiedDelivery, 2, 10);
    let mut peer = runs(row("bugfix", "wf-a", "harness-b", "model-b"), 10, 8);
    peer = available(peer, OutcomeEvidenceKind::Phase3VerifiedDelivery, 3, 10);

    let recommendations = evaluate_recommendations(&[target.clone(), peer]);

    let rec = recommendations
        .iter()
        .find(|rec| rec.rule == RecommendationRule::LowAcceptance)
        .expect("low acceptance recommendation");
    assert_eq!(rec.severity, RecommendationSeverity::Critical);
    assert_eq!(rec.subject_group_key, target.group_key);
    assert!(rec.summary.contains("acceptance"));
    assert!(rec.advice.as_ref().unwrap().contains("Review"));
    assert_eq!(rec.thresholds.min_sample_size, 10);
    assert_eq!(rec.thresholds.acceptance_below_ratio, Some((60, 100)));
    assert_eq!(
        rec.thresholds.peer_acceptance_at_least_ratio,
        Some((80, 100))
    );
    assert_eq!(
        rec.metric_availability.source_family,
        OutcomeEvidenceKind::Phase3VerifiedDelivery
    );
    assert!(rec.metric_availability.available);
    assert_eq!(
        rec.comparison_evidence.as_ref().unwrap().comparable_median,
        None
    );
    assert_eq!(rec.evidence.numerator, Some(5));
    assert_eq!(rec.evidence.denominator, Some(10));
    assert_eq!(rec.source_counts.active_source_count, 2);
    assert_eq!(rec.sample_size, 10);
    assert!(!rec.suppressed);
    assert_eq!(rec.suppression_reason, None);

    let payload = serde_json::to_string(&recommendations)
        .unwrap()
        .to_lowercase();
    for forbidden in [
        "command", "patch", "routing", "policy", "config", "ticket", "approval", "dispatch",
        "must", "apply", "execute",
    ] {
        assert!(
            !payload.contains(forbidden),
            "forbidden token {forbidden} in {payload}"
        );
    }
}

#[test]
fn exact_plan_rules_emit_advice_only_when_thresholds_and_comparable_groups_match() {
    let mut target = observed(runs(row("bugfix", "wf-a", "harness-a", "model-a"), 10, 5));
    target.metrics.reworked = 3;
    target.metrics.ci_failed = 8;
    target.metrics.ci_recovered = 6;
    target.metrics.reverted = 1;
    target.metrics.average_cost_micro_usd = Some(150);
    target.metrics.cost_sample_size = 8;
    target.metrics.p95_lead_time_ms = Some(150);
    target.metrics.lead_time_sample_size = 8;
    target.metrics.human_interventions = 3;
    target.metrics.intervention_sample_size = 8;
    target.metrics.recurrence_count = 3;
    target.metrics.recurrence_sample_size = 5;

    let mut peer_a = observed(runs(row("bugfix", "wf-a", "harness-b", "model-b"), 10, 8));
    peer_a.metrics.ci_failed = 8;
    peer_a.metrics.ci_recovered = 8;
    peer_a.metrics.ci_failed = 1;
    peer_a.metrics.average_cost_micro_usd = Some(100);
    peer_a.metrics.cost_sample_size = 8;
    peer_a.metrics.p95_lead_time_ms = Some(100);
    peer_a.metrics.lead_time_sample_size = 8;

    let mut peer_b = observed(runs(row("bugfix", "wf-a", "harness-c", "model-c"), 10, 9));
    peer_b.metrics.ci_failed = 8;
    peer_b.metrics.ci_recovered = 8;
    peer_b.metrics.ci_failed = 1;
    peer_b.metrics.average_cost_micro_usd = Some(80);
    peer_b.metrics.cost_sample_size = 8;
    peer_b.metrics.p95_lead_time_ms = Some(80);
    peer_b.metrics.lead_time_sample_size = 8;

    let rules: Vec<_> = evaluate_recommendations(&[target, peer_a, peer_b])
        .into_iter()
        .filter(|rec| !rec.suppressed)
        .map(|rec| rec.rule)
        .collect();

    for rule in [
        RecommendationRule::LowAcceptance,
        RecommendationRule::HighRework,
        RecommendationRule::CiInstability,
        RecommendationRule::Reverts,
        RecommendationRule::HighCost,
        RecommendationRule::SlowLeadTime,
        RecommendationRule::HumanIntervention,
        RecommendationRule::Recurrence,
    ] {
        assert!(rules.contains(&rule), "missing {rule:?} in {rules:?}");
    }
    assert_eq!(rules.len(), 8);
}

#[test]
fn observed_below_threshold_samples_emit_suppressed_rows_with_reasons() {
    let mut target = observed(runs(row("bugfix", "wf-a", "harness-a", "model-a"), 10, 10));
    target.metrics.ci_failed = 1;
    target.metrics.ci_recovered = 0;
    target.metrics.ci_sample_size = 7;
    target.metrics.average_cost_micro_usd = Some(999);
    target.metrics.cost_sample_size = 7;
    target.metrics.p95_lead_time_ms = Some(999);
    target.metrics.lead_time_sample_size = 7;
    target.metrics.human_interventions = 2;
    target.metrics.intervention_sample_size = 7;
    target.metrics.recurrence_count = 2;
    target.metrics.recurrence_sample_size = 4;

    let report = evaluate_recommendation_report(&[target]);

    assert!(report.recommendations.iter().all(|rec| rec.suppressed));
    let suppressed: Vec<_> = report
        .recommendations
        .iter()
        .map(|rec| (rec.rule, rec.suppression_reason))
        .collect();
    assert!(suppressed.contains(&(
        RecommendationRule::CiInstability,
        Some(SuppressionReason::LowSample)
    )));
    assert!(suppressed.contains(&(
        RecommendationRule::HighCost,
        Some(SuppressionReason::LowSample)
    )));
    assert!(suppressed.contains(&(
        RecommendationRule::SlowLeadTime,
        Some(SuppressionReason::LowSample)
    )));
    assert!(suppressed.contains(&(
        RecommendationRule::HumanIntervention,
        Some(SuppressionReason::LowSample)
    )));
    assert!(suppressed.contains(&(
        RecommendationRule::Recurrence,
        Some(SuppressionReason::LowSample)
    )));
}

#[test]
fn unavailable_metrics_warn_and_suppress_without_advice() {
    let mut target = runs(row("bugfix", "wf-a", "harness-a", "model-a"), 10, 10);
    target.metrics.average_cost_micro_usd = Some(9_000_000);
    target.metrics.cost_sample_size = 10;
    target = unavailable(target, OutcomeEvidenceKind::PricingSnapshot);

    let report = evaluate_recommendation_report(&[target]);

    assert!(report
        .recommendations
        .iter()
        .all(|rec| rec.advice.is_none()));
    assert!(report
        .recommendations
        .iter()
        .any(|rec| rec.rule == RecommendationRule::HighCost
            && rec.suppressed
            && rec.suppression_reason == Some(SuppressionReason::MetricUnavailable)));
    assert!(report
        .warnings
        .iter()
        .any(|warning| warning.contains("high_cost")));
}

#[test]
fn cross_group_rows_are_not_comparable_evidence_for_peer_rules() {
    let mut target = observed(runs(row("bugfix", "wf-a", "harness-a", "model-a"), 10, 5));
    target.metrics.average_cost_micro_usd = Some(150);
    target.metrics.cost_sample_size = 8;
    target.metrics.p95_lead_time_ms = Some(150);
    target.metrics.lead_time_sample_size = 8;

    let mut other_workflow = observed(runs(row("bugfix", "wf-b", "harness-b", "model-b"), 10, 10));
    other_workflow.metrics.average_cost_micro_usd = Some(50);
    other_workflow.metrics.cost_sample_size = 8;
    other_workflow.metrics.p95_lead_time_ms = Some(50);
    other_workflow.metrics.lead_time_sample_size = 8;

    let mut other_class = observed(runs(row("feature", "wf-a", "harness-c", "model-c"), 10, 10));
    other_class.metrics.average_cost_micro_usd = Some(50);
    other_class.metrics.cost_sample_size = 8;
    other_class.metrics.p95_lead_time_ms = Some(50);
    other_class.metrics.lead_time_sample_size = 8;

    let recommendations = evaluate_recommendations(&[target, other_workflow, other_class]);

    assert!(!recommendations
        .iter()
        .any(|rec| rec.rule == RecommendationRule::LowAcceptance && !rec.suppressed));
    assert!(!recommendations
        .iter()
        .any(|rec| rec.rule == RecommendationRule::HighCost && !rec.suppressed));
    assert!(!recommendations
        .iter()
        .any(|rec| rec.rule == RecommendationRule::SlowLeadTime && !rec.suppressed));
}

#[test]
fn stable_order_is_severity_rule_task_class_workflow_harness_model_id() {
    let mut z = observed(runs(row("zeta", "wf-b", "harness-b", "model-b"), 10, 10));
    z.metrics.reworked = 3;
    let mut a = observed(runs(row("alpha", "wf-a", "harness-a", "model-a"), 10, 10));
    a.metrics.reworked = 3;

    let recs = evaluate_recommendations(&[z, a]);
    let unsuppressed: Vec<_> = recs.into_iter().filter(|rec| !rec.suppressed).collect();

    assert_eq!(unsuppressed[0].subject_group_key.task_class, "alpha");
    assert_eq!(unsuppressed[1].subject_group_key.task_class, "zeta");
}

#[test]
fn projected_rows_are_ignored_so_evaluation_is_over_scorecard_observations_only() {
    let mut projected = runs(row("bugfix", "wf-a", "harness-a", "model-a"), 20, 20);
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

#[test]
fn unknown_task_class_or_workflow_suppresses_peer_comparison_rules() {
    let mut target = observed(runs(row("unknown", "wf-a", "harness-a", "model-a"), 10, 5));
    target.metrics.ci_failed = 8;
    target.metrics.ci_recovered = 0;
    target.metrics.average_cost_micro_usd = Some(150);
    target.metrics.cost_sample_size = 8;
    target.metrics.p95_lead_time_ms = Some(150);
    target.metrics.lead_time_sample_size = 8;

    let mut peer = observed(runs(row("unknown", "wf-a", "harness-b", "model-b"), 10, 10));
    peer.metrics.ci_failed = 8;
    peer.metrics.ci_recovered = 8;
    peer.metrics.average_cost_micro_usd = Some(1);
    peer.metrics.cost_sample_size = 8;
    peer.metrics.p95_lead_time_ms = Some(1);
    peer.metrics.lead_time_sample_size = 8;

    let recs = evaluate_recommendations(&[target, peer]);

    for rule in [
        RecommendationRule::LowAcceptance,
        RecommendationRule::CiInstability,
        RecommendationRule::HighCost,
        RecommendationRule::SlowLeadTime,
    ] {
        let rec = recs
            .iter()
            .find(|rec| rec.rule == rule)
            .expect("suppressed peer rule");
        assert!(rec.suppressed, "{rule:?} should be suppressed");
        assert_eq!(rec.advice, None);
        assert_eq!(
            rec.suppression_reason,
            Some(SuppressionReason::NoComparablePeer)
        );
        assert_eq!(rec.subject_group_key.task_class, "unknown");
    }
}

#[test]
fn full_composite_key_separates_subjects_with_same_task_class_and_workflow() {
    let mut harness_a = observed(runs(row("bugfix", "wf-a", "harness-a", "model-a"), 10, 10));
    harness_a.metrics.reworked = 3;
    let mut harness_b = observed(runs(row("bugfix", "wf-a", "harness-b", "model-a"), 10, 10));
    harness_b.metrics.reworked = 3;

    let active: Vec<_> = evaluate_recommendations(&[harness_b.clone(), harness_a.clone()])
        .into_iter()
        .filter(|rec| rec.rule == RecommendationRule::HighRework && !rec.suppressed)
        .collect();

    assert_eq!(active.len(), 2);
    assert_eq!(active[0].subject_group_key, harness_a.group_key);
    assert_eq!(active[1].subject_group_key, harness_b.group_key);
    assert_ne!(active[0].id, active[1].id);
}

#[test]
fn low_sample_suppression_uses_metric_observed_denominator_not_runs() {
    let mut target = observed(runs(row("bugfix", "wf-a", "harness-a", "model-a"), 100, 0));
    target.metrics.accepted_sample_size = 1;
    target.metrics.reworked = 1;
    target.metrics.rework_sample_size = 1;
    target.metrics.reverted = 1;
    target.metrics.revert_sample_size = 1;

    let report = evaluate_recommendation_report(&[target]);

    for rule in [
        RecommendationRule::LowAcceptance,
        RecommendationRule::HighRework,
        RecommendationRule::Reverts,
    ] {
        let rec = report
            .recommendations
            .iter()
            .find(|rec| rec.rule == rule)
            .expect("suppressed recommendation");
        assert!(rec.suppressed, "{rule:?} should be suppressed");
        assert_eq!(rec.suppression_reason, Some(SuppressionReason::LowSample));
        assert_eq!(rec.evidence.denominator, Some(1));
        assert_eq!(rec.sample_size, 1);
    }
}

#[test]
fn ci_instability_uses_ci_availability_denominator_and_reports_recovery_separately() {
    let mut target = observed(runs(
        row("bugfix", "wf-a", "harness-a", "model-a"),
        100,
        100,
    ));
    target.metrics.ci_failed = 2;
    target.metrics.ci_recovered = 1;
    target.metrics.ci_sample_size = 10;

    let mut peer = observed(runs(
        row("bugfix", "wf-a", "harness-b", "model-b"),
        100,
        100,
    ));
    peer.metrics.ci_failed = 1;
    peer.metrics.ci_recovered = 1;
    peer.metrics.ci_sample_size = 10;

    let rec = evaluate_recommendations(&[target, peer])
        .into_iter()
        .find(|rec| rec.rule == RecommendationRule::CiInstability)
        .expect("ci recommendation");

    assert!(!rec.suppressed);
    assert_eq!(rec.evidence.numerator, Some(2));
    assert_eq!(rec.evidence.denominator, Some(10));
    assert_eq!(rec.evidence.metric_value, Some(1));
}

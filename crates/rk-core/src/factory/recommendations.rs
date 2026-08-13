use serde::{Deserialize, Serialize};

use super::{
    outcome_facts::{OutcomeEvidenceKind, SourceCounts},
    scorecards::{FactoryScorecard, ScorecardGroupKey},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecommendationId {
    LowAcceptance,
    HighRework,
    CiInstability,
    Reverts,
    HighCost,
    SlowLeadTime,
    HumanIntervention,
    Recurrence,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecommendationThresholds {
    pub min_runs: u32,
    pub min_metric_samples: u32,
    pub low_acceptance_peer_gap_pp: u32,
    pub high_rework_percent: u32,
    pub ci_unrecovered_percent: u32,
    pub revert_percent: u32,
    pub high_cost_micro_usd: u64,
    pub slow_median_lead_time_ms: u64,
    pub human_intervention_percent: u32,
    pub recurrence_percent: u32,
}

impl Default for RecommendationThresholds {
    fn default() -> Self {
        Self {
            min_runs: 10,
            min_metric_samples: 5,
            low_acceptance_peer_gap_pp: 20,
            high_rework_percent: 25,
            ci_unrecovered_percent: 50,
            revert_percent: 5,
            high_cost_micro_usd: 2_000_000,
            slow_median_lead_time_ms: 6 * 60 * 60 * 1000,
            human_intervention_percent: 40,
            recurrence_percent: 40,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecommendationEvidence {
    pub numerator: u64,
    pub denominator: u64,
    pub peer_numerator: Option<u64>,
    pub peer_denominator: Option<u64>,
    pub metric_value: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactoryRecommendation {
    pub id: RecommendationId,
    pub nature: &'static str,
    pub note: &'static str,
    pub task_class: String,
    pub thresholds: RecommendationThresholds,
    pub evidence: RecommendationEvidence,
    pub source_family: OutcomeEvidenceKind,
    pub source_counts: SourceCounts,
    pub suppressions: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvisoryRecommendationReport {
    pub nature: &'static str,
    pub recommendations: Vec<FactoryRecommendation>,
    pub suppressions: Vec<RecommendationSuppression>,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecommendationSuppression {
    pub id: RecommendationId,
    pub reason: &'static str,
    pub task_class: String,
    pub source_family: OutcomeEvidenceKind,
    pub source_counts: SourceCounts,
}

pub fn evaluate_recommendations(rows: &[FactoryScorecard]) -> Vec<FactoryRecommendation> {
    evaluate_recommendation_report(rows).recommendations
}

pub fn evaluate_recommendation_report(rows: &[FactoryScorecard]) -> AdvisoryRecommendationReport {
    let thresholds = RecommendationThresholds::default();
    let candidates: Vec<&FactoryScorecard> = rows
        .iter()
        .filter(|row| !row.projected && row.metrics.runs >= thresholds.min_runs)
        .collect();
    let mut report = AdvisoryRecommendationReport {
        nature: "advisory",
        recommendations: Vec::new(),
        suppressions: Vec::new(),
        warnings: Vec::new(),
    };

    for row in &candidates {
        push_low_acceptance(&mut report.recommendations, row, &candidates, &thresholds);
        push_rate(
            &mut report.recommendations,
            row,
            &thresholds,
            RecommendationId::HighRework,
            OutcomeEvidenceKind::StructuredReviewerRework,
            row.metrics.reworked,
            row.metrics.runs,
            thresholds.high_rework_percent,
            "advisory: rework rate is elevated for this observed class",
        );
        let unrecovered = row
            .metrics
            .ci_failed
            .saturating_sub(row.metrics.ci_recovered);
        push_rate(
            &mut report.recommendations,
            row,
            &thresholds,
            RecommendationId::CiInstability,
            OutcomeEvidenceKind::Phase4CiSignal,
            unrecovered,
            row.metrics.ci_failed,
            thresholds.ci_unrecovered_percent,
            "advisory: unrecovered CI failures are elevated",
        );
        push_rate(
            &mut report.recommendations,
            row,
            &thresholds,
            RecommendationId::Reverts,
            OutcomeEvidenceKind::StructuredRevert,
            row.metrics.reverted,
            row.metrics.runs,
            thresholds.revert_percent,
            "advisory: revert rate is elevated for this observed class",
        );
        push_value(
            &mut report,
            row,
            &thresholds,
            RecommendationId::HighCost,
            OutcomeEvidenceKind::PricingSnapshot,
            row.metrics.average_cost_micro_usd,
            row.metrics.cost_sample_size,
            thresholds.high_cost_micro_usd,
            "advisory: average cost is elevated for sampled runs",
        );
        push_value(
            &mut report,
            row,
            &thresholds,
            RecommendationId::SlowLeadTime,
            OutcomeEvidenceKind::AgentRecord,
            row.metrics.median_lead_time_ms,
            row.metrics.lead_time_sample_size,
            thresholds.slow_median_lead_time_ms,
            "advisory: median lead time is elevated for sampled runs",
        );
        push_rate_with_samples(
            &mut report.recommendations,
            row,
            &thresholds,
            RecommendationId::HumanIntervention,
            OutcomeEvidenceKind::HumanGateDecision,
            row.metrics.human_interventions,
            row.metrics.intervention_sample_size,
            thresholds.human_intervention_percent,
            "advisory: human intervention rate is elevated",
        );
        push_rate_with_samples(
            &mut report.recommendations,
            row,
            &thresholds,
            RecommendationId::Recurrence,
            OutcomeEvidenceKind::RecurrenceKey,
            row.metrics.recurrence_count,
            row.metrics.recurrence_sample_size,
            thresholds.recurrence_percent,
            "advisory: recurrence rate is elevated",
        );
    }

    report.recommendations.sort_by(|left, right| {
        (
            rule_order(left.id),
            &left.task_class,
            left.evidence.denominator,
            left.evidence.numerator,
        )
            .cmp(&(
                rule_order(right.id),
                &right.task_class,
                right.evidence.denominator,
                right.evidence.numerator,
            ))
    });
    report.suppressions.sort_by(|left, right| {
        (rule_order(left.id), &left.task_class).cmp(&(rule_order(right.id), &right.task_class))
    });
    report.warnings.sort();
    report
}

fn push_low_acceptance(
    out: &mut Vec<FactoryRecommendation>,
    row: &FactoryScorecard,
    rows: &[&FactoryScorecard],
    thresholds: &RecommendationThresholds,
) {
    if !available(row, OutcomeEvidenceKind::Phase3VerifiedDelivery) {
        return;
    }
    let Some(peer) = rows
        .iter()
        .copied()
        .filter(|peer| is_low_acceptance_peer(row, peer, thresholds))
        .max_by(|left, right| {
            ratio_cmp(
                left.metrics.accepted,
                left.metrics.runs,
                right.metrics.accepted,
                right.metrics.runs,
            )
        })
    else {
        return;
    };
    let target_scaled = u128::from(row.metrics.accepted) * 100 * u128::from(peer.metrics.runs);
    let peer_scaled = u128::from(peer.metrics.accepted) * 100 * u128::from(row.metrics.runs);
    let required_gap = u128::from(thresholds.low_acceptance_peer_gap_pp)
        * u128::from(row.metrics.runs)
        * u128::from(peer.metrics.runs);
    if peer_scaled >= target_scaled.saturating_add(required_gap) {
        out.push(recommendation(
            RecommendationId::LowAcceptance,
            row,
            thresholds,
            OutcomeEvidenceKind::Phase3VerifiedDelivery,
            RecommendationEvidence {
                numerator: u64::from(row.metrics.accepted),
                denominator: u64::from(row.metrics.runs),
                peer_numerator: Some(u64::from(peer.metrics.accepted)),
                peer_denominator: Some(u64::from(peer.metrics.runs)),
                metric_value: None,
            },
            "advisory: acceptance trails comparable observed peers",
        ));
    }
}

fn is_low_acceptance_peer(
    row: &FactoryScorecard,
    peer: &FactoryScorecard,
    thresholds: &RecommendationThresholds,
) -> bool {
    !std::ptr::eq(row, peer)
        && same_task_class_workflow(&row.group_key, &peer.group_key)
        && peer.metrics.runs >= thresholds.min_runs
        && available(peer, OutcomeEvidenceKind::Phase3VerifiedDelivery)
}

fn same_task_class_workflow(left: &ScorecardGroupKey, right: &ScorecardGroupKey) -> bool {
    left.task_class == right.task_class && left.workflow == right.workflow
}

fn push_rate(
    out: &mut Vec<FactoryRecommendation>,
    row: &FactoryScorecard,
    thresholds: &RecommendationThresholds,
    id: RecommendationId,
    family: OutcomeEvidenceKind,
    numerator: u32,
    denominator: u32,
    percent: u32,
    note: &'static str,
) {
    if denominator == 0 || !available(row, family) {
        return;
    }
    if ratio_at_least(numerator, denominator, percent) {
        out.push(recommendation(
            id,
            row,
            thresholds,
            family,
            RecommendationEvidence {
                numerator: u64::from(numerator),
                denominator: u64::from(denominator),
                peer_numerator: None,
                peer_denominator: None,
                metric_value: None,
            },
            note,
        ));
    }
}

fn push_rate_with_samples(
    out: &mut Vec<FactoryRecommendation>,
    row: &FactoryScorecard,
    thresholds: &RecommendationThresholds,
    id: RecommendationId,
    family: OutcomeEvidenceKind,
    numerator: u32,
    denominator: u32,
    percent: u32,
    note: &'static str,
) {
    if denominator < thresholds.min_metric_samples || !available(row, family) {
        return;
    }
    push_rate(
        out,
        row,
        thresholds,
        id,
        family,
        numerator,
        denominator,
        percent,
        note,
    );
}

fn push_value(
    report: &mut AdvisoryRecommendationReport,
    row: &FactoryScorecard,
    thresholds: &RecommendationThresholds,
    id: RecommendationId,
    family: OutcomeEvidenceKind,
    value: Option<u64>,
    samples: u32,
    limit: u64,
    note: &'static str,
) {
    if !available(row, family) {
        suppress(report, id, row, family, "metric_unavailable");
        return;
    }
    if samples < thresholds.min_metric_samples {
        suppress(report, id, row, family, "low_sample");
        return;
    }
    if let Some(value) = value.filter(|value| *value >= limit) {
        report.recommendations.push(recommendation(
            id,
            row,
            thresholds,
            family,
            RecommendationEvidence {
                numerator: value,
                denominator: u64::from(samples),
                peer_numerator: None,
                peer_denominator: None,
                metric_value: Some(value),
            },
            note,
        ));
    }
}

fn suppress(
    report: &mut AdvisoryRecommendationReport,
    id: RecommendationId,
    row: &FactoryScorecard,
    family: OutcomeEvidenceKind,
    reason: &'static str,
) {
    report.suppressions.push(RecommendationSuppression {
        id,
        reason,
        task_class: row.group_key.task_class.clone(),
        source_family: family,
        source_counts: row
            .source_counts
            .by_family
            .get(&family)
            .cloned()
            .unwrap_or_default(),
    });
    if reason == "metric_unavailable" {
        report
            .warnings
            .push(format!("metric_unavailable: {}", id_slug(id)));
    }
}

fn recommendation(
    id: RecommendationId,
    row: &FactoryScorecard,
    thresholds: &RecommendationThresholds,
    family: OutcomeEvidenceKind,
    evidence: RecommendationEvidence,
    note: &'static str,
) -> FactoryRecommendation {
    FactoryRecommendation {
        id,
        nature: "advisory",
        note,
        task_class: row.group_key.task_class.clone(),
        thresholds: thresholds.clone(),
        evidence,
        source_family: family,
        source_counts: row
            .source_counts
            .by_family
            .get(&family)
            .cloned()
            .unwrap_or_default(),
        suppressions: Vec::new(),
        warnings: Vec::new(),
    }
}

fn available(row: &FactoryScorecard, family: OutcomeEvidenceKind) -> bool {
    row.availability
        .by_family
        .get(&family)
        .is_some_and(|availability| availability.available)
}

fn ratio_at_least(numerator: u32, denominator: u32, percent: u32) -> bool {
    u128::from(numerator) * 100 >= u128::from(percent) * u128::from(denominator)
}

fn ratio_cmp(left_n: u32, left_d: u32, right_n: u32, right_d: u32) -> std::cmp::Ordering {
    (u128::from(left_n) * u128::from(right_d)).cmp(&(u128::from(right_n) * u128::from(left_d)))
}

fn rule_order(id: RecommendationId) -> u8 {
    match id {
        RecommendationId::LowAcceptance => 0,
        RecommendationId::HighRework => 1,
        RecommendationId::CiInstability => 2,
        RecommendationId::Reverts => 3,
        RecommendationId::HighCost => 4,
        RecommendationId::SlowLeadTime => 5,
        RecommendationId::HumanIntervention => 6,
        RecommendationId::Recurrence => 7,
    }
}

fn id_slug(id: RecommendationId) -> &'static str {
    match id {
        RecommendationId::LowAcceptance => "low_acceptance",
        RecommendationId::HighRework => "high_rework",
        RecommendationId::CiInstability => "ci_instability",
        RecommendationId::Reverts => "reverts",
        RecommendationId::HighCost => "high_cost",
        RecommendationId::SlowLeadTime => "slow_lead_time",
        RecommendationId::HumanIntervention => "human_intervention",
        RecommendationId::Recurrence => "recurrence",
    }
}

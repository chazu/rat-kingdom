use serde::{Deserialize, Serialize};

use super::{
    outcome_facts::{OutcomeEvidenceKind, SourceAvailability, SourceCounts},
    scorecards::{FactoryScorecard, ScorecardGroupKey},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecommendationRule {
    LowAcceptance,
    HighRework,
    CiInstability,
    Reverts,
    HighCost,
    SlowLeadTime,
    HumanIntervention,
    Recurrence,
}

pub type RecommendationId = RecommendationRule;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecommendationQuery {
    pub min_sample_size: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecommendationSeverity {
    Critical,
    Warning,
    Info,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuppressionReason {
    BelowThreshold,
    LowSample,
    MetricUnavailable,
    NoComparablePeer,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecommendationThresholds {
    pub min_sample_size: u32,
    pub acceptance_below_ratio: Option<(u32, u32)>,
    pub peer_acceptance_at_least_ratio: Option<(u32, u32)>,
    pub rework_at_least_ratio: Option<(u32, u32)>,
    pub ci_at_least_ratio: Option<(u32, u32)>,
    pub revert_at_least_ratio: Option<(u32, u32)>,
    pub cost_multiplier_ratio: Option<(u32, u32)>,
    pub cost_absolute_min_micro_usd: Option<u64>,
    pub lead_time_p95_multiplier_ratio: Option<(u32, u32)>,
    pub intervention_at_least_ratio: Option<(u32, u32)>,
    pub recurrence_min_count: Option<u32>,
}

impl RecommendationThresholds {
    fn for_rule(rule: RecommendationRule) -> Self {
        match rule {
            RecommendationRule::LowAcceptance => Self {
                min_sample_size: 10,
                acceptance_below_ratio: Some((60, 100)),
                peer_acceptance_at_least_ratio: Some((80, 100)),
                ..Self::empty()
            },
            RecommendationRule::HighRework => Self {
                min_sample_size: 10,
                rework_at_least_ratio: Some((25, 100)),
                ..Self::empty()
            },
            RecommendationRule::CiInstability => Self {
                min_sample_size: 8,
                ci_at_least_ratio: Some((15, 100)),
                ..Self::empty()
            },
            RecommendationRule::Reverts => Self {
                min_sample_size: 5,
                revert_at_least_ratio: Some((10, 100)),
                ..Self::empty()
            },
            RecommendationRule::HighCost => Self {
                min_sample_size: 8,
                cost_multiplier_ratio: Some((3, 2)),
                cost_absolute_min_micro_usd: Some(1),
                ..Self::empty()
            },
            RecommendationRule::SlowLeadTime => Self {
                min_sample_size: 8,
                lead_time_p95_multiplier_ratio: Some((3, 2)),
                ..Self::empty()
            },
            RecommendationRule::HumanIntervention => Self {
                min_sample_size: 8,
                intervention_at_least_ratio: Some((30, 100)),
                ..Self::empty()
            },
            RecommendationRule::Recurrence => Self {
                min_sample_size: 5,
                recurrence_min_count: Some(3),
                ..Self::empty()
            },
        }
    }

    fn empty() -> Self {
        Self {
            min_sample_size: 0,
            acceptance_below_ratio: None,
            peer_acceptance_at_least_ratio: None,
            rework_at_least_ratio: None,
            ci_at_least_ratio: None,
            revert_at_least_ratio: None,
            cost_multiplier_ratio: None,
            cost_absolute_min_micro_usd: None,
            lead_time_p95_multiplier_ratio: None,
            intervention_at_least_ratio: None,
            recurrence_min_count: None,
        }
    }
}

impl Default for RecommendationThresholds {
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecommendationEvidence {
    pub numerator: Option<u64>,
    pub denominator: Option<u64>,
    pub metric_value: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComparisonEvidence {
    pub peer_numerator: Option<u64>,
    pub peer_denominator: Option<u64>,
    pub comparable_median: Option<u64>,
    pub comparable_sample_size: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactoryRecommendation {
    pub id: String,
    pub severity: RecommendationSeverity,
    pub rule: RecommendationRule,
    pub subject_group_key: ScorecardGroupKey,
    pub summary: String,
    pub advice: Option<String>,
    pub thresholds: RecommendationThresholds,
    pub metric_availability: SourceAvailability,
    pub comparison_evidence: Option<ComparisonEvidence>,
    pub evidence: RecommendationEvidence,
    pub source_counts: SourceCounts,
    pub evidence_count: u32,
    pub source_count: u32,
    pub sample_size: u32,
    pub suppressed: bool,
    pub suppression_reason: Option<SuppressionReason>,
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
    pub rule: RecommendationRule,
    pub reason: SuppressionReason,
    pub subject_group_key: ScorecardGroupKey,
    pub source_family: OutcomeEvidenceKind,
    pub source_counts: SourceCounts,
}

pub fn evaluate_recommendations(rows: &[FactoryScorecard]) -> Vec<FactoryRecommendation> {
    evaluate_recommendation_report(rows).recommendations
}

pub fn evaluate_recommendation_report(rows: &[FactoryScorecard]) -> AdvisoryRecommendationReport {
    let observed: Vec<&FactoryScorecard> = rows
        .iter()
        .filter(|row| !row.projected && row.metrics.runs > 0)
        .collect();
    let mut report = AdvisoryRecommendationReport {
        nature: "advisory",
        recommendations: Vec::new(),
        suppressions: Vec::new(),
        warnings: Vec::new(),
    };
    for row in &observed {
        eval_low_acceptance(&mut report, row, &observed);
        eval_rate(
            &mut report,
            row,
            RateRuleInput::new(
                RecommendationRule::HighRework,
                OutcomeEvidenceKind::StructuredReviewerRework,
                row.metrics.reworked,
                row.metrics.rework_sample_size,
                10,
                (25, 100),
            ),
        );
        eval_ci(&mut report, row, &observed);
        eval_rate(
            &mut report,
            row,
            RateRuleInput::new(
                RecommendationRule::Reverts,
                OutcomeEvidenceKind::StructuredRevert,
                row.metrics.reverted,
                row.metrics.revert_sample_size,
                5,
                (10, 100),
            ),
        );
        eval_cost(&mut report, row, &observed);
        eval_lead(&mut report, row, &observed);
        eval_rate(
            &mut report,
            row,
            RateRuleInput::new(
                RecommendationRule::HumanIntervention,
                OutcomeEvidenceKind::HumanGateDecision,
                row.metrics.human_interventions,
                row.metrics.intervention_sample_size,
                8,
                (30, 100),
            ),
        );
        eval_recurrence(&mut report, row);
    }
    report
        .recommendations
        .sort_by(|a, b| sort_key(a).cmp(&sort_key(b)));
    report.suppressions.sort_by(|a, b| {
        (rule_order(a.rule), &a.subject_group_key).cmp(&(rule_order(b.rule), &b.subject_group_key))
    });
    report.warnings.sort();
    report
}

fn eval_low_acceptance(
    report: &mut AdvisoryRecommendationReport,
    row: &FactoryScorecard,
    rows: &[&FactoryScorecard],
) {
    let rule = RecommendationRule::LowAcceptance;
    let family = OutcomeEvidenceKind::Phase3VerifiedDelivery;
    if !is_available(row, family) {
        push_suppressed(
            report,
            row,
            rule,
            family,
            SuppressionReason::MetricUnavailable,
            evidence(row.metrics.accepted, row.metrics.accepted_sample_size, None),
            None,
        );
        return;
    }
    if row.metrics.accepted_sample_size < 10 {
        push_suppressed(
            report,
            row,
            rule,
            family,
            SuppressionReason::LowSample,
            evidence(row.metrics.accepted, row.metrics.accepted_sample_size, None),
            None,
        );
        return;
    }
    if has_unknown_comparable_dimension(row) {
        push_suppressed(
            report,
            row,
            rule,
            family,
            SuppressionReason::NoComparablePeer,
            evidence(row.metrics.accepted, row.metrics.accepted_sample_size, None),
            None,
        );
        return;
    }
    let peer = rows
        .iter()
        .copied()
        .filter(|p| {
            !std::ptr::eq(*p, row)
                && same_group(row, p)
                && is_available(p, family)
                && p.metrics.accepted_sample_size >= 10
                && at_least(p.metrics.accepted, p.metrics.accepted_sample_size, 80, 100)
        })
        .max_by(|a, b| {
            ratio_cmp(
                a.metrics.accepted,
                a.metrics.accepted_sample_size,
                b.metrics.accepted,
                b.metrics.accepted_sample_size,
            )
        });
    let comparison = peer.map(|p| ComparisonEvidence {
        peer_numerator: Some(p.metrics.accepted.into()),
        peer_denominator: Some(p.metrics.accepted_sample_size.into()),
        comparable_median: None,
        comparable_sample_size: 1,
    });
    if !below(
        row.metrics.accepted,
        row.metrics.accepted_sample_size,
        60,
        100,
    ) || peer.is_none()
    {
        push_suppressed(
            report,
            row,
            rule,
            family,
            if peer.is_none() {
                SuppressionReason::NoComparablePeer
            } else {
                SuppressionReason::BelowThreshold
            },
            evidence(row.metrics.accepted, row.metrics.accepted_sample_size, None),
            comparison,
        );
        return;
    }
    push_active(
        report,
        row,
        rule,
        family,
        evidence(row.metrics.accepted, row.metrics.accepted_sample_size, None),
        comparison,
    );
}

fn eval_ci(
    report: &mut AdvisoryRecommendationReport,
    row: &FactoryScorecard,
    rows: &[&FactoryScorecard],
) {
    let rule = RecommendationRule::CiInstability;
    let family = OutcomeEvidenceKind::Phase4CiSignal;
    let n = row.metrics.ci_sample_size;
    if !is_available(row, family) {
        push_suppressed(
            report,
            row,
            rule,
            family,
            SuppressionReason::MetricUnavailable,
            evidence(
                row.metrics.ci_failed,
                n,
                Some(row.metrics.ci_recovered.into()),
            ),
            None,
        );
        return;
    }
    if n < 8 {
        push_suppressed(
            report,
            row,
            rule,
            family,
            SuppressionReason::LowSample,
            evidence(
                row.metrics.ci_failed,
                n,
                Some(row.metrics.ci_recovered.into()),
            ),
            None,
        );
        return;
    }
    if has_unknown_comparable_dimension(row) {
        push_suppressed(
            report,
            row,
            rule,
            family,
            SuppressionReason::NoComparablePeer,
            evidence(
                row.metrics.ci_failed,
                n,
                Some(row.metrics.ci_recovered.into()),
            ),
            None,
        );
        return;
    }
    let median = comparable_median(rows, row, family, |p| {
        Some(rate_per_million(
            p.metrics.ci_failed,
            p.metrics.ci_sample_size,
        ))
        .filter(|_| p.metrics.ci_sample_size >= 8)
    });
    let comp = median.map(|m| ComparisonEvidence {
        peer_numerator: None,
        peer_denominator: None,
        comparable_median: Some(m),
        comparable_sample_size: comparable_count(rows, row, family),
    });
    if at_least(row.metrics.ci_failed, n, 15, 100)
        && median.is_some_and(|m| rate_per_million(row.metrics.ci_failed, n) > m)
    {
        push_active(
            report,
            row,
            rule,
            family,
            evidence(
                row.metrics.ci_failed,
                n,
                Some(row.metrics.ci_recovered.into()),
            ),
            comp,
        );
    } else {
        push_suppressed(
            report,
            row,
            rule,
            family,
            SuppressionReason::BelowThreshold,
            evidence(
                row.metrics.ci_failed,
                n,
                Some(row.metrics.ci_recovered.into()),
            ),
            comp,
        );
    }
}

struct RateRuleInput {
    rule: RecommendationRule,
    family: OutcomeEvidenceKind,
    numerator: u32,
    denominator: u32,
    min_sample_size: u32,
    threshold: (u32, u32),
}

impl RateRuleInput {
    fn new(
        rule: RecommendationRule,
        family: OutcomeEvidenceKind,
        numerator: u32,
        denominator: u32,
        min_sample_size: u32,
        threshold: (u32, u32),
    ) -> Self {
        Self {
            rule,
            family,
            numerator,
            denominator,
            min_sample_size,
            threshold,
        }
    }
}

fn eval_rate(
    report: &mut AdvisoryRecommendationReport,
    row: &FactoryScorecard,
    input: RateRuleInput,
) {
    if !is_available(row, input.family) {
        push_suppressed(
            report,
            row,
            input.rule,
            input.family,
            SuppressionReason::MetricUnavailable,
            evidence(input.numerator, input.denominator, None),
            None,
        );
        return;
    }
    if input.denominator < input.min_sample_size {
        push_suppressed(
            report,
            row,
            input.rule,
            input.family,
            SuppressionReason::LowSample,
            evidence(input.numerator, input.denominator, None),
            None,
        );
        return;
    }
    if at_least(
        input.numerator,
        input.denominator,
        input.threshold.0,
        input.threshold.1,
    ) {
        push_active(
            report,
            row,
            input.rule,
            input.family,
            evidence(input.numerator, input.denominator, None),
            None,
        );
    } else {
        push_suppressed(
            report,
            row,
            input.rule,
            input.family,
            SuppressionReason::BelowThreshold,
            evidence(input.numerator, input.denominator, None),
            None,
        );
    }
}

fn eval_cost(
    report: &mut AdvisoryRecommendationReport,
    row: &FactoryScorecard,
    rows: &[&FactoryScorecard],
) {
    let rule = RecommendationRule::HighCost;
    let family = OutcomeEvidenceKind::PricingSnapshot;
    let ev = RecommendationEvidence {
        numerator: None,
        denominator: Some(row.metrics.cost_sample_size.into()),
        metric_value: row.metrics.average_cost_micro_usd,
    };
    if !is_available(row, family) {
        push_suppressed(
            report,
            row,
            rule,
            family,
            SuppressionReason::MetricUnavailable,
            ev,
            None,
        );
        return;
    }
    if row.metrics.cost_sample_size < 8 {
        push_suppressed(
            report,
            row,
            rule,
            family,
            SuppressionReason::LowSample,
            ev,
            None,
        );
        return;
    }
    if has_unknown_comparable_dimension(row) {
        push_suppressed(
            report,
            row,
            rule,
            family,
            SuppressionReason::NoComparablePeer,
            ev,
            None,
        );
        return;
    }
    let median = comparable_median(rows, row, family, |p| {
        p.metrics
            .average_cost_micro_usd
            .filter(|_| p.metrics.cost_sample_size >= 8)
    });
    let comp = median.map(|m| ComparisonEvidence {
        peer_numerator: None,
        peer_denominator: None,
        comparable_median: Some(m),
        comparable_sample_size: comparable_count(rows, row, family),
    });
    let value = row.metrics.average_cost_micro_usd.unwrap_or(0);
    if value >= 1 && median.is_some_and(|m| u128::from(value) * 2 >= u128::from(m) * 3) {
        push_active(report, row, rule, family, ev, comp);
    } else {
        push_suppressed(
            report,
            row,
            rule,
            family,
            if median.is_none() {
                SuppressionReason::NoComparablePeer
            } else {
                SuppressionReason::BelowThreshold
            },
            ev,
            comp,
        );
    }
}

fn eval_lead(
    report: &mut AdvisoryRecommendationReport,
    row: &FactoryScorecard,
    rows: &[&FactoryScorecard],
) {
    let rule = RecommendationRule::SlowLeadTime;
    let family = OutcomeEvidenceKind::AgentRecord;
    let ev = RecommendationEvidence {
        numerator: None,
        denominator: Some(row.metrics.lead_time_sample_size.into()),
        metric_value: row.metrics.p95_lead_time_ms,
    };
    if !is_available(row, family) {
        push_suppressed(
            report,
            row,
            rule,
            family,
            SuppressionReason::MetricUnavailable,
            ev,
            None,
        );
        return;
    }
    if row.metrics.lead_time_sample_size < 8 {
        push_suppressed(
            report,
            row,
            rule,
            family,
            SuppressionReason::LowSample,
            ev,
            None,
        );
        return;
    }
    if has_unknown_comparable_dimension(row) {
        push_suppressed(
            report,
            row,
            rule,
            family,
            SuppressionReason::NoComparablePeer,
            ev,
            None,
        );
        return;
    }
    let median = comparable_median(rows, row, family, |p| {
        p.metrics
            .p95_lead_time_ms
            .filter(|_| p.metrics.lead_time_sample_size >= 8)
    });
    let comp = median.map(|m| ComparisonEvidence {
        peer_numerator: None,
        peer_denominator: None,
        comparable_median: Some(m),
        comparable_sample_size: comparable_count(rows, row, family),
    });
    let value = row.metrics.p95_lead_time_ms.unwrap_or(0);
    if median.is_some_and(|m| u128::from(value) * 2 >= u128::from(m) * 3) {
        push_active(report, row, rule, family, ev, comp);
    } else {
        push_suppressed(
            report,
            row,
            rule,
            family,
            if median.is_none() {
                SuppressionReason::NoComparablePeer
            } else {
                SuppressionReason::BelowThreshold
            },
            ev,
            comp,
        );
    }
}

fn eval_recurrence(report: &mut AdvisoryRecommendationReport, row: &FactoryScorecard) {
    let rule = RecommendationRule::Recurrence;
    let family = OutcomeEvidenceKind::RecurrenceKey;
    if !is_available(row, family) {
        push_suppressed(
            report,
            row,
            rule,
            family,
            SuppressionReason::MetricUnavailable,
            evidence(
                row.metrics.recurrence_count,
                row.metrics.recurrence_sample_size,
                None,
            ),
            None,
        );
        return;
    }
    if row.metrics.recurrence_sample_size < 5 {
        push_suppressed(
            report,
            row,
            rule,
            family,
            SuppressionReason::LowSample,
            evidence(
                row.metrics.recurrence_count,
                row.metrics.recurrence_sample_size,
                None,
            ),
            None,
        );
        return;
    }
    if row.metrics.recurrence_count >= 3 {
        push_active(
            report,
            row,
            rule,
            family,
            evidence(
                row.metrics.recurrence_count,
                row.metrics.recurrence_sample_size,
                None,
            ),
            None,
        );
    } else {
        push_suppressed(
            report,
            row,
            rule,
            family,
            SuppressionReason::BelowThreshold,
            evidence(
                row.metrics.recurrence_count,
                row.metrics.recurrence_sample_size,
                None,
            ),
            None,
        );
    }
}

fn push_active(
    report: &mut AdvisoryRecommendationReport,
    row: &FactoryScorecard,
    rule: RecommendationRule,
    family: OutcomeEvidenceKind,
    ev: RecommendationEvidence,
    comp: Option<ComparisonEvidence>,
) {
    report
        .recommendations
        .push(make_rec(row, rule, family, ev, comp, false, None));
}
fn push_suppressed(
    report: &mut AdvisoryRecommendationReport,
    row: &FactoryScorecard,
    rule: RecommendationRule,
    family: OutcomeEvidenceKind,
    reason: SuppressionReason,
    ev: RecommendationEvidence,
    comp: Option<ComparisonEvidence>,
) {
    if reason == SuppressionReason::MetricUnavailable {
        report
            .warnings
            .push(format!("metric_unavailable: {}", rule_slug(rule)));
    }
    report.suppressions.push(RecommendationSuppression {
        rule,
        reason,
        subject_group_key: row.group_key.clone(),
        source_family: family,
        source_counts: source_counts(row, family),
    });
    report
        .recommendations
        .push(make_rec(row, rule, family, ev, comp, true, Some(reason)));
}

fn make_rec(
    row: &FactoryScorecard,
    rule: RecommendationRule,
    family: OutcomeEvidenceKind,
    evidence: RecommendationEvidence,
    comparison_evidence: Option<ComparisonEvidence>,
    suppressed: bool,
    suppression_reason: Option<SuppressionReason>,
) -> FactoryRecommendation {
    let source_counts = source_counts(row, family);
    FactoryRecommendation {
        id: format!(
            "{}:{}:{}:{}:{}:{}",
            rule_slug(rule),
            row.group_key.task_class,
            row.group_key.workflow,
            row.group_key.harness,
            row.group_key.model,
            if suppressed { "suppressed" } else { "active" }
        ),
        severity: severity(rule),
        rule,
        subject_group_key: row.group_key.clone(),
        summary: summary(rule).into(),
        advice: (!suppressed).then(|| advice(rule).into()),
        thresholds: RecommendationThresholds::for_rule(rule),
        metric_availability: availability(row, family),
        comparison_evidence,
        evidence,
        evidence_count: source_counts.event_count,
        source_count: source_counts.active_source_count + source_counts.archived_source_count,
        source_counts,
        sample_size: row.sample_size,
        suppressed,
        suppression_reason,
    }
}

fn evidence(num: u32, den: u32, metric: Option<u64>) -> RecommendationEvidence {
    RecommendationEvidence {
        numerator: Some(num.into()),
        denominator: Some(den.into()),
        metric_value: metric,
    }
}
fn source_counts(row: &FactoryScorecard, family: OutcomeEvidenceKind) -> SourceCounts {
    row.source_counts
        .by_family
        .get(&family)
        .cloned()
        .unwrap_or_default()
}
fn availability(row: &FactoryScorecard, family: OutcomeEvidenceKind) -> SourceAvailability {
    row.availability
        .by_family
        .get(&family)
        .cloned()
        .unwrap_or(SourceAvailability {
            source_family: family,
            available: false,
        })
}
fn is_available(row: &FactoryScorecard, family: OutcomeEvidenceKind) -> bool {
    row.availability
        .by_family
        .get(&family)
        .is_some_and(|a| a.available)
}
fn same_group(a: &FactoryScorecard, b: &FactoryScorecard) -> bool {
    a.group_key.task_class == b.group_key.task_class && a.group_key.workflow == b.group_key.workflow
}
fn has_unknown_comparable_dimension(row: &FactoryScorecard) -> bool {
    row.group_key.task_class == "unknown" || row.group_key.workflow == "unknown"
}
fn at_least(n: u32, d: u32, tn: u32, td: u32) -> bool {
    d > 0 && u128::from(n) * u128::from(td) >= u128::from(tn) * u128::from(d)
}
fn below(n: u32, d: u32, tn: u32, td: u32) -> bool {
    d > 0 && u128::from(n) * u128::from(td) < u128::from(tn) * u128::from(d)
}
fn rate_per_million(n: u32, d: u32) -> u64 {
    if d == 0 {
        0
    } else {
        ((u128::from(n) * 1_000_000) / u128::from(d)) as u64
    }
}
fn ratio_cmp(an: u32, ad: u32, bn: u32, bd: u32) -> std::cmp::Ordering {
    (u128::from(an) * u128::from(bd)).cmp(&(u128::from(bn) * u128::from(ad)))
}
fn comparable_median(
    rows: &[&FactoryScorecard],
    row: &FactoryScorecard,
    family: OutcomeEvidenceKind,
    value: impl Fn(&FactoryScorecard) -> Option<u64>,
) -> Option<u64> {
    let mut vals: Vec<_> = rows
        .iter()
        .copied()
        .filter(|p| !std::ptr::eq(*p, row) && same_group(row, p) && is_available(p, family))
        .filter_map(value)
        .collect();
    vals.sort();
    vals.get(vals.len().checked_sub(1)? / 2).copied()
}
fn comparable_count(
    rows: &[&FactoryScorecard],
    row: &FactoryScorecard,
    family: OutcomeEvidenceKind,
) -> u32 {
    rows.iter()
        .copied()
        .filter(|p| !std::ptr::eq(*p, row) && same_group(row, p) && is_available(p, family))
        .count() as u32
}
fn sort_key(
    rec: &FactoryRecommendation,
) -> (
    RecommendationSeverity,
    u8,
    &String,
    &String,
    &String,
    &String,
    &String,
) {
    (
        rec.severity,
        rule_order(rec.rule),
        &rec.subject_group_key.task_class,
        &rec.subject_group_key.workflow,
        &rec.subject_group_key.harness,
        &rec.subject_group_key.model,
        &rec.id,
    )
}
fn rule_order(rule: RecommendationRule) -> u8 {
    match rule {
        RecommendationRule::LowAcceptance => 0,
        RecommendationRule::HighRework => 1,
        RecommendationRule::CiInstability => 2,
        RecommendationRule::Reverts => 3,
        RecommendationRule::HighCost => 4,
        RecommendationRule::SlowLeadTime => 5,
        RecommendationRule::HumanIntervention => 6,
        RecommendationRule::Recurrence => 7,
    }
}
fn severity(rule: RecommendationRule) -> RecommendationSeverity {
    match rule {
        RecommendationRule::LowAcceptance | RecommendationRule::Reverts => {
            RecommendationSeverity::Critical
        }
        RecommendationRule::HighRework
        | RecommendationRule::CiInstability
        | RecommendationRule::HighCost
        | RecommendationRule::SlowLeadTime => RecommendationSeverity::Warning,
        RecommendationRule::HumanIntervention | RecommendationRule::Recurrence => {
            RecommendationSeverity::Info
        }
    }
}
fn rule_slug(rule: RecommendationRule) -> &'static str {
    match rule {
        RecommendationRule::LowAcceptance => "low_acceptance",
        RecommendationRule::HighRework => "high_rework",
        RecommendationRule::CiInstability => "ci_instability",
        RecommendationRule::Reverts => "reverts",
        RecommendationRule::HighCost => "high_cost",
        RecommendationRule::SlowLeadTime => "slow_lead_time",
        RecommendationRule::HumanIntervention => "human_intervention",
        RecommendationRule::Recurrence => "recurrence",
    }
}
fn summary(rule: RecommendationRule) -> &'static str {
    match rule {
        RecommendationRule::LowAcceptance => "acceptance is below comparable peers",
        RecommendationRule::HighRework => "rework is elevated",
        RecommendationRule::CiInstability => "unrecovered CI failures are elevated",
        RecommendationRule::Reverts => "reverts are elevated",
        RecommendationRule::HighCost => "cost is elevated versus comparable median",
        RecommendationRule::SlowLeadTime => "p95 lead time is elevated versus comparable median",
        RecommendationRule::HumanIntervention => "human intervention rate is elevated",
        RecommendationRule::Recurrence => "recurrence count is elevated",
    }
}
fn advice(rule: RecommendationRule) -> &'static str {
    match rule {
        RecommendationRule::LowAcceptance => "Review acceptance evidence against comparable peers.",
        RecommendationRule::HighRework => "Review rework evidence for repeated reviewer findings.",
        RecommendationRule::CiInstability => "Review CI failure evidence and recovery patterns.",
        RecommendationRule::Reverts => "Review revert evidence for preventable regressions.",
        RecommendationRule::HighCost => "Review cost evidence against comparable medians.",
        RecommendationRule::SlowLeadTime => "Review lead time evidence against comparable medians.",
        RecommendationRule::HumanIntervention => "Review human gate evidence for automation gaps.",
        RecommendationRule::Recurrence => "Review recurrence evidence for repeated outcomes.",
    }
}

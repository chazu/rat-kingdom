use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::outcome_facts::{
    OutcomeEvidenceKind, OutcomeFact, OutcomeFactGroupKey, OutcomeMetricKind, OutcomeStatus,
    SourceCounts,
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScorecardQuery {
    pub include_archived: bool,
    pub projections: Vec<ScorecardProjection>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ScorecardProjection {
    Composite,
    TaskClass,
    Workflow,
    Harness,
    Model,
    TaskClassWorkflow,
    All,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ScorecardGroupKey {
    pub task_class: String,
    pub workflow: String,
    pub harness: String,
    pub model: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScorecardMetrics {
    pub runs: u32,
    pub accepted: u32,
    pub reworked: u32,
    pub ci_failed: u32,
    pub ci_recovered: u32,
    pub reverted: u32,
    pub unknown: u32,
    pub unobserved: u32,
    pub active_runs: u32,
    pub archived_runs: u32,
    pub total_cost_micro_usd: u64,
    pub average_cost_micro_usd: Option<u64>,
    pub cost_sample_size: u32,
    pub median_lead_time_ms: Option<u64>,
    pub p95_lead_time_ms: Option<u64>,
    pub lead_time_sample_size: u32,
    pub human_interventions: u32,
    pub intervention_sample_size: u32,
    pub recurrence_count: u32,
    pub distinct_recurrence_keys: u32,
    pub recurrence_sample_size: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScorecardEvidenceCounts {
    pub by_kind: BTreeMap<OutcomeEvidenceKind, u32>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScorecardSourceCounts {
    pub by_family: BTreeMap<OutcomeEvidenceKind, SourceCounts>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricAvailability {
    pub by_family: BTreeMap<OutcomeEvidenceKind, super::outcome_facts::SourceAvailability>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactoryScorecard {
    pub group_key: ScorecardGroupKey,
    pub projection: ScorecardProjection,
    pub projected: bool,
    pub metrics: ScorecardMetrics,
    pub status_counts: BTreeMap<OutcomeStatus, u32>,
    pub evidence_counts: ScorecardEvidenceCounts,
    pub source_counts: ScorecardSourceCounts,
    pub availability: MetricAvailability,
    pub sample_size: u32,
    pub metric_sort_key: String,
}

#[derive(Default)]
struct Accumulator {
    row: FactoryScorecard,
    costs: Vec<u64>,
    lead_times: Vec<u64>,
    recurrence_keys: Vec<(ScorecardGroupKey, String)>,
    active_sources: BTreeMap<OutcomeEvidenceKind, BTreeSet<SourceDedupeKey>>,
    archived_sources: BTreeMap<OutcomeEvidenceKind, BTreeSet<SourceDedupeKey>>,
    metadata_source_counts: BTreeMap<OutcomeEvidenceKind, SourceCounts>,
}

type SourceDedupeKey = (OutcomeEvidenceKind, String, String);

impl Default for FactoryScorecard {
    fn default() -> Self {
        Self {
            group_key: ScorecardGroupKey::default(),
            projection: ScorecardProjection::Composite,
            projected: false,
            metrics: ScorecardMetrics::default(),
            status_counts: BTreeMap::new(),
            evidence_counts: ScorecardEvidenceCounts::default(),
            source_counts: ScorecardSourceCounts::default(),
            availability: MetricAvailability::default(),
            sample_size: 0,
            metric_sort_key: String::new(),
        }
    }
}

pub fn aggregate_scorecards(facts: &[OutcomeFact], query: ScorecardQuery) -> Vec<FactoryScorecard> {
    let mut accs: BTreeMap<(ScorecardGroupKey, ScorecardProjection), Accumulator> = BTreeMap::new();
    let mut projections = vec![ScorecardProjection::Composite];
    projections.extend(query.projections);
    projections.sort();
    projections.dedup();

    for fact in facts {
        for projection in &projections {
            let key = project_key(&fact.group_key, projection);
            let acc = accs
                .entry((key.clone(), projection.clone()))
                .or_insert_with(|| {
                    let mut acc = Accumulator::default();
                    acc.row.group_key = key.clone();
                    acc.row.projection = projection.clone();
                    acc.row.projected = projection != &ScorecardProjection::Composite;
                    acc.row.metric_sort_key = metric_sort_key(projection);
                    acc
                });
            add_fact(acc, fact, query.include_archived);
        }
    }

    let mut rows: Vec<_> = accs.into_values().map(finalize).collect();
    rows.sort_by(|l, r| {
        (&l.group_key, &l.projection, &l.metric_sort_key).cmp(&(
            &r.group_key,
            &r.projection,
            &r.metric_sort_key,
        ))
    });
    rows
}

fn add_fact(acc: &mut Accumulator, fact: &OutcomeFact, include_archived: bool) {
    if fact.availability.available {
        *acc.row
            .evidence_counts
            .by_kind
            .entry(fact.evidence_kind)
            .or_default() += 1;
    }
    acc.row
        .availability
        .by_family
        .entry(fact.availability.source_family)
        .and_modify(|availability| {
            availability.available |= fact.availability.available;
        })
        .or_insert_with(|| fact.availability.clone());
    let counts = acc
        .row
        .source_counts
        .by_family
        .entry(fact.evidence_kind)
        .or_default();
    counts.event_count = counts.event_count.saturating_add(1);
    if !fact.availability.available {
        let metadata_counts = acc
            .metadata_source_counts
            .entry(fact.evidence_kind)
            .or_default();
        metadata_counts.active_source_count = metadata_counts
            .active_source_count
            .saturating_add(fact.source_counts.active_source_count);
        metadata_counts.archived_source_count = metadata_counts
            .archived_source_count
            .saturating_add(fact.source_counts.archived_source_count);
        metadata_counts.event_count = metadata_counts
            .event_count
            .saturating_add(fact.source_counts.event_count);
        counts.event_count = counts
            .event_count
            .saturating_add(fact.source_counts.event_count.saturating_sub(1));
    }
    if fact.availability.available {
        let source_key = source_dedupe_key(fact);
        if fact.archived {
            acc.archived_sources
                .entry(fact.evidence_kind)
                .or_default()
                .insert(source_key);
        } else {
            acc.active_sources
                .entry(fact.evidence_kind)
                .or_default()
                .insert(source_key);
        }
    }

    if fact.archived && !include_archived {
        return;
    }

    if is_explicit_run_fact(fact) {
        acc.row.metrics.runs += 1;
        acc.row.sample_size += 1;
        if fact.archived {
            acc.row.metrics.archived_runs += 1;
        } else {
            acc.row.metrics.active_runs += 1;
        }
    }
    if contributes_status(fact) {
        *acc.row
            .status_counts
            .entry(fact.status.clone())
            .or_default() += 1;
        match fact.status {
            OutcomeStatus::Accepted => acc.row.metrics.accepted += 1,
            OutcomeStatus::Reworked => acc.row.metrics.reworked += 1,
            OutcomeStatus::CiFailed => acc.row.metrics.ci_failed += 1,
            OutcomeStatus::CiRecovered => acc.row.metrics.ci_recovered += 1,
            OutcomeStatus::Reverted => acc.row.metrics.reverted += 1,
            OutcomeStatus::Unknown => acc.row.metrics.unknown += 1,
            OutcomeStatus::Unobserved => acc.row.metrics.unobserved += 1,
        }
    }
    if fact.evidence_kind == OutcomeEvidenceKind::AgentRecord {
        if let Some(cost) = fact.cost_micro_usd {
            acc.costs.push(cost);
        }
    }
    if let Some(ms) = fact.lead_time_ms {
        acc.lead_times.push(ms);
    }
    if fact.human_interventions > 0 {
        acc.row.metrics.human_interventions = acc
            .row
            .metrics
            .human_interventions
            .saturating_add(fact.human_interventions);
        acc.row.metrics.intervention_sample_size += 1;
    }
    if let Some(key) = fact.recurrence_key.as_ref().filter(|key| !key.is_empty()) {
        acc.recurrence_keys.push((
            project_key(&fact.group_key, &ScorecardProjection::Composite),
            key.clone(),
        ));
        acc.row.metrics.recurrence_sample_size += 1;
    }
}

fn source_dedupe_key(fact: &OutcomeFact) -> SourceDedupeKey {
    (
        fact.evidence_kind,
        fact.source
            .repo
            .as_deref()
            .unwrap_or(fact.repo.as_str())
            .to_string(),
        fact.source.source_id.clone(),
    )
}

fn is_explicit_run_fact(fact: &OutcomeFact) -> bool {
    fact.evidence_kind == OutcomeEvidenceKind::AgentRecord
        && fact.metric_kind == OutcomeMetricKind::Run
}

fn contributes_status(fact: &OutcomeFact) -> bool {
    if fact.status == OutcomeStatus::Unobserved {
        return true;
    }
    matches!(
        fact.metric_kind,
        OutcomeMetricKind::Accepted
            | OutcomeMetricKind::Reworked
            | OutcomeMetricKind::Ci
            | OutcomeMetricKind::Reverted
    )
}

fn finalize(mut acc: Accumulator) -> FactoryScorecard {
    acc.costs.sort_unstable();
    acc.lead_times.sort_unstable();
    acc.recurrence_keys.sort();
    for (kind, sources) in acc.active_sources {
        let counts = acc
            .row
            .source_counts
            .by_family
            .entry(kind)
            .or_default();
        counts.active_source_count = counts
            .active_source_count
            .max(sources.len().try_into().unwrap_or(u32::MAX));
    }
    for (kind, sources) in acc.archived_sources {
        let counts = acc
            .row
            .source_counts
            .by_family
            .entry(kind)
            .or_default();
        counts.archived_source_count = counts
            .archived_source_count
            .max(sources.len().try_into().unwrap_or(u32::MAX));
    }
    for (kind, metadata_counts) in acc.metadata_source_counts {
        let counts = acc
            .row
            .source_counts
            .by_family
            .entry(kind)
            .or_default();
        counts.active_source_count = counts
            .active_source_count
            .max(metadata_counts.active_source_count);
        counts.archived_source_count = counts
            .archived_source_count
            .max(metadata_counts.archived_source_count);
        counts.event_count = counts.event_count.max(metadata_counts.event_count);
    }
    let total_cost: u128 = acc.costs.iter().map(|cost| u128::from(*cost)).sum();
    acc.row.metrics.total_cost_micro_usd = total_cost.min(u128::from(u64::MAX)) as u64;
    acc.row.metrics.cost_sample_size = acc.costs.len() as u32;
    acc.row.metrics.average_cost_micro_usd = if acc.costs.is_empty() {
        None
    } else {
        Some(div_round_half_away(total_cost, acc.costs.len() as u128))
    };
    acc.row.metrics.lead_time_sample_size = acc.lead_times.len() as u32;
    acc.row.metrics.median_lead_time_ms = nearest_rank(&acc.lead_times, 50, 100);
    acc.row.metrics.p95_lead_time_ms = nearest_rank(&acc.lead_times, 95, 100);

    let mut counts: BTreeMap<(ScorecardGroupKey, String), u32> = BTreeMap::new();
    for key in acc.recurrence_keys {
        *counts.entry(key).or_default() += 1;
    }
    let repeated: BTreeMap<_, _> = counts
        .into_iter()
        .filter(|(_, count)| *count >= 2)
        .collect();
    acc.row.metrics.distinct_recurrence_keys = repeated.len() as u32;
    acc.row.metrics.recurrence_count = repeated.values().sum();
    acc.row
}

fn div_round_half_away(total: u128, denom: u128) -> u64 {
    let rounded = total
        .saturating_add(denom / 2)
        .checked_div(denom)
        .unwrap_or(u128::from(u64::MAX));
    rounded.min(u128::from(u64::MAX)) as u64
}

fn nearest_rank(values: &[u64], numerator: usize, denominator: usize) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    let n = values.len();
    let rank = (numerator * n).div_ceil(denominator).max(1);
    values.get(rank - 1).copied()
}

fn project_key(key: &OutcomeFactGroupKey, projection: &ScorecardProjection) -> ScorecardGroupKey {
    let composite = ScorecardGroupKey {
        task_class: dim(&key.task_class),
        workflow: dim(&key.workflow),
        harness: dim(&key.harness),
        model: dim(&key.model),
    };
    match projection {
        ScorecardProjection::Composite => composite,
        ScorecardProjection::TaskClass => ScorecardGroupKey {
            task_class: composite.task_class,
            workflow: "*".into(),
            harness: "*".into(),
            model: "*".into(),
        },
        ScorecardProjection::Workflow => ScorecardGroupKey {
            task_class: "*".into(),
            workflow: composite.workflow,
            harness: "*".into(),
            model: "*".into(),
        },
        ScorecardProjection::Harness => ScorecardGroupKey {
            task_class: "*".into(),
            workflow: "*".into(),
            harness: composite.harness,
            model: "*".into(),
        },
        ScorecardProjection::Model => ScorecardGroupKey {
            task_class: "*".into(),
            workflow: "*".into(),
            harness: "*".into(),
            model: composite.model,
        },
        ScorecardProjection::TaskClassWorkflow => ScorecardGroupKey {
            task_class: composite.task_class,
            workflow: composite.workflow,
            harness: "*".into(),
            model: "*".into(),
        },
        ScorecardProjection::All => ScorecardGroupKey {
            task_class: "*".into(),
            workflow: "*".into(),
            harness: "*".into(),
            model: "*".into(),
        },
    }
}

fn dim(value: &Option<String>) -> String {
    value
        .as_deref()
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown")
        .to_string()
}

fn metric_sort_key(projection: &ScorecardProjection) -> String {
    format!("{:?}", projection)
}

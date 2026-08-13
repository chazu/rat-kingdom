use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use super::outcome_events::{
    stable_hash, FactoryMetricPayload, FactoryOutcomeEvent, StableHash, StructuredOutcomeInput,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum OutcomeStatus {
    Accepted,
    Reworked,
    CiFailed,
    CiRecovered,
    Reverted,
    Unknown,
    Unobserved,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum OutcomeEvidenceKind {
    AgentRecord,
    WorkflowInstance,
    Phase3Contract,
    Phase3VerifiedDelivery,
    StructuredReviewerRework,
    Phase4CiSignal,
    StructuredRevert,
    HumanGateDecision,
    RecurrenceKey,
    PricingSnapshot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum OutcomeMetricKind {
    Run,
    TaskClass,
    Accepted,
    Reworked,
    Ci,
    Reverted,
    HumanIntervention,
    Recurrence,
    Cost,
    LeadTime,
    Unknown,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OutcomeFactGroupKey {
    pub task_class: Option<String>,
    pub workflow: Option<String>,
    pub harness: Option<String>,
    pub model: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeFactSource {
    pub kind: OutcomeEvidenceKind,
    pub source_id: String,
    pub warnings: Vec<String>,
    pub repo: Option<String>,
    pub group_key: Option<OutcomeFactGroupKey>,
}

impl OutcomeFactSource {
    pub fn unavailable(kind: OutcomeEvidenceKind) -> Self {
        Self {
            kind,
            source_id: "unavailable".into(),
            warnings: vec!["source_family_unobserved".into()],
            repo: None,
            group_key: None,
        }
    }

    pub fn unavailable_for(
        kind: OutcomeEvidenceKind,
        repo: impl Into<String>,
        group_key: OutcomeFactGroupKey,
    ) -> Self {
        Self {
            kind,
            source_id: "unavailable".into(),
            warnings: vec!["source_family_unobserved".into()],
            repo: Some(repo.into()),
            group_key: Some(group_key),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceAvailability {
    pub source_family: OutcomeEvidenceKind,
    pub available: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceCounts {
    pub active_source_count: u32,
    pub archived_source_count: u32,
    pub event_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeFact {
    pub fact_id: StableHash,
    pub event_id: Option<StableHash>,
    pub repo: String,
    pub group_key: OutcomeFactGroupKey,
    pub status: OutcomeStatus,
    pub evidence_kind: OutcomeEvidenceKind,
    pub source: OutcomeFactSource,
    pub availability: SourceAvailability,
    pub source_counts: SourceCounts,
    pub metric_kind: OutcomeMetricKind,
    pub archived: bool,
    pub archive_source_family: Option<OutcomeEvidenceKind>,
    pub human_interventions: u32,
    pub recurrence_count: u32,
    pub recurrence_key: Option<String>,
    pub cost_micro_usd: Option<u64>,
    pub lead_time_ms: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct OutcomeFactBuilder {
    events: Vec<FactoryOutcomeEvent>,
    unavailable: Vec<OutcomeFactSource>,
    include_archived: bool,
}

impl OutcomeFactBuilder {
    pub fn from_structured_inputs<I, U>(inputs: I, unavailable: U) -> Self
    where
        I: IntoIterator<Item = StructuredOutcomeInput>,
        U: IntoIterator<Item = OutcomeFactSource>,
    {
        Self {
            events: inputs.into_iter().map(FactoryOutcomeEvent::from).collect(),
            unavailable: unavailable.into_iter().collect(),
            include_archived: false,
        }
    }

    pub fn include_archived(mut self, include_archived: bool) -> Self {
        self.include_archived = include_archived;
        self
    }

    pub fn build(mut self) -> Vec<OutcomeFact> {
        self.events
            .sort_by(|left, right| left.event_id.cmp(&right.event_id));
        let mut facts: Vec<_> = self
            .events
            .iter()
            .filter(|event| self.include_archived || !event.archived)
            .map(|event| fact_from_event(event, &self.events))
            .collect();

        let default_repo = single_repo(&self.events).unwrap_or_else(|| "unknown".into());
        let default_group_key = single_group_key(&self.events).unwrap_or_else(unknown_group_key);
        facts.extend(self.unavailable.into_iter().map(|source| {
            let repo = source.repo.clone().unwrap_or_else(|| default_repo.clone());
            let group_key = source
                .group_key
                .clone()
                .unwrap_or_else(|| default_group_key.clone());
            unobserved_fact(source, repo, group_key, SourceCounts::default())
        }));
        if !self.include_archived {
            for source in archived_only_sources(&self.events) {
                facts.push(unobserved_fact(
                    OutcomeFactSource {
                        kind: source.1,
                        source_id: "archived_only".into(),
                        warnings: vec!["source_family_archived_only".into()],
                        repo: Some(source.0.clone()),
                        group_key: Some(source.2.clone()),
                    },
                    source.0,
                    source.2,
                    source.3,
                ));
            }
        }
        facts.sort_by(|left, right| {
            (&left.repo, &left.group_key, &left.fact_id).cmp(&(
                &right.repo,
                &right.group_key,
                &right.fact_id,
            ))
        });
        facts
    }
}

fn fact_from_event(event: &FactoryOutcomeEvent, all_events: &[FactoryOutcomeEvent]) -> OutcomeFact {
    let mut warnings = Vec::new();
    let task_class = if event
        .task_class
        .as_deref()
        .is_some_and(|task_class| !task_class.trim().is_empty())
        && task_class_source_allows(event)
    {
        event
            .task_class
            .as_deref()
            .map(|task_class| task_class.trim().to_owned())
    } else {
        if event.task_class.is_some() {
            warnings.push("task_class_forbidden_source: explicit Phase 3 contract, ticket, or structured outcome provenance required".into());
        } else {
            warnings.push("task_class_unobserved: explicit Phase 3 contract, ticket, or outcome field required".into());
        }
        Some("unknown".into())
    };

    let status = status_from_structured_payload(event, all_events);
    let human_interventions = match (&event.source_family, &event.metric_payload) {
        (
            OutcomeEvidenceKind::HumanGateDecision,
            FactoryMetricPayload::HumanIntervention { count },
        ) => *count,
        _ => 0,
    };
    let recurrence_count = match (&event.source_family, &event.metric_payload) {
        (OutcomeEvidenceKind::RecurrenceKey, FactoryMetricPayload::Recurrence)
            if event
                .recurrence_key
                .as_deref()
                .is_some_and(|key| !key.trim().is_empty())
                || event
                    .coalesce_key
                    .as_deref()
                    .is_some_and(|key| !key.trim().is_empty()) =>
        {
            1
        }
        _ => 0,
    };
    let recurrence_key = if recurrence_count > 0 {
        event
            .recurrence_key
            .as_deref()
            .or(event.coalesce_key.as_deref())
            .map(str::trim)
            .filter(|key| !key.is_empty())
            .map(str::to_owned)
    } else {
        None
    };
    let metric_kind = metric_kind_from_payload(&event.metric_payload);
    let cost_micro_usd = match (&event.source_family, &event.metric_payload) {
        (
            OutcomeEvidenceKind::AgentRecord,
            FactoryMetricPayload::Cost {
                micro_usd,
                pricing_evidence_id: Some(pricing_evidence_id),
            },
        ) if !pricing_evidence_id.trim().is_empty() => Some(*micro_usd),
        _ => None,
    };
    let lead_time_ms = match event.metric_payload {
        FactoryMetricPayload::LeadTime {
            started_at_ms,
            completed_at_ms,
            ref run_id,
            ref completed_run_id,
        } if matches!(
            event.source_family,
            OutcomeEvidenceKind::AgentRecord
                | OutcomeEvidenceKind::WorkflowInstance
                | OutcomeEvidenceKind::Phase3VerifiedDelivery
        ) && event
            .workflow_instance_id
            .as_deref()
            .is_some_and(|id| !id.trim().is_empty() && id == run_id)
            && run_id == completed_run_id
            && completed_at_ms >= started_at_ms =>
        {
            completed_at_ms
                .checked_sub(started_at_ms)
                .and_then(|ms| u64::try_from(ms).ok())
        }
        _ => None,
    };
    let source_counts = source_counts_for(event, all_events);

    let mut fact = OutcomeFact {
        fact_id: String::new(),
        event_id: Some(event.event_id.clone()),
        repo: event.repo.clone(),
        group_key: OutcomeFactGroupKey {
            task_class,
            workflow: event.workflow.clone().or_else(|| Some("unknown".into())),
            harness: event.harness.clone().or_else(|| Some("unknown".into())),
            model: event.model.clone().or_else(|| Some("unknown".into())),
        },
        status,
        evidence_kind: event.source_family,
        source: OutcomeFactSource {
            kind: event.source_family,
            source_id: event.source_id.clone(),
            warnings,
            repo: Some(event.repo.clone()),
            group_key: Some(group_key_for_event(event)),
        },
        availability: SourceAvailability {
            source_family: event.source_family,
            available: true,
        },
        source_counts,
        metric_kind,
        archived: event.archived,
        archive_source_family: event.archived.then_some(event.source_family),
        human_interventions,
        recurrence_count,
        recurrence_key,
        cost_micro_usd,
        lead_time_ms,
    };
    fact.fact_id = stable_hash(&fact);
    fact
}

fn unobserved_fact(
    source: OutcomeFactSource,
    repo: String,
    group_key: OutcomeFactGroupKey,
    source_counts: SourceCounts,
) -> OutcomeFact {
    let mut fact = OutcomeFact {
        fact_id: String::new(),
        event_id: None,
        repo,
        group_key,
        status: OutcomeStatus::Unobserved,
        evidence_kind: source.kind,
        availability: SourceAvailability {
            source_family: source.kind,
            available: false,
        },
        source_counts,
        metric_kind: OutcomeMetricKind::Unknown,
        source,
        archived: false,
        archive_source_family: None,
        human_interventions: 0,
        recurrence_count: 0,
        recurrence_key: None,
        cost_micro_usd: None,
        lead_time_ms: None,
    };
    fact.fact_id = stable_hash(&fact);
    fact
}

fn metric_kind_from_payload(payload: &FactoryMetricPayload) -> OutcomeMetricKind {
    match payload {
        FactoryMetricPayload::Run { .. } => OutcomeMetricKind::Run,
        FactoryMetricPayload::TaskClass => OutcomeMetricKind::TaskClass,
        FactoryMetricPayload::Accepted { .. } => OutcomeMetricKind::Accepted,
        FactoryMetricPayload::Reworked { .. } => OutcomeMetricKind::Reworked,
        FactoryMetricPayload::Ci { .. } => OutcomeMetricKind::Ci,
        FactoryMetricPayload::Reverted { .. } => OutcomeMetricKind::Reverted,
        FactoryMetricPayload::HumanIntervention { .. } => OutcomeMetricKind::HumanIntervention,
        FactoryMetricPayload::Recurrence => OutcomeMetricKind::Recurrence,
        FactoryMetricPayload::Cost { .. } => OutcomeMetricKind::Cost,
        FactoryMetricPayload::LeadTime { .. } => OutcomeMetricKind::LeadTime,
        FactoryMetricPayload::Unknown => OutcomeMetricKind::Unknown,
    }
}

fn source_counts_for(
    event: &FactoryOutcomeEvent,
    all_events: &[FactoryOutcomeEvent],
) -> SourceCounts {
    let mut counts = SourceCounts::default();
    let mut active_sources = BTreeSet::new();
    let mut archived_sources = BTreeSet::new();
    for candidate in all_events.iter().filter(|candidate| {
        candidate.repo == event.repo && candidate.source_family == event.source_family
    }) {
        counts.event_count = counts.event_count.saturating_add(1);
        if candidate.archived {
            archived_sources.insert(candidate.source_id.clone());
        } else {
            active_sources.insert(candidate.source_id.clone());
        }
    }
    counts.active_source_count = active_sources.len().try_into().unwrap_or(u32::MAX);
    counts.archived_source_count = archived_sources.len().try_into().unwrap_or(u32::MAX);
    counts
}

fn task_class_source_allows(event: &FactoryOutcomeEvent) -> bool {
    matches!(
        event.source_family,
        OutcomeEvidenceKind::Phase3Contract | OutcomeEvidenceKind::Phase3VerifiedDelivery
    ) || event
        .ticket_id
        .as_deref()
        .is_some_and(|id| !id.trim().is_empty())
        || event
            .phase3_outcome_id
            .as_deref()
            .is_some_and(|id| !id.trim().is_empty())
}

fn status_from_structured_payload(
    event: &FactoryOutcomeEvent,
    all_events: &[FactoryOutcomeEvent],
) -> OutcomeStatus {
    match (&event.source_family, &event.metric_payload) {
        (
            OutcomeEvidenceKind::Phase3VerifiedDelivery,
            FactoryMetricPayload::Accepted {
                verified_delivery,
                landed,
            },
        ) if *verified_delivery || *landed => OutcomeStatus::Accepted,
        (
            OutcomeEvidenceKind::StructuredReviewerRework,
            FactoryMetricPayload::Reworked { requested: true },
        ) => OutcomeStatus::Reworked,
        (OutcomeEvidenceKind::Phase4CiSignal, FactoryMetricPayload::Ci { failed: true, .. }) => {
            OutcomeStatus::CiFailed
        }
        (
            OutcomeEvidenceKind::Phase4CiSignal,
            FactoryMetricPayload::Ci {
                recovered: true, ..
            },
        ) if has_prior_failed_ci_signal(event, all_events) => OutcomeStatus::CiRecovered,
        (
            OutcomeEvidenceKind::StructuredRevert,
            FactoryMetricPayload::Reverted { reverted: true },
        ) => OutcomeStatus::Reverted,
        _ => OutcomeStatus::Unknown,
    }
}

fn has_prior_failed_ci_signal(
    recovered: &FactoryOutcomeEvent,
    all_events: &[FactoryOutcomeEvent],
) -> bool {
    let Some(failed_signal_id) = recovered.phase4_signal_id.as_deref() else {
        return false;
    };
    if failed_signal_id.trim().is_empty()
        || recovered
            .workflow
            .as_deref()
            .is_none_or(|workflow| workflow.trim().is_empty())
        || recovered
            .workflow_instance_id
            .as_deref()
            .is_none_or(|run| run.trim().is_empty())
        || recovered
            .source_version
            .as_deref()
            .is_none_or(|commit| commit.trim().is_empty())
    {
        return false;
    }
    all_events.iter().any(|event| {
        event.source_family == OutcomeEvidenceKind::Phase4CiSignal
            && event.repo == recovered.repo
            && event.source_id == failed_signal_id
            && event.workflow == recovered.workflow
            && event.workflow_instance_id == recovered.workflow_instance_id
            && event.source_version == recovered.source_version
            && event.observed_at_ms < recovered.observed_at_ms
            && matches!(
                event.metric_payload,
                FactoryMetricPayload::Ci {
                    failed: true,
                    recovered: false
                }
            )
    })
}

fn unknown_group_key() -> OutcomeFactGroupKey {
    OutcomeFactGroupKey {
        task_class: Some("unknown".into()),
        workflow: Some("unknown".into()),
        harness: Some("unknown".into()),
        model: Some("unknown".into()),
    }
}

fn group_key_for_event(event: &FactoryOutcomeEvent) -> OutcomeFactGroupKey {
    OutcomeFactGroupKey {
        task_class: event
            .task_class
            .as_deref()
            .filter(|task_class| !task_class.trim().is_empty() && task_class_source_allows(event))
            .map(str::to_owned)
            .or_else(|| Some("unknown".into())),
        workflow: event.workflow.clone().or_else(|| Some("unknown".into())),
        harness: event.harness.clone().or_else(|| Some("unknown".into())),
        model: event.model.clone().or_else(|| Some("unknown".into())),
    }
}

fn single_repo(events: &[FactoryOutcomeEvent]) -> Option<String> {
    let repos: BTreeSet<_> = events.iter().map(|event| event.repo.clone()).collect();
    (repos.len() == 1).then(|| repos.into_iter().next().unwrap())
}

fn single_group_key(events: &[FactoryOutcomeEvent]) -> Option<OutcomeFactGroupKey> {
    let group_keys: BTreeSet<_> = events.iter().map(group_key_for_event).collect();
    (group_keys.len() == 1).then(|| group_keys.into_iter().next().unwrap())
}

fn archived_only_sources(
    events: &[FactoryOutcomeEvent],
) -> Vec<(
    String,
    OutcomeEvidenceKind,
    OutcomeFactGroupKey,
    SourceCounts,
)> {
    let mut sources = Vec::new();
    let mut scopes = BTreeSet::new();
    for event in events {
        scopes.insert((
            event.repo.clone(),
            event.source_family,
            group_key_for_event(event),
        ));
    }

    for (repo, source_family, group_key) in scopes {
        let matching: Vec<_> = events
            .iter()
            .filter(|event| {
                event.repo == repo
                    && event.source_family == source_family
                    && group_key_for_event(event) == group_key
            })
            .collect();
        if matching.iter().all(|event| event.archived) {
            let counts = source_counts_for_scope(&repo, source_family, &group_key, events);
            sources.push((repo, source_family, group_key, counts));
        }
    }

    sources
}

fn source_counts_for_scope(
    repo: &str,
    source_family: OutcomeEvidenceKind,
    group_key: &OutcomeFactGroupKey,
    all_events: &[FactoryOutcomeEvent],
) -> SourceCounts {
    let mut counts = SourceCounts::default();
    let mut active_sources = BTreeSet::new();
    let mut archived_sources = BTreeSet::new();
    for candidate in all_events.iter().filter(|candidate| {
        candidate.repo == repo
            && candidate.source_family == source_family
            && group_key_for_event(candidate) == *group_key
    }) {
        counts.event_count = counts.event_count.saturating_add(1);
        if candidate.archived {
            archived_sources.insert(candidate.source_id.clone());
        } else {
            active_sources.insert(candidate.source_id.clone());
        }
    }
    counts.active_source_count = active_sources.len().try_into().unwrap_or(u32::MAX);
    counts.archived_source_count = archived_sources.len().try_into().unwrap_or(u32::MAX);
    counts
}

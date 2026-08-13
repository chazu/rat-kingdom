use serde::{Deserialize, Serialize};

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
}

impl OutcomeFactSource {
    pub fn unavailable(kind: OutcomeEvidenceKind) -> Self {
        Self {
            kind,
            source_id: "unavailable".into(),
            warnings: vec!["source_family_unobserved".into()],
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
    pub archived: bool,
    pub archive_source_family: Option<OutcomeEvidenceKind>,
    pub human_interventions: u32,
    pub recurrence_count: u32,
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
            .map(fact_from_event)
            .collect();

        facts.extend(self.unavailable.into_iter().map(unobserved_fact));
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

fn fact_from_event(event: &FactoryOutcomeEvent) -> OutcomeFact {
    let mut warnings = Vec::new();
    let task_class = if task_class_source_allows(event.source_family) {
        event.task_class.clone()
    } else {
        if event.task_class.is_none() {
            warnings.push("task_class_unobserved: explicit Phase 3 contract, ticket, or outcome field required".into());
        }
        None
    };

    let status = status_from_structured_payload(event);
    let human_interventions = match (&event.source_family, &event.metric_payload) {
        (
            OutcomeEvidenceKind::HumanGateDecision,
            FactoryMetricPayload::HumanIntervention { count },
        ) => *count,
        _ => 0,
    };
    let recurrence_count = match (&event.source_family, &event.metric_payload) {
        (OutcomeEvidenceKind::RecurrenceKey, FactoryMetricPayload::Recurrence)
            if event.recurrence_key.is_some() || event.coalesce_key.is_some() =>
        {
            1
        }
        _ => 0,
    };
    let cost_micro_usd = match (&event.source_family, &event.metric_payload) {
        (OutcomeEvidenceKind::PricingSnapshot, FactoryMetricPayload::Cost { micro_usd }) => {
            Some(*micro_usd)
        }
        _ => None,
    };
    let lead_time_ms = match event.metric_payload {
        FactoryMetricPayload::LeadTime { ms } => Some(ms),
        _ => None,
    };

    let mut fact = OutcomeFact {
        fact_id: String::new(),
        event_id: Some(event.event_id.clone()),
        repo: event.repo.clone(),
        group_key: OutcomeFactGroupKey {
            task_class,
            workflow: event.workflow.clone(),
            harness: event.harness.clone(),
            model: event.model.clone(),
        },
        status,
        evidence_kind: event.source_family,
        source: OutcomeFactSource {
            kind: event.source_family,
            source_id: event.source_id.clone(),
            warnings,
        },
        availability: SourceAvailability {
            source_family: event.source_family,
            available: true,
        },
        source_counts: SourceCounts {
            active_source_count: u32::from(!event.archived),
            archived_source_count: u32::from(event.archived),
            event_count: 1,
        },
        archived: event.archived,
        archive_source_family: event.archived.then_some(event.source_family),
        human_interventions,
        recurrence_count,
        cost_micro_usd,
        lead_time_ms,
    };
    fact.fact_id = stable_hash(&fact);
    fact
}

fn unobserved_fact(source: OutcomeFactSource) -> OutcomeFact {
    let mut fact = OutcomeFact {
        fact_id: String::new(),
        event_id: None,
        repo: String::new(),
        group_key: OutcomeFactGroupKey::default(),
        status: OutcomeStatus::Unobserved,
        evidence_kind: source.kind,
        availability: SourceAvailability {
            source_family: source.kind,
            available: false,
        },
        source_counts: SourceCounts::default(),
        source,
        archived: false,
        archive_source_family: None,
        human_interventions: 0,
        recurrence_count: 0,
        cost_micro_usd: None,
        lead_time_ms: None,
    };
    fact.fact_id = stable_hash(&fact);
    fact
}

fn task_class_source_allows(kind: OutcomeEvidenceKind) -> bool {
    matches!(
        kind,
        OutcomeEvidenceKind::Phase3Contract | OutcomeEvidenceKind::Phase3VerifiedDelivery
    )
}

fn status_from_structured_payload(event: &FactoryOutcomeEvent) -> OutcomeStatus {
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
        ) => OutcomeStatus::CiRecovered,
        (
            OutcomeEvidenceKind::StructuredRevert,
            FactoryMetricPayload::Reverted { reverted: true },
        ) => OutcomeStatus::Reverted,
        _ => OutcomeStatus::Unknown,
    }
}

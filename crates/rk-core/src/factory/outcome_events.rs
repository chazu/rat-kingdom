use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::outcome_facts::OutcomeEvidenceKind;

pub type StableHash = String;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactoryOutcomeEvent {
    pub schema_version: u32,
    pub event_id: StableHash,
    pub repo: String,
    pub source_family: OutcomeEvidenceKind,
    pub source_id: String,
    pub source_version: Option<String>,
    pub archived: bool,
    pub archive_reason: Option<String>,
    pub observed_at_ms: i64,
    pub task_class: Option<String>,
    pub workflow: Option<String>,
    pub harness: Option<String>,
    pub model: Option<String>,
    pub agent_id: Option<String>,
    pub workflow_instance_id: Option<String>,
    pub ticket_id: Option<String>,
    pub phase3_outcome_id: Option<String>,
    pub phase4_signal_id: Option<String>,
    pub recurrence_key: Option<String>,
    pub coalesce_key: Option<String>,
    pub metric_payload: FactoryMetricPayload,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuredOutcomeInput {
    pub repo: String,
    pub source_family: OutcomeEvidenceKind,
    pub source_id: String,
    pub source_version: Option<String>,
    pub archived: bool,
    pub archive_reason: Option<String>,
    pub observed_at_ms: i64,
    pub task_class: Option<String>,
    pub workflow: Option<String>,
    pub harness: Option<String>,
    pub model: Option<String>,
    pub agent_id: Option<String>,
    pub workflow_instance_id: Option<String>,
    pub ticket_id: Option<String>,
    pub phase3_outcome_id: Option<String>,
    pub phase4_signal_id: Option<String>,
    pub recurrence_key: Option<String>,
    pub coalesce_key: Option<String>,
    pub payload: FactoryMetricPayload,
    /// Test-only/provenance prose is intentionally never read by normalization.
    pub decoy_prose: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FactoryMetricPayload {
    Run {
        count: u32,
    },
    TaskClass,
    Accepted {
        verified_delivery: bool,
        landed: bool,
    },
    Reworked {
        requested: bool,
    },
    Ci {
        failed: bool,
        recovered: bool,
    },
    Reverted {
        reverted: bool,
    },
    HumanIntervention {
        count: u32,
    },
    Recurrence,
    Cost {
        micro_usd: u64,
    },
    LeadTime {
        ms: u64,
    },
    Unknown,
}

impl From<StructuredOutcomeInput> for FactoryOutcomeEvent {
    fn from(input: StructuredOutcomeInput) -> Self {
        let mut event = Self {
            schema_version: 1,
            event_id: String::new(),
            repo: input.repo,
            source_family: input.source_family,
            source_id: input.source_id,
            source_version: input.source_version,
            archived: input.archived,
            archive_reason: input.archive_reason,
            observed_at_ms: input.observed_at_ms,
            task_class: input.task_class,
            workflow: input.workflow,
            harness: input.harness,
            model: input.model,
            agent_id: input.agent_id,
            workflow_instance_id: input.workflow_instance_id,
            ticket_id: input.ticket_id,
            phase3_outcome_id: input.phase3_outcome_id,
            phase4_signal_id: input.phase4_signal_id,
            recurrence_key: input.recurrence_key,
            coalesce_key: input.coalesce_key,
            metric_payload: input.payload,
        };
        event.event_id = stable_hash(&event);
        event
    }
}

pub(crate) fn stable_hash<T: Serialize>(value: &T) -> StableHash {
    let bytes = serde_json::to_vec(value).expect("factory outcome serialization is deterministic");
    let digest = Sha256::digest(bytes);
    hex::encode(digest)
}

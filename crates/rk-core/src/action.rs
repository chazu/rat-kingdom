//! Canonical typed factory actions and digest-bound approval records.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::Result;

/// Stable wire identifier for a supported factory action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ActionKind {
    #[serde(rename = "workflow.run")]
    WorkflowRun,
}

/// Coarse safety classification used by every action surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ActionRisk {
    Read,
    Mutation,
    Dangerous,
}

/// Daemon-resolved repository identity included in an approval digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RepoScope {
    pub identity: String,
    pub path: String,
}

/// Resources to which an action and its approval are bound.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ActionScope {
    pub repo: RepoScope,
}

/// The initial typed mutating action supported by the factory API.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct WorkflowRunAction {
    pub name: String,
    pub repo: String,
    pub repo_identity: String,
    pub repo_path: String,
    pub params: BTreeMap<String, Value>,
    pub coordinator: Option<String>,
}

/// A canonical factory action. Human-readable commands are never digest input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind")]
pub enum FactoryAction {
    #[serde(rename = "workflow.run")]
    WorkflowRun(WorkflowRunAction),
}

impl FactoryAction {
    #[must_use]
    pub const fn kind(&self) -> ActionKind {
        match self {
            Self::WorkflowRun(_) => ActionKind::WorkflowRun,
        }
    }

    #[must_use]
    pub const fn risk(&self) -> ActionRisk {
        match self {
            Self::WorkflowRun(_) => ActionRisk::Mutation,
        }
    }
}

/// Digest-bearing request for operator approval.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ActionProposal {
    pub schema: u32,
    pub id: String,
    pub digest: String,
    pub kind: ActionKind,
    pub risk: ActionRisk,
    pub scope: ActionScope,
    pub requester: String,
    pub action: FactoryAction,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub status: String,
}

/// Durable lifecycle of an exact digest approval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ApprovalStatus {
    Approved,
    Executing,
    Consumed,
    Failed,
}

/// Operator approval bound to one requester, action kind, scope, and digest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ApprovalGrant {
    pub schema: u32,
    pub proposal_id: String,
    pub digest: String,
    pub kind: ActionKind,
    pub scope: ActionScope,
    pub requester: String,
    pub approved_by: String,
    pub status: ApprovalStatus,
    pub approved_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub execution_id: Option<String>,
    pub instance_id: Option<String>,
    pub failure: Option<String>,
    pub consumed_at: Option<DateTime<Utc>>,
}

/// Serialize deterministic compact JSON with every object key sorted recursively.
pub fn canonical_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let value = serde_json::to_value(value)?;
    serde_json::to_vec(&sort_object_keys(value)).map_err(Into::into)
}

/// Return the lowercase hexadecimal SHA-256 of canonical typed JSON.
pub fn canonical_digest<T: Serialize>(value: &T) -> Result<String> {
    Ok(hex::encode(Sha256::digest(canonical_json_bytes(value)?)))
}

fn sort_object_keys(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(sort_object_keys).collect()),
        Value::Object(values) => {
            let sorted: BTreeMap<_, _> = values
                .into_iter()
                .map(|(key, value)| (key, sort_object_keys(value)))
                .collect();
            Value::Object(Map::from_iter(sorted))
        }
        scalar => scalar,
    }
}

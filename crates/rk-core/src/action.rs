//! Canonical typed factory actions and digest-bound approval records.

use std::collections::BTreeMap;
use std::fmt;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{ser, Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::Result;

/// Stable wire identifier for a supported factory action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ActionKind {
    #[serde(rename = "workflow.run")]
    WorkflowRun,
    #[serde(rename = "ticket_graph.apply")]
    TicketGraphApply,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TicketGraphApplyAction {
    pub repo: String,
    pub repo_identity: String,
    pub repo_path: String,
    pub graph: Value,
    pub initiative: Value,
    pub topological_order: Vec<String>,
    pub mutations: Vec<Value>,
}

/// A canonical factory action. Human-readable commands are never digest input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind")]
pub enum FactoryAction {
    #[serde(rename = "workflow.run")]
    WorkflowRun(WorkflowRunAction),
    #[serde(rename = "ticket_graph.apply")]
    TicketGraphApply(TicketGraphApplyAction),
}

impl FactoryAction {
    #[must_use]
    pub const fn kind(&self) -> ActionKind {
        match self {
            Self::WorkflowRun(_) => ActionKind::WorkflowRun,
            Self::TicketGraphApply(_) => ActionKind::TicketGraphApply,
        }
    }

    #[must_use]
    pub const fn risk(&self) -> ActionRisk {
        match self {
            Self::WorkflowRun(_) => ActionRisk::Mutation,
            Self::TicketGraphApply(_) => ActionRisk::Mutation,
        }
    }

    #[must_use]
    pub fn repo_scope(&self) -> RepoScope {
        match self {
            Self::WorkflowRun(action) => RepoScope {
                identity: action.repo_identity.clone(),
                path: action.repo_path.clone(),
            },
            Self::TicketGraphApply(action) => RepoScope {
                identity: action.repo_identity.clone(),
                path: action.repo_path.clone(),
            },
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
    pub nonce: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub status: String,
}

/// Immutable canonical payload covered by an action approval digest.
///
/// Proposal lifecycle fields such as `id`, `digest`, `created_at`, and `status` are intentionally
/// excluded so the digest binds only the immutable action approval contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ActionDigestPayload {
    pub schema: u32,
    pub kind: ActionKind,
    pub risk: ActionRisk,
    pub scope: ActionScope,
    pub requester: String,
    pub action: FactoryAction,
    pub nonce: String,
    pub expires_at: DateTime<Utc>,
}

impl ActionDigestPayload {
    #[must_use]
    pub fn from_proposal(proposal: &ActionProposal) -> Self {
        Self {
            schema: proposal.schema,
            kind: proposal.kind,
            risk: proposal.risk,
            scope: proposal.scope.clone(),
            requester: proposal.requester.clone(),
            action: proposal.action.clone(),
            nonce: proposal.nonce.clone(),
            expires_at: proposal.expires_at,
        }
    }
}

impl ActionProposal {
    #[must_use]
    pub fn digest_payload(&self) -> ActionDigestPayload {
        ActionDigestPayload::from_proposal(self)
    }
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
    let value = value
        .serialize(FiniteValueSerializer)
        .map_err(|err| crate::Error::other(err.to_string()))?;
    serde_json::to_vec(&sort_object_keys(value)).map_err(Into::into)
}

/// Return the lowercase hexadecimal SHA-256 of canonical typed JSON.
pub fn canonical_digest<T: Serialize>(value: &T) -> Result<String> {
    Ok(hex::encode(Sha256::digest(canonical_json_bytes(value)?)))
}

/// Return the lowercase hexadecimal SHA-256 of a narrow action approval payload.
pub fn action_digest(payload: &ActionDigestPayload) -> Result<String> {
    canonical_digest(payload)
}

struct FiniteValueSerializer;

#[derive(Debug)]
struct FiniteSerError(String);

impl fmt::Display for FiniteSerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FiniteSerError {}

impl ser::Error for FiniteSerError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self(msg.to_string())
    }
}

type SerResult<T> = std::result::Result<T, FiniteSerError>;

impl ser::Serializer for FiniteValueSerializer {
    type Ok = Value;
    type Error = FiniteSerError;
    type SerializeSeq = FiniteSeq;
    type SerializeTuple = FiniteSeq;
    type SerializeTupleStruct = FiniteSeq;
    type SerializeTupleVariant = FiniteTupleVariant;
    type SerializeMap = FiniteMap;
    type SerializeStruct = FiniteMap;
    type SerializeStructVariant = FiniteStructVariant;

    fn serialize_bool(self, v: bool) -> SerResult<Value> {
        Ok(Value::Bool(v))
    }
    fn serialize_i8(self, v: i8) -> SerResult<Value> {
        Ok(Value::from(v))
    }
    fn serialize_i16(self, v: i16) -> SerResult<Value> {
        Ok(Value::from(v))
    }
    fn serialize_i32(self, v: i32) -> SerResult<Value> {
        Ok(Value::from(v))
    }
    fn serialize_i64(self, v: i64) -> SerResult<Value> {
        Ok(Value::from(v))
    }
    fn serialize_u8(self, v: u8) -> SerResult<Value> {
        Ok(Value::from(v))
    }
    fn serialize_u16(self, v: u16) -> SerResult<Value> {
        Ok(Value::from(v))
    }
    fn serialize_u32(self, v: u32) -> SerResult<Value> {
        Ok(Value::from(v))
    }
    fn serialize_u64(self, v: u64) -> SerResult<Value> {
        Ok(Value::from(v))
    }
    fn serialize_f32(self, v: f32) -> SerResult<Value> {
        finite_number(f64::from(v))
    }
    fn serialize_f64(self, v: f64) -> SerResult<Value> {
        finite_number(v)
    }
    fn serialize_char(self, v: char) -> SerResult<Value> {
        Ok(Value::String(v.to_string()))
    }
    fn serialize_str(self, v: &str) -> SerResult<Value> {
        Ok(Value::String(v.to_owned()))
    }
    fn serialize_bytes(self, v: &[u8]) -> SerResult<Value> {
        Ok(Value::Array(v.iter().copied().map(Value::from).collect()))
    }
    fn serialize_none(self) -> SerResult<Value> {
        Ok(Value::Null)
    }
    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> SerResult<Value> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> SerResult<Value> {
        Ok(Value::Null)
    }
    fn serialize_unit_struct(self, _name: &'static str) -> SerResult<Value> {
        Ok(Value::Null)
    }
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> SerResult<Value> {
        Ok(Value::String(variant.to_owned()))
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> SerResult<Value> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> SerResult<Value> {
        let mut map = Map::new();
        map.insert(variant.to_owned(), value.serialize(FiniteValueSerializer)?);
        Ok(Value::Object(map))
    }
    fn serialize_seq(self, _len: Option<usize>) -> SerResult<FiniteSeq> {
        Ok(FiniteSeq(Vec::new()))
    }
    fn serialize_tuple(self, _len: usize) -> SerResult<FiniteSeq> {
        Ok(FiniteSeq(Vec::new()))
    }
    fn serialize_tuple_struct(self, _name: &'static str, _len: usize) -> SerResult<FiniteSeq> {
        Ok(FiniteSeq(Vec::new()))
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> SerResult<FiniteTupleVariant> {
        Ok(FiniteTupleVariant {
            variant,
            values: Vec::new(),
        })
    }
    fn serialize_map(self, _len: Option<usize>) -> SerResult<FiniteMap> {
        Ok(FiniteMap(Map::new(), None))
    }
    fn serialize_struct(self, _name: &'static str, _len: usize) -> SerResult<FiniteMap> {
        Ok(FiniteMap(Map::new(), None))
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> SerResult<FiniteStructVariant> {
        Ok(FiniteStructVariant {
            variant,
            fields: Map::new(),
        })
    }
}

fn finite_number(v: f64) -> SerResult<Value> {
    if !v.is_finite() {
        return Err(FiniteSerError(
            "non-finite float cannot be canonicalized".into(),
        ));
    }
    serde_json::Number::from_f64(v)
        .map(Value::Number)
        .ok_or_else(|| FiniteSerError("non-finite float cannot be canonicalized".into()))
}

struct FiniteSeq(Vec<Value>);

impl ser::SerializeSeq for FiniteSeq {
    type Ok = Value;
    type Error = FiniteSerError;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> SerResult<()> {
        self.0.push(value.serialize(FiniteValueSerializer)?);
        Ok(())
    }
    fn end(self) -> SerResult<Value> {
        Ok(Value::Array(self.0))
    }
}
impl ser::SerializeTuple for FiniteSeq {
    type Ok = Value;
    type Error = FiniteSerError;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> SerResult<()> {
        ser::SerializeSeq::serialize_element(self, value)
    }
    fn end(self) -> SerResult<Value> {
        ser::SerializeSeq::end(self)
    }
}
impl ser::SerializeTupleStruct for FiniteSeq {
    type Ok = Value;
    type Error = FiniteSerError;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> SerResult<()> {
        ser::SerializeSeq::serialize_element(self, value)
    }
    fn end(self) -> SerResult<Value> {
        ser::SerializeSeq::end(self)
    }
}

struct FiniteMap(Map<String, Value>, Option<String>);

impl ser::SerializeMap for FiniteMap {
    type Ok = Value;
    type Error = FiniteSerError;
    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> SerResult<()> {
        let key = key.serialize(FiniteValueSerializer)?;
        let Value::String(key) = key else {
            return Err(FiniteSerError(
                "canonical object key must be a string".into(),
            ));
        };
        self.1 = Some(key);
        Ok(())
    }
    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> SerResult<()> {
        let key = self
            .1
            .take()
            .ok_or_else(|| FiniteSerError("canonical object value missing key".into()))?;
        self.0.insert(key, value.serialize(FiniteValueSerializer)?);
        Ok(())
    }
    fn end(self) -> SerResult<Value> {
        Ok(Value::Object(self.0))
    }
}

impl ser::SerializeStruct for FiniteMap {
    type Ok = Value;
    type Error = FiniteSerError;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> SerResult<()> {
        self.0
            .insert(key.to_owned(), value.serialize(FiniteValueSerializer)?);
        Ok(())
    }
    fn end(self) -> SerResult<Value> {
        Ok(Value::Object(self.0))
    }
}

struct FiniteTupleVariant {
    variant: &'static str,
    values: Vec<Value>,
}

impl ser::SerializeTupleVariant for FiniteTupleVariant {
    type Ok = Value;
    type Error = FiniteSerError;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> SerResult<()> {
        self.values.push(value.serialize(FiniteValueSerializer)?);
        Ok(())
    }
    fn end(self) -> SerResult<Value> {
        let mut map = Map::new();
        map.insert(self.variant.to_owned(), Value::Array(self.values));
        Ok(Value::Object(map))
    }
}

struct FiniteStructVariant {
    variant: &'static str,
    fields: Map<String, Value>,
}

impl ser::SerializeStructVariant for FiniteStructVariant {
    type Ok = Value;
    type Error = FiniteSerError;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> SerResult<()> {
        self.fields
            .insert(key.to_owned(), value.serialize(FiniteValueSerializer)?);
        Ok(())
    }
    fn end(self) -> SerResult<Value> {
        let mut map = Map::new();
        map.insert(self.variant.to_owned(), Value::Object(self.fields));
        Ok(Value::Object(map))
    }
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

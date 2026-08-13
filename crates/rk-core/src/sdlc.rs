use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalKind {
    CiFailed,
    CiRecovered,
    DeploymentSucceeded,
    ProductionAlertFiring,
    ProductionAlertResolved,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ConfiguredSourceName(String);

impl ConfiguredSourceName {
    pub fn new(name: impl Into<String>) -> Result<Self, SignalValidationError> {
        let name = name.into();
        validate_identity("source", &name)?;
        if name.starts_with("source:") {
            return Err(SignalValidationError::InvalidIdentity("source"));
        }
        Ok(Self(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ConfiguredSourceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalSourcePrincipal(String);

impl SignalSourcePrincipal {
    pub fn for_source(source: &ConfiguredSourceName) -> Self {
        Self(format!("source:{}", source.as_str()))
    }

    pub fn from_inline(_principal: &str) -> Result<Self, SignalValidationError> {
        Err(SignalValidationError::InlinePrincipalRejected)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceToken {
    source: ConfiguredSourceName,
    digest: String,
}

impl SourceToken {
    pub fn derive(source: &ConfiguredSourceName, secret: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"rk-sdlc-source-token-v1");
        hasher.update([0]);
        hasher.update(source.as_str().as_bytes());
        hasher.update([0]);
        hasher.update(secret);
        Self {
            source: source.clone(),
            digest: hex::encode(hasher.finalize()),
        }
    }

    pub fn source_name(&self) -> &ConfiguredSourceName {
        &self.source
    }

    pub fn verify(
        &self,
        source: &ConfiguredSourceName,
        secret: &[u8],
    ) -> Result<SignalSourcePrincipal, SignalValidationError> {
        if &self.source != source || *self != Self::derive(source, secret) {
            return Err(SignalValidationError::InvalidSourceToken);
        }
        Ok(SignalSourcePrincipal::for_source(source))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Correlation {
    pub repo: Option<String>,
    pub branch: Option<String>,
    pub workflow: Option<String>,
    pub job: Option<String>,
    pub commit_sha: Option<String>,
    pub environment: Option<String>,
    pub service: Option<String>,
    pub alert_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalRef {
    pub label: String,
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CiSignal {
    pub status: String,
    pub conclusion: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentSignal {
    pub environment: String,
    pub service: String,
    pub version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductionAlertSignal {
    pub environment: String,
    pub service: String,
    pub alert_key: String,
    pub severity: Option<String>,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SignalPayload {
    Ci(CiSignal),
    Deployment(DeploymentSignal),
    ProductionAlert(ProductionAlertSignal),
}

impl From<CiSignal> for SignalPayload {
    fn from(value: CiSignal) -> Self {
        Self::Ci(value)
    }
}

impl From<DeploymentSignal> for SignalPayload {
    fn from(value: DeploymentSignal) -> Self {
        Self::Deployment(value)
    }
}

impl From<ProductionAlertSignal> for SignalPayload {
    fn from(value: ProductionAlertSignal) -> Self {
        Self::ProductionAlert(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalEnvelope {
    pub kind: SignalKind,
    pub source: ConfiguredSourceName,
    pub delivery_id: String,
    pub occurred_at: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
    pub correlation: Correlation,
    pub summary: String,
    pub refs: Vec<SignalRef>,
    pub attributes: BTreeMap<String, String>,
    pub payload: SignalPayload,
}

impl SignalEnvelope {
    pub fn validate(&self, limits: &SignalLimits) -> Result<(), SignalValidationError> {
        OccurrenceId::new(self.source.clone(), self.delivery_id.clone())?;
        validate_identity("summary", &self.summary)?;
        if self.summary.len() > limits.max_summary_len {
            return Err(SignalValidationError::LimitExceeded("summary"));
        }
        if self.refs.len() > limits.max_refs {
            return Err(SignalValidationError::LimitExceeded("refs"));
        }
        if self.attributes.len() > limits.max_attributes {
            return Err(SignalValidationError::LimitExceeded("attributes"));
        }
        for signal_ref in &self.refs {
            reject_secret_like("ref", &signal_ref.label)?;
            validate_identity("ref.label", &signal_ref.label)?;
            validate_identity("ref.url", &signal_ref.url)?;
        }
        for (key, value) in &self.attributes {
            reject_secret_like("attribute", key)?;
            validate_identity("attribute.key", key)?;
            validate_identity("attribute.value", value)?;
        }
        self.validate_correlation()
    }

    fn validate_correlation(&self) -> Result<(), SignalValidationError> {
        match self.kind {
            SignalKind::CiFailed | SignalKind::CiRecovered => {
                require(&self.correlation.repo, "repo")?;
                require(&self.correlation.branch, "branch")?;
                require(&self.correlation.workflow, "workflow")?;
                require(&self.correlation.job, "job")?;
                require(&self.correlation.commit_sha, "commit_sha")?;
            }
            SignalKind::DeploymentSucceeded => {
                require(&self.correlation.environment, "environment")?;
                require(&self.correlation.service, "service")?;
            }
            SignalKind::ProductionAlertFiring | SignalKind::ProductionAlertResolved => {
                require(&self.correlation.environment, "environment")?;
                require(&self.correlation.service, "service")?;
                require(&self.correlation.alert_key, "alert_key")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalLimits {
    pub max_summary_len: usize,
    pub max_refs: usize,
    pub max_attributes: usize,
}

impl Default for SignalLimits {
    fn default() -> Self {
        Self {
            max_summary_len: 512,
            max_refs: 16,
            max_attributes: 64,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OccurrenceId {
    pub source: ConfiguredSourceName,
    pub delivery_id: String,
}

impl OccurrenceId {
    pub fn new(
        source: ConfiguredSourceName,
        delivery_id: impl Into<String>,
    ) -> Result<Self, SignalValidationError> {
        let delivery_id = delivery_id.into();
        validate_identity("delivery_id", &delivery_id)?;
        Ok(Self {
            source,
            delivery_id,
        })
    }
}

impl fmt::Display for OccurrenceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.source, self.delivery_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticStateDigest(String);

impl SemanticStateDigest {
    pub fn for_envelope(envelope: &SignalEnvelope) -> Result<Self, SignalValidationError> {
        envelope.validate(&SignalLimits::default())?;
        let canonical = CanonicalState::from(envelope);
        let bytes =
            serde_json::to_vec(&canonical).map_err(|_| SignalValidationError::DigestFailed)?;
        let digest = Sha256::digest(bytes);
        Ok(Self(format!("sha256:{}", hex::encode(digest))))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Serialize)]
struct CanonicalState<'a> {
    kind: &'a SignalKind,
    source: &'a ConfiguredSourceName,
    correlation: &'a Correlation,
    refs: Vec<&'a SignalRef>,
    attributes: &'a BTreeMap<String, String>,
    payload: &'a SignalPayload,
}

impl<'a> From<&'a SignalEnvelope> for CanonicalState<'a> {
    fn from(envelope: &'a SignalEnvelope) -> Self {
        let mut refs: Vec<&SignalRef> = envelope.refs.iter().collect();
        refs.sort_by(|left, right| {
            left.label
                .cmp(&right.label)
                .then_with(|| left.url.cmp(&right.url))
        });
        Self {
            kind: &envelope.kind,
            source: &envelope.source,
            correlation: &envelope.correlation,
            refs,
            attributes: &envelope.attributes,
            payload: &envelope.payload,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalReceipt {
    pub receipt_id: String,
    pub source: SignalSourcePrincipal,
    pub delivery_id: String,
    pub accepted_at: DateTime<Utc>,
    pub semantic_state_digest: SemanticStateDigest,
    pub projected_event_id: String,
    pub projected_fact_ids: Vec<String>,
    pub transition_emitted: bool,
}

impl SignalReceipt {
    #[allow(clippy::too_many_arguments)]
    pub fn accepted(
        receipt_id: String,
        source: SignalSourcePrincipal,
        delivery_id: String,
        accepted_at: DateTime<Utc>,
        semantic_state_digest: SemanticStateDigest,
        projected_event_id: String,
        projected_fact_ids: Vec<String>,
        transition_emitted: bool,
    ) -> Self {
        Self {
            receipt_id,
            source,
            delivery_id,
            accepted_at,
            semantic_state_digest,
            projected_event_id,
            projected_fact_ids,
            transition_emitted,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SignalValidationError {
    #[error("invalid empty identity field: {0}")]
    InvalidIdentity(&'static str),
    #[error("limit exceeded: {0}")]
    LimitExceeded(&'static str),
    #[error("secret-like {0} field rejected: {1}")]
    SecretLikeField(&'static str, String),
    #[error("inline principals are rejected")]
    InlinePrincipalRejected,
    #[error("invalid source token")]
    InvalidSourceToken,
    #[error("semantic digest failed")]
    DigestFailed,
}

fn require(value: &Option<String>, field: &'static str) -> Result<(), SignalValidationError> {
    match value {
        Some(value) => validate_identity(field, value),
        None => Err(SignalValidationError::InvalidIdentity(field)),
    }
}

fn validate_identity(field: &'static str, value: &str) -> Result<(), SignalValidationError> {
    if value.trim().is_empty() {
        Err(SignalValidationError::InvalidIdentity(field))
    } else {
        Ok(())
    }
}

fn reject_secret_like(kind: &'static str, key: &str) -> Result<(), SignalValidationError> {
    let lower = key.to_ascii_lowercase();
    let secret_words = [
        "secret",
        "token",
        "password",
        "authorization",
        "cookie",
        "credential",
        "header",
        "raw",
        "telemetry",
        "stack",
        "trace",
        "env",
        "command",
        "shell",
        "kubectl",
        "terraform",
    ];
    if secret_words.iter().any(|word| lower.contains(word)) {
        Err(SignalValidationError::SecretLikeField(kind, key.into()))
    } else {
        Ok(())
    }
}

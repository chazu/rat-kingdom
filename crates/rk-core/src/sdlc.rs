use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
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

impl<'de> Deserialize<'de> for ConfiguredSourceName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for ConfiguredSourceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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

impl<'de> Deserialize<'de> for SignalSourcePrincipal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let _ = String::deserialize(deserializer)?;
        Err(serde::de::Error::custom(
            SignalValidationError::InlinePrincipalRejected,
        ))
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
            reject_secret_like("ref", &signal_ref.url)?;
            validate_identity("ref.label", &signal_ref.label)?;
            validate_identity("ref.url", &signal_ref.url)?;
        }
        for (key, value) in &self.attributes {
            reject_secret_like("attribute", key)?;
            reject_secret_like("attribute", value)?;
            validate_identity("attribute.key", key)?;
            validate_identity("attribute.value", value)?;
        }
        ConfiguredSourceName::new(self.source.as_str())?;
        self.validate_correlation()?;
        self.validate_payload()
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

    fn validate_payload(&self) -> Result<(), SignalValidationError> {
        match (&self.kind, &self.payload) {
            (SignalKind::CiFailed | SignalKind::CiRecovered, SignalPayload::Ci(payload)) => {
                validate_identity("status", &payload.status)?;
                reject_secret_like("payload", &payload.status)?;
                if let Some(conclusion) = &payload.conclusion {
                    reject_secret_like("payload", conclusion)?;
                }
            }
            (SignalKind::DeploymentSucceeded, SignalPayload::Deployment(payload)) => {
                validate_identity("environment", &payload.environment)?;
                validate_identity("service", &payload.service)?;
                require_matching(
                    &self.correlation.environment,
                    &payload.environment,
                    "environment",
                )?;
                require_matching(&self.correlation.service, &payload.service, "service")?;
                if let Some(version) = &payload.version {
                    validate_identity("version", version)?;
                    reject_secret_like("payload", version)?;
                }
            }
            (
                SignalKind::ProductionAlertFiring | SignalKind::ProductionAlertResolved,
                SignalPayload::ProductionAlert(payload),
            ) => {
                validate_identity("environment", &payload.environment)?;
                validate_identity("service", &payload.service)?;
                validate_identity("alert_key", &payload.alert_key)?;
                validate_identity("state", &payload.state)?;
                require_matching(
                    &self.correlation.environment,
                    &payload.environment,
                    "environment",
                )?;
                require_matching(&self.correlation.service, &payload.service, "service")?;
                require_matching(&self.correlation.alert_key, &payload.alert_key, "alert_key")?;
                reject_secret_like("payload", &payload.state)?;
                if let Some(severity) = &payload.severity {
                    validate_identity("severity", severity)?;
                    reject_secret_like("payload", severity)?;
                }
            }
            _ => return Err(SignalValidationError::PayloadKindMismatch),
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
    state: CanonicalStatePayload<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum CanonicalStatePayload<'a> {
    Ci {
        repo: &'a str,
        branch: &'a str,
        workflow: &'a str,
        job: &'a str,
        commit_sha: &'a str,
        status: &'a str,
        conclusion: &'a Option<String>,
    },
    Deployment {
        environment: &'a str,
        service: &'a str,
        version: &'a Option<String>,
    },
    ProductionAlert {
        environment: &'a str,
        service: &'a str,
        alert_key: &'a str,
        state: &'a str,
        severity: &'a Option<String>,
    },
}

impl<'a> From<&'a SignalEnvelope> for CanonicalState<'a> {
    fn from(envelope: &'a SignalEnvelope) -> Self {
        let state = match &envelope.payload {
            SignalPayload::Ci(payload) => CanonicalStatePayload::Ci {
                repo: envelope.correlation.repo.as_deref().unwrap_or_default(),
                branch: envelope.correlation.branch.as_deref().unwrap_or_default(),
                workflow: envelope.correlation.workflow.as_deref().unwrap_or_default(),
                job: envelope.correlation.job.as_deref().unwrap_or_default(),
                commit_sha: envelope
                    .correlation
                    .commit_sha
                    .as_deref()
                    .unwrap_or_default(),
                status: &payload.status,
                conclusion: &payload.conclusion,
            },
            SignalPayload::Deployment(payload) => CanonicalStatePayload::Deployment {
                environment: &payload.environment,
                service: &payload.service,
                version: &payload.version,
            },
            SignalPayload::ProductionAlert(payload) => CanonicalStatePayload::ProductionAlert {
                environment: &payload.environment,
                service: &payload.service,
                alert_key: &payload.alert_key,
                state: &payload.state,
                severity: &payload.severity,
            },
        };
        Self {
            kind: &envelope.kind,
            source: &envelope.source,
            state,
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
    #[error("signal kind does not match payload family")]
    PayloadKindMismatch,
    #[error("payload field does not match correlation: {0}")]
    PayloadCorrelationMismatch(&'static str),
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

fn require_matching(
    correlation: &Option<String>,
    payload: &str,
    field: &'static str,
) -> Result<(), SignalValidationError> {
    require(correlation, field)?;
    if correlation.as_deref() == Some(payload) {
        Ok(())
    } else {
        Err(SignalValidationError::PayloadCorrelationMismatch(field))
    }
}

fn reject_secret_like(kind: &'static str, key: &str) -> Result<(), SignalValidationError> {
    let lower = key.to_ascii_lowercase();
    if is_url_like(&lower) {
        if url_contains_secret_like_parts(key) {
            return Err(SignalValidationError::SecretLikeField(
                kind,
                "<redacted>".into(),
            ));
        }
        return Ok(());
    }

    if contains_secret_word(&lower) {
        return Err(SignalValidationError::SecretLikeField(
            kind,
            "<redacted>".into(),
        ));
    }

    match percent_decode_ascii_fixed_point(key) {
        Ok(normalized) if normalized != lower && contains_secret_word(&normalized) => {
            return Err(SignalValidationError::SecretLikeField(
                kind,
                "<redacted>".into(),
            ));
        }
        Err(()) => {
            return Err(SignalValidationError::SecretLikeField(
                kind,
                "<redacted>".into(),
            ));
        }
        _ => {}
    }

    Ok(())
}

fn is_url_like(value: &str) -> bool {
    value.contains("://") || value.starts_with("//")
}

fn contains_secret_word(value: &str) -> bool {
    let secret_words = [
        "secret",
        "token",
        "bearer",
        "api_key",
        "apikey",
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
    secret_words.iter().any(|word| value.contains(word))
}

fn url_contains_secret_like_parts(value: &str) -> bool {
    let normalized = match percent_decode_ascii_fixed_point(value) {
        Ok(normalized) => normalized,
        Err(()) => return true,
    };
    url_contains_userinfo(value)
        || url_contains_userinfo(&normalized)
        || url_contains_sensitive_query_name(value)
        || url_contains_sensitive_query_name(&normalized)
}

fn url_contains_userinfo(value: &str) -> bool {
    let Some(authority_start) = authority_start(value) else {
        return false;
    };
    value[authority_start..]
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default()
        .contains('@')
}

fn url_contains_sensitive_query_name(value: &str) -> bool {
    let Some(query) = value.split_once('?').map(|(_, query)| query) else {
        return false;
    };
    let query = query.split('#').next().unwrap_or_default();
    query.split('&').any(|part| {
        let name = part.split(['=', ';']).next().unwrap_or_default();
        let name = match percent_decode_ascii_fixed_point(name) {
            Ok(name) => name,
            Err(()) => return true,
        };
        let name = name.split(['=', ';']).next().unwrap_or_default();
        is_sensitive_parameter_name(name)
    })
}

fn authority_start(value: &str) -> Option<usize> {
    if value.starts_with("//") {
        Some(2)
    } else {
        value.find("://").map(|index| index + 3)
    }
}

fn is_sensitive_parameter_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "secret"
            | "token"
            | "access_token"
            | "refresh_token"
            | "api_key"
            | "apikey"
            | "password"
            | "authorization"
            | "cookie"
            | "credential"
    )
}

fn percent_decode_ascii_fixed_point(value: &str) -> Result<String, ()> {
    const MAX_PASSES: usize = 8;
    let mut current = value.to_ascii_lowercase();
    for _ in 0..MAX_PASSES {
        let next = percent_decode_ascii_once_bounded(&current)?;
        if next == current {
            return Ok(next);
        }
        current = next;
    }
    if percent_decode_ascii_once_bounded(&current)? == current {
        Ok(current)
    } else {
        Err(())
    }
}

fn percent_decode_ascii_once_bounded(value: &str) -> Result<String, ()> {
    const MAX_NORMALIZED_BYTES: usize = 4096;

    let bytes = value.as_bytes();
    let mut normalized = String::with_capacity(value.len().min(MAX_NORMALIZED_BYTES));
    let mut index = 0;
    while index < bytes.len() && normalized.len() < MAX_NORMALIZED_BYTES {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let (Some(high), Some(low)) =
                (hex_value(bytes[index + 1]), hex_value(bytes[index + 2]))
            {
                normalized.push(char::from((high << 4) | low).to_ascii_lowercase());
                index += 3;
                continue;
            }
        }

        normalized.push(char::from(bytes[index]).to_ascii_lowercase());
        index += 1;
    }

    if index == bytes.len() {
        Ok(normalized)
    } else {
        Err(())
    }
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

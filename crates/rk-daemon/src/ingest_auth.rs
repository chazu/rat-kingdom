use rk_core::config::IngestConfig;
use rk_core::sdlc::{ConfiguredSourceName, SignalEnvelope, SignalLimits};
use serde::Deserialize;
use std::collections::HashSet;

pub const SOURCE_CALLER_PREFIX: &str = "source:";

#[derive(Debug, Clone)]
pub struct SourcePrincipal {
    pub name: String,
    allowed_kinds: HashSet<String>,
    max_state_limit: usize,
    limits: SignalLimits,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestEventParams {
    pub source: String,
    pub envelope: SignalEnvelope,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestStateParams {
    #[serde(default)]
    pub repo: Option<String>,
    #[serde(default)]
    pub environment: Option<String>,
    #[serde(default)]
    pub service: Option<String>,
    #[serde(default)]
    pub alert_key: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

impl SourcePrincipal {
    pub fn from_request(
        config: &IngestConfig,
        root_token: &str,
        caller: &str,
        auth: &str,
    ) -> Option<Self> {
        let name = caller.strip_prefix(SOURCE_CALLER_PREFIX)?;
        let source = config
            .sources
            .iter()
            .find(|source| source.name == name && source.enabled)?;
        if source.token_derivation != "source" {
            return None;
        }
        if auth != rk_core::paths::derive_agent_token(root_token, &format!("source:{name}")) {
            return None;
        }
        let allowed_kinds = source.allowed_kinds.iter().cloned().collect();
        Some(Self {
            name: name.to_string(),
            allowed_kinds,
            max_state_limit: source.max_state_limit.min(config.max_state_limit),
            limits: SignalLimits {
                max_summary_len: source.max_summary_len.min(config.max_summary_len),
                max_refs: source.max_refs.min(config.max_refs),
                max_attributes: source.max_attributes.min(config.max_attributes),
            },
        })
    }

    pub fn allows_kind(&self, kind: &str) -> bool {
        self.allowed_kinds.contains(kind)
    }

    pub fn max_state_limit(&self) -> usize {
        self.max_state_limit
    }

    pub fn limits(&self) -> &SignalLimits {
        &self.limits
    }

    pub fn configured_source(&self) -> Result<ConfiguredSourceName, rk_core::sdlc::SignalValidationError> {
        ConfiguredSourceName::new(self.name.clone())
    }
}

pub fn derive_source_token(root_token: &str, name: &str) -> String {
    rk_core::paths::derive_agent_token(root_token, &format!("source:{name}"))
}

pub fn source_caller(name: &str) -> String {
    format!("{SOURCE_CALLER_PREFIX}{name}")
}

pub fn validate_event(
    principal: &SourcePrincipal,
    params: &IngestEventParams,
) -> Result<(), String> {
    if params.source != principal.name {
        return Err("requested source does not match authenticated principal".into());
    }
    if params.envelope.source.as_str() != principal.name {
        return Err("envelope source does not match authenticated principal".into());
    }
    let kind = serde_json::to_string(&params.envelope.kind)
        .map_err(|_| "invalid signal kind".to_string())?
        .trim_matches('"')
        .to_string();
    if !principal.allows_kind(&kind) {
        return Err(format!(
            "source {} may not ingest kind {}",
            principal.name, kind
        ));
    }
    params.envelope.validate(principal.limits()).map_err(|e| e.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::IngestStateParams;
    use serde_json::json;

    #[test]
    fn ingest_state_params_reject_unknown_fields() {
        let err = serde_json::from_value::<IngestStateParams>(
            json!({"limit": 5, "principal": "operator"}),
        )
        .expect_err("ingest.state params must reject unexpected fields");

        assert!(
            err.to_string().contains("unknown field `principal`"),
            "expected unknown field error, got: {err}"
        );
    }
}

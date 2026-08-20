//! The authority ladder: how a castle's declared policy
//! (`rk_core::config::PolicyConfig`) governs what an attention item
//! (`crate::reconcile::Violation`, surfaced through `crate::attention`) may
//! trigger without a human in the loop.
//!
//! Conservative by construction: an override can only NARROW a violation
//! kind's built-in authority toward [`Authority::Human`] — validated here,
//! at load time, fail-closed on any attempt to widen it — and the
//! orchestrator action allowlist defaults to empty. Both are read once, from
//! `config.toml`, when the daemon starts; no RPC method writes either, so an
//! orchestrator session cannot widen its own authority at runtime, no matter
//! what it does over the wire.

use crate::reconcile::{builtin_authority, Authority, Violation};
use rk_core::config::PolicyConfig;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct AuthorityPolicy {
    overrides: HashMap<String, Authority>,
    orchestrator_allowlist: HashSet<String>,
    pub rate_cap: crate::recovery::RateCap,
    pub lease_ttl_secs: i64,
}

impl AuthorityPolicy {
    /// Build the effective policy from a loaded `[policy]` section,
    /// rejecting fail-closed any override that would grant a violation kind
    /// MORE autonomy than its built-in default, or that names a kind or
    /// authority value this build does not recognize. A typo here must not
    /// silently fall back to "no override" — that would look like a widening
    /// refusal but actually just be an ignored config line.
    pub fn from_config(cfg: &PolicyConfig) -> rk_core::Result<Self> {
        let mut overrides = HashMap::new();
        for (kind, value) in &cfg.authority_overrides {
            let Some(builtin) = builtin_authority(kind) else {
                return Err(rk_core::Error::Config(format!(
                    "policy.authority_overrides names an unknown convergence violation kind: {kind}"
                )));
            };
            let Some(requested) = Authority::parse(value) else {
                return Err(rk_core::Error::Config(format!(
                    "policy.authority_overrides.{kind} is not mechanical|orchestrator|human: {value}"
                )));
            };
            if requested.rank() < builtin.rank() {
                return Err(rk_core::Error::Config(format!(
                    "policy.authority_overrides.{kind} widens {kind} from its built-in {builtin:?} to {requested:?} — an override may only narrow toward human"
                )));
            }
            overrides.insert(kind.clone(), requested);
        }
        Ok(Self {
            overrides,
            orchestrator_allowlist: cfg.orchestrator_action_allowlist.iter().cloned().collect(),
            rate_cap: crate::recovery::RateCap {
                max: cfg.orchestrator_rate_cap,
                window: Duration::from_secs(cfg.orchestrator_rate_window_secs),
            },
            lease_ttl_secs: cfg.orchestrator_lease_ttl_secs,
        })
    }

    /// The authority a violation is actually resolved under: the configured
    /// override for its kind if one exists (already validated as
    /// narrow-only), else `reconcile::build`'s own assignment.
    pub fn effective_authority(&self, v: &Violation) -> Authority {
        self.overrides.get(&v.kind).copied().unwrap_or(v.authority)
    }

    /// Whether an orchestrator-authority decision may resolve this violation
    /// kind at all. Conservative default (empty allowlist) means every
    /// orchestrator-classified item stays unresolved until a human names its
    /// kind here explicitly.
    pub fn orchestrator_may_act(&self, kind: &str) -> bool {
        self.orchestrator_allowlist.contains(kind)
    }
}

impl Default for AuthorityPolicy {
    fn default() -> Self {
        Self::from_config(&PolicyConfig::default())
            .expect("the default policy config is always a valid authority policy")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reconcile::kind;

    #[test]
    fn narrowing_an_override_toward_human_is_accepted() {
        let mut cfg = PolicyConfig::default();
        cfg.authority_overrides
            .insert(kind::DELIVERED_BUT_OPEN.into(), "human".into());
        let policy = AuthorityPolicy::from_config(&cfg).unwrap();
        assert_eq!(
            policy.overrides.get(kind::DELIVERED_BUT_OPEN).copied(),
            Some(Authority::Human)
        );
    }

    #[test]
    fn widening_an_override_is_rejected() {
        let mut cfg = PolicyConfig::default();
        // TRACKER_CONTRADICTS_GIT's built-in is Human; Orchestrator is a widen.
        cfg.authority_overrides
            .insert(kind::TRACKER_CONTRADICTS_GIT.into(), "orchestrator".into());
        let err = AuthorityPolicy::from_config(&cfg).unwrap_err().to_string();
        assert!(err.contains("widens"), "{err}");
    }

    #[test]
    fn an_unknown_kind_override_is_rejected() {
        let mut cfg = PolicyConfig::default();
        cfg.authority_overrides
            .insert("not-a-real-kind".into(), "human".into());
        let err = AuthorityPolicy::from_config(&cfg).unwrap_err().to_string();
        assert!(err.contains("unknown"), "{err}");
    }

    #[test]
    fn an_unknown_authority_value_is_rejected() {
        let mut cfg = PolicyConfig::default();
        cfg.authority_overrides
            .insert(kind::DELIVERED_BUT_OPEN.into(), "godmode".into());
        let err = AuthorityPolicy::from_config(&cfg).unwrap_err().to_string();
        assert!(err.contains("mechanical|orchestrator|human"), "{err}");
    }

    #[test]
    fn default_policy_denies_every_orchestrator_kind() {
        let policy = AuthorityPolicy::default();
        assert!(!policy.orchestrator_may_act(kind::TERMINAL_ASSIGNEE_ACTIVE_WORK));
        assert!(!policy.orchestrator_may_act(kind::CONFLICT_HELD_LANDING));
    }

    #[test]
    fn an_allowlisted_kind_is_permitted() {
        let mut cfg = PolicyConfig::default();
        cfg.orchestrator_action_allowlist
            .push(kind::TERMINAL_ASSIGNEE_ACTIVE_WORK.into());
        let policy = AuthorityPolicy::from_config(&cfg).unwrap();
        assert!(policy.orchestrator_may_act(kind::TERMINAL_ASSIGNEE_ACTIVE_WORK));
        assert!(!policy.orchestrator_may_act(kind::CONFLICT_HELD_LANDING));
    }
}

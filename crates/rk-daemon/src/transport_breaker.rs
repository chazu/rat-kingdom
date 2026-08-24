//! Castle-wide, per-provider circuit breaker for pre-work harness transport
//! outages (TKT-01M0HND8M25GYN1ZTRET3S5769).
//!
//! Durable — a daemon restart must not silently re-open a breaker that was
//! protecting a genuinely down provider — and shared across every agent of
//! that provider, not just the one generation that tripped it: a castle-wide
//! outage should refuse every claude (or every codex) launch, not just the
//! unlucky first one.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProviderBreakerState {
    pub consecutive_failures: u32,
    /// Set (and re-armed, extending the cooldown) on every failure once the
    /// breaker has tripped; `None` means closed. Recovery is age-based
    /// rather than an explicit half-open state: [`TransportBreakers::is_open`]
    /// answers "still open" purely from this timestamp and the configured
    /// cooldown, so it closes itself the instant enough quiet time passes —
    /// no separate timer or sweep action is needed to reopen for a trial.
    pub opened_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TransportBreakers {
    #[serde(skip)]
    path: PathBuf,
    providers: HashMap<String, ProviderBreakerState>,
}

impl TransportBreakers {
    pub fn load(path: &Path) -> rk_core::Result<Self> {
        let mut breakers: Self = if path.exists() {
            let data = std::fs::read_to_string(path)?;
            serde_json::from_str(&data)?
        } else {
            Self::default()
        };
        breakers.path = path.to_path_buf();
        Ok(breakers)
    }

    fn persist(&self) -> rk_core::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(self)?)?;
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }

    /// Whether `provider` currently refuses new launches. `cooldown_secs ==
    /// 0` disables the breaker outright (never open).
    pub fn is_open(&self, provider: &str, now: DateTime<Utc>, cooldown_secs: u64) -> bool {
        if cooldown_secs == 0 {
            return false;
        }
        self.providers
            .get(provider)
            .and_then(|s| s.opened_at)
            .is_some_and(|opened_at| {
                ((now - opened_at).num_seconds().max(0) as u64) < cooldown_secs
            })
    }

    /// Record one castle-wide pre-work transport failure for `provider`.
    /// Trips (or re-arms, extending the cooldown) once
    /// `consecutive_failures` reaches `threshold`. `threshold == 0` disables
    /// the breaker (failures still count, but it never trips).
    pub fn record_failure(&mut self, provider: &str, threshold: u32, now: DateTime<Utc>) {
        let state = self.providers.entry(provider.to_string()).or_default();
        state.consecutive_failures += 1;
        if threshold > 0 && state.consecutive_failures >= threshold {
            state.opened_at = Some(now);
        }
        let _ = self.persist();
    }

    /// A generation of `provider` reached `Started` — proof of life. Closes
    /// the breaker and resets its failure streak.
    pub fn record_success(&mut self, provider: &str) {
        if let Some(state) = self.providers.get_mut(provider) {
            if state.consecutive_failures != 0 || state.opened_at.is_some() {
                state.consecutive_failures = 0;
                state.opened_at = None;
                let _ = self.persist();
            }
        }
    }

    #[cfg(test)]
    pub fn consecutive_failures(&self, provider: &str) -> u32 {
        self.providers
            .get(provider)
            .map(|s| s.consecutive_failures)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trips_open_at_threshold_and_recovers_after_cooldown() {
        let mut breakers = TransportBreakers::default();
        let t0 = Utc::now();
        breakers.record_failure("claude", 3, t0);
        breakers.record_failure("claude", 3, t0);
        assert!(!breakers.is_open("claude", t0, 60));
        breakers.record_failure("claude", 3, t0);
        assert!(breakers.is_open("claude", t0, 60), "trips at threshold");
        assert!(
            !breakers.is_open("claude", t0 + chrono::Duration::seconds(61), 60),
            "closes once the cooldown elapses"
        );
    }

    #[test]
    fn is_open_is_per_provider() {
        let mut breakers = TransportBreakers::default();
        let t0 = Utc::now();
        breakers.record_failure("claude", 1, t0);
        assert!(breakers.is_open("claude", t0, 60));
        assert!(!breakers.is_open("codex", t0, 60));
    }

    #[test]
    fn success_closes_and_resets_the_streak() {
        let mut breakers = TransportBreakers::default();
        let t0 = Utc::now();
        breakers.record_failure("claude", 2, t0);
        breakers.record_failure("claude", 2, t0);
        assert!(breakers.is_open("claude", t0, 60));
        breakers.record_success("claude");
        assert!(!breakers.is_open("claude", t0, 60));
        assert_eq!(breakers.consecutive_failures("claude"), 0);
    }

    #[test]
    fn continued_failure_while_open_extends_the_cooldown() {
        let mut breakers = TransportBreakers::default();
        let t0 = Utc::now();
        breakers.record_failure("claude", 1, t0);
        assert!(breakers.is_open("claude", t0, 60));
        let t1 = t0 + chrono::Duration::seconds(30);
        breakers.record_failure("claude", 1, t1);
        // Had it not re-armed, 61s past t0 would already be closed.
        assert!(breakers.is_open("claude", t0 + chrono::Duration::seconds(61), 60));
    }

    #[test]
    fn zero_threshold_never_trips() {
        let mut breakers = TransportBreakers::default();
        let t0 = Utc::now();
        for _ in 0..10 {
            breakers.record_failure("claude", 0, t0);
        }
        assert!(!breakers.is_open("claude", t0, 60));
    }

    #[test]
    fn zero_cooldown_disables_the_breaker() {
        let mut breakers = TransportBreakers::default();
        let t0 = Utc::now();
        breakers.record_failure("claude", 1, t0);
        assert!(!breakers.is_open("claude", t0, 0));
    }

    #[test]
    fn persists_and_reloads_across_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("transport_breaker.json");
        let t0 = Utc::now();
        {
            let mut breakers = TransportBreakers::load(&path).unwrap();
            breakers.record_failure("codex", 1, t0);
        }
        let reloaded = TransportBreakers::load(&path).unwrap();
        assert!(reloaded.is_open("codex", t0, 60));
    }
}

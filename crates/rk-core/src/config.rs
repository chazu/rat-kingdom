//! Layered configuration: defaults < `config.toml` < `RK_*` environment.

use figment::providers::{Env, Format, Serialized, Toml};
use figment::Figment;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct Config {
    /// Castle (machine/instance) name; defaults to the hostname at first run.
    pub castle_name: Option<String>,
    pub log: LogConfig,
    pub harness: HarnessConfig,
    pub budget: BudgetConfig,
    pub sync: SyncConfig,
    pub reactor: ReactorConfig,
    /// Named agent profiles: [agents.<name>] harness/model/permission_mode.
    /// The "default" profile applies to all spawns that name no profile.
    pub agents: std::collections::HashMap<String, AgentProfileConfig>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct AgentProfileConfig {
    pub harness: Option<String>,
    pub model: Option<String>,
    pub permission_mode: Option<String>,
}

/// Multiplayer sync via git notes in the RK_HOME state repo.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct SyncConfig {
    pub enabled: bool,
    /// Git remote URL for the shared sync repo.
    pub remote_url: Option<String>,
    pub interval_secs: u64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            remote_url: None,
            interval_secs: 30,
        }
    }
}

/// The daemon tuple-reactor: reactions that fire workflows when tuples matching
/// a registered `#Trigger` land in the space. The live feed is only a wake
/// signal; dispatch is driven by a durable cursor scan so no event is missed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct ReactorConfig {
    /// Master switch. When false the reactor loop never starts.
    pub enabled: bool,
    /// Fallback scan cadence. Feed events also wake a cycle; this bounds the
    /// worst-case latency if the lossy feed drops the waking event entirely.
    pub interval_secs: u64,
    /// Rolling window (seconds) over which a trigger's fires are rate-capped.
    pub window_secs: u64,
    /// Default per-trigger fire cap within `window_secs`; a `#Trigger` may lower
    /// it with `maxFires`. Bounded to <=100 to mirror the `repeat` discipline.
    pub max_fires: u32,
    /// How long an idempotency marker (one per fired `(trigger, tuple)`) lives.
    /// Must outlast any at-least-once redelivery; defaults to a week.
    pub marker_ttl_secs: u64,
    /// Tuple authors the reactor never reacts to, in addition to its own output
    /// (always excluded). Use this to break re-entrancy from known agents.
    pub exclude_instances: Vec<String>,
    /// Distinct-endorser count at which a `Suggestion` is promoted to a
    /// `Convention`. Counted per suggestion at scan time (not off the lossy
    /// feed), so a proposal that misses quorum before its endorsements decay
    /// simply never promotes. Zero disables quorum promotion entirely.
    pub quorum: u32,
}

impl Default for ReactorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 30,
            window_secs: 60,
            max_fires: 20,
            marker_ttl_secs: 7 * 24 * 3600,
            exclude_instances: Vec::new(),
            quorum: 3,
        }
    }
}

/// Per-agent budget caps. Zero = unlimited.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct BudgetConfig {
    pub max_usd: f64,
    pub max_tokens: u64,
    /// Fraction of a cap at which the warning fires.
    pub warn_at: f64,
}

impl Default for BudgetConfig {
    fn default() -> Self {
        Self {
            max_usd: 0.0,
            max_tokens: 0,
            warn_at: 0.8,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct LogConfig {
    /// tracing env-filter, e.g. "info" or "rk_daemon=debug,info".
    pub filter: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            filter: "info".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct HarnessConfig {
    /// Default harness kind for spawned rats: "claude" | "codex" | "axe".
    pub default: String,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        Self {
            default: "claude".into(),
        }
    }
}

impl Config {
    /// Load config layered from defaults, an optional TOML file, and
    /// `RK_CONFIG_*` env vars (nested keys split on `_`, e.g.
    /// `RK_CONFIG_LOG_FILTER`). The prefix is deliberately NOT plain `RK_`:
    /// runtime identity vars (RK_AGENT, RK_LOG, RK_HOME...) must never leak
    /// into config parsing.
    pub fn load(config_file: &Path) -> crate::Result<Self> {
        Figment::from(Serialized::defaults(Config::default()))
            .merge(Toml::file(config_file))
            .merge(Env::prefixed("RK_CONFIG_").split("_"))
            .extract()
            .map_err(|e| crate::Error::Config(e.to_string()))
    }

    /// The effective castle name (config override or hostname).
    pub fn castle_name(&self) -> String {
        if let Some(name) = &self.castle_name {
            return name.clone();
        }
        hostname()
    }
}

fn hostname() -> String {
    std::process::Command::new("hostname")
        .arg("-s")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "castle".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_load_without_a_file() {
        let cfg = Config::load(Path::new("/nonexistent/config.toml")).unwrap();
        assert_eq!(cfg, Config::default());
        assert_eq!(cfg.harness.default, "claude");
    }

    #[test]
    fn toml_file_overrides_defaults() {
        let dir = std::env::temp_dir().join(format!("rk-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("config.toml");
        std::fs::write(
            &file,
            "castle_name = \"burrow\"\n[log]\nfilter = \"debug\"\n",
        )
        .unwrap();
        let cfg = Config::load(&file).unwrap();
        assert_eq!(cfg.castle_name.as_deref(), Some("burrow"));
        assert_eq!(cfg.log.filter, "debug");
        std::fs::remove_dir_all(&dir).ok();
    }
}

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
    pub scheduler: SchedulerConfig,
    pub supervisor: SupervisorConfig,
    pub drain: DrainConfig,
    pub evaporation: EvaporationConfig,
    pub policy: PolicyConfig,
    /// Named agent profiles: [agents.<name>] harness/model/permission_mode.
    /// The "default" profile applies to all spawns that name no profile.
    pub agents: std::collections::HashMap<String, AgentProfileConfig>,
    /// Cost-tier routing: map ticket labels/priority to an agent-profile name.
    /// Drives fan-out spawns onto cheap or premium tiers so a fixed budget runs
    /// a wider fleet. See [`TierRoutingConfig`].
    pub tiers: TierRoutingConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct AgentProfileConfig {
    pub harness: Option<String>,
    pub model: Option<String>,
    pub permission_mode: Option<String>,
}

/// Global cost-tier routing table: `[[tiers.rules]]` in config.toml. Each rule
/// maps a ticket's labels/priority to a `tier` (an `[agents.<tier>]` profile
/// name). First matching rule wins.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct TierRoutingConfig {
    pub rules: Vec<TierRuleConfig>,
}

/// One tier routing rule. `priority` and `label` are AND'd; either unset means
/// "any". Both unset is an unconditional catch-all.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct TierRuleConfig {
    pub priority: Option<String>,
    pub label: Option<String>,
    pub tier: String,
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
    /// Distinct-reporter count at which repeated obstacles/needs on one
    /// normalised topic are coalesced into a single durable ticket. Counted per
    /// topic at scan time, so a hot wall that many rats hit files exactly one
    /// backlog item. Zero disables obstacle coalescence entirely.
    pub coalesce_quorum: u32,
    /// Active operator push: when the steward escalates a STOP/unknown verdict
    /// via a `need` (identity `steward`), fire a desktop notification through
    /// herdr so the operator is pushed at, not only queued in `rk inbox`. A
    /// no-op when no herdr server is running, so it never blocks a headless
    /// castle. Set false to keep escalations purely on the passive inbox queue.
    pub notify_escalations: bool,
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
            coalesce_quorum: 3,
            notify_escalations: true,
        }
    }
}

/// The daemon scheduler: fires workflows on a cron cadence, adding the TIME axis
/// to autonomy (groom/drain/prompt-refine with zero operator initiation). A
/// scheduled fire is a time-sourced trigger reusing the reactor's dispatch path
/// (`engine.run`). Overlap is prevented by a per-schedule single-flight guard, so
/// a slow nightly drain never stacks on itself.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct SchedulerConfig {
    /// Master switch. When false the scheduler loop never starts.
    pub enabled: bool,
    /// How often the scheduler wakes to check whether a cron minute has elapsed.
    /// Clamped to [1, 60] by the loop: it must tick at least once a minute so a
    /// matching minute is never skipped.
    pub interval_secs: u64,
    /// Bound on catch-up after downtime: on boot (or after a long stall) the
    /// scheduler looks back at most this many minutes for missed cron minutes,
    /// firing each schedule at most once. Zero means no catch-up — only the
    /// current minute is evaluated (plain-cron semantics, like `cron` without
    /// `anacron`).
    pub catchup_minutes: u64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 30,
            // A day of catch-up: a daemon down overnight still runs each missed
            // daily/hourly schedule once on the next boot, without replaying a
            // week of minutes.
            catchup_minutes: 24 * 60,
        }
    }
}

/// Supervisor liveness/burn-rate sweep. A periodic pass over live headless rats
/// that flags ones which have gone silent (STUCK) or are burning cost with no
/// completion in sight (RUNNING AWAY), and applies the same graduated response
/// as the budget machinery: obstacle tuple -> steer -> kill after a grace window.
/// Budget checks fire only on Usage events, so a rat hung mid-tool-call emitting
/// nothing is invisible to them; this sweep is the out-of-band liveness probe.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct SupervisorConfig {
    /// Master switch. When false the sweep loop never starts.
    pub enabled: bool,
    /// Sweep cadence.
    pub interval_secs: u64,
    /// A live rat whose last event (usage/started) is older than this is STUCK.
    /// Zero disables stuck detection. Kept generous: a soft steer fires first,
    /// so a legitimately-slow silent step (compile/test) is nudged, not killed.
    pub stuck_after_secs: u64,
    /// Sustained burn (USD/minute across sweeps) above this is RUNNING AWAY.
    /// Zero disables burn detection (off by default — it needs per-harness and
    /// per-pricing tuning, and absolute cost caps already live in `budget`).
    pub burn_usd_per_min: f64,
    /// After the first (soft) flag steers the rat, how long to wait before
    /// escalating to a kill if it is STILL flagged. Prefer steer-then-wait.
    pub kill_grace_secs: u64,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 60,
            stuck_after_secs: 900,
            burn_usd_per_min: 0.0,
            kill_grace_secs: 600,
        }
    }
}

/// The continuous-drain controller: a WIP-limited fleet autoscaler. Maintains a
/// target live-agent concurrency (`max_wip`) by continuously claiming the
/// highest-priority ready ticket and spawning a rat whenever the fleet has a
/// free slot — the always-on refill counterpart to a one-shot backlog-drain
/// workflow, turning "keep the fleet busy" from one operator spawn per ticket
/// into a single config dial. Combined with the steward closing each merged
/// item it is a closed loop: the operator grooms/prioritises, the fleet
/// executes. Off by default: the per-spawn fleet/repo budget cap (the wallet
/// kill-switch) and the liveness sweep (which reaps stuck rats, freeing slots)
/// are its safety net, but enabling it still hands the dispatch loop to the
/// daemon, so it is opt-in.
///
/// `max_wip` is the fleet-wide concurrency ceiling. The optional [`repos`] map
/// partitions that ceiling per repo (per-repo enable + cap dials) so a single
/// busy repo cannot monopolize the whole fleet.
///
/// [`repos`]: DrainConfig::repos
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct DrainConfig {
    /// Master switch. Off by default — turning it on makes the fleet
    /// self-dispatch from the ready backlog with no operator in the loop.
    pub enabled: bool,
    /// Target concurrency W: the controller keeps up to this many rats live,
    /// spawning the highest-priority ready ticket whenever a slot frees. Zero
    /// (the default) also disables the loop, so `enabled` alone is inert until a
    /// cap is set.
    pub max_wip: usize,
    /// Fallback refill cadence (seconds). A freed slot — a completion or
    /// dismissal — also wakes a refill through the tuple feed; this bounds the
    /// worst-case latency if that wake is dropped, and paces retries while the
    /// backlog is empty or the budget cap is holding dispatch.
    pub interval_secs: u64,
    /// Restrict dispatch to a single repo scope (by name). Unset drains every
    /// registered repo's ready backlog; system-scope tickets, which resolve to
    /// no registered repo, are never dispatched. Ignored when [`repos`] is set —
    /// the per-repo partition map takes precedence.
    ///
    /// [`repos`]: DrainConfig::repos
    pub repo: Option<String>,
    /// Cross-repo WIP partition: per-repo enable/cap dials keyed by repo name.
    /// When non-empty this becomes an **allowlist** — only listed, enabled repos
    /// are drained (`repo` above is ignored) — and each entry's `max_wip`
    /// subdivides the fleet-wide [`max_wip`] ceiling, so one busy repo cannot
    /// monopolize the whole fleet. Empty (the default) keeps the fleet-wide
    /// behaviour: every registered repo (or the single `repo` pin) competes for
    /// one shared budget. See [`RepoDrainConfig`].
    ///
    /// [`max_wip`]: DrainConfig::max_wip
    #[serde(default)]
    pub repos: std::collections::HashMap<String, RepoDrainConfig>,
    /// Priority aging: seconds of waiting that buy one level of effective
    /// priority boost, so a low-priority ticket cannot starve behind a steady
    /// stream of higher-priority work. Zero disables aging (strict priority,
    /// oldest ticket first).
    pub aging_secs: u64,
}

/// Per-repo dial in the cross-repo WIP partition ([`DrainConfig::repos`]). A
/// `[drain.repos.<name>]` table caps how much of the fleet-wide budget a single
/// repo may hold and can gate a repo off without unregistering it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct RepoDrainConfig {
    /// Per-repo master switch. A bare `[drain.repos.foo]` (or one that only sets
    /// a cap) stays enabled; set `false` to keep the repo in the allowlist shape
    /// while pausing its dispatch.
    pub enabled: bool,
    /// Max rats this repo may hold live at once. `0` (the default) means no
    /// per-repo cap — the repo is bounded only by the fleet-wide
    /// [`DrainConfig::max_wip`] ceiling. A positive cap partitions WIP: e.g.
    /// `max_wip=4` fleet-wide with two repos capped at `2` each guarantees
    /// neither starves the other however deep its backlog.
    pub max_wip: usize,
}

impl Default for RepoDrainConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_wip: 0,
        }
    }
}

impl Default for DrainConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_wip: 0,
            interval_secs: 30,
            repo: None,
            repos: std::collections::HashMap::new(),
            // An hour of waiting buys one priority level — a low ticket outranks
            // a fresh normal one after ~2h, a fresh high after ~3h.
            aging_secs: 3600,
        }
    }
}

/// Pheromone evaporation: how fast a refreshable trail (claim / obstacle / need)
/// loses strength when its author stops reinforcing it. Each GC cycle subtracts
/// `decay` from every trail's strength (starting at `FULL_STRENGTH` = 1.0) and
/// collects it at zero. With the 60s GC cadence the default `decay` gives an
/// unreinforced lifetime of ~30 minutes, matching the default claim TTL, so a
/// live agent's trail is unaffected while an abandoned one fades on its own.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct EvaporationConfig {
    pub decay: f64,
}

impl Default for EvaporationConfig {
    fn default() -> Self {
        // ~30 GC cycles (60s each) to fade from full → ~30 min.
        Self { decay: 1.0 / 30.0 }
    }
}

/// Workflow-execution policy. The seed of the #19 policy engine: today it gates
/// the one primitive that can run arbitrary shell — the workflow `run` step.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct PolicyConfig {
    /// When true, a workflow `run` step may ONLY reference a repo-registered
    /// named check (`check: "<name>"` resolved from `<repo>/.rk/checks.cue`); a
    /// raw inline `command` is refused fail-closed. This stops a compromised or
    /// untrusted workflow definition from executing arbitrary shell in an
    /// agent's worktree — it can invoke only the checks the repo owner declared.
    /// Defaults to false (raw commands allowed) for backward compatibility.
    pub require_named_checks: bool,
}

/// Budget caps. `max_usd`/`max_tokens` are per-agent (graduated warn→steer→kill
/// mid-run). `fleet_max_usd`/`repo_max_usd` are hierarchical caps layered above:
/// the SUM of all agents' cost fleet-wide and per-repo, enforced as a dispatch
/// preflight (once hit, new spawns are refused). Zero = unlimited.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct BudgetConfig {
    pub max_usd: f64,
    pub max_tokens: u64,
    /// Fraction of a cap at which the warning fires (per-agent and fleet/repo).
    pub warn_at: f64,
    /// Fleet-wide USD cap across every agent in every repo — the wallet
    /// kill-switch for continuous/nightly runs. Zero = unlimited.
    pub fleet_max_usd: f64,
    /// Per-repo USD cap across every agent in one repo. Zero = unlimited.
    pub repo_max_usd: f64,
}

impl Default for BudgetConfig {
    fn default() -> Self {
        Self {
            max_usd: 0.0,
            max_tokens: 0,
            warn_at: 0.8,
            fleet_max_usd: 0.0,
            repo_max_usd: 0.0,
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

    #[test]
    fn tier_rules_parse_from_toml() {
        let dir = std::env::temp_dir().join(format!("rk-cfg-tiers-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("config.toml");
        std::fs::write(
            &file,
            r#"
[agents.cheap]
harness = "axe"
model = "haiku"

[[tiers.rules]]
label = "mechanical"
tier = "cheap"

[[tiers.rules]]
priority = "high"
tier = "premium"

[[tiers.rules]]
tier = "cheap"
"#,
        )
        .unwrap();
        let cfg = Config::load(&file).unwrap();
        assert_eq!(cfg.tiers.rules.len(), 3);
        assert_eq!(cfg.tiers.rules[0].label.as_deref(), Some("mechanical"));
        assert_eq!(cfg.tiers.rules[0].tier, "cheap");
        assert_eq!(cfg.tiers.rules[1].priority.as_deref(), Some("high"));
        // A rule with neither predicate is the catch-all fallback.
        assert_eq!(cfg.tiers.rules[2].priority, None);
        assert_eq!(cfg.tiers.rules[2].label, None);
        std::fs::remove_dir_all(&dir).ok();
    }
}

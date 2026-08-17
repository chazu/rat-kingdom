//! Layered configuration: defaults < `config.toml` < `RK_*` environment.

use figment::providers::{Env, Format, Serialized, Toml};
use figment::Figment;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct Config {
    /// Operator-facing DISPLAY alias for this castle (e.g. "Nikaido"), shown in
    /// `rk status`/`rk top`, log lines, and tuple author columns via
    /// [`crate::identity::CastleDisplay`]. PRESENTATION-ONLY (TKT-124): the signed
    /// wire identity is always the Ed25519 actor id (`castle-<hex>`), so the alias
    /// never enters a `SyncRecord`, a git ref, arbitration, or trust. Unset ⇒ the
    /// actor id is shown verbatim (no behaviour change).
    pub castle_name: Option<String>,
    pub log: LogConfig,
    pub harness: HarnessConfig,
    pub budget: BudgetConfig,
    pub sync: SyncConfig,
    pub reactor: ReactorConfig,
    pub scheduler: SchedulerConfig,
    pub supervisor: SupervisorConfig,
    pub review_sweep: ReviewSweepConfig,
    pub worktree_sweep: WorktreeSweepConfig,
    pub disk: DiskConfig,
    pub drain: DrainConfig,
    pub evaporation: EvaporationConfig,
    pub ingest: IngestConfig,
    pub policy: PolicyConfig,
    /// Named agent profiles: [agents.<name>] harness/model/permission_mode.
    /// The "default" profile applies centrally to every ordinary spawn that
    /// does not override a field, including direct, nested, workflow, and drain
    /// dispatch. Onboarders keep their daemon-enforced restricted policy.
    pub agents: std::collections::HashMap<String, AgentProfileConfig>,
    /// Cost-tier routing: map ticket labels/priority to an agent-profile name.
    /// Drives fan-out spawns onto cheap or premium tiers so a fixed budget runs
    /// a wider fleet. See [`TierRoutingConfig`].
    pub tiers: TierRoutingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct IngestConfig {
    /// No default sources: every local ingest source must be operator-configured.
    pub sources: Vec<IngestSourceConfig>,
    /// Daemon-wide read cap for ingest.state.
    pub max_state_limit: usize,
    /// Daemon-wide ceiling for accepted SDLC signal summaries.
    pub max_summary_len: usize,
    /// Daemon-wide ceiling for accepted SDLC signal refs.
    pub max_refs: usize,
    /// Daemon-wide ceiling for accepted SDLC signal attributes.
    pub max_attributes: usize,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            sources: Vec::new(),
            max_state_limit: 1_000,
            max_summary_len: crate::sdlc::SignalLimits::default().max_summary_len,
            max_refs: crate::sdlc::SignalLimits::default().max_refs,
            max_attributes: crate::sdlc::SignalLimits::default().max_attributes,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct IngestSourceConfig {
    pub name: String,
    pub enabled: bool,
    pub allowed_kinds: Vec<String>,
    pub token_derivation: String,
    pub max_state_limit: usize,
    pub max_summary_len: usize,
    pub max_refs: usize,
    pub max_attributes: usize,
}

impl Default for IngestSourceConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            enabled: true,
            allowed_kinds: Vec::new(),
            token_derivation: "source".into(),
            max_state_limit: 100,
            max_summary_len: crate::sdlc::SignalLimits::default().max_summary_len,
            max_refs: crate::sdlc::SignalLimits::default().max_refs,
            max_attributes: crate::sdlc::SignalLimits::default().max_attributes,
        }
    }
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
    /// firing each schedule at most once. The runtime caps this at seven days
    /// so a malformed or extreme value cannot create an unbounded replay. Zero
    /// means no catch-up — only the current minute is evaluated (plain-cron
    /// semantics, like `cron` without `anacron`).
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

/// Fetch-driven awaiting-review clear (TKT-70). A periodic background pass that
/// `git fetch --prune`es each repo with an open PR/MR and checks whether the
/// forge has since merged or deleted the branch — advancing `<remote>/<target>`
/// where the operator's local target has not moved. On a forge-side merge/delete
/// it emits a `pull_request_closed` event, which `rk inbox` consults to drop the
/// stale awaiting-review row without waiting for a local pull.
///
/// Off by default and coarse-cadenced: a fetch touches the network and can hang,
/// so this stays opt-in and out of the hot inbox read path (the read path never
/// fetches; it only reads the events this sweep emitted).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct ReviewSweepConfig {
    /// Master switch. When false the sweep loop never starts and no fetch runs.
    pub enabled: bool,
    /// How often to fetch+prune each repo with open PRs and re-check the forge.
    /// Coarse by default — a forge merge is not time-critical and each cycle
    /// shells out to the network.
    pub interval_secs: u64,
    /// Remote to fetch and resolve `<remote>/<branch>` / `<remote>/<target>`
    /// against.
    pub remote: String,
    /// Hard timeout for a single `git fetch --prune`, so a stuck network fetch
    /// (unreachable host, missing credentials) cannot pin the sweep.
    pub fetch_timeout_secs: u64,
}

impl Default for ReviewSweepConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            // Five minutes: a forge merge takes time for a human to do, and the
            // fetch is a network cost we do not want to pay every few seconds.
            interval_secs: 300,
            remote: "origin".into(),
            fetch_timeout_secs: 30,
        }
    }
}

/// Periodic, unattended reclaim of git leftovers (worktree + local branch) for
/// terminal agent records whose branch has already landed or is gone — the
/// automated half of `rk prune --reap-git`, run on a timer instead of waiting
/// for an operator to remember it. Root-caused by the 2026-08-16 incident: 104
/// agent worktrees (298 GB, mostly `target/` dirs) leaked over ~3 weeks
/// because steward/workflow failure paths skip their own `dismiss` step, and
/// nothing ever swept the residue until the disk hit 97% full and daemon
/// writes started failing with "terminal state persistence failed: io".
///
/// Reuses `Supervisor::archive_agents`'s existing cutoff/reap machinery
/// (`rk-daemon` `supervisor.rs`) — this loop just fires it unattended. Safety
/// is unchanged from the manual path: `Supervisor::reap_git` only reclaims a
/// worktree whose branch is already merged into its target (or gone), and
/// only when the worktree itself has no uncommitted changes; either
/// condition failing leaves the worktree standing, reported, never deleted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct WorktreeSweepConfig {
    /// Master switch for the PERIODIC loop. When false the timer never
    /// starts and `rk prune` remains the only way to reclaim leaked
    /// worktrees. Independent of [`finalize_cleanup_enabled`](Self::finalize_cleanup_enabled)
    /// — the finalize-time guarantee below.
    pub enabled: bool,
    /// Sweep cadence.
    pub interval_secs: u64,
    /// A terminal agent record (Completed/Failed/Dismissed) untouched for at
    /// least this many days becomes eligible for archiving + git reclaim.
    pub after_days: u64,
    /// Master switch for the finalize-time safety net
    /// (`WorkflowEngine::finalize` → `Supervisor::dismiss_orphaned_instance_agents`):
    /// every terminalizing workflow instance dismisses (worktree-only,
    /// no-merge) any agent it spawned that reached Completed/Failed without
    /// going through its own `dismiss` step. This is the "guaranteed cleanup"
    /// half of TKT-01M04N6W4X47KMXDA6MH0WPH8H and is intentionally a
    /// SEPARATE toggle from `enabled` (the periodic timer): an operator who
    /// disables the hourly sweep should not lose the per-workflow guarantee.
    /// Defaults true for real deployments. Bare/test daemon constructors
    /// default this false (mirroring `enabled`'s existing test default) —
    /// left on unconditionally, it made every workflow-based e2e test in the
    /// workspace do extra synchronous git reclaim work at finalize time,
    /// adding enough load under `cargo test --workspace`'s full parallel run
    /// to tip unrelated tests' fixed polling timeouts over the edge
    /// (rework of TKT-01M04N6W4X47KMXDA6MH0WPH8H: two different steward gate
    /// failures, each passing standalone). Tests that specifically cover
    /// this guarantee opt back in explicitly via `set_worktree_sweep_config`.
    pub finalize_cleanup_enabled: bool,
}

impl Default for WorktreeSweepConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            // Hourly: leaked worktrees accumulate slowly (one per skipped
            // dismiss), so there is no benefit to a tighter cadence — this
            // just needs to run often enough that disk pressure never has a
            // multi-week window to build up again.
            interval_secs: 3600,
            after_days: 3,
            finalize_cleanup_enabled: true,
        }
    }
}

/// Disk-pressure preflight guard: refuse a new spawn (and thus a new
/// worktree) when free space under `RK_HOME` is already below this floor,
/// rather than letting the disk run to zero and failing deep inside an io
/// path — the failure mode that turned the 2026-08-16 leaked-worktree
/// incident into a daemon outage ("terminal state persistence failed: io").
/// Checked at the single choke point every spawn funnels through
/// (`Supervisor::spawn`), so both `agent.spawn` and a workflow `spawn` step
/// are covered by one guard.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct DiskConfig {
    /// Minimum free space (GB) required under `RK_HOME` before a spawn may
    /// proceed. Zero disables the guard.
    pub min_free_gb: u64,
}

impl Default for DiskConfig {
    fn default() -> Self {
        // The operator's own emergency-sweep threshold from the 2026-08-16
        // incident write-up: comfortably above the daemon's own working-set
        // (space.db, logs, in-flight worktrees) so a refusal always leaves
        // enough room for the daemon itself to keep operating.
        Self { min_free_gb: 10 }
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
    ///
    /// INVARIANT (order-your-timers-below-workflow-waits): must stay
    /// comfortably below any workflow's `wait` timeout that blocks on this
    /// rat's completion — e.g. the review-only steward's `reviewTimeout`
    /// (examples/workflows/steward-review.cue, default 15m — same default the
    /// daemon-native landing pipeline's `GateConfig`/`RepositoryPolicy.landing`
    /// use, crates/rk-daemon/src/landing.rs). If this value is >= that
    /// timeout, the workflow gives up and hard-fails the wait before the sweep
    /// has even flagged the rat as stuck, so the soft steer below never gets a
    /// chance to nudge it back to a clean `rk done`. See
    /// [`STEWARD_DEFAULT_REVIEW_TIMEOUT_SECS`] and
    /// `SupervisorConfig::review_timeout_warning`, which checks this
    /// invariant at daemon startup.
    pub stuck_after_secs: u64,
    /// Sustained burn (USD/minute across sweeps) above this is RUNNING AWAY.
    /// Zero disables burn detection. Shipped default 4.0: normal rats run
    /// p99 $1.24/min lifetime-average (573-rat archive), observed runaways
    /// sustained ~$7/min — roughly 3x margin on both sides, and matches the
    /// operator's own live `config.toml` override.
    pub burn_usd_per_min: f64,
    /// After the first (soft) flag steers the rat, how long to wait before
    /// escalating to a kill if it is STILL flagged. Prefer steer-then-wait.
    pub kill_grace_secs: u64,
    /// Self-healing respawn: when true, the same sweep auto-`respawn`s agents
    /// that crashed out of their run (Orphaned by a daemon restart, or Failed)
    /// instead of leaving them for a manual `rk respawn`. Off by default — it
    /// relaunches agents (and spends), so it is opt-in like burn detection.
    /// An agent whose branch already merged is never auto-respawned.
    pub respawn_enabled: bool,
    /// Crash-loop bound: how many times the sweep will auto-respawn one agent
    /// before giving up and escalating a `need` for a human. Zero disables
    /// auto-respawn even when `respawn_enabled` is true.
    pub respawn_max_attempts: u32,
    /// Base backoff (seconds) between auto-respawns of the same agent. Grows
    /// exponentially per attempt (`base * 2^(attempt-1)`) so a genuinely-broken
    /// task backs off instead of respawn-looping hot. The first attempt fires
    /// immediately; the backoff gates every retry after it.
    pub respawn_backoff_secs: u64,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 60,
            // 10m: comfortably below STEWARD_DEFAULT_REVIEW_TIMEOUT_SECS (15m) —
            // see the invariant on `stuck_after_secs` above.
            stuck_after_secs: 600,
            burn_usd_per_min: 4.0,
            kill_grace_secs: 600,
            respawn_enabled: false,
            respawn_max_attempts: 3,
            respawn_backoff_secs: 60,
        }
    }
}

/// The review-only steward's shipped default `reviewTimeout`
/// (examples/workflows/steward-review.cue: `reviewTimeout: {..., default: "15m"}`
/// — the same default `rk_workflow::LandingPolicy::review_timeout` and
/// `crates/rk-daemon/src/landing.rs`'s `GateConfig` use for the daemon-native
/// landing pipeline). Duplicated here (rather than parsed from the `.cue`
/// source) because rk-core does not depend on rk-workflow/CUE — kept in sync
/// by hand, cross-referenced from both sides. See the invariant on
/// [`SupervisorConfig::stuck_after_secs`] and `SupervisorConfig::review_timeout_warning`.
pub const STEWARD_DEFAULT_REVIEW_TIMEOUT_SECS: u64 = 15 * 60;

impl SupervisorConfig {
    /// Structural check for the order-your-timers-below-workflow-waits
    /// invariant: a stuck rat must be flagged (and soft-steered) well before
    /// a workflow's `wait` on that rat's completion gives up, or the steer
    /// never gets a chance to work. Returns a warning message when
    /// `stuck_after_secs` is not safely below the steward's shipped
    /// `reviewTimeout` default; `None` when stuck detection is off (0) or the
    /// ordering is safe.
    pub fn review_timeout_warning(&self) -> Option<String> {
        if self.stuck_after_secs == 0 {
            return None;
        }
        if self.stuck_after_secs >= STEWARD_DEFAULT_REVIEW_TIMEOUT_SECS {
            return Some(format!(
                "supervisor.stuck_after_secs ({}s) >= the steward workflow's shipped \
                 reviewTimeout default ({}s): a waiting steward review will hard-fail \
                 before the stuck sweep ever flags the rat, so its soft steer never gets \
                 a chance to help. Lower supervisor.stuck_after_secs or raise the \
                 deployed steward's reviewTimeout so the sweep gets a real intervention \
                 window.",
                self.stuck_after_secs, STEWARD_DEFAULT_REVIEW_TIMEOUT_SECS
            ));
        }
        None
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

/// How an agent's branch reaches its base once the work is done: `Direct` is a
/// plain git merge into the base (the historical behaviour); `Pr` opens a
/// pull/merge request via git and leaves the branch for review rather than
/// merging it. Per-repo on [`crate::config`]-consuming `RepoRecord`; the
/// fleet-wide fallback for repos registered without an explicit mode is
/// [`PolicyConfig::default_merge_mode`].
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MergeMode {
    #[default]
    Direct,
    Pr,
}

/// Workflow-execution policy. The seed of the #19 policy engine: today it gates
/// the one primitive that can run arbitrary shell — the workflow `run` step.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct PolicyConfig {
    /// When true, a workflow `run` step may ONLY reference a repo-registered
    /// named check (`check: "<name>"` resolved from `<repo>/.rk/checks.cue`); a
    /// raw inline `command` is refused fail-closed. This stops a compromised or
    /// untrusted workflow definition from executing arbitrary shell in an
    /// agent's worktree — it can invoke only the checks the repo owner declared.
    /// Defaults to true so an unattended workflow cannot introduce arbitrary
    /// shell through a definition edit. Set false only for explicitly trusted
    /// legacy definitions.
    pub require_named_checks: bool,
    /// Require an explicit human approval gate to have granted access before a
    /// workflow may land a branch or open a PR. Managed global definitions in
    /// `automated_landing_workflows` are the narrow exception for `land` only;
    /// `open_pr` remains human-gated.
    pub require_approval_for_landing: bool,
    /// Managed global workflow names allowed to land without a human approval
    /// gate. The executor binds this authority to a definition loaded directly
    /// from the operator-owned global workflow directory; a repo-local file
    /// with the same name cannot inherit it.
    ///
    /// NARROWED SCOPE since the daemon-native landing pipeline (Phase 3/4 of
    /// the steward remediation, `crates/rk-daemon/src/landing.rs`): the
    /// primary unattended-landing path no longer goes through a workflow
    /// `land` step at all — `LandingPipeline` calls `Supervisor::land`
    /// directly on an APPROVE/gates-passed decision, so this list is never
    /// consulted for it. This knob now exists only for an operator-authored
    /// CUSTOM workflow that still uses an explicit `land` step (e.g. a
    /// bespoke `curator` workflow); it is not, and no longer needs to be, the
    /// fleet's primary landing authority.
    pub automated_landing_workflows: Vec<String>,
    /// Fleet-wide default merge mode for a repo registered without an explicit
    /// `rk repo add --merge-mode`. A repo's own `RepoRecord.merge_mode` overrides
    /// this. Defaults to `Direct` (plain git merge) for backward compatibility.
    pub default_merge_mode: MergeMode,
    /// Exact branch names workflow `land`/`open_pr` may target. This is an
    /// explicit allowlist because those steps can change shared repository
    /// state. Configure the project base branch here when it is not `main` or
    /// `master`; an empty list denies all workflow landing targets.
    pub allowed_target_branches: Vec<String>,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            require_named_checks: true,
            require_approval_for_landing: true,
            automated_landing_workflows: vec!["steward".into()],
            default_merge_mode: MergeMode::default(),
            allowed_target_branches: vec!["main".into(), "master".into()],
        }
    }
}

/// Budget caps. `max_usd`/`max_tokens` are per-agent (graduated warn→steer→kill
/// mid-run). `fleet_max_usd`/`repo_max_usd` are hierarchical caps layered above:
/// the SUM of the *live* fleet's cost fleet-wide and per-repo, enforced as a
/// dispatch preflight (once hit, new spawns are refused). Only running agents
/// count toward the tally — a record that has left the live fleet (completed,
/// failed, dismissed, or orphaned) drops off, so these stay standing guardrails
/// on the current live/concurrent fleet spend rather than cumulative lifetime
/// ceilings that would refuse all spawns once lifetime spend crossed the cap.
/// Zero = unlimited.
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
    /// Default harness kind for spawned rats: "claude" | "codex" | "jcode".
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
        assert_eq!(cfg.policy.automated_landing_workflows, ["steward"]);
    }

    #[test]
    fn default_stuck_after_secs_stays_below_shipped_review_timeout() {
        // The coincidence this guards: both timers defaulted to 900s (15m),
        // so a stuck reviewer's soft steer never got a window to run before
        // the steward's own wait gave up.
        let cfg = SupervisorConfig::default();
        assert!(
            cfg.stuck_after_secs < STEWARD_DEFAULT_REVIEW_TIMEOUT_SECS,
            "stuck_after_secs ({}) must stay below the steward's shipped \
             reviewTimeout ({}) so the sweep gets an intervention window",
            cfg.stuck_after_secs,
            STEWARD_DEFAULT_REVIEW_TIMEOUT_SECS
        );
        assert!(cfg.review_timeout_warning().is_none());
    }

    #[test]
    fn review_timeout_warning_fires_when_stuck_after_secs_catches_up() {
        let mut cfg = SupervisorConfig {
            stuck_after_secs: STEWARD_DEFAULT_REVIEW_TIMEOUT_SECS,
            ..SupervisorConfig::default()
        };
        assert!(cfg.review_timeout_warning().is_some());

        cfg.stuck_after_secs = STEWARD_DEFAULT_REVIEW_TIMEOUT_SECS + 1;
        assert!(cfg.review_timeout_warning().is_some());
    }

    #[test]
    fn review_timeout_warning_silent_when_stuck_detection_disabled() {
        let cfg = SupervisorConfig {
            stuck_after_secs: 0,
            ..SupervisorConfig::default()
        };
        assert!(cfg.review_timeout_warning().is_none());
    }

    #[test]
    fn toml_file_overrides_defaults() {
        let dir = std::env::temp_dir().join(format!("rk-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("config.toml");
        std::fs::write(
            &file,
            "castle_name = \"burrow\"\n[log]\nfilter = \"debug\"\n[policy]\nautomated_landing_workflows = [\"curator\"]\n",
        )
        .unwrap();
        let cfg = Config::load(&file).unwrap();
        assert_eq!(cfg.castle_name.as_deref(), Some("burrow"));
        assert_eq!(cfg.log.filter, "debug");
        assert_eq!(cfg.policy.automated_landing_workflows, ["curator"]);
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
harness = "codex"
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

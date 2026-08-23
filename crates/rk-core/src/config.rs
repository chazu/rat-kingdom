//! Layered configuration: defaults < `config.toml` < `RK_*` environment.

use figment::providers::{Env, Format, Serialized, Toml};
use figment::Figment;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
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
    pub gate_worktree_sweep: GateWorktreeSweepConfig,
    pub landing_queue: LandingQueueConfig,
    pub recovery_sweep: RecoverySweepConfig,
    pub instance_timeout_sweep: InstanceTimeoutSweepConfig,
    pub ticket_reopen_sweep: TicketReopenSweepConfig,
    pub disk: DiskConfig,
    pub machine: MachineConfig,
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
    /// Where escalations get pushed. See [`NotifyConfig`].
    pub notify: NotifyConfig,
}

/// Operator push channels for escalations (`[[notify.sinks]]`).
///
/// Empty by default, which is not the same as "no notifications": an empty list
/// means *use the built-in default*, so an existing castle that never heard of
/// this section keeps the herdr desktop push it always had. See
/// [`NotifyConfig::resolved`] for the exact back-compat mapping onto the older
/// `reactor.notify_escalations` switch.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct NotifyConfig {
    /// Configured channels, in delivery order. Each entry is a `[[notify.sinks]]`
    /// table.
    pub sinks: Vec<SinkConfig>,
}

impl NotifyConfig {
    /// The channels that should actually be built, given the legacy master
    /// switch.
    ///
    /// * `notify_escalations = false` ⇒ no sinks at all. That bool predates this
    ///   section and documented itself as "keep escalations purely on the
    ///   passive inbox queue", so it stays a hard kill switch rather than
    ///   silently losing its meaning to a config the operator never wrote.
    /// * no `[[notify.sinks]]` ⇒ exactly the historical behaviour, expressed as
    ///   one default herdr sink.
    /// * any `[[notify.sinks]]` ⇒ the operator's list, verbatim. Adding a second
    ///   channel means adding a table, with no change at any escalation source.
    pub fn resolved(&self, notify_escalations: bool) -> Vec<SinkConfig> {
        if !notify_escalations {
            return Vec::new();
        }
        if self.sinks.is_empty() {
            return vec![SinkConfig::of_kind(HERDR_SINK_KIND)];
        }
        self.sinks.clone()
    }
}

/// The sink kind implemented by `rk_mux::HerdrSink` — the historical (and
/// default) escalation channel.
pub const HERDR_SINK_KIND: &str = "herdr";

/// One `[[notify.sinks]]` table: which implementation, and which notices reach
/// it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct SinkConfig {
    /// Operator-facing name, unique within the registry. It is the dedup key,
    /// so renaming a sink lets already-pushed notices through once more.
    /// Unset ⇒ the kind, which is what a single-channel castle wants. Optional
    /// rather than defaulted-to-kind because `#[serde(default)]` fills a
    /// missing field from `Default::default()`, which would hand every unnamed
    /// sink the *default* kind's name.
    pub name: Option<String>,
    /// Which implementation to build (`herdr`, …). An unknown kind is warned
    /// about and skipped, never fatal.
    pub kind: String,
    /// Set false to keep a sink configured but silent.
    pub enabled: bool,
    /// Notice classes this sink accepts (e.g. `steward-escalation`). Empty
    /// accepts every class — the common case for a single desktop channel.
    pub classes: Vec<String>,
    /// Severity floor. `info` (the default) accepts everything.
    pub min_severity: crate::notify::Severity,
    /// Per-kind parameters (`[notify.sinks.options]`), interpreted by the sink
    /// implementation and opaque to everything else.
    ///
    /// This is what keeps "add a channel by editing config" true for a channel
    /// that needs to be *told* something — the `command` kind's program, a
    /// future webhook's URL — without every new kind adding a field here that no
    /// other kind reads. A kind that needs an option it did not get must fail to
    /// build (see [`crate::notify::CommandSink::from_config`]) so the operator
    /// hears about it.
    pub options: BTreeMap<String, String>,
}

impl SinkConfig {
    /// A sink of `kind`, named after it, accepting everything.
    pub fn of_kind(kind: impl Into<String>) -> Self {
        Self {
            name: None,
            kind: kind.into(),
            enabled: true,
            classes: Vec::new(),
            min_severity: crate::notify::Severity::Info,
            options: BTreeMap::new(),
        }
    }

    /// Builder for [`Self::options`], for tests and programmatic wiring.
    pub fn with_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    /// One `[notify.sinks.options]` entry, trimmed. An option present but blank
    /// reads as absent: a half-filled TOML template should hit the sink's
    /// required-option error, not be taken literally.
    pub fn option(&self, key: &str) -> Option<&str> {
        self.options
            .get(key)
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
    }

    /// The registry/dedup key: the operator's name, else the kind.
    pub fn name(&self) -> &str {
        self.name.as_deref().unwrap_or(&self.kind)
    }

    /// Does this sink want `notice`? Enabled, class in the allow-list (or the
    /// list is empty), severity at or above the floor.
    pub fn accepts(&self, notice: &crate::notify::EscalationNotice) -> bool {
        self.enabled
            && (self.classes.is_empty() || self.classes.iter().any(|c| c == &notice.class))
            && notice.severity >= self.min_severity
    }
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self::of_kind(HERDR_SINK_KIND)
    }
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
    /// Age (in hours) past which a schedule's single-flight `Running` instance
    /// no longer blocks its next fire. Guards against a wedged instance making
    /// a schedule skip forever: above rat p99 runtime (~5h), well below the
    /// typical 24h nightly cadence. A bypassed stale instance is escalated via
    /// a `need` tuple rather than silently ignored.
    pub stale_running_hours: u64,
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
            stale_running_hours: 6,
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

/// Re-notify sweep for unacked automated-recovery escalations (strategic
/// review B2). An automated recovery action (auto-respawn, kill-at-`rk done`,
/// stale-instance timeout, orphaned-ticket reopen — B3/B5/B8/B9, all built on
/// [`crate::notify`]'s announce helper) writes a durable escalation and pushes
/// it through the configured `[[notify.sinks]]` once. Nothing else re-pushes
/// it — exactly the gap that let a finished-but-unmerged branch sit unseen for
/// two days (TKT-147). This sweep is the fix: an unacked escalation (`rk inbox
/// ack <id>`) re-notifies at `first_renotify_secs`, then every
/// `repeat_renotify_secs`, up to `max_renotifies` times, after which it stands
/// as a passive `rk inbox` row with no further pushes — the same "dead sink
/// degrades to the passive queue" philosophy B1 established, applied to a
/// human who has not looked yet rather than a channel that cannot deliver.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct RecoverySweepConfig {
    /// Master switch. When false the sweep loop never starts; unacked
    /// escalations still surface on `rk inbox`, they just never re-push.
    pub enabled: bool,
    /// How often the sweep checks for a due re-notify.
    pub interval_secs: u64,
    /// Delay after an escalation is written before its FIRST re-notify.
    pub first_renotify_secs: u64,
    /// Delay between each re-notify after the first.
    pub repeat_renotify_secs: u64,
    /// How many re-notifies an unacked escalation gets before the sweep
    /// leaves it alone for good.
    pub max_renotifies: u32,
}

impl Default for RecoverySweepConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 300,
            first_renotify_secs: 4 * 3600,
            repeat_renotify_secs: 24 * 3600,
            max_renotifies: 3,
        }
    }
}

/// Stale-`Running`-instance hard timeout sweep (strategic review B8). A panic
/// in an instance's execution future skips `WorkflowEngine::finalize`
/// (`rk-daemon` `workflow_exec.rs`), so the instance would otherwise stay
/// `Running` forever with no live task backing it — observed: a 6.4-day
/// wedged outlier. Deliberately a TIMEOUT, not a liveness probe: "Running with
/// no live future" is not decidable from durable state alone (see the S4 note
/// in the strategic review). Past `default_timeout_secs` wall-clock (a
/// workflow's own `staleTimeout:` field overrides this per-instance), the
/// sweep marks the instance failed, finalizes it (including the
/// guaranteed-cleanup agent sweep), and escalates through the B2 announce
/// helper (`crate::recovery` in `rk-daemon`, mirrored by [`RecoverySweepConfig`]
/// above).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct InstanceTimeoutSweepConfig {
    /// Master switch. When false the sweep loop never starts; a wedged
    /// instance stays `Running` until an operator notices and clears it by
    /// hand.
    pub enabled: bool,
    /// How often the sweep checks every live instance's wall-clock age.
    pub interval_secs: u64,
    /// Default hard timeout in seconds, used unless a workflow's own
    /// `staleTimeout:` overrides it. 12h — more than 2x the slowest
    /// legitimate run observed.
    pub default_timeout_secs: u64,
    /// Rate cap on the `instance-timeout` recovery-action kind: at most this
    /// many timeout-driven failures announced per rolling hour (jittered
    /// ±10%, see `RecoveryAnnouncer`). Timeouts are inherently rare (a 12h
    /// dwell before one can even fire), so this exists as a backstop against
    /// a pathological case — e.g. a bug that wedges many instances into the
    /// same state at once — not because steady volume is expected.
    pub rate_cap_per_hour: u32,
}

impl Default for InstanceTimeoutSweepConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            // Ten minutes: the timeout itself is 12h, so a sweep running far
            // more often than that costs nothing and keeps worst-case
            // detection lag small relative to the timeout it enforces.
            interval_secs: 600,
            default_timeout_secs: 12 * 3600,
            rate_cap_per_hour: 20,
        }
    }
}

/// Sweep for orphaned `in_progress` tickets (strategic review B9, seam 5).
/// Drain only refills from `status = open` (`rk-daemon` `tickets.rs`), and an
/// errored rat leaves its ticket `in_progress` with nothing to reopen it —
/// the backlog silently loses a slot forever. This sweep is the fix: an
/// `in_progress` ticket whose assignee has had no LIVE agent record for
/// `stale_after_secs` reopens to `open` (drain-eligible again) and announces
/// through the B2 recovery-announce helper (`rk-daemon` `recovery.rs`).
///
/// The staleness clock is anchored on the more recent of the ticket's own
/// last edit and the assignee's own last state transition, so the delay
/// covers two distinct races, not just one: spawn handoff (`status` flips to
/// `in_progress` before `assignee` is recorded — a few seconds, not never)
/// and restart recovery (an `Orphaned` agent gets the B3 respawn sweep's own
/// backoff window to reclaim it before this sweep gives up).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct TicketReopenSweepConfig {
    /// Master switch. When false the sweep loop never starts; a ticket whose
    /// rat died stays `in_progress` until an operator reopens it by hand.
    pub enabled: bool,
    /// Sweep cadence. Short relative to `stale_after_secs` so the reopen
    /// lands close to the acceptance bound (a dead rat's ticket reopens
    /// within roughly `stale_after_secs + interval_secs`).
    pub interval_secs: u64,
    /// How long an `in_progress` ticket may go with no live owning agent
    /// before it reopens.
    pub stale_after_secs: u64,
}

impl Default for TicketReopenSweepConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 60,
            // 15 minutes: long enough that the B3 respawn sweep's backoff
            // window (attempts span ~15min) gets a real chance to reclaim an
            // orphaned agent before this sweep gives up on its ticket.
            stale_after_secs: 15 * 60,
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
    /// Does NOT gate [`artifact_paths`](Self::artifact_paths) reclaim — that
    /// runs every sweep tick against every still-live terminal record with no
    /// age cutoff, since a `target/` dir is exactly as regenerable the moment
    /// its agent goes terminal as it is `after_days` later.
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
    /// Fleet-wide fallback list of regenerable build-artifact paths (relative
    /// to a worktree root) reclaimed from EVERY terminal agent's worktree —
    /// Completed/Failed/Dismissed — regardless of merge state AND regardless
    /// of [`after_days`](Self::after_days): every sweep tick reaps these
    /// paths from every still-live terminal record immediately, not only once
    /// a record ages into archiving. Unlike the git reclaim above, an
    /// unmerged branch's build output is exactly as regenerable as a merged
    /// one's: only these named paths are removed, never the worktree, branch,
    /// or any source/git state. STACK NEUTRALITY: the daemon has no built-in
    /// notion of what any language's build directory is called, so this
    /// defaults to EMPTY (reap nothing) — a repo's own `.rk/repo.cue`
    /// `reap.artifactPaths` (`rk_workflow::ReapPolicy`) is the intended source
    /// and always takes precedence when a repo declares one; this field and
    /// [`artifact_paths_by_repo`](Self::artifact_paths_by_repo) exist only as
    /// an operator-set fallback for repos that have not (yet) activated a
    /// policy naming their own paths. Root-caused by the 2026-08-18 O12
    /// incident (docs/2026-08-18-drain-probe-log.md): a probe day left 231 GB
    /// of terminal rats' `target/` dirs standing because the sweep only
    /// reclaimed MERGED branches' worktrees wholesale, tripping `[disk]
    /// min_free_gb` and silently stalling drain — gating the artifact reap on
    /// the same `after_days` cutoff as archiving would have reproduced that
    /// exact gap for the default 3-day window.
    pub artifact_paths: Vec<String>,
    /// Per-repo override of `artifact_paths`, keyed by repo name — a repo
    /// with an entry here uses THAT list instead of `artifact_paths` (not
    /// merged with it) whenever its `.rk/repo.cue` declares no
    /// `reap.artifactPaths` of its own.
    #[serde(default)]
    pub artifact_paths_by_repo: std::collections::HashMap<String, Vec<String>>,
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
            // STACK NEUTRALITY: no language/toolchain assumption belongs in a
            // daemon-wide default — see the field doc above. Reaping only
            // happens for a repo that opts in through its own policy.
            artifact_paths: Vec::new(),
            artifact_paths_by_repo: std::collections::HashMap::new(),
        }
    }
}

/// Periodic retention for the landing pipeline's persistent, daemon-owned
/// gate worktrees (`<home>/gate-worktrees/<repo>/<target>`, `rk-daemon`
/// `landing.rs`, docs/proposals/daemon-native-landing-pipeline.md §2.2).
/// Unlike an agent's worktree — created fresh per spawn and cleaned up on
/// `dismiss` — a gate worktree is created once per `(repo, target)` and
/// reused across every landing attempt against that target, so nothing ever
/// removes it on its own: a repo that renames its default branch, or one
/// with many long-lived release targets, accumulates one multi-GB checkout
/// per target forever (design doc §5 open question 4, filed as a follow-up
/// rather than fixed in T1-T4).
///
/// Two independent eviction rules, either of which can reclaim a worktree:
/// least-recently-used (a target unused for `max_age_days` is stale) and a
/// per-repo cap (`max_per_repo`, keeping only the N most recently used
/// targets). Both are `0`-disables, matching this codebase's existing
/// `steward-diff-scope`/`DiskConfig` convention for an off switch. A
/// worktree is NEVER reclaimed while its `(repo, target)` key has a live
/// `rk-daemon` `LandingQueue` entry — the same fail-closed posture
/// `Supervisor::reap_git` applies to agent worktrees, so this sweep can run
/// unattended without racing an in-flight gate run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct GateWorktreeSweepConfig {
    /// Master switch for the periodic loop. When false the timer never
    /// starts; the reclaim logic itself is still reachable manually via `rk
    /// prune --reap-git` (`agent.archive`'s `gate_worktrees` extension).
    pub enabled: bool,
    /// Sweep cadence.
    pub interval_secs: u64,
    /// A gate worktree not reset for a landing attempt in at least this many
    /// days becomes eligible for reclaim. `0` disables the age rule.
    pub max_age_days: u64,
    /// Keep only the `max_per_repo` most-recently-used target worktrees per
    /// repo; older ones are reclaimed regardless of age. `0` disables the
    /// cap.
    pub max_per_repo: u64,
}

impl Default for GateWorktreeSweepConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            // Same cadence as `worktree_sweep`: gate worktrees accumulate
            // slowly (one per distinct landing target), so there is no
            // benefit to a tighter interval.
            interval_secs: 3600,
            max_age_days: 14,
            max_per_repo: 5,
        }
    }
}

/// Landing-queue staleness threshold (probe O18): depth and per-entry age are
/// always visible on `status`/`rk top`; this only governs when the oldest
/// pending entry additionally raises a `landing-queue-stalled` `rk inbox`
/// row. Without it a slowly-draining queue and a wedged one look identical
/// from the outside, which cost an operator an unnecessary hand-land during
/// probe O18 — the pipeline was working a deep serial backlog the whole time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct LandingQueueConfig {
    /// Age (from first enqueue, surviving any stale-target requeue) past
    /// which the oldest pending entry for a `(repo, target)` key raises a
    /// row. `0` disables the row entirely — depth/age still show on
    /// `status`/`rk top`.
    pub stale_after_secs: u64,
}

impl Default for LandingQueueConfig {
    fn default() -> Self {
        Self {
            // Matches probe O18's own "oldest waiting 3h" framing: long
            // enough that a healthy queue draining a normal backlog never
            // trips it, short enough that a genuine wedge is caught well
            // inside a working day.
            stale_after_secs: 3 * 3600,
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
    /// When true, a spawned agent's `CARGO_TARGET_DIR` points at
    /// `<RK_HOME>/cargo-target-cache/<repo>` (one shared build cache per repo)
    /// instead of that worktree's own `target/`. Root-caused by
    /// TKT-01M04D1QDBNCF0T0D0EHRVNJV5: with a per-worktree `target/`, disk
    /// usage multiplies by the number of concurrently live worktrees on a
    /// repo (60+ observed, 3-7 GB each) until `cargo test --workspace` fails
    /// mid-run on ENOSPC even though nothing is actually leaked.
    ///
    /// Defaults **false** (TKT-01M0EXYHV1GR9Z75QSS42HXBVK), reversing the
    /// earlier default. The doc comment this replaced described the tradeoff
    /// as "cargo's own target-dir file lock serializing overlapping builds —
    /// slower under heavy concurrency, but never a hard failure." That is
    /// wrong: sharing one `CARGO_TARGET_DIR` across worktrees corrupts builds
    /// even with **zero** concurrency. Confirmed by a real two-`git worktree`
    /// reproduction (two checkouts of this repo at different commits,
    /// built *sequentially*, no overlap): the second build silently linked
    /// against the first worktree's stale compiled `rk-core`, producing a
    /// hard compile error (`E0560`, a struct field the first worktree's
    /// checkout didn't have yet) — not a flake, 100% reproducible, and also
    /// reproduced with a minimal two-crate fixture workspace (see
    /// `crates/rk-core/tests/shared_cargo_target_worktree_isolation.rs`).
    /// Cargo does not fully key a workspace-member unit's fingerprint by the
    /// checkout's absolute path, so two worktrees of the same repo can
    /// collide onto the same cached artifact regardless of timing. A
    /// build-phase lock (cargo's own, or the daemon's `TestExecLock`) cannot
    /// fix this because there is no race to serialize against — the wrong
    /// answer is cached, not merely contended for.
    ///
    /// The original ENOSPC concern this flag traded against now has an
    /// independent fix: [`WorktreeSweepConfig`] (enabled by default) reaps
    /// each terminal worktree's own `target/` directory hourly, and
    /// `min_free_gb` above refuses new spawns before a live batch can run a
    /// repo out of room. An operator who still wants cross-worktree build
    /// sharing despite the correctness risk can opt back in explicitly; nothing
    /// downstream ([`crate::config`]'s wiring, `TestExecLock`, the
    /// contention-retry in `run_check_in`) depends on the default.
    pub shared_cargo_target: bool,
}

impl Default for DiskConfig {
    fn default() -> Self {
        // The operator's own emergency-sweep threshold from the 2026-08-16
        // incident write-up: comfortably above the daemon's own working-set
        // (space.db, logs, in-flight worktrees) so a refusal always leaves
        // enough room for the daemon itself to keep operating.
        Self {
            min_free_gb: 10,
            shared_cargo_target: false,
        }
    }
}

/// Machine-load admission guard, the CPU half of the scarce-resource signal
/// `[disk] min_free_gb` supplies the storage half of. Both are sampled together
/// and evaluated together (`rk_daemon::machine`), so a refusal for either
/// reason reports both numbers.
///
/// Unlike the disk floor this defaults to DISABLED, and deliberately so: a
/// castle running a fleet of build-heavy rats sits at a high load average as
/// its normal, healthy operating state, so a shipped default would refuse
/// legitimate work on day one. The dial exists for an operator who has measured
/// their own machine's cliff.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct MachineConfig {
    /// Refuse a spawn when the 1-minute load average divided by the CPU count
    /// exceeds this. Normalised per CPU so one value is portable across
    /// castles of different sizes. Zero (the default) disables the guard.
    pub max_load_per_cpu: f64,
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
    /// Grace window after a clean `rk done` before a still-lingering harness
    /// process is SIGKILLed (seam 7, strategic-review B5): `sweep()` only
    /// ever looks at `is_live()` agents, so a `Completed` record's process —
    /// interactive harnesses stay alive between turns — is otherwise never
    /// checked again. Long enough for the transcript to flush / the harness
    /// to exit on its own; short enough that a runaway process left behind
    /// by `rk done` does not sit burning tokens indefinitely.
    pub done_kill_grace_secs: u64,
    /// Self-healing respawn: when true, the same sweep auto-`respawn`s agents
    /// that crashed out of their run (Orphaned by a daemon restart, or Failed)
    /// instead of leaving them for a manual `rk respawn`. Shipped enabled
    /// (strategic review B3) now that `respawn_rate_cap_per_hour` bounds a
    /// fleet-wide storm and every action announces via the recovery helper;
    /// an agent whose branch already merged is never auto-respawned.
    pub respawn_enabled: bool,
    /// Crash-loop bound: how many times the sweep will auto-respawn one agent
    /// before giving up and escalating a `need` for a human. Zero disables
    /// auto-respawn even when `respawn_enabled` is true.
    pub respawn_max_attempts: u32,
    /// Base backoff (seconds) between auto-respawns of the same agent. Grows
    /// exponentially per attempt (`base * 2^(attempt-1)`) so a genuinely-broken
    /// task backs off instead of respawn-looping hot. The first attempt fires
    /// immediately; the backoff gates every retry after it. Shipped default
    /// 300s (up from an earlier 60s): three attempts then span ~15 minutes
    /// instead of ~3, so a systemic failure (bad redeploy, the TKT-146 class)
    /// does not burn through the whole crash-loop budget before a human even
    /// notices.
    pub respawn_backoff_secs: u64,
    /// Castle-wide rolling cap: at most this many auto-respawns (any agent,
    /// any repo) within a trailing hour. Zero disables the cap. Distinct from
    /// `respawn_max_attempts`, which bounds one agent's own crash loop — 200
    /// of 786 archived rats failed, often in correlated incidents (a daemon
    /// restart orphans the whole fleet at once), so a per-agent cap alone
    /// cannot stop a fleet-wide respawn storm. Enforced by the
    /// `RecoveryAnnouncer` rate-cap helper (`rk-daemon::recovery`, strategic
    /// review B2/B3): the action past the cap is HELD (not fired) and
    /// escalated at raised severity instead, same as any other rate-capped
    /// recovery action.
    pub respawn_rate_cap_per_hour: u32,
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
            done_kill_grace_secs: 60,
            respawn_enabled: true,
            respawn_max_attempts: 3,
            respawn_backoff_secs: 300,
            respawn_rate_cap_per_hour: 10,
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
    /// The authority-ladder matrix (`crate::action`'s unattended-orchestration
    /// counterpart to the mechanical/orchestrator/human split
    /// `rk-daemon::reconcile::Authority` assigns each cross-ledger
    /// convergence violation kind). A kind absent here keeps its conservative
    /// built-in default. Values are validated at daemon startup
    /// (`rk-daemon::authority::AuthorityPolicy::from_config`) against
    /// `rk-daemon::reconcile::builtin_authority`, which rk-core does not
    /// depend on — so this stays untyped strings here
    /// (`"mechanical" | "orchestrator" | "human"`) and an override may only
    /// NARROW a kind's authority toward `"human"`, never widen it. Combined
    /// with there being no RPC method that writes `config.toml`, this is what
    /// makes an orchestrator session unable to widen its own authority: the
    /// only way to grant more is a human editing this file and restarting
    /// the daemon.
    pub authority_overrides: BTreeMap<String, String>,
    /// Violation `kind`s an `Orchestrator`-authority attention item may be
    /// resolved for without a human in the loop. Conservative default: empty
    /// — every orchestrator-classified item stays unresolved until a human
    /// explicitly names its kind here.
    pub orchestrator_action_allowlist: Vec<String>,
    /// Max orchestrator-authority decisions a lease may act on per rolling
    /// window, fleet-wide. `0` means unlimited (mirrors `RecoveryAction`'s
    /// own `RateCap` convention).
    pub orchestrator_rate_cap: u32,
    pub orchestrator_rate_window_secs: u64,
    /// Durable orchestrator lease TTL (seconds): how long a lease holder has
    /// before a different holder may preempt it.
    pub orchestrator_lease_ttl_secs: i64,
    /// Fleet-wide default max concurrent daemon-managed verification runs
    /// (`WorkflowEngine::run_check_in`, gated to checks that set
    /// `sharedCargoTarget` — the CPU/wall-clock-heavy ones, e.g. `verify`) for
    /// one repository at a time: landing gates, workflow `run` steps, AND
    /// `verify.run`-mediated agent/reviewer self-checks all share this ONE
    /// per-repo bound (TKT-01M0HNESEECWWFQF8X6VH1XSJ6). `0` (the default)
    /// disables admission control entirely — zero behaviour change from
    /// before this existed — matching the `0 = unlimited/disabled` convention
    /// already used by [`SupervisorConfig::min_free_gb`]-style caps elsewhere
    /// in this file. An operator who has observed real contention (concurrent
    /// full-suite runs starving each other of CPU) sets this to a small
    /// positive number; raising it above 1 is what "repository policy limit
    /// greater than one" in the ticket means in practice.
    pub verification_admission_limit: u32,
    /// Per-repo override of [`verification_admission_limit`](Self::verification_admission_limit),
    /// keyed by repo name — same fallback role as
    /// [`crate::config`]'s other per-repo overrides (e.g.
    /// `rk_daemon::Reap::artifact_paths_by_repo`). A repo absent here uses the
    /// fleet-wide default above.
    pub verification_admission_limit_by_repo: BTreeMap<String, u32>,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            require_named_checks: true,
            require_approval_for_landing: true,
            automated_landing_workflows: vec!["steward".into()],
            default_merge_mode: MergeMode::default(),
            allowed_target_branches: vec!["main".into(), "master".into()],
            authority_overrides: BTreeMap::new(),
            orchestrator_action_allowlist: Vec::new(),
            orchestrator_rate_cap: 5,
            orchestrator_rate_window_secs: 3600,
            orchestrator_lease_ttl_secs: 300,
            verification_admission_limit: 0,
            verification_admission_limit_by_repo: BTreeMap::new(),
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
    /// Per-agent USD cap for `role == "reviewer"` ONLY, checked in place of
    /// (never in addition to) `max_usd` for that role. Reviewers were
    /// observed uncapped in production (one hit $27, above the worker cap)
    /// — this is a distinct, graduated warn→stop cap set above the cost of a
    /// legitimate deep review so a genuinely thorough review is never cut
    /// off mid-verdict. Zero = unlimited.
    pub reviewer_max_usd: f64,
}

impl Default for BudgetConfig {
    fn default() -> Self {
        Self {
            max_usd: 0.0,
            max_tokens: 0,
            warn_at: 0.8,
            fleet_max_usd: 0.0,
            repo_max_usd: 0.0,
            reviewer_max_usd: 30.0,
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

    /// TKT-01M0EXYHV1GR9Z75QSS42HXBVK: a shared `CARGO_TARGET_DIR` corrupts
    /// builds across worktrees with no concurrency required (see the doc
    /// comment on `DiskConfig::shared_cargo_target` and
    /// `crates/rk-core/tests/shared_cargo_target_worktree_isolation.rs` for
    /// the reproduction). Pin the default off so a future edit cannot flip it
    /// back to `true` without this test naming what breaks.
    #[test]
    fn shared_cargo_target_defaults_off() {
        assert!(
            !DiskConfig::default().shared_cargo_target,
            "a shared CARGO_TARGET_DIR corrupts cross-worktree builds even \
             with zero concurrency (TKT-01M0EXYHV1GR9Z75QSS42HXBVK) — this \
             must stay opt-in"
        );
    }

    /// TKT-01M0E8PN9C41BWECGNW0990R3J's "repository or castle policy declares
    /// the initial authority matrix" acceptance item: a castle's own
    /// `config.toml` — not merely `PolicyConfig::default()` in Rust — is what
    /// a real daemon loads its authority ladder from. This parses an actual
    /// declared `[policy]` section (conservative: only ONE kind explicitly
    /// promoted to `orchestrator`, with a narrowing override recorded
    /// alongside it) through the real `Config::load` layering path, proving
    /// the file — not code — is the source of truth for what gets activated.
    #[test]
    fn a_declared_policy_file_narrows_and_allowlists_exactly_what_it_names() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
[policy]
# Conservative, explicit human approval: only this ONE violation kind may be
# resolved by an orchestrator session without a human in the loop.
orchestrator_action_allowlist = ["terminal-assignee-active-work"]
# A human has decided this castle wants EXTRA caution on mechanical repairs:
# narrow delivered-but-open from its Mechanical default toward Human.
[policy.authority_overrides]
delivered-but-open = "human"
"#,
        )
        .unwrap();

        let cfg = Config::load(&path).unwrap();
        assert_eq!(
            cfg.policy.orchestrator_action_allowlist,
            vec!["terminal-assignee-active-work".to_string()]
        );
        assert_eq!(
            cfg.policy.authority_overrides.get("delivered-but-open"),
            Some(&"human".to_string())
        );
        // Everything the file did not mention keeps its conservative
        // built-in: no other kind is allowlisted, and the rate cap / lease
        // TTL stay at their shipped defaults.
        assert_eq!(cfg.policy.orchestrator_action_allowlist.len(), 1);
        assert_eq!(cfg.policy.orchestrator_rate_cap, 5);
        assert_eq!(cfg.policy.orchestrator_lease_ttl_secs, 300);
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

    /// The B1 promise at the config layer: a second channel is `[[notify.sinks]]`
    /// tables in the operator's file, options and all, reaching
    /// [`NotifyConfig::resolved`] verbatim.
    #[test]
    fn notify_sinks_parse_from_toml_with_per_kind_options() {
        let dir = std::env::temp_dir().join(format!("rk-cfg-notify-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("config.toml");
        std::fs::write(
            &file,
            r#"
[[notify.sinks]]
kind = "herdr"

[[notify.sinks]]
name = "ops-chat"
kind = "command"
classes = ["steward-escalation"]
min_severity = "warn"

[notify.sinks.options]
command = "/usr/local/bin/rk-notify-chat"
timeout_secs = "30"
"#,
        )
        .unwrap();

        let cfg = Config::load(&file).unwrap();
        let sinks = cfg.notify.resolved(true);
        assert_eq!(sinks.len(), 2, "the operator's list, verbatim");
        assert_eq!(sinks[0].name(), "herdr", "unnamed falls back to the kind");
        assert!(sinks[0].options.is_empty());

        let chat = &sinks[1];
        assert_eq!(chat.name(), "ops-chat");
        assert_eq!(chat.kind, "command");
        assert_eq!(chat.classes, ["steward-escalation"]);
        assert_eq!(chat.min_severity, crate::notify::Severity::Warn);
        assert_eq!(
            chat.option("command"),
            Some("/usr/local/bin/rk-notify-chat")
        );
        assert_eq!(chat.option("timeout_secs"), Some("30"));
        assert_eq!(chat.option("nope"), None);

        // The legacy master switch still wins over any list.
        assert!(cfg.notify.resolved(false).is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }
}

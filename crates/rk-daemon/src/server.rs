//! The daemon server: accepts NDJSON requests on a Unix socket and dispatches
//! them. Hosts the tuplespace; `space.watch` upgrades a connection to a
//! server-push event stream.

use crate::coordinator::CoordinatorFilter;
use crate::proto::{codes, Request, Response};
use chrono::{DateTime, Utc};
use rk_core::id::RecordId;
use rk_core::paths::Layout;
use rk_core::tuple::{Category, Lifecycle, Pattern, Tuple, SYSTEM_SCOPE};
use rk_space::{CoordinatorEvent, Space};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, watch};
use tracing::{debug, info, warn};

const GC_INTERVAL: Duration = Duration::from_secs(60);
// Default lifetime for a pheromone trail (claim / obstacle / need) written
// without an explicit TTL — the hard-TTL backstop for strength decay — lives in
// rk-core so daemon-internal trail writers (supervisor, syncer) age on the same
// clock as this RPC boundary.
use rk_core::tuple::{DEFAULT_TRAIL_TTL, MAX_TRAIL_TTL};
/// Ceiling for blocking reads so a lost client cannot pin a connection task
/// forever; clients requesting more get clamped.
const MAX_BLOCK: Duration = Duration::from_secs(3600);
const DEFAULT_BLOCK: Duration = Duration::from_secs(5);
const MAX_FRAME_BYTES: usize = 1024 * 1024;
const MAX_SCAN_TUPLES: usize = 10_000;
/// Inbox is an aggregation endpoint, so cap both its source histories and its
/// final response. Newest-first source scans preserve the current state of
/// event reducers when old history is truncated.
const MAX_INBOX_ITEMS: usize = 2_048;
/// The caller id a human at a terminal authenticates as. `Client` sends this
/// whenever `RK_AGENT` is unset, and an empty caller means the same thing.
const OPERATOR_ACTOR: &str = "operator";

type FactVoteKey = (String, String, String);
type FactVoteState = (DateTime<Utc>, RecordId, String);

/// Who may close a ballot: its proposer, or the operator (TKT-184).
///
/// Withdrawal is destructive-in-effect and unretractable in practice — it
/// permanently suppresses a promotion — so it is gated to the two parties with
/// standing. The proposer, because pulling your own proposal is the ordinary
/// case and needs no ceremony. The operator, because they are the only party who
/// is always reachable, and the ballot's author is usually a rat that has been
/// dead for days by the time anyone decides the proposal is going nowhere;
/// author-only would mean the common case has no one who can act.
///
/// A peer rat is deliberately NOT permitted. Endorsement is the fleet's
/// mechanism for disagreeing with a proposal — you decline to endorse it — and
/// letting any rat close any other's ballot would make a norm program where one
/// dissenter beats three endorsers. Quorum is the vote; this is not a veto.
///
/// An empty caller is the operator: `Client` sends `operator` when `RK_AGENT` is
/// unset, and pre-auth/local callers arrive blank, which the rest of the server
/// already reads as the operator.
fn may_withdraw(caller: &str, proposer: &str) -> bool {
    caller.is_empty() || caller == OPERATOR_ACTOR || caller == proposer
}

pub struct Daemon {
    layout: Layout,
    space: Space,
    /// The wire identity: this castle's Ed25519 actor id (`castle-<hex>`). Every
    /// daemon-authored tuple's `instance`, the sync author, and arbitration key
    /// on it — NEVER the display alias (TKT-124).
    castle: String,
    /// Operator-facing display string for this castle: the `castle_name` alias if
    /// set, else `castle` verbatim. Presentation-only — used in `status`/logs.
    castle_display: String,
    supervisor: Arc<crate::supervisor::Supervisor>,
    syncer: Option<Arc<crate::sync::Syncer>>,
    sync_interval: Duration,
    reactor_config: rk_core::config::ReactorConfig,
    scheduler_config: rk_core::config::SchedulerConfig,
    sweep_config: rk_core::config::SupervisorConfig,
    review_sweep_config: rk_core::config::ReviewSweepConfig,
    drain_config: rk_core::config::DrainConfig,
    evaporation_decay: f64,
    global_agents: std::collections::HashMap<String, rk_workflow::AgentProfile>,
    tier_routing: rk_workflow::TierRouting,
    default_harness: String,
    /// When set, workflow `run` steps may only invoke repo-registered named
    /// checks; raw inline commands are refused (TKT-30, `[policy]`).
    require_named_checks: bool,
    require_approval_for_landing: bool,
    /// Fleet-wide default merge mode a repo is registered with when `rk repo
    /// add` names no explicit `--merge-mode` (`[policy] default_merge_mode`).
    default_merge_mode: rk_core::config::MergeMode,
    allowed_target_branches: Vec<String>,
    auth_token: String,
    engine: std::sync::OnceLock<Arc<crate::workflow_exec::WorkflowEngine>>,
    repos: std::sync::Mutex<crate::repos::RepoRegistry>,
    onboarding_sessions: std::sync::Mutex<crate::onboarding_sessions::OnboardingSessions>,
    /// Serializes the Git apply/commit/verification recovery window. Session
    /// persistence supplies restart safety; this lock prevents concurrent
    /// operator retries in one daemon from racing the same worktree.
    onboarding_apply_lock: tokio::sync::Mutex<()>,
    tickets: Arc<crate::tickets::Tickets>,
    coordinator_sessions: std::sync::Mutex<crate::coordinator::CoordinatorSessions>,
    /// Serializes read/append cycles for one agent's effective fact vote.
    fact_vote_lock: std::sync::Mutex<()>,
    started: Instant,
    shutdown_tx: watch::Sender<bool>,
}

#[derive(Debug, Default)]
struct PeerOrigin {
    pid_observed: bool,
    supervised_agents: std::collections::HashSet<String>,
}

impl Daemon {
    #[cfg(test)]
    pub(crate) fn space_handle(&self) -> Space {
        self.space.clone()
    }

    pub fn new(layout: Layout, config: &rk_core::config::Config) -> rk_core::Result<Self> {
        layout.ensure()?;
        let space = Space::open(&layout.db_path())?;
        let global_agents: HashMap<String, rk_workflow::AgentProfile> = config
            .agents
            .iter()
            .map(|(name, p)| {
                (
                    name.clone(),
                    rk_workflow::AgentProfile {
                        harness: p.harness.clone(),
                        model: p.model.clone(),
                        permission_mode: p.permission_mode.clone(),
                    },
                )
            })
            .collect();
        let default_agent = global_agents.get("default").cloned().unwrap_or_default();
        let budget = rk_ledger::Budget {
            max_usd: config.budget.max_usd,
            max_tokens: config.budget.max_tokens,
            warn_at: config.budget.warn_at,
        };
        let fleet_budget = rk_ledger::FleetBudget {
            fleet_max_usd: config.budget.fleet_max_usd,
            repo_max_usd: config.budget.repo_max_usd,
            warn_at: config.budget.warn_at,
        };
        // Castle identity: the wire id is ALWAYS the stable, authenticated actor
        // id derived from this castle's Ed25519 key (TKT-59) — it signs every
        // replicated op and keys arbitration. A configured `castle_name` is a
        // PRESENTATION-ONLY alias (TKT-124), applied only at render time; it must
        // never become the wire id, or it would leak into signed records.
        let actor = rk_core::identity::CastleIdentity::load_or_create(&layout.castle_key_path())?
            .actor()
            .to_string();
        let display =
            rk_core::identity::CastleDisplay::new(actor.clone(), config.castle_name.clone());
        let castle_display = display.own().to_string();
        let mut daemon = Self::with_space_and_default_agent(
            layout,
            actor,
            config.harness.default.clone(),
            default_agent,
            budget,
            fleet_budget,
            space,
        )?;
        daemon.castle_display = castle_display;
        daemon.global_agents = global_agents;
        daemon.tier_routing = rk_workflow::TierRouting {
            rules: config
                .tiers
                .rules
                .iter()
                .map(|r| rk_workflow::TierRule {
                    priority: r.priority.clone(),
                    label: r.label.clone(),
                    tier: r.tier.clone(),
                })
                .collect(),
        };
        daemon.reactor_config = config.reactor.clone();
        daemon.scheduler_config = config.scheduler.clone();
        daemon.sweep_config = config.supervisor.clone();
        daemon.review_sweep_config = config.review_sweep.clone();
        daemon.drain_config = config.drain.clone();
        daemon.evaporation_decay = config.evaporation.decay;
        daemon.require_named_checks = config.policy.require_named_checks;
        daemon.require_approval_for_landing = config.policy.require_approval_for_landing;
        daemon.default_merge_mode = config.policy.default_merge_mode;
        daemon.allowed_target_branches = config.policy.allowed_target_branches.clone();
        if config.sync.enabled {
            let syncer = crate::sync::Syncer::new(
                &daemon.layout,
                &daemon.castle,
                config.sync.remote_url.as_deref(),
            )?;
            daemon.syncer = Some(Arc::new(syncer));
            daemon.sync_interval = Duration::from_secs(config.sync.interval_secs.max(5));
        }
        Ok(daemon)
    }

    #[doc(hidden)]
    pub fn new_in_memory(layout: Layout, castle: String) -> rk_core::Result<Self> {
        let space = Space::open_in_memory()?;
        Self::with_space(
            layout,
            castle,
            "fake".into(),
            rk_ledger::Budget::default(),
            rk_ledger::FleetBudget::default(),
            space,
        )
    }

    #[doc(hidden)]
    pub fn with_space_for_tests(
        layout: Layout,
        castle: String,
        default_harness: String,
        budget: rk_ledger::Budget,
        space: Space,
    ) -> rk_core::Result<Self> {
        Self::with_space(
            layout,
            castle,
            default_harness,
            budget,
            rk_ledger::FleetBudget::default(),
            space,
        )
    }

    /// Like [`with_space_for_tests`] but with an explicit fleet/repo cap, for
    /// exercising the pre-dispatch wallet kill-switch.
    #[doc(hidden)]
    pub fn with_fleet_budget_for_tests(
        layout: Layout,
        castle: String,
        default_harness: String,
        budget: rk_ledger::Budget,
        fleet_budget: rk_ledger::FleetBudget,
        space: Space,
    ) -> rk_core::Result<Self> {
        Self::with_space(layout, castle, default_harness, budget, fleet_budget, space)
    }

    #[doc(hidden)]
    pub fn set_sweep_config(&mut self, cfg: rk_core::config::SupervisorConfig) {
        self.sweep_config = cfg;
    }

    #[doc(hidden)]
    pub fn set_require_named_checks(&mut self, v: bool) {
        self.require_named_checks = v;
    }

    #[doc(hidden)]
    pub fn set_drain_config(&mut self, cfg: rk_core::config::DrainConfig) {
        self.drain_config = cfg;
    }

    #[doc(hidden)]
    pub fn set_review_sweep_config(&mut self, cfg: rk_core::config::ReviewSweepConfig) {
        self.review_sweep_config = cfg;
    }

    fn with_space(
        layout: Layout,
        castle: String,
        default_harness: String,
        budget: rk_ledger::Budget,
        fleet_budget: rk_ledger::FleetBudget,
        space: Space,
    ) -> rk_core::Result<Self> {
        Self::with_space_and_default_agent(
            layout,
            castle,
            default_harness,
            rk_workflow::AgentProfile::default(),
            budget,
            fleet_budget,
            space,
        )
    }

    fn with_space_and_default_agent(
        layout: Layout,
        castle: String,
        default_harness: String,
        default_agent: rk_workflow::AgentProfile,
        budget: rk_ledger::Budget,
        fleet_budget: rk_ledger::FleetBudget,
        space: Space,
    ) -> rk_core::Result<Self> {
        layout.ensure()?;
        let auth_token = layout.auth_token()?;
        // One Tickets instance, shared by the RPC handlers and the supervisor,
        // so ticket-lifecycle writes serialize on a single lock.
        let tickets = Arc::new(crate::tickets::Tickets::new(space.clone(), castle.clone()));
        let supervisor = Arc::new(crate::supervisor::Supervisor::new_with_agent_defaults(
            layout.clone(),
            castle.clone(),
            crate::supervisor::AgentDefaults::new(default_harness.clone(), default_agent),
            budget,
            fleet_budget,
            space.clone(),
            tickets.clone(),
        )?);
        let (shutdown_tx, _) = watch::channel(false);
        let repos = std::sync::Mutex::new(crate::repos::RepoRegistry::load(
            &layout.home().join("repos.json"),
        )?);
        let onboarding_sessions =
            std::sync::Mutex::new(crate::onboarding_sessions::OnboardingSessions::load(
                &layout.home().join("onboarding-sessions.json"),
            )?);
        let coordinator_sessions =
            std::sync::Mutex::new(crate::coordinator::CoordinatorSessions::load(
                &layout.home().join("coordinator-sessions.json"),
            )?);
        Ok(Self {
            layout,
            space,
            // Default the display to the wire id; Daemon::new overrides it with
            // the configured alias. Test constructors keep id == display.
            castle_display: castle.clone(),
            castle,
            supervisor,
            syncer: None,
            sync_interval: Duration::from_secs(30),
            reactor_config: rk_core::config::ReactorConfig::default(),
            scheduler_config: rk_core::config::SchedulerConfig::default(),
            sweep_config: rk_core::config::SupervisorConfig::default(),
            review_sweep_config: rk_core::config::ReviewSweepConfig::default(),
            drain_config: rk_core::config::DrainConfig::default(),
            evaporation_decay: rk_core::config::EvaporationConfig::default().decay,
            global_agents: Default::default(),
            tier_routing: Default::default(),
            default_harness,
            require_named_checks: false,
            require_approval_for_landing: true,
            default_merge_mode: rk_core::config::MergeMode::default(),
            allowed_target_branches: rk_core::config::PolicyConfig::default()
                .allowed_target_branches,
            auth_token,
            engine: std::sync::OnceLock::new(),
            repos,
            onboarding_sessions,
            onboarding_apply_lock: tokio::sync::Mutex::new(()),
            tickets,
            coordinator_sessions,
            fact_vote_lock: std::sync::Mutex::new(()),
            started: Instant::now(),
            shutdown_tx,
        })
    }

    /// Bind the socket (clearing a stale one if the previous daemon died) and
    /// serve until a `stop` request or SIGTERM/SIGINT arrives.
    pub async fn run(self) -> rk_core::Result<()> {
        self.layout.ensure()?;
        let sock = self.layout.socket_path();

        if sock.exists() {
            if UnixStream::connect(&sock).await.is_ok() {
                return Err(rk_core::Error::other(format!(
                    "daemon already running on {}",
                    sock.display()
                )));
            }
            // Connect failure alone does not prove staleness — THIS process
            // may be sandboxed away from the socket. Only reclaim it if the
            // recorded owner pid is actually dead.
            if let Some(pid) = read_pid(&self.layout) {
                if process_alive(pid) {
                    return Err(rk_core::Error::other(format!(
                        "daemon pid {pid} appears alive but its socket is unreachable \
                         from this process (sandbox?) — refusing to clobber {}",
                        sock.display()
                    )));
                }
            }
            debug!(path = %sock.display(), "removing stale socket");
            std::fs::remove_file(&sock)?;
        }

        let listener = UnixListener::bind(&sock)?;
        // A Unix socket's mode is otherwise inherited from the process umask.
        // The token is the primary credential, but filesystem permissions are
        // the first and cheapest boundary for local clients.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o600))?;
        }
        std::fs::write(self.layout.pid_file(), std::process::id().to_string())?;
        info!(socket = %sock.display(), pid = std::process::id(), castle = %self.castle_display, "daemon listening");
        // Only now that the bind is won may shared state be touched.
        self.supervisor.on_daemon_started();
        match self.onboarding_sessions.lock() {
            Ok(mut sessions) => {
                if let Err(error) = sessions.orphan_nonterminal() {
                    warn!(%error, "failed to orphan onboarding sessions");
                }
            }
            Err(_) => warn!("onboarding session registry lock poisoned"),
        }

        let daemon = Arc::new(self);
        let mut shutdown_rx = daemon.shutdown_tx.subscribe();

        // GC loop: decay pheromone trails and collect faded/expired tuples —
        // escalation/analytics live elsewhere.
        {
            let space = daemon.space.clone();
            let decay = daemon.evaporation_decay;
            let mut gc_shutdown = daemon.shutdown_tx.subscribe();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(GC_INTERVAL);
                loop {
                    tokio::select! {
                        _ = tick.tick() => match space.gc_expired(decay) {
                            Ok(0) => {}
                            Ok(n) => debug!(collected = n, "gc collected faded/expired tuples"),
                            Err(e) => warn!(error = %e, "gc failed"),
                        },
                        _ = gc_shutdown.changed() => break,
                    }
                }
            });
        }

        // Supervisor liveness/burn-rate sweep: catch rats hung mid-tool-call
        // (silent, so budget checks never see them) or running cost away with no
        // completion. Graduated: obstacle + steer, then kill after a grace pass.
        if daemon.sweep_config.enabled {
            let supervisor = Arc::clone(&daemon.supervisor);
            let cfg = daemon.sweep_config.clone();
            let mut sweep_shutdown = daemon.shutdown_tx.subscribe();
            let interval = Duration::from_secs(cfg.interval_secs.max(1));
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(interval);
                // Consume the immediate first tick so freshly-spawned rats get a
                // full interval of grace before the first sweep looks at them.
                tick.tick().await;
                loop {
                    tokio::select! {
                        _ = tick.tick() => {
                            let supervisor = Arc::clone(&supervisor);
                            let cfg = cfg.clone();
                            let handle = tokio::runtime::Handle::current();
                            if let Err(e) = tokio::task::spawn_blocking(move || {
                                let _entered = handle.enter();
                                supervisor.sweep(&cfg);
                                // Self-healing respawn rides the same tick (TKT-53):
                                // relaunch crashed/orphaned rats with crash-loop
                                // backoff. No-op unless [supervisor].respawn_enabled.
                                supervisor.respawn_sweep(&cfg);
                            })
                            .await
                            {
                                warn!(error = %e, "supervisor sweep task failed");
                            }
                        }
                        _ = sweep_shutdown.changed() => break,
                    }
                }
            });
        }

        // Fetch-driven awaiting-review clear (TKT-70). Periodically fetch+prune
        // each repo with an open PR and check whether the forge merged/deleted
        // the branch upstream — clearing the inbox row for a merge the operator
        // never pulled. Off by default (fetch is network + can hang) and coarse;
        // the fetch stays here, off the hot inbox read path, which only reads the
        // `pull_request_closed` events this loop emits.
        if daemon.review_sweep_config.enabled {
            let daemon_ref = Arc::clone(&daemon);
            let mut rs_shutdown = daemon.shutdown_tx.subscribe();
            let interval = Duration::from_secs(daemon.review_sweep_config.interval_secs.max(1));
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(interval);
                // Consume the immediate first tick: give a freshly-opened PR a
                // full interval before the first fetch, and don't fetch on boot.
                tick.tick().await;
                loop {
                    tokio::select! {
                        _ = tick.tick() => {
                            let d = Arc::clone(&daemon_ref);
                            match tokio::task::spawn_blocking(move || d.review_sweep_once()).await {
                                Ok(0) => {}
                                Ok(n) => debug!(closed = n, "review sweep cleared awaiting-review rows"),
                                Err(e) => warn!(error = %e, "review sweep task panicked"),
                            }
                        }
                        _ = rs_shutdown.changed() => break,
                    }
                }
            });
        }

        // Multiplayer sync loop (git shell-outs are blocking → spawn_blocking).
        if let Some(syncer) = daemon.syncer.clone() {
            let space = daemon.space.clone();
            let interval = daemon.sync_interval;
            let mut sync_shutdown = daemon.shutdown_tx.subscribe();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(interval);
                loop {
                    tokio::select! {
                        _ = tick.tick() => {
                            let syncer = Arc::clone(&syncer);
                            let space = space.clone();
                            let result = tokio::task::spawn_blocking(move || {
                                syncer.run_cycle(&space)
                            })
                            .await;
                            match result {
                                Ok(Ok(stats)) => debug!(?stats, "sync cycle"),
                                Ok(Err(e)) => warn!(error = %e, "sync cycle failed"),
                                Err(e) => warn!(error = %e, "sync task panicked"),
                            }
                        }
                        _ = sync_shutdown.changed() => break,
                    }
                }
            });
        }

        // Reactor loop: fire registered #Trigger workflows on matching tuples.
        // The lossy feed is only a wake signal; a durable cursor scan is the
        // source of truth, so no event is missed even when the feed drops it.
        if daemon.reactor_config.enabled {
            let reactor = Arc::new(crate::reactor::Reactor::new(
                daemon.space.clone(),
                daemon.engine(),
                daemon.tickets.clone(),
                // The live-session owner, so a promoted convention can be steered
                // into already-running rats (TKT-34).
                Some(Arc::clone(&daemon.supervisor)),
                daemon.layout.clone(),
                daemon.reactor_config.clone(),
            ));
            // Baseline the cursor so a fresh daemon does not react to the whole
            // pre-existing backlog on first boot.
            if let Err(e) = reactor.initialize_cursor() {
                warn!(error = %e, "reactor cursor init failed");
            }
            let mut feed = daemon.space.subscribe();
            let mut reactor_shutdown = daemon.shutdown_tx.subscribe();
            let interval = Duration::from_secs(daemon.reactor_config.interval_secs.max(1));
            // A cycle runs on a blocking thread (it shells out to `cue`), but its
            // dispatch calls `engine.run`, which `tokio::spawn`s the workflow — so
            // the blocking thread must enter the runtime context first.
            let handle = tokio::runtime::Handle::current();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(interval);
                loop {
                    tokio::select! {
                        _ = tick.tick() => {}
                        recv = feed.recv() => match recv {
                            // Coalesce a burst: drain what is already queued so a
                            // single scan covers the whole batch.
                            Ok(_) => while feed.try_recv().is_ok() {},
                            // Dropped events are exactly why dispatch is scan-driven.
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        },
                        _ = reactor_shutdown.changed() => break,
                    }
                    let reactor = Arc::clone(&reactor);
                    let handle = handle.clone();
                    match tokio::task::spawn_blocking(move || {
                        let _guard = handle.enter();
                        reactor.run_cycle()
                    })
                    .await
                    {
                        Ok(Ok(0)) => {}
                        Ok(Ok(n)) => debug!(fired = n, "reactor cycle fired workflows"),
                        Ok(Err(e)) => warn!(error = %e, "reactor cycle failed"),
                        Err(e) => warn!(error = %e, "reactor task panicked"),
                    }
                }
            });
        }

        // Scheduler loop: fire registered #Schedule workflows on a cron cadence.
        // The TIME-axis sibling of the reactor — a purely clock-driven trigger.
        // A durable minute-cursor makes it catch-up-once after downtime, and each
        // schedule is single-flight so a slow run never stacks on itself.
        if daemon.scheduler_config.enabled {
            let scheduler = Arc::new(crate::scheduler::Scheduler::new(
                daemon.engine(),
                daemon.layout.clone(),
                daemon.scheduler_config.clone(),
            ));
            // Baseline the cursor so a fresh daemon does not fire schedules for
            // minutes that elapsed before it started.
            if let Err(e) = scheduler.initialize_cursor() {
                warn!(error = %e, "scheduler cursor init failed");
            }
            let mut scheduler_shutdown = daemon.shutdown_tx.subscribe();
            // Must tick at least once a minute or a matching minute is skipped.
            let interval = Duration::from_secs(daemon.scheduler_config.interval_secs.clamp(1, 60));
            // A cycle runs on a blocking thread (it shells out to `cue`), and its
            // dispatch calls `engine.run`, which `tokio::spawn`s the workflow — so
            // the blocking thread must enter the runtime context first.
            let handle = tokio::runtime::Handle::current();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(interval);
                loop {
                    tokio::select! {
                        _ = tick.tick() => {}
                        _ = scheduler_shutdown.changed() => break,
                    }
                    let scheduler = Arc::clone(&scheduler);
                    let handle = handle.clone();
                    match tokio::task::spawn_blocking(move || {
                        let _guard = handle.enter();
                        scheduler.run_cycle()
                    })
                    .await
                    {
                        Ok(Ok(0)) => {}
                        Ok(Ok(n)) => debug!(fired = n, "scheduler cycle fired workflows"),
                        Ok(Err(e)) => warn!(error = %e, "scheduler cycle failed"),
                        Err(e) => warn!(error = %e, "scheduler task panicked"),
                    }
                }
            });
        }

        // Continuous-drain loop: a WIP-limited fleet autoscaler. While fewer than
        // `max_wip` rats are live and the ready backlog is non-empty, claim the
        // highest-priority ready ticket and spawn a rat — the always-on refill
        // counterpart to a one-shot backlog-drain workflow. Off unless explicitly
        // enabled *and* given a positive cap (handing the dispatch loop to the
        // daemon is opt-in). Wakes on the tuple feed (a completion frees a slot)
        // with the interval as a fallback, mirroring the reactor.
        if daemon.drain_config.enabled && daemon.drain_config.max_wip > 0 {
            let drain = Arc::new(crate::drain::Drain::new(
                Arc::clone(&daemon.supervisor),
                daemon.tickets.clone(),
                daemon.layout.clone(),
                daemon.drain_config.clone(),
                daemon.tier_routing.clone(),
                daemon.global_agents.clone(),
                daemon.default_harness.clone(),
            ));
            let mut feed = daemon.space.subscribe();
            let mut drain_shutdown = daemon.shutdown_tx.subscribe();
            let interval = Duration::from_secs(daemon.drain_config.interval_secs.max(1));
            // Unlike the reactor/scheduler, a drain cycle shells out to nothing
            // (it claims tickets and spawns) so it runs directly in this async
            // task — the same context the RPC spawn path already uses.
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(interval);
                loop {
                    tokio::select! {
                        _ = tick.tick() => {}
                        recv = feed.recv() => match recv {
                            // Coalesce a burst so one refill covers the batch.
                            Ok(_) => while feed.try_recv().is_ok() {},
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        },
                        _ = drain_shutdown.changed() => break,
                    }
                    match drain.run_cycle().await {
                        Ok(0) => {}
                        Ok(n) => debug!(spawned = n, "drain cycle refilled fleet"),
                        Err(e) => warn!(error = %e, "drain cycle failed"),
                    }
                }
            });
        }

        // Rehydrate persisted workflow instances (TKT-52): restore status/list
        // history and RESUME any that were mid-run when the daemon last stopped
        // — a crash/restart no longer silently drops in-flight instances
        // (parked gates, fan-outs awaiting wait_all). Runs after the socket bind
        // (shared state is safe to touch) and after the reactor/scheduler are up
        // so a resumed instance's completion event is observed by them.
        daemon.engine().rehydrate();

        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _)) => {
                            let peer_pid = stream
                                .peer_cred()
                                .ok()
                                .and_then(|credentials| credentials.pid())
                                .and_then(|pid| u32::try_from(pid).ok());
                            let origin = PeerOrigin {
                                pid_observed: peer_pid.is_some(),
                                supervised_agents: peer_pid
                                    .map(|pid| daemon.supervisor.supervised_agents_for_peer(pid))
                                    .unwrap_or_default(),
                            };
                            let daemon = Arc::clone(&daemon);
                            tokio::spawn(async move {
                                if let Err(e) = daemon.serve_conn(stream, origin).await {
                                    debug!(error = %e, "connection ended with error");
                                }
                            });
                        }
                        Err(e) => warn!(error = %e, "accept failed"),
                    }
                }
                _ = shutdown_rx.changed() => {
                    info!("shutdown requested");
                    break;
                }
                _ = shutdown_signal() => {
                    info!("signal received, shutting down");
                    break;
                }
            }
        }

        // Remove the socket/pid files only if they are still OURS — a newer
        // daemon may have already bound a fresh socket at the same path, and
        // unlinking it would strand that daemon unreachable.
        let ours = std::fs::read_to_string(daemon.layout.pid_file())
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok())
            == Some(std::process::id());
        if ours {
            std::fs::remove_file(daemon.layout.socket_path()).ok();
            std::fs::remove_file(daemon.layout.pid_file()).ok();
        }
        Ok(())
    }

    fn engine(&self) -> Arc<crate::workflow_exec::WorkflowEngine> {
        Arc::clone(self.engine.get_or_init(|| {
            Arc::new(crate::workflow_exec::WorkflowEngine::new(
                self.layout.clone(),
                Arc::clone(&self.supervisor),
                self.space.clone(),
                Arc::clone(&self.tickets),
                self.global_agents.clone(),
                self.tier_routing.clone(),
                self.default_harness.clone(),
                self.require_named_checks,
                // A crashed rat may still be revived by the self-healing sweep;
                // a `wait` only gives up on one when it cannot be (TKT-147).
                self.sweep_config.respawn_enabled && self.sweep_config.respawn_max_attempts > 0,
                self.require_approval_for_landing,
                self.allowed_target_branches.clone(),
            ))
        }))
    }

    async fn serve_conn(&self, stream: UnixStream, origin: PeerOrigin) -> std::io::Result<()> {
        let (read, mut write) = stream.into_split();
        let mut read = BufReader::new(read);
        let mut buf = Vec::new();

        loop {
            buf.clear();
            loop {
                let available = tokio::time::timeout(Duration::from_secs(30), read.fill_buf())
                    .await
                    .map_err(|_| {
                        std::io::Error::new(std::io::ErrorKind::TimedOut, "request timeout")
                    })??;
                if available.is_empty() {
                    if buf.is_empty() {
                        return Ok(());
                    }
                    break;
                }
                let newline = available.iter().position(|byte| *byte == b'\n');
                let take = newline.map_or(available.len(), |position| position + 1);
                if buf.len() + take > MAX_FRAME_BYTES {
                    write_json_line(
                        &mut write,
                        &Response::err("", codes::FRAME_TOO_LARGE, "request exceeds 1 MiB"),
                    )
                    .await?;
                    return Ok(());
                }
                buf.extend_from_slice(&available[..take]);
                read.consume(take);
                if newline.is_some() {
                    break;
                }
            }
            let line = String::from_utf8_lossy(&buf);
            if line.trim().is_empty() {
                continue;
            }
            let outcome = match serde_json::from_str::<Request>(&line) {
                Ok(req) if !self.authenticated(&req) => Outcome::Reply(Response::err(
                    req.id,
                    codes::UNAUTHORIZED,
                    "invalid daemon token",
                )),
                Ok(req) if !self.authorized(&req, &origin) => Outcome::Reply(Response::err(
                    req.id,
                    codes::FORBIDDEN,
                    format!("{} is not authorized for {}", req.caller, req.method),
                )),
                Ok(req) => self.dispatch(req).await,
                Err(e) => Outcome::Reply(Response::err(
                    "",
                    codes::BAD_PARAMS,
                    format!("bad request: {e}"),
                )),
            };
            match outcome {
                Outcome::Reply(response) => {
                    write_json_line(&mut write, &response).await?;
                }
                Outcome::Watch { response, pattern } => {
                    write_json_line(&mut write, &response).await?;
                    return self.stream_watch(write, pattern).await;
                }
                Outcome::CoordinatorWatch {
                    response,
                    filter,
                    boundary,
                    rx,
                } => {
                    write_json_line(&mut write, &response).await?;
                    return self.stream_coordinator(write, filter, boundary, rx).await;
                }
                Outcome::LogFollow {
                    response,
                    agent,
                    generation,
                } => {
                    write_json_line(&mut write, &response).await?;
                    return self.stream_log(write, agent, generation).await;
                }
            }
        }
    }

    fn authorized(&self, req: &Request, origin: &PeerOrigin) -> bool {
        let operator = req.caller == "operator" || req.caller.is_empty();
        // The bearer root token alone is not operator authority. A local
        // connection must have a kernel-observed pid, and a connection rooted
        // in exactly one supervised agent may claim only that agent. This
        // closes both `env -u RK_AGENT -u RK_AUTH_TOKEN rk ...` and
        // cross-agent token derivation from the same-user root credential.
        if !origin.pid_observed && operator {
            return false;
        }
        if !origin.supervised_agents.is_empty()
            && (origin.supervised_agents.len() != 1
                || !origin.supervised_agents.contains(&req.caller))
        {
            return false;
        }
        if operator {
            return true;
        }
        if req.auth != rk_core::paths::derive_agent_token(&self.auth_token, &req.caller) {
            return false;
        }
        if let Some(record) = self.supervisor.status(&req.caller) {
            if record.role == crate::onboarding_sessions::ONBOARDER_ROLE {
                return self.onboarder_authorized(req);
            }
            if crate::supervisor::validate_role(&record.role).is_err() {
                return false;
            }
        }
        if !matches!(
            req.method.as_str(),
            "stop"
                | "agent.spawn"
                | "agent.respawn"
                | "agent.dismiss"
                | "agent.interrupt"
                | "agent.steer"
                | "agent.archive"
                | "agent.unarchive"
                | "agent.revert"
                | "repo.add"
                | "repo.remove"
                | "repo.onboard.start"
                | "repo.onboard.propose"
                | "repo.onboard.approve"
                | "repo.onboard.decline"
                | "repo.onboard.apply"
                | "repo.onboard.activate"
                | "repo.onboard.decline_activation"
                | "repo.onboard.cleanup"
                | "repo.onboard.resume"
                | "repo.onboard.status"
                | "repo.onboard.report"
                | "workflow.run"
                | "workflow.approve"
                | "workflow.archive"
                | "workflow.unarchive"
                | "coordinator.snapshot"
                | "coordinator.watch"
                | "coordinator.register"
                | "coordinator.pending"
                | "coordinator.ack"
                | "sync.now"
                | "sync.peers"
                | "ticket.update"
                | "ticket.dep"
        ) {
            return true;
        }

        // A foreman gets only the child-management subset. Each target is
        // checked again at dispatch time against the structural parent edge;
        // this broad authorization is only the first gate.
        self.supervisor.is_foreman(&req.caller)
            && matches!(
                req.method.as_str(),
                "agent.spawn"
                    | "agent.respawn"
                    | "agent.dismiss"
                    | "agent.interrupt"
                    | "agent.steer"
            )
    }

    /// Enforced capability profile for the onboarding role. This is
    /// intentionally much smaller than the ordinary rat surface: inspection
    /// reads, self progress, and the one completion event required by `rk
    /// done`. It is evaluated after peer-origin and token binding, so clearing
    /// ambient identity cannot select the operator arm above.
    fn onboarder_authorized(&self, req: &Request) -> bool {
        match req.method.as_str() {
            "ping"
            | "status"
            | "space.scan"
            | "space.rd"
            | "repo.list"
            | "repo.get"
            | "repo.onboard.inspect"
            | "repo.onboard.propose"
            | "agent.status"
            | "agent.log"
            | "agent.progress" => true,
            "space.out" => {
                req.params.get("category").and_then(Value::as_str) == Some("event")
                    && req.params.get("identity").and_then(Value::as_str) == Some("task_done")
                    && req
                        .params
                        .get("instance")
                        .and_then(Value::as_str)
                        .is_none_or(|instance| instance == req.caller)
                    && req
                        .params
                        .get("payload")
                        .and_then(|payload| payload.get("agent"))
                        .and_then(Value::as_str)
                        == Some(req.caller.as_str())
            }
            _ => false,
        }
    }

    fn authenticated(&self, req: &Request) -> bool {
        if req.caller == "operator" || req.caller.is_empty() {
            req.auth == self.auth_token
        } else {
            req.auth == rk_core::paths::derive_agent_token(&self.auth_token, &req.caller)
        }
    }

    /// Push matching tuples as notification lines until the client goes away.
    async fn stream_watch(
        &self,
        mut write: tokio::net::unix::OwnedWriteHalf,
        pattern: Pattern,
    ) -> std::io::Result<()> {
        let mut rx = self.space.subscribe();
        loop {
            match rx.recv().await {
                Ok(tuple) if pattern.matches(&tuple) => {
                    let note = json!({"method": "tuple", "params": tuple});
                    write_json_line(&mut write, &note).await?;
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                    let note = json!({"method": "lagged", "params": {"missed": missed}});
                    write_json_line(&mut write, &note).await?;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }

    /// Push durable coordinator events after the snapshot/replay response.
    /// `rx` was subscribed before the durable backlog scan, so the feed is only
    /// a wake/continuation channel; journal sequences at or before `boundary`
    /// are skipped as already covered by the response.
    async fn stream_coordinator(
        &self,
        mut write: tokio::net::unix::OwnedWriteHalf,
        filter: CoordinatorFilter,
        boundary: Option<u64>,
        mut rx: broadcast::Receiver<CoordinatorEvent>,
    ) -> std::io::Result<()> {
        let mut cursor = boundary;
        loop {
            match rx.recv().await {
                Ok(coordinator_event)
                    if filter.matches_event(&coordinator_event.event)
                        && cursor.is_none_or(|seen| coordinator_event.cursor > seen) =>
                {
                    cursor = Some(coordinator_event.cursor);
                    let note = json!({
                        "method": "coordinator.event",
                        "params": {
                            "cursor": coordinator_event.cursor,
                            "event": coordinator_event.event,
                        },
                    });
                    write_json_line(&mut write, &note).await?;
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                    let note = json!({
                        "method": "lagged",
                        "params": {"missed": missed, "resync_required": true},
                    });
                    write_json_line(&mut write, &note).await?;
                    return Ok(());
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }

    fn coordinator_snapshot(&self, filter: &CoordinatorFilter) -> rk_core::Result<Value> {
        let events = self.space.coordinator_events_after(
            None,
            crate::coordinator::MAX_REPLAY_EVENTS.saturating_mul(4),
        )?;
        let snapshot = crate::coordinator::hierarchical_snapshot(
            &self.engine().list(),
            &self.supervisor.list(),
            &events,
            filter,
        );
        Ok(json!({
            "snapshot": snapshot,
            "cursor": self.latest_event_cursor(),
        }))
    }

    fn prepare_coordinator_watch(
        &self,
        id: String,
        filter: CoordinatorFilter,
    ) -> rk_core::Result<Outcome> {
        // Subscribe before taking either the cursor boundary or the durable
        // replay scan. Events written after this point are guaranteed to be in
        // either the replay result or the live receiver, then deduplicated by
        // journal sequence in stream_coordinator.
        let rx = self.space.subscribe_coordinator();
        let baseline = self.latest_event_cursor();
        let scanned = if filter.after.is_some() {
            self.space
                .coordinator_events_after(filter.after, crate::coordinator::MAX_REPLAY_EVENTS + 1)?
        } else {
            Vec::new()
        };
        let replay = crate::coordinator::replay(scanned, &filter);
        let boundary = max_cursor(baseline, replay.boundary);
        let events = self.space.coordinator_events_after(
            None,
            crate::coordinator::MAX_REPLAY_EVENTS.saturating_mul(4),
        )?;
        let snapshot = crate::coordinator::hierarchical_snapshot(
            &self.engine().list(),
            &self.supervisor.list(),
            &events,
            &filter,
        );
        Ok(Outcome::CoordinatorWatch {
            response: Response::ok(
                id,
                json!({
                    "snapshot": snapshot,
                    "cursor": boundary,
                    "events": replay
                        .events
                        .iter()
                        .map(|event| json!({"cursor": event.cursor, "event": event.event}))
                        .collect::<Vec<_>>(),
                    "truncated": replay.truncated,
                    "resync_required": replay.truncated,
                }),
            ),
            filter,
            boundary,
            rx,
        })
    }

    fn latest_event_cursor(&self) -> Option<u64> {
        self.space.coordinator_latest_sequence().ok().flatten()
    }

    fn handle_coordinator_register(&self, req: Request) -> Response {
        let params: CoordinatorRegisterParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        if params.coordinator.trim().is_empty() {
            return Response::err(req.id, codes::BAD_PARAMS, "coordinator is required");
        }
        let mut sessions = match self.coordinator_sessions.lock() {
            Ok(sessions) => sessions,
            Err(poisoned) => poisoned.into_inner(),
        };
        match sessions.register(&params.coordinator, params.after) {
            Ok(session) => Response::ok(req.id, json!({"session": session})),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    fn handle_coordinator_pending(&self, req: Request) -> Response {
        let mut filter: CoordinatorFilter = match parse_params(&req.params) {
            Ok(filter) => filter,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let Some(coordinator) = filter.coordinator.clone() else {
            return Response::err(req.id, codes::BAD_PARAMS, "coordinator is required");
        };
        let cursor = {
            let sessions = match self.coordinator_sessions.lock() {
                Ok(sessions) => sessions,
                Err(poisoned) => poisoned.into_inner(),
            };
            sessions
                .cursor(&coordinator)
                .unwrap_or(filter.after.unwrap_or(0))
        };
        filter.after = Some(cursor);
        match self.coordinator_pending(&filter) {
            Ok(result) => Response::ok(req.id, result),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    fn handle_coordinator_ack(&self, req: Request) -> Response {
        let params: CoordinatorAckParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let mut sessions = match self.coordinator_sessions.lock() {
            Ok(sessions) => sessions,
            Err(poisoned) => poisoned.into_inner(),
        };
        match sessions.acknowledge(&params.coordinator, params.cursor) {
            Ok(session) => Response::ok(req.id, json!({"session": session})),
            Err(e) => Response::err(req.id, codes::BAD_PARAMS, e.to_string()),
        }
    }

    fn coordinator_pending(&self, filter: &CoordinatorFilter) -> rk_core::Result<Value> {
        let scanned = self
            .space
            .coordinator_events_after(filter.after, crate::coordinator::MAX_REPLAY_EVENTS + 1)?;
        let replay = crate::coordinator::replay(scanned, filter);
        let snapshot = self.coordinator_snapshot(filter)?;
        Ok(json!({
            "snapshot": snapshot["snapshot"],
            "cursor": snapshot["cursor"],
            "events": replay.events.iter().map(|event| json!({"cursor": event.cursor, "event": event.event})).collect::<Vec<_>>(),
            "truncated": replay.truncated,
            "resync_required": replay.truncated,
            "ack_after": replay.boundary.or(snapshot["cursor"].as_u64()),
        }))
    }

    /// Push `agent`'s new transcript entries as they land, until the client goes
    /// away. The backlog was already sent as the `agent.log` reply; this is the
    /// live tail (there may be a momentary overlap of one boundary entry).
    async fn stream_log(
        &self,
        mut write: tokio::net::unix::OwnedWriteHalf,
        agent: String,
        generation: Option<DateTime<Utc>>,
    ) -> std::io::Result<()> {
        let mut rx = self.supervisor.log().subscribe();
        loop {
            match rx.recv().await {
                Ok(rec) if rec.agent == agent && Some(rec.generation) == generation => {
                    let note = json!({"method": "log", "params": rec.entry});
                    write_json_line(&mut write, &note).await?;
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                    let note = json!({"method": "lagged", "params": {"missed": missed}});
                    write_json_line(&mut write, &note).await?;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }

    async fn dispatch(&self, req: Request) -> Outcome {
        debug!(method = %req.method, id = %req.id, "dispatch");
        let id = req.id.clone();
        let reply = |r: Response| Outcome::Reply(r);
        match req.method.as_str() {
            "ping" => reply(Response::ok(id, json!("pong"))),
            "status" => reply(Response::ok(id, self.status())),
            "stop" => {
                let resp = Response::ok(id, json!({"stopping": true}));
                let _ = self.shutdown_tx.send(true);
                reply(resp)
            }
            "space.out" => reply(self.handle_out(req)),
            "space.withdraw" => reply(self.handle_withdraw(req)),
            "fact.vote" => reply(self.handle_fact_vote(req)),
            "space.scan" => reply(self.handle_scan(req)),
            "space.take" => reply(self.handle_blocking(req, true).await),
            "space.rd" => reply(self.handle_blocking(req, false).await),
            "space.watch" => match parse_params::<PatternParams>(&req.params) {
                Ok(p) => Outcome::Watch {
                    response: Response::ok(id, json!({"watching": true})),
                    pattern: p.pattern,
                },
                Err(e) => reply(Response::err(id, codes::BAD_PARAMS, e)),
            },
            "coordinator.snapshot" => match parse_params::<CoordinatorFilter>(&req.params) {
                Ok(filter) if filter_is_valid(&filter) => {
                    match self.coordinator_snapshot(&filter) {
                        Ok(snapshot) => reply(Response::ok(id, snapshot)),
                        Err(e) => reply(Response::err(id, codes::INTERNAL, e.to_string())),
                    }
                }
                Ok(_) => reply(Response::err(
                    id,
                    codes::BAD_PARAMS,
                    "a coordinator scope is required (instance, coordinator, subtree, or repo)",
                )),
                Err(e) => reply(Response::err(id, codes::BAD_PARAMS, e)),
            },
            "coordinator.register" => reply(self.handle_coordinator_register(req)),
            "coordinator.pending" => reply(self.handle_coordinator_pending(req)),
            "coordinator.ack" => reply(self.handle_coordinator_ack(req)),
            "coordinator.watch" => match parse_params::<CoordinatorFilter>(&req.params) {
                Ok(filter) if filter_is_valid(&filter) => {
                    match self.prepare_coordinator_watch(id, filter) {
                        Ok(outcome) => outcome,
                        Err(e) => reply(Response::err(req.id, codes::INTERNAL, e.to_string())),
                    }
                }
                Ok(_) => reply(Response::err(
                    id,
                    codes::BAD_PARAMS,
                    "a coordinator scope is required (instance, coordinator, subtree, or repo)",
                )),
                Err(e) => reply(Response::err(id, codes::BAD_PARAMS, e)),
            },
            "agent.spawn" => reply(self.handle_spawn(req).await),
            "agent.progress" => reply(self.handle_progress(req)),
            "agent.respawn" => reply(self.handle_respawn(req).await),
            "agent.list" => reply(match parse_params::<AgentListParams>(&req.params) {
                Ok(p) => {
                    let agents = if p.archived_only {
                        self.supervisor.list_archived()
                    } else if p.include_archived {
                        self.supervisor.list_all()
                    } else {
                        self.supervisor.list()
                    };
                    Response::ok(id, json!({ "agents": agents }))
                }
                Err(e) => Response::err(id, codes::BAD_PARAMS, e),
            }),
            "agent.archive" => reply(self.handle_agent_archive(req).await),
            "agent.unarchive" => {
                reply(self.handle_named(req, |sup, name| sup.unarchive_agent(&name)))
            }
            "budget.rollup" => reply(Response::ok(id, self.supervisor.fleet_rollup())),
            "inbox.list" => reply(self.handle_inbox(id).await),
            "agent.status" => reply(self.handle_named(req, |sup, name| {
                sup.status(&name)
                    .map(|r| json!({"agent": r}))
                    .ok_or_else(|| rk_core::Error::other(format!("no such agent: {name}")))
            })),
            "agent.log" => {
                let params: LogParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                // A name can have named more than one rat (the TKT-136 archiving
                // window did that to 24 of them), so resolve which generation is
                // meant instead of keying the read on the name alone. Default:
                // the newest, which is what an operator typing a name means.
                let generations = self.supervisor.log_generations(&params.name);
                let selected = match params.generation {
                    Some(n) => match n.checked_sub(1).and_then(|i| generations.get(i)) {
                        Some(g) => g.clone(),
                        None => {
                            let msg = format!(
                                "{} has {} log generation(s); no generation {n} (1 = oldest)",
                                params.name,
                                generations.len()
                            );
                            return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, msg));
                        }
                    },
                    None => generations
                        .last()
                        .cloned()
                        .unwrap_or_else(|| crate::agent_log::Generation::unrecorded(&params.name)),
                };
                let backlog = self.supervisor.log().read(&selected, params.tail);
                let response = Response::ok(
                    id,
                    json!({
                        "entries": backlog,
                        // How many rats have carried this name, and which one
                        // this is (1 = oldest; 0 = no record at all), so the
                        // client can disclose that a name is ambiguous.
                        "generations": generations.len(),
                        "generation": params.generation.unwrap_or(generations.len()),
                        "created_at": selected.start,
                    }),
                );
                if params.follow {
                    Outcome::LogFollow {
                        response,
                        agent: params.name,
                        generation: selected.start,
                    }
                } else {
                    reply(response)
                }
            }
            "agent.steer" => reply(self.handle_steer(req).await),
            "agent.interrupt" => {
                let params: NameParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                if let Err(e) = self.authorize_foreman_child(&req.caller, &params.name) {
                    return reply(Response::err(id, codes::FORBIDDEN, e.to_string()));
                }
                reply(match self.supervisor.interrupt(&params.name).await {
                    Ok(()) => Response::ok(id, json!({"interrupted": true})),
                    Err(e) => Response::err(id, codes::INTERNAL, e.to_string()),
                })
            }
            "agent.dismiss" => {
                let params: DismissParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                if let Err(e) = self.authorize_foreman_child(&req.caller, &params.name) {
                    return reply(Response::err(id, codes::FORBIDDEN, e.to_string()));
                }
                reply(
                    match self.supervisor.dismiss(&params.name, params.no_merge).await {
                        Ok(v) => Response::ok(id, v),
                        Err(e) => Response::err(id, codes::INTERNAL, e.to_string()),
                    },
                )
            }
            "agent.revert" => {
                let params: RevertParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                reply(
                    match self.supervisor.revert(&params.name, params.block).await {
                        Ok(v) => Response::ok(id, v),
                        Err(e) => Response::err(id, codes::INTERNAL, e.to_string()),
                    },
                )
            }
            "workflow.run" => reply(self.handle_workflow_run(req).await),
            "workflow.list" => {
                let params: WorkflowListParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                let engine = self.engine();
                let instances = match (params.archived, params.all) {
                    (true, _) => engine.list_archived(),
                    (false, true) => engine.list_all(),
                    (false, false) => engine.list(),
                };
                reply(Response::ok(id, json!({ "instances": instances })))
            }
            "workflow.archive" => reply(self.handle_workflow_archive(req).await),
            "workflow.unarchive" => {
                let params: NameParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                reply(match self.engine().unarchive(&params.name) {
                    Ok(Some(instance)) => Response::ok(id, json!({ "instance": instance })),
                    Ok(None) => Response::err(
                        id,
                        codes::INTERNAL,
                        format!("no archived workflow instance: {}", params.name),
                    ),
                    Err(e) => Response::err(id, codes::INTERNAL, e.to_string()),
                })
            }
            "workflow.status" => {
                let params: NameParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                reply(match self.engine().status_any(&params.name) {
                    Some(instance) => Response::ok(id, json!({"instance": instance})),
                    None => Response::err(
                        id,
                        codes::INTERNAL,
                        format!("no such instance: {}", params.name),
                    ),
                })
            }
            "workflow.timeline" => {
                let params: NameParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                let engine = self.engine();
                let name = params.name;
                let lookup_name = name.clone();
                let result =
                    tokio::task::spawn_blocking(move || engine.timeline(&lookup_name)).await;
                reply(match result {
                    Ok(Some((instance, steps))) => {
                        Response::ok(id, json!({"instance": instance, "steps": steps}))
                    }
                    Ok(None) => {
                        Response::err(id, codes::INTERNAL, format!("no such instance: {name}"))
                    }
                    Err(e) => Response::err(
                        id,
                        codes::INTERNAL,
                        format!("workflow timeline task failed: {e}"),
                    ),
                })
            }
            "workflow.approve" => {
                let params: WorkflowApproveParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                reply(
                    match self.engine().approve(
                        &params.instance,
                        params.approved,
                        &params.by,
                        params.reason,
                    ) {
                        Ok(()) => Response::ok(
                            id,
                            json!({"instance": params.instance, "approved": params.approved}),
                        ),
                        Err(e) => Response::err(id, codes::INTERNAL, e.to_string()),
                    },
                )
            }
            "workflow.definitions" => {
                let params: WorkflowDefsParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                let engine = self.engine();
                let result =
                    tokio::task::spawn_blocking(move || engine.definitions(&params.repo)).await;
                reply(match result {
                    Ok(definitions) => Response::ok(id, json!({"definitions": definitions})),
                    Err(e) => Response::err(
                        id,
                        codes::INTERNAL,
                        format!("workflow definitions task failed: {e}"),
                    ),
                })
            }
            "sync.now" => {
                let Some(syncer) = self.syncer.clone() else {
                    return Outcome::Reply(Response::err(
                        id,
                        codes::INTERNAL,
                        "sync is not enabled ([sync] enabled = true in config.toml)",
                    ));
                };
                let space = self.space.clone();
                let result = tokio::task::spawn_blocking(move || syncer.run_cycle(&space)).await;
                reply(match result {
                    Ok(Ok(stats)) => Response::ok(id, json!(stats)),
                    Ok(Err(e)) => Response::err(id, codes::INTERNAL, e.to_string()),
                    Err(e) => Response::err(id, codes::INTERNAL, e.to_string()),
                })
            }
            "sync.peers" => {
                let Some(syncer) = self.syncer.clone() else {
                    return Outcome::Reply(Response::err(
                        id,
                        codes::INTERNAL,
                        "sync is not enabled ([sync] enabled = true in config.toml)",
                    ));
                };
                let result = tokio::task::spawn_blocking(move || syncer.peers()).await;
                reply(match result {
                    Ok(Ok(peers)) => Response::ok(id, json!({"peers": peers})),
                    Ok(Err(e)) => Response::err(id, codes::INTERNAL, e.to_string()),
                    Err(e) => Response::err(id, codes::INTERNAL, e.to_string()),
                })
            }
            "repo.add" => reply(self.handle_repo_add(req).await),
            "repo.list" => reply(match self.repos.lock() {
                Ok(reg) => Response::ok(id, json!({"repos": reg.list()})),
                Err(_) => Response::err(id, codes::INTERNAL, "repo registry lock poisoned"),
            }),
            "repo.get" => reply(self.handle_repo_get(req)),
            "repo.onboard.start" => reply(self.handle_onboarding_start(req).await),
            "repo.onboard.propose" => reply(self.handle_onboarding_propose(req)),
            "repo.onboard.approve" => reply(self.handle_onboarding_approve(req)),
            "repo.onboard.decline" => reply(self.handle_onboarding_decline(req)),
            "repo.onboard.apply" => reply(self.handle_onboarding_apply(req).await),
            "repo.onboard.activate" => reply(self.handle_onboarding_activate(req).await),
            "repo.onboard.decline_activation" => {
                reply(self.handle_onboarding_decline_activation(req).await)
            }
            "repo.onboard.cleanup" => reply(self.handle_onboarding_cleanup(req).await),
            "repo.onboard.resume" => reply(self.handle_onboarding_resume(req).await),
            "repo.onboard.status" => reply(self.handle_onboarding_status(req)),
            "repo.onboard.report" => reply(self.handle_onboarding_report(req)),
            "repo.onboard.inspect" => {
                let params: RepoInspectParams = match parse_params(&req.params) {
                    Ok(params) => params,
                    Err(error) => {
                        return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, error));
                    }
                };
                let registered = match self.repos.lock() {
                    Ok(registry) => registry.list(),
                    Err(_) => {
                        return Outcome::Reply(Response::err(
                            id,
                            codes::INTERNAL,
                            "repo registry lock poisoned",
                        ));
                    }
                };
                let context = crate::onboarding::InspectContext {
                    default_harness: self.default_harness.clone(),
                    require_named_checks: self.require_named_checks,
                };
                let result = tokio::task::spawn_blocking(move || {
                    crate::onboarding::inspect(&params.target, &registered, &context)
                })
                .await;
                reply(match result {
                    Ok(report) => Response::ok(id, json!({"report": report})),
                    Err(error) => Response::err(
                        id,
                        codes::INTERNAL,
                        format!("repository inspection task failed: {error}"),
                    ),
                })
            }
            "ticket.new" => reply(self.handle_ticket_new(req).await),
            "ticket.list" => reply(self.handle_ticket_list(req)),
            "ticket.get" => reply(self.handle_ticket_get(req)),
            "ticket.update" => reply(self.handle_ticket_update(req).await),
            "ticket.dep" => reply(self.handle_ticket_dep(req).await),
            "ticket.ready" => reply(self.handle_ticket_ready(req)),
            other => reply(Response::err(
                id,
                codes::UNKNOWN_METHOD,
                format!("unknown method: {other}"),
            )),
        }
    }

    /// Union everything awaiting a human — failed/orphaned agents, failed or
    /// gate-parked workflow instances, obstacle and need tuples, and open PRs
    /// awaiting review — into one ranked triage list. Pure read-side
    /// aggregation; no new storage.
    async fn handle_inbox(&self, id: String) -> Response {
        let agents = self.supervisor.list();
        let instances = self.engine().list();
        let mut source_truncated = false;
        let mut scan = |pattern: &Pattern| {
            let mut tuples = self
                .space
                .scan_newest_limited(pattern, MAX_SCAN_TUPLES.saturating_add(1));
            if let Ok(rows) = &mut tuples {
                if rows.len() > MAX_SCAN_TUPLES {
                    source_truncated = true;
                    rows.truncate(MAX_SCAN_TUPLES);
                }
            }
            tuples
        };
        let obstacles = match scan(&Pattern::category(Category::Obstacle)) {
            Ok(t) => t,
            Err(e) => return Response::err(id, codes::INTERNAL, e.to_string()),
        };
        let needs = match scan(&Pattern::category(Category::Need)) {
            Ok(t) => t,
            Err(e) => return Response::err(id, codes::INTERNAL, e.to_string()),
        };
        // Open PRs/MRs: a PR-mode dismiss/land emits a `pull_request_opened`
        // event, then the run completes — nothing else tracks the pushed branch.
        let pull_requests =
            match scan(&Pattern::category(Category::Event).identity("pull_request_opened")) {
                Ok(t) => t,
                Err(e) => return Response::err(id, codes::INTERNAL, e.to_string()),
            };
        // `pull_request_closed` events are emitted by the fetch-driven review
        // sweep (TKT-70): a background pass fetched the forge and saw the branch
        // merged/deleted upstream even though the operator never pulled, so the
        // LOCAL detection below could not see it. `build` folds their branches
        // into the same suppression. Reading the events is cheap and stays on
        // the hot path; the fetch that produces them does not.
        let pull_requests_closed =
            match scan(&Pattern::category(Category::Event).identity("pull_request_closed")) {
                Ok(t) => t,
                Err(e) => return Response::err(id, codes::INTERNAL, e.to_string()),
            };
        // Every `land` step records its own outcome as a `branch_landed` event.
        // A land that neither merged nor opened a PR left the branch standing
        // outside the target, and reports that as a clean `{merged: false}`
        // rather than an error — so unless the workflow definition happened to
        // carry an `evaluate {expect: {merged: true}}` after its `land`, the
        // drop is silent (TKT-171). `build` asserts the invariant here instead,
        // for every workflow.
        let lands = match scan(&Pattern::category(Category::Event).identity("branch_landed")) {
            Ok(t) => t,
            Err(e) => return Response::err(id, codes::INTERNAL, e.to_string()),
        };
        // Reduce the land events to the branches that are actually candidate
        // rows BEFORE the git check below. `branch_landed` accumulates one event
        // per land the fleet has ever performed and never shrinks, while the
        // drops are a handful; the git check is a subprocess per branch and this
        // is the hot read path behind `rk top`.
        let lands: Vec<Tuple> = crate::inbox::dropped_lands(&lands)
            .into_iter()
            .cloned()
            .collect();
        // Open ballots (TKT-167). A `Suggestion` promotes to a permanent
        // `Convention` at quorum and otherwise stays open indefinitely (durable
        // since TKT-168), and nothing else in the fleet announces a vote — so a
        // proposal is only ever endorsed by a peer who goes looking for one it
        // has no reason to suspect exists. Measured 2026-07-25: zero conventions
        // had ever reached quorum over 277 spawns. Surfacing the ballot here
        // puts the always-reachable endorser — the operator — in front of it.
        // The three scans are read-side only; `build` does the counting.
        // `Withdrawal` is the fourth: the other settled state, and the only one
        // that retires a row now that a ballot no longer decays (TKT-184).
        let mut ballot_tuples = |category| scan(&Pattern::category(category));
        let (suggestions, endorsements, conventions, withdrawals) = match (
            ballot_tuples(Category::Suggestion),
            ballot_tuples(Category::Endorsement),
            ballot_tuples(Category::Convention),
            ballot_tuples(Category::Withdrawal),
        ) {
            (Ok(s), Ok(e), Ok(c), Ok(w)) => (s, e, c, w),
            (Err(e), _, _, _) | (_, Err(e), _, _) | (_, _, Err(e), _) | (_, _, _, Err(e)) => {
                return Response::err(id, codes::INTERNAL, e.to_string());
            }
        };
        // Both branch-shaped rows auto-clear once their branch is merged into
        // the target (or gone), and nothing emits a record when that happens —
        // no close event when a human merges a PR on the forge, and nothing at
        // all when a human hand-merges a dropped land. Detect it locally:
        // resolve each event's repo and ask git whether the branch has landed.
        // Local-only (no fetch, no forge API), so a row clears when the merge
        // reaches the local target — the operator's pull or a Direct-mode
        // fast-forward. The `pull_request_closed` events above close the same
        // gap for a forge merge the operator has NOT pulled (TKT-70).
        let cleared = match self.cleared_branches(&[&pull_requests, &lands]).await {
            Ok(cleared) => cleared,
            Err(e) => return Response::err(id, codes::INTERNAL, e.to_string()),
        };
        let items = crate::inbox::build(
            &agents,
            &instances,
            &obstacles,
            &needs,
            &crate::inbox::BranchEvents {
                cleared,
                pull_requests: &pull_requests,
                pull_requests_closed: &pull_requests_closed,
                lands: &lands,
            },
            &crate::inbox::Ballots {
                suggestions: &suggestions,
                endorsements: &endorsements,
                conventions: &conventions,
                withdrawals: &withdrawals,
                // The reactor's own quorum, so the tally shown is the tally that
                // promotes — and a configured 0 (promotion disabled) raises no
                // rows rather than offering a vote that can never resolve.
                quorum: self.reactor_config.quorum as usize,
                now: chrono::Utc::now(),
            },
        );
        let mut items = items;
        let mut response_truncated = source_truncated || items.len() > MAX_INBOX_ITEMS;
        items.truncate(MAX_INBOX_ITEMS);
        // A single tuple can carry a large operator-authored detail string, so
        // the item count cap alone does not prove the serialized response fits
        // the NDJSON frame. Drop lowest-priority tail rows until it does.
        while serde_json::to_vec(&json!({
            "items": &items,
            "truncated": response_truncated,
        }))
        .map(|bytes| bytes.len() > MAX_FRAME_BYTES)
        .unwrap_or(true)
        {
            if items.pop().is_none() {
                break;
            }
            response_truncated = true;
        }
        Response::ok(id, json!({"items": items, "truncated": response_truncated}))
    }

    /// (scope, branch) pairs among the given branch-shaped events whose branch
    /// has since been merged into its target or deleted locally — the rows that
    /// have auto-cleared and must drop out of the inbox. Resolves each event's
    /// scope to a registered repo path and asks git; an unregistered scope or
    /// unopenable repo means "cannot tell", so the row stays (fails toward
    /// surfacing, never hiding).
    ///
    /// Both branch-shaped sources share this check, because they ask the same
    /// question of git — did this branch reach its target? — about the same
    /// `{branch, target}` payload shape: `pull_request_opened` events (a PR the
    /// human merged on the forge, TKT-67/70) and `branch_landed` events (a land
    /// that dropped its branch and was later resolved by any route, TKT-171).
    /// One git call per distinct branch covers both.
    async fn cleared_branches(
        &self,
        event_sets: &[&[Tuple]],
    ) -> rk_core::Result<HashSet<(String, String)>> {
        let events: Vec<(String, String, String)> = event_sets
            .iter()
            .flat_map(|s| s.iter())
            .filter_map(|t| {
                let branch = t.payload.get("branch").and_then(|v| v.as_str())?;
                let target = t
                    .payload
                    .get("target")
                    .and_then(|v| v.as_str())
                    .unwrap_or("main");
                Some((t.scope.clone(), branch.to_string(), target.to_string()))
            })
            .collect();
        // Resolve scopes to paths once, under the registry lock, then release it
        // before shelling out to git. The actual Git calls run in a blocking
        // worker so a slow or locked repository cannot starve RPC handling.
        let paths = {
            let mut paths: HashMap<String, std::path::PathBuf> = HashMap::new();
            let reg = self
                .repos
                .lock()
                .map_err(|_| rk_core::Error::other("repo registry lock poisoned"))?;
            for (scope, _, _) in &events {
                if !paths.contains_key(scope) {
                    if let Some(rec) = reg.get(scope) {
                        paths.insert(scope.clone(), rec.path.clone());
                    }
                }
            }
            paths
        };
        tokio::task::spawn_blocking(move || cleared_branches_for_paths(events, paths))
            .await
            .map_err(|e| rk_core::Error::other(format!("inbox Git check task failed: {e}")))
    }

    /// One fetch-driven review-sweep cycle (TKT-70). For each repo carrying an
    /// open PR/MR whose branch has not already been closed, `git fetch --prune`
    /// the remote and ask whether the forge has since merged the branch into
    /// `<remote>/<target>` or deleted it — the case the local-only clear in
    /// [`cleared_pull_requests`](Daemon::cleared_pull_requests) misses because
    /// the operator never pulled. Each newly-resolved branch gets a durable
    /// `pull_request_closed` event, which `handle_inbox` folds into the
    /// awaiting-review suppression set. Returns the number of events emitted.
    ///
    /// Blocking (shells out to `git fetch`), so the caller runs it on a blocking
    /// thread; each fetch is hard-timeout-bounded. Idempotent: a branch already
    /// carrying a `pull_request_closed` event is skipped, so the durable event
    /// (and its rk-sync replication) is written once.
    fn review_sweep_once(&self) -> usize {
        let remote = self.review_sweep_config.remote.clone();
        let timeout = Duration::from_secs(self.review_sweep_config.fetch_timeout_secs.max(1));

        let open = match self
            .space
            .scan(&Pattern::category(Category::Event).identity("pull_request_opened"))
        {
            Ok(t) => t,
            Err(e) => {
                warn!(error = %e, "review sweep: scanning open PRs failed");
                return 0;
            }
        };
        if open.is_empty() {
            return 0;
        }
        let closed = self
            .space
            .scan(&Pattern::category(Category::Event).identity("pull_request_closed"))
            .unwrap_or_default();
        // (scope, branch) already resolved — never re-emit. The durable event
        // also replicates via rk-sync, so this guard keeps the sweep write-once.
        let mut already: HashSet<(String, String)> = HashSet::new();
        for t in &closed {
            if let Some(b) = t.payload.get("branch").and_then(|v| v.as_str()) {
                already.insert((t.scope.clone(), b.to_string()));
            }
        }
        // Still-open (scope, branch) -> target, deduped (a re-land repeats the
        // event for one branch; target is stable per branch).
        let mut pending: std::collections::HashMap<(String, String), String> =
            std::collections::HashMap::new();
        for t in &open {
            let Some(branch) = t.payload.get("branch").and_then(|v| v.as_str()) else {
                continue;
            };
            let key = (t.scope.clone(), branch.to_string());
            if already.contains(&key) {
                continue;
            }
            let target = t
                .payload
                .get("target")
                .and_then(|v| v.as_str())
                .unwrap_or("main")
                .to_string();
            pending.insert(key, target);
        }
        if pending.is_empty() {
            return 0;
        }
        // Group by scope so each repo is fetched exactly once per cycle.
        let mut by_scope: std::collections::HashMap<String, Vec<(String, String)>> =
            std::collections::HashMap::new();
        for ((scope, branch), target) in pending {
            by_scope.entry(scope).or_default().push((branch, target));
        }
        // Resolve scopes to repo paths once, under the registry lock, then
        // release it before shelling out to git.
        let mut paths: std::collections::HashMap<String, std::path::PathBuf> =
            std::collections::HashMap::new();
        if let Ok(reg) = self.repos.lock() {
            for scope in by_scope.keys() {
                if let Some(rec) = reg.get(scope) {
                    paths.insert(scope.clone(), rec.path.clone());
                }
            }
        }

        let mut emitted = 0;
        for (scope, branches) in by_scope {
            // Unregistered scope or unopenable repo: cannot fetch, so the row
            // stays surfaced (fails toward surfacing, never hiding).
            let Some(path) = paths.get(&scope) else {
                continue;
            };
            let Ok(repo) = rk_git::Repo::discover(path) else {
                continue;
            };
            if let Err(e) = repo.fetch_prune(&remote, timeout) {
                // A failed/timed-out fetch leaves every row of this repo intact;
                // the next cycle retries. Never hide a row on a network hiccup.
                warn!(error = %e, scope = %scope, "review sweep: fetch failed");
                continue;
            }
            for (branch, target) in branches {
                if repo.remote_branch_merged_or_gone(&branch, &target, &remote) {
                    self.emit_event(
                        &scope,
                        "pull_request_closed",
                        json!({
                            "branch": branch,
                            "target": target,
                            "remote": remote,
                            "reason": "forge merged or deleted the branch",
                        }),
                    );
                    emitted += 1;
                }
            }
        }
        emitted
    }

    /// Append an `Event` tuple authored by this castle. Best-effort: a store
    /// error is logged, not propagated (event emission is never on a caller's
    /// critical path).
    fn emit_event(&self, scope: &str, identity: &str, payload: Value) {
        let tuple = Tuple::new(
            Category::Event,
            scope.to_string(),
            identity.to_string(),
            self.castle.clone(),
            payload,
        );
        if let Err(e) = self.space.out(tuple) {
            warn!(error = %e, identity, "failed to emit event tuple");
        }
    }

    async fn handle_repo_add(&self, req: Request) -> Response {
        let params: RepoAddParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let path = std::path::PathBuf::from(&params.path);
        if !path.exists() {
            return Response::err(
                req.id,
                codes::BAD_PARAMS,
                format!("path does not exist: {}", params.path),
            );
        }
        let remote = params.remote;
        let remote_name = remote.as_deref().unwrap_or("origin");
        let path_for_remote = path.clone();
        let remote_name = remote_name.to_string();
        let host = match tokio::task::spawn_blocking(move || {
            repo_remote_url(&path_for_remote, &remote_name)
                .and_then(|url| crate::repos::infer_host(&url))
        })
        .await
        {
            Ok(host) => host,
            Err(e) => {
                return Response::err(
                    req.id,
                    codes::INTERNAL,
                    format!("repo inspection task failed: {e}"),
                );
            }
        };
        let record = crate::repos::RepoRecord {
            name: params.name,
            path,
            created_at: chrono::Utc::now(),
            merge_mode: params.merge_mode.unwrap_or(self.default_merge_mode),
            remote,
            host,
        };
        let mut reg = match self.repos.lock() {
            Ok(r) => r,
            Err(_) => return Response::err(req.id, codes::INTERNAL, "repo registry lock poisoned"),
        };
        match reg.add(record.clone()) {
            Ok(()) => Response::ok(req.id, json!({"repo": record})),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    fn handle_repo_get(&self, req: Request) -> Response {
        let params: NameParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let reg = match self.repos.lock() {
            Ok(r) => r,
            Err(_) => return Response::err(req.id, codes::INTERNAL, "repo registry lock poisoned"),
        };
        match reg.get(&params.name) {
            Some(record) => Response::ok(req.id, json!({"repo": record})),
            None => Response::ok(req.id, json!({"repo": null})),
        }
    }

    async fn handle_onboarding_start(&self, req: Request) -> Response {
        let params: RepoOnboardingStartParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        let harness = params
            .harness
            .clone()
            .unwrap_or_else(|| self.default_harness.clone());
        let registered = match self.repos.lock() {
            Ok(registry) => registry.list(),
            Err(_) => return Response::err(req.id, codes::INTERNAL, "repo registry lock poisoned"),
        };
        let target = params.target.clone();
        let context = crate::onboarding::InspectContext {
            default_harness: harness.clone(),
            require_named_checks: self.require_named_checks,
        };
        let assessment = match tokio::task::spawn_blocking(move || {
            crate::onboarding::inspect(&target, &registered, &context)
        })
        .await
        {
            Ok(assessment) => assessment,
            Err(error) => {
                return Response::err(
                    req.id,
                    codes::INTERNAL,
                    format!("repository inspection task failed: {error}"),
                );
            }
        };
        let Some(repo_path) = assessment
            .identity
            .canonical_path
            .as_deref()
            .map(std::path::PathBuf::from)
        else {
            return Response::err(
                req.id,
                codes::BAD_PARAMS,
                "repository identity did not resolve; run `rk repo onboard inspect` for findings",
            );
        };
        let path_for_git = repo_path.clone();
        let (repo_name, base_branch) = match tokio::task::spawn_blocking(move || {
            let repo = rk_git::Repo::discover(&path_for_git)?;
            Ok::<_, rk_core::Error>((repo.name(), repo.current_branch()?))
        })
        .await
        {
            Ok(Ok(identity)) => identity,
            Ok(Err(error)) => return Response::err(req.id, codes::BAD_PARAMS, error.to_string()),
            Err(error) => {
                return Response::err(
                    req.id,
                    codes::INTERNAL,
                    format!("repository identity task failed: {error}"),
                );
            }
        };
        let candidate = crate::onboarding_sessions::OnboardingSession::starting(
            params.target,
            repo_name,
            repo_path,
            base_branch,
            harness,
            params.attach,
            assessment,
            &self.layout.worktrees_dir(),
        );
        let (session, created) = {
            let mut sessions = match self.onboarding_sessions.lock() {
                Ok(sessions) => sessions,
                Err(_) => {
                    return Response::err(
                        req.id,
                        codes::INTERNAL,
                        "onboarding session registry lock poisoned",
                    );
                }
            };
            match sessions.insert_if_absent(candidate) {
                Ok(result) => result,
                Err(error) => return Response::err(req.id, codes::INTERNAL, error.to_string()),
            }
        };
        if !created {
            return match self.reconcile_onboarding_session(&session.id) {
                Ok(session) => Response::ok(req.id, onboarding_payload(&session, true)),
                Err(error) => Response::err(req.id, codes::FORBIDDEN, error.to_string()),
            };
        }

        match self.launch_onboarding_agent(&session, params.attach).await {
            Ok(record) => {
                let updated =
                    match self.update_onboarding_from_agent(&session.id, &record, params.attach) {
                        Ok(updated) => updated,
                        Err(error) => {
                            return Response::err(req.id, codes::INTERNAL, error.to_string());
                        }
                    };
                Response::ok(req.id, onboarding_payload(&updated, false))
            }
            Err(error) => {
                let _ = self.update_onboarding_failure(&session.id, error.to_string());
                Response::err(
                    req.id,
                    codes::INTERNAL,
                    format!(
                        "onboarding session {} was journaled but launch failed: {error}",
                        session.id
                    ),
                )
            }
        }
    }

    async fn handle_onboarding_resume(&self, req: Request) -> Response {
        let params: RepoOnboardingResumeParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        let session = match self.reconcile_onboarding_session(&params.session) {
            Ok(session) => session,
            Err(error) => return Response::err(req.id, codes::FORBIDDEN, error.to_string()),
        };
        if matches!(
            session.state,
            crate::onboarding_sessions::OnboardingSessionState::Running
                | crate::onboarding_sessions::OnboardingSessionState::Starting
                | crate::onboarding_sessions::OnboardingSessionState::Completed
        ) {
            return Response::ok(req.id, onboarding_payload(&session, true));
        }

        let resumed = if let Some(agent) = session.agent.as_deref() {
            if session.worktree.exists() {
                self.supervisor
                    .respawn_onboarding_async(agent.to_string(), params.attach)
                    .await
            } else {
                self.launch_onboarding_agent(&session, params.attach).await
            }
        } else {
            self.launch_onboarding_agent(&session, params.attach).await
        };
        match resumed {
            Ok(record) => {
                match self.update_onboarding_from_agent(&session.id, &record, params.attach) {
                    Ok(updated) => Response::ok(req.id, onboarding_payload(&updated, false)),
                    Err(error) => Response::err(req.id, codes::INTERNAL, error.to_string()),
                }
            }
            Err(error) => {
                let _ = self.update_onboarding_failure(&session.id, error.to_string());
                Response::err(req.id, codes::INTERNAL, error.to_string())
            }
        }
    }

    fn handle_onboarding_propose(&self, req: Request) -> Response {
        let params: RepoOnboardingProposeParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        let session = match self.onboarding_sessions.lock() {
            Ok(sessions) => sessions.get(&params.session),
            Err(_) => {
                return Response::err(
                    req.id,
                    codes::INTERNAL,
                    "onboarding session registry lock poisoned",
                );
            }
        };
        let Some(session) = session else {
            return Response::err(
                req.id,
                codes::BAD_PARAMS,
                format!("no such onboarding session: {}", params.session),
            );
        };
        if req.caller != crate::client::OPERATOR
            && session.agent.as_deref() != Some(req.caller.as_str())
        {
            return Response::err(
                req.id,
                codes::FORBIDDEN,
                format!(
                    "{} does not own onboarding session {}",
                    req.caller, params.session
                ),
            );
        }
        let tree_revision =
            match crate::onboarding_proposals::onboarding_tree_revision(&session.worktree) {
                Ok(revision) => revision,
                Err(error) => {
                    return Response::err(req.id, codes::BAD_PARAMS, error.to_string());
                }
            };
        let result = self
            .onboarding_sessions
            .lock()
            .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
            .and_then(|mut sessions| {
                sessions.propose(
                    &params.session,
                    params.proposal,
                    req.caller.clone(),
                    tree_revision,
                )
            });
        match result {
            Ok((proposal, created)) => {
                Response::ok(req.id, json!({"proposal": proposal, "created": created}))
            }
            Err(error) => Response::err(req.id, codes::BAD_PARAMS, error.to_string()),
        }
    }

    fn handle_onboarding_approve(&self, req: Request) -> Response {
        let params: RepoOnboardingDecisionParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        self.handle_onboarding_decision(
            req,
            params,
            crate::onboarding_proposals::OnboardingDecision::Approve,
        )
    }

    fn handle_onboarding_decline(&self, req: Request) -> Response {
        let params: RepoOnboardingDecisionParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        self.handle_onboarding_decision(
            req,
            params,
            crate::onboarding_proposals::OnboardingDecision::Decline,
        )
    }

    async fn handle_onboarding_apply(&self, req: Request) -> Response {
        let params: RepoOnboardingApplyParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        let _apply_guard = self.onboarding_apply_lock.lock().await;
        let actor = format!("{}@{}", req.caller, self.castle);
        let (session, proposal) = match self.onboarding_sessions.lock() {
            Ok(sessions) => {
                let Some(session) = sessions.get(&params.session) else {
                    return Response::err(
                        req.id,
                        codes::BAD_PARAMS,
                        format!("no such onboarding session: {}", params.session),
                    );
                };
                let Some(proposal) = session
                    .proposals
                    .iter()
                    .find(|proposal| proposal.id == params.proposal)
                    .cloned()
                else {
                    return Response::err(
                        req.id,
                        codes::BAD_PARAMS,
                        format!(
                            "no such onboarding proposal in {}: {}",
                            params.session, params.proposal
                        ),
                    );
                };
                (session, proposal)
            }
            Err(_) => {
                return Response::err(
                    req.id,
                    codes::INTERNAL,
                    "onboarding session registry lock poisoned",
                );
            }
        };
        if proposal.digest != params.digest {
            return Response::err(
                req.id,
                codes::BAD_PARAMS,
                format!("stale proposal digest for {}", proposal.id),
            );
        }
        if !matches!(
            proposal.status,
            crate::onboarding_proposals::OnboardingProposalStatus::Approved
                | crate::onboarding_proposals::OnboardingProposalStatus::Applied
                | crate::onboarding_proposals::OnboardingProposalStatus::Failed
                | crate::onboarding_proposals::OnboardingProposalStatus::Verified
        ) {
            return Response::err(
                req.id,
                codes::BAD_PARAMS,
                format!(
                    "proposal {} must be approved before apply; found {}",
                    proposal.id, proposal.status
                ),
            );
        }

        let apply_session = session.clone();
        let apply_proposal = proposal.clone();
        let apply_actor = actor.clone();
        let application = tokio::task::spawn_blocking(move || {
            crate::onboarding_apply::ensure_application(
                &apply_session,
                &apply_proposal,
                &apply_actor,
            )
        })
        .await;
        let application = match application {
            Ok(Ok(application)) => application,
            Ok(Err(error)) => {
                let detail = error.to_string();
                let _ = self
                    .onboarding_sessions
                    .lock()
                    .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
                    .and_then(|mut sessions| {
                        sessions.record_application_failure(
                            &params.session,
                            &params.proposal,
                            &params.digest,
                            actor,
                            detail.clone(),
                        )
                    });
                return Response::err(req.id, codes::BAD_PARAMS, detail);
            }
            Err(error) => {
                let detail = format!("onboarding application task failed: {error}");
                let _ = self
                    .onboarding_sessions
                    .lock()
                    .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
                    .and_then(|mut sessions| {
                        sessions.record_application_failure(
                            &params.session,
                            &params.proposal,
                            &params.digest,
                            actor,
                            detail.clone(),
                        )
                    });
                return Response::err(req.id, codes::INTERNAL, detail);
            }
        };
        if proposal.status == crate::onboarding_proposals::OnboardingProposalStatus::Verified {
            return Response::ok(
                req.id,
                json!({"proposal": proposal, "applied": false, "verified": false}),
            );
        }
        let (applied_proposal, applied) = match self
            .onboarding_sessions
            .lock()
            .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
            .and_then(|mut sessions| {
                sessions.record_application(
                    &params.session,
                    &params.proposal,
                    &params.digest,
                    application,
                )
            }) {
            Ok(result) => result,
            Err(error) => return Response::err(req.id, codes::INTERNAL, error.to_string()),
        };

        let proposal = if let Some(contract) = applied_proposal.named_check.as_ref() {
            let attempt = applied_proposal.verification_results.len() as u32 + 1;
            let verification =
                match crate::onboarding_apply::verify(&session, &applied_proposal, &actor, attempt)
                    .await
                {
                    Ok(verification) => verification,
                    Err(error) => {
                        let now = chrono::Utc::now();
                        crate::onboarding_proposals::OnboardingVerification {
                            attempt,
                            actor: actor.clone(),
                            started_at: now,
                            finished_at: now,
                            check_name: contract.name.clone(),
                            command: contract.command.clone(),
                            cwd: contract.cwd.clone(),
                            expected_exit: contract.expect_exit,
                            timeout: contract.timeout.clone(),
                            environment_policy: contract.environment_policy,
                            toolchain: contract.toolchain.clone(),
                            exit_status: None,
                            timed_out: false,
                            passed: false,
                            output_summary: format!("verification setup failed: {error}"),
                            unresolved_risks: vec![
                            "verification could not execute; onboarding branch is not ready to land"
                                .into(),
                        ],
                        }
                    }
                };
            match self
                .onboarding_sessions
                .lock()
                .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
                .and_then(|mut sessions| {
                    sessions.record_verification(
                        &params.session,
                        &params.proposal,
                        &params.digest,
                        verification,
                    )
                }) {
                Ok(proposal) => proposal,
                Err(error) => {
                    return Response::err(req.id, codes::INTERNAL, error.to_string());
                }
            }
        } else {
            let attempt = applied_proposal.validation_results.len() as u32 + 1;
            let validation = match crate::onboarding_apply::validate_automation(
                &session,
                &applied_proposal,
                &actor,
                attempt,
            ) {
                Ok(validation) => validation,
                Err(error) => {
                    return Response::err(req.id, codes::INTERNAL, error.to_string());
                }
            };
            match self
                .onboarding_sessions
                .lock()
                .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
                .and_then(|mut sessions| {
                    sessions.record_validation(
                        &params.session,
                        &params.proposal,
                        &params.digest,
                        validation,
                    )
                }) {
                Ok(proposal) => proposal,
                Err(error) => {
                    return Response::err(req.id, codes::INTERNAL, error.to_string());
                }
            }
        };
        let passed =
            proposal.status == crate::onboarding_proposals::OnboardingProposalStatus::Verified;
        Response::ok(
            req.id,
            json!({"proposal": proposal, "applied": applied, "verified": passed}),
        )
    }

    async fn handle_onboarding_activate(&self, req: Request) -> Response {
        let params: RepoOnboardingApplyParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        let _apply_guard = self.onboarding_apply_lock.lock().await;
        let actor = format!("{}@{}", req.caller, self.castle);
        let (session, proposal) = match self.onboarding_sessions.lock() {
            Ok(sessions) => {
                let Some(session) = sessions.get(&params.session) else {
                    return Response::err(
                        req.id,
                        codes::BAD_PARAMS,
                        format!("no such onboarding session: {}", params.session),
                    );
                };
                let Some(proposal) = session
                    .proposals
                    .iter()
                    .find(|proposal| proposal.id == params.proposal)
                    .cloned()
                else {
                    return Response::err(
                        req.id,
                        codes::BAD_PARAMS,
                        format!(
                            "no such onboarding proposal in {}: {}",
                            params.session, params.proposal
                        ),
                    );
                };
                (session, proposal)
            }
            Err(_) => {
                return Response::err(
                    req.id,
                    codes::INTERNAL,
                    "onboarding session registry lock poisoned",
                );
            }
        };
        if proposal.digest != params.digest {
            return Response::err(
                req.id,
                codes::BAD_PARAMS,
                format!("stale proposal digest for {}", proposal.id),
            );
        }
        let contract_session = session.clone();
        let contract_proposal = proposal.clone();
        let contract = match tokio::task::spawn_blocking(move || {
            crate::onboarding_activation::contract(&contract_session, &contract_proposal)
        })
        .await
        {
            Ok(Ok(contract)) => contract,
            Ok(Err(error)) => {
                return Response::err(req.id, codes::BAD_PARAMS, error.to_string());
            }
            Err(error) => {
                return Response::err(
                    req.id,
                    codes::INTERNAL,
                    format!("onboarding activation preflight task failed: {error}"),
                );
            }
        };
        let (intent, intent_created) = match self
            .onboarding_sessions
            .lock()
            .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
            .and_then(|mut sessions| {
                sessions.begin_activation(
                    &params.session,
                    &params.proposal,
                    &params.digest,
                    contract.operation_id.clone(),
                    actor,
                    contract.expected_base_commit.clone(),
                    contract.approved_commit.clone(),
                    contract.approved_tree_revision.clone(),
                    contract.target_digest.clone(),
                )
            }) {
            Ok(result) => result,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error.to_string()),
        };
        if intent.activation.as_ref().is_some_and(|activation| {
            activation.status == crate::onboarding_proposals::OnboardingActivationStatus::Activated
        }) {
            return Response::ok(
                req.id,
                json!({"proposal": intent, "activated": true, "changed": false}),
            );
        }

        let activation_session = session;
        let activation_proposal = intent;
        let activation_contract = contract.clone();
        let evidence = tokio::task::spawn_blocking(move || {
            crate::onboarding_activation::ensure_activation(
                &activation_session,
                &activation_proposal,
                &activation_contract,
            )
        })
        .await;
        let evidence = match evidence {
            Ok(Ok(evidence)) => evidence,
            Ok(Err(error)) => {
                let detail = error.to_string();
                let _ = self
                    .onboarding_sessions
                    .lock()
                    .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
                    .and_then(|mut sessions| {
                        sessions.fail_activation(
                            &params.session,
                            &params.proposal,
                            &params.digest,
                            &contract.operation_id,
                            detail.clone(),
                        )
                    });
                return Response::err(req.id, codes::BAD_PARAMS, detail);
            }
            Err(error) => {
                let detail = format!("onboarding activation task failed: {error}");
                let _ = self
                    .onboarding_sessions
                    .lock()
                    .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
                    .and_then(|mut sessions| {
                        sessions.fail_activation(
                            &params.session,
                            &params.proposal,
                            &params.digest,
                            &contract.operation_id,
                            detail.clone(),
                        )
                    });
                return Response::err(req.id, codes::INTERNAL, detail);
            }
        };
        let (proposal, changed) = match self
            .onboarding_sessions
            .lock()
            .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
            .and_then(|mut sessions| {
                sessions.finish_activation(
                    &params.session,
                    &params.proposal,
                    &params.digest,
                    &contract.operation_id,
                    evidence.registered_commit,
                    evidence.detail,
                )
            }) {
            Ok(result) => result,
            Err(error) => return Response::err(req.id, codes::INTERNAL, error.to_string()),
        };
        Response::ok(
            req.id,
            json!({
                "proposal": proposal,
                "activated": true,
                "changed": changed,
                "intent_created": intent_created,
            }),
        )
    }

    async fn handle_onboarding_decline_activation(&self, req: Request) -> Response {
        let params: RepoOnboardingDecisionParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        let _apply_guard = self.onboarding_apply_lock.lock().await;
        let actor = format!("{}@{}", req.caller, self.castle);
        match self
            .onboarding_sessions
            .lock()
            .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
            .and_then(|mut sessions| {
                sessions.decline_activation(
                    &params.session,
                    &params.proposal,
                    &params.digest,
                    actor,
                    params.reason,
                )
            }) {
            Ok((proposal, changed)) => Response::ok(
                req.id,
                json!({"proposal": proposal, "declined": true, "changed": changed}),
            ),
            Err(error) => Response::err(req.id, codes::BAD_PARAMS, error.to_string()),
        }
    }

    async fn handle_onboarding_cleanup(&self, req: Request) -> Response {
        let params: RepoOnboardingSessionParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        let _apply_guard = self.onboarding_apply_lock.lock().await;
        let session = match self.reconcile_onboarding_session(&params.session) {
            Ok(session) => session,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error.to_string()),
        };
        if session.cleanup.is_some() {
            return Response::ok(
                req.id,
                json!({"session": session.status(), "cleaned": false}),
            );
        }
        let actor = format!("{}@{}", req.caller, self.castle);
        let cleanup_session = session.clone();
        let cleanup_actor = actor.clone();
        let cleanup = match tokio::task::spawn_blocking(move || {
            crate::onboarding_activation::ensure_cleanup(&cleanup_session, &cleanup_actor)
        })
        .await
        {
            Ok(Ok(cleanup)) => cleanup,
            Ok(Err(error)) => {
                return Response::err(req.id, codes::BAD_PARAMS, error.to_string());
            }
            Err(error) => {
                return Response::err(
                    req.id,
                    codes::INTERNAL,
                    format!("onboarding cleanup task failed: {error}"),
                );
            }
        };
        match self
            .onboarding_sessions
            .lock()
            .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
            .and_then(|mut sessions| sessions.record_cleanup(&params.session, cleanup))
        {
            Ok((session, changed)) => Response::ok(
                req.id,
                json!({"session": session.status(), "cleaned": changed}),
            ),
            Err(error) => Response::err(req.id, codes::INTERNAL, error.to_string()),
        }
    }

    fn handle_onboarding_decision(
        &self,
        req: Request,
        params: RepoOnboardingDecisionParams,
        decision: crate::onboarding_proposals::OnboardingDecision,
    ) -> Response {
        let worktree = match self.onboarding_sessions.lock() {
            Ok(sessions) => match sessions.get(&params.session) {
                Some(session) => session.worktree,
                None => {
                    return Response::err(
                        req.id,
                        codes::BAD_PARAMS,
                        format!("no such onboarding session: {}", params.session),
                    );
                }
            },
            Err(_) => {
                return Response::err(
                    req.id,
                    codes::INTERNAL,
                    "onboarding session registry lock poisoned",
                );
            }
        };
        let tree_revision = match crate::onboarding_proposals::onboarding_tree_revision(&worktree) {
            Ok(revision) => revision,
            Err(error) => {
                return Response::err(req.id, codes::BAD_PARAMS, error.to_string());
            }
        };
        // The actor is derived only after RPC authentication. No request field
        // can claim a human identity; the castle-qualified operator channel is
        // the durable attribution available today.
        let actor = format!("{}@{}", req.caller, self.castle);
        let result = self
            .onboarding_sessions
            .lock()
            .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
            .and_then(|mut sessions| {
                sessions.decide(
                    &params.session,
                    &params.proposal,
                    &params.digest,
                    &tree_revision,
                    decision,
                    actor,
                    params.reason,
                )
            });
        match result {
            Ok((proposal, changed)) => {
                Response::ok(req.id, json!({"proposal": proposal, "changed": changed}))
            }
            Err(error) => Response::err(req.id, codes::BAD_PARAMS, error.to_string()),
        }
    }

    fn handle_onboarding_status(&self, req: Request) -> Response {
        let params: RepoOnboardingSessionParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        match self.reconcile_onboarding_session(&params.session) {
            Ok(session) => Response::ok(req.id, json!({"session": session.status()})),
            Err(error) => Response::err(req.id, codes::BAD_PARAMS, error.to_string()),
        }
    }

    fn handle_onboarding_report(&self, req: Request) -> Response {
        let params: RepoOnboardingSessionParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        match self.reconcile_onboarding_session(&params.session) {
            Ok(session) => Response::ok(req.id, json!({"report": session.report()})),
            Err(error) => Response::err(req.id, codes::BAD_PARAMS, error.to_string()),
        }
    }

    async fn launch_onboarding_agent(
        &self,
        session: &crate::onboarding_sessions::OnboardingSession,
        attach: bool,
    ) -> rk_core::Result<crate::agents::AgentRecord> {
        let assessment = serde_json::to_string_pretty(&session.assessment)?;
        self.supervisor
            .spawn_async(crate::supervisor::SpawnParams {
                repo: session.repo_path.to_string_lossy().into_owned(),
                task: session.id.clone(),
                prompt: Some(format!(
                    "Assess this repository read-only for onboarding session {}. \
                     The daemon's deterministic starting assessment follows. Confirm \
                     evidence and report ambiguity. Journal concrete advice with \
                     `rk repo onboard propose`; that records a proposal but grants no \
                     approval or mutation authority. Finish with `rk done`; do not edit \
                     or commit anything.\n\n{}",
                    session.id, assessment
                )),
                role: crate::onboarding_sessions::ONBOARDER_ROLE.into(),
                coordination: None,
                harness: Some(session.harness.clone()),
                parent: None,
                base: Some(session.base_branch.clone()),
                model: None,
                permission_mode: None,
                attach,
                workflow_instance: None,
                coordinator: None,
                instance_max_usd: None,
            })
            .await
    }

    fn reconcile_onboarding_session(
        &self,
        id: &str,
    ) -> rk_core::Result<crate::onboarding_sessions::OnboardingSession> {
        let session = self
            .onboarding_sessions
            .lock()
            .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))?
            .get(id)
            .ok_or_else(|| rk_core::Error::other(format!("no such onboarding session: {id}")))?;
        let recovered = session
            .agent
            .as_deref()
            .and_then(|name| self.supervisor.status(name))
            .or_else(|| self.supervisor.onboarding_agent(id));
        let Some(recovered) = recovered else {
            return Ok(session);
        };
        let agent_name = recovered.name.clone();
        let agent = self
            .supervisor
            .reconcile_attached(&agent_name)?
            .or_else(|| self.supervisor.status(&agent_name))
            .ok_or_else(|| {
                rk_core::Error::other(format!(
                    "onboarding session {id} references missing agent {agent_name}"
                ))
            })?;
        if agent.role != crate::onboarding_sessions::ONBOARDER_ROLE {
            return Err(rk_core::Error::other(format!(
                "onboarding session {id} agent {} has downgraded role {:?}",
                agent.name, agent.role
            )));
        }
        self.update_onboarding_from_agent(id, &agent, session.attached)
    }

    fn update_onboarding_from_agent(
        &self,
        id: &str,
        agent: &crate::agents::AgentRecord,
        attached: bool,
    ) -> rk_core::Result<crate::onboarding_sessions::OnboardingSession> {
        let state = match agent.state {
            crate::agents::AgentState::Spawning => {
                crate::onboarding_sessions::OnboardingSessionState::Starting
            }
            crate::agents::AgentState::Running => {
                crate::onboarding_sessions::OnboardingSessionState::Running
            }
            crate::agents::AgentState::Completed => {
                crate::onboarding_sessions::OnboardingSessionState::Completed
            }
            crate::agents::AgentState::Failed | crate::agents::AgentState::Dismissed => {
                crate::onboarding_sessions::OnboardingSessionState::Failed
            }
            crate::agents::AgentState::Orphaned => {
                crate::onboarding_sessions::OnboardingSessionState::Orphaned
            }
        };
        self.onboarding_sessions
            .lock()
            .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))?
            .update(id, |session| {
                session.agent = Some(agent.name.clone());
                session.branch = agent
                    .branch
                    .clone()
                    .unwrap_or_else(|| session.branch.clone());
                session.worktree = agent
                    .worktree
                    .clone()
                    .unwrap_or_else(|| session.worktree.clone());
                session.attached = attached;
                session.attach_target = agent.attach_target.clone();
                session.agent_result = agent.result.clone();
                session.state = state;
            })?
            .ok_or_else(|| rk_core::Error::other(format!("no such onboarding session: {id}")))
    }

    fn update_onboarding_failure(&self, id: &str, detail: String) -> rk_core::Result<()> {
        self.onboarding_sessions
            .lock()
            .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))?
            .update(id, |session| {
                session.state = crate::onboarding_sessions::OnboardingSessionState::Failed;
                session.agent_result = Some(detail);
            })?;
        Ok(())
    }

    async fn handle_ticket_new(&self, req: Request) -> Response {
        let mut params: crate::tickets::NewTicket = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        // Filing follow-up work is agent-safe, but the author identity is not a
        // caller-controlled field. Otherwise an agent could create a ticket
        // that presents itself as the operator or another castle.
        if req.caller != "operator" && !req.caller.is_empty() {
            params.created_by = Some(req.caller.clone());
        }
        match self.tickets.create(params).await {
            Ok(tuple) => Response::ok(req.id, json!({"ticket": tuple})),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    fn handle_ticket_list(&self, req: Request) -> Response {
        let params: TicketListParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        match self
            .tickets
            .list(params.scope, params.status, params.parent)
        {
            Ok(tickets) => {
                let blocked = self.tickets.blocked_ids(&tickets).unwrap_or_default();
                Response::ok(req.id, json!({"tickets": tickets, "blocked": blocked}))
            }
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    fn handle_ticket_get(&self, req: Request) -> Response {
        let params: TicketGetParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        match self.tickets.get(&params.id) {
            Ok(ticket) => {
                let blockers = self.tickets.blockers(&params.id).ok().flatten();
                Response::ok(req.id, json!({"ticket": ticket, "blockers": blockers}))
            }
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    async fn handle_ticket_update(&self, req: Request) -> Response {
        let params: TicketUpdateParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        match self.tickets.update(&params.id, params.changes).await {
            Ok(ticket) => Response::ok(req.id, json!({"ticket": ticket})),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    async fn handle_ticket_dep(&self, req: Request) -> Response {
        let params: TicketDepParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let result = if params.remove {
            self.tickets.remove_dep(&params.id, &params.dep).await
        } else {
            self.tickets.add_dep(&params.id, &params.dep).await
        };
        match result {
            Ok(ticket) => Response::ok(req.id, json!({"ticket": ticket})),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    fn handle_ticket_ready(&self, req: Request) -> Response {
        let params: TicketReadyParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        match self.tickets.ready(params.scope) {
            Ok(tickets) => Response::ok(req.id, json!({"tickets": tickets})),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    async fn handle_spawn(&self, req: Request) -> Response {
        let params: crate::supervisor::SpawnParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let mut params = if req.caller == "operator" || req.caller.is_empty() {
            params
        } else {
            match self.supervisor.prepare_foreman_spawn(&req.caller, params) {
                Ok(p) => p,
                Err(e) => return Response::err(req.id, codes::FORBIDDEN, e.to_string()),
            }
        };
        // Workflow-spawned foremen inherit the instance cap for children they
        // dispatch. The workflow engine remains the source of that definition;
        // the child spawn must not silently become an uncapped side door.
        if params.instance_max_usd.is_none() {
            if let Some(instance) = params.workflow_instance.as_deref() {
                params.instance_max_usd = self.engine().instance_budget(instance);
            }
        }
        match self.supervisor.spawn_async(params).await {
            Ok(record) => Response::ok(req.id, json!({"agent": record})),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    fn handle_progress(&self, req: Request) -> Response {
        if req.caller.is_empty() || req.caller == "operator" {
            return Response::err(
                req.id,
                codes::FORBIDDEN,
                "progress must be reported by an authenticated agent",
            );
        }
        let params: ProgressParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        match self.supervisor.record_progress(
            &req.caller,
            params.summary,
            params.next,
            params.status,
        ) {
            Ok(agent) => Response::ok(
                req.id,
                json!({"agent": agent.name, "progress": agent.progress}),
            ),
            Err(e) => Response::err(req.id, codes::BAD_PARAMS, e.to_string()),
        }
    }

    async fn handle_respawn(&self, req: Request) -> Response {
        let params: NameParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        if let Err(e) = self.authorize_foreman_child(&req.caller, &params.name) {
            return Response::err(req.id, codes::FORBIDDEN, e.to_string());
        }
        let supervisor = Arc::clone(&self.supervisor);
        let name = params.name;
        let handle = tokio::runtime::Handle::current();
        let result = tokio::task::spawn_blocking(move || {
            let _entered = handle.enter();
            supervisor
                .respawn(&name)
                .map(|record| json!({"agent": record}))
        })
        .await;
        match result {
            Ok(Ok(value)) => Response::ok(req.id, value),
            Ok(Err(e)) => Response::err(req.id, codes::INTERNAL, e.to_string()),
            Err(e) => Response::err(req.id, codes::INTERNAL, format!("respawn task failed: {e}")),
        }
    }

    async fn handle_workflow_run(&self, req: Request) -> Response {
        let params: WorkflowRunParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let engine = self.engine();
        let handle = tokio::runtime::Handle::current();
        let result = tokio::task::spawn_blocking(move || {
            let _entered = handle.enter();
            engine.run_owned(
                &params.name,
                &params.repo,
                params.params,
                params.coordinator,
            )
        })
        .await;
        match result {
            Ok(Ok(instance)) => Response::ok(req.id, json!({"instance": instance})),
            Ok(Err(e)) => Response::err(req.id, codes::INTERNAL, e.to_string()),
            Err(e) => Response::err(
                req.id,
                codes::INTERNAL,
                format!("workflow task failed: {e}"),
            ),
        }
    }

    /// `agent.archive` — offload settled terminal records out of the default
    /// views. The daemon owns `agents.json` and rewrites it on every mutation,
    /// so this has to be an RPC: an external edit would be clobbered by the
    /// next `Registry::persist`.
    async fn handle_agent_archive(&self, req: Request) -> Response {
        let params: AgentArchiveParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let now = chrono::Utc::now();
        // `all` means "everything eligible right now" — a cutoff of now, since
        // eligibility is `updated_at < cutoff`.
        let cutoff = if params.all {
            now
        } else {
            match crate::agents::cutoff_from_spec(params.before.as_deref().unwrap_or("7d"), now) {
                Ok(c) => c,
                Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e.to_string()),
            }
        };
        let reap = crate::supervisor::Reap {
            git: params.reap_git,
            logs: params.reap_logs,
        };
        let supervisor = Arc::clone(&self.supervisor);
        let engine = self.engine();
        let handle = tokio::runtime::Handle::current();
        let result = tokio::task::spawn_blocking(move || {
            let _entered = handle.enter();
            let mut value = supervisor.archive_agents(cutoff, params.dry_run, reap)?;
            // The same sweep clears the workflow side of the board (TKT-177).
            // An operator's "clear what's settled" is one gesture, and before
            // this a failed instance had no `rk` path off `rk inbox` at all.
            let selection = crate::workflow_exec::Selection::Before(cutoff);
            let instances = if params.dry_run {
                engine.archivable(&selection)?
            } else {
                engine.archive(&selection)?
            };
            value["instances"] = json!(instances);
            Ok::<_, rk_core::Error>(value)
        })
        .await;
        match result {
            Ok(Ok(value)) => Response::ok(req.id, value),
            Ok(Err(e)) => Response::err(req.id, codes::INTERNAL, e.to_string()),
            Err(e) => Response::err(req.id, codes::INTERNAL, format!("archive task failed: {e}")),
        }
    }

    /// `workflow.archive` — the targeted counterpart to the `agent.archive`
    /// sweep: prune named terminal instances (the `rk inbox` row action) or
    /// every one settled before a cutoff. The daemon owns the instance store
    /// and rewrites it on every mutation, so this has to be an RPC — moving the
    /// JSON aside by hand only works with the daemon stopped.
    async fn handle_workflow_archive(&self, req: Request) -> Response {
        let params: WorkflowArchiveParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let selection = if params.ids.is_empty() {
            let now = chrono::Utc::now();
            // `all` means "everything settled right now" — a cutoff of now,
            // since eligibility is `settled_at < cutoff`.
            let cutoff = if params.all {
                now
            } else {
                match crate::agents::cutoff_from_spec(params.before.as_deref().unwrap_or("7d"), now)
                {
                    Ok(c) => c,
                    Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e.to_string()),
                }
            };
            crate::workflow_exec::Selection::Before(cutoff)
        } else {
            crate::workflow_exec::Selection::Ids(params.ids)
        };
        let engine = self.engine();
        let result = tokio::task::spawn_blocking(move || {
            if params.dry_run {
                engine.archivable(&selection)
            } else {
                engine.archive(&selection)
            }
        })
        .await;
        match result {
            Ok(Ok(instances)) => Response::ok(
                req.id,
                json!({
                    "dry_run": params.dry_run,
                    "count": instances.len(),
                    "instances": instances,
                }),
            ),
            Ok(Err(e)) => Response::err(req.id, codes::INTERNAL, e.to_string()),
            Err(e) => Response::err(
                req.id,
                codes::INTERNAL,
                format!("workflow archive task failed: {e}"),
            ),
        }
    }

    fn authorize_foreman_child(&self, caller: &str, child: &str) -> rk_core::Result<()> {
        if caller == "operator" || caller.is_empty() {
            return Ok(());
        }
        self.supervisor.authorize_child(caller, child)
    }

    fn handle_named<F>(&self, req: Request, f: F) -> Response
    where
        F: FnOnce(&Arc<crate::supervisor::Supervisor>, String) -> rk_core::Result<Value>,
    {
        let params: NameParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        match f(&self.supervisor, params.name) {
            Ok(v) => Response::ok(req.id, v),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    async fn handle_steer(&self, req: Request) -> Response {
        let params: SteerParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        if let Err(e) = self.authorize_foreman_child(&req.caller, &params.name) {
            return Response::err(req.id, codes::FORBIDDEN, e.to_string());
        }
        match self.supervisor.steer(&params.name, &params.message).await {
            Ok(()) => Response::ok(req.id, json!({"steered": true})),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    fn handle_out(&self, req: Request) -> Response {
        let params: OutParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let caller = req.caller.clone();
        let is_agent = caller != "operator" && !caller.is_empty();
        let attention = if is_agent
            && matches!(params.category, Category::Obstacle | Category::Need)
            && self.supervisor.is_reporting_boundary(&caller)
        {
            Some((
                params.category.as_str().to_string(),
                params.scope.clone(),
                params.identity.clone(),
                params.payload.clone(),
            ))
        } else {
            None
        };
        if is_agent {
            if params.lifecycle == Some(Lifecycle::Furniture)
                || matches!(
                    params.category,
                    Category::Convention
                        | Category::Task
                        | Category::Available
                        // `Withdrawal` is on this list for a different reason
                        // than the rest: it is not that agents have no business
                        // closing a ballot — the proposer is exactly who should
                        // — but that a raw `out` carries no proof of WHOSE
                        // ballot it is. `handle_out` only checks that a tuple's
                        // `instance` is the caller, which a withdrawal keyed
                        // `identity = <sug-id>` satisfies trivially, so leaving
                        // it writable here would let any rat close any peer's
                        // proposal. `space.withdraw` is the only route, and it
                        // checks authorship against the Suggestion (TKT-184).
                        | Category::Withdrawal
                        | Category::FactVote
                )
            {
                return Response::err(
                    req.id,
                    codes::FORBIDDEN,
                    "agents cannot write furniture, convention, task, or available tuples \
                     (withdraw a ballot with `rk withdraw`, which checks authorship)",
                );
            }
            if params
                .instance
                .as_deref()
                .is_some_and(|instance| instance != caller)
            {
                return Response::err(
                    req.id,
                    codes::FORBIDDEN,
                    "agents may only write tuples for their own instance",
                );
            }
            if params.category == Category::Event {
                if params.identity != "task_done" {
                    return Response::err(
                        req.id,
                        codes::FORBIDDEN,
                        "agents may only write the task_done event",
                    );
                }
                if params.payload.get("agent").and_then(Value::as_str) != Some(caller.as_str()) {
                    return Response::err(
                        req.id,
                        codes::FORBIDDEN,
                        "task_done must identify the authenticated agent",
                    );
                }
            }
        }
        let mut tuple = Tuple::new(
            params.category,
            params.scope,
            params.identity,
            params.instance.unwrap_or_else(|| {
                if is_agent {
                    caller.clone()
                } else {
                    self.castle.clone()
                }
            }),
            params.payload,
        );
        let explicit_lifecycle = params.lifecycle.is_some() || params.ttl_secs.is_some();
        if let Some(lifecycle) = params.lifecycle {
            tuple = tuple.with_lifecycle(lifecycle);
        }
        if let Some(ttl_secs) = params.ttl_secs {
            if ttl_secs > MAX_TRAIL_TTL.as_secs() {
                return Response::err(
                    req.id,
                    codes::BAD_PARAMS,
                    format!(
                        "ttl_secs exceeds the maximum supported TTL of {} seconds",
                        MAX_TRAIL_TTL.as_secs()
                    ),
                );
            }
            tuple.lifecycle = Lifecycle::Ephemeral;
            tuple.expires_at = chrono::Utc::now().checked_add_signed(
                chrono::Duration::from_std(Duration::from_secs(ttl_secs))
                    .expect("MAX_TRAIL_TTL must fit chrono::Duration"),
            );
        }
        // Pheromone trails carry a decaying strength and default to an Ephemeral
        // lifetime so an abandoned one evaporates instead of lingering forever.
        if tuple.category.evaporates() {
            tuple.strength = Some(rk_core::tuple::FULL_STRENGTH);
            if !explicit_lifecycle {
                tuple.lifecycle = Lifecycle::Ephemeral;
                tuple.expires_at = chrono::Utc::now().checked_add_signed(
                    chrono::Duration::from_std(DEFAULT_TRAIL_TTL)
                        .expect("DEFAULT_TRAIL_TTL must fit chrono::Duration"),
                );
            }
        }
        // Evaporating writes reinforce an existing trail in place (refresh, no
        // duplicate); everything else is a plain append.
        let written = if tuple.category.evaporates() {
            self.space.reinforce(tuple)
        } else {
            self.space.out(tuple.clone()).map(|()| tuple)
        };
        match written {
            Ok(t) => {
                if let Some((category, scope, identity, payload)) = attention {
                    self.supervisor.publish_coordination_attention(
                        &caller, &category, &scope, &identity, &payload,
                    );
                }
                Response::ok(req.id, json!({"id": t.id, "written": true}))
            }
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    /// `space.withdraw` — close a losing ballot explicitly (TKT-184).
    ///
    /// This is its own RPC rather than a `space.out` of a `Withdrawal` because
    /// the act needs an authorisation that `handle_out` structurally cannot
    /// perform. `handle_out` authenticates a WRITER (a tuple's `instance` must
    /// be the caller); withdrawal has to authorise against a SUBJECT — the
    /// proposer recorded on a different tuple — and only a handler that reads
    /// the `Suggestion` first can do that. See [`may_withdraw`].
    ///
    /// Ordered so the cheap terminal answers come before the authorisation
    /// check, because they are answers the caller wants regardless of who they
    /// are: a promoted ballot cannot be withdrawn by anyone (its Convention is
    /// permanent and unretractable, so "withdrawn" would be a lie the space
    /// cannot honour), and an already-withdrawn one is a no-op that must report
    /// success — the resolving command on an inbox row has to be safe to run
    /// twice, and the operator re-running it after a `rk sync` pulled a peer's
    /// withdrawal should not read as a failure.
    fn handle_withdraw(&self, req: Request) -> Response {
        let params: WithdrawParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let sug_id = params.suggestion.trim().to_string();
        if sug_id.is_empty() {
            return Response::err(req.id, codes::BAD_PARAMS, "suggestion id must not be empty");
        }
        let ballot = |category| {
            self.space.scan(
                &Pattern::category(category)
                    .scope(SYSTEM_SCOPE)
                    .identity(sug_id.as_str()),
            )
        };

        let suggestions = match ballot(Category::Suggestion) {
            Ok(t) => t,
            Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
        };
        // The proposer is read off the Suggestion, never taken from the caller:
        // that tuple is the only durable record of whose ballot this is.
        let Some(suggestion) = suggestions.first() else {
            return Response::err(
                req.id,
                codes::BAD_PARAMS,
                format!("no open suggestion {sug_id} on the system scope"),
            );
        };

        match ballot(Category::Convention) {
            Ok(c) if !c.is_empty() => {
                return Response::err(
                    req.id,
                    codes::FORBIDDEN,
                    format!(
                        "{sug_id} already promoted to a convention — a promoted norm is \
                         permanent and cannot be withdrawn"
                    ),
                );
            }
            Ok(_) => {}
            Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
        }

        match ballot(Category::Withdrawal) {
            Ok(w) if !w.is_empty() => {
                return Response::ok(
                    req.id,
                    json!({"withdrawn": sug_id, "already": true, "written": false}),
                );
            }
            Ok(_) => {}
            Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
        }

        let caller = req.caller.clone();
        if !may_withdraw(&caller, &suggestion.instance) {
            return Response::err(
                req.id,
                codes::FORBIDDEN,
                format!(
                    "only {proposer} (who proposed {sug_id}) or the operator may withdraw it",
                    proposer = suggestion.instance
                ),
            );
        }
        // An operator's caller id is `operator` or empty; record the former
        // either way so the ledger names an actor rather than a blank, and so
        // the operator reads as ONE withdrawer however many shells they use —
        // the same reason `rk endorse` votes as `operator`.
        let by = if caller.is_empty() {
            OPERATOR_ACTOR.to_string()
        } else {
            caller
        };
        let withdrawal = Tuple::new(
            Category::Withdrawal,
            SYSTEM_SCOPE,
            sug_id.as_str(),
            by.as_str(),
            json!({
                "suggestion": sug_id,
                "withdrawn_by": by,
                "proposer": suggestion.instance,
                "text": suggestion.payload.get("text").cloned().unwrap_or(Value::Null),
            }),
        )
        // Furniture for the same two reasons the Convention is: a closed ballot
        // must stay closed (an evaporating withdrawal would silently reopen the
        // row it retired, and re-arm the promotion it suppressed), and `in` must
        // not be able to consume it — otherwise `rk in withdrawal system <id>`
        // is an unauthorised reopen with no authorship check anywhere.
        // Furniture also replicates, so a ballot withdrawn in one castle does
        // not keep nagging — or promote — in another.
        .with_lifecycle(Lifecycle::Furniture);
        match self.space.out(withdrawal) {
            Ok(()) => Response::ok(
                req.id,
                json!({"withdrawn": sug_id, "already": false, "written": true, "by": by}),
            ),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    /// Cast, change, or retract the authenticated caller's vote on one Fact.
    /// Votes are append-only ledger tuples: the latest entry for
    /// `(fact, voter)` is effective, while the history preserves provenance.
    fn handle_fact_vote(&self, req: Request) -> Response {
        let params: FactVoteParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let fact_id = match params.fact.trim().parse::<RecordId>() {
            Ok(id) => id,
            Err(e) => {
                return Response::err(
                    req.id,
                    codes::BAD_PARAMS,
                    format!("fact must be a tuple id: {e}"),
                );
            }
        };
        let fact = match self.space.get(fact_id) {
            Ok(Some(tuple)) if tuple.category == Category::Fact => tuple,
            Ok(Some(_)) => {
                return Response::err(req.id, codes::BAD_PARAMS, "target tuple is not a fact");
            }
            Ok(None) => return Response::err(req.id, codes::BAD_PARAMS, "fact tuple not found"),
            Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
        };
        let vote = params.vote.trim().to_ascii_lowercase();
        if !matches!(vote.as_str(), "up" | "down" | "clear") {
            return Response::err(req.id, codes::BAD_PARAMS, "vote must be up, down, or clear");
        }
        let voter = if req.caller.is_empty() {
            OPERATOR_ACTOR.to_string()
        } else {
            req.caller.clone()
        };
        let _guard = match self.fact_vote_lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let fact_key = fact_id.to_string();
        let mut vote_pattern = Pattern::category(Category::FactVote)
            .scope(fact.scope.clone())
            .identity(fact_key.clone());
        vote_pattern.instance = Some(voter.clone());
        let existing = match self.space.scan(&vote_pattern) {
            Ok(tuples) => tuples,
            Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
        };
        let current = existing
            .iter()
            .max_by(|left, right| {
                left.created_at
                    .cmp(&right.created_at)
                    .then_with(|| left.id.cmp(&right.id))
            })
            .and_then(|tuple| tuple.payload.get("vote"))
            .and_then(Value::as_str);
        let already = match vote.as_str() {
            "clear" => current.is_none() || current == Some("clear"),
            _ => current == Some(vote.as_str()),
        };
        if already {
            return Response::ok(
                req.id,
                json!({
                    "fact": fact_id,
                    "vote": vote,
                    "voter": voter,
                    "already": true,
                    "written": false,
                }),
            );
        }
        let tuple = Tuple::new(
            Category::FactVote,
            fact.scope,
            fact_key.clone(),
            voter.clone(),
            json!({"fact": fact_key, "vote": vote, "voter": voter}),
        );
        match self.space.out(tuple) {
            Ok(()) => Response::ok(
                req.id,
                json!({
                    "fact": fact_id,
                    "vote": vote,
                    "voter": voter,
                    "already": false,
                    "written": true,
                }),
            ),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    fn handle_scan(&self, req: Request) -> Response {
        let params: ScanParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        // `--hot`, or any `--top N` cap, follows the strongest trail first;
        // otherwise the default oldest-first scan is unchanged.
        let requested_top = params.top;
        let limit = requested_top
            .unwrap_or(MAX_SCAN_TUPLES)
            .min(MAX_SCAN_TUPLES);
        let result = if params.hot || params.top.is_some() {
            self.space
                .scan_hot(&params.pattern, Some(limit.saturating_add(1)))
        } else {
            self.space
                .scan_limited(&params.pattern, limit.saturating_add(1))
        };
        match result {
            Ok(mut tuples) => {
                let truncated = tuples.len() > limit
                    || requested_top.is_some_and(|requested| requested > MAX_SCAN_TUPLES);
                tuples.truncate(limit);
                let fact_votes = if params.pattern.category == Some(Category::Fact) {
                    match self.fact_vote_counts(&tuples) {
                        Ok(counts) => Some(counts),
                        Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
                    }
                } else {
                    None
                };
                let mut result = json!({"tuples": tuples, "truncated": truncated});
                if let Some(counts) = fact_votes {
                    result["fact_votes"] = counts;
                }
                Response::ok(req.id, result)
            }
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    async fn handle_blocking(&self, req: Request, destructive: bool) -> Response {
        let params: BlockingParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let timeout = params
            .timeout_ms
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_BLOCK)
            .min(MAX_BLOCK);
        let result = if destructive {
            self.space.take(&params.pattern.pattern, timeout).await
        } else {
            self.space.rd(&params.pattern.pattern, timeout).await
        };
        match result {
            Ok(Some(tuple)) => {
                let fact_votes = if tuple.category == Category::Fact {
                    match self.fact_vote_counts(std::slice::from_ref(&tuple)) {
                        Ok(counts) => Some(counts),
                        Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
                    }
                } else {
                    None
                };
                let mut result = json!({"tuple": tuple});
                if let Some(counts) = fact_votes {
                    result["fact_votes"] = counts;
                }
                Response::ok(req.id, result)
            }
            Ok(None) => Response::ok(req.id, json!({"tuple": null, "timed_out": true})),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    /// Reduce the append-only vote ledger to the latest effective vote from
    /// each voter, then return counts keyed by the fact id being read.
    fn fact_vote_counts(&self, facts: &[Tuple]) -> rk_core::Result<Value> {
        let targets: HashSet<(String, String)> = facts
            .iter()
            .map(|fact| (fact.scope.clone(), fact.id.to_string()))
            .collect();
        let votes = self.space.scan(&Pattern::category(Category::FactVote))?;
        let mut latest: HashMap<FactVoteKey, FactVoteState> = HashMap::new();
        for vote in votes {
            let key = (vote.scope, vote.identity, vote.instance);
            let state = vote
                .payload
                .get("vote")
                .and_then(Value::as_str)
                .unwrap_or("clear")
                .to_string();
            if latest.get(&key).is_none_or(|(current_at, current_id, _)| {
                vote.created_at > *current_at
                    || (vote.created_at == *current_at && vote.id > *current_id)
            }) {
                latest.insert(key, (vote.created_at, vote.id, state));
            }
        }
        let mut counts: HashMap<(String, String), (u64, u64)> = HashMap::new();
        for ((scope, fact_id, _voter), (_created_at, _id, state)) in latest {
            if !targets.contains(&(scope.clone(), fact_id.clone())) {
                continue;
            }
            let entry = counts.entry((scope, fact_id)).or_default();
            match state.as_str() {
                "up" => entry.0 += 1,
                "down" => entry.1 += 1,
                _ => {}
            }
        }
        let mut result = serde_json::Map::new();
        for fact in facts {
            let key = (fact.scope.clone(), fact.id.to_string());
            let (up, down) = counts.get(&key).copied().unwrap_or_default();
            result.insert(
                fact.id.to_string(),
                json!({"up": up, "down": down, "score": up as i64 - down as i64}),
            );
        }
        Ok(Value::Object(result))
    }

    fn status(&self) -> Value {
        json!({
            "version": env!("CARGO_PKG_VERSION"),
            "pid": std::process::id(),
            // Operator-facing: the friendly alias if configured, else the actor
            // id. The wire id (self.castle) is never exposed here as a name.
            "castle": self.castle_display,
            "uptime_secs": self.started.elapsed().as_secs(),
            "socket": self.layout.socket_path(),
            "tuples": self.space.count().unwrap_or(0),
        })
    }
}

fn cleared_branches_for_paths(
    events: Vec<(String, String, String)>,
    paths: HashMap<String, std::path::PathBuf>,
) -> HashSet<(String, String)> {
    let mut cleared = HashSet::new();
    // Ask git once per distinct (scope, branch): the same branch commonly
    // carries several events (a retried land, a re-push), and the answer cannot
    // differ between them.
    let mut asked: HashSet<(String, String)> = HashSet::new();
    for (scope, branch, target) in events {
        let key = (scope.clone(), branch.clone());
        if !asked.insert(key.clone()) {
            continue;
        }
        let Some(path) = paths.get(&scope) else {
            continue;
        };
        let Ok(repo) = rk_git::Repo::discover(path) else {
            continue;
        };
        if repo.branch_merged_or_gone(&branch, &target) {
            cleared.insert(key);
        }
    }
    cleared
}

fn max_cursor(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, None) => left,
        (None, right) => right,
    }
}

fn filter_is_valid(filter: &CoordinatorFilter) -> bool {
    filter.instance.is_some()
        || filter.coordinator.is_some()
        || filter.subtree.is_some()
        || filter.repo.is_some()
}

enum Outcome {
    Reply(Response),
    Watch {
        response: Response,
        pattern: Pattern,
    },
    CoordinatorWatch {
        response: Response,
        filter: CoordinatorFilter,
        boundary: Option<u64>,
        rx: broadcast::Receiver<CoordinatorEvent>,
    },
    /// Reply with the backlog, then stream that agent's new log entries live.
    LogFollow {
        response: Response,
        agent: String,
        /// The generation being followed. Only the newest generation of a name
        /// can still be writing, so following an older one correctly streams
        /// nothing rather than leaking a namesake's live output.
        generation: Option<DateTime<Utc>>,
    },
}

#[derive(Deserialize)]
struct LogParams {
    name: String,
    /// Only the last N entries of the backlog (all if unset).
    #[serde(default)]
    tail: Option<usize>,
    /// Keep the connection open and push new entries as they land.
    #[serde(default)]
    follow: bool,
    /// Which generation of this name to read, 1-based oldest-first. Unset = the
    /// newest, i.e. the rat an operator means when they type a bare name.
    #[serde(default)]
    generation: Option<usize>,
}

#[derive(Deserialize)]
struct OutParams {
    category: Category,
    scope: String,
    identity: String,
    #[serde(default)]
    instance: Option<String>,
    #[serde(default)]
    payload: Value,
    #[serde(default)]
    lifecycle: Option<Lifecycle>,
    #[serde(default)]
    ttl_secs: Option<u64>,
}

/// `space.withdraw` params: the ballot to close. Everything else the record
/// needs — proposer, text, withdrawer — is read from the space and the
/// authenticated caller, never accepted from the wire, so a caller cannot
/// misattribute the close.
#[derive(Deserialize)]
struct WithdrawParams {
    suggestion: String,
}

#[derive(Deserialize)]
struct FactVoteParams {
    fact: String,
    vote: String,
}

#[derive(Deserialize, Default)]
struct PatternParams {
    #[serde(flatten)]
    pattern: Pattern,
}

/// `space.scan` params: a match pattern plus the optional hot-ranking sugar.
/// `hot` reorders by `category_weight × recency × strength` (strongest first);
/// `top` caps to the N strongest and implies `hot`.
#[derive(Deserialize, Default)]
struct ScanParams {
    #[serde(flatten)]
    pattern: Pattern,
    #[serde(default)]
    hot: bool,
    #[serde(default)]
    top: Option<usize>,
}

#[derive(Deserialize)]
struct WorkflowRunParams {
    name: String,
    repo: String,
    #[serde(default)]
    params: std::collections::HashMap<String, Value>,
    /// Stable coordinator-session identity for owned-workflow monitoring.
    #[serde(default)]
    coordinator: Option<String>,
}

#[derive(Deserialize)]
struct CoordinatorRegisterParams {
    coordinator: String,
    #[serde(default)]
    after: Option<u64>,
}

#[derive(Deserialize)]
struct CoordinatorAckParams {
    coordinator: String,
    cursor: u64,
}

#[derive(Deserialize)]
struct ProgressParams {
    summary: String,
    #[serde(default)]
    next: Option<String>,
    #[serde(default = "default_progress_status")]
    status: String,
}

fn default_progress_status() -> String {
    "working".into()
}

#[derive(Deserialize)]
struct WorkflowDefsParams {
    repo: String,
}

#[derive(Deserialize)]
struct WorkflowApproveParams {
    instance: String,
    approved: bool,
    by: String,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Deserialize)]
struct NameParams {
    name: String,
}

/// `agent.list` view selector. Defaults keep the reply to the live registry so
/// every caller (`rk list`, `rk top`, `rk cost`) gets the current fleet unless
/// it opts into history.
#[derive(Deserialize, Default)]
#[serde(default)]
struct AgentListParams {
    /// Live + archived records.
    include_archived: bool,
    /// Archived records only (wins over `include_archived`).
    archived_only: bool,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct AgentArchiveParams {
    /// Cutoff: a duration (`7d`, `24h`) or a date. Defaults to `7d`.
    before: Option<String>,
    /// Archive every eligible record regardless of age (overrides `before`).
    all: bool,
    /// Report what would be archived without mutating the registry.
    dry_run: bool,
    /// Also reclaim each archived agent's worktree + local branch, but only
    /// when the branch has already landed.
    reap_git: bool,
    /// Also delete each archived agent's transcript file. One-way.
    reap_logs: bool,
}

/// Which slice of the instance store `workflow.list` returns. Defaults (both
/// false) to the live store, so every existing caller is unchanged.
#[derive(Deserialize, Default)]
#[serde(default)]
struct WorkflowListParams {
    /// Pruned instances only.
    archived: bool,
    /// Live + pruned — the full run history. Ignored when `archived` is set.
    all: bool,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct WorkflowArchiveParams {
    /// Prune exactly these instance ids. Non-empty ids override the window:
    /// an unknown or still-running id is an error, never a silent no-op.
    ids: Vec<String>,
    /// Cutoff for the windowed form: a duration (`7d`, `24h`) or a date.
    /// Defaults to `7d`.
    before: Option<String>,
    /// Prune every settled instance regardless of age (overrides `before`).
    all: bool,
    /// Report what would be pruned without touching the store.
    dry_run: bool,
}

#[derive(Deserialize)]
struct SteerParams {
    name: String,
    message: String,
}

#[derive(Deserialize)]
struct DismissParams {
    name: String,
    #[serde(default)]
    no_merge: bool,
}

#[derive(Deserialize)]
struct RevertParams {
    name: String,
    /// Reopen the agent's ticket as `blocked` instead of `open`.
    #[serde(default)]
    block: bool,
}

#[derive(Deserialize)]
struct BlockingParams {
    #[serde(flatten)]
    pattern: PatternParams,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

/// Read a repo's configured URL for `remote` by shelling to git, so the host
/// can be inferred at registration time. Returns `None` when the path is not a
/// repo or has no such remote — host inference is best-effort, never fatal.
fn repo_remote_url(path: &std::path::Path, remote: &str) -> Option<String> {
    let out = std::process::Command::new("git")
        .args(["-C"])
        .arg(path)
        .args(["remote", "get-url", remote])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let url = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if url.is_empty() {
        None
    } else {
        Some(url)
    }
}

#[derive(Deserialize)]
struct RepoAddParams {
    name: String,
    path: String,
    /// Explicit merge mode; when absent the daemon's `[policy]` default applies.
    #[serde(default)]
    merge_mode: Option<rk_core::config::MergeMode>,
    /// Explicit remote name; when absent, `origin` is used at operation time.
    #[serde(default)]
    remote: Option<String>,
}

#[derive(Deserialize)]
struct RepoInspectParams {
    target: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RepoOnboardingStartParams {
    target: String,
    #[serde(default)]
    harness: Option<String>,
    #[serde(default)]
    attach: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RepoOnboardingProposeParams {
    session: String,
    proposal: crate::onboarding_proposals::OnboardingProposalDraft,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RepoOnboardingDecisionParams {
    session: String,
    proposal: String,
    digest: String,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RepoOnboardingApplyParams {
    session: String,
    proposal: String,
    digest: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RepoOnboardingResumeParams {
    session: String,
    #[serde(default)]
    attach: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RepoOnboardingSessionParams {
    session: String,
}

fn onboarding_payload(
    session: &crate::onboarding_sessions::OnboardingSession,
    reused: bool,
) -> Value {
    json!({
        "session": session.status(),
        "report": session.report(),
        "reused": reused,
    })
}

#[derive(Deserialize)]
struct TicketListParams {
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    parent: Option<String>,
}

#[derive(Deserialize)]
struct TicketGetParams {
    id: String,
}

#[derive(Deserialize)]
struct TicketDepParams {
    id: String,
    dep: String,
    #[serde(default)]
    remove: bool,
}

#[derive(Deserialize)]
struct TicketReadyParams {
    #[serde(default)]
    scope: Option<String>,
}

#[derive(Deserialize)]
struct TicketUpdateParams {
    id: String,
    #[serde(flatten)]
    changes: crate::tickets::TicketChanges,
}

fn parse_params<T: serde::de::DeserializeOwned>(params: &Value) -> Result<T, String> {
    serde_json::from_value(params.clone()).map_err(|e| e.to_string())
}

async fn write_json_line<W, T>(write: &mut W, value: &T) -> std::io::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
    T: serde::Serialize,
{
    let mut out = serde_json::to_vec(value)?;
    if out.len() > MAX_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "response exceeds 1 MiB",
        ));
    }
    out.push(b'\n');
    write.write_all(&out).await
}

fn read_pid(layout: &Layout) -> Option<u32> {
    std::fs::read_to_string(layout.pid_file())
        .ok()?
        .trim()
        .parse()
        .ok()
}

fn process_alive(pid: u32) -> bool {
    // kill(pid, 0): 0 or EPERM = alive; ESRCH = gone.
    unsafe { kill_probe(pid as i32, 0) == 0 || last_errno_is_eperm() }
}

extern "C" {
    #[link_name = "kill"]
    fn kill_probe(pid: i32, sig: i32) -> i32;
}

fn last_errno_is_eperm() -> bool {
    std::io::Error::last_os_error().raw_os_error() == Some(1)
}

async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(_) => return std::future::pending().await,
    };
    let mut int = match signal(SignalKind::interrupt()) {
        Ok(s) => s,
        Err(_) => return std::future::pending().await,
    };
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
}

#[cfg(test)]
mod withdraw_authorisation_tests {
    //! TKT-184: who may close a ballot. Pinned as a pure predicate because the
    //! wire tests cannot reach the interesting case — a test client with no
    //! `RK_AGENT` authenticates as `operator`, which is authorised for every
    //! ballot, so the peer-rat denial would never be exercised end to end.
    use super::*;

    #[test]
    fn the_proposer_and_the_operator_may_withdraw() {
        assert!(may_withdraw("Camembert-2", "Camembert-2"), "the proposer");
        assert!(may_withdraw(OPERATOR_ACTOR, "Camembert-2"), "the operator");
        // A local/pre-auth caller arrives blank and is the operator, exactly as
        // `handle_out` and the ticket handlers already read it.
        assert!(may_withdraw("", "Camembert-2"), "a blank caller");
    }

    #[test]
    fn a_peer_rat_may_not_close_someone_elses_ballot() {
        // The load-bearing denial. Declining to endorse is how a rat disagrees
        // with a proposal; if any rat could withdraw any other's, one dissenter
        // would outrank three endorsers and quorum would stop meaning anything.
        assert!(!may_withdraw("Gruyere-2", "Camembert-2"));
        // Name prefixes are distinct rats — a namesake generation must not
        // inherit standing over its predecessor's ballot (the TKT-146 lesson).
        assert!(!may_withdraw("Camembert-2", "Camembert"));
        assert!(!may_withdraw("Camembert", "Camembert-2"));
    }

    /// A ballot proposed BY the operator stays withdrawable by the operator, and
    /// is not thereby opened to every rat.
    #[test]
    fn an_operator_authored_ballot_is_still_operator_only() {
        assert!(may_withdraw(OPERATOR_ACTOR, OPERATOR_ACTOR));
        assert!(!may_withdraw("Gruyere-2", OPERATOR_ACTOR));
    }
}

#[cfg(test)]
mod agent_fact_authorisation_tests {
    use super::*;

    #[test]
    fn an_agent_may_write_a_fact_for_its_own_instance() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Daemon::new_in_memory(Layout::at(dir.path()), "test-castle".into()).unwrap();
        let response = daemon.handle_out(Request {
            id: "fact-1".into(),
            method: "space.out".into(),
            auth: String::new(),
            caller: "Whisker".into(),
            params: json!({
                "category": "fact",
                "scope": "rat-kingdom",
                "identity": "observed-rate-limit",
                "payload": {"limit": 100}
            }),
        });

        assert!(
            response.error.is_none(),
            "agent fact was rejected: {response:?}"
        );
        let facts = daemon
            .space
            .scan(&Pattern::category(Category::Fact).identity("observed-rate-limit"))
            .unwrap();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].instance, "Whisker");
        assert_eq!(facts[0].payload["limit"], 100);
    }

    #[test]
    fn an_agent_may_not_impersonate_another_fact_author() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Daemon::new_in_memory(Layout::at(dir.path()), "test-castle".into()).unwrap();
        let response = daemon.handle_out(Request {
            id: "fact-2".into(),
            method: "space.out".into(),
            auth: String::new(),
            caller: "Whisker".into(),
            params: json!({
                "category": "fact",
                "scope": "rat-kingdom",
                "identity": "forged-observation",
                "instance": "Nibbles",
                "payload": {"forged": true}
            }),
        });

        assert_eq!(
            response.error.as_ref().map(|error| error.code.as_str()),
            Some(codes::FORBIDDEN)
        );
    }

    #[test]
    fn agents_can_vote_change_and_retract_facts_with_aggregate_counts() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Daemon::new_in_memory(Layout::at(dir.path()), "test-castle".into()).unwrap();
        let fact = daemon.handle_out(Request {
            id: "fact-vote-source".into(),
            method: "space.out".into(),
            auth: String::new(),
            caller: "Whisker".into(),
            params: json!({
                "category": "fact",
                "scope": "rat-kingdom",
                "identity": "observed-rate-limit",
                "payload": {"limit": 100}
            }),
        });
        let fact_id = fact.result.unwrap()["id"].as_str().unwrap().to_string();

        let vote = |id: &str, caller: &str, value: &str| {
            daemon.handle_fact_vote(Request {
                id: id.into(),
                method: "fact.vote".into(),
                auth: String::new(),
                caller: caller.into(),
                params: json!({"fact": fact_id, "vote": value}),
            })
        };
        assert_eq!(
            vote("up-1", "Whisker", "up").result.unwrap()["written"],
            true
        );
        assert_eq!(
            vote("up-2", "Whisker", "up").result.unwrap()["already"],
            true
        );
        assert_eq!(
            vote("down-1", "Whisker", "down").result.unwrap()["written"],
            true
        );
        assert_eq!(
            vote("up-3", "Nibbles", "up").result.unwrap()["written"],
            true
        );

        let scan = daemon.handle_scan(Request {
            id: "scan-1".into(),
            method: "space.scan".into(),
            auth: String::new(),
            caller: "operator".into(),
            params: json!({"category": "fact"}),
        });
        assert_eq!(
            scan.result.as_ref().unwrap()["fact_votes"][&fact_id]["up"],
            1
        );
        assert_eq!(
            scan.result.as_ref().unwrap()["fact_votes"][&fact_id]["down"],
            1
        );
        assert_eq!(
            scan.result.as_ref().unwrap()["fact_votes"][&fact_id]["score"],
            0
        );

        assert_eq!(
            vote("clear-1", "Whisker", "clear").result.unwrap()["written"],
            true
        );
        assert_eq!(
            vote("clear-2", "Whisker", "clear").result.unwrap()["already"],
            true
        );
        let scan = daemon.handle_scan(Request {
            id: "scan-2".into(),
            method: "space.scan".into(),
            auth: String::new(),
            caller: "operator".into(),
            params: json!({"category": "fact"}),
        });
        assert_eq!(
            scan.result.as_ref().unwrap()["fact_votes"][&fact_id]["up"],
            1
        );
        assert_eq!(
            scan.result.as_ref().unwrap()["fact_votes"][&fact_id]["down"],
            0
        );
        assert_eq!(
            scan.result.as_ref().unwrap()["fact_votes"][&fact_id]["score"],
            1
        );

        let raw = daemon.handle_out(Request {
            id: "forged-vote".into(),
            method: "space.out".into(),
            auth: String::new(),
            caller: "Whisker".into(),
            params: json!({
                "category": "fact_vote",
                "scope": "rat-kingdom",
                "identity": fact_id,
                "instance": "Nibbles",
                "payload": {"vote": "up"}
            }),
        });
        assert_eq!(
            raw.error.as_ref().map(|error| error.code.as_str()),
            Some(codes::FORBIDDEN)
        );
    }
}

#[cfg(test)]
mod default_agent_profile_tests {
    use super::*;
    use rk_core::config::AgentProfileConfig;
    use std::process::Command;

    fn git(repo: &std::path::Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[tokio::test]
    async fn config_default_profile_reaches_a_direct_spawn() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        std::fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "-b", "main"]);
        git(&repo, &["config", "user.email", "rat@example.com"]);
        git(&repo, &["config", "user.name", "Rat"]);
        std::fs::write(repo.join("README.md"), "test\n").unwrap();
        git(&repo, &["add", "."]);
        git(&repo, &["commit", "-m", "initial"]);

        let mut config = rk_core::config::Config::default();
        config.harness.default = "claude".into();
        config.agents.insert(
            "default".into(),
            AgentProfileConfig {
                harness: Some("fake".into()),
                model: Some("profile-model".into()),
                permission_mode: Some("workspace-write".into()),
            },
        );
        let daemon = Daemon::new(Layout::at(dir.path().join("rk-home")), &config).unwrap();
        let record = daemon
            .supervisor
            .spawn_async(crate::supervisor::SpawnParams {
                repo: repo.display().to_string(),
                task: "direct-defaults".into(),
                prompt: Some("finish".into()),
                role: "rat".into(),
                coordination: None,
                harness: None,
                parent: None,
                base: None,
                model: None,
                permission_mode: None,
                attach: false,
                workflow_instance: None,
                coordinator: None,
                instance_max_usd: None,
            })
            .await
            .unwrap();

        assert_eq!(record.harness, "fake");
        assert_eq!(record.model.as_deref(), Some("profile-model"));
        assert_eq!(record.permission_mode.as_deref(), Some("workspace-write"));
    }
}

#[cfg(test)]
mod display_alias_tests {
    //! TKT-124: the `castle_name` alias is presentation-only. These tests pin the
    //! load-bearing invariant that the alias is confined to the display field and
    //! NEVER becomes the wire identity (`self.castle`) — the string handed to the
    //! syncer and stamped on every daemon-authored tuple's `instance`. If a
    //! future refactor re-routes `castle_name` back into the wire id, the alias
    //! would leak into signed `SyncRecord`s; that regression fails here.
    use super::*;
    use rk_core::config::Config;

    fn daemon_with_alias(alias: Option<&str>) -> (tempfile::TempDir, Daemon) {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        let config = Config {
            castle_name: alias.map(|s| s.to_string()),
            ..Config::default()
        };
        let daemon = Daemon::new(layout, &config).unwrap();
        (dir, daemon)
    }

    #[test]
    fn alias_is_the_display_but_never_the_wire_id() {
        let (_dir, daemon) = daemon_with_alias(Some("Nikaido"));
        // Wire id is the crypto actor, NOT the alias — so nothing the syncer
        // exports (author == self.castle) can carry "Nikaido".
        assert!(daemon.castle.starts_with("castle-"));
        assert_ne!(daemon.castle, "Nikaido");
        // Display + the status render path both show the friendly alias.
        assert_eq!(daemon.castle_display, "Nikaido");
        assert_eq!(daemon.status()["castle"], json!("Nikaido"));
    }

    #[test]
    fn unset_alias_shows_the_actor_id_unchanged() {
        let (_dir, daemon) = daemon_with_alias(None);
        assert!(daemon.castle.starts_with("castle-"));
        // No behaviour change when unset: display == wire id == status.
        assert_eq!(daemon.castle_display, daemon.castle);
        assert_eq!(daemon.status()["castle"], json!(daemon.castle));
    }

    /// The faithful end-to-end regression: with an alias configured AND sync on,
    /// read back the records this castle actually exported (its own notes ref)
    /// and assert the alias appears in NONE of them — every record carries only
    /// the crypto actor id.
    #[test]
    fn alias_never_appears_in_an_exported_sync_record() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        let mut config = Config {
            castle_name: Some("Nikaido".into()),
            ..Config::default()
        };
        config.sync.enabled = true;
        let daemon = Daemon::new(layout, &config).unwrap();

        // Syncer::new wrote the castle-presence record at construction. Read it
        // back through a reader bound to the same identity/ref.
        let syncer = daemon
            .syncer
            .as_ref()
            .expect("sync enabled → syncer present");
        let identity =
            rk_core::identity::CastleIdentity::load_or_create(&daemon.layout.castle_key_path())
                .unwrap();
        let reader = rk_sync::NotesSync::new(syncer.repo_path(), identity);
        let records = reader.own_records().unwrap();

        assert!(!records.is_empty(), "expected at least the presence record");
        for r in &records {
            let json = serde_json::to_string(r).unwrap();
            assert!(
                !json.contains("Nikaido"),
                "presentation alias leaked into a SyncRecord: {json}"
            );
            assert!(
                r.actor.starts_with("castle-"),
                "record actor must be the crypto id, got {}",
                r.actor
            );
        }
    }
}

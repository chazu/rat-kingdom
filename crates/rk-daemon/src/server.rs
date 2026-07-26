//! The daemon server: accepts NDJSON requests on a Unix socket and dispatches
//! them. Hosts the tuplespace; `space.watch` upgrades a connection to a
//! server-push event stream.

use crate::proto::{codes, Request, Response};
use chrono::{DateTime, Utc};
use rk_core::paths::Layout;
use rk_core::tuple::{Category, Lifecycle, Pattern, Tuple};
use rk_space::Space;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;
use tracing::{debug, info, warn};

const GC_INTERVAL: Duration = Duration::from_secs(60);
// Default lifetime for a pheromone trail (claim / obstacle / need) written
// without an explicit TTL — the hard-TTL backstop for strength decay — lives in
// rk-core so daemon-internal trail writers (supervisor, syncer) age on the same
// clock as this RPC boundary.
use rk_core::tuple::DEFAULT_TRAIL_TTL;
/// Ceiling for blocking reads so a lost client cannot pin a connection task
/// forever; clients requesting more get clamped.
const MAX_BLOCK: Duration = Duration::from_secs(3600);
const DEFAULT_BLOCK: Duration = Duration::from_secs(5);

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
    /// Fleet-wide default merge mode a repo is registered with when `rk repo
    /// add` names no explicit `--merge-mode` (`[policy] default_merge_mode`).
    default_merge_mode: rk_core::config::MergeMode,
    engine: std::sync::OnceLock<Arc<crate::workflow_exec::WorkflowEngine>>,
    repos: std::sync::Mutex<crate::repos::RepoRegistry>,
    tickets: Arc<crate::tickets::Tickets>,
    started: Instant,
    shutdown_tx: watch::Sender<bool>,
}

impl Daemon {
    pub fn new(layout: Layout, config: &rk_core::config::Config) -> rk_core::Result<Self> {
        layout.ensure()?;
        let space = Space::open(&layout.db_path())?;
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
        let mut daemon = Self::with_space(
            layout,
            actor,
            config.harness.default.clone(),
            budget,
            fleet_budget,
            space,
        )?;
        daemon.castle_display = castle_display;
        daemon.global_agents = config
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
        daemon.default_merge_mode = config.policy.default_merge_mode;
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
        layout.ensure()?;
        // One Tickets instance, shared by the RPC handlers and the supervisor,
        // so ticket-lifecycle writes serialize on a single lock.
        let tickets = Arc::new(crate::tickets::Tickets::new(space.clone(), castle.clone()));
        let supervisor = Arc::new(crate::supervisor::Supervisor::new(
            layout.clone(),
            castle.clone(),
            default_harness.clone(),
            budget,
            fleet_budget,
            space.clone(),
            tickets.clone(),
        )?);
        let (shutdown_tx, _) = watch::channel(false);
        let repos = std::sync::Mutex::new(crate::repos::RepoRegistry::load(
            &layout.home().join("repos.json"),
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
            default_merge_mode: rk_core::config::MergeMode::default(),
            engine: std::sync::OnceLock::new(),
            repos,
            tickets,
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
        std::fs::write(self.layout.pid_file(), std::process::id().to_string())?;
        info!(socket = %sock.display(), pid = std::process::id(), castle = %self.castle_display, "daemon listening");
        // Only now that the bind is won may shared state be touched.
        self.supervisor.on_daemon_started();

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
                            supervisor.sweep(&cfg);
                            // Self-healing respawn rides the same tick (TKT-53):
                            // relaunch crashed/orphaned rats with crash-loop
                            // backoff. No-op unless [supervisor].respawn_enabled.
                            supervisor.respawn_sweep(&cfg);
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
                            let daemon = Arc::clone(&daemon);
                            tokio::spawn(async move {
                                if let Err(e) = daemon.serve_conn(stream).await {
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
            ))
        }))
    }

    async fn serve_conn(&self, stream: UnixStream) -> std::io::Result<()> {
        let (read, mut write) = stream.into_split();
        let mut lines = BufReader::new(read).lines();

        while let Some(line) = lines.next_line().await? {
            if line.trim().is_empty() {
                continue;
            }
            let outcome = match serde_json::from_str::<Request>(&line) {
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
        Ok(())
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
            "agent.spawn" => reply(self.handle_spawn(req)),
            "agent.respawn" => reply(self.handle_named(req, |sup, name| {
                sup.respawn(&name).map(|r| json!({"agent": r}))
            })),
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
            "agent.archive" => reply(self.handle_agent_archive(req)),
            "agent.unarchive" => {
                reply(self.handle_named(req, |sup, name| sup.unarchive_agent(&name)))
            }
            "budget.rollup" => reply(Response::ok(id, self.supervisor.fleet_rollup())),
            "inbox.list" => reply(self.handle_inbox(id)),
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
            "workflow.run" => {
                let params: WorkflowRunParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                reply(
                    match self.engine().run(&params.name, &params.repo, params.params) {
                        Ok(instance) => Response::ok(id, json!({"instance": instance})),
                        Err(e) => Response::err(id, codes::INTERNAL, e.to_string()),
                    },
                )
            }
            "workflow.list" => reply(Response::ok(id, json!({"instances": self.engine().list()}))),
            "workflow.status" => {
                let params: NameParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                reply(match self.engine().status(&params.name) {
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
                reply(match self.engine().timeline(&params.name) {
                    Some((instance, steps)) => {
                        Response::ok(id, json!({"instance": instance, "steps": steps}))
                    }
                    None => Response::err(
                        id,
                        codes::INTERNAL,
                        format!("no such instance: {}", params.name),
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
                reply(Response::ok(
                    id,
                    json!({"definitions": self.engine().definitions(&params.repo)}),
                ))
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
            "repo.add" => reply(self.handle_repo_add(req)),
            "repo.list" => reply(match self.repos.lock() {
                Ok(reg) => Response::ok(id, json!({"repos": reg.list()})),
                Err(_) => Response::err(id, codes::INTERNAL, "repo registry lock poisoned"),
            }),
            "repo.get" => reply(self.handle_repo_get(req)),
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
    fn handle_inbox(&self, id: String) -> Response {
        let agents = self.supervisor.list();
        let instances = self.engine().list();
        let obstacles = match self.space.scan(&Pattern::category(Category::Obstacle)) {
            Ok(t) => t,
            Err(e) => return Response::err(id, codes::INTERNAL, e.to_string()),
        };
        let needs = match self.space.scan(&Pattern::category(Category::Need)) {
            Ok(t) => t,
            Err(e) => return Response::err(id, codes::INTERNAL, e.to_string()),
        };
        // Open PRs/MRs: a PR-mode dismiss/land emits a `pull_request_opened`
        // event, then the run completes — nothing else tracks the pushed branch.
        let pull_requests = match self
            .space
            .scan(&Pattern::category(Category::Event).identity("pull_request_opened"))
        {
            Ok(t) => t,
            Err(e) => return Response::err(id, codes::INTERNAL, e.to_string()),
        };
        // `pull_request_closed` events are emitted by the fetch-driven review
        // sweep (TKT-70): a background pass fetched the forge and saw the branch
        // merged/deleted upstream even though the operator never pulled, so the
        // LOCAL detection below could not see it. `build` folds their branches
        // into the same suppression. Reading the events is cheap and stays on
        // the hot path; the fetch that produces them does not.
        let pull_requests_closed = match self
            .space
            .scan(&Pattern::category(Category::Event).identity("pull_request_closed"))
        {
            Ok(t) => t,
            Err(e) => return Response::err(id, codes::INTERNAL, e.to_string()),
        };
        // An awaiting-review row auto-clears once its branch is merged into the
        // target (or gone) — the PR-mode land opened a PR but nothing emits a
        // close event when the human merges on the forge. Detect it locally:
        // resolve each open PR's repo and ask git whether the branch has landed.
        // Local-only (no fetch, no forge API), so the row clears when the merge
        // reaches the local target — the operator's pull or a Direct-mode
        // fast-forward. The `pull_request_closed` events above close the same gap
        // for a forge merge the operator has NOT pulled (TKT-70).
        let cleared_prs = self.cleared_pull_requests(&pull_requests);
        let items = crate::inbox::build(
            &agents,
            &instances,
            &obstacles,
            &needs,
            &pull_requests,
            &pull_requests_closed,
            &cleared_prs,
        );
        Response::ok(id, crate::inbox::to_json(&items))
    }

    /// (scope, branch) pairs among the open-PR events whose branch has since
    /// been merged into its target or deleted locally — the rows to drop from
    /// the awaiting-review queue. Resolves each event's scope to a registered
    /// repo path and asks git; an unregistered scope or unopenable repo means
    /// "cannot tell", so the row stays (fails toward surfacing, never hiding).
    fn cleared_pull_requests(&self, pull_requests: &[Tuple]) -> HashSet<(String, String)> {
        let mut cleared = HashSet::new();
        // Resolve scopes to paths once, under the registry lock, then release it
        // before shelling out to git.
        let mut paths: std::collections::HashMap<String, std::path::PathBuf> =
            std::collections::HashMap::new();
        if let Ok(reg) = self.repos.lock() {
            for t in pull_requests {
                if !paths.contains_key(&t.scope) {
                    if let Some(rec) = reg.get(&t.scope) {
                        paths.insert(t.scope.clone(), rec.path.clone());
                    }
                }
            }
        }
        for t in pull_requests {
            let Some(branch) = t.payload.get("branch").and_then(|v| v.as_str()) else {
                continue;
            };
            let target = t
                .payload
                .get("target")
                .and_then(|v| v.as_str())
                .unwrap_or("main");
            let Some(path) = paths.get(&t.scope) else {
                continue;
            };
            let Ok(repo) = rk_git::Repo::discover(path) else {
                continue;
            };
            if repo.branch_merged_or_gone(branch, target) {
                cleared.insert((t.scope.clone(), branch.to_string()));
            }
        }
        cleared
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

    fn handle_repo_add(&self, req: Request) -> Response {
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
        let host = repo_remote_url(&path, remote_name).and_then(|url| crate::repos::infer_host(&url));
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

    async fn handle_ticket_new(&self, req: Request) -> Response {
        let params: crate::tickets::NewTicket = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
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

    fn handle_spawn(&self, req: Request) -> Response {
        let params: crate::supervisor::SpawnParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        match self.supervisor.spawn(params) {
            Ok(record) => Response::ok(req.id, json!({"agent": record})),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    /// `agent.archive` — offload settled terminal records out of the default
    /// views. The daemon owns `agents.json` and rewrites it on every mutation,
    /// so this has to be an RPC: an external edit would be clobbered by the
    /// next `Registry::persist`.
    fn handle_agent_archive(&self, req: Request) -> Response {
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
        match self.supervisor.archive_agents(cutoff, params.dry_run, reap) {
            Ok(v) => Response::ok(req.id, v),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
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
        let mut tuple = Tuple::new(
            params.category,
            params.scope,
            params.identity,
            params.instance.unwrap_or_else(|| self.castle.clone()),
            params.payload,
        );
        let explicit_lifecycle = params.lifecycle.is_some() || params.ttl_secs.is_some();
        if let Some(lifecycle) = params.lifecycle {
            tuple = tuple.with_lifecycle(lifecycle);
        }
        if let Some(ttl_secs) = params.ttl_secs {
            tuple.lifecycle = Lifecycle::Ephemeral;
            tuple.expires_at =
                Some(chrono::Utc::now() + chrono::Duration::seconds(ttl_secs as i64));
        }
        // Pheromone trails carry a decaying strength and default to an Ephemeral
        // lifetime so an abandoned one evaporates instead of lingering forever.
        if tuple.category.evaporates() {
            tuple.strength = Some(rk_core::tuple::FULL_STRENGTH);
            if !explicit_lifecycle {
                tuple.lifecycle = Lifecycle::Ephemeral;
                tuple.expires_at = Some(
                    chrono::Utc::now()
                        + chrono::Duration::seconds(DEFAULT_TRAIL_TTL.as_secs() as i64),
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
            Ok(t) => Response::ok(req.id, json!({"id": t.id, "written": true})),
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
        let result = if params.hot || params.top.is_some() {
            self.space.scan_hot(&params.pattern, params.top)
        } else {
            self.space.scan(&params.pattern)
        };
        match result {
            Ok(tuples) => Response::ok(req.id, json!({"tuples": tuples})),
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
            Ok(Some(tuple)) => Response::ok(req.id, json!({"tuple": tuple})),
            Ok(None) => Response::ok(req.id, json!({"tuple": null, "timed_out": true})),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
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

enum Outcome {
    Reply(Response),
    Watch {
        response: Response,
        pattern: Pattern,
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

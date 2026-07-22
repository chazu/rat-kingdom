//! The daemon server: accepts NDJSON requests on a Unix socket and dispatches
//! them. Hosts the tuplespace; `space.watch` upgrades a connection to a
//! server-push event stream.

use crate::proto::{codes, Request, Response};
use rk_core::paths::Layout;
use rk_core::tuple::{Category, Lifecycle, Pattern, Tuple};
use rk_space::Space;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;
use tracing::{debug, info, warn};

const GC_INTERVAL: Duration = Duration::from_secs(60);
/// Ceiling for blocking reads so a lost client cannot pin a connection task
/// forever; clients requesting more get clamped.
const MAX_BLOCK: Duration = Duration::from_secs(3600);
const DEFAULT_BLOCK: Duration = Duration::from_secs(5);

pub struct Daemon {
    layout: Layout,
    space: Space,
    castle: String,
    supervisor: Arc<crate::supervisor::Supervisor>,
    syncer: Option<Arc<crate::sync::Syncer>>,
    sync_interval: Duration,
    global_agents: std::collections::HashMap<String, rk_workflow::AgentProfile>,
    default_harness: String,
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
        let mut daemon = Self::with_space(
            layout,
            config.castle_name(),
            config.harness.default.clone(),
            budget,
            space,
        )?;
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
        Self::with_space(layout, castle, default_harness, budget, space)
    }

    fn with_space(
        layout: Layout,
        castle: String,
        default_harness: String,
        budget: rk_ledger::Budget,
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
            castle,
            supervisor,
            syncer: None,
            sync_interval: Duration::from_secs(30),
            global_agents: Default::default(),
            default_harness,
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
        info!(socket = %sock.display(), pid = std::process::id(), castle = %self.castle, "daemon listening");
        // Only now that the bind is won may shared state be touched.
        self.supervisor.on_daemon_started();

        let daemon = Arc::new(self);
        let mut shutdown_rx = daemon.shutdown_tx.subscribe();

        // GC loop: TTL expiry only — escalation/analytics live elsewhere.
        {
            let space = daemon.space.clone();
            let mut gc_shutdown = daemon.shutdown_tx.subscribe();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(GC_INTERVAL);
                loop {
                    tokio::select! {
                        _ = tick.tick() => match space.gc_expired() {
                            Ok(0) => {}
                            Ok(n) => debug!(collected = n, "gc collected expired tuples"),
                            Err(e) => warn!(error = %e, "gc failed"),
                        },
                        _ = gc_shutdown.changed() => break,
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
                self.default_harness.clone(),
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
            "agent.list" => reply(Response::ok(id, json!({"agents": self.supervisor.list()}))),
            "agent.status" => reply(self.handle_named(req, |sup, name| {
                sup.status(&name)
                    .map(|r| json!({"agent": r}))
                    .ok_or_else(|| rk_core::Error::other(format!("no such agent: {name}")))
            })),
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
        let record = crate::repos::RepoRecord {
            name: params.name,
            path,
            created_at: chrono::Utc::now(),
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
        match self.tickets.list(params.scope, params.status, params.parent) {
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
        if let Some(lifecycle) = params.lifecycle {
            tuple = tuple.with_lifecycle(lifecycle);
        }
        if let Some(ttl_secs) = params.ttl_secs {
            tuple.lifecycle = Lifecycle::Ephemeral;
            tuple.expires_at =
                Some(chrono::Utc::now() + chrono::Duration::seconds(ttl_secs as i64));
        }
        match self.space.out(tuple.clone()) {
            Ok(()) => Response::ok(req.id, json!({"id": tuple.id, "written": true})),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    fn handle_scan(&self, req: Request) -> Response {
        let params: PatternParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        match self.space.scan(&params.pattern) {
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
            "castle": self.castle,
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
struct NameParams {
    name: String,
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
struct BlockingParams {
    #[serde(flatten)]
    pattern: PatternParams,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Deserialize)]
struct RepoAddParams {
    name: String,
    path: String,
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

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
    started: Instant,
    shutdown_tx: watch::Sender<bool>,
}

impl Daemon {
    pub fn new(layout: Layout, castle: String) -> rk_core::Result<Self> {
        layout.ensure()?;
        let space = Space::open(&layout.db_path())?;
        let (shutdown_tx, _) = watch::channel(false);
        Ok(Self {
            layout,
            space,
            castle,
            started: Instant::now(),
            shutdown_tx,
        })
    }

    #[doc(hidden)]
    pub fn new_in_memory(layout: Layout, castle: String) -> rk_core::Result<Self> {
        let space = Space::open_in_memory()?;
        let (shutdown_tx, _) = watch::channel(false);
        Ok(Self {
            layout,
            space,
            castle,
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
            debug!(path = %sock.display(), "removing stale socket");
            std::fs::remove_file(&sock)?;
        }

        let listener = UnixListener::bind(&sock)?;
        std::fs::write(self.layout.pid_file(), std::process::id().to_string())?;
        info!(socket = %sock.display(), pid = std::process::id(), castle = %self.castle, "daemon listening");

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

        std::fs::remove_file(daemon.layout.socket_path()).ok();
        std::fs::remove_file(daemon.layout.pid_file()).ok();
        Ok(())
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
            other => reply(Response::err(
                id,
                codes::UNKNOWN_METHOD,
                format!("unknown method: {other}"),
            )),
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
struct BlockingParams {
    #[serde(flatten)]
    pattern: PatternParams,
    #[serde(default)]
    timeout_ms: Option<u64>,
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

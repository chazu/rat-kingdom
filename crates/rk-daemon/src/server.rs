//! The daemon server: accepts NDJSON requests on a Unix socket and dispatches
//! them. Phase 0 handles lifecycle methods; the tuplespace mounts here in
//! Phase 1 as additional methods on [`Dispatcher`].

use crate::proto::{codes, Request, Response};
use rk_core::paths::Layout;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;
use tracing::{debug, info, warn};

pub struct Daemon {
    layout: Layout,
    started: Instant,
    shutdown_tx: watch::Sender<bool>,
}

impl Daemon {
    pub fn new(layout: Layout) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            layout,
            started: Instant::now(),
            shutdown_tx,
        }
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
        info!(socket = %sock.display(), pid = std::process::id(), "daemon listening");

        let daemon = Arc::new(self);
        let mut shutdown_rx = daemon.shutdown_tx.subscribe();

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
            let response = match serde_json::from_str::<Request>(&line) {
                Ok(req) => self.dispatch(req).await,
                Err(e) => Response::err("", codes::BAD_PARAMS, format!("bad request: {e}")),
            };
            let mut out = serde_json::to_vec(&response)?;
            out.push(b'\n');
            write.write_all(&out).await?;
        }
        Ok(())
    }

    async fn dispatch(&self, req: Request) -> Response {
        debug!(method = %req.method, id = %req.id, "dispatch");
        match req.method.as_str() {
            "ping" => Response::ok(req.id, json!("pong")),
            "status" => Response::ok(req.id, self.status()),
            "stop" => {
                let resp = Response::ok(req.id, json!({"stopping": true}));
                let _ = self.shutdown_tx.send(true);
                resp
            }
            other => Response::err(
                req.id,
                codes::UNKNOWN_METHOD,
                format!("unknown method: {other}"),
            ),
        }
    }

    fn status(&self) -> Value {
        json!({
            "version": env!("CARGO_PKG_VERSION"),
            "pid": std::process::id(),
            "uptime_secs": self.started.elapsed().as_secs(),
            "socket": self.layout.socket_path(),
        })
    }
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

//! Client side: connect to the daemon socket, lazily spawning the server if it
//! isn't running (the tmux/herdr model — no separate install step).

use crate::proto::{Request, Response};
use rk_core::paths::Layout;
use serde_json::Value;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tracing::debug;

pub struct Client {
    stream: BufReader<UnixStream>,
    next_id: u64,
}

impl Client {
    /// Connect to a running daemon; error if none is listening.
    pub async fn connect(layout: &Layout) -> rk_core::Result<Self> {
        let sock = layout.socket_path();
        let stream = UnixStream::connect(&sock)
            .await
            .map_err(|_| rk_core::Error::DaemonNotRunning(sock.display().to_string()))?;
        Ok(Self {
            stream: BufReader::new(stream),
            next_id: 0,
        })
    }

    /// Connect, auto-starting a detached daemon process if needed.
    pub async fn connect_or_spawn(layout: &Layout) -> rk_core::Result<Self> {
        if let Ok(client) = Self::connect(layout).await {
            return Ok(client);
        }
        spawn_detached_daemon(layout)?;
        // Poll for the socket to come up.
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if let Ok(client) = Self::connect(layout).await {
                return Ok(client);
            }
        }
        Err(rk_core::Error::DaemonNotRunning(
            layout.socket_path().display().to_string(),
        ))
    }

    /// Upgrade this connection to a watch stream: sends `space.watch`, then
    /// notifications arrive via [`WatchStream::next`].
    pub async fn watch(mut self, params: Value) -> rk_core::Result<WatchStream> {
        self.call("space.watch", params).await?;
        Ok(WatchStream {
            stream: self.stream,
        })
    }

    pub async fn call(&mut self, method: &str, params: Value) -> rk_core::Result<Value> {
        self.next_id += 1;
        let req = Request {
            id: self.next_id.to_string(),
            method: method.to_string(),
            params,
        };
        let mut line = serde_json::to_vec(&req)?;
        line.push(b'\n');
        self.stream.get_mut().write_all(&line).await?;

        let mut buf = String::new();
        let n = self.stream.read_line(&mut buf).await?;
        if n == 0 {
            return Err(rk_core::Error::Protocol("daemon closed connection".into()));
        }
        let resp: Response = serde_json::from_str(&buf)?;
        if let Some(err) = resp.error {
            return Err(rk_core::Error::Protocol(format!(
                "{}: {}",
                err.code, err.message
            )));
        }
        Ok(resp.result.unwrap_or(Value::Null))
    }
}

/// A connection upgraded to a live tuple feed by [`Client::watch`].
pub struct WatchStream {
    stream: BufReader<UnixStream>,
}

impl WatchStream {
    /// The next pushed notification (`{"method": "tuple"|"lagged", "params": ...}`),
    /// or `None` when the daemon closes the stream.
    pub async fn next(&mut self) -> rk_core::Result<Option<Value>> {
        let mut buf = String::new();
        let n = self.stream.read_line(&mut buf).await?;
        if n == 0 {
            return Ok(None);
        }
        Ok(Some(serde_json::from_str(&buf)?))
    }
}

/// Start `rk daemon run` as a detached child of init, disowned from our tty and
/// process group, logging to the layout's log dir.
fn spawn_detached_daemon(layout: &Layout) -> rk_core::Result<()> {
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};

    layout.ensure()?;
    let exe = std::env::current_exe()?;
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(layout.log_dir().join("daemon.log"))?;

    debug!(exe = %exe.display(), "auto-starting daemon");
    Command::new(exe)
        .args(["daemon", "run"])
        .env("RK_HOME", layout.home())
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .process_group(0)
        .spawn()?;
    Ok(())
}

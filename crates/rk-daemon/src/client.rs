//! Client side: connect to the daemon socket, lazily spawning the server if it
//! isn't running (the tmux/herdr model — no separate install step).

use crate::proto::{Request, Response, RpcError};
use rk_core::paths::Layout;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tracing::debug;

pub struct Client {
    stream: BufReader<UnixStream>,
    next_id: u64,
    auth_token: String,
    caller: String,
}

#[derive(Debug)]
pub enum ClientRpcError {
    Rpc(RpcError),
    Transport(rk_core::Error),
}

impl std::fmt::Display for ClientRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rpc(err) => write!(f, "{}: {}", err.code, err.message),
            Self::Transport(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for ClientRpcError {}

impl From<rk_core::Error> for ClientRpcError {
    fn from(value: rk_core::Error) -> Self {
        Self::Transport(value)
    }
}

/// The caller name a connection carries when no agent identity applies.
pub const OPERATOR: &str = "operator";

/// Resolve the `(caller, token)` an ambient connection speaks as, reading
/// identity from `var` rather than the process environment directly so the
/// rules stay testable without mutating global state.
///
/// `RK_AGENT` names the caller; `RK_AUTH_TOKEN` overrides the token the
/// supervisor already handed the agent, which is why an agent session never
/// has to read the operator token off disk. This selects the wire identity but
/// does not grant authority by itself: the server also binds the connection to
/// its kernel-observed process origin.
fn ambient_identity(
    layout: &Layout,
    var: impl Fn(&str) -> Option<String>,
) -> rk_core::Result<(String, String)> {
    let caller = var("RK_AGENT").unwrap_or_else(|| OPERATOR.into());
    let auth_token = match var("RK_AUTH_TOKEN") {
        Some(token) => token,
        None => token_for(layout, &caller)?,
    };
    Ok((caller, auth_token))
}

/// The token `caller` authenticates with against `layout`, derived from that
/// layout's own root token — never from the environment.
fn token_for(layout: &Layout, caller: &str) -> rk_core::Result<String> {
    if caller == OPERATOR {
        layout.auth_token()
    } else {
        layout.agent_auth_token(caller)
    }
}

impl Client {
    /// Connect to a running daemon as whoever the environment says we are;
    /// error if none is listening.
    ///
    /// This is the identity `rk` itself should use: inside a supervised agent
    /// session the spawn env names the rat, and everywhere else the caller is
    /// the operator. Code that drives a daemon it constructed — tests, above
    /// all — must use [`Client::connect_as_operator`] instead, so an ambient
    /// `RK_AGENT` cannot silently downgrade it to an agent caller.
    pub async fn connect(layout: &Layout) -> rk_core::Result<Self> {
        Self::open(layout, |layout| {
            ambient_identity(layout, |key| std::env::var(key).ok())
        })
        .await
    }

    /// Connect as an explicitly named caller, ignoring `RK_AGENT` and
    /// `RK_AUTH_TOKEN` entirely; the token is derived from `layout`.
    ///
    /// Use this whenever the identity is a property of the code rather than of
    /// the process that happens to be running it.
    pub async fn connect_as(layout: &Layout, caller: &str) -> rk_core::Result<Self> {
        Self::open(layout, |layout| {
            Ok((caller.to_string(), token_for(layout, caller)?))
        })
        .await
    }

    /// Connect as the operator, whatever the ambient environment claims.
    ///
    /// Tests run inside rats, whose spawn env sets `RK_AGENT`/`RK_AUTH_TOKEN`,
    /// and test processes inherit it. A test daemon driven through
    /// [`Client::connect`] would therefore speak as that rat — against the
    /// *live* fleet's derived token, not the temp layout's — and be refused
    /// for every operator-only method (`workflow.run`, `agent.spawn`, ...).
    /// Naming the caller here keeps a test's authority a property of the test.
    pub async fn connect_as_operator(layout: &Layout) -> rk_core::Result<Self> {
        Self::connect_as(layout, OPERATOR).await
    }

    /// Open the socket, then resolve the identity to bind to it.
    ///
    /// The socket comes first so a down daemon still reports as
    /// `DaemonNotRunning` rather than as whatever the token lookup says —
    /// `connect_or_spawn` and `rk daemon status` both branch on that.
    async fn open(
        layout: &Layout,
        identity: impl FnOnce(&Layout) -> rk_core::Result<(String, String)>,
    ) -> rk_core::Result<Self> {
        let sock = layout.socket_path();
        let stream = UnixStream::connect(&sock)
            .await
            .map_err(|_| rk_core::Error::DaemonNotRunning(sock.display().to_string()))?;
        let (caller, auth_token) = identity(layout)?;
        Ok(Self {
            stream: BufReader::new(stream),
            next_id: 0,
            auth_token,
            caller,
        })
    }

    /// Connect, auto-starting a detached daemon process if needed.
    ///
    /// Never auto-spawns from inside an agent session (`RK_AGENT` set): the
    /// agent may be running under a harness sandbox where socket connects
    /// fail — spawning a daemon there would clobber the real daemon's socket
    /// and registry from inside the sandbox.
    pub async fn connect_or_spawn(layout: &Layout) -> rk_core::Result<Self> {
        if let Ok(client) = Self::connect(layout).await {
            return Ok(client);
        }
        if std::env::var("RK_AGENT").is_ok() {
            return Err(rk_core::Error::other(
                "cannot reach the rk daemon from this agent session — if your harness \
                 sandbox blocks unix sockets, the orchestrator must launch you with \
                 socket access (rk never starts daemons from inside agent sessions)",
            ));
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

    /// Send one request, keep its reply, then upgrade the same connection to a
    /// notification stream (used by `agent.log --follow`: the reply carries the
    /// backlog, the stream carries new entries).
    pub async fn call_then_stream(
        mut self,
        method: &str,
        params: Value,
    ) -> rk_core::Result<(Value, WatchStream)> {
        let reply = self.call(method, params).await?;
        Ok((
            reply,
            WatchStream {
                stream: self.stream,
            },
        ))
    }

    pub async fn call(&mut self, method: &str, params: Value) -> rk_core::Result<Value> {
        let resp = self.call_raw(method, params).await?;
        if let Some(err) = resp.error {
            return Err(rk_core::Error::Protocol(format!(
                "{}: {}",
                err.code, err.message
            )));
        }
        Ok(resp.result.unwrap_or(Value::Null))
    }

    pub async fn call_raw(&mut self, method: &str, params: Value) -> rk_core::Result<Response> {
        self.next_id += 1;
        let req = Request {
            id: self.next_id.to_string(),
            method: method.to_string(),
            auth: self.auth_token.clone(),
            caller: self.caller.clone(),
            client_version: Some(rk_core::version::BUILD_VERSION.into()),
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
        let response: Response = serde_json::from_str(&buf)?;
        warn_on_version_mismatch(response.server_version.as_deref());
        Ok(response)
    }

    pub async fn call_typed<T: DeserializeOwned>(
        &mut self,
        method: &str,
        params: Value,
    ) -> Result<T, ClientRpcError> {
        let resp = self.call_raw(method, params).await?;
        if let Some(err) = resp.error {
            return Err(ClientRpcError::Rpc(err));
        }
        let value = resp.result.unwrap_or(Value::Null);
        serde_json::from_value(value).map_err(|err| rk_core::Error::from(err).into())
    }
}

/// Whether this process has already said its piece about a build mismatch.
static VERSION_WARNED: AtomicBool = AtomicBool::new(false);

/// Complain to stderr when the daemon on the other end is a different build.
///
/// Once per process, not once per call: a single `rk` invocation makes several
/// RPCs and the operator needs the warning to be impossible to miss, not
/// repeated until it is noise. Stderr keeps `--json` stdout parseable.
///
/// `None` — a daemon too old to stamp its replies at all — is itself proof of
/// a mismatch, since every build carrying this code stamps every response.
fn warn_on_version_mismatch(server_version: Option<&str>) {
    let remote = server_version.unwrap_or("an unstamped build (predates this handshake)");
    let Some(warning) = rk_core::version::mismatch_warning(rk_core::version::BUILD_VERSION, remote)
    else {
        return;
    };
    if VERSION_WARNED.swap(true, Ordering::Relaxed) {
        return;
    }
    eprintln!("{warning}");
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

#[cfg(test)]
mod tests {
    use super::*;

    /// These exercise identity resolution through an injected lookup rather
    /// than `std::env::set_var`, which is process-global and races cargo's
    /// threaded test runner.
    fn env_of(pairs: Vec<(&'static str, &'static str)>) -> impl Fn(&str) -> Option<String> {
        move |key| {
            pairs
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
        }
    }

    #[test]
    fn ambient_identity_defaults_to_the_operator_and_its_root_token() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        let (caller, token) = ambient_identity(&layout, env_of(vec![])).unwrap();
        assert_eq!(caller, OPERATOR);
        assert_eq!(token, layout.auth_token().unwrap());
    }

    /// The production behaviour TKT-182 must not break: a real agent session
    /// still speaks as its rat, with the token the supervisor handed it.
    #[test]
    fn ambient_identity_honours_an_agent_session() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());

        let (caller, token) =
            ambient_identity(&layout, env_of(vec![("RK_AGENT", "Provolone-2")])).unwrap();
        assert_eq!(caller, "Provolone-2");
        assert_eq!(token, layout.agent_auth_token("Provolone-2").unwrap());

        // An explicit RK_AUTH_TOKEN wins, so an agent never reads the
        // operator token off disk.
        let (caller, token) = ambient_identity(
            &layout,
            env_of(vec![
                ("RK_AGENT", "Provolone-2"),
                ("RK_AUTH_TOKEN", "handed-down"),
            ]),
        )
        .unwrap();
        assert_eq!(caller, "Provolone-2");
        assert_eq!(token, "handed-down");
    }

    /// The fix itself: an explicitly named caller is unmoved by the very
    /// environment that made `cargo test --workspace` fail inside a rat.
    #[test]
    fn explicit_callers_ignore_the_ambient_agent_environment() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        let rat_env = env_of(vec![
            ("RK_AGENT", "Provolone-2"),
            ("RK_AUTH_TOKEN", "live-fleet"),
        ]);

        // What `Client::connect` would have picked up...
        let (ambient_caller, ambient_token) = ambient_identity(&layout, rat_env).unwrap();
        assert_eq!(ambient_caller, "Provolone-2");
        assert_eq!(ambient_token, "live-fleet");

        // ...versus what `connect_as_operator` binds, derived from the layout
        // under test and nothing else.
        assert_eq!(
            token_for(&layout, OPERATOR).unwrap(),
            layout.auth_token().unwrap()
        );
        assert_ne!(token_for(&layout, OPERATOR).unwrap(), ambient_token);
    }

    #[test]
    fn explicit_agent_callers_derive_their_token_from_the_layout() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        assert_eq!(
            token_for(&layout, "Whisker").unwrap(),
            layout.agent_auth_token("Whisker").unwrap()
        );
    }
}

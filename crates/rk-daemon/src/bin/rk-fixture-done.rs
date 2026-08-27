//! `rk done` for the fake harness — a test fixture, not a shipped command.
//!
//! Integration tests model a rat with a bash script in `RK_FAKE_HARNESS_CMD`,
//! and since TKT-175 a generation that never writes a `task_done` publishes as
//! a FAILURE. So a fixture that means to model a rat which *finished* has to
//! declare done the way a real primed rat does: write the tuple, then let the
//! harness report the turn.
//!
//! A fixture cannot do this from the test process. Exact tuple attribution
//! requires the spawn id that does not exist until after dispatch, and a
//! workflow `for_each` fan-out names its rats itself, so
//! there is no point at which the test knows who to write it for. It has to
//! come from inside the fake — which means a process on the far side of the
//! daemon socket, which means a binary. `env!("CARGO_BIN_EXE_rk-fixture-done")`
//! resolves it for `crates/rk-daemon/tests/*` because it lives in this package;
//! `rk` itself does not, being in `rk-cli` (which the rk-cli tests use instead).
//!
//! Kept behind the `test-fixtures` feature so it is not part of a normal build.
//! Deliberately a near-copy of `rk-cli`'s `done` rather than a shared helper:
//! the fixture's job is to be an INDEPENDENT witness that the supervisor's gate
//! reads a plain tuple, and sharing code with the thing under test would let
//! the two drift into agreement about a wrong payload shape.

use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::json;
use std::time::Duration;

const CONNECT_ATTEMPTS: usize = 50;
const CONNECT_RETRY_DELAY: Duration = Duration::from_millis(20);

fn env(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!("rk-fixture-done: {key} is not set"))
}

async fn connect_with_retry(layout: &Layout) -> rk_core::Result<Client> {
    for attempt in 1..=CONNECT_ATTEMPTS {
        match Client::connect(layout).await {
            Ok(client) => return Ok(client),
            Err(error) => {
                if !matches!(error, rk_core::Error::DaemonNotRunning(_))
                    || attempt == CONNECT_ATTEMPTS
                {
                    return Err(error);
                }
                tokio::time::sleep(CONNECT_RETRY_DELAY).await;
            }
        }
    }
    unreachable!("CONNECT_ATTEMPTS is non-zero")
}

#[tokio::main]
async fn main() {
    let layout = Layout::at(env("RK_HOME"));
    let mut client = connect_with_retry(&layout)
        .await
        .expect("rk-fixture-done: no daemon on the socket");
    client
        .call(
            "space.out",
            json!({
                "category": "event",
                "scope": env("RK_REPO"),
                "identity": "task_done",
                "payload": {
                    "task": env("RK_TASK"),
                    // Display identity; exact attribution uses `spawn` below.
                    "agent": env("RK_AGENT"),
                    // Exact generation join key. Missing RK_SPAWN is a broken
                    // fixture environment, never a name/time fallback.
                    "spawn": env("RK_SPAWN"),
                    "branch": std::env::var("RK_BRANCH").ok(),
                    "summary": std::env::args().nth(1).unwrap_or_else(|| "done".into()),
                },
            }),
        )
        .await
        .expect("rk-fixture-done: space.out failed");
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;

    #[tokio::test]
    async fn connect_retries_until_the_existing_daemon_socket_is_ready() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        let socket = layout.socket_path();
        let listener = tokio::spawn(async move {
            tokio::time::sleep(CONNECT_RETRY_DELAY * 2).await;
            let listener = UnixListener::bind(socket).unwrap();
            listener.accept().await.unwrap();
        });

        let client = connect_with_retry(&layout).await.unwrap();

        drop(client);
        listener.await.unwrap();
    }

    #[tokio::test]
    async fn connect_retry_stays_bounded_when_the_socket_never_appears() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        let upper_bound = CONNECT_RETRY_DELAY * CONNECT_ATTEMPTS as u32 * 2;

        let result = tokio::time::timeout(upper_bound, connect_with_retry(&layout))
            .await
            .expect("fixture connection retry exceeded its bounded policy");

        assert!(result.is_err());
    }
}

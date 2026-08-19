//! `rk done` for the fake harness — a test fixture, not a shipped command.
//!
//! Integration tests model a rat with a bash script in `RK_FAKE_HARNESS_CMD`,
//! and since TKT-175 a generation that never writes a `task_done` publishes as
//! a FAILURE. So a fixture that means to model a rat which *finished* has to
//! declare done the way a real primed rat does: write the tuple, then let the
//! harness report the turn.
//!
//! A fixture cannot do this from the test process. `Pattern::for_agent_since`
//! bounds the lookup to a generation whose agent name does not exist until
//! after the spawn, and a workflow `for_each` fan-out names its rats itself, so
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

fn env(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!("rk-fixture-done: {key} is not set"))
}

#[tokio::main]
async fn main() {
    let layout = Layout::at(env("RK_HOME"));
    let mut client = Client::connect(&layout)
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
                    // The field `Pattern::for_agent_since` searches for.
                    // Everything else here is dressing.
                    "agent": env("RK_AGENT"),
                    // `Pattern::for_spawn`'s join key (C1/S3a, docs/2026-08-17-
                    // tkt-c1-generation-identity.md). Optional: only set once
                    // the daemon mints RK_SPAWN (C6), so a fixture run against
                    // an unmigrated daemon still falls back to the name+floor
                    // predicate above instead of writing a null field.
                    "spawn": std::env::var("RK_SPAWN").ok(),
                    "branch": std::env::var("RK_BRANCH").ok(),
                    "summary": std::env::args().nth(1).unwrap_or_else(|| "done".into()),
                },
            }),
        )
        .await
        .expect("rk-fixture-done: space.out failed");
}

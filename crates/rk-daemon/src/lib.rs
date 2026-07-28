//! rat-kingdom daemon: NDJSON-over-UDS server hosting the tuplespace, plus the
//! client used by `rk`.

pub mod agent_log;
pub mod agents;
pub mod client;
pub mod coordinator;
pub mod cron;
pub mod drain;
pub mod inbox;
pub mod proto;
pub mod reactor;
pub mod repos;
pub mod scheduler;
pub mod server;
pub mod supervisor;
pub mod sync;
pub mod tickets;
pub mod workflow_exec;

pub use client::{Client, WatchStream};
pub use server::Daemon;

#[cfg(test)]
mod tests {
    use super::*;
    use rk_core::paths::Layout;
    use crate::proto::{Request, Response};
    use serde_json::json;
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixStream;

    async fn start_daemon() -> (
        tempfile::TempDir,
        Layout,
        tokio::task::JoinHandle<rk_core::Result<()>>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
        let handle = tokio::spawn(daemon.run());
        (dir, layout, handle)
    }

    /// Always an explicit operator connection: an ambient `RK_AGENT` (every
    /// test run inside a supervised rat has one) would otherwise make these
    /// tests speak as that rat and be refused the operator-only methods.
    async fn connect(layout: &Layout) -> Client {
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            if let Ok(c) = Client::connect_as_operator(layout).await {
                return c;
            }
        }
        panic!("daemon did not come up");
    }

    #[tokio::test]
    async fn ping_status_stop_round_trip() {
        let (_dir, layout, handle) = start_daemon().await;
        let mut client = connect(&layout).await;

        let pong = client.call("ping", json!({})).await.unwrap();
        assert_eq!(pong, json!("pong"));

        let status = client.call("status", json!({})).await.unwrap();
        assert_eq!(status["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(status["castle"], "test-castle");

        assert!(client.call("nope", json!({})).await.is_err());

        client.call("stop", json!({})).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("daemon did not stop")
            .unwrap();
        assert!(result.is_ok());
        assert!(!layout.socket_path().exists());
    }

    #[tokio::test]
    async fn agent_rpc_is_authenticated_and_instance_scoped() {
        let (_dir, layout, handle) = start_daemon().await;
        let agent_token = layout.agent_auth_token("Whisker").unwrap();
        let mut operator = connect(&layout).await;

        let mut stream = BufReader::new(UnixStream::connect(layout.socket_path()).await.unwrap());
        let forbidden = Request {
            id: "1".into(),
            method: "space.out".into(),
            auth: agent_token.clone(),
            caller: "Whisker".into(),
            params: json!({
                "category": "need",
                "scope": "repo",
                "identity": "help",
                "instance": "another-agent"
            }),
        };
        let mut line = serde_json::to_vec(&forbidden).unwrap();
        line.push(b'\n');
        stream.get_mut().write_all(&line).await.unwrap();
        let mut response = String::new();
        stream.read_line(&mut response).await.unwrap();
        let decoded: Response = serde_json::from_str(&response).unwrap();
        assert_eq!(decoded.error.unwrap().code, crate::proto::codes::FORBIDDEN);

        let denied_task = Request {
            id: "1-task".into(),
            method: "space.out".into(),
            auth: agent_token.clone(),
            caller: "Whisker".into(),
            params: json!({
                "category": "task",
                "scope": "repo",
                "identity": "operator-work"
            }),
        };
        let mut line = serde_json::to_vec(&denied_task).unwrap();
        line.push(b'\n');
        stream.get_mut().write_all(&line).await.unwrap();
        response.clear();
        stream.read_line(&mut response).await.unwrap();
        let decoded: Response = serde_json::from_str(&response).unwrap();
        assert_eq!(decoded.error.unwrap().code, crate::proto::codes::FORBIDDEN);

        let denied_event = Request {
            id: "1-event".into(),
            method: "space.out".into(),
            auth: agent_token.clone(),
            caller: "Whisker".into(),
            params: json!({
                "category": "event",
                "scope": "repo",
                "identity": "workflow_approval",
                "payload": {"instance": "wf-other", "approved": true}
            }),
        };
        let mut line = serde_json::to_vec(&denied_event).unwrap();
        line.push(b'\n');
        stream.get_mut().write_all(&line).await.unwrap();
        response.clear();
        stream.read_line(&mut response).await.unwrap();
        let decoded: Response = serde_json::from_str(&response).unwrap();
        assert_eq!(decoded.error.unwrap().code, crate::proto::codes::FORBIDDEN);

        for (request_id, method, params) in [
            (
                "1-update",
                "ticket.update",
                json!({"id": "TKT-01ARZ3NDEKTSV4RRFFQ69G5FAV", "status": "done"}),
            ),
            (
                "1-dep",
                "ticket.dep",
                json!({"id": "TKT-01ARZ3NDEKTSV4RRFFQ69G5FAV", "dep": "TKT-01ARZ3NDEKTSV4RRFFQ69G5FAV"}),
            ),
        ] {
            let denied = Request {
                id: request_id.into(),
                method: method.into(),
                auth: agent_token.clone(),
                caller: "Whisker".into(),
                params,
            };
            let mut line = serde_json::to_vec(&denied).unwrap();
            line.push(b'\n');
            stream.get_mut().write_all(&line).await.unwrap();
            response.clear();
            stream.read_line(&mut response).await.unwrap();
            let decoded: Response = serde_json::from_str(&response).unwrap();
            assert_eq!(decoded.error.unwrap().code, crate::proto::codes::FORBIDDEN);
        }

        let allowed = Request {
            id: "2".into(),
            method: "space.out".into(),
            auth: agent_token.clone(),
            caller: "Whisker".into(),
            params: json!({"category": "need", "scope": "repo", "identity": "help"}),
        };
        let mut line = serde_json::to_vec(&allowed).unwrap();
        line.push(b'\n');
        stream.get_mut().write_all(&line).await.unwrap();
        response.clear();
        stream.read_line(&mut response).await.unwrap();
        let decoded: Response = serde_json::from_str(&response).unwrap();
        assert!(decoded.error.is_none());

        assert_eq!(operator.call("ping", json!({})).await.unwrap(), json!("pong"));
        operator.call("stop", json!({})).await.unwrap();
        handle.await.unwrap().unwrap();
    }

    /// TKT-182: an operator-only method must succeed on a connection whose
    /// caller is named by the code, whatever `RK_AGENT` says about the process
    /// running the test — and must still be refused for a real agent caller.
    ///
    /// This is the end-to-end shape of the bug: every test above reaches the
    /// daemon through `connect`, which used to resolve identity from the
    /// environment. Inside a rat that env names the rat, so the whole
    /// daemon-backed suite failed with `forbidden: <Agent> is not authorized`
    /// — and `cargo test --workspace`, the command the completion protocol
    /// tells every rat to verify with, could only pass in an operator shell.
    #[tokio::test]
    async fn explicit_callers_decide_authority_not_the_environment() {
        let (_dir, layout, _handle) = start_daemon().await;
        let snapshot_of = json!({"repo": "myrepo", "instance": "wf-1"});

        // `coordinator.snapshot` is on Server::authorized's operator-only list.
        let mut operator = connect(&layout).await;
        let snapshot = operator
            .call("coordinator.snapshot", snapshot_of.clone())
            .await
            .unwrap();
        assert!(snapshot["snapshot"]["workflows"].is_array());

        // Production semantics are untouched: naming an agent caller still
        // authenticates it and still refuses it the operator-only method.
        let mut agent = Client::connect_as(&layout, "Whisker").await.unwrap();
        let err = agent
            .call("coordinator.snapshot", snapshot_of)
            .await
            .expect_err("an agent must not read the coordinator snapshot");
        assert!(
            err.to_string().contains("forbidden"),
            "expected a forbidden error, got: {err}"
        );
        // ...while a method agents *are* allowed still works on that same
        // connection, so the refusal above is about authority, not a broken
        // token.
        assert_eq!(agent.call("ping", json!({})).await.unwrap(), json!("pong"));
    }

    #[tokio::test]
    async fn space_out_scan_take_over_the_wire() {
        let (_dir, layout, _handle) = start_daemon().await;
        let mut client = connect(&layout).await;

        let written = client
            .call(
                "space.out",
                json!({
                    "category": "event",
                    "scope": "myrepo",
                    "identity": "task_done",
                    "payload": {"agent": "Whisker"},
                }),
            )
            .await
            .unwrap();
        assert_eq!(written["written"], true);

        let scanned = client
            .call(
                "space.scan",
                json!({"category": "event", "scope": "myrepo"}),
            )
            .await
            .unwrap();
        assert_eq!(scanned["tuples"].as_array().unwrap().len(), 1);
        // Instance defaulted to the castle name.
        assert_eq!(scanned["tuples"][0]["instance"], "test-castle");

        let taken = client
            .call(
                "space.take",
                json!({"category": "event", "identity": "task_done", "timeout_ms": 500}),
            )
            .await
            .unwrap();
        assert_eq!(taken["tuple"]["payload"]["agent"], "Whisker");

        let empty = client
            .call("space.scan", json!({"category": "event"}))
            .await
            .unwrap();
        assert_eq!(empty["tuples"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn blocked_take_over_the_wire_wakes_on_out() {
        let (_dir, layout, _handle) = start_daemon().await;
        let mut taker = connect(&layout).await;
        let mut writer = connect(&layout).await;

        let take_task = tokio::spawn(async move {
            taker
                .call(
                    "space.take",
                    json!({"category": "need", "identity": "help", "timeout_ms": 5000}),
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        writer
            .call(
                "space.out",
                json!({"category": "need", "scope": "myrepo", "identity": "help"}),
            )
            .await
            .unwrap();
        let taken = take_task.await.unwrap().unwrap();
        assert_eq!(taken["tuple"]["identity"], "help");
    }

    #[tokio::test]
    async fn watch_streams_matching_tuples() {
        let (_dir, layout, _handle) = start_daemon().await;
        let watcher = connect(&layout).await;
        let mut writer = connect(&layout).await;

        let mut stream = watcher.watch(json!({"scope": "watched"})).await.unwrap();

        writer
            .call(
                "space.out",
                json!({"category": "event", "scope": "ignored", "identity": "x"}),
            )
            .await
            .unwrap();
        writer
            .call(
                "space.out",
                json!({"category": "event", "scope": "watched", "identity": "y"}),
            )
            .await
            .unwrap();

        let note = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("watch timed out")
            .unwrap()
            .expect("stream closed");
        assert_eq!(note["method"], "tuple");
        assert_eq!(note["params"]["scope"], "watched");
        assert_eq!(note["params"]["identity"], "y");
    }

    #[tokio::test]
    async fn inbox_aggregates_and_ranks_over_the_wire() {
        let (_dir, layout, _handle) = start_daemon().await;
        let mut client = connect(&layout).await;

        // A budget-exceeded obstacle and a plain need, written straight to the
        // space, are the two attention items with no agents/workflows around.
        client
            .call(
                "space.out",
                json!({
                    "category": "obstacle",
                    "scope": "myrepo",
                    "identity": "Nibbles",
                    "payload": {"type": "budget_exceeded", "cost_usd": 3.5, "tokens": 800000},
                }),
            )
            .await
            .unwrap();
        client
            .call(
                "space.out",
                json!({
                    "category": "need",
                    "scope": "myrepo",
                    "identity": "Scamper",
                    "payload": {"text": "need a reviewer"},
                }),
            )
            .await
            .unwrap();

        let inbox = client.call("inbox.list", json!({})).await.unwrap();
        let items = inbox["items"].as_array().unwrap();
        assert_eq!(items.len(), 2);
        // Budget obstacle outranks the need and carries its resolving command.
        assert_eq!(items[0]["kind"], "obstacle");
        assert_eq!(items[0]["action"], "rk status Nibbles");
        assert_eq!(items[1]["kind"], "need");
    }

    /// TKT-167: an open ballot reaches the operator over the wire, tallied
    /// against the reactor's own configured quorum. Without this the whole norms
    /// program is invisible — the only endorser who is always reachable never
    /// learns a vote was open, and nothing else in the fleet announces one.
    ///
    /// The ballot below is written Ephemeral (`ttl_secs`) on purpose even though
    /// TKT-168 made that the non-default: it is the legacy shape still sitting in
    /// live spaces, and the row has to keep rendering for it. The durable shape
    /// is pinned separately by `a_ballot_written_without_a_window_is_durable`.
    #[tokio::test]
    async fn open_suggestion_surfaces_in_the_inbox_over_the_wire() {
        let (_dir, layout, _handle) = start_daemon().await;
        let mut client = connect(&layout).await;

        // A system-scope Suggestion authored by the proposing agent, carrying the
        // voting window `rk suggest` used to apply by default (TKT-168 dropped
        // it; `--ttl` and legacy tuples still produce this shape).
        client
            .call(
                "space.out",
                json!({
                    "category": "suggestion",
                    "scope": "system",
                    "identity": "sug-8nsqa4132x",
                    "instance": "rat-28",
                    "payload": {"agent": "rat-28", "text": "a pre-existing failure is a ticket"},
                    "ttl_secs": 86400,
                }),
            )
            .await
            .unwrap();
        // ... and what `rk endorse` writes: one vote from a distinct agent.
        client
            .call(
                "space.out",
                json!({
                    "category": "endorsement",
                    "scope": "system",
                    "identity": "sug-8nsqa4132x",
                    "instance": "rat-36",
                    "payload": {"agent": "rat-36", "suggestion": "sug-8nsqa4132x"},
                    "ttl_secs": 86400,
                }),
            )
            .await
            .unwrap();

        let inbox = client.call("inbox.list", json!({})).await.unwrap();
        let items = inbox["items"].as_array().unwrap();
        assert_eq!(items.len(), 1, "{items:?}");
        assert_eq!(items[0]["kind"], "open-suggestion");
        assert_eq!(items[0]["subject"], "sug-8nsqa4132x");
        assert_eq!(items[0]["action"], "rk endorse sug-8nsqa4132x");
        // Tallied against the configured quorum, not a number invented here.
        let detail = items[0]["detail"].as_str().unwrap();
        assert!(
            detail.starts_with("1/3 endorsers (23h"),
            "unexpected detail: {detail}"
        );
        assert!(detail.contains("rat-28 proposes: a pre-existing failure"));
    }

    #[tokio::test]
    async fn ttl_writes_become_ephemeral() {
        let (_dir, layout, _handle) = start_daemon().await;
        let mut client = connect(&layout).await;
        client
            .call(
                "space.out",
                json!({
                    "category": "claim",
                    "scope": "myrepo",
                    "identity": "task-1",
                    "ttl_secs": 3600,
                }),
            )
            .await
            .unwrap();
        let scanned = client
            .call("space.scan", json!({"category": "claim"}))
            .await
            .unwrap();
        assert_eq!(scanned["tuples"][0]["lifecycle"], "ephemeral");
        assert!(scanned["tuples"][0]["expires_at"].is_string());
    }

    #[tokio::test]
    async fn coordinator_watch_replays_from_cursor_and_delivers_live_events() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
        let space = daemon.space_handle();
        let _handle = tokio::spawn(daemon.run());

        let first_cursor = space
            .out_coordinator(
                rk_core::tuple::Tuple::new(
                    rk_core::tuple::Category::Event,
                    "myrepo",
                    "workflow_state_changed",
                    "daemon",
                    json!({"instance": "wf-1", "revision": 1, "status": "running"}),
                )
                .with_lifecycle(rk_core::tuple::Lifecycle::Furniture),
            )
            .unwrap();

        space
            .out_coordinator(
                rk_core::tuple::Tuple::new(
                    rk_core::tuple::Category::Event,
                    "myrepo",
                    "workflow_state_changed",
                    "daemon",
                    json!({"instance": "wf-1", "revision": 2, "status": "completed"}),
                )
                .with_lifecycle(rk_core::tuple::Lifecycle::Furniture),
            )
            .unwrap();

        let watcher = connect(&layout).await;
        let (initial, mut stream) = watcher
            .call_then_stream(
                "coordinator.watch",
                json!({"repo": "myrepo", "instance": "wf-1", "after": first_cursor}),
            )
            .await
        .unwrap();
        assert_eq!(initial["events"].as_array().unwrap().len(), 1);
        assert_eq!(initial["events"][0]["event"]["payload"]["revision"], 2);
        assert_eq!(initial["snapshot"]["workflows"].as_array().unwrap().len(), 0);
        assert_eq!(initial["resync_required"], false);

        space
            .out_coordinator(
                rk_core::tuple::Tuple::new(
                    rk_core::tuple::Category::Event,
                    "myrepo",
                    "workflow_state_changed",
                    "daemon",
                    json!({"instance": "wf-1", "revision": 3, "status": "failed"}),
                )
                .with_lifecycle(rk_core::tuple::Lifecycle::Furniture),
            )
            .unwrap();
        let note = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("coordinator event did not arrive")
            .unwrap()
            .unwrap();
        assert_eq!(note["method"], "coordinator.event");
        assert_eq!(note["params"]["event"]["payload"]["revision"], 3);
    }
}

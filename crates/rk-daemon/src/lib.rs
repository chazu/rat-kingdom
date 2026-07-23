//! rat-kingdom daemon: NDJSON-over-UDS server hosting the tuplespace, plus the
//! client used by `rk`.

pub mod agent_log;
pub mod agents;
pub mod client;
pub mod inbox;
pub mod proto;
pub mod reactor;
pub mod repos;
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
    use serde_json::json;
    use std::time::Duration;

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

    async fn connect(layout: &Layout) -> Client {
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            if let Ok(c) = Client::connect(layout).await {
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
}

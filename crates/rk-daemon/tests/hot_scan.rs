//! `--hot`/`--top` ranked scans, end-to-end through the daemon RPC (TKT-27).
//!
//! The scoring (category_weight × recency × strength) is unit-tested in
//! rk-space; this proves the `space.scan` wiring: a plain scan still returns
//! oldest-first, `hot:true` reorders strongest-first, and `top:N` caps to the
//! strongest N — all read-only sugar over the same match predicate.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::time::Duration;

async fn connect(layout: &Layout) -> Client {
    for _ in 0..100 {
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("daemon never came up");
}

fn identities(scan: &serde_json::Value) -> Vec<String> {
    scan["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["identity"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn hot_scan_ranks_strongest_first_and_top_caps() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // A heavyweight fact and a lightweight event, written fact-first so the
    // default oldest-first order is fact-then-event and any reordering under
    // --hot is the ranker, not insertion order.
    client
        .call(
            "space.out",
            json!({
                "category": "fact",
                "scope": "myrepo",
                "identity": "heavy-fact",
                "payload": {},
            }),
        )
        .await
        .unwrap();
    // ULIDs minted in the same millisecond have undefined ordering (see
    // store.rs), so nudge the clock forward to guarantee the fact gets a
    // strictly smaller id than the event. Without this the plain oldest-first
    // assertion below is flaky.
    tokio::time::sleep(Duration::from_millis(2)).await;
    client
        .call(
            "space.out",
            json!({
                "category": "event",
                "scope": "myrepo",
                "identity": "light-event",
                "payload": {},
            }),
        )
        .await
        .unwrap();

    // Default scan: oldest-first (insertion order), untouched by ranking.
    let plain = client
        .call("space.scan", json!({"scope": "myrepo"}))
        .await
        .unwrap();
    assert_eq!(identities(&plain), vec!["heavy-fact", "light-event"]);

    // --hot: the heavier category ranks first. (Both fresh, so recency ties and
    // the category weight decides.)
    let hot = client
        .call("space.scan", json!({"scope": "myrepo", "hot": true}))
        .await
        .unwrap();
    assert_eq!(identities(&hot), vec!["heavy-fact", "light-event"]);

    // --top 1: only the single strongest match comes back.
    let top = client
        .call("space.scan", json!({"scope": "myrepo", "top": 1}))
        .await
        .unwrap();
    assert_eq!(identities(&top), vec!["heavy-fact"]);
}

//! TKT-01M04D394PQ8VS5N3V441D1MDD: a second daemon started against an
//! `RK_HOME` a live daemon already owns must refuse to bind, not contend
//! with the first over the socket and tuplespace WAL.
//!
//! The crash-recovery half of the guarantee (a killed holder's lock is
//! released with no separate stale-lock cleanup) is a real-OS-process test
//! and lives as a unit test next to `acquire_singleton_lock` in
//! `rk-daemon/src/server.rs`, where the private helper is directly callable.
//! This file proves the end-to-end wiring instead: `Daemon::run()` itself
//! refuses when another daemon already holds the lock for the same home.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use std::time::Duration;

async fn connect(layout: &Layout) -> Client {
    for _ in 0..200 {
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("daemon never came up");
}

#[tokio::test]
async fn second_daemon_against_the_same_home_is_refused() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();

    let first = Daemon::new_in_memory(layout.clone(), "test-castle-1".into()).unwrap();
    let _first_handle = tokio::spawn(first.run());
    // Prove the first daemon is actually up and serving before racing the
    // second — otherwise a fast failure could be "didn't bind yet", not
    // "correctly refused".
    let _client = connect(&layout).await;

    let second = Daemon::new_in_memory(layout.clone(), "test-castle-2".into()).unwrap();
    let err = second
        .run()
        .await
        .expect_err("a second daemon against a live home must refuse to start");
    let msg = err.to_string();
    assert!(
        msg.contains("already holds the lock"),
        "unexpected refusal message: {msg}"
    );
    assert!(
        msg.contains(&std::process::id().to_string()),
        "refusal should name the holder pid so an operator can act on it: {msg}"
    );
}

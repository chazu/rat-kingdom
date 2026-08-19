//! Pheromone evaporation + reinforcement, end-to-end through the daemon RPC.
//!
//! The strength decay and in-place refresh are unit-tested in rk-space; these
//! prove the wiring at the `space.out` boundary: evaporating categories default
//! to an Ephemeral lifetime carrying strength, and re-issuing the same trail
//! reinforces it in place rather than appending a duplicate.

mod support;

use rk_core::paths::Layout;
use rk_daemon::Daemon;
use serde_json::json;
use support::connect;

#[tokio::test]
async fn claim_reinforcement_refreshes_in_place_without_duplicating() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let claim = json!({
        "category": "claim",
        "scope": "myrepo",
        "identity": "src/lib.rs",
        "instance": "Nibbles",
        "payload": {"agent": "Nibbles"},
        "ttl_secs": 1800,
    });
    let first = client.call("space.out", claim.clone()).await.unwrap();
    let second = client.call("space.out", claim).await.unwrap();
    assert_eq!(
        first["id"], second["id"],
        "re-claiming the same area reinforces the same trail id"
    );

    let scan = client
        .call(
            "space.scan",
            json!({"category": "claim", "scope": "myrepo"}),
        )
        .await
        .unwrap();
    let claims = scan["tuples"].as_array().unwrap();
    assert_eq!(
        claims.len(),
        1,
        "reinforcement does not duplicate the claim"
    );
    // Fresh trails decay with elapsed wall-clock, so this may dip just under
    // 1.0 by the time the scan lands; assert a fresh-trail range, not exact 1.0.
    let strength = claims[0]["strength"].as_f64().unwrap();
    assert!(
        strength > 0.9 && strength <= 1.0,
        "trail carries essentially full strength, got {strength}"
    );
    assert!(
        claims[0]["expires_at"].is_string(),
        "claim is Ephemeral with a TTL"
    );

    // A different agent claiming the same area is a distinct trail, not a refresh.
    client
        .call(
            "space.out",
            json!({
                "category": "claim",
                "scope": "myrepo",
                "identity": "src/lib.rs",
                "instance": "Whisker",
                "ttl_secs": 1800,
            }),
        )
        .await
        .unwrap();
    let scan = client
        .call(
            "space.scan",
            json!({"category": "claim", "scope": "myrepo"}),
        )
        .await
        .unwrap();
    assert_eq!(
        scan["tuples"].as_array().unwrap().len(),
        2,
        "distinct agents hold distinct trails"
    );
}

#[tokio::test]
async fn obstacle_written_without_ttl_defaults_to_an_evaporating_trail() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // No lifecycle, no ttl: the daemon must still make it a strength-bearing,
    // Ephemeral trail so an abandoned obstacle evaporates instead of lingering.
    client
        .call(
            "space.out",
            json!({
                "category": "obstacle",
                "scope": "myrepo",
                "identity": "Nibbles",
                "instance": "Nibbles",
                "payload": {"text": "merge conflict"},
            }),
        )
        .await
        .unwrap();

    let scan = client
        .call("space.scan", json!({"category": "obstacle"}))
        .await
        .unwrap();
    let obstacles = scan["tuples"].as_array().unwrap();
    assert_eq!(obstacles.len(), 1);
    // Fresh trails decay with elapsed wall-clock, so this may dip just under
    // 1.0 by the time the scan lands; assert a fresh-trail range, not exact 1.0.
    let strength = obstacles[0]["strength"].as_f64().unwrap();
    assert!(
        strength > 0.9 && strength <= 1.0,
        "obstacle carries essentially full strength, got {strength}"
    );
    assert!(
        obstacles[0]["expires_at"].is_string(),
        "obstacle defaulted to an Ephemeral TTL"
    );
}

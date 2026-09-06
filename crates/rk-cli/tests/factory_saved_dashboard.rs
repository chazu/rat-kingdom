//! Saved evidence must remain renderable without a running daemon.

use serde_json::json;
use std::{fs, path::Path, process::Command};

fn render(home: &Path, snapshot: &Path, events: &Path, extra: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rk"))
        .args(["factory", "render", "--snapshot"])
        .arg(snapshot)
        .arg("--events")
        .arg(events)
        .args(extra)
        .env("RK_HOME", home)
        .env_remove("RK_AGENT")
        .env_remove("RK_AUTH_TOKEN")
        .output()
        .unwrap()
}

#[test]
fn saved_dashboard_preserves_native_evidence_without_starting_a_daemon() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("absent-daemon");
    let snapshot = temp.path().join("snapshot.json");
    let events = temp.path().join("events.json");
    let output = temp.path().join("dashboard.md");
    fs::write(&snapshot, json!({
        "schema": 1, "cursor": 42,
        "snapshot": {
            "agents": [{"name": "hidden-agent"}, {"name": "current-agent", "state": "running"}],
            "workflows": [{"id": "wf-1", "status": "running", "started_at": "2026-09-05T00:00:00Z"}],
            "tickets": [{"identity": "TKT-test", "payload": {"status": "open", "title": "pipe | <script>\nnext"}}],
            "inbox": [], "budget": {},
            "approvals": {"proposals": [{"id": "proposal-1", "status": "pending", "digest": "abc123"}], "grants": []},
            "repo_resync": {"required": true}
        }
    }).to_string()).unwrap();
    fs::write(&events, json!({
        "schema": 1, "truncated": true, "boundary": 40,
        "events": [{"cursor": 40, "kind": "hidden-event"}, {"cursor": 41, "kind": "workflow.started", "summary": "recorded event"}]
    }).to_string()).unwrap();

    let result = render(
        &home,
        &snapshot,
        &events,
        &[
            "--output",
            output.to_str().unwrap(),
            "--row-limit",
            "1",
            "--event-limit",
            "1",
        ],
    );
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let text = fs::read_to_string(&output).unwrap();
    for expected in [
        "SAVED",
        "NOT CONNECTED",
        "RESYNCING",
        "`42`",
        "boundary `40`",
        "proposal-1",
        "pending",
        "abc123",
        "current-agent",
        "2026-09-05T00:00:00Z",
        "TKT-test",
        "pipe \\| &lt;script&gt; next",
        "workflow.started",
    ] {
        assert!(text.contains(expected), "missing {expected}: {text}");
    }
    assert!(!text.contains("hidden-agent"));
    assert!(!text.contains("hidden-event"));
    assert!(!text.contains("Connection: **CONNECTED**"));
    assert!(
        !home.exists(),
        "offline presentation must not initialize daemon state"
    );
}

#[test]
fn incomplete_saved_sources_are_degraded_instead_of_empty_and_healthy() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("absent-daemon");
    let snapshot = temp.path().join("snapshot.json");
    let events = temp.path().join("events.json");
    fs::write(
        &snapshot,
        json!({"schema": 1, "cursor": 7, "snapshot": {"inbox_error": "unavailable"}}).to_string(),
    )
    .unwrap();
    fs::write(
        &events,
        json!({"schema": 1, "events": [], "boundary": null, "truncated": false}).to_string(),
    )
    .unwrap();
    let result = render(&home, &snapshot, &events, &[]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let text = String::from_utf8_lossy(&result.stdout);
    assert!(text.contains("DEGRADED"), "{text}");
    assert!(text.contains("agents unavailable"), "{text}");
    assert!(!text.contains("State: **OK**"), "{text}");
    assert!(!home.exists());
}

#[test]
fn obsolete_flattened_snapshot_is_rejected_without_daemon_startup() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("absent-daemon");
    let snapshot = temp.path().join("snapshot.json");
    let events = temp.path().join("events.json");
    fs::write(
        &snapshot,
        json!({"schema": 1, "agents": [], "cursor": "evt-42"}).to_string(),
    )
    .unwrap();
    fs::write(
        &events,
        json!({"schema": 1, "events": [], "truncated": false}).to_string(),
    )
    .unwrap();
    let result = render(&home, &snapshot, &events, &[]);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("native factory snapshot"));
    assert!(!home.exists());
}

#[test]
fn saved_render_preserves_input_evidence_and_json_provenance() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("absent-daemon");
    let snapshot = temp.path().join("snapshot.json");
    let events = temp.path().join("events.json");
    let original = json!({"schema": 1, "cursor": 9, "snapshot": {}}).to_string();
    fs::write(&snapshot, &original).unwrap();
    fs::write(
        &events,
        json!({"schema": 1, "events": [], "truncated": false, "boundary": null}).to_string(),
    )
    .unwrap();

    let refused = render(
        &home,
        &snapshot,
        &events,
        &["--output", snapshot.to_str().unwrap()],
    );
    assert!(!refused.status.success());
    assert_eq!(fs::read_to_string(&snapshot).unwrap(), original);

    let result = render(&home, &snapshot, &events, &["--json"]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let rendered: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(rendered["schema"], "factory.dashboard.v1");
    assert_eq!(rendered["source"], "saved");
    assert_eq!(rendered["snapshot"]["cursor"], 9);
    assert!(!home.exists());
}

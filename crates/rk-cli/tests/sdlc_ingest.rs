use rk_core::config::{Config, IngestSourceConfig};
use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::process::Command;
use std::time::Duration;

fn rk(layout: &Layout, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rk"))
        .args(args)
        .env("RK_HOME", layout.home())
        .env_remove("RK_AGENT")
        .env_remove("RK_AUTH_TOKEN")
        .env_remove("RK_TASK")
        .env_remove("RK_REPO")
        .env_remove("RK_ROLE")
        .env_remove("RK_BRANCH")
        .env_remove("RK_WORKTREE")
        .output()
        .unwrap()
}

async fn rk_async(layout: &Layout, args: Vec<&'static str>) -> std::process::Output {
    let layout = layout.clone();
    tokio::task::spawn_blocking(move || rk(&layout, &args))
        .await
        .unwrap()
}

fn config() -> Config {
    let mut config = Config::default();
    config.ingest.sources = vec![IngestSourceConfig {
        name: "probe".into(),
        allowed_kinds: vec!["ci_failed".into(), "ci_recovered".into()],
        ..Default::default()
    }];
    config
}

async fn start() -> (
    tempfile::TempDir,
    Layout,
    tokio::task::JoinHandle<rk_core::Result<()>>,
) {
    let dir = tempfile::tempdir().unwrap();
    let layout = Layout::at(dir.path());
    let daemon = Daemon::new(layout.clone(), &config()).unwrap();
    let handle = tokio::spawn(daemon.run());
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if Client::connect_as_operator(&layout).await.is_ok() {
            return (dir, layout, handle);
        }
    }
    panic!("daemon did not start");
}

async fn stop(layout: &Layout, handle: tokio::task::JoinHandle<rk_core::Result<()>>) {
    let mut client = Client::connect_as_operator(layout).await.unwrap();
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

fn ci_args(delivery_id: &str) -> Vec<&str> {
    ci_args_for_kind("ci_failed", delivery_id, "ci failed")
}

fn ci_args_for_kind<'a>(kind: &'a str, delivery_id: &'a str, summary: &'a str) -> Vec<&'a str> {
    vec![
        "--json",
        "ingest",
        "event",
        "--source",
        "probe",
        "--kind",
        kind,
        "--delivery-id",
        delivery_id,
        "--summary",
        summary,
        "--repo",
        "repo",
        "--branch",
        "main",
        "--workflow",
        "ci",
        "--job",
        "test",
        "--commit-sha",
        "abc123",
    ]
}

fn ci_recovered_args(delivery_id: &'static str) -> Vec<&'static str> {
    ci_args_for_kind("ci_recovered", delivery_id, "ci recovered")
}

#[tokio::test]
async fn test_ingest_event_cli_builds_canonical_ci_failed_envelope() {
    let (_dir, layout, handle) = start().await;
    let output = rk_async(&layout, ci_args("cli-build-1")).await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["accepted"], true);
    assert_eq!(value["receipt"]["delivery_id"], "cli-build-1");
    stop(&layout, handle).await;
}

#[tokio::test]
async fn test_ingest_event_cli_builds_successful_ci_recovered_envelope() {
    let (_dir, layout, handle) = start().await;
    let output = rk_async(&layout, ci_recovered_args("cli-recovered-1")).await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let state = rk_async(
        &layout,
        vec![
            "--json", "ingest", "state", "--source", "probe", "--repo", "repo",
        ],
    )
    .await;
    assert!(
        state.status.success(),
        "{}",
        String::from_utf8_lossy(&state.stderr)
    );
    let value: Value = serde_json::from_slice(&state.stdout).unwrap();
    assert_eq!(value["facts"][0]["payload"]["current"]["status"], "success");
    assert_eq!(
        value["facts"][0]["payload"]["current"]["conclusion"],
        "success"
    );
    stop(&layout, handle).await;
}

#[test]
fn test_ingest_event_cli_rejects_raw_telemetry_file_flag() {
    let dir = tempfile::tempdir().unwrap();
    let layout = Layout::at(dir.path());
    let output = rk(
        &layout,
        &[
            "ingest",
            "event",
            "--source",
            "probe",
            "--raw-telemetry-file",
            "vendor.json",
        ],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("raw-telemetry-file") || stderr.contains("unexpected"),
        "{stderr}"
    );
}

#[test]
fn test_ingest_event_cli_rejects_secret_like_attr_keys() {
    let dir = tempfile::tempdir().unwrap();
    let layout = Layout::at(dir.path());
    let mut args = ci_args("secret-attr");
    args.extend(["--attr", "api_token=redacted"]);
    let output = rk(&layout, &args);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("attribute key rejected"));
}

#[test]
fn test_ingest_event_cli_file_accepts_canonical_envelope_only() {
    let dir = tempfile::tempdir().unwrap();
    let layout = Layout::at(dir.path());
    let file = dir.path().join("vendor.json");
    std::fs::write(&file, r#"{"raw":"telemetry"}"#).unwrap();
    let output = rk(
        &layout,
        &[
            "ingest",
            "event",
            "--source",
            "probe",
            "--file",
            file.to_str().unwrap(),
        ],
    );
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("canonical SignalEnvelope"));
}

#[tokio::test]
async fn test_ingest_state_cli_calls_daemon_read_only_handler() {
    let (_dir, layout, handle) = start().await;
    let ingest = rk_async(&layout, ci_args("state-1")).await;
    assert!(
        ingest.status.success(),
        "{}",
        String::from_utf8_lossy(&ingest.stderr)
    );
    let output = rk_async(
        &layout,
        vec![
            "--json", "ingest", "state", "--source", "probe", "--repo", "repo",
        ],
    )
    .await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["facts"].as_array().unwrap().len(), 1);
    assert!(value["facts"][0]["payload"].get("payload").is_none());
    stop(&layout, handle).await;
}

#[tokio::test]
async fn test_ingest_event_cli_prints_receipt_with_global_json() {
    let (_dir, layout, handle) = start().await;
    let output = rk_async(&layout, ci_args("json-receipt-1")).await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(value["receipt"].is_object());
    assert!(value["receipt"]["semantic_state_digest"]
        .as_str()
        .unwrap()
        .starts_with("sha256:"));
    stop(&layout, handle).await;
}

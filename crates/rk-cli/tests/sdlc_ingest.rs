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
        allowed_kinds: vec![
            "ci_failed".into(),
            "ci_recovered".into(),
            "deployment_succeeded".into(),
        ],
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

async fn restart(layout: &Layout) -> tokio::task::JoinHandle<rk_core::Result<()>> {
    let daemon = Daemon::new(layout.clone(), &config()).unwrap();
    let handle = tokio::spawn(daemon.run());
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if Client::connect_as_operator(layout).await.is_ok() {
            return handle;
        }
    }
    panic!("daemon did not restart");
}

fn deployment_envelope_json(repo: &str, delivery_id: &str) -> String {
    let now = chrono::Utc::now().to_rfc3339();
    json!({
        "kind": "deployment_succeeded",
        "source": "probe",
        "delivery_id": delivery_id,
        "occurred_at": now,
        "observed_at": now,
        "correlation": {"repo": repo, "environment": "local-production", "service": "rk-pair"},
        "summary": "deploy succeeded",
        "refs": [],
        "attributes": {},
        "payload": {"type": "deployment", "environment": "local-production", "service": "rk-pair", "version": "v1"}
    })
    .to_string()
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

async fn rk_async_owned(layout: &Layout, args: Vec<String>) -> std::process::Output {
    let layout = layout.clone();
    tokio::task::spawn_blocking(move || {
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        rk(&layout, &refs)
    })
    .await
    .unwrap()
}

async fn ingest_deployment(
    scratch: &tempfile::TempDir,
    layout: &Layout,
    repo: &str,
    delivery_id: &str,
) -> std::process::Output {
    let file = scratch.path().join(format!("{delivery_id}.json"));
    std::fs::write(&file, deployment_envelope_json(repo, delivery_id)).unwrap();
    rk_async_owned(
        layout,
        vec![
            "ingest".into(),
            "event".into(),
            "--source".into(),
            "probe".into(),
            "--file".into(),
            file.to_str().unwrap().into(),
        ],
    )
    .await
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

#[tokio::test]
async fn test_ingest_state_repo_filter_matches_current_deployment_fact() {
    let (_dir, layout, handle) = start().await;
    let scratch = tempfile::tempdir().unwrap();
    let ingest = ingest_deployment(&scratch, &layout, "rat-kingdom", "deploy-1").await;
    assert!(
        ingest.status.success(),
        "{}",
        String::from_utf8_lossy(&ingest.stderr)
    );

    let output = rk_async(
        &layout,
        vec![
            "--json",
            "ingest",
            "state",
            "--source",
            "probe",
            "--environment",
            "local-production",
            "--service",
            "rk-pair",
            "--repo",
            "rat-kingdom",
        ],
    )
    .await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    let facts = value["facts"].as_array().unwrap();
    assert_eq!(
        facts.len(),
        1,
        "expected the matching deployment fact: {value}"
    );
    assert_eq!(facts[0]["payload"]["current"]["repo"], "rat-kingdom");
    assert_eq!(
        facts[0]["payload"]["current"]["environment"],
        "local-production"
    );
    assert_eq!(facts[0]["payload"]["current"]["service"], "rk-pair");
    stop(&layout, handle).await;
}

#[tokio::test]
async fn test_ingest_state_repo_filter_excludes_other_repo_deployment_fact() {
    let (_dir, layout, handle) = start().await;
    let scratch = tempfile::tempdir().unwrap();
    let ingest = ingest_deployment(&scratch, &layout, "rat-kingdom", "deploy-2").await;
    assert!(
        ingest.status.success(),
        "{}",
        String::from_utf8_lossy(&ingest.stderr)
    );

    let output = rk_async(
        &layout,
        vec![
            "--json",
            "ingest",
            "state",
            "--source",
            "probe",
            "--environment",
            "local-production",
            "--service",
            "rk-pair",
            "--repo",
            "some-other-repo",
        ],
    )
    .await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        value["facts"].as_array().unwrap().len(),
        0,
        "a differently-repo'd query must not match: {value}"
    );
    stop(&layout, handle).await;
}

#[tokio::test]
async fn test_ingest_state_deployment_query_without_repo_still_returns_current_fact() {
    let (_dir, layout, handle) = start().await;
    let scratch = tempfile::tempdir().unwrap();
    let ingest = ingest_deployment(&scratch, &layout, "rat-kingdom", "deploy-3").await;
    assert!(
        ingest.status.success(),
        "{}",
        String::from_utf8_lossy(&ingest.stderr)
    );

    let output = rk_async(
        &layout,
        vec![
            "--json",
            "ingest",
            "state",
            "--source",
            "probe",
            "--environment",
            "local-production",
            "--service",
            "rk-pair",
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
    stop(&layout, handle).await;
}

#[tokio::test]
async fn test_ingest_state_repo_filtered_deployment_fact_survives_daemon_restart() {
    let (_dir, layout, handle) = start().await;
    let scratch = tempfile::tempdir().unwrap();
    let ingest = ingest_deployment(&scratch, &layout, "rat-kingdom", "deploy-4").await;
    assert!(
        ingest.status.success(),
        "{}",
        String::from_utf8_lossy(&ingest.stderr)
    );
    stop(&layout, handle).await;

    let handle = restart(&layout).await;
    let output = rk_async(
        &layout,
        vec![
            "--json",
            "ingest",
            "state",
            "--source",
            "probe",
            "--environment",
            "local-production",
            "--service",
            "rk-pair",
            "--repo",
            "rat-kingdom",
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
    stop(&layout, handle).await;
}

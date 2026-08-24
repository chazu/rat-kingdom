//! End-to-end proof for the pre-work harness transport-outage lifecycle.
//!
//! Focused unit tests own the pure classifier, backoff, breaker, generic-
//! respawn exclusion, and inbox projection decisions. This test crosses the
//! seams they cannot: a real Claude adapter child fails before `Started`, the
//! daemon persists the typed episode, a replacement daemon resumes the same
//! generation, and the retry ceiling settles exactly once.

mod support;

use rk_core::config::SupervisorConfig;
use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use rk_ledger::Budget;
use rk_space::Space;
use serde_json::{json, Value};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

static HARNESS_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn git(dir: &Path, args: &[&str]) {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn init_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# transport fixture\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

fn executable(path: &Path, body: &str) {
    std::fs::write(path, body).unwrap();
    let mut permissions = std::fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(path, permissions).unwrap();
}

fn sweep_config() -> SupervisorConfig {
    SupervisorConfig {
        enabled: true,
        interval_secs: 1,
        stuck_after_secs: 0,
        burn_usd_per_min: 0.0,
        kill_grace_secs: 1,
        respawn_enabled: false,
        transport_retry_max_attempts: 2,
        transport_retry_backoff_secs: 0,
        transport_retry_jitter_secs: 0,
        transport_breaker_trip_threshold: 1,
        // Kept deliberately long. The test advances persisted breaker state
        // instead of racing a production clock or widening a sleep margin.
        transport_breaker_cooldown_secs: 3_600,
        ..SupervisorConfig::default()
    }
}

fn daemon(layout: &Layout) -> Daemon {
    let space = Space::open(&layout.db_path()).unwrap();
    let mut daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "claude".into(),
        Budget::default(),
        space,
    )
    .unwrap();
    daemon.set_sweep_config(sweep_config());
    daemon
}

async fn wait_for_status(
    client: &mut Client,
    name: &str,
    predicate: impl Fn(&Value) -> bool,
    description: &str,
) -> Value {
    for _ in 0..600 {
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if predicate(&status) {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {description}");
}

fn marker_count(path: &Path) -> usize {
    std::fs::read_to_string(path)
        .map(|s| s.lines().count())
        .unwrap_or(0)
}

fn backdate_retry_state(layout: &Layout, name: &str) {
    let agents_path = layout.home().join("agents.json");
    let mut agents: Value = serde_json::from_slice(&std::fs::read(&agents_path).unwrap()).unwrap();
    let agent = agents
        .get_mut(name)
        .unwrap_or_else(|| panic!("missing persisted agent {name}"));
    agent["transport_outage"]["last_failure_at"] = json!("2000-01-01T00:00:00Z");
    // Non-zero ledger values make the restart/respawn preservation assertion
    // meaningful even though a pre-work failure naturally reports no usage.
    agent["cost_usd"] = json!(7.25);
    agent["usage"] = json!({
        "input": 41,
        "output": 17,
        "cache_read": 13,
        "cache_creation": 5
    });
    std::fs::write(&agents_path, serde_json::to_vec_pretty(&agents).unwrap()).unwrap();

    let breaker_path = layout.home().join("transport_breaker.json");
    let mut breakers: Value =
        serde_json::from_slice(&std::fs::read(&breaker_path).unwrap()).unwrap();
    breakers["providers"]["claude"]["opened_at"] = json!("2000-01-01T00:00:00Z");
    std::fs::write(breaker_path, serde_json::to_vec_pretty(&breakers).unwrap()).unwrap();
}

async fn transport_rows(client: &mut Client, name: &str) -> Vec<Value> {
    let inbox = client.call("inbox.list", json!({})).await.unwrap();
    inbox["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["subject"] == name && row["kind"] == "transport-outage")
        .cloned()
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outage_retry_survives_restart_without_duplicate_launch_or_ledger_reset() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    let marker = home.path().join("claude-attempts");
    let recovered = home.path().join("provider-recovered");
    let claude = home.path().join("claude-fixture");
    executable(
        &claude,
        &format!(
            r#"#!/bin/sh
printf 'attempt\n' >> '{}'
if [ -f '{}' ]; then
  printf '%s\n' '{{"type":"system","subtype":"init","session_id":"transport-recovered"}}'
  exit 0
fi
printf '%s\n' 'TLS handshake failed: unable to get local issuer certificate' >&2
exit 1
"#,
            marker.display(),
            recovered.display()
        ),
    );
    let codex = home.path().join("codex-fixture");
    executable(
        &codex,
        "#!/bin/sh\nprintf '%s\\n' 'ordinary local fixture exit' >&2\nexit 1\n",
    );
    std::env::set_var("RK_CLAUDE_BIN", &claude);
    std::env::set_var("RK_CODEX_BIN", &codex);

    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let handle_a = tokio::spawn(daemon(&layout).run());
    let mut client = connect(&layout).await;

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "transport-restart",
                "harness": "claude"
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();
    let generation = spawned["agent"]["spawn"].clone();
    let created_at = spawned["agent"]["created_at"].clone();

    let first = wait_for_status(
        &mut client,
        &name,
        |s| s["agent"]["state"] == "failed" && s["agent"]["transport_outage"]["attempts"] == 1,
        "the first typed transport failure",
    )
    .await;
    assert_eq!(first["agent"]["transport_outage"]["provider"], "claude");
    assert_eq!(first["agent"]["transport_outage"]["class"], "certificate");
    assert_eq!(marker_count(&marker), 1);

    // Threshold 1 opens the Claude breaker immediately. Admission refusal is
    // before name/WIP/worktree allocation, while another provider remains
    // independent and is still admitted.
    let agents_before = client.call("agent.list", json!({})).await.unwrap()["agents"]
        .as_array()
        .unwrap()
        .len();
    let refused = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "must-not-allocate",
                "harness": "claude"
            }),
        )
        .await
        .expect_err("an open Claude breaker must refuse a new Claude launch");
    assert!(refused
        .to_string()
        .contains("transport circuit breaker open"));
    let agents_after = client.call("agent.list", json!({})).await.unwrap()["agents"]
        .as_array()
        .unwrap()
        .len();
    assert_eq!(
        agents_before, agents_after,
        "refusal must not allocate a row"
    );
    client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "other-provider",
                "harness": "codex"
            }),
        )
        .await
        .expect("the Codex provider must remain independent");

    assert_eq!(transport_rows(&mut client, &name).await.len(), 1);

    // Replace the daemon, then inject a due schedule using persisted state.
    // No elapsed-time margin decides whether the retry is eligible.
    handle_a.abort();
    let _ = handle_a.await;
    std::fs::remove_file(layout.pid_file()).ok();
    std::fs::remove_file(layout.socket_path()).ok();
    backdate_retry_state(&layout, &name);
    assert_eq!(marker_count(&marker), 1, "restart itself must not launch");

    let handle_b = tokio::spawn(daemon(&layout).run());
    let mut client = connect(&layout).await;
    let exhausted = wait_for_status(
        &mut client,
        &name,
        |s| {
            s["agent"]["state"] == "failed"
                && s["agent"]["transport_outage"]["attempts"] == 2
                && s["agent"]["transport_outage"]["ceiling_hit"] == true
        },
        "the resumed retry to exhaust its ceiling",
    )
    .await;

    assert_eq!(marker_count(&marker), 2, "exactly one retry must launch");
    assert_eq!(exhausted["agent"]["spawn"], generation);
    assert_eq!(exhausted["agent"]["created_at"], created_at);
    assert_eq!(exhausted["agent"]["cost_usd"], json!(7.25));
    assert_eq!(
        exhausted["agent"]["usage"],
        json!({"input":41,"output":17,"cache_read":13,"cache_creation":5})
    );
    assert_eq!(transport_rows(&mut client, &name).await.len(), 1);

    // Cross another sweep tick and prove a settled ceiling neither retries
    // nor emits a duplicate typed inbox row.
    tokio::time::sleep(Duration::from_millis(1_250)).await;
    assert_eq!(
        marker_count(&marker),
        2,
        "ceiling must stop further launches"
    );
    assert_eq!(transport_rows(&mut client, &name).await.len(), 1);

    // An operator-directed recovery trial that reaches Started clears both
    // the per-generation episode and the castle-wide breaker. A subsequent
    // fresh Claude spawn is admitted again.
    std::fs::write(&recovered, "ready\n").unwrap();
    client
        .call("agent.respawn", json!({"name": name}))
        .await
        .unwrap();
    wait_for_status(
        &mut client,
        &name,
        |s| s["agent"]["transport_outage"].is_null(),
        "Started to clear the transport episode",
    )
    .await;
    client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "provider-recovered",
                "harness": "claude"
            }),
        )
        .await
        .expect("Started proof must close the Claude breaker");

    handle_b.abort();
    let _ = handle_b.await;
    std::env::remove_var("RK_CLAUDE_BIN");
    std::env::remove_var("RK_CODEX_BIN");
}

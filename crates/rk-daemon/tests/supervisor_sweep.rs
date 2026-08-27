//! Phase 4: liveness sweep — a periodic supervisor pass that catches rats
//! budget enforcement cannot see (it only fires on Usage events).
//!
//! - STUCK: a rat that goes silent mid-run (no usage, no completion) is
//!   soft-steered, then killed after the grace window.
//! - RUNNING AWAY: a rat burning cost fast with no completion is flagged by
//!   its USD/minute burn rate even while it keeps emitting.
//!
//! Both leave obstacle tuples (`stuck` / `runaway`) for the coordinator/reactor.

use rk_core::config::SupervisorConfig;
mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use rk_ledger::Budget;
use rk_space::Space;
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

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
    std::fs::write(dir.join("f"), "x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    support::install_default_repository_policy(dir);
}

/// One fake, behaviour selected by the task carried in the prompt (the fake
/// harness exports it as `$RK_FAKE_PROMPT`). Both tests set the SAME script
/// value, so the process-global env var they share never races on content.
///
/// - `*burn*`: keep emitting usage (never silent) while burning cost fast.
/// - anything else: report started, then go silent forever — a mid-tool hang.
const COMBINED_FAKE: &str = r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"sweep-fake-1"}'
case "$RK_FAKE_PROMPT" in
  *burn*)
    for i in $(seq 1 60); do
      echo '{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"burning"}],"usage":{"input_tokens":200000,"output_tokens":40000,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}'
      sleep 0.3
    done
    ;;
  *)
    sleep 120
    ;;
esac
"#;

async fn spawn_daemon(home: &Path, cfg: SupervisorConfig) -> (Layout, Client) {
    let layout = Layout::at(home);
    let space = Space::open_in_memory().unwrap();
    // Budget stays unlimited so ONLY the sweep can flag these rats.
    let mut daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "fake".into(),
        Budget::default(),
        space,
    )
    .unwrap();
    daemon.set_sweep_config(cfg);
    tokio::spawn(daemon.run());
    let client = connect(&layout).await;
    (layout, client)
}

async fn obstacle_kinds(client: &mut Client) -> Vec<String> {
    let obstacles = client
        .call("space.scan", json!({"category": "obstacle"}))
        .await
        .unwrap();
    obstacles["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["payload"]["type"].as_str().unwrap_or("").to_string())
        .collect()
}

#[tokio::test]
async fn silent_rat_is_flagged_stuck_then_killed_after_grace() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", COMBINED_FAKE);
    // Tight thresholds so stuck->steer->kill runs in seconds: 1s of silence
    // flags it, the next sweep past the 2s grace escalates to a kill.
    let (_layout, mut client) = spawn_daemon(
        home.path(),
        SupervisorConfig {
            enabled: true,
            interval_secs: 1,
            stuck_after_secs: 1,
            burn_usd_per_min: 0.0,
            kill_grace_secs: 2,
            ..SupervisorConfig::default()
        },
    )
    .await;
    support::register_repo(&mut client, repo_dir.path()).await;

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "hung-1",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    // The sweep kills the silent rat: killed-without-completing => failed.
    let mut killed = false;
    for _ in 0..120 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if status["agent"]["state"].as_str() == Some("failed") {
            killed = true;
            break;
        }
    }
    assert!(killed, "silent rat was not swept");

    let kinds = obstacle_kinds(&mut client).await;
    assert!(
        kinds.iter().any(|k| k == "stuck"),
        "a stuck obstacle fired: {kinds:?}"
    );
}

#[tokio::test]
async fn busy_runaway_rat_is_flagged_runaway_by_burn_rate() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", COMBINED_FAKE);
    // Stuck detection off (this rat is noisy, never silent); the burn arm fires
    // once a baseline sweep establishes the USD/min delta.
    let (_layout, mut client) = spawn_daemon(
        home.path(),
        SupervisorConfig {
            enabled: true,
            interval_secs: 1,
            stuck_after_secs: 0,
            burn_usd_per_min: 1.0,
            kill_grace_secs: 1,
            ..SupervisorConfig::default()
        },
    )
    .await;
    support::register_repo(&mut client, repo_dir.path()).await;

    // Model set so pricing applies (haiku: each burst ≈ $0.40, ~$1+/sec).
    client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "burn-1",
                "harness": "fake",
                "model": "haiku",
            }),
        )
        .await
        .unwrap();

    let mut runaway = false;
    for _ in 0..120 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if obstacle_kinds(&mut client)
            .await
            .iter()
            .any(|k| k == "runaway")
        {
            runaway = true;
            break;
        }
    }
    assert!(runaway, "a runaway obstacle fired for the burning rat");
}

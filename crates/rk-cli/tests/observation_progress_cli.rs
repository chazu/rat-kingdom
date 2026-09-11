//! D1 acceptance through the real CLI and a live, isolated RK daemon.
use rk_core::{config::Config, paths::Layout};
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::{
    path::Path,
    process::{Command, Output},
    time::Duration,
};

fn cli(layout: &Layout, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rk"));
    for key in rk_core::review::STRIPPED_RK_SPAWN_ENV {
        command.env_remove(key);
    }
    command
        .env("RK_HOME", layout.home())
        .args(args)
        .output()
        .unwrap()
}

fn git(repo: &Path, args: &[&str]) {
    let output = Command::new("git")
        .current_dir(repo)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn silent_running_worker_is_detected_and_observer_restart_preserves_the_stall() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let run_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path();
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.name", "Test"]);
    git(repo, &["config", "user.email", "test@example.invalid"]);
    std::fs::create_dir(repo.join(".rk")).unwrap();
    std::fs::write(repo.join(".rk/repo.cue"), r#"repo: {
        delivery: {target: "agent-base", mode: "merge", remote: "origin", remoteBranch: "{{branch}}", deleteSource: true}
    }"#).unwrap();
    git(repo, &["add", ".rk/repo.cue"]);
    git(repo, &["commit", "-m", "test policy"]);
    // The real worker stays alive and emits nothing after initialization.
    // No language model or external provider is used.
    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"silent-worker"}'
read -r _hold
"#,
    );
    let layout = Layout::at(home.path());
    let mut config = Config::default();
    config.supervisor.enabled = false;
    config.drain.enabled = false;
    let daemon = Daemon::new(layout.clone(), &config).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Ok(client) = Client::connect_as_operator(&layout).await {
                break client;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    client
        .call("repo.add", json!({"name":"observed-repo", "path":repo}))
        .await
        .unwrap();
    let ticket = client
        .call(
            "ticket.new",
            json!({"scope":"observed-repo", "title":"silent worker"}),
        )
        .await
        .unwrap();
    let ticket_id = ticket["ticket"]["identity"].as_str().unwrap();
    let spawned = client
        .call(
            "agent.spawn",
            json!({"repo":repo,"task":ticket_id,"harness":"fake"}),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let status = client
                .call("agent.status", json!({"name":name}))
                .await
                .unwrap();
            if status["agent"]["state"] == "running"
                && status["agent"]["session_id"] == "silent-worker"
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("fixture worker must remain running");
    let run = run_dir.path().join("silent-run");
    let output = cli(
        &layout,
        &[
            "--json",
            "observe",
            "start",
            "--repo",
            "observed-repo",
            "--name",
            "silent-running-counterexample",
            "--ticket",
            ticket_id,
            "--interval",
            "1s",
            "--duration",
            "4s",
            "--progress-stall-after",
            "1s",
            "--output",
            run.to_str().unwrap(),
        ],
    );
    assert!(
        !output.status.success(),
        "silent running worker must fail observation"
    );
    let report: Value =
        serde_json::from_slice(&std::fs::read(run.join("report.json")).unwrap()).unwrap();
    assert_eq!(
        report["max_progress_stalled_tickets"],
        1,
        "report: {report}; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(report["progress_stall_episodes"], 1);
    assert_eq!(report["max_progress_unresolved_tickets"], 0);
    let status = client
        .call("agent.status", json!({"name":name}))
        .await
        .unwrap();
    assert_eq!(
        status["agent"]["state"], "running",
        "observer must not repair its subject"
    );
    client.call("status", json!({})).await.unwrap();
    // A new observer process reopens the checkpoint without resetting silence.
    let after = cli(
        &layout,
        &["--json", "observe", "sample", run.to_str().unwrap()],
    );
    assert!(
        after.status.success(),
        "{}",
        String::from_utf8_lossy(&after.stderr)
    );
    let after: Value = serde_json::from_slice(&after.stdout).unwrap();
    assert_eq!(after["metrics"]["progress_stalled_tickets"], 1);
    assert_eq!(after["metrics"]["progress_stall_episodes"], 1);
    let replay = cli(
        &layout,
        &["--json", "observe", "report", run.to_str().unwrap()],
    );
    assert!(!replay.status.success());
    let replay: Value = serde_json::from_slice(&replay.stdout).unwrap();
    assert_eq!(replay["progress_stall_episodes"], 1);
    std::fs::remove_file(run.join("collector.json")).unwrap();
    let rebuilt = cli(
        &layout,
        &["--json", "observe", "sample", run.to_str().unwrap()],
    );
    assert!(rebuilt.status.success());
    let rebuilt: Value = serde_json::from_slice(&rebuilt.stdout).unwrap();
    assert_eq!(rebuilt["metrics"]["progress_stalled_tickets"], 1);
    assert_eq!(rebuilt["metrics"]["progress_stall_episodes"], 1);
    client
        .call("agent.dismiss", json!({"name":name}))
        .await
        .unwrap();
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

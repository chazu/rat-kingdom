use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

fn git(root: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .env("LC_ALL", "C")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn repository(name: &str) -> tempfile::TempDir {
    let dir = tempfile::Builder::new().prefix(name).tempdir().unwrap();
    git(dir.path(), &["init", "-b", "main"]);
    git(dir.path(), &["config", "user.email", "test@example.com"]);
    git(dir.path(), &["config", "user.name", "Test"]);
    std::fs::write(
        dir.path().join("README.md"),
        "# Fixture\n\nVerify with `cargo test`.\n",
    )
    .unwrap();
    git(dir.path(), &["add", "."]);
    git(dir.path(), &["commit", "-m", "initial"]);
    dir
}

async fn connect(layout: &Layout) -> Client {
    for _ in 0..100 {
        if let Ok(client) = Client::connect_as_operator(layout).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("daemon did not start");
}

fn run_rk(layout: &Layout, args: &[&str]) -> std::process::Output {
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

fn successful_json(output: std::process::Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

async fn wait_for_session(layout: &Layout, id: &str, expected: &str) -> Value {
    for _ in 0..200 {
        let output = run_rk(layout, &["--json", "repo", "onboard", "status", id]);
        if output.status.success() {
            let status: Value = serde_json::from_slice(&output.stdout).unwrap();
            if status["state"] == expected {
                return status;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("session {id} did not reach {expected}");
}

fn install_fake_herdr(root: &Path) -> (PathBuf, PathBuf) {
    let bin = root.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let script = bin.join("herdr");
    let state = root.join("herdr-agent");
    let argv = root.join("herdr-start-argv");
    std::fs::write(
        &script,
        r#"#!/bin/bash
set -eu
case "${1:-} ${2:-}" in
  "status server")
    exit 0
    ;;
  "agent start")
    printf '%s' "$3" > "$RK_TEST_HERDR_STATE"
    printf '%s\n' "$*" > "$RK_TEST_HERDR_ARGV"
    exit 0
    ;;
  "api snapshot")
    name="$(cat "$RK_TEST_HERDR_STATE" 2>/dev/null || true)"
    printf '{"result":{"snapshot":{"agents":[{"name":"%s","agent_status":"working","pane_id":"pane-1"}]}}}\n' "$name"
    ;;
  "agent wait"|"agent send"|"pane send-keys"|"pane close"|"notification show")
    exit 0
    ;;
  *)
    exit 1
    ;;
esac
"#,
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    std::env::set_var("RK_TEST_HERDR_STATE", &state);
    std::env::set_var("RK_TEST_HERDR_ARGV", &argv);
    let path = std::env::var_os("PATH").unwrap_or_default();
    std::env::set_var(
        "PATH",
        format!("{}:{}", bin.display(), path.to_string_lossy()),
    );
    (state, argv)
}

const COMPLETE_AND_PROBE: &str = r#"
echo '{"type":"system","subtype":"init","session_id":"onboarding-fake"}'
read -r _first_message
printf '%s' "$RK_FAKE_SYSTEM_PROMPT" > onboarder-prime
set +e
env -u RK_AGENT -u RK_AUTH_TOKEN "$RK_TEST_RK_BIN" peers > self-elevation-result 2>&1
code=$?
set -e
printf '%s' "$code" > self-elevation-code
echo '{"type":"result","subtype":"success","is_error":false,"result":"assessment complete","session_id":"onboarding-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5}}'
"#;

const HANGING: &str = r#"
echo '{"type":"system","subtype":"init","session_id":"onboarding-hang"}'
read -r _first_message
printf '%s' "$$" > hanging-pid
sleep 300
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn onboarding_sessions_are_durable_resumable_and_capability_scoped() {
    let home = tempfile::tempdir().unwrap();
    let (_herdr_state, herdr_argv) = install_fake_herdr(home.path());
    std::env::set_var("RK_TEST_RK_BIN", env!("CARGO_BIN_EXE_rk"));
    std::env::set_var("RK_FAKE_HARNESS_CMD", COMPLETE_AND_PROBE);

    let layout = Layout::at(home.path());
    let daemon_a = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut operator = connect(&layout).await;

    // A normal terminal/headless start gets one stable session, branch and
    // isolated worktree. Re-entering by path reuses all three.
    let completed_repo = repository("completed-onboarding");
    let human_head = git(completed_repo.path(), &["rev-parse", "HEAD"]);
    let human_branch = git(completed_repo.path(), &["branch", "--show-current"]);
    let first = successful_json(run_rk(
        &layout,
        &[
            "--json",
            "repo",
            "onboard",
            "start",
            completed_repo.path().to_str().unwrap(),
            "--harness",
            "fake",
        ],
    ));
    let first_status = &first["session"];
    let completed_id = first_status["id"].as_str().unwrap().to_string();
    let completed_agent = first_status["agent"].as_str().unwrap().to_string();
    let onboarding_branch = first_status["branch"].as_str().unwrap();
    let onboarding_worktree = PathBuf::from(first_status["worktree"].as_str().unwrap());
    assert!(onboarding_branch.starts_with("onboarding/onb-"));
    assert_ne!(onboarding_worktree, completed_repo.path());
    assert!(onboarding_worktree.join(".git").exists());
    assert_eq!(
        git(completed_repo.path(), &["rev-parse", "HEAD"]),
        human_head
    );
    assert_eq!(
        git(completed_repo.path(), &["branch", "--show-current"]),
        human_branch
    );
    assert_eq!(git(completed_repo.path(), &["status", "--porcelain"]), "");

    let second = successful_json(run_rk(
        &layout,
        &[
            "--json",
            "repo",
            "onboard",
            "start",
            completed_repo.path().to_str().unwrap(),
            "--harness",
            "fake",
        ],
    ));
    assert_eq!(second["reused"], true);
    assert_eq!(second["session"]["id"], completed_id);
    assert_eq!(second["session"]["agent"], completed_agent);
    assert_eq!(second["session"]["branch"], onboarding_branch);
    assert_eq!(
        second["session"]["worktree"],
        onboarding_worktree.to_string_lossy().as_ref()
    );

    let completed = wait_for_session(&layout, &completed_id, "completed").await;
    assert_eq!(completed["agent"], completed_agent);
    let report = successful_json(run_rk(
        &layout,
        &["--json", "repo", "onboard", "report", &completed_id],
    ));
    assert_eq!(report["schema_version"], 2);
    assert_eq!(report["session"]["id"], completed_id);
    assert_eq!(report["assessment"]["schema_version"], 1);
    assert_eq!(report["agent_result"], "assessment complete");

    // The onboarder is not an ordinary rat: read-only calls and self progress
    // are available, but operator mutations are denied by the server profile.
    let mut onboarder = Client::connect_as(&layout, &completed_agent).await.unwrap();
    assert_eq!(onboarder.call("ping", json!({})).await.unwrap(), "pong");
    assert!(onboarder
        .call(
            "space.scan",
            json!({"pattern": {"scope": completed_repo.path().display().to_string()}})
        )
        .await
        .is_ok());
    assert!(onboarder
        .call(
            "repo.add",
            json!({"name": "elevated", "path": completed_repo.path()})
        )
        .await
        .is_err());
    assert!(onboarder
        .call(
            "space.out",
            json!({
                "category": "claim",
                "scope": "repo",
                "identity": "ordinary-rat-write",
            })
        )
        .await
        .is_err());

    let elevation_code =
        std::fs::read_to_string(onboarding_worktree.join("self-elevation-code")).unwrap();
    let elevation_result =
        std::fs::read_to_string(onboarding_worktree.join("self-elevation-result")).unwrap();
    assert_ne!(elevation_code, "0");
    assert!(
        elevation_result.contains("forbidden"),
        "clearing identity must stay forbidden: {elevation_result}"
    );
    let prime = std::fs::read_to_string(onboarding_worktree.join("onboarder-prime")).unwrap();
    assert!(prime.contains("forced into a read-only mode"));
    assert!(!prime.contains("Commit BEFORE you verify"));

    // Role typos and attempts to smuggle a downgraded role into the onboarding
    // RPC are rejected before a branch or agent can be created.
    assert!(operator
        .call(
            "agent.spawn",
            json!({
                "repo": completed_repo.path(),
                "task": "bad-role",
                "role": "onbaorder",
                "harness": "fake",
            })
        )
        .await
        .is_err());
    assert!(operator
        .call(
            "repo.onboard.start",
            json!({
                "target": completed_repo.path(),
                "harness": "fake",
                "role": "rat",
            })
        )
        .await
        .is_err());

    // Headless interruption and attached presentation use the same persisted
    // session/report shape. Both become orphaned on restart and resume in their
    // original isolated worktrees.
    std::env::set_var("RK_FAKE_HARNESS_CMD", HANGING);
    let headless_repo = repository("headless-onboarding");
    let headless = successful_json(run_rk(
        &layout,
        &[
            "--json",
            "repo",
            "onboard",
            "start",
            headless_repo.path().to_str().unwrap(),
            "--harness",
            "fake",
        ],
    ));
    let headless_id = headless["session"]["id"].as_str().unwrap().to_string();
    let headless_worktree = PathBuf::from(headless["session"]["worktree"].as_str().unwrap());
    for _ in 0..100 {
        if headless_worktree.join("hanging-pid").exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(headless_worktree.join("hanging-pid").exists());

    let attached_repo = repository("attached-onboarding");
    let attached = successful_json(run_rk(
        &layout,
        &[
            "--json",
            "repo",
            "onboard",
            "start",
            attached_repo.path().to_str().unwrap(),
            "--harness",
            "codex",
            "--attach",
        ],
    ));
    let attached_id = attached["session"]["id"].as_str().unwrap().to_string();
    assert_eq!(attached["session"]["attached"], true);
    assert_eq!(attached["session"]["state"], "running");
    assert!(attached["session"]["attach_target"].is_string());
    let herdr_start = std::fs::read_to_string(&herdr_argv).unwrap();
    assert!(
        herdr_start.contains("--sandbox read-only"),
        "attached onboarder did not force read-only sandbox: {herdr_start}"
    );

    // A lost CLI attachment is harmless: a fresh process sees the same state
    // and report before any daemon restart.
    drop(operator);
    let attached_status = successful_json(run_rk(
        &layout,
        &["--json", "repo", "onboard", "status", &attached_id],
    ));
    assert_eq!(attached_status["id"], attached_id);
    assert_eq!(attached_status["state"], "running");
    assert_eq!(
        successful_json(run_rk(
            &layout,
            &["--json", "repo", "onboard", "report", &attached_id,],
        ))["schema_version"],
        report["schema_version"]
    );

    let mut stopper = connect(&layout).await;
    stopper.call("stop", json!({})).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle_a)
        .await
        .expect("daemon A did not stop")
        .unwrap()
        .unwrap();

    // Simulate the narrow crash window after the supervisor journaled its
    // agent but before the session journal linked that name. Recovery matches
    // the dedicated role + stable session task instead of allocating a second
    // owner for the same branch/worktree.
    let sessions_path = home.path().join("onboarding-sessions.json");
    let mut sessions: Value =
        serde_json::from_slice(&std::fs::read(&sessions_path).unwrap()).unwrap();
    sessions[headless_id.as_str()]["agent"] = Value::Null;
    sessions[headless_id.as_str()]["state"] = json!("starting");
    std::fs::write(
        &sessions_path,
        serde_json::to_vec_pretty(&sessions).unwrap(),
    )
    .unwrap();

    let daemon_b = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle_b = tokio::spawn(daemon_b.run());
    let mut operator = connect(&layout).await;
    assert_eq!(
        wait_for_session(&layout, &headless_id, "orphaned").await["id"],
        headless_id
    );
    assert_eq!(
        wait_for_session(&layout, &attached_id, "orphaned").await["id"],
        attached_id
    );

    // Stop the pre-restart fake process after daemon B has loaded the orphaned
    // registry, then resume with a completing fake in the same worktree.
    let hanging_pid = std::fs::read_to_string(headless_worktree.join("hanging-pid")).unwrap();
    let _ = Command::new("kill").arg(hanging_pid.trim()).status();
    std::env::set_var("RK_FAKE_HARNESS_CMD", COMPLETE_AND_PROBE);
    let resumed_headless = successful_json(run_rk(
        &layout,
        &["--json", "repo", "onboard", "resume", &headless_id],
    ));
    assert_eq!(resumed_headless["session"]["id"], headless_id);
    assert_eq!(
        resumed_headless["session"]["worktree"],
        headless_worktree.to_string_lossy().as_ref()
    );
    wait_for_session(&layout, &headless_id, "completed").await;

    let resumed_attached = successful_json(run_rk(
        &layout,
        &[
            "--json",
            "repo",
            "onboard",
            "resume",
            &attached_id,
            "--attach",
        ],
    ));
    assert_eq!(resumed_attached["session"]["id"], attached_id);
    assert_eq!(resumed_attached["session"]["state"], "running");
    assert_eq!(resumed_attached["session"]["attached"], true);

    let persisted = std::fs::read_to_string(&sessions_path).unwrap();
    assert!(persisted.contains(&completed_id));
    assert!(persisted.contains(&headless_id));
    assert!(persisted.contains(&attached_id));

    operator.call("stop", json!({})).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle_b)
        .await
        .expect("daemon B did not stop")
        .unwrap()
        .unwrap();
}

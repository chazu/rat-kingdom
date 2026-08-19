use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

// A clean turn that never calls `rk done` now parks the agent as `Paused`
// (awaiting resume) rather than `Completed`, so the onboarding session would
// never reach `state: completed` and every `wait_completed` call below would
// time out. Declare done exactly the way a real primed onboarder does before
// reporting the turn — see the identical fix in `daemon_rollover.rs`.
fn complete_script() -> String {
    format!(
        r#"
echo '{{"type":"system","subtype":"init","session_id":"onboarding-activation"}}'
read -r _first_message
"{}" done "assessment complete" >/dev/null 2>&1 || true
echo '{{"type":"result","subtype":"success","is_error":false,"result":"assessment complete","session_id":"onboarding-activation","total_cost_usd":0.001,"usage":{{"input_tokens":10,"output_tokens":5}}}}'
"#,
        env!("CARGO_BIN_EXE_rk")
    )
}

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
        "# Fixture\n\nAutomation is activated only by reviewed onboarding.\n",
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

async fn wait_completed(client: &mut Client, session: &str) {
    for _ in 0..200 {
        let status = client
            .call("repo.onboard.status", json!({"session": session}))
            .await
            .unwrap();
        if status["session"]["state"] == "completed" {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("onboarding session {session} did not complete");
}

fn new_file_diff(target: &str, source: &str) -> String {
    let body = source
        .lines()
        .map(|line| format!("+{line}\n"))
        .collect::<String>();
    format!(
        "diff --git a/{target} b/{target}\nnew file mode 100644\n--- /dev/null\n+++ b/{target}\n@@ -0,0 +1,{} @@\n{body}",
        source.lines().count()
    )
}

fn automation_draft(kind: &str, action: &str, target: &str, source: &str) -> Value {
    json!({
        "kind": kind,
        "title": format!("Stage and validate {target}"),
        "evidence": ["repository onboarding assessment recommended this automation"],
        "target_path": target,
        "action": action,
        "diff": new_file_diff(target, source),
        "risk": "high",
        "verification": ["validate exact CUE schema without activation"],
    })
}

async fn start_session(client: &mut Client, repo: &Path) -> Value {
    client
        .call(
            "repo.onboard.start",
            json!({"target": repo, "harness": "fake"}),
        )
        .await
        .unwrap()
}

async fn stage(
    client: &mut Client,
    session: &str,
    kind: &str,
    action: &str,
    target: &str,
    source: &str,
) -> Value {
    let proposed = client
        .call(
            "repo.onboard.propose",
            json!({
                "session": session,
                "proposal": automation_draft(kind, action, target, source),
            }),
        )
        .await
        .unwrap()["proposal"]
        .clone();
    client
        .call(
            "repo.onboard.approve",
            json!({
                "session": session,
                "proposal": proposed["id"],
                "digest": proposed["digest"],
            }),
        )
        .await
        .unwrap();
    let applied = client
        .call(
            "repo.onboard.apply",
            json!({
                "session": session,
                "proposal": proposed["id"],
                "digest": proposed["digest"],
            }),
        )
        .await
        .unwrap();
    assert_eq!(applied["proposal"]["status"], "verified");
    assert_eq!(applied["proposal"]["validation_results"][0]["passed"], true);
    assert_eq!(
        applied["proposal"]["validation_results"][0]["target_path"],
        target
    );
    applied["proposal"].clone()
}

async fn activate(client: &mut Client, session: &str, proposal: &Value) -> rk_core::Result<Value> {
    client
        .call(
            "repo.onboard.activate",
            json!({
                "session": session,
                "proposal": proposal["id"],
                "digest": proposal["digest"],
            }),
        )
        .await
}

fn install_fake_herdr(root: &Path) -> PathBuf {
    let bin = root.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let script = bin.join("herdr");
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
    exit 0
    ;;
  "api snapshot")
    name="$(cat "$RK_TEST_HERDR_STATE" 2>/dev/null || true)"
    printf '{"result":{"snapshot":{"agents":[{"name":"%s","agent_status":"working","pane_id":"pane-activation"}]}}}\n' "$name"
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
    let state = root.join("herdr-agent");
    std::env::set_var("RK_TEST_HERDR_STATE", &state);
    let path = std::env::var_os("PATH").unwrap_or_default();
    std::env::set_var(
        "PATH",
        format!("{}:{}", bin.display(), path.to_string_lossy()),
    );
    state
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automation_activation_is_explicit_content_bound_and_restart_safe() {
    let home = tempfile::tempdir().unwrap();
    std::env::set_var("RK_FAKE_HARNESS_CMD", complete_script());
    let layout = Layout::at(home.path());
    let daemon_a = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut operator = connect(&layout).await;

    // Workflow staging and schema validation are inert. Only the later
    // activation RPC advances the registered checkout, and duplicate delivery
    // does not create another commit.
    let workflow_repo = repository("onboard-workflow");
    let workflow_start = start_session(&mut operator, workflow_repo.path()).await;
    let workflow_session = workflow_start["session"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let workflow_worktree = PathBuf::from(workflow_start["session"]["worktree"].as_str().unwrap());
    let workflow_source = r#"workflow: {
	name: "onboarded"
	description: "validated without running"
	steps: [{type: "stop", reason: "fixture"}]
}
"#;
    let workflow = stage(
        &mut operator,
        &workflow_session,
        "workflow_activation",
        "activate_workflow",
        ".rk/workflows/onboarded.cue",
        workflow_source,
    )
    .await;
    assert!(!workflow_repo
        .path()
        .join(".rk/workflows/onboarded.cue")
        .exists());
    assert!(workflow_worktree
        .join(".rk/workflows/onboarded.cue")
        .exists());
    let staged_report = operator
        .call("repo.onboard.report", json!({"session": workflow_session}))
        .await
        .unwrap();
    assert_eq!(
        staged_report["report"]["summary"]["verified"],
        json!([workflow["id"].clone()])
    );

    let activated = activate(&mut operator, &workflow_session, &workflow)
        .await
        .unwrap();
    assert_eq!(activated["proposal"]["activation"]["status"], "activated");
    assert_eq!(activated["changed"], true);
    assert!(workflow_repo
        .path()
        .join(".rk/workflows/onboarded.cue")
        .exists());
    let activated_head = git(workflow_repo.path(), &["rev-parse", "HEAD"]);
    let replay = activate(&mut operator, &workflow_session, &workflow)
        .await
        .unwrap();
    assert_eq!(replay["changed"], false);
    assert_eq!(
        git(workflow_repo.path(), &["rev-parse", "HEAD"]),
        activated_head
    );

    // A validated trigger can be explicitly refused. Refusal is durable,
    // idempotent, and does not make the auto-discovered file live.
    let trigger_repo = repository("onboard-trigger");
    let trigger_start = start_session(&mut operator, trigger_repo.path()).await;
    let trigger_session = trigger_start["session"]["id"].as_str().unwrap().to_string();
    let trigger_source = r#"triggers: [{
	name: "onboarded-trigger"
	match: {category: "event", identity: "never"}
	run: "onboarded"
	maxFires: 1
}]
"#;
    let trigger = stage(
        &mut operator,
        &trigger_session,
        "trigger_activation",
        "activate_trigger",
        ".rk/triggers.cue",
        trigger_source,
    )
    .await;
    let declined = operator
        .call(
            "repo.onboard.decline_activation",
            json!({
                "session": trigger_session,
                "proposal": trigger["id"],
                "digest": trigger["digest"],
                "reason": "continuous automation is not wanted",
            }),
        )
        .await
        .unwrap();
    assert_eq!(declined["proposal"]["activation"]["status"], "declined");
    assert!(activate(&mut operator, &trigger_session, &trigger)
        .await
        .is_err());
    assert!(!trigger_repo.path().join(".rk/triggers.cue").exists());

    // A registered base movement after staging invalidates schedule activation
    // even though the approved onboarding branch and digest are unchanged.
    let schedule_repo = repository("onboard-schedule");
    let schedule_start = start_session(&mut operator, schedule_repo.path()).await;
    let schedule_session = schedule_start["session"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let schedule_source = r#"schedules: [{
	name: "onboarded-schedule"
	cron: "@daily"
	run: "onboarded"
}]
"#;
    let schedule = stage(
        &mut operator,
        &schedule_session,
        "schedule_activation",
        "activate_schedule",
        ".rk/schedules.cue",
        schedule_source,
    )
    .await;
    std::fs::write(schedule_repo.path().join("HUMAN"), "branch moved\n").unwrap();
    git(schedule_repo.path(), &["add", "HUMAN"]);
    git(schedule_repo.path(), &["commit", "-m", "human moves base"]);
    let stale = activate(&mut operator, &schedule_session, &schedule)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        stale.contains("base branch moved after approval"),
        "{stale}"
    );
    assert!(!schedule_repo.path().join(".rk/schedules.cue").exists());
    let failed_report = operator
        .call("repo.onboard.report", json!({"session": schedule_session}))
        .await
        .unwrap();
    assert_eq!(
        failed_report["report"]["summary"]["failed"],
        json!([schedule["id"].clone()])
    );

    // A hook activation proposal validates against the same #Hook CUE schema
    // the reactor's lifecycle-hook dispatch loads, exactly like trigger and
    // schedule activation above.
    let hook_repo = repository("onboard-hook");
    let hook_start = start_session(&mut operator, hook_repo.path()).await;
    let hook_session = hook_start["session"]["id"].as_str().unwrap().to_string();
    let hook_source = r#"hooks: [{
	name: "onboarded-hook"
	events: ["branch_landed"]
	command: "/usr/local/bin/rk-hook"
}]
"#;
    let hook = stage(
        &mut operator,
        &hook_session,
        "hook_activation",
        "activate_hook",
        ".rk/hooks.cue",
        hook_source,
    )
    .await;
    assert_eq!(hook["validation_results"][0]["automation_kind"], "hook");
    assert!(!hook_repo.path().join(".rk/hooks.cue").exists());

    // Uncommitted edits in the staged worktree are not silently ignored even
    // though activation would read the approved commit rather than those
    // bytes. The operator must restore or repropose the changed content.
    let changed_repo = repository("onboard-changed-file");
    let changed_start = start_session(&mut operator, changed_repo.path()).await;
    let changed_session = changed_start["session"]["id"].as_str().unwrap().to_string();
    let changed_worktree = PathBuf::from(changed_start["session"]["worktree"].as_str().unwrap());
    let changed = stage(
        &mut operator,
        &changed_session,
        "trigger_activation",
        "activate_trigger",
        ".rk/triggers.cue",
        trigger_source,
    )
    .await;
    std::fs::write(
        changed_worktree.join(".rk/triggers.cue"),
        format!("{trigger_source}// changed after validation\n"),
    )
    .unwrap();
    let changed_error = activate(&mut operator, &changed_session, &changed)
        .await
        .unwrap_err()
        .to_string();
    assert!(changed_error.contains("dirty checkout"), "{changed_error}");
    assert!(!changed_repo.path().join(".rk/triggers.cue").exists());

    // Different repository sessions remain distinct and concurrent activation
    // delivery is serialized without cross-contaminating either checkout.
    let concurrent_a = repository("onboard-concurrent-a");
    let concurrent_b = repository("onboard-concurrent-b");
    let mut start_a = Client::connect_as_operator(&layout).await.unwrap();
    let mut start_b = Client::connect_as_operator(&layout).await.unwrap();
    let (started_a, started_b) = tokio::join!(
        start_session(&mut start_a, concurrent_a.path()),
        start_session(&mut start_b, concurrent_b.path())
    );
    let session_a = started_a["session"]["id"].as_str().unwrap().to_string();
    let session_b = started_b["session"]["id"].as_str().unwrap().to_string();
    assert_ne!(session_a, session_b);
    let proposal_a = stage(
        &mut operator,
        &session_a,
        "workflow_activation",
        "activate_workflow",
        ".rk/workflows/onboarded.cue",
        workflow_source,
    )
    .await;
    let proposal_b = stage(
        &mut operator,
        &session_b,
        "workflow_activation",
        "activate_workflow",
        ".rk/workflows/onboarded.cue",
        workflow_source,
    )
    .await;
    let mut activate_a = Client::connect_as_operator(&layout).await.unwrap();
    let mut activate_b = Client::connect_as_operator(&layout).await.unwrap();
    let (result_a, result_b) = tokio::join!(
        activate(&mut activate_a, &session_a, &proposal_a),
        activate(&mut activate_b, &session_b, &proposal_b)
    );
    assert_eq!(
        result_a.unwrap()["proposal"]["activation"]["status"],
        "activated"
    );
    assert_eq!(
        result_b.unwrap()["proposal"]["activation"]["status"],
        "activated"
    );
    assert!(concurrent_a
        .path()
        .join(".rk/workflows/onboarded.cue")
        .exists());
    assert!(concurrent_b
        .path()
        .join(".rk/workflows/onboarded.cue")
        .exists());

    // Cleanup refuses a live attachment rather than severing a long-running
    // human session.
    let _herdr_state = install_fake_herdr(home.path());
    let attached_repo = repository("onboard-attached");
    let attached = operator
        .call(
            "repo.onboard.start",
            json!({
                "target": attached_repo.path(),
                "harness": "codex",
                "attach": true,
            }),
        )
        .await
        .unwrap();
    let attached_session = attached["session"]["id"].as_str().unwrap();
    assert_eq!(attached["session"]["state"], "running");
    let cleanup_error = operator
        .call("repo.onboard.cleanup", json!({"session": attached_session}))
        .await
        .unwrap_err()
        .to_string();
    assert!(cleanup_error.contains("long-lived"), "{cleanup_error}");
    assert_eq!(
        operator
            .call("repo.onboard.status", json!({"session": attached_session}))
            .await
            .unwrap()["session"]["attach_target"],
        attached["session"]["attach_target"]
    );

    wait_completed(&mut operator, &workflow_session).await;
    operator.call("stop", json!({})).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle_a)
        .await
        .expect("daemon A did not stop")
        .unwrap()
        .unwrap();

    // Simulate the exact crash window after the registered checkout advanced
    // but before the activation result reached the session journal.
    let sessions_path = home.path().join("onboarding-sessions.json");
    let mut persisted: Value =
        serde_json::from_slice(&std::fs::read(&sessions_path).unwrap()).unwrap();
    let proposals = persisted[workflow_session.as_str()]["proposals"]
        .as_array_mut()
        .unwrap();
    let workflow_record = proposals
        .iter_mut()
        .find(|proposal| proposal["id"] == workflow["id"])
        .unwrap();
    workflow_record["activation"]["status"] = json!("activating");
    workflow_record["activation"]["completed_at"] = Value::Null;
    workflow_record["activation"]["registered_commit"] = Value::Null;
    std::fs::write(
        &sessions_path,
        serde_json::to_vec_pretty(&persisted).unwrap(),
    )
    .unwrap();

    let daemon_b = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle_b = tokio::spawn(daemon_b.run());
    let mut operator = connect(&layout).await;
    let recovered = activate(&mut operator, &workflow_session, &workflow)
        .await
        .unwrap();
    assert_eq!(recovered["proposal"]["activation"]["status"], "activated");
    assert_eq!(
        git(workflow_repo.path(), &["rev-parse", "HEAD"]),
        activated_head,
        "restart replay must not create another landing commit"
    );
    let report = operator
        .call("repo.onboard.report", json!({"session": workflow_session}))
        .await
        .unwrap();
    assert_eq!(
        report["report"]["summary"]["activated"],
        json!([workflow["id"].clone()])
    );
    assert_eq!(report["report"]["summary"]["unresolved"], json!([]));

    // Terminal cleanup removes only the clean worktree. The branch and report
    // remain durable, and duplicate cleanup delivery is a no-op.
    let cleaned = operator
        .call("repo.onboard.cleanup", json!({"session": workflow_session}))
        .await
        .unwrap();
    assert_eq!(cleaned["cleaned"], true);
    assert!(!workflow_worktree.exists());
    assert_eq!(
        git(
            workflow_repo.path(),
            &[
                "branch",
                "--list",
                workflow_start["session"]["branch"].as_str().unwrap(),
            ],
        )
        .trim(),
        format!(
            "{} {}",
            if workflow_start["session"]["branch"] == "main" {
                "*"
            } else {
                ""
            },
            workflow_start["session"]["branch"].as_str().unwrap()
        )
        .trim()
    );
    let cleanup_replay = operator
        .call("repo.onboard.cleanup", json!({"session": workflow_session}))
        .await
        .unwrap();
    assert_eq!(cleanup_replay["cleaned"], false);

    operator.call("stop", json!({})).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle_b)
        .await
        .expect("daemon B did not stop")
        .unwrap()
        .unwrap();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

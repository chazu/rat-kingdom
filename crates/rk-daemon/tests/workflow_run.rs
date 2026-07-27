//! Phase 5 end to end: a CUE-defined workflow (spawn → wait → evaluate →
//! dismiss, with an aspect and per-node agent profiles) runs against the fake
//! harness, and the runner resolves harness/model through the layered agent
//! config.

mod fixture;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

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

async fn connect(layout: &Layout) -> Client {
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

const WORKING_FAKE: &str = r#"
read -r _prompt
echo "work for $RK_TASK by $RK_AGENT (model: $RK_MODEL_MARKER)" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"wf-fake"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"did the work","session_id":"wf-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

const WORKFLOW: &str = r#"
workflow: {
    name: "build-and-check"
    params: {
        taskId: {type: "string", required: true}
    }
    agents: {
        default: {harness: "fake", model: "sonnet"}
    }
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId, description: "Do the thing for " + _input.taskId}},
        {type: "wait", timeout: "30s"},
        {type: "evaluate", expect: {is_error: false}},
        {type: "dismiss"},
    ]
    aspects: [
        {match: {type: "dismiss"}, before: [{type: "gate", gateType: "timer", duration: "1s"}]},
    ]
}
"#;

#[tokio::test]
async fn cue_workflow_runs_end_to_end_with_agent_resolution() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);

    // Definition discovered from the repo-local workflows dir.
    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("build-and-check.cue"), WORKFLOW).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    std::env::set_var("RK_MODEL_MARKER", "unset");
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let defs = client
        .call(
            "workflow.definitions",
            json!({"repo": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();
    assert!(defs["definitions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|d| d == "build-and-check"));

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "build-and-check",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"taskId": "wf-task-1"},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();
    // Aspect added the timer gate: 5 steps total.
    assert_eq!(started["instance"]["total_steps"], 5);

    let mut completed = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        match status["instance"]["status"].as_str().unwrap_or("") {
            "completed" => {
                completed = true;
                break;
            }
            "failed" => panic!("workflow failed: {}", status["instance"]["error"]),
            _ => {}
        }
    }
    assert!(completed, "workflow did not complete");

    // The coordinator view gets a durable state story from the same run: the
    // initial snapshot, step mutations, and terminal transition all carry the
    // workflow instance and a strictly increasing per-instance revision.
    let transitions = client
        .call(
            "space.scan",
            json!({"category": "event", "identity": "workflow_state_changed"}),
        )
        .await
        .unwrap();
    let transitions: Vec<_> = transitions["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["payload"]["instance"].as_str() == Some(id.as_str()))
        .collect();
    assert!(transitions.len() >= 2, "workflow transitions: {transitions:?}");
    assert_eq!(transitions[0]["payload"]["reason"], "started");
    let revisions: Vec<_> = transitions
        .iter()
        .map(|event| event["payload"]["revision"].as_u64().unwrap())
        .collect();
    let mut sorted = revisions.clone();
    sorted.sort_unstable();
    assert!(
        sorted.windows(2).all(|pair| pair[0] < pair[1]),
        "workflow revisions were not unique: {revisions:?}"
    );

    // The spawned rat resolved through agents.default (harness fake, model
    // sonnet — recorded on the agent).
    let agents = client.call("agent.list", json!({})).await.unwrap();
    let agent = &agents["agents"][0];
    assert_eq!(agent["harness"], "fake");
    assert_eq!(agent["model"], "sonnet");
    assert_eq!(agent["state"], "dismissed");

    // The dismiss step merged the rat's work into main.
    let files = Command::new("git")
        .arg("-C")
        .arg(repo_dir.path())
        .args(["ls-tree", "--name-only", "main"])
        .output()
        .unwrap();
    let listing = String::from_utf8_lossy(&files.stdout).to_string();
    assert!(
        listing.contains("work-"),
        "merged work file in main: {listing}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

// spawn → wait → evaluate → approval gate → evaluate(approved) → dismiss.
const GATED_WORKFLOW: &str = r#"
workflow: {
    name: "gated"
    params: {taskId: {type: "string", required: true}}
    agents: {default: {harness: "fake", model: "sonnet"}}
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId, description: "do " + _input.taskId}},
        {type: "wait", timeout: "30s"},
        {type: "evaluate", expect: {is_error: false}},
        {type: "gate", gateType: "approval", timeout: "30s"},
        {type: "evaluate", expect: {approved: true}},
        {type: "dismiss"},
    ]
}
"#;

/// The approval gate blocks the run until `rk approve` (here: the
/// `workflow.approve` RPC) supplies a decision for this instance; on approval
/// the branch merges. This is the safety-valve happy path.
#[tokio::test]
async fn approval_gate_blocks_until_approved_then_merges() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("gated.cue"), GATED_WORKFLOW).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    std::env::set_var("RK_MODEL_MARKER", "unset");
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "gated",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"taskId": "gated-1"},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    // Wait until the run parks at the approval gate (step index 3).
    let mut parked = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        let inst = &status["instance"];
        assert_ne!(
            inst["status"], "failed",
            "run failed before the gate: {}",
            inst["error"]
        );
        if inst["status"] == "running" && inst["current_step"] == 3 {
            parked = true;
            break;
        }
    }
    assert!(parked, "workflow never parked at the approval gate");

    // A human approves; the gate wakes and the run merges.
    client
        .call(
            "workflow.approve",
            json!({"instance": id, "approved": true, "by": "operator"}),
        )
        .await
        .unwrap();

    let mut completed = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        match status["instance"]["status"].as_str().unwrap_or("") {
            "completed" => {
                completed = true;
                break;
            }
            "failed" => panic!("workflow failed: {}", status["instance"]["error"]),
            _ => {}
        }
    }
    assert!(completed, "approved workflow did not complete");

    let files = Command::new("git")
        .arg("-C")
        .arg(repo_dir.path())
        .args(["ls-tree", "--name-only", "main"])
        .output()
        .unwrap();
    let listing = String::from_utf8_lossy(&files.stdout).to_string();
    assert!(listing.contains("work-"), "merged work in main: {listing}");

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// Rejection fails the run at the `{approved: true}` evaluate; the branch is
/// left unmerged (fail-closed veto).
#[tokio::test]
async fn approval_gate_rejection_leaves_branch_unmerged() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("gated.cue"), GATED_WORKFLOW).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    std::env::set_var("RK_MODEL_MARKER", "unset");
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "gated",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"taskId": "gated-2"},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    let mut parked = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        if status["instance"]["current_step"] == 3 && status["instance"]["status"] == "running" {
            parked = true;
            break;
        }
    }
    assert!(parked, "workflow never parked at the approval gate");

    client
        .call(
            "workflow.approve",
            json!({"instance": id, "approved": false, "by": "operator", "reason": "not yet"}),
        )
        .await
        .unwrap();

    let mut failed = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        match status["instance"]["status"].as_str().unwrap_or("") {
            "failed" => {
                failed = true;
                break;
            }
            "completed" => panic!("rejected workflow should not complete"),
            _ => {}
        }
    }
    assert!(failed, "rejected workflow did not fail");

    // The rat's work never reached main.
    let files = Command::new("git")
        .arg("-C")
        .arg(repo_dir.path())
        .args(["ls-tree", "--name-only", "main"])
        .output()
        .unwrap();
    let listing = String::from_utf8_lossy(&files.stdout).to_string();
    assert!(
        !listing.contains("work-"),
        "rejected work must not merge: {listing}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

// spawn → wait → run (the repo's real check, green) → evaluate {exit:0} → dismiss.
// The run command interpolates the active agent and asserts the rat's committed
// work file exists in the worktree — proving the command runs in that worktree's
// cwd and that {exit,stdout,stderr} lands in ctx.previousResult for the evaluate.
const RUN_GREEN_WORKFLOW: &str = r#"
workflow: {
    name: "run-green"
    params: {taskId: {type: "string", required: true}}
    agents: {default: {harness: "fake", model: "sonnet"}}
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId, description: "do " + _input.taskId}},
        {type: "wait", timeout: "30s"},
        {type: "run", command: "test -f work-{{ctx.activeAgent}}.txt", timeout: "30s"},
        {type: "evaluate", expect: {exit: 0}},
        {type: "dismiss"},
    ]
}
"#;

/// A green `run` gate (command exits 0 in the worktree) unifies with the
/// following `evaluate {expect: {exit: 0}}` and the branch merges. This is the
/// deterministic quality gate's happy path — the suite is green, so it lands.
#[tokio::test]
async fn run_step_green_check_gates_and_merges() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("run-green.cue"), RUN_GREEN_WORKFLOW).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    std::env::set_var("RK_MODEL_MARKER", "unset");
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "run-green",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"taskId": "run-green-1"},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    let mut completed = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        match status["instance"]["status"].as_str().unwrap_or("") {
            "completed" => {
                completed = true;
                break;
            }
            "failed" => panic!("green run gate failed: {}", status["instance"]["error"]),
            _ => {}
        }
    }
    assert!(completed, "green-gated workflow did not complete");

    let files = Command::new("git")
        .arg("-C")
        .arg(repo_dir.path())
        .args(["ls-tree", "--name-only", "main"])
        .output()
        .unwrap();
    let listing = String::from_utf8_lossy(&files.stdout).to_string();
    assert!(listing.contains("work-"), "green work must merge: {listing}");

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

// spawn → wait → run (red, inline expectExit gate) → dismiss (never reached).
// The command exits non-zero; the inline `expectExit: 0` fails the instance
// closed before the dismiss, so the branch is never merged.
const RUN_RED_WORKFLOW: &str = r#"
workflow: {
    name: "run-red"
    params: {taskId: {type: "string", required: true}}
    agents: {default: {harness: "fake", model: "sonnet"}}
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId, description: "do " + _input.taskId}},
        {type: "wait", timeout: "30s"},
        {type: "run", command: "echo boom >&2; exit 1", expectExit: 0, timeout: "30s"},
        {type: "dismiss"},
    ]
}
"#;

/// A red `run` gate (command exits non-zero) fails the instance closed via its
/// inline `expectExit`, so the dismiss never runs and the rat's work never
/// reaches main. This is the teeth: "the rat says it passed" cannot override
/// "the suite is red, so it does not land."
#[tokio::test]
async fn run_step_red_check_fails_closed_and_holds_branch() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("run-red.cue"), RUN_RED_WORKFLOW).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    std::env::set_var("RK_MODEL_MARKER", "unset");
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "run-red",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"taskId": "run-red-1"},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    let mut failed = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        match status["instance"]["status"].as_str().unwrap_or("") {
            "failed" => {
                let err = status["instance"]["error"].as_str().unwrap_or("");
                assert!(
                    err.contains("exited 1") && err.contains("expected 0"),
                    "expected a fail-closed run-gate error, got: {err}"
                );
                failed = true;
                break;
            }
            "completed" => panic!("red run gate must not complete"),
            _ => {}
        }
    }
    assert!(failed, "red-gated workflow did not fail closed");

    // The rat's work never reached main — the gate held the branch.
    let files = Command::new("git")
        .arg("-C")
        .arg(repo_dir.path())
        .args(["ls-tree", "--name-only", "main"])
        .output()
        .unwrap();
    let listing = String::from_utf8_lossy(&files.stdout).to_string();
    assert!(
        !listing.contains("work-"),
        "red work must not merge: {listing}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

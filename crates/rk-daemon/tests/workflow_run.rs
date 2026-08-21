//! Phase 5 end to end: a CUE-defined workflow (spawn → wait → evaluate →
//! dismiss, with an aspect and per-node agent profiles) runs against the fake
//! harness, and the runner resolves harness/model through the layered agent
//! config.

mod fixture;

mod support;

use rk_core::paths::Layout;
use rk_daemon::Daemon;
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

// These tests configure the fake harness through a process-global environment
// variable. Keep the variable stable until the daemon and its child harness
// have finished; otherwise a sibling test can remove/replace it between
// workflow.run and the spawn step, silently selecting the wrong fixture.
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
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    support::install_passing_landing_checks(repo_dir.path());

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
    assert!(
        transitions.len() >= 2,
        "workflow transitions: {transitions:?}"
    );
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
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    support::install_passing_landing_checks(repo_dir.path());

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
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    support::install_passing_landing_checks(repo_dir.path());

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
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    support::install_passing_landing_checks(repo_dir.path());

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
    assert!(
        listing.contains("work-"),
        "green work must merge: {listing}"
    );

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
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    support::install_passing_landing_checks(repo_dir.path());

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

// spawn → wait → run (a REPOSITORY-OWNED named check that fails behind a
// successful output consumer) → dismiss (never reached). The check is
// referenced by name, not inlined, so this exercises the same `.rk/checks.cue`
// path a real steward gate uses.
const RUN_MASKED_CHECK_WORKFLOW: &str = r#"
workflow: {
    name: "run-masked-check"
    params: {taskId: {type: "string", required: true}}
    agents: {default: {harness: "fake", model: "sonnet"}}
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId, description: "do " + _input.taskId}},
        {type: "wait", timeout: "30s"},
        {type: "run", check: "verify", expectExit: 0, timeout: "60s"},
        {type: "dismiss"},
    ]
}
"#;

/// The repository-owned `verify` check fails (exit 3) with its output piped
/// into a consumer that succeeds — `cat`, standing in for the `tee`/`tail`/
/// renderer shape that made Basil-10 and Cluny-10 read a red suite as green —
/// and floods well past `MAX_RUN_OUTPUT_BYTES` on the way.
///
/// TKT-01M0H5JNZQKZ35V87Q4H4N3EPH. Two things have to hold together, which is
/// why they are asserted in one run:
///
/// 1. The gate is reported the CHECK's exit status, 3, not the consumer's 0.
///    RK's own output consumer (`read_capped`) succeeds independently of the
///    child and must not launder that failure into a pass, and RK's `sh -c`
///    wrap must not defeat the `set -o pipefail` the check author wrote to
///    unmask their own pipeline — the exact remedy the completion protocol
///    tells agents to use.
/// 2. Output stays BOUNDED while that happens: ~350KB of check stdout must not
///    ride into the instance error. `check_failure_detail` keeps a 400-char
///    tail per stream, so the whole error stays small.
///
/// Deliberately asserted against absolute expectations rather than a reference
/// `sh -c` run of the same command: such an oracle agrees with RK by
/// construction and would pass even if the failure were fully masked.
#[tokio::test]
async fn run_step_fails_closed_on_a_failing_check_piped_to_a_successful_consumer() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);

    // Same shape as `support::install_passing_landing_checks`, except `verify`
    // is the masked-failure check under test: a failing stage whose status the
    // author unmasks with `set -o pipefail`, piped to a successful `cat`, and
    // emitting ~350KB so RK's bounded reader genuinely truncates.
    let rk_dir = repo_dir.path().join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(
        rk_dir.join("checks.cue"),
        r#"checks: [
    {name: "steward-protected-paths", command: "true", timeout: "30s"},
    {name: "steward-diff-scope", command: "true", timeout: "30s"},
    {name: "verify", command: "bash -c 'set -o pipefail; { seq 1 60000; exit 3; } | cat'", timeout: "60s"},
]
"#,
    )
    .unwrap();
    git(repo_dir.path(), &["add", ".rk/checks.cue"]);
    git(repo_dir.path(), &["commit", "-m", "test: register checks"]);

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(
        wf_dir.join("run-masked-check.cue"),
        RUN_MASKED_CHECK_WORKFLOW,
    )
    .unwrap();

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
                "name": "run-masked-check",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"taskId": "run-masked-check-1"},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    let mut failed = false;
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        match status["instance"]["status"].as_str().unwrap_or("") {
            "failed" => {
                let err = status["instance"]["error"].as_str().unwrap_or("");
                assert!(
                    err.contains("exited 3") && err.contains("expected 0"),
                    "the check's OWN exit 3 must reach the gate, not the consumer's 0: {err}"
                );
                // Bounded output is retained: ~350KB of check stdout, at most a
                // 400-char tail per stream in the error.
                assert!(
                    err.len() < 2_000,
                    "gate error must stay bounded, got {} bytes",
                    err.len()
                );
                failed = true;
                break;
            }
            "completed" => panic!(
                "a check that exits 3 behind a successful output consumer must not pass the gate"
            ),
            _ => {}
        }
    }
    assert!(
        failed,
        "run gate did not fail closed on the check's own exit status"
    );

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
        "work behind a masked-but-failing check must not merge: {listing}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

// spawn → wait → run (red, cargo-test-shaped failure, inline expectExit gate)
// → dismiss (never reached). Prints lines shaped like a real `cargo test`
// failure summary so the gate-failure artifact's `failing_tests` extraction
// has something real to find.
const RUN_RED_CARGO_SHAPED_WORKFLOW: &str = r#"
workflow: {
    name: "run-red-cargo-shaped"
    params: {taskId: {type: "string", required: true}}
    agents: {default: {harness: "fake", model: "sonnet"}}
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId, description: "do " + _input.taskId}},
        {type: "wait", timeout: "30s"},
        {
            type: "run"
            command: """
                echo 'test suite::flaky_test ... FAILED'
                echo 'test result: FAILED. 0 passed; 1 failed; 0 ignored'
                exit 1
                """
            expectExit: 0
            timeout: "30s"
        },
        {type: "dismiss"},
    ]
}
"#;

/// TKT-01M02AMKD24WZVVMARJPXKYKSW: a failed run gate must leave durable,
/// bounded evidence naming the failing tests — not just a composed one-line
/// instance error. `ctx.previous_result` is gone by the time a later step
/// (or a fresh `workflow.status` read after the instance has moved on) could
/// otherwise inspect it; the `(artifact, <repo>, gate-failure)` tuple is the
/// only copy that survives.
#[tokio::test]
async fn run_step_failure_persists_a_durable_gate_failure_artifact() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    support::install_passing_landing_checks(repo_dir.path());

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(
        wf_dir.join("run-red-cargo-shaped.cue"),
        RUN_RED_CARGO_SHAPED_WORKFLOW,
    )
    .unwrap();

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
                "name": "run-red-cargo-shaped",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"taskId": "run-red-cargo-shaped-1"},
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
        if status["instance"]["status"].as_str().unwrap_or("") == "failed" {
            failed = true;
            break;
        }
    }
    assert!(failed, "red-gated workflow did not fail closed");

    let scanned = client
        .call("space.scan", json!({"category": "artifact"}))
        .await
        .unwrap();
    let gate_failures: Vec<&Value> = scanned["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|t| t["identity"].as_str() == Some("gate-failure"))
        .filter(|t| t["payload"]["instance"].as_str() == Some(id.as_str()))
        .collect();
    assert_eq!(
        gate_failures.len(),
        1,
        "expected exactly one gate-failure artifact for this instance: {scanned}"
    );
    let payload = &gate_failures[0]["payload"];
    assert_eq!(payload["exit"].as_i64(), Some(1));
    assert_eq!(payload["verdict"].as_str(), Some("fail"));
    assert_eq!(payload["timed_out"].as_bool(), Some(false));
    let failing_tests: Vec<&str> = payload["failing_tests"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(failing_tests, vec!["suite::flaky_test"]);
    assert!(payload["stdout_tail"]
        .as_str()
        .unwrap()
        .contains("suite::flaky_test"));

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

// spawn → wait → run (fails once, retryOnFail:1 recovers on the second
// attempt) → dismiss.
const RUN_RETRY_RECOVERS_WORKFLOW: &str = r#"
workflow: {
    name: "run-retry-recovers"
    params: {taskId: {type: "string", required: true}}
    agents: {default: {harness: "fake", model: "sonnet"}}
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId, description: "do " + _input.taskId}},
        {type: "wait", timeout: "30s"},
        {
            type: "run"
            command: "test -f retry-marker && exit 0 || { touch retry-marker; exit 1; }"
            expectExit:   0
            retryOnFail:  1
            timeout:      "30s"
            into:         "gateResult"
        },
        {type: "dismiss"},
    ]
}
"#;

/// A check already characterized as flaky (`retryOnFail: 1`) gets one extra
/// attempt before the gate holds the branch. A transient first failure that
/// recovers on retry must (a) let the workflow complete and (b) record that a
/// retry happened, rather than silently looking like a first-try pass.
#[tokio::test]
async fn run_step_retry_on_fail_recovers_and_records_the_retry() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    support::install_passing_landing_checks(repo_dir.path());

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(
        wf_dir.join("run-retry-recovers.cue"),
        RUN_RETRY_RECOVERS_WORKFLOW,
    )
    .unwrap();

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
                "name": "run-retry-recovers",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"taskId": "run-retry-recovers-1"},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    let mut completed = false;
    let mut last_status = json!(null);
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        last_status = status.clone();
        match status["instance"]["status"].as_str().unwrap_or("") {
            "completed" => {
                completed = true;
                break;
            }
            "failed" => panic!(
                "retry should have recovered: {}",
                status["instance"]["error"]
            ),
            _ => {}
        }
    }
    assert!(
        completed,
        "retried workflow did not complete: {last_status}"
    );

    let gate_result = &last_status["instance"]["context"]["vars"]["gateResult"];
    assert_eq!(gate_result["verdict"].as_str(), Some("pass"));
    let retries = gate_result["retries"].as_array().expect("retries recorded");
    assert_eq!(
        retries.len(),
        1,
        "exactly one failed attempt before recovery"
    );
    assert_eq!(retries[0]["verdict"].as_str(), Some("fail"));

    // The final verdict passed, so no gate-failure artifact for this instance
    // — a recovered flake must not look identical to an unrecorded one.
    let scanned = client
        .call("space.scan", json!({"category": "artifact"}))
        .await
        .unwrap();
    let gate_failures = scanned["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|t| t["identity"].as_str() == Some("gate-failure"))
        .filter(|t| t["payload"]["instance"].as_str() == Some(id.as_str()))
        .count();
    assert_eq!(
        gate_failures, 0,
        "a recovered retry must not write gate-failure"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

// spawn → wait → run (sleeps past its 1s timeout, default onTimeout: "fail")
// → dismiss (never reached).
const RUN_TIMEOUT_DEFAULT_FAIL_WORKFLOW: &str = r#"
workflow: {
    name: "run-timeout-default-fail"
    params: {taskId: {type: "string", required: true}}
    agents: {default: {harness: "fake", model: "sonnet"}}
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId, description: "do " + _input.taskId}},
        {type: "wait", timeout: "30s"},
        {
            type: "run"
            command: "sleep 5"
            timeout: "1s"
        },
        {type: "dismiss"},
    ]
}
"#;

/// TKT-01M02QT9KTDY2CN6YJEVP3VCF8: a `run` step that blows its timeout under
/// the default `onTimeout: "fail"` policy must leave the same durable
/// gate-failure evidence a non-timeout fail does — not just the composed
/// instance error. Before this fix, `collect_child_output` returned the
/// timeout as an `Err` straight out of `spawn_check_child`, before
/// `run_command`'s loop (where `record_gate_failure` lives) ever saw it, so
/// this exact path left no artifact.
#[tokio::test]
async fn run_step_default_timeout_persists_a_durable_gate_failure_artifact() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    support::install_passing_landing_checks(repo_dir.path());

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(
        wf_dir.join("run-timeout-default-fail.cue"),
        RUN_TIMEOUT_DEFAULT_FAIL_WORKFLOW,
    )
    .unwrap();

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
                "name": "run-timeout-default-fail",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"taskId": "run-timeout-default-fail-1"},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    let mut failed = false;
    let mut last_status = json!(null);
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        last_status = status.clone();
        if status["instance"]["status"].as_str().unwrap_or("") == "failed" {
            failed = true;
            break;
        }
    }
    assert!(
        failed,
        "timed-out run gate did not fail closed: {last_status}"
    );
    let err = last_status["instance"]["error"].as_str().unwrap_or("");
    assert!(err.contains("timed out"), "unexpected error: {err}");

    let scanned = client
        .call("space.scan", json!({"category": "artifact"}))
        .await
        .unwrap();
    let gate_failures: Vec<&Value> = scanned["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|t| t["identity"].as_str() == Some("gate-failure"))
        .filter(|t| t["payload"]["instance"].as_str() == Some(id.as_str()))
        .collect();
    assert_eq!(
        gate_failures.len(),
        1,
        "expected exactly one gate-failure artifact for this instance: {scanned}"
    );
    let payload = &gate_failures[0]["payload"];
    assert_eq!(payload["verdict"].as_str(), Some("timeout"));
    assert_eq!(payload["timed_out"].as_bool(), Some(true));
    assert_eq!(payload["exit"].as_i64(), Some(124));
    assert!(payload["stderr_tail"]
        .as_str()
        .unwrap()
        .contains("timed out"));

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

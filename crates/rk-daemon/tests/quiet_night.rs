//! A quiet night is a normal outcome, not an instance failure (TKT-170).
//!
//! `nightly-self-improve` fans out one rat per *ready* ticket. On a night where
//! the backlog is already drained the `for_each` query matches nothing, and the
//! `wait_all`/`dismiss_all` that close the fan-out used to refuse an empty set —
//! failing the whole instance, skipping the phase after the drain, and landing
//! in `rk inbox` as an operator-attention item with nothing to attend to.
//!
//! These two tests pin the distinction the fix rests on. An empty fan-out (a
//! `for_each` ran, its query matched nothing) joins and dismisses as a no-op so
//! the night finishes; a *missing* fan-out (no `for_each` at all) is still an
//! authoring error and still fails the instance.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

fn git(dir: &Path, args: &[&str]) -> String {
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
    String::from_utf8_lossy(&out.stdout).to_string()
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

fn init_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

async fn await_terminal(client: &mut Client, id: &str) -> serde_json::Value {
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        match status["instance"]["status"].as_str().unwrap_or("") {
            "completed" | "failed" => return status["instance"].clone(),
            _ => {}
        }
    }
    panic!("instance {id} never reached a terminal status");
}

// Every spawned rat writes a distinct per-agent file (so merges never conflict)
// and reports a clean success.
const WORKING_FAKE: &str = r#"
read -r _prompt
echo "$RK_TASK by $RK_AGENT" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"quiet-fake"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"ok","session_id":"quiet-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

// The nightly-self-improve shape: groom (noMerge) -> drain fan-out -> refine
// (merge). Run against an empty ready queue so the drain phase fans out nothing.
const QUIET_WORKFLOW: &str = r#"
workflow: {
    name: "quiet-night-test"
    params: {repo: {type: "string", required: false, default: ""}}
    agents: {default: {harness: "fake", model: "sonnet"}}
    steps: [
        {type: "spawn", role: "rat", task: {title: "groom-backlog", description: "groom"}},
        {type: "wait", timeout: "60s"},
        {type: "dismiss", noMerge: true},

        {type: "for_each", query: {status: "ready", limit: 5}, role: "rat",
            task: {title: "{{item.id}}", description: "Implement {{item.title}}: {{item.body}}"}},
        {type: "wait_all", timeout: "60s"},
        {type: "evaluate", expect: {all_ok: true}},
        {type: "dismiss_all"},

        {type: "spawn", role: "rat", branch: "main", task: {title: "refine-prompts", description: "refine"}},
        {type: "wait", timeout: "60s"},
        {type: "evaluate", expect: {is_error: false}},
        {type: "dismiss"},
    ]
}
"#;

// No ready tickets: the drain phase fans out zero rats. wait_all joins the empty
// set to the vacuous aggregate (count 0, all_ok true), the evaluate passes over
// it, dismiss_all merges nothing — and the refine phase after the fan-out still
// runs, so the night completes rather than failing into rk inbox.
#[tokio::test]
async fn quiet_night_completes_and_still_runs_the_phase_after_the_drain() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("quiet-night-test.cue"), QUIET_WORKFLOW).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // Deliberately no tickets: this is the quiet night.
    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "quiet-night-test",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    let instance = await_terminal(&mut client, &id).await;
    assert_eq!(
        instance["status"], "completed",
        "an empty ticket query is a quiet night, not a failure: {}",
        instance["error"]
    );

    // The refine phase ran and merged: exactly one work file on main (groom is
    // noMerge, the drain fanned out nobody). Proves the steps AFTER the empty
    // fan-out still executed.
    let base = repo_dir.path();
    git(base, &["checkout", "main"]);
    let tracked = git(base, &["ls-files"]);
    let merged: Vec<&str> = tracked.lines().filter(|f| f.starts_with("work-")).collect();
    assert_eq!(
        merged.len(),
        1,
        "expected only the refine work file on main (groom is noMerge, drain was empty), got: {tracked}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

// A wait_all with no for_each ahead of it at all is a different thing from an
// empty fan-out — the workflow is malformed, and it must still fail loudly.
const NO_FOR_EACH_WORKFLOW: &str = r#"
workflow: {
    name: "orphan-wait-all-test"
    params: {repo: {type: "string", required: false, default: ""}}
    agents: {default: {harness: "fake", model: "sonnet"}}
    steps: [
        {type: "wait_all", timeout: "60s"},
    ]
}
"#;

#[tokio::test]
async fn wait_all_without_a_for_each_is_still_an_error() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(
        wf_dir.join("orphan-wait-all-test.cue"),
        NO_FOR_EACH_WORKFLOW,
    )
    .unwrap();

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "orphan-wait-all-test",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    let instance = await_terminal(&mut client, &id).await;
    assert_eq!(
        instance["status"], "failed",
        "a wait_all with no for_each ahead of it is an authoring error"
    );
    let error = instance["error"].as_str().unwrap_or_default();
    assert!(
        error.contains("no preceding for_each"),
        "the failure should name the missing for_each, got: {error}"
    );
}

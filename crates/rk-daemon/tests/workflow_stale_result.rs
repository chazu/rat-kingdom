//! TKT-146 regression: a workflow-spawned rat must run to completion, never be
//! dismissed one second into its task because the `wait` step matched somebody
//! else's `harness_result`.
//!
//! The failure this pins down: agent names are recycled once an old record is
//! archived, but the `harness_result` events those names are stamped into are
//! durable and outlive the record forever. A `wait` that searches only for
//! `"agent":"<name>"` therefore matched a PREDECESSOR's tuple and returned in
//! milliseconds; the `evaluate` behind it judged a stranger's work, and the
//! `dismiss` behind THAT killed a rat that had barely started (SIGTERM, so the
//! record ends `state=dismissed`, no session, zero tokens, `process exited
//! (code None) without completing`). Whole workflows reported success having
//! done nothing — nightly-self-improve was a silent no-op.
//!
//! Two tests, one per layer of the fix:
//!  1. the `wait` predicate is bounded to the waiting rat's own generation, so
//!     a stale namesake tuple cannot satisfy it even if a name IS reused;
//!  2. archiving no longer recycles the name in the first place.

mod fixture;

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

/// A rat that takes a beat before reporting. The delay is what makes the bug
/// observable: under the old code the `wait` was already satisfied by the stale
/// tuple and the `dismiss` killed this process mid-sleep, so it never got to
/// commit or report. It must be comfortably longer than the workflow's own
/// step latency and comfortably shorter than the `wait` timeout.
const SLOW_FAKE: &str = r#"
read -r _prompt
sleep 3
echo "work by $RK_AGENT" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"stale-fake"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"FRESH work by this generation","session_id":"stale-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

const WORKFLOW: &str = r#"
workflow: {
    name: "stale-result-test"
    agents: {
        default: {harness: "fake", model: "sonnet"}
    }
    steps: [
        {type: "spawn", role: "rat", task: {title: "do-the-thing", description: "Do the thing"}},
        {type: "wait", timeout: "60s"},
        {type: "evaluate", expect: {is_error: false}},
        {type: "dismiss"},
    ]
}
"#;

fn init_repo(dir: &Path) -> String {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    let wf_dir = dir.join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("stale-result-test.cue"), WORKFLOW).unwrap();
    dir.file_name().unwrap().to_string_lossy().to_string()
}

async fn run_workflow(client: &mut Client, repo_dir: &Path) -> String {
    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "stale-result-test",
                "repo": repo_dir.to_string_lossy(),
                "params": {},
            }),
        )
        .await
        .unwrap();
    started["instance"]["id"].as_str().unwrap().to_string()
}

/// Poll until the instance settles; returns its terminal status.
async fn await_instance(client: &mut Client, id: &str) -> String {
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        let s = status["instance"]["status"]
            .as_str()
            .unwrap_or("")
            .to_string();
        if s == "completed" || s == "failed" {
            return s;
        }
    }
    panic!("workflow instance {id} never settled");
}

async fn agents(client: &mut Client) -> Vec<serde_json::Value> {
    client
        .call("agent.list", json!({"include_archived": true}))
        .await
        .unwrap()["agents"]
        .as_array()
        .unwrap()
        .clone()
}

/// A durable `harness_result` left behind by an earlier rat of the same name
/// must NOT satisfy a fresh rat's `wait`. The stale tuple here is deliberately
/// a *clean* one (`is_error: false`), which is the nastiest shape: the old code
/// sailed through the `evaluate`, dismissed the live rat, and reported the
/// whole workflow completed having done nothing at all.
#[tokio::test]
async fn wait_ignores_a_predecessors_harness_result() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_name = init_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(SLOW_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // The registry is empty, so this is exactly the name the workflow's rat
    // will be given — the same collision archiving used to manufacture.
    let doomed = rk_core::names::next_name(Vec::new());

    // A predecessor's result, still in the space long after that rat is gone.
    client
        .call(
            "space.out",
            json!({
                "category": "event",
                "scope": repo_name,
                "identity": "harness_result",
                "payload": {
                    "agent": doomed,
                    "role": "rat",
                    "task": "some-ancient-task",
                    "is_error": false,
                    "result": "STALE result from a rat that is long gone",
                    "cost_usd": 0.5,
                    "tokens": 1234,
                },
            }),
        )
        .await
        .unwrap();

    let id = run_workflow(&mut client, repo_dir.path()).await;
    let status = await_instance(&mut client, &id).await;
    assert_eq!(status, "completed", "instance {id} should complete");

    let listed = agents(&mut client).await;
    assert_eq!(listed.len(), 1, "one rat should have run, got: {listed:?}");
    let rat = &listed[0];
    assert_eq!(rat["name"].as_str().unwrap(), doomed);

    // The heart of it: this rat reported its OWN result, after actually
    // working. Under the bug it was SIGTERMed ~1s in and its record read
    // `process exited (code None) without completing` with zero tokens.
    let result = rat["result"].as_str().unwrap_or("");
    assert!(
        result.contains("FRESH"),
        "the rat's own result should be recorded, got: {result:?}"
    );
    assert!(
        !result.contains("process exited"),
        "the rat was killed before completing: {result:?}"
    );
    assert!(
        rat["usage"]["output"].as_u64().unwrap_or(0) > 0,
        "a rat that ran to completion reports usage, got: {:?}",
        rat["usage"]
    );

    // And it did real work that landed: the killed rat never committed.
    git(repo_dir.path(), &["checkout", "main"]);
    let tracked = git(repo_dir.path(), &["ls-files"]);
    assert!(
        tracked.lines().any(|f| f.starts_with("work-")),
        "the rat's work should have merged into main, got: {tracked}"
    );
}

/// The root cause, one layer down: archiving a terminal record must not return
/// its name to the pool. A name is stamped into durable tuples, agent logs,
/// branches and worktree paths that outlive the record, so recycling it makes
/// every reader that keys on it ambiguous across time.
#[tokio::test]
async fn archiving_does_not_hand_a_name_to_the_next_workflow_rat() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(SLOW_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // First generation: runs, completes, is dismissed by the workflow.
    let first_id = run_workflow(&mut client, repo_dir.path()).await;
    assert_eq!(await_instance(&mut client, &first_id).await, "completed");
    let first_name = agents(&mut client).await[0]["name"]
        .as_str()
        .unwrap()
        .to_string();

    // `rk prune --all`: the record settles into the archive, out of the default
    // fleet views. Its name must stay spent.
    let archived = client
        .call("agent.archive", json!({"all": true}))
        .await
        .unwrap();
    assert_eq!(
        archived["count"].as_u64(),
        Some(1),
        "the dismissed rat should archive, got: {archived:?}"
    );

    // Second generation, same workflow, same repo.
    let second_id = run_workflow(&mut client, repo_dir.path()).await;
    assert_eq!(
        await_instance(&mut client, &second_id).await,
        "completed",
        "the second run must not be poisoned by the first rat's leftovers"
    );

    let listed = agents(&mut client).await;
    assert_eq!(listed.len(), 2, "both generations kept, got: {listed:?}");
    let second = listed
        .iter()
        .find(|a| a["name"].as_str() != Some(first_name.as_str()))
        .unwrap_or_else(|| {
            panic!("the second rat reused the archived name {first_name}: {listed:?}")
        });
    let result = second["result"].as_str().unwrap_or("");
    assert!(
        result.contains("FRESH") && !result.contains("process exited"),
        "the second rat should have run to completion, got: {result:?}"
    );
}

//! Fan-out merge end to end: a `for_each` fans out one rat per ready ticket,
//! `wait_all` joins them, then `dismiss_all` merges every rat's branch into the
//! base in one parallel sweep and clears the fan-out set. Proves the symmetric
//! close to the fan-out — where a single `dismiss` merges the one active
//! branch, `dismiss_all` merges every parked branch (TKT-7).

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

// Each rat writes a distinct per-agent file (so the merges never conflict) and
// reports a clean success.
const WORKING_FAKE: &str = r#"
read -r _prompt
echo "drained $RK_TASK by $RK_AGENT" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"drain-fake"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"drained","session_id":"drain-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

// for_each → wait_all → evaluate → dismiss_all: fan out, join, then merge every
// parked branch in parallel.
const WORKFLOW: &str = r#"
workflow: {
    name: "drain-merge-test"
    params: {
        repo: {type: "string", required: false, default: ""}
    }
    agents: {
        default: {harness: "fake", model: "sonnet"}
    }
    steps: [
        {
            type: "for_each"
            query: {status: "ready", limit: 5}
            role: "rat"
            task: {title: "{{item.id}}", description: "Implement {{item.title}}: {{item.body}}"}
        },
        {type: "wait_all", timeout: "60s"},
        {type: "evaluate", expect: {all_ok: true}},
        {type: "dismiss_all"},
    ]
}
"#;

#[tokio::test]
async fn dismiss_all_merges_every_fanout_branch() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);

    let repo_name = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("drain-merge-test.cue"), WORKFLOW).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // Two ready tickets in this repo → two fanned-out rats → two branches.
    for title in ["add caching", "fix pagination"] {
        client
            .call(
                "ticket.new",
                json!({"title": title, "body": "do it", "scope": repo_name}),
            )
            .await
            .unwrap();
    }

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "drain-merge-test",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();
    // for_each, wait_all, evaluate, dismiss_all.
    assert_eq!(started["instance"]["total_steps"], 4);

    let mut completed = false;
    for _ in 0..300 {
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

    // dismiss_all merged both rat branches into main and deleted them: the base
    // now carries each rat's work file, and only `main` remains.
    let base = repo_dir.path();
    git(base, &["checkout", "main"]);
    let tracked = git(base, &["ls-files"]);
    let merged_files: Vec<&str> = tracked
        .lines()
        .filter(|f| f.starts_with("work-"))
        .collect();
    assert_eq!(
        merged_files.len(),
        2,
        "both rats' work files should be merged into main, got: {tracked}"
    );

    let branches = git(base, &["branch", "--format=%(refname:short)"]);
    let names: Vec<&str> = branches.lines().map(str::trim).collect();
    assert_eq!(
        names,
        vec!["main"],
        "every fanned-out branch should be merged and deleted, leaving only main"
    );

    // Each merged ticket-rat closes its ticket for good.
    let mut all_closed = false;
    for _ in 0..50 {
        let tickets = client
            .call("ticket.list", json!({"scope": repo_name}))
            .await
            .unwrap();
        all_closed = tickets["tickets"]
            .as_array()
            .unwrap()
            .iter()
            .all(|t| t["payload"]["status"] == "closed");
        if all_closed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        all_closed,
        "both drained tickets should reach status closed after merge"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

//! Fan-out + parallel join end to end: `backlog-drain` enumerates ready
//! tickets, spawns one rat per ticket in parallel (`for_each`), blocks until
//! all have finished (`wait_all`), and evaluates the aggregate. Exercises the
//! new multi-active-agent path against the fake harness.

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

// Each rat writes a per-agent file and reports a clean success result.
const WORKING_FAKE: &str = r#"
read -r _prompt
echo "drained $RK_TASK by $RK_AGENT" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"drain-fake"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"drained","session_id":"drain-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

const WORKFLOW: &str = r#"
workflow: {
    name: "drain-test"
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
    ]
}
"#;

#[tokio::test]
async fn backlog_drain_fans_out_and_joins() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);

    // Tickets are scoped by repo *name* — the worktree dir basename, which is
    // what the fan-out query resolves the repo path to.
    let repo_name = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("drain-test.cue"), WORKFLOW).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // Two ready tickets in this repo, plus one in another scope that must NOT
    // be drained (proves the query is repo-scoped).
    for title in ["add caching", "fix pagination"] {
        client
            .call(
                "ticket.new",
                json!({"title": title, "body": "do it", "scope": repo_name}),
            )
            .await
            .unwrap();
    }
    client
        .call(
            "ticket.new",
            json!({"title": "elsewhere", "scope": "other-repo"}),
        )
        .await
        .unwrap();

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "drain-test",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();
    // for_each, wait_all, evaluate — no aspects.
    assert_eq!(started["instance"]["total_steps"], 3);

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

    // Exactly two rats fanned out (the other-repo ticket was not drained).
    let agents = client.call("agent.list", json!({})).await.unwrap();
    let list = agents["agents"].as_array().unwrap();
    assert_eq!(list.len(), 2, "one rat per ready ticket in this repo");
    for agent in list {
        assert_eq!(agent["harness"], "fake");
    }

    // Both drained tickets closed their loop: the supervisor marks a clean
    // finish done (fire-and-forget, so poll). The other-repo ticket is untouched.
    let mut all_done = false;
    for _ in 0..50 {
        let tickets = client
            .call("ticket.list", json!({"scope": repo_name}))
            .await
            .unwrap();
        all_done = tickets["tickets"]
            .as_array()
            .unwrap()
            .iter()
            .all(|t| t["payload"]["status"] == "done");
        if all_done {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(all_done, "both drained tickets should reach status done");
    let other = client
        .call("ticket.list", json!({"scope": "other-repo"}))
        .await
        .unwrap();
    assert_eq!(other["tickets"][0]["payload"]["status"], "open");

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

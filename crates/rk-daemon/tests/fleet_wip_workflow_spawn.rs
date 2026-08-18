//! End-to-end regression for the atomic fleet-WIP reservation shared by the
//! drain autoscaler and workflow `spawn` steps (rework of the admission
//! control TOCTOU: two concurrent admitters reading the same free slot
//! before either's spawn lands in the registry).
//!
//! Drives two workflow instances whose `spawn` steps genuinely overlap in
//! time, under a fleet cap of 1, and asserts:
//!   - the sampled peak live-agent count never exceeds the cap (the ceiling
//!     holds even under real concurrency, not just a single spawn at a time);
//!   - both instances still complete (the loser of the race waits and
//!     retries — `Step::Spawn` treats a fleet-WIP refusal as "try again",
//!     not a step failure);
//!   - exactly one rat per instance is dispatched (no double-spawn).

mod fixture;

use rk_core::config::DrainConfig;
use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::collections::HashMap;
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
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

// Long enough that both spawns' live windows reliably overlap across 50ms
// polls, so a cap that is respected keeps the sampled peak at 1. See
// continuous_drain.rs's SLOW_FAKE for why this is never `remove_var`'d.
const SLOW_FAKE: &str = r#"
read -r _prompt
sleep 0.4
echo '{"type":"system","subtype":"init","session_id":"wip-fake"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"did the work","session_id":"wip-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

const WORKFLOW: &str = r#"
workflow: {
    name: "solo-spawn"
    params: {
        taskId: {type: "string", required: true}
    }
    agents: {
        default: {harness: "fake", model: "sonnet"}
    }
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId, description: "do " + _input.taskId}},
        {type: "wait", timeout: "30s"},
        {type: "evaluate", expect: {is_error: false}},
        {type: "dismiss"},
    ]
}
"#;

const WIP_DEADLINE: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

#[tokio::test]
async fn two_concurrent_workflow_spawns_share_the_fleet_wip_cap_and_both_complete() {
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
    std::fs::write(wf_dir.join("solo-spawn.cue"), WORKFLOW).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(SLOW_FAKE));
    let layout = Layout::at(home.path());
    let mut daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    // Drain itself disabled (never dispatches from a backlog) — only its
    // `max_wip` matters here, which the workflow engine shares as its own
    // fleet-WIP ceiling regardless of whether drain's own refill loop runs
    // (see `ServerState::engine` in server.rs). This isolates the assertion
    // to the workflow `spawn` step's admission path.
    daemon.set_drain_config(DrainConfig {
        enabled: false,
        max_wip: 1,
        interval_secs: 3600,
        repo: None,
        repos: HashMap::new(),
        aging_secs: 0,
    });
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let repo = repo_dir.path().to_string_lossy().to_string();
    let mut ids = Vec::new();
    for task in ["wip-a", "wip-b"] {
        let started = client
            .call(
                "workflow.run",
                json!({
                    "name": "solo-spawn",
                    "repo": repo,
                    "params": {"taskId": task},
                }),
            )
            .await
            .unwrap();
        ids.push(started["instance"]["id"].as_str().unwrap().to_string());
    }

    let mut peak_live = 0usize;
    let mut both_done = false;
    let deadline = tokio::time::Instant::now() + WIP_DEADLINE;
    while tokio::time::Instant::now() < deadline {
        let agents = client.call("agent.list", json!({})).await.unwrap();
        let live = agents["agents"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|a| matches!(a["state"].as_str(), Some("spawning") | Some("running")))
            .count();
        peak_live = peak_live.max(live);

        let mut done = 0;
        for id in &ids {
            let status = client
                .call("workflow.status", json!({"name": id}))
                .await
                .unwrap();
            match status["instance"]["status"].as_str().unwrap_or("") {
                "completed" => done += 1,
                "failed" => panic!("workflow {id} failed: {}", status["instance"]["error"]),
                _ => {}
            }
        }
        if done == ids.len() {
            both_done = true;
            break;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    assert!(
        both_done,
        "both concurrent workflow instances should complete"
    );
    // The cap is an UPPER bound, sampled — see continuous_drain.rs for why
    // this is not also asserted as a lower bound.
    assert!(
        peak_live <= 1,
        "fleet-WIP cap of 1 must hold across concurrent workflow spawns: peak live was {peak_live}"
    );

    // One rat per instance, no double-spawn from either the race or a retry.
    let agents = client.call("agent.list", json!({})).await.unwrap();
    assert_eq!(
        agents["agents"].as_array().unwrap().len(),
        2,
        "exactly one rat per workflow instance"
    );
    // NOTE: deliberately no `remove_var("RK_FAKE_HARNESS_CMD")` — see SLOW_FAKE.
}

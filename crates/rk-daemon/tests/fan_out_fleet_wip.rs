//! Regression: `for_each` fan-out must share the fleet-WIP admission path
//! with every other spawn source (rework 2 of TKT-01M036NWE1EW5B1PWSHK0MKX8E
//! — before this fix, `workflow_exec.rs`'s fan-out loop called `spawn_agent`
//! with a hardcoded cap of `0`, meaning a `for_each` step could blow straight
//! past `[drain] max_wip` no matter how low the operator set it).
//!
//! Drives a `for_each` fan-out over 5 ready tickets under a fleet cap of 2 and
//! asserts:
//!   - the sampled peak live-agent count never exceeds the cap;
//!   - the fan-out still completes and dispatches all 5 (a refusal retries
//!     under the same wait/poll a `Step::Spawn` uses, it does not drop or
//!     fail the ticket);
//!   - each ticket is claimed and dispatched exactly once.

mod fixture;

use rk_core::config::DrainConfig;
mod support;

use rk_core::paths::Layout;
use rk_daemon::Daemon;
use serde_json::json;
use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

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

// Long enough that live windows reliably overlap across 50ms polls, so a cap
// that is respected keeps the sampled peak at or below it. See
// continuous_drain.rs's SLOW_FAKE for why this is never `remove_var`'d.
const SLOW_FAKE: &str = r#"
read -r _prompt
sleep 0.4
echo '{"type":"system","subtype":"init","session_id":"fanout-fake"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"fanned","session_id":"fanout-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

const WORKFLOW: &str = r#"
workflow: {
    name: "fanout-test"
    agents: {
        default: {harness: "fake", model: "sonnet"}
    }
    steps: [
        {
            type: "for_each"
            query: {status: "ready", limit: 10}
            role: "rat"
            task: {title: "{{item.id}}", description: "do {{item.title}}"}
        },
        {type: "wait_all", timeout: "60s"},
        {type: "evaluate", expect: {all_ok: true}},
    ]
}
"#;

const FANOUT_DEADLINE: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

#[tokio::test]
async fn for_each_fan_out_shares_the_fleet_wip_cap_and_still_completes() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    support::install_default_repository_policy(repo_dir.path());

    let repo_name = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("fanout-test.cue"), WORKFLOW).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(SLOW_FAKE));
    let layout = Layout::at(home.path());
    let mut daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    // Drain itself disabled (it never dispatches from a backlog on its own) —
    // only its `max_wip` matters, which the workflow engine shares as its own
    // fleet-WIP ceiling regardless of whether drain's own refill loop runs
    // (mirrors fleet_wip_workflow_spawn.rs). This isolates the assertion to
    // the `for_each` fan-out's own admission path.
    daemon.set_drain_config(DrainConfig {
        enabled: false,
        max_wip: 2,
        interval_secs: 3600,
        repo: None,
        repos: HashMap::new(),
        aging_secs: 0,
    });
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;

    // Five ready tickets — deeper than the fleet cap of 2, so at least some
    // fan-out spawns must be refused and retried rather than dispatched
    // straight through.
    for i in 0..5 {
        client
            .call(
                "ticket.new",
                json!({"title": format!("task {i}"), "body": "do it", "scope": repo_name}),
            )
            .await
            .unwrap();
    }

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "fanout-test",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    let mut peak_live = 0usize;
    let mut completed = false;
    let deadline = tokio::time::Instant::now() + FANOUT_DEADLINE;
    while tokio::time::Instant::now() < deadline {
        let agents = client.call("agent.list", json!({})).await.unwrap();
        let live = agents["agents"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|a| matches!(a["state"].as_str(), Some("spawning") | Some("running")))
            .count();
        peak_live = peak_live.max(live);

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
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    assert!(
        completed,
        "the fan-out must still complete despite fleet-wip contention; a hardcoded 0 cap \
         (bypassing admission) would race past the ceiling instead, and a naive fix that \
         errors instead of retrying would fail the step here"
    );
    // The cap is an UPPER bound, sampled — see continuous_drain.rs for why
    // this is not also asserted as a lower bound.
    assert!(
        peak_live <= 2,
        "fleet-WIP cap of 2 must hold across the for_each fan-out: peak live was {peak_live}"
    );

    // One rat per ticket, no double-spawn from a retry.
    let agents = client.call("agent.list", json!({})).await.unwrap();
    assert_eq!(
        agents["agents"].as_array().unwrap().len(),
        5,
        "exactly one rat per fanned-out ticket"
    );
    // NOTE: deliberately no `remove_var("RK_FAKE_HARNESS_CMD")` — see
    // continuous_drain.rs's SLOW_FAKE for why.
}

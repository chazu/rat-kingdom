//! Regression for the drain-side half of the admission-control TOCTOU: a
//! ticket claimed by `Drain::run_cycle_at` right before its `spawn_async` call
//! is refused by a concurrent admitter (rework 2 of
//! TKT-01M036NWE1EW5B1PWSHK0MKX8E). Before the fix the claimed ticket was
//! left `in_progress` forever on that refusal — ready work silently stranded,
//! with no rat ever assigned to it and no future cycle able to re-claim it
//! (`Tickets::claim` only wins from `open`).
//!
//! There is no hook to force the exact TOCTOU window deterministically from
//! outside the daemon, so this drives the same real-concurrency race
//! `fleet_wip_workflow_spawn.rs` uses for the workflow-spawn side: a fleet-WIP
//! cap of 1, one ready ticket for the drain to claim, and a competing
//! workflow `spawn` step hammering the SAME cap concurrently. Under the old
//! code, any refusal during that contention permanently strands the ticket at
//! `in_progress` and it can never reach `done` — the test times out. Under the
//! fix, a refused claim is reopened to `open`, so a later cycle (drain's own,
//! once contention eases) picks it back up and it reaches `done` within the
//! deadline.

mod fixture;

use rk_core::config::DrainConfig;
mod support;

use rk_core::paths::Layout;
use rk_daemon::Daemon;
use rk_ledger::Budget;
use rk_space::Space;
use serde_json::json;
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

fn init_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

// Short so the hammer loop below cycles through many spawns within the test
// deadline, maximizing how often its own admission attempt genuinely overlaps
// drain's. See continuous_drain.rs's SLOW_FAKE for why this is never
// `remove_var`'d.
const FAST_FAKE: &str = r#"
read -r _prompt
sleep 0.15
echo '{"type":"system","subtype":"init","session_id":"race-fake"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"raced","session_id":"race-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

const OCCUPANT_WORKFLOW: &str = r#"
workflow: {
    name: "occupant"
    params: {
        taskId: {type: "string", required: true}
    }
    agents: {
        default: {harness: "fake", model: "sonnet"}
    }
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId, description: "occupy the fleet-wip slot"}},
        {type: "wait", timeout: "30s"},
        {type: "evaluate", expect: {is_error: false}},
        {type: "dismiss"},
    ]
}
"#;

const RACE_DEADLINE: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

#[tokio::test]
async fn drain_reopens_ticket_refused_by_fleet_wip_cap_instead_of_stranding_it() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    let repo_name = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("occupant.cue"), OCCUPANT_WORKFLOW).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(FAST_FAKE));
    let layout = Layout::at(home.path());
    let space = Space::open_in_memory().unwrap();
    let mut daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "fake".into(),
        Budget::default(),
        space,
    )
    .unwrap();
    // A cap of 1 makes every occupant spawn a direct competitor for the exact
    // slot drain wants for its own ticket.
    daemon.set_drain_config(DrainConfig {
        enabled: true,
        max_wip: 1,
        interval_secs: 1,
        repo: None,
        repos: std::collections::HashMap::new(),
        aging_secs: 3600,
    });
    tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    // Several CONCURRENT hammer tasks (not one sequential loop) so multiple
    // `workflow.run` calls are genuinely in flight against the daemon at
    // once, each racing to reserve the same cap-of-1 slot the instant its own
    // `spawn` step's precheck sees it free. This is what actually produces
    // the TOCTOU window `Registry::try_reserve_wip` exists to close: two
    // threads both past their precheck and racing the same registry-lock
    // reservation. Started before the ticket exists and left running well
    // past it, so contention is already established by the time drain makes
    // its first claim attempt.
    let hammer_repo = repo_dir.path().to_string_lossy().to_string();
    let mut hammers = Vec::new();
    for h in 0..6 {
        let hammer_layout = layout.clone();
        let hammer_repo = hammer_repo.clone();
        hammers.push(tokio::spawn(async move {
            let mut c = connect(&hammer_layout).await;
            let mut i = 0u32;
            loop {
                let _ = c
                    .call(
                        "workflow.run",
                        json!({
                            "name": "occupant",
                            "repo": hammer_repo,
                            "params": {"taskId": format!("occupant-{h}-{i}")},
                        }),
                    )
                    .await;
                i += 1;
            }
        }));
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    client
        .call(
            "ticket.new",
            json!({"title": "race ticket", "body": "do it", "scope": repo_name}),
        )
        .await
        .unwrap();

    let mut done = false;
    let deadline = tokio::time::Instant::now() + RACE_DEADLINE;
    while tokio::time::Instant::now() < deadline {
        let tickets = client
            .call("ticket.list", json!({"scope": repo_name}))
            .await
            .unwrap();
        let status = tickets["tickets"][0]["payload"]["status"]
            .as_str()
            .unwrap_or("");
        if status == "done" {
            done = true;
            break;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    for h in hammers {
        h.abort();
    }

    assert!(
        done,
        "the ready ticket must eventually reach done despite fleet-wip contention with \
         a concurrent workflow spawn; a claim stranded in_progress by a refusal would hang \
         here forever since Tickets::claim only ever wins from `open`"
    );
    // NOTE: deliberately no `remove_var("RK_FAKE_HARNESS_CMD")` — see
    // continuous_drain.rs's SLOW_FAKE for why.
}

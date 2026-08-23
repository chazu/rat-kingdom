//! Load/integration proof for the implementation and review capacity lanes
//! (TKT-01M0P2KM83Y4MD5QYETR3JCKF2): drives a genuine implementation burst
//! against ONE repository through all three real dispatch callers the ticket
//! names — the drain autoscaler, a workflow `for_each` fan-out, and direct
//! operator `agent.spawn` RPC calls — all competing for the SAME
//! `implementation_admission_limit_by_repo` cap, while a concurrent reviewer
//! spawn proves the review lane is never starved by that burst. Also proves
//! no dispatch path double-launches: every distinct logical request (a
//! ticket, an operator task, the reviewer) produces EXACTLY one agent record.
//!
//! The old fleet-wide `[drain] max_wip` ceiling is deliberately disabled
//! (`max_wip: 0`) so this test isolates the NEW per-repo lane as the only
//! thing bounding concurrency — a regression here could not hide behind the
//! pre-existing ceiling picking up the slack.

mod fixture;

use rk_core::config::DrainConfig;
mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use rk_ledger::Budget;
use rk_space::Space;
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

// Push-branch delivery: a clean `rk_done` alone closes a ticket, no merge
// required — see drain_fleet_wip_refusal.rs's identical rationale. This test
// cares about dispatch/admission, not delivery mechanics.
const PUSH_BRANCH_POLICY: &str = r#"
repo: {
    delivery: {
        mode: "push-branch"
    }
}
"#;

fn init_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::create_dir_all(dir.join(".rk")).unwrap();
    std::fs::write(dir.join(".rk/repo.cue"), PUSH_BRANCH_POLICY).unwrap();
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

// Short enough that the burst cycles through many spawns within the
// deadline, long enough that live windows reliably overlap 50ms polls — see
// continuous_drain.rs's SLOW_FAKE for why this script is never
// `remove_var`'d afterward.
const FAST_FAKE: &str = r#"
read -r _prompt
sleep 0.2
echo '{"type":"system","subtype":"init","session_id":"lane-load-fake"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"lane-load-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

const FANOUT_WORKFLOW: &str = r#"
workflow: {
    name: "lane-load-fanout"
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

const LOAD_DEADLINE: Duration = Duration::from_secs(60);
const REVIEWER_START_DEADLINE: Duration = Duration::from_secs(10);
const VERIFY_START_DEADLINE: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// A fast, single-shot "verify" check the load test's concurrent `verify.run`
/// call resolves — proves the wholly independent verification lane
/// (`verification_admission_limit_by_repo`, the ticket's third lane) is
/// exercised at the same time the implementation/review lanes are saturated,
/// not just configured. Appends one line to `verify-runs.log` on every
/// execution so the test can prove the check ran EXACTLY once — no dispatch
/// path double-launches it, and no admission race causes the daemon to run
/// it twice.
const VERIFY_CHECK: &str = r#"checks: [
    {name: "verify", command: "echo run >> verify-runs.log", timeout: "10s", environmentPolicy: "strip_rk_spawn"},
]
"#;

/// Unlike `.rk/repo.cue`, the operator's `verify.run` path resolves the
/// check registry from the repo's registered root checkout directly
/// (`Server::handle_verify_run`'s operator branch), not a git worktree — so,
/// unlike `managed_verification_cancel_e2e.rs`'s own `install_verify_check`,
/// this deliberately does NOT commit: an uncommitted `checks.cue` proves the
/// operator path never needed git in the first place.
fn install_verify_check(dir: &Path) {
    std::fs::write(dir.join(".rk").join("checks.cue"), VERIFY_CHECK).unwrap();
}

/// Unlike drain (reopens the ticket, a later cycle reclaims it) and a
/// workflow `spawn`/`for_each` step (its own internal poll-and-retry loop on
/// `is_fleet_wip_refusal`), a direct `agent.spawn` RPC call has NO built-in
/// retry: a lane refusal is a plain terminal error to whatever called it —
/// by design, this is a synchronous request/response API, not a queue a
/// caller hands work to. A real operator/orchestrator wrapper around this RPC
/// is expected to retry on exactly this error the same way drain/workflow do
/// internally; this helper stands in for that wrapper.
async fn spawn_with_retry(
    client: &mut rk_daemon::Client,
    params: serde_json::Value,
    deadline: Duration,
) -> serde_json::Value {
    let start = tokio::time::Instant::now();
    loop {
        match client.call("agent.spawn", params.clone()).await {
            Ok(v) => return v,
            Err(e) if e.to_string().contains("lane at capacity") => {
                if start.elapsed() > deadline {
                    panic!("operator spawn never admitted within {deadline:?}: {e}");
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("unexpected agent.spawn error: {e}"),
        }
    }
}

#[tokio::test]
async fn implementation_burst_across_all_three_dispatch_paths_stays_bounded_and_never_starves_review(
) {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    install_verify_check(repo_dir.path());
    let repo_name = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("lane-load-fanout.cue"), FANOUT_WORKFLOW).unwrap();

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
    // Old fleet-wide ceiling OFF (0 = disabled) — this test isolates the new
    // per-repo implementation lane as the only thing bounding concurrency.
    daemon.set_drain_config(DrainConfig {
        enabled: true,
        max_wip: 0,
        interval_secs: 1,
        repo: None,
        repos: HashMap::new(),
        aging_secs: 3600,
    });
    daemon.set_implementation_admission_limits(0, HashMap::from([(repo_name.clone(), 2)]));
    daemon.set_review_admission_limits(0, HashMap::from([(repo_name.clone(), 1)]));
    daemon.set_verification_admission_limits(0, HashMap::from([(repo_name.clone(), 1)]));
    tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    // Six ready tickets: drain's own refill loop and the workflow fan-out
    // below both draw from this SAME pool concurrently. `Tickets::claim` is
    // exactly-once, so however the six split between the two dispatch paths,
    // no ticket may ever produce two agents.
    for i in 0..6 {
        client
            .call(
                "ticket.new",
                json!({"title": format!("lane-load-{i}"), "body": "do it", "scope": repo_name}),
            )
            .await
            .unwrap();
    }

    // Dispatch path 2: workflow `for_each` fan-out over the same ready pool.
    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "lane-load-fanout",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {},
            }),
        )
        .await
        .unwrap();
    let instance_id = started["instance"]["id"].as_str().unwrap().to_string();

    // Concurrently with the burst above — and the implementation-lane
    // contention it creates between drain, the fan-out, and the operator
    // dispatch below — issue a REAL managed `verify.run` against the SAME
    // repo through the wholly independent verification lane. Spawned as its
    // own task right as the burst begins, so it genuinely races the
    // saturation instead of running only after it settles; proves a final-
    // target verification is never starved by implementation-lane pressure,
    // the same guarantee already proven for the review lane below.
    let verify_started = tokio::time::Instant::now();
    let verify_layout = layout.clone();
    let repo_for_verify = repo_name.clone();
    let mut verify_task: Option<tokio::task::JoinHandle<rk_core::Result<serde_json::Value>>> =
        Some(tokio::spawn(async move {
            let mut verify_client = Client::connect_as_operator(&verify_layout).await.unwrap();
            verify_client
                .call(
                    "verify.run",
                    json!({"repo": repo_for_verify, "check": "verify"}),
                )
                .await
        }));

    // Dispatch path 3: direct operator `agent.spawn` RPC calls — bypasses
    // tickets and the workflow engine entirely, the exact path
    // `Server::handle_spawn` used to pass a hardcoded `fleet_wip_cap: 0` for
    // (still does, for the OLD ceiling — the new per-repo lane check inside
    // `Supervisor::spawn` is unconditional on that).
    for i in 0..2 {
        spawn_with_retry(
            &mut client,
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": format!("operator-extra-{i}"),
                "harness": "fake",
            }),
            Duration::from_secs(30),
        )
        .await;
    }

    // Concurrently, a reviewer spawn for the SAME repo — must not be starved
    // by the implementation burst above, since it admits against the wholly
    // separate review lane. Also routed through the retry helper (cheap
    // insurance: the review lane is uncontended here, so this should be
    // admitted on its very first attempt).
    let reviewer_spawn_started = tokio::time::Instant::now();
    spawn_with_retry(
        &mut client,
        json!({
            "repo": repo_dir.path().to_string_lossy(),
            "task": "lane-load-review",
            "role": "reviewer",
            "harness": "fake",
        }),
        Duration::from_secs(30),
    )
    .await;

    let mut peak_live_non_reviewer = 0usize;
    let mut reviewer_went_live_at: Option<Duration> = None;
    let mut fanout_completed = false;
    let mut verify_completed_at: Option<Duration> = None;
    let mut verify_result: Option<serde_json::Value> = None;
    let deadline = tokio::time::Instant::now() + LOAD_DEADLINE;
    while tokio::time::Instant::now() < deadline {
        let agents = client.call("agent.list", json!({})).await.unwrap();
        let agents = agents["agents"].as_array().unwrap();

        let live_non_reviewer = agents
            .iter()
            .filter(|a| {
                a["repo_name"].as_str() == Some(repo_name.as_str())
                    && a["role"].as_str() != Some("reviewer")
                    && matches!(a["state"].as_str(), Some("spawning") | Some("running"))
            })
            .count();
        peak_live_non_reviewer = peak_live_non_reviewer.max(live_non_reviewer);

        if reviewer_went_live_at.is_none() {
            let reviewer_live = agents.iter().any(|a| {
                a["repo_name"].as_str() == Some(repo_name.as_str())
                    && a["role"].as_str() == Some("reviewer")
                    && matches!(
                        a["state"].as_str(),
                        Some("spawning") | Some("running") | Some("completed")
                    )
            });
            if reviewer_live {
                reviewer_went_live_at = Some(reviewer_spawn_started.elapsed());
            }
        }

        if !fanout_completed {
            let status = client
                .call("workflow.status", json!({"name": instance_id}))
                .await
                .unwrap();
            match status["instance"]["status"].as_str().unwrap_or("") {
                "completed" => fanout_completed = true,
                "failed" => panic!("fan-out workflow failed: {}", status["instance"]["error"]),
                _ => {}
            }
        }

        if verify_completed_at.is_none() && verify_task.as_ref().is_some_and(|h| h.is_finished()) {
            let outcome = verify_task
                .take()
                .unwrap()
                .await
                .expect("verify.run task must not panic");
            verify_result = Some(outcome.unwrap_or_else(|e| panic!("verify.run RPC failed: {e}")));
            verify_completed_at = Some(verify_started.elapsed());
        }

        if fanout_completed && reviewer_went_live_at.is_some() && verify_completed_at.is_some() {
            break;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    assert!(
        fanout_completed,
        "the fan-out must still complete despite implementation-lane contention with drain \
         and operator dispatch"
    );
    assert!(
        reviewer_went_live_at.is_some(),
        "the reviewer spawn must eventually start — a starved review lane would never let it \
         go live at all"
    );
    assert!(
        reviewer_went_live_at.unwrap() <= REVIEWER_START_DEADLINE,
        "the reviewer must start within a bounded time even while the implementation lane is \
         saturated by drain + fan-out + operator dispatch: took {:?}",
        reviewer_went_live_at.unwrap()
    );
    assert!(
        peak_live_non_reviewer <= 2,
        "the implementation-lane cap of 2 must hold across ALL THREE dispatch paths \
         (drain, for_each fan-out, and operator agent.spawn) at once: peak was \
         {peak_live_non_reviewer}"
    );

    assert!(
        verify_completed_at.is_some(),
        "the concurrent verify.run against the same repo's verification lane must complete \
         despite implementation-lane saturation from drain + fan-out + operator dispatch — a \
         starved verification lane would never let it finish at all"
    );
    assert!(
        verify_completed_at.unwrap() <= VERIFY_START_DEADLINE,
        "verify.run must start and complete within a bounded time even while the \
         implementation lane is saturated by the burst: took {:?}",
        verify_completed_at.unwrap()
    );
    let verify_result = verify_result.expect("verify_completed_at implies a recorded result");
    assert_eq!(
        verify_result["verdict"],
        json!("pass"),
        "the concurrent verify check must pass: {verify_result:#?}"
    );
    assert_eq!(verify_result["exit"], json!(0));
    let verify_log =
        std::fs::read_to_string(repo_dir.path().join("verify-runs.log")).unwrap_or_default();
    assert_eq!(
        verify_log.lines().count(),
        1,
        "the verify check must have run EXACTLY once — no duplicate launch from the \
         implementation burst racing its admission: log was {verify_log:?}"
    );

    // No duplicate launches anywhere: exactly one agent per ticket (6),
    // exactly one per direct operator spawn (2), and exactly one reviewer —
    // 9 total, no more, no fewer. `verify.run` is a managed check execution,
    // not an agent, so it must never contribute to this count either way —
    // and this read is race-robust rather than a lucky snapshot: the poll
    // loop above only breaks once the fan-out, the reviewer, AND the
    // concurrent verify.run have ALL settled, so nothing here can still be
    // mid-flight and land an agent record after this list is taken.
    let final_agents = client.call("agent.list", json!({})).await.unwrap();
    let final_agents = final_agents["agents"].as_array().unwrap();
    let repo_agents: Vec<_> = final_agents
        .iter()
        .filter(|a| a["repo_name"].as_str() == Some(repo_name.as_str()))
        .collect();
    assert_eq!(
        repo_agents.len(),
        9,
        "expected exactly 9 agents (6 tickets + 2 operator + 1 reviewer), got {}: {:#?}",
        repo_agents.len(),
        repo_agents
    );
    let reviewer_count = repo_agents
        .iter()
        .filter(|a| a["role"].as_str() == Some("reviewer"))
        .count();
    assert_eq!(
        reviewer_count, 1,
        "exactly one reviewer agent, no duplicate"
    );

    // Every ticket was claimed exactly once — none left stranded `ready`/`open`.
    let tickets = client
        .call("ticket.list", json!({"scope": repo_name}))
        .await
        .unwrap();
    let stranded = tickets["tickets"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|t| {
            matches!(
                t["payload"]["status"].as_str(),
                Some("ready") | Some("open")
            )
        })
        .count();
    assert_eq!(
        stranded, 0,
        "every ticket must be claimed by exactly one of drain/fan-out, none left stranded"
    );

    // NOTE: deliberately no `remove_var("RK_FAKE_HARNESS_CMD")` — see
    // continuous_drain.rs's SLOW_FAKE for why.
}

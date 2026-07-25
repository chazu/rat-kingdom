//! TKT-147 regression: a workflow must never report a crashed rat's run as a
//! clean one.
//!
//! TKT-146 fixed *why* a workflow-spawned rat got killed one second into its
//! task. It did not teach the chain to notice: an `evaluate` unifies against
//! whatever landed in `ctx.previousResult` and has no notion of "this rat never
//! really ran", so the instance still reported `Completed` over a rat with no
//! session, zero tokens and `process exited (code None) without completing`.
//! Any other path that kills or crashes a rat could be reported the same way —
//! and a silent no-op is the worst failure mode there is for a self-driving
//! loop (nightly-self-improve looked green for two runs while grooming
//! nothing).
//!
//! Three tests:
//!  1. a rat that dies without reporting fails its `wait` *promptly* — the
//!     instance lands in `rk inbox` instead of blocking to the step timeout;
//!  2. a result the crashed rat could not have produced is rejected even when
//!     it is sitting in the space and unifies with `expect` — the exact shape
//!     that made TKT-146 silent;
//!  3. the same in fan-out: a crashed rat fails its `wait_all` join rather than
//!     being counted as one more clean member of the batch.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

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

/// A rat whose process dies without the harness ever reporting a verdict: no
/// `system/init` line, no `result` line, no work. This is what a SIGTERM (or a
/// crash, or a budget kill) leaves behind — the record ends `Failed` with
/// `process exited (code ...) without completing`, no session, zero tokens, and
/// there is no `harness_result` for it anywhere in the space.
const CRASHING_FAKE: &str = r#"
read -r _prompt
sleep 1
exit 3
"#;

/// The `wait` timeout is deliberately far longer than the test's patience: a
/// crash must fail the step on its own, not by running the clock out.
const WORKFLOW: &str = r#"
workflow: {
    name: "crash-gate-test"
    agents: {
        default: {harness: "fake", model: "sonnet"}
    }
    steps: [
        {type: "spawn", role: "rat", task: {title: "do-the-thing", description: "Do the thing"}},
        {type: "wait", timeout: "30m"},
        {type: "evaluate", expect: {is_error: false}},
        {type: "dismiss"},
    ]
}
"#;

/// The fan-out shape of the same chain — `nightly-self-improve`'s grooming
/// phase, and the one that reported two green runs while doing nothing.
const FANOUT_WORKFLOW: &str = r#"
workflow: {
    name: "crash-gate-fanout"
    agents: {
        default: {harness: "fake", model: "sonnet"}
    }
    steps: [
        {
            type: "for_each"
            query: {status: "ready", limit: 5}
            role: "rat"
            task: {title: "{{item.id}}", description: "Implement {{item.title}}"}
        },
        {type: "wait_all", timeout: "30m"},
        {type: "evaluate", expect: {all_ok: true}},
        {type: "dismiss_all"},
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
    std::fs::write(wf_dir.join("crash-gate-test.cue"), WORKFLOW).unwrap();
    std::fs::write(wf_dir.join("crash-gate-fanout.cue"), FANOUT_WORKFLOW).unwrap();
    dir.file_name().unwrap().to_string_lossy().to_string()
}

async fn run_workflow(client: &mut Client, repo_dir: &Path) -> String {
    run_named(client, repo_dir, "crash-gate-test").await
}

async fn run_named(client: &mut Client, repo_dir: &Path, name: &str) -> String {
    let started = client
        .call(
            "workflow.run",
            json!({
                "name": name,
                "repo": repo_dir.to_string_lossy(),
                "params": {},
            }),
        )
        .await
        .unwrap();
    started["instance"]["id"].as_str().unwrap().to_string()
}

/// Poll until the instance settles; returns `(status, error)`.
async fn await_instance(client: &mut Client, id: &str) -> (String, String) {
    for _ in 0..600 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        let inst = &status["instance"];
        let s = inst["status"].as_str().unwrap_or("").to_string();
        if s == "completed" || s == "failed" {
            return (s, inst["error"].as_str().unwrap_or("").to_string());
        }
    }
    panic!("workflow instance {id} never settled");
}

/// The whole point: a rat that dies without reporting must fail its chain, and
/// must do so on the crash rather than at the end of a 30-minute `wait`.
#[tokio::test]
async fn a_rat_that_dies_without_reporting_fails_the_chain() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", CRASHING_FAKE);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let started = Instant::now();
    let id = run_workflow(&mut client, repo_dir.path()).await;
    let (status, error) = await_instance(&mut client, &id).await;

    assert_eq!(
        status, "failed",
        "a rat that produced nothing must not read as a clean run: {error}"
    );
    assert!(
        error.contains("without reporting") || error.contains("never ran"),
        "the failure should name the crash, got: {error}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(60),
        "the wait should give up when the rat dies, not run its 30m timeout out \
         (took {:?})",
        started.elapsed()
    );

    // And the operator is told: a failed instance is an `rk inbox` row.
    let inbox = client.call("inbox.list", json!({})).await.unwrap();
    let rows = inbox["items"].as_array().unwrap();
    assert!(
        rows.iter().any(|r| r.to_string().contains(&id)),
        "the failed instance should surface in rk inbox, got: {rows:?}"
    );

    // Nothing was merged: the rat never committed anything to merge.
    let log = git(repo_dir.path(), &["log", "--oneline", "main"]);
    assert_eq!(log.lines().count(), 1, "main should be untouched: {log}");
}

/// The silent-no-op shape itself: a `harness_result` that unifies with the
/// gate's `expect` is sitting in the space under the crashed rat's name, newer
/// than the rat's own record (so the TKT-146 generation floor does not catch
/// it). The old chain read it, evaluated it clean, dismissed the rat and
/// reported `Completed` — having done nothing at all. The liveness assertion
/// rejects it because the rat it is attributed to never ran.
#[tokio::test]
async fn a_result_the_crashed_rat_could_not_have_produced_is_rejected() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_name = init_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", CRASHING_FAKE);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let id = run_workflow(&mut client, repo_dir.path()).await;

    // Wait for the rat to exist, then plant a clean result under its name —
    // after its record's `created_at`, so it passes the generation floor.
    let mut doomed = String::new();
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let agents = client.call("agent.list", json!({})).await.unwrap();
        if let Some(first) = agents["agents"].as_array().and_then(|a| a.first()) {
            doomed = first["name"].as_str().unwrap().to_string();
            break;
        }
    }
    assert!(!doomed.is_empty(), "the workflow never spawned its rat");

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
                    "task": "do-the-thing",
                    "is_error": false,
                    "result": "a clean-looking result this rat never produced",
                    "cost_usd": 0.5,
                    "tokens": 1234,
                },
            }),
        )
        .await
        .unwrap();

    let (status, error) = await_instance(&mut client, &id).await;
    assert_eq!(
        status, "failed",
        "a result the rat could not have produced must not pass the gate: {error}"
    );
    assert!(
        error.contains(&doomed),
        "the failure should name the rat whose liveness failed, got: {error}"
    );

    // The rat is left for inspection rather than dismissed as a job well done.
    let agents = client.call("agent.list", json!({})).await.unwrap();
    let rat = &agents["agents"].as_array().unwrap()[0];
    assert_ne!(
        rat["state"].as_str(),
        Some("dismissed"),
        "the crashed rat should not have been dismissed as complete: {rat:?}"
    );
}

/// The fan-out arm: a `wait_all` must not count a rat that died without
/// reporting as one more clean member of the batch. This is the exact chain
/// `nightly-self-improve` grooms with — `for_each` → `wait_all` → `evaluate
/// {all_ok: true}` → `dismiss_all` — so a crashed groomer used to leave the
/// nightly run reporting `Completed` over an empty night.
#[tokio::test]
async fn a_crashed_rat_in_a_fan_out_fails_the_join() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_name = init_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", CRASHING_FAKE);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    for title in ["add caching", "fix pagination"] {
        client
            .call(
                "ticket.new",
                json!({"title": title, "body": "do it", "scope": repo_name}),
            )
            .await
            .unwrap();
    }

    let started = Instant::now();
    let id = run_named(&mut client, repo_dir.path(), "crash-gate-fanout").await;
    let (status, error) = await_instance(&mut client, &id).await;

    assert_eq!(
        status, "failed",
        "a fan-out of rats that produced nothing must not read as a clean batch: {error}"
    );
    assert!(
        error.starts_with("wait_all failed"),
        "the join should be what fails, got: {error}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(60),
        "the join should give up when its rats die, not run its 30m timeout out (took {:?})",
        started.elapsed()
    );

    // The tickets stay open for a human: nothing was drained.
    let tickets = client
        .call("ticket.list", json!({"scope": repo_name}))
        .await
        .unwrap();
    for t in tickets["tickets"].as_array().unwrap() {
        assert_ne!(
            t["payload"]["status"].as_str(),
            Some("done"),
            "a crashed rat must not close its ticket: {t:?}"
        );
    }
}

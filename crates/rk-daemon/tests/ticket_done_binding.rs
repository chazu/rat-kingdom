//! Binds a ticket's `done` transition to actual delivery, per the repo's
//! delivery-mode policy (strategic-review C3,
//! TKT-01M08HB566GFBZVMDKZ8DT1ES0). Closes the "approved but never merged"
//! class (TKT-18/46/147) structurally: a merge-mode ticket must not read
//! `done` while its branch is still unmerged, whether that transition would
//! have come from the rat's own completion or from an explicit
//! `ticket.update --status done`.

mod fixture;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

static HARNESS_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const WORKING_FAKE: &str = r#"
read -r _prompt
echo "ticket work" > ticket-work.txt
git add ticket-work.txt >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: ticket"
echo '{"type":"system","subtype":"init","session_id":"ticket-fake"}'
rk_done "ticket work complete"
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"ticket-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

async fn connect(layout: &Layout) -> Client {
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(client) = Client::connect_as_operator(layout).await {
            return client;
        }
    }
    panic!("daemon did not come up");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_mode_ticket_stays_open_until_actually_merged() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_path = repo_dir.path().join("donebindingrepo");
    std::fs::create_dir(&repo_path).unwrap();
    let repo = repo_path.as_path();
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "r@x"]);
    git(repo, &["config", "user.name", "R"]);
    std::fs::write(repo.join("README.md"), "# ticket done binding\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "init"]);
    // Default activated policy: delivery mode "merge", target "agent-base"
    // (resolves to "main" here) — no `.rk/repo.cue` override needed.

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "done-binding-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": "donebindingrepo", "path": repo.to_string_lossy()}),
        )
        .await
        .unwrap();

    let ticket = client
        .call("ticket.new", json!({"title": "bind done to delivery"}))
        .await
        .unwrap();
    let ticket_id = ticket["ticket"]["identity"].as_str().unwrap().to_string();

    let spawned = client
        .call(
            "agent.spawn",
            json!({"repo": repo.to_string_lossy(), "task": ticket_id, "harness": "fake"}),
        )
        .await
        .unwrap();
    let agent = spawned["agent"]["name"].as_str().unwrap().to_string();
    let branch = spawned["agent"]["branch"].as_str().unwrap().to_string();
    assert_eq!(spawned["agent"]["target_branch"], "main");

    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if client
            .call("agent.status", json!({"name": agent}))
            .await
            .unwrap()["agent"]["state"]
            == "completed"
        {
            break;
        }
    }
    // Give the fire-and-forget completion routing (route_completion's spawned
    // task) a moment to run and hit the delivery gate.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let unmerged = client
        .call("ticket.get", json!({"id": ticket_id}))
        .await
        .unwrap();
    assert_eq!(
        unmerged["ticket"]["payload"]["status"], "open",
        "a clean rat completion must not auto-mark a merge-mode ticket done \
         while its branch is unmerged: {unmerged}"
    );

    // The manual path ("steward mark-done") is refused the same way, with a
    // pointed error rather than a silent no-op.
    let refused = client
        .call(
            "ticket.update",
            json!({"id": ticket_id, "status": "done"}),
        )
        .await;
    let err = refused.expect_err("marking an unmerged merge-mode ticket done must be refused");
    assert!(
        err.to_string().contains(&branch) && err.to_string().to_lowercase().contains("merge"),
        "refusal must name the unmerged branch and the delivery mode: {err}"
    );

    // Deliver for real: merge the branch into its target directly (mirrors
    // what `deliver_branch` does for Merge-mode on dismiss).
    git(repo, &["merge", "--no-ff", "-m", "land it", &branch]);

    // The merged path is unaffected: the same manual request now succeeds.
    let updated = client
        .call(
            "ticket.update",
            json!({"id": ticket_id, "status": "done"}),
        )
        .await
        .unwrap();
    assert_eq!(updated["ticket"]["payload"]["status"], "done", "{updated}");

    handle.abort();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_mode_ticket_stays_open_when_branch_deleted_without_merging() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_path = repo_dir.path().join("donebindingrepo-deleted");
    std::fs::create_dir(&repo_path).unwrap();
    let repo = repo_path.as_path();
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "r@x"]);
    git(repo, &["config", "user.name", "R"]);
    std::fs::write(repo.join("README.md"), "# ticket done binding\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "init"]);

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "done-binding-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": "donebindingrepo-deleted", "path": repo.to_string_lossy()}),
        )
        .await
        .unwrap();

    let ticket = client
        .call("ticket.new", json!({"title": "deleted unmerged branch"}))
        .await
        .unwrap();
    let ticket_id = ticket["ticket"]["identity"].as_str().unwrap().to_string();

    let spawned = client
        .call(
            "agent.spawn",
            json!({"repo": repo.to_string_lossy(), "task": ticket_id, "harness": "fake"}),
        )
        .await
        .unwrap();
    let agent = spawned["agent"]["name"].as_str().unwrap().to_string();
    let branch = spawned["agent"]["branch"].as_str().unwrap().to_string();
    let worktree = spawned["agent"]["worktree"].as_str().unwrap().to_string();

    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if client
            .call("agent.status", json!({"name": agent}))
            .await
            .unwrap()["agent"]["state"]
            == "completed"
        {
            break;
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Delete the branch WITHOUT ever merging it — a lost-work scenario, not
    // the daemon's own post-merge cleanup. The branch is checked out in the
    // agent's worktree, so drop that first (mirrors
    // branch_merged_or_gone_when_branch_deleted in rk-git).
    git(repo, &["worktree", "remove", "--force", &worktree]);
    git(repo, &["branch", "-D", &branch]);

    // The manual path must still refuse: a gone-but-never-merged branch is
    // not a verified merge, so it must not read as delivered.
    let refused = client
        .call("ticket.update", json!({"id": ticket_id, "status": "done"}))
        .await;
    let err = refused.expect_err(
        "marking a merge-mode ticket done must be refused when its branch was deleted \
         without ever merging — 'gone' is not proof of delivery",
    );
    assert!(
        err.to_string().to_lowercase().contains("merge"),
        "refusal must reference the delivery mode: {err}"
    );

    let unmerged = client
        .call("ticket.get", json!({"id": ticket_id}))
        .await
        .unwrap();
    assert_eq!(
        unmerged["ticket"]["payload"]["status"], "open",
        "a deleted-but-unmerged branch must not auto-mark the ticket done: {unmerged}"
    );

    handle.abort();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_mode_ticket_done_refused_when_repo_unresolvable() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_path = repo_dir.path().join("donebindingrepo-vanish");
    std::fs::create_dir(&repo_path).unwrap();
    let repo = repo_path.as_path();
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "r@x"]);
    git(repo, &["config", "user.name", "R"]);
    std::fs::write(repo.join("README.md"), "# ticket done binding\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "init"]);

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "done-binding-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": "donebindingrepo-vanish", "path": repo.to_string_lossy()}),
        )
        .await
        .unwrap();

    let ticket = client
        .call("ticket.new", json!({"title": "repo vanishes before delivery"}))
        .await
        .unwrap();
    let ticket_id = ticket["ticket"]["identity"].as_str().unwrap().to_string();

    let spawned = client
        .call(
            "agent.spawn",
            json!({"repo": repo.to_string_lossy(), "task": ticket_id, "harness": "fake"}),
        )
        .await
        .unwrap();
    let agent = spawned["agent"]["name"].as_str().unwrap().to_string();

    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if client
            .call("agent.status", json!({"name": agent}))
            .await
            .unwrap()["agent"]["state"]
            == "completed"
        {
            break;
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The repo root itself is gone (e.g. an operator deleted/moved it, or a
    // worktree was reaped from under the record): Repo::discover fails. The
    // gate must fail CLOSED — refuse `done` rather than silently letting it
    // through because nothing could be checked.
    std::fs::remove_dir_all(repo_path.parent().unwrap()).unwrap();

    let refused = client
        .call("ticket.update", json!({"id": ticket_id, "status": "done"}))
        .await;
    refused.expect_err(
        "an unresolvable repo must refuse the delivery check, not silently pass it",
    );

    handle.abort();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

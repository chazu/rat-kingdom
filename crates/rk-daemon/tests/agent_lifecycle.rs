//! Phase 2 exit criteria, end to end over the socket: spawn a (fake-harness)
//! rat into a real worktree, watch it complete, verify parent routing, dismiss
//! with merge, and confirm main received the work.

mod fixture;

mod support;

use rk_core::paths::Layout;
use rk_daemon::Daemon;
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

/// `RK_FAKE_HARNESS_CMD` is process-global (`std::env::set_var`), and cargo
/// runs this file's `#[tokio::test]`s concurrently within one process by
/// default. This file got away with that for a long time because its
/// original two tests both pointed the var at the same `WORKING_FAKE`
/// script — a second, differently-scripted test (content-free branch,
/// TKT-01M0C663BZ86SMA2PVMFP5QJ8D) can otherwise have another test's
/// concurrent `set_var` overwrite its harness mid-spawn, exactly the race
/// `ticket_done_binding.rs`'s own `HARNESS_ENV_LOCK` guards against.
static HARNESS_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

fn git_out(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).to_string()
}

fn scratch_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "rat@example.com"]);
    git(dir, &["config", "user.name", "Rat"]);
    std::fs::write(dir.join("README.md"), "# scratch\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

/// Fake harness script: does real git work in its cwd (the worktree), then
/// emits a Claude-style completion.
const WORKING_FAKE: &str = r#"
read -r _prompt
echo "gnawed by $RK_AGENT for task $RK_TASK" > gnawed.txt
git add gnawed.txt >/dev/null 2>&1
git -c user.email=rat@x -c user.name=Rat commit -q -m "rat work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"fake-e2e"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"committed gnawed.txt","session_id":"fake-e2e","total_cost_usd":0.002,"usage":{"input_tokens":50,"output_tokens":25,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

#[tokio::test]
async fn spawn_complete_route_dismiss_merge() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // Spawn with a parent so completion routing is exercised.
    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "gnaw-1",
                "harness": "fake",
                "parent": "KingRat",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();
    assert_eq!(spawned["agent"]["state"], "running");
    assert_eq!(spawned["agent"]["parent"], "KingRat");

    // Wait for completion (structural: registry state, no sleep-polling the pane).
    let mut completed = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if status["agent"]["state"] == "completed" {
            assert_eq!(status["agent"]["result"], "committed gnawed.txt");
            assert_eq!(status["agent"]["cost_usd"], 0.002);
            assert_eq!(status["agent"]["session_id"], "fake-e2e");
            completed = true;
            break;
        }
    }
    assert!(completed, "agent never completed");

    // Completion was routed: repo event + directed message to the parent.
    let events = client
        .call(
            "space.scan",
            json!({"category": "event", "identity": "harness_result"}),
        )
        .await
        .unwrap();
    assert_eq!(events["tuples"].as_array().unwrap().len(), 1);
    let payload = &events["tuples"][0]["payload"];
    // C3 (docs/2026-08-17-tkt-c1-generation-identity.md): the producer side
    // of the spawn-keyed join — a real completion must carry the minted
    // generation id, not just the display name.
    assert!(
        payload["spawn"].as_str().is_some_and(|s| !s.is_empty()),
        "harness_result must carry the completing generation's spawn id: {payload:?}"
    );
    // Review-tiering fields (TKT-01M036N1RT74H6NPRH5FMM8A6T): head_sha is the
    // branch tip the fake rat produced; the one-file, one-line commit
    // classifies as trivial.
    let branch = payload["branch"].as_str().unwrap().to_string();
    let expected_sha = git_out(repo_dir.path(), &["rev-parse", &branch])
        .trim()
        .to_string();
    assert_eq!(payload["head_sha"], expected_sha);
    assert_eq!(payload["diff_files"], 1);
    assert_eq!(payload["diff_lines"], 1);
    assert_eq!(payload["diff_class"], "trivial");
    let parent_msg = client
        .call(
            "space.scan",
            json!({"category": "message", "identity": "KingRat"}),
        )
        .await
        .unwrap();
    assert_eq!(parent_msg["tuples"][0]["payload"]["child"], name);
    assert_eq!(parent_msg["tuples"][0]["payload"]["is_error"], false);

    // Dismiss merges the rat's branch into main.
    let dismissed = client
        .call("agent.dismiss", json!({"name": name}))
        .await
        .unwrap();
    assert_eq!(dismissed["merged"], true, "detail: {}", dismissed["detail"]);

    let files = git_out(repo_dir.path(), &["ls-tree", "--name-only", "main"]);
    assert!(files.contains("gnawed.txt"), "main has the rat's work");
    let log = git_out(repo_dir.path(), &["log", "--oneline", "main"]);
    assert!(log.contains("rat work: gnaw-1"));

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// A rat dispatched with a ticket id as its task closes the ticket's loop —
/// but under the default (merge) delivery policy, `done` is bound to actual
/// delivery (TKT-01M08HB566GFBZVMDKZ8DT1ES0 / strategic-review C3): a clean
/// completion alone must not flip the ticket to `done` while its branch is
/// still unmerged (that was the TKT-18/46/147 "approved but never merged"
/// class). It only closes once dismiss actually merges the branch, going
/// straight from `open` to `closed`.
#[tokio::test]
async fn ticket_dispatched_rat_closes_its_ticket() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // File a ticket, then dispatch a rat whose task IS that ticket id.
    let ticket = client
        .call(
            "ticket.new",
            json!({"title": "do the thing", "scope": "svc"}),
        )
        .await
        .unwrap();
    let id = ticket["ticket"]["identity"].as_str().unwrap().to_string();

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": id,
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap()["agent"]["state"]
            == "completed"
        {
            break;
        }
    }
    // Give the fire-and-forget completion routing a moment to hit the
    // delivery gate before asserting it left the ticket alone.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let t = client.call("ticket.get", json!({"id": id})).await.unwrap();
    assert_eq!(
        t["ticket"]["payload"]["status"], "open",
        "a clean completion must not mark a merge-mode ticket done before its branch merges"
    );

    // Dismiss with merge closes it for good — straight from `open`, never
    // having passed through `done`.
    let dismissed = client
        .call("agent.dismiss", json!({"name": name}))
        .await
        .unwrap();
    assert_eq!(dismissed["merged"], true);
    let t = client.call("ticket.get", json!({"id": id})).await.unwrap();
    assert_eq!(t["ticket"]["payload"]["status"], "closed");
    assert_eq!(
        t["ticket"]["payload"]["delivery"]["merge_commit"], dismissed["merge_commit"],
        "dismiss writes the same durable delivery proof as the landing pipeline"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// A fake harness that exits without ever touching the worktree or
/// declaring itself done — models a rat dispatched onto a ticket whose real
/// work already landed elsewhere and finds nothing left to do (Emmental-8,
/// which reported `is_error=true declared_done=false diff_files=0
/// diff_lines=0`, per TKT-01M0C663BZ86SMA2PVMFP5QJ8D). Not calling `rk_done`
/// means this also exercises the intended code path in isolation: the
/// unrelated C3 completion-time gate (`ticket_delivered`,
/// supervisor.rs:3060) only runs `if !is_error`, so a `rk_done`-calling
/// (clean) fixture here would mark the ticket `done` at harness-completion
/// time — before dismiss ever runs — for the same "empty branch reads
/// merged" reason this ticket's fix targets, but at a different call site
/// this ticket does not touch.
const NO_OP_FAKE: &str = r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"fake-noop"}'
echo '{"type":"result","subtype":"error","is_error":true,"result":"nothing to do","session_id":"fake-noop","total_cost_usd":0.001,"usage":{"input_tokens":5,"output_tokens":2,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

/// TKT-01M0C663BZ86SMA2PVMFP5QJ8D: `dismiss`'s "a merged ticket-rat closes
/// its ticket for good" rule must not fire for a rat whose branch carried no
/// content over its target. A `merge --no-ff` of an already-up-to-date
/// branch cleanly reports `merged: true` (`advance_via_worktree` moves the
/// target ref onto its own current commit) even though nothing was actually
/// delivered — the same shape a duplicate rat's branch has when it is
/// dispatched onto a ticket whose real work already landed under it, so its
/// dismiss must not be mistaken for that ticket's delivery.
#[tokio::test]
async fn ticket_dispatched_rat_with_content_free_branch_does_not_close_its_ticket() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(NO_OP_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let ticket = client
        .call(
            "ticket.new",
            json!({"title": "already delivered elsewhere", "scope": "svc"}),
        )
        .await
        .unwrap();
    let id = ticket["ticket"]["identity"].as_str().unwrap().to_string();

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": id,
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap()["agent"]["state"]
            == "completed"
        {
            break;
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Dismiss "merges" (the branch is already up to date with its target,
    // so the merge is a clean no-op that still reports `merged: true`) but
    // must NOT close the ticket — nothing was actually delivered.
    let dismissed = client
        .call("agent.dismiss", json!({"name": name}))
        .await
        .unwrap();
    assert_eq!(dismissed["merged"], true, "detail: {}", dismissed["detail"]);
    let t = client.call("ticket.get", json!({"id": id})).await.unwrap();
    assert_eq!(
        t["ticket"]["payload"]["status"], "open",
        "a content-free branch merging on dismiss must not read as ticket delivery: {t}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// TKT-01M0CTC4DYBRX6P5X2NPEZF0EZ (probes O8/O17): the dismiss-time closer
/// must consult landing-queue membership by task, the same way the reopen
/// sweep does (`Server::ticket_reopen_sweep_at`) — a ticket whose branch is
/// still `queued`/`running_gates`/`awaiting_review` in the daemon-native
/// landing pipeline must not be closed out from under it just because some
/// dismiss for that same task merges cleanly (a duplicate dispatched onto
/// the ticket, or the pipeline's own delivery racing this dismiss).
#[tokio::test]
async fn ticket_with_a_queued_landing_entry_is_not_closed_on_dismiss() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let ticket = client
        .call(
            "ticket.new",
            json!({"title": "still queued elsewhere", "scope": "svc"}),
        )
        .await
        .unwrap();
    let id = ticket["ticket"]["identity"].as_str().unwrap().to_string();

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": id,
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap()["agent"]["state"]
            == "completed"
        {
            break;
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Simulate the ticket's real branch still sitting mid-pipeline at the
    // moment this dismiss runs.
    let repo_scope = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    client
        .call(
            "space.out",
            json!({
                "category": "event",
                "scope": repo_scope,
                "identity": "landing_queue_entry",
                "lifecycle": "furniture",
                "payload": {
                    "repo_name": repo_scope,
                    "repo_path": repo_dir.path().to_string_lossy(),
                    "branch": "rat/some-other-rat/task",
                    "target": "main",
                    "head_sha": "deadbeef",
                    "diff_class": "trivial",
                    "task": id,
                    "seq": 1,
                    "status": "running_gates",
                    "rev": 0,
                },
            }),
        )
        .await
        .unwrap();

    // Dismiss merges this rat's own branch cleanly, but the ticket's real
    // work is still mid-pipeline — it must not close.
    let dismissed = client
        .call("agent.dismiss", json!({"name": name}))
        .await
        .unwrap();
    assert_eq!(dismissed["merged"], true, "detail: {}", dismissed["detail"]);
    let t = client.call("ticket.get", json!({"id": id})).await.unwrap();
    assert_eq!(
        t["ticket"]["payload"]["status"], "open",
        "a ticket whose branch is still queued for landing must not close on an unrelated dismiss: {t}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

//! TKT-54: `rk revert <agent>` — the operator undo for a bad unattended
//! auto-merge. A dismissed rat's merge commit is recorded on its registry
//! record; `agent.revert` revert-merges it on the target, reopens the rat's
//! ticket (`open`, or `blocked` with `block`), and emits a `fact` tuple.
//! A durable operation survives interrupted finalization; replay returns the
//! same revert and never reopens later work or mints another completion fact.

mod fixture;
mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
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
    support::install_passing_landing_checks(dir);
}

/// Fake harness: commits a file in its worktree, reports a clean success.
/// Declares `rk done` before its result line: a clean turn that never does
/// now parks the agent as `Paused` (awaiting resume) rather than `Completed`,
/// which every test here waits on.
///
/// `RK_FAKE_HARNESS_CMD` is process-global, and this binary's two tests run
/// concurrently, so neither test may ever `remove_var` it: doing so at the
/// end of one test can unset the fake mid-flight for the other test's still-
/// spawning agent, which then falls back to a different default script and
/// never reaches the state either test is waiting on (TKT-88 — mirrors the
/// same precaution in fleet_budget.rs/merge_queue.rs/pr_mode.rs). Both tests
/// set the identical value, so leaving it set for the whole process is
/// harmless.
fn working_fake() -> String {
    fixture::with_rk_done(
        r#"
read -r _prompt
echo "bad work by $RK_AGENT for $RK_TASK" > regression.txt
git add regression.txt >/dev/null 2>&1
git -c user.email=rat@x -c user.name=Rat commit -q -m "rat work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"revert-fake"}'
rk_done "done"
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"revert-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_revert_boundary_survives_a_daemon_restart_without_duplicate_effects() {
    std::env::set_var("RK_FAKE_HARNESS_CMD", working_fake());
    for barrier in [
        "revert-after-intent",
        "revert-after-prepared",
        "revert-after-git",
        "revert-after-registry",
        "revert-after-ticket",
        "revert-after-evidence",
    ] {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        scratch_repo(repo_dir.path());
        let layout = Layout::at(home.path());
        let config = rk_core::config::Config::default();
        let daemon = Daemon::new(layout.clone(), &config).unwrap();
        let handle = tokio::spawn(daemon.run());
        let mut client = connect(&layout).await;
        let (name, ticket) = merge_one_rat(&mut client, repo_dir.path()).await;
        let merged_tip = git_out(repo_dir.path(), &["rev-parse", "main"]);
        std::fs::write(home.path().join("fault-barrier"), barrier).unwrap();
        let request_name = name.clone();
        let request = tokio::spawn(async move {
            client
                .call("agent.revert", json!({"name": request_name, "block": true}))
                .await
        });
        tokio::time::timeout(Duration::from_secs(15), async {
            while !home.path().join("fault-barrier.reached").exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("never reached {barrier}"));
        handle.abort();
        let _ = handle.await;
        request.abort();
        let _ = request.await;
        // The process stays alive in this test; a real restart observes its
        // predecessor's dead pid. Remove only the disposable test endpoint.
        std::fs::remove_file(layout.pid_file()).ok();
        std::fs::remove_file(layout.socket_path()).ok();
        std::fs::remove_file(home.path().join("fault-barrier")).unwrap();
        let tip_at_crash = git_out(repo_dir.path(), &["rev-parse", "main"]);
        let already_advanced = !matches!(barrier, "revert-after-intent" | "revert-after-prepared");
        assert_eq!(tip_at_crash != merged_tip, already_advanced, "{barrier}");
        let operation = {
            let space = rk_space::Space::open(&layout.db_path()).unwrap();
            space
                .scan(
                    &rk_core::tuple::Pattern::category(rk_core::tuple::Category::Event)
                        .identity("revert_operation"),
                )
                .unwrap()[0]
                .payload["id"]
                .clone()
        };
        if barrier == "revert-after-registry" {
            let mut registry =
                rk_daemon::agents::Registry::load(&home.path().join("agents.json")).unwrap();
            let mut replacement = registry.get(&name).unwrap().clone();
            registry
                .archive(chrono::Utc::now() + chrono::Duration::seconds(1))
                .unwrap();
            replacement.spawn = Some(rk_core::id::SpawnId::new());
            replacement.created_at = chrono::Utc::now();
            replacement.merge_commit = Some("new-generation-delivery".into());
            registry.insert(replacement).unwrap();
        }
        let daemon = Daemon::new(layout.clone(), &config).unwrap();
        let handle = tokio::spawn(daemon.run());
        let mut client = connect(&layout).await;
        if barrier == "revert-after-prepared" {
            // Startup GC must keep the only ref pinning the unadvanced revert.
            let git = rk_git::Repo::discover(repo_dir.path()).unwrap();
            assert_eq!(git.candidate_refs().unwrap().len(), 1);
            git_out(repo_dir.path(), &["gc", "--prune=now"]);
        }
        let settled = client
            .call(
                "agent.revert",
                json!({"name": name, "block": true, "operation": operation}),
            )
            .await
            .unwrap();
        assert_eq!(settled["reverted"], true, "{barrier}: {settled}");
        assert_eq!(settled["operation_id"], operation);
        if barrier == "revert-after-registry" {
            let current = client
                .call("agent.status", json!({"name": name}))
                .await
                .unwrap();
            assert_eq!(current["agent"]["merge_commit"], "new-generation-delivery");
        }
        let final_tip = git_out(repo_dir.path(), &["rev-parse", "main"]);
        if already_advanced {
            assert_eq!(tip_at_crash, final_tip, "{barrier}: duplicate Git effect");
        }
        assert!(!repo_dir.path().join("regression.txt").exists());
        let current = client
            .call("ticket.get", json!({"id": ticket}))
            .await
            .unwrap();
        assert_eq!(current["ticket"]["payload"]["status"], "blocked");
        assert!(current["ticket"]["payload"]["delivery"].is_null());
        assert_eq!(
            current["ticket"]["payload"]["revert_operations"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        // Reopen/claim later work before replaying the completed operation.
        client
            .call("ticket.reopen", json!({"id": ticket, "status": "open"}))
            .await
            .unwrap();
        let again = client
            .call(
                "agent.revert",
                json!({"name": name, "block": true, "operation": operation}),
            )
            .await
            .unwrap();
        assert_eq!(again["operation_id"], settled["operation_id"]);
        assert_eq!(again["revert_commit"], settled["revert_commit"]);
        let current = client
            .call("ticket.get", json!({"id": ticket}))
            .await
            .unwrap();
        assert_eq!(
            current["ticket"]["payload"]["status"], "open",
            "replay must not reset newer work"
        );
        let facts = client
            .call(
                "space.scan",
                json!({"category": "fact",
            "identity": format!("merge-reverted-{name}")}),
            )
            .await
            .unwrap();
        assert_eq!(
            facts["tuples"].as_array().unwrap().len(),
            1,
            "{barrier}: duplicate evidence"
        );
        assert!(client
            .call(
                "agent.revert",
                json!({"name": name, "block": false, "operation": operation})
            )
            .await
            .is_err());
        handle.abort();
        let _ = handle.await;
    }
}

#[tokio::test]
async fn required_write_failures_remain_retryable_and_never_report_completion() {
    std::env::set_var("RK_FAKE_HARNESS_CMD", working_fake());
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());
    let layout = Layout::at(home.path());
    let daemon = Daemon::new(layout.clone(), &rk_core::config::Config::default()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    let (name, ticket) = merge_one_rat(&mut client, repo_dir.path()).await;
    let db = rusqlite::Connection::open(layout.db_path()).unwrap();
    let registry = home.path().join("agents.json");
    let saved_registry = home.path().join("agents.before-revert.json");
    std::fs::rename(&registry, &saved_registry).unwrap();
    std::fs::create_dir(&registry).unwrap();
    let failed = client.call("agent.revert", json!({"name": name})).await;
    assert!(
        failed.is_err(),
        "registry persistence failure must propagate"
    );
    let agent = client
        .call("agent.status", json!({"name": name}))
        .await
        .unwrap();
    assert!(
        agent["agent"]["merge_commit"].is_string(),
        "failed persistence restores memory"
    );
    std::fs::remove_dir(&registry).unwrap();
    std::fs::rename(&saved_registry, &registry).unwrap();
    let mut reverted_tip = Some(git_out(repo_dir.path(), &["rev-parse", "main"]));
    for (stage, condition) in [
        ("ticket", "NEW.category = 'task' AND json_extract(NEW.payload, '$.revert_operations') IS NOT NULL"),
        ("evidence", "NEW.category = 'fact' AND NEW.identity LIKE 'merge-reverted-%'"),
        ("completion", "NEW.identity = 'revert_operation' AND json_extract(NEW.payload, '$.phase.state') = 'complete'")
    ] {
        db.execute_batch(&format!("CREATE TRIGGER reject_revert_write BEFORE INSERT ON tuples
            WHEN {condition} BEGIN SELECT RAISE(ABORT, 'injected {stage} write failure'); END;")).unwrap();
        let result = client.call("agent.revert", json!({"name": name})).await;
        assert!(result.is_err(), "{stage} failure must not report completion: {result:?}");
        let tip = git_out(repo_dir.path(), &["rev-parse", "main"]);
        if let Some(prior) = &reverted_tip { assert_eq!(&tip, prior, "{stage}: Git ran again"); }
        reverted_tip = Some(tip);
        let current = client.call("ticket.get", json!({"id": ticket})).await.unwrap();
        if stage == "ticket" {
            assert_eq!(current["ticket"]["payload"]["status"], "closed", "failed replacement keeps old ticket");
            assert!(current["ticket"]["payload"]["delivery"].is_object());
        } else {
            assert_eq!(current["ticket"]["payload"]["status"], "open");
            assert!(current["ticket"]["payload"]["delivery"].is_null());
        }
        db.execute_batch("DROP TRIGGER reject_revert_write;").unwrap();
    }
    let result = client
        .call("agent.revert", json!({"name": name}))
        .await
        .unwrap();
    assert_eq!(result["reverted"], true);
    assert_eq!(
        git_out(repo_dir.path(), &["rev-parse", "main"]),
        reverted_tip.unwrap()
    );
    let facts = client
        .call(
            "space.scan",
            json!({"category": "fact", "identity": format!("merge-reverted-{name}")}),
        )
        .await
        .unwrap();
    assert_eq!(facts["tuples"].as_array().unwrap().len(), 1);
    handle.abort();
    let _ = handle.await;
}

/// Spawn a ticket-dispatched rat, wait for completion, dismiss (auto-merge).
/// Returns (agent name, ticket id).
async fn merge_one_rat(client: &mut Client, repo: &Path) -> (String, String) {
    support::register_repo(client, repo).await;
    let ticket = client
        .call(
            "ticket.new",
            json!({"title": "do the thing", "scope": repo.file_name().unwrap().to_string_lossy()}),
        )
        .await
        .unwrap();
    let ticket_id = ticket["ticket"]["identity"].as_str().unwrap().to_string();

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo.to_string_lossy(),
                "task": ticket_id,
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();
    let branch = spawned["agent"]["branch"].as_str().unwrap().to_string();

    let mut completed = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": &name}))
            .await
            .unwrap();
        if status["agent"]["state"] == "completed" {
            completed = true;
            break;
        }
    }
    assert!(completed, "rat {name} never completed");

    let dismissed = client
        .call("agent.dismiss", json!({"name": &name}))
        .await
        .unwrap();
    assert_eq!(
        dismissed["merged"], false,
        "detail: {}",
        dismissed["detail"]
    );
    let landed = client
        .call(
            "repo.land",
            json!({"repo": repo, "branch": branch, "target": "main"}),
        )
        .await
        .unwrap();
    assert_eq!(landed["merged"], true, "detail: {}", landed["detail"]);
    assert!(
        landed["merge_commit"]
            .as_str()
            .is_some_and(|c| !c.is_empty()),
        "gated land records the merge commit"
    );
    (name, ticket_id)
}

#[tokio::test]
async fn revert_undoes_merge_reopens_ticket_and_emits_fact() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", working_fake());
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let (name, ticket_id) = merge_one_rat(&mut client, repo_dir.path()).await;
    assert!(repo_dir.path().join("regression.txt").exists());
    let t = client
        .call("ticket.get", json!({"id": &ticket_id}))
        .await
        .unwrap();
    assert_eq!(t["ticket"]["payload"]["status"], "closed");

    // The undo: revert-merge the landed commit.
    let reverted = client
        .call("agent.revert", json!({"name": &name}))
        .await
        .unwrap();
    assert_eq!(reverted["reverted"], true, "detail: {}", reverted["detail"]);
    assert!(
        reverted["revert_commit"]
            .as_str()
            .is_some_and(|c| !c.is_empty()),
        "revert reports the revert commit"
    );

    // The bad work is gone from main's tree AND the root checkout; history
    // keeps both the merge and the revert.
    let files = git_out(repo_dir.path(), &["ls-tree", "--name-only", "main"]);
    assert!(
        !files.contains("regression.txt"),
        "main tree still has the bad file"
    );
    assert!(!repo_dir.path().join("regression.txt").exists());
    let log = git_out(repo_dir.path(), &["log", "--oneline", "main"]);
    assert!(log.contains("Revert"));

    // The ticket the bad merge closed is back on the backlog.
    let t = client
        .call("ticket.get", json!({"id": &ticket_id}))
        .await
        .unwrap();
    assert_eq!(t["ticket"]["payload"]["status"], "open");

    // The revert left a durable fact tuple behind.
    let facts = client
        .call(
            "space.scan",
            json!({"category": "fact", "identity": format!("merge-reverted-{name}")}),
        )
        .await
        .unwrap();
    let fact = &facts["tuples"][0];
    assert_eq!(fact["payload"]["agent"], name.as_str());
    assert_eq!(fact["payload"]["task"], ticket_id.as_str());
    assert_eq!(fact["payload"]["ticket_status"], "open");
    assert!(fact["payload"]["revert_commit"].as_str().is_some());

    let again = client
        .call("agent.revert", json!({"name": &name}))
        .await
        .unwrap();
    assert_eq!(again["operation_id"], reverted["operation_id"]);
    assert_eq!(again["revert_commit"], reverted["revert_commit"]);
}

/// The bug TKT-01M0P96ZSQAJGRE7WTGDBWAXJ9 exists to fix: before
/// `finalize_delivery`, only manual `rk land` recorded the agent-side merge
/// pointer, so `rk revert` on anything the reactor's own `action: "land"`
/// trigger landed automatically (no `agent.dismiss`, no manual `repo.land`)
/// failed with "no recorded merge commit" even though the ticket showed
/// delivered. This drives that exact path end to end.
#[tokio::test]
async fn automatic_reactor_landing_can_be_reverted() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", working_fake());
    let layout = Layout::at(home.path());
    std::fs::create_dir_all(layout.triggers_dir()).unwrap();
    std::fs::write(
        layout.triggers_dir().join("landing.cue"),
        r#"triggers: [{name: "legacy-landing-on-completion", action: "land",
            match: {category: "event", identity: "harness_result", search: "\"role\":\"rat\""},
            maxFires: 20}]"#,
    )
    .unwrap();
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let repo_name = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    client
        .call(
            "repo.add",
            json!({"name": &repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();
    let ticket = client
        .call(
            "ticket.new",
            json!({"title": "do the thing", "scope": &repo_name}),
        )
        .await
        .unwrap();
    let ticket_id = ticket["ticket"]["identity"].as_str().unwrap().to_string();
    let spawned = client
        .call(
            "agent.spawn",
            json!({"repo": repo_dir.path().to_string_lossy(), "task": &ticket_id, "harness": "fake"}),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    let mut merge_commit = None;
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": &name}))
            .await
            .unwrap();
        if let Some(c) = status["agent"]["merge_commit"]
            .as_str()
            .filter(|c| !c.is_empty())
        {
            merge_commit = Some(c.to_string());
            break;
        }
    }
    assert!(
        merge_commit.is_some(),
        "the reactor's automatic land never derived this generation's merge pointer"
    );

    let t = client
        .call("ticket.get", json!({"id": &ticket_id}))
        .await
        .unwrap();
    assert_eq!(t["ticket"]["payload"]["status"], "closed");

    let reverted = client
        .call("agent.revert", json!({"name": &name}))
        .await
        .unwrap();
    assert_eq!(reverted["reverted"], true, "detail: {}", reverted["detail"]);

    let t = client
        .call("ticket.get", json!({"id": &ticket_id}))
        .await
        .unwrap();
    assert_eq!(
        t["ticket"]["payload"]["status"], "open",
        "revert must reopen the ticket the automatic landing closed"
    );
    // No `remove_var` here: this binary's tests run concurrently and share
    // one process env (see `working_fake`'s doc) — both set the identical
    // command, so leaving it set is harmless, but unsetting mid-flight would
    // break a sibling test still spawning its own rat.
}

#[tokio::test]
async fn revert_block_reopens_ticket_blocked_and_never_merged_errors() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", working_fake());
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let (name, ticket_id) = merge_one_rat(&mut client, repo_dir.path()).await;

    // --block holds the reopened ticket out of the auto-dispatch backlog.
    let reverted = client
        .call("agent.revert", json!({"name": &name, "block": true}))
        .await
        .unwrap();
    assert_eq!(reverted["reverted"], true, "detail: {}", reverted["detail"]);
    assert_eq!(reverted["ticket_status"], "blocked");
    let t = client
        .call("ticket.get", json!({"id": &ticket_id}))
        .await
        .unwrap();
    assert_eq!(t["ticket"]["payload"]["status"], "blocked");

    // A rat dismissed WITHOUT a merge has no anchor: revert errors.
    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "held-work",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let held = spawned["agent"]["name"].as_str().unwrap().to_string();
    let mut completed = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": &held}))
            .await
            .unwrap();
        if status["agent"]["state"] == "completed" {
            completed = true;
            break;
        }
    }
    assert!(completed, "rat {held} never completed");
    let dismissed = client
        .call("agent.dismiss", json!({"name": &held, "no_merge": true}))
        .await
        .unwrap();
    assert_eq!(dismissed["merged"], false);
    let denied = client.call("agent.revert", json!({"name": &held})).await;
    assert!(denied.is_err(), "revert of a never-merged agent must error");
}

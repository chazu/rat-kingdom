//! TKT-01M0EHSWMA24S6J2NAFSR14TZ2: preserve ticket identity when
//! resubmitting a recovery branch to landing.
//!
//! `Supervisor::task_for_branch` only ever resolves a task by matching a
//! *live agent record* to the branch being landed. A hand-built recovery
//! branch — e.g. an orchestrating LLM recreating a dead reviewer's exact
//! head under a fresh branch name and resubmitting it with `rk land` — has
//! no such record, so the landing pipeline used to enqueue it with
//! `task: ""`: no ticket/spec for the reviewer, no delivery record written,
//! nothing closed or unblocked. `repo.land`'s `task` param closes that gap
//! by letting the submission carry an explicit task identity, validated
//! against the ticket store rather than trusted blindly.

mod fixture;
mod support;

use rk_core::paths::Layout;
use rk_daemon::Daemon;
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::{connect, install_passing_landing_checks};

const WORKING_FAKE: &str = r#"
read -r _prompt
echo "work" > agent-work.txt
git add agent-work.txt >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work"
echo '{"type":"system","subtype":"init","session_id":"land-task-fake"}'
rk_done "done"
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"land-task-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

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

fn init_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    install_passing_landing_checks(dir);
}

/// A hand-built branch with no agent record behind it — exactly what a
/// recovery resubmission looks like once `agent.spawn` never touched it.
fn recovery_branch(repo: &Path, name: &str, file: &str) -> String {
    git(repo, &["checkout", "-b", name]);
    std::fs::write(repo.join(file), "recovered\n").unwrap();
    git(repo, &["add", file]);
    git(repo, &["commit", "-m", "recovery"]);
    git(repo, &["checkout", "main"]);
    name.to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_task_binds_a_recovery_branch_with_no_agent_record() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path().join("landtaskrepo");
    std::fs::create_dir(&repo).unwrap();
    init_repo(&repo);

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let ticket = client
        .call(
            "ticket.new",
            json!({"title": "recovery resubmit", "scope": "landtaskrepo"}),
        )
        .await
        .unwrap();
    let ticket_id = ticket["ticket"]["identity"].as_str().unwrap().to_string();

    let branch = recovery_branch(&repo, "rat/recovery-retry/tkt-x", "recovered.txt");

    let result = client
        .call(
            "repo.land",
            json!({
                "repo": repo.to_string_lossy(),
                "branch": branch,
                "target": "main",
                "task": ticket_id,
            }),
        )
        .await
        .unwrap();
    assert_eq!(result["merged"], true, "{result}");
    let merge_commit = result["merge_commit"].clone();
    assert!(
        merge_commit.as_str().is_some_and(|c| !c.is_empty()),
        "{result}"
    );

    let after = client
        .call("ticket.get", json!({"id": ticket_id}))
        .await
        .unwrap();
    assert_eq!(
        after["ticket"]["payload"]["status"], "closed",
        "landing on an explicit --task must close the ticket it names: {after}"
    );
    assert_eq!(
        after["ticket"]["payload"]["delivery"]["merge_commit"], merge_commit,
        "the delivery record must carry the real merge commit: {after}"
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_task_must_name_an_existing_ticket() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path().join("landtaskrepo-missing");
    std::fs::create_dir(&repo).unwrap();
    init_repo(&repo);

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let branch = recovery_branch(&repo, "rat/recovery-retry/tkt-y", "recovered.txt");

    let refused = client
        .call(
            "repo.land",
            json!({
                "repo": repo.to_string_lossy(),
                "branch": branch,
                "target": "main",
                "task": "TKT-DOESNOTEXIST",
            }),
        )
        .await;
    let err = refused.expect_err("an unknown --task must fail closed, not be trusted blindly");
    assert!(
        err.to_string().to_lowercase().contains("no such ticket"),
        "{err}"
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_task_must_match_the_repo_being_landed_into() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path().join("landtaskrepo-scope");
    std::fs::create_dir(&repo).unwrap();
    init_repo(&repo);

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // A real ticket, but scoped to a different repo entirely.
    let ticket = client
        .call(
            "ticket.new",
            json!({"title": "belongs elsewhere", "scope": "some-other-repo"}),
        )
        .await
        .unwrap();
    let ticket_id = ticket["ticket"]["identity"].as_str().unwrap().to_string();

    let branch = recovery_branch(&repo, "rat/recovery-retry/tkt-z", "recovered.txt");

    let refused = client
        .call(
            "repo.land",
            json!({
                "repo": repo.to_string_lossy(),
                "branch": branch,
                "target": "main",
                "task": ticket_id,
            }),
        )
        .await;
    let err = refused.expect_err(
        "a --task scoped to a different repo must fail closed rather than bind the wrong \
         repo's ticket",
    );
    assert!(err.to_string().contains("some-other-repo"), "{err}");

    let untouched = client
        .call("ticket.get", json!({"id": ticket_id}))
        .await
        .unwrap();
    assert_eq!(
        untouched["ticket"]["payload"]["status"], "open",
        "the rejected ticket must not be touched: {untouched}"
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_task_disagreeing_with_a_real_agent_record_fails_closed() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path().join("landtaskrepo-mismatch");
    std::fs::create_dir(&repo).unwrap();
    init_repo(&repo);

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let real_ticket = client
        .call(
            "ticket.new",
            json!({"title": "the real work", "scope": "landtaskrepo-mismatch"}),
        )
        .await
        .unwrap();
    let real_id = real_ticket["ticket"]["identity"]
        .as_str()
        .unwrap()
        .to_string();
    let wrong_ticket = client
        .call(
            "ticket.new",
            json!({"title": "a different ticket entirely", "scope": "landtaskrepo-mismatch"}),
        )
        .await
        .unwrap();
    let wrong_id = wrong_ticket["ticket"]["identity"]
        .as_str()
        .unwrap()
        .to_string();

    let spawned = client
        .call(
            "agent.spawn",
            json!({"repo": repo.to_string_lossy(), "task": real_id, "harness": "fake"}),
        )
        .await
        .unwrap();
    let agent = spawned["agent"]["name"].as_str().unwrap().to_string();
    let branch = spawned["agent"]["branch"].as_str().unwrap().to_string();

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
    client
        .call("agent.dismiss", json!({"name": agent}))
        .await
        .unwrap();

    let refused = client
        .call(
            "repo.land",
            json!({
                "repo": repo.to_string_lossy(),
                "branch": branch,
                "target": "main",
                "task": wrong_id,
            }),
        )
        .await;
    let err = refused.expect_err(
        "an explicit --task that disagrees with the branch's own agent record must fail \
         closed, not silently override real evidence",
    );
    assert!(
        err.to_string().contains(&real_id) && err.to_string().contains(&wrong_id),
        "{err}"
    );

    let wrong_after = client
        .call("ticket.get", json!({"id": wrong_id}))
        .await
        .unwrap();
    assert_eq!(
        wrong_after["ticket"]["payload"]["status"], "open",
        "the wrongly-named ticket must not be touched by a refused land: {wrong_after}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_submission_with_explicit_task_is_idempotent() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path().join("landtaskrepo-dup");
    std::fs::create_dir(&repo).unwrap();
    init_repo(&repo);

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let ticket = client
        .call(
            "ticket.new",
            json!({"title": "retry submission", "scope": "landtaskrepo-dup"}),
        )
        .await
        .unwrap();
    let ticket_id = ticket["ticket"]["identity"].as_str().unwrap().to_string();

    let branch = recovery_branch(&repo, "rat/recovery-retry/tkt-dup", "recovered.txt");

    // `keep_branch: true` so the branch ref survives the first land — the
    // dedup key is `(repo, branch, head_sha)`, which the second submission
    // can only be checked against if it can still resolve `branch` at all.
    let first = client
        .call(
            "repo.land",
            json!({
                "repo": repo.to_string_lossy(),
                "branch": branch,
                "target": "main",
                "task": ticket_id,
                "keep_branch": true,
            }),
        )
        .await
        .unwrap();
    assert_eq!(first["merged"], true, "{first}");

    let second = client
        .call(
            "repo.land",
            json!({
                "repo": repo.to_string_lossy(),
                "branch": branch,
                "target": "main",
                "task": ticket_id,
                "keep_branch": true,
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        second["already_processed"], true,
        "resubmitting the exact same branch/head must be a suppressed no-op, not a second \
         delivery: {second}"
    );

    let after = client
        .call("ticket.get", json!({"id": ticket_id}))
        .await
        .unwrap();
    assert_eq!(after["ticket"]["payload"]["status"], "closed", "{after}");

    handle.abort();
}

/// A recovery branch with neither an agent record nor an explicit `--task`
/// must be refused outright, not silently enqueued with `task: ""` — that
/// used to mean no ticket/spec for the reviewer, no delivery record, and
/// nothing ever closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unbound_recovery_branch_without_task_fails_closed() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path().join("landtaskrepo-unbound");
    std::fs::create_dir(&repo).unwrap();
    init_repo(&repo);

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let branch = recovery_branch(&repo, "rat/recovery-retry/tkt-unbound", "recovered.txt");

    let refused = client
        .call(
            "repo.land",
            json!({
                "repo": repo.to_string_lossy(),
                "branch": branch,
                "target": "main",
            }),
        )
        .await;
    let err = refused.expect_err(
        "a branch with no agent record and no --task must fail closed rather than land with an \
         empty task identity",
    );
    assert!(
        err.to_string().to_lowercase().contains("no task identity"),
        "{err}"
    );

    handle.abort();
}

/// Resubmitting the exact same branch/head under a DIFFERENT same-repo
/// ticket must fail closed, not report `already_processed` — that would
/// silently look like a no-op while leaving the newly-named ticket untouched
/// and misleading the operator into thinking it landed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resubmission_with_a_different_task_fails_closed() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path().join("landtaskrepo-mismatch-retry");
    std::fs::create_dir(&repo).unwrap();
    init_repo(&repo);

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let first_ticket = client
        .call(
            "ticket.new",
            json!({"title": "first submission", "scope": "landtaskrepo-mismatch-retry"}),
        )
        .await
        .unwrap();
    let first_id = first_ticket["ticket"]["identity"]
        .as_str()
        .unwrap()
        .to_string();
    let second_ticket = client
        .call(
            "ticket.new",
            json!({"title": "wrongly retried under this one", "scope": "landtaskrepo-mismatch-retry"}),
        )
        .await
        .unwrap();
    let second_id = second_ticket["ticket"]["identity"]
        .as_str()
        .unwrap()
        .to_string();

    // `keep_branch: true` so the branch/head survive the first land — the
    // dedup key is `(repo, branch, head_sha)`, unchanged across the retry.
    let branch = recovery_branch(&repo, "rat/recovery-retry/tkt-remismatch", "recovered.txt");
    let first = client
        .call(
            "repo.land",
            json!({
                "repo": repo.to_string_lossy(),
                "branch": branch,
                "target": "main",
                "task": first_id,
                "keep_branch": true,
            }),
        )
        .await
        .unwrap();
    assert_eq!(first["merged"], true, "{first}");

    let refused = client
        .call(
            "repo.land",
            json!({
                "repo": repo.to_string_lossy(),
                "branch": branch,
                "target": "main",
                "task": second_id,
                "keep_branch": true,
            }),
        )
        .await;
    let err = refused.expect_err(
        "retrying the same branch/head under a different task must fail closed, not report \
         already_processed"
    );
    assert!(
        err.to_string().contains(&first_id) && err.to_string().contains(&second_id),
        "{err}"
    );

    let first_after = client
        .call("ticket.get", json!({"id": first_id}))
        .await
        .unwrap();
    assert_eq!(
        first_after["ticket"]["payload"]["status"], "closed",
        "the real landing's ticket must remain closed: {first_after}"
    );
    let second_after = client
        .call("ticket.get", json!({"id": second_id}))
        .await
        .unwrap();
    assert_eq!(
        second_after["ticket"]["payload"]["status"], "open",
        "a refused resubmission must not touch the wrongly-named ticket: {second_after}"
    );

    handle.abort();
}

/// `--force` bypasses the landing pipeline entirely — it never runs
/// `Supervisor::resolve_land_task`, never writes a `landing_processed`
/// marker, and never records a delivery. Silently accepting `--task`
/// alongside it would look like the ticket was bound when nothing recorded
/// that; the combination must be refused, not ignored.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn force_landing_rejects_explicit_task() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path().join("landtaskrepo-force");
    std::fs::create_dir(&repo).unwrap();
    init_repo(&repo);

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let ticket = client
        .call(
            "ticket.new",
            json!({"title": "force landing attempt", "scope": "landtaskrepo-force"}),
        )
        .await
        .unwrap();
    let ticket_id = ticket["ticket"]["identity"].as_str().unwrap().to_string();

    let branch = recovery_branch(&repo, "rat/recovery-retry/tkt-force", "recovered.txt");

    let refused = client
        .call(
            "repo.land",
            json!({
                "repo": repo.to_string_lossy(),
                "branch": branch,
                "target": "main",
                "force": true,
                "reason": "emergency landing",
                "task": ticket_id,
            }),
        )
        .await;
    let err = refused.expect_err(
        "--force combined with --task must be refused, not silently land without binding the \
         ticket"
    );
    assert!(
        err.to_string().to_lowercase().contains("force")
            && err.to_string().to_lowercase().contains("task"),
        "{err}"
    );

    let untouched = client
        .call("ticket.get", json!({"id": ticket_id}))
        .await
        .unwrap();
    assert_eq!(
        untouched["ticket"]["payload"]["status"], "open",
        "a refused force+task combination must not touch the ticket: {untouched}"
    );

    handle.abort();
}

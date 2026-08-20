//! End-to-end coverage for `reconcile.repair` (`crate::reconcile_repair`):
//! the RPC assembly, the real git subprocess calls it depends on
//! (`Server::repair_git_facts`, `Server::merge_commit_ancestry`), and the
//! `Tickets` compare-and-swap writers must actually work together, not just
//! the pure `plan()`/`apply()` unit tests in `rk-daemon/src/reconcile_repair.rs`.
//!
//! Two fixtures: a clean delivered-but-open ticket converges end to end
//! (dry-run previews with zero mutation, apply closes it and journals the
//! repair, a replay is a no-op), and a ticket whose delivery record git
//! actively disputes holds with zero mutation in both dry-run and apply.

mod support;

use rk_core::paths::Layout;
use rk_daemon::Daemon;
use serde_json::json;
use std::path::Path;
use std::process::Command;
use support::connect;

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
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// The delivered-but-open shape only ever arises from a direct payload
/// write or a status regression after delivery (see `reconcile.rs`'s own
/// doc comment) — never from the ordinary ticket API, which always closes
/// atomically with the record. Seeded the same way here, via a raw
/// `space.out` as the operator.
fn seed_delivered_but_open(
    scope: &str,
    id: &str,
    merge_commit: &str,
    target: &str,
    branch: &str,
) -> serde_json::Value {
    json!({
        "category": "task",
        "scope": scope,
        "identity": id,
        "payload": {
            "title": "work",
            "status": "in_progress",
            "assignee": serde_json::Value::Null,
            "delivery": {
                "merge_commit": merge_commit,
                "branch": branch,
                "target": target,
                "landed_at": "2026-08-19T00:00:00Z",
            },
        },
        "lifecycle": "session",
    })
}

/// Two commits, not one: `reconcile_repair`'s protected-path check diffs a
/// delivered commit against its own parent (`<sha>^`), which a root commit
/// does not have. The second commit is the one tests hand back as the
/// "delivered" sha.
fn init_repo(repo_path: &Path) -> String {
    git(repo_path, &["init", "-b", "main"]);
    git(repo_path, &["config", "user.email", "r@x"]);
    git(repo_path, &["config", "user.name", "R"]);
    std::fs::write(repo_path.join("README.md"), "# x\n").unwrap();
    git(repo_path, &["add", "."]);
    git(repo_path, &["commit", "-m", "init"]);
    std::fs::write(repo_path.join("README.md"), "# x, delivered\n").unwrap();
    git(repo_path, &["commit", "-am", "delivered work"]);
    git(repo_path, &["rev-parse", "HEAD"])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dry_run_previews_then_apply_closes_a_delivered_but_open_ticket_and_journals_it() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_path = repo_dir.path();
    let head = init_repo(repo_path);

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let repo_name = "repair-repo".to_string();
    client
        .call(
            "repo.add",
            json!({"name": repo_name, "path": repo_path.to_string_lossy()}),
        )
        .await
        .unwrap();

    client
        .call(
            "space.out",
            seed_delivered_but_open(&repo_name, "TKT-1", &head, "main", "rat/x/tkt-1"),
        )
        .await
        .unwrap();

    // Dry-run previews with zero mutation.
    let preview = client
        .call(
            "reconcile.repair",
            json!({"repo": repo_name, "apply": false}),
        )
        .await
        .unwrap();
    assert_eq!(preview["mode"], "dry_run");
    let results = preview["results"].as_array().cloned().unwrap_or_default();
    assert_eq!(results.len(), 1, "{preview}");
    assert_eq!(results[0]["kind"], "delivered-but-open");
    assert_eq!(results[0]["outcome"]["status"], "would_apply");

    let ticket = client
        .call("ticket.get", json!({"id": "TKT-1"}))
        .await
        .unwrap();
    assert_eq!(
        ticket["ticket"]["payload"]["status"], "in_progress",
        "dry-run must not mutate: {ticket}"
    );

    // Apply executes it for real.
    let applied = client
        .call(
            "reconcile.repair",
            json!({"repo": repo_name, "apply": true}),
        )
        .await
        .unwrap();
    let applied_results = applied["results"].as_array().cloned().unwrap_or_default();
    assert_eq!(applied_results.len(), 1, "{applied}");
    assert_eq!(applied_results[0]["outcome"]["status"], "applied");
    let journal_id = applied_results[0]["outcome"]["journal"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(!journal_id.is_empty());

    let ticket = client
        .call("ticket.get", json!({"id": "TKT-1"}))
        .await
        .unwrap();
    assert_eq!(ticket["ticket"]["payload"]["status"], "closed");

    // The journal/announcement event is durable and carries the evidence and
    // the mechanical authority tag.
    let events = client
        .call(
            "space.scan",
            json!({"category": "event", "scope": repo_name, "identity": "reconcile_repair_applied"}),
        )
        .await
        .unwrap();
    let tuples = events["tuples"].as_array().cloned().unwrap_or_default();
    assert_eq!(tuples.len(), 1, "{events}");
    assert_eq!(tuples[0]["payload"]["authority"], "mechanical");
    assert_eq!(
        tuples[0]["payload"]["violation_id"],
        "delivered-but-open:TKT-1"
    );

    // Replaying apply against fresh state is an idempotent no-op: the ticket
    // no longer trips the violation at all, so there is nothing to plan.
    let replay = client
        .call(
            "reconcile.repair",
            json!({"repo": repo_name, "apply": true}),
        )
        .await
        .unwrap();
    assert!(replay["results"].as_array().unwrap().is_empty(), "{replay}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_record_git_disputes_holds_with_zero_mutation_in_dry_run_and_apply() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_path = repo_dir.path();
    init_repo(repo_path);

    // A commit git can resolve, but that never reached main — the delivery
    // record's own claim is false.
    git(repo_path, &["checkout", "-b", "rat/x/tkt-2"]);
    std::fs::write(repo_path.join("README.md"), "# rat\n").unwrap();
    git(repo_path, &["commit", "-am", "rat work"]);
    let stray = git(repo_path, &["rev-parse", "HEAD"]);
    git(repo_path, &["checkout", "main"]);

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let repo_name = "repair-repo-ambiguous".to_string();
    client
        .call(
            "repo.add",
            json!({"name": repo_name, "path": repo_path.to_string_lossy()}),
        )
        .await
        .unwrap();
    client
        .call(
            "space.out",
            seed_delivered_but_open(&repo_name, "TKT-2", &stray, "main", "rat/x/tkt-2"),
        )
        .await
        .unwrap();

    let preview = client
        .call(
            "reconcile.repair",
            json!({"repo": repo_name, "apply": false}),
        )
        .await
        .unwrap();
    let results = preview["results"].as_array().cloned().unwrap_or_default();
    assert_eq!(results.len(), 1, "{preview}");
    assert_eq!(results[0]["outcome"]["status"], "held");
    assert_eq!(results[0]["outcome"]["reason"], "ambiguous_delivery");

    // Apply must be a no-op too: identical fresh evidence, identical hold.
    let applied = client
        .call(
            "reconcile.repair",
            json!({"repo": repo_name, "apply": true}),
        )
        .await
        .unwrap();
    let applied_results = applied["results"].as_array().cloned().unwrap_or_default();
    assert_eq!(applied_results.len(), 1, "{applied}");
    assert_eq!(applied_results[0]["outcome"]["status"], "held");

    let ticket = client
        .call("ticket.get", json!({"id": "TKT-2"}))
        .await
        .unwrap();
    assert_eq!(
        ticket["ticket"]["payload"]["status"], "in_progress",
        "a disputed delivery record must never be auto-closed: {ticket}"
    );

    let events = client
        .call(
            "space.scan",
            json!({"category": "event", "scope": repo_name, "identity": "reconcile_repair_applied"}),
        )
        .await
        .unwrap();
    assert!(
        events["tuples"].as_array().unwrap().is_empty(),
        "a held item must never journal a repair that did not happen: {events}"
    );
}

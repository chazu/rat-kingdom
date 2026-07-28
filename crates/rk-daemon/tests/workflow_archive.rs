//! TKT-177 end to end over the socket: a failed workflow instance has an `rk`
//! path off `rk inbox`.
//!
//! `rk inbox` surfaces every `status: failed` instance, but nothing retired
//! one: `rk prune` only archived AGENT records, `rk workflow` had no
//! rm/archive/ack verb, and the inbox row's only action was the inspect-only
//! `rk workflow status`. Clearing the board meant stopping the daemon and
//! moving `workflow-instances/*.json` aside by hand. These tests pin the
//! contract of the fix: only settled instances may leave the live view, a
//! running one never does, the run itself survives the move, the clear holds
//! across a restart, and the whole thing round-trips back.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

/// One `wait` with nothing to wait for: fails inline ("wait step with no active
/// agent"), so the instance settles `Failed` with no harness in play.
const FLOPS: &str = r#"
workflow: {
    name: "flops"
    steps: [{type: "wait", timeout: "1s"}]
}
"#;

/// One short timer gate: settles `Completed` on its own.
const WORKS: &str = r#"
workflow: {
    name: "works"
    steps: [{type: "gate", gateType: "timer", duration: "1s"}]
}
"#;

/// One long timer gate: stays `Running` for the whole test — the guardrail's
/// subject, since an in-flight run must never be prunable.
const PARKS: &str = r#"
workflow: {
    name: "parks"
    steps: [{type: "gate", gateType: "timer", duration: "600s"}]
}
"#;

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

/// A scratch repo carrying all three definitions in its repo-local workflows
/// dir.
fn init_repo() -> tempfile::TempDir {
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-b", "main"]);
    git(repo.path(), &["config", "user.email", "r@x"]);
    git(repo.path(), &["config", "user.name", "R"]);
    std::fs::write(repo.path().join("README.md"), "# x\n").unwrap();
    git(repo.path(), &["add", "."]);
    git(repo.path(), &["commit", "-m", "init"]);

    let wf_dir = repo.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("flops.cue"), FLOPS).unwrap();
    std::fs::write(wf_dir.join("works.cue"), WORKS).unwrap();
    std::fs::write(wf_dir.join("parks.cue"), PARKS).unwrap();
    repo
}

async fn connect(layout: &Layout) -> Client {
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

async fn run(client: &mut Client, repo: &Path, name: &str) -> String {
    let started = client
        .call(
            "workflow.run",
            json!({"name": name, "repo": repo.to_string_lossy(), "params": {}}),
        )
        .await
        .unwrap();
    started["instance"]["id"].as_str().unwrap().to_string()
}

async fn wait_status(client: &mut Client, id: &str, want: &str) {
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        if status["instance"]["status"] == want {
            return;
        }
    }
    panic!("instance {id} never reached status {want}");
}

async fn list(client: &mut Client, params: Value) -> Vec<Value> {
    client.call("workflow.list", params).await.unwrap()["instances"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

fn ids(instances: &[Value]) -> Vec<String> {
    let mut out: Vec<String> = instances
        .iter()
        .map(|i| i["id"].as_str().unwrap_or("?").to_string())
        .collect();
    out.sort();
    out
}

async fn inbox_subjects(client: &mut Client, kind: &str) -> Vec<String> {
    client.call("inbox.list", json!({})).await.unwrap()["items"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter(|i| i["kind"] == kind)
        .map(|i| i["subject"].as_str().unwrap_or("?").to_string())
        .collect()
}

/// Wait for a stopped daemon to release its socket so a successor can bind.
async fn wait_socket_gone(layout: &Layout) {
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if !layout.socket_path().exists() {
            return;
        }
    }
    panic!("daemon did not release its socket on shutdown");
}

/// The whole targeted capability in one pass: the failed instance shows up in
/// `rk inbox` carrying a prune action, a dry run previews without mutating, the
/// real prune moves ONLY the named instance, the live list and the inbox both
/// drop it while `--archived`/`--all` surface it, its error and step trace
/// survive, and `workflow.unarchive` puts it back.
#[tokio::test]
async fn prune_clears_a_failed_instance_and_round_trips() {
    let home = tempfile::tempdir().unwrap();
    let repo = init_repo();
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let failed = run(&mut client, repo.path(), "flops").await;
    wait_status(&mut client, &failed, "failed").await;
    let running = run(&mut client, repo.path(), "parks").await;

    // The row this ticket is about, and the action it now carries.
    let row = client.call("inbox.list", json!({})).await.unwrap()["items"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .find(|i| i["kind"] == "workflow-failed" && i["subject"] == json!(&failed))
        .expect("the failed instance should be in the inbox");
    let action = row["action"].as_str().unwrap_or_default().to_string();
    assert!(
        action.contains(&format!("rk workflow prune {failed}")),
        "the inbox row must carry a command that RESOLVES it, got: {action}"
    );

    // A running instance is never eligible, by id or by window.
    let refused = client
        .call("workflow.archive", json!({"ids": [&running]}))
        .await;
    assert!(
        refused.is_err(),
        "pruning an in-flight instance must be refused, not silently skipped"
    );
    let unknown = client
        .call("workflow.archive", json!({"ids": ["wf-nope"]}))
        .await;
    assert!(unknown.is_err(), "an unknown id must be an error, not a no-op");

    // Dry run: reports the instance, touches nothing.
    let preview = client
        .call(
            "workflow.archive",
            json!({"ids": [&failed], "dry_run": true}),
        )
        .await
        .unwrap();
    assert_eq!(preview["dry_run"], true);
    assert_eq!(preview["count"], 1);
    assert_eq!(ids(preview["instances"].as_array().unwrap()), vec![&failed]);
    assert_eq!(
        ids(&list(&mut client, json!({})).await),
        ids(&[json!({"id": &failed}), json!({"id": &running})]),
        "a dry run must not mutate the instance store"
    );

    // The real thing.
    let pruned = client
        .call("workflow.archive", json!({"ids": [&failed]}))
        .await
        .unwrap();
    assert_eq!(pruned["count"], 1);
    assert!(
        pruned["instances"][0]["archived_at"].is_string(),
        "a pruned instance carries its archived_at stamp"
    );

    // Live view: only the running instance. The inbox row is gone with it.
    assert_eq!(ids(&list(&mut client, json!({})).await), vec![running.clone()]);
    assert!(
        inbox_subjects(&mut client, "workflow-failed")
            .await
            .is_empty(),
        "pruning must clear the inbox row it was raised for"
    );

    // Opt in and it is back in view, flagged.
    assert_eq!(
        ids(&list(&mut client, json!({"archived": true})).await),
        vec![failed.clone()]
    );
    assert_eq!(
        ids(&list(&mut client, json!({"all": true})).await),
        ids(&[json!({"id": &failed}), json!({"id": &running})])
    );

    // Nothing was deleted: the run still reads, error and all, and its step
    // trace still renders.
    let status = client
        .call("workflow.status", json!({"name": &failed}))
        .await
        .unwrap();
    assert_eq!(status["instance"]["status"], "failed");
    assert!(
        status["instance"]["error"]
            .as_str()
            .unwrap_or_default()
            .contains("no active agent"),
        "the failure text must survive the prune: {}",
        status["instance"]["error"]
    );
    let timeline = client
        .call("workflow.timeline", json!({"name": &failed}))
        .await
        .unwrap();
    assert_eq!(timeline["steps"].as_array().map(Vec::len), Some(1));

    // ...and it round-trips.
    let restored = client
        .call("workflow.unarchive", json!({"name": &failed}))
        .await
        .unwrap();
    assert!(restored["instance"]["archived_at"].is_null());
    assert_eq!(
        ids(&list(&mut client, json!({})).await),
        ids(&[json!({"id": &failed}), json!({"id": &running})])
    );
    assert_eq!(
        inbox_subjects(&mut client, "workflow-failed").await,
        vec![failed.clone()],
        "an unarchived failure is a live problem again"
    );
    assert!(
        list(&mut client, json!({"archived": true})).await.is_empty(),
        "unarchiving must empty the archive store, not copy out of it"
    );
}

/// The clear has to HOLD. Hand-moving the JSON worked only because the daemon
/// was stopped; a prune that the next `rehydrate` undid would put every row
/// straight back on the board.
#[tokio::test]
async fn pruned_instance_stays_pruned_across_a_restart() {
    let home = tempfile::tempdir().unwrap();
    let repo = init_repo();
    let layout = Layout::at(home.path());

    let daemon_a = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;
    let failed = run(&mut client, repo.path(), "flops").await;
    wait_status(&mut client, &failed, "failed").await;
    client
        .call("workflow.archive", json!({"ids": [&failed]}))
        .await
        .unwrap();

    // The snapshot moved on disk rather than being deleted — the archive store
    // is what `rk workflow list --archived` reads back.
    let live_file = home
        .path()
        .join("workflow-instances")
        .join(format!("{failed}.json"));
    let archived_file = home
        .path()
        .join("workflow-instances-archive")
        .join(format!("{failed}.json"));
    assert!(!live_file.exists(), "the live snapshot should be gone");
    assert!(archived_file.exists(), "the run should be preserved, not deleted");

    client.call("stop", json!({})).await.ok();
    wait_socket_gone(&layout).await;
    let _ = handle_a.await;

    // Fresh daemon, same home.
    let daemon_b = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;

    assert!(
        list(&mut client, json!({})).await.is_empty(),
        "a pruned instance must not come back live on restart"
    );
    assert!(
        inbox_subjects(&mut client, "workflow-failed")
            .await
            .is_empty(),
        "...nor reappear in the inbox"
    );
    assert_eq!(
        ids(&list(&mut client, json!({"archived": true})).await),
        vec![failed.clone()],
        "it rehydrates into the archive store, still readable"
    );
}

/// `rk prune` is the operator's "clear what's settled" gesture; after TKT-177
/// it clears both halves of the board in one pass, on one window.
#[tokio::test]
async fn agent_prune_sweeps_settled_instances_too() {
    let home = tempfile::tempdir().unwrap();
    let repo = init_repo();
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let failed = run(&mut client, repo.path(), "flops").await;
    let completed = run(&mut client, repo.path(), "works").await;
    let running = run(&mut client, repo.path(), "parks").await;
    wait_status(&mut client, &failed, "failed").await;
    wait_status(&mut client, &completed, "completed").await;
    let settled = ids(&[json!({"id": &failed}), json!({"id": &completed})]);

    // The default window is 7d, so a run that settled seconds ago is not yet
    // eligible — the sweep is age-based for instances exactly as it is for
    // agent records.
    let untouched = client.call("agent.archive", json!({})).await.unwrap();
    assert_eq!(
        untouched["instances"].as_array().map(Vec::len),
        Some(0),
        "a fresh instance must not fall inside the default 7d window"
    );

    let preview = client
        .call("agent.archive", json!({"all": true, "dry_run": true}))
        .await
        .unwrap();
    assert_eq!(
        ids(preview["instances"].as_array().unwrap()),
        settled,
        "the preview must list both settled instances and neither running one"
    );
    assert_eq!(
        ids(&list(&mut client, json!({})).await).len(),
        3,
        "a dry run must not mutate the instance store"
    );

    let swept = client
        .call("agent.archive", json!({"all": true}))
        .await
        .unwrap();
    assert_eq!(ids(swept["instances"].as_array().unwrap()), settled);
    assert_eq!(
        ids(&list(&mut client, json!({})).await),
        vec![running.clone()],
        "only the in-flight instance survives the sweep"
    );
    assert_eq!(
        ids(&list(&mut client, json!({"archived": true})).await),
        settled
    );
}

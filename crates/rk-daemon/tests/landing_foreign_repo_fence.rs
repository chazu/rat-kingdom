//! Regression coverage for TKT-kavok-didit-nojid: the reactor's `action:
//! "land"` dispatch (`Reactor::fire_land_action`, `crates/rk-daemon/src/reactor.rs`)
//! must refuse to enqueue a landing candidate whose resolved destination repo
//! disagrees with the completion's own authoritative source repo
//! (`tuple.scope`, stamped by `Supervisor::route_completion`).
//!
//! The live incident: Glossolalia's deployed `.rk/triggers.cue` retained an
//! unscoped `action: "land"` trigger (no `repo:` override, no `match.scope`).
//! Reactor::try_fire's repo-resolution fallback (`trigger.repo` ->
//! `loaded.source_repo` -> `tuple.scope`) picked Glossolalia purely because
//! that is where the trigger file lives, regardless of which repo actually
//! produced the completion — silently enqueuing two Rat Kingdom completions'
//! branch/head/task into Glossolalia's `LandingQueue`, spinning
//! `running_gates` forever since those branches never existed there.

mod support;

use rk_core::id::SpawnId;
use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::time::Duration;
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

fn init_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    support::install_default_repository_policy(dir);
}

fn write_checks(repo: &Path, src: &str) {
    let rk_dir = repo.join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(rk_dir.join("checks.cue"), src).unwrap();
    git(repo, &["add", ".rk/checks.cue"]);
    git(repo, &["commit", "-m", "add checks registry"]);
}

const FAST_CHECKS: &str = r#"
checks: [
    {name: "landing-protected-paths", command: "true", timeout: "30s"},
    {name: "landing-diff-scope", command: "true", timeout: "30s"},
    {name: "verify", command: "true", timeout: "30s"},
]
"#;

fn make_branch(repo: &Path, name: &str, file: &str, content: &str) -> String {
    git(repo, &["checkout", "-b", name]);
    std::fs::write(repo.join(file), content).unwrap();
    git(repo, &["add", file]);
    git(repo, &["commit", "-m", "work"]);
    let sha = git(repo, &["rev-parse", "HEAD"]);
    git(repo, &["checkout", "main"]);
    sha
}

/// The exact shape of the deployed hazard: no `repo:` override, no
/// `match.scope` — the fallback chain resolves the destination purely from
/// wherever this file happens to be deployed.
const UNSCOPED_LAND_TRIGGER: &str = r#"
triggers: [
    {
        name:   "steward-landing-on-completion"
        action: "land"
        match: {category: "event", identity: "harness_result", search: "\"role\":\"rat\""}
        maxFires: 20
    },
]
"#;

async fn emit_harness_result(
    client: &mut Client,
    repo_name: &str,
    branch: &str,
    head_sha: &str,
    task: &str,
) {
    let spawn = SpawnId::new();
    client
        .call(
            "space.out",
            json!({
                "category": "event",
                "scope": repo_name,
                "identity": "harness_result",
                "payload": {
                    "agent": "rat-1",
                    "spawn": spawn.to_string(),
                    "role": "rat",
                    "task": task,
                    "branch": branch,
                    "target": "main",
                    "parent": Value::Null,
                    "is_error": false,
                    "head_sha": head_sha,
                    "diff_files": 1,
                    "diff_lines": 1,
                    "diff_class": "trivial",
                    "declared_done": true,
                    "result": "done",
                    "cost_usd": 0.0,
                    "tokens": 0,
                },
            }),
        )
        .await
        .unwrap();
}

async fn queue_entries(client: &mut Client, repo_name: &str) -> Vec<Value> {
    let res = client
        .call(
            "space.scan",
            json!({"category": "event", "scope": repo_name, "identity": "landing_queue_entry"}),
        )
        .await
        .unwrap();
    res["tuples"].as_array().cloned().unwrap_or_default()
}

async fn processed_markers(client: &mut Client, repo_name: &str) -> Vec<Value> {
    let res = client
        .call(
            "space.scan",
            json!({"category": "event", "scope": repo_name, "identity": "landing_processed"}),
        )
        .await
        .unwrap();
    res["tuples"].as_array().cloned().unwrap_or_default()
}

async fn gave_up_obstacles(client: &mut Client) -> Vec<Value> {
    let res = client
        .call(
            "space.scan",
            json!({"category": "obstacle", "identity": "reactor_fire_gave_up"}),
        )
        .await
        .unwrap();
    res["tuples"].as_array().cloned().unwrap_or_default()
}

/// Wakes the reactor's feed-driven loop without advancing anything it
/// actually cares about: the cursor does not advance past our target tuple
/// while `try_fire` keeps returning a retryable `Err`, so every extra wake
/// re-evaluates it and burns another of `MAX_FIRE_ATTEMPTS` (5) — this is how
/// the test reaches the "gave up, wrote a diagnostic" terminal state without
/// waiting out the real ~30s fallback interval tick.
async fn nudge_reactor(client: &mut Client) {
    client
        .call(
            "space.out",
            json!({"category": "obstacle", "scope": "system", "identity": "test_nudge", "payload": {}}),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
}

/// The Glossolalia incident, reproduced exactly: two registered repos, only
/// the ATTACKER carries a repo-local `action: "land"` trigger, and it is
/// unscoped. A completion that actually happened in the VICTIM repo must
/// never enqueue into the attacker's `LandingQueue` — not even transiently —
/// and the reactor must surface a bounded diagnostic (a `reactor_fire_gave_up`
/// obstacle) rather than silently dropping it or retrying forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repo_local_unscoped_land_trigger_never_captures_a_foreign_repos_completion() {
    let home = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();

    let victim = root.path().join("victim-repo");
    std::fs::create_dir(&victim).unwrap();
    init_repo(&victim);
    let head_sha = make_branch(&victim, "rat/x/tkt-1", "work.txt", "v1\n");

    let attacker = root.path().join("attacker-repo");
    std::fs::create_dir(&attacker).unwrap();
    init_repo(&attacker);
    std::fs::write(
        attacker.join(".rk").join("triggers.cue"),
        UNSCOPED_LAND_TRIGGER,
    )
    .unwrap();

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let victim_name = "victim-repo".to_string();
    let attacker_name = "attacker-repo".to_string();
    client
        .call(
            "repo.add",
            json!({"name": &victim_name, "path": victim.to_string_lossy()}),
        )
        .await
        .unwrap();
    client
        .call(
            "repo.add",
            json!({"name": &attacker_name, "path": attacker.to_string_lossy()}),
        )
        .await
        .unwrap();

    emit_harness_result(&mut client, &victim_name, "rat/x/tkt-1", &head_sha, "tkt-1").await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Never even transiently: no queue entry appears in the attacker's
    // scope at any point, not just "eventually empty".
    assert!(
        queue_entries(&mut client, &attacker_name).await.is_empty(),
        "a foreign completion must never enqueue into the wrong repo's landing queue"
    );
    assert!(
        queue_entries(&mut client, &victim_name).await.is_empty(),
        "the victim repo has no trigger of its own in this test and must stay empty too"
    );

    // Drive enough extra reactor wakes to exhaust MAX_FIRE_ATTEMPTS and reach
    // the bounded give-up diagnostic.
    for _ in 0..8 {
        if !gave_up_obstacles(&mut client).await.is_empty() {
            break;
        }
        nudge_reactor(&mut client).await;
    }

    let gave_up = gave_up_obstacles(&mut client).await;
    assert!(
        !gave_up.is_empty(),
        "a repo-identity mismatch must surface a bounded diagnostic instead of silently vanishing"
    );
    assert!(
        gave_up.iter().any(|o| o["payload"]["reason"]
            .as_str()
            .unwrap_or("")
            .contains("cross-repo landing candidate")),
        "{gave_up:?}"
    );

    // Still, after giving up, never a queue entry.
    assert!(queue_entries(&mut client, &attacker_name).await.is_empty());

    handle.abort();
}

/// The legitimate counterpart (acceptance: "preserve legitimate same-repo
/// global/local feeds"): a repo-local `action: "land"` trigger with the
/// identical unscoped shape must still land a completion that DID happen in
/// its own repo — the fence compares the resolved destination against the
/// tuple's own scope, and for a same-repo completion those always agree.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repo_local_unscoped_land_trigger_still_lands_its_own_repos_completion() {
    let home = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();

    let repo = root.path().join("solo-repo");
    std::fs::create_dir(&repo).unwrap();
    init_repo(&repo);
    write_checks(&repo, FAST_CHECKS);
    std::fs::write(repo.join(".rk").join("triggers.cue"), UNSCOPED_LAND_TRIGGER).unwrap();
    let head_sha = make_branch(&repo, "rat/x/tkt-2", "work.txt", "v1\n");

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let repo_name = "solo-repo".to_string();
    client
        .call(
            "repo.add",
            json!({"name": &repo_name, "path": repo.to_string_lossy()}),
        )
        .await
        .unwrap();

    emit_harness_result(&mut client, &repo_name, "rat/x/tkt-2", &head_sha, "tkt-2").await;

    let mut merged = false;
    for _ in 0..200 {
        if !processed_markers(&mut client, &repo_name).await.is_empty()
            && queue_entries(&mut client, &repo_name).await.is_empty()
        {
            merged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert!(
        merged,
        "an unscoped repo-local land trigger must still land a completion from its own repo"
    );
    assert!(gave_up_obstacles(&mut client).await.is_empty());

    handle.abort();
}

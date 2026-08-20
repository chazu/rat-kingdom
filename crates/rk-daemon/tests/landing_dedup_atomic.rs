//! Live reproduction, 2026-08-20: a rat resumed after a paused background
//! verifier, observed its earlier `task_done`, and called `rk done` again.
//! The daemon admitted two live landing entries (seq 126 and seq 127) for
//! the identical branch, head, target, and task.
//!
//! `LandingPipeline::enqueue_disposition` (`crates/rk-daemon/src/landing.rs`)
//! already guards admission with a `Mutex`-held read-then-write: a fresh
//! candidate is checked against both a terminal `landing_processed` marker
//! and `LandingQueue::contains_work_key`, which scans every LIVE queue tuple
//! regardless of status (`Queued`, `RunningGates`, `AwaitingReview`,
//! `Landing`) for the same `(branch, target, head_sha, task)`. This test
//! file proves that guard end to end, through the live daemon, for exactly
//! the shapes the ticket's acceptance criteria name: sequential retry,
//! genuine concurrent retry, retry while the first entry is running gates,
//! restart, and that a different head or task remains independently
//! admissible.

mod support;

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
}

/// Commits `.rk/checks.cue` onto `main` before any branch forks off it, so
/// both a candidate branch and the daemon's detached gate worktree carry it
/// (`landing.rs`'s `write_checks` doc comment: commit order matters here).
fn write_checks(repo: &Path, src: &str) {
    let rk_dir = repo.join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(rk_dir.join("checks.cue"), src).unwrap();
    git(repo, &["add", ".rk/checks.cue"]);
    git(repo, &["commit", "-m", "add checks registry"]);
}

const FAST_CHECKS: &str = r#"
checks: [
    {name: "steward-protected-paths", command: "true", timeout: "30s"},
    {name: "steward-diff-scope", command: "true", timeout: "30s"},
    {name: "verify", command: "true", timeout: "30s"},
]
"#;

/// A hand-built branch with a single commit, forked from `main` and left in
/// place (not deleted) so its `head_sha` stays resolvable by callers that
/// address it directly. Returns the new commit's sha.
fn make_branch(repo: &Path, name: &str, file: &str, content: &str) -> String {
    git(repo, &["checkout", "-b", name]);
    std::fs::write(repo.join(file), content).unwrap();
    git(repo, &["add", file]);
    git(repo, &["commit", "-m", "work"]);
    let sha = git(repo, &["rev-parse", "HEAD"]);
    git(repo, &["checkout", "main"]);
    sha
}

fn repo_name_of(repo: &Path) -> String {
    repo.file_name().unwrap().to_string_lossy().to_string()
}

/// The daemon-native landing pipeline's completion feed, reproduced here
/// (not read from `examples/`) so this test does not depend on that file's
/// path or content staying byte-identical — same rationale as
/// `live_landing_restart.rs`/`live_landing_burst.rs`.
const LANDING_TRIGGER: &str = r#"
triggers: [
    {
        name:   "landing-on-completion"
        action: "land"
        match: {category: "event", identity: "harness_result", search: "\"role\":\"rat\""}
        maxFires: 20
    },
]
"#;

/// Hand-crafts a `harness_result` completion event exactly as
/// `Supervisor::route_completion` would, and writes it straight to the
/// space over `space.out` as the operator — this is what lets a test drive
/// the reactor's `action: "land"` path (the ticket's actual incident path)
/// without needing to reproduce the full respawn/pause machinery that
/// produced the duplicate `task_done` in the field.
async fn emit_harness_result(
    client: &mut Client,
    repo_name: &str,
    agent: &str,
    branch: &str,
    target: &str,
    head_sha: &str,
    task: &str,
    diff_class: &str,
) {
    client
        .call(
            "space.out",
            json!({
                "category": "event",
                "scope": repo_name,
                "identity": "harness_result",
                "payload": {
                    "agent": agent,
                    "spawn": format!("spawn-{agent}"),
                    "role": "rat",
                    "task": task,
                    "branch": branch,
                    "target": target,
                    "parent": Value::Null,
                    "is_error": false,
                    "head_sha": head_sha,
                    "diff_files": 1,
                    "diff_lines": 1,
                    "diff_class": diff_class,
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

async fn wait_until_queue_empty(client: &mut Client, repo_name: &str, iters: usize) -> bool {
    for _ in 0..iters {
        if queue_entries(client, repo_name).await.is_empty() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    false
}

/// Unlike [`wait_until_queue_empty`], safe to use as the FIRST wait after
/// firing a completion: polling queue emptiness alone can false-positive if
/// it happens to check before the reactor has enqueued anything yet. This
/// only returns `true` once real terminal progress (a processed marker) has
/// actually been observed.
async fn wait_until_marker_count(
    client: &mut Client,
    repo_name: &str,
    count: usize,
    iters: usize,
) -> bool {
    for _ in 0..iters {
        if processed_markers(client, repo_name).await.len() >= count {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    false
}

async fn wait_until_status(
    client: &mut Client,
    repo_name: &str,
    status: &str,
    iters: usize,
) -> bool {
    for _ in 0..iters {
        let entries = queue_entries(client, repo_name).await;
        if entries.iter().any(|e| e["payload"]["status"] == status) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

/// Sequential retry: the exact incident shape. A completion lands for real,
/// then an identical redelivered completion (the "observed its earlier
/// task_done and called rk done again" resumption) arrives afterward. It
/// must dedupe to zero effect — no second queue entry, no second processed
/// marker, no second merge.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequential_duplicate_reactor_completions_converge_to_one_live_entry() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path().join("dedup-seq");
    std::fs::create_dir(&repo).unwrap();
    init_repo(&repo);
    write_checks(&repo, FAST_CHECKS);
    std::fs::write(repo.join(".rk").join("triggers.cue"), LANDING_TRIGGER).unwrap();

    let repo_name = repo_name_of(&repo);
    let head_sha = make_branch(&repo, "rat/dedup-seq/tkt-1", "work.txt", "v1\n");

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "dedup-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": &repo_name, "path": repo.to_string_lossy()}),
        )
        .await
        .unwrap();

    emit_harness_result(
        &mut client,
        &repo_name,
        "Deyna-9",
        "rat/dedup-seq/tkt-1",
        "main",
        &head_sha,
        "dedup-seq-task",
        "trivial",
    )
    .await;

    assert!(
        wait_until_marker_count(&mut client, &repo_name, 1, 200).await,
        "the first completion never reached a processed marker"
    );
    assert!(
        wait_until_queue_empty(&mut client, &repo_name, 50).await,
        "the first completion never drained the queue"
    );
    let markers_after_first = processed_markers(&mut client, &repo_name).await;
    assert_eq!(markers_after_first.len(), 1, "{markers_after_first:?}");
    let main_after_first = git(&repo, &["rev-parse", "main"]);

    // The resumed background verifier's redundant `rk done`: a second,
    // distinct `harness_result` tuple for the identical work key.
    emit_harness_result(
        &mut client,
        &repo_name,
        "Deyna-9",
        "rat/dedup-seq/tkt-1",
        "main",
        &head_sha,
        "dedup-seq-task",
        "trivial",
    )
    .await;

    // Give the feed-woken reactor a beat to evaluate the duplicate.
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert!(
        queue_entries(&mut client, &repo_name).await.is_empty(),
        "a redelivered completion must never leave a live queue entry behind"
    );
    let markers_after_second = processed_markers(&mut client, &repo_name).await;
    assert_eq!(
        markers_after_second.len(),
        1,
        "a redelivered completion must not create a second processed marker: {markers_after_second:?}"
    );
    let main_after_second = git(&repo, &["rev-parse", "main"]);
    assert_eq!(
        main_after_first, main_after_second,
        "a redelivered completion must not merge a second time"
    );

    handle.abort();
}

/// Concurrent retry: two `repo.land` submissions for the identical
/// branch/head/target/task fired over separate RPC connections without
/// either awaiting the other first — genuine concurrency at the admission
/// mutex, not a sequential loop that happens to look concurrent. Must still
/// converge to exactly one live landing entry (one processed marker, one
/// closed ticket, one merge).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_duplicate_land_submissions_produce_one_live_landing_entry() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path().join("dedup-concurrent");
    std::fs::create_dir(&repo).unwrap();
    init_repo(&repo);
    write_checks(&repo, FAST_CHECKS);
    let repo_name = repo_name_of(&repo);

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "dedup-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let ticket = client
        .call(
            "ticket.new",
            json!({"title": "concurrent retry", "scope": &repo_name}),
        )
        .await
        .unwrap();
    let ticket_id = ticket["ticket"]["identity"].as_str().unwrap().to_string();

    // `keep_branch: true` so the branch/head survive whichever submission
    // wins — the loser's `repo.land` call still needs to resolve the
    // branch's head_sha to build its own candidate entry.
    let branch = "rat/dedup-concurrent/tkt-1".to_string();
    git(&repo, &["checkout", "-b", &branch]);
    std::fs::write(repo.join("recovered.txt"), "recovered\n").unwrap();
    git(&repo, &["add", "recovered.txt"]);
    git(&repo, &["commit", "-m", "recovery"]);
    git(&repo, &["checkout", "main"]);

    let repo_str = repo.to_string_lossy().to_string();
    let mut client_a = connect(&layout).await;
    let mut client_b = connect(&layout).await;

    let params = json!({
        "repo": repo_str,
        "branch": branch,
        "target": "main",
        "task": ticket_id,
        "keep_branch": true,
    });
    let params_b = params.clone();

    let (first, second) = tokio::join!(
        client_a.call("repo.land", params),
        client_b.call("repo.land", params_b),
    );
    let first = first.unwrap();
    let second = second.unwrap();

    assert!(
        first["merged"] == true || second["merged"] == true,
        "at least one concurrent submission must land: {first} / {second}"
    );

    assert!(
        wait_until_queue_empty(&mut client, &repo_name, 200).await,
        "the live queue never drained after the concurrent retry"
    );
    let markers = processed_markers(&mut client, &repo_name).await;
    assert_eq!(
        markers.len(),
        1,
        "a concurrent duplicate retry must converge to exactly one processed marker: {markers:?}"
    );
    assert_eq!(markers[0]["payload"]["outcome"], "landed", "{markers:?}");

    let ticket_after = client
        .call("ticket.get", json!({"id": ticket_id}))
        .await
        .unwrap();
    assert_eq!(
        ticket_after["ticket"]["payload"]["status"], "closed",
        "the ticket must be closed exactly once: {ticket_after}"
    );

    handle.abort();
}

/// Retry while the first entry is running gates: a duplicate completion
/// arrives while the live entry is durably `running_gates`, not merely
/// `queued`. Must converge without a second gate run — the acceptance
/// criterion "without a second gate or reviewer" made concrete via a
/// counter file the `verify` check increments on every invocation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_while_running_gates_converges_without_a_second_gate_run() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path().join("dedup-midgate");
    std::fs::create_dir(&repo).unwrap();
    init_repo(&repo);

    let barrier_dir = tempfile::tempdir().unwrap();
    let run_count = barrier_dir.path().join("run-count.log");
    let checks = format!(
        r#"
checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "echo run >> \"{log}\"; sleep 0.6 && true", timeout: "30s"}},
]
"#,
        log = run_count.display(),
    );
    write_checks(&repo, &checks);
    std::fs::write(repo.join(".rk").join("triggers.cue"), LANDING_TRIGGER).unwrap();

    let repo_name = repo_name_of(&repo);
    let head_sha = make_branch(&repo, "rat/dedup-midgate/tkt-1", "work.txt", "v1\n");

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "dedup-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": &repo_name, "path": repo.to_string_lossy()}),
        )
        .await
        .unwrap();

    emit_harness_result(
        &mut client,
        &repo_name,
        "Deyna-9",
        "rat/dedup-midgate/tkt-1",
        "main",
        &head_sha,
        "dedup-midgate-task",
        "trivial",
    )
    .await;

    assert!(
        wait_until_status(&mut client, &repo_name, "running_gates", 150).await,
        "candidate never reached running_gates before the retry"
    );

    // The redelivered completion, fired while the original is still
    // durably `running_gates` — proof the dedup check sees non-terminal
    // live entries, not just completed ones.
    emit_harness_result(
        &mut client,
        &repo_name,
        "Deyna-9",
        "rat/dedup-midgate/tkt-1",
        "main",
        &head_sha,
        "dedup-midgate-task",
        "trivial",
    )
    .await;

    assert!(
        wait_until_queue_empty(&mut client, &repo_name, 300).await,
        "the candidate never drained after the mid-gate retry"
    );

    let markers = processed_markers(&mut client, &repo_name).await;
    assert_eq!(
        markers.len(),
        1,
        "a mid-gate retry must converge to exactly one processed marker: {markers:?}"
    );

    let runs = std::fs::read_to_string(&run_count).unwrap_or_default();
    let run_lines = runs.lines().count();
    assert_eq!(
        run_lines, 1,
        "a mid-gate retry must not run the gate a second time: {runs:?}"
    );

    handle.abort();
}

/// Restart preserves the invariant: a completion lands for real under
/// daemon A, which is then killed and replaced by a fresh `Daemon` over the
/// SAME on-disk home — and only then does the duplicate completion arrive.
/// Matches the field incident's actual shape: the resumed rat's redundant
/// `rk done` is observed well after the original work already landed, with
/// a daemon restart in between. Proves the durable `landing_processed`
/// marker (not just the in-process mutex, which resets on restart) is what
/// closes the race.
///
/// Deliberately does NOT kill mid-gate: `Daemon::new`'s reactor and
/// landing-pipeline consumer loops are independent `tokio::spawn` tasks, not
/// children of the `Daemon::run()` future, so `handle_a.abort()` cancels
/// only the latter — a gate run already in flight under daemon A would keep
/// running as an orphaned task on the SAME test-process runtime even after
/// the "kill", free to race daemon B's own resume (two independent
/// `LandingPipeline` instances, each with its own in-process locks and
/// neither aware of the other). Only a real process crash — not a
/// same-process task abort — actually prevents that, so it is a
/// test-harness artifact rather than the dedup gap this file exists to
/// prove; `live_landing_restart.rs` already separately covers a mid-gate
/// kill resuming correctly. Settling before the kill sidesteps it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_preserves_the_landing_dedup_invariant() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path().join("dedup-restart");
    std::fs::create_dir(&repo).unwrap();
    init_repo(&repo);
    write_checks(&repo, FAST_CHECKS);
    let repo_name = repo_name_of(&repo);
    let head_sha = make_branch(&repo, "rat/dedup-restart/tkt-1", "work.txt", "v1\n");
    let main_before = git(&repo, &["rev-parse", "main"]);

    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    std::fs::create_dir_all(layout.triggers_dir()).unwrap();
    std::fs::write(layout.triggers_dir().join("landing.cue"), LANDING_TRIGGER).unwrap();

    let mut config = rk_core::config::Config::default();
    config.harness.default = "fake".into();

    let daemon_a = Daemon::new(layout.clone(), &config).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": &repo_name, "path": repo.to_string_lossy()}),
        )
        .await
        .unwrap();

    emit_harness_result(
        &mut client,
        &repo_name,
        "Deyna-9",
        "rat/dedup-restart/tkt-1",
        "main",
        &head_sha,
        "dedup-restart-task",
        "trivial",
    )
    .await;

    assert!(
        wait_until_marker_count(&mut client, &repo_name, 1, 200).await,
        "the candidate never landed under daemon A"
    );
    assert!(
        wait_until_queue_empty(&mut client, &repo_name, 50).await,
        "the candidate never drained under daemon A"
    );
    let main_after = git(&repo, &["rev-parse", "main"]);
    assert_ne!(
        main_before, main_after,
        "the branch must have landed onto main under daemon A"
    );

    handle_a.abort();
    let _ = handle_a.await;
    std::fs::remove_file(layout.pid_file()).ok();
    std::fs::remove_file(layout.socket_path()).ok();

    let daemon_b = Daemon::new(layout.clone(), &config).unwrap();
    let handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;

    // The resumed rat's redundant `rk done`, arriving only now — after a
    // full daemon restart, with the original work already durably landed.
    emit_harness_result(
        &mut client,
        &repo_name,
        "Deyna-9",
        "rat/dedup-restart/tkt-1",
        "main",
        &head_sha,
        "dedup-restart-task",
        "trivial",
    )
    .await;

    tokio::time::sleep(Duration::from_millis(400)).await;

    let markers = processed_markers(&mut client, &repo_name).await;
    assert_eq!(
        markers.len(),
        1,
        "a post-restart retry must not create a second processed marker: {markers:?}"
    );
    assert!(
        queue_entries(&mut client, &repo_name).await.is_empty(),
        "a post-restart retry must never leave a live queue entry behind"
    );
    let main_after_retry = git(&repo, &["rev-parse", "main"]);
    assert_eq!(
        main_after, main_after_retry,
        "a post-restart retry must not merge a second time"
    );

    handle_b.abort();
    let _ = handle_b.await;
}

/// A different head under the SAME task must remain independently
/// admissible — the dedup key is `(branch, target, head_sha, task)`
/// together, not `task` alone. Two distinct branches/heads under one
/// ticket both land, producing two processed markers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_different_head_under_the_same_task_is_independently_admissible() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path().join("dedup-distinct-head");
    std::fs::create_dir(&repo).unwrap();
    init_repo(&repo);
    write_checks(&repo, FAST_CHECKS);
    std::fs::write(repo.join(".rk").join("triggers.cue"), LANDING_TRIGGER).unwrap();

    let repo_name = repo_name_of(&repo);
    let head_a = make_branch(&repo, "rat/dedup-distinct-head/tkt-1a", "a.txt", "a\n");
    let head_b = make_branch(&repo, "rat/dedup-distinct-head/tkt-1b", "b.txt", "b\n");
    assert_ne!(head_a, head_b);

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "dedup-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": &repo_name, "path": repo.to_string_lossy()}),
        )
        .await
        .unwrap();

    // Same task identity on both — only the head differs.
    emit_harness_result(
        &mut client,
        &repo_name,
        "Rat-A",
        "rat/dedup-distinct-head/tkt-1a",
        "main",
        &head_a,
        "shared-task",
        "trivial",
    )
    .await;
    emit_harness_result(
        &mut client,
        &repo_name,
        "Rat-B",
        "rat/dedup-distinct-head/tkt-1b",
        "main",
        &head_b,
        "shared-task",
        "trivial",
    )
    .await;

    assert!(
        wait_until_marker_count(&mut client, &repo_name, 2, 300).await,
        "both distinct-head candidates never reached processed markers"
    );
    assert!(
        wait_until_queue_empty(&mut client, &repo_name, 50).await,
        "both distinct-head candidates never drained the queue"
    );

    let markers = processed_markers(&mut client, &repo_name).await;
    assert_eq!(
        markers.len(),
        2,
        "a different head under the same task must be admitted independently, not deduped: {markers:?}"
    );

    let outcomes: Vec<Value> = markers
        .iter()
        .map(|m| m["payload"]["outcome"].clone())
        .collect();
    assert!(
        outcomes.iter().all(|o| o == "landed"),
        "both distinct-head candidates must have landed: {outcomes:?}"
    );

    let log = git(&repo, &["log", "--oneline", "main"]);
    assert!(log.contains("a.txt") || repo.join("a.txt").exists());
    assert!(repo.join("a.txt").exists() && repo.join("b.txt").exists());

    handle.abort();
}

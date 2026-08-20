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

/// Fields for a hand-crafted `harness_result` completion — bundled into one
/// struct rather than passed as separate args to [`emit_harness_result`]
/// (clippy's `too_many_arguments`).
struct Completion<'a> {
    agent: &'a str,
    branch: &'a str,
    target: &'a str,
    head_sha: &'a str,
    task: &'a str,
    diff_class: &'a str,
}

/// Hand-crafts a `harness_result` completion event exactly as
/// `Supervisor::route_completion` would, and writes it straight to the
/// space over `space.out` as the operator — this is what lets a test drive
/// the reactor's `action: "land"` path (the ticket's actual incident path)
/// without needing to reproduce the full respawn/pause machinery that
/// produced the duplicate `task_done` in the field.
async fn emit_harness_result(client: &mut Client, repo_name: &str, c: Completion<'_>) {
    client
        .call(
            "space.out",
            json!({
                "category": "event",
                "scope": repo_name,
                "identity": "harness_result",
                "payload": {
                    "agent": c.agent,
                    "spawn": format!("spawn-{}", c.agent),
                    "role": "rat",
                    "task": c.task,
                    "branch": c.branch,
                    "target": c.target,
                    "parent": Value::Null,
                    "is_error": false,
                    "head_sha": c.head_sha,
                    "diff_files": 1,
                    "diff_lines": 1,
                    "diff_class": c.diff_class,
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
        Completion {
            agent: "Deyna-9",
            branch: "rat/dedup-seq/tkt-1",
            target: "main",
            head_sha: &head_sha,
            task: "dedup-seq-task",
            diff_class: "trivial",
        },
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
        Completion {
            agent: "Deyna-9",
            branch: "rat/dedup-seq/tkt-1",
            target: "main",
            head_sha: &head_sha,
            task: "dedup-seq-task",
            diff_class: "trivial",
        },
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
        Completion {
            agent: "Deyna-9",
            branch: "rat/dedup-midgate/tkt-1",
            target: "main",
            head_sha: &head_sha,
            task: "dedup-midgate-task",
            diff_class: "trivial",
        },
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
        Completion {
            agent: "Deyna-9",
            branch: "rat/dedup-midgate/tkt-1",
            target: "main",
            head_sha: &head_sha,
            task: "dedup-midgate-task",
            diff_class: "trivial",
        },
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
/// Deliberately does NOT kill mid-gate — see
/// `restart_mid_gate_kill_does_not_race_a_second_landing_processed_marker`
/// below, which now owns that case since `Daemon::run()`'s background loops
/// were moved into a `JoinSet` local to the future (TKT-01M0G2VXS8PQYZN2X3ZWXDFC5B),
/// making `handle.abort()` tear down the whole task tree instead of leaving
/// orphans. Settling before the kill here keeps this test's only variable
/// the durable marker, not the restart timing.
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
        Completion {
            agent: "Deyna-9",
            branch: "rat/dedup-restart/tkt-1",
            target: "main",
            head_sha: &head_sha,
            task: "dedup-restart-task",
            diff_class: "trivial",
        },
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
        Completion {
            agent: "Deyna-9",
            branch: "rat/dedup-restart/tkt-1",
            target: "main",
            head_sha: &head_sha,
            task: "dedup-restart-task",
            diff_class: "trivial",
        },
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

/// TKT-01M0G2VXS8PQYZN2X3ZWXDFC5B: the actual mid-gate kill
/// `restart_preserves_the_landing_dedup_invariant` above deliberately avoids.
/// Daemon A's `verify` check is mid-`sleep` (durably `running_gates`) when
/// `handle_a.abort()` fires — before this fix, `Daemon::run()`'s reactor and
/// landing-pipeline consumer loops were independent `tokio::spawn` tasks, not
/// children of the aborted future, so that in-flight gate run kept executing
/// as an orphan on this same test-process runtime. Daemon B's own fresh
/// `LandingPipeline` then resumed and re-ran the SAME gate concurrently — two
/// independent instances, each with its own in-process locks, racing to
/// write the terminal `landing_processed` marker for one work key. That
/// produced two markers 100% of 5 runs in the field investigation. Now that
/// every background loop is spawned into a `JoinSet` local to `run()`,
/// aborting `handle_a` drops that `JoinSet` and aborts every task still in
/// it, so daemon A's orphaned gate run is torn down before it can finish —
/// only daemon B's resume ever completes the candidate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_mid_gate_kill_does_not_race_a_second_landing_processed_marker() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path().join("dedup-restart-midgate");
    std::fs::create_dir(&repo).unwrap();
    init_repo(&repo);

    let barrier_dir = tempfile::tempdir().unwrap();
    let run_count = barrier_dir.path().join("run-count.log");
    let checks = format!(
        r#"
checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "echo run >> \"{log}\"; sleep 0.8 && true", timeout: "30s"}},
]
"#,
        log = run_count.display(),
    );
    write_checks(&repo, &checks);

    let repo_name = repo_name_of(&repo);
    let head_sha = make_branch(&repo, "rat/dedup-restart-midgate/tkt-1", "work.txt", "v1\n");
    let main_before = git(&repo, &["rev-parse", "main"]);

    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    std::fs::create_dir_all(layout.triggers_dir()).unwrap();
    std::fs::write(layout.triggers_dir().join("landing.cue"), LANDING_TRIGGER).unwrap();

    let mut config = rk_core::config::Config::default();
    config.harness.default = "fake".into();

    // Daemon A: a genuinely on-disk `Space` (`Daemon::new`, not
    // `new_in_memory`) — a second `Daemon::new` below must inherit this
    // one's durable state, not start from an empty store.
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
        Completion {
            agent: "Deyna-9",
            branch: "rat/dedup-restart-midgate/tkt-1",
            target: "main",
            head_sha: &head_sha,
            task: "dedup-restart-midgate-task",
            diff_class: "trivial",
        },
    )
    .await;

    assert!(
        wait_until_status(&mut client, &repo_name, "running_gates", 150).await,
        "candidate never reached running_gates before the kill"
    );

    // The kill: abort the daemon's task outright, genuinely mid-gate — the
    // landing consumer loop's shutdown signal is only checked BETWEEN
    // cycles, so a graceful `stop` would let an already-started `run_cycle`
    // finish first and defeat the point of this test.
    handle_a.abort();
    let _ = handle_a.await;
    std::fs::remove_file(layout.pid_file()).ok();
    std::fs::remove_file(layout.socket_path()).ok();

    // Daemon B: a fresh `Daemon::new` over the SAME on-disk home, with the
    // SAME installed trigger rediscovered off disk.
    let daemon_b = Daemon::new(layout.clone(), &config).unwrap();
    let handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;

    assert!(
        wait_until_queue_empty(&mut client, &repo_name, 300).await,
        "the candidate never drained after the mid-gate restart"
    );

    let markers = processed_markers(&mut client, &repo_name).await;
    assert_eq!(
        markers.len(),
        1,
        "an orphaned daemon-A gate run must not race daemon B's resume into a \
         second processed marker: {markers:?}"
    );
    assert_eq!(markers[0]["payload"]["outcome"], "landed", "{markers:?}");

    let main_after = git(&repo, &["rev-parse", "main"]);
    assert_ne!(
        main_before, main_after,
        "the branch must have landed onto main after the restart"
    );
    // A single landing always advances main by exactly 2 commits — `rk`
    // lands with `--no-ff` (`rk-git/src/lib.rs`), so a 1-commit branch
    // yields the replayed commit plus the merge commit. A racing second
    // merge (the bug this test guards) would add 2 more.
    let merge_count = git(&repo, &["rev-list", "--count", &format!("{main_before}..main")]);
    assert_eq!(
        merge_count, "2",
        "the candidate must merge exactly once, not once per racing daemon: \
         {merge_count} commits landed onto main"
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
        Completion {
            agent: "Rat-A",
            branch: "rat/dedup-distinct-head/tkt-1a",
            target: "main",
            head_sha: &head_a,
            task: "shared-task",
            diff_class: "trivial",
        },
    )
    .await;
    emit_harness_result(
        &mut client,
        &repo_name,
        Completion {
            agent: "Rat-B",
            branch: "rat/dedup-distinct-head/tkt-1b",
            target: "main",
            head_sha: &head_b,
            task: "shared-task",
            diff_class: "trivial",
        },
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

/// A different head under a DIFFERENT task must also remain independently
/// admissible — distinct-head admission does not depend on the two
/// completions sharing a task identity. Operator-clarified acceptance
/// (2026-08-20): "independent admission means a different head, including a
/// different task on that head." Two distinct branches/heads, each under its
/// own task, both land, producing two processed markers each recording its
/// own task.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_different_head_under_a_different_task_is_independently_admissible() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path().join("dedup-distinct-head-task");
    std::fs::create_dir(&repo).unwrap();
    init_repo(&repo);
    write_checks(&repo, FAST_CHECKS);
    std::fs::write(repo.join(".rk").join("triggers.cue"), LANDING_TRIGGER).unwrap();

    let repo_name = repo_name_of(&repo);
    let head_a = make_branch(&repo, "rat/dedup-distinct-head-task/tkt-a", "a.txt", "a\n");
    let head_b = make_branch(&repo, "rat/dedup-distinct-head-task/tkt-b", "b.txt", "b\n");
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

    // Both branch AND task differ between the two completions.
    emit_harness_result(
        &mut client,
        &repo_name,
        Completion {
            agent: "Rat-A",
            branch: "rat/dedup-distinct-head-task/tkt-a",
            target: "main",
            head_sha: &head_a,
            task: "task-a",
            diff_class: "trivial",
        },
    )
    .await;
    emit_harness_result(
        &mut client,
        &repo_name,
        Completion {
            agent: "Rat-B",
            branch: "rat/dedup-distinct-head-task/tkt-b",
            target: "main",
            head_sha: &head_b,
            task: "task-b",
            diff_class: "trivial",
        },
    )
    .await;

    assert!(
        wait_until_marker_count(&mut client, &repo_name, 2, 300).await,
        "both distinct-head/distinct-task candidates never reached processed markers"
    );
    assert!(
        wait_until_queue_empty(&mut client, &repo_name, 50).await,
        "both distinct-head/distinct-task candidates never drained the queue"
    );

    let markers = processed_markers(&mut client, &repo_name).await;
    assert_eq!(
        markers.len(),
        2,
        "a different head under a different task must be admitted independently, not deduped: {markers:?}"
    );

    let outcomes: Vec<Value> = markers
        .iter()
        .map(|m| m["payload"]["outcome"].clone())
        .collect();
    assert!(
        outcomes.iter().all(|o| o == "landed"),
        "both distinct-head/distinct-task candidates must have landed: {outcomes:?}"
    );

    let mut tasks: Vec<String> = markers
        .iter()
        .map(|m| {
            m["payload"]["task"]
                .as_str()
                .unwrap_or_default()
                .to_string()
        })
        .collect();
    tasks.sort();
    assert_eq!(
        tasks,
        vec!["task-a".to_string(), "task-b".to_string()],
        "each marker must record its own task, not collapse to one: {markers:?}"
    );

    assert!(repo.join("a.txt").exists() && repo.join("b.txt").exists());

    handle.abort();
}

/// The identical repo/branch/head/target rebound to a DIFFERENT non-empty
/// task must fail closed rather than double-process the code or close the
/// wrong ticket — operator-clarified acceptance (2026-08-20) preserves this
/// existing rule. Driven through the same reactor/`harness_result` path as
/// the rest of this suite (not `repo.land` directly, which
/// `land_task_identity.rs::resubmission_with_a_different_task_fails_closed`
/// already covers) so the actual incident shape — a redelivered completion
/// that happens to carry a different task string — is exercised end to end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_head_resubmitted_under_a_different_task_fails_closed() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path().join("dedup-task-mismatch");
    std::fs::create_dir(&repo).unwrap();
    init_repo(&repo);
    write_checks(&repo, FAST_CHECKS);
    std::fs::write(repo.join(".rk").join("triggers.cue"), LANDING_TRIGGER).unwrap();

    let repo_name = repo_name_of(&repo);
    let head_sha = make_branch(&repo, "rat/dedup-task-mismatch/tkt-1", "work.txt", "v1\n");

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
        Completion {
            agent: "Deyna-9",
            branch: "rat/dedup-task-mismatch/tkt-1",
            target: "main",
            head_sha: &head_sha,
            task: "real-task",
            diff_class: "trivial",
        },
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
    assert_eq!(
        markers_after_first[0]["payload"]["task"], "real-task",
        "{markers_after_first:?}"
    );
    let main_after_first = git(&repo, &["rev-parse", "main"]);

    // Same branch/head/target resubmitted under a DIFFERENT, wrong task —
    // the shape an operator typo or a stale `--task` reused after the real
    // one already landed would produce.
    emit_harness_result(
        &mut client,
        &repo_name,
        Completion {
            agent: "Deyna-9",
            branch: "rat/dedup-task-mismatch/tkt-1",
            target: "main",
            head_sha: &head_sha,
            task: "wrong-task",
            diff_class: "trivial",
        },
    )
    .await;

    // Give the feed-woken reactor a beat to evaluate (and fail closed on)
    // the mismatched-task resubmission.
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert!(
        queue_entries(&mut client, &repo_name).await.is_empty(),
        "a different-task resubmission must never leave a live queue entry behind"
    );
    let markers_after_second = processed_markers(&mut client, &repo_name).await;
    assert_eq!(
        markers_after_second.len(),
        1,
        "a different-task resubmission must fail closed, not create a second processed marker: \
         {markers_after_second:?}"
    );
    assert_eq!(
        markers_after_second[0]["payload"]["task"], "real-task",
        "the surviving marker must still record the original task, not the mismatched retry: \
         {markers_after_second:?}"
    );
    let main_after_second = git(&repo, &["rev-parse", "main"]);
    assert_eq!(
        main_after_first, main_after_second,
        "a different-task resubmission must not merge (double-process) a second time"
    );

    handle.abort();
}

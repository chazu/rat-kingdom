//! Bounded reproduction + regression test for TKT-pijug-puzav-tihud:
//! `work.current`/`reconcile.report` exceeded the observer's 5s per-RPC
//! deadline while concurrent managed verification ran (2026-09-12 king/BBS
//! repeat trial, `primary-observation/samples.jsonl` indices 2/21/22/26/64/
//! 103/105/112/117/122 — `["work.current: RPC deadline exceeded", ...]` with
//! `elapsed_ms` 9.5-13s against a 5s per-RPC / 20s per-sample bound). Idle
//! timing on the same host was ~1s and never reproduced it.
//!
//! Root cause, read directly from the code (not guessed from the source
//! pointers in the original ticket, which are leads only):
//! `cleared_branches_for_paths` (`crates/rk-daemon/src/server.rs`) re-opened
//! the SAME scope's git checkout — two `git` subprocess spawns via
//! `Repo::open_checkout` (`rev-parse --show-toplevel` then `discover`'s own
//! `rev-parse --git-common-dir`) — once per dropped-land BRANCH instead of
//! once per distinct scope, and `Daemon::reconcile_report` ran its two
//! independent git-heavy reads (`cleared_branches`, `merge_commit_ancestry`)
//! sequentially instead of concurrently, even though neither depends on the
//! other's result. Neither costs much in isolation (a handful of git
//! subprocess spawns, comfortably sub-second at idle) — but under concurrent
//! managed verification, real `git`/`cargo`/`rustc` processes competing for
//! the same fork/exec and CPU scheduling make each spawn measurably more
//! expensive, and redundant/serial spawns compound past the observer's 5s
//! per-RPC deadline exactly the way the load report shows.
//!
//! This builds a repo with representative history (branching commits, two
//! dropped lands, one delivered ticket — not a bare single-commit fixture),
//! drives REAL concurrent managed verification through the same `verify.run`
//! admission path `verification_saturation_regression.rs` uses (real `git`
//! subprocess bursts against the SAME repo, not a synthetic sleep), and
//! samples `work.current`/`reconcile.report` at the real observer's own 5s
//! per-RPC / 20s per-sample bounds throughout. It prints measured stage
//! timings and repo/source sizes (history length, branch count, load shape)
//! and asserts every sampled RPC still completes inside its 5s budget with
//! the fix in place.
//!
//! TKT-matom-livag-zohut diagnosed a later protected-landing gate failure of
//! this same test (candidate 5de6060, 2026-09-14T13:53:37Z, gate-failure
//! artifact 01M2G33QCXBQRNKYDBZY033VY0) whose tail-truncated capture omitted
//! the actual panic — the real cause of that specific 13:53 failure could
//! not be recovered byte-for-byte, and this note does not claim otherwise.
//! What was established directly:
//!
//! - An isolated replay of the exact failing binary (hash bb3ed21…) passed
//!   cleanly (22.10s, max RPC 322ms), and the daemon-side fix this test
//!   guards (open-once-per-scope in `cleared_branches_for_paths`, concurrent
//!   `cleared_branches`/`merge_commit_ancestry` reads in `reconcile_report`)
//!   was confirmed still present by reading `server.rs`. No evidence of a
//!   regression in the RPC-latency path itself was found.
//! - The pre-fix fixture's own synthetic load generator (see
//!   `load_check_body`'s prior doc comment / git history) bounded each
//!   check's CPU burn by a fixed iteration count, checked against its 60s
//!   `timeout` only implicitly (by finishing or not) rather than against
//!   wall-clock time. Measured directly: the identical loop body took 19.1s
//!   on a lightly-loaded host and 106.7s under a representative 65-way CPU-
//!   contention scenario (8 cores) — over the 60s cap. Reproduced through
//!   the REAL `verify.run` path with the unmodified pre-fix binary under
//!   96-way contention: `verify.run` itself failed for 3 of 6 checks with
//!   "... timed out after 60s and was killed", panicking this test at its
//!   `run_verify` call site — a demonstrated, real failure mode of the old
//!   fixture, distinct from (and easily mistaken for, given a truncated
//!   tail) the RPC-deadline assertions this test exists to guard.
//!
//! Fixed by making the load generator's own budget wall-clock-bounded and
//! checked at a fine (small-batch) granularity rather than only at the end
//! of a fixed-size run — see `load_check_body` for why granularity matters
//! as much as the wall-clock basis, and why this does not reduce load
//! coverage or weaken the observer's 5s/20s budgets.

mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use rk_ledger::Budget;
use rk_space::Space;
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};
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

fn init_repo(dir: &Path) -> String {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    support::install_default_repository_policy(dir);
    dir.file_name().unwrap().to_string_lossy().to_string()
}

/// Representative history: sequential commits on `main` plus a handful of
/// feature branches, so repo/source size is nontrivial rather than a bare
/// single-commit toy fixture.
fn grow_representative_history(dir: &Path, commits: usize, branches: usize) {
    for i in 0..commits {
        std::fs::write(dir.join("history.txt"), format!("line {i}\n")).unwrap();
        git(dir, &["add", "-A"]);
        git(dir, &["commit", "-m", &format!("history {i}")]);
    }
    for b in 0..branches {
        git(dir, &["checkout", "-b", &format!("feature/{b}"), "main"]);
        std::fs::write(dir.join(format!("feature-{b}.txt")), "work\n").unwrap();
        git(dir, &["add", "."]);
        git(dir, &["commit", "-m", &format!("feature {b} work")]);
        git(dir, &["checkout", "main"]);
    }
}

/// Escapes a shell command string for embedding inside a CUE double-quoted
/// `command: "..."` field. Mirrors `verification_saturation_regression.rs`.
fn cue_command(body: &str) -> String {
    body.replace('\\', "\\\\").replace('"', "\\\"")
}

/// A managed-verification check body that generates REAL CPU and
/// fork/exec contention — the same combination a parallel `cargo`/`rustc`
/// build puts on a shared host (CPU-bound codegen plus periodic subprocess
/// spawns for the linker/build scripts) — against the same repository
/// `reconcile.report` reads, without needing an actual Cargo project. Pure
/// POSIX shell arithmetic for the CPU burn (portable, no `seq`/brace
/// expansion/external CPU-spin binary) run in small fixed-size batches, with
/// a real `git` subprocess spawned roughly every 100k arithmetic iterations
/// so fork/exec contention is present too, not just CPU pressure. The check
/// runner execs this via `sh` (`managed_verification.rs`'s
/// `Command::new("sh")`), so it deliberately avoids `$SECONDS` and other
/// bash/ksh-only builtins in favor of the POSIX `date +%s` utility this file
/// already relies on elsewhere for portability.
///
/// Bounded by wall-clock, not by a fixed iteration count, and checked every
/// `BATCH` (small) arithmetic iterations rather than only once at the very
/// end — the granularity matters as much as the wall-clock basis. TKT-
/// matom-livag-zohut measured why: the same CPU burn written as a single
/// `while [ $i -lt N ]` loop up to a fixed count of 2,000,000 took 19.1s on a
/// lightly-loaded host but 106.7s under a representative 65-way CPU-
/// contention scenario (8 cores) — a 5.6x slowdown that blows straight
/// through this check's own 60s `timeout`, and was independently reproduced
/// through the real `verify.run` path with the unmodified pre-fix binary
/// (96-way contention, `verify.run` itself failed: "... timed out after 60s
/// and was killed" for 3 of 6 checks). A host-speed-dependent, checked-once
/// budget can therefore fail this test on its OWN load generator instead of
/// the actual subject under test (the observer's RPC deadline) — an
/// ambiguity this diagnosis could not rule out for the original gate
/// failure, whose panic text did not survive tail truncation. Checking the
/// deadline every small batch (not every 2,000,000-iteration run) bounds the
/// worst-case overrun to roughly one batch's duration regardless of how slow
/// the host is — not eliminating scheduling delay, but keeping any overrun
/// small and non-multiplicative, unlike the original design where the ENTIRE
/// workload's slowdown compounded before the deadline was ever consulted.
/// Wall-clock bounding also does not reduce load coverage: on a slower host
/// it does fewer arithmetic iterations in the same `duration_secs`, but it
/// still spends the full `duration_secs` generating CPU and git-subprocess
/// contention overlapping the observer's sampling window — which is the
/// dimension this test actually asserts on, not a specific op count.
fn load_check_body(repo: &Path, duration_secs: u64) -> String {
    let repo = repo.display();
    const BATCH: u64 = 5_000;
    // Roughly one `git` spawn per 100k arithmetic iterations, preserved from
    // the original design: one every `CHECKS_PER_GIT_SPAWN` batches.
    const CHECKS_PER_GIT_SPAWN: u64 = 100_000 / BATCH;
    format!(
        "i=0; c=0; start=$(date +%s); end=$((start + {duration_secs})); \
         while true; do \
         b=0; while [ $b -lt {BATCH} ]; do i=$((i+1)); b=$((b+1)); done; \
         c=$((c+1)); \
         now=$(date +%s); \
         if [ \"$now\" -ge \"$end\" ]; then break; fi; \
         if [ $((c % {CHECKS_PER_GIT_SPAWN})) -eq 0 ]; then git -C {repo} rev-parse HEAD >/dev/null 2>&1; fi; \
         done"
    )
}

fn write_load_checks(repo: &Path, n: usize, duration_secs: u64) {
    let mut checks = String::from("checks: [\n");
    for i in 0..n {
        let body = load_check_body(repo, duration_secs);
        checks.push_str(&format!(
            "    {{name: \"load-{i}\", command: \"{}\", timeout: \"60s\", environmentPolicy: \"strip_rk_spawn\"}},\n",
            cue_command(&body)
        ));
    }
    checks.push_str("]\n");
    let rk_dir = repo.join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(rk_dir.join("checks.cue"), checks).unwrap();
}

async fn run_verify(layout: &Layout, repo: &str, check: &str) -> Value {
    let mut client = Client::connect_as_operator(layout).await.unwrap();
    client
        .call("verify.run", json!({"repo": repo, "check": check}))
        .await
        .unwrap_or_else(|e| panic!("verify.run({repo}, {check}) failed: {e}"))
}

/// The real king observer's own bounds (`rk-king-bbs-repeat-20260912T151803Z/
/// primary-observation/manifest.json`): 5s per RPC, 20s per sample.
const RPC_DEADLINE: Duration = Duration::from_secs(5);
const SAMPLE_DEADLINE: Duration = Duration::from_secs(20);
const N_CHECKS: usize = 6;
// Wall-clock, not an iteration count — see `load_check_body`'s doc comment
// for why a work-based budget is unsafe on a contended host. 25s overlaps
// most of the 45s sampling window while leaving ample margin under each
// check's own 60s timeout even under heavy contention.
const LOAD_CHECK_DURATION_SECS: u64 = 25;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn work_current_and_reconcile_report_stay_inside_the_observer_rpc_budget_under_concurrent_verification(
) {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_path = repo_dir.path();
    let repo_name = init_repo(repo_path);
    grow_representative_history(repo_path, 40, 3);

    let commit_count: usize = git(repo_path, &["rev-list", "--count", "HEAD"])
        .parse()
        .unwrap();
    let branch_count = git(repo_path, &["branch", "--list"]).lines().count();

    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = Space::open(&layout.db_path()).unwrap();
    let daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "fake".into(),
        Budget::default(),
        space,
    )
    .unwrap();
    tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // Two dropped-land events across distinct branches — the exact shape
    // `cleared_branches` must resolve, once per branch, on every
    // `reconcile.report` call.
    for (i, branch) in ["feature/0", "feature/1"].iter().enumerate() {
        client
            .call(
                "space.out",
                json!({
                    "category": "event", "scope": &repo_name, "identity": "branch_landed",
                    "payload": {
                        "branch": branch, "target": "main", "merged": false, "pr_opened": false,
                        "content_free": false,
                        "chain_key": format!("{repo_name}\0{branch}\0deadbeef{i}\0main\0TKT-{i}\0TKT-{i}-rework"),
                        "detail": "conflict",
                    },
                    "lifecycle": "furniture",
                }),
            )
            .await
            .unwrap();
    }

    // One delivered ticket — the shape `merge_commit_ancestry` must resolve.
    let feature_2_head = git(repo_path, &["rev-parse", "feature/2"]);
    client
        .call(
            "space.out",
            json!({
                "category": "task", "scope": &repo_name, "identity": "TKT-DELIVERED",
                "payload": {
                    "title": "t", "status": "closed", "assignee": Value::Null,
                    "delivery": {
                        "merge_commit": feature_2_head, "branch": "feature/2", "target": "main",
                        "landed_at": "2026-09-12T00:00:00Z",
                    },
                },
                "lifecycle": "session",
            }),
        )
        .await
        .unwrap();

    // Concurrent managed verification, real subprocess contention against
    // the SAME repo — written before `repo.add`, matching
    // `verification_saturation_regression.rs`'s own ordering.
    write_load_checks(repo_path, N_CHECKS, LOAD_CHECK_DURATION_SECS);
    client
        .call(
            "repo.add",
            json!({"name": &repo_name, "path": repo_path.to_string_lossy()}),
        )
        .await
        .unwrap();

    let mut load_handles = Vec::new();
    for i in 0..N_CHECKS {
        let layout = layout.clone();
        let repo_name = repo_name.clone();
        load_handles.push(tokio::spawn(async move {
            run_verify(&layout, &repo_name, &format!("load-{i}")).await
        }));
    }

    // Sample `work.current`/`reconcile.report` at the real observer's own
    // bounds for as long as the load runs, over ONE persistent connection —
    // the same round-trip shape a real observer's sampling loop uses.
    let mut sample_client = connect(&layout).await;
    let mut samples: Vec<(&'static str, Duration)> = Vec::new();
    let sampling_deadline = Instant::now() + Duration::from_secs(45);
    while !load_handles.iter().all(|h| h.is_finished()) && Instant::now() < sampling_deadline {
        for method in ["work.current", "reconcile.report"] {
            let started = Instant::now();
            let result = tokio::time::timeout(
                SAMPLE_DEADLINE,
                sample_client.call(method, json!({"repo": &repo_name})),
            )
            .await;
            let elapsed = started.elapsed();
            match result {
                Ok(Ok(_)) => samples.push((method, elapsed)),
                Ok(Err(e)) => panic!("{method} returned an RPC error under load: {e}"),
                Err(_) => panic!(
                    "{method} exceeded the {SAMPLE_DEADLINE:?} observer sample budget under \
                     concurrent verification load — elapsed {elapsed:?}"
                ),
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    for h in load_handles {
        let result = h.await.unwrap();
        assert_eq!(
            result["exit"],
            json!(0),
            "load check must complete cleanly: {result:#?}"
        );
    }

    assert!(
        !samples.is_empty(),
        "the sampling loop must have observed at least one RPC round trip during the load window"
    );

    let max_elapsed = samples.iter().map(|(_, d)| *d).max().unwrap();
    eprintln!(
        "observer_rpc_deadline_under_verification_load: {} samples, max elapsed {max_elapsed:?}, \
         repo history: {commit_count} commits / {branch_count} branches, {N_CHECKS} concurrent \
         checks x {LOAD_CHECK_DURATION_SECS}s CPU-bound each",
        samples.len(),
    );
    for (method, elapsed) in &samples {
        if *elapsed > RPC_DEADLINE {
            eprintln!(
                "  {method}: {elapsed:?} (exceeded the real observer's {RPC_DEADLINE:?} \
                 per-RPC deadline)"
            );
        }
    }
    let over_budget = samples.iter().filter(|(_, d)| *d > RPC_DEADLINE).count();
    assert_eq!(
        over_budget,
        0,
        "work.current/reconcile.report must stay inside the {RPC_DEADLINE:?} per-RPC deadline \
         even under concurrent managed verification (TKT-pijug-puzav-tihud): {over_budget}/{} \
         samples exceeded it: {samples:?}",
        samples.len(),
    );
}

//! Real cross-process proof for TKT-kujab-momum-vazug's landing-queue
//! settlement: a queued entry whose target is not an existing branch must be
//! durably quarantined and leave the active queue, must be discovered and
//! settled by a genuinely fresh daemon PROCESS that never touched it before
//! (not merely a same-process `Space`/`LandingPipeline` reopen — that
//! variant is `landing::tests::malformed_landing_target_is_quarantined_and_survives_a_crash_before_queue_removal`
//! in `rk-daemon/src/landing.rs`, kept as complementary same-process
//! evidence), must survive a further real restart without duplicating
//! evidence, and must never let one queue revision's stale identity retire
//! a DIFFERENT one's decision.
//!
//! The fixture is seeded directly into the on-disk `Space` (via the public
//! `rk_space`/`rk_core::tuple` API, already an established pattern for this
//! exact purpose — see `bbs_capture_export_cli.rs`'s own doc comment) WHILE
//! NO DAEMON IS RUNNING AT ALL. This is what makes the crash-recovery proof
//! real rather than assumed: the first daemon process this test ever starts
//! opens a home whose queue already contains the malformed entries, so its
//! settlement of them is a genuine cold start over persisted state, not a
//! same-process replay of work it just did. It is not a production write —
//! no `agent.spawn` + completion cycle ever ran, and no live daemon RPC was
//! used to create it — and it is not a raw database edit either: it goes
//! through the same public `Tuple`/`Space::out` construction the daemon's
//! own `LandingQueue::write` uses for every entry it ever persists.

use rk_core::tuple::{Category, Lifecycle, Tuple};
use rk_space::Space;
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

/// Run `cmd` to completion, killing it and panicking if it does not exit
/// within `timeout`. Every subprocess this file waits on (`rk`, `git`) is a
/// local, ordinarily-sub-second call — without an actual per-command bound,
/// a single hung invocation blocks forever inside `Command::output`, which
/// no surrounding `until` retry loop can ever observe or time out on: the
/// loop never gets control back to check its own deadline.
fn bounded_output(mut cmd: Command, timeout: Duration) -> std::process::Output {
    let child = cmd
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn subprocess");
    let pid = child.id();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(child.wait_with_output());
    });
    match rx.recv_timeout(timeout) {
        Ok(output) => output.expect("collect subprocess output"),
        Err(_) => {
            let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
            panic!("subprocess (pid {pid}) exceeded its {timeout:?} bound");
        }
    }
}

const CMD_TIMEOUT: Duration = Duration::from_secs(15);

fn rk(home: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rk"));
    cmd.env("RK_HOME", home);
    cmd.env_remove("RK_AGENT");
    cmd.env_remove("RK_AUTH_TOKEN");
    cmd
}

fn git(dir: &Path, args: &[&str]) -> String {
    let mut cmd = Command::new("git");
    cmd.arg("-C").arg(dir).args(args);
    let out = bounded_output(cmd, CMD_TIMEOUT);
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn json_stdout(out: &std::process::Output) -> Value {
    assert!(
        out.status.success(),
        "rk failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "bad json: {e}\nstdout: {}",
            String::from_utf8_lossy(&out.stdout)
        )
    })
}

fn process_alive(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Poll `attempt` until it yields `Some`, or panic with `what` after 30s.
fn until<T>(what: &str, mut attempt: impl FnMut() -> Option<T>) -> T {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(30) {
        if let Some(v) = attempt() {
            return v;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("timed out after 30s waiting for: {what}");
}

fn daemon_pid(home: &Path) -> Option<u32> {
    let out = bounded_output(
        {
            let mut c = rk(home);
            c.args(["--json", "daemon", "status"]);
            c
        },
        CMD_TIMEOUT,
    );
    if !out.status.success() {
        return None;
    }
    serde_json::from_slice::<Value>(&out.stdout).ok()?["pid"]
        .as_u64()
        .map(|p| p as u32)
}

/// Bring a daemon up over `home` the way the field does — an ordinary RPC
/// call through `Client::connect_or_spawn` auto-starts one.
fn start_daemon(home: &Path) -> u32 {
    until("a daemon to come up over the home", || {
        let mut c = rk(home);
        c.args(["--json", "list"]);
        bounded_output(c, CMD_TIMEOUT)
            .status
            .success()
            .then_some(())
    });
    until("the freshly started daemon to report its pid", || {
        daemon_pid(home)
    })
}

/// A genuine SIGKILL — no graceful shutdown, no in-process `Drop` — then a
/// bounded wait for the OS to finish reaping it.
fn kill_daemon(pid: u32) {
    let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
    let deadline = Instant::now() + Duration::from_secs(5);
    while process_alive(pid) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(!process_alive(pid), "daemon {pid} survived SIGKILL");
}

/// Kill whichever daemon currently owns `home`, unless it is this test
/// process itself. Best-effort: a stale/unreadable pid file just means
/// nothing gets signalled, same as if no daemon were up.
fn kill_owning_daemon(home: &Path) {
    let Some(pid) = std::fs::read_to_string(home.join("rk.pid"))
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok())
    else {
        return;
    };
    if pid == std::process::id() {
        return;
    }
    let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
    let deadline = Instant::now() + Duration::from_secs(5);
    while process_alive(pid) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// RAII teardown: whatever daemon currently owns `home` is killed on both
/// the success and the panic path, so an assertion failure midway through
/// this test cannot leak a live daemon process onto the host running the
/// suite. Declared AFTER the `TempDir` it guards (reverse drop order) so it
/// runs before that directory is removed — the pid file must still exist
/// when this reads it.
struct DaemonGuard {
    home: std::path::PathBuf,
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        kill_owning_daemon(&self.home);
    }
}

fn tuples(home: &Path, scope: &str, category: &str, identity: &str) -> Vec<Value> {
    let mut c = rk(home);
    c.args(["--json", "scan", category, scope, identity]);
    json_stdout(&bounded_output(c, CMD_TIMEOUT))["tuples"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

fn out_tuple_live(home: &Path, category: &str, scope: &str, identity: &str, payload: &Value) {
    let mut c = rk(home);
    c.args([
        "out",
        category,
        scope,
        identity,
        "--payload",
        &payload.to_string(),
        "--lifecycle",
        "furniture",
    ]);
    let out = bounded_output(c, CMD_TIMEOUT);
    assert!(
        out.status.success(),
        "rk out failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// The exact durable shape `LandingQueue::write` persists for every entry it
/// ever enqueues (`crates/rk-daemon/src/landing.rs`): `Category::Event`,
/// identity `landing_queue_entry`, `Lifecycle::Furniture`, instance
/// `"daemon"`. Writing it directly through `Space::out` — never through a
/// live daemon's RPC — is what lets this be seeded while no daemon process
/// exists at all.
fn seed_queue_entry(space: &Space, repo_name: &str, payload: Value) {
    space
        .out(
            Tuple::new(
                Category::Event,
                repo_name,
                "landing_queue_entry",
                "daemon",
                payload,
            )
            .with_lifecycle(Lifecycle::Furniture),
        )
        .unwrap();
}

#[test]
fn malformed_landing_target_settles_once_across_a_real_daemon_process_restart() {
    let home = tempfile::tempdir().unwrap();
    let _guard = DaemonGuard {
        home: home.path().to_path_buf(),
    };
    // Fast reactor cadence so a live daemon drains the landing queue inside
    // this test's polling window instead of the 60s production default.
    std::fs::write(
        home.path().join("config.toml"),
        "[disk]\nmin_free_gb = 0\n\n[harness]\ndefault = \"fake\"\n\n[reactor]\ninterval_secs = 1\n",
    )
    .unwrap();

    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path();
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "r@x"]);
    git(repo, &["config", "user.name", "R"]);
    std::fs::create_dir_all(repo.join(".rk")).unwrap();
    std::fs::write(
        repo.join(".rk/checks.cue"),
        "checks: [\n    {name: \"landing-protected-paths\", command: \"true\", timeout: \"30s\"},\n    \
         {name: \"landing-diff-scope\", command: \"true\", timeout: \"30s\"},\n    \
         {name: \"verify\", command: \"true\", timeout: \"30s\"},\n]\n",
    )
    .unwrap();
    std::fs::write(repo.join(".rk/repo.cue"), "repo: {}\n").unwrap();
    std::fs::write(repo.join("README.md"), "# x\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "init"]);
    // A real commit that is deliberately not a branch tip — the exact shape
    // of the original operator mistake this settles.
    let detached_sha = git(repo, &["rev-parse", "main"]);

    git(repo, &["checkout", "-b", "feature"]);
    std::fs::write(repo.join("feature.md"), "feature work\n").unwrap();
    git(repo, &["add", "feature.md"]);
    git(repo, &["commit", "-m", "feat: add feature"]);
    let feature_sha = git(repo, &["rev-parse", "feature"]);
    git(repo, &["checkout", "main"]);

    git(repo, &["checkout", "-b", "second"]);
    std::fs::write(repo.join("second.md"), "second work\n").unwrap();
    git(repo, &["add", "second.md"]);
    git(repo, &["commit", "-m", "feat: add second"]);
    let second_sha = git(repo, &["rev-parse", "second"]);
    git(repo, &["checkout", "main"]);

    let repo_name = "myrepo";
    let repo_path_str = repo.to_string_lossy().to_string();

    // --- Seed while NO daemon is running at all. -----------------------
    // A queue is single-consumer per `seq` (`LandingQueue::scan_current`
    // self-heals any duplicate sharing one `seq` down to a single
    // survivor) — so this fixture, and the identity-isolation revision
    // injected below once this row is gone, run SEQUENTIALLY, not as two
    // simultaneously-queued rows.
    {
        let space = Space::open(&rk_core::paths::Layout::at(home.path()).db_path()).unwrap();
        seed_queue_entry(
            &space,
            repo_name,
            json!({
                "repo_name": repo_name,
                "repo_path": repo_path_str,
                "branch": "feature",
                "target": detached_sha,
                "head_sha": feature_sha,
                "diff_class": "doc-only",
                "seq": 100,
                "task": "malformed-fixture-task-A",
                "enqueued_at": now_rfc3339(),
                "phase_entered_at": now_rfc3339(),
            }),
        );
    } // `space` dropped here — closed before any daemon process exists.

    // --- First real daemon process: a genuine cold start over a home ----
    // whose queue already holds one permanently-invalid entry neither it
    // nor any other process has ever seen before.
    let pid1 = start_daemon(home.path());
    let added = json_stdout(&bounded_output(
        {
            let mut c = rk(home.path());
            c.args(["--json", "repo", "add", &repo_path_str, "--name", repo_name]);
            c
        },
        CMD_TIMEOUT,
    ));
    assert!(
        added["activated_policy"]["digest"].as_str().is_some(),
        "registration must bind an activated policy: {added}"
    );

    until(
        "the malformed entry to be durably quarantined by the fresh process",
        || {
            (!tuples(home.path(), repo_name, "event", "landing_queue_quarantine").is_empty())
                .then_some(())
        },
    );
    assert!(
        tuples(home.path(), repo_name, "event", "landing_queue_entry").is_empty(),
        "the quarantined entry must leave the active queue"
    );
    let quarantines = tuples(home.path(), repo_name, "event", "landing_queue_quarantine");
    assert_eq!(quarantines.len(), 1, "{quarantines:?}");
    assert_eq!(
        quarantines[0]["payload"]["entry"]["task"],
        "malformed-fixture-task-A"
    );
    assert_eq!(quarantines[0]["payload"]["entry"]["target"], detached_sha);
    assert_eq!(quarantines[0]["payload"]["entry"]["seq"], 100);

    // --- A DIFFERENT queue revision reusing the SAME seq/branch/head_sha/ --
    // target/source_spawn, but a DIFFERENT task, must get its OWN fresh
    // decision — not silently retired by the stale evidence above. This is
    // the precise probe for the fix's own claim: a per-repo `seq` counter
    // alone must never let one revision's quarantine answer for a
    // different task's queue revision. Safe to inject now (live, via the
    // daemon's own tuple-write RPC): the prior row sharing this `seq` is
    // already gone from the active queue, so there is no collision for
    // `scan_current` to self-heal.
    out_tuple_live(
        home.path(),
        "event",
        repo_name,
        "landing_queue_entry",
        &json!({
            "repo_name": repo_name,
            "repo_path": repo_path_str,
            "branch": "feature",
            "target": detached_sha,
            "head_sha": feature_sha,
            "diff_class": "doc-only",
            "seq": 100,
            "task": "malformed-fixture-task-B",
            "enqueued_at": now_rfc3339(),
            "phase_entered_at": now_rfc3339(),
        }),
    );
    until(
        "the distinct task's revision to earn its OWN quarantine decision",
        || {
            (tuples(home.path(), repo_name, "event", "landing_queue_quarantine").len() >= 2)
                .then_some(())
        },
    );
    let quarantines = tuples(home.path(), repo_name, "event", "landing_queue_quarantine");
    assert_eq!(
        quarantines.len(),
        2,
        "a shared seq/branch/head_sha/target/source_spawn must not let task-A's \
         quarantine answer for task-B's queue revision: {quarantines:?}"
    );
    let mut tasks: Vec<&str> = quarantines
        .iter()
        .map(|t| t["payload"]["entry"]["task"].as_str().unwrap())
        .collect();
    tasks.sort_unstable();
    assert_eq!(
        tasks,
        vec!["malformed-fixture-task-A", "malformed-fixture-task-B"]
    );
    assert!(tuples(home.path(), repo_name, "event", "landing_queue_entry").is_empty());

    // --- A genuine SIGKILL, then a real second process. ------------------
    kill_daemon(pid1);
    let pid2 = start_daemon(home.path());
    assert_ne!(pid1, pid2, "the restart must be a genuinely new OS process");

    // Give the restarted daemon several reactor cycles to (not) redo
    // anything, then confirm nothing duplicated.
    std::thread::sleep(Duration::from_secs(2));
    let quarantines = tuples(home.path(), repo_name, "event", "landing_queue_quarantine");
    assert_eq!(
        quarantines.len(),
        2,
        "a further restart must replay the existing verdicts, not duplicate evidence: {quarantines:?}"
    );
    assert!(tuples(home.path(), repo_name, "event", "landing_queue_entry").is_empty());

    // --- A DIFFERENT entry — distinct task/head_sha/branch/target, no ----
    // shared seq — must still land for real through the same process, and
    // must not be conflated with (or blocked by) the settled decisions
    // above.
    out_tuple_live(
        home.path(),
        "event",
        repo_name,
        "landing_queue_entry",
        &json!({
            "repo_name": repo_name,
            "repo_path": repo_path_str,
            "branch": "second",
            "target": "main",
            "head_sha": second_sha,
            "diff_class": "doc-only",
            "task": "distinct-valid-task",
            "enqueued_at": now_rfc3339(),
            "phase_entered_at": now_rfc3339(),
        }),
    );
    until("the valid entry to land onto main", || {
        let main_sha = git(repo, &["rev-parse", "main"]);
        (main_sha != detached_sha).then_some(())
    });
    until("the landed entry to also leave the active queue", || {
        tuples(home.path(), repo_name, "event", "landing_queue_entry")
            .is_empty()
            .then_some(())
    });
    assert_eq!(
        tuples(home.path(), repo_name, "event", "landing_queue_quarantine").len(),
        2,
        "the valid entry's landing must not retire or duplicate the unrelated quarantines"
    );

    kill_daemon(pid2);
}

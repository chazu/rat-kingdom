//! Genuine cross-process interruption/recovery proof for `release.prepare`
//! (P6.1 correction, TKT-divah-duzuf-hajub): SIGKILL a real daemon *process*
//! while it is genuinely blocked inside a real `cargo build`, bring a second
//! real daemon process up over the same on-disk home through the ordinary
//! `connect_or_spawn` stale-socket reclaim path, and prove recovery.
//!
//! # Why not `crates/rk-daemon/tests/release_prepare.rs`'s prior in-process attempt
//!
//! That version drove the daemon via `tokio::spawn(daemon.run())` inside the
//! test process and "crashed" it with `JoinHandle::abort()`. `abort()` only
//! cancels the listener's own top-level task; `Server::run`'s accept loop
//! spawns an INDEPENDENT task per connection (never a child of the listener
//! task), so the task actually running the in-flight `release.prepare` call —
//! and, through it, the real `cargo build` child it owns — survives the
//! abort untouched. Awaiting that same client's in-flight `prepare` call
//! after the "crash" then blocks behind the very build the test means to
//! interrupt, for up to `release::BUILD_TIMEOUT` (20 minutes): a hang, not a
//! crash. A real `SIGKILL` of a real OS process has no such gap — the whole
//! process, every one of its tasks, dies atomically. See
//! `crates/rk-cli/tests/review_ceiling_crash_barrier.rs` for the same
//! reasoning applied to a different daemon-owned transition, and
//! `crates/rk-cli/tests/daemon_rollover.rs` for the same real-subprocess
//! daemon pattern used here.
//!
//! # Bounded process ownership
//!
//! Every `rk` invocation below runs through [`bounded_output`] rather than a
//! bare `Command::output()`: a stuck RPC (e.g. against a daemon this test
//! just killed) must fail this test loudly within a fixed bound, never hang
//! the suite. [`DaemonGuard`] SIGKILLs whichever daemon currently owns
//! `home` when the test function returns — on the success path AND on a
//! panicking assertion — so a failed assertion partway through can never
//! leak a live daemon process (mirroring
//! `review_ceiling_crash_barrier.rs`'s `DaemonGuard`/`kill_owning_daemon`).
//!
//! `DaemonGuard` alone is NOT enough to reap the real `cargo build` child:
//! `release::run_recipe` spawns it with `.process_group(0)`, its own process
//! group distinct from the daemon's — the whole reason it survives a real
//! daemon SIGKILL as a genuine orphan for `reap_stale_managed_children` to
//! find on the next daemon's startup (see the module doc above). SIGKILLing
//! the daemon's own pid does not reach that separate group. On the
//! successful path, starting daemon B naturally reaps it before this test's
//! own assertions ever run. But if a panic strikes BEFORE daemon B ever
//! starts (e.g. the build never reaches its barrier, or the pre-kill
//! `Preparing` check never observes it), nothing would otherwise touch that
//! orphaned group at all — and a panic can strike well BEFORE that, since
//! `cargo`/`rustc` are already running inside the recipe's process group the
//! moment the daemon spawns it, long before the fixture's own build-script
//! barrier is ever reached. [`BuildGroupGuard`] closes that gap by keying off
//! the daemon's OWN [`managed_child_pids`] record (written the instant
//! `run_recipe` spawns the child) rather than the build-script marker file,
//! so ownership is captured as early as the daemon's bookkeeping allows.
//! Once known, it SIGKILLs that pid's whole process group on drop — a no-op
//! if the group is already gone (the ordinary already-reaped case).

use serde_json::Value;
use std::cell::Cell;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

/// Bound for an ordinary `rk` RPC round-trip (list/show/status/repo-add):
/// generous against real host contention, but short enough that a genuinely
/// stuck call fails the test instead of hanging it.
const RPC_BOUND: Duration = Duration::from_secs(30);
/// Bound for an `rk release prepare` call that actually runs the fixture's
/// real (sub-second) `cargo build` — wider than [`RPC_BOUND`] to leave room
/// for a cold `cargo` invocation on a loaded test runner, still far short of
/// `release::BUILD_TIMEOUT`'s 20 minutes.
const PREPARE_BOUND: Duration = Duration::from_secs(90);

fn rk(home: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rk"));
    cmd.env("RK_HOME", home);
    // A rat's spawn env would otherwise pick an agent identity; this drives
    // the CLI as the operator, same as `daemon_rollover.rs`.
    cmd.env_remove("RK_AGENT");
    cmd.env_remove("RK_AUTH_TOKEN");
    cmd
}

/// Run `cmd` to completion, killing it and panicking if it does not exit
/// within `bound` — see the module doc's "Bounded process ownership"
/// section. Stdout/stderr are drained concurrently with the wait (via
/// `Child::wait_with_output` on a helper thread), so a chatty child can never
/// deadlock this against a full pipe buffer the way polling `try_wait`
/// without reading would.
fn bounded_output(mut cmd: Command, bound: Duration) -> std::process::Output {
    let child = cmd
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("failed to spawn rk");
    let pid = child.id();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(child.wait_with_output());
    });
    match rx.recv_timeout(bound) {
        Ok(Ok(output)) => output,
        Ok(Err(e)) => panic!("rk invocation failed to run to completion: {e}"),
        Err(_) => {
            let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
            panic!("rk invocation exceeded its {bound:?} bound and was killed");
        }
    }
}

/// See `daemon_rollover.rs`'s identical helper: a constrained CI temp
/// filesystem can already be under the default `[disk] min_free_gb = 10`
/// floor before this test's own behavior is even exercised.
fn disable_disk_floor(home: &Path) {
    std::fs::write(home.join("config.toml"), "[disk]\nmin_free_gb = 0\n").unwrap();
}

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

/// A dependency-free two-package Cargo workspace — the same shape as
/// `crates/rk-daemon/tests/release_prepare.rs`'s own fixture (kept
/// independent rather than shared across crates; see that file's module doc
/// for exactly what it builds and why building it is cheap): `rk-cli` (bin
/// `rk`) prints a stamp and exits 0, `rk-mcp` (bin `rk-mcp`) speaks the same
/// minimal `initialize` handshake the real `rk-mcp` does.
fn write_fixture_source(dir: &Path, stamp: &str) {
    std::fs::write(
        dir.join("Cargo.toml"),
        "[workspace]\nmembers = [\"rk-cli\", \"rk-mcp\"]\nresolver = \"2\"\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.join("rk-cli/src")).unwrap();
    std::fs::write(
        dir.join("rk-cli/Cargo.toml"),
        "[package]\nname = \"rk-cli\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\n\
         [[bin]]\nname = \"rk\"\npath = \"src/main.rs\"\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("rk-cli/src/main.rs"),
        format!("fn main() {{ println!(\"fixture-rk {stamp}\"); }}\n"),
    )
    .unwrap();
    std::fs::create_dir_all(dir.join("rk-mcp/src")).unwrap();
    std::fs::write(
        dir.join("rk-mcp/Cargo.toml"),
        "[package]\nname = \"rk-mcp\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\n\
         [[bin]]\nname = \"rk-mcp\"\npath = \"src/main.rs\"\n",
    )
    .unwrap();
    let mcp_main = r#"use std::io::{self, BufRead, Write};
fn main() {
    for line in io::stdin().lock().lines() {
        let line = line.unwrap();
        if line.contains("initialize") {
            let response = "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocolVersion\":\"2024-11-05\",\"capabilities\":{},\"serverInfo\":{\"name\":\"fixture-rk-mcp\",\"version\":\"0.0.0\"}}}\n";
            io::stdout().write_all(response.as_bytes()).unwrap();
            io::stdout().flush().unwrap();
        }
    }
}
"#;
    std::fs::write(dir.join("rk-mcp/src/main.rs"), mcp_main).unwrap();
}

fn init_fixture_repo(dir: &Path, stamp: &str) {
    write_fixture_source(dir, stamp);
    git(dir, &["init", "-q", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    git(dir, &["add", "."]);
    git(dir, &["commit", "-qm", "fixture v1"]);
}

/// A REAL Cargo build-script barrier: `rk-cli/build.rs` blocks (polling for a
/// marker file's removal) before the crate compiles, writing its OWN pid to
/// `started` first — a genuine, independent OS process, not an inference
/// from the daemon's own bookkeeping.
fn write_blocking_build_script(cli_dir: &Path, blocker: &Path, started: &Path) {
    let build_rs = format!(
        "fn main() {{\n    \
             let blocker = std::path::Path::new(\"{}\");\n    \
             if blocker.exists() {{\n        \
                 std::fs::write(\"{}\", std::process::id().to_string()).unwrap();\n        \
                 while blocker.exists() {{\n            \
                     std::thread::sleep(std::time::Duration::from_millis(50));\n        \
                 }}\n    \
             }}\n}}\n",
        blocker.display(),
        started.display(),
    );
    std::fs::write(cli_dir.join("build.rs"), build_rs).unwrap();
}

fn json_stdout(out: &std::process::Output) -> Value {
    assert!(
        out.status.success(),
        "{}",
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

/// Poll `attempt` until it yields `Some`, or panic with `what` after 60s.
fn until<T>(what: &str, mut attempt: impl FnMut() -> Option<T>) -> T {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(60) {
        if let Some(v) = attempt() {
            return v;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("timed out after 60s waiting for: {what}");
}

/// The pid of the daemon currently owning `home`, or `None` if none is up.
fn daemon_pid(home: &Path) -> Option<u32> {
    let out = bounded_output(
        {
            let mut cmd = rk(home);
            cmd.args(["--json", "daemon", "status"]);
            cmd
        },
        RPC_BOUND,
    );
    if !out.status.success() {
        return None;
    }
    serde_json::from_slice::<Value>(&out.stdout).ok()?["pid"]
        .as_u64()
        .map(|p| p as u32)
}

/// Bring a daemon up over `home` the way the field does — an ordinary RPC
/// call through `Client::connect_or_spawn` — and return its pid. After a
/// SIGKILL, this is the path that exercises `Server::run`'s stale-socket
/// reclamation (it refuses to clobber a socket whose recorded pid is still
/// alive, and only reclaims one whose owner is genuinely dead).
fn start_daemon(home: &Path) -> u32 {
    until("a daemon to come up over the home", || {
        let mut cmd = rk(home);
        cmd.args(["--json", "release", "list"]);
        bounded_output(cmd, RPC_BOUND)
            .status
            .success()
            .then_some(())
    });
    until("the freshly started daemon to report its pid", || {
        daemon_pid(home)
    })
}

/// Kill whichever daemon currently owns `home`, unless it is this test
/// process itself or `spare` (a pid this test already knows is dead and has
/// no reason to signal again). Reads `home`'s pid file directly rather than
/// round-tripping through `daemon status` — teardown must not depend on the
/// very RPC path a failing test might be leaving in a bad state. Best-effort:
/// a stale/unreadable pid file, or a `kill` that fails to spawn, just means
/// nothing gets signalled — same reasoning as
/// `review_ceiling_crash_barrier.rs`'s identical helper.
fn kill_owning_daemon(home: &Path, spare: Option<u32>) {
    let Some(pid) = std::fs::read_to_string(home.join("rk.pid"))
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok())
    else {
        return;
    };
    if pid == std::process::id() || Some(pid) == spare {
        return;
    }
    let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
    let deadline = Instant::now() + Duration::from_secs(5);
    while process_alive(pid) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// RAII teardown: SIGKILLs whichever daemon owns `home` when this guard
/// drops, on both the success path and a panicking assertion. Declared
/// *after* the `TempDir` it guards so it drops *before* that `TempDir`'s own
/// destructor removes the directory (Rust drops locals in reverse
/// declaration order) — the pid file must still exist when this reads it.
struct DaemonGuard {
    home: std::path::PathBuf,
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        kill_owning_daemon(&self.home, None);
    }
}

/// SIGKILL the whole process group `pid` belongs to, not just `pid` itself —
/// `release::run_recipe` spawns the real `cargo build` child with
/// `.process_group(0)`, its own group, so cargo/rustc siblings do not die
/// with any single member alone. Best-effort: a `ps` lookup that fails (the
/// pid is already gone) or a group id of `0`/`1` (never a legitimate
/// leader for a child this test spawned) means nothing to clean up.
fn kill_process_group_of(pid: u32) {
    let Ok(out) = Command::new("ps")
        .args(["-o", "pgid=", "-p", &pid.to_string()])
        .output()
    else {
        return;
    };
    let Ok(pgid) = String::from_utf8_lossy(&out.stdout).trim().parse::<i64>() else {
        return;
    };
    if pgid <= 1 {
        return;
    }
    let _ = Command::new("kill")
        .args(["-9", &format!("-{pgid}")])
        .status();
}

/// The pids currently recorded under `home`'s managed-children directory —
/// `crate::managed_verification::ManagedChildMarker` in production, written
/// the instant `release::run_recipe` spawns the real `sh -c cargo build ...`
/// child (`.process_group(0)`, its own process group) and removed the
/// instant that child exits. This is the daemon's OWN durable record of the
/// exact process it owns, not an inference from a marker file the fixture's
/// build script writes minutes later.
fn managed_child_pids(home: &Path) -> std::collections::BTreeSet<u32> {
    let dir = rk_core::paths::Layout::at(home).managed_children_dir();
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Default::default();
    };
    entries
        .flatten()
        .filter_map(|e| e.file_name().to_string_lossy().parse::<u32>().ok())
        .collect()
}

/// RAII teardown for the real `cargo build` child's process group — see the
/// module doc's "Bounded process ownership" section for why [`DaemonGuard`]
/// alone cannot reach it. `owned_pid` starts unset and is filled in via
/// `Cell::set` as soon as this test observes the daemon's own
/// [`managed_child_pids`] record for the recipe child — BEFORE waiting on
/// the fixture's own build-script barrier marker, which only appears well
/// after `cargo`/`rustc` are already running inside that same process group.
/// `Drop` reads whatever was captured by then, so a panic before the recipe
/// was ever spawned at all simply has nothing to clean up.
struct BuildGroupGuard {
    owned_pid: Cell<Option<u32>>,
}

impl Drop for BuildGroupGuard {
    fn drop(&mut self) {
        if let Some(pid) = self.owned_pid.get() {
            kill_process_group_of(pid);
        }
    }
}

/// `Child` does not kill or reap on drop; own the detached `release prepare`
/// invocation through both the successful path and any assertion panic.
struct TestChild(std::process::Child);

impl Drop for TestChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn interrupted_preparation_is_reported_and_recovers_after_a_real_daemon_death() {
    let home = tempfile::tempdir().unwrap();
    // Declared right after `home`, so it drops right before `home`'s own
    // destructor removes the directory — see `DaemonGuard`'s doc comment.
    // Covers BOTH daemon A (if a panic strikes before the deliberate kill
    // below) and daemon B (there is no other cleanup for it at all).
    let _daemon_guard = DaemonGuard {
        home: home.path().to_path_buf(),
    };
    // Filled in once the real build reaches its barrier — see
    // `BuildGroupGuard`'s doc comment for why this is needed alongside
    // `_daemon_guard` rather than instead of it.
    let _build_group_guard = BuildGroupGuard {
        owned_pid: Cell::new(None),
    };
    disable_disk_floor(home.path());
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_name = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    init_fixture_repo(repo_dir.path(), "v1");

    // The first `rk` call auto-starts daemon A.
    json_stdout(&bounded_output(
        {
            let mut cmd = rk(home.path());
            cmd.args(["--json", "repo", "add", repo_dir.path().to_str().unwrap()]);
            cmd
        },
        RPC_BOUND,
    ));

    // An earlier, harmless release, prepared and left alone: proves the
    // crash/restart/retry sequence below never disturbs a bundle that was
    // never even part of the interrupted attempt ("preserved older bundles").
    let earlier = json_stdout(&bounded_output(
        {
            let mut cmd = rk(home.path());
            cmd.args([
                "--json",
                "release",
                "prepare",
                "--repo",
                &repo_name,
                "--candidate",
                "main",
            ]);
            cmd
        },
        PREPARE_BOUND,
    ));
    assert_eq!(earlier["release"]["status"], "prepared", "{earlier}");
    let earlier_id = earlier["release"]["id"].as_str().unwrap().to_string();

    // Now commit a blocking build script — `main` moves to a new commit, so
    // the next candidate this test resolves gets its own distinct release id
    // from the one prepared above.
    let scratch = tempfile::tempdir().unwrap();
    let blocker = scratch.path().join("hold-build");
    let started = scratch.path().join("build-started");
    std::fs::write(&blocker, b"hold").unwrap();
    write_blocking_build_script(&repo_dir.path().join("rk-cli"), &blocker, &started);
    git(repo_dir.path(), &["add", "."]);
    git(
        repo_dir.path(),
        &["commit", "-qm", "add blocking build script"],
    );

    let daemon_a = daemon_pid(home.path()).expect("daemon A must report a real pid");
    assert_ne!(
        daemon_a,
        std::process::id(),
        "refusing to SIGKILL this test process"
    );

    // Snapshot the daemon's own managed-children record BEFORE firing
    // prepare, so the pid that appears afterward is unambiguously the new
    // recipe child, not a leftover from the earlier harmless prepare above
    // (which already exited and had its own marker removed by the time it
    // returned).
    let managed_before = managed_child_pids(home.path());

    // Fire prepare on a detached process — it will block inside the real
    // `cargo build` until `blocker` is removed. Owned by `TestChild` so a
    // panic anywhere below still reaps it rather than leaking a process
    // stuck talking to a home this test is about to tear down.
    let _prepare_child = TestChild(
        rk(home.path())
            .args([
                "release",
                "prepare",
                "--repo",
                &repo_name,
                "--candidate",
                "main",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );

    // Capture ownership of the recipe's process group as soon as the daemon
    // itself records having spawned it — well BEFORE `cargo`/`rustc` reach
    // the fixture's own build-script barrier below. This is what closes the
    // pre-marker gap: `_build_group_guard` can now clean up the whole group
    // even if a panic strikes before that barrier is ever reached.
    let recipe_pid = until("the daemon to record its own owned recipe child", || {
        managed_child_pids(home.path())
            .difference(&managed_before)
            .next()
            .copied()
    });
    _build_group_guard.owned_pid.set(Some(recipe_pid));

    // Wait for the real build-script child to signal it's actually running.
    let build_pid = until("the real owned build to reach the barrier", || {
        std::fs::read_to_string(&started)
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok())
    });
    assert!(
        process_alive(build_pid),
        "the build-script child must be genuinely alive"
    );

    // Confirm the registry already shows durable intent before the crash.
    until("a durable Preparing intent to be recorded", || {
        let mut cmd = rk(home.path());
        cmd.args(["--json", "release", "list", "--repo", &repo_name]);
        let out = bounded_output(cmd, RPC_BOUND);
        if !out.status.success() {
            return None;
        }
        json_stdout(&out)
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .any(|r| r["status"] == "preparing")
            .then_some(())
    });
    assert!(
        process_alive(daemon_a),
        "daemon {daemon_a} must still be alive right before the kill — otherwise it is not \
         what crashed it"
    );

    // THE kill: a real SIGKILL of a real daemon process. No Drop, no
    // graceful shutdown, no hand-cleanup of the pid file or socket it leaves
    // behind — unlike the prior in-process `handle.abort()` version, this
    // reaches every task the process was running, including the one that
    // owned the in-flight `release.prepare` call.
    Command::new("kill")
        .args(["-9", &daemon_a.to_string()])
        .status()
        .unwrap();
    until("daemon A to actually die", || {
        (!process_alive(daemon_a)).then_some(())
    });

    // Bring a second real daemon up the way the field does: an ordinary RPC
    // call auto-starts one, and `Server::run`'s stale-socket reclamation
    // takes over because daemon A's recorded pid is now confirmed dead.
    let daemon_b = start_daemon(home.path());
    assert_ne!(
        daemon_b, daemon_a,
        "daemon B must be a genuinely new process, not the one we killed"
    );

    // No live prepare is running under daemon B for this stale intent —
    // `effective_status` must downgrade it rather than claim an active
    // build.
    let listed = json_stdout(&bounded_output(
        {
            let mut cmd = rk(home.path());
            cmd.args(["--json", "release", "list", "--repo", &repo_name]);
            cmd
        },
        RPC_BOUND,
    ));
    let releases = listed.as_array().cloned().unwrap_or_default();
    assert_eq!(releases.len(), 2, "{releases:?}");
    let interrupted = releases
        .iter()
        .find(|r| r["id"].as_str() != Some(earlier_id.as_str()))
        .expect("the interrupted attempt must have its own registry entry");
    assert_eq!(interrupted["status"], "unknown", "{releases:?}");

    // Confirmed original owned descendant gone: `reap_stale_managed_children`
    // runs at the very start of `Daemon::run`, before it can serve a single
    // request, so daemon B's own startup — already observed above via
    // `start_daemon` — has already reaped the orphaned build-script child by
    // this point.
    assert!(
        !process_alive(build_pid),
        "the orphaned build-script child must not survive daemon B's startup reap"
    );

    // Release the (already-reaped) build barrier so a fresh retry's OWN
    // build can actually complete — the fixture source still carries the
    // blocking build script at this exact commit.
    std::fs::remove_file(&blocker).ok();

    let resumed = json_stdout(&bounded_output(
        {
            let mut cmd = rk(home.path());
            cmd.args([
                "--json",
                "release",
                "prepare",
                "--repo",
                &repo_name,
                "--candidate",
                "main",
            ]);
            cmd
        },
        PREPARE_BOUND,
    ));
    assert_eq!(resumed["release"]["status"], "prepared", "{resumed}");
    let id = resumed["release"]["id"].as_str().unwrap().to_string();
    let shown = json_stdout(&bounded_output(
        {
            let mut cmd = rk(home.path());
            cmd.args(["--json", "release", "show", &id]);
            cmd
        },
        RPC_BOUND,
    ));
    assert_eq!(shown["content_verified"], true, "{shown}");

    // Preserved older bundle: the earlier, unrelated release survived the
    // whole crash/restart/retry sequence untouched and still verifies.
    let earlier_shown = json_stdout(&bounded_output(
        {
            let mut cmd = rk(home.path());
            cmd.args(["--json", "release", "show", &earlier_id]);
            cmd
        },
        RPC_BOUND,
    ));
    assert_eq!(
        earlier_shown["release"]["status"], "prepared",
        "{earlier_shown}"
    );
    assert_eq!(earlier_shown["content_verified"], true, "{earlier_shown}");

    // No manual `rk daemon stop`/pid-file cleanup here: `_daemon_guard`'s
    // `Drop` tears down whichever daemon (B, or A again if something above
    // panicked before the kill) still owns `home`, on every exit path.
}

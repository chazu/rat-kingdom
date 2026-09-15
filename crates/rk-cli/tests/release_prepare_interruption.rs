//! Real process-death and restart recovery for release.prepare.
//! Aborting a listener task leaves per-connection builds alive, so this test SIGKILLs a daemon.
//! Every RPC is bounded; RAII guards clean the daemon and separately owned build group.
//! Capture build ownership from the daemon marker before the fixture barrier, and recheck the
//! recorded process-start signature before signalling a group whose PID might be reused.

use serde_json::Value;
use std::cell::Cell;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

/// Bound for an ordinary `rk` RPC round-trip (list/show/status/repo-add):
/// generous against real host contention, but short enough that a genuinely
/// stuck call fails the test instead of hanging it.
const RPC_BOUND: Duration = Duration::from_secs(30);
/// Bound the small real fixture build while allowing startup on a loaded host.
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

/// Drain output while waiting; kill and fail at the deadline without filling a pipe buffer.
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

/// Dependency-free paired Cargo fixture: CLI stamp and a minimal MCP initialize responder.
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

/// A real build-script process records its PID and blocks until its marker is removed.
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

/// Exercise ordinary connect_or_spawn and stale-socket reclaim after real daemon death.
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

/// Best-effort cleanup from the owned home pidfile, without relying on a responsive RPC.
/// Never signal this test or the already-dead spare PID.
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

/// Drop before the guarded TempDir so its pidfile still exists during daemon cleanup.
struct DaemonGuard {
    home: std::path::PathBuf,
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        kill_owning_daemon(&self.home, None);
    }
}

/// Kill the validated build process group, excluding missing or invalid group IDs.
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

/// Use the same live start-time signature as the daemon marker; errors return None.
fn process_start_signature(pid: u32) -> Option<String> {
    let out = Command::new("ps")
        .args(["-o", "lstart=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!text.is_empty()).then_some(text)
}

/// Read each daemon-owned child PID with its recorded start signature, not just its filename.
fn managed_children(home: &Path) -> std::collections::BTreeMap<u32, String> {
    let dir = rk_core::paths::Layout::at(home).managed_children_dir();
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Default::default();
    };
    entries
        .flatten()
        .filter_map(|e| {
            let pid = e.file_name().to_string_lossy().parse::<u32>().ok()?;
            let recorded = std::fs::read_to_string(e.path()).ok()?.trim().to_string();
            (!recorded.is_empty()).then_some((pid, recorded))
        })
        .collect()
}

/// Capture ownership before the fixture barrier; revalidate the start signature before cleanup.
struct BuildGroupGuard {
    owned: Cell<Option<(u32, String)>>,
}

impl Drop for BuildGroupGuard {
    fn drop(&mut self) {
        if let Some((pid, recorded)) = self.owned.take() {
            if process_start_signature(pid).as_deref() == Some(recorded.as_str()) {
                kill_process_group_of(pid);
            }
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
    // Clean either daemon before the home directory is removed, including on panic.
    let _daemon_guard = DaemonGuard {
        home: home.path().to_path_buf(),
    };
    // Filled in once the real build reaches its barrier — see
    // `BuildGroupGuard`'s doc comment for why this is needed alongside
    // `_daemon_guard` rather than instead of it.
    let _build_group_guard = BuildGroupGuard {
        owned: Cell::new(None),
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

    // Snapshot prior markers so the new recipe child is identifiable.
    let managed_before = managed_children(home.path());

    // Own the blocked prepare client so panic cleanup cannot leave it waiting on a dead daemon.
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

    // Capture the daemon marker before the build-script barrier to cover early failures.
    let (recipe_pid, recipe_signature) =
        until("the daemon to record its own owned recipe child", || {
            managed_children(home.path())
                .into_iter()
                .find(|(pid, _)| !managed_before.contains_key(pid))
        });
    _build_group_guard
        .owned
        .set(Some((recipe_pid, recipe_signature)));

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

    // Kill the actual daemon process and all its tasks; leave stale state for normal recovery.
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

    // Daemon startup must have reaped the original owned build before serving requests.
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

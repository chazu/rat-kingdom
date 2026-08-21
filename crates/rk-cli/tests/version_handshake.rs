//! A stale daemon must be impossible to miss, and a matched one must be
//! silent.
//!
//! Both halves are checked against the real `rk` binary over a real socket.
//! The daemon on the far end is a hand-rolled stub rather than a `Daemon`,
//! because the case under test — a daemon running *different code* — cannot
//! otherwise be staged: everything in one `cargo test` run is a single build
//! and so, by construction, agrees with itself.
//!
//! The CLI side of that agreement is *not* free, though: `CARGO_BIN_EXE_rk`
//! names a fixed path under `target/`, and the `verify` check's
//! `sharedCargoTarget` policy runs many worktrees' builds against that same
//! `target/` concurrently, so the binary sitting there when a test spawns it
//! can be a different commit's build than the one this test binary itself
//! was linked against — this test process's own `rk_core::version::BUILD_VERSION`
//! then disagrees with what the spawned `rk` actually reports about itself.
//! Every fixture below asks the freshly spawned `rk` what build it thinks it
//! is (`cli_local_version`) instead of assuming that agreement.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::process::Command;

use rk_core::paths::Layout;

/// Serve one `rk` invocation, answering every request with `pong` stamped as
/// coming from `server_version`. Returns once the client disconnects.
fn stub_daemon(layout: &Layout, server_version: String) -> std::thread::JoinHandle<()> {
    let listener = UnixListener::bind(layout.socket_path()).expect("bind stub socket");
    std::thread::spawn(move || {
        let Ok((stream, _)) = listener.accept() else {
            return;
        };
        let mut write = stream.try_clone().expect("clone stub stream");
        let mut read = BufReader::new(stream);
        let mut line = String::new();
        while read.read_line(&mut line).unwrap_or(0) > 0 {
            let req: serde_json::Value = serde_json::from_str(&line).expect("stub got bad json");
            let id = req["id"].as_str().unwrap_or("0");
            let reply = serde_json::json!({
                "id": id,
                "result": "pong",
                "server_version": server_version,
            });
            if writeln!(write, "{reply}").is_err() {
                return;
            }
            line.clear();
        }
    })
}

/// Serve one `rk` invocation with an unstamped reply — exactly what a daemon
/// built before this handshake sends back.
fn stub_daemon_unstamped(layout: &Layout) -> std::thread::JoinHandle<()> {
    let listener = UnixListener::bind(layout.socket_path()).expect("bind stub socket");
    std::thread::spawn(move || {
        let Ok((stream, _)) = listener.accept() else {
            return;
        };
        let mut write = stream.try_clone().expect("clone stub stream");
        let mut read = BufReader::new(stream);
        let mut line = String::new();
        while read.read_line(&mut line).unwrap_or(0) > 0 {
            let req: serde_json::Value = serde_json::from_str(&line).expect("stub got bad json");
            let id = req["id"].as_str().unwrap_or("0");
            if writeln!(write, r#"{{"id":"{id}","result":"pong"}}"#).is_err() {
                return;
            }
            line.clear();
        }
    })
}

/// `rk ping`, whose stderr is the operator-facing surface under test.
fn ping(home: &std::path::Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rk"))
        .arg("ping")
        .env("RK_HOME", home)
        // A rat's spawn env would otherwise pick the agent identity; the
        // handshake is about builds, not callers, so pin the plain case.
        .env_remove("RK_AGENT")
        .env_remove("RK_AUTH_TOKEN")
        .output()
        .expect("run rk ping")
}

fn home_with_token() -> tempfile::TempDir {
    let home = tempfile::tempdir().unwrap();
    // Mint the bearer token up front: the client resolves it before it ever
    // writes to the socket, so a missing one fails the call for the wrong
    // reason.
    Layout::at(home.path()).auth_token().unwrap();
    home
}

/// The build version the freshly spawned `rk` binary reports about itself,
/// discovered by asking it (via the "too old to stamp" branch, which always
/// names the CLI's own build) rather than assumed to equal this test
/// process's own `rk_core::version::BUILD_VERSION` — see the module doc for
/// why those two can differ.
fn cli_local_version() -> String {
    let home = home_with_token();
    let layout = Layout::at(home.path());
    let server = stub_daemon_unstamped(&layout);

    let out = ping(home.path());
    server.join().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);

    let marker = "this CLI is ";
    let start = stderr
        .find(marker)
        .unwrap_or_else(|| panic!("expected a build-mismatch warning naming the CLI: {stderr}"))
        + marker.len();
    let rest = &stderr[start..];
    let end = rest
        .find(',')
        .unwrap_or_else(|| panic!("expected the CLI's build id to end in a comma: {stderr}"));
    rest[..end].to_string()
}

#[test]
fn a_daemon_on_another_build_warns_naming_both() {
    let local = cli_local_version();
    let home = home_with_token();
    let layout = Layout::at(home.path());
    let server = stub_daemon(&layout, "0.1.0+deadbeefcafe".to_string());

    let out = ping(home.path());
    let stderr = String::from_utf8_lossy(&out.stderr);

    assert!(out.status.success(), "the call still succeeds: {stderr}");
    assert!(
        stderr.contains("0.1.0+deadbeefcafe"),
        "warning must name the daemon's build: {stderr}"
    );
    assert!(
        stderr.contains(&local),
        "warning must name the CLI's own build: {stderr}"
    );
    assert!(
        stderr.contains("rk daemon rollover"),
        "warning must say how to fix it: {stderr}"
    );
    // The warning is advice, not an error: a mismatched daemon still answers,
    // and stdout stays clean for anything parsing it.
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "pong");

    drop(out);
    server.join().unwrap();
}

#[test]
fn a_matched_daemon_says_nothing() {
    let local = cli_local_version();
    let home = home_with_token();
    let layout = Layout::at(home.path());
    let server = stub_daemon(&layout, local);

    let out = ping(home.path());
    let stderr = String::from_utf8_lossy(&out.stderr);

    assert!(out.status.success(), "{stderr}");
    assert!(
        !stderr.contains("build mismatch"),
        "matched builds must be silent, got: {stderr}"
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "pong");

    drop(out);
    server.join().unwrap();
}

#[test]
fn a_daemon_too_old_to_stamp_its_replies_still_warns() {
    let home = home_with_token();
    let layout = Layout::at(home.path());
    let server = stub_daemon_unstamped(&layout);

    let out = ping(home.path());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("build mismatch") && stderr.contains("this CLI is "),
        "an unstamped reply is itself proof of an older build: {stderr}"
    );

    drop(out);
    server.join().unwrap();
}

#[test]
fn the_warning_is_printed_once_however_many_calls_it_takes() {
    let home = home_with_token();
    let layout = Layout::at(home.path());
    let server = stub_daemon(&layout, "0.1.0+deadbeefcafe".to_string());

    let out = ping(home.path());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(
        stderr.matches("build mismatch").count(),
        1,
        "one invocation, one warning — repeated it becomes noise: {stderr}"
    );

    drop(out);
    server.join().unwrap();
}

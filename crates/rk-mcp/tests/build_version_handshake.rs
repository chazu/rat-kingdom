//! `rk-mcp` is its own standalone executable — not a subcommand of `rk` — so
//! `crates/rk-mcp/build.rs` stamps its commit independently and
//! `rk_core::version::init_build_sha` must be called at the top of *its*
//! `main`, separately from `rk-cli`'s. This mirrors
//! `crates/rk-cli/tests/version_handshake.rs` (the same shared
//! `rk_daemon::client::warn_on_version_mismatch` logic, exercised through the
//! real `rk-mcp` binary instead of `rk`), to prove the MCP leaf actually got
//! that same treatment rather than silently reporting the fallback
//! `0.1.0+unknown` to every daemon it talks to.

use std::io::Write;
use std::os::unix::net::UnixListener;
use std::process::{Command, Output, Stdio};

use rk_core::paths::Layout;
use serde_json::{json, Value};

fn stub_daemon(layout: &Layout, server_version: String) -> std::thread::JoinHandle<()> {
    let listener = UnixListener::bind(layout.socket_path()).expect("bind stub socket");
    std::thread::spawn(move || {
        let Ok((stream, _)) = listener.accept() else {
            return;
        };
        let mut write = stream.try_clone().expect("clone stub stream");
        let mut read = std::io::BufReader::new(stream);
        let mut line = String::new();
        use std::io::BufRead;
        while read.read_line(&mut line).unwrap_or(0) > 0 {
            let req: Value = serde_json::from_str(&line).expect("stub got bad json");
            let id = req["id"].as_str().unwrap_or("0");
            let reply = json!({
                "id": id,
                "result": {"ok": true},
                "server_version": server_version,
            });
            if writeln!(write, "{reply}").is_err() {
                return;
            }
            line.clear();
        }
    })
}

fn stub_daemon_unstamped(layout: &Layout) -> std::thread::JoinHandle<()> {
    let listener = UnixListener::bind(layout.socket_path()).expect("bind stub socket");
    std::thread::spawn(move || {
        let Ok((stream, _)) = listener.accept() else {
            return;
        };
        let mut write = stream.try_clone().expect("clone stub stream");
        let mut read = std::io::BufReader::new(stream);
        let mut line = String::new();
        use std::io::BufRead;
        while read.read_line(&mut line).unwrap_or(0) > 0 {
            let req: Value = serde_json::from_str(&line).expect("stub got bad json");
            let id = req["id"].as_str().unwrap_or("0");
            if writeln!(write, r#"{{"id":"{id}","result":{{"ok":true}}}}"#).is_err() {
                return;
            }
            line.clear();
        }
    })
}

fn home_with_token() -> tempfile::TempDir {
    let home = tempfile::tempdir().unwrap();
    Layout::at(home.path()).auth_token().unwrap();
    home
}

fn tool_call_request(id: u64) -> Vec<u8> {
    let req = json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "tools/call",
        "params": {
            "name": "factory_snapshot",
            "arguments": {"schema": 1, "repo": "x"}
        }
    });
    let mut bytes = serde_json::to_vec(&req).unwrap();
    bytes.push(b'\n');
    bytes
}

/// Drives the real `rk-mcp` binary over stdio: one `tools/call`, which is the
/// only path that opens a real daemon connection (`initialize`/`tools/list`
/// never do).
fn call_rk_mcp(home: &std::path::Path) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_rk-mcp"))
        .env("RK_HOME", home)
        // A rat's spawn env would otherwise pick the agent identity; the
        // handshake is about builds, not callers, so pin the plain case.
        .env_remove("RK_AGENT")
        .env_remove("RK_AUTH_TOKEN")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn rk-mcp");
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(&tool_call_request(1))
        .unwrap();
    drop(child.stdin.take());
    child.wait_with_output().expect("run rk-mcp")
}

/// The build version the freshly spawned `rk-mcp` binary reports about
/// itself — discovered the same way `cli_local_version()` does in the `rk-cli`
/// handshake test, by asking it rather than assuming it matches this test
/// binary's own build.
fn mcp_local_version() -> String {
    let home = home_with_token();
    let layout = Layout::at(home.path());
    let server = stub_daemon_unstamped(&layout);

    let out = call_rk_mcp(home.path());
    server.join().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);

    let marker = "this CLI is ";
    let start = stderr
        .find(marker)
        .unwrap_or_else(|| panic!("expected a build-mismatch warning naming rk-mcp: {stderr}"))
        + marker.len();
    let rest = &stderr[start..];
    let end = rest
        .find(',')
        .unwrap_or_else(|| panic!("expected rk-mcp's build id to end in a comma: {stderr}"));
    rest[..end].to_string()
}

#[test]
fn a_daemon_on_another_build_warns_naming_both() {
    let local = mcp_local_version();
    let home = home_with_token();
    let layout = Layout::at(home.path());
    let server = stub_daemon(&layout, "0.1.0+deadbeefcafe".to_string());

    let out = call_rk_mcp(home.path());
    let stderr = String::from_utf8_lossy(&out.stderr);

    assert!(
        stderr.contains("0.1.0+deadbeefcafe"),
        "warning must name the daemon's build: {stderr}"
    );
    assert!(
        stderr.contains(&local),
        "warning must name rk-mcp's own build, not the fallback 0.1.0+unknown \
         a missing init_build_sha call would report: {stderr}"
    );
    assert!(
        stderr.contains("rk daemon rollover"),
        "warning must say how to fix it: {stderr}"
    );

    drop(out);
    server.join().unwrap();
}

#[test]
fn a_matched_daemon_says_nothing() {
    let local = mcp_local_version();
    let home = home_with_token();
    let layout = Layout::at(home.path());
    let server = stub_daemon(&layout, local);

    let out = call_rk_mcp(home.path());
    let stderr = String::from_utf8_lossy(&out.stderr);

    assert!(
        !stderr.contains("build mismatch"),
        "matched builds must be silent, got: {stderr}"
    );

    drop(out);
    server.join().unwrap();
}

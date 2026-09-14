//! P6.1 (TKT-kogoj-lupun-gurab) end-to-end acceptance for `release.prepare`/
//! `release.list`/`release.show`, through the real daemon RPC/public-command
//! boundary — never by calling `rk_daemon::release`'s internals directly.
//!
//! The "recipe" under test really does invoke `cargo build --release -p
//! rk-cli -p rk-mcp`, but against a tiny, dependency-free, two-package fixture
//! workspace this file generates on the fly (packages literally named
//! `rk-cli`/`rk-mcp`, producing `rk`/`rk-mcp` binaries — matching the one
//! recipe `release::prepare` supports) rather than rebuilding the real
//! multi-hundred-file rat-kingdom workspace. A release build of this fixture
//! takes well under a second.

mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::{connect, register_repo};

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

/// A dependency-free two-package Cargo workspace: `rk-cli` (bin `rk`) prints
/// `stamp` and exits 0 on any args (a real, cheap stand-in for `rk --help`'s
/// "launches and exits cleanly" contract); `rk-mcp` (bin `rk-mcp`) speaks the
/// same minimal `initialize` JSON-RPC handshake the real `rk-mcp` does,
/// exiting cleanly on stdin EOF. Building this compiles in well under a
/// second — no external crates, so no network/registry access either.
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

fn init_fixture_repo(dir: &Path, stamp: &str) -> String {
    write_fixture_source(dir, stamp);
    git(dir, &["init", "-q", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    git(dir, &["add", "."]);
    git(dir, &["commit", "-qm", "fixture v1"]);
    git(dir, &["rev-parse", "HEAD"])
}

fn repo_name_of(repo: &Path) -> String {
    repo.file_name().unwrap().to_string_lossy().to_string()
}

async fn prepare(
    client: &mut Client,
    repo: &str,
    candidate: &str,
) -> Result<Value, rk_core::Error> {
    client
        .call(
            "release.prepare",
            json!({"repo": repo, "candidate": candidate}),
        )
        .await
}

async fn show(client: &mut Client, id: &str) -> Value {
    client
        .call("release.show", json!({"id": id}))
        .await
        .unwrap()
}

#[tokio::test]
async fn prepare_lists_and_shows_a_verified_release() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let source = init_fixture_repo(repo_dir.path(), "v1");
    let repo_name = repo_name_of(repo_dir.path());

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    register_repo(&mut client, repo_dir.path()).await;

    let result = prepare(&mut client, &repo_name, "main").await.unwrap();
    let release = &result["release"];
    assert_eq!(release["status"], "prepared", "{result}");
    assert_eq!(result["already_prepared"], false);
    let manifest = &release["manifest"];
    assert_eq!(manifest["source"]["resolved_commit"], source);
    assert_eq!(
        manifest["binaries"].as_object().unwrap().keys().collect::<Vec<_>>().len(),
        2
    );
    assert!(manifest["binaries"]["rk"]["sha256"].as_str().unwrap().len() == 64);
    let checks = manifest["checks"].as_array().unwrap();
    assert_eq!(checks.len(), 2, "{checks:?}");
    assert!(checks.iter().all(|c| c["passed"] == true), "{checks:?}");

    let id = release["id"].as_str().unwrap().to_string();
    let shown = show(&mut client, &id).await;
    assert_eq!(shown["release"]["status"], "prepared");
    assert_eq!(shown["content_verified"], true, "{shown}");

    let listed = client
        .call("release.list", json!({"repo": repo_name}))
        .await
        .unwrap();
    let releases = listed["releases"].as_array().unwrap();
    assert_eq!(releases.len(), 1);
    assert_eq!(releases[0]["id"], id);
}

#[tokio::test]
async fn duplicate_prepare_is_idempotent() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_fixture_repo(repo_dir.path(), "v1");
    let repo_name = repo_name_of(repo_dir.path());

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    register_repo(&mut client, repo_dir.path()).await;

    let first = prepare(&mut client, &repo_name, "main").await.unwrap();
    let second = prepare(&mut client, &repo_name, "main").await.unwrap();
    assert_eq!(first["release"]["id"], second["release"]["id"]);
    assert_eq!(second["already_prepared"], true, "{second}");
    assert_eq!(
        first["release"]["manifest"]["binaries"],
        second["release"]["manifest"]["binaries"]
    );
}

#[tokio::test]
async fn different_candidates_get_different_releases_with_correct_provenance() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let source_v1 = init_fixture_repo(repo_dir.path(), "v1");
    write_fixture_source(repo_dir.path(), "v2");
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-qm", "fixture v2"]);
    let source_v2 = git(repo_dir.path(), &["rev-parse", "HEAD"]);
    let repo_name = repo_name_of(repo_dir.path());

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    register_repo(&mut client, repo_dir.path()).await;

    let first = prepare(&mut client, &repo_name, &source_v1).await.unwrap();
    let second = prepare(&mut client, &repo_name, &source_v2).await.unwrap();
    assert_ne!(first["release"]["id"], second["release"]["id"]);
    assert_eq!(
        first["release"]["manifest"]["source"]["resolved_commit"],
        source_v1
    );
    assert_eq!(
        second["release"]["manifest"]["source"]["resolved_commit"],
        source_v2
    );
    assert_ne!(
        first["release"]["manifest"]["binaries"]["rk"]["sha256"],
        second["release"]["manifest"]["binaries"]["rk"]["sha256"],
        "different source must produce different binary content"
    );
    // The earlier release must remain untouched and independently verified —
    // preparing a second candidate must not disturb the first.
    let id = first["release"]["id"].as_str().unwrap().to_string();
    assert_eq!(show(&mut client, &id).await["content_verified"], true);
}

#[tokio::test]
async fn unsupported_recipe_is_rejected() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_fixture_repo(repo_dir.path(), "v1");
    let repo_name = repo_name_of(repo_dir.path());

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    register_repo(&mut client, repo_dir.path()).await;

    let err = client
        .call(
            "release.prepare",
            json!({"repo": repo_name, "candidate": "main", "recipe": "bogus-recipe"}),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unsupported recipe"), "{err}");
}

#[tokio::test]
async fn restart_recovers_the_prepared_release() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_fixture_repo(repo_dir.path(), "v1");
    let repo_name = repo_name_of(repo_dir.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let config = rk_core::config::Config::default();

    let daemon_a = Daemon::new(layout.clone(), &config).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;
    register_repo(&mut client, repo_dir.path()).await;
    let prepared = prepare(&mut client, &repo_name, "main").await.unwrap();
    let id = prepared["release"]["id"].as_str().unwrap().to_string();

    handle_a.abort();
    let _ = handle_a.await;
    std::fs::remove_file(layout.pid_file()).ok();
    std::fs::remove_file(layout.socket_path()).ok();

    let daemon_b = Daemon::new(layout.clone(), &config).unwrap();
    let _handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;
    let shown = show(&mut client, &id).await;
    assert_eq!(shown["release"]["status"], "prepared", "{shown}");
    assert_eq!(shown["content_verified"], true, "{shown}");
}

#[tokio::test]
async fn tampered_binary_is_detected_and_rejected() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_fixture_repo(repo_dir.path(), "v1");
    let repo_name = repo_name_of(repo_dir.path());

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    register_repo(&mut client, repo_dir.path()).await;
    let prepared = prepare(&mut client, &repo_name, "main").await.unwrap();
    let id = prepared["release"]["id"].as_str().unwrap().to_string();

    let bin_path = layout.home().join("releases").join(&id).join("rk");
    let original = std::fs::read(&bin_path).unwrap();
    let mut tampered = original.clone();
    tampered.extend_from_slice(b"CORRUPTION");
    std::fs::write(&bin_path, &tampered).unwrap();

    let shown = show(&mut client, &id).await;
    assert_eq!(shown["content_verified"], false, "{shown}");

    let retry = prepare(&mut client, &repo_name, "main").await;
    assert!(retry.is_err(), "a tampered release must refuse to be reused: {retry:?}");
    // Refusing to reuse it must not silently repair it either.
    assert_eq!(std::fs::read(&bin_path).unwrap(), tampered);
}

#[tokio::test]
async fn tampered_manifest_field_is_detected_even_without_touching_binaries() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_fixture_repo(repo_dir.path(), "v1");
    let repo_name = repo_name_of(repo_dir.path());

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    register_repo(&mut client, repo_dir.path()).await;
    let prepared = prepare(&mut client, &repo_name, "main").await.unwrap();
    let id = prepared["release"]["id"].as_str().unwrap().to_string();

    let manifest_path = layout.home().join("releases").join(&id).join("manifest.json");
    let original = std::fs::read(&manifest_path).unwrap();
    let mut edited: Value = serde_json::from_slice(&original).unwrap();
    // Edit a field that isn't a binary hash — proves the whole-manifest
    // digest catches more than just binary tampering.
    edited["config_provenance"]["cargo_build_jobs_env"] = json!("77");
    std::fs::write(&manifest_path, serde_json::to_vec(&edited).unwrap()).unwrap();

    let shown = show(&mut client, &id).await;
    assert_eq!(shown["content_verified"], false, "{shown}");
    let retry = prepare(&mut client, &repo_name, "main").await;
    assert!(retry.is_err(), "{retry:?}");
}

#[tokio::test]
async fn prepared_entry_with_missing_manifest_is_rejected_not_rebuilt() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_fixture_repo(repo_dir.path(), "v1");
    let repo_name = repo_name_of(repo_dir.path());

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    register_repo(&mut client, repo_dir.path()).await;
    let prepared = prepare(&mut client, &repo_name, "main").await.unwrap();
    let id = prepared["release"]["id"].as_str().unwrap().to_string();

    let manifest_path = layout.home().join("releases").join(&id).join("manifest.json");
    std::fs::remove_file(&manifest_path).unwrap();

    let retry = prepare(&mut client, &repo_name, "main").await;
    assert!(retry.is_err(), "{retry:?}");
    assert!(
        retry.unwrap_err().to_string().contains("manifest is missing"),
        "must name the actual failure"
    );
    assert!(
        !manifest_path.exists(),
        "a missing prepared manifest must not be silently rebuilt"
    );
}

#[tokio::test]
async fn unsupported_manifest_schema_is_refused() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_fixture_repo(repo_dir.path(), "v1");
    let repo_name = repo_name_of(repo_dir.path());

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    register_repo(&mut client, repo_dir.path()).await;
    let prepared = prepare(&mut client, &repo_name, "main").await.unwrap();
    let id = prepared["release"]["id"].as_str().unwrap().to_string();

    let manifest_path = layout.home().join("releases").join(&id).join("manifest.json");
    let mut edited: Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    edited["schema_version"] = json!(999_999);
    std::fs::write(&manifest_path, serde_json::to_vec(&edited).unwrap()).unwrap();

    let err = client
        .call("release.show", json!({"id": id}))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("schema_version"), "{err}");
}

/// A REAL Cargo build-script barrier: `rk-cli/build.rs` blocks (polling for a
/// marker file's removal) before the actual crate compiles, giving a
/// deterministic window in which to abort the daemon mid-build and prove the
/// daemon-owned child survives the abort as an orphan (`ManagedChildMarker`),
/// then gets reaped and the interrupted `Preparing` record is recoverable on
/// restart — never silently rebuilt into a false "prepared" claim, and never
/// stuck unrecoverable either.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_preparation_is_reported_and_recovers_on_retry_after_restart() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_fixture_repo(repo_dir.path(), "v1");
    let repo_name = repo_name_of(repo_dir.path());
    let scratch = tempfile::tempdir().unwrap();
    let blocker = scratch.path().join("hold-build");
    let started = scratch.path().join("build-started");
    std::fs::write(&blocker, b"hold").unwrap();
    write_blocking_build_script(&repo_dir.path().join("rk-cli"), &blocker, &started);
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-qm", "add blocking build script"]);

    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let config = rk_core::config::Config::default();
    let daemon_a = Daemon::new(layout.clone(), &config).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;
    register_repo(&mut client, repo_dir.path()).await;

    // Fire prepare on a background task — it will block inside the real
    // `cargo build` until `blocker` is removed.
    let mut prepare_client = connect(&layout).await;
    let repo_name_bg = repo_name.clone();
    let prepare_task = tokio::spawn(async move {
        prepare(&mut prepare_client, &repo_name_bg, "main").await
    });

    // Wait for the real build-script child to signal it's actually running.
    let mut waited = Duration::ZERO;
    while !started.exists() && waited < Duration::from_secs(30) {
        tokio::time::sleep(Duration::from_millis(50)).await;
        waited += Duration::from_millis(50);
    }
    assert!(started.exists(), "the real owned build never reached the barrier");

    // Confirm the registry already shows durable intent before the crash.
    let mut saw_preparing = false;
    for _ in 0..50 {
        let listed = client
            .call("release.list", json!({"repo": &repo_name}))
            .await
            .unwrap();
        if listed["releases"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["status"] == "preparing")
        {
            saw_preparing = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(saw_preparing, "no durable Preparing intent was ever recorded");

    // The kill: abort the daemon's task outright (see live_landing_restart.rs
    // for why this — not a graceful stop — is what actually cuts an in-flight
    // async future off mid-build). The real `sh -c cargo build ...` child
    // (and the build.rs it's blocked in) is NOT killed by this — it has no
    // `kill_on_drop` (see `managed_verification`/`release::run_smoke_check`'s
    // doc comments) — so it survives as a genuine orphan, exactly the
    // scenario `ManagedChildMarker`/`reap_stale_managed_children` exists for.
    handle_a.abort();
    let _ = handle_a.await;
    let _ = prepare_task.await;
    std::fs::remove_file(layout.pid_file()).ok();
    std::fs::remove_file(layout.socket_path()).ok();

    let daemon_b = Daemon::new(layout.clone(), &config).unwrap();
    let _handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;

    let listed = client
        .call("release.list", json!({"repo": &repo_name}))
        .await
        .unwrap();
    let releases = listed["releases"].as_array().unwrap();
    assert_eq!(releases.len(), 1, "{releases:?}");
    // No live prepare is running in the new daemon for this stale intent —
    // `effective_status` must downgrade it rather than claim an active build.
    assert_eq!(releases[0]["status"], "unknown", "{releases:?}");

    // Release the real orphaned build so the retry's OWN build (or the
    // reaped orphan, if `reap_stale_managed_children` lets it finish instead
    // of killing it) can actually complete.
    std::fs::remove_file(&blocker).ok();

    let resumed = prepare(&mut client, &repo_name, "main").await.unwrap();
    assert_eq!(resumed["release"]["status"], "prepared", "{resumed}");
    let id = resumed["release"]["id"].as_str().unwrap().to_string();
    assert_eq!(show(&mut client, &id).await["content_verified"], true);
}

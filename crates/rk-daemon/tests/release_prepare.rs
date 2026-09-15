//! Release prepare/list/show through a real daemon and a tiny dependency-free Cargo fixture.
//! Tests cover immutable pairing, reuse, integrity, and publication/recovery boundaries.

mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
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

/// A cheap paired Cargo workspace: CLI stamp and a minimal MCP initialize responder.
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
        manifest["binaries"]
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>()
            .len(),
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
    assert!(
        retry.is_err(),
        "a tampered release must refuse to be reused: {retry:?}"
    );
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

    let manifest_path = layout
        .home()
        .join("releases")
        .join(&id)
        .join("manifest.json");
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

    let manifest_path = layout
        .home()
        .join("releases")
        .join(&id)
        .join("manifest.json");
    std::fs::remove_file(&manifest_path).unwrap();

    let retry = prepare(&mut client, &repo_name, "main").await;
    assert!(retry.is_err(), "{retry:?}");
    assert!(
        retry
            .unwrap_err()
            .to_string()
            .contains("manifest is missing"),
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

    let manifest_path = layout
        .home()
        .join("releases")
        .join(&id)
        .join("manifest.json");
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

/// An internally consistent manifest without a prior registry digest must not be adopted.
#[tokio::test]
async fn self_consistent_manifest_without_a_registry_digest_is_quarantined_not_adopted() {
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
    let original_binary =
        std::fs::read(layout.home().join("releases").join(&id).join("rk")).unwrap();

    // Erase the registry's memory of this release entirely — the manifest
    // and binaries on disk remain perfectly self-consistent with each other,
    // but no digest this daemon committed backs them any more.
    let registry_path = layout.home().join("releases.json");
    let mut registry: Value =
        serde_json::from_slice(&std::fs::read(&registry_path).unwrap()).unwrap();
    registry.as_object_mut().unwrap().remove(&id);
    std::fs::write(&registry_path, serde_json::to_vec(&registry).unwrap()).unwrap();

    // A fresh `prepare` for the same candidate resolves to the same
    // content-derived id, finds a self-consistent-looking manifest with no
    // registry-backed trust, and must NOT adopt it as-is.
    let rebuilt = prepare(&mut client, &repo_name, "main").await.unwrap();
    assert_eq!(rebuilt["release"]["id"], id, "{rebuilt}");
    assert_eq!(rebuilt["release"]["status"], "prepared", "{rebuilt}");
    // Rebuilt fresh (not merely re-adopted), and still content-verifies —
    // the same deterministic fixture source produces byte-identical output.
    let rebuilt_binary =
        std::fs::read(layout.home().join("releases").join(&id).join("rk")).unwrap();
    assert_eq!(rebuilt_binary, original_binary);
    let shown = show(&mut client, &id).await;
    assert_eq!(shown["content_verified"], true, "{shown}");

    // The unattested manifest was preserved as inspectable quarantined
    // evidence, not deleted, and not silently reused in place.
    let quarantine_root = layout.home().join("releases-partial");
    let quarantined = std::fs::read_dir(&quarantine_root)
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().starts_with(&id));
    assert!(
        quarantined,
        "the unattested manifest/binaries must be quarantined, not silently discarded"
    );
}

// Real process-death/restart/retry coverage lives in rk-cli/tests/release_prepare_interruption.rs.

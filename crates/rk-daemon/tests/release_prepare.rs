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

/// P4.1 (TKT-nibuv-gokun-sibin): `[policy] release_build_admission_enabled`
/// routes the `paired-rk-mcp` build subprocess through the SAME P3.1
/// aggregate `HostVerificationAdmission` semaphore every managed named check
/// already shares. These tests reuse `host_verification_aggregate_cap.rs`'s
/// barrier-check-plus-status-poll technique (a controlled named check on a
/// SECOND fixture repo, never a fixed sleep as the success criterion) rather
/// than rebuilding a whole project per scenario — the release build itself
/// is the existing tiny two-package Cargo fixture above.
mod host_admission {
    use super::*;
    use std::time::{Duration, Instant};

    /// One barrier-controlled named check: writes its own pid to
    /// `<shared>/<marker>.pid` the instant it starts, then blocks — polling a
    /// short fixed interval, never sleeping past a bounded budget — until the
    /// test deposits `<shared>/<marker>.release`. Same technique as
    /// `host_verification_aggregate_cap.rs::barrier_check_body`.
    fn barrier_check_body(shared: &Path, marker: &str) -> String {
        let shared = shared.display();
        format!(
            r#"echo $$ > "{shared}/{marker}.pid"; for i in $(seq 1 600); do [ -f "{shared}/{marker}.release" ] && exit 0; sleep 0.05; done; echo "barrier {marker} never released" 1>&2; exit 9"#
        )
    }

    fn write_barrier_check(repo: &Path, shared: &Path, name: &str, marker: &str) {
        let body = barrier_check_body(shared, marker)
            .replace('\\', "\\\\")
            .replace('"', "\\\"");
        let cue = format!(
            "checks: [{{name: \"{name}\", command: \"{body}\", timeout: \"30s\", environmentPolicy: \"strip_rk_spawn\"}}]\n"
        );
        let rk_dir = repo.join(".rk");
        std::fs::create_dir_all(&rk_dir).unwrap();
        std::fs::write(rk_dir.join("checks.cue"), cue).unwrap();
    }

    /// A committed, policy-registered checker repo carrying one barrier
    /// check under `marker` — matches
    /// `host_verification_aggregate_cap.rs::init_repo` +
    /// `prepare_repo`'s exact sequencing (repository policy committed
    /// first, the check written uncommitted afterward).
    fn init_checker_repo(dir: &Path, shared: &Path, marker: &str) -> String {
        git(dir, &["init", "-q", "-b", "main"]);
        git(dir, &["config", "user.email", "r@x"]);
        git(dir, &["config", "user.name", "R"]);
        std::fs::write(dir.join("README.md"), "# checker\n").unwrap();
        git(dir, &["add", "."]);
        git(dir, &["commit", "-qm", "init"]);
        support::install_default_repository_policy(dir);
        write_barrier_check(dir, shared, "go", marker);
        repo_name_of(dir)
    }

    fn release_marker(shared: &Path, marker: &str) {
        std::fs::write(shared.join(format!("{marker}.release")), b"go").unwrap();
    }

    fn pid_path(shared: &Path, marker: &str) -> std::path::PathBuf {
        shared.join(format!("{marker}.pid"))
    }

    const POLL_DEADLINE: Duration = Duration::from_secs(15);
    const POLL_INTERVAL: Duration = Duration::from_millis(30);

    async fn wait_for_start(path: &Path) {
        let deadline = Instant::now() + POLL_DEADLINE;
        loop {
            if path.exists() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "check never started (no pid file at {}) within {POLL_DEADLINE:?}",
                path.display()
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn status(client: &mut Client) -> Value {
        client.call("status", json!({})).await.unwrap()
    }

    fn host_executing(s: &Value) -> u64 {
        s["verification_host"]["executing"].as_u64().unwrap_or(0)
    }

    fn host_waiting(s: &Value) -> u64 {
        s["verification_host"]["waiting"].as_u64().unwrap_or(0)
    }

    async fn poll_status_until(
        client: &mut Client,
        description: &str,
        mut pred: impl FnMut(&Value) -> bool,
    ) -> Value {
        let deadline = Instant::now() + POLL_DEADLINE;
        loop {
            let s = status(client).await;
            if pred(&s) {
                return s;
            }
            assert!(
                Instant::now() < deadline,
                "condition never became true within {POLL_DEADLINE:?}: {description}; last status: {s}"
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Spawn `release.prepare` for `(repo, candidate)` over its own fresh
    /// connection — genuine concurrency with a status-polling connection
    /// needs a separate one, same reasoning as
    /// `host_verification_aggregate_cap.rs::spawn_verify`.
    fn spawn_prepare(
        layout: Layout,
        repo: String,
        candidate: String,
    ) -> tokio::task::JoinHandle<Value> {
        tokio::spawn(async move {
            let mut client = Client::connect_as_operator(&layout).await.unwrap();
            client
                .call(
                    "release.prepare",
                    json!({"repo": repo, "candidate": candidate}),
                )
                .await
                .unwrap_or_else(|e| panic!("release.prepare({repo}) failed: {e}"))
        })
    }

    /// Disabled (the default): a release build must ignore a fully saturated
    /// aggregate cap entirely — it neither waits on, nor is counted by,
    /// `verification_host`. Proven by completing the build while a barrier
    /// check on a SEPARATE repo holds the aggregate cap's one and only
    /// permit open for the whole test, never released until after.
    #[tokio::test]
    async fn disabled_by_default_ignores_a_saturated_aggregate_cap() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        layout.ensure().unwrap();
        let shared = tempfile::tempdir().unwrap();
        let checker_dir = tempfile::tempdir().unwrap();
        let checker_name = init_checker_repo(checker_dir.path(), shared.path(), "chk");
        let release_dir = tempfile::tempdir().unwrap();
        init_fixture_repo(release_dir.path(), "v1");
        let release_name = repo_name_of(release_dir.path());

        let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
        daemon.set_verification_admission_aggregate_limit(1);
        // Admission left disabled (default `false`) — the switch under test.
        let _handle = tokio::spawn(daemon.run());
        let mut client = connect(&layout).await;
        register_repo(&mut client, checker_dir.path()).await;
        register_repo(&mut client, release_dir.path()).await;

        let checker_layout = layout.clone();
        let checker_repo = checker_name.clone();
        let checker_call = tokio::spawn(async move {
            let mut c = Client::connect_as_operator(&checker_layout).await.unwrap();
            c.call("verify.run", json!({"repo": checker_repo, "check": "go"}))
                .await
        });
        wait_for_start(&pid_path(shared.path(), "chk")).await;
        poll_status_until(
            &mut client,
            "checker occupies the one aggregate permit",
            |s| host_executing(s) == 1,
        )
        .await;

        // The release build must complete WITHOUT ever waiting on the
        // saturated aggregate cap — bounded well under the checker's own 30s
        // barrier budget, which is never released during this await.
        let result = tokio::time::timeout(
            Duration::from_secs(20),
            prepare(&mut client, &release_name, "main"),
        )
        .await
        .expect("a disabled release build must not block on the saturated aggregate cap")
        .unwrap();
        assert_eq!(result["release"]["status"], "prepared", "{result}");
        assert_eq!(
            result["release"]["manifest"]["recipe_bounds"]["host_admission"],
            Value::Null,
            "disabled admission must record no host_admission telemetry: {result}"
        );

        release_marker(shared.path(), "chk");
        checker_call.await.unwrap().unwrap();
    }

    /// Enabled: a release build genuinely queues behind, and is admitted
    /// alongside, an ordinary named check on a DIFFERENT repo through the
    /// SAME aggregate semaphore — real cross-repo, cross-request-type
    /// sharing, not a separate release-only capacity pool.
    #[tokio::test]
    async fn enabled_shares_the_aggregate_cap_with_a_concurrent_named_check() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        layout.ensure().unwrap();
        let shared = tempfile::tempdir().unwrap();
        let checker_dir = tempfile::tempdir().unwrap();
        let checker_name = init_checker_repo(checker_dir.path(), shared.path(), "chk");
        let release_dir = tempfile::tempdir().unwrap();
        init_fixture_repo(release_dir.path(), "v1");
        let release_name = repo_name_of(release_dir.path());

        let mut daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
        daemon.set_verification_admission_aggregate_limit(1);
        daemon.set_release_build_admission_enabled(true);
        let _handle = tokio::spawn(daemon.run());
        let mut client = connect(&layout).await;
        register_repo(&mut client, checker_dir.path()).await;
        register_repo(&mut client, release_dir.path()).await;

        let checker_layout = layout.clone();
        let checker_repo = checker_name.clone();
        let checker_call = tokio::spawn(async move {
            let mut c = Client::connect_as_operator(&checker_layout).await.unwrap();
            c.call("verify.run", json!({"repo": checker_repo, "check": "go"}))
                .await
        });
        wait_for_start(&pid_path(shared.path(), "chk")).await;
        poll_status_until(
            &mut client,
            "checker occupies the one aggregate permit",
            |s| host_executing(s) == 1,
        )
        .await;

        // The release build must now genuinely queue behind the checker on
        // the SAME host-wide semaphore — proven via the real status RPC, not
        // inferred from elapsed time.
        let prepare_call = spawn_prepare(layout.clone(), release_name.clone(), "main".into());
        poll_status_until(
            &mut client,
            "release build is genuinely queued behind the saturated aggregate cap",
            |s| host_executing(s) == 1 && host_waiting(s) == 1,
        )
        .await;

        release_marker(shared.path(), "chk");
        checker_call.await.unwrap().unwrap();

        let result = prepare_call.await.unwrap();
        assert_eq!(result["release"]["status"], "prepared", "{result}");
        let host_admission = &result["release"]["manifest"]["recipe_bounds"]["host_admission"];
        assert_eq!(
            host_admission["recipe_identity"],
            json!("release-build:paired-rk-mcp"),
            "{result}"
        );
        assert_eq!(host_admission["weight"], json!(1), "{result}");
        assert!(
            host_admission["admission_wait_ms"].as_u64().unwrap() > 0,
            "the build genuinely waited for the checker's permit, so its recorded wait must be \
             nonzero: {result}"
        );

        poll_status_until(&mut client, "capacity fully drains, no leak", |s| {
            host_executing(s) == 0 && host_waiting(s) == 0
        })
        .await;
    }

    /// P4.1 cancellation (TKT-nibuv-gokun-sibin): `release.prepare` now
    /// routes through `dispatch_watching_disconnect`, the same explicit
    /// RPC-disconnect signal `verify.run` already uses — registering with,
    /// and being cancelled through, the identical
    /// `ManagedVerificationRuns` registry, keyed on the identical
    /// `request_key`. A caller that disconnects while its build is
    /// genuinely QUEUED for the aggregate permit must release that wait
    /// promptly, and never falsely settle as `prepared`.
    #[tokio::test]
    async fn cancelling_a_queued_release_build_releases_the_wait_without_falsely_settling() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        layout.ensure().unwrap();
        let shared = tempfile::tempdir().unwrap();
        let checker_dir = tempfile::tempdir().unwrap();
        let checker_name = init_checker_repo(checker_dir.path(), shared.path(), "chk");
        let release_dir = tempfile::tempdir().unwrap();
        init_fixture_repo(release_dir.path(), "v1");
        let release_name = repo_name_of(release_dir.path());

        let mut daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
        daemon.set_verification_admission_aggregate_limit(1);
        daemon.set_release_build_admission_enabled(true);
        let _handle = tokio::spawn(daemon.run());
        let mut client = connect(&layout).await;
        register_repo(&mut client, checker_dir.path()).await;
        register_repo(&mut client, release_dir.path()).await;

        let checker_layout = layout.clone();
        let checker_repo = checker_name.clone();
        let checker_call = tokio::spawn(async move {
            let mut c = Client::connect_as_operator(&checker_layout).await.unwrap();
            c.call("verify.run", json!({"repo": checker_repo, "check": "go"}))
                .await
        });
        wait_for_start(&pid_path(shared.path(), "chk")).await;
        poll_status_until(
            &mut client,
            "checker occupies the one aggregate permit",
            |s| host_executing(s) == 1,
        )
        .await;

        // Fire the release build on its OWN connection so cancelling it does
        // not touch the polling connection — `JoinHandle::abort`, not a bare
        // `drop`, actually tears the connection down (same reasoning as
        // `host_verification_aggregate_cap.rs`'s own cancellation tests).
        let prepare_layout = layout.clone();
        let repo_for_call = release_name.clone();
        let prepare_conn = tokio::spawn(async move {
            let mut c = Client::connect_as_operator(&prepare_layout).await.unwrap();
            c.call(
                "release.prepare",
                json!({"repo": repo_for_call, "candidate": "main"}),
            )
            .await
        });
        poll_status_until(
            &mut client,
            "release build is genuinely queued behind the saturated aggregate cap",
            |s| host_executing(s) == 1 && host_waiting(s) == 1,
        )
        .await;

        prepare_conn.abort();
        let _ = prepare_conn.await;

        // Cancelling a QUEUED wait must never touch the permit the checker
        // still holds — only the waiting count drops. Nothing else could
        // ever free this wait: the checker's own barrier is never released
        // until after this assertion.
        poll_status_until(
            &mut client,
            "the cancelled release build's wait drops out of the queue",
            |s| host_executing(s) == 1 && host_waiting(s) == 0,
        )
        .await;

        // Truthful state: the release entry was durably marked `Preparing`
        // before the build ever waited on admission, and a cancelled build
        // never reaches its own `Prepared`/`Failed` write — `effective_status`
        // (release.rs) is what turns a stale `Preparing` with a now-free
        // `release_prepare_lock` into an honest `unknown`, never a false
        // `prepared`.
        let listed = client
            .call("release.list", json!({"repo": release_name}))
            .await
            .unwrap();
        let releases = listed["releases"].as_array().unwrap();
        assert_eq!(releases.len(), 1, "{listed:?}");
        assert_eq!(releases[0]["status"], json!("unknown"), "{listed:?}");

        release_marker(shared.path(), "chk");
        checker_call.await.unwrap().unwrap();
    }

    /// A `build.rs` script that, when compiled as part of the fixture's
    /// `rk-cli` package, writes its own pid then blocks on an explicit
    /// release file — the same barrier technique as
    /// `barrier_check_body`, applied to a REAL `cargo build` step instead of
    /// a named check, so a cancellation test can prove the release build's
    /// own owned child (not a stand-in) actually dies. The paths are baked
    /// in as string literals at fixture-generation time — no environment
    /// variables cross the `cargo build` boundary, so this cannot leak into
    /// or race with any other concurrently-running test in this binary.
    fn write_build_barrier(dir: &Path, shared: &Path, marker: &str) {
        let shared_display = shared.display();
        let build_rs = format!(
            r#"fn main() {{
    std::fs::write("{shared_display}/{marker}.pid", std::process::id().to_string()).unwrap();
    let release = std::path::Path::new("{shared_display}").join("{marker}.release");
    for _ in 0..600 {{
        if release.exists() {{
            return;
        }}
        std::thread::sleep(std::time::Duration::from_millis(50));
    }}
    panic!("build barrier {marker} never released");
}}
"#
        );
        std::fs::write(dir.join("rk-cli/build.rs"), build_rs).unwrap();
        let cargo_toml = std::fs::read_to_string(dir.join("rk-cli/Cargo.toml")).unwrap();
        let cargo_toml = cargo_toml.replacen("[package]\n", "[package]\nbuild = \"build.rs\"\n", 1);
        std::fs::write(dir.join("rk-cli/Cargo.toml"), cargo_toml).unwrap();
    }

    /// A committed fixture repo whose `rk-cli` package pauses at
    /// `write_build_barrier`'s marker partway through a real `cargo build`.
    fn init_fixture_repo_with_build_barrier(dir: &Path, shared: &Path, marker: &str) -> String {
        write_fixture_source(dir, "barrier");
        write_build_barrier(dir, shared, marker);
        git(dir, &["init", "-q", "-b", "main"]);
        git(dir, &["config", "user.email", "r@x"]);
        git(dir, &["config", "user.name", "R"]);
        git(dir, &["add", "."]);
        git(dir, &["commit", "-qm", "fixture with build barrier"]);
        git(dir, &["rev-parse", "HEAD"])
    }

    /// A caller that disconnects while its build is genuinely EXECUTING (its
    /// own real owned child paused mid-compile at a marker, holding the one
    /// aggregate permit) must actually kill that child — via the same
    /// `ProcessGroupGuard`-on-drop discipline `verify.run`'s own cancellation
    /// relies on, `HostVerificationAdmission` has no way to know a permit is
    /// abandoned other than the guard dropping — release its permit with no
    /// leak, and never falsely settle as `prepared`.
    #[tokio::test]
    async fn cancelling_an_executing_release_build_kills_its_owned_child_and_releases_the_permit() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        layout.ensure().unwrap();
        let shared = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_fixture_repo_with_build_barrier(repo_dir.path(), shared.path(), "build");
        let repo_name = repo_name_of(repo_dir.path());

        let mut daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
        daemon.set_verification_admission_aggregate_limit(1);
        daemon.set_release_build_admission_enabled(true);
        let _handle = tokio::spawn(daemon.run());
        let mut client = connect(&layout).await;
        register_repo(&mut client, repo_dir.path()).await;

        let prepare_layout = layout.clone();
        let repo_for_call = repo_name.clone();
        let prepare_conn = tokio::spawn(async move {
            let mut c = Client::connect_as_operator(&prepare_layout).await.unwrap();
            c.call(
                "release.prepare",
                json!({"repo": repo_for_call, "candidate": "main"}),
            )
            .await
        });

        // Wait for the build to genuinely reach its barrier — real
        // compilation underway, not merely admitted — and for the aggregate
        // permit to show as held.
        wait_for_start(&pid_path(shared.path(), "build")).await;
        poll_status_until(
            &mut client,
            "the release build occupies the one aggregate permit",
            |s| host_executing(s) == 1,
        )
        .await;
        let build_pid: i32 = std::fs::read_to_string(pid_path(shared.path(), "build"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();

        prepare_conn.abort();
        let _ = prepare_conn.await;

        // The real owned build child must actually die — a concrete OS pid
        // check, never inferred from daemon bookkeeping alone, same
        // technique as `host_verification_aggregate_cap.rs`.
        let deadline = Instant::now() + POLL_DEADLINE;
        loop {
            if Command::new("kill")
                .args(["-0", &build_pid.to_string()])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .map(|s| !s.success())
                .unwrap_or(true)
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "cancelling release.prepare must reap its owned build child (pid {build_pid})"
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }

        // No permit leak.
        poll_status_until(&mut client, "capacity fully drains, no leak", |s| {
            host_executing(s) == 0 && host_waiting(s) == 0
        })
        .await;

        // Truthful state: never falsely reported `prepared`.
        let listed = client
            .call("release.list", json!({"repo": repo_name}))
            .await
            .unwrap();
        let releases = listed["releases"].as_array().unwrap();
        assert_eq!(releases.len(), 1, "{listed:?}");
        assert_ne!(releases[0]["status"], json!("prepared"), "{listed:?}");
    }

    /// Enabled with the aggregate cap itself still disabled (`0`, the
    /// default) is a documented no-op: `HostVerificationAdmission::acquire`
    /// returns immediately, so the build never actually waits, matching
    /// every named check's own behavior under a disabled aggregate cap.
    #[tokio::test]
    async fn enabled_with_aggregate_cap_disabled_never_waits() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_fixture_repo(repo_dir.path(), "v1");
        let repo_name = repo_name_of(repo_dir.path());

        let layout = Layout::at(home.path());
        let mut daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
        daemon.set_release_build_admission_enabled(true);
        // Aggregate limit left at its default (0 = disabled).
        let _handle = tokio::spawn(daemon.run());
        let mut client = connect(&layout).await;
        register_repo(&mut client, repo_dir.path()).await;

        let result = tokio::time::timeout(
            Duration::from_secs(20),
            prepare(&mut client, &repo_name, "main"),
        )
        .await
        .expect("an enabled build must never wait when the aggregate cap itself is disabled")
        .unwrap();
        assert_eq!(result["release"]["status"], "prepared", "{result}");
        let host_admission = &result["release"]["manifest"]["recipe_bounds"]["host_admission"];
        assert_eq!(host_admission["weight"], json!(1), "{result}");
        assert!(
            host_admission["admission_wait_ms"].as_u64().unwrap() < 50,
            "a disabled aggregate cap must never make the build genuinely wait: {result}"
        );
    }
}

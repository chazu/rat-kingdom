//! P5.1 (`TKT-ratik-rivam-jadud`): `release.select` resolves an activated
//! repo's `release.integrationBranch` head and prepares (or idempotently
//! reuses) an immutable release for it — the first useful "select a frozen
//! release candidate while later integration continues" journey. Every
//! guarantee here comes from `release.prepare`'s existing content-addressed
//! identity (`crates/rk-daemon/tests/release_prepare.rs` covers that core
//! directly); these tests cover the new policy-driven candidate resolution
//! and the resulting immutability across a moving integration branch.

mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
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

/// Repo policy activating the P5.1 release role: `integration` is where
/// ordinary deliveries land, `main` stays the protected release target.
const RELEASE_ROLE_POLICY: &str = r#"
repo: {
    landing: {
        protectedTargets: ["main"]
    }
    release: {
        integrationBranch: "integration"
        releaseTarget: "main"
    }
}
"#;

fn write_release_role_policy(repo: &Path) {
    let rk_dir = repo.join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(rk_dir.join("repo.cue"), RELEASE_ROLE_POLICY).unwrap();
}

fn repo_name_of(repo: &Path) -> String {
    repo.file_name().unwrap().to_string_lossy().to_string()
}

async fn select(client: &mut Client, repo: &str) -> Result<Value, rk_core::Error> {
    client.call("release.select", json!({"repo": repo})).await
}

async fn show(client: &mut Client, id: &str) -> Value {
    client
        .call("release.show", json!({"id": id}))
        .await
        .unwrap()
}

/// Registration with no `.rk/repo.cue` at all leaves `activated_policy`
/// unset entirely — a different, pre-existing failure mode from the
/// half-configured-release-role case below. `release.select` must report the
/// SAME "no activated .rk/repo.cue policy" error every other operation
/// (e.g. `agent.spawn`) already reports for an inactive repo, not invent its
/// own message.
#[tokio::test]
async fn select_without_any_activated_policy_reports_the_existing_inactive_repo_error() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    write_fixture_source(repo_dir.path(), "v1");
    git(repo_dir.path(), &["init", "-q", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-qm", "fixture v1"]);
    let repo_name = repo_name_of(repo_dir.path());

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    client
        .call(
            "repo.add",
            json!({"name": repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    let err = select(&mut client, &repo_name).await.unwrap_err();
    assert!(
        err.to_string().contains("no activated .rk/repo.cue policy"),
        "{err}"
    );
}

/// A repo can activate a policy without ever configuring the release role
/// (both fields empty is a valid, activatable policy — see
/// `repository_policy_defaults_preserve_existing_behavior` in rk-workflow).
/// `release.select` must fail closed with an actionable message distinct
/// from "not activated at all", not silently fall back to some inferred
/// branch.
#[tokio::test]
async fn select_reports_release_role_not_configured_when_policy_is_active_but_unset() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    write_fixture_source(repo_dir.path(), "v1");
    git(repo_dir.path(), &["init", "-q", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::create_dir_all(repo_dir.path().join(".rk")).unwrap();
    std::fs::write(repo_dir.path().join(".rk/repo.cue"), "repo: {}\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-qm", "fixture v1"]);
    let repo_name = repo_name_of(repo_dir.path());

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    let added = client
        .call(
            "repo.add",
            json!({"name": repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();
    assert!(
        added["repo"]["activated_policy"]["digest"]
            .as_str()
            .is_some(),
        "an empty repo: {{}} policy must still activate: {added}"
    );

    let err = select(&mut client, &repo_name).await.unwrap_err();
    assert!(
        err.to_string().contains("no activated release role"),
        "{err}"
    );
}

/// The core journey: activate the release role, select resolves the
/// integration branch's CURRENT head (not `main`), and a second call against
/// an unchanged branch is idempotent — same release id, `already_prepared`.
#[tokio::test]
async fn select_resolves_the_integration_branch_head_and_is_idempotent() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    write_fixture_source(repo_dir.path(), "v1");
    git(repo_dir.path(), &["init", "-q", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    write_release_role_policy(repo_dir.path());
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-qm", "fixture v1"]);
    // The integration branch diverges from main immediately, so a correct
    // select must resolve ITS head, never main's.
    git(repo_dir.path(), &["checkout", "-qb", "integration"]);
    std::fs::write(repo_dir.path().join("integration-only.txt"), "x").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(
        repo_dir.path(),
        &["commit", "-qm", "integration-only change"],
    );
    let integration_head = git(repo_dir.path(), &["rev-parse", "HEAD"]);
    git(repo_dir.path(), &["checkout", "-q", "main"]);
    let repo_name = repo_name_of(repo_dir.path());

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    client
        .call(
            "repo.add",
            json!({"name": repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    let first = select(&mut client, &repo_name).await.unwrap();
    assert_eq!(first["release"]["status"], "prepared", "{first}");
    assert_eq!(first["already_prepared"], false, "{first}");
    assert_eq!(
        first["release"]["manifest"]["source"]["resolved_commit"], integration_head,
        "select must resolve the integration branch head, not main: {first}"
    );
    let first_id = first["release"]["id"].as_str().unwrap().to_string();

    let again = select(&mut client, &repo_name).await.unwrap();
    assert_eq!(again["already_prepared"], true, "{again}");
    assert_eq!(again["release"]["id"], first_id, "{again}");
}

/// Later integration continuing on the integration branch must never mutate
/// an already-selected candidate: a second `select` after a new integration
/// commit produces a SEPARATE, independently immutable release, and the
/// FIRST release's manifest/content verification is untouched.
#[tokio::test]
async fn later_integration_never_mutates_an_already_selected_candidate() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    write_fixture_source(repo_dir.path(), "v1");
    git(repo_dir.path(), &["init", "-q", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    write_release_role_policy(repo_dir.path());
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-qm", "fixture v1"]);
    git(repo_dir.path(), &["checkout", "-qb", "integration"]);
    let repo_name = repo_name_of(repo_dir.path());

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    client
        .call(
            "repo.add",
            json!({"name": repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    let first = select(&mut client, &repo_name).await.unwrap();
    let first_id = first["release"]["id"].as_str().unwrap().to_string();
    let first_commit = first["release"]["manifest"]["source"]["resolved_commit"]
        .as_str()
        .unwrap()
        .to_string();

    // Ordinary integration continues on the branch after the first candidate
    // was selected — this must not disturb the running/selected candidate.
    std::fs::write(repo_dir.path().join("more-work.txt"), "y").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-qm", "more integrated work"]);
    let second_commit = git(repo_dir.path(), &["rev-parse", "HEAD"]);
    assert_ne!(first_commit, second_commit);

    let second = select(&mut client, &repo_name).await.unwrap();
    let second_id = second["release"]["id"].as_str().unwrap().to_string();
    assert_ne!(
        second_id, first_id,
        "a later integration commit must select a separate, distinct release"
    );
    assert_eq!(
        second["release"]["manifest"]["source"]["resolved_commit"],
        second_commit
    );

    // The first candidate's manifest is untouched and still verifies exactly
    // as it did the moment it was selected.
    let first_shown = show(&mut client, &first_id).await;
    assert_eq!(
        first_shown["release"]["status"], "prepared",
        "{first_shown}"
    );
    assert_eq!(first_shown["content_verified"], true, "{first_shown}");
    assert_eq!(
        first_shown["release"]["manifest"]["source"]["resolved_commit"], first_commit,
        "the first selected candidate's frozen commit must never change: {first_shown}"
    );
}

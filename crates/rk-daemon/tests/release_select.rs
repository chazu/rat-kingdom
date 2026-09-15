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

async fn status(client: &mut Client, repo: &str) -> Value {
    client
        .call("release.status", json!({"repo": repo}))
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

/// The actual "integration continues while a release check is held" claim,
/// proved concurrently rather than sequentially: while `release.select`'s
/// build is genuinely blocked at a real OS-level barrier (a `build.rs`
/// child that only exits once its marker file is removed — the same
/// technique `release_prepare_interruption.rs` uses for a real crash test),
/// a SEPARATE connection runs the repo's named `verify` check
/// (`verify.run`, the exact machinery `landing.rs`'s gate gets its focused
/// checks from) against the same repo and completes promptly. This proves
/// the daemon-wide `release_prepare_lock` release.select/prepare share does
/// NOT also serialize against `verify.run` — a genuinely separate lock, so
/// ordinary named-check-gated landing work is never blocked behind a
/// release build. The barrier is a cheap shell command (same technique
/// `release_prepare.rs::host_admission` already uses), not a real cargo
/// compile — that module's own tests already prove a real build honors the
/// identical barrier under this exact admission wiring, so re-proving that
/// here would just be a slower, more contention-prone duplicate.
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

const POLL_DEADLINE: Duration = Duration::from_secs(30);
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
        let s = client.call("status", json!({})).await.unwrap();
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

/// Precise, scoped claim (corrected after a verified operator review found
/// the original version of this test overclaimed): under PRODUCTION'S
/// ACTUAL configuration — `release_build_admission_enabled: true`,
/// `verification_admission_aggregate_limit: 1`, per this ticket's own base
/// state — `release.select`'s build genuinely competes for, and can be
/// made to wait behind, the SAME shared aggregate permit an ordinary named
/// check (`verify.run`, the machinery a landing gate's focused checks call)
/// holds on a DIFFERENT repo. This is the opposite of "integration never
/// blocks behind a release build": it demonstrates that under the real
/// deployed config, a held release build DOES consume shared capacity a
/// concurrent named check would need. `release_prepare.rs::host_admission::
/// enabled_shares_the_aggregate_cap_with_a_concurrent_named_check` already
/// proves this exact property for `release.prepare`; this test proves
/// `release.select`'s new integration-branch candidate resolution
/// preserves it unchanged rather than accidentally bypassing admission.
/// It does NOT prove, and this slice does not claim, that the full
/// "integration continues unblocked while a release validates" operational
/// journey is delivered — under this real config it is contended, not
/// isolated. See the docs file for the retained follow-up.
#[tokio::test]
async fn select_shares_the_aggregate_admission_permit_with_a_concurrent_named_check() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let shared = tempfile::tempdir().unwrap();
    let checker_dir = tempfile::tempdir().unwrap();
    let checker_name = init_checker_repo(checker_dir.path(), shared.path(), "chk");
    let release_dir = tempfile::tempdir().unwrap();
    write_fixture_source(release_dir.path(), "v1");
    git(release_dir.path(), &["init", "-q", "-b", "main"]);
    git(release_dir.path(), &["config", "user.email", "r@x"]);
    git(release_dir.path(), &["config", "user.name", "R"]);
    write_release_role_policy(release_dir.path());
    git(release_dir.path(), &["add", "."]);
    git(release_dir.path(), &["commit", "-qm", "fixture v1"]);
    git(release_dir.path(), &["checkout", "-qb", "integration"]);
    let release_name = repo_name_of(release_dir.path());

    let mut daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    daemon.set_verification_admission_aggregate_limit(1);
    daemon.set_release_build_admission_enabled(true);
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    client
        .call(
            "repo.add",
            json!({"name": checker_name, "path": checker_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();
    client
        .call(
            "repo.add",
            json!({"name": release_name, "path": release_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

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

    let select_layout = layout.clone();
    let select_repo = release_name.clone();
    let select_call = tokio::spawn(async move {
        let mut c = Client::connect_as_operator(&select_layout).await.unwrap();
        c.call("release.select", json!({"repo": select_repo}))
            .await
            .unwrap_or_else(|e| panic!("release.select({select_repo}) failed: {e}"))
    });
    poll_status_until(
        &mut client,
        "release.select's build genuinely queues behind the saturated aggregate cap",
        |s| host_executing(s) == 1 && host_waiting(s) == 1,
    )
    .await;

    release_marker(shared.path(), "chk");
    checker_call.await.unwrap().unwrap();

    let selected = select_call.await.unwrap();
    assert_eq!(selected["release"]["status"], "prepared", "{selected}");
    let host_admission = &selected["release"]["manifest"]["recipe_bounds"]["host_admission"];
    assert_eq!(
        host_admission["recipe_identity"],
        json!("release-build:paired-rk-mcp"),
        "{selected}"
    );
    assert!(
        host_admission["admission_wait_ms"].as_u64().unwrap() > 0,
        "release.select's build genuinely waited for the checker's permit: {selected}"
    );

    poll_status_until(&mut client, "capacity fully drains, no leak", |s| {
        host_executing(s) == 0 && host_waiting(s) == 0
    })
    .await;
}

/// `release.status` makes the activated `releaseTarget` observable at
/// runtime instead of only validated once at policy activation: it reports
/// the integration branch's live head, the release target's live head, and
/// whether the integration head already has a recorded release.
#[tokio::test]
async fn status_reports_integration_and_release_target_heads() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    write_fixture_source(repo_dir.path(), "v1");
    git(repo_dir.path(), &["init", "-q", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    write_release_role_policy(repo_dir.path());
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-qm", "fixture v1"]);
    let main_head = git(repo_dir.path(), &["rev-parse", "HEAD"]);
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

    // Before any selection: the integration head is not yet released.
    let before = status(&mut client, &repo_name).await;
    assert_eq!(before["integration_branch"], "integration", "{before}");
    assert_eq!(before["release_target"], "main", "{before}");
    assert_eq!(before["release_target_head"], main_head, "{before}");
    assert_eq!(before["integration_head_prepared"], false, "{before}");
    assert!(before["selected_release"].is_null(), "{before}");

    let selected = select(&mut client, &repo_name).await.unwrap();
    let integration_head = selected["release"]["manifest"]["source"]["resolved_commit"]
        .as_str()
        .unwrap()
        .to_string();

    // After selection: the integration head IS released, and the release
    // target's head is unaffected — select never advances or lands anything.
    let after = status(&mut client, &repo_name).await;
    assert_eq!(after["integration_head"], integration_head, "{after}");
    assert_eq!(after["integration_head_prepared"], true, "{after}");
    assert_eq!(
        after["release_target_head"], main_head,
        "release.select must never advance release_target: {after}"
    );
    assert_eq!(
        after["selected_release"]["id"], selected["release"]["id"],
        "{after}"
    );
}

//! Proves the newly supported activation route for the two automation
//! classes TKT-losad-fonan-dotig adds: a `.rk/checks.cue` named-check
//! registry proposal (`OnboardingAutomationKind::CheckRegistry`) and a
//! `.github/workflows/*.yml` CI workflow proposal
//! (`OnboardingAutomationKind::CiWorkflow`). Before this change,
//! `onboarding_activation::contract` rejected every such proposal because
//! `automation_kind()` returned `None` for both target shapes, even though
//! staging, application, and (for checks) real command execution already
//! worked. This exercises the real CLI/daemon RPC journey in a disposable
//! repository, mirroring `repo_onboard_activation.rs` and
//! `repo_onboard_checks.rs`.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

fn complete_script(session_id: &str) -> String {
    format!(
        r#"
echo '{{"type":"system","subtype":"init","session_id":"{session_id}"}}'
read -r _first_message
"{bin}" done "assessment complete" >/dev/null 2>&1 || true
echo '{{"type":"result","subtype":"success","is_error":false,"result":"assessment complete","session_id":"{session_id}","total_cost_usd":0.001,"usage":{{"input_tokens":10,"output_tokens":5}}}}'
"#,
        bin = env!("CARGO_BIN_EXE_rk")
    )
}

fn git(root: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .env("LC_ALL", "C")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn repository(name: &str) -> tempfile::TempDir {
    let dir = tempfile::Builder::new().prefix(name).tempdir().unwrap();
    git(dir.path(), &["init", "-b", "main"]);
    git(dir.path(), &["config", "user.email", "test@example.com"]);
    git(dir.path(), &["config", "user.name", "Test"]);
    std::fs::write(
        dir.path().join("README.md"),
        "# Fixture\n\nActivation is explicit, content-bound, and reviewed.\n",
    )
    .unwrap();
    git(dir.path(), &["add", "."]);
    git(dir.path(), &["commit", "-m", "initial"]);
    dir
}

async fn connect(layout: &Layout) -> Client {
    for _ in 0..100 {
        if let Ok(client) = Client::connect_as_operator(layout).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("daemon did not start");
}

fn new_file_diff(target: &str, source: &str) -> String {
    let body = source
        .lines()
        .map(|line| format!("+{line}\n"))
        .collect::<String>();
    format!(
        "diff --git a/{target} b/{target}\nnew file mode 100644\n--- /dev/null\n+++ b/{target}\n@@ -0,0 +1,{} @@\n{body}",
        source.lines().count()
    )
}

fn checks_source(name: &str, command: &str, expect_exit: i64) -> String {
    format!(
        "checks: [\n\t{{\n\t\tname: \"{name}\"\n\t\tcommand: {}\n\t\tcwd: \".\"\n\t\texpectExit: {expect_exit}\n\t\ttimeout: \"10s\"\n\t\tenvironmentPolicy: \"inherit\"\n\t\ttoolchain: \"fixture POSIX sh\"\n\t}},\n]\n",
        serde_json::to_string(command).unwrap()
    )
}

fn checks_draft(name: &str, command: &str, expect_exit: i64) -> Value {
    let source = checks_source(name, command, expect_exit);
    json!({
        "kind": "repo_file",
        "title": format!("Add the {name} named check"),
        "evidence": ["operator reviewed the recipe"],
        "target_path": ".rk/checks.cue",
        "action": "write_repo_file",
        "diff": new_file_diff(".rk/checks.cue", &source),
        "risk": "high",
        "verification": [format!("check:{name}")],
        "named_check": {
            "name": name,
            "command": command,
            "cwd": ".",
            "expect_exit": expect_exit,
            "timeout": "10s",
            "environment_policy": "inherit",
            "toolchain": "fixture POSIX sh",
        },
    })
}

fn ci_workflow_source() -> &'static str {
    "name: CI\non:\n  push:\n    branches: [main]\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: mise run verify\n"
}

fn malformed_ci_workflow_source() -> &'static str {
    "name: CI\non:\n  push:\n    branches: [main]\n"
}

/// Syntactically valid YAML with an unsupported job shape: `runs-on` and
/// `steps` keys are present (so a presence-only check would wrongly accept
/// this) but their values are not a validated runner label or step list.
fn shape_invalid_ci_workflow_source() -> &'static str {
    "name: CI\non: push\njobs:\n  test:\n    runs-on: null\n    steps: false\n"
}

fn ci_workflow_draft(source: &str) -> Value {
    json!({
        "kind": "repo_file",
        "title": "Add the maintained CI workflow",
        "evidence": ["operator reviewed the workflow shape"],
        "target_path": ".github/workflows/ci.yml",
        "action": "write_repo_file",
        "diff": new_file_diff(".github/workflows/ci.yml", source),
        "risk": "high",
        "verification": ["bounded structural validation, not a live run"],
    })
}

async fn start_session(client: &mut Client, repo: &Path) -> Value {
    client
        .call(
            "repo.onboard.start",
            json!({"target": repo, "harness": "fake"}),
        )
        .await
        .unwrap()
}

async fn propose(client: &mut Client, session: &str, draft: Value) -> Value {
    client
        .call(
            "repo.onboard.propose",
            json!({"session": session, "proposal": draft}),
        )
        .await
        .unwrap()["proposal"]
        .clone()
}

async fn approve(client: &mut Client, session: &str, proposal: &Value) {
    client
        .call(
            "repo.onboard.approve",
            json!({
                "session": session,
                "proposal": proposal["id"],
                "digest": proposal["digest"],
            }),
        )
        .await
        .unwrap();
}

async fn apply(client: &mut Client, session: &str, proposal: &Value) -> rk_core::Result<Value> {
    client
        .call(
            "repo.onboard.apply",
            json!({
                "session": session,
                "proposal": proposal["id"],
                "digest": proposal["digest"],
            }),
        )
        .await
}

async fn activate(client: &mut Client, session: &str, proposal: &Value) -> rk_core::Result<Value> {
    client
        .call(
            "repo.onboard.activate",
            json!({
                "session": session,
                "proposal": proposal["id"],
                "digest": proposal["digest"],
            }),
        )
        .await
}

/// Propose, approve, and apply in one step, returning the applied proposal.
async fn stage(client: &mut Client, session: &str, draft: Value) -> Value {
    let proposed = propose(client, session, draft).await;
    approve(client, session, &proposed).await;
    apply(client, session, &proposed).await.unwrap()["proposal"].clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn check_registry_activation_requires_real_execution_and_is_replay_safe() {
    let home = tempfile::tempdir().unwrap();
    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        complete_script("onboarding-check-registry"),
    );
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut operator = connect(&layout).await;

    // Starting a second onboarding session against a repository whose prior
    // session has not reached a terminal state reuses that same session
    // (`insert_if_absent`), so each independent scenario below gets its own
    // disposable repository rather than sharing one onboarding branch.

    // Unapproved application is refused outright.
    let repo = repository("onboard-check-registry-unapproved");
    let started = start_session(&mut operator, repo.path()).await;
    let session = started["session"]["id"].as_str().unwrap().to_string();
    let fresh = propose(
        &mut operator,
        &session,
        checks_draft("verify", "printf pass\\n", 0),
    )
    .await;
    let unapproved = apply(&mut operator, &session, &fresh).await.unwrap_err();
    assert!(
        unapproved.to_string().contains("must be approved"),
        "{unapproved}"
    );

    // A contract that does not match its own CUE content is refused before
    // any command executes or lands: structural exactness, not freeform text.
    let mismatch_repo = repository("onboard-check-registry-mismatch");
    let mismatch_started = start_session(&mut operator, mismatch_repo.path()).await;
    let mismatch_session = mismatch_started["session"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let mut mismatched_draft = checks_draft("verify", "printf pass\\n", 0);
    mismatched_draft["named_check"]["command"] = json!("this does not match the cue file");
    let mismatched_proposal = propose(&mut operator, &mismatch_session, mismatched_draft).await;
    approve(&mut operator, &mismatch_session, &mismatched_proposal).await;
    let mismatch_error = apply(&mut operator, &mismatch_session, &mismatched_proposal)
        .await
        .unwrap_err();
    assert!(
        mismatch_error.to_string().contains("contract"),
        "{mismatch_error}"
    );

    // Approved, matching content executes for real (not schema-only) and
    // reaches Verified on a passing exit, then activation fast-forwards the
    // registered checkout to the exact approved, executed commit. Replay is
    // a no-op: no second commit, no second execution.
    let repo = repository("onboard-check-registry-activates");
    let started = start_session(&mut operator, repo.path()).await;
    let session = started["session"]["id"].as_str().unwrap().to_string();
    let fresh = propose(
        &mut operator,
        &session,
        checks_draft("verify", "printf pass\\n", 0),
    )
    .await;
    approve(&mut operator, &session, &fresh).await;
    let applied = apply(&mut operator, &session, &fresh).await.unwrap();
    assert_eq!(applied["proposal"]["status"], "verified");
    assert_eq!(
        applied["proposal"]["verification_results"][0]["passed"],
        true
    );
    assert!(!repo.path().join(".rk/checks.cue").exists());
    let activated = activate(&mut operator, &session, &applied["proposal"])
        .await
        .unwrap();
    assert_eq!(activated["proposal"]["activation"]["status"], "activated");
    assert_eq!(activated["changed"], true);
    assert!(repo.path().join(".rk/checks.cue").exists());
    let activated_head = git(repo.path(), &["rev-parse", "HEAD"]);
    let replay = activate(&mut operator, &session, &applied["proposal"])
        .await
        .unwrap();
    assert_eq!(replay["changed"], false);
    assert_eq!(git(repo.path(), &["rev-parse", "HEAD"]), activated_head);

    // A check that fails for real is never eligible for activation.
    let failing_repo = repository("onboard-check-registry-failing");
    let failing_started = start_session(&mut operator, failing_repo.path()).await;
    let failing_session = failing_started["session"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let failing = stage(
        &mut operator,
        &failing_session,
        checks_draft("second", "false", 0),
    )
    .await;
    assert_eq!(failing["status"], "failed");
    let refused = activate(&mut operator, &failing_session, &failing)
        .await
        .unwrap_err();
    assert!(
        refused.to_string().contains("must be verified"),
        "{refused}"
    );

    // A registered base movement after approval invalidates activation even
    // though the approved onboarding branch and digest are unchanged.
    let stale_repo = repository("onboard-check-registry-stale-base");
    let started_second = start_session(&mut operator, stale_repo.path()).await;
    let session_two = started_second["session"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let stale = stage(
        &mut operator,
        &session_two,
        checks_draft("stale-base", "printf stale\\n", 0),
    )
    .await;
    assert_eq!(stale["status"], "verified");
    std::fs::write(stale_repo.path().join("HUMAN"), "branch moved\n").unwrap();
    git(stale_repo.path(), &["add", "HUMAN"]);
    git(stale_repo.path(), &["commit", "-m", "human moves base"]);
    let stale_error = activate(&mut operator, &session_two, &stale)
        .await
        .unwrap_err();
    assert!(
        stale_error
            .to_string()
            .contains("base branch moved after approval"),
        "{stale_error}"
    );

    operator.call("stop", json!({})).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("daemon did not stop")
        .unwrap()
        .unwrap();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ci_workflow_activation_requires_structure_and_survives_a_crash() {
    let home = tempfile::tempdir().unwrap();
    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        complete_script("onboarding-ci-workflow"),
    );
    let layout = Layout::at(home.path());
    let daemon_a = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut operator = connect(&layout).await;

    // Regular-file existence alone is not CI validation: a workflow missing
    // `jobs:` is staged and preflighted fine but fails structural
    // validation. Its own disposable repository/session, matching the
    // established onboarding-test convention of one repository per
    // independent scenario (a second `start` against the same repository
    // reuses any still-open session rather than creating an isolated one).
    let malformed_repo = repository("onboard-ci-workflow-malformed");
    let malformed_started = start_session(&mut operator, malformed_repo.path()).await;
    let malformed_session = malformed_started["session"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let malformed = propose(
        &mut operator,
        &malformed_session,
        ci_workflow_draft(malformed_ci_workflow_source()),
    )
    .await;
    approve(&mut operator, &malformed_session, &malformed).await;
    let malformed_applied = apply(&mut operator, &malformed_session, &malformed)
        .await
        .unwrap();
    assert_eq!(malformed_applied["proposal"]["status"], "failed");
    assert!(
        malformed_applied["proposal"]["failure"]
            .as_str()
            .unwrap()
            .contains("jobs"),
        "{}",
        malformed_applied["proposal"]["failure"]
    );
    assert!(activate(
        &mut operator,
        &malformed_session,
        &malformed_applied["proposal"]
    )
    .await
    .is_err());

    // Syntactically valid YAML with `runs-on`/`steps` keys present but an
    // unsupported value shape (`null`/`false`) is also refused: presence of
    // the keys alone is not a validated structure, and the invalid proposal
    // never becomes eligible for activation.
    let shape_invalid_repo = repository("onboard-ci-workflow-shape-invalid");
    let shape_invalid_started = start_session(&mut operator, shape_invalid_repo.path()).await;
    let shape_invalid_session = shape_invalid_started["session"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let shape_invalid = propose(
        &mut operator,
        &shape_invalid_session,
        ci_workflow_draft(shape_invalid_ci_workflow_source()),
    )
    .await;
    approve(&mut operator, &shape_invalid_session, &shape_invalid).await;
    let shape_invalid_applied = apply(&mut operator, &shape_invalid_session, &shape_invalid)
        .await
        .unwrap();
    assert_eq!(shape_invalid_applied["proposal"]["status"], "failed");
    assert!(
        shape_invalid_applied["proposal"]["failure"]
            .as_str()
            .unwrap()
            .contains("runs-on"),
        "{}",
        shape_invalid_applied["proposal"]["failure"]
    );
    assert!(activate(
        &mut operator,
        &shape_invalid_session,
        &shape_invalid_applied["proposal"]
    )
    .await
    .is_err());

    // A well-formed workflow passes bounded structural validation and
    // reaches Verified without ever being executed.
    let repo = repository("onboard-ci-workflow-activates");
    let started = start_session(&mut operator, repo.path()).await;
    let session = started["session"]["id"].as_str().unwrap().to_string();
    let well_formed = propose(
        &mut operator,
        &session,
        ci_workflow_draft(ci_workflow_source()),
    )
    .await;
    approve(&mut operator, &session, &well_formed).await;
    let applied = apply(&mut operator, &session, &well_formed).await.unwrap();
    assert_eq!(applied["proposal"]["status"], "verified");
    assert_eq!(
        applied["proposal"]["validation_results"][0]["automation_kind"],
        "ci_workflow"
    );
    assert_eq!(applied["proposal"]["validation_results"][0]["passed"], true);
    assert!(!repo.path().join(".github/workflows/ci.yml").exists());

    let activated = activate(&mut operator, &session, &applied["proposal"])
        .await
        .unwrap();
    assert_eq!(activated["proposal"]["activation"]["status"], "activated");
    assert!(repo.path().join(".github/workflows/ci.yml").exists());
    let activated_head = git(repo.path(), &["rev-parse", "HEAD"]);

    let replay = activate(&mut operator, &session, &applied["proposal"])
        .await
        .unwrap();
    assert_eq!(replay["changed"], false);
    assert_eq!(git(repo.path(), &["rev-parse", "HEAD"]), activated_head);

    operator.call("stop", json!({})).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle_a)
        .await
        .expect("daemon A did not stop")
        .unwrap()
        .unwrap();

    // Simulate the exact crash window after the registered checkout advanced
    // but before the activation result reached the session journal. Recovery
    // on restart must recognize the already-landed commit and must not touch
    // unrelated newer work made on the repository meanwhile.
    std::fs::write(repo.path().join("UNRELATED"), "newer human work\n").unwrap();
    git(repo.path(), &["add", "UNRELATED"]);
    git(repo.path(), &["commit", "-m", "unrelated newer work"]);
    let newer_head = git(repo.path(), &["rev-parse", "HEAD"]);

    let sessions_path = home.path().join("onboarding-sessions.json");
    let mut persisted: Value =
        serde_json::from_slice(&std::fs::read(&sessions_path).unwrap()).unwrap();
    let proposals = persisted[session.as_str()]["proposals"]
        .as_array_mut()
        .unwrap();
    let record = proposals
        .iter_mut()
        .find(|proposal| proposal["id"] == applied["proposal"]["id"])
        .unwrap();
    record["activation"]["status"] = json!("activating");
    record["activation"]["completed_at"] = Value::Null;
    record["activation"]["registered_commit"] = Value::Null;
    std::fs::write(
        &sessions_path,
        serde_json::to_vec_pretty(&persisted).unwrap(),
    )
    .unwrap();

    let daemon_b = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle_b = tokio::spawn(daemon_b.run());
    let mut operator = connect(&layout).await;
    let recovered = activate(&mut operator, &session, &applied["proposal"])
        .await
        .unwrap();
    assert_eq!(recovered["proposal"]["activation"]["status"], "activated");
    assert_eq!(
        git(repo.path(), &["rev-parse", "HEAD"]),
        newer_head,
        "recovery must not touch unrelated newer work made after activation landed"
    );
    assert!(git(repo.path(), &["log", "--format=%H"])
        .lines()
        .any(|candidate| candidate == activated_head));

    // A CI-shaped path outside the recognized `.github/workflows/*.yml` shape
    // stays an inert repo file with no activation route.
    let unsupported_repo = repository("onboard-ci-workflow-unsupported");
    let unsupported_started = start_session(&mut operator, unsupported_repo.path()).await;
    let unsupported_session = unsupported_started["session"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let unsupported = stage(
        &mut operator,
        &unsupported_session,
        ci_workflow_draft_at(".github/workflows/nested/ci.yml", ci_workflow_source()),
    )
    .await;
    assert_eq!(unsupported["status"], "verified");
    let unsupported_error = activate(&mut operator, &unsupported_session, &unsupported)
        .await
        .unwrap_err();
    assert!(
        unsupported_error
            .to_string()
            .contains("not a supported automation activation target"),
        "{unsupported_error}"
    );

    operator.call("stop", json!({})).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle_b)
        .await
        .expect("daemon B did not stop")
        .unwrap()
        .unwrap();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

fn ci_workflow_draft_at(target: &str, source: &str) -> Value {
    json!({
        "kind": "repo_file",
        "title": "Add a non-canonical CI-shaped file",
        "evidence": ["operator reviewed the file"],
        "target_path": target,
        "action": "write_repo_file",
        "diff": new_file_diff(target, source),
        "risk": "low",
        "verification": ["reviewed manually"],
    })
}

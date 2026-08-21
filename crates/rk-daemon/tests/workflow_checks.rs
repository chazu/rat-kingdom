//! TKT-30 end to end: a workflow `run` step resolves a repo-registered NAMED
//! check (`<repo>/.rk/checks.cue`) instead of a raw command, and the
//! `require_named_checks` policy refuses a raw inline command fail-closed so a
//! compromised/untrusted workflow definition cannot execute arbitrary shell.

mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

// These tests configure the fake harness through a process-global environment
// variable. Keep the variable stable until the daemon and its child harness
// have finished; otherwise a sibling test can remove it between workflow.run
// and the spawn step, silently selecting the default no-op fake.
static HARNESS_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn git(dir: &Path, args: &[&str]) {
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
}

const WORKING_FAKE: &str = r#"
read -r _prompt
echo "work for $RK_TASK by $RK_AGENT" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"wf-fake"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"did the work","session_id":"wf-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

/// The named check `worktree-has-work` asserts the rat's committed work file
/// exists in the worktree and carries its own inline `expectExit: 0` gate.
const CHECKS: &str = r#"
checks: [
    {name: "steward-protected-paths", command: "true", timeout: "30s"},
    {name: "steward-diff-scope", command: "true", timeout: "30s"},
    {name: "verify", command: "true", timeout: "30s"},
    {name: "worktree-has-work", command: "test -f work-{{ctx.activeAgent}}.txt", expectExit: 0, timeout: "30s"},
    {name: "check-inputs-arrive", command: "test \"$RK_CHECK_TASK\" = env-1 && test -n \"$RK_CHECK_AGENT\"", expectExit: 0, timeout: "30s"},
]
"#;

// spawn → wait → run (by NAME, not raw command) → dismiss. The check's own
// expectExit gate fails closed on a red result; here it is green so it merges.
const CHECK_WORKFLOW: &str = r#"
workflow: {
    name: "named-check"
    params: {taskId: {type: "string", required: true}}
    agents: {default: {harness: "fake", model: "sonnet"}}
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId, description: "do " + _input.taskId}},
        {type: "wait", timeout: "30s"},
        {type: "run", check: "worktree-has-work"},
        {type: "dismiss"},
    ]
}
"#;

// spawn → wait → run (RAW command) → dismiss. Refused fail-closed under policy.
const RAW_WORKFLOW: &str = r#"
workflow: {
    name: "raw-run"
    params: {taskId: {type: "string", required: true}}
    agents: {default: {harness: "fake", model: "sonnet"}}
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId, description: "do " + _input.taskId}},
        {type: "wait", timeout: "30s"},
        {type: "run", command: "echo pwned", expectExit: 0, timeout: "30s"},
        {type: "dismiss"},
    ]
}
"#;

const CHECK_ENV_WORKFLOW: &str = r#"
workflow: {
    name: "named-check-env"
    params: {taskId: {type: "string", required: true}}
    agents: {default: {harness: "fake", model: "sonnet"}}
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId, description: "do " + _input.taskId}},
        {type: "wait", timeout: "30s"},
        {
            type: "run"
            check: "check-inputs-arrive"
            env: {
                RK_CHECK_TASK:  _input.taskId
                RK_CHECK_AGENT: "{{ctx.activeAgent}}"
            }
        },
        {type: "dismiss"},
    ]
}
"#;

fn init_repo(repo: &Path) {
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "r@x"]);
    git(repo, &["config", "user.name", "R"]);
    std::fs::write(repo.join("README.md"), "# x\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "init"]);
    support::install_passing_landing_checks(repo);
}

fn write_def(repo: &Path, name: &str, src: &str) {
    let wf_dir = repo.join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join(format!("{name}.cue")), src).unwrap();
}

fn write_checks(repo: &Path, src: &str) {
    let rk_dir = repo.join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(rk_dir.join("checks.cue"), src).unwrap();
}

async fn await_status(client: &mut Client, id: &str, want: &str) -> serde_json::Value {
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        match status["instance"]["status"].as_str().unwrap_or("") {
            s if s == want => return status,
            "failed" if want != "failed" => {
                panic!(
                    "workflow failed unexpectedly: {}",
                    status["instance"]["error"]
                )
            }
            "completed" if want != "completed" => panic!("workflow completed unexpectedly"),
            _ => {}
        }
    }
    panic!("workflow never reached status {want}");
}

fn main_listing(repo: &Path) -> String {
    let files = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["ls-tree", "--name-only", "main"])
        .output()
        .unwrap();
    String::from_utf8_lossy(&files.stdout).to_string()
}

/// A `run` step referencing a repo-registered named check resolves its command
/// from `<repo>/.rk/checks.cue`, runs it in the worktree, and — even with the
/// require_named_checks policy ON — the check runs and its green inline gate
/// lets the branch merge. This is the sanctioned path.
#[tokio::test]
async fn named_check_resolves_and_merges_under_policy() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    write_def(repo_dir.path(), "named-check", CHECK_WORKFLOW);
    write_checks(repo_dir.path(), CHECKS);

    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);
    let layout = Layout::at(home.path());
    let mut daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    // Policy ON: raw commands are refused, but named checks still run.
    daemon.set_require_named_checks(true);
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "named-check",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"taskId": "named-1"},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    await_status(&mut client, &id, "completed").await;
    let listing = main_listing(repo_dir.path());
    assert!(
        listing.contains("work-"),
        "named-check green work must merge: {listing}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// Workflow data may parameterize a fixed repository-owned command only via
/// the RK_CHECK_* namespace. Values are interpolated at execution time while
/// the command remains the exact text declared by the repository.
#[tokio::test]
async fn named_check_receives_namespaced_data_inputs_under_policy() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    write_def(repo_dir.path(), "named-check-env", CHECK_ENV_WORKFLOW);
    write_checks(repo_dir.path(), CHECKS);

    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);
    let layout = Layout::at(home.path());
    let mut daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    daemon.set_require_named_checks(true);
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "named-check-env",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"taskId": "env-1"},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    await_status(&mut client, &id, "completed").await;
    assert!(main_listing(repo_dir.path()).contains("work-"));

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// With require_named_checks ON, a `run` step carrying a RAW `command` is
/// refused fail-closed: the instance fails, the dismiss never runs, and the
/// rat's work never reaches main. A compromised workflow def cannot run
/// arbitrary shell.
#[tokio::test]
async fn raw_command_refused_under_policy_fails_closed() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    write_def(repo_dir.path(), "raw-run", RAW_WORKFLOW);

    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);
    let layout = Layout::at(home.path());
    let mut daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    daemon.set_require_named_checks(true);
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "raw-run",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"taskId": "raw-1"},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    let status = await_status(&mut client, &id, "failed").await;
    let err = status["instance"]["error"].as_str().unwrap_or("");
    assert!(
        err.contains("require_named_checks") || err.contains("refused by policy"),
        "expected a policy refusal, got: {err}"
    );

    let listing = main_listing(repo_dir.path());
    assert!(
        !listing.contains("work-"),
        "policy-refused work must not merge: {listing}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// With the policy OFF (default), the same raw-command workflow runs normally —
/// the policy is opt-in and backward compatible.
#[tokio::test]
async fn raw_command_runs_when_policy_off() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    write_def(repo_dir.path(), "raw-run", RAW_WORKFLOW);

    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    // Policy OFF by default.
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "raw-run",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"taskId": "raw-2"},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    await_status(&mut client, &id, "completed").await;
    let listing = main_listing(repo_dir.path());
    assert!(
        listing.contains("work-"),
        "raw work must merge when policy off: {listing}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// Regression: a `strip_rk_spawn` check must remove the exact-review binding
/// (`RK_REVIEW_*`, see `rk_core::review`) in addition to supervised spawn
/// identity. Before this fix, a nested check that itself exercised
/// reviewer-role, env-driven logic (a repo's own test suite spinning up a
/// synthetic reviewer that writes an artifact for its own synthetic task)
/// inherited an outer reviewer's real `RK_REVIEW_TASK`/etc, which then
/// mismatched its own synthetic identity and got rejected — exactly what
/// happened live when steward reviewer Pumpernickel-10 ran the repository's
/// `verify` check for Widget-10 (TKT-01M0GDHKYSKEGVZR7QY9FP1VKK).
const REVIEW_BINDING_CHECKS: &str = r#"
checks: [
    {name: "steward-protected-paths", command: "true", timeout: "30s"},
    {name: "steward-diff-scope", command: "true", timeout: "30s"},
    {name: "verify", command: "true", timeout: "30s"},
    {name: "leaks-without-strip", command: "test \"$RK_REVIEW_TASK\" = outer-task", expectExit: 0, timeout: "30s"},
    {
        name: "stripped-clears-review-binding"
        command: "env | grep -E '^(RK_AGENT|RK_TASK|RK_REPO|RK_ROLE|RK_HOME|RK_BRANCH|RK_WORKTREE|RK_AUTH_TOKEN|RK_REVIEW_BRANCH|RK_REVIEW_HEAD|RK_REVIEW_TARGET|RK_REVIEW_TASK|RK_REVIEW_ATTEMPT)=' && exit 1 || exit 0"
        expectExit: 0
        timeout: "30s"
        environmentPolicy: "strip_rk_spawn"
    },
]
"#;

const REVIEW_BINDING_WORKFLOW: &str = r#"
workflow: {
    name: "review-binding-strip"
    params: {taskId: {type: "string", required: true}}
    agents: {default: {harness: "fake", model: "sonnet"}}
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId, description: "do " + _input.taskId}},
        {type: "wait", timeout: "30s"},
        {type: "run", check: "leaks-without-strip"},
        {type: "run", check: "stripped-clears-review-binding"},
        {type: "dismiss"},
    ]
}
"#;

/// The unstripped control check proves the ambient `RK_REVIEW_*` binding
/// really was present (simulating a check invoked from within an outer
/// reviewer's exact-review environment, as `.rk/checks.cue`'s own `verify`
/// entry declares `environmentPolicy: "strip_rk_spawn"` for exactly this
/// reason). The stripped check proves the isolated child never sees it —
/// neither the supervised spawn identity nor the review binding.
#[tokio::test]
async fn strip_rk_spawn_removes_review_binding_but_inherit_still_sees_it() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    write_def(
        repo_dir.path(),
        "review-binding-strip",
        REVIEW_BINDING_WORKFLOW,
    );
    write_checks(repo_dir.path(), REVIEW_BINDING_CHECKS);

    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);
    // Simulate the daemon's own process carrying an outer reviewer's
    // exact-review binding — as it does when a steward reviewer runs the
    // repository's declared `verify` check from within its own worktree.
    std::env::set_var("RK_REVIEW_BRANCH", "rat/outer-reviewer/tkt-outer");
    std::env::set_var("RK_REVIEW_HEAD", "deadbeef");
    std::env::set_var("RK_REVIEW_TARGET", "main");
    std::env::set_var("RK_REVIEW_TASK", "outer-task");
    std::env::set_var("RK_REVIEW_ATTEMPT", "landing-review-1");

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "review-binding-strip",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"taskId": "review-binding-1"},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    await_status(&mut client, &id, "completed").await;
    let listing = main_listing(repo_dir.path());
    assert!(
        listing.contains("work-"),
        "both checks must pass for the branch to merge: {listing}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
    std::env::remove_var("RK_REVIEW_BRANCH");
    std::env::remove_var("RK_REVIEW_HEAD");
    std::env::remove_var("RK_REVIEW_TARGET");
    std::env::remove_var("RK_REVIEW_TASK");
    std::env::remove_var("RK_REVIEW_ATTEMPT");
}

/// A `run` step referencing a check name that is not in the registry fails
/// closed — a typo or a stale reference never silently runs nothing.
#[tokio::test]
async fn unknown_check_fails_closed() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    // Workflow references "worktree-has-work" but the registry is empty.
    write_def(repo_dir.path(), "named-check", CHECK_WORKFLOW);
    write_checks(repo_dir.path(), "checks: []\n");

    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "named-check",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"taskId": "named-2"},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    let status = await_status(&mut client, &id, "failed").await;
    let err = status["instance"]["error"].as_str().unwrap_or("");
    assert!(
        err.contains("no check named 'worktree-has-work'"),
        "expected an unknown-check error, got: {err}"
    );

    let listing = main_listing(repo_dir.path());
    assert!(
        !listing.contains("work-"),
        "unknown-check work must not merge: {listing}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

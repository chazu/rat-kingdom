//! Regression (TKT-01M0GDHKYSKEGVZR7QY9FP1VKK): a `strip_rk_spawn` check must
//! clear the exact-review binding (`RK_REVIEW_*`), proven through the REAL
//! `rk out artifact <repo> review` path — the path that actually broke — rather
//! than by grepping the child environment.
//!
//! What broke live. A rat's `rk` client auto-starts the daemon, so a daemon
//! first reached from inside a reviewer's worktree inherits that reviewer's
//! `RK_REVIEW_*` binding, and every check child it later spawns inherits it in
//! turn. rat-kingdom's own `verify` check runs a test suite that spins up a
//! synthetic reviewer writing a verdict artifact for its own synthetic task
//! (`crates/rk-cli/tests/reviewer_drives_rework.rs`), and `rk out artifact …
//! review` binds that payload against ambient `RK_REVIEW_*` before it ever
//! reaches the socket (`rk_cli::space_cmds::bind_review_payload`). The outer
//! reviewer's real `RK_REVIEW_TASK` then mismatches the synthetic task, the
//! write is rejected, no verdict tuple lands, and the nested suite's `read`
//! step burns its whole ceiling waiting for one — which is how a steward
//! reviewer turned a green branch red.
//!
//! Division of labour with `crates/rk-daemon/tests/workflow_checks.rs`: that
//! test pins the environment SURFACE (exactly which names a stripped child does
//! and does not carry). This one pins the CONSEQUENCE — under one ambient
//! binding, the same real artifact write is rejected from an `inherit` check
//! and accepted from a `strip_rk_spawn` one, the workflow completes, and the
//! tuple is really in the space afterwards.
//!
//! The nested daemon at its own `RK_HOME` is not scaffolding for its own sake:
//! it is what the live nested suite does. `cargo test` builds a tempdir home
//! and talks to a daemon of its own, whose agent registry knows nothing of the
//! outer reviewer's worktree — so the nested write authenticates as operator,
//! and the ONLY thing that can reject it is the review binding under test.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

/// Scope and task the nested "synthetic reviewer" writes its verdict for. The
/// task deliberately differs from `OUTER_REVIEW_TASK` below: that difference is
/// the mismatch the leaked binding rejects on.
const NESTED_SCOPE: &str = "nested-repo";
const NESTED_TASK: &str = "nested-task";
const OUTER_REVIEW_TASK: &str = "TKT-OUTER-REVIEW";

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

/// Connect as the operator explicitly (TKT-182): this test drives
/// `workflow.run`, which is operator-only, and a rat's spawn env sets
/// `RK_AGENT`, which test processes inherit.
async fn connect(layout: &Layout) -> Client {
    for _ in 0..1500 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up at {layout:?}");
}

const WORKING_FAKE: &str = r#"
read -r _prompt
echo "work for $RK_TASK by $RK_AGENT" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"wf-fake"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"did the work","session_id":"wf-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

/// Both checks run the SAME real `rk out artifact … review` command against the
/// SAME nested `RK_HOME`; the only difference between them is
/// `environmentPolicy`. The control also drops `RK_AGENT`/`RK_AUTH_TOKEN`,
/// which the `inherit` branch of `spawn_check_child` injects so a check can
/// shell back into `rk` as the agent whose worktree it runs in — without that,
/// the control would be refused by the nested daemon for an unrelated reason
/// (an agent caller it has never registered) and would prove nothing about the
/// review binding. `--field` builds the payload without JSON quoting, exactly
/// as a shell-side check is meant to.
fn checks(rk_bin: &str, nested_home: &str) -> String {
    format!(
        r#"
checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "true", timeout: "30s"}},
    {{
        name: "leaked-binding-rejects-review-artifact"
        command: "env -u RK_AGENT -u RK_AUTH_TOKEN RK_HOME='{nested_home}' '{rk_bin}' out artifact {NESTED_SCOPE} review --field task={NESTED_TASK} --field recommendation=APPROVE 2>&1 | grep -q 'review artifact binding mismatch for task'"
        expectExit: 0
        timeout: "120s"
    }},
    {{
        name: "stripped-binding-accepts-review-artifact"
        command: "env RK_HOME='{nested_home}' '{rk_bin}' out artifact {NESTED_SCOPE} review --field task={NESTED_TASK} --field recommendation=APPROVE"
        expectExit: 0
        timeout: "120s"
        environmentPolicy: "strip_rk_spawn"
    }},
]
"#
    )
}

const WORKFLOW: &str = r#"
workflow: {
    name: "review-binding-artifact"
    params: {taskId: {type: "string", required: true}}
    agents: {default: {harness: "fake", model: "sonnet"}}
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId, description: "do " + _input.taskId}},
        {type: "wait", timeout: "60s"},
        {type: "run", check: "leaked-binding-rejects-review-artifact"},
        {type: "run", check: "stripped-binding-accepts-review-artifact"},
        {type: "dismiss"},
    ]
}
"#;

fn init_repo(repo: &Path) {
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "r@x"]);
    git(repo, &["config", "user.name", "R"]);
    std::fs::write(repo.join("README.md"), "# x\n").unwrap();
    let rk_dir = repo.join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    // The stack-neutral landing checks must be COMMITTED: the dismiss-time
    // merge resolves them from the repository's own tree, not from the
    // uncommitted registry the test overwrites below.
    std::fs::write(
        rk_dir.join("checks.cue"),
        r#"checks: [
    {name: "steward-protected-paths", command: "true", timeout: "30s"},
    {name: "steward-diff-scope", command: "true", timeout: "30s"},
    {name: "verify", command: "true", timeout: "30s"},
]
"#,
    )
    .unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "init"]);
}

/// Every `review` artifact in the nested space.
async fn nested_review_artifacts(client: &mut Client) -> Vec<Value> {
    let scanned = client
        .call("space.scan", json!({"category": "artifact"}))
        .await
        .unwrap();
    scanned["tuples"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|t| t["identity"] == "review")
        .collect()
}

#[tokio::test]
async fn strip_rk_spawn_lets_a_nested_reviewer_write_its_own_verdict_artifact() {
    let home = tempfile::tempdir().unwrap();
    let nested_home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("review-binding-artifact.cue"), WORKFLOW).unwrap();
    std::fs::write(
        repo_dir.path().join(".rk").join("checks.cue"),
        checks(
            env!("CARGO_BIN_EXE_rk"),
            &nested_home.path().to_string_lossy(),
        ),
    )
    .unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);
    // The outer reviewer's exact-review binding, carried by this process and so
    // by every daemon and check child started from it — the leak the fix has to
    // contain. Set BEFORE either daemon starts, exactly as a daemon
    // auto-started from inside a reviewer's worktree inherits it.
    std::env::set_var("RK_REVIEW_BRANCH", "rat/outer-reviewer/tkt-outer");
    std::env::set_var("RK_REVIEW_HEAD", "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
    std::env::set_var("RK_REVIEW_TARGET", "main");
    std::env::set_var("RK_REVIEW_TASK", OUTER_REVIEW_TASK);
    std::env::set_var("RK_REVIEW_ATTEMPT", "landing-review-1");

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // The "repo's own test suite" daemon: its own home, its own empty agent
    // registry. Both checks write here.
    let nested_layout = Layout::at(nested_home.path());
    let nested_daemon =
        Daemon::new_in_memory(nested_layout.clone(), "nested-castle".into()).unwrap();
    let _nested_handle = tokio::spawn(nested_daemon.run());
    let mut nested_client = connect(&nested_layout).await;
    assert!(
        nested_review_artifacts(&mut nested_client).await.is_empty(),
        "the nested space must start with no verdict"
    );

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "review-binding-artifact",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"taskId": "review-binding-artifact-1"},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    // Both `run` steps carry a 120s ceiling of their own and each spawns a real
    // `rk` process; five minutes leaves headroom above that under
    // workspace-wide `cargo test` contention without ever masking a step that
    // failed at its own ceiling (which flips the instance to `failed` and is
    // caught immediately below).
    let mut completed = false;
    for _ in 0..3000 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        match status["instance"]["status"].as_str().unwrap_or("") {
            "completed" => {
                completed = true;
                break;
            }
            // A failure here is the regression itself: either the control check
            // found no rejection (the binding did not leak, so the test proves
            // nothing) or the stripped check's write was rejected (the binding
            // was NOT stripped).
            "failed" => panic!("workflow failed: {}", status["instance"]["error"]),
            _ => {}
        }
    }
    assert!(completed, "workflow never completed");

    // The stripped write really landed — the check exiting 0 could otherwise be
    // a command that did nothing.
    let verdicts = nested_review_artifacts(&mut nested_client).await;
    assert_eq!(
        verdicts.len(),
        1,
        "exactly one verdict should exist: the stripped write. The control's \
         write must have been rejected, not merely mis-bound: {verdicts:?}"
    );
    let payload = &verdicts[0]["payload"];
    assert_eq!(
        payload["task"].as_str(),
        Some(NESTED_TASK),
        "the verdict must carry the NESTED reviewer's own task: {payload}"
    );
    // With the binding stripped, `review_context_from_env` finds nothing and so
    // stamps nothing. A `branch`/`head_sha` here would mean the child still saw
    // the outer reviewer's `RK_REVIEW_*` and merely happened not to conflict.
    assert!(
        payload.get("branch").is_none() && payload.get("head_sha").is_none(),
        "a stripped child must not bind the outer review context at all: {payload}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
    std::env::remove_var("RK_REVIEW_BRANCH");
    std::env::remove_var("RK_REVIEW_HEAD");
    std::env::remove_var("RK_REVIEW_TARGET");
    std::env::remove_var("RK_REVIEW_TASK");
    std::env::remove_var("RK_REVIEW_ATTEMPT");
}

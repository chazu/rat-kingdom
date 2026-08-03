//! An ungated `land` must fail closed before it can create a misleadingly
//! successful workflow result or a branch-lifecycle side effect.

mod fixture;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

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
    String::from_utf8_lossy(&out.stdout).to_string()
}

async fn connect(layout: &Layout) -> Client {
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

/// The rat rewrites the one file main also has, so its branch and main touch
/// the same line — the land below cannot fast-forward or auto-merge.
const CONFLICTING_FAKE: &str = r#"
read -r _prompt
echo '# rat version' > README.md
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"drop-fake"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"drop-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

/// A workflow that lands WITHOUT a human approval gate. The `run` step moves
/// main onto the same line the rat changed, but the safety precondition should
/// reject `land` before Git is asked to merge it.
const UNGATED_LAND: &str = r#"
workflow: {
	name:        "ungated-land"
	description: "land a conflicting branch with no evaluate on the result"
	params: {taskId: {type: "string", required: false, default: "t"}}
	agents: {default: {harness: "fake"}}
	steps: [
		{type: "spawn", role: "rat", task: {title: "conflicting-work", description: "edit README"}},
		{type: "wait", timeout: "60s"},
		{type: "evaluate", expect: {is_error: false}},
		// Advance main onto the same line, from the rat's worktree: the repo
		// root is the parent of the common git dir, and it is checked out on
		// main.
		{
			type:    "run"
			command: "root=$(cd $(git rev-parse --git-common-dir)/.. && pwd); echo '# human version' > $root/README.md; git -C $root add README.md; git -C $root commit -q -m 'human edit'"
			timeout: "60s"
		},
		{type: "evaluate", expect: {exit: 0}},
		// No `evaluate {expect: {merged: true}}` after this — the whole point.
		{type: "land", branch: "{{ctx.activeBranch}}", target: "main"},
	]
}
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_ungated_land_fails_closed_before_git_side_effects() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_path = repo_dir.path();
    git(repo_path, &["init", "-b", "main"]);
    git(repo_path, &["config", "user.email", "r@x"]);
    git(repo_path, &["config", "user.name", "R"]);
    std::fs::write(repo_path.join("README.md"), "# x\n").unwrap();
    git(repo_path, &["add", "."]);
    git(repo_path, &["commit", "-m", "init"]);

    let wf_dir = repo_path.join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("ungated-land.cue"), UNGATED_LAND).unwrap();

    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        fixture::with_rk_done(CONFLICTING_FAKE),
    );
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // The inbox resolves a scope to a repo path through the registry, so the
    // repo has to be registered for the git-backed clear to be answerable.
    let repo_name = repo_path.file_name().unwrap().to_string_lossy().to_string();
    client
        .call(
            "repo.add",
            json!({"name": repo_name, "path": repo_path.to_string_lossy()}),
        )
        .await
        .unwrap();

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "ungated-land",
                "repo": repo_path.to_string_lossy(),
                "params": {"taskId": "TKT-drop"},
            }),
        )
        .await
        .unwrap();
    let instance = started["instance"]["id"].as_str().unwrap().to_string();

    let mut status = json!(null);
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let res = client
            .call("workflow.status", json!({"name": instance}))
            .await
            .unwrap();
        if res["instance"]["status"] != "running" {
            status = res;
            break;
        }
    }
    // The workflow must fail at the capability boundary, regardless of what
    // the definition author forgot to put after the land step.
    assert_eq!(
        status["instance"]["status"], "failed",
        "an ungated land must fail closed: {status}"
    );
    assert_eq!(
        status["instance"]["error"],
        "land step requires a prior approved human gate or a trusted automated workflow",
        "the failure should explain the required operator action: {status}"
    );
    let branch = status["instance"]["context"]["active_branch"]
        .as_str()
        .expect("instance holds the branch")
        .to_string();

    // Sanity: main really does not have the work.
    let merged = Command::new("git")
        .arg("-C")
        .arg(repo_path)
        .args(["merge-base", "--is-ancestor", &branch, "main"])
        .status()
        .unwrap();
    assert!(
        !merged.success(),
        "sanity: {branch} must NOT be an ancestor of main"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

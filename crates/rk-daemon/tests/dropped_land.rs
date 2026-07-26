//! TKT-171: a `land` that does not merge must not be able to disappear.
//!
//! `land` reports a merge conflict as a clean `{merged: false}` rather than an
//! error — deliberately, so a workflow can gate on the outcome and retry. The
//! catch is that the gate lives in the workflow DEFINITION
//! (`evaluate {expect: {merged: true}}`), and a definition is a file: it can be
//! stale, copied per repo, or hand-edited. When the steward that reviewed
//! TKT-147 ran against a definition missing that gate, the land conflicted, the
//! instance COMPLETED, and the reviewed fix sat off main for two days with
//! nothing anywhere naming it — no failed agent, no failed instance, no inbox
//! row. The tuplespace held the evidence the whole time (a `branch_landed`
//! event with `merged: false`); nothing read it.
//!
//! So the invariant is asserted at read time in `rk inbox`, over the event every
//! land already emits, for every workflow — including the ones that forgot the
//! gate. This test runs exactly that: a deliberately UNGATED workflow whose land
//! conflicts, and proves the drop surfaces anyway, and then clears itself when
//! the branch actually reaches main.

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
        if let Ok(c) = Client::connect(layout).await {
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
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"drop-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

/// A workflow that lands WITHOUT gating the result — the shape of the stale
/// steward definition that dropped TKT-147. The `run` step moves main onto the
/// same line the rat changed, so the land that follows conflicts.
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
async fn a_land_that_conflicts_surfaces_even_when_the_workflow_never_gated_it() {
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

    std::env::set_var("RK_FAKE_HARNESS_CMD", CONFLICTING_FAKE);
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
    // THE FAILURE MODE: the run reports success. Nothing about the instance
    // says the work never landed — which is precisely why the inbox has to.
    assert_eq!(
        status["instance"]["status"], "completed",
        "an ungated land completes clean; that is the bug being guarded: {status}"
    );
    let branch = status["instance"]["context"]["active_branch"]
        .as_str()
        .expect("instance holds the landed branch")
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

    let inbox = client.call("inbox.list", json!({})).await.unwrap();
    let items = inbox["items"].as_array().expect("inbox has items array");
    let row = items
        .iter()
        .find(|it| it["kind"] == "unlanded-branch")
        .unwrap_or_else(|| panic!("a dropped land must surface in the inbox: {inbox}"));
    assert_eq!(row["subject"], branch, "row is keyed on the branch: {row}");
    let detail = row["detail"].as_str().unwrap_or("");
    assert!(
        detail.contains(&branch) && detail.contains("main"),
        "detail names the branch and the target it never reached: {row}"
    );
    // The reason git gave has to reach the operator. `git merge` writes its
    // diagnostic to stdout with an empty stderr, so this used to read
    // `... failed:` and stop there.
    assert!(
        detail.contains("CONFLICT") && detail.contains("README.md"),
        "detail must carry git's own reason, naming the conflicting file: {row}"
    );
    assert!(
        row["action"].as_str().unwrap_or("").contains("git merge"),
        "row carries the command that resolves it: {row}"
    );

    // Now resolve it the way an operator would — merge the branch by hand,
    // keeping main's side of the conflict. Nothing records that this happened;
    // the row has to clear off git alone.
    git(repo_path, &["checkout", "main"]);
    git(
        repo_path,
        &["merge", "-s", "ours", "-m", "resolve", &branch],
    );
    let inbox = client.call("inbox.list", json!({})).await.unwrap();
    let items = inbox["items"].as_array().expect("inbox has items array");
    assert!(
        !items
            .iter()
            .any(|it| it["kind"] == "unlanded-branch" && it["subject"] == branch),
        "a branch that has since reached main must stop nagging: {inbox}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

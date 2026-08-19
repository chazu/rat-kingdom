//! nightly-self-improve end to end: one workflow instance runs three phases in
//! order — a single-spawn GROOM (dismissed noMerge), a fan-out DRAIN (joined and
//! dismiss_all'd), then a single-spawn REFINE (dismissed with a merge). This is
//! the genuinely new shape TKT-23 introduces: a single spawn, a fan-out, and
//! another single spawn welded into ONE instance so a single schedule fires the
//! whole chain under one single-flight lock. The test proves the executor drives
//! all three phases and honours each phase's merge policy: the two drain branches
//! and the refine branch land on main, while the noMerge groom branch does not.

mod fixture;

mod support;

use rk_core::paths::Layout;
use rk_daemon::Daemon;
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
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
    String::from_utf8_lossy(&out.stdout).to_string()
}

// Every spawned rat — groom, each drain rat, and refine — writes a distinct
// per-agent file (so merges never conflict) and reports a clean success.
const WORKING_FAKE: &str = r#"
read -r _prompt
echo "$RK_TASK by $RK_AGENT" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"nsi-fake"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"ok","session_id":"nsi-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

// The nightly-self-improve shape with the fake harness: single-spawn groom
// (noMerge) -> fan-out drain (evaluate all_ok -> dismiss_all) -> single-spawn
// refine (evaluate is_error:false -> dismiss/merge). 11 steps.
const WORKFLOW: &str = r#"
workflow: {
    name: "nsi-test"
    params: {repo: {type: "string", required: false, default: ""}}
    agents: {default: {harness: "fake", model: "sonnet"}}
    steps: [
        {type: "spawn", role: "rat", task: {title: "groom-backlog", description: "groom"}},
        {type: "wait", timeout: "60s"},
        {type: "dismiss", noMerge: true},

        {type: "for_each", query: {status: "ready", limit: 5}, role: "rat",
            task: {title: "{{item.id}}", description: "Implement {{item.title}}: {{item.body}}"}},
        {type: "wait_all", timeout: "60s"},
        {type: "evaluate", expect: {all_ok: true}},
        {type: "dismiss_all"},

        {type: "spawn", role: "rat", branch: "main", task: {title: "refine-prompts", description: "refine"}},
        {type: "wait", timeout: "60s"},
        {type: "evaluate", expect: {is_error: false}},
        {type: "dismiss"},
    ]
}
"#;

#[tokio::test]
async fn nightly_self_improve_runs_all_three_phases() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    support::install_passing_landing_checks(repo_dir.path());

    let repo_name = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("nsi-test.cue"), WORKFLOW).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // Two ready tickets → two fanned-out drain rats → two drain branches.
    for title in ["add caching", "fix pagination"] {
        client
            .call(
                "ticket.new",
                json!({"title": title, "body": "do it", "scope": repo_name}),
            )
            .await
            .unwrap();
    }

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "nsi-test",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();
    // groom(spawn,wait,dismiss) + drain(for_each,wait_all,evaluate,dismiss_all)
    // + refine(spawn,wait,evaluate,dismiss) = 11 steps.
    assert_eq!(started["instance"]["total_steps"], 11);

    let mut completed = false;
    for _ in 0..300 {
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
            "failed" => panic!("workflow failed: {}", status["instance"]["error"]),
            _ => {}
        }
    }
    assert!(completed, "workflow did not complete all three phases");

    // main now carries exactly three merged work files: two from the drain
    // fan-out and one from the refine single-spawn. The groom phase dismissed
    // noMerge, so its branch's work file is NOT on main — the merge policy of
    // each phase was honoured within the one instance.
    let base = repo_dir.path();
    git(base, &["checkout", "main"]);
    let tracked = git(base, &["ls-files"]);
    let merged: Vec<&str> = tracked.lines().filter(|f| f.starts_with("work-")).collect();
    assert_eq!(
        merged.len(),
        3,
        "expected 2 drain + 1 refine work files on main (groom is noMerge), got: {tracked}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

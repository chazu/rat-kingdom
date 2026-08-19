//! TKT-57 end to end: a `sub_workflow` step runs another named workflow inline,
//! to completion, and joins the child's result into the parent's
//! ctx.previousResult — so a parent `evaluate` can gate on how the child
//! finished. A second test proves the nesting depth cap fails a workflow cycle
//! closed instead of recursing without end.

mod fixture;

mod support;

use rk_core::paths::Layout;
use rk_daemon::Daemon;
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

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

async fn wait_socket_gone(layout: &Layout) {
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if !layout.socket_path().exists() {
            return;
        }
    }
    panic!("daemon did not release its socket on shutdown");
}

fn init_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

const WORKING_FAKE: &str = r#"
read -r _prompt
echo "work for $RK_TASK by $RK_AGENT" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"wf-fake"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"did the work","session_id":"wf-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

// The child: a plain build-and-merge. Its final step is `dismiss`, so its
// ctx.previousResult ends as the dismiss outcome ({merged: true, ...}) — that is
// the value the parent joins on.
const CHILD: &str = r#"
workflow: {
    name: "child-build"
    params: {taskId: {type: "string", required: true}}
    agents: {default: {harness: "fake", model: "sonnet"}}
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId, description: "do " + _input.taskId}},
        {type: "wait", timeout: "30s"},
        {type: "evaluate", expect: {is_error: false}},
        {type: "dismiss"},
    ]
}
"#;

// The parent: run the child by name (forwarding a templated param), then gate on
// the child's joined result — {merged: true} proves the child ran to completion
// AND its dismiss outcome reached the parent's ctx.previousResult.
const PARENT: &str = r#"
workflow: {
    name: "parent-compose"
    params: {taskId: {type: "string", required: true}}
    steps: [
        {type: "sub_workflow", workflow: "child-build", params: {taskId: _input.taskId}},
        {type: "evaluate", expect: {merged: true}},
    ]
}
"#;

const NESTED_PARENT: &str = r#"
workflow: {
    name: "nested-parent-compose"
    params: {taskId: {type: "string", required: true}}
    steps: [{
        type: "when"
        var: "unset"
        cases: {}
        default: [
            {type: "sub_workflow", workflow: "child-build", params: {taskId: _input.taskId}},
            {type: "evaluate", expect: {merged: true}},
        ]
    }]
}
"#;

#[tokio::test]
async fn sub_workflow_runs_child_and_joins_its_result() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("child-build.cue"), CHILD).unwrap();
    std::fs::write(wf_dir.join("parent-compose.cue"), PARENT).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "parent-compose",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"taskId": "compose-1"},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

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
            "failed" => panic!("parent workflow failed: {}", status["instance"]["error"]),
            _ => {}
        }
    }
    assert!(completed, "parent workflow did not complete");

    // The child's dismiss merged its rat's work into main — proof the parent's
    // sub_workflow step drove the child all the way through its own dismiss.
    let files = Command::new("git")
        .arg("-C")
        .arg(repo_dir.path())
        .args(["ls-tree", "--name-only", "main"])
        .output()
        .unwrap();
    let listing = String::from_utf8_lossy(&files.stdout).to_string();
    assert!(
        listing.contains("work-"),
        "child work must merge into main: {listing}"
    );

    // The child instance is recorded independently (own id, completed), so it
    // shows up in `rk workflow list` just like a top-level run.
    let list = client.call("workflow.list", json!({})).await.unwrap();
    let instances = list["instances"].as_array().unwrap();
    let child = instances
        .iter()
        .find(|i| i["workflow"] == "child-build")
        .expect("child instance recorded");
    assert_eq!(child["status"], "completed");
    assert_ne!(
        child["id"].as_str().unwrap(),
        id,
        "child has its own instance id"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

#[tokio::test]
async fn nested_sub_workflow_clears_its_link_only_with_the_top_level_cursor() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("child-build.cue"), CHILD).unwrap();
    std::fs::write(wf_dir.join("nested-parent-compose.cue"), NESTED_PARENT).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "nested-parent-compose",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"taskId": "nested-compose-1"},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        match status["instance"]["status"].as_str().unwrap_or("") {
            "completed" => {
                assert_eq!(status["instance"]["current_step"], 1);
                assert!(status["instance"]["context"]["active_subworkflow"].is_null());
                std::env::remove_var("RK_FAKE_HARNESS_CMD");
                return;
            }
            "failed" => panic!("nested parent failed: {}", status["instance"]["error"]),
            _ => {}
        }
    }
    panic!("nested parent did not complete");
}

// A workflow whose only step runs itself — an unbounded cycle but for the depth
// cap that fails it closed.
const CYCLE: &str = r#"
workflow: {
    name: "cycle"
    steps: [
        {type: "sub_workflow", workflow: "cycle"},
    ]
}
"#;

#[tokio::test]
async fn sub_workflow_cycle_fails_closed_at_depth_cap() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("cycle.cue"), CYCLE).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "cycle",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    let mut failed = false;
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        match status["instance"]["status"].as_str().unwrap_or("") {
            "failed" => {
                let err = status["instance"]["error"].as_str().unwrap_or("");
                assert!(
                    err.contains("nesting too deep"),
                    "expected a depth-cap failure, got: {err}"
                );
                failed = true;
                break;
            }
            "completed" => panic!("a workflow cycle must not complete"),
            _ => {}
        }
    }
    assert!(
        failed,
        "workflow cycle did not fail closed at the depth cap"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

const PARKED_CHILD: &str = r#"
workflow: {
    name: "parked-child"
    steps: [{type: "gate", gateType: "timer", duration: "600s"}]
}
"#;

const PARKED_PARENT: &str = r#"
workflow: {
    name: "parked-parent"
    steps: [{type: "sub_workflow", workflow: "parked-child"}]
}
"#;

fn snapshot_for(home: &Path, workflow: &str) -> (std::path::PathBuf, serde_json::Value) {
    std::fs::read_dir(home.join("workflow-instances"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find_map(|path| {
            let value: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            (value["workflow"] == workflow).then_some((path, value))
        })
        .unwrap()
}

#[tokio::test]
async fn interrupted_sub_workflow_rejoins_the_same_durable_child_after_restart() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("parked-child.cue"), PARKED_CHILD).unwrap();
    std::fs::write(wf_dir.join("parked-parent.cue"), PARKED_PARENT).unwrap();

    let layout = Layout::at(home.path());
    let daemon_a = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;
    client
        .call(
            "workflow.run",
            json!({
                "name": "parked-parent",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {},
            }),
        )
        .await
        .unwrap();

    let original_child = loop {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let list = client.call("workflow.list", json!({})).await.unwrap();
        if let Some(child) = list["instances"]
            .as_array()
            .and_then(|instances| instances.iter().find(|i| i["workflow"] == "parked-child"))
        {
            break child["id"].as_str().unwrap().to_string();
        }
    };

    client.call("stop", json!({})).await.ok();
    wait_socket_gone(&layout).await;
    let _ = handle_a.await;

    let daemon_b = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let list = client.call("workflow.list", json!({})).await.unwrap();
    let children: Vec<_> = list["instances"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|i| i["workflow"] == "parked-child")
        .collect();

    assert_eq!(
        children.len(),
        1,
        "restart must not mint a second child: {list}"
    );
    assert_eq!(children[0]["id"], original_child);
}

#[tokio::test]
async fn legacy_unlinked_running_child_fails_closed_instead_of_duplicating() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("parked-child.cue"), PARKED_CHILD).unwrap();
    std::fs::write(wf_dir.join("parked-parent.cue"), PARKED_PARENT).unwrap();

    let layout = Layout::at(home.path());
    let daemon_a = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;
    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "parked-parent",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {},
            }),
        )
        .await
        .unwrap();
    let parent_id = started["instance"]["id"].as_str().unwrap().to_string();

    loop {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let list = client.call("workflow.list", json!({})).await.unwrap();
        if list["instances"]
            .as_array()
            .is_some_and(|instances| instances.iter().any(|i| i["workflow"] == "parked-child"))
        {
            break;
        }
    }
    client.call("stop", json!({})).await.ok();
    wait_socket_gone(&layout).await;
    let _ = handle_a.await;

    let parent_file = home
        .path()
        .join("workflow-instances")
        .join(format!("{parent_id}.json"));
    let mut parent: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&parent_file).unwrap()).unwrap();
    parent["context"]
        .as_object_mut()
        .unwrap()
        .remove("active_subworkflow");
    std::fs::write(&parent_file, serde_json::to_vec_pretty(&parent).unwrap()).unwrap();

    let daemon_b = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let list = client.call("workflow.list", json!({})).await.unwrap();
    let instances = list["instances"].as_array().unwrap();
    assert_eq!(
        instances
            .iter()
            .filter(|i| i["workflow"] == "parked-child")
            .count(),
        1,
        "an unlinked legacy child must not cause a replacement child launch: {list}"
    );
    assert_eq!(
        instances.iter().find(|i| i["id"] == parent_id).unwrap()["status"],
        "failed",
        "ambiguous legacy ownership must fail closed"
    );
}

#[tokio::test]
async fn legacy_unlinked_completed_child_fails_parent_closed_without_relaunch() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("parked-child.cue"), PARKED_CHILD).unwrap();
    std::fs::write(wf_dir.join("parked-parent.cue"), PARKED_PARENT).unwrap();

    let layout = Layout::at(home.path());
    let daemon_a = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;
    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "parked-parent",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {},
            }),
        )
        .await
        .unwrap();
    let parent_id = started["instance"]["id"].as_str().unwrap().to_string();
    loop {
        tokio::time::sleep(Duration::from_millis(25)).await;
        if client.call("workflow.list", json!({})).await.unwrap()["instances"]
            .as_array()
            .is_some_and(|instances| instances.iter().any(|i| i["workflow"] == "parked-child"))
        {
            break;
        }
    }
    client.call("stop", json!({})).await.ok();
    wait_socket_gone(&layout).await;
    let _ = handle_a.await;

    let (parent_path, mut parent) = snapshot_for(home.path(), "parked-parent");
    parent["context"]
        .as_object_mut()
        .unwrap()
        .remove("active_subworkflow");
    std::fs::write(&parent_path, serde_json::to_vec_pretty(&parent).unwrap()).unwrap();
    let (child_path, mut child) = snapshot_for(home.path(), "parked-child");
    child["status"] = json!("completed");
    child["completed_at"] = json!(chrono::Utc::now());
    std::fs::write(&child_path, serde_json::to_vec_pretty(&child).unwrap()).unwrap();

    let daemon_b = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let list = client.call("workflow.list", json!({})).await.unwrap();
    let instances = list["instances"].as_array().unwrap();
    assert_eq!(
        instances
            .iter()
            .filter(|i| i["workflow"] == "parked-child")
            .count(),
        1,
        "a terminal legacy child must not be relaunched: {list}"
    );
    assert_eq!(
        instances.iter().find(|i| i["id"] == parent_id).unwrap()["status"],
        "failed"
    );
}

#[tokio::test]
async fn terminal_parent_does_not_shield_a_running_linked_child_from_recovery() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("parked-child.cue"), PARKED_CHILD).unwrap();
    std::fs::write(wf_dir.join("parked-parent.cue"), PARKED_PARENT).unwrap();

    let layout = Layout::at(home.path());
    let daemon_a = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;
    client
        .call(
            "workflow.run",
            json!({
                "name": "parked-parent",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {},
            }),
        )
        .await
        .unwrap();
    loop {
        tokio::time::sleep(Duration::from_millis(25)).await;
        if client.call("workflow.list", json!({})).await.unwrap()["instances"]
            .as_array()
            .is_some_and(|instances| instances.iter().any(|i| i["workflow"] == "parked-child"))
        {
            break;
        }
    }
    client.call("stop", json!({})).await.ok();
    wait_socket_gone(&layout).await;
    let _ = handle_a.await;

    let (parent_path, mut parent) = snapshot_for(home.path(), "parked-parent");
    parent["status"] = json!("failed");
    parent["error"] = json!("injected terminal parent");
    parent["completed_at"] = json!(chrono::Utc::now());
    std::fs::write(&parent_path, serde_json::to_vec_pretty(&parent).unwrap()).unwrap();

    let daemon_b = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let list = client.call("workflow.list", json!({})).await.unwrap();
    let child = list["instances"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["workflow"] == "parked-child")
        .unwrap();
    assert_eq!(
        child["status"], "failed",
        "a terminal parent cannot own a running child after restart: {list}"
    );
}

#[tokio::test]
async fn mismatched_linked_child_is_failed_with_its_parent_instead_of_becoming_a_zombie() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("parked-child.cue"), PARKED_CHILD).unwrap();
    std::fs::write(wf_dir.join("parked-parent.cue"), PARKED_PARENT).unwrap();

    let layout = Layout::at(home.path());
    let daemon_a = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;
    client
        .call(
            "workflow.run",
            json!({
                "name": "parked-parent",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {},
            }),
        )
        .await
        .unwrap();
    loop {
        tokio::time::sleep(Duration::from_millis(25)).await;
        if client.call("workflow.list", json!({})).await.unwrap()["instances"]
            .as_array()
            .is_some_and(|instances| instances.iter().any(|i| i["workflow"] == "parked-child"))
        {
            break;
        }
    }
    client.call("stop", json!({})).await.ok();
    wait_socket_gone(&layout).await;
    let _ = handle_a.await;

    let (child_path, mut child) = snapshot_for(home.path(), "parked-child");
    child["definition"] = json!("different-child");
    std::fs::write(&child_path, serde_json::to_vec_pretty(&child).unwrap()).unwrap();

    let daemon_b = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let list = client.call("workflow.list", json!({})).await.unwrap();
    let instances = list["instances"].as_array().unwrap();
    assert_eq!(
        instances
            .iter()
            .find(|i| i["workflow"] == "parked-parent")
            .unwrap()["status"],
        "failed"
    );
    assert_eq!(
        instances
            .iter()
            .find(|i| i["workflow"] == "parked-child")
            .unwrap()["status"],
        "failed",
        "a rejected linked child must not remain Running forever: {list}"
    );
}

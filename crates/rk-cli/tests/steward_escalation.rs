//! End-to-end proof of the steward escalation path: a failed run gate must
//! produce a VISIBLE need row, and a failing escalation must not mask the
//! gate's own result (TKT-01M00WPWEFZVPW3YBNX3825MBG).
//!
//! The regression this pins: run-step children execute inside the active
//! agent's worktree, which the server treats as that agent's authority domain
//! — so an escalation check shelling back into `rk` with no identity was
//! FORBIDDEN deterministically, exited 1, and the instance error replaced the
//! gate failure with the report command's own. The daemon now hands run-step
//! children the active agent's credential set, and composes both failures.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

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

async fn connect(layout: &Layout) -> Client {
    for _ in 0..1500 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

const RESULT_LINE: &str = r#"echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"wf-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'"#;

/// Fake harness: the rat commits a file and declares itself done.
fn fake_harness(rk_bin: &str) -> String {
    format!(
        r#"
read -r _prompt
echo '{{"type":"system","subtype":"init","session_id":"wf-fake"}}'
echo "work by $RK_AGENT" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
"{rk_bin}" done "did the work" >/dev/null 2>&1
{RESULT_LINE}
"#
    )
}

/// A steward-shaped workflow: spawn -> wait -> failing gate -> escalation.
/// The escalation command is the real shape from `.rk/checks.cue`: shell back
/// into `rk out need … --field …` from inside the agent's worktree.
fn escalation_workflow(escalation_command: &str, rk_bin: &str) -> String {
    format!(
        r#"
workflow: {{
	name: "escalate"
	params: {{taskId: {{type: "string", required: true}}}}
	agents: {{default: {{harness: "fake", model: "sonnet"}}}}
	steps: [
		{{type: "spawn", role: "rat", task: {{title: _input.taskId, description: "noop"}}}},
		{{type: "wait", timeout: "2m"}},
		{{
			type:    "run"
			command: "echo boom >&2; exit 1"
			field:   "verdict"
			into:    "gate"
		}},
		{{
			type: "when"
			var:  "gate"
			cases: {{"pass": []}}
			default: [
				{{
					type:    "run"
					command: {escalation_command}
					env: {{
						RK_CHECK_RK:      "{rk_bin}"
						RK_CHECK_REPO:    "fixture"
						RK_CHECK_TASK_ID: _input.taskId
					}}
					expectExit: 0
					timeout:    "2m"
				}},
				{{type: "dismiss", noMerge: true}},
			]
		}},
	]
}}
"#
    )
}

async fn fixture(
    workflow_source: &str,
) -> (
    tempfile::TempDir,
    tempfile::TempDir,
    Client,
    tokio::task::JoinHandle<rk_core::Result<()>>,
) {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    std::fs::create_dir_all(repo_dir.path().join(".rk")).unwrap();
    std::fs::write(repo_dir.path().join(".rk/repo.cue"), "repo: {}\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("escalate.cue"), workflow_source).unwrap();

    // Same value in every test in this file: RK_FAKE_HARNESS_CMD is
    // process-global, and identical writes cannot race destructively.
    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        fake_harness(env!("CARGO_BIN_EXE_rk")),
    );
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    client
        .call(
            "repo.add",
            json!({"name": "steward-escalation", "path": repo_dir.path()}),
        )
        .await
        .unwrap();
    (home, repo_dir, client, handle)
}

async fn run_to_terminal(client: &mut Client, repo: &Path, task_id: &str) -> Value {
    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "escalate",
                "repo": repo.to_string_lossy(),
                "params": {"taskId": task_id},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();
    let mut last = json!(null);
    for _ in 0..600 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        last = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        let instance = last["instance"].clone();
        match instance["status"].as_str() {
            Some("completed") | Some("failed") => return instance,
            _ => {}
        }
    }
    panic!("workflow did not reach a terminal state: {last}");
}

/// The happy escalation: gate fails -> the check writes the need through the
/// real `rk` binary from inside the agent's worktree -> the row is visible.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_gate_produces_a_visible_need_row() {
    let escalation = r#""$RK_CHECK_RK" out need "$RK_CHECK_REPO" steward --field agent=steward --field "task=$RK_CHECK_TASK_ID" --field "text=steward: run gate FAILED for $RK_CHECK_TASK_ID — branch held unmerged""#;
    let wf = escalation_workflow(&format!("{escalation:?}"), env!("CARGO_BIN_EXE_rk"));
    let (home, repo, mut client, _handle) = fixture(&wf).await;

    let status = run_to_terminal(&mut client, repo.path(), "esc-e2e-1").await;
    assert_eq!(status["status"], "completed", "status: {status}");

    // The need tuple exists, attributed to the steward role, carrying the task.
    let scanned = client
        .call(
            "space.scan",
            json!({"category": "need", "scope": "fixture", "identity": "steward"}),
        )
        .await
        .unwrap();
    let tuples = scanned["tuples"].as_array().unwrap();
    assert_eq!(tuples.len(), 1, "scanned: {scanned}");
    assert_eq!(tuples[0]["payload"]["agent"], "steward");
    assert_eq!(tuples[0]["payload"]["task"], "esc-e2e-1");

    // And it is VISIBLE where a human looks: the inbox carries the need text.
    let inbox = Command::new(env!("CARGO_BIN_EXE_rk"))
        .args(["--json", "inbox"])
        .env("RK_HOME", home.path())
        .env_remove("RK_AGENT")
        .env_remove("RK_AUTH_TOKEN")
        .output()
        .unwrap();
    assert!(inbox.status.success());
    let rendered = String::from_utf8_lossy(&inbox.stdout);
    assert!(
        rendered.contains("run gate FAILED for esc-e2e-1"),
        "inbox missing the escalation row: {rendered}"
    );
}

/// The unhappy escalation: even when the report command ITSELF dies, the
/// instance error leads with the gate's result and carries both failures.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dead_escalation_check_does_not_mask_the_gate_failure() {
    let escalation = r#""echo escalate-broke >&2; exit 3""#;
    let wf = escalation_workflow(escalation, env!("CARGO_BIN_EXE_rk"));
    let (_home, repo, mut client, _handle) = fixture(&wf).await;

    let status = run_to_terminal(&mut client, repo.path(), "esc-e2e-2").await;
    assert_eq!(status["status"], "failed", "status: {status}");
    let error = status["error"].as_str().unwrap();
    assert!(
        error.starts_with("gate failed first: verdict fail, exit 1"),
        "error must lead with the gate result: {error}"
    );
    assert!(
        error.contains("stderr: boom"),
        "gate stderr present: {error}"
    );
    assert!(
        error.contains("exited 3, expected 0") && error.contains("escalate-broke"),
        "escalation failure present: {error}"
    );
}

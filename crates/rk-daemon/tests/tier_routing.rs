//! Cost-tier routing end to end: a `for_each` fans out one rat per ready
//! ticket, and each ticket's labels/priority route its spawn to a tier — an
//! agent profile — via the workflow's `tiers` table. Proves the full wiring
//! (ticket labels/priority → routing rule → resolved model) by reading back
//! each fanned-out rat's resolved model from `agent.list` (TKT-26).

mod support;

use rk_core::paths::Layout;
use rk_daemon::Daemon;
use serde_json::json;
use std::path::Path;
use std::time::Duration;
use support::connect;

fn git(dir: &Path, args: &[&str]) {
    let out = std::process::Command::new("git")
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

// Minimal successful rat: commit nothing controversial, report a clean success.
const WORKING_FAKE: &str = r#"
read -r _prompt
echo "done $RK_TASK" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"tier-fake"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"ok","session_id":"tier-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

// for_each → wait_all only: we assert on the fanned-out rats' resolved models
// before any dismiss, so the records stay in the registry. Cheap and premium
// are both the `fake` harness (so they actually run) but carry distinct models,
// which is exactly what the routing must select per ticket.
const WORKFLOW: &str = r#"
workflow: {
    name: "tier-route-test"
    params: {repo: {type: "string", required: false, default: ""}}
    agents: {
        default: {harness: "fake", model: "sonnet"}
        cheap:   {harness: "fake", model: "haiku"}
        premium: {harness: "fake", model: "opus"}
    }
    tiers: {
        rules: [
            {label: "mechanical", tier: "cheap"},
            {priority: "high", tier: "premium"},
        ]
    }
    steps: [
        {
            type: "for_each"
            query: {status: "ready", limit: 5}
            role: "rat"
            task: {title: "{{item.id}}", description: "Implement {{item.title}}"}
        },
        {type: "wait_all", timeout: "60s"},
    ]
}
"#;

#[tokio::test]
async fn tickets_route_to_their_cost_tier() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    support::install_default_repository_policy(repo_dir.path());

    let repo_name = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("tier-route-test.cue"), WORKFLOW).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;

    // A mechanical-labelled ticket → cheap tier; a high-priority one → premium.
    let mech = client
        .call(
            "ticket.new",
            json!({"title": "tidy", "body": "chore", "scope": repo_name, "labels": ["mechanical"]}),
        )
        .await
        .unwrap();
    let mech_id = mech["ticket"]["identity"].as_str().unwrap().to_string();
    let hard = client
        .call(
            "ticket.new",
            json!({"title": "hard problem", "body": "think", "scope": repo_name, "priority": "high"}),
        )
        .await
        .unwrap();
    let hard_id = hard["ticket"]["identity"].as_str().unwrap().to_string();

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "tier-route-test",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {},
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
            "failed" => panic!("workflow failed: {}", status["instance"]["error"]),
            _ => {}
        }
    }
    assert!(completed, "workflow did not complete");

    // Read back each fanned-out rat's resolved model: the mechanical ticket's
    // rat must have run on the cheap tier (haiku), the high-priority one on the
    // premium tier (opus) — the routing rules, applied per ticket.
    let agents = client.call("agent.list", json!({})).await.unwrap();
    let records = agents["agents"].as_array().unwrap();
    let model_for = |ticket: &str| -> Option<String> {
        records
            .iter()
            .find(|r| r["task"].as_str() == Some(ticket))
            .and_then(|r| r["model"].as_str().map(String::from))
    };
    assert_eq!(
        model_for(&mech_id).as_deref(),
        Some("haiku"),
        "mechanical-labelled ticket should route to the cheap tier"
    );
    assert_eq!(
        model_for(&hard_id).as_deref(),
        Some("opus"),
        "high-priority ticket should route to the premium tier"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

// A single `spawn` step (how every reviewer dispatches — `for_each` fan-out
// is worker-only) with a literal `priority` bound from a workflow param, so
// its resolved model must come from the SAME `tiers` table `for_each` uses,
// not the step's own named `reviewer` profile. Proves Step::Spawn now
// consults tier routing at all — previously it never did.
const REVIEWER_SPAWN_WORKFLOW: &str = r#"
workflow: {
    name: "reviewer-tier-route-test"
    params: {
        priority: {type: "string", required: false, default: ""}
        repo:     {type: "string", required: false, default: ""}
    }
    agents: {
        default:  {harness: "fake", model: "sonnet"}
        reviewer: {harness: "fake", model: "gpt-5.6-luna"}
        premium:  {harness: "fake", model: "opus"}
    }
    tiers: {
        rules: [
            {priority: "high", tier: "premium"},
        ]
    }
    steps: [
        {
            type:     "spawn"
            role:     "reviewer"
            agent:    "reviewer"
            priority: _input.priority
            task: {title: "review", description: "review it"}
        },
        {type: "wait", timeout: "60s"},
    ]
}
"#;

#[tokio::test]
async fn spawn_step_reviewer_routes_to_its_cost_tier() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    support::install_default_repository_policy(repo_dir.path());

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(
        wf_dir.join("reviewer-tier-route-test.cue"),
        REVIEWER_SPAWN_WORKFLOW,
    )
    .unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "reviewer-tier-route-test",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"priority": "high"},
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
            "failed" => panic!("workflow failed: {}", status["instance"]["error"]),
            _ => {}
        }
    }
    assert!(completed, "workflow did not complete");

    let agents = client.call("agent.list", json!({})).await.unwrap();
    let records = agents["agents"].as_array().unwrap();
    let reviewer = records
        .iter()
        .find(|r| r["role"].as_str() == Some("reviewer"))
        .expect("reviewer record");
    assert_eq!(
        reviewer["model"].as_str(),
        Some("opus"),
        "a high-priority review must route to the premium tier, overriding the \
         step's named `reviewer` profile (gpt-5.6-luna)"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

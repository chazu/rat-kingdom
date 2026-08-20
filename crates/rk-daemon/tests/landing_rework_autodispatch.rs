//! Live proof of the bounded reviewer-REWORK loop. This deliberately crosses
//! the RPC, supervisor, reactor, landing queue, git, ticket, and review-cache
//! boundaries instead of calling `LandingPipeline` internals.

mod fixture;
mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::{connect, install_passing_landing_checks};

static HARNESS_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const LANDING_TRIGGER: &str = r#"
triggers: [{
    name: "landing-rework-e2e"
    action: "land"
    match: {category: "event", identity: "harness_result", search: "\"role\":\"rat\""}
    maxFires: 20
}]
"#;

const REVIEW_WORKFLOW: &str = r#"
package workflow
workflow: {
    name: "steward-review"
    params: {
        taskId: {type: "string", required: false, default: "unknown"}
        branch: {type: "string", required: true}
        repo: {type: "string", required: false, default: "unknown"}
        target: {type: "string", required: false, default: "main"}
        headSha: {type: "string", required: false, default: ""}
        reviewTimeout: {type: "string", required: false, default: "30s"}
    }
    agents: {default: {harness: "fake"}, reviewer: {harness: "fake"}}
    steps: [
        {type: "spawn", role: "reviewer", agent: "reviewer", branch: _input.branch,
         task: {title: "fresh-review-" + _input.taskId, description: "review exact corrected head"}},
        {type: "wait", timeout: _input.reviewTimeout},
        {type: "evaluate", expect: {is_error: false}},
    ]
}
"#;

const FAKE: &str = r#"
read -r prompt
if [ "$RK_ROLE" = "reviewer" ]; then
  i=0
  while [ ! -f "$RK_HOME/release-reviewer" ] && [ "$i" -lt 600 ]; do
    sleep 0.05
    i=$((i+1))
  done
  echo '{"type":"system","subtype":"init","session_id":"rework-reviewer"}'
  rk_done "review complete"
  echo '{"type":"result","subtype":"success","is_error":false,"result":"reviewed","session_id":"rework-reviewer","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
else
  printf '%s' "$prompt" > "$RK_HOME/rework-prompt"
  mkdir -p docs
  echo "bounded correction" > docs/rework.md
  git add docs/rework.md >/dev/null 2>&1
  git -c user.email=r@x -c user.name=R commit -q -m "fix: bounded reviewer correction"
  echo '{"type":"system","subtype":"init","session_id":"rework-rat"}'
  rk_done "correction complete"
  echo '{"type":"result","subtype":"success","is_error":false,"result":"corrected","session_id":"rework-rat","total_cost_usd":0.01,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
fi
"#;

fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn init_repo(repo: &Path) -> String {
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "r@x"]);
    git(repo, &["config", "user.name", "R"]);
    std::fs::write(repo.join("README.md"), "# rework e2e\n").unwrap();
    git(repo, &["add", "README.md"]);
    git(repo, &["commit", "-m", "init"]);
    install_passing_landing_checks(repo);
    git(repo, &["checkout", "-b", "feature"]);
    let source: String = (0..200)
        .map(|index| format!("fn original_{index}() {{}}\n"))
        .collect();
    std::fs::write(repo.join("src.rs"), source).unwrap();
    git(repo, &["add", "src.rs"]);
    git(repo, &["commit", "-m", "feat: original work"]);
    let head = git(repo, &["rev-parse", "feature"]);
    git(repo, &["checkout", "main"]);
    head
}

async fn scan(client: &mut Client, scope: &str, identity: &str) -> Vec<Value> {
    client
        .call(
            "space.scan",
            json!({"category": "event", "scope": scope, "identity": identity}),
        )
        .await
        .unwrap()["tuples"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

async fn wait_for_parent_resubmission(client: &mut Client, scope: &str) -> String {
    for _ in 0..400 {
        for tuple in scan(client, scope, "landing_queue_entry").await {
            let payload = &tuple["payload"];
            if payload["branch"] == "feature" && payload["target"] == "main" {
                return payload["head_sha"].as_str().unwrap().to_string();
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("corrected parent was never resubmitted");
}

fn ref_contains(repo: &Path, rev: &str, path: &str) -> bool {
    Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["cat-file", "-e", &format!("{rev}:{path}")])
        .output()
        .is_ok_and(|output| output.status.success())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_rework_lands_intermediately_then_resubmits_parent_exactly_once() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(FAKE));

    let home = tempfile::tempdir().unwrap();
    let repo_root = tempfile::tempdir().unwrap();
    let repo = repo_root.path().join("rework-e2e-repo");
    std::fs::create_dir(&repo).unwrap();
    let reviewed_head = init_repo(&repo);
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    std::fs::create_dir_all(layout.triggers_dir()).unwrap();
    std::fs::write(layout.triggers_dir().join("landing.cue"), LANDING_TRIGGER).unwrap();
    std::fs::create_dir_all(layout.workflows_dir()).unwrap();
    std::fs::write(
        layout.workflows_dir().join("steward-review.cue"),
        REVIEW_WORKFLOW,
    )
    .unwrap();

    let daemon = Daemon::new_in_memory(layout.clone(), "rework-e2e".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    client
        .call("repo.add", json!({"name": "rework-e2e-repo", "path": repo}))
        .await
        .unwrap();
    let ticket = client
        .call(
            "ticket.new",
            json!({"title": "original work", "scope": "rework-e2e-repo"}),
        )
        .await
        .unwrap();
    let original_ticket = ticket["ticket"]["identity"].as_str().unwrap().to_string();
    client
        .call(
            "space.out",
            json!({
                "category": "artifact", "scope": "rework-e2e-repo", "identity": "review",
                "payload": {"task": original_ticket, "recommendation": "REWORK",
                    "notes": "add the bounded correction only", "bounded": true,
                    "branch": "feature", "head_sha": reviewed_head}
            }),
        )
        .await
        .unwrap();

    let routed = client
        .call(
            "repo.land",
            json!({"repo": repo, "branch": "feature", "target": "main", "task": original_ticket}),
        )
        .await
        .unwrap();
    assert_eq!(routed["status"], "rework-filed", "{routed}");

    let corrected_head = wait_for_parent_resubmission(&mut client, "rework-e2e-repo").await;
    let prompt = std::fs::read_to_string(layout.home().join("rework-prompt")).unwrap();
    for evidence in [
        "YOUR BASE IS `feature`",
        reviewed_head.as_str(),
        "add the bounded correction only",
        original_ticket.as_str(),
    ] {
        assert!(prompt.contains(evidence), "missing {evidence:?}: {prompt}");
    }
    client
        .call(
            "space.out",
            json!({
                "category": "artifact", "scope": "rework-e2e-repo", "identity": "review",
                "payload": {"task": original_ticket, "recommendation": "APPROVE",
                    "notes": "fresh corrected head is clean", "branch": "feature",
                    "head_sha": corrected_head}
            }),
        )
        .await
        .unwrap();
    std::fs::write(layout.home().join("release-reviewer"), "").unwrap();

    for _ in 0..400 {
        if ref_contains(&repo, "main", "src.rs") && ref_contains(&repo, "main", "docs/rework.md") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(ref_contains(&repo, "main", "src.rs"));
    assert!(ref_contains(&repo, "main", "docs/rework.md"));
    assert_eq!(
        scan(&mut client, "rework-e2e-repo", "landing_rework_dispatch")
            .await
            .len(),
        1
    );
    assert_eq!(
        scan(
            &mut client,
            "rework-e2e-repo",
            "landing_rework_resubmission"
        )
        .await
        .len(),
        1
    );
    let ticket = client
        .call("ticket.get", json!({"id": original_ticket}))
        .await
        .unwrap();
    assert_eq!(ticket["ticket"]["payload"]["status"], "closed", "{ticket}");

    handle.abort();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

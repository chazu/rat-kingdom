use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

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

fn repository() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    git(dir.path(), &["init", "-b", "main"]);
    git(dir.path(), &["config", "user.email", "test@example.com"]);
    git(dir.path(), &["config", "user.name", "Test"]);
    std::fs::write(
        dir.path().join("README.md"),
        "# Fixture\n\nVerify with `cargo test`.\n",
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

fn draft(title: &str, suffix: &str) -> Value {
    json!({
        "kind": "repo_file",
        "title": title,
        "evidence": ["README documents cargo test"],
        "target_path": ".rk/checks.cue",
        "action": "write_repo_file",
        "diff": format!(
            "--- /dev/null\n+++ b/.rk/checks.cue\n+{suffix}: {{ command: \"cargo test\" }}\n"
        ),
        "risk": "low",
        "verification": ["cargo test"],
        "named_check": {
            "name": suffix,
            "command": "cargo test",
            "cwd": ".",
            "expect_exit": 0,
            "timeout": "10m",
            "environment_policy": "strip_rk_spawn",
            "toolchain": "repository Rust toolchain",
        },
    })
}

async fn propose(client: &mut Client, session: &str, title: &str, suffix: &str) -> Value {
    client
        .call(
            "repo.onboard.propose",
            json!({
                "session": session,
                "proposal": draft(title, suffix),
            }),
        )
        .await
        .unwrap()["proposal"]
        .clone()
}

async fn decision(
    client: &mut Client,
    method: &str,
    session: &str,
    proposal: &Value,
) -> rk_core::Result<Value> {
    client
        .call(
            method,
            json!({
                "session": session,
                "proposal": proposal["id"],
                "digest": proposal["digest"],
            }),
        )
        .await
}

const COMPLETE: &str = r#"
echo '{"type":"system","subtype":"init","session_id":"onboarding-proposal"}'
read -r _first_message
echo '{"type":"result","subtype":"success","is_error":false,"result":"assessment complete","session_id":"onboarding-proposal","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5}}'
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proposals_are_content_bound_operator_decisions_and_restart_durable() {
    let home = tempfile::tempdir().unwrap();
    let repo = repository();
    std::env::set_var("RK_FAKE_HARNESS_CMD", COMPLETE);
    let layout = Layout::at(home.path());
    let daemon_a = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut operator = connect(&layout).await;

    let started = operator
        .call(
            "repo.onboard.start",
            json!({
                "target": repo.path(),
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let session = started["session"]["id"].as_str().unwrap().to_string();
    let agent = started["session"]["agent"].as_str().unwrap().to_string();
    let worktree = started["session"]["worktree"].as_str().unwrap().to_string();
    let mut onboarder = Client::connect_as(&layout, &agent).await.unwrap();

    let approved = propose(&mut onboarder, &session, "Add named check", "verify").await;
    assert_eq!(approved["status"], "proposed");
    assert_eq!(approved["risk"], "low");
    assert_eq!(approved["target_path"], ".rk/checks.cue");
    assert!(approved["digest"].as_str().unwrap().len() == 64);
    assert!(approved["tree_revision"].as_str().unwrap().len() >= 40);
    assert_eq!(
        approved["transitions"][0]["actor"], agent,
        "proposal attribution must come from the authenticated onboarder"
    );

    // A proposal is advice, not authority. The same onboarder cannot approve
    // it and cannot smuggle either decision or repository identity fields.
    assert!(
        decision(&mut onboarder, "repo.onboard.approve", &session, &approved)
            .await
            .is_err()
    );
    assert!(onboarder
        .call(
            "repo.onboard.propose",
            json!({
                "session": session,
                "proposal": {
                    "repository_identity": "forged",
                    "kind": "repo_file",
                    "title": "Forged identity",
                    "evidence": ["none"],
                    "target_path": ".rk/checks.cue",
                    "action": "write_repo_file",
                    "diff": "+forged",
                    "risk": "low",
                    "verification": ["none"],
                },
            }),
        )
        .await
        .is_err());
    assert!(operator
        .call(
            "repo.onboard.approve",
            json!({
                "session": session,
                "proposal": approved["id"],
                "digest": approved["digest"],
                "actor": "forged-human",
            }),
        )
        .await
        .is_err());

    // Two simultaneous approvals are one CAS decision. Both callers get the
    // same record, exactly one reports a state change, and the timestamp/actor
    // are server-derived and stable on retry.
    let mut operator_a = Client::connect_as_operator(&layout).await.unwrap();
    let mut operator_b = Client::connect_as_operator(&layout).await.unwrap();
    let (first, second) = tokio::join!(
        decision(&mut operator_a, "repo.onboard.approve", &session, &approved),
        decision(&mut operator_b, "repo.onboard.approve", &session, &approved)
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_ne!(first["changed"], second["changed"]);
    assert_eq!(first["proposal"], second["proposal"]);
    assert_eq!(first["proposal"]["status"], "approved");
    assert_eq!(first["proposal"]["decision_actor"], "operator@test-castle");
    assert!(first["proposal"]["decision_at"].is_string());
    assert!(
        decision(&mut operator, "repo.onboard.decline", &session, &approved)
            .await
            .is_err(),
        "the opposite decision cannot overwrite the first one"
    );

    // Opposing concurrent decisions have one visible winner and one conflict,
    // rather than two successful first decisions.
    let contested = propose(&mut onboarder, &session, "Add lint named check", "lint").await;
    let mut operator_a = Client::connect_as_operator(&layout).await.unwrap();
    let mut operator_b = Client::connect_as_operator(&layout).await.unwrap();
    let (approve, decline) = tokio::join!(
        decision(
            &mut operator_a,
            "repo.onboard.approve",
            &session,
            &contested
        ),
        decision(
            &mut operator_b,
            "repo.onboard.decline",
            &session,
            &contested
        )
    );
    assert_ne!(approve.is_ok(), decline.is_ok());
    let winner = approve.or(decline).unwrap();
    assert!(matches!(
        winner["proposal"]["status"].as_str(),
        Some("approved" | "declined")
    ));

    // Branch movement after review invalidates an otherwise exact digest.
    let stale = propose(&mut onboarder, &session, "Add build named check", "build").await;
    std::fs::write(Path::new(&worktree).join("REVISION"), "changed\n").unwrap();
    git(Path::new(&worktree), &["add", "REVISION"]);
    git(Path::new(&worktree), &["commit", "-m", "move tree"]);
    let stale_error = decision(&mut operator, "repo.onboard.approve", &session, &stale)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        stale_error.contains("stale onboarding tree"),
        "{stale_error}"
    );

    // A fresh proposal at the new tree survives restart. Hand-editing its
    // persisted diff without updating the digest is detected before approval.
    let edited = propose(&mut onboarder, &session, "Add test named check", "test").await;
    operator.call("stop", json!({})).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle_a)
        .await
        .expect("daemon A did not stop")
        .unwrap()
        .unwrap();

    let sessions_path = home.path().join("onboarding-sessions.json");
    let persisted: Value = serde_json::from_slice(&std::fs::read(&sessions_path).unwrap()).unwrap();
    let proposals = persisted[session.as_str()]["proposals"].as_array().unwrap();
    let approved_after_restart = proposals
        .iter()
        .find(|proposal| proposal["id"] == approved["id"])
        .unwrap();
    assert_eq!(approved_after_restart["status"], "approved");
    assert!(approved_after_restart["decision_at"].is_string());

    let mut tampered = persisted;
    let proposals = tampered[session.as_str()]["proposals"]
        .as_array_mut()
        .unwrap();
    let edited_record = proposals
        .iter_mut()
        .find(|proposal| proposal["id"] == edited["id"])
        .unwrap();
    edited_record["diff"] = json!("+content changed after review\n");
    std::fs::write(
        &sessions_path,
        serde_json::to_vec_pretty(&tampered).unwrap(),
    )
    .unwrap();

    let daemon_b = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle_b = tokio::spawn(daemon_b.run());
    let mut operator = connect(&layout).await;
    let status = operator
        .call("repo.onboard.status", json!({"session": session}))
        .await
        .unwrap();
    let approved_status = status["session"]["proposals"]
        .as_array()
        .unwrap()
        .iter()
        .find(|proposal| proposal["id"] == approved["id"])
        .unwrap();
    assert_eq!(approved_status["status"], "approved");
    assert!(approved_status["evidence"].is_array());
    assert!(approved_status["diff"].is_string());
    assert!(approved_status["risk"].is_string());
    assert!(approved_status["digest"].is_string());
    assert!(approved_status["decision_actor"].is_string());
    assert!(approved_status["decision_at"].is_string());

    let edited_error = decision(&mut operator, "repo.onboard.approve", &session, &edited)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        edited_error.contains("no longer matches its canonical digest"),
        "{edited_error}"
    );
    let report = operator
        .call("repo.onboard.report", json!({"session": session}))
        .await
        .unwrap();
    assert_eq!(
        report["report"]["proposals"],
        report["report"]["session"]["proposals"]
    );

    operator.call("stop", json!({})).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle_b)
        .await
        .expect("daemon B did not stop")
        .unwrap()
        .unwrap();
}

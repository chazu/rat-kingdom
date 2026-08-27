//! Program C5: the diagnostician role is read-only *by enforcement*, over the
//! wire, using the machinery extracted from the onboarder.
//!
//! The unit tests in `read_only_roles` cover the allowlist as a function. This
//! covers the thing that actually matters: a real spawned diagnostician holding
//! a real, valid agent token is still refused every state-changing call, and is
//! handed a harness mode it cannot write files or push from.

mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use rk_ledger::Budget;
use rk_space::Space;
use serde_json::json;
use std::path::Path;
use std::process::Command;
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

/// Idles so the record stays live while the test drives its identity.
const IDLE_FAKE: &str = r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"diag-1"}'
sleep 30
"#;

#[tokio::test]
async fn diagnostician_is_confined_to_reading() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("f"), "x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    support::install_default_repository_policy(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", IDLE_FAKE);
    let layout = Layout::at(home.path());
    let daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "fake".into(),
        Budget::default(),
        Space::open_in_memory().unwrap(),
    )
    .unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut operator = connect(&layout).await;
    support::register_repo(&mut operator, repo_dir.path()).await;

    let spawned = operator
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "diag-1",
                "role": "diagnostician",
                "harness": "fake",
            }),
        )
        .await
        .expect("workflow dispatch must be able to spawn this role");
    let agent = &spawned["agent"];
    let name = agent["name"].as_str().unwrap().to_string();

    // Layer 1 — the harness cannot write files or push: the mode is forced by
    // role, not requested by the caller.
    assert_eq!(
        agent["permission_mode"].as_str(),
        Some("read-only"),
        "diagnostician must inherit the forced read-only harness mode"
    );

    // An ordinary branch and worktree, unlike the onboarder's session-scoped
    // pair — the confinement is the permission mode, not a special layout.
    assert!(agent["branch"].as_str().is_some_and(|b| !b.is_empty()));

    // Layer 2 — a genuine token for this agent still buys nothing mutating.
    let mut rat = Client::connect_as(&layout, &name).await.unwrap();
    for (method, params) in [
        ("agent.spawn", json!({"repo": "r", "task": "t"})),
        ("agent.dismiss", json!({"name": name})),
        ("agent.steer", json!({"name": name, "text": "go"})),
        ("repo.add", json!({"path": "/tmp"})),
        ("ticket.new", json!({"title": "t"})),
        ("workflow.run", json!({"name": "steward"})),
        (
            "space.out",
            json!({"scope": "r", "category": "artifact", "identity": "finding",
                   "payload": {"agent": name}}),
        ),
        (
            "space.out",
            json!({"scope": "r", "category": "claim", "identity": "src/",
                   "payload": {"agent": name}}),
        ),
    ] {
        let err = rat
            .call(method, params)
            .await
            .expect_err(&format!("{method} must be refused to a diagnostician"));
        assert!(
            err.to_string().contains("forbidden"),
            "{method} refused for the wrong reason: {err}"
        );
    }

    // What it CAN do: read, and declare its diagnosis via `rk done`.
    rat.call("space.scan", json!({"scope": "r", "category": "fact"}))
        .await
        .expect("a diagnostician must be able to read the tuplespace");
    rat.call("agent.status", json!({"name": name}))
        .await
        .expect("a diagnostician must be able to read agent status");
    rat.call(
        "space.out",
        json!({
            "scope": "r",
            "category": "event",
            "identity": "task_done",
            "payload": {"agent": name, "summary": "diagnosis"},
        }),
    )
    .await
    .expect("`rk done` is the diagnostician's one write");

    let _ = operator.call("agent.dismiss", json!({"name": name})).await;
}

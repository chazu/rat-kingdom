//! `control.verify` exercised through the real authenticated wire path: a
//! real spawned agent's own token, dispatched through `authorize_reasoned`
//! and `capabilities::method_policy`, not a direct in-process call to the
//! handler. A prior missing-RPC-grant regression (the queue-repair incident)
//! looked correct in a unit test that called the handler directly while
//! still being refused for every real caller, because the method was never
//! registered in `capabilities::method_policy` — this file is the guard
//! against that exact class of gap for `control.verify`.

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

/// Idles so the record stays live while the test drives its identity, and
/// swallows the steer's stdin line so the fake process doesn't die on a
/// closed pipe.
const IDLE_FAKE: &str = r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"cv-1"}'
read -r _steer
sleep 30
"#;

struct Fixture {
    layout: Layout,
    operator: Client,
    repo_path: String,
    _home: tempfile::TempDir,
    _repo_dir: tempfile::TempDir,
}

async fn setup() -> Fixture {
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
    tokio::spawn(daemon.run());
    let mut operator = connect(&layout).await;
    let repo_path = repo_dir.path().to_string_lossy().to_string();
    support::register_repo(&mut operator, repo_dir.path()).await;
    Fixture {
        layout,
        operator,
        repo_path,
        _home: home,
        _repo_dir: repo_dir,
    }
}

async fn spawn(fx: &mut Fixture, task: &str, role: &str) -> String {
    let spawned = fx
        .operator
        .call(
            "agent.spawn",
            json!({
                "repo": fx.repo_path,
                "task": task,
                "role": role,
                "harness": "fake",
            }),
        )
        .await
        .expect("spawn must succeed");
    spawned["agent"]["name"].as_str().unwrap().to_string()
}

/// The core wire-authenticated round trip: a real steer, a real spawned
/// agent's own token calling `control.verify` for itself, and the daemon's
/// answer proving the record — not a direct in-process call to the handler
/// (see this file's module doc for why that distinction is the point).
#[tokio::test]
async fn real_agent_verifies_its_own_genuine_steer() {
    let mut fx = setup().await;
    let name = spawn(&mut fx, "cv-1", "rat").await;

    let steered = fx
        .operator
        .call(
            "agent.steer",
            json!({"name": name, "message": "pause before rk done"}),
        )
        .await
        .expect("steer must succeed");
    let message_id = steered["message_id"].as_str().unwrap().to_string();

    let mut rat = Client::connect_as(&fx.layout, &name).await.unwrap();
    let result = rat
        .call("control.verify", json!({"message_id": message_id}))
        .await
        .expect("an ordinary rat must be granted control.verify");
    assert_eq!(result["verified"], json!(true));
    assert_eq!(result["text"], json!("pause before rk done"));
    assert_eq!(result["sender"], json!("operator"));

    let _ = fx
        .operator
        .call("agent.dismiss", json!({"name": name}))
        .await;
}

/// Repeat/concurrent verification of the same message must not multiply the
/// durable observed-record without bound — it is a re-confirmation, not a
/// new occurrence, and creates no new turn.
#[tokio::test]
async fn repeated_verification_is_idempotent() {
    let mut fx = setup().await;
    let name = spawn(&mut fx, "cv-2", "rat").await;
    let steered = fx
        .operator
        .call("agent.steer", json!({"name": name, "message": "continue"}))
        .await
        .unwrap();
    let message_id = steered["message_id"].as_str().unwrap().to_string();

    let mut rat = Client::connect_as(&fx.layout, &name).await.unwrap();
    for _ in 0..3 {
        let result = rat
            .call("control.verify", json!({"message_id": message_id.clone()}))
            .await
            .unwrap();
        assert_eq!(result["verified"], json!(true));
    }

    let observed = fx
        .operator
        .call(
            "space.scan",
            json!({"category": "event", "identity": "rk_control_observed"}),
        )
        .await
        .unwrap();
    let count = observed["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|t| t["payload"]["message_id"] == json!(message_id))
        .count();
    assert_eq!(
        count, 1,
        "three verifications of the same message must record one observation, not three"
    );

    let _ = fx
        .operator
        .call("agent.dismiss", json!({"name": name}))
        .await;
}

/// A genuine control message addressed to one agent must not verify for a
/// different agent, even though both are live, authenticated callers.
#[tokio::test]
async fn verification_is_refused_for_a_foreign_target() {
    let mut fx = setup().await;
    let a = spawn(&mut fx, "cv-3a", "rat").await;
    let b = spawn(&mut fx, "cv-3b", "rat").await;
    let steered = fx
        .operator
        .call("agent.steer", json!({"name": a, "message": "for A only"}))
        .await
        .unwrap();
    let message_id = steered["message_id"].as_str().unwrap().to_string();

    let mut rat_b = Client::connect_as(&fx.layout, &b).await.unwrap();
    let result = rat_b
        .call("control.verify", json!({"message_id": message_id}))
        .await
        .unwrap();
    assert_eq!(result["verified"], json!(false));
    assert_eq!(result["reason"], json!("not_found"));

    let _ = fx.operator.call("agent.dismiss", json!({"name": a})).await;
    let _ = fx.operator.call("agent.dismiss", json!({"name": b})).await;
}

/// An unknown message id (never enqueued, or a lookalike invented from
/// nothing) must fail cleanly rather than error.
#[tokio::test]
async fn unknown_message_id_is_not_found() {
    let mut fx = setup().await;
    let name = spawn(&mut fx, "cv-4", "rat").await;
    let mut rat = Client::connect_as(&fx.layout, &name).await.unwrap();
    let result = rat
        .call("control.verify", json!({"message_id": "never-existed"}))
        .await
        .unwrap();
    assert_eq!(result["verified"], json!(false));
    assert_eq!(result["reason"], json!("not_found"));
    let _ = fx
        .operator
        .call("agent.dismiss", json!({"name": name}))
        .await;
}

/// The exact "including intended restricted roles" case: a diagnostician —
/// confined from every mutating RPC — must still be able to verify a claimed
/// steer, since the lookup never touches its task, git, or ticket state.
#[tokio::test]
async fn restricted_read_only_role_can_still_verify() {
    let mut fx = setup().await;
    let name = spawn(&mut fx, "cv-5", "diagnostician").await;
    let steered = fx
        .operator
        .call("agent.steer", json!({"name": name, "message": "reassess"}))
        .await
        .unwrap();
    let message_id = steered["message_id"].as_str().unwrap().to_string();

    let mut diag = Client::connect_as(&fx.layout, &name).await.unwrap();
    let result = diag
        .call("control.verify", json!({"message_id": message_id}))
        .await
        .expect("a diagnostician must be granted control.verify despite being read-only");
    assert_eq!(result["verified"], json!(true));

    let _ = fx
        .operator
        .call("agent.dismiss", json!({"name": name}))
        .await;
}

/// The operator identity itself has no session generation to verify
/// against and must be refused distinctly from a real agent's failed
/// lookup.
#[tokio::test]
async fn operator_caller_is_refused() {
    let mut fx = setup().await;
    let err = fx
        .operator
        .call("control.verify", json!({"message_id": "whatever"}))
        .await
        .expect_err("operator has no session to verify against");
    assert!(err.to_string().contains("forbidden") || err.to_string().contains("spawned agent"));
}

//! TKT-lurin-bulif-gabik: the real `verify.run` RPC/CLI journey a caller who
//! loses this exact call's own stdout/stderr relies on — run a failing named
//! managed check for real (a genuine subprocess, not a fake harness), then
//! retrieve the exact bounded failure diagnostic later, from a completely
//! independent reader of the durable store, without rerunning the check.
//!
//! Closest existing templates: `verification_saturation_regression.rs`
//! (daemon-spinning `verify.run` fixture) and `host_verification_aggregate_cap.rs`.

mod support;

use rk_core::paths::Layout;
use rk_core::tuple::{Category, Pattern};
use rk_daemon::Daemon;
use rk_ledger::Budget;
use rk_space::Space;
use serde_json::json;
use std::path::Path;
use std::process::Command;

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
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn init_repo(dir: &Path) -> String {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    support::install_default_repository_policy(dir);
    dir.file_name().unwrap().to_string_lossy().to_string()
}

/// A caller runs a genuinely failing named check over the real `verify.run`
/// RPC, discards this call's own response entirely (simulating a caller
/// whose shell wrapper swallowed it), then a completely separate reader —
/// a fresh [`Space::open`] handle over the SAME on-disk store, standing in
/// for a reconnected caller or a different process entirely — retrieves the
/// exact bounded diagnostic without rerunning the check.
#[tokio::test]
async fn a_failing_named_check_leaves_a_receipt_a_reconnected_reader_can_retrieve_without_rerunning(
) {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_name = init_repo(repo_dir.path());
    std::fs::write(
        repo_dir.path().join(".rk/checks.cue"),
        r#"checks: [{name: "verify",
        command: "echo real-stdout-marker; echo real-stderr-marker 1>&2; exit 3",
        timeout: "30s", environmentPolicy: "strip_rk_spawn", sharedCargoTarget: false}]"#,
    )
    .unwrap();

    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = Space::open(&layout.db_path()).unwrap();
    let daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "fake".into(),
        Budget::default(),
        space,
    )
    .unwrap();
    tokio::spawn(daemon.run());
    let mut client = support::connect(&layout).await;
    client
        .call(
            "repo.add",
            json!({"name": &repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    let result = client
        .call("verify.run", json!({"repo": &repo_name, "check": "verify"}))
        .await
        .unwrap();
    assert_eq!(result["verdict"], "fail");
    assert_eq!(result["exit"], 3);
    let receipt_id = result["failure_receipt_id"]
        .as_str()
        .expect("a real failing verify.run must expose a failure_receipt_id")
        .to_string();

    // The caller's own connection and its in-memory `result` are gone now —
    // deliberately dropped, never consulted again below.
    drop(client);
    drop(result);

    // A genuinely independent reader: its own fresh `Space` handle opened
    // against the same on-disk sqlite file, not the daemon's in-process
    // `Arc`. Standing in for a reconnected caller (or an entirely different
    // process) reading durable evidence cold.
    let reconnected = Space::open(&layout.db_path()).unwrap();
    let tuples = reconnected
        .scan(
            &Pattern::category(Category::Artifact)
                .identity("verification-failure-receipt")
                .scope(&repo_name),
        )
        .unwrap();
    assert_eq!(tuples.len(), 1, "exactly one receipt for the one real run");
    let receipt = &tuples[0];
    assert_eq!(receipt.id.to_string(), receipt_id);
    assert_eq!(receipt.payload["exit"], 3);
    assert_eq!(receipt.payload["verdict"], "fail");
    assert!(receipt.payload["stdout_tail"]
        .as_str()
        .unwrap()
        .contains("real-stdout-marker"));
    assert!(receipt.payload["stderr_tail"]
        .as_str()
        .unwrap()
        .contains("real-stderr-marker"));

    // The very candidate sha this real run tested must still have NO
    // reusable pass proof — a failure receipt existing must never satisfy a
    // later caller's proof lookup for the same candidate/check.
    let candidate_sha = git(repo_dir.path(), &["rev-parse", "HEAD"]);
    let proofs = reconnected
        .scan(
            &Pattern::category(Category::Event)
                .identity("verification_proof")
                .scope(&repo_name),
        )
        .unwrap();
    assert!(
        proofs.is_empty(),
        "a failing run must never write a reusable pass proof for candidate {candidate_sha}"
    );
}

//! RPC-to-adapter proof for the Maki lifecycle/safety-policy integration
//! (TKT-zazif-zavos-gamur): an ordinary worker's spawn reaches the real
//! `maki` adapter with the daemon's full-access permission translation and
//! the adapter's own isolation flags intact, a self-reported zero cost never
//! overwrites the pricing-based estimate the daemon accumulated from usage,
//! and a restricted role is rejected before any durable spawn side effect.
//!
//! `crates/rk-harness/src/maki.rs`'s own unit tests already prove the
//! adapter's argv/parsing/caps in isolation; this file proves the daemon
//! actually drives that adapter the same way in practice, over the real RPC
//! surface `rk` uses.

mod fixture;
mod support;

use rk_core::paths::Layout;
use rk_daemon::Daemon;
use serde_json::json;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;
use std::time::Duration;
use support::connect;

/// `RK_MAKI_BIN` is process-global (`std::env::set_var`), and cargo runs this
/// file's `#[tokio::test]`s concurrently within one process by default.
static ENV_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

async fn env_lock() -> tokio::sync::MutexGuard<'static, ()> {
    ENV_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

fn git(dir: &Path, args: &[&str]) {
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
}

fn init_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "rat@example.test"]);
    git(dir, &["config", "user.name", "Rat"]);
    std::fs::write(dir.join("README.md"), "# maki lifecycle\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    support::install_default_repository_policy(dir);
}

/// A fake Maki binary: captures its exact argv (NUL-separated) to
/// `args_file`, reads and discards the stream-json initial-prompt line
/// delivered on stdin, does real git work, declares done, and reports the
/// same `total_cost_usd: 0` Maki emits for an unpriced/OAuth-backed turn
/// alongside nonzero token usage (see `maki::parse_event_line`).
fn install_maki(bin: &Path, args_file: &Path) {
    let body = fixture::with_rk_done(&format!(
        r#"printf '%s\036' "$@" > "{args_file}"
IFS= read -r _first_line
echo '{{"type":"system","subtype":"init","session_id":"maki-e2e"}}'
echo "gnawed by $RK_AGENT for task $RK_TASK" > gnawed.txt
git add gnawed.txt >/dev/null 2>&1
git -c user.email=rat@x -c user.name=Rat commit -q -m "rat work: $RK_TASK" >/dev/null
echo '{{"type":"assistant","message":{{"role":"assistant","content":[{{"type":"text","text":"gnawing"}}],"usage":{{"input_tokens":10000,"output_tokens":5000,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}}}'
rk_done "work done"
echo '{{"type":"result","subtype":"success","is_error":false,"result":"committed gnawed.txt","session_id":"maki-e2e","total_cost_usd":0.0,"usage":{{"input_tokens":10000,"output_tokens":5000,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}'
"#,
        args_file = args_file.display(),
    ));
    std::fs::write(bin, format!("#!/bin/sh\n{body}\n")).unwrap();
    std::fs::set_permissions(bin, std::fs::Permissions::from_mode(0o755)).unwrap();
}

fn read_argv(args_file: &Path) -> Vec<String> {
    std::fs::read(args_file)
        .unwrap()
        .split(|byte| *byte == 0x1e)
        .filter(|arg| !arg.is_empty())
        .map(|arg| String::from_utf8(arg.to_vec()).unwrap())
        .collect()
}

#[tokio::test]
async fn maki_ordinary_worker_spawns_isolated_full_access_and_completes_with_pricing_based_cost() {
    let _env_lock = env_lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let bin_dir = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let binary = bin_dir.path().join("maki-fake");
    let args_file = bin_dir.path().join("args");
    install_maki(&binary, &args_file);
    std::env::set_var("RK_MAKI_BIN", &binary);

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _daemon = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo.path()).await;

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo.path().to_string_lossy(),
                "task": "maki-e2e",
                "harness": "maki",
                "model": "haiku",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();
    assert_eq!(spawned["agent"]["harness"], "maki");
    assert_eq!(spawned["agent"]["permission_mode"], "danger-full-access");

    let mut completed = None;
    for _ in 0..200 {
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if status["agent"]["state"] == "completed" {
            completed = Some(status["agent"].clone());
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let completed = completed.expect("maki agent never completed");
    assert_eq!(completed["result"], "committed gnawed.txt");
    assert_eq!(completed["session_id"], "maki-e2e");
    // Maki's self-reported `total_cost_usd: 0` for this turn must never
    // overwrite the daemon's own pricing-table estimate accumulated from the
    // reported usage — a real zero here would silently defeat a USD cap.
    assert!(
        completed["cost_usd"].as_f64().unwrap() > 0.0,
        "zero self-reported cost must not overwrite the pricing-based estimate: {completed}"
    );

    let argv = read_argv(&args_file);
    for flag in [
        "--no-plugins",
        "--no-commands",
        "--dangerously-skip-permissions",
    ] {
        assert!(
            argv.contains(&flag.to_string()),
            "missing {flag} in {argv:?}"
        );
    }
    let disallowed_idx = argv
        .iter()
        .position(|a| a == "--disallowed-tools")
        .expect("--disallowed-tools must be present");
    assert_eq!(
        argv[disallowed_idx + 1],
        "Task,Memory",
        "native Task/Memory tools must be denied on every launch"
    );

    let _ = client.call("stop", json!({})).await;
    std::env::remove_var("RK_MAKI_BIN");
}

#[tokio::test]
async fn restricted_role_maki_spawn_is_rejected_before_any_durable_side_effect() {
    let _env_lock = env_lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _daemon = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo.path()).await;

    let branches_before = git_branches(repo.path());

    let error = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo.path().to_string_lossy(),
                "task": "maki-onboard",
                "role": "onboarder",
                "harness": "maki",
                "base": "main",
            }),
        )
        .await
        .expect_err("maki has no enforced read-only mode yet and must fail closed");
    assert!(error.to_string().contains("no enforced read-only mode"));

    // No worktree/branch/registry row from a rejected pairing: the harness is
    // resolved and validated before any of that is created.
    assert_eq!(git_branches(repo.path()), branches_before);
    let agents = client.call("agent.list", json!({})).await.unwrap();
    assert!(
        agents["agents"].as_array().unwrap().is_empty(),
        "a rejected spawn must leave no durable agent record: {agents}"
    );

    let _ = client.call("stop", json!({})).await;
}

fn git_branches(dir: &Path) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["branch", "--list"])
        .output()
        .unwrap();
    String::from_utf8_lossy(&output.stdout).to_string()
}

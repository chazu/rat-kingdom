//! TKT-hisag-nubaf-kugon REWORK finding #3: a bounded, REAL native
//! completion -> automatic reactor `action: "land"` enqueue -> gate journey
//! through the live daemon, including a genuinely failing gate that blocks
//! target advancement. No `repo.land` RPC appears anywhere in this test —
//! peer review flagged that a manual `repo.land` call is not proof of
//! automatic completion routing (BBS artifact 01M2JBWE1XG3EDJ79RWKNP22EN),
//! and `landing_need_retirement.rs`'s existing failing-gate coverage goes
//! through exactly that manual call. This test instead spawns a real "rat"
//! (`agent.spawn`) and lets the reactor's own trigger dispatch the
//! completion, mirroring `live_landing_burst.rs`'s happy-path fixture but
//! with a check that always fails.

mod fixture;
mod support;

use rk_core::paths::Layout;
use rk_daemon::Daemon;
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

/// Same repo-local completion feed `live_landing_burst.rs` and
/// `landing_rework_autodispatch.rs` install: the reactor resolves it exactly
/// like the global trigger directory (`Reactor::trigger_files`).
const LANDING_TRIGGER: &str = r#"
triggers: [
	{
		name:   "landing-on-completion"
		action: "land"
		match: {category: "event", identity: "harness_result", search: "\"role\":\"rat\""}
		maxFires: 20
	},
]
"#;

const FAILING_GATE_FAKE: &str = r#"
read -r _prompt
echo "candidate work" > candidate.txt
git add candidate.txt >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "feat: candidate"
echo '{"type":"system","subtype":"init","session_id":"gate-fail-fake"}'
rk_done "candidate complete"
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"gate-fail-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

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

/// Real, content-independent failing check (`exit 1`) — not a synthetic
/// tuple standing in for a gate failure. `landing-protected-paths`/
/// `landing-diff-scope` pass so the failure is unambiguously `verify`'s.
fn init_repo_with_failing_gate_and_land_trigger(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    support::install_default_repository_policy(dir);
    let rk_dir = dir.join(".rk");
    std::fs::write(
        rk_dir.join("checks.cue"),
        r#"checks: [
    {name: "landing-protected-paths", command: "true", timeout: "30s"},
    {name: "landing-diff-scope", command: "true", timeout: "30s"},
    {name: "verify", command: "exit 1", timeout: "30s"},
]
"#,
    )
    .unwrap();
    std::fs::write(rk_dir.join("triggers.cue"), LANDING_TRIGGER).unwrap();
    git(dir, &["add", ".rk/checks.cue", ".rk/triggers.cue"]);
    git(
        dir,
        &[
            "commit",
            "-m",
            "test: register a real failing gate and a landing trigger",
        ],
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn automatic_completion_enqueues_and_a_failing_gate_blocks_advancement() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path();
    init_repo_with_failing_gate_and_land_trigger(repo);
    let repo_name = repo.file_name().unwrap().to_string_lossy().to_string();
    let main_before = git(repo, &["rev-parse", "main"]);

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "gate-fail-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo).await;

    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        fixture::with_rk_done(FAILING_GATE_FAKE),
    );

    // A plain agent.spawn — no repo.land call anywhere in this test. The rat
    // completes, the reactor's own `action: "land"` trigger fires on the
    // resulting `harness_result`, and the daemon's own landing-pipeline
    // consumer loop (server.rs) drains the queue and runs the real (failing)
    // gate — the exact automatic path finding #3 asked to be proven.
    let spawned = client
        .call(
            "agent.spawn",
            json!({"repo": repo.to_string_lossy(), "task": "gate-fail-1", "harness": "fake"}),
        )
        .await
        .unwrap();
    let branch = spawned["agent"]["branch"].as_str().unwrap().to_string();

    let mut enqueued = false;
    for _ in 0..400 {
        let queue = client
            .call(
                "space.scan",
                json!({"category": "event", "identity": "landing_queue_entry", "scope": repo_name}),
            )
            .await
            .unwrap();
        if queue["tuples"].as_array().unwrap().iter().any(|t| {
            t["payload"]["branch"] == json!(branch) && t["payload"]["target"] == json!("main")
        }) {
            enqueued = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        enqueued,
        "the reactor never automatically enqueued the completion onto the landing queue"
    );

    // Wait for the queue to drain (the gate ran to completion) and the real
    // durable landing Need a genuine gate failure produces (the same signal
    // `landing_need_retirement.rs` reads off a manual `repo.land` call) to
    // appear — never fabricated by this test.
    let mut needs = Vec::new();
    for _ in 0..400 {
        let scanned = client
            .call(
                "space.scan",
                json!({"category": "need", "scope": repo_name, "identity": "landing"}),
            )
            .await
            .unwrap();
        needs = scanned["tuples"].as_array().unwrap().clone();
        let queue = client
            .call(
                "space.scan",
                json!({"category": "event", "identity": "landing_queue_entry", "scope": repo_name}),
            )
            .await
            .unwrap();
        if !needs.is_empty() && queue["tuples"].as_array().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        needs.len(),
        1,
        "a real failing gate reached automatically must produce exactly one \
         landing Need: {needs:?}"
    );
    assert!(
        needs[0]["payload"]["text"]
            .as_str()
            .unwrap_or_default()
            .contains("run gate FAILED"),
        "the automatically-produced Need must record the real gate failure: {needs:?}"
    );

    let main_after = git(repo, &["rev-parse", "main"]);
    assert_eq!(
        main_before, main_after,
        "a failing automatic gate must never advance the target branch"
    );

    handle.abort();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

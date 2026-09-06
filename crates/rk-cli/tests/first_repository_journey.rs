//! The first-repository guide's CLI path against a disposable daemon and repo.
//! Only the coding provider is replaced; inspection, approvals, activation,
//! task completion, delivery gates and cleanup use production CLI/RPC code.
use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::Value;
use std::path::Path;
use std::time::Duration;

const POLICY: &str = include_str!("../../../examples/first-repo/repo.cue");
const CHECKS: &str = include_str!("../../../examples/first-repo/checks.cue");
const FAKE: &str = r#"
set -eu
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"first-repository"}'
if [ "$RK_ROLE" = rat ]; then
    printf '\nVerified by Rat Kingdom.\n' >> README.md
    git add README.md
    git -c user.email=rat@example.com -c user.name=Rat commit -qm 'docs: verify first repository'
    "$RK_TEST_RK_BIN" done 'Added the requested README sentence' >/dev/null
fi
echo '{"type":"result","subtype":"success","is_error":false,"result":"complete","session_id":"first-repository","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5}}'
"#;

#[test]
fn first_repository_gates_enforce_line_budget_and_protected_paths() {
    let checks = rk_workflow::load_checks_str(CHECKS).unwrap();
    for (path, lines, scope_pass, protected_pass) in [
        ("README.md", 20, true, true),
        ("README.md", 21, false, true),
        (".rk/settings", 1, false, false),
        ("other.txt", 1, false, true),
    ] {
        let repo = tempfile::tempdir().unwrap();
        git(repo.path(), &["init", "-b", "main"]);
        git(repo.path(), &["config", "user.name", "Gate Test"]);
        git(repo.path(), &["config", "user.email", "gate@example.com"]);
        std::fs::write(repo.path().join("README.md"), "# First repository\n").unwrap();
        git(repo.path(), &["add", "README.md"]);
        git(repo.path(), &["commit", "-m", "base"]);
        git(repo.path(), &["checkout", "-b", "candidate"]);
        let destination = repo.path().join(path);
        std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
        let mut content = std::fs::read_to_string(&destination).unwrap_or_default();
        content.push_str(&"added line\n".repeat(lines));
        std::fs::write(&destination, content).unwrap();
        git(repo.path(), &["add", path]);
        git(repo.path(), &["commit", "-m", "candidate"]);
        for (name, expected) in [
            ("steward-diff-scope", scope_pass),
            ("steward-protected-paths", protected_pass),
        ] {
            let check = checks.iter().find(|check| check.name == name).unwrap();
            let output = std::process::Command::new("sh")
                .args(["-c", &check.command])
                .current_dir(repo.path())
                .env("RK_CHECK_TARGET", "main")
                .env("RK_CHECK_MAX_DIFF_FILES", "1")
                .env("RK_CHECK_MAX_DIFF_LINES", "20")
                .env("RK_CHECK_PROTECTED_PATHS", r"(^|/)(\.github|\.rk)/")
                .output()
                .unwrap();
            assert_eq!(
                output.status.success(),
                expected,
                "{name}: {path}, {lines} added lines: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}

fn git(repo: &Path, args: &[&str]) -> String {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().into()
}

async fn cli(layout: &Layout, repo: &Path, args: &[&str], success: bool) -> Value {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_rk"));
    command
        .arg("--json")
        .args(args)
        .current_dir(repo)
        .env("RK_HOME", layout.home());
    for name in [
        "RK_AGENT",
        "RK_AUTH_TOKEN",
        "RK_TASK",
        "RK_REPO",
        "RK_ROLE",
        "RK_BRANCH",
        "RK_WORKTREE",
        "RK_SPAWN",
    ] {
        command.env_remove(name);
    }
    let out = tokio::time::timeout(Duration::from_secs(20), command.output())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        out.status.success(),
        success,
        "rk {args:?}: {}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap_or(Value::Null)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_repository_reaches_gated_delivery_and_retained_cleanup_evidence() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let remote = tempfile::tempdir().unwrap();
    git(remote.path(), &["init", "--bare"]);
    git(repo.path(), &["init", "-b", "main"]);
    git(
        repo.path(),
        &["remote", "add", "origin", remote.path().to_str().unwrap()],
    );
    git(repo.path(), &["config", "user.name", "First Repository"]);
    git(repo.path(), &["config", "user.email", "first@example.com"]);
    std::fs::write(repo.path().join("README.md"), "# First repository\n").unwrap();
    std::fs::create_dir(repo.path().join(".rk")).unwrap();
    std::fs::write(repo.path().join(".rk/checks.cue"), CHECKS).unwrap();
    git(repo.path(), &["add", "README.md", ".rk/checks.cue"]);
    git(
        repo.path(),
        &[
            "commit",
            "-m",
            "Initialize first repository and named checks",
        ],
    );
    std::env::set_var("RK_FAKE_HARNESS_CMD", FAKE);
    std::env::set_var("RK_TEST_RK_BIN", env!("CARGO_BIN_EXE_rk"));
    let layout = Layout::at(home.path());
    let mut config = rk_core::config::Config::default();
    config.harness.default = "fake".into();
    let daemon = Daemon::new(layout.clone(), &config).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut connected = false;
    for _ in 0..100 {
        if Client::connect_as_operator(&layout).await.is_ok() {
            connected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(connected);
    cli(&layout, repo.path(), &["ping"], true).await;
    cli(
        &layout,
        repo.path(),
        &[
            "repo",
            "add",
            repo.path().to_str().unwrap(),
            "--name",
            "first-repo",
        ],
        true,
    )
    .await;
    let initial_head = git(repo.path(), &["rev-parse", "HEAD"]);
    let before = cli(
        &layout,
        repo.path(),
        &["repo", "onboard", "inspect", "first-repo"],
        true,
    )
    .await;
    assert_eq!(before["ready"], true);
    assert!(before["findings"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["kind"] == "repository_policy_missing"));
    assert_eq!(git(repo.path(), &["rev-parse", "HEAD"]), initial_head);
    assert!(git(repo.path(), &["status", "--porcelain"]).is_empty());

    let started = cli(
        &layout,
        repo.path(),
        &[
            "repo",
            "onboard",
            "start",
            "first-repo",
            "--harness",
            "fake",
        ],
        true,
    )
    .await;
    let session = started["session"]["id"].as_str().unwrap();
    let worktree = started["session"]["worktree"].as_str().unwrap();
    let diff = format!("diff --git a/.rk/repo.cue b/.rk/repo.cue\nnew file mode 100644\n--- /dev/null\n+++ b/.rk/repo.cue\n@@ -0,0 +1,{} @@\n{}",
        POLICY.lines().count(), POLICY.lines().map(|line| format!("+{line}\n")).collect::<String>());
    let proposed = cli(
        &layout,
        repo.path(),
        &[
            "repo",
            "onboard",
            "propose",
            session,
            "--kind",
            "repo_file",
            "--title",
            "First repository: local gated delivery",
            "--evidence",
            "Reviewed main target, local merge, isolated worktrees and source cleanup",
            "--target",
            ".rk/repo.cue",
            "--action",
            "write_repo_file",
            "--diff",
            &diff,
            "--risk",
            "high",
            "--verification",
            "Validate repository policy and exact activation",
        ],
        true,
    )
    .await;
    let proposal = proposed["proposal"]["id"].as_str().unwrap();
    let digest = proposed["proposal"]["digest"].as_str().unwrap();
    cli(
        &layout,
        repo.path(),
        &[
            "repo", "onboard", "apply", session, proposal, "--digest", digest,
        ],
        false,
    )
    .await;
    cli(
        &layout,
        repo.path(),
        &[
            "repo", "onboard", "approve", session, proposal, "--digest", digest,
        ],
        true,
    )
    .await;
    cli(
        &layout,
        repo.path(),
        &[
            "repo", "onboard", "apply", session, proposal, "--digest", digest,
        ],
        true,
    )
    .await;
    assert!(
        !repo.path().join(".rk/repo.cue").exists(),
        "apply must preserve the registered checkout"
    );
    cli(
        &layout,
        repo.path(),
        &[
            "repo", "onboard", "activate", session, proposal, "--digest", digest,
        ],
        true,
    )
    .await;
    assert_eq!(
        std::fs::read_to_string(repo.path().join(".rk/repo.cue")).unwrap(),
        POLICY
    );
    let ready = cli(
        &layout,
        repo.path(),
        &["repo", "onboard", "inspect", "first-repo"],
        true,
    )
    .await;
    assert_eq!(ready["ready"], true);
    cli(
        &layout,
        repo.path(),
        &["verify", "--repo", "first-repo"],
        true,
    )
    .await;

    let ticket = cli(&layout, repo.path(), &["ticket", "new", "Add the first README sentence", "--repo", "first-repo",
        "--body", "Append Verified by Rat Kingdom. to README.md, change no other file, commit, run rk done."], true).await;
    let ticket = ticket["identity"].as_str().unwrap();
    let spawned = cli(
        &layout,
        repo.path(),
        &["spawn", "--ticket", ticket, "--harness", "fake"],
        true,
    )
    .await;
    let agent = spawned["name"].as_str().unwrap();
    let branch = spawned["branch"].as_str().unwrap();
    let agent_worktree = spawned["worktree"].as_str().unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let status = cli(&layout, repo.path(), &["status", agent], true).await;
            if status["state"] == "completed" {
                break;
            }
            assert_ne!(status["state"], "failed", "{status}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    cli(&layout, repo.path(), &["dismiss", agent], true).await;
    let landed = cli(
        &layout,
        repo.path(),
        &[
            "land",
            branch,
            "--repo",
            "first-repo",
            "--target",
            "main",
            "--task",
            ticket,
        ],
        true,
    )
    .await;
    assert_eq!(landed["merged"], true, "{landed}");
    let delivery = cli(&layout, repo.path(), &["ticket", "show", ticket], true).await;
    assert!(
        delivery["payload"]["delivery"]["merge_commit"].is_string(),
        "{delivery}"
    );
    let delivered_sha = &delivery["payload"]["delivery"]["merge_commit"];
    assert_eq!(*delivered_sha, git(repo.path(), &["rev-parse", "main"]));
    let mut client = Client::connect_as_operator(&layout).await.unwrap();
    let proofs = client
        .call(
            "space.scan",
            serde_json::json!({"category": "event", "scope": "first-repo", "identity": "landing_gate_pass"}),
        )
        .await
        .unwrap();
    let proof = proofs["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .find(|proof| proof["payload"]["candidate_sha"] == *delivered_sha)
        .expect("the delivered commit must have its own gate-pass evidence");
    let checks = proof["payload"]["checks"].as_array().unwrap();
    for check in ["verify", "steward-protected-paths", "steward-diff-scope"] {
        assert!(checks.iter().any(|name| name == check), "{proof}");
    }
    assert!(std::fs::read_to_string(repo.path().join("README.md"))
        .unwrap()
        .contains("Verified by Rat Kingdom."));
    assert!(!Path::new(agent_worktree).exists());
    assert!(git(repo.path(), &["branch", "--list", branch]).is_empty());
    let work = cli(&layout, repo.path(), &["work", "first-repo"], true).await;
    assert!(work["stalled"].as_array().unwrap().is_empty(), "{work}");
    let cleanup = cli(
        &layout,
        repo.path(),
        &["repo", "onboard", "cleanup", session],
        true,
    )
    .await;
    assert_eq!(cleanup["cleaned"], true);
    assert!(!Path::new(worktree).exists());
    let report = cli(
        &layout,
        repo.path(),
        &["repo", "onboard", "report", session],
        true,
    )
    .await;
    assert!(!report.is_null());
    assert!(git(repo.path(), &["status", "--porcelain"]).is_empty());
    let mut client = Client::connect_as_operator(&layout).await.unwrap();
    client.call("stop", serde_json::json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

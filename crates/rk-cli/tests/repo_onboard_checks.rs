use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

const COMPLETE: &str = r#"
echo '{"type":"system","subtype":"init","session_id":"onboarding-checks"}'
read -r _first_message
echo '{"type":"result","subtype":"success","is_error":false,"result":"assessment complete","session_id":"onboarding-checks","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5}}'
"#;

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
        "# Fixture\n\nThe documented verification runner is the `verify` named check.\n",
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

fn checks_source(command: &str) -> String {
    format!(
        "checks: [\n\t{{\n\t\tname: \"verify\"\n\t\tcommand: {}\n\t\tcwd: \".\"\n\t\texpectExit: 0\n\t\ttimeout: \"30s\"\n\t\tenvironmentPolicy: \"strip_rk_spawn\"\n\t\ttoolchain: \"fixture POSIX sh\"\n\t}},\n]\n",
        serde_json::to_string(command).unwrap()
    )
}

fn new_file_diff(source: &str) -> String {
    let body = source
        .lines()
        .map(|line| format!("+{line}\n"))
        .collect::<String>();
    format!(
        "diff --git a/.rk/checks.cue b/.rk/checks.cue\nnew file mode 100644\n--- /dev/null\n+++ b/.rk/checks.cue\n@@ -0,0 +1,{} @@\n{body}",
        source.lines().count()
    )
}

fn proposal_draft(command: &str, source: &str) -> Value {
    json!({
        "kind": "repo_file",
        "title": "Add the documented verify check",
        "evidence": ["README names the verify runner"],
        "target_path": ".rk/checks.cue",
        "action": "write_repo_file",
        "diff": new_file_diff(source),
        "risk": "low",
        "verification": ["check:verify"],
        "named_check": {
            "name": "verify",
            "command": command,
            "cwd": ".",
            "expect_exit": 0,
            "timeout": "30s",
            "environment_policy": "strip_rk_spawn",
            "toolchain": "fixture POSIX sh",
        },
    })
}

struct Fixture {
    _home: tempfile::TempDir,
    repo: tempfile::TempDir,
    handle: tokio::task::JoinHandle<rk_core::Result<()>>,
    client: Client,
    session: String,
    worktree: PathBuf,
}

impl Fixture {
    async fn start() -> Self {
        let home = tempfile::tempdir().unwrap();
        let repo = repository();
        let layout = Layout::at(home.path());
        let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
        let handle = tokio::spawn(daemon.run());
        let mut client = connect(&layout).await;
        let started = client
            .call(
                "repo.onboard.start",
                json!({"target": repo.path(), "harness": "fake"}),
            )
            .await
            .unwrap();
        Self {
            _home: home,
            repo,
            handle,
            client,
            session: started["session"]["id"].as_str().unwrap().to_string(),
            worktree: PathBuf::from(started["session"]["worktree"].as_str().unwrap()),
        }
    }

    async fn propose_and_approve(&mut self, command: &str, source: &str) -> Value {
        let proposed = self
            .client
            .call(
                "repo.onboard.propose",
                json!({
                    "session": self.session,
                    "proposal": proposal_draft(command, source),
                }),
            )
            .await
            .unwrap()["proposal"]
            .clone();
        self.client
            .call(
                "repo.onboard.approve",
                json!({
                    "session": self.session,
                    "proposal": proposed["id"],
                    "digest": proposed["digest"],
                }),
            )
            .await
            .unwrap();
        proposed
    }

    async fn apply(&mut self, proposal: &Value) -> rk_core::Result<Value> {
        self.client
            .call(
                "repo.onboard.apply",
                json!({
                    "session": self.session,
                    "proposal": proposal["id"],
                    "digest": proposal["digest"],
                }),
            )
            .await
    }

    async fn proposal(&mut self, proposal: &Value) -> Value {
        let status = self
            .client
            .call("repo.onboard.status", json!({"session": self.session}))
            .await
            .unwrap();
        status["session"]["proposals"]
            .as_array()
            .unwrap()
            .iter()
            .find(|candidate| candidate["id"] == proposal["id"])
            .unwrap()
            .clone()
    }

    async fn stop(mut self) {
        self.client.call("stop", json!({})).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), self.handle)
            .await
            .expect("daemon did not stop")
            .unwrap()
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn approved_checks_apply_verify_fail_closed_and_recover_without_drift() {
    std::env::set_var("RK_FAKE_HARNESS_CMD", COMPLETE);

    // Valid: apply only in the owned onboarding worktree, validate through CUE,
    // execute with RK identity stripped, and report the exact contract.
    let mut valid = Fixture::start().await;
    let human_head = git(valid.repo.path(), &["rev-parse", "HEAD"]);
    let command = "test -z \"${RK_AGENT:-}\" && printf 'fixture passed\\n'";
    let source = checks_source(command);
    let proposal = valid.propose_and_approve(command, &source).await;
    let applied = valid.apply(&proposal).await.unwrap();
    assert_eq!(applied["proposal"]["status"], "verified");
    assert_eq!(
        applied["proposal"]["application"]["commit"]
            .as_str()
            .unwrap()
            .len(),
        40
    );
    let result = &applied["proposal"]["verification_results"][0];
    assert_eq!(result["command"], command);
    assert_eq!(result["cwd"], ".");
    assert_eq!(result["expected_exit"], 0);
    assert_eq!(result["timeout"], "30s");
    assert_eq!(result["environment_policy"], "strip_rk_spawn");
    assert_eq!(result["toolchain"], "fixture POSIX sh");
    assert_eq!(result["exit_status"], 0);
    assert!(result["output_summary"]
        .as_str()
        .unwrap()
        .contains("fixture passed"));
    assert_eq!(result["unresolved_risks"], json!([]));
    assert!(!valid.repo.path().join(".rk/checks.cue").exists());
    assert_eq!(git(valid.repo.path(), &["rev-parse", "HEAD"]), human_head);
    assert!(valid.worktree.join(".rk/checks.cue").exists());

    let replay = valid.apply(&proposal).await.unwrap();
    assert_eq!(replay["proposal"]["status"], "verified");
    assert_eq!(
        replay["proposal"]["verification_results"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "a clean replay must neither recommit nor rerun"
    );
    assert_eq!(git(&valid.worktree, &["rev-list", "--count", "HEAD"]), "2");
    let report = valid
        .client
        .call("repo.onboard.report", json!({"session": valid.session}))
        .await
        .unwrap();
    assert_eq!(report["report"]["proposals"][0]["status"], "verified");
    valid.stop().await;

    // Malformed: the exact patch remains recoverable in the owned worktree,
    // while the durable proposal fails before a commit or command execution.
    let mut malformed = Fixture::start().await;
    let malformed_source = "checks: [{name: \"verify\", command: ]\n";
    let malformed_proposal = malformed
        .propose_and_approve("printf never", malformed_source)
        .await;
    assert!(malformed.apply(&malformed_proposal).await.is_err());
    let failed = malformed.proposal(&malformed_proposal).await;
    assert_eq!(failed["status"], "failed");
    assert!(
        failed["failure"]
            .as_str()
            .unwrap()
            .contains("cue export failed"),
        "{failed}"
    );
    assert!(failed["application"].is_null());
    assert!(malformed.worktree.join(".rk/checks.cue").exists());
    assert!(malformed.apply(&malformed_proposal).await.is_err());
    assert_eq!(
        git(&malformed.worktree, &["rev-list", "--count", "HEAD"]),
        "1",
        "retry must not duplicate the interrupted patch"
    );
    malformed.stop().await;

    // Command failure: the application commit is durable. Once the external
    // prerequisite appears, retry reuses that commit and only reruns the check.
    let mut failing = Fixture::start().await;
    let marker = failing.repo.path().join("external-ready");
    let command = format!("test -f {}", marker.display());
    let source = checks_source(&command);
    let failing_proposal = failing.propose_and_approve(&command, &source).await;
    let first = failing.apply(&failing_proposal).await.unwrap();
    assert_eq!(first["proposal"]["status"], "failed");
    assert_eq!(
        first["proposal"]["verification_results"][0]["exit_status"],
        1
    );
    let application_commit = first["proposal"]["application"]["commit"]
        .as_str()
        .unwrap()
        .to_string();
    std::fs::write(&marker, "ready\n").unwrap();
    let recovered = failing.apply(&failing_proposal).await.unwrap();
    assert_eq!(recovered["proposal"]["status"], "verified");
    assert_eq!(
        recovered["proposal"]["application"]["commit"],
        application_commit
    );
    assert_eq!(
        recovered["proposal"]["verification_results"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        git(&failing.worktree, &["rev-list", "--count", "HEAD"]),
        "2"
    );
    failing.stop().await;

    // Dirty worktree: unrelated state is never swept into the proposal commit.
    // Cleaning it permits a retry of the same immutable proposal.
    let mut dirty = Fixture::start().await;
    let command = "printf 'clean retry\\n'";
    let source = checks_source(command);
    let dirty_proposal = dirty.propose_and_approve(command, &source).await;
    let unrelated = dirty.worktree.join("UNRELATED");
    std::fs::write(&unrelated, "human work\n").unwrap();
    let error = dirty.apply(&dirty_proposal).await.unwrap_err().to_string();
    assert!(error.contains("observed UNRELATED"), "{error}");
    assert!(!dirty.worktree.join(".rk/checks.cue").exists());
    std::fs::remove_file(unrelated).unwrap();
    let recovered = dirty.apply(&dirty_proposal).await.unwrap();
    assert_eq!(recovered["proposal"]["status"], "verified");
    dirty.stop().await;

    // Rerun drift: a verified proposal is still checked against its recorded
    // commit/content before an idempotent response. Restoring the exact tree
    // makes the failed proposal recoverable without another application commit.
    let mut drift = Fixture::start().await;
    let command = "printf 'drift proof\\n'";
    let source = checks_source(command);
    let drift_proposal = drift.propose_and_approve(command, &source).await;
    let first = drift.apply(&drift_proposal).await.unwrap();
    let commit = first["proposal"]["application"]["commit"]
        .as_str()
        .unwrap()
        .to_string();
    std::fs::write(
        drift.worktree.join(".rk/checks.cue"),
        format!("{source}// unapproved drift\n"),
    )
    .unwrap();
    let error = drift.apply(&drift_proposal).await.unwrap_err().to_string();
    assert!(error.contains("dirty onboarding worktree"), "{error}");
    assert_eq!(drift.proposal(&drift_proposal).await["status"], "failed");
    git(&drift.worktree, &["restore", "--", ".rk/checks.cue"]);
    let recovered = drift.apply(&drift_proposal).await.unwrap();
    assert_eq!(recovered["proposal"]["status"], "verified");
    assert_eq!(recovered["proposal"]["application"]["commit"], commit);
    assert_eq!(git(&drift.worktree, &["rev-list", "--count", "HEAD"]), "2");
    drift.stop().await;

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

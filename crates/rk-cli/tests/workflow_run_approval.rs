use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{Value, json};
use std::io::{BufRead, BufReader};
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::Duration;

fn git(root: &Path, args: &[&str]) {
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
}

fn repository() -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix("rk-factory-cli")
        .tempdir()
        .unwrap();
    git(dir.path(), &["init", "-b", "main"]);
    git(dir.path(), &["config", "user.email", "test@example.com"]);
    git(dir.path(), &["config", "user.name", "Test"]);
    std::fs::create_dir_all(dir.path().join(".rk/workflows")).unwrap();
    std::fs::write(dir.path().join("README.md"), "# Fixture\n").unwrap();
    std::fs::write(
        dir.path().join(".rk/workflows/noop.cue"),
        r#"
workflow: {
    name: "noop"
    params: {taskId: {type: "string", required: true}}
    agents: {default: {harness: "fake", model: "sonnet"}}
    steps: [
        {type: "run", command: "true"},
        {type: "run", command: "true"},
    ]
}
"#,
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

async fn fixture() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    tokio::task::JoinHandle<rk_core::Result<()>>,
) {
    let home = tempfile::tempdir().unwrap();
    let repo = repository();
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    client
        .call(
            "repo.add",
            json!({"name": "fixture", "path": repo.path().to_string_lossy()}),
        )
        .await
        .unwrap();
    (home, repo, handle)
}

fn run_rk(layout: &Layout, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rk"))
        .args(args)
        .env("RK_HOME", layout.home())
        .env_remove("RK_AGENT")
        .env_remove("RK_AUTH_TOKEN")
        .env_remove("RK_TASK")
        .env_remove("RK_REPO")
        .env_remove("RK_ROLE")
        .env_remove("RK_BRANCH")
        .env_remove("RK_WORKTREE")
        .output()
        .unwrap()
}

fn successful_json(output: Output) -> Value {
    assert!(
        output.status.success(),
        "stderr: {}\nstdout: {}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn failed(output: Output, code: i32) -> String {
    assert_eq!(
        output.status.code(),
        Some(code),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stderr).to_string()
}

struct ChildReaper {
    child: Child,
}

impl ChildReaper {
    fn new(child: Child) -> Self {
        Self { child }
    }

    fn stdout(&mut self) -> std::process::ChildStdout {
        self.child.stdout.take().unwrap()
    }

    fn kill_and_wait(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

impl Drop for ChildReaper {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_factory_snapshot_json_uses_read_rpc_without_mutation() {
    let (home, _repo, handle) = fixture().await;
    let layout = Layout::at(home.path());

    let value = successful_json(run_rk(
        &layout,
        &[
            "--json",
            "factory",
            "snapshot",
            "--repo",
            "fixture",
            "--coordinator",
            "coord-1",
            "--include-archived",
        ],
    ));

    assert_eq!(value["schema"], 1);
    assert!(value["snapshot"].is_object());
    let replay = successful_json(run_rk(
        &layout,
        &["--json", "factory", "events", "replay", "--limit", "20"],
    ));
    let events = replay["events"].as_array().unwrap();
    assert!(events.iter().all(|event| {
        !matches!(
            event["source"].as_str(),
            Some("factory.propose_action" | "factory.execute_action" | "workflow.run")
        )
    }));
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_factory_events_replay_is_finite_and_filters_kind() {
    let (home, _repo, handle) = fixture().await;
    let layout = Layout::at(home.path());
    successful_json(run_rk(
        &layout,
        &[
            "--json",
            "factory",
            "propose-workflow",
            "noop",
            "--repo",
            "fixture",
            "--param",
            "taskId=hello",
        ],
    ));

    let value = successful_json(run_rk(
        &layout,
        &[
            "--json",
            "factory",
            "events",
            "replay",
            "--repo",
            "fixture",
            "--kind",
            "approval.changed",
            "--limit",
            "1",
        ],
    ));

    assert_eq!(value["schema"], 1);
    assert_eq!(value["events"].as_array().unwrap().len(), 1);
    assert_eq!(value["events"][0]["repo"], "fixture");
    assert_eq!(value["events"][0]["kind"], "approval.changed");
    assert!(value["boundary"].as_u64().is_some());
    handle.abort();
}

#[test]
fn test_factory_read_commands_do_not_autostart_missing_daemon() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());

    let stderr = failed(run_rk(&layout, &["--json", "factory", "snapshot"]), 1);

    assert!(
        stderr.contains("connect") || stderr.contains("No such file") || stderr.contains("daemon")
    );
    assert!(
        !layout.socket_path().exists(),
        "read command must not create daemon socket"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_factory_events_watch_streams_native_ndjson() {
    let (home, _repo, handle) = fixture().await;
    let layout = Layout::at(home.path());
    let child = Command::new(env!("CARGO_BIN_EXE_rk"))
        .args([
            "--json",
            "factory",
            "events",
            "watch",
            "--repo",
            "fixture",
            "--kind",
            "approval.changed",
        ])
        .env("RK_HOME", layout.home())
        .env_remove("RK_AGENT")
        .env_remove("RK_AUTH_TOKEN")
        .env_remove("RK_TASK")
        .env_remove("RK_REPO")
        .env_remove("RK_ROLE")
        .env_remove("RK_BRANCH")
        .env_remove("RK_WORKTREE")
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut child = ChildReaper::new(child);
    std::thread::sleep(Duration::from_millis(200));
    successful_json(run_rk(
        &layout,
        &[
            "--json",
            "factory",
            "propose-workflow",
            "noop",
            "--repo",
            "fixture",
            "--param",
            "taskId=hello",
        ],
    ));

    let stdout = child.stdout();
    let mut lines = BufReader::new(stdout).lines();
    let line = lines.next().unwrap().unwrap();
    child.kill_and_wait();
    let event: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(event["kind"], "approval.changed");
    assert_eq!(event["repo"], "fixture");
    handle.abort();
}

#[test]
fn test_factory_events_watch_surfaces_resync_ndjson_on_stdout() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let socket = layout.socket_path();
    let listener = UnixListener::bind(&socket).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = String::new();
        BufReader::new(stream.try_clone().unwrap())
            .read_line(&mut request)
            .unwrap();
        let request: Value = serde_json::from_str(&request).unwrap();
        assert_eq!(request["method"], "factory.events.watch");
        use std::io::Write;
        writeln!(
            stream,
            "{}",
            json!({"id": request["id"], "result": {"schema": 1, "events": [], "boundary": 299, "truncated": true}})
        )
        .unwrap();
        writeln!(
            stream,
            "{}",
            json!({"method":"factory.resync", "params":{"truncated": true, "resync_required": true, "boundary": 300}})
        )
        .unwrap();
        writeln!(
            stream,
            "{}",
            json!({"method":"lagged", "params":{"missed": 7}})
        )
        .unwrap();
    });

    let output = run_rk(
        &layout,
        &["--json", "factory", "events", "watch", "--after", "42"],
    );
    server.join().unwrap();
    assert!(
        output.status.success(),
        "stderr: {}\nstdout: {}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines = stdout.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 1, "stdout should contain only NDJSON resync");
    let resync: Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(resync["resync_required"], true);
    assert_eq!(resync["boundary"], 300);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("factory events lagged: missed 7"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_factory_propose_workflow_sends_typed_action_not_shell_command() {
    let (home, _repo, handle) = fixture().await;
    let layout = Layout::at(home.path());

    let value = successful_json(run_rk(
        &layout,
        &[
            "--json",
            "factory",
            "propose-workflow",
            "noop",
            "--repo",
            "fixture",
            "--param",
            "taskId=hello",
            "--coordinator",
            "coord-1",
        ],
    ));

    assert_eq!(value["schema"], "factory.proposal.v1");
    assert_eq!(value["risk"], "mutation");
    assert!(value["digest"].as_str().is_some());
    assert_eq!(value["action"]["name"], "noop");
    assert_eq!(value["action"]["repo"], "fixture");
    assert_eq!(value["action"]["params"]["taskId"], "hello");
    assert_eq!(value["action"]["coordinator"], "coord-1");
    assert_eq!(value["proposal_id"], value["proposal"]["id"]);
    assert_eq!(value["kind"], "workflow.run");
    assert_eq!(value["execution_action"]["name"], "noop");
    assert_eq!(value["execution_action"]["repo"], "fixture");
    assert_eq!(value["execution_action"]["params"]["taskId"], "hello");
    assert_eq!(value["execution_action"]["coordinator"], "coord-1");
    assert!(
        value.get("argv").is_none(),
        "factory proposal must not expose shell argv authority"
    );
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_factory_proposal_file_can_be_approved_and_executed_exactly() {
    let (home, _repo, handle) = fixture().await;
    let layout = Layout::at(home.path());
    let proposal = successful_json(run_rk(
        &layout,
        &[
            "--json",
            "factory",
            "propose-workflow",
            "noop",
            "--repo",
            "fixture",
            "--param",
            "taskId=value with spaces",
            "--coordinator",
            "coord-file",
        ],
    ));
    let proposal_file = home.path().join("workflow-proposal.json");
    std::fs::write(&proposal_file, serde_json::to_vec_pretty(&proposal).unwrap()).unwrap();

    let approved = successful_json(run_rk(
        &layout,
        &[
            "--json",
            "factory",
            "approve",
            "--proposal-file",
            proposal_file.to_str().unwrap(),
        ],
    ));
    assert_eq!(approved["approval"]["proposal_id"], proposal["proposal_id"]);

    let executed = successful_json(run_rk(
        &layout,
        &[
            "--json",
            "factory",
            "execute-action",
            "--proposal-file",
            proposal_file.to_str().unwrap(),
        ],
    ));
    assert_eq!(executed["instance"]["workflow"], "noop");
    assert_eq!(executed["instance"]["params"]["taskId"], "value with spaces");
    assert_eq!(executed["instance"]["coordinator"], "coord-file");
    assert_eq!(executed["approval"]["status"], "consumed");

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_factory_proposal_file_rejects_malformed_nested_proposal_metadata() {
    let (home, _repo, handle) = fixture().await;
    let layout = Layout::at(home.path());
    let digest = "a".repeat(64);

    for (name, nested, expected) in [
        ("not-object", json!([]), "proposal must be a JSON object"),
        (
            "missing-id",
            json!({"digest": digest, "kind": "workflow.run"}),
            "proposal.id",
        ),
        (
            "wrong-id-type",
            json!({"id": 7, "digest": digest, "kind": "workflow.run"}),
            "proposal.id",
        ),
    ] {
        let proposal_file = home.path().join(format!("malformed-{name}.json"));
        std::fs::write(
            &proposal_file,
            serde_json::to_vec_pretty(&json!({
                "schema": "factory.proposal.v1",
                "proposal_id": digest,
                "digest": digest,
                "kind": "workflow.run",
                "execution_action": {"name": "noop", "repo": "fixture", "params": {}},
                "proposal": nested,
            }))
            .unwrap(),
        )
        .unwrap();

        let stderr = failed(
            run_rk(
                &layout,
                &[
                    "--json",
                    "factory",
                    "execute-action",
                    "--proposal-file",
                    proposal_file.to_str().unwrap(),
                ],
            ),
            1,
        );
        assert!(stderr.contains(expected), "{name}: {stderr}");
    }

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_factory_approve_sends_no_identity_param() {
    let (home, _repo, handle) = fixture().await;
    let layout = Layout::at(home.path());
    let proposal = successful_json(run_rk(
        &layout,
        &[
            "--json",
            "factory",
            "propose-workflow",
            "noop",
            "--repo",
            "fixture",
            "--param",
            "taskId=hello",
        ],
    ));
    let proposal_id = proposal["proposal"]["id"].as_str().unwrap();
    let digest = proposal["digest"].as_str().unwrap();

    let approved = successful_json(run_rk(
        &layout,
        &["--json", "factory", "approve", proposal_id, digest],
    ));
    assert_eq!(approved["approval"]["proposal_id"], proposal_id);
    assert_eq!(approved["approval"]["digest"], digest);
    assert!(approved["approval"].get("actor").is_none());

    let stderr = failed(
        run_rk(
            &layout,
            &[
                "--json",
                "factory",
                "approve",
                proposal_id,
                digest,
                "--by",
                "mallory",
            ],
        ),
        2,
    );
    assert!(stderr.contains("unexpected") || stderr.contains("unrecognized"));
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_factory_approve_requires_exact_digest() {
    let (home, _repo, handle) = fixture().await;
    let layout = Layout::at(home.path());

    failed(
        run_rk(&layout, &["--json", "factory", "approve", "proposal-1"]),
        2,
    );
    let stderr = failed(
        run_rk(
            &layout,
            &["--json", "factory", "approve", "proposal-1", "not-a-digest"],
        ),
        2,
    );
    assert!(stderr.contains("digest") || stderr.contains("64"));
    let stderr = failed(
        run_rk(
            &layout,
            &[
                "--json",
                "factory",
                "approve",
                "proposal-1",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            ],
        ),
        2,
    );
    assert!(stderr.contains("digest") || stderr.contains("lowercase"));
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_factory_execute_workflow_preserves_param_values_with_spaces() {
    let (home, _repo, handle) = fixture().await;
    let layout = Layout::at(home.path());
    let proposal = successful_json(run_rk(
        &layout,
        &[
            "--json",
            "factory",
            "propose-workflow",
            "noop",
            "--repo",
            "fixture",
            "--param",
            "taskId=value with spaces",
        ],
    ));
    let proposal_id = proposal["proposal"]["id"].as_str().unwrap();
    let digest = proposal["digest"].as_str().unwrap();
    successful_json(run_rk(
        &layout,
        &["--json", "factory", "approve", proposal_id, digest],
    ));

    let executed = successful_json(run_rk(
        &layout,
        &[
            "--json",
            "factory",
            "execute-workflow",
            proposal_id,
            digest,
            "--workflow",
            "noop",
            "--repo",
            "fixture",
            "--param",
            "taskId=value with spaces",
        ],
    ));

    assert_eq!(
        executed["instance"]["params"]["taskId"],
        "value with spaces"
    );
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_factory_execute_prints_instance_json_on_success() {
    let (home, repo, handle) = fixture().await;
    let layout = Layout::at(home.path());
    let proposal = successful_json(run_rk(
        &layout,
        &[
            "--json",
            "factory",
            "propose-workflow",
            "noop",
            "--repo",
            "fixture",
            "--param",
            "taskId=hello",
        ],
    ));
    let proposal_id = proposal["proposal"]["id"].as_str().unwrap();
    let digest = proposal["digest"].as_str().unwrap();
    successful_json(run_rk(
        &layout,
        &["--json", "factory", "approve", proposal_id, digest],
    ));

    let executed = successful_json(run_rk(
        &layout,
        &[
            "--json",
            "factory",
            "execute-workflow",
            proposal_id,
            digest,
            "--workflow",
            "noop",
            "--repo",
            "fixture",
            "--param",
            "taskId=hello",
        ],
    ));

    assert_eq!(executed["instance"]["workflow"], "noop");
    assert_eq!(
        executed["instance"]["repo"],
        std::fs::canonicalize(repo.path())
            .unwrap()
            .to_string_lossy()
            .as_ref()
    );
    assert_eq!(executed["approval"]["status"], "consumed");
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_factory_execute_rejects_param_without_equals() {
    let (home, _repo, handle) = fixture().await;
    let layout = Layout::at(home.path());

    let stderr = failed(
        run_rk(
            &layout,
            &[
                "--json",
                "factory",
                "execute-workflow",
                "proposal-1",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "--workflow",
                "noop",
                "--repo",
                "fixture",
                "--param",
                "broken",
            ],
        ),
        2,
    );
    assert!(stderr.contains("--param") || stderr.contains("key=value"));
    handle.abort();
}

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
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
        "git {:?}: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
}

fn repository() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    git(dir.path(), &["init", "-b", "main"]);
    git(dir.path(), &["config", "user.email", "test@example.com"]);
    git(dir.path(), &["config", "user.name", "Test"]);
    std::fs::write(
        dir.path().join("README.md"),
        "# Fixture\n\nVerify with `mise run verify`.\n\n```sh\nmise run verify\n```\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("mise.toml"),
        "[tasks.verify]\nrun = \"cargo test\"\n",
    )
    .unwrap();
    git(dir.path(), &["add", "."]);
    git(dir.path(), &["commit", "-m", "initial"]);
    git(
        dir.path(),
        &["remote", "add", "origin", "git@example.com:org/repo.git"],
    );
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

fn regular_files(root: &Path) -> BTreeMap<PathBuf, (Vec<u8>, std::time::SystemTime)> {
    fn visit(
        root: &Path,
        dir: &Path,
        files: &mut BTreeMap<PathBuf, (Vec<u8>, std::time::SystemTime)>,
    ) {
        let mut entries = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        entries.sort();
        for path in entries {
            let metadata = std::fs::symlink_metadata(&path).unwrap();
            if metadata.file_type().is_symlink() {
                continue;
            }
            if metadata.is_dir() {
                visit(root, &path, files);
            } else if metadata.is_file() {
                files.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    (std::fs::read(&path).unwrap(), metadata.modified().unwrap()),
                );
            }
        }
    }

    let mut files = BTreeMap::new();
    visit(root, root, &mut files);
    files
}

fn run_rk(layout: &Layout, args: &[&str]) -> std::process::Output {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inspect_cli_and_rpc_are_deterministic_and_read_only() {
    let castle = tempfile::tempdir().unwrap();
    let layout = Layout::at(castle.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let repo = repository();
    client
        .call(
            "repo.add",
            json!({
                "name": "fixture",
                "path": std::fs::canonicalize(repo.path()).unwrap(),
            }),
        )
        .await
        .unwrap();

    let repo_before = regular_files(repo.path());
    let castle_before = regular_files(castle.path());

    let first = run_rk(
        &layout,
        &["--json", "repo", "onboard", "inspect", "fixture"],
    );
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let second = run_rk(
        &layout,
        &["--json", "repo", "onboard", "inspect", "fixture"],
    );
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    assert_eq!(first.stdout, second.stdout);

    let report: Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(report["schema_version"], 1);
    assert_eq!(report["ready"], true);
    assert_eq!(report["identity"]["registered_name"], "fixture");
    assert!(report["findings"]
        .as_array()
        .unwrap()
        .iter()
        .all(|finding| {
            finding["kind"].is_string()
                && finding["severity"].is_string()
                && finding["evidence"].is_array()
        }));

    let text = run_rk(&layout, &["repo", "onboard", "inspect", "fixture"]);
    assert!(
        text.status.success(),
        "{}",
        String::from_utf8_lossy(&text.stderr)
    );
    let text = String::from_utf8(text.stdout).unwrap();
    assert!(text.contains("ready  yes"));
    assert!(text.contains("git_base_detected"));
    assert!(text.contains("evidence [observed]"));

    assert_eq!(regular_files(repo.path()), repo_before);
    assert_eq!(regular_files(castle.path()), castle_before);

    std::fs::write(repo.path().join("dirty.txt"), "uncommitted\n").unwrap();
    let dirty_before = regular_files(repo.path());
    let failed = run_rk(
        &layout,
        &["--json", "repo", "onboard", "inspect", "fixture"],
    );
    assert!(!failed.status.success());
    let failed_report: Value = serde_json::from_slice(&failed.stdout).unwrap();
    assert_eq!(failed_report["ready"], false);
    assert!(failed_report["findings"]
        .as_array()
        .unwrap()
        .iter()
        .any(|finding| { finding["kind"] == "git_state_dirty" && finding["severity"] == "error" }));
    assert_eq!(regular_files(repo.path()), dirty_before);
    assert_eq!(regular_files(castle.path()), castle_before);

    client.call("stop", json!({})).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("daemon did not stop")
        .unwrap()
        .unwrap();
}

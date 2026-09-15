//! TKT-lurin-bulif-gabik: a real operator CLI probe reproduced a defect in
//! the documented public retrieval command for a `verify.run` failure
//! receipt — `rk scan artifact <repo> verification-failure-receipt --search
//! <receipt_id>` returned `{tuples: [], truncated: false}`. Root cause:
//! `--search` is `payload_search`, a substring test over the SERIALIZED
//! PAYLOAD (`Pattern::payload_search`); the receipt id lived only in the
//! tuple's own `id`, never inside its payload, so the search could never
//! match. Fixed by mirroring the id into the payload as `receipt_id`.
//!
//! This test executes the exact real, public two-command journey through the
//! compiled `rk` binary: `rk verify` against a genuinely failing named
//! check, then `rk scan artifact <repo> verification-failure-receipt
//! --search <receipt_id>` — proving the documented retrieval command
//! actually finds the receipt, not just that the underlying RPC/storage
//! layer can.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::Value;
use std::path::Path;
use std::process::{Command, Output};
use std::time::Duration;

fn cli(layout: &Layout, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rk"));
    command.args(args);
    for key in rk_core::review::STRIPPED_RK_SPAWN_ENV {
        command.env_remove(key);
    }
    command.env("RK_HOME", layout.home());
    command.output().unwrap()
}

fn json(output: Output) -> Value {
    assert!(
        output.status.success() || output.status.code() != Some(0),
        "unexpected signal termination: {output:?}"
    );
    serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "not valid JSON ({e}): {}",
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

async fn start(layout: &Layout) -> tokio::task::JoinHandle<rk_core::Result<()>> {
    let daemon = Daemon::new(layout.clone(), &rk_core::config::Config::default()).unwrap();
    let handle = tokio::spawn(daemon.run());
    for _ in 0..250 {
        if Client::connect_as_operator(layout).await.is_ok() {
            return handle;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("daemon did not start");
}

fn write_failing_check(repo: &Path) {
    std::fs::create_dir_all(repo.join(".rk")).unwrap();
    std::fs::write(
        repo.join(".rk/checks.cue"),
        r#"checks: [{name: "verify",
        command: "echo probe-stdout-marker; exit 4", timeout: "30s",
        environmentPolicy: "strip_rk_spawn", sharedCargoTarget: false}]"#,
    )
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rk_scan_with_search_actually_finds_the_failure_receipt_it_documents() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_name = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    write_failing_check(repo_dir.path());
    let layout = Layout::at(home.path());
    let _daemon = start(&layout).await;

    let add = json(cli(
        &layout,
        &[
            "--json",
            "repo",
            "add",
            &repo_dir.path().to_string_lossy(),
            "--name",
            &repo_name,
        ],
    ));
    assert!(add.get("error").is_none(), "repo add failed: {add}");

    // Step 1 of the real public journey: run the failing check for real.
    let verify = json(cli(
        &layout,
        &[
            "--json", "verify", "--repo", &repo_name, "--check", "verify",
        ],
    ));
    assert_eq!(verify["verdict"], "fail");
    assert_eq!(verify["exit"], 4);
    let receipt_id = verify["failure_receipt_id"]
        .as_str()
        .expect("a real failing rk verify must expose a failure_receipt_id")
        .to_string();

    // Step 2: the EXACT documented retrieval command, discarding everything
    // from step 1 except the id — as a caller who lost the rest of that
    // response would.
    let scanned = json(cli(
        &layout,
        &[
            "--json",
            "scan",
            "artifact",
            &repo_name,
            "verification-failure-receipt",
            "--search",
            &receipt_id,
        ],
    ));
    let tuples = scanned["tuples"]
        .as_array()
        .unwrap_or_else(|| panic!("no tuples array in scan response: {scanned}"));
    assert_eq!(
        tuples.len(),
        1,
        "the documented `rk scan ... --search <receipt_id>` command must find exactly the \
         one receipt it names: {scanned}"
    );
    assert_eq!(tuples[0]["id"], receipt_id);
    assert_eq!(tuples[0]["payload"]["receipt_id"], receipt_id);
    assert_eq!(tuples[0]["payload"]["exit"], 4);
    assert!(tuples[0]["payload"]["stdout_tail"]
        .as_str()
        .unwrap()
        .contains("probe-stdout-marker"));
}

//! `rk spawn --ticket <alias>` for a legacy `TKT-<ULID>` ticket must store
//! the ticket's canonical durable identity in the spawned agent's `task`
//! field, never the raw `--ticket` spelling the caller happened to type.
//!
//! `record.task` is persisted verbatim into worktree/branch names and the
//! agent record, then later compared by raw string equality in two
//! safety-critical places: the delivery guard (`ticket_has_delivery_candidate`,
//! now fixed to check every spelling via `Tickets::id_spellings`) and the
//! live-agent rescue sweep (`ticket_reopen_sweep_at`'s
//! `a.task.as_deref() == Some(ticket.identity.as_str())`, which is NOT
//! spelling-tolerant). If `rk spawn --ticket <alias>` stored the alias
//! verbatim, a legacy ticket dispatched by its alias would desync from both
//! call sites the moment its identity is looked up by ULID elsewhere.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

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

async fn connect(layout: &Layout) -> Client {
    for _ in 0..1500 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

const RESULT_LINE: &str = r#"echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"wf-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'"#;

const FAKE_HARNESS: &str = r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"wf-fake"}'
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_by_legacy_alias_stores_canonical_ulid_as_task() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    std::fs::create_dir_all(repo_dir.path().join(".rk")).unwrap();
    std::fs::write(repo_dir.path().join(".rk/repo.cue"), "repo: {}\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);

    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        format!("{FAKE_HARNESS}{RESULT_LINE}\n"),
    );

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": "aliasrepo", "path": repo_dir.path()}),
        )
        .await
        .unwrap();

    // `ticket.new` only mints proquint ids now, so seed a legacy ULID-identity
    // ticket directly, mirroring ticket_done_binding.rs's approach.
    let legacy_id = "TKT-01J000000000000000000099";
    client
        .call(
            "space.out",
            json!({
                "category": "task",
                "scope": "aliasrepo",
                "identity": legacy_id,
                "payload": {
                    "title": "legacy ticket dispatched by alias",
                    "status": "open",
                    "parent": null,
                    "priority": "normal",
                    "labels": [],
                    "depends_on": [],
                    "assignee": null,
                    "created_by": "operator",
                    "created_at": "2026-08-19T00:00:00Z",
                    "updated_at": "2026-08-19T00:00:00Z",
                },
                "lifecycle": "session",
            }),
        )
        .await
        .unwrap();

    let fetched = client
        .call("ticket.get", json!({"id": legacy_id}))
        .await
        .unwrap();
    let alias = fetched["ticket"]["alias"]
        .as_str()
        .expect("a legacy ULID ticket must surface a proquint alias")
        .to_string();
    assert_ne!(alias, legacy_id, "the alias must be a distinct spelling");

    // Dispatch by the ALIAS, not the ULID.
    let output = Command::new(env!("CARGO_BIN_EXE_rk"))
        .args(["--json", "spawn", "--ticket", &alias, "--harness", "fake"])
        .env("RK_HOME", home.path())
        .env_remove("RK_AGENT")
        .env_remove("RK_AUTH_TOKEN")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "spawn --ticket failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let agent: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        agent["task"].as_str().unwrap(),
        legacy_id,
        "record.task must be canonicalized to the ticket's durable identity, \
         never persisted as the raw --ticket alias spelling: {agent}"
    );
}

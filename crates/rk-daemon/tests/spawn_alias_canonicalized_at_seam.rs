//! `Supervisor::spawn` must canonicalize a legacy ticket's proquint alias to
//! its durable `TKT-<ULID>` identity itself, not rely on a caller (like
//! `rk spawn --ticket`, fixed by 5c484db) to have done it first.
//!
//! This calls `agent.spawn` directly over RPC with `task` set to the raw
//! alias spelling — the same shape a workflow's `Step::Spawn` produces after
//! `interpolate`ing task text with zero canonicalization of its own. If the
//! seam in `Supervisor::spawn` regressed, `record.task` would carry the
//! alias instead of the ULID identity, silently desyncing from every
//! internal cross-reference keyed on identity (delivery, rework/conflict
//! journaling, the reopen sweep).

mod support;

use rk_core::paths::Layout;
use rk_daemon::Daemon;
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_spawn_rpc_by_legacy_alias_canonicalizes_task_at_the_seam() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path();
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "r@x"]);
    git(repo, &["config", "user.name", "R"]);
    std::fs::write(repo.join("README.md"), "# alias seam\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "init"]);
    support::install_default_repository_policy(repo);

    let captured = home.path().join("observed-task");
    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        format!(
            r#"read -r _prompt
echo '{{"type":"system","subtype":"init","session_id":"alias-seam-fake"}}'
printf '%s' "$RK_TASK" > '{}'
read -r _hold
"#,
            captured.display()
        ),
    );
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "alias-seam-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": "aliasseamrepo", "path": repo.to_string_lossy()}),
        )
        .await
        .unwrap();

    // `ticket.new` only mints proquint ids now, so seed a legacy ULID-identity
    // ticket directly (mirrors ticket_done_binding.rs's approach) to get a
    // distinct alias spelling to dispatch by.
    let legacy_id = "TKT-01J000000000000000000077";
    client
        .call(
            "space.out",
            json!({
                "category": "task",
                "scope": "aliasseamrepo",
                "identity": legacy_id,
                "payload": {
                    "title": "legacy ticket dispatched via raw RPC by alias",
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

    // All calls bypass the CLI. Inspect the actual worker environment as
    // well as the persisted record, and preserve non-ticket task strings.
    for (supplied, expected) in [
        (alias.as_str(), legacy_id),
        (
            "investigate a free text task",
            "investigate a free text task",
        ),
        ("onb-stable-session", "onb-stable-session"),
    ] {
        let _ = std::fs::remove_file(&captured);
        let spawned = client
            .call(
                "agent.spawn",
                json!({
                    "repo": repo.to_string_lossy(), "task": supplied, "harness": "fake"
                }),
            )
            .await
            .unwrap();
        assert_eq!(spawned["agent"]["task"], expected);
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Ok(value) = std::fs::read_to_string(&captured) {
                    if value == expected {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker must see the canonical task in RK_TASK");
        client
            .call("agent.dismiss", json!({"name": spawned["agent"]["name"]}))
            .await
            .unwrap();
    }

    handle.abort();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

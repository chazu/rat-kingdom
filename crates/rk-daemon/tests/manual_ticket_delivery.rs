//! Content-bound operator delivery for work landed outside the rat pipeline.

mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use support::connect;

fn git(dir: &Path, args: &[&str]) -> String {
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
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

#[tokio::test]
async fn manual_delivery_is_reachable_operator_only_idempotent_and_unblocks_dependents() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path();
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "r@x"]);
    git(repo, &["config", "user.name", "R"]);
    std::fs::write(repo.join("one"), "one\n").unwrap();
    git(repo, &["add", "one"]);
    git(repo, &["commit", "-m", "one"]);
    let delivered_commit = git(repo, &["rev-parse", "HEAD"]);
    std::fs::write(repo.join("two"), "two\n").unwrap();
    git(repo, &["add", "two"]);
    git(repo, &["commit", "-m", "two"]);
    let other_reachable_commit = git(repo, &["rev-parse", "HEAD"]);
    git(repo, &["switch", "-c", "side"]);
    std::fs::write(repo.join("side"), "side\n").unwrap();
    git(repo, &["add", "side"]);
    git(repo, &["commit", "-m", "side"]);
    let unreachable_commit = git(repo, &["rev-parse", "HEAD"]);
    git(repo, &["switch", "main"]);

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "manual-delivery-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut operator = connect(&layout).await;
    operator
        .call("repo.add", json!({"name": "manualrepo", "path": repo}))
        .await
        .unwrap();
    let prerequisite = operator
        .call(
            "ticket.new",
            json!({"title": "landed elsewhere", "scope": "manualrepo"}),
        )
        .await
        .unwrap();
    let prerequisite = prerequisite["ticket"]["identity"]
        .as_str()
        .unwrap()
        .to_string();
    let dependent = operator
        .call(
            "ticket.new",
            json!({
                "title": "next slice",
                "scope": "manualrepo",
                "depends_on": [prerequisite.clone()],
            }),
        )
        .await
        .unwrap();
    let dependent = dependent["ticket"]["identity"]
        .as_str()
        .unwrap()
        .to_string();
    let params = json!({
        "id": prerequisite,
        "repo": "manualrepo",
        "commit": delivered_commit,
        "target": "main",
        "verification": "mise run verify passed externally",
        "source_branch": "human/fix",
    });

    let mut rat = Client::connect_as(&layout, "rat-a").await.unwrap();
    assert!(rat.call("ticket.deliver", params.clone()).await.is_err());
    assert!(operator
        .call(
            "ticket.deliver",
            json!({
                "id": prerequisite,
                "repo": "manualrepo",
                "commit": unreachable_commit,
                "target": "main",
                "verification": "claimed",
            }),
        )
        .await
        .is_err());

    let recorded = operator
        .call("ticket.deliver", params.clone())
        .await
        .unwrap();
    assert_eq!(recorded["already_recorded"], false);
    assert_eq!(recorded["ticket"]["payload"]["status"], "closed");
    assert_eq!(
        recorded["ticket"]["payload"]["delivery"]["merge_commit"],
        delivered_commit
    );

    let ready = operator
        .call("ticket.ready", json!({"scope": "manualrepo"}))
        .await
        .unwrap();
    assert!(ready["tickets"]
        .as_array()
        .unwrap()
        .iter()
        .any(|ticket| ticket["identity"] == dependent));
    let dependent_after = operator
        .call("ticket.get", json!({"id": dependent}))
        .await
        .unwrap();
    assert_eq!(
        dependent_after["ticket"]["payload"]["depends_on"],
        json!([prerequisite])
    );

    let replay = operator.call("ticket.deliver", params).await.unwrap();
    assert_eq!(replay["already_recorded"], true);
    assert!(operator
        .call(
            "ticket.deliver",
            json!({
                "id": prerequisite,
                "repo": "manualrepo",
                "commit": other_reachable_commit,
                "target": "main",
                "verification": "different delivery",
                "source_branch": "human/fix",
            }),
        )
        .await
        .is_err());

    let events = operator
        .call(
            "space.scan",
            json!({"category": "event", "identity": "ticket_manual_delivery"}),
        )
        .await
        .unwrap();
    let events = events["tuples"].as_array().unwrap();
    assert_eq!(events.len(), 1, "replay must not duplicate the audit event");
    assert_eq!(events[0]["payload"]["by"], "human-operator");
    assert_eq!(
        events[0]["payload"]["verification"],
        "mise run verify passed externally"
    );

    operator.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

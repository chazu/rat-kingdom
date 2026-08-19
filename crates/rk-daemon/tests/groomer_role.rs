//! The groomer role over the wire: a real spawned groomer holding a real,
//! valid agent token keeps the ordinary rat surface (it can file tickets,
//! read the backlog, write artifacts/obstacles/needs) but is refused every
//! operator-only method except one narrowly shaped `ticket.update` — closing
//! a ticket with recorded evidence. See `read_only_roles.rs` for the unit
//! coverage of the shape check itself; this is the thing that actually
//! matters: the daemon enforces it for a real caller, not just the function.

mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use rk_ledger::Budget;
use rk_space::Space;
use serde_json::json;
use std::path::Path;
use std::process::Command;
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

/// Idles so the record stays live while the test drives its identity.
const IDLE_FAKE: &str = r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"groomer-1"}'
sleep 30
"#;

#[tokio::test]
async fn groomer_keeps_the_ordinary_surface_but_only_closes_tickets_with_evidence() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("f"), "x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);

    std::env::set_var("RK_FAKE_HARNESS_CMD", IDLE_FAKE);
    let layout = Layout::at(home.path());
    let daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "fake".into(),
        Budget::default(),
        Space::open_in_memory().unwrap(),
    )
    .unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut operator = connect(&layout).await;

    let target_closed = operator
        .call("ticket.new", json!({"title": "TKT-target"}))
        .await
        .unwrap();
    let target_id = target_closed["ticket"]["identity"]
        .as_str()
        .unwrap()
        .to_string();
    operator
        .call("ticket.update", json!({"id": &target_id, "status": "done"}))
        .await
        .unwrap();

    let rework = operator
        .call(
            "ticket.new",
            json!({"title": format!("rework: {target_id}")}),
        )
        .await
        .unwrap();
    let rework_id = rework["ticket"]["identity"].as_str().unwrap().to_string();

    let spawned = operator
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "groomer-1",
                "role": "groomer",
                "harness": "fake",
            }),
        )
        .await
        .expect("workflow dispatch must be able to spawn this role");
    let agent = &spawned["agent"];
    let name = agent["name"].as_str().unwrap().to_string();

    // Layer 1 — same forced read-only harness as onboarder/diagnostician.
    assert_eq!(
        agent["permission_mode"].as_str(),
        Some("read-only"),
        "groomer must inherit the forced read-only harness mode"
    );

    let mut groomer = Client::connect_as(&layout, &name).await.unwrap();

    // Layer 2 — the ordinary rat surface is intact: this is NOT the strict
    // read-only allowlist. A groomer needs the backlog and the hand-off
    // fallback (artifact/ticket/need/obstacle) or it could never document
    // what it chose not to close.
    groomer
        .call("ticket.list", json!({"status": "open"}))
        .await
        .expect("a groomer must be able to read the backlog");
    groomer
        .call("ticket.get", json!({"id": rework_id}))
        .await
        .expect("a groomer must be able to read a single ticket");
    groomer
        .call("ticket.new", json!({"title": "follow-up filed by groomer"}))
        .await
        .expect("a groomer must retain the ordinary ticket.new grant");
    groomer
        .call(
            "space.out",
            json!({"scope": "r", "category": "artifact", "identity": "backlog-groom",
                   "payload": {"agent": name, "handed_off": 1}}),
        )
        .await
        .expect("a groomer must retain the ordinary artifact hand-off grant");

    // Still refused every other operator-only method — the narrow grant does
    // not widen into the rest of that list.
    for (method, params) in [
        ("agent.spawn", json!({"repo": "r", "task": "t"})),
        ("workflow.run", json!({"name": "steward"})),
        ("repo.add", json!({"path": "/tmp"})),
        ("ticket.dep", json!({"id": rework_id, "dep": target_id})),
        ("ticket.update", json!({"id": rework_id, "status": "done"})),
        ("ticket.update", json!({"id": rework_id, "status": "open"})),
        (
            "ticket.update",
            json!({"id": rework_id, "title": "sneak in a title change"}),
        ),
        (
            "ticket.update",
            json!({"id": rework_id, "status": "closed"}),
        ),
    ] {
        let err = groomer
            .call(method, params)
            .await
            .expect_err(&format!("{method} must be refused to a groomer"));
        assert!(
            err.to_string().contains("forbidden"),
            "{method} refused for the wrong reason: {err}"
        );
    }

    // The one grant: closing with recorded evidence.
    let closed = groomer
        .call(
            "ticket.update",
            json!({"id": &rework_id, "status": "closed",
                   "reason": {"reason": "stale-rework",
                              "evidence": format!("{target_id} done")}}),
        )
        .await
        .expect("a groomer must be able to close a ticket with recorded evidence");
    assert_eq!(closed["ticket"]["payload"]["status"], "closed");

    let events = operator
        .call(
            "space.scan",
            json!({"category": "event", "identity": "ticket-groomed"}),
        )
        .await
        .unwrap();
    let events = events["tuples"].as_array().cloned().unwrap_or_default();
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0]["payload"]["ticket"], rework_id);
    assert_eq!(events[0]["payload"]["groomer"], name);

    let _ = operator.call("agent.dismiss", json!({"name": name})).await;
}

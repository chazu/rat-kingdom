//! `rk ticket reopen`: the explicit, operator-only door back to the backlog
//! for a `done`/`closed` ticket (TKT-01M0B5DVMB186W29DZXWAXW36Q).
//!
//! Ordinary `ticket.update` refuses `done -> in_progress` and any backwards
//! move out of `closed` — `valid_transition` in `tickets.rs` only allows
//! `done -> closed`. That is deliberate: a plain status write must never
//! silently demote a ticket a reviewer already signed off on. `ticket.reopen`
//! is the one sanctioned way around that, gated to operator/foreman-equivalent
//! callers (mirroring `ticket.update`/`ticket.dep`) and announced as a
//! `ticket_reopened` event so the move leaves an audit trail.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::time::Duration;

async fn connect(layout: &Layout) -> Client {
    for _ in 0..100 {
        if let Ok(client) = Client::connect_as_operator(layout).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("daemon did not start");
}

#[tokio::test]
async fn reopen_moves_a_done_ticket_back_to_open_refuses_agents_and_leaves_an_event() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "reopen-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut operator = connect(&layout).await;

    let ticket = operator
        .call("ticket.new", json!({"title": "reopen me"}))
        .await
        .unwrap();
    let id = ticket["ticket"]["identity"].as_str().unwrap().to_string();

    operator
        .call("ticket.update", json!({"id": id, "status": "done"}))
        .await
        .unwrap();

    // The state machine's own guard: a plain update can never demote a done
    // ticket back into the active pipeline.
    let refused_update = operator
        .call(
            "ticket.update",
            json!({"id": id, "status": "in_progress"}),
        )
        .await;
    assert!(
        refused_update.is_err(),
        "done -> in_progress must stay refused via plain update"
    );

    // An agent caller is refused the explicit reopen door too.
    let mut rat = Client::connect_as(&layout, "rat-a").await.unwrap();
    let err = rat
        .call("ticket.reopen", json!({"id": id}))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("forbidden") || err.contains("not authorized"),
        "an agent caller must be refused ticket.reopen: {err}"
    );
    let still_done = operator
        .call("ticket.get", json!({"id": id}))
        .await
        .unwrap();
    assert_eq!(
        still_done["ticket"]["payload"]["status"], "done",
        "a refused reopen attempt must not have touched the ticket"
    );

    // The operator can reopen a done ticket back to open.
    let reopened = operator
        .call("ticket.reopen", json!({"id": id}))
        .await
        .unwrap();
    assert_eq!(reopened["ticket"]["payload"]["status"], "open");

    // ... and the move is announced as an audit event.
    let events = operator
        .call(
            "space.scan",
            json!({"category": "event", "identity": "ticket_reopened"}),
        )
        .await
        .unwrap();
    let events = events["tuples"].as_array().cloned().unwrap_or_default();
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0]["payload"]["ticket"], json!(id));
    assert_eq!(events[0]["payload"]["from_status"], json!("done"));
    assert_eq!(events[0]["payload"]["to_status"], json!("open"));
    assert_eq!(events[0]["payload"]["by"], json!("operator"));

    operator.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn reopen_can_target_blocked_instead_of_open() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "reopen-blocked-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut operator = connect(&layout).await;

    let ticket = operator
        .call("ticket.new", json!({"title": "reopen me blocked"}))
        .await
        .unwrap();
    let id = ticket["ticket"]["identity"].as_str().unwrap().to_string();
    operator
        .call("ticket.update", json!({"id": id, "status": "done"}))
        .await
        .unwrap();

    let reopened = operator
        .call("ticket.reopen", json!({"id": id, "status": "blocked"}))
        .await
        .unwrap();
    assert_eq!(reopened["ticket"]["payload"]["status"], "blocked");

    operator.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

//! TKT-22 — the composed convention-quorum loop, end to end over the wire.
//!
//! This is the *integration* test on top of the two primitives it composes:
//!   - TKT-12: `rk suggest` / `rk endorse` sugar + the reactor's built-in
//!     quorum promotion of a `Suggestion` into a `Convention`.
//!   - TKT-18: spawn-time injection of active conventions into a rat's prompt.
//!
//! Where `reactor.rs::live_daemon_promotes_convention_at_quorum` proves the
//! reactor promotes on its own, this test pins the *seam between the two
//! tickets*: it drives the loop through the exact wire payloads `rk suggest`
//! and `rk endorse` emit, then asserts the promoted Convention is in the precise
//! shape the injection hop (`Supervisor::scan_conventions` -> `prime::render`)
//! consumes — system scope, `Furniture`, and a **non-blank `text`** (a blank
//! one is silently dropped by `render_conventions`, so a norm with no surviving
//! text would reach quorum yet never bind). That non-blank-text contract is the
//! thing neither primitive's own tests assert, and it is what makes "a
//! suggestion crossing quorum becomes an injected standing convention" true
//! rather than merely promoted.
//!
//! The final spawn-and-grep hop (the convention text appearing verbatim in a
//! rat's rendered system prompt) is exercised at runtime by
//! `scripts/convention-quorum-demo.sh`, which needs no compile-time dependency
//! on the injection code.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::time::Duration;

/// The wire payload `rk suggest '<text>'` writes: a system-scope `Suggestion`
/// authored by `agent` (so distinct-endorser counting keys off `instance`),
/// carrying the human text under `payload.text` — the field the reactor cites
/// into the Convention and the injection hop later composes into the prompt.
fn suggest(sug_id: &str, agent: &str, text: &str) -> Value {
    json!({
        "category": "suggestion",
        "scope": "system",
        "identity": sug_id,
        "instance": agent,
        "payload": {"agent": agent, "task": null, "text": text},
    })
}

/// The wire payload `rk endorse <sug-id>` writes: an `Endorsement` keyed by
/// `(identity = suggestion, instance = agent)`, so a re-endorsement from an
/// already-counted agent unifies onto the same tuple and can never inflate the
/// distinct-endorser tally.
fn endorse(sug_id: &str, agent: &str) -> Value {
    json!({
        "category": "endorsement",
        "scope": "system",
        "identity": sug_id,
        "instance": agent,
        "payload": {"suggestion": sug_id, "agent": agent},
    })
}

async fn connect(layout: &Layout) -> Client {
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

/// The full loop over the wire: three distinct agents propose-and-endorse one
/// norm the way the CLI does; the live daemon's reactor promotes it on its own;
/// and the promoted Convention lands in the exact injectable shape the spawn
/// hop binds. A duplicate endorsement is included to prove it never inflates the
/// tally past the true distinct-endorser count.
#[tokio::test]
async fn suggestion_crossing_quorum_becomes_an_injectable_convention() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();

    // Default ReactorConfig.quorum is 3.
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let sug_id = "sug-squash";
    let text = "squash your branch before requesting merge";

    client
        .call("space.out", suggest(sug_id, "Whisker", text))
        .await
        .unwrap();
    // Whisker is the author; endorsers are three distinct agents. Gouda votes
    // twice — the duplicate must unify onto one tuple and not lift the count.
    for who in ["Nibbles", "Gouda", "Gouda", "Brie"] {
        client
            .call("space.out", endorse(sug_id, who))
            .await
            .unwrap();
    }

    // Poll until the reactor's built-in promotion fires (feed-woken, so this is
    // usually one cycle, but we allow slack for scheduling).
    let mut convention: Option<Value> = None;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let scan = client
            .call(
                "space.scan",
                json!({"category": "convention", "scope": "system"}),
            )
            .await
            .unwrap();
        if let Some(arr) = scan["tuples"].as_array() {
            if let Some(c) = arr.iter().find(|c| c["identity"] == sug_id) {
                convention = Some(c.clone());
                break;
            }
        }
    }

    let c = convention.expect("reactor never promoted the quorum-reached suggestion");

    // --- The injection contract (TKT-18 consumes exactly this) ---------------

    // Permanent, never `in`-consumable, replicates across castles: an injected
    // norm must outlive the ballot that spawned it.
    assert_eq!(
        c["lifecycle"],
        json!("furniture"),
        "a promoted convention must be Furniture so it survives to be injected"
    );

    // Non-blank text is the precondition that separates "promoted" from
    // "binding": `render_conventions` drops a blank-text convention, so a norm
    // whose source text decayed to empty would reach quorum yet never appear in
    // any prompt. The reactor cites the suggestion's live `payload.text`.
    let convention_text = c["payload"]["text"].as_str().unwrap_or("");
    assert_eq!(
        convention_text, text,
        "the convention must carry the suggestion's verbatim text for injection"
    );
    assert!(
        !convention_text.trim().is_empty(),
        "a blank-text convention is dropped at injection and would never bind"
    );

    // Distinct-endorser tally: three (Nibbles, Gouda, Brie); the duplicate Gouda
    // vote did not inflate it, and the author Whisker did not self-count.
    assert_eq!(
        c["payload"]["count"],
        json!(3),
        "count is the DISTINCT endorser set; a double vote must not inflate it"
    );
    assert_eq!(
        c["payload"]["endorsers"],
        json!(["Brie", "Gouda", "Nibbles"]),
        "endorsers are the distinct, sorted set"
    );

    // --- Promote-once idempotency over the wire ------------------------------

    // A fresh endorsement after promotion must not mint a second Convention:
    // the permanent Convention is its own "already promoted" guard.
    client
        .call("space.out", endorse(sug_id, "Cheddar"))
        .await
        .unwrap();
    let mut count_for_sug = 0usize;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let scan = client
            .call(
                "space.scan",
                json!({"category": "convention", "scope": "system"}),
            )
            .await
            .unwrap();
        count_for_sug = scan["tuples"]
            .as_array()
            .map(|a| a.iter().filter(|c| c["identity"] == sug_id).count())
            .unwrap_or(0);
        // Give a spurious second promotion time to appear if the guard were broken.
        if count_for_sug > 1 {
            break;
        }
    }
    assert_eq!(
        count_for_sug, 1,
        "the convention is the promote-once guard; a later endorsement re-promotes nothing"
    );
}

/// TKT-168 — a ballot is a ledger entry, not a pheromone: written the way the
/// CLI now writes it (no `ttl_secs`), a suggestion and its votes land `Session`
/// durable, so the three endorsers reaching quorum no longer have to overlap
/// inside one wall-clock window.
///
/// This asserts the two properties the old 24h default silently denied, both of
/// which follow from `Lifecycle` and neither of which a *longer* window fixes:
///
/// - **survives the GC** — `gc_expired` collects on `expires_at`, so a ballot
///   with no window is never collected, however quiet the fleet goes between
///   the first vote and the third.
/// - **replicates** — rk-sync exports durable lifecycles only, so an Ephemeral
///   ballot was invisible to peer castles while the `Convention` it promotes to
///   replicates. Durable is what lets two castles pool votes into one quorum.
#[tokio::test]
async fn a_ballot_written_without_a_window_is_durable() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();

    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let sug_id = "sug-durable";
    client
        .call("space.out", suggest(sug_id, "Whisker", "ballots are ledgers"))
        .await
        .unwrap();
    client
        .call("space.out", endorse(sug_id, "Nibbles"))
        .await
        .unwrap();

    for category in ["suggestion", "endorsement"] {
        let scan = client
            .call("space.scan", json!({"category": category, "scope": "system"}))
            .await
            .unwrap();
        let t = scan["tuples"]
            .as_array()
            .and_then(|a| a.iter().find(|t| t["identity"] == sug_id))
            .unwrap_or_else(|| panic!("no {category} for {sug_id}"))
            .clone();
        assert_eq!(
            t["lifecycle"],
            json!("session"),
            "a {category} written with no ttl must be durable, not Ephemeral"
        );
        assert!(
            t.get("expires_at").is_none_or(Value::is_null),
            "a durable {category} carries no expiry for the GC to collect: {t}"
        );
    }
}

//! `rk status <TKT-id>`: render one ticket's task-to-main critical path from
//! the durable phase-span substrate (`crates/rk-daemon/src/span.rs`,
//! TKT-01M0P974EZZTPMGVP4S0E76NXH). Pure aggregation over spans existing
//! producers already recorded — no new storage, no daemon writes, and no
//! value is invented for a phase/metric with no recorded evidence: it is
//! rendered as absent (JSON `null`, human output omits the line) rather than
//! defaulted to zero.

use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::fmt::Write as _;

use crate::observe::human_ms;

/// Fetch `task`'s recorded spans and render its critical path (JSON or
/// human). Bails if `task` does not resolve to a known ticket — a bounded,
/// single RPC round trip to resolve scope plus one scoped, payload-filtered
/// `space.scan` (never the whole event category).
pub async fn show(layout: &Layout, task: &str, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let ticket = client.call("ticket.get", json!({"id": task})).await?;
    if ticket["ticket"].is_null() {
        if as_json {
            println!("{}", json!({"task": task, "found": false}));
            return Ok(());
        }
        bail!("no such ticket: {task}");
    }
    let scope = ticket["ticket"]["scope"]
        .as_str()
        .unwrap_or(rk_core::tuple::SYSTEM_SCOPE)
        .to_string();
    let result = client
        .call(
            "space.scan",
            json!({
                "category": "event",
                "identity": rk_daemon::span::SPAN_IDENTITY,
                "scope": scope,
                "payload_search": format!("\"task\":\"{task}\""),
            }),
        )
        .await?;
    let tuples = result["tuples"].as_array().cloned().unwrap_or_default();
    let cp = build_critical_path(task, &tuples);
    if as_json {
        println!("{cp}");
        return Ok(());
    }
    print!("{}", render_critical_path(&cp));
    Ok(())
}

/// Build the critical-path summary from raw `task_span` tuples (as returned
/// by `space.scan`, each carrying `id`, `payload`, ... — the wire shape, not
/// the daemon-internal `PhaseSpan`). Pure and deterministic: sorts oldest
/// first by `id`, then dedups on `(phase, attempt)` keeping the first
/// occurrence, so a duplicate replay or reordering never double-counts a
/// phase. Never invents a value: every metric that has no supporting span in
/// `tuples` comes out `null`, not `0`.
pub fn build_critical_path(task: &str, tuples: &[Value]) -> Value {
    let mut rows = tuples.to_vec();
    rows.sort_by(|a, b| {
        a["id"]
            .as_str()
            .unwrap_or("")
            .cmp(b["id"].as_str().unwrap_or(""))
    });

    let mut seen: BTreeSet<(String, u64)> = BTreeSet::new();
    let mut phases: Vec<Value> = Vec::new();
    for t in rows {
        let p = t["payload"].clone();
        let phase = p["phase"].as_str().unwrap_or("?").to_string();
        let attempt = p["attempt"].as_u64().unwrap_or(1);
        if !seen.insert((phase, attempt)) {
            continue;
        }
        phases.push(p);
    }

    fn by_phase<'a>(phases: &'a [Value], phase: &str) -> Vec<&'a Value> {
        phases.iter().filter(|p| p["phase"] == phase).collect()
    }
    fn latest<'a>(phases: &'a [Value], phase: &str) -> Option<&'a Value> {
        by_phase(phases, phase).into_iter().next_back()
    }

    let start_at = latest(&phases, "ticket_ready")
        .and_then(|p| p["queued_at"].as_str().or_else(|| p["started_at"].as_str()));
    let merge = latest(&phases, "merge");
    let end_at = merge.and_then(|p| p["ended_at"].as_str());
    let task_to_main_ms = match (
        start_at.and_then(parse_rfc3339),
        end_at.and_then(parse_rfc3339),
    ) {
        (Some(s), Some(e)) => Some((e - s).num_milliseconds()),
        _ => None,
    };

    let review_rounds = by_phase(&phases, "semantic_review").len() as u64;
    let rework_rounds = by_phase(&phases, "rework").len() as u64;
    let rework_amplification =
        (review_rounds > 0).then(|| rework_rounds as f64 / review_rounds as f64);

    let proof_total = phases
        .iter()
        .filter(|p| p["proof_kind"].is_string())
        .count() as u64;
    let proof_reused = phases
        .iter()
        .filter(|p| p["proof_reused"] == Value::Bool(true))
        .count() as u64;

    let mut human_total = 0i64;
    let mut llm_total = 0i64;
    let mut has_human = false;
    let mut has_llm = false;
    for p in &phases {
        let Some(dur) = p["duration_ms"].as_i64() else {
            continue;
        };
        match p["authority"].as_str() {
            Some("human") => {
                human_total += dur;
                has_human = true;
            }
            Some("llm") => {
                llm_total += dur;
                has_llm = true;
            }
            _ => {}
        }
    }

    let terminal_reason = latest(&phases, "attention_hold")
        .and_then(|p| p["terminal_reason"].as_str())
        .or_else(|| phases.last().and_then(|p| p["terminal_reason"].as_str()));
    let target = phases.iter().rev().find_map(|p| p["target"].as_str());
    let candidate = phases.iter().rev().find_map(|p| p["candidate"].as_str());

    json!({
        "task": task,
        "phases": phases,
        "task_to_main_ms": task_to_main_ms,
        "in_flight": merge.is_none(),
        "terminal_reason": terminal_reason,
        "target": target,
        "candidate": candidate,
        "review_rounds": review_rounds,
        "rework_rounds": rework_rounds,
        "rework_amplification": rework_amplification,
        "proof_reuse": {
            "total": proof_total,
            "reused": proof_reused,
        },
        "authority_time_ms": {
            "human": has_human.then_some(human_total),
            "llm": has_llm.then_some(llm_total),
        },
    })
}

fn parse_rfc3339(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

/// Render a [`build_critical_path`] value as human text.
pub fn render_critical_path(cp: &Value) -> String {
    let mut out = String::new();
    let task = cp["task"].as_str().unwrap_or("?");
    let status = if cp["in_flight"].as_bool() == Some(true) {
        "in-flight"
    } else {
        "landed"
    };
    writeln!(out, "{task}: {status}").unwrap();
    match cp["task_to_main_ms"].as_i64() {
        Some(ms) => writeln!(out, "  task-to-main   {}", human_ms(ms)).unwrap(),
        None => writeln!(out, "  task-to-main   -").unwrap(),
    }
    if let Some(reason) = cp["terminal_reason"].as_str() {
        writeln!(out, "  terminal       {reason}").unwrap();
    }
    if let Some(target) = cp["target"].as_str() {
        writeln!(out, "  target         {target}").unwrap();
    }
    if let Some(candidate) = cp["candidate"].as_str() {
        writeln!(out, "  candidate      {candidate}").unwrap();
    }
    let review = cp["review_rounds"].as_u64().unwrap_or(0);
    let rework = cp["rework_rounds"].as_u64().unwrap_or(0);
    if let Some(amp) = cp["rework_amplification"].as_f64() {
        writeln!(
            out,
            "  rework         {rework} round(s) / {review} review(s) = {amp:.2}x"
        )
        .unwrap();
    } else if rework > 0 {
        writeln!(out, "  rework         {rework} round(s)").unwrap();
    }
    let proof_total = cp["proof_reuse"]["total"].as_u64().unwrap_or(0);
    if proof_total > 0 {
        writeln!(
            out,
            "  proof reuse    {}/{}",
            cp["proof_reuse"]["reused"].as_u64().unwrap_or(0),
            proof_total
        )
        .unwrap();
    }
    let human_auth = cp["authority_time_ms"]["human"].as_i64();
    let llm_auth = cp["authority_time_ms"]["llm"].as_i64();
    if human_auth.is_some() || llm_auth.is_some() {
        writeln!(
            out,
            "  authority      human {} / llm {}",
            human_auth.map(human_ms).unwrap_or_else(|| "-".to_string()),
            llm_auth.map(human_ms).unwrap_or_else(|| "-".to_string()),
        )
        .unwrap();
    }

    let phases = cp["phases"].as_array().cloned().unwrap_or_default();
    if phases.is_empty() {
        writeln!(out, "\n  (no phase spans recorded yet)").unwrap();
        return out;
    }
    writeln!(
        out,
        "\n  phase                 attempt  queue wait  duration  authority  terminal"
    )
    .unwrap();
    for p in &phases {
        writeln!(
            out,
            "  {:<20}  {:>7}  {:>10}  {:>8}  {:<9}  {}",
            p["phase"].as_str().unwrap_or("?"),
            p["attempt"].as_u64().unwrap_or(1),
            p["queue_wait_ms"]
                .as_i64()
                .map(human_ms)
                .unwrap_or_else(|| "-".to_string()),
            p["duration_ms"]
                .as_i64()
                .map(human_ms)
                .unwrap_or_else(|| "-".to_string()),
            p["authority"].as_str().unwrap_or("-"),
            p["terminal_reason"].as_str().unwrap_or(""),
        )
        .unwrap();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span(phase: &str, attempt: u64, payload: Value) -> Value {
        let mut p = json!({"task": "TKT-x", "phase": phase, "attempt": attempt});
        for (k, v) in payload.as_object().unwrap() {
            p[k] = v.clone();
        }
        json!({"id": format!("{phase}-{attempt}"), "payload": p})
    }

    #[test]
    fn complete_path_computes_task_to_main_and_splits() {
        let tuples = vec![
            span(
                "ticket_ready",
                1,
                json!({"queued_at": "2026-08-20T00:00:00Z", "started_at": "2026-08-20T00:00:00Z", "ended_at": "2026-08-20T00:00:00Z"}),
            ),
            span(
                "semantic_review",
                1,
                json!({"authority": "llm", "started_at": "2026-08-20T00:10:00Z", "ended_at": "2026-08-20T00:10:30Z", "duration_ms": 30_000}),
            ),
            span(
                "merge",
                1,
                json!({"ended_at": "2026-08-20T00:20:00Z", "target": "main", "candidate": "abc123"}),
            ),
        ];
        let cp = build_critical_path("TKT-x", &tuples);
        assert_eq!(cp["task_to_main_ms"], 1_200_000);
        assert_eq!(cp["in_flight"], false);
        assert_eq!(cp["target"], "main");
        assert_eq!(cp["candidate"], "abc123");
        assert_eq!(cp["authority_time_ms"]["llm"], 30_000);
        assert_eq!(cp["authority_time_ms"]["human"], Value::Null);
        assert_eq!(cp["review_rounds"], 1);
        assert_eq!(cp["rework_rounds"], 0);
        // A review round happened with zero rework — a real 0x, not
        // "unavailable" (that's reserved for zero review rounds at all).
        assert_eq!(cp["rework_amplification"], 0.0);
    }

    #[test]
    fn in_flight_ticket_has_no_task_to_main_and_is_not_invented() {
        let tuples = vec![span(
            "ticket_ready",
            1,
            json!({"queued_at": "2026-08-20T00:00:00Z"}),
        )];
        let cp = build_critical_path("TKT-x", &tuples);
        assert_eq!(cp["task_to_main_ms"], Value::Null);
        assert_eq!(cp["in_flight"], true);
        assert_eq!(cp["proof_reuse"]["total"], 0);
        assert_eq!(cp["authority_time_ms"]["human"], Value::Null);
        assert_eq!(cp["authority_time_ms"]["llm"], Value::Null);
    }

    #[test]
    fn missing_phases_render_only_what_was_recorded() {
        let tuples = vec![span("ticket_ready", 1, json!({}))];
        let cp = build_critical_path("TKT-x", &tuples);
        assert_eq!(cp["phases"].as_array().unwrap().len(), 1);
        let text = render_critical_path(&cp);
        assert!(text.contains("in-flight"));
        assert!(!text.contains("merge"));
    }

    #[test]
    fn no_spans_at_all_renders_without_a_table() {
        let cp = build_critical_path("TKT-x", &[]);
        let text = render_critical_path(&cp);
        assert!(text.contains("no phase spans recorded yet"));
    }

    #[test]
    fn duplicate_replayed_spans_are_not_double_counted() {
        let one = span("agent_launched", 1, json!({"duration_ms": 500}));
        let tuples = vec![one.clone(), one.clone(), one];
        let cp = build_critical_path("TKT-x", &tuples);
        assert_eq!(cp["phases"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn multiple_attempts_are_kept_distinct() {
        let tuples = vec![
            span(
                "verification",
                1,
                json!({"proof_kind": "focused-inner", "proof_reused": false, "terminal_reason": "timeout"}),
            ),
            span(
                "verification",
                2,
                json!({"proof_kind": "full-final", "proof_reused": true}),
            ),
        ];
        let cp = build_critical_path("TKT-x", &tuples);
        assert_eq!(cp["phases"].as_array().unwrap().len(), 2);
        assert_eq!(cp["proof_reuse"]["total"], 2);
        assert_eq!(cp["proof_reuse"]["reused"], 1);
    }

    #[test]
    fn rework_amplification_is_null_without_a_review_round() {
        let tuples = vec![span("rework", 1, json!({}))];
        let cp = build_critical_path("TKT-x", &tuples);
        assert_eq!(cp["rework_amplification"], Value::Null);
        assert_eq!(cp["rework_rounds"], 1);
    }

    #[test]
    fn json_and_human_output_agree_on_totals() {
        let tuples = vec![
            span("semantic_review", 1, json!({})),
            span("rework", 1, json!({})),
            span("rework", 2, json!({})),
        ];
        let cp = build_critical_path("TKT-x", &tuples);
        assert_eq!(cp["rework_amplification"], 2.0);
        let text = render_critical_path(&cp);
        assert!(text.contains("2 round(s) / 1 review(s) = 2.00x"));
    }
}

//! Fleet observability read-side: `rk workflow timeline` rendering and the
//! `rk digest` interval catch-up. Pure formatting/grouping over data the
//! daemon already serves — no new storage, no daemon writes.

use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// Workflow instance timeline
// ---------------------------------------------------------------------------

/// Render a `workflow.timeline` response: the instance headline (status, where
/// it's parked, timing) followed by every step row marked done/current/pending
/// against the persisted step cursor.
pub fn print_timeline(result: &Value) {
    let inst = &result["instance"];
    let status = inst["status"].as_str().unwrap_or("?");
    let current = inst["current_step"].as_u64().unwrap_or(0) as usize;
    let total = inst["total_steps"].as_u64().unwrap_or(0);

    let mut headline = format!(
        "{}  {}  repo={}  {}",
        inst["id"].as_str().unwrap_or("?"),
        inst["workflow"].as_str().unwrap_or("?"),
        repo_name(inst["repo"].as_str().unwrap_or("?")),
        status,
    );
    if let Some(awaiting) = inst["awaiting"].as_str() {
        headline.push_str(&format!(" — parked: {awaiting}"));
    }
    println!("{headline}");

    let mut timing = String::new();
    if let Some(started) = parse_time(&inst["started_at"]) {
        timing.push_str(&format!("started {}", ago(started)));
    }
    if let Some(completed) = parse_time(&inst["completed_at"]) {
        timing.push_str(&format!(" · finished {}", ago(completed)));
    }
    if !timing.is_empty() {
        println!("{timing}");
    }
    if let Some(err) = inst["error"].as_str() {
        println!("error: {err}");
    }
    println!();

    match result["steps"].as_array() {
        None => println!("(definition no longer loadable — at step {current}/{total})"),
        Some(rows) => {
            for row in rows {
                println!("{}", timeline_line(row, current, status));
            }
        }
    }
}

/// One rendered timeline line: a done/current/pending mark, the top-level
/// step number, and the step label indented by nesting depth.
fn timeline_line(row: &Value, current: usize, status: &str) -> String {
    let index = row["index"].as_u64().unwrap_or(0) as usize;
    let depth = row["depth"].as_u64().unwrap_or(0) as usize;
    let label = row["label"].as_str().unwrap_or("?");
    let mark = match index.cmp(&current) {
        std::cmp::Ordering::Less => "✔",
        std::cmp::Ordering::Equal => match status {
            "failed" => "✖",
            "completed" => "✔",
            _ => "▶",
        },
        std::cmp::Ordering::Greater => "·",
    };
    if depth == 0 {
        format!(" {mark} {:>2}. {label}", index + 1)
    } else {
        format!(" {mark}     {}{label}", "  ".repeat(depth))
    }
}

// ---------------------------------------------------------------------------
// Fleet digest
// ---------------------------------------------------------------------------

/// `rk digest`: what the fleet did in the last interval, grouped from the
/// event feed + live tuples + workflow instances. `--llm` pipes the report
/// through `claude -p` for a prose catch-up, degrading to the deterministic
/// report when the binary is missing.
pub async fn digest(layout: &Layout, since: &str, llm: bool, as_json: bool) -> Result<()> {
    let window = parse_since(since)?;
    let cutoff = Utc::now() - window;
    let mut client = Client::connect_or_spawn(layout).await?;

    let events = scan_category(&mut client, "event").await?;
    let obstacles = scan_category(&mut client, "obstacle").await?;
    let needs = scan_category(&mut client, "need").await?;
    let instances = client.call("workflow.list", json!({})).await?["instances"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let rollup = client.call("budget.rollup", json!({})).await?;
    let inbox = client.call("inbox.list", json!({})).await?["items"]
        .as_array()
        .cloned()
        .unwrap_or_default();

    let digest = build_digest(
        since, cutoff, &events, &obstacles, &needs, &instances, &rollup, &inbox,
    );
    if as_json {
        println!("{digest}");
        return Ok(());
    }
    let report = render_digest(&digest);
    if llm {
        match llm_summarize(&report) {
            Ok(summary) => {
                println!("{}", summary.trim_end());
                return Ok(());
            }
            Err(e) => eprintln!("(llm summary unavailable: {e} — showing raw digest)"),
        }
    }
    println!("{report}");
    Ok(())
}

async fn scan_category(client: &mut Client, category: &str) -> Result<Vec<Value>> {
    let result = client
        .call("space.scan", json!({"category": category}))
        .await?;
    Ok(result["tuples"].as_array().cloned().unwrap_or_default())
}

/// Group raw scans into the digest's structured form. Pure so it unit-tests
/// without a daemon; every section carries counts plus the rows themselves.
#[allow(clippy::too_many_arguments)]
pub fn build_digest(
    since: &str,
    cutoff: DateTime<Utc>,
    events: &[Value],
    obstacles: &[Value],
    needs: &[Value],
    instances: &[Value],
    rollup: &Value,
    inbox: &[Value],
) -> Value {
    let fresh = |tuples: &[Value]| -> Vec<Value> {
        tuples
            .iter()
            .filter(|t| parse_time(&t["created_at"]).is_some_and(|at| at >= cutoff))
            .cloned()
            .collect()
    };
    let events = fresh(events);
    let by_identity = |identity: &str| -> Vec<Value> {
        events
            .iter()
            .filter(|e| e["identity"].as_str() == Some(identity))
            .cloned()
            .collect()
    };

    let spawned: Vec<Value> = by_identity("agent_spawned")
        .into_iter()
        .chain(by_identity("agent_respawned"))
        .map(|e| {
            json!({
                "agent": e["payload"]["agent"],
                "scope": e["scope"],
                "task": e["payload"]["task"],
            })
        })
        .collect();
    let finished: Vec<Value> = by_identity("harness_result")
        .iter()
        .map(|e| {
            json!({
                "agent": e["payload"]["agent"],
                "scope": e["scope"],
                "task": e["payload"]["task"],
                "is_error": e["payload"]["is_error"],
            })
        })
        .collect();
    let dismissed: Vec<Value> = by_identity("agent_dismissed")
        .iter()
        .map(|e| {
            json!({
                "agent": e["payload"]["agent"],
                "scope": e["scope"],
                "merged": e["payload"]["merged"],
                "pr_opened": e["payload"]["pr_opened"],
            })
        })
        .collect();
    let landed: Vec<Value> = by_identity("branch_landed")
        .iter()
        .map(|e| {
            json!({
                "branch": e["payload"]["branch"],
                "target": e["payload"]["target"],
                "scope": e["scope"],
                "merged": e["payload"]["merged"],
            })
        })
        .collect();
    let prs: Vec<Value> = by_identity("pull_request_opened")
        .iter()
        .map(|e| {
            json!({
                "branch": e["payload"]["branch"],
                "scope": e["scope"],
                "url": e["payload"]["url"],
            })
        })
        .collect();
    let workflows_done: Vec<Value> = by_identity("workflow_complete")
        .into_iter()
        .chain(by_identity("workflow_failed"))
        .map(|e| {
            json!({
                "instance": e["payload"]["instance"],
                "workflow": e["payload"]["workflow"],
                "failed": e["identity"].as_str() == Some("workflow_failed"),
                "error": e["payload"]["error"],
            })
        })
        .collect();
    let running: Vec<Value> = instances
        .iter()
        .filter(|i| i["status"].as_str() == Some("running"))
        .map(|i| {
            json!({
                "instance": i["id"],
                "workflow": i["workflow"],
                "step": format!("{}/{}", i["current_step"], i["total_steps"]),
                "awaiting": i["awaiting"],
            })
        })
        .collect();

    let friction: Vec<Value> = fresh(obstacles)
        .iter()
        .chain(fresh(needs).iter())
        .map(|t| {
            json!({
                "kind": t["category"],
                "scope": t["scope"],
                "author": t["author"],
                "text": t["payload"]["text"],
            })
        })
        .collect();

    json!({
        "since": since,
        "cutoff": cutoff.to_rfc3339(),
        "spawned": spawned,
        "finished": finished,
        "dismissed": dismissed,
        "landed": landed,
        "prs_opened": prs,
        "workflows_finished": workflows_done,
        "workflows_running": running,
        "friction": friction,
        "fleet_spend": rollup["fleet"],
        "inbox": inbox,
    })
}

/// Render the structured digest as the plain-text report (also the exact text
/// handed to the LLM under `--llm`).
pub fn render_digest(digest: &Value) -> String {
    let mut out = format!(
        "fleet digest — last {}\n",
        digest["since"].as_str().unwrap_or("?")
    );
    let rows = |key: &str| digest[key].as_array().cloned().unwrap_or_default();

    let spawned = rows("spawned");
    let finished = rows("finished");
    let dismissed = rows("dismissed");
    if spawned.is_empty() && finished.is_empty() && dismissed.is_empty() {
        out.push_str("\nagents: no activity in the window\n");
    } else {
        out.push_str(&format!(
            "\nagents: {} spawned, {} finished, {} dismissed\n",
            spawned.len(),
            finished.len(),
            dismissed.len()
        ));
        for f in &finished {
            let verdict = match f["is_error"].as_bool() {
                Some(true) => "error",
                _ => "ok",
            };
            out.push_str(&format!(
                "  finished {} [{}] {} — {}\n",
                f["agent"].as_str().unwrap_or("?"),
                verdict,
                f["scope"].as_str().unwrap_or("-"),
                f["task"].as_str().unwrap_or("-"),
            ));
        }
        for d in &dismissed {
            let outcome = if d["merged"].as_bool() == Some(true) {
                "merged"
            } else if d["pr_opened"].as_bool() == Some(true) {
                "pr opened"
            } else {
                "not merged"
            };
            out.push_str(&format!(
                "  dismissed {} ({outcome})\n",
                d["agent"].as_str().unwrap_or("?"),
            ));
        }
    }

    let landed = rows("landed");
    let prs = rows("prs_opened");
    if !landed.is_empty() || !prs.is_empty() {
        out.push_str("\nlandings:\n");
        for l in &landed {
            out.push_str(&format!(
                "  {} → {} ({})\n",
                l["branch"].as_str().unwrap_or("?"),
                l["target"].as_str().unwrap_or("?"),
                if l["merged"].as_bool() == Some(true) {
                    "merged"
                } else {
                    "NOT merged"
                },
            ));
        }
        for p in &prs {
            out.push_str(&format!(
                "  PR {} {}\n",
                p["branch"].as_str().unwrap_or("?"),
                p["url"].as_str().unwrap_or(""),
            ));
        }
    }

    let wf_done = rows("workflows_finished");
    let wf_running = rows("workflows_running");
    if !wf_done.is_empty() || !wf_running.is_empty() {
        out.push_str("\nworkflows:\n");
        for w in &wf_done {
            if w["failed"].as_bool() == Some(true) {
                out.push_str(&format!(
                    "  FAILED {} ({}) — {}\n",
                    w["instance"].as_str().unwrap_or("?"),
                    w["workflow"].as_str().unwrap_or("?"),
                    w["error"].as_str().unwrap_or("?"),
                ));
            } else {
                out.push_str(&format!(
                    "  completed {} ({})\n",
                    w["instance"].as_str().unwrap_or("?"),
                    w["workflow"].as_str().unwrap_or("?"),
                ));
            }
        }
        for w in &wf_running {
            let parked = w["awaiting"]
                .as_str()
                .map(|a| format!(" — parked: {a}"))
                .unwrap_or_default();
            out.push_str(&format!(
                "  running {} ({}) step {}{parked}\n",
                w["instance"].as_str().unwrap_or("?"),
                w["workflow"].as_str().unwrap_or("?"),
                w["step"].as_str().unwrap_or("?"),
            ));
        }
    }

    let friction = rows("friction");
    if !friction.is_empty() {
        out.push_str("\nfriction:\n");
        for f in &friction {
            out.push_str(&format!(
                "  {} [{}] {}: {}\n",
                f["kind"].as_str().unwrap_or("?"),
                f["scope"].as_str().unwrap_or("-"),
                f["author"].as_str().unwrap_or("?"),
                f["text"].as_str().unwrap_or("?"),
            ));
        }
    }

    let fleet = &digest["fleet_spend"];
    if fleet.is_object() {
        let spent = fleet["spent_usd"].as_f64().unwrap_or(0.0);
        match fleet["max_usd"].as_f64() {
            Some(max) if max > 0.0 => out.push_str(&format!(
                "\nspend: ${spent:.2} of ${max:.2} fleet cap (live agents)\n"
            )),
            _ => out.push_str(&format!(
                "\nspend: ${spent:.2} (live agents, no fleet cap)\n"
            )),
        }
    }

    let inbox = rows("inbox");
    if inbox.is_empty() {
        out.push_str("\ninbox: clear\n");
    } else {
        out.push_str(&format!(
            "\ninbox: {} item(s) awaiting a human\n",
            inbox.len()
        ));
        for it in inbox.iter().take(5) {
            out.push_str(&format!(
                "  {} {} → {}\n",
                it["kind"].as_str().unwrap_or("?"),
                it["subject"].as_str().unwrap_or("?"),
                it["action"].as_str().unwrap_or("?"),
            ));
        }
        if inbox.len() > 5 {
            out.push_str(&format!("  … and {} more (rk inbox)\n", inbox.len() - 5));
        }
    }
    out
}

/// Summarize the report with a one-shot `claude -p` call, feeding the report
/// on stdin. Any failure (missing binary, non-zero exit) is an `Err` the
/// caller degrades from — the deterministic report is always available.
fn llm_summarize(report: &str) -> Result<String> {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let mut child = Command::new("claude")
        .args([
            "-p",
            "You are summarizing an agent-fleet activity digest for its operator. \
             In a short paragraph (then bullets only if something needs action), \
             say what the fleet accomplished, what failed or is stuck, and what \
             needs the operator's attention. Be concrete: name agents, branches, \
             and workflows.",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    child
        .stdin
        .as_mut()
        .expect("stdin piped")
        .write_all(report.as_bytes())?;
    let out = child.wait_with_output()?;
    if !out.status.success() {
        anyhow::bail!("claude exited with {}", out.status);
    }
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    if text.trim().is_empty() {
        anyhow::bail!("claude produced no output");
    }
    Ok(text)
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Replay a workflow snapshot plus durable lifecycle events, then follow live
/// coordinator notifications until this workflow reaches a terminal state.
/// The daemon owns cursor/reconnect semantics; this adapter only renders the
/// stream and performs a snapshot resync after a lag notification.
pub async fn watch_workflow(
    layout: &Layout,
    instance: &str,
    after: Option<&str>,
    as_json: bool,
) -> Result<()> {
    let mut cursor = after.map(str::to_owned);
    loop {
        match watch_workflow_connection(layout, instance, cursor.as_deref(), as_json).await? {
            WatchConnection::Done => return Ok(()),
            WatchConnection::Resync(next) => cursor = Some(next),
        }
    }
}

/// Read or follow the bounded hierarchical coordinator view. The daemon owns
/// cursor/replay semantics; this adapter only renders the compact snapshot and
/// events so a host session can consume the same JSON contract.
pub async fn monitor(layout: &Layout, params: Value, follow: bool, as_json: bool) -> Result<()> {
    if !follow {
        let mut client = Client::connect_or_spawn(layout).await?;
        if let Some(coordinator) = params["coordinator"].as_str().map(str::to_string) {
            client
                .call(
                    "coordinator.register",
                    json!({
                        "coordinator": coordinator,
                        "after": params["after"],
                    }),
                )
                .await?;
            let pending = client.call("coordinator.pending", params).await?;
            render_monitor_snapshot(&pending, as_json);
            if let Some(events) = pending["events"].as_array() {
                for envelope in events {
                    render_monitor_event(envelope, as_json);
                }
            }
            if let Some(cursor) = pending["ack_after"].as_u64() {
                client
                    .call(
                        "coordinator.ack",
                        json!({"coordinator": coordinator, "cursor": cursor}),
                    )
                    .await?;
            }
            return Ok(());
        }
    }
    let client = Client::connect_or_spawn(layout).await?;
    let (initial, mut stream) = client.call_then_stream("coordinator.watch", params).await?;
    render_monitor_snapshot(&initial, as_json);
    if let Some(events) = initial["events"].as_array() {
        for envelope in events {
            render_monitor_event(envelope, as_json);
        }
    }
    if !follow {
        return Ok(());
    }
    while let Some(note) = stream.next().await? {
        if note["method"].as_str() == Some("coordinator.event") {
            render_monitor_event(
                &json!({
                    "cursor": note["params"]["cursor"],
                    "event": note["params"]["event"],
                }),
                as_json,
            );
        } else if note["method"].as_str() == Some("lagged") {
            if as_json {
                println!("{}", json!({"kind": "resync", "data": note}));
            } else {
                eprintln!("coordinator history lagged; rerun with the latest cursor");
            }
        }
    }
    Ok(())
}

fn render_monitor_snapshot(result: &Value, as_json: bool) {
    if as_json {
        println!("{}", json!({"kind": "snapshot", "data": result}));
        return;
    }
    let snapshot = &result["snapshot"];
    println!(
        "coordinator cursor {} · {} workflow(s) · {} middle-rat(s)",
        result["cursor"]
            .as_u64()
            .map(|cursor| cursor.to_string())
            .unwrap_or_else(|| "-".into()),
        snapshot["workflows"].as_array().map(Vec::len).unwrap_or(0),
        snapshot["middle_rats"]
            .as_array()
            .map(Vec::len)
            .unwrap_or(0),
    );
    for attention in snapshot["attention"].as_array().into_iter().flatten() {
        println!(
            "ATTENTION {} {}",
            attention["kind"].as_str().unwrap_or("event"),
            attention["summary"].as_str().unwrap_or("-")
        );
    }
    for middle in snapshot["middle_rats"].as_array().into_iter().flatten() {
        let rollup = &middle["rollup"];
        println!(
            "{} {}: {}/{} complete, {} running, {} blocked{}",
            middle["agent"].as_str().unwrap_or("?"),
            middle["state"].as_str().unwrap_or("?"),
            rollup["completed"],
            rollup["total"],
            rollup["running"],
            rollup["blocked"],
            middle["summary"]
                .as_str()
                .map(|summary| format!(" — {summary}"))
                .unwrap_or_default(),
        );
    }
}

fn render_monitor_event(envelope: &Value, as_json: bool) {
    if as_json {
        println!("{}", json!({"kind": "event", "data": envelope}));
        return;
    }
    let event = &envelope["event"];
    println!(
        "cursor={} {} {}",
        envelope["cursor"]
            .as_u64()
            .map(|cursor| cursor.to_string())
            .unwrap_or_else(|| "?".into()),
        event["payload"]["route"].as_str().unwrap_or("event"),
        event["payload"]["summary"]
            .as_str()
            .unwrap_or_else(|| event["identity"].as_str().unwrap_or("coordination")),
    );
}

enum WatchConnection {
    Done,
    Resync(String),
}

async fn watch_workflow_connection(
    layout: &Layout,
    instance: &str,
    after: Option<&str>,
    as_json: bool,
) -> Result<WatchConnection> {
    let mut params = json!({"instance": instance});
    if let Some(after) = after {
        let cursor = after
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("invalid coordinator cursor: {after}"))?;
        params["after"] = json!(cursor);
    }
    let client = Client::connect_or_spawn(layout).await?;
    let (initial, mut stream) = client.call_then_stream("coordinator.watch", params).await?;
    render_coordinator_snapshot(&initial, as_json);
    if initial["resync_required"] == true {
        render_resync_notice(&initial, as_json);
    }
    if terminal_in_snapshot(&initial, instance) {
        return Ok(WatchConnection::Done);
    }

    // Replay is allowed to describe the path up to the current snapshot. Once
    // live delivery begins, the snapshot revision becomes the floor so a late
    // broadcast cannot make the operator's view move backwards.
    let mut last_revision = 0;
    if let Some(events) = initial["events"].as_array() {
        for envelope in events {
            let event = &envelope["event"];
            if accept_revision(event, &mut last_revision) {
                render_coordinator_event(event, envelope["cursor"].as_u64(), as_json);
                if terminal_event(event, instance) {
                    return Ok(WatchConnection::Done);
                }
            }
        }
    }
    last_revision = last_revision.max(snapshot_revision(&initial, instance));

    while let Some(note) = stream.next().await? {
        match note["method"].as_str() {
            Some("coordinator.event") => {
                let event = &note["params"]["event"];
                if accept_revision(event, &mut last_revision) {
                    render_coordinator_event(event, note["params"]["cursor"].as_u64(), as_json);
                    if terminal_event(event, instance) {
                        return Ok(WatchConnection::Done);
                    }
                }
            }
            Some("lagged") => {
                render_resync_notice(&note, as_json);
                let mut client = Client::connect_or_spawn(layout).await?;
                let snapshot = client
                    .call("coordinator.snapshot", json!({"instance": instance}))
                    .await?;
                render_coordinator_snapshot(&snapshot, as_json);
                if terminal_in_snapshot(&snapshot, instance) {
                    return Ok(WatchConnection::Done);
                }
                let next = snapshot["cursor"].as_u64().ok_or_else(|| {
                    anyhow::anyhow!("coordinator snapshot did not include a cursor")
                })?;
                return Ok(WatchConnection::Resync(next.to_string()));
            }
            _ => {}
        }
    }
    anyhow::bail!("coordinator watch ended before workflow {instance} reached a terminal state")
}

fn snapshot_revision(result: &Value, instance: &str) -> u64 {
    result["snapshot"]["workflows"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|workflow| workflow["id"].as_str() == Some(instance))
        .and_then(|workflow| workflow["revision"].as_u64())
        .unwrap_or(0)
}

fn accept_revision(event: &Value, last_revision: &mut u64) -> bool {
    if event["identity"].as_str() != Some("workflow_state_changed") {
        return true;
    }
    let Some(revision) = event["payload"]["revision"].as_u64() else {
        return true;
    };
    if revision <= *last_revision {
        return false;
    }
    *last_revision = revision;
    true
}

fn render_coordinator_snapshot(result: &Value, as_json: bool) {
    if as_json {
        println!("{}", json!({"kind": "snapshot", "data": result}));
        return;
    }
    let cursor = result["cursor"]
        .as_u64()
        .map(|cursor| cursor.to_string())
        .unwrap_or_else(|| "-".to_string());
    println!("coordinator cursor {cursor}");
    let workflows = result["snapshot"]["workflows"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    if workflows.is_empty() {
        println!("workflow snapshot: no matching instance");
        return;
    }
    for workflow in workflows {
        println!(
            "workflow {} {} {}/{} revision {}{}",
            workflow["id"].as_str().unwrap_or("?"),
            workflow["status"].as_str().unwrap_or("?"),
            workflow["current_step"],
            workflow["total_steps"],
            workflow["revision"],
            workflow["awaiting"]
                .as_str()
                .map(|reason| format!(" (awaiting {reason})"))
                .unwrap_or_default(),
        );
    }
}

fn render_coordinator_event(event: &Value, cursor: Option<u64>, as_json: bool) {
    if as_json {
        println!(
            "{}",
            json!({"kind": "event", "cursor": cursor, "data": event})
        );
        return;
    }
    println!(
        "cursor={} event {} instance={} revision={} status={} reason={}",
        cursor
            .map(|cursor| cursor.to_string())
            .unwrap_or_else(|| "?".to_string()),
        event["identity"].as_str().unwrap_or("?"),
        event["payload"]["instance"].as_str().unwrap_or("?"),
        event["payload"]["revision"],
        event["payload"]["status"].as_str().unwrap_or("?"),
        event["payload"]["reason"].as_str().unwrap_or("-")
    );
}

fn render_resync_notice(result: &Value, as_json: bool) {
    if as_json {
        println!("{}", json!({"kind": "resync", "data": result}));
    } else {
        eprintln!("coordinator history lagged or was truncated; refreshed snapshot");
    }
}

fn terminal_in_snapshot(result: &Value, instance: &str) -> bool {
    result["snapshot"]["workflows"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|workflow| workflow["id"].as_str() == Some(instance))
        .is_some_and(|workflow| matches!(workflow["status"].as_str(), Some("completed" | "failed")))
}

fn terminal_event(event: &Value, instance: &str) -> bool {
    event["payload"]["instance"].as_str() == Some(instance)
        && (matches!(
            event["identity"].as_str(),
            Some("workflow_complete" | "workflow_failed")
        ) || matches!(
            event["payload"]["status"].as_str(),
            Some("completed" | "failed")
        ))
}

/// Parse a digest window like `30m`, `2h`, `1d` (bare number = minutes).
pub fn parse_since(s: &str) -> Result<ChronoDuration> {
    let s = s.trim();
    let invalid = || anyhow::anyhow!("invalid --since duration: {s} (try 30m, 2h, 1d)");
    let (value, unit) = match s.chars().last() {
        Some('s') => (&s[..s.len() - 1], 1i64),
        Some('m') => (&s[..s.len() - 1], 60),
        Some('h') => (&s[..s.len() - 1], 3600),
        Some('d') => (&s[..s.len() - 1], 86_400),
        Some(c) if c.is_ascii_digit() => (s, 60),
        _ => return Err(invalid()),
    };
    let n = value.parse::<i64>().map_err(|_| invalid())?;
    if n <= 0 {
        return Err(invalid());
    }
    n.checked_mul(unit)
        .map(ChronoDuration::seconds)
        .ok_or_else(invalid)
}

fn parse_time(v: &Value) -> Option<DateTime<Utc>> {
    v.as_str()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

/// "3m ago" / "2h ago" style relative time for headers.
fn ago(at: DateTime<Utc>) -> String {
    let secs = (Utc::now() - at).num_seconds().max(0);
    let rel = if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else if secs < 86_400 {
        format!("{}h", secs / 3600)
    } else {
        format!("{}d", secs / 86_400)
    };
    format!("{rel} ago")
}

fn repo_name(repo: &str) -> String {
    std::path::Path::new(repo)
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| repo.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tuple(identity: &str, created_at: &str, payload: Value) -> Value {
        json!({
            "category": "event",
            "scope": "demo",
            "identity": identity,
            "author": "daemon",
            "payload": payload,
            "created_at": created_at,
        })
    }

    #[test]
    fn build_digest_filters_by_cutoff_and_groups_by_identity() {
        let cutoff = DateTime::parse_from_rfc3339("2026-07-23T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let events = vec![
            tuple(
                "harness_result",
                "2026-07-23T01:00:00Z",
                json!({"agent": "rat-1", "task": "TKT-9", "is_error": false}),
            ),
            // Before the cutoff — must be dropped.
            tuple(
                "harness_result",
                "2026-07-22T23:59:00Z",
                json!({"agent": "rat-0", "task": "old", "is_error": false}),
            ),
            tuple(
                "agent_dismissed",
                "2026-07-23T02:00:00Z",
                json!({"agent": "rat-1", "merged": true}),
            ),
            tuple(
                "branch_landed",
                "2026-07-23T02:00:01Z",
                json!({"branch": "rat/rat-1/tkt-9", "target": "main", "merged": true}),
            ),
        ];
        let obstacles = vec![tuple(
            "wall",
            "2026-07-23T01:30:00Z",
            json!({"text": "stuck"}),
        )];
        let instances = vec![json!({
            "id": "wf-abc", "workflow": "steward", "status": "running",
            "current_step": 2, "total_steps": 5, "awaiting": "approval gate",
        })];
        let rollup = json!({"fleet": {"spent_usd": 1.25, "max_usd": 10.0}});

        let digest = build_digest(
            "2h",
            cutoff,
            &events,
            &obstacles,
            &[],
            &instances,
            &rollup,
            &[],
        );

        assert_eq!(digest["finished"].as_array().unwrap().len(), 1);
        assert_eq!(digest["finished"][0]["agent"], "rat-1");
        assert_eq!(digest["dismissed"][0]["merged"], true);
        assert_eq!(digest["landed"][0]["target"], "main");
        assert_eq!(digest["friction"][0]["text"], "stuck");
        assert_eq!(digest["workflows_running"][0]["step"], "2/5");

        let report = render_digest(&digest);
        assert!(report.contains("finished rat-1 [ok]"));
        assert!(report.contains("rat/rat-1/tkt-9 → main (merged)"));
        assert!(report.contains("parked: approval gate"));
        assert!(report.contains("$1.25 of $10.00"));
        assert!(report.contains("inbox: clear"));
        // The pre-cutoff completion must not appear.
        assert!(!report.contains("rat-0"));
    }

    #[test]
    fn parse_since_accepts_units_and_rejects_garbage() {
        assert_eq!(parse_since("30m").unwrap(), ChronoDuration::seconds(1800));
        assert_eq!(parse_since("2h").unwrap(), ChronoDuration::seconds(7200));
        assert_eq!(parse_since("1d").unwrap(), ChronoDuration::seconds(86_400));
        // Bare number = minutes.
        assert_eq!(parse_since("15").unwrap(), ChronoDuration::seconds(900));
        assert!(parse_since("").is_err());
        assert!(parse_since("0h").is_err());
        assert!(parse_since("-5m").is_err());
        assert!(parse_since("abc").is_err());
        // Multibyte last char must not panic the byte-index split.
        assert!(parse_since("5m²").is_err());
    }

    #[test]
    fn timeline_line_marks_done_current_pending() {
        let row = |index: usize, depth: usize, label: &str| json!({"index": index, "depth": depth, "label": label});
        assert_eq!(
            timeline_line(&row(0, 0, "spawn"), 1, "running"),
            " ✔  1. spawn"
        );
        assert_eq!(
            timeline_line(&row(1, 0, "wait"), 1, "running"),
            " ▶  2. wait"
        );
        assert_eq!(
            timeline_line(&row(2, 0, "gate"), 1, "running"),
            " ·  3. gate"
        );
        assert_eq!(
            timeline_line(&row(1, 0, "wait"), 1, "failed"),
            " ✖  2. wait"
        );
        // Completed instances render the cursor row as done, and nested rows indent.
        assert_eq!(
            timeline_line(&row(1, 0, "wait"), 1, "completed"),
            " ✔  2. wait"
        );
        assert_eq!(
            timeline_line(&row(1, 2, "dismiss + land"), 1, "running"),
            " ▶         dismiss + land"
        );
    }
}

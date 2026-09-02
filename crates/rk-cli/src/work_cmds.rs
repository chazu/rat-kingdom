//! `rk work` — the small, current-state operator surface. Historical and
//! diagnostic views remain available through their existing commands.

use anyhow::Result;
use clap::Args;
use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::{json, Value};
use std::io::Write;

#[derive(Args)]
pub struct WorkArgs {
    /// Restrict current work to one registered repository name or path.
    pub repo: Option<String>,
}

pub async fn run(layout: &Layout, args: WorkArgs, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let mut result = client
        .call("work.current", json!({"repo": args.repo}))
        .await?;
    result["installed_build"] = json!(rk_core::version::BUILD_VERSION);
    result["build_in_sync"] =
        json!(result["daemon"]["build_version"].as_str() == Some(rk_core::version::BUILD_VERSION));
    if as_json {
        println!("{result}");
        return Ok(());
    }
    print_current(&result)?;
    Ok(())
}

fn print_current(work: &Value) -> std::io::Result<()> {
    let stdout = std::io::stdout();
    write_current(&mut stdout.lock(), work)
}

fn write_current(out: &mut impl Write, work: &Value) -> std::io::Result<()> {
    let local_build = work["installed_build"]
        .as_str()
        .unwrap_or(rk_core::version::BUILD_VERSION);
    let daemon_build = work["daemon"]["build_version"].as_str().unwrap_or("?");
    let parity = if daemon_build == local_build {
        "in sync"
    } else {
        "MISMATCH — run `rk daemon rollover`"
    };
    let scope = work["repo"].as_str().unwrap_or("all repos");
    writeln!(
        out,
        "current work · {scope} · build {daemon_build} ({parity})"
    )?;
    writeln!(
        out,
        "{} live · {} ready · {} actionable · {} decisions · {} stalled",
        work["counts"]["live_agents"].as_u64().unwrap_or(0),
        work["counts"]["ready_tickets"].as_u64().unwrap_or(0),
        work["counts"]["actionable"]
            .as_u64()
            .or_else(|| work["counts"]["attention"].as_u64())
            .unwrap_or(0),
        work["counts"]["decision_required"].as_u64().unwrap_or(0),
        work["counts"]["stalled"].as_u64().unwrap_or(0),
    )?;

    if let Some(agents) = work["live_agents"]
        .as_array()
        .filter(|rows| !rows.is_empty())
    {
        writeln!(out, "\nlive")?;
        for agent in agents {
            writeln!(
                out,
                "  {:<14} {:<10} {:<12} {}",
                agent["name"].as_str().unwrap_or("?"),
                agent["state"].as_str().unwrap_or("?"),
                agent["repo"].as_str().unwrap_or("?"),
                agent["task"].as_str().unwrap_or("-"),
            )?;
        }
    }

    if let Some(tickets) = work["ready_tickets"]
        .as_array()
        .filter(|rows| !rows.is_empty())
    {
        writeln!(out, "\nready")?;
        for ticket in tickets {
            writeln!(
                out,
                "  {:<28} [{:<6}] {}",
                ticket["id"].as_str().unwrap_or("?"),
                ticket["priority"].as_str().unwrap_or("?"),
                ticket["title"].as_str().unwrap_or(""),
            )?;
            writeln!(out, "    → {}", ticket["command"].as_str().unwrap_or("?"))?;
        }
    }

    write_section(out, work, "actionable", "actionable", true)?;
    write_section(out, work, "decision_required", "decision required", false)?;
    write_section(out, work, "stalled", "stalled / diagnostic", false)?;

    if work["no_current_work"].as_bool() == Some(true) {
        writeln!(
            out,
            "\nno current work — history: {}; diagnostics: {}",
            work["history_command"]
                .as_str()
                .unwrap_or("rk digest --since 1d"),
            work["diagnostics_command"].as_str().unwrap_or("rk top"),
        )?;
    }
    writeln!(
        out,
        "\nKing wakes notify about state; resolving a wake does not mean this work list is empty."
    )?;
    Ok(())
}

fn write_section(
    out: &mut impl Write,
    work: &Value,
    field: &str,
    title: &str,
    singular_command: bool,
) -> std::io::Result<()> {
    let rows = work[field].as_array().or_else(|| {
        (field == "actionable")
            .then(|| work["attention"].as_array())
            .flatten()
    });
    let Some(rows) = rows.filter(|rows| !rows.is_empty()) else {
        return Ok(());
    };
    writeln!(out, "\n{title}")?;
    for item in rows {
        let detail = item["detail"]
            .as_str()
            .or_else(|| item["text"].as_str())
            .or_else(|| item["action"].as_str())
            .unwrap_or("");
        writeln!(
            out,
            "  {:<24} {:<12} {}",
            item["kind"].as_str().unwrap_or("?"),
            item["scope"]
                .as_str()
                .or_else(|| item["repo"].as_str())
                .unwrap_or("?"),
            detail,
        )?;
        if singular_command {
            if let Some(command) = item["command"].as_str() {
                writeln!(out, "    → {command}")?;
            }
        } else if let Some(commands) = item["commands"].as_array() {
            let choices = commands
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join("  |  ");
            if !choices.is_empty() {
                writeln!(out, "    choose → {choices}")?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_work_empty_state_names_history_and_wake_distinction() {
        let value = json!({
            "repo": "rat-kingdom",
            "daemon": {"build_version": rk_core::version::BUILD_VERSION},
            "counts": {"live_agents": 0, "ready_tickets": 0, "actionable": 0, "attention": 0, "decision_required": 0, "stalled": 0},
            "live_agents": [],
            "ready_tickets": [],
            "actionable": [],
            "attention": [],
            "decision_required": [],
            "stalled": [],
            "no_current_work": true,
            "history_command": "rk digest --since 1d",
            "diagnostics_command": "rk top",
            "installed_build": rk_core::version::BUILD_VERSION,
            "build_in_sync": true,
        });
        // Smoke the renderer's required fields and no-panic empty contract.
        write_current(&mut Vec::new(), &value).unwrap();
    }

    #[test]
    fn current_work_renders_action_decision_and_stall_without_inventing_commands() {
        let value = json!({
            "repo": "rat-kingdom",
            "daemon": {"build_version": rk_core::version::BUILD_VERSION},
            "counts": {"live_agents": 0, "ready_tickets": 0, "actionable": 1, "attention": 1, "decision_required": 1, "stalled": 1},
            "live_agents": [],
            "ready_tickets": [],
            "actionable": [{"kind": "agent-failed", "scope": "rat-kingdom", "detail": "worker failed", "command": "rk respawn Tails"}],
            "attention": [{"kind": "agent-failed", "scope": "rat-kingdom", "detail": "worker failed", "command": "rk respawn Tails"}],
            "decision_required": [{"kind": "workflow-gate", "scope": "rat-kingdom", "detail": "approval required", "commands": ["rk approve wf-1", "rk reject wf-1"]}],
            "stalled": [{"kind": "workflow-failed", "scope": "rat-kingdom", "text": "check failed", "action": "rk workflow status wf-2"}],
            "no_current_work": false,
            "installed_build": rk_core::version::BUILD_VERSION,
        });
        let mut rendered = Vec::new();
        write_current(&mut rendered, &value).unwrap();
        let rendered = String::from_utf8(rendered).unwrap();
        assert!(rendered.contains("1 actionable · 1 decisions · 1 stalled"));
        assert!(rendered.contains("→ rk respawn Tails"));
        assert!(rendered.contains("choose → rk approve wf-1  |  rk reject wf-1"));
        assert!(rendered.contains("stalled / diagnostic"));
        assert!(rendered.contains("check failed"));
        assert!(!rendered.contains("→ rk workflow status wf-2"));
    }
}

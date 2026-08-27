//! `rk work` — the small, current-state operator surface. Historical and
//! diagnostic views remain available through their existing commands.

use anyhow::Result;
use clap::Args;
use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::{json, Value};

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
    print_current(&result);
    Ok(())
}

fn print_current(work: &Value) {
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
    println!("current work · {scope} · build {daemon_build} ({parity})");
    println!(
        "{} live · {} ready · {} attention",
        work["counts"]["live_agents"].as_u64().unwrap_or(0),
        work["counts"]["ready_tickets"].as_u64().unwrap_or(0),
        work["counts"]["attention"].as_u64().unwrap_or(0),
    );

    if let Some(agents) = work["live_agents"]
        .as_array()
        .filter(|rows| !rows.is_empty())
    {
        println!("\nlive");
        for agent in agents {
            println!(
                "  {:<14} {:<10} {:<12} {}",
                agent["name"].as_str().unwrap_or("?"),
                agent["state"].as_str().unwrap_or("?"),
                agent["repo"].as_str().unwrap_or("?"),
                agent["task"].as_str().unwrap_or("-"),
            );
        }
    }

    if let Some(tickets) = work["ready_tickets"]
        .as_array()
        .filter(|rows| !rows.is_empty())
    {
        println!("\nready");
        for ticket in tickets {
            println!(
                "  {:<28} [{:<6}] {}",
                ticket["id"].as_str().unwrap_or("?"),
                ticket["priority"].as_str().unwrap_or("?"),
                ticket["title"].as_str().unwrap_or(""),
            );
            println!("    → {}", ticket["command"].as_str().unwrap_or("?"));
        }
    }

    if let Some(attention) = work["attention"].as_array().filter(|rows| !rows.is_empty()) {
        println!("\nattention");
        for item in attention {
            println!(
                "  {:<24} {:<12} {}",
                item["kind"].as_str().unwrap_or("?"),
                item["scope"]
                    .as_str()
                    .or_else(|| item["repo"].as_str())
                    .unwrap_or("?"),
                item["detail"].as_str().unwrap_or(""),
            );
            println!("    → {}", item["command"].as_str().unwrap_or("?"));
        }
    }

    if work["no_current_work"].as_bool() == Some(true) {
        println!(
            "\nno current work — history: {}; diagnostics: {}",
            work["history_command"]
                .as_str()
                .unwrap_or("rk digest --since 1d"),
            work["diagnostics_command"].as_str().unwrap_or("rk top"),
        );
    }
    println!(
        "\nKing wakes notify about state; resolving a wake does not mean this work list is empty."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_work_empty_state_names_history_and_wake_distinction() {
        let value = json!({
            "repo": "rat-kingdom",
            "daemon": {"build_version": rk_core::version::BUILD_VERSION},
            "counts": {"live_agents": 0, "ready_tickets": 0, "attention": 0},
            "live_agents": [],
            "ready_tickets": [],
            "attention": [],
            "no_current_work": true,
            "history_command": "rk digest --since 1d",
            "diagnostics_command": "rk top",
            "installed_build": rk_core::version::BUILD_VERSION,
            "build_in_sync": true,
        });
        // Smoke the renderer's required fields. Output capture belongs to the
        // CLI integration test; this guards the no-panic empty contract.
        print_current(&value);
    }
}

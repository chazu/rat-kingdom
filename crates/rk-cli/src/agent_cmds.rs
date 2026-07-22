//! Agent lifecycle subcommands.

use anyhow::Result;
use clap::Args;
use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::{json, Value};

#[derive(Args)]
pub struct SpawnArgs {
    /// Task identifier (e.g. ".rk-42" or a short slug).
    #[arg(long)]
    pub task: String,
    /// Repository path (defaults to the current directory).
    #[arg(long, default_value = ".")]
    pub repo: String,
    /// Task description / initial prompt.
    #[arg(long)]
    pub prompt: Option<String>,
    /// Agent role: rat | reviewer.
    #[arg(long, default_value = "rat")]
    pub role: String,
    /// Harness kind: claude | fake (default from config).
    #[arg(long)]
    pub harness: Option<String>,
    /// Spawning agent name (structural parent for completion routing).
    #[arg(long)]
    pub parent: Option<String>,
    /// Base/merge-target branch (defaults to the repo's current branch).
    #[arg(long)]
    pub base: Option<String>,
    /// Model override.
    #[arg(long)]
    pub model: Option<String>,
    /// Harness permission mode (e.g. acceptEdits, bypassPermissions).
    #[arg(long)]
    pub permission_mode: Option<String>,
}

#[derive(Args)]
pub struct NameArg {
    /// Agent name.
    pub name: String,
}

#[derive(Args)]
pub struct SteerArgs {
    /// Agent name.
    pub name: String,
    /// Guidance to inject into the running session.
    pub message: String,
}

#[derive(Args)]
pub struct DismissArgs {
    /// Agent name.
    pub name: String,
    /// Preserve the branch instead of merging.
    #[arg(long)]
    pub no_merge: bool,
}

pub async fn spawn(layout: &Layout, args: SpawnArgs, as_json: bool) -> Result<()> {
    let repo = std::fs::canonicalize(&args.repo)?;
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo.to_string_lossy(),
                "task": args.task,
                "prompt": args.prompt,
                "role": args.role,
                "harness": args.harness,
                "parent": args.parent,
                "base": args.base,
                "model": args.model,
                "permission_mode": args.permission_mode,
            }),
        )
        .await?;
    let agent = &result["agent"];
    if as_json {
        println!("{agent}");
    } else {
        println!(
            "spawned {} ({} · {} · branch {})",
            agent["name"].as_str().unwrap_or("?"),
            agent["role"].as_str().unwrap_or("?"),
            agent["harness"].as_str().unwrap_or("?"),
            agent["branch"].as_str().unwrap_or("?"),
        );
    }
    Ok(())
}

pub async fn list(layout: &Layout, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client.call("agent.list", json!({})).await?;
    if as_json {
        println!("{}", result["agents"]);
        return Ok(());
    }
    let agents = result["agents"].as_array().cloned().unwrap_or_default();
    if agents.is_empty() {
        println!("(no agents)");
        return Ok(());
    }
    println!(
        "{:<12} {:<9} {:<8} {:<12} {:<14} {:>10} {:>8}",
        "NAME", "STATE", "ROLE", "REPO", "TASK", "TOKENS", "COST"
    );
    for a in agents {
        println!(
            "{:<12} {:<9} {:<8} {:<12} {:<14} {:>10} {:>8}",
            a["name"].as_str().unwrap_or("?"),
            a["state"].as_str().unwrap_or("?"),
            a["role"].as_str().unwrap_or("?"),
            a["repo_name"].as_str().unwrap_or("?"),
            a["task"].as_str().unwrap_or("-"),
            total_tokens(&a["usage"]),
            format!("${:.4}", a["cost_usd"].as_f64().unwrap_or(0.0)),
        );
    }
    Ok(())
}

fn total_tokens(usage: &Value) -> u64 {
    ["input", "output", "cache_read", "cache_creation"]
        .iter()
        .map(|k| usage[k].as_u64().unwrap_or(0))
        .sum()
}

pub async fn status(layout: &Layout, args: NameArg, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call("agent.status", json!({"name": args.name}))
        .await?;
    if as_json {
        println!("{}", result["agent"]);
    } else {
        let a = &result["agent"];
        println!(
            "{}: {}",
            a["name"].as_str().unwrap_or("?"),
            a["state"].as_str().unwrap_or("?")
        );
        println!("  role     {}", a["role"].as_str().unwrap_or("?"));
        println!("  harness  {}", a["harness"].as_str().unwrap_or("?"));
        println!("  repo     {}", a["repo_root"].as_str().unwrap_or("?"));
        println!("  task     {}", a["task"].as_str().unwrap_or("-"));
        println!("  branch   {}", a["branch"].as_str().unwrap_or("-"));
        println!("  session  {}", a["session_id"].as_str().unwrap_or("-"));
        println!("  tokens   {}", total_tokens(&a["usage"]));
        println!("  cost     ${:.4}", a["cost_usd"].as_f64().unwrap_or(0.0));
        if let Some(result_text) = a["result"].as_str() {
            println!("  result   {result_text}");
        }
    }
    Ok(())
}

pub async fn steer(layout: &Layout, args: SteerArgs, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    client
        .call(
            "agent.steer",
            json!({"name": args.name, "message": args.message}),
        )
        .await?;
    if as_json {
        println!("{}", json!({"steered": true}));
    } else {
        println!("steered {}", args.name);
    }
    Ok(())
}

pub async fn interrupt(layout: &Layout, args: NameArg, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    client
        .call("agent.interrupt", json!({"name": args.name}))
        .await?;
    if as_json {
        println!("{}", json!({"interrupted": true}));
    } else {
        println!("interrupted {}", args.name);
    }
    Ok(())
}

pub async fn dismiss(layout: &Layout, args: DismissArgs, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call(
            "agent.dismiss",
            json!({"name": args.name, "no_merge": args.no_merge}),
        )
        .await?;
    if as_json {
        println!("{result}");
    } else {
        println!(
            "dismissed {} — {}",
            args.name,
            result["detail"].as_str().unwrap_or("?")
        );
    }
    Ok(())
}

pub async fn respawn(layout: &Layout, args: NameArg, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call("agent.respawn", json!({"name": args.name}))
        .await?;
    if as_json {
        println!("{}", result["agent"]);
    } else {
        println!("respawned {}", args.name);
    }
    Ok(())
}

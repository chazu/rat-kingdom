//! Agent lifecycle subcommands.

use anyhow::Result;
use clap::Args;
use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::{json, Value};

#[derive(Args)]
pub struct SpawnArgs {
    /// Task identifier (e.g. ".rk-42" or a short slug). Optional if --ticket is given.
    #[arg(long)]
    pub task: Option<String>,
    /// Dispatch an existing ticket: fills task + prompt from it, resolves the
    /// repo from its scope, and flips it to in_progress.
    #[arg(long)]
    pub ticket: Option<String>,
    /// Repository: a path, or a registered repo name (defaults to the current directory).
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
    /// Run interactively in a herdr pane (human-attachable).
    #[arg(long)]
    pub attach: bool,
    /// Dispatch a ticket even if its dependencies are unmet.
    #[arg(long)]
    pub force: bool,
}

#[derive(Args)]
pub struct NameArg {
    /// Agent name.
    pub name: String,
}

#[derive(Args)]
pub struct LogArgs {
    /// Agent name.
    pub name: String,
    /// Stream new entries live as the agent produces them.
    #[arg(long, short)]
    pub follow: bool,
    /// Show only the last N entries (default: all).
    #[arg(long, short = 'n')]
    pub tail: Option<usize>,
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
    let mut client = Client::connect_or_spawn(layout).await?;

    // Resolve task / prompt / repo, optionally from a ticket.
    let (task, prompt, repo_arg) = if let Some(ticket_id) = &args.ticket {
        let result = client
            .call("ticket.get", json!({ "id": ticket_id }))
            .await?;
        if result["ticket"].is_null() {
            anyhow::bail!("no such ticket: {ticket_id}");
        }
        // Refuse to dispatch a ticket whose dependencies are unmet.
        let blockers: Vec<String> = result["blockers"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        if !blockers.is_empty() && !args.force {
            anyhow::bail!(
                "{ticket_id} is blocked by {} (finish them first, or pass --force)",
                blockers.join(", ")
            );
        }
        let ticket = &result["ticket"];
        let payload = &ticket["payload"];
        let title = payload["title"].as_str().unwrap_or("");
        let body = payload["body"].as_str().unwrap_or("");
        let prompt = args.prompt.clone().unwrap_or_else(|| {
            if body.is_empty() {
                title.to_string()
            } else {
                format!("{title}\n\n{body}")
            }
        });
        // Explicit --repo wins; otherwise take the ticket's scope.
        let repo_arg = if args.repo == "." {
            ticket["scope"].as_str().unwrap_or(".").to_string()
        } else {
            args.repo.clone()
        };
        (ticket_id.clone(), Some(prompt), repo_arg)
    } else {
        let task = args
            .task
            .clone()
            .ok_or_else(|| anyhow::anyhow!("provide --task <id> or --ticket <id>"))?;
        (task, args.prompt.clone(), args.repo.clone())
    };

    let repo = crate::repo_cmds::resolve_path(&mut client, &repo_arg).await?;

    // Mark the ticket in_progress BEFORE launching the rat. A fast rat can
    // finish (and auto-set `done`) before this returns, so it must not run
    // after the spawn or it would clobber that `done`.
    if let Some(ticket_id) = &args.ticket {
        let _ = client
            .call(
                "ticket.update",
                json!({ "id": ticket_id, "status": "in_progress" }),
            )
            .await;
    }

    let result = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo,
                "task": task,
                "prompt": prompt,
                "role": args.role,
                "harness": args.harness,
                "parent": args.parent,
                "base": args.base,
                "model": args.model,
                "permission_mode": args.permission_mode,
                "attach": args.attach,
            }),
        )
        .await?;
    let agent = &result["agent"];

    // Record the assignee only — never the status — so this can't overwrite a
    // `done` the rat may already have set on completion.
    if let Some(ticket_id) = &args.ticket {
        let name = agent["name"].as_str().unwrap_or("");
        let _ = client
            .call(
                "ticket.update",
                json!({ "id": ticket_id, "assignee": name }),
            )
            .await;
    }

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

/// Render the unified operator attention queue — one ranked triage list of
/// everything awaiting a human, each row carrying its resolving command.
pub async fn inbox(layout: &Layout, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client.call("inbox.list", json!({})).await?;
    if as_json {
        println!("{}", result["items"]);
        return Ok(());
    }
    let items = result["items"].as_array().cloned().unwrap_or_default();
    if items.is_empty() {
        println!("inbox clear — nothing awaiting a human");
        return Ok(());
    }
    println!("{:<16} {:<14} {:<10} DETAIL", "KIND", "SUBJECT", "SCOPE");
    for it in &items {
        println!(
            "{:<16} {:<14} {:<10} {}",
            it["kind"].as_str().unwrap_or("?"),
            it["subject"].as_str().unwrap_or("?"),
            it["scope"].as_str().unwrap_or("-"),
            it["detail"].as_str().unwrap_or(""),
        );
        println!("  → {}", it["action"].as_str().unwrap_or("?"));
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

/// Print an agent's transcript (assistant text, tool calls, retries). With
/// `--follow`, print the backlog then stream new entries until interrupted.
pub async fn log(layout: &Layout, args: LogArgs, as_json: bool) -> Result<()> {
    let params = json!({"name": args.name, "tail": args.tail, "follow": args.follow});
    if !args.follow {
        let mut client = Client::connect_or_spawn(layout).await?;
        let result = client.call("agent.log", params).await?;
        print_log_entries(&result["entries"], as_json);
        return Ok(());
    }
    let client = Client::connect_or_spawn(layout).await?;
    let (backlog, mut stream) = client.call_then_stream("agent.log", params).await?;
    print_log_entries(&backlog["entries"], as_json);
    while let Some(note) = stream.next().await? {
        match note["method"].as_str() {
            Some("log") => print_log_entry(&note["params"], as_json),
            Some("lagged") => eprintln!("(log lagged: missed {})", note["params"]["missed"]),
            _ => {}
        }
    }
    Ok(())
}

fn print_log_entries(entries: &Value, as_json: bool) {
    match entries.as_array() {
        Some(arr) if !arr.is_empty() => arr.iter().for_each(|e| print_log_entry(e, as_json)),
        _ if as_json => {}
        _ => println!("(no log entries)"),
    }
}

/// One transcript line: `HH:MM:SS  KIND  detail`, or raw JSON with --json.
fn print_log_entry(entry: &Value, as_json: bool) {
    if as_json {
        println!("{entry}");
        return;
    }
    let ts = entry["ts"]
        .as_str()
        .and_then(|s| s.get(11..19))
        .unwrap_or("--:--:--");
    match entry["kind"].as_str() {
        Some("text") => println!("{ts}  text   {}", entry["text"].as_str().unwrap_or("")),
        Some("tool") => println!("{ts}  tool   {}", entry["name"].as_str().unwrap_or("?")),
        Some("retry") => println!(
            "{ts}  retry  attempt {}: {}",
            entry["attempt"].as_u64().unwrap_or(0),
            entry["error"].as_str().unwrap_or("")
        ),
        _ => println!("{ts}  {entry}"),
    }
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

/// Exec into the herdr attach for a running attach-mode rat.
pub async fn attach(layout: &Layout, args: NameArg) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call("agent.status", json!({"name": args.name}))
        .await?;
    let Some(target) = result["agent"]["attach_target"].as_str() else {
        anyhow::bail!(
            "{} is not an attach-mode rat (spawn with --attach to get a herdr pane)",
            args.name
        );
    };
    use std::os::unix::process::CommandExt;
    let argv = rk_mux::HerdrMux::attach_argv(target);
    let err = std::process::Command::new(&argv[0]).args(&argv[1..]).exec();
    Err(anyhow::anyhow!("failed to exec herdr attach: {err}"))
}

pub async fn cost(layout: &Layout, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client.call("agent.list", json!({})).await?;
    let agents = result["agents"].as_array().cloned().unwrap_or_default();
    let mut total_tokens_all = 0u64;
    let mut total_cost = 0.0f64;
    let mut rows = Vec::new();
    for a in &agents {
        let tokens = total_tokens(&a["usage"]);
        let cost = a["cost_usd"].as_f64().unwrap_or(0.0);
        total_tokens_all += tokens;
        total_cost += cost;
        rows.push(json!({
            "agent": a["name"],
            "harness": a["harness"],
            "model": a["model"],
            "task": a["task"],
            "state": a["state"],
            "tokens": tokens,
            "usage": a["usage"],
            "cost_usd": cost,
        }));
    }
    if as_json {
        println!(
            "{}",
            json!({"agents": rows, "total_tokens": total_tokens_all, "total_cost_usd": total_cost})
        );
        return Ok(());
    }
    if rows.is_empty() {
        println!("(no agents)");
        return Ok(());
    }
    println!(
        "{:<12} {:<8} {:<14} {:<10} {:>12} {:>10}",
        "AGENT", "HARNESS", "TASK", "STATE", "TOKENS", "COST"
    );
    for r in &rows {
        println!(
            "{:<12} {:<8} {:<14} {:<10} {:>12} {:>10}",
            r["agent"].as_str().unwrap_or("?"),
            r["harness"].as_str().unwrap_or("?"),
            r["task"].as_str().unwrap_or("-"),
            r["state"].as_str().unwrap_or("?"),
            r["tokens"].as_u64().unwrap_or(0),
            format!("${:.4}", r["cost_usd"].as_f64().unwrap_or(0.0)),
        );
    }
    println!(
        "{:<12} {:<8} {:<14} {:<10} {:>12} {:>10}",
        "TOTAL",
        "",
        "",
        "",
        total_tokens_all,
        format!("${total_cost:.4}")
    );
    Ok(())
}

//! Ticket subcommands: durable work items backed by `task` tuples.

use anyhow::{bail, Result};
use clap::Args;
use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::{json, Value};

#[derive(Args)]
pub struct NewArgs {
    /// Ticket title (short summary of the work).
    pub title: String,
    /// Longer description.
    #[arg(long)]
    pub body: Option<String>,
    /// Repo scope (a registered repo name) or "system" for cross-repo work.
    /// Defaults to "system", unless --parent is set, in which case the
    /// sub-ticket inherits the parent ticket's scope.
    #[arg(long)]
    pub repo: Option<String>,
    /// Parent ticket id — creates a sub-ticket (decomposition).
    #[arg(long)]
    pub parent: Option<String>,
    /// Priority (e.g. low, normal, high).
    #[arg(long)]
    pub priority: Option<String>,
    /// Label (repeatable).
    #[arg(long = "label")]
    pub labels: Vec<String>,
    /// Ticket id this one is blocked by (repeatable).
    #[arg(long = "depends-on")]
    pub depends_on: Vec<String>,
}

#[derive(Args)]
pub struct ListArgs {
    /// Only tickets in this repo scope.
    #[arg(long)]
    pub repo: Option<String>,
    /// Only tickets with this status.
    #[arg(long)]
    pub status: Option<String>,
    /// Only sub-tickets of this parent id.
    #[arg(long)]
    pub parent: Option<String>,
}

#[derive(Args)]
pub struct ReopenArgs {
    /// Ticket id (for example, TKT-01J...).
    pub id: String,
    /// Reopen as `blocked` instead of `open`, holding it out of the
    /// auto-dispatch backlog until a human looks at it.
    #[arg(long)]
    pub block: bool,
}

#[derive(Args)]
pub struct UpdateArgs {
    /// Ticket id (for example, TKT-01J...).
    pub id: String,
    /// New status: open | claimed | in_progress | blocked | done | closed.
    #[arg(long)]
    pub status: Option<String>,
    /// Replace the title.
    #[arg(long)]
    pub title: Option<String>,
    /// Replace the body.
    #[arg(long)]
    pub body: Option<String>,
    /// Replace the priority.
    #[arg(long)]
    pub priority: Option<String>,
    /// Set the assignee (agent name).
    #[arg(long)]
    pub assignee: Option<String>,
    /// Re-parent under another ticket.
    #[arg(long)]
    pub parent: Option<String>,
    /// Add a label, keeping the ones already set (repeatable). This is how a
    /// backlog groom applies a freeze tag, e.g.
    /// `--add-label frozen:onboarding-wizard`.
    #[arg(long = "add-label")]
    pub add_labels: Vec<String>,
    /// Remove a label (repeatable) — e.g. lifting a freeze tag when a
    /// subsystem thaws.
    #[arg(long = "remove-label")]
    pub remove_labels: Vec<String>,
    /// Short reason slug for a groomer closure (e.g. `stale-rework`,
    /// `stale-flake`). Required together with --evidence for a groomer's
    /// `--status closed`; ignored by an ordinary update.
    #[arg(long)]
    pub reason: Option<String>,
    /// Evidence backing --reason (a target ticket id, commit sha, or test-run
    /// result). Required together with --reason.
    #[arg(long)]
    pub evidence: Option<String>,
}

pub async fn new(layout: &Layout, args: NewArgs, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let mut params = json!({
        "title": args.title,
        "labels": args.labels,
        "depends_on": args.depends_on,
    });
    if let Some(repo) = args.repo {
        params["scope"] = json!(repo);
    }
    if let Some(body) = args.body {
        params["body"] = json!(body);
    }
    if let Some(parent) = args.parent {
        params["parent"] = json!(parent);
    }
    if let Some(priority) = args.priority {
        params["priority"] = json!(priority);
    }
    // Attribute agent-filed tickets when spawned into an agent session.
    if let Ok(agent) = std::env::var("RK_AGENT") {
        params["created_by"] = json!(agent);
    }
    let result = client.call("ticket.new", params).await?;
    let ticket = &result["ticket"];
    if as_json {
        println!("{ticket}");
    } else {
        println!(
            "created {} — {}",
            ticket["identity"].as_str().unwrap_or("?"),
            ticket["payload"]["title"].as_str().unwrap_or("")
        );
    }
    Ok(())
}

pub async fn list(layout: &Layout, args: ListArgs, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let mut params = serde_json::Map::new();
    if let Some(repo) = args.repo {
        params.insert("scope".into(), json!(repo));
    }
    if let Some(status) = args.status {
        params.insert("status".into(), json!(status));
    }
    if let Some(parent) = args.parent {
        params.insert("parent".into(), json!(parent));
    }
    let result = client.call("ticket.list", Value::Object(params)).await?;
    let tickets = result["tickets"].as_array().cloned().unwrap_or_default();
    if as_json {
        println!("{}", result["tickets"]);
        return Ok(());
    }
    if tickets.is_empty() {
        println!("(no tickets)");
        return Ok(());
    }
    let blocked: Vec<&str> = result["blocked"]
        .as_array()
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    println!(
        "{:<9} {:<12} {:<9} {:<12} TITLE",
        "ID", "STATUS", "PRIORITY", "SCOPE"
    );
    for t in &tickets {
        let id = t["identity"].as_str().unwrap_or("?");
        print_ticket_row(t, blocked.contains(&id));
    }
    Ok(())
}

pub async fn ready(layout: &Layout, repo: Option<String>, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let mut params = serde_json::Map::new();
    if let Some(repo) = repo {
        params.insert("scope".into(), json!(repo));
    }
    let result = client.call("ticket.ready", Value::Object(params)).await?;
    let tickets = result["tickets"].as_array().cloned().unwrap_or_default();
    if as_json {
        println!("{}", result["tickets"]);
        return Ok(());
    }
    if tickets.is_empty() {
        println!("(nothing ready — all open tickets are blocked or none exist)");
        return Ok(());
    }
    println!(
        "{:<9} {:<12} {:<9} {:<12} TITLE",
        "ID", "STATUS", "PRIORITY", "SCOPE"
    );
    for t in &tickets {
        print_ticket_row(t, false);
    }
    Ok(())
}

pub async fn dep(
    layout: &Layout,
    id: String,
    dep: String,
    remove: bool,
    as_json: bool,
) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call(
            "ticket.dep",
            json!({ "id": id, "dep": dep, "remove": remove }),
        )
        .await?;
    let ticket = &result["ticket"];
    if as_json {
        println!("{ticket}");
    } else if remove {
        println!("{id} no longer depends on {dep}");
    } else {
        println!("{id} now depends on {dep}");
    }
    Ok(())
}

pub async fn show(layout: &Layout, id: String, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client.call("ticket.get", json!({ "id": id })).await?;
    if result["ticket"].is_null() {
        if as_json {
            println!("{}", json!({ "ticket": null }));
            return Ok(());
        }
        bail!("no such ticket: {id}");
    }
    let t = &result["ticket"];
    if as_json {
        println!("{t}");
        return Ok(());
    }
    let p = &t["payload"];
    println!(
        "{}: {}",
        t["identity"].as_str().unwrap_or("?"),
        p["title"].as_str().unwrap_or("")
    );
    println!("  status    {}", p["status"].as_str().unwrap_or("?"));
    println!("  priority  {}", p["priority"].as_str().unwrap_or("normal"));
    println!("  scope     {}", t["scope"].as_str().unwrap_or("?"));
    println!("  parent    {}", p["parent"].as_str().unwrap_or("-"));
    println!("  assignee  {}", p["assignee"].as_str().unwrap_or("-"));
    println!("  by        {}", p["created_by"].as_str().unwrap_or("-"));
    // Dependencies, annotated with which are still blocking.
    if let Some(deps) = p["depends_on"].as_array() {
        if !deps.is_empty() {
            let blockers: Vec<&str> = result["blockers"]
                .as_array()
                .map(|a| a.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            let rendered: Vec<String> = deps
                .iter()
                .filter_map(Value::as_str)
                .map(|d| {
                    if blockers.contains(&d) {
                        format!("{d} (blocking)")
                    } else {
                        format!("{d} ✓")
                    }
                })
                .collect();
            let state = if blockers.is_empty() {
                "ready"
            } else {
                "BLOCKED"
            };
            println!("  deps      {} [{}]", rendered.join(", "), state);
        }
    }
    if let Some(labels) = p["labels"].as_array() {
        if !labels.is_empty() {
            let names: Vec<&str> = labels.iter().filter_map(Value::as_str).collect();
            println!("  labels    {}", names.join(", "));
        }
    }
    if let Some(body) = p["body"].as_str() {
        if !body.is_empty() {
            println!("\n{body}");
        }
    }
    // Children (decomposition).
    let kids = client.call("ticket.list", json!({ "parent": id })).await?;
    let kids = kids["tickets"].as_array().cloned().unwrap_or_default();
    if !kids.is_empty() {
        println!("\nsub-tickets:");
        for k in &kids {
            println!(
                "  {:<8} [{}] {}",
                k["identity"].as_str().unwrap_or("?"),
                k["payload"]["status"].as_str().unwrap_or("?"),
                k["payload"]["title"].as_str().unwrap_or("")
            );
        }
    }
    Ok(())
}

pub async fn update(layout: &Layout, args: UpdateArgs, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let mut params = json!({ "id": args.id });
    for (key, value) in [
        ("status", args.status),
        ("title", args.title),
        ("body", args.body),
        ("priority", args.priority),
        ("assignee", args.assignee),
        ("parent", args.parent),
    ] {
        if let Some(v) = value {
            params[key] = json!(v);
        }
    }
    if !args.add_labels.is_empty() {
        params["add_labels"] = json!(args.add_labels);
    }
    if !args.remove_labels.is_empty() {
        params["remove_labels"] = json!(args.remove_labels);
    }
    match (args.reason, args.evidence) {
        (Some(reason), Some(evidence)) => {
            params["reason"] = json!({ "reason": reason, "evidence": evidence });
        }
        (None, None) => {}
        _ => bail!("--reason and --evidence must be given together"),
    }
    let result = client.call("ticket.update", params).await?;
    let ticket = &result["ticket"];
    if as_json {
        println!("{ticket}");
    } else {
        println!(
            "updated {} — status {}",
            ticket["identity"].as_str().unwrap_or("?"),
            ticket["payload"]["status"].as_str().unwrap_or("?")
        );
    }
    Ok(())
}

pub async fn reopen(layout: &Layout, args: ReopenArgs, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let status = if args.block { "blocked" } else { "open" };
    let result = client
        .call("ticket.reopen", json!({ "id": args.id, "status": status }))
        .await?;
    let ticket = &result["ticket"];
    if as_json {
        println!("{ticket}");
    } else {
        println!(
            "reopened {} — status {}",
            ticket["identity"].as_str().unwrap_or("?"),
            ticket["payload"]["status"].as_str().unwrap_or("?")
        );
    }
    Ok(())
}

fn print_ticket_row(t: &Value, blocked: bool) {
    let p = &t["payload"];
    let title = p["title"].as_str().unwrap_or("");
    let mut title = match p["parent"].as_str() {
        Some(par) => format!("{title}  (↳ {par})"),
        None => title.to_string(),
    };
    if blocked {
        title = format!("🔒 {title}");
    }
    println!(
        "{:<9} {:<12} {:<9} {:<12} {}",
        t["identity"].as_str().unwrap_or("?"),
        p["status"].as_str().unwrap_or("?"),
        p["priority"].as_str().unwrap_or("normal"),
        t["scope"].as_str().unwrap_or("?"),
        title
    );
}

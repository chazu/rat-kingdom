//! Agent lifecycle subcommands.

use anyhow::Result;
use clap::Args;
use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::{json, Value};
use std::fmt::Write as _;

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
    /// Agent role: rat | reviewer | foreman.
    #[arg(long, default_value = "rat")]
    pub role: String,
    /// Harness kind: claude | codex | jcode | fake (default from config).
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
    /// Harness permission mode (autonomous: bypassPermissions or danger-full-access).
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
pub struct ListArgs {
    /// Include archived records alongside the live fleet.
    #[arg(long)]
    pub all: bool,
    /// Show only archived records.
    #[arg(long)]
    pub archived: bool,
}

#[derive(Args)]
pub struct PruneArgs {
    /// Archive terminal records last touched before this point: a duration
    /// (30m, 24h, 7d, 2w) or a date (2026-07-24 / RFC3339).
    #[arg(long, default_value = "7d")]
    pub before: String,
    /// Archive every eligible record now, regardless of age.
    #[arg(long)]
    pub all: bool,
    /// List what would be archived without touching the registry.
    #[arg(long)]
    pub dry_run: bool,
    /// Also reclaim each archived agent's worktree and local branch — but only
    /// where the branch has already merged into its target (or is gone). An
    /// unmerged branch is always left standing.
    #[arg(long)]
    pub reap_git: bool,
    /// Also delete each archived agent's `rk log` transcript. One-way: the
    /// record still archives and `rk unarchive` still restores it, but the
    /// transcript is gone for good. Only a record actually being archived
    /// loses one.
    #[arg(long)]
    pub reap_logs: bool,
    /// Also delete each archived agent's regenerable build-artifact paths
    /// (`target` by default; see `[worktree_sweep]`) from its worktree —
    /// regardless of merge state, unlike `--reap-git`. Source, git history,
    /// and the worktree itself are never touched.
    #[arg(long)]
    pub reap_artifacts: bool,
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
    /// Which rat of this name to read, 1 = oldest (default: the newest). Only
    /// matters for the handful of names that named two rats.
    #[arg(long, short = 'g')]
    pub generation: Option<usize>,
}

#[derive(Args)]
pub struct SteerArgs {
    /// Agent name.
    pub name: String,
    /// Guidance to inject into the running session.
    pub message: String,
}

#[derive(Args)]
pub struct ProgressArgs {
    /// Bounded semantic checkpoint for the current agent generation.
    #[arg(long)]
    pub summary: String,
    /// The next meaningful action.
    #[arg(long)]
    pub next: Option<String>,
    /// working | blocked | complete.
    #[arg(long, default_value = "working")]
    pub status: String,
}

#[derive(Args)]
pub struct DismissArgs {
    /// Agent name.
    pub name: String,
    /// Preserve the branch instead of merging.
    #[arg(long)]
    pub no_merge: bool,
}

#[derive(Args)]
pub struct RevertArgs {
    /// Dismissed agent whose landed merge to undo.
    pub name: String,
    /// Reopen the agent's ticket as `blocked` instead of `open`, holding it
    /// out of the auto-dispatch backlog until a human looks at it.
    #[arg(long)]
    pub block: bool,
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
        // Explicit --repo wins; otherwise take the ticket's scope. A
        // system-scoped ticket with a repo-scoped parent (e.g. an older
        // sub-ticket minted before scope inheritance landed) resolves
        // through the parent instead of erroring.
        let repo_arg = if args.repo == "." {
            let ticket_scope = ticket["scope"].as_str().unwrap_or(".").to_string();
            if ticket_scope == rk_core::tuple::SYSTEM_SCOPE {
                if let Some(parent_id) = payload["parent"].as_str() {
                    let parent = client
                        .call("ticket.get", json!({ "id": parent_id }))
                        .await?;
                    match parent["ticket"]["scope"].as_str() {
                        Some(s) if s != rk_core::tuple::SYSTEM_SCOPE => {
                            eprintln!(
                                "note: {ticket_id} is system-scoped; resolving repo through parent {parent_id} ({s})"
                            );
                            s.to_string()
                        }
                        _ => ticket_scope,
                    }
                } else {
                    ticket_scope
                }
            } else {
                ticket_scope
            }
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

pub async fn list(layout: &Layout, args: ListArgs, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call(
            "agent.list",
            json!({"include_archived": args.all, "archived_only": args.archived}),
        )
        .await?;
    if as_json {
        println!("{}", result["agents"]);
        return Ok(());
    }
    let agents = result["agents"].as_array().cloned().unwrap_or_default();
    if agents.is_empty() {
        println!(
            "{}",
            if args.archived {
                "(no archived agents)"
            } else {
                "(no agents)"
            }
        );
        return Ok(());
    }
    println!(
        "{:<12} {:<9} {:<8} {:<12} {:<14} {:>10} {:>8}",
        "NAME", "STATE", "ROLE", "REPO", "TASK", "TOKENS", "COST"
    );
    for a in &agents {
        // An archived row is only reachable via --all/--archived, so mark it
        // rather than letting it read as part of the current fleet.
        let state = match a["archived_at"].as_str() {
            Some(_) => format!("{}*", a["state"].as_str().unwrap_or("?")),
            None => a["state"].as_str().unwrap_or("?").to_string(),
        };
        println!(
            "{:<12} {:<9} {:<8} {:<12} {:<14} {:>10} {:>8}",
            a["name"].as_str().unwrap_or("?"),
            state,
            a["role"].as_str().unwrap_or("?"),
            a["repo_name"].as_str().unwrap_or("?"),
            a["task"].as_str().unwrap_or("-"),
            total_tokens(&a["usage"]),
            format!("${:.4}", a["cost_usd"].as_f64().unwrap_or(0.0)),
        );
    }
    if agents.iter().any(|a| !a["archived_at"].is_null()) {
        println!("(* archived — `rk unarchive <name>` restores one)");
    }
    Ok(())
}

pub async fn progress(layout: &Layout, args: ProgressArgs, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call(
            "agent.progress",
            json!({
                "summary": args.summary,
                "next": args.next,
                "status": args.status,
            }),
        )
        .await?;
    if as_json {
        println!("{result}");
    } else {
        println!(
            "progress recorded for {} (revision {})",
            result["agent"].as_str().unwrap_or("?"),
            result["progress"]["revision"].as_u64().unwrap_or(0)
        );
    }
    Ok(())
}

/// `rk prune` — offload settled terminal records (Completed/Failed/Dismissed)
/// into the archive store so `rk list` and `rk top` show only what's current.
/// Nothing is deleted: archived records keep their cost/usage/lineage and stay
/// readable via `rk list --archived` / `rk status`.
///
/// The same pass clears the workflow half of the board: settled instances are
/// archived on the same window, so one gesture empties `rk inbox` instead of
/// leaving failed instances with no `rk` path off it at all (TKT-177).
pub async fn prune(layout: &Layout, args: PruneArgs, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call(
            "agent.archive",
            json!({
                "before": args.before,
                "all": args.all,
                "dry_run": args.dry_run,
                "reap_git": args.reap_git,
                "reap_logs": args.reap_logs,
                "reap_artifacts": args.reap_artifacts,
            }),
        )
        .await?;
    if as_json {
        println!("{result}");
        return Ok(());
    }
    let agents = result["agents"].as_array().cloned().unwrap_or_default();
    let instances = result["instances"].as_array().cloned().unwrap_or_default();
    // Only present when `--reap-git` was passed; driven purely by disk state
    // under `gate-worktrees/`, independent of whether any agent record was
    // archived this pass — a repo can accumulate stale gate worktrees with
    // zero terminal agents due for archiving.
    let gate_worktrees = result["gate_worktrees"].as_array().cloned().unwrap_or_default();
    let window = if args.all {
        "all eligible".to_string()
    } else {
        format!("older than {}", args.before)
    };
    if agents.is_empty() && instances.is_empty() && gate_worktrees.is_empty() {
        println!("nothing to archive ({window})");
        return Ok(());
    }
    let verb = if args.dry_run {
        "would archive"
    } else {
        "archived"
    };
    if !agents.is_empty() {
        println!("{verb} {} record(s) ({window}):", agents.len());
    }
    for a in &agents {
        println!(
            "  {:<12} {:<10} {:<14} ${:.4}",
            a["name"].as_str().unwrap_or("?"),
            a["state"].as_str().unwrap_or("?"),
            a["task"].as_str().unwrap_or("-"),
            a["cost_usd"].as_f64().unwrap_or(0.0),
        );
    }
    if !instances.is_empty() {
        println!(
            "{verb} {} workflow instance(s) ({window}):",
            instances.len()
        );
        for i in &instances {
            print_pruned_instance(i);
        }
    }
    // Both reap passes report the same row shape, so print them the same way;
    // only the leading tag says which artifact a row is about.
    for (kind, rows) in [
        ("git", "reaped"),
        ("log", "reaped_logs"),
        ("artifacts", "reaped_artifacts"),
    ] {
        for r in result[rows].as_array().cloned().unwrap_or_default() {
            println!(
                "  {kind} {:<8} {:<12} {}",
                if r["reaped"] == json!(true) {
                    "reaped"
                } else {
                    "kept"
                },
                r["agent"].as_str().unwrap_or("?"),
                r["reason"].as_str().unwrap_or(""),
            );
        }
    }
    if !gate_worktrees.is_empty() {
        println!(
            "{verb} {} gate worktree(s) (LRU/cap retention):",
            gate_worktrees.len()
        );
        for r in &gate_worktrees {
            println!(
                "  gate {:<8} {:<12} {:<20} {}",
                if r["reclaimed"] == json!(true) {
                    "reaped"
                } else {
                    "kept"
                },
                r["repo"].as_str().unwrap_or("?"),
                r["target"].as_str().unwrap_or("?"),
                r["reason"].as_str().unwrap_or(""),
            );
        }
    }
    if args.dry_run {
        println!("(dry run — re-run without --dry-run to archive)");
    }
    Ok(())
}

/// One archived workflow instance as a `rk prune` / `rk workflow prune` row.
/// Shared so the sweeping and the targeted commands report identically; the
/// failure text is what the operator was staring at in `rk inbox`, so it is
/// echoed (truncated) rather than dropped.
pub fn print_pruned_instance(instance: &Value) {
    let detail = match instance["error"].as_str() {
        Some(error) if !error.is_empty() => {
            let line = error.lines().next().unwrap_or(error);
            format!("  {}", line.chars().take(60).collect::<String>())
        }
        _ => String::new(),
    };
    println!(
        "  {:<14} {:<12} {:<10}{detail}",
        instance["id"].as_str().unwrap_or("?"),
        instance["workflow"].as_str().unwrap_or("?"),
        instance["status"].as_str().unwrap_or("?"),
    );
}

/// `rk unarchive` — restore one archived record to the live registry.
pub async fn unarchive(layout: &Layout, args: NameArg, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call("agent.unarchive", json!({"name": args.name}))
        .await?;
    if as_json {
        println!("{}", result["agent"]);
    } else {
        println!("unarchived {}", args.name);
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

/// `rk inbox ack <id>` — durably acknowledge a `recovery-action` escalation
/// (B2) so the daemon's re-notify sweep stops pushing it. Sink-agnostic: this
/// CLI path is the same one a future rat-king sink would go through.
pub async fn inbox_ack(layout: &Layout, id: String, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client.call("inbox.ack", json!({"id": id})).await?;
    if as_json {
        println!("{result}");
        return Ok(());
    }
    if result["already"].as_bool().unwrap_or(false) {
        println!("{id} was already acked");
    } else {
        println!("acked {id} — the re-notify sweep will stop pushing it");
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
        print!("{}", format_status(&result["agent"]));
    }
    Ok(())
}

fn format_status(a: &Value) -> String {
    let mut text = String::new();
    writeln!(
        text,
        "{}: {}",
        a["name"].as_str().unwrap_or("?"),
        a["state"].as_str().unwrap_or("?"),
    )
    .unwrap();
    for (label, value) in [
        ("role", a["role"].as_str().unwrap_or("?").to_string()),
        ("harness", a["harness"].as_str().unwrap_or("?").to_string()),
        ("model", a["model"].as_str().unwrap_or("-").to_string()),
        (
            "permissions",
            a["permission_mode"].as_str().unwrap_or("-").to_string(),
        ),
        ("repo", a["repo_root"].as_str().unwrap_or("?").to_string()),
        ("task", a["task"].as_str().unwrap_or("-").to_string()),
        ("branch", a["branch"].as_str().unwrap_or("-").to_string()),
        (
            "base",
            a["target_branch"].as_str().unwrap_or("-").to_string(),
        ),
        (
            "session",
            a["session_id"].as_str().unwrap_or("-").to_string(),
        ),
        ("tokens", total_tokens(&a["usage"]).to_string()),
        (
            "cost",
            format!("${:.4}", a["cost_usd"].as_f64().unwrap_or(0.0)),
        ),
    ] {
        writeln!(text, "  {label:<11} {value}").unwrap();
    }
    if let Some(result_text) = a["result"].as_str() {
        writeln!(text, "  {:<11} {result_text}", "result").unwrap();
    }
    text
}

/// Print an agent's transcript (assistant text, tool calls, retries). With
/// `--follow`, print the backlog then stream new entries until interrupted.
pub async fn log(layout: &Layout, args: LogArgs, as_json: bool) -> Result<()> {
    let params = json!({
        "name": args.name,
        "tail": args.tail,
        "follow": args.follow,
        "generation": args.generation,
    });
    if !args.follow {
        let mut client = Client::connect_or_spawn(layout).await?;
        let result = client.call("agent.log", params).await?;
        if let Some(note) = generation_note(&result, &args.name, as_json) {
            eprintln!("{note}");
        }
        print_log_entries(&result["entries"], as_json);
        return Ok(());
    }
    let client = Client::connect_or_spawn(layout).await?;
    let (backlog, mut stream) = client.call_then_stream("agent.log", params).await?;
    if let Some(note) = generation_note(&backlog, &args.name, as_json) {
        eprintln!("{note}");
    }
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

/// Say so when a name is ambiguous, i.e. when it named more than one rat.
/// Showing the newest silently would read as "this is the whole history of that
/// name", which for those names it is not. `None` = nothing worth saying; the
/// caller prints to stderr so piping the transcript stays clean.
fn generation_note(result: &Value, name: &str, as_json: bool) -> Option<String> {
    let total = result["generations"].as_u64().unwrap_or(0);
    if as_json || total <= 1 {
        return None;
    }
    let shown = result["generation"].as_u64().unwrap_or(total);
    let spawned = result["created_at"].as_str().unwrap_or("?");
    Some(format!(
        "note: {total} rats have been named {name}; showing #{shown} (spawned {spawned}). \
         `rk log {name} --generation N` for another (1 = oldest)."
    ))
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
        Some("stderr") => println!(
            "{ts}  harness-stderr  {}",
            entry["text"].as_str().unwrap_or("")
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

/// Undo a bad auto-merge: revert-merge the commit a dismissal landed and put
/// the agent's ticket back on the backlog.
pub async fn revert(layout: &Layout, args: RevertArgs, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call(
            "agent.revert",
            json!({"name": args.name, "block": args.block}),
        )
        .await?;
    if as_json {
        println!("{result}");
    } else if result["reverted"].as_bool().unwrap_or(false) {
        println!(
            "reverted {} — {}",
            args.name,
            result["detail"].as_str().unwrap_or("?")
        );
        if let Some(status) = result["ticket_status"].as_str() {
            println!(
                "ticket {} reopened as {status}",
                result["task"].as_str().unwrap_or("?")
            );
        }
    } else {
        println!(
            "revert failed for {} — {}",
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
    // Lifetime spend, so `include_archived`: `rk prune` retires a record from
    // the fleet views but must never make spend disappear from the ledger.
    let result = client
        .call("agent.list", json!({"include_archived": true}))
        .await?;
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

/// `rk cost --fleet`: the hierarchical fleet/repo budget rollup — current spend
/// vs the configured caps — so an operator can see how close the wallet
/// kill-switch is to refusing new dispatch.
pub async fn cost_fleet(layout: &Layout, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let rollup = client.call("budget.rollup", json!({})).await?;
    if as_json {
        println!("{rollup}");
        return Ok(());
    }
    let fmt_cap = |cap: f64| {
        if cap > 0.0 {
            format!("${cap:.4}")
        } else {
            "unlimited".to_string()
        }
    };
    println!(
        "{:<20} {:>12} {:>12} {:>12} {:>10}",
        "SCOPE", "SPENT", "CAP", "REMAINING", "STATUS"
    );
    let row = |label: &str, s: &serde_json::Value| {
        let cap = s["cap_usd"].as_f64().unwrap_or(0.0);
        let remaining = if cap > 0.0 {
            format!("${:.4}", s["remaining_usd"].as_f64().unwrap_or(0.0))
        } else {
            "-".to_string()
        };
        println!(
            "{:<20} {:>12} {:>12} {:>12} {:>10}",
            label,
            format!("${:.4}", s["spent_usd"].as_f64().unwrap_or(0.0)),
            fmt_cap(cap),
            remaining,
            s["status"].as_str().unwrap_or("?"),
        );
    };
    row("fleet", &rollup["fleet"]);
    for r in rollup["repos"].as_array().cloned().unwrap_or_default() {
        let repo = r["repo"].as_str().unwrap_or("?").to_string();
        row(&format!("repo:{repo}"), &r);
    }
    // Per-workflow-instance spend. The cap lives on each workflow's `budget:`
    // field (enforced at dispatch), not in the fleet config, so this rollup
    // reports current burn only — cap/status columns stay blank.
    for i in rollup["instances"].as_array().cloned().unwrap_or_default() {
        let inst = i["instance"].as_str().unwrap_or("?").to_string();
        println!(
            "{:<20} {:>12} {:>12} {:>12} {:>10}",
            format!("wf:{inst}"),
            format!("${:.4}", i["spent_usd"].as_f64().unwrap_or(0.0)),
            "-",
            "-",
            "-",
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reply(generations: u64, generation: u64) -> Value {
        json!({
            "entries": [],
            "generations": generations,
            "generation": generation,
            "created_at": "2026-07-23T03:10:47.893987Z",
        })
    }

    /// The common case: a name names one rat, so there is nothing to disclose.
    #[test]
    fn one_generation_says_nothing() {
        assert_eq!(generation_note(&reply(1, 1), "Whisker", false), None);
        assert_eq!(generation_note(&reply(0, 0), "ghost", false), None);
    }

    /// An ambiguous name must say which rat it is showing, and how to reach the
    /// other — otherwise a partial answer reads as the whole history.
    #[test]
    fn an_ambiguous_name_discloses_the_choice() {
        let note = generation_note(&reply(2, 2), "Gouda", false).expect("a note");
        assert!(note.contains("2 rats have been named Gouda"), "{note}");
        assert!(note.contains("showing #2"), "{note}");
        assert!(note.contains("2026-07-23T03:10:47"), "{note}");
        assert!(note.contains("--generation N"), "{note}");
    }

    /// `--json` output stays machine-clean; the reply already carries the
    /// generation fields for a caller that wants them.
    #[test]
    fn json_output_carries_no_prose() {
        assert_eq!(generation_note(&reply(2, 2), "Gouda", true), None);
    }

    #[test]
    fn status_shows_effective_model_and_permission_mode() {
        let rendered = format_status(&json!({
            "name": "Rizzo",
            "state": "running",
            "role": "rat",
            "harness": "claude",
            "permission_mode": "bypassPermissions",
            "model": "sonnet",
            "repo_root": "/tmp/repo",
            "task": "research",
            "branch": "rat/rizzo/research",
            "target_branch": "integration",
            "session_id": "session",
            "usage": {},
            "cost_usd": 0.0,
        }));
        assert!(rendered.contains("model       sonnet"), "{rendered}");
        assert!(
            rendered.contains("permissions bypassPermissions"),
            "{rendered}"
        );
        assert!(rendered.contains("base        integration"), "{rendered}");
    }
}

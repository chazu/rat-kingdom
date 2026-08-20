//! `rk reconcile-repair` — dry-run (default) or apply mechanical repair for
//! the two convergence violations durable evidence alone proves and fixes:
//! `delivered-but-open` and `terminal-assignee-active-work` (stale
//! ownership). Everything else the convergence report surfaces stays
//! report-only and untouched by this command.

use anyhow::Result;
use clap::Args;
use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::json;

#[derive(Args)]
pub struct RepairArgs {
    /// Repo name (as registered with `rk repo add`) or path.
    pub repo: String,
    /// Execute the plan. Without this flag, the command previews what would
    /// happen with zero mutation (the default, and the safe way to inspect
    /// a plan before committing to it).
    #[arg(long)]
    pub apply: bool,
}

pub async fn repair(layout: &Layout, args: RepairArgs, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call(
            "reconcile.repair",
            json!({"repo": args.repo, "apply": args.apply}),
        )
        .await?;
    if as_json {
        println!("{result}");
        return Ok(());
    }
    let scope = result["scope"].as_str().unwrap_or(&args.repo);
    let mode = result["mode"].as_str().unwrap_or("dry_run");
    let results = result["results"].as_array().cloned().unwrap_or_default();
    if results.is_empty() {
        println!("{scope}: nothing to repair [{mode}]");
        return Ok(());
    }
    println!("{scope}: {} candidate(s) [{mode}]", results.len());
    println!("{:<16} {:<32} {:<28} OUTCOME", "STATUS", "KIND", "SUBJECT");
    for r in &results {
        let outcome = &r["outcome"];
        let status = outcome["status"].as_str().unwrap_or("?");
        let note = match status {
            "held" => format!(
                "{}: {}",
                outcome["reason"].as_str().unwrap_or("?"),
                outcome["detail"].as_str().unwrap_or("")
            ),
            _ => outcome["detail"].as_str().unwrap_or("").to_string(),
        };
        println!(
            "{:<16} {:<32} {:<28} {}",
            status,
            r["kind"].as_str().unwrap_or("?"),
            r["subject"].as_str().unwrap_or("?"),
            note,
        );
    }
    Ok(())
}

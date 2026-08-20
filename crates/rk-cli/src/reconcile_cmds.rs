//! `rk reconcile` — the cross-ledger convergence report for one repository.
//! Read-only: this never writes a tuple.

use anyhow::Result;
use clap::Args;
use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::json;

#[derive(Args)]
pub struct ReportArgs {
    /// Repo name (as registered with `rk repo add`) or path.
    pub repo: String,
}

pub async fn report(layout: &Layout, args: ReportArgs, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call("reconcile.report", json!({"repo": args.repo}))
        .await?;
    if as_json {
        println!("{result}");
        return Ok(());
    }
    let scope = result["scope"].as_str().unwrap_or(&args.repo);
    let violations = result["violations"].as_array().cloned().unwrap_or_default();
    if violations.is_empty() {
        println!("{scope}: converged — no contradictions detected");
        return Ok(());
    }
    println!("{scope}: {} violation(s)", violations.len());
    println!(
        "{:<14} {:<32} {:<28} DETAIL",
        "AUTHORITY", "KIND", "SUBJECT"
    );
    for v in &violations {
        println!(
            "{:<14} {:<32} {:<28} {}",
            v["authority"].as_str().unwrap_or("?"),
            v["kind"].as_str().unwrap_or("?"),
            v["subject"].as_str().unwrap_or("?"),
            v["detail"].as_str().unwrap_or(""),
        );
    }
    Ok(())
}

//! `rk lease` / `rk attention` — TKT-01M0E8PN9C41BWECGNW0990R3J's unattended
//! authority-ladder surface: acquire a durable orchestrator lease, consume
//! the resumable attention queue derived from `rk reconcile`, and decide one
//! item within approved authority.

use anyhow::Result;
use clap::{Args, Subcommand};
use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::json;

#[derive(Subcommand)]
pub enum LeaseCommand {
    /// Acquire (or resume) the orchestrator lease for a repository.
    Acquire(LeaseArgs),
    /// Renew a held lease's TTL without disturbing its generation or cursor.
    Renew(LeaseRenewArgs),
}

#[derive(Args)]
pub struct LeaseArgs {
    /// Repo name (as registered with `rk repo add`) or path.
    pub repo: String,
    /// Stable identity of the orchestrator session acquiring the lease.
    /// Present the SAME holder again to resume after a disconnect or a
    /// daemon restart.
    #[arg(long)]
    pub holder: String,
    #[arg(long)]
    pub ttl_secs: Option<i64>,
}

#[derive(Args)]
pub struct LeaseRenewArgs {
    pub repo: String,
    #[arg(long)]
    pub holder: String,
    #[arg(long)]
    pub generation: u64,
    #[arg(long)]
    pub ttl_secs: Option<i64>,
}

pub async fn lease_acquire(layout: &Layout, args: LeaseArgs, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call(
            "lease.acquire",
            json!({"repo": args.repo, "holder": args.holder, "ttl_secs": args.ttl_secs}),
        )
        .await?;
    print_lease(&result, as_json);
    Ok(())
}

pub async fn lease_renew(layout: &Layout, args: LeaseRenewArgs, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call(
            "lease.renew",
            json!({
                "repo": args.repo,
                "holder": args.holder,
                "generation": args.generation,
                "ttl_secs": args.ttl_secs,
            }),
        )
        .await?;
    print_lease(&result, as_json);
    Ok(())
}

fn print_lease(result: &serde_json::Value, as_json: bool) {
    if as_json {
        println!("{result}");
        return;
    }
    println!(
        "lease {} holder={} generation={} expires_at={} cursor={}",
        result["scope"].as_str().unwrap_or("?"),
        result["holder"].as_str().unwrap_or("?"),
        result["generation"].as_u64().unwrap_or(0),
        result["expires_at"].as_str().unwrap_or("?"),
        result["cursor"].as_str().unwrap_or("-"),
    );
}

#[derive(Subcommand)]
pub enum AttentionCommand {
    /// The next resumable attention item for a repository (or none, if
    /// converged / fully consumed).
    Next(AttentionNextArgs),
    /// Resolve one attention item, dispatched by its effective authority.
    Decide(AttentionDecideArgs),
}

#[derive(Args)]
pub struct AttentionNextArgs {
    pub repo: String,
}

#[derive(Args)]
pub struct AttentionDecideArgs {
    pub repo: String,
    /// The attention item's id, as shown by `rk attention next`.
    pub item: String,
    /// Required only when the item's effective authority is `orchestrator`.
    #[arg(long)]
    pub holder: Option<String>,
    #[arg(long)]
    pub generation: Option<u64>,
    #[arg(long)]
    pub budget_usd: Option<f64>,
    #[arg(long)]
    pub budget_tokens: Option<u64>,
}

pub async fn attention_next(layout: &Layout, args: AttentionNextArgs, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call("attention.next", json!({"repo": args.repo}))
        .await?;
    if as_json {
        println!("{result}");
        return Ok(());
    }
    match result.get("item").filter(|v| !v.is_null()) {
        None => println!("{}: no attention items pending", args.repo),
        Some(item) => println!(
            "[{:<12}] {:<32} {:<28} {}",
            item["effective_authority"].as_str().unwrap_or("?"),
            item["kind"].as_str().unwrap_or("?"),
            item["subject"].as_str().unwrap_or("?"),
            item["detail"].as_str().unwrap_or(""),
        ),
    }
    Ok(())
}

pub async fn attention_decide(
    layout: &Layout,
    args: AttentionDecideArgs,
    as_json: bool,
) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call(
            "attention.decide",
            json!({
                "repo": args.repo,
                "item": args.item,
                "holder": args.holder,
                "generation": args.generation,
                "budget_usd": args.budget_usd,
                "budget_tokens": args.budget_tokens,
            }),
        )
        .await?;
    if as_json {
        println!("{result}");
        return Ok(());
    }
    println!("{result:#}");
    Ok(())
}

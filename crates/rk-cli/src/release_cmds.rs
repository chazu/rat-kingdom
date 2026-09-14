//! Release subcommands: prepare and inspect immutable paired RK/MCP releases
//! (P6.1). See `rk_daemon::release` for the durable identity/content model —
//! this module is thin wire glue over `release.prepare`/`release.list`/
//! `release.show`.

use anyhow::{bail, Result};
use clap::{Args, Subcommand};
use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::{json, Value};

#[derive(Subcommand)]
pub enum ReleaseCommand {
    /// Build (or idempotently return) one immutable paired rk/rk-mcp release.
    Prepare(PrepareArgs),
    /// List prepared/failed/in-progress releases.
    List(ListArgs),
    /// Inspect one release: source, config provenance, binary hashes, checks.
    Show {
        /// Release id (e.g. `rel-...`).
        id: String,
    },
}

#[derive(Args)]
pub struct PrepareArgs {
    /// Registered repository name.
    #[arg(long)]
    pub repo: String,
    /// Exact source to build: a branch, tag, or commit sha.
    #[arg(long)]
    pub candidate: String,
    /// Build recipe. Only `paired-rk-mcp` exists today.
    #[arg(long)]
    pub recipe: Option<String>,
}

#[derive(Args)]
pub struct ListArgs {
    /// Only releases for this repo.
    #[arg(long)]
    pub repo: Option<String>,
}

pub async fn run(layout: &Layout, command: ReleaseCommand, as_json: bool) -> Result<()> {
    match command {
        ReleaseCommand::Prepare(args) => prepare(layout, args, as_json).await,
        ReleaseCommand::List(args) => list(layout, args, as_json).await,
        ReleaseCommand::Show { id } => show(layout, id, as_json).await,
    }
}

async fn prepare(layout: &Layout, args: PrepareArgs, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let mut params = json!({
        "repo": args.repo,
        "candidate": args.candidate,
    });
    if let Some(recipe) = args.recipe {
        params["recipe"] = json!(recipe);
    }
    let result = client.call("release.prepare", params).await?;
    if as_json {
        println!("{result}");
        return Ok(());
    }
    let release = &result["release"];
    let already = result["already_prepared"].as_bool().unwrap_or(false);
    println!(
        "{} {} — {} (repo {}, recipe {})",
        if already {
            "already prepared"
        } else {
            "prepared"
        },
        release["id"].as_str().unwrap_or("?"),
        release["status"].as_str().unwrap_or("?"),
        release["repo"].as_str().unwrap_or("?"),
        release["recipe"].as_str().unwrap_or("?"),
    );
    if let Some(manifest) = release.get("manifest").filter(|m| !m.is_null()) {
        print_manifest_summary(manifest);
    }
    Ok(())
}

async fn list(layout: &Layout, args: ListArgs, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let mut params = serde_json::Map::new();
    if let Some(repo) = args.repo {
        params.insert("repo".into(), json!(repo));
    }
    let result = client.call("release.list", Value::Object(params)).await?;
    let releases = result["releases"].as_array().cloned().unwrap_or_default();
    if as_json {
        println!("{}", result["releases"]);
        return Ok(());
    }
    if releases.is_empty() {
        println!("(no releases)");
        return Ok(());
    }
    println!(
        "{:<24} {:<12} {:<14} {:<12} SOURCE",
        "ID", "STATUS", "REPO", "RECIPE"
    );
    for r in &releases {
        println!(
            "{:<24} {:<12} {:<14} {:<12} {}",
            r["id"].as_str().unwrap_or("?"),
            r["status"].as_str().unwrap_or("?"),
            r["repo"].as_str().unwrap_or("?"),
            r["recipe"].as_str().unwrap_or("?"),
            r["requested_source"].as_str().unwrap_or("?"),
        );
    }
    Ok(())
}

async fn show(layout: &Layout, id: String, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client.call("release.show", json!({ "id": id })).await?;
    if result["release"].is_null() {
        if as_json {
            println!("{}", json!({ "release": null }));
            return Ok(());
        }
        bail!("no such release: {id}");
    }
    if as_json {
        // `content_verified` is a sibling of `release` in the RPC response,
        // not a field of it (see `handle_release_show`) — print the whole
        // result so a JSON consumer sees the integrity verdict too, not just
        // the (possibly stale/tampered) `release` record.
        println!("{result}");
        return Ok(());
    }
    let release = &result["release"];
    println!(
        "{}: {} (repo {}, recipe {})",
        release["id"].as_str().unwrap_or("?"),
        release["status"].as_str().unwrap_or("?"),
        release["repo"].as_str().unwrap_or("?"),
        release["recipe"].as_str().unwrap_or("?"),
    );
    println!(
        "  source     {}",
        release["requested_source"].as_str().unwrap_or("?")
    );
    println!(
        "  created    {}",
        release["created_at"].as_str().unwrap_or("?")
    );
    if let Some(detail) = release["detail"].as_str() {
        println!("  detail     {detail}");
    }
    // Read from the top-level result, not `release` — see the comment above.
    match result.get("content_verified").and_then(Value::as_bool) {
        Some(true) => println!("  content    verified"),
        Some(false) => println!("  content    TAMPERED OR CORRUPTED — does not match its manifest"),
        None => {}
    }
    if let Some(manifest) = release.get("manifest").filter(|m| !m.is_null()) {
        print_manifest_summary(manifest);
    }
    Ok(())
}

fn print_manifest_summary(manifest: &Value) {
    println!(
        "  source     resolved {} (tree {})",
        manifest["source"]["resolved_commit"]
            .as_str()
            .unwrap_or("?"),
        manifest["source"]["tree_sha"].as_str().unwrap_or("?"),
    );
    println!(
        "  toolchain  {}",
        manifest["toolchain"].as_str().unwrap_or("(unavailable)")
    );
    if let Some(binaries) = manifest["binaries"].as_object() {
        for (name, artifact) in binaries {
            println!(
                "  binary     {name}  sha256={}  {} bytes",
                artifact["sha256"].as_str().unwrap_or("?"),
                artifact["size_bytes"].as_u64().unwrap_or(0),
            );
        }
    }
    if let Some(checks) = manifest["checks"].as_array() {
        for check in checks {
            println!(
                "  check      {}  exit={:?}  passed={}",
                check["name"].as_str().unwrap_or("?"),
                check["exit_code"],
                check["passed"].as_bool().unwrap_or(false),
            );
        }
    }
    println!(
        "  compatibility_checked  {} (bounded smoke checks only — see docs)",
        manifest["config_provenance"]["compatibility_checked"]
            .as_bool()
            .unwrap_or(false),
    );
}

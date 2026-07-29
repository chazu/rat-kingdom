//! Repo subcommands: register repositories so the system knows where they live.

use anyhow::{bail, Result};
use rk_core::paths::Layout;
use rk_daemon::onboarding::AssessmentReport;
use rk_daemon::Client;
use serde_json::json;

pub async fn add(
    layout: &Layout,
    path: String,
    name: Option<String>,
    merge_mode: Option<String>,
    remote: Option<String>,
    as_json: bool,
) -> Result<()> {
    let canonical = std::fs::canonicalize(&path)
        .map_err(|e| anyhow::anyhow!("cannot resolve path {path}: {e}"))?;
    let name = match name {
        Some(n) => n,
        None => canonical
            .file_name()
            .and_then(|s| s.to_str())
            .map(str::to_string)
            .ok_or_else(|| anyhow::anyhow!("cannot infer a name from {}; pass --name", canonical.display()))?,
    };
    let mut params = json!({ "name": name, "path": canonical.to_string_lossy() });
    if let Some(mode) = merge_mode {
        params["merge_mode"] = json!(mode);
    }
    if let Some(remote) = remote {
        params["remote"] = json!(remote);
    }
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client.call("repo.add", params).await?;
    let repo = &result["repo"];
    if as_json {
        println!("{repo}");
    } else {
        println!(
            "registered {} → {} ({} mode)",
            repo["name"].as_str().unwrap_or("?"),
            repo["path"].as_str().unwrap_or("?"),
            repo["merge_mode"].as_str().unwrap_or("direct")
        );
    }
    Ok(())
}

pub async fn list(layout: &Layout, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client.call("repo.list", json!({})).await?;
    if as_json {
        println!("{}", result["repos"]);
        return Ok(());
    }
    let repos = result["repos"].as_array().cloned().unwrap_or_default();
    if repos.is_empty() {
        println!("(no repos registered — rk repo add <path>)");
        return Ok(());
    }
    println!("{:<16} PATH", "NAME");
    for r in &repos {
        println!(
            "{:<16} {}",
            r["name"].as_str().unwrap_or("?"),
            r["path"].as_str().unwrap_or("?")
        );
    }
    Ok(())
}

pub async fn show(layout: &Layout, name: String, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client.call("repo.get", json!({ "name": name })).await?;
    if result["repo"].is_null() {
        if as_json {
            println!("{}", json!({ "repo": null }));
            return Ok(());
        }
        bail!("no such repo: {name} (rk repo add <path>)");
    }
    let repo = &result["repo"];
    if as_json {
        println!("{repo}");
        return Ok(());
    }
    println!("{}", repo["name"].as_str().unwrap_or("?"));
    println!("  path       {}", repo["path"].as_str().unwrap_or("?"));
    println!("  registered {}", repo["created_at"].as_str().unwrap_or("?"));
    println!("  merge      {}", repo["merge_mode"].as_str().unwrap_or("direct"));
    println!("  remote     {}", repo["remote"].as_str().unwrap_or("origin"));
    if let Some(host) = repo["host"].as_str() {
        println!("  host       {host}");
    }
    // Show its open tickets as a convenience.
    let tickets = client
        .call("ticket.list", json!({ "scope": name, "status": "open" }))
        .await?;
    let tickets = tickets["tickets"].as_array().cloned().unwrap_or_default();
    if !tickets.is_empty() {
        println!("\nopen tickets:");
        for t in &tickets {
            println!(
                "  {:<8} {}",
                t["identity"].as_str().unwrap_or("?"),
                t["payload"]["title"].as_str().unwrap_or("")
            );
        }
    }
    Ok(())
}

pub async fn onboard_inspect(layout: &Layout, target: String, as_json: bool) -> Result<()> {
    // Inspection is a strict read-only path: unlike ordinary convenience
    // commands, it must not auto-start a daemon (which would write the castle
    // socket, pid, logs, and identity files).
    let mut client = Client::connect(layout).await?;
    let result = client
        .call("repo.onboard.inspect", json!({ "target": target }))
        .await?;
    let report: AssessmentReport = serde_json::from_value(result["report"].clone())?;

    if as_json {
        println!("{}", serde_json::to_string(&report)?);
    } else {
        print_assessment(&report);
    }
    if !report.ready {
        bail!("repository assessment failed closed; resolve the error findings above");
    }
    Ok(())
}

fn print_assessment(report: &AssessmentReport) {
    let identity = report
        .identity
        .registered_name
        .as_deref()
        .unwrap_or(&report.identity.target);
    let path = report
        .identity
        .canonical_path
        .as_deref()
        .unwrap_or("(unresolved)");
    println!("repository assessment: {identity}");
    println!("  path   {path}");
    println!(
        "  ready  {}",
        if report.ready {
            "yes"
        } else {
            "no (fail-closed)"
        }
    );
    println!("  schema {}", report.schema_version);
    println!("\nfindings:");
    for finding in &report.findings {
        println!(
            "  {:<7} {:<28} {}",
            finding.severity.to_string().to_uppercase(),
            finding.kind,
            finding.summary
        );
        for evidence in &finding.evidence {
            if let Some(command) = &evidence.command {
                println!(
                    "           evidence [{}] {}: {} — `{}`",
                    evidence.origin, evidence.source, evidence.detail, command
                );
            } else {
                println!(
                    "           evidence [{}] {}: {}",
                    evidence.origin, evidence.source, evidence.detail
                );
            }
        }
        if let Some(ambiguity) = &finding.unresolved_ambiguity {
            println!("           unresolved ambiguity: {ambiguity}");
        }
        if let Some(recommendation) = &finding.recommendation {
            if let Some(command) = &recommendation.command {
                println!(
                    "           recommendation [{}]: {} — `{}`",
                    recommendation.origin, recommendation.action, command
                );
            } else {
                println!(
                    "           recommendation [{}]: {}",
                    recommendation.origin, recommendation.action
                );
            }
        }
    }
}

/// Resolve a repo argument that may be either a filesystem path or a registered
/// repo name into an absolute path string. Used by `rk spawn`.
pub async fn resolve_path(client: &mut Client, repo: &str) -> Result<String> {
    if let Ok(path) = std::fs::canonicalize(repo) {
        return Ok(path.to_string_lossy().into_owned());
    }
    let result = client.call("repo.get", json!({ "name": repo })).await?;
    if let Some(path) = result["repo"]["path"].as_str() {
        return Ok(path.to_string());
    }
    bail!("'{repo}' is neither a path nor a registered repo (rk repo add <path>)");
}

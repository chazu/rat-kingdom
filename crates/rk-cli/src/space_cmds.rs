//! Tuplespace subcommands, including the env-autofilled sugar commands.
//!
//! Sugar commands are the schema-enforcement layer: they construct payloads
//! themselves from `RK_*` env (set at spawn), so agents cannot write malformed
//! coordination tuples, and a missing env var is an explicit error instead of
//! a silently absent field (the predecessor's foreman-routing lesson).

use anyhow::{bail, Context, Result};
use clap::Args;
use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::{json, Value};

#[derive(Args)]
pub struct OutArgs {
    /// Tuple category (fact, task, claim, obstacle, need, artifact, event, ...).
    pub category: String,
    /// Scope (repo name, or "system").
    pub scope: String,
    /// Identity (what the tuple is about).
    pub identity: String,
    /// JSON payload.
    #[arg(long, default_value = "null")]
    pub payload: String,
    /// Lifecycle class: furniture | session | ephemeral.
    #[arg(long)]
    pub lifecycle: Option<String>,
    /// TTL like "90s", "5m", "2h" (implies ephemeral).
    #[arg(long)]
    pub ttl: Option<String>,
}

#[derive(Args)]
pub struct ReadArgs {
    /// Tuple category to match.
    pub category: Option<String>,
    /// Scope to match.
    pub scope: Option<String>,
    /// Identity to match.
    pub identity: Option<String>,
    /// Substring to require in the serialized payload.
    #[arg(long)]
    pub search: Option<String>,
    /// How long to block, like "30s" or "10m".
    #[arg(long, default_value = "5s")]
    pub timeout: String,
}

#[derive(Args)]
pub struct ScanArgs {
    /// Tuple category to match.
    pub category: Option<String>,
    /// Scope to match.
    pub scope: Option<String>,
    /// Identity to match.
    pub identity: Option<String>,
    /// Substring to require in the serialized payload.
    #[arg(long)]
    pub search: Option<String>,
}

#[derive(Args)]
pub struct DoneArgs {
    /// Optional summary of what was accomplished.
    pub summary: Option<String>,
}

#[derive(Args)]
pub struct TextArgs {
    /// Description of the obstacle/need.
    pub text: String,
}

#[derive(Args)]
pub struct ClaimArgs {
    /// Task id being claimed.
    pub task: String,
}

fn parse_duration(s: &str) -> Result<std::time::Duration> {
    let s = s.trim();
    let (num, unit) = s.split_at(s.len().saturating_sub(1));
    let (value, mult) = match unit {
        "s" => (num, 1u64),
        "m" => (num, 60),
        "h" => (num, 3600),
        _ => (s, 1), // bare number = seconds
    };
    let n: u64 = value
        .parse()
        .with_context(|| format!("invalid duration: {s}"))?;
    Ok(std::time::Duration::from_secs(n * mult))
}

fn pattern_params(
    category: &Option<String>,
    scope: &Option<String>,
    identity: &Option<String>,
    search: &Option<String>,
) -> Value {
    let mut p = serde_json::Map::new();
    if let Some(c) = category {
        p.insert("category".into(), json!(c));
    }
    if let Some(s) = scope {
        p.insert("scope".into(), json!(s));
    }
    if let Some(i) = identity {
        p.insert("identity".into(), json!(i));
    }
    if let Some(q) = search {
        p.insert("payload_search".into(), json!(q));
    }
    Value::Object(p)
}

fn env_required(name: &str) -> Result<String> {
    std::env::var(name).map_err(|_| {
        anyhow::anyhow!(
            "{name} is not set — sugar commands need the spawn environment \
             (RK_AGENT, RK_REPO, RK_TASK, ...); use `rk out` for manual writes"
        )
    })
}

fn print_tuples(tuples: &Value, as_json: bool) {
    if as_json {
        println!("{tuples}");
        return;
    }
    let Some(arr) = tuples.as_array() else {
        println!("{tuples}");
        return;
    };
    if arr.is_empty() {
        println!("(no tuples)");
        return;
    }
    for t in arr {
        print_tuple_line(t);
    }
}

fn print_tuple_line(t: &Value) {
    println!(
        "{:10} {:12} {:24} [{}] {}",
        t["category"].as_str().unwrap_or("?"),
        t["scope"].as_str().unwrap_or("?"),
        t["identity"].as_str().unwrap_or("?"),
        t["instance"].as_str().unwrap_or("?"),
        t["payload"]
    );
}

pub async fn out(layout: &Layout, args: OutArgs, as_json: bool) -> Result<()> {
    let payload: Value =
        serde_json::from_str(&args.payload).context("--payload must be valid JSON")?;
    let mut params = json!({
        "category": args.category,
        "scope": args.scope,
        "identity": args.identity,
        "payload": payload,
    });
    if let Some(l) = &args.lifecycle {
        params["lifecycle"] = json!(l);
    }
    if let Some(ttl) = &args.ttl {
        params["ttl_secs"] = json!(parse_duration(ttl)?.as_secs());
    }
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client.call("space.out", params).await?;
    if as_json {
        println!("{result}");
    } else {
        println!("written {}", result["id"].as_str().unwrap_or("?"));
    }
    Ok(())
}

pub async fn blocking_read(
    layout: &Layout,
    args: ReadArgs,
    destructive: bool,
    as_json: bool,
) -> Result<()> {
    let mut params = pattern_params(&args.category, &args.scope, &args.identity, &args.search);
    params["timeout_ms"] = json!(parse_duration(&args.timeout)?.as_millis() as u64);
    let mut client = Client::connect_or_spawn(layout).await?;
    let method = if destructive {
        "space.take"
    } else {
        "space.rd"
    };
    let result = client.call(method, params).await?;
    if result["tuple"].is_null() {
        if as_json {
            println!("{result}");
        } else {
            eprintln!("timed out");
        }
        std::process::exit(2);
    }
    if as_json {
        println!("{}", result["tuple"]);
    } else {
        print_tuple_line(&result["tuple"]);
    }
    Ok(())
}

pub async fn scan(layout: &Layout, args: ScanArgs, as_json: bool) -> Result<()> {
    let params = pattern_params(&args.category, &args.scope, &args.identity, &args.search);
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client.call("space.scan", params).await?;
    print_tuples(&result["tuples"], as_json);
    Ok(())
}

pub async fn watch(layout: &Layout, args: ScanArgs) -> Result<()> {
    let params = pattern_params(&args.category, &args.scope, &args.identity, &args.search);
    let client = Client::connect_or_spawn(layout).await?;
    let mut stream = client.watch(params).await?;
    while let Some(note) = stream.next().await? {
        match note["method"].as_str() {
            Some("tuple") => println!("{}", note["params"]),
            Some("lagged") => eprintln!("(watch lagged: missed {})", note["params"]["missed"]),
            _ => {}
        }
    }
    Ok(())
}

pub async fn done(layout: &Layout, args: DoneArgs, as_json: bool) -> Result<()> {
    let agent = env_required("RK_AGENT")?;
    let repo = env_required("RK_REPO")?;
    let task = env_required("RK_TASK")?;
    let mut payload = json!({
        "task": task,
        "agent": agent,
        "branch": std::env::var("RK_BRANCH").ok(),
        "parent": std::env::var("RK_PARENT").ok(),
    });
    if let Some(summary) = args.summary {
        payload["summary"] = json!(summary);
    }
    write_sugar(layout, "event", &repo, "task_done", payload, as_json).await
}

pub async fn report(layout: &Layout, args: TextArgs, category: &str, as_json: bool) -> Result<()> {
    if args.text.trim().is_empty() {
        bail!("description must not be empty");
    }
    let agent = env_required("RK_AGENT")?;
    let repo = env_required("RK_REPO")?;
    let payload = json!({
        "agent": agent,
        "task": std::env::var("RK_TASK").ok(),
        "text": args.text,
    });
    write_sugar(layout, category, &repo, &agent, payload, as_json).await
}

pub async fn claim(layout: &Layout, args: ClaimArgs, as_json: bool) -> Result<()> {
    let agent = env_required("RK_AGENT")?;
    let repo = env_required("RK_REPO")?;
    let payload = json!({
        "agent": agent,
        "claimed_at": chrono::Utc::now().to_rfc3339(),
    });
    write_sugar(layout, "claim", &repo, &args.task, payload, as_json).await
}

async fn write_sugar(
    layout: &Layout,
    category: &str,
    scope: &str,
    identity: &str,
    payload: Value,
    as_json: bool,
) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call(
            "space.out",
            json!({
                "category": category,
                "scope": scope,
                "identity": identity,
                "payload": payload,
            }),
        )
        .await?;
    if as_json {
        println!("{result}");
    } else {
        println!("{category} recorded");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_parse() {
        assert_eq!(parse_duration("30s").unwrap().as_secs(), 30);
        assert_eq!(parse_duration("5m").unwrap().as_secs(), 300);
        assert_eq!(parse_duration("2h").unwrap().as_secs(), 7200);
        assert_eq!(parse_duration("45").unwrap().as_secs(), 45);
        assert!(parse_duration("nope").is_err());
    }
}

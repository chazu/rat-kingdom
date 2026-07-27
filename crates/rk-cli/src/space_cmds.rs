//! Tuplespace subcommands, including the env-autofilled sugar commands.
//!
//! Sugar commands are the schema-enforcement layer: they construct payloads
//! themselves from `RK_*` env (set at spawn), so agents cannot write malformed
//! coordination tuples, and a missing env var is an explicit error instead of
//! a silently absent field (the predecessor's foreman-routing lesson).

use anyhow::{bail, Context, Result};
use clap::Args;
use rk_core::identity::CastleDisplay;
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
    /// Backlink this artifact to the obstacle/need id it resolves. The reactor
    /// then retires that wall and lays a decaying (topic -> this artifact) trail
    /// so the next rat hitting the same wall is steered here (stigmergy P8).
    #[arg(long, value_name = "OBSTACLE_OR_NEED_ID")]
    pub resolves: Option<String>,
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
pub struct HotScanArgs {
    #[command(flatten)]
    pub base: ScanArgs,
    /// Rank strongest-first (category × recency × strength) instead of
    /// oldest-first — follow the hottest trail (stigmergy P7).
    #[arg(long)]
    pub hot: bool,
    /// Return only the top N strongest matches (implies --hot).
    #[arg(long, value_name = "N")]
    pub top: Option<usize>,
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
    /// The area you are working in (a path, glob, or label) — or a task id.
    pub area: String,
    /// How long the claim lives before evaporating, like "30m" or "2h".
    #[arg(long, default_value = "30m")]
    pub ttl: String,
}

#[derive(Args)]
pub struct SuggestArgs {
    /// The proposed norm, in one line (e.g. "always rebase, never merge").
    pub text: String,
    /// Voting window before the proposal decays if it misses quorum.
    #[arg(long, default_value = "24h")]
    pub ttl: String,
}

#[derive(Args)]
pub struct EndorseArgs {
    /// The suggestion id to endorse (printed by `rk suggest`).
    pub suggestion: String,
    /// Voting window before the endorsement decays.
    #[arg(long, default_value = "24h")]
    pub ttl: String,
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

fn print_tuples(tuples: &Value, as_json: bool, display: &CastleDisplay) {
    // JSON output is the raw wire shape (author = actor id) so machine consumers
    // and `--json` piping never see a presentation alias in place of the id.
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
        print_tuple_line(t, display);
    }
}

fn print_tuple_line(t: &Value, display: &CastleDisplay) {
    let author = t["instance"].as_str().unwrap_or("?");
    println!(
        "{:10} {:12} {:24} [{}] {}",
        t["category"].as_str().unwrap_or("?"),
        t["scope"].as_str().unwrap_or("?"),
        t["identity"].as_str().unwrap_or("?"),
        // Author column: resolve THIS castle's own actor id to its friendly alias
        // for the human; every other author is shown verbatim.
        display.resolve(author),
        t["payload"]
    );
}

/// Stamp the writing agent's name into an object payload that does not already
/// name one — the raw-`rk out` counterpart to the sugar commands' env autofill.
///
/// A durable tuple is otherwise anonymous, so nothing downstream can tell one
/// author's record from a concurrent peer's. That is TKT-161: a `steward`
/// reviewer records `artifact/<repo>/review`, and with several stewards in
/// flight on one repo (the reactor fires one per rat completion, by design) the
/// newest `review` in that scope may be the OTHER instance's. A workflow `read`
/// with `fromAgent: true` keys on this stamp, so putting it here rather than in
/// the prompt means no reviewer can forget it and route a land onto a stranger's
/// verdict.
///
/// An explicit `agent` in the payload is left alone: `rk out need … '{"agent":
/// "steward", …}'` is naming the *role* that speaks, not the process that ran,
/// and overwriting it would rewrite what `rk inbox` shows the operator.
fn stamp_author(payload: &mut Value, agent: Option<String>) {
    let (Some(agent), Some(obj)) = (agent, payload.as_object_mut()) else {
        return;
    };
    obj.entry("agent").or_insert_with(|| json!(agent));
}

pub async fn out(layout: &Layout, args: OutArgs, as_json: bool) -> Result<()> {
    let mut payload: Value =
        serde_json::from_str(&args.payload).context("--payload must be valid JSON")?;
    // --resolves rides in the payload as a backlink the reactor keys on. It only
    // has meaning on an artifact, but we attach it structurally rather than
    // second-guess the category here — the reactor reacts to artifacts alone.
    if let Some(resolves) = &args.resolves {
        if !payload.is_object() {
            payload = json!({});
        }
        payload["resolves"] = json!(resolves);
    }
    stamp_author(&mut payload, std::env::var("RK_AGENT").ok());
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
    display: &CastleDisplay,
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
        print_tuple_line(&result["tuple"], display);
    }
    Ok(())
}

pub async fn scan(
    layout: &Layout,
    args: HotScanArgs,
    as_json: bool,
    display: &CastleDisplay,
) -> Result<()> {
    let base = &args.base;
    let mut params =
        pattern_params(&base.category, &base.scope, &base.identity, &base.search);
    if args.hot {
        params["hot"] = json!(true);
    }
    if let Some(n) = args.top {
        params["top"] = json!(n);
    }
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client.call("space.scan", params).await?;
    if as_json {
        println!("{result}");
    } else {
        print_tuples(&result["tuples"], false, display);
        if result["truncated"].as_bool() == Some(true) {
            eprintln!("(scan truncated at 10,000 tuples; narrow the pattern or use --top)");
        }
    }
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
        "workflow_instance": std::env::var("RK_WORKFLOW_INSTANCE").ok(),
    });
    if let Some(summary) = args.summary {
        payload["summary"] = json!(summary);
    }
    write_sugar(
        layout, "event", &repo, "task_done", None, payload, None, as_json,
    )
    .await
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
    // instance = agent so a rat's own re-stated obstacle/need reinforces its
    // one trail, and never collides with a supervisor-authored obstacle on the
    // same identity (those keep instance = castle). TTL/strength are defaulted
    // by the daemon for evaporating categories.
    write_sugar(
        layout,
        category,
        &repo,
        &agent,
        Some(&agent),
        payload,
        None,
        as_json,
    )
    .await
}

pub async fn claim(layout: &Layout, args: ClaimArgs, as_json: bool) -> Result<()> {
    let agent = env_required("RK_AGENT")?;
    let repo = env_required("RK_REPO")?;
    // Claims are advisory trails, not locks: written Ephemeral so an abandoned
    // claim evaporates instead of becoming a permanent no-go zone. Re-claiming
    // the same area reinforces the trail (keyed on identity=area, instance=agent)
    // — refreshing its TTL and strength rather than piling up duplicates.
    let ttl = parse_duration(&args.ttl)?;
    let payload = json!({
        "agent": agent,
        "area": args.area,
        "claimed_at": chrono::Utc::now().to_rfc3339(),
    });
    write_sugar(
        layout,
        "claim",
        &repo,
        &args.area,
        Some(&agent),
        payload,
        Some(ttl.as_secs()),
        as_json,
    )
    .await
}

/// Propose a norm. Writes a `Suggestion` on the system scope authored by this
/// agent (`instance = RK_AGENT`, so distinct-endorser counting works), mints a
/// human-scale suggestion id, and prints it so peers can `rk endorse <id>`.
pub async fn suggest(layout: &Layout, args: SuggestArgs, as_json: bool) -> Result<()> {
    if args.text.trim().is_empty() {
        bail!("suggestion text must not be empty");
    }
    let agent = env_required("RK_AGENT")?;
    let ttl = parse_duration(&args.ttl)?;
    let sug_id = rk_core::id::prefixed_id("sug");
    let payload = json!({
        "agent": agent,
        "task": std::env::var("RK_TASK").ok(),
        "text": args.text,
    });
    let params = json!({
        "category": "suggestion",
        "scope": rk_core::tuple::SYSTEM_SCOPE,
        "identity": sug_id,
        "instance": agent,
        "payload": payload,
        "ttl_secs": ttl.as_secs(),
    });
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client.call("space.out", params).await?;
    if as_json {
        println!("{}", json!({ "suggestion": sug_id, "id": result["id"] }));
    } else {
        println!("suggestion {sug_id} recorded — endorse it with `rk endorse {sug_id}`");
    }
    Ok(())
}

/// Endorser identity for a vote cast outside the spawn environment — a human at
/// a terminal rather than a rat. Fixed rather than per-shell so the operator is
/// exactly ONE distinct endorser however many times they run it, and so a repeat
/// vote stays idempotent.
const OPERATOR_ENDORSER: &str = "operator";

/// Vote for a suggestion. Writes an `Endorsement` keyed by
/// `(identity = suggestion, instance = RK_AGENT)`, so re-endorsing is
/// idempotent. The reactor counts distinct endorsers and promotes at quorum.
///
/// Unlike every other sugar command this one does NOT require the spawn
/// environment: `rk endorse <sug-id>` is the resolving command on an
/// `open-suggestion` inbox row (TKT-167), so a human runs it from a plain shell.
/// A row whose action errors out on a missing `RK_AGENT` is not a resolving
/// command, and the operator is the one endorser who is always reachable when a
/// ballot is about to decay — so an unset `RK_AGENT` votes as `operator`.
pub async fn endorse(layout: &Layout, args: EndorseArgs, as_json: bool) -> Result<()> {
    let agent = std::env::var("RK_AGENT").unwrap_or_else(|_| OPERATOR_ENDORSER.to_string());
    let ttl = parse_duration(&args.ttl)?;
    let mut client = Client::connect_or_spawn(layout).await?;

    // Idempotent: one endorsement per (suggestion, agent). A duplicate would not
    // inflate the reactor's distinct-instance tally anyway, but skipping it keeps
    // the space clean and the command honest about being a no-op.
    let existing = client
        .call(
            "space.scan",
            json!({
                "category": "endorsement",
                "scope": rk_core::tuple::SYSTEM_SCOPE,
                "identity": args.suggestion,
                "instance": agent,
            }),
        )
        .await?;
    if existing["tuples"]
        .as_array()
        .map(|a| !a.is_empty())
        .unwrap_or(false)
    {
        if as_json {
            println!("{}", json!({ "endorsed": args.suggestion, "already": true }));
        } else {
            println!("already endorsed {}", args.suggestion);
        }
        return Ok(());
    }

    let payload = json!({
        "agent": agent,
        "suggestion": args.suggestion,
    });
    let params = json!({
        "category": "endorsement",
        "scope": rk_core::tuple::SYSTEM_SCOPE,
        "identity": args.suggestion,
        "instance": agent,
        "payload": payload,
        "ttl_secs": ttl.as_secs(),
    });
    let result = client.call("space.out", params).await?;
    if as_json {
        println!("{result}");
    } else {
        println!("endorsed {}", args.suggestion);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn write_sugar(
    layout: &Layout,
    category: &str,
    scope: &str,
    identity: &str,
    instance: Option<&str>,
    payload: Value,
    ttl_secs: Option<u64>,
    as_json: bool,
) -> Result<()> {
    let mut params = json!({
        "category": category,
        "scope": scope,
        "identity": identity,
        "payload": payload,
    });
    if let Some(inst) = instance {
        params["instance"] = json!(inst);
    }
    if let Some(ttl) = ttl_secs {
        params["ttl_secs"] = json!(ttl);
    }
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client.call("space.out", params).await?;
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

    /// The stamp is what makes a durable tuple attributable, so a workflow
    /// `read` with `fromAgent: true` can tell this instance's reviewer from a
    /// concurrent instance's (TKT-161).
    #[test]
    fn out_stamps_the_writing_agent() {
        let mut payload = json!({"recommendation": "APPROVE"});
        stamp_author(&mut payload, Some("Filch-2".into()));
        assert_eq!(payload["agent"], json!("Filch-2"));
        assert_eq!(payload["recommendation"], json!("APPROVE"));
        // Exactly the substring `Pattern::for_agent_since` searches for.
        assert!(payload.to_string().contains("\"agent\":\"Filch-2\""));
    }

    #[test]
    fn out_never_overwrites_an_explicit_agent() {
        // `rk out need … '{"agent":"steward", …}'` names the role that speaks;
        // rewriting it would change what `rk inbox` shows the operator.
        let mut payload = json!({"agent": "steward", "text": "needs a human"});
        stamp_author(&mut payload, Some("Filch-2".into()));
        assert_eq!(payload["agent"], json!("steward"));
    }

    #[test]
    fn out_leaves_non_objects_and_operator_shells_alone() {
        // No RK_AGENT: a hand-run `rk out` from an operator shell is not an
        // agent and must not claim to be one.
        let mut payload = json!({"recommendation": "APPROVE"});
        stamp_author(&mut payload, None);
        assert_eq!(payload, json!({"recommendation": "APPROVE"}));

        // A scalar/null payload has nowhere to put the stamp; leave it as-is
        // rather than reshaping what the caller asked to write.
        let mut scalar = json!(null);
        stamp_author(&mut scalar, Some("Filch-2".into()));
        assert_eq!(scalar, json!(null));
    }
}

//! Tuplespace subcommands, including the env-autofilled sugar commands.
//!
//! Sugar commands are the schema-enforcement layer: they construct payloads
//! themselves from `RK_*` env (set at spawn), so agents cannot write malformed
//! coordination tuples, and a missing env var is an explicit error instead of
//! a silently absent field (the predecessor's foreman-routing lesson).

use anyhow::{bail, Context, Result};
use clap::{Args, ValueEnum};
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
    /// Add one string field to the payload object as KEY=VALUE (repeatable).
    /// Fields are applied on top of --payload and need no JSON quoting, so
    /// shell callers (workflow checks, hooks) can build payloads without jq.
    #[arg(long, value_name = "KEY=VALUE")]
    pub field: Vec<String>,
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
    /// Optional voting window. Omitted (the default) the ballot is durable and
    /// closes on its outcome, not on a clock — see [`ballot_ttl_secs`].
    #[arg(long)]
    pub ttl: Option<String>,
}

#[derive(Args)]
pub struct EndorseArgs {
    /// The suggestion id to endorse (printed by `rk suggest`).
    pub suggestion: String,
    /// Optional lifetime for this vote. Omitted (the default) the vote is
    /// durable — see [`ballot_ttl_secs`].
    #[arg(long)]
    pub ttl: Option<String>,
}

#[derive(Args)]
pub struct WithdrawArgs {
    /// The suggestion id to withdraw (printed by `rk suggest`).
    pub suggestion: String,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum FactVote {
    Up,
    Down,
    Clear,
}

impl FactVote {
    fn as_str(self) -> &'static str {
        match self {
            Self::Up => "up",
            Self::Down => "down",
            Self::Clear => "clear",
        }
    }
}

#[derive(Args)]
pub struct FactVoteArgs {
    /// The fact tuple id shown by `rk scan fact ...` or in the priming prompt.
    pub fact: String,
    /// The vote to cast; `clear` retracts this agent's current vote.
    pub vote: FactVote,
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

/// Resolve the optional voting window a ballot (`rk suggest` / `rk endorse`)
/// was given into the `ttl_secs` the RPC `out` boundary wants, or `None` to
/// send no window at all — which lands the tuple `Session`-durable (TKT-168).
///
/// Ballots used to default to a 24h window and the whole norms program produced
/// zero conventions in the fleet's life, because quorum then means *3 distinct
/// rats inside one overlapping 24h window* — a wall-clock bound on a fleet whose
/// activity is bursty and whose rats live minutes. Durable is the right default
/// for three reasons, none of which a longer window would fix:
///
/// - **A vote cannot be reinforced.** Every other TTL'd tuple is a pheromone its
///   author refreshes while the condition holds — a rat re-`claim`s the area it
///   is still editing. An endorser is dead minutes after voting, so nobody can
///   ever re-cast its vote; decay destroys information that cannot be
///   regenerated. Reinforcement is not merely unused here, it is impossible.
/// - **Decay buys no freshness.** Promotion mints a `Furniture` Convention that
///   is permanent regardless, so expiring the ballot can never make the *output*
///   more current. It only ever makes promotion harder: a pure cost.
/// - **Ephemeral tuples do not replicate.** `ttl_secs` forces `Lifecycle::
///   Ephemeral` at the `out` boundary, and rk-sync exports durable lifecycles
///   only — so a windowed ballot was invisible to every other castle while the
///   Convention it would promote to replicates. Votes could never pool.
///
/// So a ballot is a ledger entry, not a trail: it closes when it promotes, and
/// three rats reach quorum by *ever* agreeing rather than by overlapping.
/// `--ttl` stays for a deliberately time-boxed vote ("this only holds through
/// the migration"), which is a real thing to want and now an explicit act.
fn ballot_ttl_secs(ttl: Option<&str>) -> Result<Option<u64>> {
    ttl.map(|t| parse_duration(t).map(|d| d.as_secs()))
        .transpose()
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

fn print_tuples(
    tuples: &Value,
    as_json: bool,
    display: &CastleDisplay,
    fact_votes: Option<&Value>,
) {
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
        print_tuple_line(t, display, fact_votes);
    }
}

fn print_tuple_line(t: &Value, display: &CastleDisplay, fact_votes: Option<&Value>) {
    let author = t["instance"].as_str().unwrap_or("?");
    let mut line = format!(
        "{:10} {:12} {:24} {:26} [{}] {}",
        t["category"].as_str().unwrap_or("?"),
        t["scope"].as_str().unwrap_or("?"),
        t["identity"].as_str().unwrap_or("?"),
        t["id"].as_str().unwrap_or("?"),
        // Author column: resolve THIS castle's own actor id to its friendly alias
        // for the human; every other author is shown verbatim.
        display.resolve(author),
        t["payload"]
    );
    if t["category"].as_str() == Some("fact") {
        if let Some(votes) = fact_votes.and_then(|all| all.get(t["id"].as_str().unwrap_or(""))) {
            line.push_str(&format!(
                " (votes +{}/-{}/{})",
                votes["up"].as_u64().unwrap_or(0),
                votes["down"].as_u64().unwrap_or(0),
                votes["score"].as_i64().unwrap_or(0)
            ));
        }
    }
    println!("{line}");
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

/// Apply `--field KEY=VALUE` entries on top of a parsed `--payload` value.
///
/// A `null` payload becomes an object so `--field` works without `--payload`;
/// a non-object payload cannot carry fields and is an explicit error rather
/// than a silent drop. Values are always JSON strings — serde does the
/// escaping the shell caller would otherwise need jq for.
fn apply_fields(payload: &mut Value, fields: &[String]) -> Result<()> {
    if fields.is_empty() {
        return Ok(());
    }
    if payload.is_null() {
        *payload = json!({});
    }
    let Some(obj) = payload.as_object_mut() else {
        bail!("--field requires an object --payload (or none)");
    };
    for field in fields {
        let Some((key, value)) = field.split_once('=') else {
            bail!("--field must be KEY=VALUE, got '{field}'");
        };
        if key.is_empty() {
            bail!("--field key must not be empty in '{field}'");
        }
        obj.insert(key.to_string(), json!(value));
    }
    Ok(())
}

pub async fn out(layout: &Layout, args: OutArgs, as_json: bool) -> Result<()> {
    let mut payload: Value =
        serde_json::from_str(&args.payload).context("--payload must be valid JSON")?;
    apply_fields(&mut payload, &args.field)?;
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
        print_tuple_line(&result["tuple"], display, result.get("fact_votes"));
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
    let mut params = pattern_params(&base.category, &base.scope, &base.identity, &base.search);
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
        print_tuples(&result["tuples"], false, display, result.get("fact_votes"));
        if result["truncated"].as_bool() == Some(true) {
            eprintln!("(scan truncated at 10,000 tuples; narrow the pattern or use --top)");
        }
    }
    Ok(())
}

pub async fn fact_vote(layout: &Layout, args: FactVoteArgs, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call(
            "fact.vote",
            json!({"fact": args.fact, "vote": args.vote.as_str()}),
        )
        .await?;
    if as_json {
        println!("{result}");
    } else if result["already"].as_bool() == Some(true) {
        println!(
            "fact {} already {} by {}",
            result["fact"].as_str().unwrap_or("?"),
            result["vote"].as_str().unwrap_or("?"),
            result["voter"].as_str().unwrap_or("?")
        );
    } else {
        let action = match result["vote"].as_str() {
            Some("up") => "upvoted",
            Some("down") => "downvoted",
            Some("clear") => "vote retracted",
            _ => "updated",
        };
        println!(
            "fact {} {} by {}",
            result["fact"].as_str().unwrap_or("?"),
            action,
            result["voter"].as_str().unwrap_or("?")
        );
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
        // `Pattern::for_spawn`'s join key (C1/S3a,
        // docs/2026-08-17-tkt-c1-generation-identity.md). Optional: RK_SPAWN
        // is only set once the daemon mints one at spawn (C6), so a `rk done`
        // run under an unmigrated daemon still resolves via the name+floor
        // fallback instead of writing a null field a reader would trip on.
        "spawn": std::env::var("RK_SPAWN").ok(),
        "branch": std::env::var("RK_BRANCH").ok(),
        "parent": std::env::var("RK_PARENT").ok(),
        "workflow_instance": std::env::var("RK_WORKFLOW_INSTANCE").ok(),
    });
    if let Some(summary) = args.summary {
        payload["summary"] = json!(summary);
    }
    write_sugar(
        layout,
        "event",
        &repo,
        "task_done",
        None,
        payload,
        None,
        as_json,
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
///
/// Durable unless `--ttl` asks otherwise ([`ballot_ttl_secs`]): the ballot stays
/// open — and stays visible as an `rk inbox` `open-suggestion` row — until it
/// promotes, rather than decaying out from under the two endorsements it had.
pub async fn suggest(layout: &Layout, args: SuggestArgs, as_json: bool) -> Result<()> {
    if args.text.trim().is_empty() {
        bail!("suggestion text must not be empty");
    }
    let agent = env_required("RK_AGENT")?;
    let ttl_secs = ballot_ttl_secs(args.ttl.as_deref())?;
    let sug_id = rk_core::id::prefixed_id("sug");
    let payload = json!({
        "agent": agent,
        "task": std::env::var("RK_TASK").ok(),
        "text": args.text,
    });
    let mut params = json!({
        "category": "suggestion",
        "scope": rk_core::tuple::SYSTEM_SCOPE,
        "identity": sug_id,
        "instance": agent,
        "payload": payload,
    });
    if let Some(secs) = ttl_secs {
        params["ttl_secs"] = json!(secs);
    }
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
/// Durable unless `--ttl` asks otherwise ([`ballot_ttl_secs`]): a cast vote is a
/// ledger entry, and its author is gone long before any window would close.
///
/// Unlike every other sugar command this one does NOT require the spawn
/// environment: `rk endorse <sug-id>` is the resolving command on an
/// `open-suggestion` inbox row (TKT-167), so a human runs it from a plain shell.
/// A row whose action errors out on a missing `RK_AGENT` is not a resolving
/// command, and the operator is the one endorser who is always reachable when a
/// ballot is about to decay — so an unset `RK_AGENT` votes as `operator`.
pub async fn endorse(layout: &Layout, args: EndorseArgs, as_json: bool) -> Result<()> {
    let agent = std::env::var("RK_AGENT").unwrap_or_else(|_| OPERATOR_ENDORSER.to_string());
    let ttl_secs = ballot_ttl_secs(args.ttl.as_deref())?;
    let mut client = Client::connect_or_spawn(layout).await?;

    // A withdrawn ballot is closed: the reactor will never promote it however
    // many votes it accumulates (TKT-184). Say so rather than writing a vote
    // that is inert by construction — an endorsement that reports success and
    // does nothing is exactly the silent failure the norms program already had
    // too much of. Advisory only (the reactor is the enforcement), so a race
    // against a concurrent withdraw costs a stray tuple, not a wrong promotion.
    let withdrawn = client
        .call(
            "space.scan",
            json!({
                "category": "withdrawal",
                "scope": rk_core::tuple::SYSTEM_SCOPE,
                "identity": args.suggestion,
            }),
        )
        .await?;
    if withdrawn["tuples"]
        .as_array()
        .map(|a| !a.is_empty())
        .unwrap_or(false)
    {
        bail!(
            "{} was withdrawn and can no longer promote — propose it again with \
             `rk suggest` if you still want the norm",
            args.suggestion
        );
    }

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
            println!(
                "{}",
                json!({ "endorsed": args.suggestion, "already": true })
            );
        } else {
            println!("already endorsed {}", args.suggestion);
        }
        return Ok(());
    }

    let payload = json!({
        "agent": agent,
        "suggestion": args.suggestion,
    });
    let mut params = json!({
        "category": "endorsement",
        "scope": rk_core::tuple::SYSTEM_SCOPE,
        "identity": args.suggestion,
        "instance": agent,
        "payload": payload,
    });
    if let Some(secs) = ttl_secs {
        params["ttl_secs"] = json!(secs);
    }
    let result = client.call("space.out", params).await?;
    if as_json {
        println!("{result}");
    } else {
        println!("endorsed {}", args.suggestion);
    }
    Ok(())
}

/// Close a losing ballot (TKT-184). The counterpart to quorum promotion on the
/// other outcome, and the act that makes the `open-suggestion` inbox rank
/// emptiable at all: ballots stopped expiring in TKT-168 — deliberately, because
/// a vote is a ledger entry nobody survives to reinforce — which also removed
/// the silent clock that used to clear a proposal nobody backed.
///
/// The decision is the daemon's, not this command's: `space.withdraw` reads the
/// proposer off the `Suggestion` and refuses anyone who is neither them nor the
/// operator. Doing it here instead would be advisory only — a rat can call the
/// RPC directly — so this is a thin wrapper on purpose.
///
/// Withdrawing does NOT delete anything. The suggestion and every endorsement on
/// it stay in the space and stay countable; `rk scan suggestion system` shows
/// the whole history. What ends is that the ballot can promote and that the
/// operator is nagged about it. There is no un-withdraw, and there does not need
/// to be: re-proposing is one `rk suggest` away, and it mints a fresh id whose
/// ballot starts from an honest zero rather than inheriting votes cast for an
/// idea that was withdrawn.
pub async fn withdraw(layout: &Layout, args: WithdrawArgs, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call("space.withdraw", json!({"suggestion": args.suggestion}))
        .await?;
    if as_json {
        println!("{result}");
    } else if result["already"].as_bool() == Some(true) {
        println!("{} was already withdrawn", args.suggestion);
    } else {
        println!(
            "withdrew {} — its endorsements stay on the record but can no longer promote",
            args.suggestion
        );
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

    /// `--field` exists so shell callers (workflow checks especially) can
    /// build valid payloads without jq on PATH — the steward escalation
    /// checks silently wrote empty payloads when jq was missing from the
    /// daemon's inherited environment. Escaping belongs to serde, not sh.
    #[test]
    fn fields_build_a_payload_without_jq() {
        let mut payload = json!(null);
        apply_fields(
            &mut payload,
            &[
                "agent=steward".into(),
                "task=TKT-1".into(),
                r#"text=quote " and \ survive"#.into(),
            ],
        )
        .unwrap();
        assert_eq!(payload["agent"], "steward");
        assert_eq!(payload["task"], "TKT-1");
        assert_eq!(payload["text"], "quote \" and \\ survive");
    }

    #[test]
    fn fields_layer_on_top_of_payload_and_reject_non_objects() {
        let mut payload = json!({"agent": "steward", "kept": true});
        apply_fields(
            &mut payload,
            &["agent=other".into(), "k=v=with=equals".into()],
        )
        .unwrap();
        assert_eq!(payload["agent"], "other");
        assert_eq!(payload["kept"], true);
        assert_eq!(payload["k"], "v=with=equals");

        let mut not_object = json!("scalar");
        assert!(apply_fields(&mut not_object, &["a=b".into()]).is_err());
        let mut ok = json!({});
        assert!(apply_fields(&mut ok, &["missing-separator".into()]).is_err());
        assert!(apply_fields(&mut ok, &["=empty-key".into()]).is_err());
    }

    #[test]
    fn durations_parse() {
        assert_eq!(parse_duration("30s").unwrap().as_secs(), 30);
        assert_eq!(parse_duration("5m").unwrap().as_secs(), 300);
        assert_eq!(parse_duration("2h").unwrap().as_secs(), 7200);
        assert_eq!(parse_duration("45").unwrap().as_secs(), 45);
        assert!(parse_duration("nope").is_err());
    }

    /// A ballot carries NO voting window unless one was asked for (TKT-168).
    /// This is the whole fix: `ttl_secs` is what forces `Lifecycle::Ephemeral`
    /// at the `out` boundary, and an Ephemeral tuple is GC'd on its clock and
    /// never replicated — so a defaulted window is exactly the thing that kept
    /// three rats from ever pooling into one quorum. Asserted through clap so a
    /// re-added `default_value` fails here rather than silently in the fleet.
    #[test]
    fn ballots_carry_no_voting_window_by_default() {
        use clap::Parser;

        #[derive(clap::Parser)]
        struct SuggestCli {
            #[command(flatten)]
            args: SuggestArgs,
        }
        #[derive(clap::Parser)]
        struct EndorseCli {
            #[command(flatten)]
            args: EndorseArgs,
        }

        let s = SuggestCli::parse_from(["rk", "always rebase"]);
        assert!(
            s.args.ttl.is_none(),
            "a proposal must not default to a window"
        );
        assert_eq!(ballot_ttl_secs(s.args.ttl.as_deref()).unwrap(), None);

        let e = EndorseCli::parse_from(["rk", "sug-8nsqa4132x"]);
        assert!(
            e.args.ttl.is_none(),
            "a cast vote must not default to a window"
        );
        assert_eq!(ballot_ttl_secs(e.args.ttl.as_deref()).unwrap(), None);

        // `--ttl` still time-boxes a ballot on purpose, and still validates.
        let boxed = SuggestCli::parse_from(["rk", "hold through the migration", "--ttl", "2h"]);
        assert_eq!(
            ballot_ttl_secs(boxed.args.ttl.as_deref()).unwrap(),
            Some(7200)
        );
        assert!(ballot_ttl_secs(Some("nope")).is_err());
    }

    #[test]
    fn fact_vote_args_accept_up_down_and_clear() {
        use clap::Parser;

        #[derive(clap::Parser)]
        struct FactCli {
            #[command(flatten)]
            args: FactVoteArgs,
        }

        for (word, expected) in [
            ("up", FactVote::Up),
            ("down", FactVote::Down),
            ("clear", FactVote::Clear),
        ] {
            let parsed = FactCli::parse_from(["rk", "01ARZ3NDEKTSV4RRFFQ69G5FAV", word]);
            assert_eq!(parsed.args.vote.as_str(), expected.as_str());
        }
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

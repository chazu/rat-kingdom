use anyhow::Result;
use clap::{Args, Subcommand, ValueEnum};
use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::{json, Map, Value};

#[derive(Subcommand)]
pub enum FactoryCommand {
    /// Read the native factory snapshot without starting the daemon.
    Snapshot(FactorySnapshotArgs),
    /// Read or watch the native factory event feed without starting the daemon.
    Events {
        #[command(subcommand)]
        command: FactoryEventsCommand,
    },
    /// Propose a typed workflow.run action for operator approval.
    ProposeWorkflow(FactoryProposeWorkflowArgs),
    /// Approve an exact factory action digest.
    Approve(FactoryApproveArgs),
    /// Execute an approved typed workflow.run action.
    ExecuteWorkflow(FactoryExecuteWorkflowArgs),
    /// Read-only self-optimization scorecards grouped by task class, workflow,
    /// harness, and model. Advisory only; never mutates routing, policy,
    /// workflows, tickets, approvals, or dispatch.
    Scorecards(FactoryAnalyticsArgs),
    /// Read-only advisory recommendations derived from scorecards. Advisory
    /// only; never mutates routing, policy, workflows, tickets, approvals, or
    /// dispatch.
    Recommend(FactoryAnalyticsArgs),
}

#[derive(Subcommand)]
pub enum FactoryEventsCommand {
    /// Replay a finite page of native factory events.
    Replay(FactoryEventsReplayArgs),
    /// Watch native factory events as NDJSON.
    Watch(FactoryEventsWatchArgs),
}

#[derive(Args)]
pub struct FactorySnapshotArgs {
    /// Registered repository name or path filter.
    #[arg(long)]
    pub repo: Option<String>,
    /// Stable coordinator-session id filter.
    #[arg(long)]
    pub coordinator: Option<String>,
    /// Include archived records in daemon snapshot views.
    #[arg(long)]
    pub include_archived: bool,
}

#[derive(Args)]
pub struct FactoryEventsReplayArgs {
    /// Replay events after this cursor.
    #[arg(long)]
    pub after: Option<u64>,
    /// Registered repository name or path filter.
    #[arg(long)]
    pub repo: Option<String>,
    /// Event kind filter. Repeat for multiple kinds.
    #[arg(long = "kind")]
    pub kinds: Vec<String>,
    /// Maximum events to replay. The daemon clamps to its native bound.
    #[arg(long)]
    pub limit: Option<usize>,
}

#[derive(Args)]
pub struct FactoryEventsWatchArgs {
    /// Watch events after this cursor.
    #[arg(long)]
    pub after: Option<u64>,
    /// Registered repository name or path filter.
    #[arg(long)]
    pub repo: Option<String>,
    /// Event kind filter. Repeat for multiple kinds.
    #[arg(long = "kind")]
    pub kinds: Vec<String>,
}

#[derive(Args)]
pub struct FactoryProposeWorkflowArgs {
    /// Workflow name.
    pub workflow: String,
    /// Registered repository name or path.
    #[arg(long)]
    pub repo: String,
    /// Workflow parameters as key=value (repeatable). Values are strings.
    #[arg(long = "param", value_parser = parse_param)]
    pub params: Vec<(String, String)>,
    /// Stable coordinator-session id that owns this workflow for monitoring.
    #[arg(long)]
    pub coordinator: Option<String>,
}

#[derive(Args)]
pub struct FactoryApproveArgs {
    /// Proposal id returned by propose-workflow.
    pub proposal_id: String,
    /// Exact 64-character lowercase hex digest returned by the daemon.
    #[arg(value_parser = parse_digest)]
    pub digest: String,
}

#[derive(Args)]
pub struct FactoryExecuteWorkflowArgs {
    /// Proposal id returned by propose-workflow.
    pub proposal_id: String,
    /// Exact 64-character lowercase hex digest returned by the daemon.
    #[arg(value_parser = parse_digest)]
    pub digest: String,
    /// Workflow name.
    #[arg(long)]
    pub workflow: String,
    /// Registered repository name or path.
    #[arg(long)]
    pub repo: String,
    /// Workflow parameters as key=value (repeatable). Values are strings.
    #[arg(long = "param", value_parser = parse_param)]
    pub params: Vec<(String, String)>,
    /// Stable coordinator-session id that owns this workflow for monitoring.
    #[arg(long)]
    pub coordinator: Option<String>,
}

#[derive(Clone, Copy, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum FactoryGroupBy {
    Composite,
    TaskClass,
    Workflow,
    Harness,
    Model,
    TaskClassWorkflow,
    All,
}

impl FactoryGroupBy {
    fn as_str(self) -> &'static str {
        match self {
            Self::Composite => "composite",
            Self::TaskClass => "task_class",
            Self::Workflow => "workflow",
            Self::Harness => "harness",
            Self::Model => "model",
            Self::TaskClassWorkflow => "task_class_workflow",
            Self::All => "all",
        }
    }
}

#[derive(Args)]
pub struct FactoryAnalyticsArgs {
    /// Registered repository name or path scope.
    #[arg(long)]
    pub repo: String,
    /// Projection: composite (default), task_class, workflow, harness, model,
    /// task_class_workflow, or all.
    #[arg(long = "group-by")]
    pub group_by: Option<FactoryGroupBy>,
    /// Include archived history in metrics and split active/archived counts.
    #[arg(long)]
    pub include_archived: bool,
    /// Only count events observed at or after this epoch-ms bound.
    #[arg(long)]
    pub since: Option<i64>,
    /// Only count events observed at or before this epoch-ms bound.
    #[arg(long)]
    pub until: Option<i64>,
    /// Minimum sample size hint forwarded to the daemon.
    #[arg(long)]
    pub min_sample: Option<u32>,
}

pub async fn run(layout: &Layout, command: FactoryCommand, json_output: bool) -> Result<()> {
    match command {
        FactoryCommand::Snapshot(args) => {
            let mut client = Client::connect(layout).await?;
            let params = snapshot_params(args);
            let result = client.call("factory.snapshot", params).await?;
            if json_output {
                println!("{result}");
            } else {
                let snapshot = &result["snapshot"];
                println!(
                    "factory snapshot: agents={} workflows={} tickets={} approvals={}",
                    snapshot["agents"].as_array().map(Vec::len).unwrap_or(0),
                    snapshot["workflows"].as_array().map(Vec::len).unwrap_or(0),
                    snapshot["tickets"].as_array().map(Vec::len).unwrap_or(0),
                    snapshot["approvals"].as_array().map(Vec::len).unwrap_or(0)
                );
            }
        }
        FactoryCommand::Events { command } => match command {
            FactoryEventsCommand::Replay(args) => {
                let mut client = Client::connect(layout).await?;
                let result = client
                    .call("factory.events.replay", replay_params(args))
                    .await?;
                if json_output {
                    println!("{result}");
                } else {
                    print_events(&result["events"]);
                }
            }
            FactoryEventsCommand::Watch(args) => {
                let client = Client::connect(layout).await?;
                let (initial, mut stream) = client
                    .call_then_stream("factory.events.watch", watch_params(args))
                    .await?;
                if json_output {
                    print_events_json(&initial["events"]);
                } else {
                    print_events(&initial["events"]);
                }
                while let Some(note) = stream.next().await? {
                    if note["method"].as_str() == Some("factory.event") {
                        if json_output {
                            println!("{}", note["params"]);
                        } else {
                            print_event(&note["params"]);
                        }
                    } else if note["method"].as_str() == Some("lagged") {
                        eprintln!(
                            "(factory events lagged: missed {})",
                            note["params"]["missed"]
                        );
                    } else if note["method"].as_str() == Some("factory.resync") {
                        if json_output {
                            println!("{}", note["params"]);
                        } else {
                            print_resync(&note["params"]);
                        }
                    }
                }
            }
        },
        FactoryCommand::ProposeWorkflow(args) => {
            let mut client = Client::connect_or_spawn(layout).await?;
            let action = workflow_action(args.workflow, args.repo, args.params, args.coordinator);
            let result = client
                .call(
                    "factory.propose_action",
                    json!({"kind": "workflow.run", "action": action}),
                )
                .await?;
            if json_output {
                let proposal = result.get("proposal").cloned().unwrap_or(Value::Null);
                println!(
                    "{}",
                    json!({
                        "schema": "factory.proposal.v1",
                        "proposal": proposal,
                        "digest": result["digest"],
                        "risk": proposal["risk"],
                        "action": proposal["action"],
                    })
                );
            } else {
                println!(
                    "proposed {} {}",
                    result["proposal"]["id"].as_str().unwrap_or("?"),
                    result["digest"].as_str().unwrap_or("?")
                );
            }
        }
        FactoryCommand::Approve(args) => {
            let mut client = Client::connect_or_spawn(layout).await?;
            let result = client
                .call(
                    "factory.approve_action",
                    json!({"proposal_id": args.proposal_id, "digest": args.digest}),
                )
                .await?;
            if json_output {
                println!("{result}");
            } else {
                println!(
                    "approved {}",
                    result["approval"]["proposal_id"].as_str().unwrap_or("?")
                );
            }
        }
        FactoryCommand::ExecuteWorkflow(args) => {
            let mut client = Client::connect_or_spawn(layout).await?;
            let action = workflow_action(args.workflow, args.repo, args.params, args.coordinator);
            let result = client
                .call(
                    "factory.execute_action",
                    json!({"proposal_id": args.proposal_id, "digest": args.digest, "action": action}),
                )
                .await?;
            if json_output {
                println!("{result}");
            } else {
                println!(
                    "started {}",
                    result["instance"]["id"].as_str().unwrap_or("?")
                );
            }
        }
        FactoryCommand::Scorecards(args) => {
            let mut client = Client::connect(layout).await?;
            let result = client
                .call("factory.scorecards", analytics_params(args))
                .await?;
            if json_output {
                println!("{result}");
            } else {
                print!("{}", render_scorecards_markdown(&result));
            }
        }
        FactoryCommand::Recommend(args) => {
            let mut client = Client::connect(layout).await?;
            let result = client
                .call("factory.recommend", analytics_params(args))
                .await?;
            if json_output {
                println!("{result}");
            } else {
                print!("{}", render_recommend_markdown(&result));
            }
        }
    }
    Ok(())
}

fn snapshot_params(args: FactorySnapshotArgs) -> Value {
    let mut map = Map::new();
    insert_some(&mut map, "repo", args.repo);
    insert_some(&mut map, "coordinator", args.coordinator);
    if args.include_archived {
        map.insert("include_archived".into(), Value::Bool(true));
    }
    Value::Object(map)
}

fn replay_params(args: FactoryEventsReplayArgs) -> Value {
    let mut map = event_filter_params(args.after, args.repo, args.kinds);
    if let Some(limit) = args.limit {
        map.insert("limit".into(), json!(limit));
    }
    Value::Object(map)
}

fn watch_params(args: FactoryEventsWatchArgs) -> Value {
    Value::Object(event_filter_params(args.after, args.repo, args.kinds))
}

fn event_filter_params(
    after: Option<u64>,
    repo: Option<String>,
    kinds: Vec<String>,
) -> Map<String, Value> {
    let mut map = Map::new();
    if let Some(after) = after {
        map.insert("after".into(), json!(after));
    }
    insert_some(&mut map, "repo", repo);
    if !kinds.is_empty() {
        map.insert("kinds".into(), json!(kinds));
    }
    map
}

fn insert_some(map: &mut Map<String, Value>, key: &str, value: Option<String>) {
    if let Some(value) = value {
        map.insert(key.into(), Value::String(value));
    }
}

fn print_events_json(events: &Value) {
    if let Some(events) = events.as_array() {
        for event in events {
            println!("{event}");
        }
    }
}

fn print_events(events: &Value) {
    match events.as_array() {
        Some(events) if !events.is_empty() => events.iter().for_each(print_event),
        _ => println!("(no factory events)"),
    }
}

fn print_event(event: &Value) {
    println!(
        "{} {} {} {}",
        event["cursor"].as_u64().unwrap_or(0),
        event["kind"].as_str().unwrap_or("?"),
        event["repo"].as_str().unwrap_or("?"),
        event["summary"].as_str().unwrap_or("")
    );
}

fn print_resync(params: &Value) {
    println!(
        "factory events resync required: boundary={}",
        params["boundary"].as_u64().unwrap_or(0)
    );
}

fn workflow_action(
    name: String,
    repo: String,
    params: Vec<(String, String)>,
    coordinator: Option<String>,
) -> Value {
    json!({
        "name": name,
        "repo": repo,
        "params": params_map(params),
        "coordinator": coordinator,
    })
}

fn params_map(params: Vec<(String, String)>) -> Value {
    let mut map = Map::new();
    for (key, value) in params {
        map.insert(key, Value::String(value));
    }
    Value::Object(map)
}

fn parse_param(pair: &str) -> Result<(String, String), String> {
    let (key, value) = pair
        .split_once('=')
        .ok_or_else(|| format!("--param must be key=value, got: {pair}"))?;
    if key.is_empty() {
        return Err("--param key cannot be empty".into());
    }
    Ok((key.to_string(), value.to_string()))
}

fn analytics_params(args: FactoryAnalyticsArgs) -> Value {
    let mut map = Map::new();
    map.insert("repo".into(), Value::String(args.repo));
    if let Some(group_by) = args.group_by {
        map.insert(
            "group_by".into(),
            Value::String(group_by.as_str().to_string()),
        );
    }
    if args.include_archived {
        map.insert("include_archived".into(), Value::Bool(true));
    }
    if let Some(since) = args.since {
        map.insert("since".into(), json!(since));
    }
    if let Some(until) = args.until {
        map.insert("until".into(), json!(until));
    }
    if let Some(min_sample) = args.min_sample {
        map.insert("min_sample".into(), json!(min_sample));
    }
    Value::Object(map)
}

fn render_source_counts(out: &mut String, result: &Value) {
    out.push_str("## Source Counts\n\n");
    if let Some(counts) = result["source_counts"].as_array() {
        for entry in counts {
            out.push_str(&format!(
                "- {}: active={} archived={} events={} available={}\n",
                entry["source_family"].as_str().unwrap_or("?"),
                entry["active_source_count"].as_u64().unwrap_or(0),
                entry["archived_source_count"].as_u64().unwrap_or(0),
                entry["event_count"].as_u64().unwrap_or(0),
                availability_of(result, entry["source_family"].as_str().unwrap_or("")),
            ));
        }
    }
    out.push('\n');
}

fn availability_of(result: &Value, family: &str) -> bool {
    result["availability"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|a| a["source_family"].as_str() == Some(family))
        .and_then(|a| a["available"].as_bool())
        .unwrap_or(false)
}

fn render_scorecard_rows(out: &mut String, result: &Value) {
    out.push_str("## Scorecards\n\n");
    match result["scorecards"].as_array() {
        Some(rows) if !rows.is_empty() => {
            for row in rows {
                let key = &row["group_key"];
                out.push_str(&format!(
                    "- ({} / {} / {} / {}) runs={} accepted={} reworked={} ci_failed={} reverted={} cost_micro_usd={}\n",
                    key["task_class"].as_str().unwrap_or("unknown"),
                    key["workflow"].as_str().unwrap_or("unknown"),
                    key["harness"].as_str().unwrap_or("unknown"),
                    key["model"].as_str().unwrap_or("unknown"),
                    row["metrics"]["runs"].as_u64().unwrap_or(0),
                    row["metrics"]["accepted"].as_u64().unwrap_or(0),
                    row["metrics"]["reworked"].as_u64().unwrap_or(0),
                    row["metrics"]["ci_failed"].as_u64().unwrap_or(0),
                    row["metrics"]["reverted"].as_u64().unwrap_or(0),
                    row["metrics"]["total_cost_micro_usd"].as_u64().unwrap_or(0),
                ));
            }
        }
        _ => out.push_str("(no observed scorecard rows)\n"),
    }
    out.push('\n');
}

fn render_warnings(out: &mut String, result: &Value) {
    if let Some(warnings) = result["warnings"].as_array() {
        if !warnings.is_empty() {
            out.push_str("## Warnings\n\n");
            for warning in warnings {
                out.push_str(&format!("- {}\n", warning.as_str().unwrap_or("")));
            }
            out.push('\n');
        }
    }
}

fn render_scorecards_markdown(result: &Value) -> String {
    let mut out = String::from("# Factory Scorecards\n\n");
    render_source_counts(&mut out, result);
    render_scorecard_rows(&mut out, result);
    render_warnings(&mut out, result);
    out
}

fn render_recommend_markdown(result: &Value) -> String {
    let mut out = String::from("# Factory Scorecards\n\n");
    render_source_counts(&mut out, result);
    render_scorecard_rows(&mut out, result);

    out.push_str("## Recommendations\n\n");
    match result["recommendations"].as_array() {
        Some(recs) if !recs.is_empty() => {
            for rec in recs {
                out.push_str(&format!(
                    "- [{}] {}: {} (advisory)\n",
                    rec["severity"].as_str().unwrap_or("info"),
                    rec["rule"].as_str().unwrap_or("?"),
                    rec["advice"].as_str().unwrap_or(""),
                ));
            }
        }
        _ => out.push_str("(no advisory recommendations)\n"),
    }
    out.push('\n');

    out.push_str("## Suppressed\n\n");
    match result["suppressions"].as_array() {
        Some(sup) if !sup.is_empty() => {
            for entry in sup {
                out.push_str(&format!(
                    "- {}: {} (low-sample or unavailable metric)\n",
                    entry["rule"].as_str().unwrap_or("?"),
                    entry["reason"].as_str().unwrap_or("?"),
                ));
            }
        }
        _ => out.push_str("(none)\n"),
    }
    out.push('\n');

    render_warnings(&mut out, result);
    out
}

fn parse_digest(digest: &str) -> Result<String, String> {
    if digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(digest.to_string())
    } else {
        Err("digest must be exactly 64 hexadecimal characters".into())
    }
}

use anyhow::{Context, Result, anyhow};
use clap::{Args, Subcommand, ValueEnum};
use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::{json, Map, Value};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

#[derive(Subcommand)]
pub enum FactoryCommand {
    /// Open a Rust-native read-only factory dashboard, auto-starting the daemon.
    Dashboard(FactoryDashboardArgs),
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
    /// Execute an exact approved factory action from a saved proposal envelope.
    ExecuteAction(FactoryExecuteActionArgs),
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

#[derive(Args)]
pub struct FactoryDashboardArgs {
    /// Registered repository name or path filter.
    #[arg(long)]
    pub repo: Option<String>,
    /// Stable coordinator-session id filter.
    #[arg(long)]
    pub coordinator: Option<String>,
    /// Include archived records in daemon snapshot views.
    #[arg(long)]
    pub include_archived: bool,
    /// Maximum rows shown in each dashboard table.
    #[arg(long, default_value_t = 20)]
    pub row_limit: usize,
    /// Maximum recent factory events shown.
    #[arg(long, default_value_t = 20)]
    pub event_limit: usize,
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
    #[arg(
        required_unless_present = "proposal_file",
        requires = "digest",
        conflicts_with = "proposal_file"
    )]
    pub proposal_id: Option<String>,
    /// Exact 64-character lowercase hex digest returned by the daemon.
    #[arg(
        required_unless_present = "proposal_file",
        requires = "proposal_id",
        conflicts_with = "proposal_file",
        value_parser = parse_digest
    )]
    pub digest: Option<String>,
    /// Saved JSON output from a typed factory proposal command.
    #[arg(long, value_name = "PATH")]
    pub proposal_file: Option<PathBuf>,
}

#[derive(Args)]
pub struct FactoryExecuteActionArgs {
    /// Saved JSON output from a typed factory proposal command.
    #[arg(long, value_name = "PATH")]
    pub proposal_file: PathBuf,
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
        FactoryCommand::Dashboard(mut args) => {
            let mut client = Client::connect_or_spawn(layout).await?;
            if let Some(repo) = args.repo.as_deref() {
                args.repo = Some(resolve_dashboard_repo(&mut client, repo).await?);
            }
            let mut snapshot = client
                .call("factory.snapshot", dashboard_snapshot_params(&args))
                .await?;
            let cursor = snapshot["cursor"].as_u64().unwrap_or(0);
            let events = client
                .call(
                    "factory.events.replay",
                    dashboard_replay_params(&args, cursor),
                )
                .await?;
            match client
                .call("inbox.list", dashboard_inbox_params(&args))
                .await
            {
                Ok(inbox) => merge_dashboard_inbox(&mut snapshot, &inbox),
                Err(error) => {
                    snapshot["snapshot"]["inbox_error"] = Value::String(error.to_string());
                }
            }
            if json_output {
                println!(
                    "{}",
                    json!({
                        "schema": "factory.dashboard.v1",
                        "repo": args.repo,
                        "snapshot": snapshot,
                        "events": events,
                    })
                );
            } else {
                print!(
                    "{}",
                    render_dashboard(&snapshot, &events, args.repo.as_deref(), args.row_limit, args.event_limit)
                );
            }
        }
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
            let execution_action = action.clone();
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
                        "proposal_id": proposal["id"],
                        "kind": "workflow.run",
                        "proposal": proposal,
                        "digest": result["digest"],
                        "risk": proposal["risk"],
                        "action": proposal["action"],
                        "execution_action": execution_action,
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
            let (proposal_id, digest) = approval_input(args)?;
            let mut client = Client::connect_or_spawn(layout).await?;
            let result = client
                .call(
                    "factory.approve_action",
                    json!({"proposal_id": proposal_id, "digest": digest}),
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
        FactoryCommand::ExecuteAction(args) => {
            let envelope = proposal_execution_envelope(&args.proposal_file)?;
            let mut client = Client::connect_or_spawn(layout).await?;
            let result = client
                .call(
                    "factory.execute_action",
                    json!({
                        "proposal_id": envelope.proposal_id,
                        "digest": envelope.digest,
                        "kind": envelope.kind,
                        "action": envelope.execution_action,
                    }),
                )
                .await?;
            if json_output {
                println!("{result}");
            } else {
                println!("executed {}", envelope.kind);
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

async fn resolve_dashboard_repo(client: &mut Client, requested: &str) -> Result<String> {
    let registry = client.call("repo.list", json!({})).await?;
    let requested_path = std::fs::canonicalize(requested).ok();
    let resolved = registry["repos"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|repo| {
            repo["name"].as_str() == Some(requested)
                || requested_path.as_ref().is_some_and(|requested_path| {
                    repo["path"]
                        .as_str()
                        .map(Path::new)
                        .is_some_and(|path| path == requested_path)
                })
        })
        .and_then(|repo| repo["name"].as_str())
        .unwrap_or(requested);
    Ok(resolved.to_string())
}

fn merge_dashboard_inbox(envelope: &mut Value, inbox: &Value) {
    let items = inbox["items"]
        .as_array()
        .into_iter()
        .flatten()
        .cloned()
        .collect();
    envelope["snapshot"]["inbox"] = Value::Array(items);
    envelope["snapshot"]["inbox_truncated"] =
        Value::Bool(inbox["truncated"].as_bool().unwrap_or(false));
}

fn dashboard_inbox_params(args: &FactoryDashboardArgs) -> Value {
    let mut map = Map::new();
    insert_some(&mut map, "repo", args.repo.clone());
    Value::Object(map)
}

fn dashboard_snapshot_params(args: &FactoryDashboardArgs) -> Value {
    let mut map = Map::new();
    insert_some(&mut map, "repo", args.repo.clone());
    insert_some(&mut map, "coordinator", args.coordinator.clone());
    if args.include_archived {
        map.insert("include_archived".into(), Value::Bool(true));
    }
    Value::Object(map)
}

fn dashboard_replay_params(args: &FactoryDashboardArgs, cursor: u64) -> Value {
    let mut map = Map::new();
    map.insert("after".into(), json!(cursor.saturating_sub(256)));
    map.insert("limit".into(), json!(256));
    insert_some(&mut map, "repo", args.repo.clone());
    insert_some(&mut map, "coordinator", args.coordinator.clone());
    Value::Object(map)
}

fn render_dashboard(
    envelope: &Value,
    replay: &Value,
    repo: Option<&str>,
    row_limit: usize,
    event_limit: usize,
) -> String {
    let snapshot = &envelope["snapshot"];
    let mut out = String::new();
    let repository = repo.unwrap_or("all registered repositories");
    let cursor = envelope["cursor"].as_u64().unwrap_or(0);
    let resync = &snapshot["repo_resync"];
    let resyncing = resync["required"].as_bool().unwrap_or(false);

    writeln!(out, "# Factory Dashboard\n").unwrap();
    writeln!(out, "- Repository: `{}`", markdown_text(repository)).unwrap();
    writeln!(out, "- Connection: **CONNECTED**").unwrap();
    writeln!(out, "- Cursor: `{cursor}`").unwrap();
    writeln!(
        out,
        "- State: **{}**\n",
        if resyncing { "RESYNC REQUIRED" } else { "OK" }
    )
    .unwrap();

    render_approvals(&mut out, &snapshot["approvals"], row_limit);
    render_rows(
        &mut out,
        "Workflow Runs",
        snapshot["workflows"].as_array(),
        &["id", "workflow", "status", "repo", "started_at"],
        row_limit,
        true,
    );
    render_rows(
        &mut out,
        "Agents",
        snapshot["agents"].as_array(),
        &["name", "state", "task", "repo_name", "updated_at"],
        row_limit,
        true,
    );
    render_rows(
        &mut out,
        "Tickets",
        snapshot["tickets"].as_array(),
        &[
            "identity",
            "payload.status",
            "payload.title",
            "scope",
            "payload.updated_at",
        ],
        row_limit,
        true,
    );
    if let Some(error) = snapshot["inbox_error"].as_str() {
        writeln!(out, "## Inbox\n").unwrap();
        writeln!(out, "- Unavailable: {}\n", markdown_text(error)).unwrap();
    } else {
        render_rows(
            &mut out,
            "Inbox",
            snapshot["inbox"].as_array(),
            &["kind", "subject", "scope", "detail", "action"],
            row_limit,
            false,
        );
    }
    render_mapping(&mut out, "Budget", &snapshot["budget"]);
    render_events(&mut out, replay, event_limit);
    out
}

fn render_approvals(out: &mut String, approvals: &Value, row_limit: usize) {
    writeln!(out, "## Approvals\n").unwrap();
    let proposals = approvals["proposals"].as_array().map(Vec::as_slice).unwrap_or(&[]);
    let grants = approvals["grants"].as_array().map(Vec::as_slice).unwrap_or(&[]);
    writeln!(out, "- Proposals: {}", proposals.len()).unwrap();
    writeln!(out, "- Grants: {}\n", grants.len()).unwrap();
    if proposals.is_empty() {
        writeln!(out, "_none_\n").unwrap();
        return;
    }
    writeln!(out, "| id | kind | status | risk | digest | expires at |").unwrap();
    writeln!(out, "| --- | --- | --- | --- | --- | --- |").unwrap();
    for proposal in proposals.iter().take(row_limit) {
        writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} |",
            cell(&proposal["id"]),
            cell(&proposal["kind"]),
            cell(&proposal["status"]),
            cell(&proposal["risk"]),
            cell(&proposal["digest"]),
            cell(&proposal["expires_at"]),
        )
        .unwrap();
    }
    writeln!(out).unwrap();
}

fn render_rows(
    out: &mut String,
    heading: &str,
    rows: Option<&Vec<Value>>,
    columns: &[&str],
    row_limit: usize,
    newest_first: bool,
) {
    let rows = rows.map(Vec::as_slice).unwrap_or(&[]);
    writeln!(out, "## {heading}\n").unwrap();
    writeln!(out, "- Total: {}\n", rows.len()).unwrap();
    if rows.is_empty() {
        writeln!(out, "_none_\n").unwrap();
        return;
    }
    let headings = columns
        .iter()
        .map(|column| column.rsplit('.').next().unwrap_or(column))
        .collect::<Vec<_>>();
    writeln!(out, "| {} |", headings.join(" | ")).unwrap();
    writeln!(out, "| {} |", vec!["---"; columns.len()].join(" | ")).unwrap();
    let mut selected = rows.iter().collect::<Vec<_>>();
    if newest_first {
        selected.reverse();
    }
    for row in selected.into_iter().take(row_limit) {
        let values = columns
            .iter()
            .map(|column| cell(value_at(row, column)))
            .collect::<Vec<_>>();
        writeln!(out, "| {} |", values.join(" | ")).unwrap();
    }
    writeln!(out).unwrap();
}

fn value_at<'a>(value: &'a Value, path: &str) -> &'a Value {
    path.split('.').fold(value, |current, key| &current[key])
}

fn render_mapping(out: &mut String, heading: &str, value: &Value) {
    writeln!(out, "## {heading}\n").unwrap();
    let Some(mapping) = value.as_object() else {
        writeln!(out, "_none_\n").unwrap();
        return;
    };
    for (key, value) in mapping {
        writeln!(out, "- {}: {}", key.replace('_', " "), markdown_text(&plain(value))).unwrap();
    }
    writeln!(out).unwrap();
}

fn render_events(out: &mut String, replay: &Value, event_limit: usize) {
    writeln!(out, "## Recent Events\n").unwrap();
    if replay["truncated"].as_bool().unwrap_or(false) {
        writeln!(
            out,
            "- Replay truncated at boundary `{}`\n",
            replay["boundary"].as_u64().unwrap_or(0)
        )
        .unwrap();
    }
    let events = replay["events"].as_array().map(Vec::as_slice).unwrap_or(&[]);
    if events.is_empty() {
        writeln!(out, "_none_\n").unwrap();
        return;
    }
    writeln!(out, "| cursor | kind | repository | summary |").unwrap();
    writeln!(out, "| --- | --- | --- | --- |").unwrap();
    for event in events.iter().rev().take(event_limit) {
        writeln!(
            out,
            "| {} | {} | {} | {} |",
            cell(&event["cursor"]),
            cell(&event["kind"]),
            cell(&event["repo"]),
            cell(&event["summary"]),
        )
        .unwrap();
    }
    writeln!(out).unwrap();
}

fn cell(value: &Value) -> String {
    markdown_text(&plain(value)).replace('|', "\\|")
}

fn markdown_text(value: &str) -> String {
    value.replace(['\n', '\r'], " ")
}

fn plain(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

struct ProposalExecutionEnvelope {
    proposal_id: String,
    digest: String,
    kind: String,
    execution_action: Value,
}

fn approval_input(args: FactoryApproveArgs) -> Result<(String, String)> {
    if let Some(path) = args.proposal_file {
        let envelope = proposal_execution_envelope(&path)?;
        return Ok((envelope.proposal_id, envelope.digest));
    }
    Ok((
        args.proposal_id
            .ok_or_else(|| anyhow!("proposal id is required"))?,
        args.digest.ok_or_else(|| anyhow!("digest is required"))?,
    ))
}

fn proposal_execution_envelope(path: &Path) -> Result<ProposalExecutionEnvelope> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read proposal file {}", path.display()))?;
    let value: Value = serde_json::from_str(&text)
        .with_context(|| format!("parse proposal file {}", path.display()))?;
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("proposal file must contain a JSON object"))?;
    let proposal_id = required_string(object, "proposal_id")?;
    let digest = parse_digest(&required_string(object, "digest")?).map_err(anyhow::Error::msg)?;
    let kind = required_string(object, "kind")?;
    if !matches!(
        kind.as_str(),
        "workflow.run" | "ticket_graph.apply" | "product_to_code.dispatch"
    ) {
        return Err(anyhow!("unsupported proposal action kind: {kind}"));
    }
    let execution_action = object
        .get("execution_action")
        .filter(|value| value.is_object())
        .cloned()
        .ok_or_else(|| anyhow!("proposal file is missing object field execution_action"))?;

    let proposal = object
        .get("proposal")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("proposal must be a JSON object"))?;
    require_matching_string(proposal, "id", &proposal_id, "proposal_id")?;
    require_matching_string(proposal, "digest", &digest, "digest")?;
    require_matching_string(proposal, "kind", &kind, "kind")?;

    Ok(ProposalExecutionEnvelope {
        proposal_id,
        digest,
        kind,
        execution_action,
    })
}

fn required_string(object: &Map<String, Value>, field: &str) -> Result<String> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| anyhow!("proposal file is missing string field {field}"))
}

fn require_matching_string(
    object: &Map<String, Value>,
    nested_field: &str,
    expected: &str,
    top_level_field: &str,
) -> Result<()> {
    let actual = object
        .get(nested_field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("proposal file is missing string field proposal.{nested_field}"))?;
    if actual != expected {
        return Err(anyhow!(
            "proposal file has conflicting {top_level_field}: top-level={expected} proposal.{nested_field}={actual}"
        ));
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
            let mut rendered = 0;
            for rec in recs {
                let advice = rec["advice"].as_str().unwrap_or("");
                if rec["suppressed"].as_bool().unwrap_or(false) || advice.trim().is_empty() {
                    continue;
                }
                out.push_str(&format!(
                    "- [{}] {}: {} (advisory)\n",
                    rec["severity"].as_str().unwrap_or("info"),
                    rec["rule"].as_str().unwrap_or("?"),
                    advice,
                ));
                rendered += 1;
            }
            if rendered == 0 {
                out.push_str("(no advisory recommendations)\n");
            }
        }
        _ => out.push_str("(no advisory recommendations)\n"),
    }
    out.push('\n');

    out.push_str("## Suppressed\n\n");
    match result["suppressions"].as_array() {
        Some(sup) if !sup.is_empty() => {
            for entry in sup {
                let source_counts = &entry["source_counts"];
                let subject = &entry["subject_group_key"];
                out.push_str(&format!(
                    "- {}: {} (source_family={} task_class={} workflow={} harness={} model={} active_source_count={} archived_source_count={} event_count={})\n",
                    entry["rule"].as_str().unwrap_or("?"),
                    entry["reason"].as_str().unwrap_or("?"),
                    entry["source_family"].as_str().unwrap_or("?"),
                    subject["task_class"].as_str().unwrap_or("?"),
                    subject["workflow"].as_str().unwrap_or("?"),
                    subject["harness"].as_str().unwrap_or("?"),
                    subject["model"].as_str().unwrap_or("?"),
                    source_counts["active_source_count"].as_u64().unwrap_or(0),
                    source_counts["archived_source_count"].as_u64().unwrap_or(0),
                    source_counts["event_count"].as_u64().unwrap_or(0),
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

#[cfg(test)]
mod tests {
    use super::{render_dashboard, render_recommend_markdown};
    use serde_json::json;

    #[test]
    fn dashboard_renders_native_ticket_payload_fields() {
        let snapshot = json!({
            "cursor": 12,
            "snapshot": {
                "agents": [],
                "workflows": [],
                "tickets": [{
                    "identity": "TKT-12",
                    "scope": "rat-kingdom",
                    "payload": {
                        "status": "open",
                        "title": "Fix the dashboard",
                        "updated_at": "2026-08-14T01:00:00Z"
                    }
                }],
                "inbox": [],
                "budget": {},
                "approvals": {"proposals": [], "grants": []},
                "repo_resync": {"required": false}
            }
        });
        let replay = json!({"events": [], "truncated": false});

        let markdown = render_dashboard(&snapshot, &replay, Some("rat-kingdom"), 20, 20);

        assert!(markdown.contains("TKT-12"), "{markdown}");
        assert!(markdown.contains("open"), "{markdown}");
        assert!(markdown.contains("Fix the dashboard"), "{markdown}");
        assert!(markdown.contains("2026-08-14T01:00:00Z"), "{markdown}");
    }

    #[test]
    fn dashboard_renders_native_workflow_started_at() {
        let snapshot = json!({
            "cursor": 12,
            "snapshot": {
                "agents": [],
                "workflows": [{
                    "id": "wf-12",
                    "workflow": "repair",
                    "status": "running",
                    "repo": "rat-kingdom",
                    "started_at": "2026-08-14T01:00:00Z"
                }],
                "tickets": [],
                "inbox": [],
                "budget": {},
                "approvals": {"proposals": [], "grants": []},
                "repo_resync": {"required": false}
            }
        });
        let replay = json!({"events": [], "truncated": false});

        let markdown = render_dashboard(&snapshot, &replay, Some("rat-kingdom"), 20, 20);

        assert!(markdown.contains("2026-08-14T01:00:00Z"), "{markdown}");
    }

    #[test]
    fn dashboard_preserves_native_inbox_priority_order() {
        let snapshot = json!({
            "cursor": 12,
            "snapshot": {
                "agents": [],
                "workflows": [],
                "tickets": [],
                "inbox": [
                    {"kind": "urgent", "subject": "first", "scope": "rat-kingdom", "detail": "high", "action": "rk first"},
                    {"kind": "passive", "subject": "second", "scope": "rat-kingdom", "detail": "low", "action": "rk second"}
                ],
                "budget": {},
                "approvals": {"proposals": [], "grants": []},
                "repo_resync": {"required": false}
            }
        });
        let replay = json!({"events": [], "truncated": false});

        let markdown = render_dashboard(&snapshot, &replay, Some("rat-kingdom"), 20, 20);

        assert!(markdown.find("first").unwrap() < markdown.find("second").unwrap(), "{markdown}");
    }

    #[test]
    fn recommend_markdown_filters_suppressed_and_empty_advice_from_active_section() {
        let result = json!({
            "schema_version": 1,
            "repo": "rat-kingdom",
            "generated_at": "2026-08-13T00:00:00Z",
            "group_by": "agent",
            "include_archived": false,
            "nature": "advisory",
            "source_counts": [],
            "availability": [],
            "scorecards": [],
            "recommendations": [
                {
                    "id": "active",
                    "severity": "warning",
                    "rule": "high_rework",
                    "summary": "rework is elevated",
                    "advice": "Review rework evidence for repeated reviewer findings.",
                    "suppressed": false,
                    "sample_size": 12,
                    "source_count": 12,
                    "source_counts": {"active_source_count": 12, "archived_source_count": 0, "event_count": 12},
                    "metric_availability": {"source_family": "StructuredReviewerRework", "available": true}
                },
                {
                    "id": "suppressed",
                    "severity": "warning",
                    "rule": "ci_instability",
                    "summary": "ci instability",
                    "advice": null,
                    "suppressed": true,
                    "suppression_reason": "metric_unavailable",
                    "sample_size": 12,
                    "source_count": 0,
                    "source_counts": {"active_source_count": 0, "archived_source_count": 0, "event_count": 0},
                    "metric_availability": {"source_family": "Phase4CiSignal", "available": false}
                },
                {
                    "id": "empty-advice",
                    "severity": "info",
                    "rule": "recurrence",
                    "summary": "recurrence",
                    "advice": "",
                    "suppressed": false,
                    "sample_size": 9,
                    "source_count": 9,
                    "source_counts": {"active_source_count": 9, "archived_source_count": 0, "event_count": 9},
                    "metric_availability": {"source_family": "RecurrenceKey", "available": true}
                }
            ],
            "suppressions": [
                {
                    "rule": "ci_instability",
                    "reason": "metric_unavailable",
                    "subject_group_key": {"task_class": "unknown", "workflow": "unknown", "harness": "unknown", "model": "unknown"},
                    "source_family": "Phase4CiSignal",
                    "source_counts": {"active_source_count": 0, "archived_source_count": 0, "event_count": 0}
                }
            ],
            "warnings": []
        });

        let markdown = render_recommend_markdown(&result);
        let active = markdown.split("## Recommendations").nth(1).unwrap().split("## Suppressed").next().unwrap();
        let suppressed = markdown.split("## Suppressed").nth(1).unwrap();

        assert!(active.contains("high_rework"), "active section: {active}");
        assert!(!active.contains("ci_instability"), "active section must omit suppressed records: {active}");
        assert!(!active.contains("recurrence"), "active section must omit empty advice: {active}");
        assert!(suppressed.contains("ci_instability"), "suppressed section: {suppressed}");
        assert!(suppressed.contains("metric_unavailable"), "suppressed section: {suppressed}");
        assert!(suppressed.contains("source_family=Phase4CiSignal"), "suppressed section: {suppressed}");
        assert!(suppressed.contains("task_class=unknown"), "suppressed section: {suppressed}");
        assert!(suppressed.contains("workflow=unknown"), "suppressed section: {suppressed}");
        assert!(suppressed.contains("harness=unknown"), "suppressed section: {suppressed}");
        assert!(suppressed.contains("model=unknown"), "suppressed section: {suppressed}");
        assert!(suppressed.contains("active_source_count=0"), "suppressed section: {suppressed}");
        assert!(suppressed.contains("archived_source_count=0"), "suppressed section: {suppressed}");
        assert!(suppressed.contains("event_count=0"), "suppressed section: {suppressed}");
        assert!(!suppressed.contains("available=false"), "suppressed section must not fabricate availability: {suppressed}");
        assert!(!suppressed.contains("sample="), "suppressed section must not relabel event_count as sample: {suppressed}");
    }
}

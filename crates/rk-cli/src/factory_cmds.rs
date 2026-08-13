use anyhow::Result;
use clap::{Args, Subcommand};
use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::{json, Map, Value};

#[derive(Subcommand)]
pub enum FactoryCommand {
    /// Propose a typed workflow.run action for operator approval.
    ProposeWorkflow(FactoryProposeWorkflowArgs),
    /// Approve an exact factory action digest.
    Approve(FactoryApproveArgs),
    /// Execute an approved typed workflow.run action.
    ExecuteWorkflow(FactoryExecuteWorkflowArgs),
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

pub async fn run(layout: &Layout, command: FactoryCommand, json_output: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    match command {
        FactoryCommand::ProposeWorkflow(args) => {
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
    }
    Ok(())
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

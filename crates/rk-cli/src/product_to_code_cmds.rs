use anyhow::{Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use rk_core::paths::Layout;
use rk_core::product_to_code::contracts::{
    ArchitectureResearchArtifact, InitiativeContract, TicketGraph,
};
use rk_daemon::Client;
use serde_json::json;
use std::path::PathBuf;

#[derive(Subcommand)]
pub enum ProductToCodeCommand {
    /// Validate and render architecture research artifacts.
    Research {
        #[command(subcommand)]
        command: ResearchCommand,
    },
    /// Validate and stage ticket graph proposals.
    Graph {
        #[command(subcommand)]
        command: GraphCommand,
    },
}

#[derive(Subcommand)]
pub enum GraphCommand {
    /// Validate a local ticket graph against an initiative.
    Validate(GraphValidateArgs),
    /// Show exact future ticket mutations without contacting the daemon.
    DryRun(GraphApplyArgs),
    /// Submit a typed ticket_graph.apply proposal to the daemon authority boundary.
    ProposeApply(GraphApplyArgs),
}

#[derive(Subcommand)]
pub enum ResearchCommand {
    /// Validate a local architecture research artifact against an initiative.
    Validate(ResearchValidateArgs),
    /// Render a local architecture research artifact deterministically.
    Render(ResearchRenderArgs),
}

#[derive(Args)]
pub struct ResearchValidateArgs {
    /// ArchitectureResearchArtifact JSON file.
    #[arg(long)]
    artifact: PathBuf,
    /// InitiativeContract JSON file referenced by the artifact.
    #[arg(long)]
    initiative: PathBuf,
}

#[derive(Args)]
pub struct ResearchRenderArgs {
    /// ArchitectureResearchArtifact JSON file.
    #[arg(long)]
    artifact: PathBuf,
    /// Output format.
    #[arg(long, value_enum, default_value_t = RenderFormat::Markdown)]
    format: RenderFormat,
}

#[derive(Args, Clone)]
pub struct GraphValidateArgs {
    /// TicketGraph JSON file.
    #[arg(long)]
    graph: PathBuf,
    /// InitiativeContract JSON file referenced by the graph.
    #[arg(long)]
    initiative: PathBuf,
}

#[derive(Args, Clone)]
pub struct GraphApplyArgs {
    /// TicketGraph JSON file.
    #[arg(long)]
    graph: PathBuf,
    /// InitiativeContract JSON file referenced by the graph.
    #[arg(long)]
    initiative: PathBuf,
    /// Registered repository name or path for future tickets.
    #[arg(long, default_value = ".")]
    repo: String,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum RenderFormat {
    Json,
    Markdown,
}

pub async fn run(layout: &Layout, command: ProductToCodeCommand, json_output: bool) -> Result<i32> {
    match command {
        ProductToCodeCommand::Research { command } => run_research(command, json_output),
        ProductToCodeCommand::Graph { command } => run_graph(layout, command, json_output).await,
    }
}

async fn run_graph(layout: &Layout, command: GraphCommand, json_output: bool) -> Result<i32> {
    match command {
        GraphCommand::Validate(args) => {
            let (graph, initiative) = read_graph_and_initiative(&args.graph, &args.initiative)?;
            let criterion_ids = criterion_ids(&initiative);
            let report = graph.validation_report(&criterion_ids);
            if json_output {
                println!("{}", serde_json::to_string(&report)?);
            } else if report.valid {
                println!("valid {}", report.graph_id);
            } else {
                for error in &report.errors {
                    eprintln!("error: {error}");
                }
            }
            Ok(if report.valid { 0 } else { 1 })
        }
        GraphCommand::DryRun(args) => {
            let (graph, initiative) = read_graph_and_initiative(&args.graph, &args.initiative)?;
            let criterion_ids = criterion_ids(&initiative);
            let report = graph.validation_report(&criterion_ids);
            if !report.valid {
                if json_output {
                    println!("{}", serde_json::to_string(&report)?);
                }
                return Ok(1);
            }
            let mutations = graph_mutations(&graph, &args.repo, &report.topological_order);
            let creates: Vec<_> = mutations
                .iter()
                .filter(|m| m["operation"] == "ticket.create")
                .cloned()
                .collect();
            let dependencies: Vec<_> = mutations
                .iter()
                .filter(|m| m["operation"] == "ticket.dep.add")
                .cloned()
                .collect();
            let output = json!({
                "schema": "product_to_code.ticket_graph.dry_run.v1",
                "pure": true,
                "daemon_connected": false,
                "authority": "local dry-run only; daemon owns ticket mutation",
                "graph_id": graph.id,
                "initiative_id": initiative.id,
                "creates": creates,
                "updates": [],
                "dependencies": dependencies,
                "dispatches": [],
                "blocked": [],
                "mutations": mutations,
            });
            if json_output {
                println!("{}", serde_json::to_string(&output)?);
            } else {
                println!(
                    "{} future mutations",
                    output["mutations"].as_array().map_or(0, Vec::len)
                );
            }
            Ok(0)
        }
        GraphCommand::ProposeApply(args) => {
            let (graph, initiative) = read_graph_and_initiative(&args.graph, &args.initiative)?;
            let criterion_ids = criterion_ids(&initiative);
            let report = graph.validation_report(&criterion_ids);
            if !report.valid {
                if json_output {
                    println!("{}", serde_json::to_string(&report)?);
                }
                return Ok(1);
            }
            let action = json!({
                "repo": args.repo,
                "graph": graph,
                "initiative": initiative,
                "topological_order": report.topological_order,
                "mutations": graph_mutations(&graph, &args.repo, &report.topological_order),
            });
            let mut client = Client::connect_or_spawn(layout).await?;
            let result = client
                .call(
                    "factory.propose_action",
                    json!({"kind": "ticket_graph.apply", "action": action}),
                )
                .await?;
            let proposal_id = result["proposal"]["id"].as_str().unwrap_or("?");
            let digest = result["digest"].as_str().unwrap_or("?");
            let output = json!({
                "schema": "product_to_code.ticket_graph.propose_apply.v1",
                "kind": "ticket_graph.apply",
                "submitted_to_daemon": true,
                "authority": "daemon accepted authority boundary; local CLI did not apply ticket mutations",
                "authority_boundary": "daemon factory.propose_action owns proposal persistence; approval/apply happen outside this CLI command; local CLI did not apply ticket mutations",
                "canonical_action": result["proposal"]["action"],
                "human_display": format!("Review and approve ticket_graph.apply proposal {proposal_id}"),
                "approval_instructions": format!("Use rk factory approve {proposal_id} {digest}; no local approved-id apply path exists here"),
                "proposal_id": proposal_id,
                "proposal": result.get("proposal").cloned().unwrap_or(serde_json::Value::Null),
                "digest": digest,
            });
            if json_output {
                println!("{}", serde_json::to_string(&output)?);
            } else {
                println!(
                    "proposed ticket_graph.apply {}",
                    output["digest"].as_str().unwrap_or("?")
                );
            }
            Ok(0)
        }
    }
}

fn read_graph_and_initiative(
    graph: &PathBuf,
    initiative: &PathBuf,
) -> Result<(TicketGraph, InitiativeContract)> {
    let initiative: InitiativeContract = read_json(initiative)?;
    initiative.validate()?;
    Ok((read_json(graph)?, initiative))
}

fn criterion_ids(initiative: &InitiativeContract) -> Vec<String> {
    initiative
        .acceptance_criteria
        .iter()
        .map(|criterion| criterion.id.clone())
        .collect()
}

fn graph_mutations(
    graph: &TicketGraph,
    repo: &str,
    topological_order: &[String],
) -> Vec<serde_json::Value> {
    let mut mutations = Vec::new();
    for id in topological_order {
        if let Some(node) = graph.nodes.iter().find(|node| &node.id == id) {
            mutations.push(json!({
                "operation": "ticket.create",
                "stable_graph_node_id": node.id,
                "repo": repo,
                "title": node.title,
                "description": node.description,
                "acceptance_criterion_ids": node.acceptance_criterion_ids,
            }));
        }
    }
    for edge in &graph.edges {
        mutations.push(json!({
            "operation": "ticket.dep.add",
            "blocked_graph_node_id": edge.to,
            "dependency_graph_node_id": edge.from,
            "relationship": edge.relationship,
        }));
    }
    mutations
}

fn run_research(command: ResearchCommand, json_output: bool) -> Result<i32> {
    match command {
        ResearchCommand::Validate(args) => {
            let artifact = read_json::<ArchitectureResearchArtifact>(&args.artifact)?;
            let initiative = read_json::<InitiativeContract>(&args.initiative)?;
            let report = artifact.validate_for_initiative(&initiative);
            if json_output {
                println!("{}", serde_json::to_string(&report)?);
            } else if report.valid {
                println!("valid {}", report.artifact_id);
            } else {
                for error in &report.errors {
                    eprintln!("error: {error}");
                }
            }
            Ok(if report.valid { 0 } else { 1 })
        }
        ResearchCommand::Render(args) => {
            let artifact = read_json::<ArchitectureResearchArtifact>(&args.artifact)?;
            match args.format {
                RenderFormat::Json => {
                    if json_output {
                        println!(
                            "{}",
                            serde_json::to_string(&json!({
                                "schema": "product_to_code.architecture_research.render.v1",
                                "artifact": artifact,
                            }))?
                        );
                    } else {
                        println!("{}", serde_json::to_string_pretty(&artifact)?);
                    }
                }
                RenderFormat::Markdown => print!("{}", artifact.render_markdown()),
            }
            Ok(0)
        }
    }
}

fn read_json<T: serde::de::DeserializeOwned>(path: &PathBuf) -> Result<T> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parse {} as JSON", path.display()))
}

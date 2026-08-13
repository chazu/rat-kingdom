use anyhow::{Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use rk_core::paths::Layout;
use rk_core::product_to_code::contracts::{
    ArchitectureResearchArtifact, GenericEvidence, InitiativeContract, TicketGraph,
    TicketGraphNode, VerificationReport,
};
use rk_core::product_to_code::evidence::{delivery_gate, dispatch_gate, validate_evidence_item};
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
    /// Validate generic evidence and gate dispatch or delivery.
    Evidence {
        #[command(subcommand)]
        command: EvidenceCommand,
    },
    /// Require impact evidence before feature dispatch.
    DispatchGate(DispatchGateArgs),
    /// Require verification and applicable delivery evidence.
    DeliveryGate(DeliveryGateArgs),
    /// Independent verification of acceptance criteria to evidence (no implementation authority).
    VerifyReport {
        #[command(subcommand)]
        command: VerifyReportCommand,
    },
    /// Compose product-to-code workflow proposals (typed daemon proposals only).
    Workflow {
        #[command(subcommand)]
        command: WorkflowCommand,
    },
}

#[derive(Subcommand)]
pub enum WorkflowCommand {
    /// Validate local artifacts, then submit a typed product_to_code.dispatch
    /// proposal referencing the prior approved graph apply execution.
    Propose(WorkflowProposeArgs),
}

#[derive(Args)]
pub struct WorkflowProposeArgs {
    /// InitiativeContract JSON file.
    #[arg(long)]
    initiative: PathBuf,
    /// ArchitectureResearchArtifact JSON file.
    #[arg(long)]
    research: PathBuf,
    /// TicketGraph JSON file.
    #[arg(long)]
    graph: PathBuf,
    /// Directory of GenericEvidence JSON files used by the dispatch gate.
    #[arg(long)]
    evidence_dir: PathBuf,
    /// Registered repository name or path for the dispatch proposal.
    #[arg(long, default_value = ".")]
    repo: String,
}

#[derive(Subcommand)]
pub enum VerifyReportCommand {
    /// Validate a verification report maps every acceptance criterion to evidence or an explicit gap.
    Validate(VerifyReportValidateArgs),
    /// Render a verification report deterministically for humans.
    Render(VerifyReportRenderArgs),
}

#[derive(Args)]
pub struct VerifyReportValidateArgs {
    /// VerificationReport JSON file.
    #[arg(long)]
    report: PathBuf,
    /// InitiativeContract JSON file referenced by the report.
    #[arg(long)]
    initiative: PathBuf,
    /// Directory of GenericEvidence JSON files referenced by the report.
    #[arg(long)]
    evidence_dir: PathBuf,
}

#[derive(Args)]
pub struct VerifyReportRenderArgs {
    /// VerificationReport JSON file.
    #[arg(long)]
    report: PathBuf,
    /// Output format.
    #[arg(long, value_enum, default_value_t = RenderFormat::Markdown)]
    format: RenderFormat,
}

#[derive(Subcommand)]
pub enum EvidenceCommand {
    /// Validate one local generic evidence JSON file against an initiative.
    Validate(EvidenceValidateArgs),
}

#[derive(Args)]
pub struct EvidenceValidateArgs {
    #[arg(long)]
    evidence: PathBuf,
    #[arg(long)]
    initiative: PathBuf,
}

#[derive(Args)]
pub struct DispatchGateArgs {
    #[arg(long)]
    ticket: PathBuf,
    #[arg(long)]
    evidence_dir: PathBuf,
}

#[derive(Args)]
pub struct DeliveryGateArgs {
    #[arg(long)]
    ticket: PathBuf,
    #[arg(long)]
    verification_report: PathBuf,
    #[arg(long)]
    evidence_dir: PathBuf,
    #[arg(long)]
    initiative: Option<PathBuf>,
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
        ProductToCodeCommand::Evidence { command } => run_evidence(command, json_output),
        ProductToCodeCommand::DispatchGate(args) => run_dispatch_gate(args, json_output),
        ProductToCodeCommand::DeliveryGate(args) => run_delivery_gate(args, json_output),
        ProductToCodeCommand::VerifyReport { command } => run_verify_report(command, json_output),
        ProductToCodeCommand::Workflow { command } => {
            run_workflow(layout, command, json_output).await
        }
    }
}

/// Thin wiring for the product-to-code workflow composition: validate local
/// artifacts (research before graph, graph before dispatch), split graph nodes
/// into dispatchable vs blocked by the generic impact-evidence dispatch gate,
/// and submit ONE typed `product_to_code.dispatch` proposal through the same
/// Phase 2 `factory.propose_action` path as graph propose-apply. No local
/// mutation and no approved-id shortcut exist here; the daemon owns approval
/// and dispatch.
async fn run_workflow(
    layout: &Layout,
    command: WorkflowCommand,
    json_output: bool,
) -> Result<i32> {
    let WorkflowCommand::Propose(args) = command;
    let initiative: InitiativeContract = read_json(&args.initiative)?;
    let research: ArchitectureResearchArtifact = read_json(&args.research)?;
    let research_report = research.validate_for_initiative(&initiative);
    if !research_report.valid {
        let output = json!({
            "schema": "product_to_code.workflow.propose.v1",
            "stage": "research",
            "valid": false,
            "submitted_to_daemon": false,
            "errors": research_report.errors,
        });
        print_json_or_errors(&output, json_output)?;
        return Ok(1);
    }
    let graph: TicketGraph = read_json(&args.graph)?;
    let graph_report = graph.validation_report_for_initiative(&initiative);
    if !graph_report.valid {
        let output = json!({
            "schema": "product_to_code.workflow.propose.v1",
            "stage": "graph",
            "valid": false,
            "submitted_to_daemon": false,
            "errors": graph_report.errors,
        });
        print_json_or_errors(&output, json_output)?;
        return Ok(1);
    }

    // Dispatch gate per node: nodes without current generic impact evidence
    // are blocked and listed separately; they are never dispatched.
    let evidence = read_evidence_dir(&args.evidence_dir)?;
    let mut dispatches = Vec::new();
    let mut blocked = Vec::new();
    for node_id in &graph_report.topological_order {
        let Some(node) = graph.nodes.iter().find(|node| &node.id == node_id) else {
            continue;
        };
        let gate = dispatch_gate(node, &evidence);
        if gate.valid {
            dispatches.push(json!({
                "graph_node_id": node.id,
                "task_description": node.description,
            }));
        } else {
            blocked.push(json!({
                "graph_node_id": node.id,
                "reasons": gate.errors,
            }));
        }
    }

    // The dispatch proposal is bound to the prior approved graph apply
    // execution. Find its proposal through the daemon's approval records.
    let mut client = Client::connect_or_spawn(layout).await?;
    let snapshot = client.call("factory.snapshot", json!({})).await?;
    let proposals = snapshot["snapshot"]["approvals"]["proposals"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let grants = snapshot["snapshot"]["approvals"]["grants"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let graph_apply_proposal_id = proposals
        .iter()
        .filter(|proposal| {
            proposal["kind"] == "ticket_graph.apply"
                && proposal["action"]["graph"]["id"] == json!(graph.id)
        })
        .filter_map(|proposal| proposal["id"].as_str())
        .find(|id| {
            grants
                .iter()
                .any(|grant| grant["proposal_id"] == json!(id) && grant["status"] == "consumed")
        })
        .map(str::to_string);
    let Some(graph_apply_proposal_id) = graph_apply_proposal_id else {
        let output = json!({
            "schema": "product_to_code.workflow.propose.v1",
            "stage": "graph_apply",
            "valid": false,
            "submitted_to_daemon": false,
            "errors": [format!(
                "no consumed ticket_graph.apply execution found for graph {}; run graph propose-apply, approve, and execute it first",
                graph.id
            )],
        });
        print_json_or_errors(&output, json_output)?;
        return Ok(1);
    };

    let action = json!({
        "repo": args.repo,
        "initiative": initiative,
        "graph_id": graph.id,
        "graph_apply_proposal_id": graph_apply_proposal_id,
        "dispatches": dispatches,
        "blocked": blocked,
    });
    let result = client
        .call(
            "factory.propose_action",
            json!({"kind": "product_to_code.dispatch", "action": action}),
        )
        .await?;
    let proposal_id = result["proposal"]["id"].as_str().unwrap_or("?");
    let digest = result["digest"].as_str().unwrap_or("?");
    let output = json!({
        "schema": "product_to_code.workflow.propose.v1",
        "stage": "dispatch",
        "kind": "product_to_code.dispatch",
        "valid": true,
        "submitted_to_daemon": true,
        "authority": "daemon accepted authority boundary; local CLI did not dispatch workflows",
        "authority_boundary": "daemon factory.propose_action owns proposal persistence; approval/dispatch happen outside this CLI command; local CLI did not dispatch workflows",
        "graph_apply_proposal_id": graph_apply_proposal_id,
        "dispatches": result["proposal"]["action"]["dispatches"],
        "blocked": result["proposal"]["action"]["blocked"],
        "canonical_action": result["proposal"]["action"],
        "human_display": format!("Review and approve product_to_code.dispatch proposal {proposal_id}"),
        "approval_instructions": format!("Use rk factory approve {proposal_id} {digest}; no local approved-id dispatch path exists here"),
        "proposal_id": proposal_id,
        "proposal": result.get("proposal").cloned().unwrap_or(serde_json::Value::Null),
        "digest": digest,
    });
    if json_output {
        println!("{}", serde_json::to_string(&output)?);
    } else {
        println!(
            "proposed product_to_code.dispatch {}",
            output["digest"].as_str().unwrap_or("?")
        );
    }
    Ok(0)
}

fn run_verify_report(command: VerifyReportCommand, json_output: bool) -> Result<i32> {
    match command {
        VerifyReportCommand::Validate(args) => {
            let report: VerificationReport = read_json(&args.report)?;
            let initiative: InitiativeContract = read_json(&args.initiative)?;
            let evidence = read_evidence_dir(&args.evidence_dir)?;
            let validation = rk_core::product_to_code::verification::validate_report(
                &report,
                &initiative,
                &evidence,
            );
            let code = if validation.valid { 0 } else { 1 };
            let output = serde_json::to_value(&validation)?;
            print_json_or_errors(&output, json_output)?;
            Ok(code)
        }
        VerifyReportCommand::Render(args) => {
            let report: VerificationReport = read_json(&args.report)?;
            match args.format {
                RenderFormat::Json => {
                    if json_output {
                        println!(
                            "{}",
                            serde_json::to_string(&json!({
                                "schema": "product_to_code.verification_report.render.v1",
                                "report": report,
                            }))?
                        );
                    } else {
                        println!("{}", serde_json::to_string_pretty(&report)?);
                    }
                }
                RenderFormat::Markdown => {
                    print!(
                        "{}",
                        rk_core::product_to_code::verification::render_markdown(&report)
                    );
                }
            }
            Ok(0)
        }
    }
}

fn run_evidence(command: EvidenceCommand, json_output: bool) -> Result<i32> {
    match command {
        EvidenceCommand::Validate(args) => {
            let evidence: GenericEvidence = read_json(&args.evidence)?;
            let initiative: InitiativeContract = read_json(&args.initiative)?;
            let errors = validate_evidence_item(&evidence, &initiative);
            let output = json!({
                "schema": "product_to_code.evidence.validate.v1",
                "valid": errors.is_empty(),
                "evidence_id": evidence.id,
                "kind": evidence.kind,
                "producer": evidence.producer,
                "errors": errors,
            });
            print_json_or_errors(&output, json_output)?;
            Ok(if output["valid"].as_bool().unwrap_or(false) {
                0
            } else {
                1
            })
        }
    }
}

fn run_dispatch_gate(args: DispatchGateArgs, json_output: bool) -> Result<i32> {
    let ticket: TicketGraphNode = read_json(&args.ticket)?;
    let evidence = read_evidence_dir(&args.evidence_dir)?;
    let report = dispatch_gate(&ticket, &evidence);
    let code = if report.valid { 0 } else { 1 };
    let output = serde_json::to_value(report)?;
    print_json_or_errors(&output, json_output)?;
    Ok(code)
}

fn run_delivery_gate(args: DeliveryGateArgs, json_output: bool) -> Result<i32> {
    let ticket: TicketGraphNode = read_json(&args.ticket)?;
    let report: VerificationReport = read_json(&args.verification_report)?;
    let initiative = if let Some(path) = args.initiative {
        read_json(&path)?
    } else {
        InitiativeContract {
            id: report.initiative_id.clone(),
            title: "delivery gate synthetic initiative".to_string(),
            scope: "delivery gate".to_string(),
            acceptance_criteria: ticket
                .acceptance_criterion_ids
                .iter()
                .map(
                    |id| rk_core::product_to_code::contracts::AcceptanceCriterion {
                        id: id.clone(),
                        text: id.clone(),
                        browser_acceptance_applicable: false,
                    },
                )
                .collect(),
            browser_acceptance_applicable: false,
        }
    };
    let evidence = read_evidence_dir(&args.evidence_dir)?;
    let gate = delivery_gate(&initiative, &ticket, &report, &evidence);
    let code = if gate.valid { 0 } else { 1 };
    let output = serde_json::to_value(gate)?;
    print_json_or_errors(&output, json_output)?;
    Ok(code)
}

async fn run_graph(layout: &Layout, command: GraphCommand, json_output: bool) -> Result<i32> {
    match command {
        GraphCommand::Validate(args) => {
            let (graph, initiative) = read_graph_and_initiative(&args.graph, &args.initiative)?;
            let report = graph.validation_report_for_initiative(&initiative);
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
            let report = graph.validation_report_for_initiative(&initiative);
            if !report.valid {
                if json_output {
                    println!("{}", serde_json::to_string(&report)?);
                }
                return Ok(1);
            }
            let apply_plan = graph.apply_plan_for_initiative(&args.repo, &initiative)?;
            let mutations = apply_plan.mutations();
            let output = json!({
                "schema": "product_to_code.ticket_graph.dry_run.v1",
                "pure": true,
                "daemon_connected": false,
                "authority": "local dry-run only; daemon owns ticket mutation",
                "graph_id": graph.id,
                "initiative_id": initiative.id,
                "topological_order": apply_plan.topological_order,
                "creates": apply_plan.creates,
                "updates": apply_plan.updates,
                "dependencies": apply_plan.dependencies,
                "dispatches": apply_plan.dispatches,
                "blocked": apply_plan.blocked,
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
            let report = graph.validation_report_for_initiative(&initiative);
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

fn read_evidence_dir(path: &PathBuf) -> Result<Vec<GenericEvidence>> {
    let mut files = std::fs::read_dir(path)
        .with_context(|| format!("read evidence dir {}", path.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    files.sort_by_key(|entry| entry.path());
    files
        .into_iter()
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("json"))
        .map(|entry| read_json(&entry.path()))
        .collect()
}

fn print_json_or_errors(value: &serde_json::Value, json_output: bool) -> Result<()> {
    if json_output {
        println!("{}", serde_json::to_string(value)?);
    } else if value["valid"].as_bool().unwrap_or(false) {
        println!("valid");
    } else if let Some(errors) = value["errors"].as_array() {
        for error in errors {
            if let Some(error) = error.as_str() {
                eprintln!("error: {error}");
            }
        }
    }
    Ok(())
}

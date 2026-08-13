use anyhow::{Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use rk_core::product_to_code::contracts::{ArchitectureResearchArtifact, InitiativeContract};
use serde_json::json;
use std::path::PathBuf;

#[derive(Subcommand)]
pub enum ProductToCodeCommand {
    /// Validate and render architecture research artifacts.
    Research {
        #[command(subcommand)]
        command: ResearchCommand,
    },
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

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum RenderFormat {
    Json,
    Markdown,
}

pub fn run(command: ProductToCodeCommand, json_output: bool) -> Result<i32> {
    match command {
        ProductToCodeCommand::Research { command } => run_research(command, json_output),
    }
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

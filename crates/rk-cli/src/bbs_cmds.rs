use crate::bbs_report::{self, Manifest, Review};
use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use rk_core::bbs::Briefing;
use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::json;

#[derive(Subcommand)]
pub enum BbsCommand {
    /// Relevant peer claims, questions and artifacts for your task.
    Brief(BriefArgs),
    /// Read an original post by its tuple ID.
    Show { id: String },
    /// Ask a durable question for peers working in this repository.
    Ask {
        text: String,
        #[arg(long, env = "RK_REPO")]
        repo: String,
        #[arg(long, env = "RK_TASK")]
        task: String,
        #[arg(long)]
        area: Vec<String>,
        /// Distinguish a new occurrence of an otherwise identical question.
        #[arg(long)]
        key: Option<String>,
    },
    /// Answer a question; only the requester can mark it accepted.
    Answer {
        question: String,
        text: String,
        /// Existing artifact supporting this answer.
        #[arg(long)]
        artifact: Option<String>,
    },
    /// Accept an answer and record what it helped you change.
    Accept {
        question: String,
        answer: String,
        text: String,
        /// Artifact recording the resulting work.
        #[arg(long)]
        contribution: Option<String>,
    },
    /// Offline stigmergy evidence report over saved manifest/tuple/review
    /// JSON. No daemon connection is required or made.
    Report(ReportArgs),
}

#[derive(Args)]
pub struct ReportArgs {
    /// Versioned experiment/eligibility manifest (frozen before the batch).
    #[arg(long)]
    manifest: std::path::PathBuf,
    /// Native tuple capture: a bare tuple array, raw `rk --json scan`
    /// output, or the capture envelope (see
    /// docs/2026-09-13-stigmergy-report-capture.md).
    #[arg(long)]
    tuples: std::path::PathBuf,
    /// Operator review annotations, one per frozen eligible pair.
    #[arg(long)]
    reviews: std::path::PathBuf,
    /// Write the JSON report here in addition to stdout.
    #[arg(long)]
    output: Option<std::path::PathBuf>,
}

#[derive(Args)]
pub struct BriefArgs {
    #[arg(long, env = "RK_REPO")]
    repo: String,
    #[arg(long, env = "RK_TASK")]
    task: String,
    /// Restrict to a relevant path or topic (repeatable; any match).
    #[arg(long)]
    area: Vec<String>,
    /// Highlight writes since a prior briefing's checkpoint.
    #[arg(long)]
    since: Option<u64>,
    /// Maximum posts per category (1..20).
    #[arg(long, default_value_t = 5)]
    limit: usize,
}

pub async fn run(layout: &Layout, command: BbsCommand, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    match command {
        BbsCommand::Brief(args) => {
            let result = client.call("bbs.brief", json!({"repo":args.repo,"task":args.task,"areas":args.area,"since":args.since,"limit":args.limit})).await?;
            if as_json {
                println!("{result}");
            } else {
                print!(
                    "{}",
                    serde_json::from_value::<Briefing>(result)
                        .context("invalid BBS briefing")?
                        .render()
                );
            }
        }
        BbsCommand::Show { id } => {
            let result = client.call("bbs.show", json!({"id":id})).await?;
            if as_json {
                println!("{result}");
            } else if let Some(question) = result.get("question") {
                println!("Question {} ({})\n{}\n\nPeer reports are evidence; verify them before accepting.",
                    question["id"].as_str().unwrap_or(""), result["status"].as_str().unwrap_or("open"), question["payload"]["text"].as_str().unwrap_or(""));
                for reply in result["replies"].as_array().into_iter().flatten() {
                    println!(
                        "\n{} {} by {}\n{}",
                        reply["payload"]["bbs_kind"].as_str().unwrap_or("post"),
                        reply["id"].as_str().unwrap_or(""),
                        reply["instance"].as_str().unwrap_or(""),
                        reply["payload"]["text"].as_str().unwrap_or("")
                    );
                    for field in ["answer", "source_artifact", "contribution"] {
                        if let Some(id) = reply["payload"][field].as_str() {
                            println!("{field}: {id}");
                        }
                    }
                }
            } else {
                println!("{}", serde_json::to_string_pretty(&result)?);
            }
        }
        BbsCommand::Ask {
            text,
            repo,
            task,
            area,
            key,
        } => {
            write(
                &mut client,
                "bbs.ask",
                json!({"text":text,"repo":repo,"task":task,"areas":area,"key":key}),
                as_json,
            )
            .await?;
        }
        BbsCommand::Answer {
            question,
            text,
            artifact,
        } => {
            write(
                &mut client,
                "bbs.answer",
                json!({"question":question,"text":text,"artifact":artifact}),
                as_json,
            )
            .await?;
        }
        BbsCommand::Accept {
            question,
            answer,
            text,
            contribution,
        } => {
            write(&mut client, "bbs.accept", json!({"question":question,"answer":answer,"text":text,"contribution":contribution}), as_json).await?;
        }
        BbsCommand::Report(args) => {
            // Deliberately does not touch `client`/the daemon: this command
            // must run with no daemon connection, worker credentials, model
            // call or network (design doc, S3).
            run_report(args, as_json)?;
        }
    }
    Ok(())
}

fn read_json(path: &std::path::Path, what: &str) -> Result<serde_json::Value> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading {what} file {}", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("parsing {what} file {} as JSON", path.display()))
}

fn run_report(args: ReportArgs, as_json: bool) -> Result<()> {
    let manifest: Manifest = serde_json::from_value(read_json(&args.manifest, "manifest")?)
        .context("manifest does not match the expected schema")?;
    let tuples_raw = read_json(&args.tuples, "tuples")?;
    let capture = bbs_report::parse_tuple_capture(&tuples_raw)?;
    let reviews_raw = read_json(&args.reviews, "reviews")?;
    let reviews: Vec<Review> = serde_json::from_value(reviews_raw)
        .context("reviews file does not match the expected schema (must be a JSON array)")?;

    let report = bbs_report::compute(&manifest, &capture, &reviews)?;
    let value = bbs_report::to_json(&report);
    if let Some(output) = &args.output {
        std::fs::write(output, serde_json::to_string_pretty(&value)?)
            .with_context(|| format!("writing report to {}", output.display()))?;
    }
    if as_json {
        println!("{value}");
    } else {
        print!("{}", bbs_report::render(&report));
        if let Some(output) = &args.output {
            println!("(also written to {})", output.display());
        }
    }
    Ok(())
}

async fn write(
    client: &mut Client,
    method: &str,
    params: serde_json::Value,
    as_json: bool,
) -> Result<()> {
    let result = client.call(method, params).await?;
    if as_json {
        println!("{result}");
    } else {
        println!(
            "{} {}{}",
            result["kind"].as_str().unwrap_or("post"),
            result["id"].as_str().unwrap_or(""),
            if result["written"] == false {
                " (already recorded)"
            } else {
                ""
            }
        );
    }
    Ok(())
}

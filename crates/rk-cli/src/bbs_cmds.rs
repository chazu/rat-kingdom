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

use crate::bbs_report::{self, Manifest, ReviewsFile};
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
    /// Publish a durable finding: a reproduction, interface constraint,
    /// reusable implementation, or failed approach useful to peers.
    Publish {
        text: String,
        #[arg(long, env = "RK_REPO")]
        repo: String,
        #[arg(long, env = "RK_TASK")]
        task: String,
        /// Applicable path or topic (repeatable; at least one required).
        #[arg(long = "area", required = true)]
        areas: Vec<String>,
        /// Lowercase hex commit id (7..64 chars) this finding is about. A
        /// claim about the source tree you looked at, not a verified match —
        /// the daemon checks its shape only, never resolves it against git.
        #[arg(long)]
        revision: String,
        /// Existing artifact supporting this finding (repeatable; at least one required).
        #[arg(long = "evidence", required = true)]
        evidence: Vec<String>,
        /// Known limits or caveats on this finding.
        #[arg(long)]
        limitations: String,
        /// Distinguish a new occurrence of an otherwise identical finding.
        #[arg(long)]
        key: Option<String>,
    },
    /// Record use of an ordinary artifact or a peer finding/answer.
    Reuse {
        /// The artifact or finding/answer this receipt is about.
        source: String,
        #[arg(long, env = "RK_TASK")]
        task: String,
        #[arg(long, value_parser = ["used", "adapted", "confirmed", "rejected"])]
        outcome: String,
        #[arg(long)]
        text: String,
        /// Existing artifact evidencing this receipt (repeatable; at least one required).
        #[arg(long = "evidence", required = true)]
        evidence: Vec<String>,
        /// Distinguish a new occurrence of an otherwise identical receipt.
        #[arg(long)]
        key: Option<String>,
    },
    /// Operator-only: assess a reuse receipt.
    Assess {
        /// The reuse receipt this assessment is about.
        receipt: String,
        #[arg(long, value_parser = ["verified", "unsupported", "incorrect"])]
        verdict: String,
        #[arg(long)]
        reason: String,
        /// Existing artifact evidencing this assessment (repeatable; at least one required).
        #[arg(long = "evidence", required = true)]
        evidence: Vec<String>,
        /// Distinguish a new occurrence of an otherwise identical assessment.
        #[arg(long)]
        key: Option<String>,
    },
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
    // `bbs report` is settled before any client exists: it must run with no
    // daemon connection, worker credentials, model call or network (design
    // doc, S3). Connecting first would both fail outright when no daemon is
    // reachable and, worse, silently spawn one as a side effect of a pure
    // offline aggregation, so the connect stays strictly below this return.
    if let BbsCommand::Report(args) = command {
        return run_report(args, as_json);
    }
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
            } else {
                if let Some(question) = result.get("question") {
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
                } else if result["tuple"]["payload"]["bbs_kind"].as_str().is_some() {
                    let tuple = &result["tuple"];
                    println!(
                        "{} {} by {}\n{}",
                        tuple["payload"]["bbs_kind"].as_str().unwrap_or("post"),
                        tuple["id"].as_str().unwrap_or(""),
                        tuple["instance"].as_str().unwrap_or(""),
                        tuple["payload"]["text"].as_str().unwrap_or("")
                    );
                } else {
                    println!("{}", serde_json::to_string_pretty(&result)?);
                }
                // Linked reuse threads onto any rendering above: an ordinary
                // artifact, a finding, an answer (also a question reply), or a
                // reuse receipt showing its own assessments.
                for receipt in result
                    .get("reuse")
                    .and_then(|r| r.as_array())
                    .into_iter()
                    .flatten()
                {
                    let r = &receipt["receipt"];
                    println!(
                        "\nreuse {} by {} — {}\n{}",
                        r["id"].as_str().unwrap_or(""),
                        r["instance"].as_str().unwrap_or(""),
                        r["payload"]["outcome"].as_str().unwrap_or(""),
                        r["payload"]["text"].as_str().unwrap_or("")
                    );
                    if let Some(current) = receipt["current_assessment"].as_object() {
                        println!(
                            "  current assessment: {} by {} — {}",
                            current["payload"]["verdict"].as_str().unwrap_or(""),
                            current["instance"].as_str().unwrap_or(""),
                            current["payload"]["reason"].as_str().unwrap_or("")
                        );
                    }
                    let count = receipt["assessments"].as_array().map_or(0, |a| a.len());
                    if count > 1 {
                        println!("  ({count} assessments recorded; showing the current one)");
                    }
                }
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
        BbsCommand::Publish {
            text,
            repo,
            task,
            areas,
            revision,
            evidence,
            limitations,
            key,
        } => {
            write(&mut client, "bbs.publish", json!({"text":text,"repo":repo,"task":task,"areas":areas,"revision":revision,"evidence":evidence,"limitations":limitations,"key":key}), as_json).await?;
        }
        BbsCommand::Reuse {
            source,
            task,
            outcome,
            text,
            evidence,
            key,
        } => {
            write(&mut client, "bbs.reuse", json!({"source":source,"task":task,"outcome":outcome,"text":text,"evidence":evidence,"key":key}), as_json).await?;
        }
        BbsCommand::Assess {
            receipt,
            verdict,
            reason,
            evidence,
            key,
        } => {
            write(&mut client, "bbs.assess", json!({"receipt":receipt,"verdict":verdict,"reason":reason,"evidence":evidence,"key":key}), as_json).await?;
        }
        BbsCommand::Report(_) => {
            unreachable!("`bbs report` returns above, before the daemon connection")
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
    let reviews: ReviewsFile = serde_json::from_value(reviews_raw).context(
        "reviews file does not match the expected schema (a JSON array of pair reviews, or an \
         object with `pairs` and `tasks`)",
    )?;

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

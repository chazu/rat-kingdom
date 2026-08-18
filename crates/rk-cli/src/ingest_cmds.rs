use anyhow::{anyhow, Result};
use chrono::Utc;
use clap::{Args, Subcommand};
use rk_core::paths::Layout;
use rk_core::sdlc::{
    CiSignal, ConfiguredSourceName, Correlation, SignalEnvelope, SignalKind, SignalPayload,
};
use rk_daemon::Client;
use serde_json::json;
use std::collections::BTreeMap;

#[derive(Subcommand)]
pub enum IngestCommand {
    /// Submit one canonical SDLC event.
    Event(IngestEventArgs),
    /// Read current SDLC facts without starting the daemon.
    State(IngestStateArgs),
}

#[derive(Args)]
pub struct IngestEventArgs {
    #[arg(long)]
    pub source: String,
    #[arg(long)]
    pub file: Option<String>,
    #[arg(long)]
    pub kind: Option<String>,
    #[arg(long)]
    pub delivery_id: Option<String>,
    #[arg(long)]
    pub summary: Option<String>,
    #[arg(long)]
    pub repo: Option<String>,
    #[arg(long)]
    pub branch: Option<String>,
    #[arg(long)]
    pub workflow: Option<String>,
    #[arg(long)]
    pub job: Option<String>,
    #[arg(long)]
    pub commit_sha: Option<String>,
    #[arg(long = "attr")]
    pub attrs: Vec<String>,
}

#[derive(Args)]
pub struct IngestStateArgs {
    #[arg(long)]
    pub source: String,
    #[arg(long)]
    pub repo: Option<String>,
    #[arg(long)]
    pub environment: Option<String>,
    #[arg(long)]
    pub service: Option<String>,
    #[arg(long = "alert-key")]
    pub alert_key: Option<String>,
}

pub async fn run(layout: &Layout, command: IngestCommand, json_out: bool) -> Result<()> {
    match command {
        IngestCommand::Event(args) => event(layout, args, json_out).await,
        IngestCommand::State(args) => state(layout, args, json_out).await,
    }
}

async fn event(layout: &Layout, args: IngestEventArgs, json_out: bool) -> Result<()> {
    let envelope = if let Some(path) = &args.file {
        if args.kind.is_some() || args.delivery_id.is_some() || args.summary.is_some() {
            return Err(anyhow!(
                "--file accepts a canonical envelope only; do not combine it with event fields"
            ));
        }
        let text = std::fs::read_to_string(path)?;
        serde_json::from_str::<SignalEnvelope>(&text)
            .map_err(|e| anyhow!("--file must contain a canonical SignalEnvelope: {e}"))?
    } else {
        build_ci_envelope(&args)?
    };
    let mut client = Client::connect_as(layout, &format!("source:{}", args.source)).await?;
    let result = client
        .call(
            "ingest.event",
            json!({"source": args.source, "envelope": envelope}),
        )
        .await?;
    if json_out {
        println!("{result}");
    } else {
        println!(
            "accepted {}",
            result["receipt"]["receipt_id"].as_str().unwrap_or("?")
        );
    }
    Ok(())
}

async fn state(layout: &Layout, args: IngestStateArgs, json_out: bool) -> Result<()> {
    let mut client = Client::connect_as(layout, &format!("source:{}", args.source)).await?;
    let result = client
        .call(
            "ingest.state",
            json!({"repo": args.repo, "environment": args.environment, "service": args.service, "alert_key": args.alert_key}),
        )
        .await?;
    if json_out {
        println!("{result}");
    } else {
        println!(
            "{} facts",
            result["facts"].as_array().map(|v| v.len()).unwrap_or(0)
        );
    }
    Ok(())
}

fn build_ci_envelope(args: &IngestEventArgs) -> Result<SignalEnvelope> {
    let kind = match args
        .kind
        .as_deref()
        .ok_or_else(|| anyhow!("--kind is required"))?
    {
        "ci_failed" => SignalKind::CiFailed,
        "ci_recovered" => SignalKind::CiRecovered,
        other => return Err(anyhow!("unsupported canonical CLI kind: {other}")),
    };
    let mut attributes = BTreeMap::new();
    for pair in &args.attrs {
        let (key, value) = pair
            .split_once('=')
            .ok_or_else(|| anyhow!("--attr must be key=value"))?;
        reject_cli_attr_key(key)?;
        attributes.insert(key.to_string(), value.to_string());
    }
    let source =
        ConfiguredSourceName::new(args.source.clone()).map_err(|e| anyhow!(e.to_string()))?;
    let now = Utc::now();
    let (status, conclusion) = match &kind {
        SignalKind::CiFailed => ("failed", "failure"),
        SignalKind::CiRecovered => ("success", "success"),
        _ => unreachable!("CLI kind parser only permits CI kinds"),
    };
    let envelope = SignalEnvelope {
        kind,
        source,
        delivery_id: args
            .delivery_id
            .clone()
            .ok_or_else(|| anyhow!("--delivery-id is required"))?,
        occurred_at: now,
        observed_at: now,
        correlation: Correlation {
            repo: args.repo.clone(),
            branch: args.branch.clone(),
            workflow: args.workflow.clone(),
            job: args.job.clone(),
            commit_sha: args.commit_sha.clone(),
            ..Default::default()
        },
        summary: args
            .summary
            .clone()
            .ok_or_else(|| anyhow!("--summary is required"))?,
        refs: Vec::new(),
        attributes,
        payload: SignalPayload::Ci(CiSignal {
            status: status.into(),
            conclusion: Some(conclusion.into()),
        }),
    };
    envelope
        .validate(&Default::default())
        .map_err(|e| anyhow!(e.to_string()))?;
    Ok(envelope)
}

fn reject_cli_attr_key(key: &str) -> Result<()> {
    let lower = key.to_ascii_lowercase();
    let banned = [
        "secret",
        "token",
        "credential",
        "password",
        "authorization",
        "raw",
        "telemetry",
        "command",
        "action",
        "executable",
    ];
    if banned.iter().any(|word| lower.contains(word)) {
        return Err(anyhow!(
            "secret-like or executable attribute key rejected: {key}"
        ));
    }
    Ok(())
}

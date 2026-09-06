//! External, repository-scoped observation runs. The collector is deliberately
//! separate from the daemon and never auto-starts or repairs what it observes.

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use clap::{Args, Subcommand, ValueEnum};
use rk_core::{id::RecordId, paths::Layout};
use rk_daemon::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

#[path = "observation_store.rs"]
mod store;
use store::ObservationLog;

const SCHEMA_VERSION: u32 = 1;
const MANIFEST: &str = "manifest.json";
const SAMPLES: &str = "samples.jsonl";
const INTERVENTIONS: &str = "interventions";
const REPORT: &str = "report.json";

#[derive(Subcommand)]
pub enum ObservationCommand {
    /// Create a run, sample until its duration elapses or Ctrl-C, then report.
    Start(StartArgs),
    /// Append one read-only sample to an existing run.
    Sample(RunPathArgs),
    /// Resume collection using the original immutable scope and thresholds.
    Resume(RunPathArgs),
    /// Record one typed intervention as an atomic evidence file.
    Record(RecordArgs),
    /// Derive a report from the run's immutable evidence.
    Report(ReportArgs),
}

#[derive(Args)]
pub struct StartArgs {
    #[arg(long)]
    repo: String,
    #[arg(long)]
    name: String,
    /// Observe only these ticket identities; empty means the whole repository.
    #[arg(long = "ticket")]
    tickets: Vec<String>,
    /// Maximum correction depth beyond the explicitly selected roots.
    #[arg(long, default_value_t = default_lineage_depth())]
    max_lineage_depth: usize,
    /// Maximum additional correction tickets in the selected cohort.
    #[arg(long, default_value_t = default_lineage_tickets())]
    max_lineage_tickets: usize,
    #[arg(long, default_value = "30s")]
    interval: String,
    #[arg(long, default_value = "5s")]
    rpc_timeout: String,
    #[arg(long, default_value = "20s")]
    sample_timeout: String,
    /// Stop after this duration; otherwise run until Ctrl-C.
    #[arg(long)]
    duration: Option<String>,
    /// Directory to create. Defaults beside, rather than inside, daemon state.
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long, default_value = "15m")]
    stale_after: String,
    #[arg(long, default_value = "10m")]
    max_landing_age: String,
    #[arg(long, default_value = "15m")]
    max_ready_age: String,
    #[arg(long)]
    max_cost_usd: Option<f64>,
    #[arg(long, default_value_t = 0)]
    max_unavailable_samples: u64,
}

#[derive(Args)]
pub struct RunPathArgs {
    run: PathBuf,
}

#[derive(Args)]
pub struct RecordArgs {
    run: PathBuf,
    #[arg(long, value_enum)]
    class: InterventionClass,
    #[arg(long)]
    summary: String,
    #[arg(long)]
    ticket: Option<String>,
    #[arg(long)]
    actor: Option<String>,
    #[arg(long = "evidence")]
    evidence: Vec<String>,
}

#[derive(Args)]
pub struct ReportArgs {
    run: PathBuf,
    /// Persist the derived report as report.json as well as printing it.
    #[arg(long)]
    finalize: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ValueEnum, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum InterventionClass {
    Mechanical,
    Llm,
    HumanGate,
    AdHoc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Manifest {
    schema_version: u32,
    id: String,
    name: String,
    repo: String,
    tickets: Vec<String>,
    #[serde(default = "default_lineage_depth")]
    max_lineage_depth: usize,
    #[serde(default = "default_lineage_tickets")]
    max_lineage_tickets: usize,
    started_at: DateTime<Utc>,
    interval_secs: u64,
    #[serde(default = "default_rpc_timeout")]
    rpc_timeout_secs: u64,
    #[serde(default = "default_sample_timeout")]
    sample_timeout_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    planned_duration_secs: Option<u64>,
    thresholds: Thresholds,
    observer_build: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Thresholds {
    stale_after_secs: u64,
    max_landing_age_secs: u64,
    max_ready_age_secs: u64,
    max_cost_usd: Option<f64>,
    max_unavailable_samples: u64,
    max_reconcile_violations: u64,
    max_forced_landings: u64,
    max_duplicate_dispatches: u64,
    max_duplicate_landings: u64,
    max_unclassified_holds: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SampleMetrics {
    live_agents: u64,
    open_tickets: u64,
    delivered_tickets: u64,
    stale_tickets: u64,
    cost_usd: f64,
    tokens: u64,
    landing_depth: u64,
    oldest_landing_age_secs: u64,
    oldest_ready_age_secs: u64,
    reconcile_violations: u64,
    actionable: u64,
    decision_required: u64,
    stalled: u64,
    unclassified_holds: u64,
    duplicate_dispatches: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Sample {
    schema_version: u32,
    sequence: u64,
    observed_at: DateTime<Utc>,
    daemon_reachable: bool,
    errors: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    king: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    work: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reconcile: Option<Value>,
    tickets: Vec<Value>,
    /// Selected correction ticket -> canonical ancestor ticket. This is
    /// structured coalesce-key provenance, never inferred from titles.
    #[serde(default)]
    lineage: BTreeMap<String, String>,
    agents: Vec<Value>,
    /// Highest repository event id seen, even when it predates this run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    event_cursor: Option<String>,
    /// New repository events since the preceding sample.
    events: Vec<Value>,
    metrics: SampleMetrics,
    #[serde(default)]
    sampling: SamplingEvidence,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SamplingEvidence {
    elapsed_ms: u64,
    gap_secs: u64,
    rpc_timeouts: Vec<String>,
    deadline_exceeded: bool,
    recovered_appends: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Intervention {
    schema_version: u32,
    id: String,
    observed_at: DateTime<Utc>,
    class: InterventionClass,
    summary: String,
    ticket: Option<String>,
    actor: String,
    evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Check {
    observed: Value,
    limit: Value,
    passed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Report {
    schema_version: u32,
    run_id: String,
    name: String,
    repo: String,
    started_at: DateTime<Utc>,
    ended_at: DateTime<Utc>,
    elapsed_secs: u64,
    samples: usize,
    max_sample_gap_secs: u64,
    unavailable_samples: u64,
    partial_samples: u64,
    #[serde(default)]
    recovered_appends: u64,
    build_mismatch_samples: u64,
    daemon_restarts: u64,
    king_replacements: u64,
    delivered_during_run: u64,
    #[serde(default)]
    correction_deliveries: u64,
    throughput_per_hour: f64,
    attributed_cost_usd: f64,
    attributed_tokens: u64,
    max_landing_depth: u64,
    max_landing_age_secs: u64,
    max_ready_age_secs: u64,
    max_reconcile_violations: u64,
    forced_landings: u64,
    duplicate_dispatches: u64,
    duplicate_landings: u64,
    max_stale_tickets: u64,
    max_unclassified_holds: u64,
    interventions: BTreeMap<String, u64>,
    checks: BTreeMap<String, Check>,
    passed: bool,
    evidence: BTreeMap<String, String>,
}

pub async fn run(layout: &Layout, command: ObservationCommand, as_json: bool) -> Result<()> {
    match command {
        ObservationCommand::Start(args) => start(layout, args, as_json).await,
        ObservationCommand::Sample(args) => {
            let sample = append_sample(layout, &args.run).await?;
            print_value(&serde_json::to_value(sample)?, as_json)
        }
        ObservationCommand::Resume(args) => collect_run(layout, &args.run, as_json).await,
        ObservationCommand::Record(args) => record(args, as_json),
        ObservationCommand::Report(args) => report(args, as_json),
    }
}

async fn start(layout: &Layout, args: StartArgs, as_json: bool) -> Result<()> {
    let interval = parse_duration(&args.interval)?;
    if interval.is_zero() {
        bail!("--interval must be greater than zero");
    }
    let duration = args.duration.as_deref().map(parse_duration).transpose()?;
    let started_at = Utc::now();
    let id = RecordId::new().to_string();
    let run_dir = args
        .output
        .unwrap_or_else(|| default_root(layout).join(&id));
    if let Some(parent) = run_dir.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create observation parent {}", parent.display()))?;
    }
    fs::create_dir(&run_dir)
        .with_context(|| format!("create observation run {}", run_dir.display()))?;
    fs::create_dir(run_dir.join(INTERVENTIONS))?;
    let manifest = Manifest {
        schema_version: SCHEMA_VERSION,
        id,
        name: nonempty(args.name, "--name")?,
        repo: nonempty(args.repo, "--repo")?,
        tickets: args.tickets,
        max_lineage_depth: args.max_lineage_depth,
        max_lineage_tickets: args.max_lineage_tickets,
        started_at,
        interval_secs: interval.as_secs(),
        rpc_timeout_secs: positive_duration(&args.rpc_timeout, "--rpc-timeout")?.as_secs(),
        sample_timeout_secs: positive_duration(&args.sample_timeout, "--sample-timeout")?.as_secs(),
        planned_duration_secs: duration.map(|value| value.as_secs()),
        thresholds: Thresholds {
            stale_after_secs: parse_duration(&args.stale_after)?.as_secs(),
            max_landing_age_secs: parse_duration(&args.max_landing_age)?.as_secs(),
            max_ready_age_secs: parse_duration(&args.max_ready_age)?.as_secs(),
            max_cost_usd: args.max_cost_usd,
            max_unavailable_samples: args.max_unavailable_samples,
            max_reconcile_violations: 0,
            max_forced_landings: 0,
            max_duplicate_dispatches: 0,
            max_duplicate_landings: 0,
            max_unclassified_holds: 0,
        },
        observer_build: rk_core::version::BUILD_VERSION.to_string(),
    };
    write_new_json(&run_dir.join(MANIFEST), &manifest)?;
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(run_dir.join(SAMPLES))?;

    if !as_json {
        println!("observation {}", run_dir.display());
        println!(
            "record interventions: rk observe record {} --class <CLASS> --summary <TEXT>",
            run_dir.display()
        );
    }
    collect_run(layout, &run_dir, as_json).await
}

async fn collect_run(layout: &Layout, run_dir: &Path, as_json: bool) -> Result<()> {
    let manifest = load_manifest(run_dir)?;
    let mut log = ObservationLog::open(run_dir, &manifest)?;
    let interval = Duration::from_secs(manifest.interval_secs);
    let end = manifest
        .planned_duration_secs
        .map(|secs| {
            let duration = chrono::Duration::from_std(Duration::from_secs(secs))?;
            manifest
                .started_at
                .checked_add_signed(duration)
                .context("observation duration exceeds the clock range")
        })
        .transpose()?;
    let mut cadence = tokio::time::interval(interval);
    cadence.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    cadence.tick().await;
    loop {
        let sample = collect_sample(layout, &manifest, &mut log).await?;
        if as_json {
            println!("{}", serde_json::to_string(&sample)?);
        } else {
            println!(
                "sample {} · daemon {} · {} live · {} open · landing {} ({}s) · USD {:.4}",
                sample.sequence,
                if sample.daemon_reachable {
                    "up"
                } else {
                    "DOWN"
                },
                sample.metrics.live_agents,
                sample.metrics.open_tickets,
                sample.metrics.landing_depth,
                sample.metrics.oldest_landing_age_secs,
                sample.metrics.cost_usd,
            );
        }
        if end.is_some_and(|end| Utc::now() >= end) {
            break;
        }
        let interrupted = tokio::select! {
            _ = cadence.tick() => false,
            _ = async {
                if let Some(end) = end {
                    tokio::time::sleep(end.signed_duration_since(Utc::now()).to_std().unwrap_or_default()).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => true,
            result = tokio::signal::ctrl_c() => {
                result.context("install Ctrl-C handler")?;
                true
            }
        };
        if interrupted {
            break;
        }
    }
    let derived = derive_report(run_dir)?;
    write_json_atomic(&run_dir.join(REPORT), &derived)?;
    if as_json {
        println!("{}", serde_json::to_string(&derived)?);
    } else {
        print_report(&derived);
    }
    if !derived.passed {
        bail!(
            "observation thresholds failed; see {}",
            run_dir.join(REPORT).display()
        );
    }
    Ok(())
}

async fn append_sample(layout: &Layout, run_dir: &Path) -> Result<Sample> {
    let manifest = load_manifest(run_dir)?;
    let mut log = ObservationLog::open(run_dir, &manifest)?;
    collect_sample(layout, &manifest, &mut log).await
}

async fn collect_sample(
    layout: &Layout,
    manifest: &Manifest,
    log: &mut ObservationLog,
) -> Result<Sample> {
    let sequence = log.next_sequence();
    let after_id = log.event_cursor().map(str::to_string);
    let started = tokio::time::Instant::now();
    let mut reader = SampleReader::new(manifest)?;
    let mut sample = Sample {
        schema_version: SCHEMA_VERSION,
        sequence,
        observed_at: Utc::now(),
        daemon_reachable: false,
        errors: Vec::new(),
        status: None,
        king: None,
        work: None,
        reconcile: None,
        tickets: Vec::new(),
        lineage: BTreeMap::new(),
        agents: Vec::new(),
        event_cursor: None,
        events: Vec::new(),
        metrics: SampleMetrics::default(),
        sampling: SamplingEvidence::default(),
    };
    sample.sampling.gap_secs = log.gap(manifest.started_at, sample.observed_at);
    sample.sampling.recovered_appends = log.recoveries().to_vec();
    for path in &sample.sampling.recovered_appends {
        sample
            .errors
            .push(format!("interrupted append evidence: {path}"));
    }
    reader.connect(layout, &mut sample.errors).await;
    sample.daemon_reachable = reader.client.is_some();
    let mut client = reader;
    sample.status = call(&mut client, "status", json!({}), &mut sample.errors).await;
    sample.king = call(&mut client, "king.status", json!({}), &mut sample.errors)
        .await
        .map(compact_king);
    sample.work = call(
        &mut client,
        "work.current",
        json!({"repo": manifest.repo}),
        &mut sample.errors,
    )
    .await;
    sample.reconcile = call(
        &mut client,
        "reconcile.report",
        json!({"repo": manifest.repo}),
        &mut sample.errors,
    )
    .await;
    if let Some(value) = call(
        &mut client,
        "ticket.list",
        json!({"scope": manifest.repo}),
        &mut sample.errors,
    )
    .await
    {
        if value["truncated"] == true {
            sample
                .errors
                .push("ticket.list: truncated source; correction lineage may be incomplete".into());
        }
        (sample.tickets, sample.lineage) =
            select_tickets(values(&value, "tickets"), manifest, &mut sample.errors);
    }
    let selected_tasks: BTreeSet<String> = sample
        .tickets
        .iter()
        .flat_map(|ticket| [ticket["identity"].as_str(), ticket["alias"].as_str()])
        .flatten()
        .map(str::to_string)
        .chain(manifest.tickets.iter().cloned())
        .collect();
    if let Some(value) = call(
        &mut client,
        "agent.list",
        json!({"include_archived": true}),
        &mut sample.errors,
    )
    .await
    {
        sample.agents = values(&value, "agents")
            .into_iter()
            .filter(|agent| {
                agent["repo_name"].as_str() == Some(manifest.repo.as_str())
                    && (manifest.tickets.is_empty()
                        || agent["task"].as_str().is_some_and(|task| {
                            manifest.tickets.iter().any(|wanted| wanted == task)
                                || selected_tasks.contains(task)
                        }))
                    && (matches!(agent["state"].as_str(), Some("spawning" | "running"))
                        || parse_time(&agent["created_at"])
                            .is_some_and(|at| at >= manifest.started_at)
                        || parse_time(&agent["updated_at"])
                            .is_some_and(|at| at >= manifest.started_at))
            })
            .map(|agent| {
                let mut agent = compact_agent(agent);
                if let Some(task) = agent["task"].as_str() {
                    if let Some(ticket) = sample
                        .tickets
                        .iter()
                        .find(|ticket| ticket["alias"].as_str() == Some(task))
                    {
                        agent["observed_task"] = json!(task);
                        agent["task"] = ticket["identity"].clone();
                    }
                }
                agent
            })
            .collect();
    }
    let mut event_params =
        json!({"category": "event", "scope": manifest.repo, "newest": after_id.is_none()});
    if let Some(after_id) = &after_id {
        event_params["after_id"] = json!(after_id);
    }
    if let Some(value) = call(&mut client, "space.scan", event_params, &mut sample.errors).await {
        if value["truncated"] == true {
            sample.errors.push(
                "space.scan: truncated event page; further history remains unobserved".into(),
            );
        }
        sample.events = values(&value, "tuples");
        sample.event_cursor = sample
            .events
            .iter()
            .filter_map(|event| event["id"].as_str())
            .max()
            .map(str::to_string)
            .or(after_id);
        sample.events.retain(|event| {
            DateTime::parse_from_rfc3339(event["created_at"].as_str().unwrap_or(""))
                .map(|at| at.with_timezone(&Utc) >= manifest.started_at)
                .unwrap_or(false)
        });
        sample
            .events
            .sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    }
    sample.metrics = derive_metrics_with_ready_age(&sample, manifest, |ticket| {
        log.ready_age(ticket, sample.observed_at)
    });
    sample.sampling.elapsed_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
    sample.sampling.rpc_timeouts = client.timeouts;
    sample.sampling.deadline_exceeded = client.deadline_exceeded;
    log.append(&sample)?;
    Ok(sample)
}

fn default_rpc_timeout() -> u64 {
    5
}
fn default_sample_timeout() -> u64 {
    20
}

struct SampleReader {
    client: Option<Client>,
    deadline: tokio::time::Instant,
    rpc_timeout: Duration,
    timeouts: Vec<String>,
    deadline_exceeded: bool,
}

impl SampleReader {
    fn new(manifest: &Manifest) -> Result<Self> {
        Ok(Self {
            client: None,
            deadline: tokio::time::Instant::now()
                .checked_add(Duration::from_secs(manifest.sample_timeout_secs))
                .context("sample deadline exceeds the clock range")?,
            rpc_timeout: Duration::from_secs(manifest.rpc_timeout_secs),
            timeouts: Vec::new(),
            deadline_exceeded: false,
        })
    }
    async fn connect(&mut self, layout: &Layout, errors: &mut Vec<String>) {
        match tokio::time::timeout_at(self.next_deadline(), Client::connect(layout)).await {
            Ok(Ok(client)) => self.client = Some(client),
            Ok(Err(error)) => errors.push(format!("connect: {error}")),
            Err(_) => self.timed_out("connect", errors),
        }
    }
    fn next_deadline(&self) -> tokio::time::Instant {
        tokio::time::Instant::now()
            .checked_add(self.rpc_timeout)
            .map_or(self.deadline, |deadline| deadline.min(self.deadline))
    }
    fn timed_out(&mut self, method: &str, errors: &mut Vec<String>) {
        self.deadline_exceeded |= tokio::time::Instant::now() >= self.deadline;
        self.timeouts.push(method.into());
        errors.push(format!(
            "{method}: {} deadline exceeded; remaining reads skipped",
            if self.deadline_exceeded {
                "sample"
            } else {
                "RPC"
            }
        ));
        // A late response must never be mistaken for the next method's reply.
        self.client = None;
    }
}

async fn call(
    reader: &mut SampleReader,
    method: &str,
    params: Value,
    errors: &mut Vec<String>,
) -> Option<Value> {
    if reader.client.is_some() && tokio::time::Instant::now() >= reader.deadline {
        reader.timed_out(method, errors);
        return None;
    }
    let deadline = reader.next_deadline();
    let client = reader.client.as_mut()?;
    match tokio::time::timeout_at(deadline, client.call(method, params)).await {
        Ok(Ok(value)) => Some(value),
        Ok(Err(error)) => {
            errors.push(format!("{method}: {error}"));
            None
        }
        Err(_) => {
            reader.timed_out(method, errors);
            None
        }
    }
}

fn derive_metrics_with_ready_age(
    sample: &Sample,
    manifest: &Manifest,
    ready_age: impl Fn(&str) -> u64,
) -> SampleMetrics {
    let mut live_tasks: BTreeSet<String> = sample
        .agents
        .iter()
        .filter(|agent| matches!(agent["state"].as_str(), Some("spawning" | "running")))
        .filter_map(|agent| agent["task"].as_str().map(str::to_string))
        .collect();
    for task in live_tasks.clone() {
        let mut current = task.as_str();
        let mut visited = BTreeSet::new();
        while let Some(parent) = sample.lineage.get(current) {
            if !visited.insert(parent) {
                break;
            }
            live_tasks.insert(parent.clone());
            current = parent;
        }
    }
    let ready = ready_ticket_ids(sample);
    let selected_ready: BTreeSet<String> = sample
        .tickets
        .iter()
        .filter_map(|ticket| ticket["identity"].as_str())
        .filter(|identity| ready.contains(*identity))
        .map(str::to_string)
        .collect();
    let mut metrics = SampleMetrics {
        live_agents: sample
            .agents
            .iter()
            .filter(|agent| matches!(agent["state"].as_str(), Some("spawning" | "running")))
            .count() as u64,
        cost_usd: sample
            .agents
            .iter()
            .filter_map(|agent| agent["cost_usd"].as_f64())
            .sum(),
        tokens: sample.agents.iter().map(agent_tokens).sum(),
        ..Default::default()
    };
    for ticket in &sample.tickets {
        if ticket_is_nonterminal(ticket) {
            metrics.open_tickets += 1;
            let identity = ticket["identity"].as_str().unwrap_or("");
            let status = ticket["payload"]["status"].as_str().unwrap_or("open");
            let stale_candidate = matches!(status, "claimed" | "in_progress" | "blocked")
                && !live_tasks.contains(identity)
                && !ready.contains(identity);
            let age = parse_time(&ticket["payload"]["updated_at"])
                .or_else(|| parse_time(&ticket["created_at"]))
                .and_then(|at| sample.observed_at.signed_duration_since(at).to_std().ok())
                .map_or(0, |age| age.as_secs());
            if stale_candidate && age > manifest.thresholds.stale_after_secs {
                metrics.stale_tickets += 1;
            }
        }
        if !ticket["payload"]["delivery"].is_null() {
            metrics.delivered_tickets += 1;
        }
    }
    if let Some(status) = &sample.status {
        let queues: Vec<_> = status["landing_queue"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|queue| queue["repo"].as_str() == Some(manifest.repo.as_str()))
            .collect();
        metrics.landing_depth = queues.iter().filter_map(|q| q["depth"].as_u64()).sum();
        metrics.oldest_landing_age_secs = queues
            .iter()
            .filter_map(|q| {
                q["oldest_age_secs"]
                    .as_u64()
                    .or_else(|| q["oldest_age_secs"].as_i64().map(|age| age.max(0) as u64))
            })
            .max()
            .unwrap_or(0);
    }
    metrics.reconcile_violations = sample
        .reconcile
        .as_ref()
        .and_then(|value| value["violations"].as_array())
        .map_or(0, |rows| rows.len() as u64);
    if let Some(work) = &sample.work {
        metrics.actionable = count_rows(work, "actionable");
        metrics.decision_required = count_rows(work, "decision_required");
        metrics.stalled = count_rows(work, "stalled");
        metrics.unclassified_holds = ["actionable", "decision_required", "stalled"]
            .into_iter()
            .flat_map(|field| work[field].as_array().into_iter().flatten())
            .filter(|row| row["kind"].as_str().is_none_or(str::is_empty))
            .count() as u64;
        metrics.oldest_ready_age_secs = selected_ready
            .iter()
            .map(|ticket| ready_age(ticket))
            .max()
            .unwrap_or(0);
    }
    let mut live_by_task: HashMap<&str, u64> = HashMap::new();
    for agent in sample
        .agents
        .iter()
        .filter(|agent| matches!(agent["state"].as_str(), Some("spawning" | "running")))
    {
        if let Some(task) = agent["task"].as_str() {
            *live_by_task.entry(task).or_default() += 1;
        }
    }
    metrics.duplicate_dispatches = live_by_task.values().filter(|&&count| count > 1).count() as u64;
    metrics
}

fn ready_ticket_ids(sample: &Sample) -> BTreeSet<String> {
    sample
        .work
        .as_ref()
        .and_then(|work| work["ready_tickets"].as_array())
        .into_iter()
        .flatten()
        .filter_map(|row| row["id"].as_str().map(str::to_string))
        .collect()
}

#[cfg(test)]
fn derive_sample_metrics(sample: &Sample, manifest: &Manifest, prior: &[Sample]) -> SampleMetrics {
    derive_metrics_with_ready_age(sample, manifest, |ticket| {
        continuous_ready_age_secs(sample, prior, ticket)
    })
}

#[cfg(test)]
fn continuous_ready_age_secs(sample: &Sample, prior: &[Sample], ticket: &str) -> u64 {
    let mut ready_since = sample.observed_at;
    for previous in prior.iter().rev() {
        if ready_ticket_ids(previous).contains(ticket) {
            ready_since = previous.observed_at;
        } else {
            break;
        }
    }
    sample
        .observed_at
        .signed_duration_since(ready_since)
        .to_std()
        .map_or(0, |age| age.as_secs())
}

fn record(args: RecordArgs, as_json: bool) -> Result<()> {
    let manifest = load_manifest(&args.run)?;
    if let Some(ticket) = &args.ticket {
        if !manifest.tickets.is_empty()
            && !manifest.tickets.contains(ticket)
            && !load_samples(&args.run)?.iter().any(|sample| {
                sample.tickets.iter().any(|row| {
                    row["identity"].as_str() == Some(ticket)
                        || row["alias"].as_str() == Some(ticket)
                })
            })
        {
            bail!("ticket {ticket} is outside observation run {}", manifest.id);
        }
    }
    let intervention = Intervention {
        schema_version: SCHEMA_VERSION,
        id: RecordId::new().to_string(),
        observed_at: Utc::now(),
        class: args.class,
        summary: nonempty(args.summary, "--summary")?,
        ticket: args.ticket,
        actor: args.actor.unwrap_or_else(|| "operator".into()),
        evidence: args.evidence,
    };
    let path = args
        .run
        .join(INTERVENTIONS)
        .join(format!("{}.json", intervention.id));
    write_new_json(&path, &intervention)?;
    if as_json {
        println!("{}", serde_json::to_string(&intervention)?);
    } else {
        println!("recorded {} ({:?})", intervention.id, intervention.class);
    }
    Ok(())
}

fn report(args: ReportArgs, as_json: bool) -> Result<()> {
    let report = derive_report(&args.run)?;
    if args.finalize {
        write_json_atomic(&args.run.join(REPORT), &report)?;
    }
    if as_json {
        println!("{}", serde_json::to_string(&report)?);
    } else {
        print_report(&report);
    }
    if !report.passed {
        bail!("observation thresholds failed");
    }
    Ok(())
}

fn derive_report(run_dir: &Path) -> Result<Report> {
    let manifest = load_manifest(run_dir)?;
    let samples = load_samples(run_dir)?;
    if samples.is_empty() {
        bail!("{} contains no samples", run_dir.display());
    }
    let interventions = load_interventions(run_dir)?;
    let ended_at = samples.last().expect("nonempty").observed_at;
    let elapsed_secs = ended_at
        .signed_duration_since(manifest.started_at)
        .to_std()
        .map_or(0, |elapsed| elapsed.as_secs());
    let unavailable_samples = samples
        .iter()
        .filter(|sample| !sample.daemon_reachable)
        .count() as u64;
    let max_sample_gap_secs = sample_gaps(&samples, manifest.started_at)
        .into_iter()
        .max()
        .unwrap_or(0);
    let partial_samples = samples
        .iter()
        .filter(|sample| sample.daemon_reachable && !sample.errors.is_empty())
        .count() as u64;
    let build_mismatch_samples = samples
        .iter()
        .filter_map(|sample| sample.status.as_ref())
        .filter(|status| status["build_version"].as_str() != Some(manifest.observer_build.as_str()))
        .count() as u64;
    let daemon_restarts = transitions(
        samples
            .iter()
            .filter_map(|sample| sample.status.as_ref()?.get("pid")?.as_u64()),
    );
    let king_replacements = transitions(samples.iter().filter_map(king_generation));
    let delivered = latest_tickets(&samples)
        .into_values()
        .filter(|ticket| delivery_in_window(ticket, manifest.started_at, ended_at))
        .collect::<Vec<_>>();
    let delivered_during_run = delivered
        .iter()
        .filter(|ticket| manifest.tickets.is_empty() || selected_root(ticket, &manifest.tickets))
        .count() as u64;
    let correction_deliveries = delivered.len() as u64 - delivered_during_run;
    let (attributed_cost_usd, attributed_tokens) = attributed_usage(&samples, manifest.started_at);
    let max_landing_depth = max_metric(&samples, |m| m.landing_depth);
    let max_landing_age_secs = max_metric(&samples, |m| m.oldest_landing_age_secs);
    let max_ready_age_secs = max_metric(&samples, |m| m.oldest_ready_age_secs);
    let max_reconcile_violations = max_metric(&samples, |m| m.reconcile_violations);
    let max_stale_tickets = max_metric(&samples, |m| m.stale_tickets);
    let max_unclassified_holds = max_metric(&samples, |m| m.unclassified_holds);
    let duplicate_dispatches = max_metric(&samples, |m| m.duplicate_dispatches)
        .max(overlapping_dispatches(&samples, ended_at));
    let events = unique_events(&samples);
    let forced_landings = events
        .values()
        .filter(|event| event["identity"] == "forced_landing")
        .count() as u64;
    let duplicate_landings = duplicate_landings(events.values().copied());
    let mut intervention_counts = BTreeMap::new();
    for intervention in &interventions {
        let class = match intervention.class {
            InterventionClass::Mechanical => "mechanical",
            InterventionClass::Llm => "llm",
            InterventionClass::HumanGate => "human-gate",
            InterventionClass::AdHoc => "ad-hoc",
        };
        *intervention_counts.entry(class.into()).or_insert(0) += 1;
    }
    let mut checks = BTreeMap::new();
    let recovery_dir = run_dir.join("recovery");
    let recovered_appends = if recovery_dir.exists() {
        fs::read_dir(recovery_dir)?
            .collect::<std::io::Result<Vec<_>>>()?
            .iter()
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "partial")
            })
            .count() as u64
    } else {
        0
    };
    check(&mut checks, "interrupted-appends", recovered_appends, 0);
    check(
        &mut checks,
        "daemon-availability",
        unavailable_samples,
        manifest.thresholds.max_unavailable_samples,
    );
    check(&mut checks, "partial-samples", partial_samples, 0);
    check(
        &mut checks,
        "sample-cadence-secs",
        max_sample_gap_secs,
        manifest.interval_secs.saturating_mul(2),
    );
    if let Some(planned) = manifest.planned_duration_secs {
        check(
            &mut checks,
            "coverage-shortfall-secs",
            planned.saturating_sub(elapsed_secs.saturating_add(manifest.interval_secs)),
            0,
        );
    }
    check(&mut checks, "build-parity", build_mismatch_samples, 0);
    check(
        &mut checks,
        "landing-queue-age-secs",
        max_landing_age_secs,
        manifest.thresholds.max_landing_age_secs,
    );
    check(
        &mut checks,
        "ready-queue-age-secs",
        max_ready_age_secs,
        manifest.thresholds.max_ready_age_secs,
    );
    check(
        &mut checks,
        "reconcile-violations",
        max_reconcile_violations,
        manifest.thresholds.max_reconcile_violations,
    );
    check(
        &mut checks,
        "forced-landings",
        forced_landings,
        manifest.thresholds.max_forced_landings,
    );
    check(
        &mut checks,
        "duplicate-dispatches",
        duplicate_dispatches,
        manifest.thresholds.max_duplicate_dispatches,
    );
    check(
        &mut checks,
        "duplicate-landings",
        duplicate_landings,
        manifest.thresholds.max_duplicate_landings,
    );
    check(&mut checks, "stale-tickets", max_stale_tickets, 0);
    check(
        &mut checks,
        "unclassified-holds",
        max_unclassified_holds,
        manifest.thresholds.max_unclassified_holds,
    );
    if let Some(limit) = manifest.thresholds.max_cost_usd {
        checks.insert(
            "attributed-cost-usd".into(),
            Check {
                observed: json!(attributed_cost_usd),
                limit: json!(limit),
                passed: attributed_cost_usd <= limit,
            },
        );
    }
    let passed = checks.values().all(|check| check.passed);
    let mut evidence = BTreeMap::new();
    evidence.insert(
        "manifest".into(),
        run_dir.join(MANIFEST).display().to_string(),
    );
    evidence.insert(
        "samples".into(),
        run_dir.join(SAMPLES).display().to_string(),
    );
    evidence.insert(
        "interventions".into(),
        run_dir.join(INTERVENTIONS).display().to_string(),
    );
    Ok(Report {
        schema_version: SCHEMA_VERSION,
        run_id: manifest.id,
        name: manifest.name,
        repo: manifest.repo,
        started_at: manifest.started_at,
        ended_at,
        elapsed_secs,
        samples: samples.len(),
        max_sample_gap_secs,
        unavailable_samples,
        partial_samples,
        recovered_appends,
        build_mismatch_samples,
        daemon_restarts,
        king_replacements,
        delivered_during_run,
        correction_deliveries,
        throughput_per_hour: if elapsed_secs == 0 {
            0.0
        } else {
            delivered_during_run as f64 * 3600.0 / elapsed_secs as f64
        },
        attributed_cost_usd,
        attributed_tokens,
        max_landing_depth,
        max_landing_age_secs,
        max_ready_age_secs,
        max_reconcile_violations,
        forced_landings,
        duplicate_dispatches,
        duplicate_landings,
        max_stale_tickets,
        max_unclassified_holds,
        interventions: intervention_counts,
        checks,
        passed,
        evidence,
    })
}

fn check(checks: &mut BTreeMap<String, Check>, name: &str, observed: u64, limit: u64) {
    checks.insert(
        name.into(),
        Check {
            observed: json!(observed),
            limit: json!(limit),
            passed: observed <= limit,
        },
    );
}

fn latest_tickets(samples: &[Sample]) -> BTreeMap<String, Value> {
    let mut tickets = BTreeMap::new();
    for ticket in samples.iter().flat_map(|sample| &sample.tickets) {
        if let Some(id) = ticket["identity"].as_str() {
            tickets.insert(id.to_string(), ticket.clone());
        }
    }
    tickets
}

fn delivery_in_window(ticket: &Value, start: DateTime<Utc>, end: DateTime<Utc>) -> bool {
    ticket["payload"]["delivery"]["landed_at"]
        .as_str()
        .and_then(|at| DateTime::parse_from_rfc3339(at).ok())
        .map(|at| {
            let at = at.with_timezone(&Utc);
            at >= start && at <= end
        })
        .unwrap_or(false)
}

fn attributed_usage(samples: &[Sample], started_at: DateTime<Utc>) -> (f64, u64) {
    let mut by_spawn: HashMap<String, (DateTime<Utc>, f64, f64, u64, u64)> = HashMap::new();
    for agent in samples.iter().flat_map(|sample| &sample.agents) {
        let key = agent["spawn"]
            .as_str()
            .or_else(|| agent["name"].as_str())
            .unwrap_or("unknown")
            .to_string();
        let created = parse_time(&agent["created_at"]).unwrap_or(started_at);
        let cost = agent["cost_usd"].as_f64().unwrap_or(0.0);
        let tokens = agent_tokens(agent);
        by_spawn
            .entry(key)
            .and_modify(|row| {
                row.1 = row.1.min(cost);
                row.2 = row.2.max(cost);
                row.3 = row.3.min(tokens);
                row.4 = row.4.max(tokens);
            })
            .or_insert((created, cost, cost, tokens, tokens));
    }
    by_spawn.values().fold((0.0, 0), |(cost, tokens), row| {
        let baseline_cost = if row.0 >= started_at { 0.0 } else { row.1 };
        let baseline_tokens = if row.0 >= started_at { 0 } else { row.3 };
        (
            cost + (row.2 - baseline_cost).max(0.0),
            tokens + row.4.saturating_sub(baseline_tokens),
        )
    })
}

fn unique_events(samples: &[Sample]) -> BTreeMap<String, &Value> {
    let mut events = BTreeMap::new();
    for event in samples.iter().flat_map(|sample| &sample.events) {
        if let Some(id) = event["id"].as_str() {
            events.insert(id.to_string(), event);
        }
    }
    events
}

fn duplicate_landings<'a>(events: impl Iterator<Item = &'a Value>) -> u64 {
    let mut by_task: HashMap<&str, BTreeSet<(&str, &str)>> = HashMap::new();
    for event in events.filter(|event| {
        event["identity"] == "landing_processed" && event["payload"]["outcome"] == "landed"
    }) {
        let Some(task) = event["payload"]["task"].as_str() else {
            continue;
        };
        let head = event["payload"]["head_sha"].as_str().unwrap_or("");
        let target = event["payload"]["target"].as_str().unwrap_or("");
        by_task.entry(task).or_default().insert((head, target));
    }
    by_task
        .values()
        .filter(|landings| landings.len() > 1)
        .count() as u64
}

fn overlapping_dispatches(samples: &[Sample], ended_at: DateTime<Utc>) -> u64 {
    type Interval = (DateTime<Utc>, DateTime<Utc>);
    let mut latest: HashMap<&str, &Value> = HashMap::new();
    for agent in samples.iter().flat_map(|sample| &sample.agents) {
        if let Some(spawn) = agent["spawn"].as_str() {
            latest.insert(spawn, agent);
        }
    }
    let mut by_task: HashMap<&str, Vec<Interval>> = HashMap::new();
    for agent in latest.values() {
        let (Some(task), Some(start)) = (agent["task"].as_str(), parse_time(&agent["created_at"]))
        else {
            continue;
        };
        let end = if matches!(agent["state"].as_str(), Some("spawning" | "running")) {
            ended_at
        } else {
            parse_time(&agent["updated_at"]).unwrap_or(ended_at)
        };
        by_task.entry(task).or_default().push((start, end));
    }
    by_task
        .values()
        .filter(|intervals| {
            intervals.iter().enumerate().any(|(index, left)| {
                intervals
                    .iter()
                    .skip(index + 1)
                    .any(|right| left.0 < right.1 && right.0 < left.1)
            })
        })
        .count() as u64
}

fn transitions<T: PartialEq>(values: impl Iterator<Item = T>) -> u64 {
    let mut prior = None;
    let mut count = 0;
    for value in values {
        if prior.as_ref().is_some_and(|prior| prior != &value) {
            count += 1;
        }
        prior = Some(value);
    }
    count
}

fn sample_gaps(samples: &[Sample], started_at: DateTime<Utc>) -> Vec<u64> {
    let mut prior = started_at;
    samples
        .iter()
        .map(|sample| {
            let gap = sample
                .observed_at
                .signed_duration_since(prior)
                .to_std()
                .map_or(0, |duration| duration.as_secs());
            prior = sample.observed_at;
            gap
        })
        .collect()
}

fn king_generation(sample: &Sample) -> Option<String> {
    let identity = &sample.king.as_ref()?["state"]["registration"]["identity"];
    identity["session_id"]
        .as_str()
        .or_else(|| identity["pane_id"].as_str())
        .map(str::to_string)
}

fn max_metric(samples: &[Sample], value: impl Fn(&SampleMetrics) -> u64) -> u64 {
    samples
        .iter()
        .map(|sample| value(&sample.metrics))
        .max()
        .unwrap_or(0)
}

fn agent_tokens(agent: &Value) -> u64 {
    let usage = &agent["usage"];
    ["input", "output", "cache_read", "cache_creation"]
        .into_iter()
        .map(|field| usage[field].as_u64().unwrap_or(0))
        .sum::<u64>()
        + [
            "input_tokens",
            "output_tokens",
            "cache_read_input_tokens",
            "cache_creation_input_tokens",
        ]
        .into_iter()
        .map(|field| usage[field].as_u64().unwrap_or(0))
        .sum::<u64>()
}

fn count_rows(value: &Value, field: &str) -> u64 {
    value[field].as_array().map_or(0, |rows| rows.len() as u64)
}

fn values(value: &Value, field: &str) -> Vec<Value> {
    value[field].as_array().cloned().unwrap_or_default()
}

fn compact_king(value: Value) -> Value {
    json!({
        "state": {
            "registration": value["state"]["registration"],
            "active_wake": value["state"]["active_wake"],
        }
    })
}

fn compact_ticket(ticket: Value) -> Value {
    json!({
        "identity": ticket["identity"],
        "alias": ticket["alias"],
        "scope": ticket["scope"],
        "created_at": ticket["created_at"],
        "payload": {
            "status": ticket["payload"]["status"],
            "updated_at": ticket["payload"]["updated_at"],
            "title": ticket["payload"]["title"],
            "priority": ticket["payload"]["priority"],
            "created_by": ticket["payload"]["created_by"],
            "coalesce_key": ticket["payload"]["coalesce_key"],
            "parent": ticket["payload"]["parent"],
            "delivery": ticket["payload"]["delivery"],
        }
    })
}

fn default_lineage_depth() -> usize {
    8
}
fn default_lineage_tickets() -> usize {
    256
}

fn selected_root(ticket: &Value, roots: &[String]) -> bool {
    [ticket["identity"].as_str(), ticket["alias"].as_str()]
        .into_iter()
        .flatten()
        .any(|id| roots.iter().any(|root| root == id))
}

fn correction_parent<'a>(ticket: &'a Value, repo: &str) -> Option<&'a str> {
    if ticket["scope"].as_str() != Some(repo) || ticket["payload"]["created_by"] != "daemon" {
        return None;
    }
    let key = ticket["payload"]["coalesce_key"].as_str()?;
    let tail = ["landing-rework", "landing-conflict-rework"]
        .into_iter()
        .find_map(|kind| key.strip_prefix(&format!("{kind}:{repo}:")))?;
    let fields = tail.split(':').collect::<Vec<_>>();
    (fields.len() == 4 && fields.iter().all(|part| !part.is_empty())).then(|| fields[3])
}

fn select_tickets(
    all: Vec<Value>,
    manifest: &Manifest,
    errors: &mut Vec<String>,
) -> (Vec<Value>, BTreeMap<String, String>) {
    let all = all
        .into_iter()
        .filter(|ticket| ticket["scope"].as_str() == Some(manifest.repo.as_str()))
        .collect::<Vec<_>>();
    let aliases: BTreeMap<_, _> = all
        .iter()
        .filter_map(|ticket| Some((ticket["alias"].as_str()?, ticket["identity"].as_str()?)))
        .collect();
    let canonical = |id: &str| aliases.get(id).copied().unwrap_or(id).to_string();
    let parents: BTreeMap<String, String> = all
        .iter()
        .filter_map(|ticket| {
            Some((
                ticket["identity"].as_str()?.to_string(),
                canonical(correction_parent(ticket, &manifest.repo)?),
            ))
        })
        .collect();
    let mut selected: BTreeSet<_> = all
        .iter()
        .filter(|ticket| {
            if manifest.tickets.is_empty() {
                ticket_is_nonterminal(ticket)
                    || parse_time(&ticket["payload"]["delivery"]["landed_at"])
                        .is_some_and(|at| at >= manifest.started_at)
            } else {
                selected_root(ticket, &manifest.tickets)
            }
        })
        .filter_map(|ticket| ticket["identity"].as_str().map(str::to_string))
        .collect();
    if !manifest.tickets.is_empty() {
        let roots = selected.len();
        let mut limited = false;
        for _ in 0..manifest.max_lineage_depth {
            let children = parents
                .iter()
                .filter(|(child, parent)| !selected.contains(*child) && selected.contains(*parent))
                .map(|(child, _)| child.clone())
                .collect::<Vec<_>>();
            if children.is_empty() {
                break;
            }
            for child in children {
                if selected.len().saturating_sub(roots) >= manifest.max_lineage_tickets {
                    limited = true;
                    break;
                }
                selected.insert(child);
            }
            if limited {
                break;
            }
        }
        limited |= parents
            .iter()
            .any(|(child, parent)| !selected.contains(child) && selected.contains(parent));
        if limited {
            errors.push(
                "ticket lineage reached its frozen depth/count bound; cohort is incomplete".into(),
            );
        }
    }
    let lineage = parents
        .into_iter()
        .filter(|(child, parent)| selected.contains(child) && selected.contains(parent))
        .collect();
    let tickets = all
        .into_iter()
        .filter(|ticket| {
            ticket["identity"]
                .as_str()
                .is_some_and(|id| selected.contains(id))
        })
        .map(compact_ticket)
        .collect();
    (tickets, lineage)
}

fn ticket_is_nonterminal(ticket: &Value) -> bool {
    !matches!(
        ticket["payload"]["status"].as_str(),
        Some("done" | "closed")
    )
}

fn compact_agent(agent: Value) -> Value {
    json!({
        "spawn": agent["spawn"],
        "name": agent["name"],
        "repo_name": agent["repo_name"],
        "task": agent["task"],
        "state": agent["state"],
        "created_at": agent["created_at"],
        "updated_at": agent["updated_at"],
        "archived_at": agent["archived_at"],
        "cost_usd": agent["cost_usd"],
        "usage": agent["usage"],
        "model": agent["model"],
        "harness": agent["harness"],
        "liveness": agent["liveness"],
        "workflow_instance": agent["workflow_instance"],
    })
}

fn parse_time(value: &Value) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value.as_str()?)
        .ok()
        .map(|at| at.with_timezone(&Utc))
}

fn load_manifest(run_dir: &Path) -> Result<Manifest> {
    let path = run_dir.join(MANIFEST);
    let manifest: Manifest = serde_json::from_reader(
        File::open(&path).with_context(|| format!("open {}", path.display()))?,
    )?;
    if manifest.schema_version != SCHEMA_VERSION {
        bail!("unsupported observation schema {}", manifest.schema_version);
    }
    if manifest.interval_secs == 0
        || manifest.rpc_timeout_secs == 0
        || manifest.sample_timeout_secs == 0
    {
        bail!("observation interval and deadlines must be greater than zero");
    }
    Ok(manifest)
}

fn load_samples(run_dir: &Path) -> Result<Vec<Sample>> {
    let path = run_dir.join(SAMPLES);
    let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
    BufReader::new(file)
        .lines()
        .enumerate()
        .filter_map(|(line_no, line)| match line {
            Ok(line) if line.trim().is_empty() => None,
            other => Some((line_no, other)),
        })
        .map(|(line_no, line)| {
            serde_json::from_str(&line?)
                .with_context(|| format!("parse {} line {}", path.display(), line_no + 1))
        })
        .collect()
}

fn load_interventions(run_dir: &Path) -> Result<Vec<Intervention>> {
    let dir = run_dir.join(INTERVENTIONS);
    let mut paths = fs::read_dir(&dir)
        .with_context(|| format!("read {}", dir.display()))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.sort();
    paths
        .into_iter()
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
        .map(|path| {
            serde_json::from_reader(File::open(&path)?)
                .with_context(|| format!("parse {}", path.display()))
        })
        .collect()
}

#[cfg(test)]
fn append_json_line(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut file = OpenOptions::new().append(true).open(path)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_data()?;
    Ok(())
}

fn write_new_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let temp = path.with_extension(format!("tmp-{}", RecordId::new()));
    write_new_json(&temp, value)?;
    fs::rename(&temp, path)?;
    Ok(())
}

fn default_root(layout: &Layout) -> PathBuf {
    layout
        .home()
        .parent()
        .unwrap_or(layout.home())
        .join(".rat-kingdom-observations")
}

fn positive_duration(value: &str, flag: &str) -> Result<Duration> {
    let duration = parse_duration(value)?;
    if duration.is_zero() {
        bail!("{flag} must be greater than zero");
    }
    Ok(duration)
}

fn parse_duration(value: &str) -> Result<Duration> {
    let value = value.trim();
    if value.is_empty() {
        bail!("duration cannot be empty");
    }
    let (number, multiplier) = match value.chars().last().expect("nonempty") {
        's' => (&value[..value.len() - 1], 1),
        'm' => (&value[..value.len() - 1], 60),
        'h' => (&value[..value.len() - 1], 3600),
        'd' => (&value[..value.len() - 1], 86400),
        _ => (value, 1),
    };
    let number: u64 = number
        .parse()
        .with_context(|| format!("invalid duration: {value}"))?;
    Ok(Duration::from_secs(number.saturating_mul(multiplier)))
}

fn nonempty(value: String, flag: &str) -> Result<String> {
    if value.trim().is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(value)
}

fn print_value(value: &Value, as_json: bool) -> Result<()> {
    if as_json {
        println!("{value}");
    } else {
        println!("{}", serde_json::to_string_pretty(value)?);
    }
    Ok(())
}

fn print_report(report: &Report) {
    println!(
        "{} · {} · {} samples · {} · {:.2} deliveries/h · USD {:.4}",
        report.name,
        report.repo,
        report.samples,
        if report.passed { "PASS" } else { "FAIL" },
        report.throughput_per_hour,
        report.attributed_cost_usd,
    );
    for (name, check) in &report.checks {
        println!(
            "  {:<28} {:<4} observed {} <= {}",
            name,
            if check.passed { "PASS" } else { "FAIL" },
            check.observed,
            check.limit,
        );
    }
    println!("  interventions {:?}", report.interventions);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn fixture_daemon(
        layout: &Layout,
        tickets: Vec<Value>,
        agents: Vec<Value>,
    ) -> tokio::task::JoinHandle<Vec<String>> {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
        layout.ensure().unwrap();
        let listener = tokio::net::UnixListener::bind(layout.socket_path()).unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = tokio::io::BufReader::new(stream);
            let mut methods = Vec::new();
            loop {
                let mut line = String::new();
                if stream.read_line(&mut line).await.unwrap() == 0 {
                    break;
                }
                let request: Value = serde_json::from_str(&line).unwrap();
                let method = request["method"].as_str().unwrap();
                methods.push(method.to_string());
                let value = match method {
                    "status" => json!({"pid": 1, "build_version": "test", "landing_queue": []}),
                    "king.status" => json!({"state": {}}),
                    "work.current" => {
                        json!({"ready_tickets": [], "actionable": [], "decision_required": [], "stalled": []})
                    }
                    "reconcile.report" => json!({"violations": []}),
                    "ticket.list" => json!({"tickets": tickets}),
                    "agent.list" => json!({"agents": agents}),
                    "space.scan" => json!({"tuples": [], "truncated": false}),
                    _ => panic!("unexpected observation RPC {method}"),
                };
                let response = json!({"id": request["id"], "result": value,
                    "server_version": rk_core::version::BUILD_VERSION});
                stream
                    .get_mut()
                    .write_all(format!("{response}\n").as_bytes())
                    .await
                    .unwrap();
            }
            methods
        })
    }

    #[tokio::test]
    async fn selected_lineage_attributes_correction_usage_liveness_and_parent_staleness() {
        let (dir, manifest) = fixture();
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        let ticket = |id: &str, key: Option<&str>| {
            json!({
                "identity": id, "scope": "repo", "created_at": manifest.started_at,
                "payload": {"created_by": "daemon", "status": "in_progress",
                    "updated_at": manifest.started_at, "coalesce_key": key}
            })
        };
        let rows = vec![
            ticket("TKT-1", None),
            ticket(
                "TKT-child",
                Some("landing-rework:repo:feature:head:main:TKT-1"),
            ),
            ticket(
                "TKT-grandchild",
                Some("landing-conflict-rework:repo:correction:head:feature:TKT-child"),
            ),
            ticket(
                "TKT-unrelated",
                Some("landing-rework:repo:other:head:main:TKT-other"),
            ),
        ];
        let agent = |id: &str, cost: f64| {
            json!({"spawn": id, "name": id, "task": id,
            "repo_name": "repo", "state": "running", "created_at": manifest.started_at,
            "updated_at": manifest.started_at, "cost_usd": cost, "usage": {"output": 20}})
        };
        let daemon = fixture_daemon(
            &layout,
            rows,
            vec![agent("TKT-grandchild", 3.0), agent("TKT-unrelated", 100.0)],
        )
        .await;
        let collected = append_sample(&layout, dir.path()).await.unwrap();
        assert_eq!(collected.tickets.len(), 3);
        assert_eq!(collected.metrics.live_agents, 1);
        assert_eq!(collected.metrics.cost_usd, 3.0);
        assert_eq!(collected.metrics.tokens, 20);
        assert_eq!(
            collected.metrics.stale_tickets, 0,
            "a live correction descendant owns progress for its held ancestors"
        );
        let report = derive_report(dir.path()).unwrap();
        assert_eq!(report.attributed_cost_usd, 3.0);
        assert_eq!(report.attributed_tokens, 20);
        assert_eq!(daemon.await.unwrap().len(), 7);
    }

    #[tokio::test]
    async fn a_stalled_rpc_finishes_a_partial_sample_with_deadline_evidence() {
        use tokio::io::AsyncBufReadExt;
        let (dir, manifest) = fixture();
        let mut value = serde_json::to_value(manifest).unwrap();
        value["rpc_timeout_secs"] = json!(1);
        value["sample_timeout_secs"] = json!(2);
        fs::write(dir.path().join(MANIFEST), value.to_string()).unwrap();
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        layout.ensure().unwrap();
        let listener = tokio::net::UnixListener::bind(layout.socket_path()).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = tokio::io::BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            std::future::pending::<()>().await;
        });
        let result =
            tokio::time::timeout(Duration::from_secs(3), append_sample(&layout, dir.path())).await;
        server.abort();
        let collected = result
            .expect("a stalled daemon must not block observation indefinitely")
            .unwrap();
        assert!(
            collected
                .errors
                .iter()
                .any(|error| error.contains("deadline")),
            "{:?}",
            collected.errors
        );
        assert_eq!(load_samples(dir.path()).unwrap().len(), 1);
        assert!(!derive_report(dir.path()).unwrap().passed);
    }

    #[test]
    fn observation_checkpoint_replays_only_the_uncheckpointed_tail() {
        let (dir, manifest) = fixture();
        let mut first = sample(1, "2026-09-02T00:00:30Z");
        first.work = Some(json!({"ready_tickets": [{"id": "TKT-1"}]}));
        first.event_cursor = Some("cursor-1".into());
        append_json_line(&dir.path().join(SAMPLES), &first).unwrap();
        let log = ObservationLog::open(dir.path(), &manifest).unwrap();
        assert_eq!(log.replayed_samples, 1);
        assert!(
            ObservationLog::open(dir.path(), &manifest).is_err(),
            "one run has exactly one collector owner"
        );
        drop(log);
        let log = ObservationLog::open(dir.path(), &manifest).unwrap();
        assert_eq!(
            log.replayed_samples, 0,
            "normal sampling must not reread history"
        );
        drop(log);

        let mut second = sample(2, "2026-09-02T00:01:00Z");
        second.work = first.work.clone();
        second.event_cursor = Some("cursor-2".into());
        // Simulate a crash after the durable append, before its checkpoint.
        append_json_line(&dir.path().join(SAMPLES), &second).unwrap();
        let log = ObservationLog::open(dir.path(), &manifest).unwrap();
        assert_eq!(log.replayed_samples, 1);
        assert_eq!(log.next_sequence(), 3);
        assert_eq!(log.event_cursor(), Some("cursor-2"));
        assert_eq!(
            log.ready_age("TKT-1", "2026-09-02T00:01:30Z".parse().unwrap()),
            60
        );
        drop(log);
        let report = serde_json::to_value(derive_report(dir.path()).unwrap()).unwrap();
        fs::write(dir.path().join("collector.json"), "damaged cache").unwrap();
        let rebuilt = ObservationLog::open(dir.path(), &manifest).unwrap();
        assert_eq!(rebuilt.replayed_samples, 2);
        assert_eq!(rebuilt.next_sequence(), 3);
        assert_eq!(
            rebuilt.ready_age("TKT-1", "2026-09-02T00:01:30Z".parse().unwrap()),
            60
        );
        assert_eq!(
            serde_json::to_value(derive_report(dir.path()).unwrap()).unwrap(),
            report,
            "report replay is independent of the collector checkpoint"
        );
    }

    #[tokio::test]
    async fn sample_deadline_bounds_a_sequence_of_individually_timely_rpcs() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
        let (dir, mut manifest) = fixture();
        manifest.rpc_timeout_secs = 2;
        manifest.sample_timeout_secs = 1;
        write_json_atomic(&dir.path().join(MANIFEST), &manifest).unwrap();
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        layout.ensure().unwrap();
        let listener = tokio::net::UnixListener::bind(layout.socket_path()).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = tokio::io::BufReader::new(stream);
            loop {
                let mut line = String::new();
                if stream.read_line(&mut line).await.unwrap() == 0 {
                    break;
                }
                let request: Value = serde_json::from_str(&line).unwrap();
                tokio::time::sleep(Duration::from_millis(600)).await;
                let response = json!({"id": request["id"], "result": {},
                    "server_version": rk_core::version::BUILD_VERSION});
                if stream
                    .get_mut()
                    .write_all(format!("{response}\n").as_bytes())
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        let collected =
            tokio::time::timeout(Duration::from_secs(3), append_sample(&layout, dir.path()))
                .await
                .unwrap()
                .unwrap();
        server.abort();
        assert!(collected.status.is_some());
        assert!(collected.king.is_none());
        assert!(collected.sampling.deadline_exceeded);
        assert_eq!(collected.sampling.rpc_timeouts, ["king.status"]);
        assert!(collected.sampling.elapsed_ms < 2000);
        assert!(!derive_report(dir.path()).unwrap().passed);
    }

    #[test]
    fn correction_deliveries_do_not_inflate_root_throughput() {
        let (dir, manifest) = fixture();
        let mut observed = sample(1, "2026-09-02T00:01:00Z");
        observed.tickets = ["TKT-1", "TKT-correction"]
            .into_iter()
            .map(|id| {
                json!({"identity": id, "scope": manifest.repo, "payload": {
                "status": "done", "delivery": {"landed_at": observed.observed_at,
                    "merge_commit": "commit", "target": "main"}}})
            })
            .collect();
        observed
            .lineage
            .insert("TKT-correction".into(), "TKT-1".into());
        observed.status = Some(json!({"landing_queue": [
            {"repo": "repo", "depth": 1, "oldest_age_secs": 10},
            {"repo": "unrelated", "depth": 90, "oldest_age_secs": 9000}
        ]}));
        observed.metrics = derive_sample_metrics(&observed, &manifest, &[]);
        assert_eq!(observed.metrics.landing_depth, 1);
        assert_eq!(observed.metrics.oldest_landing_age_secs, 10);
        append_json_line(&dir.path().join(SAMPLES), &observed).unwrap();
        let report = derive_report(dir.path()).unwrap();
        assert_eq!(report.delivered_during_run, 1);
        assert_eq!(report.correction_deliveries, 1);
        record(
            RecordArgs {
                run: dir.path().into(),
                class: InterventionClass::Mechanical,
                summary: "correction recovered".into(),
                ticket: Some("TKT-correction".into()),
                actor: None,
                evidence: vec![],
            },
            false,
        )
        .unwrap();
    }

    #[tokio::test]
    async fn an_interrupted_append_is_preserved_and_resume_records_the_gap() {
        let (dir, manifest) = fixture();
        let mut log = ObservationLog::open(dir.path(), &manifest).unwrap();
        log.append(&sample(1, "2026-09-02T00:00:30Z")).unwrap();
        drop(log);
        let committed = fs::read(dir.path().join(SAMPLES)).unwrap();
        let fragment = b"{\"schema_version\":1,\"sequence\":2,";
        OpenOptions::new()
            .append(true)
            .open(dir.path().join(SAMPLES))
            .unwrap()
            .write_all(fragment)
            .unwrap();
        let manifest_bytes = fs::read(dir.path().join(MANIFEST)).unwrap();
        let home = dir.path().join("absent-daemon");
        let recovered = append_sample(&Layout::at(&home), dir.path()).await.unwrap();
        assert_eq!(recovered.sequence, 2);
        assert!(recovered.sampling.gap_secs > manifest.interval_secs * 2);
        assert_eq!(recovered.sampling.recovered_appends.len(), 1);
        let evidence = dir.path().join(&recovered.sampling.recovered_appends[0]);
        assert_eq!(fs::read(evidence).unwrap(), fragment);
        assert!(fs::read(dir.path().join(SAMPLES))
            .unwrap()
            .starts_with(&committed));
        assert_eq!(load_samples(dir.path()).unwrap().len(), 2);
        assert_eq!(fs::read(dir.path().join(MANIFEST)).unwrap(), manifest_bytes);
        let report = derive_report(dir.path()).unwrap();
        assert_eq!(report.recovered_appends, 1);
        assert!(!report.checks["interrupted-appends"].passed);
        assert!(!report.passed);
        assert!(
            !home.exists(),
            "observing a missing daemon must not initialize it"
        );
    }

    #[test]
    fn lineage_bounds_and_provenance_prevent_unrelated_attribution() {
        let (_, mut manifest) = fixture();
        let row = |id: &str, key: Option<&str>| {
            json!({
                "identity": id, "scope": "repo", "payload": {"status": "open", "created_by": "daemon", "coalesce_key": key}
            })
        };
        let mut root = row("TKT-1", None);
        root["alias"] = json!("root-alias");
        let child = row(
            "TKT-child",
            Some("landing-rework:repo:branch:sha:main:root-alias"),
        );
        let grandchild = row(
            "TKT-grandchild",
            Some("landing-conflict-rework:repo:child:sha:branch:TKT-child"),
        );
        let mut unrelated = row("TKT-manual", None);
        unrelated["payload"]["parent"] = json!("TKT-1");
        let foreign = row(
            "TKT-foreign",
            Some("landing-rework:another-repo:branch:sha:main:TKT-1"),
        );
        let mut errors = Vec::new();
        manifest.max_lineage_depth = 1;
        let (tickets, lineage) = select_tickets(
            vec![
                root.clone(),
                child.clone(),
                grandchild.clone(),
                unrelated.clone(),
                foreign.clone(),
            ],
            &manifest,
            &mut errors,
        );
        assert_eq!(tickets.len(), 2);
        assert_eq!(lineage["TKT-child"], "TKT-1");
        assert_eq!(
            errors.len(),
            1,
            "a bounded-out correction must not look fully observed"
        );
        errors.clear();
        manifest.max_lineage_depth = 8;
        manifest.max_lineage_tickets = 1;
        let (tickets, _) = select_tickets(
            vec![root.clone(), child.clone(), grandchild.clone()],
            &manifest,
            &mut errors,
        );
        assert_eq!(tickets.len(), 2);
        assert_eq!(errors.len(), 1);
        errors.clear();
        manifest.max_lineage_tickets = 256;
        let (tickets, _) = select_tickets(
            vec![root, child, grandchild, unrelated, foreign],
            &manifest,
            &mut errors,
        );
        assert_eq!(tickets.len(), 3);
        assert!(errors.is_empty());
    }

    fn fixture() -> (TempDir, Manifest) {
        let dir = TempDir::new().unwrap();
        fs::create_dir(dir.path().join(INTERVENTIONS)).unwrap();
        let manifest = Manifest {
            schema_version: SCHEMA_VERSION,
            id: "run-1".into(),
            name: "release-soak".into(),
            repo: "repo".into(),
            tickets: vec!["TKT-1".into()],
            max_lineage_depth: default_lineage_depth(),
            max_lineage_tickets: default_lineage_tickets(),
            started_at: "2026-09-02T00:00:00Z".parse().unwrap(),
            interval_secs: 30,
            rpc_timeout_secs: default_rpc_timeout(),
            sample_timeout_secs: default_sample_timeout(),
            planned_duration_secs: None,
            thresholds: Thresholds {
                stale_after_secs: 900,
                max_landing_age_secs: 600,
                max_ready_age_secs: 900,
                max_cost_usd: Some(2.0),
                max_unavailable_samples: 0,
                max_reconcile_violations: 0,
                max_forced_landings: 0,
                max_duplicate_dispatches: 0,
                max_duplicate_landings: 0,
                max_unclassified_holds: 0,
            },
            observer_build: "test".into(),
        };
        write_new_json(&dir.path().join(MANIFEST), &manifest).unwrap();
        File::create(dir.path().join(SAMPLES)).unwrap();
        (dir, manifest)
    }

    fn sample(sequence: u64, at: &str) -> Sample {
        Sample {
            schema_version: SCHEMA_VERSION,
            sequence,
            observed_at: at.parse().unwrap(),
            daemon_reachable: true,
            errors: vec![],
            status: Some(json!({"pid": 1, "build_version": "test", "landing_queue": []})),
            king: None,
            work: Some(json!({"actionable": [], "decision_required": [], "stalled": []})),
            reconcile: Some(json!({"violations": []})),
            tickets: vec![],
            lineage: BTreeMap::new(),
            agents: vec![],
            event_cursor: None,
            events: vec![],
            metrics: SampleMetrics::default(),
            sampling: SamplingEvidence::default(),
        }
    }

    #[test]
    fn outage_is_evidence_and_fails_closed() {
        let (dir, _) = fixture();
        let mut down = sample(1, "2026-09-02T00:00:30Z");
        down.daemon_reachable = false;
        down.status = None;
        append_json_line(&dir.path().join(SAMPLES), &down).unwrap();
        let report = derive_report(dir.path()).unwrap();
        assert_eq!(report.unavailable_samples, 1);
        assert!(!report.checks["daemon-availability"].passed);
        assert!(!report.passed);
    }

    #[test]
    fn daemon_build_mismatch_fails_before_a_run_can_pass() {
        let (dir, _) = fixture();
        let mut value = sample(1, "2026-09-02T00:00:30Z");
        value.status = Some(json!({"pid": 1, "build_version": "old", "landing_queue": []}));
        append_json_line(&dir.path().join(SAMPLES), &value).unwrap();
        let report = derive_report(dir.path()).unwrap();
        assert_eq!(report.build_mismatch_samples, 1);
        assert!(!report.checks["build-parity"].passed);
        assert!(!report.passed);
    }

    #[test]
    fn transient_queue_age_is_retained_by_maximum() {
        let (dir, _) = fixture();
        let first = sample(1, "2026-09-02T00:00:30Z");
        let mut second = sample(2, "2026-09-02T00:01:00Z");
        second.metrics.oldest_landing_age_secs = 700;
        let third = sample(3, "2026-09-02T00:01:30Z");
        for value in [first, second, third] {
            append_json_line(&dir.path().join(SAMPLES), &value).unwrap();
        }
        let report = derive_report(dir.path()).unwrap();
        assert_eq!(report.max_landing_age_secs, 700);
        assert!(!report.checks["landing-queue-age-secs"].passed);
    }

    #[test]
    fn newly_observed_ready_ticket_does_not_inherit_preflight_creation_age() {
        let (_, manifest) = fixture();
        let mut value = sample(1, "2026-09-02T00:00:30Z");
        value.work = Some(json!({
            "ready_tickets": [{"id": "TKT-1"}],
            "actionable": [],
            "decision_required": [],
            "stalled": [],
        }));
        value.tickets = vec![json!({
            "identity": "TKT-1",
            "created_at": "2026-09-01T00:00:00Z",
            "payload": {
                "status": "open",
                "updated_at": "2026-09-01T00:00:00Z",
                "delivery": null,
            },
        })];

        let metrics = derive_sample_metrics(&value, &manifest, &[]);
        assert_eq!(metrics.oldest_ready_age_secs, 0);
    }

    #[test]
    fn ready_age_tracks_only_the_current_observed_ready_streak() {
        let (_, manifest) = fixture();
        let mut first = sample(1, "2026-09-02T00:00:00Z");
        first.work = Some(json!({
            "ready_tickets": [{"id": "TKT-1"}],
            "actionable": [], "decision_required": [], "stalled": [],
        }));
        let mut second = sample(2, "2026-09-02T00:05:00Z");
        second.work = Some(json!({
            "ready_tickets": [{"id": "TKT-1"}],
            "actionable": [], "decision_required": [], "stalled": [],
        }));
        let mut current = sample(3, "2026-09-02T00:10:00Z");
        current.work = Some(json!({
            "ready_tickets": [{"id": "TKT-1"}],
            "actionable": [], "decision_required": [], "stalled": [],
        }));
        current.tickets = vec![json!({
            "identity": "TKT-1",
            "created_at": "2026-09-01T00:00:00Z",
            "payload": {"status": "open", "delivery": null},
        })];

        let metrics = derive_sample_metrics(&current, &manifest, &[first, second]);
        assert_eq!(metrics.oldest_ready_age_secs, 600);

        let mut not_ready = sample(4, "2026-09-02T00:11:00Z");
        not_ready.work = Some(json!({
            "ready_tickets": [],
            "actionable": [], "decision_required": [], "stalled": [],
        }));
        let mut ready_again = sample(5, "2026-09-02T00:12:00Z");
        ready_again.work = current.work.clone();
        ready_again.tickets = current.tickets.clone();
        let metrics = derive_sample_metrics(&ready_again, &manifest, &[current, not_ready]);
        assert_eq!(metrics.oldest_ready_age_secs, 0);
    }

    #[test]
    fn ready_age_ignores_repo_work_outside_the_selected_ticket_set() {
        let (_, manifest) = fixture();
        let mut previous = sample(1, "2026-09-02T00:00:00Z");
        previous.work = Some(json!({
            "ready_tickets": [{"id": "TKT-OTHER"}],
            "actionable": [], "decision_required": [], "stalled": [],
        }));
        let mut current = sample(2, "2026-09-02T01:00:00Z");
        current.work = previous.work.clone();
        current.tickets = vec![json!({
            "identity": "TKT-1",
            "created_at": "2026-09-01T00:00:00Z",
            "payload": {"status": "open", "delivery": null},
        })];

        let metrics = derive_sample_metrics(&current, &manifest, &[previous]);
        assert_eq!(metrics.oldest_ready_age_secs, 0);
    }

    #[test]
    fn dependency_blocked_open_ticket_is_not_stale() {
        let (_, manifest) = fixture();
        let mut value = sample(1, "2026-09-02T01:00:00Z");
        value.work = Some(json!({
            "ready_tickets": [],
            "actionable": [], "decision_required": [], "stalled": [],
        }));
        value.tickets = vec![json!({
            "identity": "TKT-1",
            "created_at": "2026-09-01T00:00:00Z",
            "payload": {
                "status": "open",
                "updated_at": "2026-09-01T00:00:00Z",
                "delivery": null,
            },
        })];

        let metrics = derive_sample_metrics(&value, &manifest, &[]);
        assert_eq!(metrics.stale_tickets, 0);
    }

    #[test]
    fn ownerless_active_status_uses_last_ticket_update_for_staleness() {
        let (_, manifest) = fixture();
        let mut value = sample(1, "2026-09-02T01:00:00Z");
        value.work = Some(json!({
            "ready_tickets": [],
            "actionable": [], "decision_required": [], "stalled": [],
        }));
        value.tickets = vec![json!({
            "identity": "TKT-1",
            "created_at": "2026-09-01T00:00:00Z",
            "payload": {
                "status": "in_progress",
                "updated_at": "2026-09-02T00:50:01Z",
                "delivery": null,
            },
        })];
        assert_eq!(
            derive_sample_metrics(&value, &manifest, &[]).stale_tickets,
            0
        );

        value.tickets[0]["payload"]["updated_at"] = json!("2026-09-02T00:40:00Z");
        assert_eq!(
            derive_sample_metrics(&value, &manifest, &[]).stale_tickets,
            1
        );

        value.agents = vec![json!({"task": "TKT-1", "state": "running"})];
        assert_eq!(
            derive_sample_metrics(&value, &manifest, &[]).stale_tickets,
            0
        );
    }

    #[test]
    fn dead_observer_gap_cannot_produce_a_passing_report() {
        let (dir, _) = fixture();
        let first = sample(1, "2026-09-02T00:00:30Z");
        let second = sample(2, "2026-09-02T00:05:00Z");
        for value in [first, second] {
            append_json_line(&dir.path().join(SAMPLES), &value).unwrap();
        }
        let report = derive_report(dir.path()).unwrap();
        assert_eq!(report.max_sample_gap_secs, 270);
        assert!(!report.checks["sample-cadence-secs"].passed);
        assert!(!report.passed);
    }

    #[test]
    fn repository_spend_is_a_run_delta_not_a_live_snapshot() {
        let (dir, _) = fixture();
        let mut first = sample(1, "2026-09-02T00:00:30Z");
        first.agents = vec![
            json!({"spawn":"S1", "created_at":"2026-09-01T00:00:00Z", "cost_usd":10.0, "usage":{"input_tokens":100}}),
        ];
        let mut second = sample(2, "2026-09-02T00:01:00Z");
        second.agents = vec![
            json!({"spawn":"S1", "created_at":"2026-09-01T00:00:00Z", "cost_usd":10.5, "usage":{"input_tokens":150}}),
            json!({"spawn":"S2", "created_at":"2026-09-02T00:00:45Z", "cost_usd":0.25, "usage":{"input_tokens":20}}),
        ];
        for value in [first, second] {
            append_json_line(&dir.path().join(SAMPLES), &value).unwrap();
        }
        let report = derive_report(dir.path()).unwrap();
        assert!((report.attributed_cost_usd - 0.75).abs() < 0.0001);
        assert_eq!(report.attributed_tokens, 70);
    }

    #[test]
    fn duplicate_landed_side_effects_are_grouped_by_task() {
        let events = [
            json!({"identity":"landing_processed", "payload":{"task":"TKT-1", "head_sha":"a", "target":"main", "outcome":"landed"}}),
            json!({"identity":"landing_processed", "payload":{"task":"TKT-1", "head_sha":"b", "target":"main", "outcome":"landed"}}),
            json!({"identity":"landing_processed", "payload":{"task":"TKT-2", "head_sha":"c", "target":"main", "outcome":"landed"}}),
        ];
        assert_eq!(duplicate_landings(events.iter()), 1);
    }

    #[test]
    fn overlapping_short_lived_dispatches_are_detected_between_samples() {
        let mut value = sample(1, "2026-09-02T00:01:00Z");
        value.agents = vec![
            json!({"spawn":"S1", "task":"TKT-1", "state":"completed", "created_at":"2026-09-02T00:00:10Z", "updated_at":"2026-09-02T00:00:40Z"}),
            json!({"spawn":"S2", "task":"TKT-1", "state":"completed", "created_at":"2026-09-02T00:00:30Z", "updated_at":"2026-09-02T00:00:50Z"}),
        ];
        assert_eq!(
            overlapping_dispatches(&[value], "2026-09-02T00:01:00Z".parse().unwrap()),
            1
        );
    }

    #[test]
    fn intervention_class_is_structural_and_file_is_atomic() {
        let (dir, _) = fixture();
        record(
            RecordArgs {
                run: dir.path().into(),
                class: InterventionClass::HumanGate,
                summary: "approved protected-path change".into(),
                ticket: Some("TKT-1".into()),
                actor: None,
                evidence: vec!["event:1".into()],
            },
            true,
        )
        .unwrap();
        let rows = load_interventions(dir.path()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].class, InterventionClass::HumanGate);
    }

    #[test]
    fn duration_parser_supports_observation_windows() {
        assert_eq!(parse_duration("30s").unwrap().as_secs(), 30);
        assert_eq!(parse_duration("15m").unwrap().as_secs(), 900);
        assert_eq!(parse_duration("2h").unwrap().as_secs(), 7200);
        assert_eq!(parse_duration("1d").unwrap().as_secs(), 86400);
    }
}

//! External, repository-scoped observation runs. The collector is deliberately
//! separate from the daemon and never auto-starts or repairs what it observes.

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use clap::{Args, Subcommand, ValueEnum};
use rk_core::action::canonical_digest;
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
const PROGRESS_EVALUATOR_VERSION: u32 = 3;
/// Bumped whenever `derive_report`'s evaluation semantics change in a way a
/// consumer must know about even though the on-disk `Report` shape only grew
/// additively (new fields are `#[serde(default)]`, never a breaking rename or
/// removal). Version 2 is the coverage-aware evaluator: an absence-based
/// check (forced/duplicate landings) or a merged-across-samples count
/// (deliveries) no longer reads as a proven zero/pass when its required
/// source never finished refreshing before the run ended. Version 3 folds
/// `attributed_cost_usd`/`attributed_tokens` per spawn by each generation's
/// LAST sampled reading instead of the historical MAX, and gains
/// `usage_coverage`: a generation still live (not harness-terminal) at its
/// last sample has not received its own final reconciliation, so summing
/// per-spawn peaks across the run could — and, in the trial evidence this
/// version fixes, did — permanently keep a pre-reconciliation ledger spike
/// the harness itself later corrected down at a turn boundary.
const REPORT_EVALUATOR_VERSION: u32 = 3;
const MANIFEST: &str = "manifest.json";
const SAMPLES: &str = "samples.jsonl";
const INTERVENTIONS: &str = "interventions";
const REPORT: &str = "report.json";
const CONTRACT_SCHEMA_VERSION: u32 = 1;
/// Bumped alongside `REPORT_EVALUATOR_VERSION`: qualification now requires
/// complete ticket/event coverage before an absence-based resource check or
/// a workload delivery count can pass — see `workload/delivery-coverage` and
/// the coverage guards on `resources/forced-landings` and
/// `resources/duplicate-landings`. Version 4 adds the same guard to
/// `resources/spend-usd`: it can no longer pass while `usage_coverage` is
/// `Incomplete`, since a still-live generation's folded cost is a
/// provisional ledger reading, not a settled total.
const QUALIFICATION_EVALUATOR_VERSION: u32 = 4;
const CONTRACT: &str = "contract.json";
const EXERCISES: &str = "exercises";
const QUALIFICATION: &str = "qualification.json";
const DEFAULT_MIN_CONTINUATION_SAMPLES: u64 = 1;
/// Rows requested per `space.scan` event page. Small enough that even a
/// heavier-than-average event payload keeps a full page well under the
/// daemon's response frame cap; halved further (see [`call_event_page`]) if a
/// page still comes back `frame_too_large`.
const EVENT_PAGE_LIMIT: usize = 500;
/// Hard ceiling on bounded event pages walked within one `collect_sample`
/// call. The per-RPC/sample deadlines are the primary bound; this keeps a
/// pathologically large backlog from looping for the entire deadline budget
/// on event history alone, starving nothing but leaving the rest for a later
/// sample.
const MAX_EVENT_PAGES_PER_SAMPLE: u32 = 20;

#[derive(Subcommand)]
pub enum ObservationCommand {
    /// Create a run, sample until its duration elapses or Ctrl-C, then report.
    Start(Box<StartArgs>),
    /// Append one read-only sample to an existing run.
    Sample(RunPathArgs),
    /// Resume collection using the original immutable scope and thresholds.
    Resume(RunPathArgs),
    /// Record one typed intervention as an atomic evidence file.
    Record(RecordArgs),
    /// Record one named continuity exercise as an atomic evidence file.
    Exercise(ExerciseArgs),
    /// Derive a report from the run's immutable evidence.
    Report(ReportArgs),
    /// Evaluate the run against its frozen acceptance contract.
    Qualify(QualifyArgs),
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
    /// Bound on a live generation's progress signature staying unchanged
    /// before it is independently counted as stalled, regardless of what the
    /// daemon's own supervisor sweep reports (D1).
    #[arg(long, default_value = "15m")]
    progress_stall_after: String,
    /// Grace period for a bounded wait (a self-declared verification/queue/
    /// gate phase) before it too counts as stalled. An expired wait cannot
    /// keep the run healthy.
    #[arg(long, default_value = "30m")]
    max_wait: String,
    #[arg(long)]
    max_cost_usd: Option<f64>,
    #[arg(long, default_value_t = 0)]
    max_unavailable_samples: u64,
    /// Repository-owned acceptance contract to freeze into this run.
    /// The frozen copy and its digest, not this path, are the run's authority.
    #[arg(long)]
    contract: Option<PathBuf>,
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
    /// Rat identity this declared wait covers, e.g. `agent["name"]`. Only a
    /// `human-gate` intervention carrying both this and `--spawn` can ever
    /// supply a bounded-wait exemption.
    #[arg(long)]
    owner: Option<String>,
    /// Exact live generation (`agent["spawn"]`) this declared wait covers.
    /// A replacement generation needs its own declaration.
    #[arg(long)]
    spawn: Option<String>,
}

#[derive(Args)]
pub struct ReportArgs {
    run: PathBuf,
    /// Persist the derived report as report.json as well as printing it.
    #[arg(long)]
    finalize: bool,
}

#[derive(Args)]
pub struct ExerciseArgs {
    run: PathBuf,
    #[arg(long, value_enum)]
    kind: ExerciseKind,
    /// Exact identity observed before the exercise (e.g. a daemon pid or King session id).
    #[arg(long)]
    before: String,
    /// Exact identity observed after the exercise. Equal to `--before` fails the exercise:
    /// a PID or King-identity change alone is not proof of continuation either way.
    #[arg(long)]
    after: String,
    #[arg(long)]
    ticket: Option<String>,
    #[arg(long, default_value = "")]
    note: String,
}

#[derive(Args)]
pub struct QualifyArgs {
    run: PathBuf,
    /// Persist the derived qualification result as qualification.json as well as printing it.
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ValueEnum, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum ExerciseKind {
    WorkerDeath,
    NamedCheckFailure,
    MergeConflict,
    DaemonRollover,
    KingReplacement,
    Custom,
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
    /// See [`StartArgs::progress_stall_after`]. Historical manifests predate
    /// this field; they fall back to the same 15m default the CLI freezes for
    /// a new run rather than failing to load.
    #[serde(default = "default_progress_stall_after_secs")]
    progress_stall_after_secs: u64,
    /// See [`StartArgs::max_wait`].
    #[serde(default = "default_max_wait_secs")]
    max_wait_secs: u64,
}

fn default_progress_stall_after_secs() -> u64 {
    15 * 60
}
fn default_max_wait_secs() -> u64 {
    30 * 60
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
    /// Tickets with a live (spawning/running) generation whose independently
    /// derived progress signature (D1) has not changed within the configured
    /// bound, and whose bounded-wait allowance (if any) has expired. Computed
    /// from raw per-generation evidence, never from the daemon's own stuck
    /// sweep or `stalled` bucket above — a failed supervisor sweep must not
    /// make this read healthy.
    #[serde(default)]
    progress_stalled_tickets: u64,
    /// Tickets with a live generation whose progress evidence is missing or
    /// ambiguous (e.g. no generation identity), so a reading cannot be made
    /// either way. Insufficient evidence is a visible coverage gap, not a
    /// silent pass.
    #[serde(default)]
    progress_unresolved_tickets: u64,
    /// Cumulative distinct stall episodes across all observed attempts,
    /// including tickets no longer live. Repair never erases an incident.
    #[serde(default)]
    progress_stall_episodes: u64,
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
    /// Exact declaration evidence visible when this sample was collected.
    /// Historical samples without this field never acquire later exemptions.
    #[serde(default)]
    declared_interventions: Vec<Intervention>,
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
    /// The `after_id` cursor value at which event collection stalled because
    /// the single next event exceeds the response frame cap on its own —
    /// pagination cannot shrink a one-row page any further. Explicit,
    /// persisted coverage evidence: the completeness frontier holds here
    /// rather than silently advancing past an event nothing ever read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    oversized_event_after: Option<String>,
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
    /// The rat identity (`agent["name"]`) a declared human-gate wait is
    /// bound to. Absent on historical records and on any intervention that
    /// is not a declared bounded wait; a missing owner never grants an
    /// exemption (see [`declared_gate_wait_since`]).
    #[serde(default)]
    owner: Option<String>,
    /// The exact live generation (`agent["spawn"]`) a declared human-gate
    /// wait is bound to. A generation replacement mints a new spawn, so a
    /// declaration recorded for a predecessor never carries over.
    #[serde(default)]
    spawn: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Check {
    observed: Value,
    limit: Value,
    passed: bool,
    /// Set only for a check whose `observed` value depends on a source that
    /// can go stale mid-run (ticket state, the event feed) or on a
    /// generation's own cost/usage ledger that has not yet reached a
    /// harness-terminal reconciliation. Absent for a check with no such
    /// dependency. See [`Coverage`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    coverage: Option<Coverage>,
}

/// Whether a merged-across-samples signal (repository ticket state, or the
/// event feed) was refreshed all the way through the end of the run.
/// `Complete` is the only state that licenses reading an absence in that
/// signal (e.g. zero deliveries, zero forced landings) as a proven fact
/// rather than an unproven lower bound — see [`CoverageStatus`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
enum Coverage {
    #[default]
    Complete,
    Incomplete,
}

/// Provenance for one such signal. `incomplete_samples` counts samples whose
/// read of this source did not finish (a cascaded RPC timeout skip, a hard
/// RPC error, or — for the event feed — an oversized-frame stall);
/// `uncovered_tail_secs`, set only when `coverage` is `Incomplete`, is the
/// wall-clock span between the last sample that fully refreshed this source
/// and the run's end. Because a ticket's delivery record is written once and
/// never retracted (closed is a terminal ticket state), a gap in the middle
/// of a run is self-healing as soon as one later sample re-reads it; only an
/// UNCLOSED tail at the end of the run can permanently hide a real change,
/// which is exactly what `uncovered_tail_secs` measures.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct CoverageStatus {
    coverage: Coverage,
    incomplete_samples: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    uncovered_tail_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Report {
    schema_version: u32,
    /// Version of `derive_report`'s evaluation semantics, independent of
    /// `schema_version` (which describes the manifest/sample format this run
    /// was collected under and never changes when only derivation logic
    /// does). See [`REPORT_EVALUATOR_VERSION`].
    #[serde(default)]
    evaluator_version: u32,
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
    /// Coverage for `delivered_during_run`/`correction_deliveries`/
    /// `throughput_per_hour`: whether `ticket.list` was successfully
    /// refreshed through the end of the run. When `Incomplete`, those three
    /// fields are a verified LOWER BOUND, not a proven count — a delivery
    /// that landed after the last successful read is invisible to this run.
    #[serde(default)]
    ticket_coverage: CoverageStatus,
    attributed_cost_usd: f64,
    attributed_tokens: u64,
    /// Coverage for `attributed_cost_usd`/`attributed_tokens`: `incomplete_samples`
    /// here counts distinct generations still live (not harness-terminal) at
    /// their last sample, and `uncovered_tail_secs` is the longest span since
    /// any such generation's last sample. Their folded cost/tokens is the
    /// harness's own last-known streaming ledger reading, not a reconciled
    /// final total — it can still move, in either direction, before that
    /// generation actually completes. See [`attributed_usage`].
    #[serde(default)]
    usage_coverage: CoverageStatus,
    max_landing_depth: u64,
    max_landing_age_secs: u64,
    max_ready_age_secs: u64,
    max_reconcile_violations: u64,
    forced_landings: u64,
    duplicate_dispatches: u64,
    duplicate_landings: u64,
    /// Coverage for `forced_landings`/`duplicate_landings`: whether the
    /// repository event feed was drained to the run's live tip by the end of
    /// the run. When `Incomplete`, a zero here is not proof no such event
    /// occurred — see [`Coverage`].
    #[serde(default)]
    event_coverage: CoverageStatus,
    max_stale_tickets: u64,
    max_unclassified_holds: u64,
    /// D1's independently derived stall count: live generations whose own
    /// progress evidence (not the daemon's stuck sweep) has not changed
    /// within the configured bound. Retained by maximum, so a stall that
    /// later resolves still fails the run (D1 point 4).
    #[serde(default)]
    max_progress_stalled_tickets: u64,
    /// Live generations whose progress evidence was missing or ambiguous in
    /// at least one sample — an explicit coverage gap, never folded into a
    /// passing result by omission.
    #[serde(default)]
    max_progress_unresolved_tickets: u64,
    /// Cumulative distinct progress-stall episodes observed for the cohort,
    /// including recurrence on the same ticket after an earlier resolution.
    #[serde(default)]
    progress_stall_episodes: u64,
    progress_episodes: Vec<ProgressEpisode>,
    interventions: BTreeMap<String, u64>,
    checks: BTreeMap<String, Check>,
    passed: bool,
    evidence: BTreeMap<String, String>,
}

/// A versioned, typed acceptance section frozen into a run. A repository-owned
/// input may propose one; the frozen copy and its digest, not the input file,
/// are the run's authority (D2, docs/2026-09-06-r1-qualification-deliverables.md).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AcceptanceContract {
    schema_version: u32,
    workload: WorkloadRequirement,
    duration: DurationRequirement,
    liveness: LivenessRequirement,
    #[serde(default)]
    exercises: Vec<ExerciseRequirement>,
    interventions: InterventionPolicy,
    build_identity: BuildIdentityRequirement,
    resources: ResourceRequirement,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkloadRequirement {
    /// Explicit root selection this contract governs; empty means the whole
    /// repository scope declared by the run's manifest.
    #[serde(default)]
    roots: Vec<String>,
    min_root_deliveries: u64,
    #[serde(default)]
    min_correction_deliveries: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurationRequirement {
    min_elapsed_secs: u64,
    max_sample_gap_secs: u64,
    #[serde(default)]
    coverage_tolerance_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LivenessRequirement {
    max_stale_tickets: u64,
    max_unclassified_holds: u64,
    /// Bounds `progress_stalled_tickets` onsets — D1's independently derived
    /// stall evidence, never the daemon's own stuck-sweep classification.
    max_stall_incidents: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExerciseRequirement {
    kind: ExerciseKind,
    #[serde(default = "default_min_count")]
    min_count: u64,
    #[serde(default)]
    min_continuation_samples: u64,
}

fn default_min_count() -> u64 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InterventionPolicy {
    /// Empty means every class is structurally allowed, subject to `max_ad_hoc`.
    #[serde(default)]
    allowed_classes: Vec<InterventionClass>,
    /// R1 qualification requires this to be zero.
    max_ad_hoc: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BuildIdentityRequirement {
    frozen_build: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ResourceRequirement {
    #[serde(default)]
    max_spend_usd: Option<f64>,
    max_landing_age_secs: u64,
    max_ready_age_secs: u64,
    max_duplicate_dispatches: u64,
    max_duplicate_landings: u64,
    max_forced_landings: u64,
    max_reconcile_violations: u64,
}

/// The run-directory-resident authority for a contract: the frozen copy plus
/// the digest that later replay revalidates against.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FrozenContract {
    contract: AcceptanceContract,
    digest: String,
    source: Option<String>,
    frozen_at: DateTime<Utc>,
}

/// A durable, typed record of one continuity exercise: an exact before/after
/// identity pair plus the moment it was declared to have occurred. A PID or
/// King-identity change alone is not proof of continuation; `derive_qualification`
/// additionally requires subsequent evidence of continuation without repeat.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Exercise {
    schema_version: u32,
    id: String,
    kind: ExerciseKind,
    occurred_at: DateTime<Utc>,
    before_identity: String,
    after_identity: String,
    ticket: Option<String>,
    note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QualificationCheck {
    requirement: String,
    passed: bool,
    detail: String,
}

/// The qualification decision for one run against its frozen contract. Distinct
/// from `Report`: a run without a contract can still produce a general `Report`,
/// but only a `QualificationResult` can claim an M3/M4 qualification outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct QualificationResult {
    schema_version: u32,
    run_id: String,
    contract_schema_version: u32,
    contract_digest: String,
    evaluator_version: u32,
    checks: Vec<QualificationCheck>,
    qualified: bool,
}

pub async fn run(layout: &Layout, command: ObservationCommand, as_json: bool) -> Result<()> {
    match command {
        ObservationCommand::Start(args) => start(layout, *args, as_json).await,
        ObservationCommand::Sample(args) => {
            let sample = append_sample(layout, &args.run).await?;
            print_value(&serde_json::to_value(sample)?, as_json)
        }
        ObservationCommand::Resume(args) => collect_run(layout, &args.run, as_json).await,
        ObservationCommand::Record(args) => record(args, as_json),
        ObservationCommand::Exercise(args) => exercise(args, as_json),
        ObservationCommand::Report(args) => report(args, as_json),
        ObservationCommand::Qualify(args) => qualify(args, as_json),
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
    fs::create_dir(run_dir.join(EXERCISES))?;
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
            progress_stall_after_secs: positive_duration(
                &args.progress_stall_after,
                "--progress-stall-after",
            )?
            .as_secs(),
            max_wait_secs: positive_duration(&args.max_wait, "--max-wait")?.as_secs(),
        },
        observer_build: rk_core::version::BUILD_VERSION.to_string(),
    };
    write_new_json(&run_dir.join(MANIFEST), &manifest)?;
    if let Some(path) = &args.contract {
        freeze_contract(&run_dir, path, &manifest, duration)?;
    }
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

/// Validate and freeze a repository-owned acceptance contract before measured
/// work starts. The frozen copy plus its digest become `contract.json`; there
/// is no update path, so a contract change requires a new run.
fn freeze_contract(
    run_dir: &Path,
    source: &Path,
    manifest: &Manifest,
    planned_duration: Option<Duration>,
) -> Result<()> {
    let raw = fs::read_to_string(source)
        .with_context(|| format!("read acceptance contract {}", source.display()))?;
    let contract: AcceptanceContract = serde_json::from_str(&raw)
        .with_context(|| format!("parse acceptance contract {}", source.display()))?;
    if contract.schema_version != CONTRACT_SCHEMA_VERSION {
        bail!(
            "unsupported acceptance contract schema {} (expected {})",
            contract.schema_version,
            CONTRACT_SCHEMA_VERSION
        );
    }
    if !contract.workload.roots.is_empty() {
        if manifest.tickets.is_empty() {
            bail!(
                "acceptance contract declares root tickets but the run has whole-repository scope"
            );
        }
        for root in &contract.workload.roots {
            if !manifest.tickets.contains(root) {
                bail!("acceptance contract root {root} is outside this run's observed tickets");
            }
        }
    }
    match planned_duration {
        Some(planned) if planned.as_secs() < contract.duration.min_elapsed_secs => bail!(
            "planned run duration {}s is shorter than the contract's minimum elapsed requirement {}s",
            planned.as_secs(),
            contract.duration.min_elapsed_secs
        ),
        None if contract.duration.min_elapsed_secs > 0 => bail!(
            "acceptance contract requires a minimum elapsed duration but the run has no planned --duration"
        ),
        _ => {}
    }
    let digest = canonical_digest(&contract)?;
    let frozen = FrozenContract {
        contract,
        digest,
        source: Some(source.display().to_string()),
        frozen_at: Utc::now(),
    };
    write_new_json(&run_dir.join(CONTRACT), &frozen)
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
        declared_interventions: Vec::new(),
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
    sample.status = call(&mut client, layout, "status", json!({}), &mut sample.errors).await;
    sample.king = call(
        &mut client,
        layout,
        "king.status",
        json!({}),
        &mut sample.errors,
    )
    .await
    .map(compact_king);
    sample.work = call(
        &mut client,
        layout,
        "work.current",
        json!({"repo": manifest.repo}),
        &mut sample.errors,
    )
    .await;
    sample.reconcile = call(
        &mut client,
        layout,
        "reconcile.report",
        json!({"repo": manifest.repo}),
        &mut sample.errors,
    )
    .await;
    if let Some(value) = call(
        &mut client,
        layout,
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
        layout,
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
    // Bound event collection: page oldest-first from an explicit cursor so no
    // one request can pull an unscoped backlog. A run with no checkpoint yet
    // starts at the run's OWN boundary (`RecordId::floor_at`), never an
    // unscoped `newest` scan of the whole category/scope history — that
    // unbounded bootstrap, repeated identically on every retry, was the
    // trial's `frame_too_large` failure mode (it can never recover, because
    // nothing about the request shrinks between attempts).
    let boundary = RecordId::floor_at(manifest.started_at).to_string();
    let mut cursor = after_id.clone().unwrap_or_else(|| boundary.clone());
    let mut cursor_confirmed = after_id.is_some();
    let mut page_limit = EVENT_PAGE_LIMIT;
    let mut pages = 0u32;
    loop {
        let event_params = json!({
            "category": "event",
            "scope": manifest.repo,
            "newest": false,
            "after_id": cursor,
            "limit": page_limit,
        });
        match call_event_page(&mut client, layout, event_params, &mut sample.errors).await {
            ScanPageOutcome::Page(value) => {
                cursor_confirmed = true;
                let truncated = value["truncated"] == true;
                let page = values(&value, "tuples");
                page_limit = EVENT_PAGE_LIMIT;
                if page.is_empty() {
                    break;
                }
                if let Some(max_id) = page.iter().filter_map(|event| event["id"].as_str()).max() {
                    cursor = max_id.to_string();
                }
                sample.events.extend(page);
                pages += 1;
                if !truncated {
                    break;
                }
                if pages >= MAX_EVENT_PAGES_PER_SAMPLE {
                    sample.errors.push(format!(
                        "space.scan: stopped after {MAX_EVENT_PAGES_PER_SAMPLE} bounded pages this sample; remaining history collects on a later sample"
                    ));
                    break;
                }
            }
            // A page shrinks as far as one row and still cannot fit: the next
            // event past `cursor` is oversized on its own. Hold the frontier
            // there — advancing it would silently move past unseen data —
            // and record which id it is stuck behind so the gap is explicit,
            // persisted coverage evidence rather than a vanished event.
            ScanPageOutcome::FrameTooLarge if page_limit > 1 => {
                page_limit = (page_limit / 2).max(1);
            }
            ScanPageOutcome::FrameTooLarge => {
                sample.sampling.oversized_event_after = Some(cursor.clone());
                sample.errors.push(format!(
                    "space.scan: the event immediately after {cursor} exceeds the response frame cap on its own; coverage frontier held, excluded pending manual recovery"
                ));
                break;
            }
            ScanPageOutcome::Stopped => break,
        }
    }
    sample.event_cursor = cursor_confirmed.then_some(cursor);
    sample.events.retain(|event| {
        DateTime::parse_from_rfc3339(event["created_at"].as_str().unwrap_or(""))
            .map(|at| at.with_timezone(&Utc) >= manifest.started_at)
            .unwrap_or(false)
    });
    sample
        .events
        .sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    log.capture_interventions(&mut sample)?;
    let progress = log.progress_metrics(&sample)?;
    sample.metrics = derive_metrics_with_ready_age(
        &sample,
        manifest,
        |ticket| log.ready_age(ticket, sample.observed_at),
        progress,
    );
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
        // A late response must never be mistaken for the next method's reply
        // on the SAME socket, so the connection itself is discarded here.
        // That is a per-RPC recovery decision, not a verdict on the rest of
        // the sample: `ensure_connected` opens a fresh one for whatever the
        // caller asks for next, as long as the overall sample deadline still
        // allows it. One slow or unavailable source must not silently
        // starve every read that was scheduled after it.
        self.client = None;
    }
    /// Guarantee a live connection before the next RPC, reconnecting after a
    /// prior timeout dropped one. A no-op when already connected. Returns
    /// whether a client is available to call.
    async fn ensure_connected(&mut self, layout: &Layout, errors: &mut Vec<String>) -> bool {
        if self.client.is_none() && tokio::time::Instant::now() < self.deadline {
            self.connect(layout, errors).await;
        }
        self.client.is_some()
    }
}

async fn call(
    reader: &mut SampleReader,
    layout: &Layout,
    method: &str,
    params: Value,
    errors: &mut Vec<String>,
) -> Option<Value> {
    if !reader.ensure_connected(layout, errors).await {
        return None;
    }
    if tokio::time::Instant::now() >= reader.deadline {
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

/// Outcome of one bounded `space.scan` event page request.
enum ScanPageOutcome {
    Page(Value),
    /// The daemon downgraded this page to `frame_too_large` (see
    /// `rk_daemon::proto::codes::FRAME_TOO_LARGE`): the requested page
    /// itself, even bounded, is still too big to fit one response frame.
    FrameTooLarge,
    /// Connect/timeout/other RPC failure; already recorded in `errors`.
    Stopped,
}

/// Like [`call`], but distinguishes a `frame_too_large` response so the
/// caller can shrink its page and retry instead of treating it like any
/// other RPC failure.
async fn call_event_page(
    reader: &mut SampleReader,
    layout: &Layout,
    params: Value,
    errors: &mut Vec<String>,
) -> ScanPageOutcome {
    if !reader.ensure_connected(layout, errors).await {
        return ScanPageOutcome::Stopped;
    }
    if tokio::time::Instant::now() >= reader.deadline {
        reader.timed_out("space.scan", errors);
        return ScanPageOutcome::Stopped;
    }
    let deadline = reader.next_deadline();
    let Some(client) = reader.client.as_mut() else {
        return ScanPageOutcome::Stopped;
    };
    match tokio::time::timeout_at(deadline, client.call("space.scan", params)).await {
        Ok(Ok(value)) => ScanPageOutcome::Page(value),
        Ok(Err(error)) => {
            let message = error.to_string();
            let frame_too_large = message.contains(rk_daemon::proto::codes::FRAME_TOO_LARGE);
            errors.push(format!("space.scan: {message}"));
            if frame_too_large {
                ScanPageOutcome::FrameTooLarge
            } else {
                ScanPageOutcome::Stopped
            }
        }
        Err(_) => {
            reader.timed_out("space.scan", errors);
            ScanPageOutcome::Stopped
        }
    }
}

/// Independent, deterministic outcome of comparing one live generation's
/// progress evidence against its own prior sample (D1). Never derived from
/// the daemon's own stuck sweep or `work.current` `stalled` bucket — those
/// are the alarm this evaluator must keep working without.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProgressReading {
    /// The signature changed, or this is a fresh generation with no clock to
    /// compare against yet.
    Progressing,
    /// A self-declared bounded wait (see [`progress_is_waiting`]) still
    /// within its allowance.
    Waiting,
    /// Unchanged signature past the configured bound, including an expired
    /// bounded-wait allowance. Independent of `AgentState`/`work.current`.
    Stalled,
    /// The generation is live but carries no usable generation identity, so
    /// no reading can be made. Insufficient evidence is a visible coverage
    /// gap, never a silent pass.
    Unresolved,
}

/// Per-attempt progress clocks are disposable cache state reconstructed from
/// immutable samples. Distinct concurrent generations never share a clock.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProgressState {
    ticket: String,
    spawn: Option<String>,
    session: Option<String>,
    attempt: Option<String>,
    signature: String,
    changed_at: DateTime<Utc>,
    last_observed: DateTime<Utc>,
    wait_started_at: Option<DateTime<Utc>>,
    stalled_since: Option<DateTime<Utc>>,
    episodes: u64,
    history: Vec<ProgressEpisode>,
    /// A productive checkpoint ends this generation's declared allowance.
    /// Re-reading or repeating its declaration cannot reactivate it.
    #[serde(default)]
    declared_wait_retired: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProgressEpisode {
    ticket: String,
    spawn: Option<String>,
    session: Option<String>,
    attempt: Option<String>,
    started_at: DateTime<Utc>,
    resolved_at: Option<DateTime<Utc>>,
    resolution: Option<String>,
}

#[derive(Default)]
struct ProgressMetrics {
    stalled: u64,
    unresolved: u64,
    episodes: u64,
}

fn progress_key(agent: &Value) -> String {
    json!([
        agent["task"],
        agent["spawn"],
        agent["session_id"],
        agent["liveness"]["session"]
    ])
    .to_string()
}

/// The content of a checkpoint, not a repeated `rk progress` call's revision,
/// is evidence. Generic timestamps and retry counters never advance the clock.
fn progress_signature(agent: &Value) -> Option<String> {
    if agent["spawn"].as_str()?.is_empty() {
        return None;
    }
    if agent["state"] == "running"
        && agent["liveness"]["output_fingerprint"].as_u64().is_none()
        && agent["progress"]["summary"].as_str().is_none()
    {
        return None;
    }
    Some(
        json!([
            agent["progress"]["summary"],
            agent["progress"]["next"],
            agent["progress"]["status"],
            agent["liveness"]["output_fingerprint"],
            agent["state"],
            agent["result"],
        ])
        .to_string(),
    )
}

fn progress_is_waiting(agent: &Value) -> bool {
    agent["progress"]["status"].as_str().is_some_and(|status| {
        matches!(
            status.to_ascii_lowercase().split([':', ' ']).next(),
            Some("verifying" | "queued" | "awaiting-review" | "human-gate" | "recovery-backoff")
        )
    })
}

fn resolve_progress_episode(state: &mut ProgressState, now: DateTime<Utc>, reason: &str) {
    if state.stalled_since.take().is_some() {
        if let Some(episode) = state.history.last_mut() {
            episode.resolved_at = Some(now);
            episode.resolution = Some(reason.into());
        }
    }
}

/// `queued_since`, despite the name, is the caller's combined durable-wait
/// evidence: either a landing-queue admission or a declared human-gate
/// intervention (see `advance_sample_progress`), whichever started earlier.
/// Both share the one `max_wait_secs` bound below.
fn advance_progress_state(
    previous: Option<&ProgressState>,
    agent: &Value,
    now: DateTime<Utc>,
    thresholds: &Thresholds,
    queued_since: Option<DateTime<Utc>>,
) -> (ProgressReading, ProgressState) {
    let spawn = agent["spawn"].as_str().map(str::to_string);
    let session = agent["session_id"].as_str().map(str::to_string);
    let attempt = agent["liveness"]["session"].as_str().map(str::to_string);
    let signature = progress_signature(agent).or_else(|| {
        // A queue row bound to this generation supplies evidence even if the
        // worker itself has never emitted a checkpoint or output fingerprint.
        queued_since
            .filter(|_| agent["spawn"].as_str().is_some_and(|s| !s.is_empty()))
            .map(|_| json!([agent["spawn"], agent["state"]]).to_string())
    });
    let replaced =
        previous.is_none_or(|s| s.spawn != spawn || s.session != session || s.attempt != attempt);
    let mut state = if replaced {
        ProgressState {
            ticket: agent["task"].as_str().unwrap_or("").into(),
            spawn,
            session,
            attempt,
            signature: signature.clone().unwrap_or_default(),
            changed_at: now,
            last_observed: now,
            wait_started_at: None,
            stalled_since: None,
            episodes: 0,
            history: Vec::new(),
            declared_wait_retired: false,
        }
    } else {
        previous.expect("same attempt has a prior state").clone()
    };
    // Clock reversal and missing evidence cannot erase a prior silence clock.
    if now < state.last_observed || signature.is_none() {
        return (ProgressReading::Unresolved, state);
    }
    state.last_observed = now;
    let signature = signature.unwrap();
    let reconnecting = agent["liveness"]["reconnect_events"].as_u64().unwrap_or(0) > 0;
    if !reconnecting && state.signature != signature {
        state.signature = signature;
        state.changed_at = now;
    }
    let waiting = queued_since.is_some() || progress_is_waiting(agent);
    if waiting {
        let since = queued_since.unwrap_or(now);
        let previous = state.wait_started_at.get_or_insert(since);
        *previous = (*previous).min(since);
    } else {
        state.wait_started_at = None;
    }
    // A continuing wait has its own fixed deadline; repeated status/checkpoint
    // updates cannot renew it. A missing sample never resets either clock.
    let (since, bound_secs) = if let Some(since) = state.wait_started_at {
        (since, thresholds.max_wait_secs)
    } else {
        (state.changed_at, thresholds.progress_stall_after_secs)
    };
    let silence = now
        .signed_duration_since(since)
        .to_std()
        .unwrap_or_default();
    let expired = silence > Duration::from_secs(bound_secs);
    if !expired {
        resolve_progress_episode(&mut state, now, "progress");
    }
    let reading = if expired {
        if state.stalled_since.is_none() {
            state.stalled_since = Some(now);
            state.episodes = state.episodes.saturating_add(1);
            state.history.push(ProgressEpisode {
                ticket: state.ticket.clone(),
                spawn: state.spawn.clone(),
                session: state.session.clone(),
                attempt: state.attempt.clone(),
                started_at: now,
                resolved_at: None,
                resolution: None,
            });
        }
        ProgressReading::Stalled
    } else if waiting {
        ProgressReading::Waiting
    } else {
        ProgressReading::Progressing
    };
    (reading, state)
}

/// Join only this repository's queued task. A recorded source generation is
/// a fence; for older unbound entries, do not excuse a worker created after the
/// queue phase began. Use the durable phase age, including time before this run.
fn landing_wait_since(sample: &Sample, agent: &Value) -> Option<DateTime<Utc>> {
    let rows = sample.status.as_ref()?["landing_queue_tasks"].as_array()?;
    rows.iter()
        .filter_map(|row| {
            if row["status"] != "queued" || row["repo"].as_str()? != agent["repo_name"].as_str()? {
                return None;
            }
            let raw_task = row["task"].as_str()?;
            let task = sample
                .tickets
                .iter()
                .find(|ticket| ticket["alias"].as_str() == Some(raw_task))
                .and_then(|ticket| ticket["identity"].as_str())
                .unwrap_or(raw_task);
            if Some(task) != agent["task"].as_str() {
                return None;
            }
            let age = row["phase_age_secs"].as_u64()?;
            let since = sample
                .observed_at
                .checked_sub_signed(chrono::Duration::try_seconds(i64::try_from(age).ok()?)?)?;
            if let Some(spawn) = row["source_spawn"].as_str() {
                if Some(spawn) != agent["spawn"].as_str() {
                    return None;
                }
            } else if parse_time(&agent["created_at"]).is_none_or(|created| created > since) {
                return None;
            }
            Some(since)
        })
        .min()
}

/// Join only a declared `human-gate` intervention recorded for this exact
/// ticket, owner and generation. A missing or mismatched owner/spawn never
/// grants an exemption -- absent or ambiguous evidence is not a trusted
/// exemption. The earliest matching declaration wins, so a duplicate or
/// repeated re-declaration for the same gate cannot push its deadline
/// later, and filtering on `observed_at <= now` means a declaration
/// recorded after a given sample can never retroactively excuse it (the
/// bound itself is still enforced by [`advance_progress_state`] against the
/// run's frozen `max_wait_secs`, exactly like the landing-queue wait).
fn declared_gate_wait_since(
    interventions: &[Intervention],
    agent: &Value,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let task = agent["task"].as_str().filter(|t| !t.is_empty())?;
    let spawn = agent["spawn"].as_str().filter(|s| !s.is_empty())?;
    let owner = agent["name"].as_str().filter(|n| !n.is_empty())?;
    interventions
        .iter()
        .filter(|iv| iv.class == InterventionClass::HumanGate)
        .filter(|iv| iv.ticket.as_deref() == Some(task))
        .filter(|iv| iv.owner.as_deref() == Some(owner))
        .filter(|iv| iv.spawn.as_deref() == Some(spawn))
        .filter(|iv| iv.observed_at <= now)
        .map(|iv| iv.observed_at)
        .min()
}

/// Shared by live collection, checkpoint reconstruction and report replay.
/// Cumulative episodes include delivered/replaced generations, not only the
/// agents that happen to remain live in this sample.
fn advance_sample_progress(
    states: &mut BTreeMap<String, ProgressState>,
    sample: &Sample,
    thresholds: &Thresholds,
    interventions: &[Intervention],
) -> ProgressMetrics {
    let canonical_task = |task: &str| {
        if sample
            .tickets
            .iter()
            .any(|t| t["identity"].as_str() == Some(task))
        {
            return Some(task.to_string());
        }
        let matches: BTreeSet<_> = sample
            .tickets
            .iter()
            .filter(|t| t["alias"].as_str() == Some(task))
            .filter_map(|t| t["identity"].as_str())
            .collect();
        match matches.len() {
            0 => Some(task.to_string()),
            1 => Some(matches.first().unwrap().to_string()),
            _ => None,
        }
    };
    let declarations: Vec<_> = interventions
        .iter()
        .cloned()
        .map(|mut iv| {
            iv.ticket = iv.ticket.as_deref().and_then(&canonical_task);
            iv
        })
        .collect();
    let mut stalled = BTreeSet::new();
    let mut unresolved = BTreeSet::new();
    let mut seen = BTreeSet::new();
    let mut live_by_ticket: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for raw_agent in &sample.agents {
        let mut agent = raw_agent.clone();
        if let Some(raw) = agent["task"].as_str() {
            let Some(task) = canonical_task(raw) else {
                unresolved.insert(raw.to_string());
                continue;
            };
            agent["task"] = json!(task);
        }
        let key = progress_key(&agent);
        if !seen.insert(key.clone()) {
            continue;
        }
        let live = matches!(agent["state"].as_str(), Some("spawning" | "running"));
        if !live {
            if let Some(state) = states.get_mut(&key) {
                resolve_progress_episode(state, sample.observed_at, "left-live-state");
            }
            continue;
        }
        let Some(ticket) = agent["task"].as_str().filter(|t| !t.is_empty()) else {
            unresolved.insert(key);
            continue;
        };
        live_by_ticket
            .entry(ticket.into())
            .or_default()
            .insert(key.clone());
        let declaration = declared_gate_wait_since(&declarations, &agent, sample.observed_at);
        let previous = states.get(&key);
        let retired = states.values().any(|state| {
            state.ticket == ticket
                && state.spawn.as_deref() == agent["spawn"].as_str()
                && state.declared_wait_retired
        }) || previous.is_some_and(|state| {
            state.declared_wait_retired
                || declaration.is_some_and(|since| {
                    since <= state.last_observed
                        && sample.observed_at >= state.last_observed
                        && !progress_is_waiting(&agent)
                        && agent["progress"]["status"]
                            .as_str()
                            .is_some_and(|s| !s.is_empty())
                        && agent["liveness"]["reconnect_events"].as_u64().unwrap_or(0) == 0
                        && serde_json::from_str::<Value>(&state.signature)
                            .ok()
                            .is_some_and(|old| {
                                // Output fingerprints and repeated timestamp updates are not
                                // evidence that a declared human gate has ended.
                                old[0] != agent["progress"]["summary"]
                                    || old[1] != agent["progress"]["next"]
                                    || old[2] != agent["progress"]["status"]
                            })
                })
        });
        let declared_since = landing_wait_since(sample, &agent)
            .into_iter()
            .chain(declaration.filter(|_| !retired))
            .min();
        let (reading, mut state) = advance_progress_state(
            previous,
            &agent,
            sample.observed_at,
            thresholds,
            declared_since,
        );
        state.declared_wait_retired = retired;
        match reading {
            ProgressReading::Stalled => {
                stalled.insert(ticket.to_string());
            }
            ProgressReading::Unresolved => {
                unresolved.insert(ticket.to_string());
            }
            _ => {}
        }
        states.insert(key, state);
    }
    for (key, state) in states.iter_mut() {
        if live_by_ticket
            .get(&state.ticket)
            .is_some_and(|keys| !keys.contains(key))
        {
            resolve_progress_episode(state, sample.observed_at, "replaced");
        }
    }
    ProgressMetrics {
        stalled: stalled.len() as u64,
        unresolved: unresolved.len() as u64,
        episodes: states.values().map(|s| s.episodes).sum(),
    }
}

fn replay_progress(samples: &mut [Sample], thresholds: &Thresholds) -> Vec<ProgressEpisode> {
    let mut states = BTreeMap::new();
    for sample in samples {
        let progress = advance_sample_progress(
            &mut states,
            sample,
            thresholds,
            &sample.declared_interventions,
        );
        sample.metrics.progress_stalled_tickets = progress.stalled;
        sample.metrics.progress_unresolved_tickets = progress.unresolved;
        sample.metrics.progress_stall_episodes = progress.episodes;
    }
    let mut episodes: Vec<_> = states.into_values().flat_map(|s| s.history).collect();
    episodes.sort_by(|a, b| {
        a.started_at
            .cmp(&b.started_at)
            .then(a.ticket.cmp(&b.ticket))
    });
    episodes
}

fn derive_metrics_with_ready_age(
    sample: &Sample,
    manifest: &Manifest,
    ready_age: impl Fn(&str) -> u64,
    progress: ProgressMetrics,
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
    metrics.progress_stalled_tickets = progress.stalled;
    metrics.progress_unresolved_tickets = progress.unresolved;
    metrics.progress_stall_episodes = progress.episodes;
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
    derive_sample_metrics_with_interventions(sample, manifest, prior, &[])
}

#[cfg(test)]
fn derive_sample_metrics_with_interventions(
    sample: &Sample,
    manifest: &Manifest,
    prior: &[Sample],
    interventions: &[Intervention],
) -> SampleMetrics {
    let mut states = BTreeMap::new();
    for previous in prior {
        advance_sample_progress(&mut states, previous, &manifest.thresholds, interventions);
    }
    let progress =
        advance_sample_progress(&mut states, sample, &manifest.thresholds, interventions);
    derive_metrics_with_ready_age(
        sample,
        manifest,
        |ticket| continuous_ready_age_secs(sample, prior, ticket),
        progress,
    )
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
        owner: args.owner,
        spawn: args.spawn,
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

fn exercise(args: ExerciseArgs, as_json: bool) -> Result<()> {
    let exercise = Exercise {
        schema_version: SCHEMA_VERSION,
        id: RecordId::new().to_string(),
        kind: args.kind,
        occurred_at: Utc::now(),
        before_identity: nonempty(args.before, "--before")?,
        after_identity: nonempty(args.after, "--after")?,
        ticket: args.ticket,
        note: args.note,
    };
    fs::create_dir_all(args.run.join(EXERCISES))?;
    let path = args
        .run
        .join(EXERCISES)
        .join(format!("{}.json", exercise.id));
    write_new_json(&path, &exercise)?;
    if as_json {
        println!("{}", serde_json::to_string(&exercise)?);
    } else {
        println!("recorded {} ({:?})", exercise.id, exercise.kind);
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

fn qualify(args: QualifyArgs, as_json: bool) -> Result<()> {
    let result = derive_qualification(&args.run)?;
    if args.finalize {
        write_json_atomic(&args.run.join(QUALIFICATION), &result)?;
    }
    if as_json {
        println!("{}", serde_json::to_string(&result)?);
    } else {
        print_qualification(&result);
    }
    if !result.qualified {
        bail!("acceptance contract requirements not met");
    }
    Ok(())
}

fn derive_report(run_dir: &Path) -> Result<Report> {
    let manifest = load_manifest(run_dir)?;
    let mut samples = load_samples(run_dir)?;
    let interventions = load_interventions(run_dir)?;
    let progress_episodes = replay_progress(&mut samples, &manifest.thresholds);
    if samples.is_empty() {
        bail!("{} contains no samples", run_dir.display());
    }
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
    let ticket_coverage = source_coverage(
        &samples,
        manifest.started_at,
        ended_at,
        sample_tickets_fresh,
    );
    let event_coverage =
        source_coverage(&samples, manifest.started_at, ended_at, sample_events_fresh);
    let delivered = latest_tickets(&samples)
        .into_values()
        .filter(|ticket| delivery_in_window(ticket, manifest.started_at, ended_at))
        .collect::<Vec<_>>();
    let delivered_during_run = delivered
        .iter()
        .filter(|ticket| manifest.tickets.is_empty() || selected_root(ticket, &manifest.tickets))
        .count() as u64;
    let correction_deliveries = delivered.len() as u64 - delivered_during_run;
    let (attributed_cost_usd, attributed_tokens, usage_coverage) =
        attributed_usage(&samples, manifest.started_at);
    let max_landing_depth = max_metric(&samples, |m| m.landing_depth);
    let max_landing_age_secs = max_metric(&samples, |m| m.oldest_landing_age_secs);
    let max_ready_age_secs = max_metric(&samples, |m| m.oldest_ready_age_secs);
    let max_reconcile_violations = max_metric(&samples, |m| m.reconcile_violations);
    let max_stale_tickets = max_metric(&samples, |m| m.stale_tickets);
    let max_unclassified_holds = max_metric(&samples, |m| m.unclassified_holds);
    let max_progress_stalled_tickets = max_metric(&samples, |m| m.progress_stalled_tickets);
    let max_progress_unresolved_tickets = max_metric(&samples, |m| m.progress_unresolved_tickets);
    // Retained by maximum, not the final sample: a ticket that stalled and
    // was later delivered drops out of the live cohort a later sample can
    // even see, but the episode it recorded while live must not disappear
    // from the run's evidence (D1 point 4).
    let progress_stall_episodes = max_metric(&samples, |m| m.progress_stall_episodes);
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
    // Event-derived absence checks: a zero here only proves no such event
    // occurred if the event feed was actually drained through the end of the
    // run. See `event_coverage` and `check_with_coverage`.
    check_with_coverage(
        &mut checks,
        "forced-landings",
        forced_landings,
        manifest.thresholds.max_forced_landings,
        event_coverage.coverage,
    );
    check(
        &mut checks,
        "duplicate-dispatches",
        duplicate_dispatches,
        manifest.thresholds.max_duplicate_dispatches,
    );
    check_with_coverage(
        &mut checks,
        "duplicate-landings",
        duplicate_landings,
        manifest.thresholds.max_duplicate_landings,
        event_coverage.coverage,
    );
    check(&mut checks, "stale-tickets", max_stale_tickets, 0);
    check(
        &mut checks,
        "unclassified-holds",
        max_unclassified_holds,
        manifest.thresholds.max_unclassified_holds,
    );
    // Independent of the two checks above: a live generation the daemon
    // still calls "running" can still fail here (D1's counterexample).
    check(
        &mut checks,
        "progress-stalled-tickets",
        max_progress_stalled_tickets,
        0,
    );
    check(
        &mut checks,
        "progress-evidence-gaps",
        max_progress_unresolved_tickets,
        0,
    );
    if let Some(limit) = manifest.thresholds.max_cost_usd {
        checks.insert(
            "attributed-cost-usd".into(),
            Check {
                observed: json!(attributed_cost_usd),
                limit: json!(limit),
                passed: attributed_cost_usd <= limit
                    && usage_coverage.coverage == Coverage::Complete,
                coverage: Some(usage_coverage.coverage),
            },
        );
    }
    // Named, durable evidence of the three merged-across-samples sources
    // themselves, independent of any single check that consumes them — see
    // `ticket_coverage`/`event_coverage`/`usage_coverage` on `Report`.
    check_coverage(&mut checks, "ticket-coverage", &ticket_coverage);
    check_coverage(&mut checks, "event-coverage", &event_coverage);
    check_coverage(&mut checks, "usage-coverage", &usage_coverage);
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
        evaluator_version: REPORT_EVALUATOR_VERSION,
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
        ticket_coverage,
        attributed_cost_usd,
        attributed_tokens,
        usage_coverage,
        max_landing_depth,
        max_landing_age_secs,
        max_ready_age_secs,
        max_reconcile_violations,
        forced_landings,
        duplicate_dispatches,
        duplicate_landings,
        event_coverage,
        max_stale_tickets,
        max_unclassified_holds,
        max_progress_stalled_tickets,
        max_progress_unresolved_tickets,
        progress_stall_episodes,
        progress_episodes,
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
            coverage: None,
        },
    );
}

/// Like [`check`], but for an absence-based check whose `observed` count is
/// derived from a source that can be mid-refresh at run end (the event
/// feed). `observed <= limit` alone can never justify `passed: true` when
/// `coverage` is `Incomplete` — a gap could be hiding the very occurrence
/// the check exists to catch. An observed value that already exceeds the
/// limit still fails regardless of coverage: that is real, positive
/// evidence, not something a gap could manufacture.
fn check_with_coverage(
    checks: &mut BTreeMap<String, Check>,
    name: &str,
    observed: u64,
    limit: u64,
    coverage: Coverage,
) {
    checks.insert(
        name.into(),
        Check {
            observed: json!(observed),
            limit: json!(limit),
            passed: observed <= limit && coverage == Coverage::Complete,
            coverage: Some(coverage),
        },
    );
}

/// A human-readable clause describing an incomplete source, empty when
/// `status` is `Complete`. Shared by qualification requirement detail text.
fn coverage_note(label: &str, status: &CoverageStatus) -> String {
    if status.coverage == Coverage::Complete {
        String::new()
    } else {
        format!(
            "; {label} coverage incomplete ({} incomplete sample(s), {}s uncovered tail) — a zero here is not proven absence",
            status.incomplete_samples,
            status.uncovered_tail_secs.unwrap_or(0)
        )
    }
}

/// Publish one source's own coverage as a named, durable check: `passed`
/// exactly when nothing about that source's evidence in this run is
/// unresolved. Distinct from `check_with_coverage`, which uses this same
/// status but as a MODIFIER on top of a separate observed/limit comparison.
fn check_coverage(checks: &mut BTreeMap<String, Check>, name: &str, status: &CoverageStatus) {
    checks.insert(
        name.into(),
        Check {
            observed: json!(status.incomplete_samples),
            limit: json!(0),
            passed: status.coverage == Coverage::Complete,
            coverage: Some(status.coverage),
        },
    );
}

/// RPC methods `collect_sample` calls, in order, up to and including
/// `ticket.list`. A `deadline exceeded; remaining reads skipped` timeout
/// (see `SampleReader::timed_out`) on any earlier method in this list drops
/// the connection and skips every read scheduled after it in the same
/// sample, `ticket.list` included — see `collect_sample`'s fixed RPC order.
const PRE_TICKET_RPCS: &[&str] = &[
    "connect",
    "status",
    "king.status",
    "work.current",
    "reconcile.report",
];
/// As [`PRE_TICKET_RPCS`], extended through `agent.list` — the last read
/// before event-page collection begins.
const PRE_EVENT_RPCS: &[&str] = &[
    "connect",
    "status",
    "king.status",
    "work.current",
    "reconcile.report",
    "ticket.list",
    "agent.list",
];

/// Whether any of `rpcs` reported the specific cascading-skip timeout that
/// `SampleReader::timed_out` emits, which drops the connection and skips
/// every subsequent read in the same sample.
fn cascaded_skip(errors: &[String], rpcs: &[&str]) -> bool {
    errors.iter().any(|error| {
        rpcs.iter()
            .any(|rpc| error.starts_with(&format!("{rpc}: ")))
            && error.contains("deadline exceeded; remaining reads skipped")
    })
}

/// Whether this sample's `tickets`/`lineage` reflect a `ticket.list` read
/// that actually ran, as opposed to being empty only because an earlier
/// timeout in the same sample skipped it (see `PRE_TICKET_RPCS`) or the read
/// itself failed.
fn sample_tickets_fresh(sample: &Sample) -> bool {
    !cascaded_skip(&sample.errors, PRE_TICKET_RPCS)
        && !sample
            .errors
            .iter()
            .any(|error| error.starts_with("ticket.list:"))
}

/// Whether this sample's event-page loop drained to the live tip: no
/// upstream cascade skip, no `space.scan` failure — old-format unbounded-scan
/// `frame_too_large` text and the newer bounded-retry/oversized-frame
/// messages alike, since both are recorded with the same `"space.scan: "`
/// prefix — and no held oversized-event frontier.
fn sample_events_fresh(sample: &Sample) -> bool {
    !cascaded_skip(&sample.errors, PRE_EVENT_RPCS)
        && !sample
            .errors
            .iter()
            .any(|error| error.starts_with("space.scan:"))
        && sample.sampling.oversized_event_after.is_none()
}

/// Coverage for one merged-across-samples source. Conservative by
/// construction: a sample this run's evaluator does not recognize as
/// "fresh" for `fresh` counts against coverage, so an unrecognized old-format
/// error still yields `Incomplete` rather than silently assuming success.
fn source_coverage(
    samples: &[Sample],
    started_at: DateTime<Utc>,
    ended_at: DateTime<Utc>,
    fresh: impl Fn(&Sample) -> bool,
) -> CoverageStatus {
    let incomplete_samples = samples.iter().filter(|sample| !fresh(sample)).count() as u64;
    let last_fresh = samples
        .iter()
        .rev()
        .find(|sample| fresh(sample))
        .map(|sample| sample.observed_at);
    match last_fresh {
        Some(at) if at >= ended_at => CoverageStatus {
            coverage: Coverage::Complete,
            incomplete_samples,
            uncovered_tail_secs: None,
        },
        other => CoverageStatus {
            coverage: Coverage::Incomplete,
            incomplete_samples,
            uncovered_tail_secs: Some(
                ended_at
                    .signed_duration_since(other.unwrap_or(started_at))
                    .to_std()
                    .map_or(0, |gap| gap.as_secs()),
            ),
        },
    }
}

/// Evaluate one run against its frozen acceptance contract. Pure over the
/// run's immutable evidence files: no daemon RPC and no LLM call, so replay is
/// deterministic for a given contract and evaluator version. A run with no
/// frozen contract has no qualification claim to evaluate, only a general
/// `Report`.
fn derive_qualification(run_dir: &Path) -> Result<QualificationResult> {
    let frozen = load_contract(run_dir)?.ok_or_else(|| {
        anyhow::anyhow!(
            "{} has no frozen acceptance contract; only a general report is available",
            run_dir.display()
        )
    })?;
    let contract = &frozen.contract;
    let manifest = load_manifest(run_dir)?;
    let mut samples = load_samples(run_dir)?;
    let interventions = load_interventions(run_dir)?;
    replay_progress(&mut samples, &manifest.thresholds);
    let report = derive_report(run_dir)?;
    let exercises = load_exercises(run_dir)?;

    let mut checks = Vec::new();
    let mut require = |requirement: &str, passed: bool, detail: String| {
        checks.push(QualificationCheck {
            requirement: requirement.into(),
            passed,
            detail,
        });
    };

    require(
        "workload/root-deliveries",
        report.delivered_during_run >= contract.workload.min_root_deliveries,
        format!(
            "{} root deliveries against a minimum of {}; idle elapsed time alone cannot satisfy workload{}",
            report.delivered_during_run,
            contract.workload.min_root_deliveries,
            coverage_note("ticket", &report.ticket_coverage)
        ),
    );
    // A minimum-count requirement already fails safe on an undercount, but a
    // gap that never resolves before the run ends must still be surfaced on
    // its own: it can otherwise vanish from view whenever the minimum
    // happens to be zero or is met by evidence gathered before the gap.
    require(
        "workload/delivery-coverage",
        report.ticket_coverage.coverage == Coverage::Complete,
        format!(
            "ticket coverage {:?}; {} incomplete sample(s){}",
            report.ticket_coverage.coverage,
            report.ticket_coverage.incomplete_samples,
            report
                .ticket_coverage
                .uncovered_tail_secs
                .map(|secs| format!(", {secs}s uncovered tail"))
                .unwrap_or_default()
        ),
    );
    require(
        "workload/correction-deliveries",
        report.correction_deliveries >= contract.workload.min_correction_deliveries,
        format!(
            "{} correction deliveries against a minimum of {}",
            report.correction_deliveries, contract.workload.min_correction_deliveries
        ),
    );

    require(
        "duration/elapsed",
        report.elapsed_secs >= contract.duration.min_elapsed_secs,
        format!(
            "{}s elapsed against a minimum of {}s",
            report.elapsed_secs, contract.duration.min_elapsed_secs
        ),
    );
    let gap_budget = contract
        .duration
        .max_sample_gap_secs
        .saturating_add(contract.duration.coverage_tolerance_secs);
    require(
        "duration/sample-gap",
        report.max_sample_gap_secs <= gap_budget,
        format!(
            "max sample gap {}s against a budget of {}s ({}s tolerance)",
            report.max_sample_gap_secs, gap_budget, contract.duration.coverage_tolerance_secs
        ),
    );

    require(
        "liveness/stale-tickets",
        report.max_stale_tickets <= contract.liveness.max_stale_tickets,
        format!(
            "max {} stale tickets against a limit of {}",
            report.max_stale_tickets, contract.liveness.max_stale_tickets
        ),
    );
    require(
        "liveness/unclassified-holds",
        report.max_unclassified_holds <= contract.liveness.max_unclassified_holds,
        format!(
            "max {} unclassified holds against a limit of {}",
            report.max_unclassified_holds, contract.liveness.max_unclassified_holds
        ),
    );
    // D1's typed, independent stall evidence: a live generation's own
    // progress signature, never the daemon's `work.current` `stalled`
    // bucket — see `progress_signature`/`advance_progress_state`. A failed
    // supervisor sweep leaves `unclassified_holds` at zero but cannot hide a
    // stall from this.
    let stall_incidents = report.progress_stall_episodes;
    require(
        "liveness/stall-incidents",
        stall_incidents <= contract.liveness.max_stall_incidents,
        format!(
            "{stall_incidents} independently-derived stall onset(s) against a limit of {}",
            contract.liveness.max_stall_incidents
        ),
    );
    require(
        "liveness/progress-evidence-gaps",
        report.max_progress_unresolved_tickets == 0,
        format!(
            "{} live generation(s) had progress evidence too ambiguous to judge; insufficient evidence cannot qualify by omission",
            report.max_progress_unresolved_tickets
        ),
    );

    for wanted in &contract.exercises {
        let matching: Vec<&Exercise> = exercises
            .iter()
            .filter(|candidate| candidate.kind == wanted.kind)
            .collect();
        require(
            &format!("exercise/{}/count", exercise_kind_name(wanted.kind)),
            matching.len() as u64 >= wanted.min_count,
            format!(
                "{} recorded {} exercise(s) against a minimum of {}",
                matching.len(),
                exercise_kind_name(wanted.kind),
                wanted.min_count
            ),
        );
        let min_continuation = if wanted.min_continuation_samples == 0 {
            DEFAULT_MIN_CONTINUATION_SAMPLES
        } else {
            wanted.min_continuation_samples
        };
        for candidate in &matching {
            let identity_changed = candidate.before_identity != candidate.after_identity;
            let continued =
                identity_changed && exercise_continuation(&samples, candidate, min_continuation);
            require(
                &format!(
                    "exercise/{}/{}/continuation",
                    exercise_kind_name(wanted.kind),
                    candidate.id
                ),
                continued,
                if !identity_changed {
                    "before/after identities are identical; a PID or King-identity change alone is not required, but an unchanged identity proves no transition occurred".into()
                } else {
                    format!(
                        "requires >= {min_continuation} subsequent live sample(s) with no repeated landing side effects"
                    )
                },
            );
        }
    }

    let ad_hoc = interventions
        .iter()
        .filter(|intervention| intervention.class == InterventionClass::AdHoc)
        .count() as u64;
    require(
        "interventions/ad-hoc-limit",
        ad_hoc <= contract.interventions.max_ad_hoc,
        format!(
            "{ad_hoc} ad-hoc intervention(s) against a limit of {} (R1 requires zero)",
            contract.interventions.max_ad_hoc
        ),
    );
    let disallowed = if contract.interventions.allowed_classes.is_empty() {
        0
    } else {
        interventions
            .iter()
            .filter(|intervention| {
                !contract
                    .interventions
                    .allowed_classes
                    .contains(&intervention.class)
            })
            .count()
    };
    require(
        "interventions/allowed-classes",
        disallowed == 0,
        format!("{disallowed} intervention(s) outside the contract's allowed classes"),
    );

    require(
        "build-identity/frozen-build",
        manifest.observer_build == contract.build_identity.frozen_build,
        format!(
            "observer build {} against the frozen build {}",
            manifest.observer_build, contract.build_identity.frozen_build
        ),
    );
    require(
        "build-identity/build-parity",
        report.build_mismatch_samples == 0,
        format!(
            "{} sample(s) observed a build other than the run's own observer build",
            report.build_mismatch_samples
        ),
    );

    require(
        "resources/landing-age",
        report.max_landing_age_secs <= contract.resources.max_landing_age_secs,
        format!(
            "max landing age {}s against a limit of {}s",
            report.max_landing_age_secs, contract.resources.max_landing_age_secs
        ),
    );
    require(
        "resources/ready-age",
        report.max_ready_age_secs <= contract.resources.max_ready_age_secs,
        format!(
            "max ready age {}s against a limit of {}s",
            report.max_ready_age_secs, contract.resources.max_ready_age_secs
        ),
    );
    require(
        "resources/duplicate-dispatches",
        report.duplicate_dispatches <= contract.resources.max_duplicate_dispatches,
        format!(
            "{} duplicate dispatch(es) against a limit of {}",
            report.duplicate_dispatches, contract.resources.max_duplicate_dispatches
        ),
    );
    require(
        "resources/duplicate-landings",
        report.duplicate_landings <= contract.resources.max_duplicate_landings
            && report.event_coverage.coverage == Coverage::Complete,
        format!(
            "{} duplicate landing(s) against a limit of {}{}",
            report.duplicate_landings,
            contract.resources.max_duplicate_landings,
            coverage_note("event", &report.event_coverage)
        ),
    );
    require(
        "resources/forced-landings",
        report.forced_landings <= contract.resources.max_forced_landings
            && report.event_coverage.coverage == Coverage::Complete,
        format!(
            "{} forced landing(s) against a limit of {}{}",
            report.forced_landings,
            contract.resources.max_forced_landings,
            coverage_note("event", &report.event_coverage)
        ),
    );
    require(
        "resources/reconcile-violations",
        report.max_reconcile_violations <= contract.resources.max_reconcile_violations,
        format!(
            "max {} reconciliation violation(s) against a limit of {}",
            report.max_reconcile_violations, contract.resources.max_reconcile_violations
        ),
    );
    if let Some(limit) = contract.resources.max_spend_usd {
        let usage_note = if report.usage_coverage.coverage == Coverage::Complete {
            String::new()
        } else {
            format!(
                "; usage coverage incomplete ({} generation(s) still live, {}s since last reconciled) \
                 — this total is a provisional ledger reading, not a settled figure",
                report.usage_coverage.incomplete_samples,
                report.usage_coverage.uncovered_tail_secs.unwrap_or(0)
            )
        };
        require(
            "resources/spend-usd",
            report.attributed_cost_usd <= limit
                && report.usage_coverage.coverage == Coverage::Complete,
            format!(
                "attributed spend {:.4} USD against a limit of {limit:.4} USD{usage_note}",
                report.attributed_cost_usd
            ),
        );
    }

    require(
        "general-report",
        report.passed,
        format!(
            "the run's own observation report passed={}; a qualification cannot pass under a failing general report",
            report.passed
        ),
    );

    let qualified = checks.iter().all(|entry| entry.passed);
    Ok(QualificationResult {
        schema_version: SCHEMA_VERSION,
        run_id: manifest.id,
        contract_schema_version: contract.schema_version,
        contract_digest: frozen.digest,
        evaluator_version: QUALIFICATION_EVALUATOR_VERSION,
        checks,
        qualified,
    })
}

fn exercise_kind_name(kind: ExerciseKind) -> &'static str {
    match kind {
        ExerciseKind::WorkerDeath => "worker-death",
        ExerciseKind::NamedCheckFailure => "named-check-failure",
        ExerciseKind::MergeConflict => "merge-conflict",
        ExerciseKind::DaemonRollover => "daemon-rollover",
        ExerciseKind::KingReplacement => "king-replacement",
        ExerciseKind::Custom => "custom",
    }
}

/// An exercise proves continuation, not merely a transition: the run must
/// stay live for the required number of subsequent samples, and any landing
/// evidence for the exercised ticket after the exercise must not repeat a
/// prior landing (the "without repeated side effects" acceptance bar).
fn exercise_continuation(
    samples: &[Sample],
    exercise: &Exercise,
    min_continuation_samples: u64,
) -> bool {
    let after: Vec<&Sample> = samples
        .iter()
        .filter(|sample| sample.observed_at > exercise.occurred_at)
        .collect();
    let live_after = after
        .iter()
        .filter(|sample| sample.daemon_reachable)
        .count() as u64;
    if live_after < min_continuation_samples {
        return false;
    }
    let repeats = duplicate_landings(after.iter().flat_map(|sample| &sample.events).filter(
        |event| {
            exercise
                .ticket
                .as_deref()
                .is_none_or(|ticket| event["payload"]["task"].as_str() == Some(ticket))
        },
    ));
    repeats == 0
}

fn load_contract(run_dir: &Path) -> Result<Option<FrozenContract>> {
    let path = run_dir.join(CONTRACT);
    if !path.exists() {
        return Ok(None);
    }
    let frozen: FrozenContract = serde_json::from_reader(
        File::open(&path).with_context(|| format!("open {}", path.display()))?,
    )
    .with_context(|| format!("parse {}", path.display()))?;
    if frozen.contract.schema_version != CONTRACT_SCHEMA_VERSION {
        bail!(
            "unsupported acceptance contract schema {}",
            frozen.contract.schema_version
        );
    }
    let recomputed = canonical_digest(&frozen.contract)?;
    if recomputed != frozen.digest {
        bail!(
            "frozen acceptance contract digest mismatch in {}: evidence was modified after freezing",
            path.display()
        );
    }
    Ok(Some(frozen))
}

fn load_exercises(run_dir: &Path) -> Result<Vec<Exercise>> {
    let dir = run_dir.join(EXERCISES);
    if !dir.exists() {
        return Ok(vec![]);
    }
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

/// One spawn's usage as folded across samples in chronological order: its
/// first-seen reading (baselines a generation already running before
/// `started_at`, so only spend accrued DURING this run is attributed), its
/// LAST-seen reading (the freshest known state), and whether that reading
/// was harness-terminal — see [`attributed_usage`].
struct SpawnUsage {
    created: DateTime<Utc>,
    first_cost: f64,
    first_tokens: u64,
    last_cost: f64,
    last_tokens: u64,
    last_seen: DateTime<Utc>,
    settled: bool,
}

/// Generation states from which no further harness event can revise
/// `cost_usd`/`usage` on the same spawn — mirrors `AgentState::is_archivable`
/// minus `dismissed`'s daemon-side archiving nuance, which does not matter
/// here (a dismissed generation's cost is equally final).
fn agent_state_settled(state: Option<&str>) -> bool {
    matches!(
        state,
        Some("completed" | "failed" | "stopped" | "dismissed")
    )
}

/// Attribute run-window cost/tokens per generation (`spawn`, falling back to
/// `name`), folding each spawn to its LAST sampled reading rather than the
/// historical maximum across the run, and report whether every folded
/// generation had settled by then.
///
/// The daemon's own incremental usage ledger (summed from streaming `Usage`
/// events) can run ahead of reality mid-turn, and an authoritative
/// `Completed`/`result` reconciliation from the harness then corrects
/// `cost_usd`/`usage` back down on the SAME spawn and session, no process
/// restart in between — observed directly in trial evidence as a
/// `running`-state peak followed by a `paused`-state correction to roughly
/// half that value at the very next sample. Taking the max across samples
/// permanently keeps whichever pre-correction peak an in-window sample
/// happened to catch even after the harness has corrected it; two such
/// peaks, summed across two generations, is exactly what overcounted a real
/// trial's reported spend against the generations' own final totals. Folding
/// by LAST instead reflects whatever the daemon has most recently
/// reconciled to. A generation still live at run end has, by definition, not
/// reached its own final reconciliation yet — its contribution is real spend
/// so far, not double-counted or dropped, but it is provisional and could
/// still move; such spawns are surfaced via the returned coverage rather
/// than silently trusted as final.
fn attributed_usage(samples: &[Sample], started_at: DateTime<Utc>) -> (f64, u64, CoverageStatus) {
    let mut by_spawn: HashMap<String, SpawnUsage> = HashMap::new();
    for sample in samples {
        for agent in &sample.agents {
            let key = agent["spawn"]
                .as_str()
                .or_else(|| agent["name"].as_str())
                .unwrap_or("unknown")
                .to_string();
            let created = parse_time(&agent["created_at"]).unwrap_or(started_at);
            let cost = agent["cost_usd"].as_f64().unwrap_or(0.0);
            let tokens = agent_tokens(agent);
            let settled = agent_state_settled(agent["state"].as_str());
            by_spawn
                .entry(key)
                .and_modify(|row| {
                    row.last_cost = cost;
                    row.last_tokens = tokens;
                    row.last_seen = sample.observed_at;
                    row.settled = settled;
                })
                .or_insert(SpawnUsage {
                    created,
                    first_cost: cost,
                    first_tokens: tokens,
                    last_cost: cost,
                    last_tokens: tokens,
                    last_seen: sample.observed_at,
                    settled,
                });
        }
    }
    let ended_at = samples
        .last()
        .map(|sample| sample.observed_at)
        .unwrap_or(started_at);
    let unsettled = by_spawn.values().filter(|row| !row.settled).count() as u64;
    let uncovered_tail_secs = by_spawn
        .values()
        .filter(|row| !row.settled)
        .map(|row| {
            ended_at
                .signed_duration_since(row.last_seen)
                .to_std()
                .map_or(0, |gap| gap.as_secs())
        })
        .max();
    let coverage = CoverageStatus {
        coverage: if unsettled == 0 {
            Coverage::Complete
        } else {
            Coverage::Incomplete
        },
        incomplete_samples: unsettled,
        uncovered_tail_secs,
    };
    let (cost, tokens) = by_spawn.values().fold((0.0, 0), |(cost, tokens), row| {
        let baseline_cost = if row.created >= started_at {
            0.0
        } else {
            row.first_cost
        };
        let baseline_tokens = if row.created >= started_at {
            0
        } else {
            row.first_tokens
        };
        (
            cost + (row.last_cost - baseline_cost).max(0.0),
            tokens + row.last_tokens.saturating_sub(baseline_tokens),
        )
    });
    (cost, tokens, coverage)
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
        "session_id": agent["session_id"],
        "name": agent["name"],
        "repo_name": agent["repo_name"],
        "task": agent["task"],
        "state": agent["state"],
        "result": agent["result"],
        "created_at": agent["created_at"],
        "updated_at": agent["updated_at"],
        "archived_at": agent["archived_at"],
        "cost_usd": agent["cost_usd"],
        "usage": agent["usage"],
        "model": agent["model"],
        "harness": agent["harness"],
        "liveness": agent["liveness"],
        // Retain structured checkpoint content. A revision or timestamp
        // changing by itself does not advance the independent progress clock.
        "progress": agent["progress"],
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
        || manifest.thresholds.progress_stall_after_secs == 0
        || manifest.thresholds.max_wait_secs == 0
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
    if report.ticket_coverage.coverage == Coverage::Incomplete {
        println!(
            "  NOTE delivered_during_run/correction_deliveries/throughput_per_hour are a LOWER BOUND: \
             ticket coverage incomplete ({} incomplete sample(s), {}s uncovered tail)",
            report.ticket_coverage.incomplete_samples,
            report.ticket_coverage.uncovered_tail_secs.unwrap_or(0),
        );
    }
    if report.event_coverage.coverage == Coverage::Incomplete {
        println!(
            "  NOTE forced_landings/duplicate_landings are NOT proven zero: \
             event coverage incomplete ({} incomplete sample(s), {}s uncovered tail)",
            report.event_coverage.incomplete_samples,
            report.event_coverage.uncovered_tail_secs.unwrap_or(0),
        );
    }
    if report.usage_coverage.coverage == Coverage::Incomplete {
        println!(
            "  NOTE attributed_cost_usd/attributed_tokens are PROVISIONAL: \
             {} generation(s) still live, last reconciled up to {}s ago",
            report.usage_coverage.incomplete_samples,
            report.usage_coverage.uncovered_tail_secs.unwrap_or(0),
        );
    }
    for (name, check) in &report.checks {
        println!(
            "  {:<28} {:<4} observed {} <= {}{}",
            name,
            if check.passed { "PASS" } else { "FAIL" },
            check.observed,
            check.limit,
            match check.coverage {
                Some(Coverage::Incomplete) => " (coverage incomplete)",
                _ => "",
            },
        );
    }
    println!("  interventions {:?}", report.interventions);
}

fn print_qualification(result: &QualificationResult) {
    println!(
        "{} · contract {} (schema {}) · evaluator {} · {}",
        result.run_id,
        &result.contract_digest[..12.min(result.contract_digest.len())],
        result.contract_schema_version,
        result.evaluator_version,
        if result.qualified {
            "QUALIFIED"
        } else {
            "NOT QUALIFIED"
        },
    );
    for check in &result.checks {
        println!(
            "  {:<32} {:<4} {}",
            check.requirement,
            if check.passed { "PASS" } else { "FAIL" },
            check.detail,
        );
    }
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

    /// TKT-maruk-fazam-gazug: a slow diagnostic source (`work.current` in the
    /// trial) must not starve every read scheduled after it. The old
    /// behaviour discarded the connection on any RPC timeout and never
    /// reopened one, so ticket/agent/event collection silently went missing
    /// for the rest of the sample. `SampleReader::ensure_connected` now
    /// reconnects (a fresh socket, never the stale one) whenever the overall
    /// sample deadline still allows it.
    #[tokio::test]
    async fn a_slow_source_does_not_starve_later_essential_reads() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
        let (dir, mut manifest) = fixture();
        manifest.rpc_timeout_secs = 1;
        manifest.sample_timeout_secs = 5;
        write_json_atomic(&dir.path().join(MANIFEST), &manifest).unwrap();
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        layout.ensure().unwrap();
        let listener = tokio::net::UnixListener::bind(layout.socket_path()).unwrap();
        let server = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut stream = tokio::io::BufReader::new(stream);
                    loop {
                        let mut line = String::new();
                        if stream.read_line(&mut line).await.unwrap_or(0) == 0 {
                            break;
                        }
                        let request: Value = serde_json::from_str(&line).unwrap();
                        let method = request["method"].as_str().unwrap().to_string();
                        if method == "work.current" {
                            // Slower than the RPC timeout; the client gives up
                            // on this connection before any reply is sent.
                            tokio::time::sleep(Duration::from_millis(1500)).await;
                            break;
                        }
                        let value = match method.as_str() {
                            "status" => {
                                json!({"pid": 1, "build_version": "test", "landing_queue": []})
                            }
                            "king.status" => json!({"state": {}}),
                            "reconcile.report" => json!({"violations": []}),
                            "ticket.list" => json!({"tickets": []}),
                            "agent.list" => json!({"agents": []}),
                            "space.scan" => json!({"tuples": [], "truncated": false}),
                            _ => panic!("unexpected observation RPC {method}"),
                        };
                        let response = json!({"id": request["id"], "result": value,
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
            }
        });
        let collected =
            tokio::time::timeout(Duration::from_secs(4), append_sample(&layout, dir.path()))
                .await
                .unwrap()
                .unwrap();
        server.abort();
        assert!(collected.status.is_some());
        assert!(collected.king.is_some());
        assert!(
            collected.work.is_none(),
            "the slow source itself never completes"
        );
        assert_eq!(collected.sampling.rpc_timeouts, ["work.current"]);
        assert!(
            !collected.sampling.deadline_exceeded,
            "the overall sample budget was not exhausted, only the one RPC"
        );
        assert!(
            collected.reconcile.is_some(),
            "an essential source recovered via reconnect after the slow one"
        );
        assert!(collected.tickets.is_empty());
        assert!(
            collected.agents.is_empty(),
            "agent.list ran too, not just the first read after the timeout"
        );
        assert_eq!(
            collected.event_cursor,
            Some(RecordId::floor_at(manifest.started_at).to_string()),
            "space.scan ran too, bootstrapped from the run's own boundary"
        );
    }

    /// TKT-maruk-fazam-gazug: bootstrap must never ask for the whole
    /// category/scope history. A run with no checkpoint starts its `after_id`
    /// at the run's own boundary (`RecordId::floor_at(started_at)`) instead
    /// of `newest: true` with no smaller bound — a pre-run event is excluded
    /// from the request itself, not filtered out after arriving. A backlog
    /// wider than one bounded page (simulated here with an artificial 2-row
    /// server page, independent of the client's much larger requested
    /// `limit`) is walked to completion within one `collect_sample` call via
    /// repeated bounded round trips, accumulating without gaps or duplicates.
    #[tokio::test]
    async fn event_bootstrap_bounds_history_to_the_runs_own_window() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
        let (dir, manifest) = fixture();
        let stale_at = manifest.started_at - chrono::Duration::seconds(60);
        let stale = json!({"id": RecordId::floor_at(stale_at).to_string(),
            "created_at": stale_at.to_rfc3339()});
        let mut events = vec![stale];
        for offset in [1, 11, 21, 31, 41] {
            let at = manifest.started_at + chrono::Duration::seconds(offset);
            events.push(
                json!({"id": RecordId::floor_at(at).to_string(), "created_at": at.to_rfc3339()}),
            );
        }
        events.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
        let in_window: Vec<Value> = events[1..].to_vec();

        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        layout.ensure().unwrap();
        let listener = tokio::net::UnixListener::bind(layout.socket_path()).unwrap();
        let scan_params = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Value>::new()));
        let scan_params_server = scan_params.clone();
        let server_events = events.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = tokio::io::BufReader::new(stream);
            loop {
                let mut line = String::new();
                if stream.read_line(&mut line).await.unwrap() == 0 {
                    break;
                }
                let request: Value = serde_json::from_str(&line).unwrap();
                let method = request["method"].as_str().unwrap();
                let value = match method {
                    "status" => json!({"pid": 1, "build_version": "test", "landing_queue": []}),
                    "king.status" => json!({"state": {}}),
                    "work.current" => {
                        json!({"ready_tickets": [], "actionable": [], "decision_required": [], "stalled": []})
                    }
                    "reconcile.report" => json!({"violations": []}),
                    "ticket.list" => json!({"tickets": []}),
                    "agent.list" => json!({"agents": []}),
                    "space.scan" => {
                        scan_params_server
                            .lock()
                            .unwrap()
                            .push(request["params"].clone());
                        let after_id = request["params"]["after_id"].as_str().unwrap();
                        let mut page: Vec<Value> = server_events
                            .iter()
                            .filter(|event| event["id"].as_str().unwrap() > after_id)
                            .cloned()
                            .collect();
                        page.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
                        // Force multiple round trips regardless of the
                        // client's requested `limit`.
                        let truncated = page.len() > 2;
                        page.truncate(2);
                        json!({"tuples": page, "truncated": truncated})
                    }
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
        });
        let collected = append_sample(&layout, dir.path()).await.unwrap();
        server.abort();

        let calls = scan_params.lock().unwrap().clone();
        assert!(
            calls.len() >= 3,
            "the 5-event in-window backlog needed more than one bounded page: {calls:?}"
        );
        assert_eq!(
            calls[0]["after_id"].as_str().unwrap(),
            RecordId::floor_at(manifest.started_at).to_string(),
            "bootstrap starts at the run's own boundary, not an unscoped newest scan"
        );
        assert_eq!(
            calls[0]["limit"].as_u64().unwrap() as usize,
            EVENT_PAGE_LIMIT,
            "every page is explicitly bounded before wire serialization"
        );
        assert_eq!(
            collected.events.len(),
            in_window.len(),
            "the pre-window event was never requested, and none of the in-window ones were dropped or duplicated"
        );
        assert_eq!(
            collected.event_cursor,
            in_window
                .last()
                .map(|event| event["id"].as_str().unwrap().to_string())
        );
    }

    /// TKT-maruk-fazam-gazug: a page that cannot shrink below one row and
    /// still comes back `frame_too_large` means a single event is oversized
    /// on its own. Recovery must hold the completeness frontier at the last
    /// confirmed cursor rather than silently skip past the unseen event, and
    /// must say explicitly which id it is stuck behind.
    #[tokio::test]
    async fn an_oversized_single_event_holds_the_frontier_with_explicit_coverage() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
        let (dir, manifest) = fixture();
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
                let method = request["method"].as_str().unwrap();
                let response = if method == "space.scan" {
                    json!({"id": request["id"], "error": {"code": "frame_too_large",
                        "message": "response too large (21100000 bytes, limit 16777216); narrow the request (e.g. drop --all/--archived, or filter by repo)"},
                        "server_version": rk_core::version::BUILD_VERSION})
                } else {
                    let value = match method {
                        "status" => json!({"pid": 1, "build_version": "test", "landing_queue": []}),
                        "king.status" => json!({"state": {}}),
                        "work.current" => {
                            json!({"ready_tickets": [], "actionable": [], "decision_required": [], "stalled": []})
                        }
                        "reconcile.report" => json!({"violations": []}),
                        "ticket.list" => json!({"tickets": []}),
                        "agent.list" => json!({"agents": []}),
                        _ => panic!("unexpected observation RPC {method}"),
                    };
                    json!({"id": request["id"], "result": value,
                        "server_version": rk_core::version::BUILD_VERSION})
                };
                stream
                    .get_mut()
                    .write_all(format!("{response}\n").as_bytes())
                    .await
                    .unwrap();
            }
        });
        let collected = append_sample(&layout, dir.path()).await.unwrap();
        server.abort();

        let boundary = RecordId::floor_at(manifest.started_at).to_string();
        assert_eq!(
            collected.sampling.oversized_event_after,
            Some(boundary.clone())
        );
        assert!(collected.events.is_empty());
        assert_eq!(
            collected.event_cursor, None,
            "the frontier must not silently advance past the oversized event"
        );
        assert!(
            collected
                .errors
                .iter()
                .any(|error| error.contains("exceeds the response frame cap")
                    && error.contains(&boundary)),
            "{:?}",
            collected.errors
        );
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
                owner: None,
                spawn: None,
            },
            false,
        )
        .unwrap();
    }

    /// TKT-durap-simip-nadim: the trial evidence this replays
    /// (rk-flow-trial-20260911T170220Z) had `work.current` starve
    /// `ticket.list` for the rest of the run right after a real delivery
    /// landed outside the observer's view. `delivered_during_run` must not
    /// read as a proven zero when the tail after the last successful ticket
    /// read is never covered again before the run ends.
    #[test]
    fn an_uncovered_ticket_tail_marks_delivery_metrics_as_a_lower_bound() {
        let (dir, _) = fixture();
        let first = sample(1, "2026-09-02T00:00:30Z");
        let mut second = sample(2, "2026-09-02T00:01:00Z");
        second.errors = vec!["work.current: RPC deadline exceeded; remaining reads skipped".into()];
        second.tickets = vec![];
        let mut third = sample(3, "2026-09-02T00:01:30Z");
        third.errors = second.errors.clone();
        third.tickets = vec![];
        for value in [first, second, third] {
            append_json_line(&dir.path().join(SAMPLES), &value).unwrap();
        }
        let report = derive_report(dir.path()).unwrap();
        assert_eq!(report.ticket_coverage.coverage, Coverage::Incomplete);
        assert_eq!(report.ticket_coverage.incomplete_samples, 2);
        assert_eq!(report.ticket_coverage.uncovered_tail_secs, Some(60));
        assert_eq!(report.delivered_during_run, 0, "no delivery was ever captured, so this is only a lower bound — see the coverage flag, not this count, for proof");
        let check = &report.checks["ticket-coverage"];
        assert!(!check.passed, "{check:?}");
        assert_eq!(check.coverage, Some(Coverage::Incomplete));
        assert!(
            !report.passed,
            "an unresolved tail gap must fail the run closed, not disappear"
        );
    }

    /// TKT-durap-simip-nadim: the same trial never successfully drained a
    /// single event page (every sample failed with `space.scan:
    /// frame_too_large`, the old unbounded-scan error text). `forced_landings`
    /// and `duplicate_landings` are absence checks — zero only proves nothing
    /// happened if the event feed was actually read. A ticket read succeeding
    /// in the same sample must not be treated as evidence about events.
    #[test]
    fn an_unread_event_feed_prevents_a_false_pass_on_forced_and_duplicate_landings() {
        let (dir, _) = fixture();
        let mut value = sample(1, "2026-09-02T00:00:30Z");
        value.errors = vec![
            "space.scan: protocol: frame_too_large: response too large (21113109 bytes, limit 16777216); narrow the request".into(),
        ];
        value.events = vec![];
        append_json_line(&dir.path().join(SAMPLES), &value).unwrap();
        let report = derive_report(dir.path()).unwrap();
        assert_eq!(report.event_coverage.coverage, Coverage::Incomplete);
        assert_eq!(
            report.ticket_coverage.coverage,
            Coverage::Complete,
            "a space.scan failure must not implicate ticket.list, which ran and completed earlier in the same sample"
        );
        for name in ["forced-landings", "duplicate-landings"] {
            let check = &report.checks[name];
            assert_eq!(check.observed, json!(0));
            assert!(
                !check.passed,
                "{name}: observed 0 must not read as a proven zero without event coverage: {check:?}"
            );
            assert_eq!(check.coverage, Some(Coverage::Incomplete));
        }
        assert!(!report.passed);
    }

    /// Acceptance counterpart to the two tests above: when every sample's
    /// read of a source actually completed, an observed zero is a KNOWN
    /// zero, not a lower bound — coverage must read `Complete` and the
    /// corresponding checks must pass on their own merits.
    #[test]
    fn complete_coverage_reports_a_known_zero_not_a_lower_bound() {
        let (dir, _) = fixture();
        append_json_line(
            &dir.path().join(SAMPLES),
            &sample(1, "2026-09-02T00:00:30Z"),
        )
        .unwrap();
        let report = derive_report(dir.path()).unwrap();
        assert_eq!(report.ticket_coverage.coverage, Coverage::Complete);
        assert_eq!(report.ticket_coverage.incomplete_samples, 0);
        assert_eq!(report.ticket_coverage.uncovered_tail_secs, None);
        assert_eq!(report.event_coverage.coverage, Coverage::Complete);
        assert_eq!(report.delivered_during_run, 0);
        assert!(report.checks["ticket-coverage"].passed);
        assert!(report.checks["event-coverage"].passed);
        assert!(report.checks["forced-landings"].passed);
        assert!(report.checks["duplicate-landings"].passed);
    }

    /// A gap that a minimum-count requirement would never surface on its own
    /// (a floor of zero is trivially met by an unproven zero) must still be
    /// visible and fail qualification closed — it cannot "disappear when
    /// another source is healthy" (acceptance, TKT-durap-simip-nadim).
    #[test]
    fn qualification_fails_on_an_uncovered_ticket_tail_even_when_the_minimum_is_zero() {
        let (dir, manifest) = fixture();
        let contract = contract_fixture(&manifest);
        assert_eq!(contract.workload.min_root_deliveries, 0);
        freeze_test_contract(&dir, &contract);
        let first = sample(1, "2026-09-02T00:00:30Z");
        let mut second = sample(2, "2026-09-02T00:01:00Z");
        second.errors = vec!["work.current: RPC deadline exceeded; remaining reads skipped".into()];
        for value in [first, second] {
            append_json_line(&dir.path().join(SAMPLES), &value).unwrap();
        }
        let result = derive_qualification(dir.path()).unwrap();
        assert!(!result.qualified);
        let root_deliveries = result
            .checks
            .iter()
            .find(|check| check.requirement == "workload/root-deliveries")
            .unwrap();
        assert!(
            root_deliveries.passed,
            "a floor of zero is trivially met even by an unproven count: {root_deliveries:?}"
        );
        let coverage = result
            .checks
            .iter()
            .find(|check| check.requirement == "workload/delivery-coverage")
            .unwrap();
        assert!(!coverage.passed, "{coverage:?}");
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
                progress_stall_after_secs: 900,
                max_wait_secs: 1800,
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
            declared_interventions: vec![],
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

    /// The exact counterexample recorded against TKT-humih-nusok-lozus / D1:
    /// a fixture with a stale running agent (1h-old progress evidence, 1s
    /// stall bound) produced zero stale tickets and would have passed a
    /// derived report even with an ad-hoc intervention recorded, because a
    /// failed supervisor sweep left the daemon calling the generation
    /// `running` with nothing to independently contradict it.
    fn stalled_agent_fixture() -> Value {
        json!({
            "spawn": "S1",
            "session_id": "SESSION-1",
            "task": "TKT-1",
            "state": "running",
            "progress": {"revision": 3, "status": "implementing", "next": null, "updated_at": "2026-09-02T00:00:00Z"},
            "liveness": {"output_fingerprint": 42},
        })
    }

    #[test]
    fn queued_admission_uses_durable_age_and_survives_progress_replay() {
        let (_, mut manifest) = fixture();
        manifest.thresholds.progress_stall_after_secs = 1;
        manifest.thresholds.max_wait_secs = 60;
        let mut first = sample(1, "2026-09-02T00:01:00Z");
        let mut agent = stalled_agent_fixture();
        agent["repo_name"] = json!("repo");
        agent["created_at"] = json!("2026-09-02T00:00:00Z");
        first.agents = vec![agent];
        first.tickets = vec![json!({"identity":"TKT-1","alias":"TKT-alias"})];
        first.status = Some(json!({"landing_queue_tasks":[{
            "repo":"repo", "task":"TKT-alias", "source_spawn":"S1",
            "status":"queued", "phase_age_secs":30
        }]}));
        let mut states = BTreeMap::new();
        assert_eq!(
            advance_sample_progress(&mut states, &first, &manifest.thresholds, &[]).stalled,
            0
        );
        let mut second = first.clone();
        second.observed_at += chrono::Duration::seconds(31);
        second.status.as_mut().unwrap()["landing_queue_tasks"][0]["phase_age_secs"] = json!(61);
        // Content chatter cannot extend an authoritative queue phase.
        second.agents[0]["progress"]["summary"] = json!("still queued");
        assert_eq!(
            advance_sample_progress(&mut states, &second, &manifest.thresholds, &[]).stalled,
            1
        );
        let mut samples = vec![first.clone(), second.clone()];
        assert_eq!(replay_progress(&mut samples, &manifest.thresholds).len(), 1);
        assert_eq!(samples[1].metrics.progress_stalled_tickets, 1);
        // Starting observation after the wait expired is already a stall.
        assert_eq!(
            advance_sample_progress(&mut BTreeMap::new(), &second, &manifest.thresholds, &[])
                .stalled,
            1
        );
        let mut missing = first;
        missing.agents[0]["progress"] = Value::Null;
        missing.agents[0]["liveness"] = Value::Null;
        let metrics =
            advance_sample_progress(&mut BTreeMap::new(), &missing, &manifest.thresholds, &[]);
        assert_eq!(
            metrics.unresolved, 0,
            "bound queue identity supplies evidence"
        );
    }

    #[test]
    fn queued_admission_never_excuses_another_repo_generation_or_completed_phase() {
        let mut sample = sample(1, "2026-09-02T00:01:00Z");
        let mut agent = stalled_agent_fixture();
        agent["repo_name"] = json!("repo");
        agent["created_at"] = json!("2026-09-02T00:00:00Z");
        let good = json!({"repo":"repo", "task":"TKT-1", "source_spawn":"S1",
            "status":"queued", "phase_age_secs":30});
        sample.status = Some(json!({"landing_queue_tasks":[good.clone()]}));
        assert!(landing_wait_since(&sample, &agent).is_some());
        for (field, value) in [
            ("repo", json!("other")),
            ("task", json!("TKT-2")),
            ("source_spawn", json!("old-generation")),
            ("status", json!("running_gates")),
            ("phase_age_secs", json!(-1)),
            ("phase_age_secs", json!(u64::MAX)),
        ] {
            let mut row = good.clone();
            row[field] = value;
            sample.status = Some(json!({"landing_queue_tasks":[row]}));
            assert!(landing_wait_since(&sample, &agent).is_none(), "bad {field}");
        }
        let mut legacy = good;
        legacy["source_spawn"] = Value::Null;
        sample.status = Some(json!({"landing_queue_tasks":[legacy]}));
        assert!(landing_wait_since(&sample, &agent).is_some());
        agent["created_at"] = json!("2026-09-02T00:00:59Z");
        assert!(landing_wait_since(&sample, &agent).is_none());
    }

    #[test]
    fn stale_running_agent_is_independently_flagged_even_though_the_daemon_calls_it_live() {
        let (_, mut manifest) = fixture();
        manifest.thresholds.progress_stall_after_secs = 1;
        let agent = stalled_agent_fixture();
        let ticket = json!({
            "identity": "TKT-1",
            "created_at": "2026-09-01T00:00:00Z",
            "payload": {
                "status": "in_progress",
                "updated_at": "2026-09-02T00:00:00Z",
                "delivery": null,
            },
        });
        let mut first = sample(1, "2026-09-02T00:00:00Z");
        first.agents = vec![agent.clone()];
        first.tickets = vec![ticket.clone()];
        let metrics_first = derive_sample_metrics(&first, &manifest, &[]);
        assert_eq!(
            metrics_first.progress_stalled_tickets, 0,
            "a fresh generation must not inherit an immediate stall"
        );

        let mut second = sample(2, "2026-09-02T01:00:00Z");
        second.agents = vec![agent];
        second.tickets = vec![ticket];

        // The daemon's own classification still calls this healthy: a live
        // agent excludes the ticket from ownerless staleness exactly as in
        // `ownerless_active_status_uses_last_ticket_update_for_staleness`.
        let metrics_second =
            derive_sample_metrics(&second, &manifest, std::slice::from_ref(&first));
        assert_eq!(metrics_second.stale_tickets, 0);
        // But an hour of byte-identical progress evidence against a
        // 1-second bound is independently a stall.
        assert_eq!(metrics_second.progress_stalled_tickets, 1);
        assert_eq!(metrics_second.progress_unresolved_tickets, 0);
        assert_eq!(metrics_second.progress_stall_episodes, 1);
    }

    #[test]
    fn a_replacement_generation_does_not_inherit_a_predecessors_stall() {
        let (_, mut manifest) = fixture();
        manifest.thresholds.progress_stall_after_secs = 1;
        let mut stalled = stalled_agent_fixture();
        let mut first = sample(1, "2026-09-02T00:00:00Z");
        first.agents = vec![stalled.clone()];
        let mut second = sample(2, "2026-09-02T01:00:00Z");
        second.agents = vec![stalled.clone()];
        assert_eq!(
            derive_sample_metrics(&second, &manifest, std::slice::from_ref(&first))
                .progress_stalled_tickets,
            1
        );

        // A resume/replacement mints a new generation identity. It must
        // start clean, not inherit the predecessor's silence clock.
        stalled["spawn"] = json!("S2");
        stalled["progress"]["revision"] = json!(0);
        let mut third = sample(3, "2026-09-02T01:00:01Z");
        third.agents = vec![stalled];
        let metrics_third =
            derive_sample_metrics(&third, &manifest, &[first.clone(), second.clone()]);
        assert_eq!(
            metrics_third.progress_stalled_tickets, 0,
            "a replacement generation must not inherit a predecessor's proof of progress"
        );
    }

    #[test]
    fn a_declared_verification_wait_is_exempt_until_its_own_deadline_expires() {
        let (_, mut manifest) = fixture();
        manifest.thresholds.progress_stall_after_secs = 1;
        manifest.thresholds.max_wait_secs = 3600;
        let mut waiting = stalled_agent_fixture();
        waiting["progress"]["status"] = json!("verifying: cargo test --workspace");
        let first = {
            let mut sample = sample(1, "2026-09-02T00:00:00Z");
            sample.agents = vec![waiting.clone()];
            sample
        };
        let mut second = sample(2, "2026-09-02T00:30:00Z");
        second.agents = vec![waiting.clone()];
        let metrics_second =
            derive_sample_metrics(&second, &manifest, std::slice::from_ref(&first));
        assert_eq!(
            metrics_second.progress_stalled_tickets, 0,
            "a declared verification wait must stay exempt within its allowance"
        );

        // Past its own deadline, the same unchanged wait cannot keep the run
        // healthy.
        let mut third = sample(3, "2026-09-02T02:00:00Z");
        third.agents = vec![waiting];
        let metrics_third = derive_sample_metrics(&third, &manifest, &[first, second]);
        assert_eq!(
            metrics_third.progress_stalled_tickets, 1,
            "an expired bounded-wait exemption must not keep the run healthy"
        );
    }

    #[test]
    fn progress_attempt_binding_and_repeated_chatter_do_not_reset_clocks() {
        let (_, mut manifest) = fixture();
        manifest.thresholds.progress_stall_after_secs = 1;
        let now: DateTime<Utc> = "2026-09-02T00:00:00Z".parse().unwrap();
        let mut agent = stalled_agent_fixture();
        agent["liveness"]["session"] = json!("attempt-one");
        agent["progress"]["summary"] = json!("same checkpoint");
        let (_, first) = advance_progress_state(None, &agent, now, &manifest.thresholds, None);
        agent["progress"]["revision"] = json!(999);
        agent["updated_at"] = json!(now + chrono::Duration::seconds(10));
        let (reading, second) = advance_progress_state(
            Some(&first),
            &agent,
            now + chrono::Duration::seconds(10),
            &manifest.thresholds,
            None,
        );
        assert_eq!(
            reading,
            ProgressReading::Stalled,
            "repeated checkpoint is not progress"
        );
        agent["liveness"]["reconnect_events"] = json!(2);
        agent["liveness"]["output_fingerprint"] = json!(12345);
        let (reading, _) = advance_progress_state(
            Some(&second),
            &agent,
            now + chrono::Duration::seconds(11),
            &manifest.thresholds,
            None,
        );
        assert_eq!(
            reading,
            ProgressReading::Stalled,
            "transport noise is not progress"
        );
        agent["liveness"]["session"] = json!("attempt-two");
        let (reading, _) = advance_progress_state(
            Some(&second),
            &agent,
            now + chrono::Duration::seconds(12),
            &manifest.thresholds,
            None,
        );
        assert_eq!(
            reading,
            ProgressReading::Progressing,
            "same provider session can host a fresh RK execution attempt"
        );
        let mut same_attempt = stalled_agent_fixture();
        same_attempt["liveness"]["session"] = json!("attempt-one");
        let (reading, backwards) = advance_progress_state(
            Some(&second),
            &same_attempt,
            now - chrono::Duration::seconds(1),
            &manifest.thresholds,
            None,
        );
        assert_eq!(reading, ProgressReading::Unresolved);
        assert_eq!(backwards.changed_at, second.changed_at);
    }

    #[test]
    fn wait_deadlines_and_cumulative_episodes_survive_churn_and_retirement() {
        let (_, mut manifest) = fixture();
        manifest.thresholds.progress_stall_after_secs = 1;
        manifest.thresholds.max_wait_secs = 2;
        let mut states = BTreeMap::new();
        let mut first = sample(1, "2026-09-02T00:00:00Z");
        let mut agent = stalled_agent_fixture();
        agent["progress"]["status"] = json!("verifying: unit tests");
        first.agents = vec![agent.clone()];
        assert_eq!(
            advance_sample_progress(&mut states, &first, &manifest.thresholds, &[]).episodes,
            0
        );
        let mut second = sample(2, "2026-09-02T00:00:03Z");
        agent["progress"]["summary"] = json!("still verifying");
        second.agents = vec![agent.clone()];
        assert_eq!(
            advance_sample_progress(&mut states, &second, &manifest.thresholds, &[]).stalled,
            1
        );
        agent["progress"]["summary"] = json!("still waiting again");
        let mut third = sample(3, "2026-09-02T00:00:04Z");
        third.agents = vec![agent.clone()];
        assert_eq!(
            advance_sample_progress(&mut states, &third, &manifest.thresholds, &[]).episodes,
            1,
            "status chatter cannot renew a wait or create another stall onset"
        );
        agent["state"] = json!("completed");
        let mut fourth = sample(4, "2026-09-02T00:00:05Z");
        fourth.agents = vec![agent];
        let metrics = advance_sample_progress(&mut states, &fourth, &manifest.thresholds, &[]);
        assert_eq!(metrics.stalled, 0);
        assert_eq!(metrics.episodes, 1);
        assert_eq!(
            states.values().next().unwrap().history[0].resolved_at,
            Some(fourth.observed_at)
        );
        let mut another = stalled_agent_fixture();
        another["task"] = json!("TKT-2");
        fourth.agents = vec![another];
        advance_sample_progress(&mut states, &fourth, &manifest.thresholds, &[]);
        let mut fifth = sample(5, "2026-09-02T00:00:08Z");
        fifth.agents = fourth.agents;
        assert_eq!(
            advance_sample_progress(&mut states, &fifth, &manifest.thresholds, &[]).episodes,
            2,
            "a retired ticket's incident must not disappear when a different ticket stalls"
        );
    }

    #[test]
    fn progress_report_replays_raw_evidence_and_rebuilds_checkpoint_identically() {
        let (dir, mut manifest) = fixture();
        manifest.thresholds.progress_stall_after_secs = 1;
        write_json_atomic(&dir.path().join(MANIFEST), &manifest).unwrap();
        let mut first = sample(1, "2026-09-02T00:00:00Z");
        first.agents = vec![stalled_agent_fixture()];
        let mut second = sample(2, "2026-09-02T00:00:05Z");
        second.agents = first.agents.clone();
        // Deliberately persist zero cached metrics; report must derive truth
        // from raw evidence instead of trusting an earlier evaluator's cache.
        append_json_line(&dir.path().join(SAMPLES), &first).unwrap();
        append_json_line(&dir.path().join(SAMPLES), &second).unwrap();
        let log = ObservationLog::open(dir.path(), &manifest).unwrap();
        let mut third = sample(3, "2026-09-02T00:00:06Z");
        third.agents = first.agents;
        let live = log.progress_metrics(&third).unwrap();
        assert_eq!(live.stalled, 1);
        drop(log);
        let report = derive_report(dir.path()).unwrap();
        assert_eq!(report.progress_stall_episodes, 1);
        assert_eq!(report.progress_episodes.len(), 1);
        assert!(!report.checks["progress-stalled-tickets"].passed);
        fs::remove_file(dir.path().join("collector.json")).unwrap();
        let rebuilt = ObservationLog::open(dir.path(), &manifest).unwrap();
        assert_eq!(
            rebuilt.progress_metrics(&third).unwrap().stalled,
            live.stalled
        );
        assert_eq!(
            rebuilt.progress_metrics(&third).unwrap().episodes,
            live.episodes
        );
    }

    #[test]
    fn missing_generation_identity_is_a_visible_evidence_gap_not_a_silent_pass() {
        let (_, manifest) = fixture();
        let mut value = sample(1, "2026-09-02T00:00:00Z");
        value.agents = vec![json!({"task": "TKT-1", "state": "running"})];
        let metrics = derive_sample_metrics(&value, &manifest, &[]);
        assert_eq!(metrics.progress_stalled_tickets, 0);
        assert_eq!(metrics.progress_unresolved_tickets, 1);
    }

    #[test]
    fn stale_running_agent_report_fails_even_with_an_ad_hoc_intervention_recorded() {
        let (dir, mut manifest) = fixture();
        manifest.thresholds.progress_stall_after_secs = 1;
        write_json_atomic(&dir.path().join(MANIFEST), &manifest).unwrap();

        let agent = stalled_agent_fixture();
        let ticket = json!({
            "identity": "TKT-1",
            "created_at": "2026-09-01T00:00:00Z",
            "payload": {
                "status": "in_progress",
                "updated_at": "2026-09-02T00:00:00Z",
                "delivery": null,
            },
        });
        let mut first = sample(1, "2026-09-02T00:00:00Z");
        first.agents = vec![agent.clone()];
        first.tickets = vec![ticket.clone()];
        let mut second = sample(2, "2026-09-02T01:00:00Z");
        second.agents = vec![agent];
        second.tickets = vec![ticket];

        // Drive both samples through the real collector path (not a hand-set
        // metrics field): `ObservationLog` must independently persist and
        // read back the same stall across the checkpoint boundary a live
        // `rk observe sample` would use.
        let mut log = ObservationLog::open(dir.path(), &manifest).unwrap();
        for mut value in [first, second] {
            let progress = log.progress_metrics(&value).unwrap();
            value.metrics = derive_metrics_with_ready_age(
                &value,
                &manifest,
                |t| log.ready_age(t, value.observed_at),
                progress,
            );
            log.append(&value).unwrap();
        }
        drop(log);

        let intervention = Intervention {
            schema_version: SCHEMA_VERSION,
            id: "int-1".into(),
            observed_at: "2026-09-02T01:00:00Z".parse().unwrap(),
            class: InterventionClass::AdHoc,
            summary: "an ad-hoc rescue that must not paper over the stall".into(),
            ticket: Some("TKT-1".into()),
            actor: "operator".into(),
            evidence: vec![],
            owner: None,
            spawn: None,
        };
        write_new_json(
            &dir.path().join(INTERVENTIONS).join("int-1.json"),
            &intervention,
        )
        .unwrap();

        let report = derive_report(dir.path()).unwrap();
        assert_eq!(
            report.max_stale_tickets, 0,
            "the daemon still calls this generation live"
        );
        assert!(!report.checks["progress-stalled-tickets"].passed);
        assert!(
            !report.passed,
            "an independently stalled generation must fail the run even with an \
             ad-hoc intervention recorded and zero daemon-reported stale tickets"
        );
    }

    #[test]
    fn a_declared_human_gate_excuses_only_its_own_ticket_owner_and_generation() {
        let (_, mut manifest) = fixture();
        manifest.thresholds.progress_stall_after_secs = 1;
        manifest.thresholds.max_wait_secs = 60;

        let mut agent = stalled_agent_fixture();
        agent["name"] = json!("Gruyere-14");
        let matching = Intervention {
            schema_version: SCHEMA_VERSION,
            id: "gate-1".into(),
            observed_at: "2026-09-02T00:00:00Z".parse().unwrap(),
            class: InterventionClass::HumanGate,
            summary: "waiting on operator sign-off".into(),
            ticket: Some("TKT-1".into()),
            actor: "chaz".into(),
            evidence: vec!["bbs:need-01".into()],
            owner: Some("Gruyere-14".into()),
            spawn: Some("S1".into()),
        };

        let now: DateTime<Utc> = "2026-09-02T00:30:00Z".parse().unwrap();
        // The unmodified declaration is the positive control: it must match
        // before any of the negative mutations below are trusted.
        assert!(
            declared_gate_wait_since(std::slice::from_ref(&matching), &agent, now).is_some(),
            "control declaration should match"
        );

        // Wrong ticket, wrong owner, wrong generation and wrong class must
        // never excuse.
        let mut wrong_ticket = matching.clone();
        wrong_ticket.ticket = Some("TKT-2".into());
        assert!(
            declared_gate_wait_since(std::slice::from_ref(&wrong_ticket), &agent, now).is_none()
        );

        let mut wrong_owner = matching.clone();
        wrong_owner.owner = Some("Other-Rat".into());
        assert!(
            declared_gate_wait_since(std::slice::from_ref(&wrong_owner), &agent, now).is_none()
        );

        let mut missing_owner = matching.clone();
        missing_owner.owner = None;
        assert!(
            declared_gate_wait_since(std::slice::from_ref(&missing_owner), &agent, now).is_none(),
            "missing owner is not authority evidence"
        );

        let mut wrong_spawn = matching.clone();
        wrong_spawn.spawn = Some("S2".into());
        assert!(
            declared_gate_wait_since(std::slice::from_ref(&wrong_spawn), &agent, now).is_none()
        );

        let mut missing_spawn = matching.clone();
        missing_spawn.spawn = None;
        assert!(
            declared_gate_wait_since(std::slice::from_ref(&missing_spawn), &agent, now).is_none(),
            "missing generation identity is not a trusted exemption"
        );

        let mut wrong_class = matching.clone();
        wrong_class.class = InterventionClass::AdHoc;
        assert!(
            declared_gate_wait_since(std::slice::from_ref(&wrong_class), &agent, now).is_none()
        );

        // A declaration recorded after the sample it would apply to cannot
        // retroactively excuse that earlier sample.
        let mut future = matching.clone();
        future.observed_at = "2026-09-02T00:05:00Z".parse().unwrap();
        assert!(
            declared_gate_wait_since(
                std::slice::from_ref(&future),
                &agent,
                "2026-09-02T00:01:00Z".parse().unwrap()
            )
            .is_none(),
            "a later declaration must not retroactively excuse an earlier sample"
        );

        // The exact matching declaration exempts the stall within its
        // allowance, and stalls again once the frozen bound expires.
        let first = {
            let mut sample = sample(1, "2026-09-02T00:00:00Z");
            sample.agents = vec![agent.clone()];
            sample
        };
        let mut second = sample(2, "2026-09-02T00:00:30Z");
        second.agents = vec![agent.clone()];
        let metrics_second = derive_sample_metrics_with_interventions(
            &second,
            &manifest,
            std::slice::from_ref(&first),
            std::slice::from_ref(&matching),
        );
        assert_eq!(
            metrics_second.progress_stalled_tickets, 0,
            "a declared human gate must stay exempt within its allowance"
        );

        let mut third = sample(3, "2026-09-02T00:05:00Z");
        third.agents = vec![agent];
        let metrics_third = derive_sample_metrics_with_interventions(
            &third,
            &manifest,
            &[first, second],
            std::slice::from_ref(&matching),
        );
        assert_eq!(
            metrics_third.progress_stalled_tickets, 1,
            "an expired declared-gate exemption must not keep the run healthy"
        );
    }

    #[test]
    fn duplicate_human_gate_declarations_cannot_extend_the_same_allowance() {
        let (_, mut manifest) = fixture();
        manifest.thresholds.progress_stall_after_secs = 1;
        manifest.thresholds.max_wait_secs = 10;
        let mut agent = stalled_agent_fixture();
        agent["name"] = json!("Gruyere-14");
        let gate = |id: &str, observed_at: &str| Intervention {
            schema_version: SCHEMA_VERSION,
            id: id.into(),
            observed_at: observed_at.parse().unwrap(),
            class: InterventionClass::HumanGate,
            summary: "waiting on operator sign-off".into(),
            ticket: Some("TKT-1".into()),
            actor: "chaz".into(),
            evidence: vec![],
            owner: Some("Gruyere-14".into()),
            spawn: Some("S1".into()),
        };
        // A second, later declaration for the exact same gate must not push
        // the deadline out past the first declaration's own allowance.
        let interventions = vec![
            gate("gate-1", "2026-09-02T00:00:00Z"),
            gate("gate-2", "2026-09-02T00:00:05Z"),
        ];
        let mut states = BTreeMap::new();
        let first = {
            let mut sample = sample(1, "2026-09-02T00:00:06Z");
            sample.agents = vec![agent.clone()];
            sample
        };
        advance_sample_progress(&mut states, &first, &manifest.thresholds, &interventions);
        // 11s after the *first* declaration (past its 10s allowance), even
        // though it is only 6s after the duplicate re-declaration.
        let mut second = sample(2, "2026-09-02T00:00:11Z");
        second.agents = vec![agent];
        let metrics =
            advance_sample_progress(&mut states, &second, &manifest.thresholds, &interventions);
        assert_eq!(
            metrics.stalled, 1,
            "a duplicate declaration must not extend the original gate's deadline"
        );
    }

    #[test]
    fn a_generation_replacement_does_not_inherit_a_predecessors_human_gate() {
        let (_, mut manifest) = fixture();
        manifest.thresholds.progress_stall_after_secs = 1;
        manifest.thresholds.max_wait_secs = 3600;
        let mut agent = stalled_agent_fixture();
        agent["name"] = json!("Gruyere-14");
        let interventions = vec![Intervention {
            schema_version: SCHEMA_VERSION,
            id: "gate-1".into(),
            observed_at: "2026-09-02T00:00:00Z".parse().unwrap(),
            class: InterventionClass::HumanGate,
            summary: "waiting on operator sign-off".into(),
            ticket: Some("TKT-1".into()),
            actor: "chaz".into(),
            evidence: vec![],
            owner: Some("Gruyere-14".into()),
            spawn: Some("S1".into()),
        }];
        let mut states = BTreeMap::new();
        let first = {
            let mut sample = sample(1, "2026-09-02T00:00:00Z");
            sample.agents = vec![agent.clone()];
            sample
        };
        advance_sample_progress(&mut states, &first, &manifest.thresholds, &interventions);

        // A respawn mints a new generation (new spawn) that the recorded
        // gate was never declared for.
        agent["spawn"] = json!("S2");
        let mut second = sample(2, "2026-09-02T00:00:01Z");
        second.agents = vec![agent];
        let metrics =
            advance_sample_progress(&mut states, &second, &manifest.thresholds, &interventions);
        assert_eq!(
            metrics.unresolved, 0,
            "a fresh generation with real progress evidence is resolved, not unresolved"
        );
        assert_eq!(
            metrics.stalled, 0,
            "a fresh generation starts its own clock rather than inheriting a stall"
        );
    }

    #[test]
    fn declared_wait_retires_on_productive_checkpoint_and_replay_matches() {
        let (dir, mut manifest) = fixture();
        manifest.thresholds.max_wait_secs = 2;
        manifest.thresholds.progress_stall_after_secs = 1;
        let mut first = sample(1, "2026-09-02T00:00:00Z");
        let mut agent = stalled_agent_fixture();
        agent["name"] = json!("Worker");
        agent["task"] = json!("TKT-alias");
        first.agents = vec![agent];
        first.tickets = vec![json!({"identity":"TKT-1", "alias":"TKT-alias"})];
        first.declared_interventions = vec![Intervention {
            schema_version: SCHEMA_VERSION,
            id: "gate-1".into(),
            observed_at: first.observed_at,
            class: InterventionClass::HumanGate,
            summary: "operator decision".into(),
            ticket: Some("TKT-alias".into()),
            actor: "operator".into(),
            evidence: vec![],
            owner: Some("Worker".into()),
            spawn: Some("S1".into()),
        }];
        let mut waiting = first.clone();
        waiting.sequence = 2;
        waiting.observed_at += chrono::Duration::seconds(2);
        waiting.agents[0]["liveness"]["output_fingerprint"] = json!(43);
        let mut expired = waiting.clone();
        expired.sequence = 3;
        expired.observed_at += chrono::Duration::seconds(1);
        let mut resumed = expired.clone();
        resumed.sequence = 4;
        resumed.observed_at += chrono::Duration::seconds(1);
        resumed.agents[0]["progress"]["summary"] = json!("implemented the selected behavior");
        let mut silent = resumed.clone();
        silent.sequence = 5;
        silent.observed_at += chrono::Duration::seconds(2);
        // Even a repeated declaration cannot grant a new allowance after resumption.
        let mut duplicate = first.declared_interventions[0].clone();
        duplicate.id = "gate-2".into();
        duplicate.observed_at = resumed.observed_at;
        silent.declared_interventions.push(duplicate);
        let mut samples = vec![first, waiting, expired, resumed, silent];
        let mut log = ObservationLog::open(dir.path(), &manifest).unwrap();
        let mut counts = Vec::new();
        for sample in &samples {
            counts.push(log.progress_metrics(sample).unwrap().stalled);
            log.append(sample).unwrap();
        }
        assert_eq!(counts, [0, 0, 1, 0, 1]);
        drop(log);
        fs::remove_file(dir.path().join("collector.json")).unwrap();
        let rebuilt = ObservationLog::open(dir.path(), &manifest).unwrap();
        assert_eq!(
            rebuilt
                .progress_metrics(samples.last().unwrap())
                .unwrap()
                .stalled,
            1
        );
        let episodes = replay_progress(&mut samples, &manifest.thresholds);
        assert_eq!(episodes.len(), 2);
        assert_eq!(episodes[0].resolved_at, Some(samples[3].observed_at));
        assert_eq!(
            samples
                .iter()
                .map(|s| s.metrics.progress_stalled_tickets)
                .collect::<Vec<_>>(),
            counts
        );
    }

    #[test]
    fn declared_wait_rejects_ambiguous_alias_and_cannot_reactivate_after_session_change() {
        let (_, mut manifest) = fixture();
        manifest.thresholds.max_wait_secs = 60;
        manifest.thresholds.progress_stall_after_secs = 1;
        let mut value = sample(1, "2026-09-02T00:00:00Z");
        let mut agent = stalled_agent_fixture();
        agent["name"] = json!("Worker");
        value.agents = vec![agent];
        value.tickets = vec![
            json!({"identity":"TKT-1","alias":"ambiguous"}),
            json!({"identity":"TKT-2","alias":"ambiguous"}),
        ];
        let mut gate = Intervention {
            schema_version: SCHEMA_VERSION,
            id: "gate".into(),
            observed_at: value.observed_at,
            class: InterventionClass::HumanGate,
            summary: "decision".into(),
            ticket: Some("ambiguous".into()),
            actor: "operator".into(),
            evidence: vec![],
            owner: Some("Worker".into()),
            spawn: Some("S1".into()),
        };
        let mut states = BTreeMap::new();
        advance_sample_progress(&mut states, &value, &manifest.thresholds, &[gate.clone()]);
        value.observed_at += chrono::Duration::seconds(3);
        assert_eq!(
            advance_sample_progress(&mut states, &value, &manifest.thresholds, &[gate.clone()])
                .stalled,
            1
        );
        value.agents[0]["task"] = json!("ambiguous");
        assert_eq!(
            advance_sample_progress(&mut states, &value, &manifest.thresholds, &[gate.clone()])
                .unresolved,
            1
        );
        value.agents[0]["task"] = json!("TKT-1");
        gate.ticket = Some("TKT-1".into());
        states.clear();
        advance_sample_progress(&mut states, &value, &manifest.thresholds, &[gate.clone()]);
        value.observed_at += chrono::Duration::seconds(1);
        value.agents[0]["progress"]["summary"] = json!("implemented decision");
        advance_sample_progress(&mut states, &value, &manifest.thresholds, &[gate.clone()]);
        assert!(states.values().any(|s| s.declared_wait_retired));
        value.observed_at += chrono::Duration::seconds(1);
        value.agents[0]["liveness"]["session"] = json!("new-attempt");
        advance_sample_progress(&mut states, &value, &manifest.thresholds, &[gate.clone()]);
        value.observed_at += chrono::Duration::seconds(2);
        assert_eq!(
            advance_sample_progress(&mut states, &value, &manifest.thresholds, &[gate]).stalled,
            1
        );
    }

    #[test]
    fn later_declaration_with_reversed_clock_cannot_rewrite_frozen_samples() {
        let (dir, mut manifest) = fixture();
        manifest.thresholds.progress_stall_after_secs = 1;
        manifest.thresholds.max_wait_secs = 60;
        let mut first = sample(1, "2026-09-02T00:00:00Z");
        let mut agent = stalled_agent_fixture();
        agent["name"] = json!("Worker");
        first.agents = vec![agent];
        let mut second = first.clone();
        second.sequence = 2;
        second.observed_at += chrono::Duration::seconds(3);
        let mut log = ObservationLog::open(dir.path(), &manifest).unwrap();
        log.capture_interventions(&mut first).unwrap();
        log.append(&first).unwrap();
        log.capture_interventions(&mut second).unwrap();
        assert_eq!(log.progress_metrics(&second).unwrap().stalled, 1);
        log.append(&second).unwrap();
        drop(log);
        let late = Intervention {
            schema_version: SCHEMA_VERSION,
            id: "late".into(),
            observed_at: first.observed_at,
            class: InterventionClass::HumanGate,
            summary: "written later with rolled-back wall clock".into(),
            ticket: Some("TKT-1".into()),
            actor: "operator".into(),
            evidence: vec![],
            owner: Some("Worker".into()),
            spawn: Some("S1".into()),
        };
        write_new_json(&dir.path().join(INTERVENTIONS).join("late.json"), &late).unwrap();
        fs::remove_file(dir.path().join("collector.json")).unwrap();
        let rebuilt = ObservationLog::open(dir.path(), &manifest).unwrap();
        assert_eq!(rebuilt.progress_metrics(&second).unwrap().stalled, 1);
        let mut frozen = vec![first, second];
        assert_eq!(replay_progress(&mut frozen, &manifest.thresholds).len(), 1);
        assert_eq!(frozen[1].metrics.progress_stalled_tickets, 1);
    }

    #[test]
    fn historical_intervention_records_missing_owner_and_spawn_compat() {
        let dir = TempDir::new().unwrap();
        let legacy_path = dir.path().join("legacy.json");
        fs::write(
            &legacy_path,
            r#"{"schema_version":1,"id":"legacy-1","observed_at":"2026-09-02T00:00:00Z",
               "class":"human-gate","summary":"pre-D1 record","ticket":"TKT-1",
               "actor":"operator","evidence":[]}"#,
        )
        .unwrap();
        let bytes = fs::read(&legacy_path).unwrap();
        let legacy: Intervention = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(legacy.owner, None);
        assert_eq!(legacy.spawn, None);

        let mut agent = stalled_agent_fixture();
        agent["name"] = json!("Gruyere-14");
        assert!(
            declared_gate_wait_since(
                std::slice::from_ref(&legacy),
                &agent,
                "2026-09-02T00:00:01Z".parse().unwrap()
            )
            .is_none(),
            "a historical record with no owner/spawn identity must never grant an exemption"
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

    /// Reproduces the trial evidence directly: a spawn's own ledger climbs
    /// while `running`, then a `paused` turn-boundary reconciliation from the
    /// harness corrects `cost_usd`/`usage` DOWN on the same spawn (no
    /// restart, no new session), before a later `completed` sample settles
    /// it slightly above the correction. The pre-fix evaluator (max across
    /// samples) would have attributed the transient `running` peak (14.0);
    /// the fix must attribute the settled final reading (7.5) instead.
    #[test]
    fn a_same_spawn_cost_correction_is_reconciled_to_the_settled_reading_not_the_peak() {
        let (dir, _) = fixture();
        let agent = |cost: f64, state: &str| {
            json!({"spawn": "S1", "name": "Pretzel-14", "created_at": "2026-09-02T00:00:00Z",
                   "state": state, "cost_usd": cost, "usage": {"output": 100}})
        };
        let mut peak = sample(1, "2026-09-02T00:00:30Z");
        peak.agents = vec![agent(14.0, "running")];
        let mut corrected = sample(2, "2026-09-02T00:01:00Z");
        corrected.agents = vec![agent(6.9, "paused")];
        let mut settled = sample(3, "2026-09-02T00:01:30Z");
        settled.agents = vec![agent(7.5, "completed")];
        for value in [peak, corrected, settled] {
            append_json_line(&dir.path().join(SAMPLES), &value).unwrap();
        }
        let report = derive_report(dir.path()).unwrap();
        assert!(
            (report.attributed_cost_usd - 7.5).abs() < 0.0001,
            "expected the settled final reading, got {}",
            report.attributed_cost_usd
        );
        assert_eq!(report.usage_coverage.coverage, Coverage::Complete);
        assert_eq!(report.usage_coverage.incomplete_samples, 0);
    }

    /// A generation still live at the run's last sample has not received its
    /// own final harness reconciliation: its folded cost is real spend so
    /// far, but provisional, and must not silently pass an under-budget
    /// check as though it were a settled total.
    #[test]
    fn a_still_live_generation_leaves_attributed_spend_coverage_incomplete() {
        let (dir, _) = fixture();
        let mut first = sample(1, "2026-09-02T00:00:30Z");
        first.agents = vec![json!({"spawn": "S1", "name": "Muenster-14",
            "created_at": "2026-09-02T00:00:00Z", "state": "running",
            "cost_usd": 0.5, "usage": {"output": 10}})];
        let mut second = sample(2, "2026-09-02T00:01:00Z");
        second.agents = vec![json!({"spawn": "S1", "name": "Muenster-14",
            "created_at": "2026-09-02T00:00:00Z", "state": "running",
            "cost_usd": 1.0, "usage": {"output": 20}})];
        for value in [first, second] {
            append_json_line(&dir.path().join(SAMPLES), &value).unwrap();
        }
        let report = derive_report(dir.path()).unwrap();
        assert!((report.attributed_cost_usd - 1.0).abs() < 0.0001);
        assert_eq!(report.usage_coverage.coverage, Coverage::Incomplete);
        assert_eq!(report.usage_coverage.incomplete_samples, 1);
        assert_eq!(report.usage_coverage.uncovered_tail_secs, Some(0));
        assert!(
            !report.checks["attributed-cost-usd"].passed,
            "an under-budget reading from a still-live generation must not pass as settled"
        );
        assert!(!report.passed);
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
                owner: None,
                spawn: None,
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

    fn contract_fixture(manifest: &Manifest) -> AcceptanceContract {
        AcceptanceContract {
            schema_version: CONTRACT_SCHEMA_VERSION,
            workload: WorkloadRequirement {
                roots: vec![],
                min_root_deliveries: 0,
                min_correction_deliveries: 0,
            },
            duration: DurationRequirement {
                min_elapsed_secs: 0,
                max_sample_gap_secs: 3600,
                coverage_tolerance_secs: 0,
            },
            liveness: LivenessRequirement {
                max_stale_tickets: 0,
                max_unclassified_holds: 0,
                max_stall_incidents: 0,
            },
            exercises: vec![],
            interventions: InterventionPolicy {
                allowed_classes: vec![],
                max_ad_hoc: 0,
            },
            build_identity: BuildIdentityRequirement {
                frozen_build: manifest.observer_build.clone(),
            },
            resources: ResourceRequirement {
                max_spend_usd: None,
                max_landing_age_secs: manifest.thresholds.max_landing_age_secs,
                max_ready_age_secs: manifest.thresholds.max_ready_age_secs,
                max_duplicate_dispatches: 0,
                max_duplicate_landings: 0,
                max_forced_landings: 0,
                max_reconcile_violations: 0,
            },
        }
    }

    fn freeze_test_contract(dir: &TempDir, contract: &AcceptanceContract) -> String {
        let digest = canonical_digest(contract).unwrap();
        let frozen = FrozenContract {
            contract: contract.clone(),
            digest: digest.clone(),
            source: Some("test".into()),
            frozen_at: Utc::now(),
        };
        write_new_json(&dir.path().join(CONTRACT), &frozen).unwrap();
        digest
    }

    fn delivered_ticket(id: &str, at: DateTime<Utc>) -> Value {
        json!({"identity": id, "scope": "repo", "payload": {
            "status": "done", "delivery": {"landed_at": at,
                "merge_commit": "commit", "target": "main"}}})
    }

    #[test]
    fn qualify_requires_a_frozen_contract() {
        let (dir, _) = fixture();
        append_json_line(
            &dir.path().join(SAMPLES),
            &sample(1, "2026-09-02T00:00:30Z"),
        )
        .unwrap();
        let error = derive_qualification(dir.path()).unwrap_err();
        assert!(
            error.to_string().contains("no frozen acceptance contract"),
            "{error}"
        );
    }

    #[test]
    fn frozen_contract_digest_detects_post_freeze_tampering() {
        let (dir, manifest) = fixture();
        let contract = contract_fixture(&manifest);
        freeze_test_contract(&dir, &contract);
        assert!(load_contract(dir.path()).unwrap().is_some());
        let mut tampered =
            serde_json::to_value(load_contract(dir.path()).unwrap().unwrap()).unwrap();
        tampered["contract"]["interventions"]["max_ad_hoc"] = json!(99);
        fs::write(dir.path().join(CONTRACT), tampered.to_string()).unwrap();
        let error = load_contract(dir.path()).unwrap_err();
        assert!(error.to_string().contains("digest mismatch"), "{error}");
    }

    #[test]
    fn freeze_contract_validates_scope_and_duration_before_measured_work_starts() {
        let (dir, manifest) = fixture();
        let mut out_of_scope = contract_fixture(&manifest);
        out_of_scope.workload.roots = vec!["TKT-not-observed".into()];
        let error =
            freeze_contract(dir.path(), Path::new("/nonexistent"), &manifest, None).unwrap_err();
        assert!(
            error.to_string().contains("read acceptance contract"),
            "{error}"
        );

        let source = dir.path().join("contract-input.json");
        fs::write(&source, serde_json::to_string(&out_of_scope).unwrap()).unwrap();
        let error = freeze_contract(dir.path(), &source, &manifest, None).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("outside this run's observed tickets"),
            "{error}"
        );

        let mut needs_duration = contract_fixture(&manifest);
        needs_duration.duration.min_elapsed_secs = 600;
        fs::write(&source, serde_json::to_string(&needs_duration).unwrap()).unwrap();
        let error = freeze_contract(dir.path(), &source, &manifest, None).unwrap_err();
        assert!(
            error.to_string().contains("no planned --duration"),
            "{error}"
        );
    }

    #[test]
    fn qualification_fails_with_no_useful_workload() {
        let (dir, manifest) = fixture();
        let mut contract = contract_fixture(&manifest);
        contract.workload.min_root_deliveries = 1;
        freeze_test_contract(&dir, &contract);
        append_json_line(
            &dir.path().join(SAMPLES),
            &sample(1, "2026-09-02T00:00:30Z"),
        )
        .unwrap();
        let result = derive_qualification(dir.path()).unwrap();
        assert!(!result.qualified);
        let workload = result
            .checks
            .iter()
            .find(|check| check.requirement == "workload/root-deliveries")
            .unwrap();
        assert!(!workload.passed, "{workload:?}");
    }

    #[test]
    fn qualification_passes_with_useful_workload_and_zero_interventions() {
        let (dir, manifest) = fixture();
        let mut contract = contract_fixture(&manifest);
        contract.workload.min_root_deliveries = 1;
        let digest = freeze_test_contract(&dir, &contract);
        let mut delivered = sample(1, "2026-09-02T00:01:00Z");
        delivered.tickets = vec![delivered_ticket("TKT-1", delivered.observed_at)];
        append_json_line(&dir.path().join(SAMPLES), &delivered).unwrap();
        let result = derive_qualification(dir.path()).unwrap();
        assert!(
            result.checks.iter().all(|check| check.passed),
            "{:?}",
            result.checks
        );
        assert!(result.qualified);
        assert_eq!(result.contract_digest, digest);
        assert_eq!(result.evaluator_version, QUALIFICATION_EVALUATOR_VERSION);
    }

    #[test]
    fn qualification_fails_on_a_missing_required_exercise() {
        let (dir, manifest) = fixture();
        let mut contract = contract_fixture(&manifest);
        contract.exercises = vec![ExerciseRequirement {
            kind: ExerciseKind::WorkerDeath,
            min_count: 1,
            min_continuation_samples: 1,
        }];
        freeze_test_contract(&dir, &contract);
        append_json_line(
            &dir.path().join(SAMPLES),
            &sample(1, "2026-09-02T00:00:30Z"),
        )
        .unwrap();
        let result = derive_qualification(dir.path()).unwrap();
        assert!(!result.qualified);
        let count = result
            .checks
            .iter()
            .find(|check| check.requirement == "exercise/worker-death/count")
            .unwrap();
        assert!(!count.passed, "{count:?}");
    }

    #[test]
    fn qualification_fails_when_a_recorded_exercise_shows_no_identity_change() {
        let (dir, manifest) = fixture();
        let mut contract = contract_fixture(&manifest);
        contract.exercises = vec![ExerciseRequirement {
            kind: ExerciseKind::DaemonRollover,
            min_count: 1,
            min_continuation_samples: 1,
        }];
        freeze_test_contract(&dir, &contract);
        append_json_line(
            &dir.path().join(SAMPLES),
            &sample(1, "2026-09-02T00:00:30Z"),
        )
        .unwrap();
        exercise(
            ExerciseArgs {
                run: dir.path().into(),
                kind: ExerciseKind::DaemonRollover,
                before: "pid-1".into(),
                after: "pid-1".into(),
                ticket: None,
                note: "no-op restart".into(),
            },
            true,
        )
        .unwrap();
        let result = derive_qualification(dir.path()).unwrap();
        assert!(!result.qualified);
        let continuation = result
            .checks
            .iter()
            .find(|check| {
                check.requirement.starts_with("exercise/daemon-rollover/")
                    && check.requirement.ends_with("/continuation")
            })
            .unwrap();
        assert!(!continuation.passed, "{continuation:?}");
        assert!(
            continuation.detail.contains("identical"),
            "{continuation:?}"
        );
    }

    #[test]
    fn qualification_fails_when_an_exercise_is_followed_by_a_repeated_landing() {
        let (dir, manifest) = fixture();
        let mut contract = contract_fixture(&manifest);
        contract.exercises = vec![ExerciseRequirement {
            kind: ExerciseKind::WorkerDeath,
            min_count: 1,
            min_continuation_samples: 1,
        }];
        freeze_test_contract(&dir, &contract);
        append_json_line(
            &dir.path().join(SAMPLES),
            &sample(1, "2026-09-02T00:00:30Z"),
        )
        .unwrap();
        exercise(
            ExerciseArgs {
                run: dir.path().into(),
                kind: ExerciseKind::WorkerDeath,
                before: "gen-1".into(),
                after: "gen-2".into(),
                ticket: Some("TKT-1".into()),
                note: "worker killed and respawned".into(),
            },
            true,
        )
        .unwrap();
        let mut after = sample(2, "2026-09-02T00:02:00Z");
        after.events = vec![
            json!({"id": "e1", "identity":"landing_processed", "payload":{"task":"TKT-1", "head_sha":"a", "target":"main", "outcome":"landed"}}),
            json!({"id": "e2", "identity":"landing_processed", "payload":{"task":"TKT-1", "head_sha":"b", "target":"main", "outcome":"landed"}}),
        ];
        append_json_line(&dir.path().join(SAMPLES), &after).unwrap();
        let result = derive_qualification(dir.path()).unwrap();
        assert!(!result.qualified);
        let continuation = result
            .checks
            .iter()
            .find(|check| {
                check.requirement.starts_with("exercise/worker-death/")
                    && check.requirement.ends_with("/continuation")
            })
            .unwrap();
        assert!(!continuation.passed, "{continuation:?}");
    }

    #[test]
    fn qualification_fails_on_excess_ad_hoc_intervention() {
        let (dir, manifest) = fixture();
        let contract = contract_fixture(&manifest);
        freeze_test_contract(&dir, &contract);
        append_json_line(
            &dir.path().join(SAMPLES),
            &sample(1, "2026-09-02T00:00:30Z"),
        )
        .unwrap();
        record(
            RecordArgs {
                run: dir.path().into(),
                class: InterventionClass::AdHoc,
                summary: "human rescued a stuck landing".into(),
                ticket: None,
                actor: None,
                evidence: vec![],
                owner: None,
                spawn: None,
            },
            true,
        )
        .unwrap();
        let result = derive_qualification(dir.path()).unwrap();
        assert!(!result.qualified);
        let ad_hoc = result
            .checks
            .iter()
            .find(|check| check.requirement == "interventions/ad-hoc-limit")
            .unwrap();
        assert!(!ad_hoc.passed, "{ad_hoc:?}");
    }

    #[test]
    fn qualification_replay_is_deterministic_for_the_same_contract_and_evaluator() {
        let (dir, manifest) = fixture();
        let mut contract = contract_fixture(&manifest);
        contract.workload.min_root_deliveries = 1;
        freeze_test_contract(&dir, &contract);
        let mut delivered = sample(1, "2026-09-02T00:01:00Z");
        delivered.tickets = vec![delivered_ticket("TKT-1", delivered.observed_at)];
        append_json_line(&dir.path().join(SAMPLES), &delivered).unwrap();
        let first = serde_json::to_value(derive_qualification(dir.path()).unwrap()).unwrap();
        let second = serde_json::to_value(derive_qualification(dir.path()).unwrap()).unwrap();
        assert_eq!(
            first, second,
            "replay of immutable evidence must be deterministic"
        );
    }
}

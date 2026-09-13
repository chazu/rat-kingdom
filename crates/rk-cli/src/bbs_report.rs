//! `rk bbs report --manifest FILE --tuples FILE --reviews FILE [--output FILE]`
//!
//! Offline, deterministic evidence report for the stigmergy program
//! (docs/2026-09-12-stigmergy-evidence-and-trial.md, S3:
//! TKT-tavik-kifos-lozuf; corrected under TKT-nonub-pugar-pilid). No daemon
//! connection, worker credentials, model call or network is required: this is
//! pure aggregation over three JSON files an operator prepares ahead of time.
//! See docs/2026-09-13-stigmergy-report-capture.md for the capture recipe,
//! the capture-envelope contract this module consumes, and compact templates
//! for each input.
//!
//! This tool has no dispatch, landing, repair or approval authority; it only
//! renders evidence that already exists.
//!
//! # What the manifest can and cannot freeze
//!
//! A source tuple id and a consumer's exact agent generation do not exist
//! before a live batch runs — a manifest written ahead of time cannot name
//! them. What CAN and MUST be frozen ahead of time is *scope*: which
//! batches exist, which consumer tasks are in play for each, the repository
//! set, the measurement window, and the (fixed, not manifest-configurable)
//! eligibility rule itself. Concrete pairs (source, consumer generation) are
//! enrolled later, in the reviews file, each bound to an already-frozen
//! batch/task. A manifest may still predeclare full pairs directly
//! (`eligible_pairs`) for deterministic fixture/replay cases where the pair
//! identities are already known.
//!
//! # What this module will and will not certify
//!
//! Every rule below exists because the weaker version of it produced a false
//! positive or erased a denominator (operator review of `43faffe`):
//!
//! * A record is only trusted when it structurally matches what the
//!   *committed producer* writes — category, lifecycle, `schema_version`,
//!   reserved identity prefix, and the `instance == payload.agent` binding
//!   that `Tuple::new` guarantees for every S1 BBS write
//!   (crates/rk-daemon/src/bbs.rs). Matching `category` + `bbs_kind` alone
//!   lets an ordinary mutable artifact masquerade as a finding.
//! * `exposure`/`open` telemetry is authored by the CASTLE, so `instance` is
//!   the castle and never the consumer. Consumer identity is only
//!   `payload.agent`/`payload.spawn`/`payload.bound`, and only
//!   `bound == "agent"` is an exact generation.
//! * A prepared exposure is not a launched consumer opportunity. It is only
//!   counted as `prepared` once native launch evidence exists for that exact
//!   generation; otherwise it is reported as `prepared_not_launched`.
//! * Author-exit is credited ONLY from an `agent_exit` observation for the
//!   source author's exact `(spawn, session)`, with no intervening relaunch
//!   before the consuming decision. `harness_result` is task-completion
//!   evidence — the supervisor emits it at `rk done`, while the OS process is
//!   still alive — and `agent_lifecycle` carries no `spawn` binding at all
//!   and may describe a *start*. Both are refused with a reason.
//! * Cost is a reported estimate, evaluated as the last reported total per
//!   proven `(spawn, session, provider_session)` segment, never summed across
//!   the results of one segment, never pooled across `cost_basis` values, and
//!   never final while the last result for a segment was `paused` and the
//!   process went on to do more work.
//! * Launch-to-exit is *process lifetime*, not measured active model work.
//!   `active_work_ms` is therefore always an explicit unknown.
//! * A bad claim removes the claim, not the opportunity: an invalid receipt
//!   leaves its pair in the eligible denominator with no claimed outcome.

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

/// Schema version this module reads on `Manifest`/`Review` inputs.
pub const SCHEMA_VERSION: u32 = 1;
/// Bumped whenever the evaluation rules below change in a way that could
/// change a report's output for unchanged inputs. Compare this, not just
/// `schema_version`, when diffing two reports across a rebuild.
///
/// * 1 — initial S3 evaluator (`TKT-tavik-kifos-lozuf`).
/// * 2 — `TKT-nonub-pugar-pilid`: native identity binding, window/repo
///   scoping, launch-joined exposure, `agent_exit`-only author exit,
///   segment-keyed cost with explicit finality, denominator retention.
pub const EVALUATOR_VERSION: u32 = 2;

/// Frozen per the design doc: "three verified used/adapted effects across at
/// least two batches, at least one after its author exited". Not
/// manifest-configurable — a manifest cannot raise or lower its own bar.
pub const MECHANISM_EFFECTS_REQUIRED: usize = 3;
pub const MECHANISM_BATCHES_REQUIRED: usize = 2;
pub const MECHANISM_AUTHOR_EXIT_REQUIRED: usize = 1;

/// Longest accepted operator free-text reference. A reviewed annotation is
/// operator judgment, so its text is bounded rather than unbounded input.
const MAX_REFERENCE_LEN: usize = 512;

// `bbs_kind` literals from the design doc's payload contract table, plus the
// two native observation kinds S2's published contract adds
// (docs/2026-09-13-s2-native-observation-and-export-contract.md, consumed via
// BBS artifact 01M2CF3RJHX58HH085WKZBJD9A).
const FINDING: &str = "finding";
const ANSWER: &str = "answer";
const REUSE: &str = "reuse";
const ASSESSMENT: &str = "assessment";
const EXPOSURE: &str = "exposure";
const OPEN: &str = "open";
const AGENT_EXIT: &str = "agent_exit";
const AGENT_FINAL_USAGE: &str = "agent_final_usage";

/// Reserved identity prefixes the daemon mints for each BBS record kind
/// (`rk_core::bbs::RESERVED_IDENTITY_PREFIXES`). A record whose identity does
/// not carry its kind's prefix was not written by the BBS write path.
fn reserved_prefix(bbs_kind: &str) -> Option<&'static str> {
    match bbs_kind {
        FINDING => Some("bbs-finding-"),
        ANSWER => Some("bbs-answer-"),
        REUSE => Some("bbs-reuse-"),
        ASSESSMENT => Some("bbs-assessment-"),
        EXPOSURE => Some("bbs-exposure-"),
        // NOTE, verified against the producer rather than the constant list:
        // `record_open` writes the identity `"bbs-open"` with NO trailing
        // dash, while `rk_core::bbs::RESERVED_IDENTITY_PREFIXES` lists
        // `"bbs-open-"`. Matching the constant here would reject every real
        // open record, so this matches what is actually written. (The daemon's
        // own reserved-prefix guard has the same gap; filed separately, not
        // patched from this ticket.)
        OPEN => Some("bbs-open"),
        AGENT_EXIT => Some("bbs-agent-exit"),
        AGENT_FINAL_USAGE => Some("bbs-agent-final-usage"),
        _ => None,
    }
}

/// `AgentState` values that mean this launch produced its *last* provider
/// result. A `paused` result may be followed by more model usage and then a
/// budget kill with no further result, which makes the reported total a
/// partial amount rather than a final cost.
const TERMINAL_USAGE_STATES: [&str; 3] = ["completed", "failed", "stopped"];

/// The only `cost_basis` a *provider-reported* total may carry. A
/// daemon-priced estimate is an estimate of an estimate and is reported in
/// its own field, never pooled with this one.
const PROVIDER_COST_BASIS: &str = "provider_reported_segment_total";
const DAEMON_COST_BASIS: &str = "daemon_priced_increments";

/// Native events that prove an agent generation actually started a process.
/// `agent_spawned`/`agent_respawned` carry `spawn` additively since S2's
/// `48fb2da`; a `harness_result` also proves the generation ran, because the
/// supervisor only emits one for a generation that produced a result.
const LAUNCH_EVENT_IDENTITIES: [&str; 2] = ["agent_spawned", "agent_respawned"];

// ---------------------------------------------------------------------
// Manifest: the versioned, frozen experiment/scope declaration.
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub schema_version: u32,
    pub experiment_id: String,
    /// Repository scopes in play. When non-empty this is ENFORCED: every
    /// batch, pair and native observation outside it is excluded with a
    /// reason rather than silently aggregated.
    #[serde(default)]
    pub repos: Vec<String>,
    /// Frozen measurement window. Enforced against every native observation
    /// (exposure, open, completion, launch, exit, final usage, phase span).
    /// Source findings/artifacts are deliberately NOT window-filtered: an
    /// older source that a batch consumer reused is exactly the case the
    /// experiment is looking for, so it is retained as linked context.
    #[serde(default)]
    pub window: Window,
    #[serde(default)]
    pub build: BuildIdentity,
    #[serde(default)]
    pub quality_criteria: Vec<String>,
    pub batches: Vec<Batch>,
    /// Frozen consumer-task scope: which tasks are in play for which batch.
    /// Required before a review may enroll a live pair for that task/batch;
    /// also the primary scope deliveries are derived from, so a frozen task
    /// that produced no source and no receipt still reports its own cost,
    /// failures and acceptance instead of disappearing.
    #[serde(default)]
    pub consumer_tasks: Vec<ConsumerTaskScope>,
    /// Full pairs known ahead of time — deterministic fixture/replay only.
    /// A live batch cannot populate this (see module docs); use reviews'
    /// `declares` instead. A fixture pair's `repo` must match its batch's
    /// `repo`: naming a known batch id is not enough.
    #[serde(default)]
    pub eligible_pairs: Vec<EligiblePair>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Window {
    #[serde(default)]
    pub since: Option<DateTime<Utc>>,
    #[serde(default)]
    pub until: Option<DateTime<Utc>>,
}

/// Where an observation falls relative to the frozen window. An observation
/// with no parsable timestamp is `Undated`, never silently treated as either
/// inside or outside: it is reported as missing coverage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFit {
    Inside,
    Outside,
    Undated,
}

impl Window {
    fn declared(&self) -> bool {
        self.since.is_some() || self.until.is_some()
    }

    fn fit(&self, at: Option<DateTime<Utc>>) -> WindowFit {
        if !self.declared() {
            return WindowFit::Inside;
        }
        match at {
            None => WindowFit::Undated,
            Some(t) => {
                if self.since.is_some_and(|s| t < s) || self.until.is_some_and(|u| t > u) {
                    WindowFit::Outside
                } else {
                    WindowFit::Inside
                }
            }
        }
    }
}

/// Exact build/deployment identity this experiment ran under. Per the
/// design doc's completion evidence: "Main, remote, installed rk/rk-mcp and
/// daemon identities are recorded at activation".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildIdentity {
    /// The `main` commit this batch's workers forked from.
    #[serde(default)]
    pub source_commit: Option<String>,
    /// `rk --version` of the installed CLI used for the batch.
    #[serde(default)]
    pub installed_rk_version: Option<String>,
    #[serde(default)]
    pub installed_rk_mcp_version: Option<String>,
    /// The running daemon's identity (short build hash or similar).
    #[serde(default)]
    pub daemon_identity: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub harness: Option<String>,
    #[serde(default)]
    pub check: Option<String>,
    #[serde(default)]
    pub wip: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Batch {
    pub id: String,
    pub arm: String,
    pub repo: String,
}

/// A consumer task frozen into scope for a batch, before that batch's
/// concrete generations exist.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerTaskScope {
    pub task: String,
    pub repo: String,
    pub batch: String,
}

/// One source/consumer pairing: "source existed before the relevant
/// decision, applies to the task, and is not the consumer's own work"
/// (design doc). `consumer_generation` is the exact agent generation
/// (matches a tuple payload's `spawn` field) credited with the reuse.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EligiblePair {
    pub id: String,
    pub source: String,
    pub consumer_task: String,
    pub consumer_generation: String,
    pub repo: String,
    pub batch: String,
}

/// The pair-identifying fields a review mints for a live-enrolled pair —
/// `EligiblePair` minus its `id` (the review's own `pair` field supplies
/// that).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PairDeclaration {
    pub source: String,
    pub consumer_task: String,
    pub consumer_generation: String,
    pub repo: String,
    pub batch: String,
}

fn validate_manifest(m: &Manifest) -> Result<()> {
    if m.schema_version != SCHEMA_VERSION {
        bail!(
            "manifest schema_version {} is not supported (expected {SCHEMA_VERSION})",
            m.schema_version
        );
    }
    if m.experiment_id.trim().is_empty() {
        bail!("manifest experiment_id must not be empty");
    }
    if m.batches.is_empty() {
        bail!("manifest must declare at least one batch");
    }
    if let (Some(since), Some(until)) = (m.window.since, m.window.until) {
        if since > until {
            bail!("manifest window.since is after window.until");
        }
    }
    let repos: BTreeSet<&str> = m.repos.iter().map(String::as_str).collect();
    let repo_in_scope = |repo: &str| repos.is_empty() || repos.contains(repo);
    let mut seen_batches: BTreeMap<&str, &str> = BTreeMap::new();
    for b in &m.batches {
        if b.id.trim().is_empty() {
            bail!("batch id must not be empty");
        }
        if seen_batches
            .insert(b.id.as_str(), b.repo.as_str())
            .is_some()
        {
            bail!("duplicate batch id: {}", b.id);
        }
        if b.arm.trim().is_empty() {
            bail!("batch {} must declare a non-empty arm", b.id);
        }
        if b.repo.trim().is_empty() {
            bail!("batch {} must declare a non-empty repo", b.id);
        }
        if !repo_in_scope(&b.repo) {
            bail!(
                "batch {} declares repo {} which is not in manifest.repos {:?}",
                b.id,
                b.repo,
                m.repos
            );
        }
    }
    let mut seen_tasks = BTreeSet::new();
    for c in &m.consumer_tasks {
        if c.task.trim().is_empty() || c.repo.trim().is_empty() || c.batch.trim().is_empty() {
            bail!("consumer_tasks entries must declare task/repo/batch");
        }
        if !seen_tasks.insert((c.task.as_str(), c.repo.as_str(), c.batch.as_str())) {
            bail!(
                "duplicate consumer_tasks entry for task {} in batch {}",
                c.task,
                c.batch
            );
        }
        if seen_batches.get(c.batch.as_str()) != Some(&c.repo.as_str()) {
            bail!(
                "consumer_tasks entry for task {} references a batch {} not declared in batches \
                 (with matching repo)",
                c.task,
                c.batch
            );
        }
    }
    let mut seen_pairs = BTreeSet::new();
    for p in &m.eligible_pairs {
        if p.id.trim().is_empty() {
            bail!("eligible pair id must not be empty");
        }
        if !seen_pairs.insert(p.id.as_str()) {
            bail!("duplicate eligible pair id in manifest: {}", p.id);
        }
        if p.source.trim().is_empty()
            || p.consumer_task.trim().is_empty()
            || p.consumer_generation.trim().is_empty()
            || p.repo.trim().is_empty()
        {
            bail!(
                "eligible pair {} must declare source/consumer_task/consumer_generation/repo",
                p.id
            );
        }
        // A fixture pair naming a known batch id is not enough: the pair's
        // repo has to be the batch's repo, or one batch silently aggregates
        // two repositories' evidence.
        match seen_batches.get(p.batch.as_str()) {
            None => bail!(
                "eligible pair {} references unknown batch {}",
                p.id,
                p.batch
            ),
            Some(batch_repo) if *batch_repo != p.repo.as_str() => bail!(
                "eligible pair {} declares repo {} but its batch {} is declared for repo {}",
                p.id,
                p.repo,
                p.batch,
                batch_repo
            ),
            Some(_) => {}
        }
        if !repo_in_scope(&p.repo) {
            bail!(
                "eligible pair {} declares repo {} which is not in manifest.repos {:?}",
                p.id,
                p.repo,
                m.repos
            );
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Tuple capture: the native tuple-list JSON, plus its ordering contract.
// ---------------------------------------------------------------------

/// Whether a tuple capture's array reflects true SQLite persistence order.
/// Plain `rk --json scan` output is a query result, not a persistence-order
/// export — its array position must never be read as supersession order,
/// and neither may a tuple's id (ULID) or `created_at`: scan order is not
/// necessarily persistence order, and a ULID/wall-clock sort is not either.
/// Only an explicit capture envelope built from `Space::persistence_delta`
/// (a bounded, sequence-ordered bbs export; S2's to supply, not this
/// module's) may claim `PersistenceSequence`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    PersistenceSequence,
    Unknown,
}

pub struct TupleCapture {
    pub order: Order,
    pub tuples: Vec<Value>,
    pub truncated: bool,
    pub source: Option<String>,
}

/// Parses either shape: a bare tuple array, the raw `rk --json scan` object
/// (`{"tuples":[...], "truncated":bool, ...}`), or the capture envelope
/// (`{"schema_version":1,"order":"persistence_sequence","tuples":[...],...}`).
/// Legacy/raw input is always `Order::Unknown` — never inferred from tuple id
/// (ULID) or `created_at`.
pub fn parse_tuple_capture(raw: &Value) -> Result<TupleCapture> {
    if let Some(arr) = raw.as_array() {
        return Ok(TupleCapture {
            order: Order::Unknown,
            tuples: arr.clone(),
            truncated: false,
            source: None,
        });
    }
    let tuples = raw
        .get("tuples")
        .and_then(Value::as_array)
        .context(
            "tuples input must be a JSON array, or an object with a `tuples` array \
             (the shape `rk --json scan` and the capture envelope both produce)",
        )?
        .clone();
    let order = match raw.get("order").and_then(Value::as_str) {
        Some("persistence_sequence") => Order::PersistenceSequence,
        _ => Order::Unknown,
    };
    let truncated = raw
        .get("truncated")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let source = raw
        .get("source")
        .and_then(Value::as_str)
        .map(str::to_string);
    Ok(TupleCapture {
        order,
        tuples,
        truncated,
        source,
    })
}

// ---------------------------------------------------------------------
// Reviews: operator judgments, not daemon facts.
// ---------------------------------------------------------------------

/// An operator reference backing a reviewed annotation. Deliberately NOT an
/// arbitrary unchecked string: it is bounded, must be non-blank, and the
/// report reports whether it resolves to a tuple actually present in the
/// capture. It is also deliberately closed to unknown fields, so a reviewer
/// cannot smuggle an invented "time saved" figure into a measurement report.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ReviewedEvidence {
    /// `"evidence": "<tuple-id-or-reference>"`
    Reference(String),
    /// `"evidence": {"reference": "...", "note": "..."}`
    Detailed {
        reference: String,
        #[serde(default)]
        note: Option<String>,
    },
}

impl ReviewedEvidence {
    pub fn reference(&self) -> &str {
        match self {
            Self::Reference(r) => r,
            Self::Detailed { reference, .. } => reference,
        }
    }

    pub fn note(&self) -> Option<&str> {
        match self {
            Self::Reference(_) => None,
            Self::Detailed { note, .. } => note.as_deref(),
        }
    }

    fn validate(&self, what: &str) -> Result<()> {
        let r = self.reference();
        if r.trim().is_empty() {
            bail!("{what} reference must not be blank");
        }
        if r.len() > MAX_REFERENCE_LEN {
            bail!("{what} reference exceeds {MAX_REFERENCE_LEN} bytes");
        }
        if let Some(note) = self.note() {
            if note.len() > MAX_REFERENCE_LEN {
                bail!("{what} note exceeds {MAX_REFERENCE_LEN} bytes");
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Coverage {
    /// An operator manually confirms this source was prepared for this
    /// consumer generation (e.g. before S2's exposure telemetry existed).
    /// Reported as `prepared_reviewed`, never merged into native prepared
    /// coverage: operator judgment and daemon telemetry stay distinguishable.
    Prepared { evidence: ReviewedEvidence },
    /// An operator manually confirms telemetry was checked and this source
    /// was absent from it.
    NotPrepared { evidence: ReviewedEvidence },
    /// No telemetry and no operator determination either way.
    Unknown { reason: String },
}

/// A reviewed observation that native telemetry cannot supply: an operator
/// intervention with no `attention_hold` span, a repeated investigation, or
/// rework the spans do not name. Each carries a reference so the claim is
/// checkable. There is deliberately NO time-saving field: the design doc
/// forbids turning an estimate of time saved into a measured saving, and
/// `deny_unknown_fields` makes adding one a hard input error rather than a
/// silently-ignored key.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewedAnnotation {
    /// Free-form short classifier, e.g. `operator-steer`, `manual-dispatch`,
    /// `repeat-investigation`.
    pub kind: String,
    pub reference: String,
    #[serde(default)]
    pub note: Option<String>,
}

impl ReviewedAnnotation {
    fn validate(&self, task: &str) -> Result<()> {
        if self.kind.trim().is_empty() {
            bail!("reviewed annotation for task {task} must declare a non-empty kind");
        }
        if self.kind.len() > MAX_REFERENCE_LEN {
            bail!("reviewed annotation kind for task {task} exceeds {MAX_REFERENCE_LEN} bytes");
        }
        ReviewedEvidence::Detailed {
            reference: self.reference.clone(),
            note: self.note.clone(),
        }
        .validate(&format!("reviewed annotation for task {task}"))
    }
}

/// Task-scoped reviewed annotations. Pair-scoped judgment lives on `Review`;
/// interventions, repeated investigations and rework are properties of a
/// task, not of a source/consumer pair, and a task with no eligible pair at
/// all still has them.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAnnotation {
    pub task: String,
    pub repo: String,
    #[serde(default)]
    pub interventions: Vec<ReviewedAnnotation>,
    #[serde(default)]
    pub repeated_investigations: Vec<ReviewedAnnotation>,
    #[serde(default)]
    pub rework: Vec<ReviewedAnnotation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Review {
    /// References a predeclared `Manifest.eligible_pairs[].id` (fixture/
    /// replay), or is a fresh id the reviewer mints for a pair discovered
    /// during a live batch — exactly one of these two, never both.
    pub pair: String,
    #[serde(default)]
    pub declares: Option<PairDeclaration>,
    #[serde(default = "unknown_coverage")]
    pub coverage: Coverage,
    /// Tuple id of native evidence that the source's authoring generation had
    /// physically EXITED before this reuse. Must resolve to an `agent_exit`
    /// observation for the source's exact `(spawn, session)` in the pair's
    /// repo. A `harness_result` is refused: the supervisor emits it at
    /// `rk done`, while the process is still alive. An `agent_lifecycle` is
    /// refused: it carries no `spawn` binding and its `change` may be
    /// `started`. See `resolve_author_exit`.
    #[serde(default)]
    pub author_terminal_evidence: Option<String>,
    /// True if the King/operator pointed the consumer at the source. Must
    /// be excluded from the mechanism goal's counted effects.
    #[serde(default)]
    pub relayed_by_operator: bool,
    #[serde(default)]
    pub regression: bool,
    #[serde(default)]
    pub notes: Option<String>,
}

fn unknown_coverage() -> Coverage {
    Coverage::Unknown {
        reason: "review did not state coverage".into(),
    }
}

/// The `--reviews FILE` payload. A bare array is the pair-only form every
/// existing fixture uses; the object form additionally carries task-scoped
/// reviewed annotations.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ReviewsFile {
    Pairs(Vec<Review>),
    Structured {
        #[serde(default)]
        pairs: Vec<Review>,
        #[serde(default)]
        tasks: Vec<TaskAnnotation>,
    },
}

impl ReviewsFile {
    pub fn pairs(&self) -> &[Review] {
        match self {
            Self::Pairs(p) => p,
            Self::Structured { pairs, .. } => pairs,
        }
    }

    pub fn tasks(&self) -> &[TaskAnnotation] {
        match self {
            Self::Pairs(_) => &[],
            Self::Structured { tasks, .. } => tasks,
        }
    }
}

fn validate_reviews(
    reviews: &[Review],
    tasks: &[TaskAnnotation],
    manifest: &Manifest,
) -> Result<()> {
    for r in reviews {
        if r.pair.trim().is_empty() {
            bail!("review pair id must not be blank");
        }
        match &r.coverage {
            Coverage::Prepared { evidence } => {
                evidence.validate(&format!("review {} coverage.prepared", r.pair))?
            }
            Coverage::NotPrepared { evidence } => {
                evidence.validate(&format!("review {} coverage.not_prepared", r.pair))?
            }
            Coverage::Unknown { reason } => {
                if reason.trim().is_empty() {
                    bail!("review {} coverage.unknown must state a reason", r.pair);
                }
            }
        }
        if let Some(id) = &r.author_terminal_evidence {
            if id.trim().is_empty() {
                bail!(
                    "review {} author_terminal_evidence must be a tuple id, not a blank string",
                    r.pair
                );
            }
        }
    }
    let frozen: BTreeSet<(&str, &str)> = manifest
        .consumer_tasks
        .iter()
        .map(|c| (c.task.as_str(), c.repo.as_str()))
        .chain(
            manifest
                .eligible_pairs
                .iter()
                .map(|p| (p.consumer_task.as_str(), p.repo.as_str())),
        )
        .collect();
    let mut seen = BTreeSet::new();
    for t in tasks {
        if t.task.trim().is_empty() || t.repo.trim().is_empty() {
            bail!("task annotation must declare task and repo");
        }
        if !seen.insert((t.task.as_str(), t.repo.as_str())) {
            bail!(
                "duplicate task annotation for task {} in repo {}",
                t.task,
                t.repo
            );
        }
        // A reviewed annotation cannot introduce a task the manifest never
        // froze: retrospective scope expansion is exactly what freezing is
        // meant to prevent.
        if !frozen.contains(&(t.task.as_str(), t.repo.as_str())) {
            bail!(
                "task annotation for {} (repo {}) is not frozen in manifest.consumer_tasks or \
                 manifest.eligible_pairs",
                t.task,
                t.repo
            );
        }
        for a in t
            .interventions
            .iter()
            .chain(&t.repeated_investigations)
            .chain(&t.rework)
        {
            a.validate(&t.task)?;
        }
    }
    Ok(())
}

/// Merges `manifest.eligible_pairs` (predeclared) with pairs minted by
/// `Review.declares` entries (live enrollment), validating that every
/// minted pair is bound to an already-frozen batch/task scope and that no
/// review redeclares a predeclared pair's identity.
fn validate_and_merge_pairs(manifest: &Manifest, reviews: &[Review]) -> Result<Vec<EligiblePair>> {
    let predeclared: BTreeSet<&str> = manifest
        .eligible_pairs
        .iter()
        .map(|p| p.id.as_str())
        .collect();
    let mut merged = manifest.eligible_pairs.clone();
    let mut seen_review_pairs = BTreeSet::new();
    for r in reviews {
        if !seen_review_pairs.insert(r.pair.as_str()) {
            bail!("duplicate review for pair {}", r.pair);
        }
        match (&r.declares, predeclared.contains(r.pair.as_str())) {
            (Some(_), true) => bail!(
                "review {} both references a predeclared pair and declares a new one; \
                 a predeclared pair's identity may not be mutated",
                r.pair
            ),
            (None, false) => bail!(
                "review references pair {} which is neither predeclared in the manifest nor \
                 declared by this review (retrospective annotations cannot introduce a pair \
                 without binding it to frozen scope via `declares`)",
                r.pair
            ),
            (None, true) => {} // adds evidence to a predeclared pair, fine
            (Some(d), false) => {
                let batch_ok = manifest
                    .batches
                    .iter()
                    .any(|b| b.id == d.batch && b.repo == d.repo);
                if !batch_ok {
                    bail!(
                        "review {} declares batch {} which is not frozen in the manifest \
                         (with matching repo {})",
                        r.pair,
                        d.batch,
                        d.repo
                    );
                }
                let task_ok = manifest
                    .consumer_tasks
                    .iter()
                    .any(|c| c.task == d.consumer_task && c.batch == d.batch && c.repo == d.repo);
                if !task_ok {
                    bail!(
                        "review {} declares consumer_task {} for batch {} which is not frozen \
                         in manifest.consumer_tasks",
                        r.pair,
                        d.consumer_task,
                        d.batch
                    );
                }
                if d.source.trim().is_empty()
                    || d.consumer_task.trim().is_empty()
                    || d.consumer_generation.trim().is_empty()
                {
                    bail!(
                        "review {} declares a pair missing source/consumer_task/consumer_generation",
                        r.pair
                    );
                }
                merged.push(EligiblePair {
                    id: r.pair.clone(),
                    source: d.source.clone(),
                    consumer_task: d.consumer_task.clone(),
                    consumer_generation: d.consumer_generation.clone(),
                    repo: d.repo.clone(),
                    batch: d.batch.clone(),
                });
            }
        }
    }
    Ok(merged)
}

// ---------------------------------------------------------------------
// Report output.
// ---------------------------------------------------------------------

/// A pair that is not a distinct valid opportunity at all. Note what is NOT
/// here: an invalid *claim*. A malformed or foreign receipt removes the
/// claim, never the opportunity — see `RejectedClaim`.
#[derive(Debug, Clone, Serialize)]
pub struct Excluded {
    pub pair: String,
    pub reason: String,
    pub detail: String,
}

/// A receipt that was refused for this pair. The pair itself stays in the
/// eligible denominator with no claimed outcome, so a bad claim cannot erase
/// a real opportunity from the discovery/reuse rates.
#[derive(Debug, Clone, Serialize)]
pub struct RejectedClaim {
    pub pair: String,
    pub record: String,
    pub reason: String,
    pub detail: String,
}

/// A native record dropped with a reason. Rejecting these silently is what
/// let an ordinary mutable artifact, a castle-authored row read as a
/// consumer, or an unattributed legacy record certify a result.
#[derive(Debug, Clone, Serialize)]
pub struct InvalidRecord {
    pub record: String,
    pub kind: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AmbiguousAssessment {
    pub pair: String,
    pub receipt: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuthorExitUnsupported {
    pub pair: String,
    pub evidence: String,
    pub reason: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct OutcomeClasses {
    pub used: usize,
    pub adapted: usize,
    pub confirmed: usize,
    pub rejected: usize,
    pub verified: usize,
    pub unsupported: usize,
    pub incorrect: usize,
}

/// The discovery denominator, reported explicitly rather than left implicit
/// in a single rate. `rate` is `None` — not `0.0` — when nothing has known
/// coverage: an unknown denominator is not a zero numerator.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Discovery {
    pub eligible_pairs: usize,
    pub known_coverage_pairs: usize,
    pub prepared_pairs: usize,
    pub prepared_native: usize,
    pub prepared_reviewed: usize,
    pub not_prepared_pairs: usize,
    pub unknown_coverage_pairs: usize,
    /// Native exposure exists, but no native launch evidence for that exact
    /// consumer generation: a prepared selection for a spawn that never ran
    /// is not a consumer opportunity.
    pub prepared_not_launched: Vec<String>,
    pub rate: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PairResult {
    pub pair: String,
    pub source: String,
    pub source_kind: String,
    /// `false` for a legacy/ordinary artifact with no author generation. An
    /// unattributed source can neither be excluded as self-use nor support an
    /// author-exit claim; it stays explicitly unknown on both.
    pub source_attributed: bool,
    pub source_evidence: String,
    pub consumer_task: String,
    pub consumer_generation: String,
    pub repo: String,
    pub batch: String,
    pub coverage_status: String,
    pub coverage_provenance: String,
    pub coverage_reference: Option<String>,
    pub coverage_reference_resolved: Option<bool>,
    pub consumer_launched: bool,
    pub opened: bool,
    pub claimed_outcome: Option<String>,
    pub claim_evidence: Option<String>,
    pub assessed_verdict: Option<String>,
    pub assessment_evidence: Option<String>,
    pub author_terminal: bool,
    pub author_terminal_evidence: Option<String>,
    pub relayed_by_operator: bool,
    pub regression: bool,
    /// A verified, changed-work (used/adapted) effect on fully established
    /// evidence. The same gate the per-task verified-reuse rate uses, so the
    /// two can never disagree.
    pub verified_effect: bool,
    /// `verified_effect` and not relayed by the operator — the unit the
    /// mechanism goal counts.
    pub counts_as_effect: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct VerifiedReuse {
    pub eligible_consumer_tasks: usize,
    pub verified_used_or_adapted_tasks: usize,
    /// `None` when there are no eligible consumer tasks — an absent
    /// denominator, not a zero rate.
    pub rate: Option<f64>,
    pub confirmed_tasks: usize,
    pub rejected_tasks: usize,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct MechanismResult {
    pub effects: usize,
    pub batches: usize,
    pub author_exit_effects: usize,
    pub effects_required: usize,
    pub batches_required: usize,
    pub author_exit_required: usize,
    pub goal_met: bool,
    pub effect_pairs: Vec<String>,
}

/// Phase durations, bucketed. Deliberately NOT one "active" number: a phase
/// duration is wall-clock time a phase span covered, which is not the same
/// thing as model-active work.
#[derive(Debug, Clone, Default, Serialize)]
pub struct PhaseDurations {
    /// Every phase that is neither verification admission nor a human wait.
    pub work_phases_ms: Option<i64>,
    /// `verification` span duration plus its own queue wait.
    pub verification_ms: Option<i64>,
    /// `attention_hold` — waiting on a human.
    pub attention_hold_ms: Option<i64>,
    /// Pre-phase queue wait on every other phase.
    pub queue_wait_ms: Option<i64>,
}

/// One observed provider-cost segment: a `(spawn, session, provider_session)`
/// triple. The provider reports a CUMULATIVE total within one segment, so the
/// last reported total is the segment's amount and repeated results must
/// never be summed.
#[derive(Debug, Clone, Serialize)]
pub struct CostSegment {
    pub spawn: String,
    pub session: String,
    pub provider_session: Option<String>,
    pub record: String,
    pub reported_usd: Option<f64>,
    pub cost_basis: String,
    pub state: Option<String>,
    /// `true` only when the last result for this segment was terminal AND no
    /// later work is observable after it. A `paused` result followed by more
    /// work and a kill leaves a partial amount, not a final cost.
    pub final_cost: bool,
    pub finality_reason: String,
}

/// One physical process launch of one generation: the `(spawn, session)` pair
/// S2's contract names as the only safe aggregation key.
#[derive(Debug, Clone, Serialize)]
pub struct LaunchObservation {
    pub session: String,
    pub launched_at: Option<String>,
    pub exited_at: Option<String>,
    pub exit_record: Option<String>,
    pub exit_code: Option<i64>,
    pub crashed: Option<bool>,
    pub prior_state: Option<String>,
    /// `exited_at - launched_at`. PROCESS LIFETIME, not measured active model
    /// work: a Claude process can sit paused awaiting verification or the
    /// operator for most of it.
    pub process_lifetime_ms: Option<i64>,
}

/// One completion (`harness_result`) for a generation. Task-completion
/// evidence only: it is emitted at `rk done`, before terminal provider usage
/// and while the OS process is still alive, so its cost is provisional.
#[derive(Debug, Clone, Serialize)]
pub struct CompletionObservation {
    pub record: String,
    pub declared_done: bool,
    pub is_error: bool,
    pub provisional_cost_usd: Option<f64>,
    pub failed: bool,
}

/// Everything observed for one agent generation on one task. Every attempt,
/// including every failure, is retained.
#[derive(Debug, Clone, Serialize)]
pub struct GenerationObservation {
    pub spawn: String,
    pub agent: Option<String>,
    pub completions: Vec<CompletionObservation>,
    pub launches: Vec<LaunchObservation>,
    pub cost_segments: Vec<CostSegment>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeliveryCost {
    pub task: String,
    pub repo: String,
    pub batches: Vec<String>,
    /// `frozen_consumer_task` (manifest scope) or `fixture_pair` (a
    /// predeclared pair's task). Deliveries are NOT derived from the pairs
    /// that survived evaluation: a frozen task with no eligible source and no
    /// receipt still owns its costs, failures and acceptance.
    pub enrollment: String,
    pub generations: Vec<GenerationObservation>,
    pub completions: usize,
    pub failed_completions: usize,
    pub launches: usize,
    pub exits: usize,
    /// Sum of the last provider-reported total per proven segment. `None`
    /// whenever any observed segment's cost is unknown or only partial —
    /// never a silent zero and never a partial sum presented as a total.
    pub reported_cost_estimate_usd: Option<f64>,
    pub reported_cost_basis: Option<String>,
    /// `complete` (every segment final), `partial` (some segment reported an
    /// amount that is not final), `missing` (no final-usage observation at
    /// all), `none_observed` (no native generation observed for this task).
    pub cost_coverage: String,
    /// Amounts that were reported but are NOT final, kept separate so a
    /// partial figure is never read as spend.
    pub partial_reported_usd: Option<f64>,
    /// The supervisor's own priced-increment fallback for harnesses that do
    /// not self-report USD. An estimate of an estimate: reported separately
    /// and never pooled with a provider-reported total.
    pub daemon_priced_estimate_usd: Option<f64>,
    /// `harness_result.cost_usd`, summed per generation. PROVISIONAL: emitted
    /// at `rk done`, before terminal provider usage. Never a final spend and
    /// never merged into `reported_cost_estimate_usd`.
    pub provisional_completion_cost_usd: Option<f64>,
    pub unknown_cost: Vec<String>,
    /// Sum of `exited_at - launched_at` over observed launches. Labelled
    /// process lifetime deliberately.
    pub process_lifetime_ms: Option<i64>,
    /// Always `None`. No native observation distinguishes model-active time
    /// from a process paused awaiting verification or the operator, so this
    /// stays an explicit unknown rather than being aliased to process
    /// lifetime or to a phase-duration sum.
    pub active_work_ms: Option<i64>,
    pub active_work_coverage: String,
    pub phase_ms: PhaseDurations,
    /// `Some(true)` only on an actual `delivery_closure` span in this repo and
    /// window. A `merge` span alone is not proof (a merge can be reverted);
    /// absence is unknown, not a negative.
    pub accepted: Option<bool>,
    pub acceptance_evidence: Option<String>,
}

/// A reviewed observation echoed into the report with its provenance and
/// whether its reference resolved against the capture.
#[derive(Debug, Clone, Serialize)]
pub struct ReviewedRef {
    pub task: String,
    pub repo: String,
    pub kind: String,
    pub reference: String,
    pub reference_resolved: bool,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct QualitySummary {
    /// `attention_hold` phase spans. A LOWER BOUND on operator
    /// interventions: an intervention that left no span is only present via a
    /// reviewed annotation.
    pub attention_hold_spans: usize,
    pub reviewed_interventions: Vec<ReviewedRef>,
    pub interventions_known: usize,
    pub interventions_coverage: String,
    pub repeated_investigations: Vec<ReviewedRef>,
    pub rework_spans: usize,
    pub reviewed_rework: Vec<ReviewedRef>,
    pub incorrect_reuse: usize,
    pub regressions: Vec<String>,
}

/// What the capture actually contained, so a scoping mistake is visible
/// instead of silently shrinking every metric.
#[derive(Debug, Clone, Default, Serialize)]
pub struct CaptureSummary {
    pub tuples: usize,
    pub observations_in_window: usize,
    pub observations_out_of_window: usize,
    pub observations_undated: usize,
    pub observations_out_of_scope_repo: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub schema_version: u32,
    pub evaluator_version: u32,
    pub experiment_id: String,
    pub build: BuildIdentity,
    pub repos: Vec<String>,
    pub window: Window,
    pub tuples_order: String,
    pub tuples_truncated: bool,
    pub tuples_source: Option<String>,
    pub capture: CaptureSummary,
    pub invalid_records: Vec<InvalidRecord>,
    /// Records that are structurally fine but cannot be placed or resolved:
    /// an undated observation, a legacy row with no generation id, an
    /// operator/unbound exposure, evidence that simply is not in this
    /// capture. These stay UNKNOWN — they are never read as a negative.
    pub unresolved_records: Vec<InvalidRecord>,
    pub eligible: usize,
    pub excluded: Vec<Excluded>,
    pub rejected_claims: Vec<RejectedClaim>,
    pub discovery: Discovery,
    pub presented_native: usize,
    pub presented_reviewed: usize,
    pub opened: usize,
    pub claimed: usize,
    pub assessed: usize,
    pub outcome_classes: OutcomeClasses,
    pub unknown_coverage: Vec<String>,
    pub ambiguous_assessments: Vec<AmbiguousAssessment>,
    pub author_exit_unsupported: Vec<AuthorExitUnsupported>,
    pub verified_reuse: VerifiedReuse,
    pub author_exit_reuse: Vec<String>,
    pub mechanism: MechanismResult,
    pub deliveries: Vec<DeliveryCost>,
    pub tasks_without_native_records: Vec<String>,
    pub quality: QualitySummary,
    pub pairs: Vec<PairResult>,
}

// ---------------------------------------------------------------------
// Native record validation. Every predicate below mirrors what the
// COMMITTED producer writes; a record that does not match is rejected with a
// reason rather than dropped or, worse, trusted.
// ---------------------------------------------------------------------

fn str_field<'a>(t: &'a Value, path: &[&str]) -> &'a str {
    let mut cur = t;
    for p in path {
        cur = &cur[*p];
    }
    cur.as_str().unwrap_or("")
}

fn record_id(t: &Value) -> String {
    t["id"].as_str().unwrap_or("<no id>").to_string()
}

/// Structural contract check for an artifact-category BBS record
/// (`finding`/`answer`/`reuse`/`assessment`). Returns the reason it is NOT
/// one, or `None` when it is genuine.
///
/// The checks are exactly the invariants `crates/rk-daemon/src/bbs.rs`
/// guarantees at write time:
/// * `Category::Artifact` with `Lifecycle::Furniture` (immutable furniture),
/// * `payload.schema_version == 1` for the S1 kinds that carry it,
/// * the kind's reserved identity prefix, and
/// * `instance == payload.agent`, which `Tuple::new(.., caller, ..)` makes
///   unconditional for every BBS write. A row where they disagree was not
///   written by that path.
fn artifact_record_defect(t: &Value, bbs_kind: &str) -> Option<String> {
    if t["category"] != "artifact" {
        return Some(format!(
            "category is {} not artifact",
            t["category"].as_str().unwrap_or("absent")
        ));
    }
    if t["lifecycle"] != "furniture" {
        return Some(format!(
            "lifecycle is {} not furniture; a BBS record is immutable furniture",
            t["lifecycle"].as_str().unwrap_or("absent")
        ));
    }
    if t["payload"]["bbs_kind"] != bbs_kind {
        return Some(format!("payload.bbs_kind is not {bbs_kind}"));
    }
    // `answer` predates the versioned S1 records and carries no
    // schema_version; every other kind must declare it.
    if bbs_kind != ANSWER && t["payload"]["schema_version"] != 1 {
        return Some("payload.schema_version is not 1".into());
    }
    if let Some(prefix) = reserved_prefix(bbs_kind) {
        let identity = str_field(t, &["identity"]);
        if !identity.starts_with(prefix) {
            return Some(format!(
                "identity {identity:?} does not carry the daemon-minted {prefix}* prefix"
            ));
        }
    }
    let instance = str_field(t, &["instance"]);
    let agent = str_field(t, &["payload", "agent"]);
    if instance.is_empty() || agent.is_empty() || instance != agent {
        return Some(format!(
            "instance {instance:?} does not match payload.agent {agent:?}; every BBS write \
             authors the tuple as its own caller"
        ));
    }
    None
}

/// Structural contract check for a daemon-authored telemetry Event
/// (`exposure`/`open`/`agent_exit`/`agent_final_usage`).
///
/// Note what is deliberately NOT checked: `instance == payload.agent`. These
/// records are authored by the CASTLE about an agent, so `instance` is the
/// castle and the consumer identity lives only in the payload. Reading
/// `instance` as the consumer generation is the mistake this comment exists
/// to prevent.
fn telemetry_record_defect(t: &Value, bbs_kind: &str) -> Option<String> {
    if t["category"] != "event" {
        return Some(format!(
            "category is {} not event",
            t["category"].as_str().unwrap_or("absent")
        ));
    }
    if t["lifecycle"] != "furniture" {
        return Some(format!(
            "lifecycle is {} not furniture; daemon telemetry is immutable furniture",
            t["lifecycle"].as_str().unwrap_or("absent")
        ));
    }
    if t["payload"]["bbs_kind"] != bbs_kind {
        return Some(format!("payload.bbs_kind is not {bbs_kind}"));
    }
    if t["payload"]["schema_version"] != 1 {
        return Some("payload.schema_version is not 1".into());
    }
    if let Some(prefix) = reserved_prefix(bbs_kind) {
        let identity = str_field(t, &["identity"]);
        if !identity.starts_with(prefix) {
            return Some(format!(
                "identity {identity:?} does not carry the daemon-minted {prefix}* prefix"
            ));
        }
    }
    // The daemon writes the repo into the payload as well as the tuple scope;
    // a row where they disagree was not produced by that path.
    let scope = str_field(t, &["scope"]);
    let repo = str_field(t, &["payload", "repo"]);
    if repo.is_empty() {
        return Some("payload.repo is absent".into());
    }
    if scope != repo {
        return Some(format!(
            "tuple scope {scope:?} disagrees with payload.repo {repo:?}"
        ));
    }
    None
}

/// Whether a payload merely CLAIMS a `bbs_kind`. Used to decide whether a row
/// is worth validating (and therefore worth reporting as invalid) at all — a
/// tuple with no `bbs_kind` is ordinary traffic, not a defective BBS record.
fn claims_kind(t: &Value, bbs_kind: &str) -> bool {
    t["payload"]["bbs_kind"] == bbs_kind
}

fn is_task_span(t: &Value) -> bool {
    t["category"] == "event" && t["identity"] == "task_span"
}

fn is_harness_result(t: &Value) -> bool {
    t["category"] == "event" && t["identity"] == "harness_result"
}

fn is_launch_event(t: &Value) -> bool {
    t["category"] == "event"
        && t["identity"]
            .as_str()
            .is_some_and(|i| LAUNCH_EVENT_IDENTITIES.contains(&i))
}

/// `assessment` is documented as operator-only. A tuple that matches the
/// structural shape but was not authored by `operator` cannot be trusted as
/// an authoritative verdict.
fn assessment_defect(t: &Value) -> Option<String> {
    if let Some(reason) = artifact_record_defect(t, ASSESSMENT) {
        return Some(reason);
    }
    if t["payload"]["agent"] != "operator" {
        return Some(format!(
            "assessment is authored by {:?}, but assessing is operator-only",
            t["payload"]["agent"].as_str().unwrap_or("absent")
        ));
    }
    None
}

/// How an evidence list resolved. `Unknown` is NOT `Invalid`: a legacy or
/// partially-captured record whose evidence simply is not in this capture
/// stays unknown and merely cannot certify anything, while a record naming a
/// non-artifact, a foreign repo, or a non-string member is malformed.
#[derive(Debug, Clone, PartialEq, Eq)]
enum EvidenceCheck {
    Resolved,
    Unknown(String),
    Invalid(String),
}

impl EvidenceCheck {
    fn label(&self) -> &'static str {
        match self {
            Self::Resolved => "resolved",
            Self::Unknown(_) => "unknown",
            Self::Invalid(_) => "invalid",
        }
    }
}

/// A record's evidence is only trustworthy when every id it names resolves to
/// a real ARTIFACT tuple present in this same repo's capture. Checking scope
/// alone let an event, a receipt, or a telemetry row stand in for an
/// artifact; filtering non-string members silently let `["ok", 7]` pass as if
/// it had named one thing.
fn check_evidence(evidence: &Value, by_id: &BTreeMap<&str, &Value>, repo: &str) -> EvidenceCheck {
    let Some(items) = evidence.as_array() else {
        return EvidenceCheck::Invalid(if evidence.is_null() {
            "evidence is absent".into()
        } else {
            "evidence is not a JSON array".into()
        });
    };
    if items.is_empty() {
        return EvidenceCheck::Invalid("evidence list is empty".into());
    }
    let mut unknown: Option<String> = None;
    for item in items {
        let Some(id) = item.as_str() else {
            return EvidenceCheck::Invalid(format!(
                "evidence contains a non-string member ({item})"
            ));
        };
        if id.trim().is_empty() {
            return EvidenceCheck::Invalid("evidence contains a blank member".into());
        }
        match by_id.get(id) {
            None => {
                unknown.get_or_insert(format!("evidence {id} is not present in the capture"));
            }
            Some(t) => {
                if t["category"] != "artifact" {
                    return EvidenceCheck::Invalid(format!(
                        "evidence {id} is a {} tuple, not an artifact",
                        t["category"].as_str().unwrap_or("category-less")
                    ));
                }
                if str_field(t, &["scope"]) != repo {
                    return EvidenceCheck::Invalid(format!(
                        "evidence {id} is scoped to {:?}, not {repo}",
                        t["scope"].as_str().unwrap_or("")
                    ));
                }
            }
        }
    }
    match unknown {
        Some(reason) => EvidenceCheck::Unknown(reason),
        None => EvidenceCheck::Resolved,
    }
}

/// Whether `source` is something a receipt may legitimately name. Mirrors the
/// daemon's own `bbs.reuse` rule (crates/rk-daemon/src/bbs.rs): an ordinary
/// artifact with NO `bbs_kind` at all, or a genuine `finding`/`answer`. A
/// receipt, an assessment, or a telemetry record is never a source, and a row
/// merely claiming `"bbs_kind":"finding"` must satisfy the real predicate.
fn source_defect(t: &Value, repo: &str) -> Option<String> {
    if t["category"] != "artifact" {
        return Some(format!(
            "source is a {} tuple; a reuse source must be an artifact",
            t["category"].as_str().unwrap_or("category-less")
        ));
    }
    if str_field(t, &["scope"]) != repo {
        return Some(format!(
            "source scope {:?} != pair repo {repo}",
            t["scope"].as_str().unwrap_or("")
        ));
    }
    match t["payload"].get("bbs_kind") {
        None => None, // ordinary artifact: reusable regardless of lifecycle
        Some(kind) => {
            let Some(kind) = kind.as_str() else {
                return Some("payload.bbs_kind is present but not a string".into());
            };
            if kind != FINDING && kind != ANSWER {
                return Some(format!(
                    "a {kind} record is not a reusable source (only an ordinary artifact or a \
                     finding/answer may be reused)"
                ));
            }
            artifact_record_defect(t, kind).map(|d| format!("claims {kind} but {d}"))
        }
    }
}

fn parse_rfc3339(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

fn created_at(t: &Value) -> Option<DateTime<Utc>> {
    t["created_at"].as_str().and_then(parse_rfc3339)
}

fn payload_time(t: &Value, field: &str) -> Option<DateTime<Utc>> {
    t["payload"][field].as_str().and_then(parse_rfc3339)
}

// ---------------------------------------------------------------------
// Native observation index: scope-, window- and identity-filtered.
// ---------------------------------------------------------------------

struct ExitRow {
    repo: String,
    task: String,
    agent: String,
    spawn: String,
    session: String,
    exited_at: Option<DateTime<Utc>>,
    launched_at: Option<DateTime<Utc>>,
    exit_code: Option<i64>,
    crashed: Option<bool>,
    prior_state: Option<String>,
    record: String,
}

struct UsageRow {
    repo: String,
    task: String,
    spawn: String,
    session: String,
    provider_session: Option<String>,
    state: Option<String>,
    cost_usd: Option<f64>,
    cost_basis: String,
    observed_at: Option<DateTime<Utc>>,
    record: String,
}

struct CompletionRow {
    repo: String,
    task: String,
    agent: String,
    spawn: String,
    declared_done: bool,
    is_error: bool,
    cost_usd: Option<f64>,
    record: String,
}

/// Native proof that a generation actually started a process, or at least ran
/// far enough to author a record of its own. Used for two distinct things: to
/// decide whether a prepared exposure was a *launched* consumer opportunity,
/// and to refuse an author-exit claim when the generation was relaunched
/// between the exit and the consuming decision.
struct LaunchRow {
    repo: String,
    spawn: String,
    /// Present only for records that identify a physical launch. An
    /// `agent_spawned`/`agent_respawned` event and an authored record do not,
    /// so a relaunch is detected by time, not by session identity alone.
    session: Option<String>,
    at: Option<DateTime<Utc>>,
    record: String,
    kind: &'static str,
}

struct Index<'a> {
    by_id: BTreeMap<&'a str, &'a Value>,
    /// `(repo, source, consumer_spawn)` -> exposure record id.
    prepared: BTreeMap<(String, String, String), String>,
    /// `(repo, source, consumer_spawn)`.
    opened: BTreeSet<(String, String, String)>,
    launches: Vec<LaunchRow>,
    exits: Vec<ExitRow>,
    usage: Vec<UsageRow>,
    completions: Vec<CompletionRow>,
    spans: Vec<&'a Value>,
    /// `(source, consumer_task)` -> candidate receipts, in capture order.
    reuse_by_source_task: BTreeMap<(String, String), Vec<&'a Value>>,
    /// `receipt id` -> `(capture position, assessment)`.
    assessment_by_receipt: BTreeMap<String, Vec<(usize, &'a Value)>>,
    invalid: Vec<InvalidRecord>,
    unresolved: Vec<InvalidRecord>,
    capture: CaptureSummary,
}

fn build_index<'a>(capture: &'a TupleCapture, manifest: &Manifest) -> Index<'a> {
    let repos: BTreeSet<&str> = manifest.repos.iter().map(String::as_str).collect();
    let in_scope = |repo: &str| repos.is_empty() || repos.contains(repo);
    let window = &manifest.window;

    let by_id: BTreeMap<&str, &Value> = capture
        .tuples
        .iter()
        .filter_map(|t| t.get("id").and_then(Value::as_str).map(|id| (id, t)))
        .collect();

    let mut idx = Index {
        by_id,
        prepared: BTreeMap::new(),
        opened: BTreeSet::new(),
        launches: Vec::new(),
        exits: Vec::new(),
        usage: Vec::new(),
        completions: Vec::new(),
        spans: Vec::new(),
        reuse_by_source_task: BTreeMap::new(),
        assessment_by_receipt: BTreeMap::new(),
        invalid: Vec::new(),
        unresolved: Vec::new(),
        capture: CaptureSummary {
            tuples: capture.tuples.len(),
            ..CaptureSummary::default()
        },
    };

    // An observation is accepted only when it is in a manifest repo AND
    // inside the frozen window. Both rejections are counted so a scoping or
    // window mistake shows up as coverage loss instead of silently shrinking
    // every metric to zero.
    let admit = |idx: &mut Index<'a>, t: &Value, at: Option<DateTime<Utc>>| -> bool {
        let scope = str_field(t, &["scope"]);
        if !in_scope(scope) {
            idx.capture.observations_out_of_scope_repo += 1;
            return false;
        }
        match window.fit(at) {
            WindowFit::Inside => {
                idx.capture.observations_in_window += 1;
                true
            }
            WindowFit::Outside => {
                idx.capture.observations_out_of_window += 1;
                false
            }
            WindowFit::Undated => {
                idx.capture.observations_undated += 1;
                idx.unresolved.push(InvalidRecord {
                    record: record_id(t),
                    kind: "observation".into(),
                    reason: "no parsable timestamp, so it cannot be placed in the frozen window"
                        .into(),
                });
                false
            }
        }
    };

    for (pos, t) in capture.tuples.iter().enumerate() {
        let scope = str_field(t, &["scope"]).to_string();

        // --- exposure -------------------------------------------------
        if claims_kind(t, EXPOSURE) {
            if let Some(reason) = telemetry_record_defect(t, EXPOSURE) {
                idx.invalid.push(InvalidRecord {
                    record: record_id(t),
                    kind: EXPOSURE.into(),
                    reason,
                });
                continue;
            }
            if !admit(&mut idx, t, created_at(t)) {
                continue;
            }
            // Only an exact agent generation counts toward agent exposure
            // rates. `bound` is the daemon's own statement about whether it
            // could establish one; operator/unbound rows are excluded by
            // design rather than attributed to a guess.
            if str_field(t, &["payload", "bound"]) != "agent" {
                idx.unresolved.push(InvalidRecord {
                    record: record_id(t),
                    kind: EXPOSURE.into(),
                    reason: format!(
                        "bound={:?}: no exact consumer generation, excluded from agent exposure",
                        t["payload"]["bound"].as_str().unwrap_or("absent")
                    ),
                });
                continue;
            }
            let spawn = str_field(t, &["payload", "spawn"]).to_string();
            if spawn.is_empty() {
                idx.invalid.push(InvalidRecord {
                    record: record_id(t),
                    kind: EXPOSURE.into(),
                    reason: "bound=agent but payload.spawn is absent".into(),
                });
                continue;
            }
            match t["payload"]["entries"].as_array() {
                None => idx.invalid.push(InvalidRecord {
                    record: record_id(t),
                    kind: EXPOSURE.into(),
                    reason: "payload.entries is not an array".into(),
                }),
                Some(entries) => {
                    for e in entries {
                        // The producer writes `source`; nothing else names the
                        // selected tuple. An entry without it is malformed,
                        // not silently skipped.
                        match e["source"].as_str() {
                            Some(src) if !src.trim().is_empty() => {
                                idx.prepared.insert(
                                    (scope.clone(), src.to_string(), spawn.clone()),
                                    record_id(t),
                                );
                            }
                            _ => idx.invalid.push(InvalidRecord {
                                record: record_id(t),
                                kind: "exposure_entry".into(),
                                reason: format!("entry has no `source` id ({e})"),
                            }),
                        }
                    }
                }
            }
            continue;
        }

        // --- open -----------------------------------------------------
        if claims_kind(t, OPEN) {
            if let Some(reason) = telemetry_record_defect(t, OPEN) {
                idx.invalid.push(InvalidRecord {
                    record: record_id(t),
                    kind: OPEN.into(),
                    reason,
                });
                continue;
            }
            if !admit(&mut idx, t, created_at(t)) {
                continue;
            }
            if str_field(t, &["payload", "bound"]) != "agent" {
                idx.unresolved.push(InvalidRecord {
                    record: record_id(t),
                    kind: OPEN.into(),
                    reason: format!(
                        "bound={:?}: no exact consumer generation, excluded from agent opens",
                        t["payload"]["bound"].as_str().unwrap_or("absent")
                    ),
                });
                continue;
            }
            let spawn = str_field(t, &["payload", "spawn"]).to_string();
            let source = str_field(t, &["payload", "source"]).to_string();
            if spawn.is_empty() || source.is_empty() {
                idx.invalid.push(InvalidRecord {
                    record: record_id(t),
                    kind: OPEN.into(),
                    reason: "payload.spawn or payload.source is absent".into(),
                });
                continue;
            }
            idx.opened.insert((scope.clone(), source, spawn));
            continue;
        }

        // --- agent_exit -----------------------------------------------
        if claims_kind(t, AGENT_EXIT) {
            if let Some(reason) = telemetry_record_defect(t, AGENT_EXIT) {
                idx.invalid.push(InvalidRecord {
                    record: record_id(t),
                    kind: AGENT_EXIT.into(),
                    reason,
                });
                continue;
            }
            let exited_at = payload_time(t, "exited_at");
            if !admit(&mut idx, t, exited_at.or_else(|| created_at(t))) {
                continue;
            }
            let spawn = str_field(t, &["payload", "spawn"]).to_string();
            let session = str_field(t, &["payload", "session"]).to_string();
            if spawn.is_empty() || session.is_empty() {
                idx.invalid.push(InvalidRecord {
                    record: record_id(t),
                    kind: AGENT_EXIT.into(),
                    reason: "an exit must identify both its generation (spawn) and its physical \
                             launch (session)"
                        .into(),
                });
                continue;
            }
            let launched_at = payload_time(t, "launched_at");
            idx.launches.push(LaunchRow {
                repo: scope.clone(),
                spawn: spawn.clone(),
                session: Some(session.clone()),
                at: launched_at,
                record: record_id(t),
                kind: AGENT_EXIT,
            });
            idx.exits.push(ExitRow {
                repo: scope.clone(),
                task: str_field(t, &["payload", "task"]).to_string(),
                agent: str_field(t, &["payload", "agent"]).to_string(),
                spawn,
                session,
                exited_at,
                launched_at,
                exit_code: t["payload"]["exit_code"].as_i64(),
                crashed: t["payload"]["crashed"].as_bool(),
                prior_state: t["payload"]["prior_state"].as_str().map(str::to_string),
                record: record_id(t),
            });
            continue;
        }

        // --- agent_final_usage ----------------------------------------
        if claims_kind(t, AGENT_FINAL_USAGE) {
            if let Some(reason) = telemetry_record_defect(t, AGENT_FINAL_USAGE) {
                idx.invalid.push(InvalidRecord {
                    record: record_id(t),
                    kind: AGENT_FINAL_USAGE.into(),
                    reason,
                });
                continue;
            }
            let observed_at = payload_time(t, "observed_at");
            if !admit(&mut idx, t, observed_at.or_else(|| created_at(t))) {
                continue;
            }
            let spawn = str_field(t, &["payload", "spawn"]).to_string();
            let session = str_field(t, &["payload", "session"]).to_string();
            if spawn.is_empty() || session.is_empty() {
                idx.invalid.push(InvalidRecord {
                    record: record_id(t),
                    kind: AGENT_FINAL_USAGE.into(),
                    reason: "a usage observation must identify both its generation (spawn) and \
                             its physical launch (session)"
                        .into(),
                });
                continue;
            }
            let cost_basis = str_field(t, &["payload", "cost_basis"]).to_string();
            if cost_basis.is_empty() {
                idx.invalid.push(InvalidRecord {
                    record: record_id(t),
                    kind: AGENT_FINAL_USAGE.into(),
                    reason: "payload.cost_basis is absent; a cost with no stated basis cannot be \
                             aggregated"
                        .into(),
                });
                continue;
            }
            idx.launches.push(LaunchRow {
                repo: scope.clone(),
                spawn: spawn.clone(),
                session: Some(session.clone()),
                at: observed_at,
                record: record_id(t),
                kind: AGENT_FINAL_USAGE,
            });
            idx.usage.push(UsageRow {
                repo: scope.clone(),
                task: str_field(t, &["payload", "task"]).to_string(),
                spawn,
                session,
                provider_session: t["payload"]["provider_session"]
                    .as_str()
                    .map(str::to_string),
                state: t["payload"]["state"].as_str().map(str::to_string),
                cost_usd: t["payload"]["cost_usd"].as_f64(),
                cost_basis,
                observed_at,
                record: record_id(t),
            });
            continue;
        }

        // --- harness_result (completion, NOT exit) --------------------
        if is_harness_result(t) {
            if !admit(&mut idx, t, created_at(t)) {
                continue;
            }
            let spawn = str_field(t, &["payload", "spawn"]).to_string();
            if spawn.is_empty() {
                // Pre-C1 completions carry no generation id. Legacy and
                // unattributed, so retained as an explicit unknown rather
                // than attributed to whoever shares the agent name.
                idx.unresolved.push(InvalidRecord {
                    record: record_id(t),
                    kind: "harness_result".into(),
                    reason: "no payload.spawn: legacy completion cannot be bound to an exact \
                             generation"
                        .into(),
                });
                continue;
            }
            idx.launches.push(LaunchRow {
                repo: scope.clone(),
                spawn: spawn.clone(),
                session: None,
                at: created_at(t),
                record: record_id(t),
                kind: "harness_result",
            });
            idx.completions.push(CompletionRow {
                repo: scope.clone(),
                task: str_field(t, &["payload", "task"]).to_string(),
                agent: str_field(t, &["payload", "agent"]).to_string(),
                spawn,
                declared_done: t["payload"]["declared_done"].as_bool().unwrap_or(false),
                is_error: t["payload"]["is_error"].as_bool().unwrap_or(false),
                cost_usd: t["payload"]["cost_usd"].as_f64(),
                record: record_id(t),
            });
            continue;
        }

        // --- agent_spawned / agent_respawned --------------------------
        if is_launch_event(t) {
            if !admit(&mut idx, t, created_at(t)) {
                continue;
            }
            // `spawn` is additive on these events (S2 `48fb2da`); older rows
            // omit it and cannot prove which generation launched.
            let spawn = str_field(t, &["payload", "spawn"]).to_string();
            if spawn.is_empty() {
                idx.unresolved.push(InvalidRecord {
                    record: record_id(t),
                    kind: str_field(t, &["identity"]).to_string(),
                    reason: "no payload.spawn: this launch event predates the additive generation \
                             field and cannot be bound to a generation"
                        .into(),
                });
                continue;
            }
            idx.launches.push(LaunchRow {
                repo: scope.clone(),
                spawn,
                session: None,
                at: created_at(t),
                record: record_id(t),
                kind: "launch_event",
            });
            continue;
        }

        // --- task_span ------------------------------------------------
        if is_task_span(t) {
            if !admit(&mut idx, t, created_at(t)) {
                continue;
            }
            idx.spans.push(t);
            continue;
        }

        // --- reuse receipts -------------------------------------------
        if claims_kind(t, REUSE) {
            if let Some(reason) = artifact_record_defect(t, REUSE) {
                idx.invalid.push(InvalidRecord {
                    record: record_id(t),
                    kind: REUSE.into(),
                    reason,
                });
                continue;
            }
            // A receipt authored by a generation is itself proof that the
            // generation ran, so it also serves as launch evidence.
            let spawn = str_field(t, &["payload", "spawn"]).to_string();
            if !spawn.is_empty() {
                idx.launches.push(LaunchRow {
                    repo: scope.clone(),
                    spawn,
                    session: None,
                    at: created_at(t),
                    record: record_id(t),
                    kind: "authored_record",
                });
            }
            let source = str_field(t, &["payload", "source"]).to_string();
            let task = str_field(t, &["payload", "task"]).to_string();
            idx.reuse_by_source_task
                .entry((source, task))
                .or_default()
                .push(t);
            continue;
        }

        // --- assessments ----------------------------------------------
        if claims_kind(t, ASSESSMENT) {
            if let Some(reason) = assessment_defect(t) {
                idx.invalid.push(InvalidRecord {
                    record: record_id(t),
                    kind: ASSESSMENT.into(),
                    reason,
                });
                continue;
            }
            let receipt = str_field(t, &["payload", "receipt"]).to_string();
            if receipt.is_empty() {
                idx.invalid.push(InvalidRecord {
                    record: record_id(t),
                    kind: ASSESSMENT.into(),
                    reason: "payload.receipt is absent".into(),
                });
                continue;
            }
            idx.assessment_by_receipt
                .entry(receipt)
                .or_default()
                .push((pos, t));
            continue;
        }

        // --- findings ------------------------------------------------
        // Validated here only so a malformed one is REPORTED; source
        // selection re-checks per pair against that pair's repo.
        if claims_kind(t, FINDING) {
            if let Some(reason) = artifact_record_defect(t, FINDING) {
                idx.invalid.push(InvalidRecord {
                    record: record_id(t),
                    kind: FINDING.into(),
                    reason,
                });
                continue;
            }
            let spawn = str_field(t, &["payload", "spawn"]).to_string();
            if !spawn.is_empty() {
                idx.launches.push(LaunchRow {
                    repo: scope.clone(),
                    spawn,
                    session: None,
                    at: created_at(t),
                    record: record_id(t),
                    kind: "authored_record",
                });
            }
        }
    }

    idx.invalid
        .sort_by(|a, b| (&a.record, &a.kind).cmp(&(&b.record, &b.kind)));
    idx.unresolved
        .sort_by(|a, b| (&a.record, &a.kind).cmp(&(&b.record, &b.kind)));
    idx
}

impl Index<'_> {
    /// Native proof that `spawn` ran in `repo`.
    fn launched(&self, repo: &str, spawn: &str) -> bool {
        self.launches
            .iter()
            .any(|l| l.repo == repo && l.spawn == spawn)
    }

    /// The earliest observed launch of `spawn` strictly after `after` and no
    /// later than `until` — i.e. a relaunch that invalidates treating an
    /// earlier exit as permanent. `agent_spawned`/`agent_respawned` and
    /// authored records do not carry a session, so this is a time test, not a
    /// session-identity test; a record belonging to the already-exited
    /// session is excluded by `exclude_session`.
    fn relaunch_between(
        &self,
        repo: &str,
        spawn: &str,
        exclude_session: &str,
        after: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Option<&LaunchRow> {
        self.launches
            .iter()
            .filter(|l| l.repo == repo && l.spawn == spawn)
            .filter(|l| l.session.as_deref() != Some(exclude_session))
            .filter(|l| l.at.is_some_and(|at| at > after && at <= until))
            .min_by_key(|l| (l.at, l.record.clone()))
    }
}

/// Validates a review's `author_terminal_evidence` id against the capture.
///
/// Only an `agent_exit` observation can establish this, and only for the
/// source author's exact `(spawn, session)` in the pair's repo, no later than
/// the reuse, with no relaunch of that generation in between. The two
/// tempting alternatives are both refused with an explicit reason:
///
/// * `harness_result` is task-completion evidence. `Supervisor::route_completion`
///   emits it when the agent routes `rk done`; the OS process is still alive
///   until `HarnessEvent::Exited`, and the provider may still report a later,
///   different total for the same query.
/// * `agent_lifecycle` carries no `spawn` field at all (its identity fields
///   are `agent` and `generation`), is not repo-bound, and its `change` may be
///   `started` — so it cannot identify a terminal generation even in
///   principle.
fn resolve_author_exit(
    evidence_id: &str,
    idx: &Index<'_>,
    repo: &str,
    source_agent: &str,
    source_spawn: &str,
    claim_created: Option<DateTime<Utc>>,
) -> std::result::Result<String, String> {
    let Some(ev) = idx.by_id.get(evidence_id) else {
        return Err(format!(
            "author_terminal_evidence {evidence_id} is not present in the tuple capture"
        ));
    };
    if is_harness_result(ev) {
        return Err(format!(
            "{evidence_id} is a harness_result: task-completion evidence emitted at `rk done` \
             while the process is still alive. Author-exit requires an agent_exit observation"
        ));
    }
    if ev["identity"] == "agent_lifecycle" {
        return Err(format!(
            "{evidence_id} is an agent_lifecycle event: it carries no `spawn` binding, is not \
             repo-bound, and its `change` may be `started`, so it cannot establish a terminal \
             generation"
        ));
    }
    if !claims_kind(ev, AGENT_EXIT) {
        return Err(format!("{evidence_id} is not an agent_exit observation"));
    }
    if let Some(reason) = telemetry_record_defect(ev, AGENT_EXIT) {
        return Err(format!(
            "{evidence_id} is not a valid agent_exit record: {reason}"
        ));
    }
    if source_spawn.is_empty() {
        return Err(format!(
            "the source carries no authoring generation, so {evidence_id} cannot be bound to it \
             (legacy/unattributed source)"
        ));
    }
    let Some(exit) = idx.exits.iter().find(|e| e.record == evidence_id) else {
        return Err(format!(
            "{evidence_id} is an agent_exit record but was excluded from the frozen repo scope or \
             measurement window"
        ));
    };
    if exit.repo != repo {
        return Err(format!(
            "{evidence_id} is scoped to repo {}, not the pair's {repo}",
            exit.repo
        ));
    }
    if exit.spawn != source_spawn {
        return Err(format!(
            "{evidence_id} records the exit of generation {}, not the source's {source_spawn}",
            exit.spawn
        ));
    }
    if !source_agent.is_empty() && !exit.agent.is_empty() && exit.agent != source_agent {
        return Err(format!(
            "{evidence_id} names agent {}, not the source's author {source_agent}",
            exit.agent
        ));
    }
    let Some(exited_at) = exit.exited_at else {
        return Err(format!("{evidence_id} carries no parsable exited_at"));
    };
    let Some(claim_at) = claim_created else {
        return Err(format!(
            "{evidence_id} cannot be ordered against the reuse: the reuse has no captured, \
             parsable timestamp"
        ));
    };
    if exited_at > claim_at {
        return Err(format!(
            "{evidence_id} records an exit at {exited_at} which is AFTER the reuse at {claim_at}"
        ));
    }
    // A manual respawn continues the same SpawnId, so an earlier exit is not
    // proof the author was gone when the consumer decided.
    if let Some(relaunch) =
        idx.relaunch_between(repo, source_spawn, &exit.session, exited_at, claim_at)
    {
        return Err(format!(
            "generation {source_spawn} was observed running again at {} ({} {}) after the exit at \
             {exited_at} and before the reuse at {claim_at}",
            relaunch
                .at
                .map(|t| t.to_rfc3339())
                .unwrap_or_else(|| "unknown time".into()),
            relaunch.kind,
            relaunch.record
        ));
    }
    Ok(evidence_id.to_string())
}

// ---------------------------------------------------------------------
// Pair evaluation.
// ---------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
fn evaluate_pair(
    pair: &EligiblePair,
    review: Option<&Review>,
    idx: &Index<'_>,
    capture_order: Order,
    excluded: &mut Vec<Excluded>,
    rejected_claims: &mut Vec<RejectedClaim>,
    unresolved: &mut Vec<InvalidRecord>,
    ambiguous_assessments: &mut Vec<AmbiguousAssessment>,
    author_exit_unsupported: &mut Vec<AuthorExitUnsupported>,
) -> Option<PairResult> {
    let Some(source) = idx.by_id.get(pair.source.as_str()) else {
        excluded.push(Excluded {
            pair: pair.id.clone(),
            reason: "source_not_captured".into(),
            detail: format!("source {} is not present in the tuple capture", pair.source),
        });
        return None;
    };
    if let Some(reason) = source_defect(source, &pair.repo) {
        excluded.push(Excluded {
            pair: pair.id.clone(),
            reason: "invalid_source".into(),
            detail: reason,
        });
        return None;
    }
    let source_kind = source["payload"]["bbs_kind"]
        .as_str()
        .unwrap_or("artifact")
        .to_string();
    let source_spawn = str_field(source, &["payload", "spawn"]).to_string();
    let source_agent = str_field(source, &["payload", "agent"]).to_string();
    let source_attributed = !source_spawn.is_empty();
    if source_attributed && source_spawn == pair.consumer_generation {
        excluded.push(Excluded {
            pair: pair.id.clone(),
            reason: "self".into(),
            detail: "source was authored by the consumer's own generation".into(),
        });
        return None;
    }
    // A finding declares its evidence; an ordinary artifact has no evidence
    // contract to check. Unresolvable evidence leaves the opportunity standing
    // but keeps it from certifying anything.
    let source_evidence = if source_kind == FINDING || source_kind == ANSWER {
        let check = check_evidence(&source["payload"]["evidence"], &idx.by_id, &pair.repo);
        match &check {
            EvidenceCheck::Invalid(reason) => {
                excluded.push(Excluded {
                    pair: pair.id.clone(),
                    reason: "invalid_source_evidence".into(),
                    detail: reason.clone(),
                });
                return None;
            }
            EvidenceCheck::Unknown(reason) => unresolved.push(InvalidRecord {
                record: pair.source.clone(),
                kind: source_kind.clone(),
                reason: reason.clone(),
            }),
            EvidenceCheck::Resolved => {}
        }
        check.label().to_string()
    } else {
        "not_applicable".to_string()
    };
    let source_created = created_at(source);

    // --- the receipt -------------------------------------------------
    // Every rejection below removes the CLAIM, not the pair: the opportunity
    // stays in the eligible denominator with no claimed outcome.
    let mut claim: Option<&Value> = None;
    if let Some(candidates) = idx
        .reuse_by_source_task
        .get(&(pair.source.clone(), pair.consumer_task.clone()))
    {
        for c in candidates {
            let cid = record_id(c);
            if str_field(c, &["scope"]) != pair.repo {
                rejected_claims.push(RejectedClaim {
                    pair: pair.id.clone(),
                    record: cid,
                    reason: "wrong_repo".into(),
                    detail: format!(
                        "receipt is scoped to {:?}, not the pair's {}",
                        c["scope"].as_str().unwrap_or(""),
                        pair.repo
                    ),
                });
                continue;
            }
            let spawn = str_field(c, &["payload", "spawn"]);
            if spawn != pair.consumer_generation {
                rejected_claims.push(RejectedClaim {
                    pair: pair.id.clone(),
                    record: cid,
                    reason: "wrong_generation".into(),
                    detail: format!(
                        "receipt was written by generation {spawn:?}, not the pair's {}",
                        pair.consumer_generation
                    ),
                });
                continue;
            }
            match check_evidence(&c["payload"]["evidence"], &idx.by_id, &pair.repo) {
                EvidenceCheck::Invalid(detail) => {
                    rejected_claims.push(RejectedClaim {
                        pair: pair.id.clone(),
                        record: cid,
                        reason: "invalid_evidence".into(),
                        detail,
                    });
                    continue;
                }
                EvidenceCheck::Unknown(detail) => {
                    rejected_claims.push(RejectedClaim {
                        pair: pair.id.clone(),
                        record: cid,
                        reason: "unresolved_evidence".into(),
                        detail,
                    });
                    continue;
                }
                EvidenceCheck::Resolved => {}
            }
            let claim_created = created_at(c);
            if claim_created.is_none() {
                rejected_claims.push(RejectedClaim {
                    pair: pair.id.clone(),
                    record: cid,
                    reason: "undated".into(),
                    detail: "receipt has no parsable created_at, so it cannot be ordered against \
                             the source"
                        .into(),
                });
                continue;
            }
            if let (Some(sc), Some(cc)) = (source_created, claim_created) {
                if sc > cc {
                    rejected_claims.push(RejectedClaim {
                        pair: pair.id.clone(),
                        record: cid,
                        reason: "future_source".into(),
                        detail: format!(
                            "source {} was created at {sc}, after the reuse it supposedly \
                             informed at {cc}",
                            pair.source
                        ),
                    });
                    continue;
                }
            }
            claim = Some(c);
            break;
        }
    }

    let claimed_outcome = claim
        .and_then(|c| c["payload"]["outcome"].as_str())
        .map(str::to_string);
    let claim_evidence = claim.map(|c| record_id(c));
    let claim_created = claim.and_then(created_at);

    // --- the verdict --------------------------------------------------
    let mut assessed_verdict: Option<String> = None;
    let mut assessment_evidence: Option<String> = None;
    if let Some(receipt) = &claim_evidence {
        if let Some(all) = idx.assessment_by_receipt.get(receipt) {
            let mut candidates: Vec<(usize, &Value)> = Vec::new();
            for (pos, a) in all {
                if str_field(a, &["scope"]) != pair.repo {
                    unresolved.push(InvalidRecord {
                        record: record_id(a),
                        kind: ASSESSMENT.into(),
                        reason: format!(
                            "scoped to {:?}, not the assessed pair's repo {}",
                            a["scope"].as_str().unwrap_or(""),
                            pair.repo
                        ),
                    });
                    continue;
                }
                match check_evidence(&a["payload"]["evidence"], &idx.by_id, &pair.repo) {
                    EvidenceCheck::Resolved => candidates.push((*pos, a)),
                    EvidenceCheck::Invalid(reason) | EvidenceCheck::Unknown(reason) => {
                        unresolved.push(InvalidRecord {
                            record: record_id(a),
                            kind: ASSESSMENT.into(),
                            reason: format!("verdict cannot be trusted: {reason}"),
                        });
                    }
                }
            }
            match candidates.len() {
                0 => {}
                1 => {
                    let (_, a) = candidates[0];
                    assessed_verdict = a["payload"]["verdict"].as_str().map(str::to_string);
                    assessment_evidence = Some(record_id(a));
                }
                n => {
                    if capture_order == Order::PersistenceSequence {
                        let (_, a) = candidates.iter().max_by_key(|(pos, _)| *pos).unwrap();
                        assessed_verdict = a["payload"]["verdict"].as_str().map(str::to_string);
                        assessment_evidence = Some(record_id(a));
                    } else {
                        ambiguous_assessments.push(AmbiguousAssessment {
                            pair: pair.id.clone(),
                            receipt: receipt.clone(),
                            reason: format!(
                                "{n} assessments for this receipt but tuple capture order is \
                                 unknown; the current one cannot be determined without \
                                 persistence order"
                            ),
                        });
                    }
                }
            }
        }
    }

    // --- coverage -----------------------------------------------------
    // Native telemetry and operator judgment are kept strictly apart, and a
    // prepared selection for a generation that never ran is neither.
    let consumer_launched = idx.launched(&pair.repo, &pair.consumer_generation);
    let native_prepared = idx.prepared.get(&(
        pair.repo.clone(),
        pair.source.clone(),
        pair.consumer_generation.clone(),
    ));
    let (coverage_status, coverage_provenance, coverage_reference, coverage_reference_resolved) =
        match (native_prepared, consumer_launched) {
            (Some(record), true) => (
                "prepared".to_string(),
                "native".to_string(),
                Some(record.clone()),
                Some(true),
            ),
            (Some(record), false) => (
                "prepared_not_launched".to_string(),
                "native".to_string(),
                Some(record.clone()),
                Some(true),
            ),
            (None, _) => match review.map(|r| &r.coverage) {
                Some(Coverage::Prepared { evidence }) => {
                    let r = evidence.reference().to_string();
                    let resolved = idx.by_id.contains_key(r.as_str());
                    (
                        "prepared".to_string(),
                        "reviewed".to_string(),
                        Some(r),
                        Some(resolved),
                    )
                }
                Some(Coverage::NotPrepared { evidence }) => {
                    let r = evidence.reference().to_string();
                    let resolved = idx.by_id.contains_key(r.as_str());
                    (
                        "not_prepared".to_string(),
                        "reviewed".to_string(),
                        Some(r),
                        Some(resolved),
                    )
                }
                _ => ("unknown".to_string(), "none".to_string(), None, None),
            },
        };
    let opened = idx.opened.contains(&(
        pair.repo.clone(),
        pair.source.clone(),
        pair.consumer_generation.clone(),
    ));

    // --- author exit --------------------------------------------------
    let mut author_terminal = false;
    let mut author_terminal_evidence = None;
    if let Some(evidence_id) = review.and_then(|r| r.author_terminal_evidence.as_deref()) {
        match resolve_author_exit(
            evidence_id,
            idx,
            &pair.repo,
            &source_agent,
            &source_spawn,
            claim_created,
        ) {
            Ok(id) => {
                author_terminal = true;
                author_terminal_evidence = Some(id);
            }
            Err(reason) => author_exit_unsupported.push(AuthorExitUnsupported {
                pair: pair.id.clone(),
                evidence: evidence_id.to_string(),
                reason,
            }),
        }
    }
    let relayed_by_operator = review.is_some_and(|r| r.relayed_by_operator);
    let regression = review.is_some_and(|r| r.regression);

    // A verified effect must never be certified on incomplete evidence.
    // Every conjunct below is a real observation, not a default:
    let temporal_order_known = source_created.is_some() && claim_created.is_some();
    let changed_work = matches!(claimed_outcome.as_deref(), Some("used") | Some("adapted"));
    let verified_effect = changed_work
        && assessed_verdict.as_deref() == Some("verified")
        && temporal_order_known
        && coverage_status == "prepared"
        && consumer_launched
        && source_evidence != "unknown"
        && claim_evidence.is_some();
    let counts_as_effect = verified_effect && !relayed_by_operator;

    Some(PairResult {
        pair: pair.id.clone(),
        source: pair.source.clone(),
        source_kind,
        source_attributed,
        source_evidence,
        consumer_task: pair.consumer_task.clone(),
        consumer_generation: pair.consumer_generation.clone(),
        repo: pair.repo.clone(),
        batch: pair.batch.clone(),
        coverage_status,
        coverage_provenance,
        coverage_reference,
        coverage_reference_resolved,
        consumer_launched,
        opened,
        claimed_outcome,
        claim_evidence,
        assessed_verdict,
        assessment_evidence,
        author_terminal,
        author_terminal_evidence,
        relayed_by_operator,
        regression,
        verified_effect,
        counts_as_effect,
    })
}

// ---------------------------------------------------------------------
// Delivery derivation: frozen task scope, NOT surviving pairs.
// ---------------------------------------------------------------------

struct TaskScope {
    task: String,
    repo: String,
    batches: BTreeSet<String>,
    enrollment: &'static str,
}

fn frozen_task_scope(manifest: &Manifest) -> Vec<TaskScope> {
    let mut scopes: BTreeMap<(String, String), TaskScope> = BTreeMap::new();
    for c in &manifest.consumer_tasks {
        let e = scopes
            .entry((c.task.clone(), c.repo.clone()))
            .or_insert_with(|| TaskScope {
                task: c.task.clone(),
                repo: c.repo.clone(),
                batches: BTreeSet::new(),
                enrollment: "frozen_consumer_task",
            });
        e.batches.insert(c.batch.clone());
        e.enrollment = "frozen_consumer_task";
    }
    // Supported fixture enrollment: a predeclared pair's consumer task is in
    // scope for delivery accounting even when the manifest lists no
    // `consumer_tasks` (the deterministic fixture/replay shape).
    for p in &manifest.eligible_pairs {
        let e = scopes
            .entry((p.consumer_task.clone(), p.repo.clone()))
            .or_insert_with(|| TaskScope {
                task: p.consumer_task.clone(),
                repo: p.repo.clone(),
                batches: BTreeSet::new(),
                enrollment: "fixture_pair",
            });
        e.batches.insert(p.batch.clone());
    }
    scopes.into_values().collect()
}

/// Per-segment cost finality. A `paused` provider result can be followed by
/// more model usage and then a budget kill with no further result; the earlier
/// cumulative total is then a partial amount, not this launch's final cost.
/// Finality therefore needs an observed exit for the same launch AND a
/// terminal last result AND an exit whose `prior_state` agrees with it.
fn segment_finality(last: &UsageRow, exit: Option<&ExitRow>) -> (bool, String) {
    let state = last.state.as_deref().unwrap_or("");
    if !TERMINAL_USAGE_STATES.contains(&state) {
        return (
            false,
            format!(
                "last result for this segment is state={state:?}, which is not terminal: more \
                 usage may follow"
            ),
        );
    }
    let Some(exit) = exit else {
        return (
            false,
            "no agent_exit observed for this launch, so the process may still be running and \
             report more usage"
                .into(),
        );
    };
    match exit.prior_state.as_deref() {
        None => (
            true,
            format!(
                "terminal result (state={state}) followed by observed exit {}",
                exit.record
            ),
        ),
        Some(prior) if prior == state => (
            true,
            format!(
                "terminal result (state={state}) followed by observed exit {}",
                exit.record
            ),
        ),
        Some(prior) => (
            false,
            format!(
                "last result said state={state:?} but the exit records prior_state={prior:?}: the \
                 launch did more work after that result, so the reported amount is partial"
            ),
        ),
    }
}

fn opt_sum(acc: &mut Option<i64>, v: Option<i64>) {
    if let Some(v) = v {
        *acc.get_or_insert(0) += v;
    }
}

/// Computes the deterministic report. Same inputs always produce the same
/// output (stable sort keys throughout; no reliance on hash-map iteration
/// order, wall-clock "now", or randomness).
#[allow(clippy::too_many_lines)]
pub fn compute(
    manifest: &Manifest,
    capture: &TupleCapture,
    reviews: &ReviewsFile,
) -> Result<Report> {
    validate_manifest(manifest)?;
    let review_list = reviews.pairs();
    let task_annotations = reviews.tasks();
    validate_reviews(review_list, task_annotations, manifest)?;
    let all_pairs = validate_and_merge_pairs(manifest, review_list)?;

    let idx = build_index(capture, manifest);
    let review_by_pair: BTreeMap<&str, &Review> =
        review_list.iter().map(|r| (r.pair.as_str(), r)).collect();

    let mut sorted_pairs = all_pairs;
    sorted_pairs.sort_by(|a, b| a.id.cmp(&b.id));

    let mut seen_keys: BTreeSet<(String, String, String, String)> = BTreeSet::new();
    let mut excluded = Vec::new();
    let mut rejected_claims = Vec::new();
    let mut unresolved = idx.unresolved.clone();
    let mut ambiguous_assessments = Vec::new();
    let mut author_exit_unsupported = Vec::new();
    let mut pairs_out = Vec::new();

    for pair in &sorted_pairs {
        // Duplicate identity is counted once and REPORTED, so a repeated
        // enrollment cannot inflate a denominator.
        let key = (
            pair.repo.clone(),
            pair.source.clone(),
            pair.consumer_task.clone(),
            pair.consumer_generation.clone(),
        );
        if !seen_keys.insert(key) {
            excluded.push(Excluded {
                pair: pair.id.clone(),
                reason: "duplicate".into(),
                detail: "identical repo/source/consumer_task/consumer_generation already counted"
                    .into(),
            });
            continue;
        }
        if let Some(result) = evaluate_pair(
            pair,
            review_by_pair.get(pair.id.as_str()).copied(),
            &idx,
            capture.order,
            &mut excluded,
            &mut rejected_claims,
            &mut unresolved,
            &mut ambiguous_assessments,
            &mut author_exit_unsupported,
        ) {
            pairs_out.push(result);
        }
    }

    // --- discovery denominator ---------------------------------------
    let eligible = pairs_out.len();
    let prepared_native = pairs_out
        .iter()
        .filter(|p| p.coverage_status == "prepared" && p.coverage_provenance == "native")
        .count();
    let prepared_reviewed = pairs_out
        .iter()
        .filter(|p| p.coverage_status == "prepared" && p.coverage_provenance == "reviewed")
        .count();
    let not_prepared_pairs = pairs_out
        .iter()
        .filter(|p| p.coverage_status == "not_prepared")
        .count();
    let mut prepared_not_launched: Vec<String> = pairs_out
        .iter()
        .filter(|p| p.coverage_status == "prepared_not_launched")
        .map(|p| p.pair.clone())
        .collect();
    prepared_not_launched.sort();
    let mut unknown_coverage: Vec<String> = pairs_out
        .iter()
        .filter(|p| p.coverage_status == "unknown")
        .map(|p| p.pair.clone())
        .collect();
    unknown_coverage.sort();
    let prepared_pairs = prepared_native + prepared_reviewed;
    // A pair whose consumer generation never launched is neither a discovery
    // success nor a discovery failure: there was no decision to inform. It is
    // reported on its own rather than distorting either side of the rate.
    let known_coverage_pairs = prepared_pairs + not_prepared_pairs;
    let discovery = Discovery {
        eligible_pairs: eligible,
        known_coverage_pairs,
        prepared_pairs,
        prepared_native,
        prepared_reviewed,
        not_prepared_pairs,
        unknown_coverage_pairs: unknown_coverage.len(),
        prepared_not_launched: prepared_not_launched.clone(),
        rate: (known_coverage_pairs > 0)
            .then(|| prepared_pairs as f64 / known_coverage_pairs as f64),
    };

    let opened = pairs_out.iter().filter(|p| p.opened).count();
    let claimed = pairs_out
        .iter()
        .filter(|p| p.claimed_outcome.is_some())
        .count();
    let assessed = pairs_out
        .iter()
        .filter(|p| p.assessed_verdict.is_some())
        .count();

    let mut outcome_classes = OutcomeClasses::default();
    for p in &pairs_out {
        match p.claimed_outcome.as_deref() {
            Some("used") => outcome_classes.used += 1,
            Some("adapted") => outcome_classes.adapted += 1,
            Some("confirmed") => outcome_classes.confirmed += 1,
            Some("rejected") => outcome_classes.rejected += 1,
            _ => {}
        }
        match p.assessed_verdict.as_deref() {
            Some("verified") => outcome_classes.verified += 1,
            Some("unsupported") => outcome_classes.unsupported += 1,
            Some("incorrect") => outcome_classes.incorrect += 1,
            _ => {}
        }
    }

    // --- verified reuse, per distinct eligible consumer task ----------
    // Uses `verified_effect` — the SAME gate the mechanism goal counts — so
    // the two can never disagree about what "verified" means. Confirmations
    // and rejections are independent counts, reported alongside rather than
    // instead of a verified effect.
    let mut tasks: BTreeMap<(&str, &str), Vec<&PairResult>> = BTreeMap::new();
    for p in &pairs_out {
        tasks
            .entry((p.repo.as_str(), p.consumer_task.as_str()))
            .or_default()
            .push(p);
    }
    let eligible_consumer_tasks = tasks.len();
    let mut verified_used_or_adapted = 0usize;
    let mut confirmed_tasks = 0usize;
    let mut rejected_tasks = 0usize;
    for ps in tasks.values() {
        if ps.iter().any(|p| p.verified_effect) {
            verified_used_or_adapted += 1;
        }
        if ps
            .iter()
            .any(|p| p.claimed_outcome.as_deref() == Some("confirmed"))
        {
            confirmed_tasks += 1;
        }
        if ps
            .iter()
            .any(|p| p.claimed_outcome.as_deref() == Some("rejected"))
        {
            rejected_tasks += 1;
        }
    }
    let verified_reuse = VerifiedReuse {
        eligible_consumer_tasks,
        verified_used_or_adapted_tasks: verified_used_or_adapted,
        rate: (eligible_consumer_tasks > 0)
            .then(|| verified_used_or_adapted as f64 / eligible_consumer_tasks as f64),
        confirmed_tasks,
        rejected_tasks,
    };

    let mut author_exit_reuse: Vec<String> = pairs_out
        .iter()
        .filter(|p| p.counts_as_effect && p.author_terminal)
        .map(|p| p.pair.clone())
        .collect();
    author_exit_reuse.sort();

    let mut effect_pairs: Vec<String> = pairs_out
        .iter()
        .filter(|p| p.counts_as_effect)
        .map(|p| p.pair.clone())
        .collect();
    effect_pairs.sort();
    let effect_batches: BTreeSet<&str> = pairs_out
        .iter()
        .filter(|p| p.counts_as_effect)
        .map(|p| p.batch.as_str())
        .collect();
    let author_exit_effects = author_exit_reuse.len();
    let mechanism = MechanismResult {
        effects: effect_pairs.len(),
        batches: effect_batches.len(),
        author_exit_effects,
        effects_required: MECHANISM_EFFECTS_REQUIRED,
        batches_required: MECHANISM_BATCHES_REQUIRED,
        author_exit_required: MECHANISM_AUTHOR_EXIT_REQUIRED,
        // A truncated capture cannot certify the goal: an assessment or reuse
        // tuple outside the truncation window could exist and change any of
        // these counts.
        goal_met: !capture.truncated
            && effect_pairs.len() >= MECHANISM_EFFECTS_REQUIRED
            && effect_batches.len() >= MECHANISM_BATCHES_REQUIRED
            && author_exit_effects >= MECHANISM_AUTHOR_EXIT_REQUIRED,
        effect_pairs,
    };

    // --- deliveries, from frozen scope -------------------------------
    let mut deliveries = Vec::new();
    let mut tasks_without_native_records = Vec::new();
    let mut attention_hold_spans = 0usize;
    let mut rework_spans = 0usize;
    for scope in frozen_task_scope(manifest) {
        // Spans are pre-filtered by repo and window here because
        // `build_critical_path` dedups on `(phase, attempt)` across whatever
        // it is handed and does not filter by scope: two repos' same-named
        // task ids would silently merge.
        let task_spans: Vec<Value> = idx
            .spans
            .iter()
            .filter(|t| {
                str_field(t, &["scope"]) == scope.repo
                    && t["payload"]["task"] == scope.task.as_str()
            })
            .map(|t| (*t).clone())
            .collect();
        let cp = crate::critical_path::build_critical_path(&scope.task, &task_spans);
        let mut phase_ms = PhaseDurations::default();
        if let Some(phases) = cp["phases"].as_array() {
            for phase in phases {
                let dur = phase["duration_ms"].as_i64();
                let wait = phase["queue_wait_ms"].as_i64();
                match phase["phase"].as_str().unwrap_or("") {
                    "verification" => {
                        // Admission/check queue AND its own run time both count
                        // as verification time, never as work.
                        opt_sum(&mut phase_ms.verification_ms, dur);
                        opt_sum(&mut phase_ms.verification_ms, wait);
                    }
                    "attention_hold" => {
                        attention_hold_spans += 1;
                        opt_sum(&mut phase_ms.attention_hold_ms, dur);
                        opt_sum(&mut phase_ms.attention_hold_ms, wait);
                    }
                    other => {
                        if other == "rework" {
                            rework_spans += 1;
                        }
                        opt_sum(&mut phase_ms.work_phases_ms, dur);
                        // Generic pre-phase queueing is a wait, not work, even
                        // for an otherwise-active phase.
                        opt_sum(&mut phase_ms.queue_wait_ms, wait);
                    }
                }
            }
        }
        let acceptance_evidence = idx
            .spans
            .iter()
            .filter(|t| {
                str_field(t, &["scope"]) == scope.repo
                    && t["payload"]["task"] == scope.task.as_str()
                    && t["payload"]["phase"] == "delivery_closure"
            })
            .map(|t| record_id(t))
            .min();
        let accepted = acceptance_evidence.as_ref().map(|_| true);

        // Every native generation observed for this task, whether or not it
        // ever produced an eligible pair or a receipt.
        let mut spawns: BTreeSet<&str> = BTreeSet::new();
        for c in &idx.completions {
            if c.repo == scope.repo && c.task == scope.task {
                spawns.insert(c.spawn.as_str());
            }
        }
        for e in &idx.exits {
            if e.repo == scope.repo && e.task == scope.task {
                spawns.insert(e.spawn.as_str());
            }
        }
        for u in &idx.usage {
            if u.repo == scope.repo && u.task == scope.task {
                spawns.insert(u.spawn.as_str());
            }
        }

        let mut generations = Vec::new();
        let mut completions_total = 0usize;
        let mut failed_completions = 0usize;
        let mut launches_total = 0usize;
        let mut exits_total = 0usize;
        let mut provisional: Option<f64> = None;
        let mut provisional_known = true;
        let mut provider_total: Option<f64> = None;
        let mut daemon_total: Option<f64> = None;
        let mut partial_total: Option<f64> = None;
        let mut all_final = true;
        let mut any_segment = false;
        let mut non_provider_basis = false;
        let mut unknown_cost: Vec<String> = Vec::new();
        let mut lifetime: Option<i64> = None;

        for spawn in spawns {
            let mut completions = Vec::new();
            for c in idx
                .completions
                .iter()
                .filter(|c| c.repo == scope.repo && c.task == scope.task && c.spawn == spawn)
            {
                completions_total += 1;
                // TKT-175: an undeclared generation is also an error, but the
                // two are recorded separately so "the rat said it was done"
                // stays distinguishable from "nothing went wrong".
                let failed = c.is_error || !c.declared_done;
                if failed {
                    failed_completions += 1;
                }
                match c.cost_usd {
                    Some(v) => *provisional.get_or_insert(0.0) += v,
                    None => provisional_known = false,
                }
                completions.push(CompletionObservation {
                    record: c.record.clone(),
                    declared_done: c.declared_done,
                    is_error: c.is_error,
                    provisional_cost_usd: c.cost_usd,
                    failed,
                });
            }
            completions.sort_by(|a, b| a.record.cmp(&b.record));

            let mut agent = idx
                .completions
                .iter()
                .find(|c| c.repo == scope.repo && c.spawn == spawn && !c.agent.is_empty())
                .map(|c| c.agent.clone());
            if agent.is_none() {
                agent = idx
                    .exits
                    .iter()
                    .find(|e| e.repo == scope.repo && e.spawn == spawn && !e.agent.is_empty())
                    .map(|e| e.agent.clone());
            }

            let mut launches = Vec::new();
            for e in idx
                .exits
                .iter()
                .filter(|e| e.repo == scope.repo && e.task == scope.task && e.spawn == spawn)
            {
                exits_total += 1;
                launches_total += 1;
                let ms = match (e.launched_at, e.exited_at) {
                    (Some(l), Some(x)) => Some((x - l).num_milliseconds()),
                    _ => None,
                };
                opt_sum(&mut lifetime, ms);
                launches.push(LaunchObservation {
                    session: e.session.clone(),
                    launched_at: e.launched_at.map(|t| t.to_rfc3339()),
                    exited_at: e.exited_at.map(|t| t.to_rfc3339()),
                    exit_record: Some(e.record.clone()),
                    exit_code: e.exit_code,
                    crashed: e.crashed,
                    prior_state: e.prior_state.clone(),
                    process_lifetime_ms: ms,
                });
            }
            launches.sort_by(|a, b| a.session.cmp(&b.session));

            // Group usage by (session, provider_session): the provider reports
            // a CUMULATIVE total within one segment, so only the last result
            // per segment is that segment's amount.
            let mut by_segment: BTreeMap<(String, Option<String>), Vec<&UsageRow>> =
                BTreeMap::new();
            for u in idx
                .usage
                .iter()
                .filter(|u| u.repo == scope.repo && u.task == scope.task && u.spawn == spawn)
            {
                by_segment
                    .entry((u.session.clone(), u.provider_session.clone()))
                    .or_default()
                    .push(u);
            }
            let mut cost_segments = Vec::new();
            for ((session, provider_session), mut rows) in by_segment {
                any_segment = true;
                rows.sort_by(|a, b| (a.observed_at, &a.record).cmp(&(b.observed_at, &b.record)));
                let last = rows.last().copied().expect("segment has at least one row");
                let exit = idx
                    .exits
                    .iter()
                    .find(|e| e.repo == scope.repo && e.spawn == spawn && e.session == session);
                let (final_cost, finality_reason) = segment_finality(last, exit);
                if !final_cost {
                    all_final = false;
                    unknown_cost.push(format!(
                        "{spawn}/{session}/{}: {finality_reason}",
                        provider_session.as_deref().unwrap_or("no-provider-session")
                    ));
                }
                if last.cost_basis != PROVIDER_COST_BASIS {
                    non_provider_basis = true;
                }
                match (final_cost, last.cost_usd, last.cost_basis.as_str()) {
                    (true, Some(v), PROVIDER_COST_BASIS) => {
                        *provider_total.get_or_insert(0.0) += v;
                    }
                    (true, Some(v), DAEMON_COST_BASIS) => {
                        *daemon_total.get_or_insert(0.0) += v;
                    }
                    (false, Some(v), _) => {
                        *partial_total.get_or_insert(0.0) += v;
                    }
                    (_, None, _) => {
                        all_final = false;
                        unknown_cost.push(format!(
                            "{spawn}/{session}/{}: reported cost is null (cost_basis={})",
                            provider_session.as_deref().unwrap_or("no-provider-session"),
                            last.cost_basis
                        ));
                    }
                    (true, Some(_), _) => {}
                }
                cost_segments.push(CostSegment {
                    spawn: spawn.to_string(),
                    session,
                    provider_session,
                    record: last.record.clone(),
                    reported_usd: last.cost_usd,
                    cost_basis: last.cost_basis.clone(),
                    state: last.state.clone(),
                    final_cost,
                    finality_reason,
                });
            }
            cost_segments.sort_by(|a, b| {
                (&a.spawn, &a.session, &a.provider_session).cmp(&(
                    &b.spawn,
                    &b.session,
                    &b.provider_session,
                ))
            });
            if cost_segments.is_empty() {
                unknown_cost.push(format!(
                    "{spawn}: no agent_final_usage observation, so this generation's provider cost \
                     is unknown"
                ));
            }
            generations.push(GenerationObservation {
                spawn: spawn.to_string(),
                agent,
                completions,
                launches,
                cost_segments,
            });
        }
        generations.sort_by(|a, b| a.spawn.cmp(&b.spawn));

        if generations.is_empty() && task_spans.is_empty() {
            tasks_without_native_records.push(format!("{}/{}", scope.repo, scope.task));
        }

        let cost_coverage = if generations.is_empty() {
            "none_observed"
        } else if !any_segment {
            "missing"
        } else if all_final && !non_provider_basis {
            "complete"
        } else {
            "partial"
        };
        // A provider total is only reported when EVERY observed segment is a
        // final, provider-reported total. A partial segment, a null cost, or a
        // daemon-priced segment all leave the task total unknown rather than
        // pooling bases or presenting a partial sum as a total.
        let reported_cost_estimate_usd = (cost_coverage == "complete")
            .then_some(provider_total)
            .flatten();
        unknown_cost.sort();

        deliveries.push(DeliveryCost {
            task: scope.task.clone(),
            repo: scope.repo.clone(),
            batches: scope.batches.iter().cloned().collect(),
            enrollment: scope.enrollment.to_string(),
            generations,
            completions: completions_total,
            failed_completions,
            launches: launches_total,
            exits: exits_total,
            reported_cost_estimate_usd,
            reported_cost_basis: reported_cost_estimate_usd
                .map(|_| PROVIDER_COST_BASIS.to_string()),
            cost_coverage: cost_coverage.to_string(),
            partial_reported_usd: partial_total,
            daemon_priced_estimate_usd: daemon_total,
            provisional_completion_cost_usd: provisional_known.then_some(provisional).flatten(),
            unknown_cost,
            process_lifetime_ms: lifetime,
            active_work_ms: None,
            active_work_coverage:
                "unknown: launch-to-exit is process lifetime, and no native observation \
                 distinguishes model-active time from a process paused awaiting verification or \
                 the operator"
                    .into(),
            phase_ms,
            accepted,
            acceptance_evidence,
        });
    }
    deliveries.sort_by(|a, b| (&a.repo, &a.task).cmp(&(&b.repo, &b.task)));
    tasks_without_native_records.sort();

    // --- quality ------------------------------------------------------
    let annotate = |items: &[ReviewedAnnotation], t: &TaskAnnotation| -> Vec<ReviewedRef> {
        items
            .iter()
            .map(|a| ReviewedRef {
                task: t.task.clone(),
                repo: t.repo.clone(),
                kind: a.kind.clone(),
                reference: a.reference.clone(),
                reference_resolved: idx.by_id.contains_key(a.reference.as_str()),
                note: a.note.clone(),
            })
            .collect()
    };
    let mut reviewed_interventions = Vec::new();
    let mut repeated_investigations = Vec::new();
    let mut reviewed_rework = Vec::new();
    for t in task_annotations {
        reviewed_interventions.extend(annotate(&t.interventions, t));
        repeated_investigations.extend(annotate(&t.repeated_investigations, t));
        reviewed_rework.extend(annotate(&t.rework, t));
    }
    let sort_refs = |v: &mut Vec<ReviewedRef>| {
        v.sort_by(|a, b| {
            (&a.repo, &a.task, &a.kind, &a.reference).cmp(&(
                &b.repo,
                &b.task,
                &b.kind,
                &b.reference,
            ))
        });
    };
    sort_refs(&mut reviewed_interventions);
    sort_refs(&mut repeated_investigations);
    sort_refs(&mut reviewed_rework);

    let incorrect_reuse = pairs_out
        .iter()
        .filter(|p| p.assessed_verdict.as_deref() == Some("incorrect"))
        .count();
    let mut regressions: Vec<String> = pairs_out
        .iter()
        .filter(|p| p.regression)
        .map(|p| p.pair.clone())
        .collect();
    regressions.sort();
    let quality = QualitySummary {
        attention_hold_spans,
        interventions_known: attention_hold_spans + reviewed_interventions.len(),
        interventions_coverage:
            "lower bound: attention_hold spans only cover waits the daemon recorded; an operator \
             intervention that left no span is present only as a reviewed annotation"
                .into(),
        reviewed_interventions,
        repeated_investigations,
        rework_spans,
        reviewed_rework,
        incorrect_reuse,
        regressions,
    };

    excluded.sort_by(|a, b| (&a.pair, &a.reason).cmp(&(&b.pair, &b.reason)));
    rejected_claims.sort_by(|a, b| (&a.pair, &a.record).cmp(&(&b.pair, &b.record)));
    ambiguous_assessments.sort_by(|a, b| a.pair.cmp(&b.pair));
    author_exit_unsupported.sort_by(|a, b| (&a.pair, &a.evidence).cmp(&(&b.pair, &b.evidence)));
    unresolved
        .sort_by(|a, b| (&a.record, &a.kind, &a.reason).cmp(&(&b.record, &b.kind, &b.reason)));
    unresolved.dedup_by(|a, b| (&a.record, &a.kind, &a.reason) == (&b.record, &b.kind, &b.reason));

    Ok(Report {
        schema_version: SCHEMA_VERSION,
        evaluator_version: EVALUATOR_VERSION,
        experiment_id: manifest.experiment_id.clone(),
        build: manifest.build.clone(),
        repos: manifest.repos.clone(),
        window: manifest.window.clone(),
        tuples_order: match capture.order {
            Order::PersistenceSequence => "persistence_sequence".into(),
            Order::Unknown => "unknown".into(),
        },
        tuples_truncated: capture.truncated,
        tuples_source: capture.source.clone(),
        capture: idx.capture.clone(),
        invalid_records: idx.invalid.clone(),
        unresolved_records: unresolved,
        eligible,
        excluded,
        rejected_claims,
        discovery,
        presented_native: prepared_native,
        presented_reviewed: prepared_reviewed,
        opened,
        claimed,
        assessed,
        outcome_classes,
        unknown_coverage,
        ambiguous_assessments,
        author_exit_unsupported,
        verified_reuse,
        author_exit_reuse,
        mechanism,
        deliveries,
        tasks_without_native_records,
        quality,
        pairs: pairs_out,
    })
}

fn opt_rate(rate: Option<f64>) -> String {
    match rate {
        Some(r) => format!("{:.1}%", r * 100.0),
        None => "unknown (no denominator)".into(),
    }
}

fn opt_usd(v: Option<f64>) -> String {
    match v {
        Some(v) => format!("${v:.4}"),
        None => "unknown".into(),
    }
}

/// Renders a `Report` as human-readable text. JSON output should use
/// `serde_json::to_string_pretty` directly on the `Report` for a byte-stable
/// machine shape.
pub fn render(report: &Report) -> String {
    let mut out = format!("## Stigmergy evidence report: {}\n\n", report.experiment_id);
    let _ = writeln!(
        out,
        "evaluator_version={} schema_version={}",
        report.evaluator_version, report.schema_version
    );
    let _ = writeln!(
        out,
        "eligible={} excluded={} rejected_claims={} opened={} claimed={} assessed={}",
        report.eligible,
        report.excluded.len(),
        report.rejected_claims.len(),
        report.opened,
        report.claimed,
        report.assessed
    );
    let _ = writeln!(
        out,
        "tuples: order={} truncated={} captured={} in_window={} out_of_window={} undated={} \
         out_of_scope_repo={}",
        report.tuples_order,
        report.tuples_truncated,
        report.capture.tuples,
        report.capture.observations_in_window,
        report.capture.observations_out_of_window,
        report.capture.observations_undated,
        report.capture.observations_out_of_scope_repo
    );
    let _ = writeln!(
        out,
        "discovery: prepared {}/{} known-coverage pairs ({}); native={} reviewed={} \
         not_prepared={} unknown={} prepared_not_launched={}",
        report.discovery.prepared_pairs,
        report.discovery.known_coverage_pairs,
        opt_rate(report.discovery.rate),
        report.discovery.prepared_native,
        report.discovery.prepared_reviewed,
        report.discovery.not_prepared_pairs,
        report.discovery.unknown_coverage_pairs,
        report.discovery.prepared_not_launched.len()
    );
    let _ = writeln!(
        out,
        "outcomes: used={} adapted={} confirmed={} rejected={} | verified={} unsupported={} incorrect={}",
        report.outcome_classes.used,
        report.outcome_classes.adapted,
        report.outcome_classes.confirmed,
        report.outcome_classes.rejected,
        report.outcome_classes.verified,
        report.outcome_classes.unsupported,
        report.outcome_classes.incorrect
    );
    let _ = writeln!(
        out,
        "verified reuse: {}/{} tasks ({}); confirmed={} rejected={}",
        report.verified_reuse.verified_used_or_adapted_tasks,
        report.verified_reuse.eligible_consumer_tasks,
        opt_rate(report.verified_reuse.rate),
        report.verified_reuse.confirmed_tasks,
        report.verified_reuse.rejected_tasks
    );
    let _ = writeln!(
        out,
        "mechanism goal: effects={}/{} batches={}/{} author_exit={}/{} met={}",
        report.mechanism.effects,
        report.mechanism.effects_required,
        report.mechanism.batches,
        report.mechanism.batches_required,
        report.mechanism.author_exit_effects,
        report.mechanism.author_exit_required,
        report.mechanism.goal_met
    );
    if !report.unknown_coverage.is_empty() {
        let _ = writeln!(
            out,
            "unknown coverage ({}): {}",
            report.unknown_coverage.len(),
            report.unknown_coverage.join(", ")
        );
    }
    if !report.discovery.prepared_not_launched.is_empty() {
        let _ = writeln!(
            out,
            "prepared but consumer never launched ({}): {}",
            report.discovery.prepared_not_launched.len(),
            report.discovery.prepared_not_launched.join(", ")
        );
    }
    if !report.excluded.is_empty() {
        out.push_str("excluded pairs (not a distinct opportunity):\n");
        for e in &report.excluded {
            let _ = writeln!(out, "  {} [{}] {}", e.pair, e.reason, e.detail);
        }
    }
    if !report.rejected_claims.is_empty() {
        out.push_str("rejected claims (the opportunity stands, the receipt does not):\n");
        for r in &report.rejected_claims {
            let _ = writeln!(
                out,
                "  {} record={} [{}] {}",
                r.pair, r.record, r.reason, r.detail
            );
        }
    }
    if !report.invalid_records.is_empty() {
        out.push_str("invalid native records:\n");
        for r in &report.invalid_records {
            let _ = writeln!(out, "  {} [{}] {}", r.record, r.kind, r.reason);
        }
    }
    if !report.unresolved_records.is_empty() {
        out.push_str("unresolved/unknown records (never read as a negative):\n");
        for r in &report.unresolved_records {
            let _ = writeln!(out, "  {} [{}] {}", r.record, r.kind, r.reason);
        }
    }
    if !report.ambiguous_assessments.is_empty() {
        out.push_str("ambiguous assessments:\n");
        for a in &report.ambiguous_assessments {
            let _ = writeln!(out, "  {} receipt={} {}", a.pair, a.receipt, a.reason);
        }
    }
    if !report.author_exit_unsupported.is_empty() {
        out.push_str("author-exit claims not established:\n");
        for a in &report.author_exit_unsupported {
            let _ = writeln!(out, "  {} evidence={} {}", a.pair, a.evidence, a.reason);
        }
    }
    if !report.deliveries.is_empty() {
        out.push_str("deliveries (reported cost ESTIMATES, not billed charges):\n");
        for d in &report.deliveries {
            let _ = writeln!(
                out,
                "  {}/{} [{}] completions={} failed={} launches={} exits={}",
                d.repo,
                d.task,
                d.enrollment,
                d.completions,
                d.failed_completions,
                d.launches,
                d.exits
            );
            let _ = writeln!(
                out,
                "    cost: reported={} coverage={} partial={} daemon_priced={} provisional_completion={}",
                opt_usd(d.reported_cost_estimate_usd),
                d.cost_coverage,
                opt_usd(d.partial_reported_usd),
                opt_usd(d.daemon_priced_estimate_usd),
                opt_usd(d.provisional_completion_cost_usd)
            );
            let _ = writeln!(
                out,
                "    time: process_lifetime_ms={:?} active_work_ms=unknown work_phases_ms={:?} \
                 verification_ms={:?} attention_hold_ms={:?} queue_wait_ms={:?}",
                d.process_lifetime_ms,
                d.phase_ms.work_phases_ms,
                d.phase_ms.verification_ms,
                d.phase_ms.attention_hold_ms,
                d.phase_ms.queue_wait_ms
            );
            let _ = writeln!(out, "    accepted={:?}", d.accepted);
            for u in &d.unknown_cost {
                let _ = writeln!(out, "    unknown cost: {u}");
            }
        }
    }
    if !report.tasks_without_native_records.is_empty() {
        let _ = writeln!(
            out,
            "frozen tasks with no native record at all: {}",
            report.tasks_without_native_records.join(", ")
        );
    }
    let _ = writeln!(
        out,
        "quality: attention_hold_spans={} reviewed_interventions={} interventions_known={} \
         rework_spans={} reviewed_rework={} repeated_investigations={} incorrect_reuse={} \
         regressions={}",
        report.quality.attention_hold_spans,
        report.quality.reviewed_interventions.len(),
        report.quality.interventions_known,
        report.quality.rework_spans,
        report.quality.reviewed_rework.len(),
        report.quality.repeated_investigations.len(),
        report.quality.incorrect_reuse,
        report.quality.regressions.join(", ")
    );
    let _ = writeln!(
        out,
        "  interventions: {}",
        report.quality.interventions_coverage
    );
    out
}

pub fn to_json(report: &Report) -> Value {
    serde_json::to_value(report).unwrap_or(json!({}))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------------------
    // Fixtures. Every one is shaped like the COMMITTED producer writes it:
    // the daemon-minted `bbs-<kind>-<digest>` identity, `Furniture`
    // lifecycle, `schema_version: 1`, and `instance == payload.agent` for the
    // S1 artifact kinds (`Tuple::new(.., caller, ..)` makes that
    // unconditional). Telemetry fixtures instead carry the castle as
    // `instance` and the consumer only in the payload, exactly as
    // `write_telemetry` does.
    // ------------------------------------------------------------------

    const CASTLE: &str = "castle-1";

    fn manifest_with(
        pairs: Vec<EligiblePair>,
        consumer_tasks: Vec<ConsumerTaskScope>,
        window: Window,
    ) -> Manifest {
        Manifest {
            schema_version: SCHEMA_VERSION,
            experiment_id: "exp-1".into(),
            repos: vec!["repo".into()],
            window,
            build: BuildIdentity::default(),
            quality_criteria: vec![],
            batches: vec![Batch {
                id: "batch-1".into(),
                arm: "real".into(),
                repo: "repo".into(),
            }],
            consumer_tasks,
            eligible_pairs: pairs,
        }
    }

    fn manifest(pairs: Vec<EligiblePair>) -> Manifest {
        manifest_with(pairs, vec![], Window::default())
    }

    fn pair(id: &str, source: &str, task: &str, gen: &str) -> EligiblePair {
        EligiblePair {
            id: id.into(),
            source: source.into(),
            consumer_task: task.into(),
            consumer_generation: gen.into(),
            repo: "repo".into(),
            batch: "batch-1".into(),
        }
    }

    fn finding(id: &str, spawn: &str, created: &str) -> Value {
        finding_in(id, "author", spawn, created, "repo")
    }

    fn finding_in(id: &str, agent: &str, spawn: &str, created: &str, scope: &str) -> Value {
        json!({
            "id": id,
            "category": "artifact",
            "scope": scope,
            "identity": format!("bbs-finding-{id}"),
            "instance": agent,
            "lifecycle": "furniture",
            "created_at": created,
            "payload": {
                "schema_version": 1, "bbs_kind": FINDING, "agent": agent, "spawn": spawn,
                "task": "TKT-source", "text": "a reusable interface constraint",
                "areas": ["src/x.rs"], "revision": "abc123", "evidence": ["ev-1"],
                "limitations": "none noted"
            }
        })
    }

    fn reuse(
        id: &str,
        source: &str,
        task: &str,
        spawn: &str,
        outcome: &str,
        created: &str,
    ) -> Value {
        json!({
            "id": id,
            "category": "artifact",
            "scope": "repo",
            "identity": format!("bbs-reuse-{id}"),
            "instance": "consumer",
            "lifecycle": "furniture",
            "created_at": created,
            "payload": {
                "schema_version": 1, "bbs_kind": REUSE, "agent": "consumer", "spawn": spawn,
                "task": task, "source": source, "outcome": outcome,
                "text": "used the constraint directly", "evidence": ["ev-2"]
            }
        })
    }

    fn assessment(id: &str, receipt: &str, verdict: &str, created: &str) -> Value {
        json!({
            "id": id,
            "category": "artifact",
            "scope": "repo",
            "identity": format!("bbs-assessment-{id}"),
            "instance": "operator",
            "lifecycle": "furniture",
            "created_at": created,
            "payload": {
                "schema_version": 1, "bbs_kind": ASSESSMENT, "agent": "operator", "spawn": null,
                "task": "TKT-source", "receipt": receipt, "verdict": verdict,
                "reason": "matches delivered work", "evidence": ["ev-3"]
            }
        })
    }

    /// `record_exposure`'s exact payload: castle-authored, consumer only in
    /// the payload, `entries[].source` naming the selected tuple.
    fn exposure(id: &str, source: &str, spawn: &str, created: &str) -> Value {
        exposure_full(id, source, spawn, created, "repo", "agent")
    }

    fn exposure_full(
        id: &str,
        source: &str,
        spawn: &str,
        created: &str,
        scope: &str,
        bound: &str,
    ) -> Value {
        json!({
            "id": id,
            "category": "event",
            "scope": scope,
            "identity": "bbs-exposure-spawn",
            "instance": CASTLE,
            "lifecycle": "furniture",
            "created_at": created,
            "payload": {
                "schema_version": 1, "bbs_kind": EXPOSURE, "surface": "spawn", "repo": scope,
                "task": "TKT-1", "agent": "consumer", "spawn": spawn, "bound": bound,
                "entries": [{"source": source, "reason": "task or dependency",
                             "kind": FINDING, "category": "artifact"}],
                "prepared": 1, "omitted": 0, "cursor": 10, "since": null,
                "semantics": "prepared"
            }
        })
    }

    fn open_record(id: &str, source: &str, spawn: &str, created: &str) -> Value {
        json!({
            "id": id,
            "category": "event",
            "scope": "repo",
            "identity": "bbs-open",
            "instance": CASTLE,
            "lifecycle": "furniture",
            "created_at": created,
            "payload": {
                "schema_version": 1, "bbs_kind": OPEN, "source": source, "source_kind": FINDING,
                "repo": "repo", "agent": "consumer", "spawn": spawn, "task": "TKT-1",
                "bound": "agent", "dedup_key": format!("{source}:{spawn}"),
                "semantics": "requested"
            }
        })
    }

    /// `harness_result`: task-completion evidence, emitted at `rk done`.
    fn harness_result(id: &str, agent: &str, spawn: &str, task: &str, created: &str) -> Value {
        json!({
            "id": id, "category": "event", "scope": "repo", "identity": "harness_result",
            "instance": spawn, "created_at": created,
            "payload": {
                "agent": agent, "spawn": spawn, "role": "rat", "task": task,
                "is_error": false, "declared_done": true, "cost_usd": 0.5, "tokens": 1000,
                "result": "done"
            }
        })
    }

    /// S2's `agent_exit`: the only physical-exit observation.
    #[allow(clippy::too_many_arguments)]
    fn agent_exit(
        id: &str,
        agent: &str,
        spawn: &str,
        session: &str,
        task: &str,
        launched_at: &str,
        exited_at: &str,
        prior_state: Option<&str>,
    ) -> Value {
        json!({
            "id": id, "category": "event", "scope": "repo", "identity": "bbs-agent-exit",
            "instance": CASTLE, "lifecycle": "furniture", "created_at": exited_at,
            "payload": {
                "schema_version": 1, "bbs_kind": AGENT_EXIT, "repo": "repo", "task": task,
                "agent": agent, "spawn": spawn, "session": session, "provider_session": "ps-1",
                "exited_at": exited_at, "exit_code": 0, "crashed": false,
                "prior_state": prior_state, "launched_at": launched_at
            }
        })
    }

    /// S2's `agent_final_usage`: one provider result for one segment.
    #[allow(clippy::too_many_arguments)]
    fn final_usage(
        id: &str,
        spawn: &str,
        session: &str,
        provider_session: &str,
        task: &str,
        state: &str,
        cost: Option<f64>,
        basis: &str,
        observed_at: &str,
    ) -> Value {
        json!({
            "id": id, "category": "event", "scope": "repo",
            "identity": "bbs-agent-final-usage", "instance": CASTLE, "lifecycle": "furniture",
            "created_at": observed_at,
            "payload": {
                "schema_version": 1, "bbs_kind": AGENT_FINAL_USAGE, "repo": "repo", "task": task,
                "agent": "author", "spawn": spawn, "session": session,
                "provider_session": provider_session, "observed_at": observed_at,
                "state": state, "declared_done": true, "cost_usd": cost, "cost_basis": basis,
                "cost_provenance": "HarnessEvent::Completed.total_cost_usd", "usage": null
            }
        })
    }

    fn span(id: &str, task: &str, phase: &str, dur: i64, wait: Option<i64>) -> Value {
        json!({
            "id": id, "category": "event", "scope": "repo", "identity": "task_span",
            "instance": CASTLE, "lifecycle": "furniture",
            "created_at": "2026-01-02T00:00:00Z",
            "payload": {
                "task": task, "phase": phase, "attempt": 1, "repo": "repo",
                "duration_ms": dur, "queue_wait_ms": wait
            }
        })
    }

    /// Wraps test tuples into a capture, auto-injecting the generic evidence
    /// artifacts the fixtures above reference by default.
    fn capture(mut tuples: Vec<Value>, order: Order) -> TupleCapture {
        for id in ["ev-1", "ev-2", "ev-3"] {
            if !tuples.iter().any(|t| t["id"] == id) {
                tuples.push(json!({
                    "id": id, "category": "artifact", "scope": "repo", "identity": id,
                    "instance": "x", "created_at": "2026-01-01T00:00:00Z", "payload": {}
                }));
            }
        }
        TupleCapture {
            order,
            tuples,
            truncated: false,
            source: None,
        }
    }

    fn reviews(rs: Vec<Review>) -> ReviewsFile {
        ReviewsFile::Pairs(rs)
    }

    fn review(pair_id: &str) -> Review {
        Review {
            pair: pair_id.into(),
            declares: None,
            coverage: unknown_coverage(),
            author_terminal_evidence: None,
            relayed_by_operator: false,
            regression: false,
            notes: None,
        }
    }

    fn review_reviewed_prepared(pair_id: &str, reference: &str) -> Review {
        Review {
            coverage: Coverage::Prepared {
                evidence: ReviewedEvidence::Reference(reference.into()),
            },
            ..review(pair_id)
        }
    }

    fn review_with_exit(pair_id: &str, exit_evidence: &str) -> Review {
        Review {
            author_terminal_evidence: Some(exit_evidence.into()),
            ..review(pair_id)
        }
    }

    /// The happy path: finding, receipt, verdict, native exposure, and a
    /// completion proving the consumer generation actually launched.
    fn verified_tuples() -> Vec<Value> {
        vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            reuse(
                "r1",
                "src-1",
                "TKT-1",
                "gen-1",
                "used",
                "2026-01-02T00:00:00Z",
            ),
            assessment("a1", "r1", "verified", "2026-01-03T00:00:00Z"),
            exposure("x1", "src-1", "gen-1", "2026-01-01T12:00:00Z"),
            harness_result("h1", "consumer", "gen-1", "TKT-1", "2026-01-02T01:00:00Z"),
        ]
    }

    fn only(report: &Report) -> &PairResult {
        assert_eq!(report.pairs.len(), 1, "expected exactly one surviving pair");
        &report.pairs[0]
    }

    fn exclusion_reasons(report: &Report) -> Vec<&str> {
        report.excluded.iter().map(|e| e.reason.as_str()).collect()
    }

    // ------------------------------------------------------------------
    // Baseline / determinism.
    // ------------------------------------------------------------------

    #[test]
    fn verified_used_effect_counts_and_is_deterministic() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let c = capture(verified_tuples(), Order::Unknown);
        let r = reviews(vec![review("p1")]);
        let first = compute(&m, &c, &r).unwrap();
        let second = compute(&m, &c, &r).unwrap();
        assert_eq!(to_json(&first), to_json(&second), "must be deterministic");
        assert_eq!(first.eligible, 1);
        assert_eq!(first.presented_native, 1, "native exposure, not reviewed");
        assert_eq!(first.presented_reviewed, 0);
        assert_eq!(first.discovery.rate, Some(1.0));
        assert_eq!(first.outcome_classes.used, 1);
        assert_eq!(first.outcome_classes.verified, 1);
        assert_eq!(first.mechanism.effects, 1);
        assert!(only(&first).verified_effect);
        assert!(!first.mechanism.goal_met, "one effect is not three");
    }

    // ------------------------------------------------------------------
    // Review finding 1: record identity binding and evidence resolution.
    // ------------------------------------------------------------------

    #[test]
    fn source_whose_instance_disagrees_with_its_author_is_rejected() {
        // FALSE POSITIVE the old evaluator produced: `is_kind` checked only
        // category + lifecycle + bbs_kind + schema_version, so a row whose
        // tuple author is not its claimed payload author passed as a genuine
        // finding. Every real BBS write authors the tuple as its own caller.
        let mut tuples = verified_tuples();
        tuples[0]["instance"] = json!("someone-else");
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &reviews(vec![review("p1")]),
        )
        .unwrap();
        assert_eq!(exclusion_reasons(&report), vec!["invalid_source"]);
        assert!(report.excluded[0].detail.contains("instance"));
        assert_eq!(report.mechanism.effects, 0);
    }

    #[test]
    fn source_without_the_daemon_minted_identity_prefix_is_rejected() {
        let mut tuples = verified_tuples();
        tuples[0]["identity"] = json!("finding-1");
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &reviews(vec![review("p1")]),
        )
        .unwrap();
        assert_eq!(exclusion_reasons(&report), vec!["invalid_source"]);
        assert!(report.excluded[0].detail.contains("bbs-finding-"));
    }

    #[test]
    fn a_receipt_is_not_a_reusable_source() {
        // The daemon refuses `bbs.reuse` on a receipt; the report must refuse
        // the same pairing rather than treat a telemetry/receipt row as a
        // shared finding.
        let source = reuse(
            "rx",
            "src-0",
            "TKT-0",
            "other-gen",
            "used",
            "2026-01-01T00:00:00Z",
        );
        let report = compute(
            &manifest(vec![pair("p1", "rx", "TKT-1", "gen-1")]),
            &capture(vec![source], Order::Unknown),
            &reviews(vec![review("p1")]),
        )
        .unwrap();
        assert_eq!(exclusion_reasons(&report), vec!["invalid_source"]);
        assert!(report.excluded[0].detail.contains("not a reusable source"));
    }

    #[test]
    fn exposure_from_another_repo_is_not_native_coverage() {
        // OMITTED SCOPE the old evaluator had: `is_event_kind` ignored tuple
        // scope entirely, so a foreign repo's exposure counted as prepared.
        let mut tuples = verified_tuples();
        tuples[3] = exposure_full(
            "x1",
            "src-1",
            "gen-1",
            "2026-01-01T12:00:00Z",
            "other",
            "agent",
        );
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &reviews(vec![review("p1")]),
        )
        .unwrap();
        assert_eq!(only(&report).coverage_status, "unknown");
        assert_eq!(report.presented_native, 0);
        assert_eq!(report.capture.observations_out_of_scope_repo, 1);
        assert!(
            !only(&report).verified_effect,
            "unknown coverage cannot certify"
        );
    }

    #[test]
    fn exposure_outside_the_frozen_window_is_not_native_coverage() {
        // Manifest.window was enforced NOWHERE before this correction.
        let m = manifest_with(
            vec![pair("p1", "src-1", "TKT-1", "gen-1")],
            vec![],
            Window {
                since: Some("2026-02-01T00:00:00Z".parse().unwrap()),
                until: None,
            },
        );
        let report = compute(
            &m,
            &capture(verified_tuples(), Order::Unknown),
            &reviews(vec![review("p1")]),
        )
        .unwrap();
        assert_eq!(only(&report).coverage_status, "unknown");
        assert!(report.capture.observations_out_of_window >= 1);
    }

    #[test]
    fn operator_bound_exposure_is_not_agent_exposure() {
        let mut tuples = verified_tuples();
        tuples[3] = exposure_full(
            "x1",
            "src-1",
            "gen-1",
            "2026-01-01T12:00:00Z",
            "repo",
            "operator",
        );
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &reviews(vec![review("p1")]),
        )
        .unwrap();
        assert_eq!(only(&report).coverage_status, "unknown");
        assert!(report
            .unresolved_records
            .iter()
            .any(|u| u.kind == EXPOSURE && u.reason.contains("no exact consumer generation")));
    }

    #[test]
    fn exposure_for_a_generation_that_never_launched_is_not_an_opportunity() {
        // A prepared selection for a spawn that never ran is neither a
        // discovery success nor a discovery failure: there was no decision.
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            exposure("x1", "src-1", "gen-1", "2026-01-01T12:00:00Z"),
        ];
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &reviews(vec![review("p1")]),
        )
        .unwrap();
        assert_eq!(only(&report).coverage_status, "prepared_not_launched");
        assert!(!only(&report).consumer_launched);
        assert_eq!(report.discovery.prepared_not_launched, vec!["p1"]);
        assert_eq!(report.discovery.known_coverage_pairs, 0);
        assert_eq!(
            report.discovery.rate, None,
            "absent denominator, not a 0% rate"
        );
    }

    #[test]
    fn evidence_naming_a_non_artifact_is_invalid_not_resolved() {
        // The old check tested only `scope`, so an EVENT could stand in for
        // the artifact a finding claims as its evidence.
        let mut tuples = verified_tuples();
        tuples.push(json!({
            "id": "ev-1", "category": "event", "scope": "repo", "identity": "something",
            "instance": "x", "created_at": "2026-01-01T00:00:00Z", "payload": {}
        }));
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &reviews(vec![review("p1")]),
        )
        .unwrap();
        assert_eq!(exclusion_reasons(&report), vec!["invalid_source_evidence"]);
        assert!(report.excluded[0].detail.contains("not an artifact"));
    }

    #[test]
    fn evidence_with_a_non_string_member_is_rejected_explicitly() {
        // The old `evidence_ids` silently filtered non-strings, so
        // `["ev-1", 7]` looked like a clean one-item list.
        let mut tuples = verified_tuples();
        tuples[0]["payload"]["evidence"] = json!(["ev-1", 7]);
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &reviews(vec![review("p1")]),
        )
        .unwrap();
        assert_eq!(exclusion_reasons(&report), vec!["invalid_source_evidence"]);
        assert!(report.excluded[0].detail.contains("non-string member"));
    }

    #[test]
    fn evidence_absent_from_the_capture_stays_unknown_rather_than_negative() {
        let mut tuples = verified_tuples();
        tuples[0]["payload"]["evidence"] = json!(["ev-missing"]);
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &reviews(vec![review("p1")]),
        )
        .unwrap();
        assert!(report.excluded.is_empty(), "the opportunity still stands");
        let p = only(&report);
        assert_eq!(p.source_evidence, "unknown");
        assert!(
            !p.verified_effect,
            "unknown evidence cannot certify an effect"
        );
        assert!(report
            .unresolved_records
            .iter()
            .any(|u| u.reason.contains("ev-missing")));
    }

    #[test]
    fn forged_assessment_not_authored_by_operator_is_not_trusted() {
        let mut tuples = verified_tuples();
        tuples[2]["instance"] = json!("consumer");
        tuples[2]["payload"]["agent"] = json!("consumer");
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &reviews(vec![review("p1")]),
        )
        .unwrap();
        assert_eq!(only(&report).assessed_verdict, None);
        assert_eq!(report.mechanism.effects, 0);
        assert!(report
            .invalid_records
            .iter()
            .any(|i| i.kind == ASSESSMENT && i.reason.contains("operator-only")));
    }

    // ------------------------------------------------------------------
    // Review finding 2: author exit.
    // ------------------------------------------------------------------

    #[test]
    fn agent_exit_for_the_source_generation_credits_author_exit() {
        let mut tuples = verified_tuples();
        tuples.push(agent_exit(
            "e1",
            "author",
            "author-gen",
            "sess-1",
            "TKT-source",
            "2026-01-01T00:00:00Z",
            "2026-01-01T18:00:00Z",
            Some("completed"),
        ));
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &reviews(vec![review_with_exit("p1", "e1")]),
        )
        .unwrap();
        assert!(report.author_exit_unsupported.is_empty());
        assert!(only(&report).author_terminal);
        assert_eq!(report.author_exit_reuse, vec!["p1"]);
        assert_eq!(report.mechanism.author_exit_effects, 1);
    }

    #[test]
    fn harness_result_is_refused_as_author_exit_evidence() {
        // FALSE POSITIVE: the old evaluator accepted `harness_result` as a
        // "lifecycle-terminal identity". The supervisor emits it when the
        // agent routes `rk done`, while the OS process is still alive and the
        // provider may still report a later total.
        let mut tuples = verified_tuples();
        tuples.push(harness_result(
            "h-author",
            "author",
            "author-gen",
            "TKT-source",
            "2026-01-01T18:00:00Z",
        ));
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &reviews(vec![review_with_exit("p1", "h-author")]),
        )
        .unwrap();
        assert!(!only(&report).author_terminal);
        assert_eq!(report.author_exit_unsupported.len(), 1);
        assert!(report.author_exit_unsupported[0]
            .reason
            .contains("still alive"));
        assert_eq!(report.mechanism.author_exit_effects, 0);
    }

    #[test]
    fn agent_lifecycle_is_refused_as_author_exit_evidence() {
        // The old evaluator listed `agent_lifecycle` as terminal, but
        // `emit_coordinator_event` writes no `spawn` field at all and its
        // `change` may be `started` — it cannot identify a terminal
        // generation even in principle.
        let mut tuples = verified_tuples();
        tuples.push(json!({
            "id": "l1", "category": "event", "scope": "repo", "identity": "agent_lifecycle",
            "instance": CASTLE, "created_at": "2026-01-01T18:00:00Z",
            "payload": {
                "route": "rollup", "severity": "info", "change": "started",
                "summary": "author started", "agent": "author",
                "generation": "2026-01-01T00:00:00Z", "declared_done": false
            }
        }));
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &reviews(vec![review_with_exit("p1", "l1")]),
        )
        .unwrap();
        assert!(!only(&report).author_terminal);
        assert!(report.author_exit_unsupported[0]
            .reason
            .contains("no `spawn` binding"));
    }

    #[test]
    fn agent_exit_followed_by_a_relaunch_before_the_reuse_is_not_credited() {
        // A manual respawn CONTINUES the same SpawnId, so an earlier exit is
        // not proof the author was gone when the consumer decided.
        let mut tuples = verified_tuples();
        tuples.push(agent_exit(
            "e1",
            "author",
            "author-gen",
            "sess-1",
            "TKT-source",
            "2026-01-01T00:00:00Z",
            "2026-01-01T06:00:00Z",
            Some("completed"),
        ));
        tuples.push(json!({
            "id": "s2", "category": "event", "scope": "repo", "identity": "agent_respawned",
            "instance": CASTLE, "created_at": "2026-01-01T12:00:00Z",
            "payload": {"agent": "author", "spawn": "author-gen", "task": "TKT-source",
                        "role": "rat", "launched_at": "2026-01-01T12:00:00Z"}
        }));
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &reviews(vec![review_with_exit("p1", "e1")]),
        )
        .unwrap();
        assert!(!only(&report).author_terminal);
        assert!(report.author_exit_unsupported[0]
            .reason
            .contains("running again"));
    }

    #[test]
    fn agent_exit_for_another_generation_or_repo_or_after_the_reuse_is_not_credited() {
        for (label, exit) in [
            (
                "wrong generation",
                agent_exit(
                    "e1",
                    "author",
                    "other-gen",
                    "s",
                    "TKT-source",
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T06:00:00Z",
                    Some("completed"),
                ),
            ),
            (
                "after the reuse",
                agent_exit(
                    "e1",
                    "author",
                    "author-gen",
                    "s",
                    "TKT-source",
                    "2026-01-01T00:00:00Z",
                    "2026-01-05T00:00:00Z",
                    Some("completed"),
                ),
            ),
        ] {
            let mut tuples = verified_tuples();
            tuples.push(exit);
            let report = compute(
                &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
                &capture(tuples, Order::Unknown),
                &reviews(vec![review_with_exit("p1", "e1")]),
            )
            .unwrap();
            assert!(
                !only(&report).author_terminal,
                "{label} must not be credited"
            );
            assert_eq!(report.author_exit_unsupported.len(), 1, "{label}");
        }
        // A foreign-repo exit is dropped from the index by scope, so it is
        // reported as "excluded from the frozen repo scope or window".
        let mut tuples = verified_tuples();
        let mut foreign = agent_exit(
            "e1",
            "author",
            "author-gen",
            "s",
            "TKT-source",
            "2026-01-01T00:00:00Z",
            "2026-01-01T06:00:00Z",
            Some("completed"),
        );
        foreign["scope"] = json!("other");
        foreign["payload"]["repo"] = json!("other");
        tuples.push(foreign);
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &reviews(vec![review_with_exit("p1", "e1")]),
        )
        .unwrap();
        assert!(
            !only(&report).author_terminal,
            "foreign repo must not be credited"
        );
    }

    // ------------------------------------------------------------------
    // Review finding 3: scope, denominators, and the verified-reuse gate.
    // ------------------------------------------------------------------

    #[test]
    fn fixture_pair_repo_must_match_its_batch_repo() {
        // The old validation only checked that the batch id existed.
        let mut m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        m.eligible_pairs[0].repo = "other".into();
        let err = validate_manifest(&m).unwrap_err().to_string();
        assert!(
            err.contains("its batch batch-1 is declared for repo repo"),
            "{err}"
        );
    }

    #[test]
    fn a_rejected_receipt_keeps_its_pair_in_the_eligible_denominator() {
        // OMITTED DENOMINATOR: the old evaluator `continue`d past the pair on
        // a wrong-generation claim, deleting a real opportunity from both the
        // discovery and verified-reuse rates. A bad claim removes the CLAIM.
        let mut tuples = verified_tuples();
        tuples[1] = reuse(
            "r1",
            "src-1",
            "TKT-1",
            "someone-else",
            "used",
            "2026-01-02T00:00:00Z",
        );
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &reviews(vec![review("p1")]),
        )
        .unwrap();
        assert!(report.excluded.is_empty(), "the opportunity is not erased");
        assert_eq!(report.eligible, 1);
        assert_eq!(report.discovery.eligible_pairs, 1);
        assert_eq!(report.claimed, 0);
        assert_eq!(report.rejected_claims.len(), 1);
        assert_eq!(report.rejected_claims[0].reason, "wrong_generation");
        assert_eq!(report.verified_reuse.eligible_consumer_tasks, 1);
        assert_eq!(report.verified_reuse.verified_used_or_adapted_tasks, 0);
        assert_eq!(report.verified_reuse.rate, Some(0.0));
    }

    #[test]
    fn duplicate_pair_is_counted_once_and_reported() {
        let m = manifest(vec![
            pair("p1", "src-1", "TKT-1", "gen-1"),
            pair("p2", "src-1", "TKT-1", "gen-1"),
        ]);
        let report = compute(
            &m,
            &capture(verified_tuples(), Order::Unknown),
            &reviews(vec![review("p1"), review("p2")]),
        )
        .unwrap();
        assert_eq!(
            report.eligible, 1,
            "a duplicate must not inflate the denominator"
        );
        assert_eq!(exclusion_reasons(&report), vec!["duplicate"]);
    }

    #[test]
    fn self_use_and_future_source_are_still_refused() {
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "author-gen")]),
            &capture(verified_tuples(), Order::Unknown),
            &reviews(vec![review("p1")]),
        )
        .unwrap();
        assert_eq!(exclusion_reasons(&report), vec!["self"]);

        let mut tuples = verified_tuples();
        tuples[0] = finding("src-1", "author-gen", "2026-01-09T00:00:00Z");
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &reviews(vec![review("p1")]),
        )
        .unwrap();
        assert_eq!(report.rejected_claims[0].reason, "future_source");
        assert_eq!(report.claimed, 0);
    }

    #[test]
    fn verified_reuse_uses_the_same_gate_as_the_mechanism_goal() {
        // The old per-task rate checked only outcome + verdict, so a pair the
        // mechanism goal refused for unknown coverage still counted as
        // verified reuse. The two can no longer disagree.
        let mut tuples = verified_tuples();
        tuples.remove(3); // drop the native exposure -> coverage unknown
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &reviews(vec![review("p1")]),
        )
        .unwrap();
        assert_eq!(only(&report).coverage_status, "unknown");
        assert_eq!(
            report.outcome_classes.verified, 1,
            "the verdict is still reported"
        );
        assert_eq!(report.verified_reuse.verified_used_or_adapted_tasks, 0);
        assert_eq!(report.mechanism.effects, 0);
        assert_eq!(report.unknown_coverage, vec!["p1"]);
    }

    #[test]
    fn a_not_prepared_pair_cannot_be_a_verified_effect() {
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(
                verified_tuples()
                    .into_iter()
                    .enumerate()
                    .filter(|(i, _)| *i != 3)
                    .map(|(_, t)| t)
                    .collect(),
                Order::Unknown,
            ),
            &reviews(vec![Review {
                coverage: Coverage::NotPrepared {
                    evidence: ReviewedEvidence::Reference("telemetry-checked".into()),
                },
                ..review("p1")
            }]),
        )
        .unwrap();
        assert_eq!(only(&report).coverage_status, "not_prepared");
        assert!(!only(&report).verified_effect);
        assert_eq!(report.discovery.known_coverage_pairs, 1);
        assert_eq!(report.discovery.rate, Some(0.0));
    }

    #[test]
    fn operator_relayed_effect_is_verified_but_excluded_from_the_goal() {
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(verified_tuples(), Order::Unknown),
            &reviews(vec![Review {
                relayed_by_operator: true,
                ..review("p1")
            }]),
        )
        .unwrap();
        assert!(only(&report).verified_effect);
        assert!(!only(&report).counts_as_effect);
        assert_eq!(report.mechanism.effects, 0);
    }

    #[test]
    fn ambiguous_assessment_order_is_not_silently_resolved_but_sequence_order_is() {
        let mut tuples = verified_tuples();
        tuples.push(assessment("a2", "r1", "incorrect", "2026-01-04T00:00:00Z"));
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let r = reviews(vec![review("p1")]);

        let unknown = compute(&m, &capture(tuples.clone(), Order::Unknown), &r).unwrap();
        assert_eq!(unknown.ambiguous_assessments.len(), 1);
        assert_eq!(only(&unknown).assessed_verdict, None);
        assert_eq!(unknown.mechanism.effects, 0);

        let ordered = compute(&m, &capture(tuples, Order::PersistenceSequence), &r).unwrap();
        assert!(ordered.ambiguous_assessments.is_empty());
        assert_eq!(
            only(&ordered).assessed_verdict.as_deref(),
            Some("incorrect")
        );
        assert_eq!(ordered.quality.incorrect_reuse, 1);
    }

    #[test]
    fn truncated_capture_cannot_certify_the_mechanism_goal() {
        let mut c = capture(verified_tuples(), Order::Unknown);
        c.truncated = true;
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &c,
            &reviews(vec![review("p1")]),
        )
        .unwrap();
        assert!(!report.mechanism.goal_met);
        assert!(report.tuples_truncated);
    }

    #[test]
    fn opens_are_reported_and_bound_to_the_consumer_generation() {
        let mut tuples = verified_tuples();
        tuples.push(open_record("o1", "src-1", "gen-1", "2026-01-01T13:00:00Z"));
        tuples.push(open_record(
            "o2",
            "src-1",
            "other-gen",
            "2026-01-01T13:00:00Z",
        ));
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &reviews(vec![review("p1")]),
        )
        .unwrap();
        assert!(only(&report).opened);
        assert_eq!(
            report.opened, 1,
            "a different generation's open is not this pair's"
        );
    }

    // ------------------------------------------------------------------
    // Review finding 4: deliveries, cost segments and durations.
    // ------------------------------------------------------------------

    #[test]
    fn a_frozen_task_with_no_pair_still_reports_its_costs_and_failures() {
        // OMITTED DENOMINATOR: deliveries used to be derived from the pairs
        // that survived evaluation, so a frozen task with no eligible source
        // and no receipt vanished along with its spend and its failures.
        let m = manifest_with(
            vec![],
            vec![ConsumerTaskScope {
                task: "TKT-lonely".into(),
                repo: "repo".into(),
                batch: "batch-1".into(),
            }],
            Window::default(),
        );
        let mut failed = harness_result("h1", "rat", "gen-9", "TKT-lonely", "2026-01-02T00:00:00Z");
        failed["payload"]["is_error"] = json!(true);
        failed["payload"]["declared_done"] = json!(false);
        let report = compute(&m, &capture(vec![failed], Order::Unknown), &reviews(vec![])).unwrap();
        assert_eq!(report.eligible, 0);
        assert_eq!(report.deliveries.len(), 1);
        let d = &report.deliveries[0];
        assert_eq!(d.task, "TKT-lonely");
        assert_eq!(d.enrollment, "frozen_consumer_task");
        assert_eq!(d.completions, 1);
        assert_eq!(d.failed_completions, 1, "a failed attempt is retained");
        assert_eq!(d.provisional_completion_cost_usd, Some(0.5));
        assert_eq!(
            d.reported_cost_estimate_usd, None,
            "provisional is never final"
        );
        assert_eq!(d.cost_coverage, "missing");
    }

    #[test]
    fn cumulative_results_within_one_segment_are_not_summed() {
        // Provider `total_cost_usd` is CUMULATIVE within one query; the last
        // reported total per segment is the amount.
        let m = manifest_with(
            vec![],
            vec![ConsumerTaskScope {
                task: "TKT-1".into(),
                repo: "repo".into(),
                batch: "batch-1".into(),
            }],
            Window::default(),
        );
        let tuples = vec![
            final_usage(
                "u1",
                "gen-1",
                "sess-1",
                "ps-1",
                "TKT-1",
                "completed",
                Some(3.0),
                PROVIDER_COST_BASIS,
                "2026-01-02T00:00:00Z",
            ),
            final_usage(
                "u2",
                "gen-1",
                "sess-1",
                "ps-1",
                "TKT-1",
                "completed",
                Some(7.25),
                PROVIDER_COST_BASIS,
                "2026-01-02T01:00:00Z",
            ),
            agent_exit(
                "e1",
                "rat",
                "gen-1",
                "sess-1",
                "TKT-1",
                "2026-01-02T00:00:00Z",
                "2026-01-02T02:00:00Z",
                Some("completed"),
            ),
        ];
        let report = compute(&m, &capture(tuples, Order::Unknown), &reviews(vec![])).unwrap();
        let d = &report.deliveries[0];
        assert_eq!(d.cost_coverage, "complete");
        assert_eq!(
            d.reported_cost_estimate_usd,
            Some(7.25),
            "last total, not 10.25"
        );
        assert_eq!(d.reported_cost_basis.as_deref(), Some(PROVIDER_COST_BASIS));
        assert_eq!(d.process_lifetime_ms, Some(2 * 60 * 60 * 1000));
        assert_eq!(
            d.active_work_ms, None,
            "process lifetime is not active work"
        );
        assert!(d.active_work_coverage.contains("unknown"));
    }

    #[test]
    fn two_provider_segments_of_one_launch_are_summed_but_a_second_launch_is_its_own_segment() {
        let m = manifest_with(
            vec![],
            vec![ConsumerTaskScope {
                task: "TKT-1".into(),
                repo: "repo".into(),
                batch: "batch-1".into(),
            }],
            Window::default(),
        );
        let tuples = vec![
            final_usage(
                "u1",
                "gen-1",
                "sess-1",
                "ps-1",
                "TKT-1",
                "completed",
                Some(2.0),
                PROVIDER_COST_BASIS,
                "2026-01-02T00:00:00Z",
            ),
            final_usage(
                "u2",
                "gen-1",
                "sess-2",
                "ps-2",
                "TKT-1",
                "completed",
                Some(3.0),
                PROVIDER_COST_BASIS,
                "2026-01-02T03:00:00Z",
            ),
            agent_exit(
                "e1",
                "rat",
                "gen-1",
                "sess-1",
                "TKT-1",
                "2026-01-02T00:00:00Z",
                "2026-01-02T01:00:00Z",
                Some("completed"),
            ),
            agent_exit(
                "e2",
                "rat",
                "gen-1",
                "sess-2",
                "TKT-1",
                "2026-01-02T02:00:00Z",
                "2026-01-02T04:00:00Z",
                Some("completed"),
            ),
        ];
        let report = compute(&m, &capture(tuples, Order::Unknown), &reviews(vec![])).unwrap();
        let d = &report.deliveries[0];
        assert_eq!(d.reported_cost_estimate_usd, Some(5.0));
        assert_eq!(d.launches, 2, "one SpawnId, two physical launches");
        assert_eq!(d.generations.len(), 1);
        assert_eq!(d.generations[0].cost_segments.len(), 2);
    }

    #[test]
    fn paused_result_then_more_work_then_a_kill_leaves_the_cost_unknown() {
        // The acceptance edge case: a `paused` provider result can be followed
        // by more usage and then a budget kill with no further result. The
        // earlier cumulative total is a partial amount, not final cost —
        // finality is NOT inferred from finding any result before an exit.
        let m = manifest_with(
            vec![],
            vec![ConsumerTaskScope {
                task: "TKT-1".into(),
                repo: "repo".into(),
                batch: "batch-1".into(),
            }],
            Window::default(),
        );
        let mut killed = agent_exit(
            "e1",
            "rat",
            "gen-1",
            "sess-1",
            "TKT-1",
            "2026-01-02T00:00:00Z",
            "2026-01-02T05:00:00Z",
            Some("running"),
        );
        killed["payload"]["exit_code"] = Value::Null;
        killed["payload"]["crashed"] = json!(true);
        let tuples = vec![
            final_usage(
                "u1",
                "gen-1",
                "sess-1",
                "ps-1",
                "TKT-1",
                "paused",
                Some(4.5),
                PROVIDER_COST_BASIS,
                "2026-01-02T01:00:00Z",
            ),
            killed,
        ];
        let report = compute(&m, &capture(tuples, Order::Unknown), &reviews(vec![])).unwrap();
        let d = &report.deliveries[0];
        assert_eq!(
            d.reported_cost_estimate_usd, None,
            "a partial amount is not a total"
        );
        assert_eq!(d.partial_reported_usd, Some(4.5));
        assert_eq!(d.cost_coverage, "partial");
        assert!(d.unknown_cost.iter().any(|u| u.contains("not terminal")));
        assert!(!d.generations[0].cost_segments[0].final_cost);
    }

    #[test]
    fn a_terminal_result_with_no_observed_exit_is_not_final_either() {
        let m = manifest_with(
            vec![],
            vec![ConsumerTaskScope {
                task: "TKT-1".into(),
                repo: "repo".into(),
                batch: "batch-1".into(),
            }],
            Window::default(),
        );
        let tuples = vec![final_usage(
            "u1",
            "gen-1",
            "sess-1",
            "ps-1",
            "TKT-1",
            "completed",
            Some(1.5),
            PROVIDER_COST_BASIS,
            "2026-01-02T01:00:00Z",
        )];
        let report = compute(&m, &capture(tuples, Order::Unknown), &reviews(vec![])).unwrap();
        let d = &report.deliveries[0];
        assert_eq!(d.reported_cost_estimate_usd, None);
        assert!(d
            .unknown_cost
            .iter()
            .any(|u| u.contains("no agent_exit observed")));
        assert_eq!(
            d.process_lifetime_ms, None,
            "never invented from a result time"
        );
    }

    #[test]
    fn a_daemon_priced_segment_is_never_pooled_with_a_provider_total() {
        let m = manifest_with(
            vec![],
            vec![ConsumerTaskScope {
                task: "TKT-1".into(),
                repo: "repo".into(),
                batch: "batch-1".into(),
            }],
            Window::default(),
        );
        let tuples = vec![
            final_usage(
                "u1",
                "gen-1",
                "sess-1",
                "ps-1",
                "TKT-1",
                "completed",
                Some(2.0),
                DAEMON_COST_BASIS,
                "2026-01-02T00:00:00Z",
            ),
            agent_exit(
                "e1",
                "rat",
                "gen-1",
                "sess-1",
                "TKT-1",
                "2026-01-02T00:00:00Z",
                "2026-01-02T01:00:00Z",
                Some("completed"),
            ),
        ];
        let report = compute(&m, &capture(tuples, Order::Unknown), &reviews(vec![])).unwrap();
        let d = &report.deliveries[0];
        assert_eq!(d.reported_cost_estimate_usd, None);
        assert_eq!(d.daemon_priced_estimate_usd, Some(2.0));
        assert_eq!(d.cost_coverage, "partial");
    }

    #[test]
    fn a_null_reported_cost_is_unknown_not_zero() {
        let m = manifest_with(
            vec![],
            vec![ConsumerTaskScope {
                task: "TKT-1".into(),
                repo: "repo".into(),
                batch: "batch-1".into(),
            }],
            Window::default(),
        );
        let tuples = vec![
            final_usage(
                "u1",
                "gen-1",
                "sess-1",
                "ps-1",
                "TKT-1",
                "completed",
                None,
                "unknown",
                "2026-01-02T00:00:00Z",
            ),
            agent_exit(
                "e1",
                "rat",
                "gen-1",
                "sess-1",
                "TKT-1",
                "2026-01-02T00:00:00Z",
                "2026-01-02T01:00:00Z",
                Some("completed"),
            ),
        ];
        let report = compute(&m, &capture(tuples, Order::Unknown), &reviews(vec![])).unwrap();
        let d = &report.deliveries[0];
        assert_eq!(d.reported_cost_estimate_usd, None);
        assert_eq!(d.partial_reported_usd, None);
        assert!(d.unknown_cost.iter().any(|u| u.contains("null")));
    }

    #[test]
    fn phase_durations_are_bucketed_and_acceptance_needs_delivery_closure() {
        let m = manifest_with(
            vec![],
            vec![ConsumerTaskScope {
                task: "TKT-1".into(),
                repo: "repo".into(),
                batch: "batch-1".into(),
            }],
            Window::default(),
        );
        let merge_only = vec![
            span("s1", "TKT-1", "agent_launched", 1_000, Some(250)),
            span("s2", "TKT-1", "verification", 5_000, Some(700)),
            span("s3", "TKT-1", "attention_hold", 9_000, None),
            span("s4", "TKT-1", "rework", 400, None),
            span("s5", "TKT-1", "merge", 100, None),
        ];
        let report = compute(
            &m,
            &capture(merge_only.clone(), Order::Unknown),
            &reviews(vec![]),
        )
        .unwrap();
        let d = &report.deliveries[0];
        assert_eq!(
            d.phase_ms.work_phases_ms,
            Some(1_500),
            "launch+rework+merge only"
        );
        assert_eq!(
            d.phase_ms.verification_ms,
            Some(5_700),
            "duration plus its own queue"
        );
        assert_eq!(d.phase_ms.attention_hold_ms, Some(9_000));
        assert_eq!(d.phase_ms.queue_wait_ms, Some(250));
        assert_eq!(d.accepted, None, "a merge alone does not prove acceptance");
        assert_eq!(report.quality.attention_hold_spans, 1);
        assert_eq!(report.quality.rework_spans, 1);

        let mut closed = merge_only;
        closed.push(span("s6", "TKT-1", "delivery_closure", 10, None));
        let report = compute(&m, &capture(closed, Order::Unknown), &reviews(vec![])).unwrap();
        assert_eq!(report.deliveries[0].accepted, Some(true));
        assert_eq!(
            report.deliveries[0].acceptance_evidence.as_deref(),
            Some("s6")
        );
    }

    #[test]
    fn spans_from_another_repo_do_not_merge_into_a_same_named_task() {
        // `build_critical_path` dedups on (phase, attempt) and does NOT filter
        // by scope, so the caller has to pre-filter or two repos' identically
        // named tasks silently merge.
        let m = manifest_with(
            vec![],
            vec![ConsumerTaskScope {
                task: "TKT-1".into(),
                repo: "repo".into(),
                batch: "batch-1".into(),
            }],
            Window::default(),
        );
        let mut foreign = span("s-foreign", "TKT-1", "delivery_closure", 10, None);
        foreign["scope"] = json!("other");
        let report = compute(
            &m,
            &capture(vec![foreign], Order::Unknown),
            &reviews(vec![]),
        )
        .unwrap();
        assert_eq!(report.deliveries[0].accepted, None);
        assert_eq!(report.capture.observations_out_of_scope_repo, 1);
        assert_eq!(report.tasks_without_native_records, vec!["repo/TKT-1"]);
    }

    // ------------------------------------------------------------------
    // Review finding 5: reviewed annotations kept apart from telemetry.
    // ------------------------------------------------------------------

    #[test]
    fn reviewed_prepared_coverage_is_labelled_reviewed_not_native() {
        let tuples: Vec<Value> = verified_tuples()
            .into_iter()
            .enumerate()
            .filter(|(i, _)| *i != 3)
            .map(|(_, t)| t)
            .collect();
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &reviews(vec![review_reviewed_prepared("p1", "operator-notebook-p3")]),
        )
        .unwrap();
        let p = only(&report);
        assert_eq!(p.coverage_status, "prepared");
        assert_eq!(p.coverage_provenance, "reviewed");
        assert_eq!(
            p.coverage_reference_resolved,
            Some(false),
            "labelled, not silently trusted"
        );
        assert_eq!(report.presented_native, 0);
        assert_eq!(report.presented_reviewed, 1);
        assert_eq!(report.discovery.prepared_native, 0);
        assert_eq!(report.discovery.prepared_reviewed, 1);
    }

    #[test]
    fn a_blank_reviewed_reference_is_rejected() {
        let err = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(verified_tuples(), Order::Unknown),
            &reviews(vec![review_reviewed_prepared("p1", "   ")]),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("must not be blank"), "{err}");
    }

    #[test]
    fn a_reviewed_annotation_cannot_smuggle_an_invented_time_saving() {
        // `deny_unknown_fields` makes an invented measurement a hard input
        // error rather than a silently-ignored key.
        let err = serde_json::from_value::<ReviewedAnnotation>(json!({
            "kind": "operator-steer",
            "reference": "01M2...",
            "time_saved_ms": 900000
        }))
        .unwrap_err()
        .to_string();
        assert!(err.contains("time_saved_ms"), "{err}");
    }

    #[test]
    fn reviewed_interventions_supplement_attention_hold_spans_as_a_lower_bound() {
        // An `attention_hold` count is NOT every intervention.
        let m = manifest_with(
            vec![],
            vec![ConsumerTaskScope {
                task: "TKT-1".into(),
                repo: "repo".into(),
                batch: "batch-1".into(),
            }],
            Window::default(),
        );
        let file: ReviewsFile = serde_json::from_value(json!({
            "pairs": [],
            "tasks": [{
                "task": "TKT-1",
                "repo": "repo",
                "interventions": [
                    {"kind": "operator-steer", "reference": "x1", "note": "re-scoped by hand"}
                ],
                "repeated_investigations": [
                    {"kind": "repeat-investigation", "reference": "x2"}
                ]
            }]
        }))
        .unwrap();
        let report = compute(
            &m,
            &capture(
                vec![span("s3", "TKT-1", "attention_hold", 5, None)],
                Order::Unknown,
            ),
            &file,
        )
        .unwrap();
        assert_eq!(report.quality.attention_hold_spans, 1);
        assert_eq!(report.quality.reviewed_interventions.len(), 1);
        assert_eq!(report.quality.interventions_known, 2);
        assert!(report
            .quality
            .interventions_coverage
            .contains("lower bound"));
        assert_eq!(report.quality.repeated_investigations.len(), 1);
        assert!(!report.quality.repeated_investigations[0].reference_resolved);
    }

    #[test]
    fn a_task_annotation_cannot_introduce_unfrozen_scope() {
        let file: ReviewsFile = serde_json::from_value(json!({
            "pairs": [],
            "tasks": [{"task": "TKT-not-frozen", "repo": "repo"}]
        }))
        .unwrap();
        let err = compute(&manifest(vec![]), &capture(vec![], Order::Unknown), &file)
            .unwrap_err()
            .to_string();
        assert!(err.contains("not frozen"), "{err}");
    }

    // ------------------------------------------------------------------
    // Input shapes and enrollment.
    // ------------------------------------------------------------------

    #[test]
    fn parses_bare_array_scan_object_and_capture_envelope() {
        assert_eq!(
            parse_tuple_capture(&json!([])).unwrap().order,
            Order::Unknown
        );
        let scan = parse_tuple_capture(&json!({"tuples": [], "truncated": true})).unwrap();
        assert_eq!(
            scan.order,
            Order::Unknown,
            "scan order is never persistence order"
        );
        assert!(scan.truncated);
        let env = parse_tuple_capture(&json!({
            "schema_version": 1, "order": "persistence_sequence",
            "source": "space.persistence_delta", "tuples": []
        }))
        .unwrap();
        assert_eq!(env.order, Order::PersistenceSequence);
        assert_eq!(env.source.as_deref(), Some("space.persistence_delta"));
        assert!(parse_tuple_capture(&json!({"rows": []})).is_err());
    }

    #[test]
    fn rejects_wrong_schema_version_and_unknown_manifest_fields() {
        let mut m = manifest(vec![]);
        m.schema_version = 2;
        assert!(validate_manifest(&m).is_err());
        let err = serde_json::from_value::<Manifest>(json!({
            "schema_version": 1, "experiment_id": "e", "batches": [],
            "windwo": {"since": "2026-01-01T00:00:00Z"}
        }))
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("windwo"),
            "a misspelled window must not be ignored: {err}"
        );
    }

    #[test]
    fn live_enrollment_mints_a_pair_bound_to_frozen_scope_only() {
        let m = manifest_with(
            vec![],
            vec![ConsumerTaskScope {
                task: "TKT-1".into(),
                repo: "repo".into(),
                batch: "batch-1".into(),
            }],
            Window::default(),
        );
        let declares = PairDeclaration {
            source: "src-1".into(),
            consumer_task: "TKT-1".into(),
            consumer_generation: "gen-1".into(),
            repo: "repo".into(),
            batch: "batch-1".into(),
        };
        let report = compute(
            &m,
            &capture(verified_tuples(), Order::Unknown),
            &reviews(vec![Review {
                declares: Some(declares.clone()),
                ..review("live-1")
            }]),
        )
        .unwrap();
        assert_eq!(report.eligible, 1);
        assert_eq!(only(&report).pair, "live-1");

        // A task outside frozen scope cannot be enrolled retrospectively.
        let mut bad = declares;
        bad.consumer_task = "TKT-elsewhere".into();
        assert!(compute(
            &m,
            &capture(verified_tuples(), Order::Unknown),
            &reviews(vec![Review {
                declares: Some(bad),
                ..review("live-2")
            }]),
        )
        .is_err());

        // A review may add evidence to a predeclared pair but never redeclare it.
        let pm = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        assert!(compute(
            &pm,
            &capture(verified_tuples(), Order::Unknown),
            &reviews(vec![Review {
                declares: Some(PairDeclaration {
                    source: "src-1".into(),
                    consumer_task: "TKT-1".into(),
                    consumer_generation: "other".into(),
                    repo: "repo".into(),
                    batch: "batch-1".into(),
                }),
                ..review("p1")
            }]),
        )
        .is_err());
        // ...and a pair nobody froze is refused outright.
        assert!(compute(
            &pm,
            &capture(verified_tuples(), Order::Unknown),
            &reviews(vec![review("ghost")]),
        )
        .is_err());
    }

    #[test]
    fn mechanism_goal_needs_three_effects_across_two_batches_with_one_author_exit() {
        let mut m = manifest(vec![]);
        m.batches.push(Batch {
            id: "batch-2".into(),
            arm: "real".into(),
            repo: "repo".into(),
        });
        let mut tuples = vec![];
        let mut pairs = vec![];
        let mut rs = vec![];
        for (n, batch) in [(1, "batch-1"), (2, "batch-1"), (3, "batch-2")] {
            let src = format!("src-{n}");
            let gen = format!("gen-{n}");
            let task = format!("TKT-{n}");
            tuples.push(finding_in(
                &src,
                &format!("author-{n}"),
                &format!("agen-{n}"),
                "2026-01-01T00:00:00Z",
                "repo",
            ));
            tuples.push(reuse(
                &format!("r{n}"),
                &src,
                &task,
                &gen,
                "used",
                "2026-01-02T00:00:00Z",
            ));
            tuples.push(assessment(
                &format!("a{n}"),
                &format!("r{n}"),
                "verified",
                "2026-01-03T00:00:00Z",
            ));
            tuples.push(exposure(
                &format!("x{n}"),
                &src,
                &gen,
                "2026-01-01T12:00:00Z",
            ));
            tuples.push(harness_result(
                &format!("h{n}"),
                "consumer",
                &gen,
                &task,
                "2026-01-02T01:00:00Z",
            ));
            pairs.push(EligiblePair {
                id: format!("p{n}"),
                source: src,
                consumer_task: task,
                consumer_generation: gen,
                repo: "repo".into(),
                batch: batch.into(),
            });
            rs.push(review(&format!("p{n}")));
        }
        // Only the third author is proven to have physically exited.
        tuples.push(agent_exit(
            "e3",
            "author-3",
            "agen-3",
            "sess-3",
            "TKT-source",
            "2026-01-01T00:00:00Z",
            "2026-01-01T06:00:00Z",
            Some("completed"),
        ));
        rs[2] = review_with_exit("p3", "e3");
        m.eligible_pairs = pairs;
        let report = compute(&m, &capture(tuples, Order::Unknown), &reviews(rs)).unwrap();
        assert_eq!(report.mechanism.effects, 3);
        assert_eq!(report.mechanism.batches, 2);
        assert_eq!(report.mechanism.author_exit_effects, 1);
        assert!(report.mechanism.goal_met);
        assert_eq!(report.verified_reuse.verified_used_or_adapted_tasks, 3);
        assert_eq!(report.verified_reuse.rate, Some(1.0));
    }

    #[test]
    fn render_reports_unknowns_as_unknown_rather_than_zero() {
        let report = compute(
            &manifest(vec![]),
            &capture(vec![], Order::Unknown),
            &reviews(vec![]),
        )
        .unwrap();
        let text = render(&report);
        assert!(text.contains("unknown (no denominator)"), "{text}");
        assert!(text.contains("evaluator_version=2"));
        assert_eq!(report.discovery.rate, None);
        assert_eq!(report.verified_reuse.rate, None);
    }

    // ------------------------------------------------------------------
    // Review finding 6: replay against a real native producer.
    // ------------------------------------------------------------------

    /// Drives the COMMITTED `task_span` producer
    /// (`rk_daemon::span::record_phase_span`) against a real in-memory
    /// `Space`, reads the rows back out, and feeds the resulting wire tuples
    /// straight into `compute`. Nothing here transcribes a field name by
    /// hand, so a producer-side rename breaks this test instead of silently
    /// zeroing a metric.
    ///
    /// LIMITATION, stated rather than hidden: only `task_span` has a producer
    /// reachable from this crate today. `harness_result` lives behind the
    /// supervisor, and `exposure`/`open`/`agent_exit`/`agent_final_usage` have
    /// no committed producer on `main` at all, so their field compatibility is
    /// still established only against S2's published contract. The combined
    /// real-CLI `rk bbs export | rk bbs report` replay belongs to S2/S4.
    #[test]
    fn native_task_span_producer_replays_directly_into_the_report() {
        use rk_daemon::span::{Phase, PhaseSpan};
        use rk_space::Space;

        let space = Space::open_in_memory().unwrap();
        // Anchor every span to one fixed instant so the wire payload — and so
        // this test — is deterministic.
        let now: DateTime<Utc> = "2026-01-02T00:00:00Z".parse().unwrap();
        for (phase, dur) in [
            (Phase::AgentLaunched, 1_000u64),
            (Phase::VerificationQueued, 5_000),
            (Phase::AttentionHold, 9_000),
            (Phase::DeliveryClosure, 10),
        ] {
            let span =
                PhaseSpan::from_durations("TKT-native", phase, None, Some(dur), now).repo("repo");
            assert!(rk_daemon::span::record_phase_span(&space, "repo", CASTLE, &span).unwrap());
        }
        let rows: Vec<Value> = space
            .scan(
                &rk_core::tuple::Pattern::category(rk_core::tuple::Category::Event)
                    .identity(rk_daemon::span::SPAN_IDENTITY)
                    .scope("repo"),
            )
            .unwrap()
            .iter()
            .map(|t| serde_json::to_value(t).unwrap())
            .collect();
        assert_eq!(rows.len(), 4, "the real producer wrote four spans");

        let m = manifest_with(
            vec![],
            vec![ConsumerTaskScope {
                task: "TKT-native".into(),
                repo: "repo".into(),
                batch: "batch-1".into(),
            }],
            Window::default(),
        );
        let report = compute(&m, &capture(rows, Order::Unknown), &reviews(vec![])).unwrap();
        let d = &report.deliveries[0];
        assert_eq!(d.task, "TKT-native");
        assert_eq!(
            d.phase_ms.work_phases_ms,
            Some(1_010),
            "agent_launched + delivery_closure, read off real producer rows"
        );
        assert_eq!(d.phase_ms.verification_ms, Some(5_000));
        assert_eq!(d.phase_ms.attention_hold_ms, Some(9_000));
        assert_eq!(d.accepted, Some(true));
        assert_eq!(report.quality.attention_hold_spans, 1);
        assert!(report.tasks_without_native_records.is_empty());
    }
}

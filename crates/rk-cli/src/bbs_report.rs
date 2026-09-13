//! `rk bbs report --manifest FILE --tuples FILE --reviews FILE [--output FILE]`
//!
//! Offline, deterministic evidence report for the stigmergy program
//! (docs/2026-09-12-stigmergy-evidence-and-trial.md, S3:
//! TKT-tavik-kifos-lozuf). No daemon connection, worker credentials, model
//! call or network is required: this is pure aggregation over three JSON
//! files an operator prepares ahead of time. See
//! docs/2026-09-13-stigmergy-report-capture.md for the capture recipe, the
//! capture-envelope contract this module consumes, and compact templates
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
//! batches exist, which consumer tasks are in play for each, and the
//! (fixed, not manifest-configurable) eligibility rule itself. So
//! `Manifest` freezes batches and consumer-task scope; concrete pairs
//! (source, consumer generation) are enrolled later, in the reviews file,
//! each bound to an already-frozen batch/task. A manifest may still
//! predeclare full pairs directly (`eligible_pairs`) for deterministic
//! fixture/replay cases where the pair identities are already known.

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
pub const EVALUATOR_VERSION: u32 = 1;

/// Frozen per the design doc: "three verified used/adapted effects across at
/// least two batches, at least one after its author exited". Not
/// manifest-configurable — a manifest cannot raise or lower its own bar.
pub const MECHANISM_EFFECTS_REQUIRED: usize = 3;
pub const MECHANISM_BATCHES_REQUIRED: usize = 2;
pub const MECHANISM_AUTHOR_EXIT_REQUIRED: usize = 1;

// `bbs_kind` literals from the design doc's payload contract table.
const FINDING: &str = "finding";
const REUSE: &str = "reuse";
const ASSESSMENT: &str = "assessment";
const EXPOSURE: &str = "exposure";
const OPEN: &str = "open";

// Event identities recognised as a generation's terminal lifecycle record,
// for validating a credited author-exit effect (see `resolve_author_exit`).
// `harness_result` is emitted exactly once per generation by the existing
// supervisor (crates/rk-daemon/src/supervisor.rs); `agent_lifecycle` is its
// coordinator-facing sibling emitted the same way.
const LIFECYCLE_TERMINAL_IDENTITIES: [&str; 2] = ["harness_result", "agent_lifecycle"];

// ---------------------------------------------------------------------
// Manifest: the versioned, frozen experiment/scope declaration.
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub schema_version: u32,
    pub experiment_id: String,
    #[serde(default)]
    pub repos: Vec<String>,
    #[serde(default)]
    pub window: Window,
    #[serde(default)]
    pub build: BuildIdentity,
    #[serde(default)]
    pub quality_criteria: Vec<String>,
    pub batches: Vec<Batch>,
    /// Frozen consumer-task scope: which tasks are in play for which batch.
    /// Required before a review may enroll a live pair for that task/batch;
    /// not required for a predeclared `eligible_pairs` fixture.
    #[serde(default)]
    pub consumer_tasks: Vec<ConsumerTaskScope>,
    /// Full pairs known ahead of time — deterministic fixture/replay only.
    /// A live batch cannot populate this (see module docs); use reviews'
    /// `declares` instead.
    #[serde(default)]
    pub eligible_pairs: Vec<EligiblePair>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Window {
    #[serde(default)]
    pub since: Option<DateTime<Utc>>,
    #[serde(default)]
    pub until: Option<DateTime<Utc>>,
}

/// Exact build/deployment identity this experiment ran under. Per the
/// design doc's completion evidence: "Main, remote, installed rk/rk-mcp and
/// daemon identities are recorded at activation".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
pub struct Batch {
    pub id: String,
    pub arm: String,
    pub repo: String,
}

/// A consumer task frozen into scope for a batch, before that batch's
/// concrete generations exist.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerTaskScope {
    pub task: String,
    pub repo: String,
    pub batch: String,
}

/// One source/consumer pairing: "source existed before the relevant
/// decision, applies to the task, and is not the consumer's own work"
/// (design doc). `consumer_generation` is the exact agent generation
/// (matches a tuple payload's `spawn` field) credited with the reuse.
///
/// Populated one of two ways: predeclared directly in
/// `Manifest.eligible_pairs` (fixture/replay, pair identities already
/// known), or minted by a `Review.declares` entry bound to an
/// already-frozen `ConsumerTaskScope` (a live batch, where the source id
/// and consumer generation only exist once the batch has actually run).
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    let mut seen_batches = BTreeSet::new();
    for b in &m.batches {
        if b.id.trim().is_empty() {
            bail!("batch id must not be empty");
        }
        if !seen_batches.insert(b.id.as_str()) {
            bail!("duplicate batch id: {}", b.id);
        }
        if b.arm.trim().is_empty() {
            bail!("batch {} must declare a non-empty arm", b.id);
        }
        if b.repo.trim().is_empty() {
            bail!("batch {} must declare a non-empty repo", b.id);
        }
    }
    for c in &m.consumer_tasks {
        if c.task.trim().is_empty() || c.repo.trim().is_empty() || c.batch.trim().is_empty() {
            bail!("consumer_tasks entries must declare task/repo/batch");
        }
        if !m
            .batches
            .iter()
            .any(|b| b.id == c.batch && b.repo == c.repo)
        {
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
        if !seen_batches.contains(p.batch.as_str()) {
            bail!(
                "eligible pair {} references unknown batch {}",
                p.id,
                p.batch
            );
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
    #[allow(dead_code)]
    pub source: Option<String>,
}

/// Parses either shape: a bare tuple array, the raw `rk --json scan` object
/// (`{"tuples":[...], "truncated":bool, ...}`), or the forward-looking
/// capture envelope (`{"schema_version":1,"order":"persistence_sequence",
/// "tuples":[...],...}`). Legacy/raw input is always `Order::Unknown` —
/// never inferred from tuple id (ULID) or `created_at`.
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Coverage {
    /// An operator manually confirms this source was prepared for this
    /// consumer generation (e.g. before S2's exposure telemetry existed).
    Prepared { evidence: String },
    /// An operator manually confirms telemetry was checked and this source
    /// was absent from it.
    NotPrepared { evidence: String },
    /// No telemetry and no operator determination either way.
    Unknown { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Review {
    /// References a predeclared `Manifest.eligible_pairs[].id` (fixture/
    /// replay), or is a fresh id the reviewer mints for a pair discovered
    /// during a live batch — exactly one of these two, never both: a
    /// review may add evidence to a frozen pair, or enroll a new one bound
    /// to frozen scope, but never redeclare/mutate a pair that is already
    /// predeclared.
    pub pair: String,
    #[serde(default)]
    pub declares: Option<PairDeclaration>,
    #[serde(default = "unknown_coverage")]
    pub coverage: Coverage,
    /// Tuple id of native evidence that the source's authoring generation
    /// had already exited before this reuse. A reviewer boolean alone
    /// cannot establish author-exit (design correction): this must resolve
    /// to a real lifecycle-terminal tuple (`harness_result`/
    /// `agent_lifecycle`) for the source's own generation, timestamped no
    /// later than the reuse — see `resolve_author_exit`.
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

#[derive(Debug, Clone, Serialize)]
pub struct Excluded {
    pub pair: String,
    pub reason: String,
    pub detail: String,
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

#[derive(Debug, Clone, Serialize)]
pub struct PairResult {
    pub pair: String,
    pub source: String,
    pub consumer_task: String,
    pub consumer_generation: String,
    pub batch: String,
    pub coverage_status: String,
    pub opened: bool,
    pub claimed_outcome: Option<String>,
    pub claim_evidence: Option<String>,
    pub assessed_verdict: Option<String>,
    pub assessment_evidence: Option<String>,
    pub author_terminal: bool,
    pub author_terminal_evidence: Option<String>,
    pub relayed_by_operator: bool,
    pub regression: bool,
    /// A verified, changed-work (used/adapted) effect not relayed by the
    /// operator — the unit the mechanism goal counts.
    pub counts_as_effect: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct VerifiedReuse {
    pub eligible_consumer_tasks: usize,
    pub verified_used_or_adapted_tasks: usize,
    pub rate: f64,
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

#[derive(Debug, Clone, Default, Serialize)]
pub struct DeliveryCost {
    pub task: String,
    pub attempts: usize,
    pub failed_attempts: usize,
    /// `None` when no `harness_result` was captured for this task, or when
    /// any captured attempt is missing its own `cost_usd` — a missing cost
    /// is an unknown, never a silent zero.
    pub cost_usd: Option<f64>,
    /// Sum of phase `duration_ms` across genuine work phases only —
    /// excludes the `verification` (admission/check) queue and the
    /// `attention_hold` (human wait) phase, both reported separately.
    pub active_ms: Option<i64>,
    /// `verification` phase queue-wait + run duration (admission/full-suite
    /// check time), kept apart from active work per the design doc.
    pub verification_ms: Option<i64>,
    /// Everything else queue-like: generic phase queue-wait plus
    /// `attention_hold` (human intervention wait) duration.
    pub queue_ms: Option<i64>,
    /// `None` unless a `delivery_closure` phase span was actually captured
    /// — a `merge` phase alone is not proof of an accepted delivery (a
    /// merge can still be reverted, or delivery can require further
    /// closure steps), and absence is not proof of the opposite either.
    pub accepted: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct QualitySummary {
    pub interventions: usize,
    pub incorrect_reuse: usize,
    pub regressions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub schema_version: u32,
    pub evaluator_version: u32,
    pub experiment_id: String,
    pub build: BuildIdentity,
    pub tuples_order: String,
    pub tuples_truncated: bool,
    pub eligible: usize,
    pub excluded: Vec<Excluded>,
    pub presented: usize,
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
    pub quality: QualitySummary,
    pub pairs: Vec<PairResult>,
}

/// Structural contract check for an artifact-category BBS record: the
/// design doc requires immutable `Furniture` tuples carrying
/// `schema_version: 1`. A tuple merely matching `category`+`bbs_kind` is not
/// enough to trust — that alone lets an ordinary, mutable, unversioned
/// artifact masquerade as a finding/reuse/assessment.
fn is_kind(t: &Value, category: &str, bbs_kind: &str) -> bool {
    t["category"] == category
        && t["lifecycle"] == "furniture"
        && t["payload"]["bbs_kind"] == bbs_kind
        && t["payload"]["schema_version"] == 1
}
fn is_event_kind(t: &Value, bbs_kind: &str) -> bool {
    t["category"] == "event" && t["payload"]["bbs_kind"] == bbs_kind
}
fn is_task_span(t: &Value) -> bool {
    t["category"] == "event" && t["identity"] == "task_span"
}
fn is_harness_result(t: &Value) -> bool {
    t["category"] == "event" && t["identity"] == "harness_result"
}

/// `assessment` is documented as operator-only. A tuple that matches the
/// structural shape but was not authored by `operator` cannot be trusted as
/// an authoritative verdict — reject it rather than let a forged or
/// ordinary-worker row masquerade as one.
fn is_authoritative_assessment(t: &Value) -> bool {
    is_kind(t, "artifact", ASSESSMENT) && t["payload"]["agent"] == "operator"
}

fn evidence_ids(evidence: &Value) -> Vec<String> {
    evidence
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|e| e.as_str().map(str::to_string))
        .collect()
}

/// A record's evidence is only trustworthy if every id it names resolves to
/// a real tuple actually present in this same repo's capture — otherwise it
/// is either forged, cross-repo, or simply absent from what was captured,
/// and must not be allowed to certify anything.
fn evidence_resolves_in_repo(evidence: &Value, by_id: &BTreeMap<&str, &Value>, repo: &str) -> bool {
    let ids = evidence_ids(evidence);
    !ids.is_empty()
        && ids
            .iter()
            .all(|id| by_id.get(id.as_str()).is_some_and(|t| t["scope"] == repo))
}

fn parse_rfc3339(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

/// Validates a review's `author_terminal_evidence` id against the tuple
/// capture: it must resolve to an actual lifecycle-terminal event
/// (`harness_result`/`agent_lifecycle`) authored by the source's own
/// generation, timestamped no later than the reuse it is meant to precede.
/// A bare reviewer assertion with no such tuple never establishes
/// author-exit.
fn resolve_author_exit(
    evidence_id: &str,
    by_id: &BTreeMap<&str, &Value>,
    source_spawn: &str,
    claim_created: Option<DateTime<Utc>>,
) -> std::result::Result<String, String> {
    let Some(ev) = by_id.get(evidence_id) else {
        return Err(format!(
            "author_terminal_evidence {evidence_id} is not present in the tuple capture"
        ));
    };
    let is_lifecycle_terminal = ev["category"] == "event"
        && ev["identity"]
            .as_str()
            .is_some_and(|id| LIFECYCLE_TERMINAL_IDENTITIES.contains(&id));
    if !is_lifecycle_terminal {
        return Err(format!(
            "evidence {evidence_id} is not a recognised lifecycle-terminal event"
        ));
    }
    let ev_spawn = ev["payload"]["spawn"].as_str().unwrap_or("");
    if ev_spawn.is_empty() || ev_spawn != source_spawn {
        return Err(format!(
            "evidence {evidence_id} is not the source's own authoring generation"
        ));
    }
    let ev_time = ev["created_at"].as_str().and_then(parse_rfc3339);
    match (ev_time, claim_created) {
        (Some(e), Some(c)) if e <= c => Ok(evidence_id.to_string()),
        (Some(_), Some(_)) => Err(format!(
            "evidence {evidence_id} does not precede the reuse it is meant to establish exit for"
        )),
        _ => Err(format!(
            "evidence {evidence_id} or the reuse it precedes is missing a timestamp"
        )),
    }
}

/// Computes the deterministic report. Same inputs always produce the same
/// output (stable sort keys throughout; no reliance on hash-map iteration
/// order, wall-clock "now", or randomness).
pub fn compute(manifest: &Manifest, capture: &TupleCapture, reviews: &[Review]) -> Result<Report> {
    validate_manifest(manifest)?;
    let all_pairs = validate_and_merge_pairs(manifest, reviews)?;

    let by_id: BTreeMap<&str, &Value> = capture
        .tuples
        .iter()
        .filter_map(|t| t.get("id").and_then(Value::as_str).map(|id| (id, t)))
        .collect();
    let review_by_pair: BTreeMap<&str, &Review> =
        reviews.iter().map(|r| (r.pair.as_str(), r)).collect();

    let mut reuse_by_source_task: BTreeMap<(String, String), Vec<&Value>> = BTreeMap::new();
    for t in &capture.tuples {
        if is_kind(t, "artifact", REUSE) {
            let source = t["payload"]["source"].as_str().unwrap_or("").to_string();
            let task = t["payload"]["task"].as_str().unwrap_or("").to_string();
            reuse_by_source_task
                .entry((source, task))
                .or_default()
                .push(t);
        }
    }
    // Only operator-authored, structurally valid assessments are trusted as
    // verdicts; a forged or session/ordinary-worker row is silently dropped
    // here rather than allowed to certify a claim later.
    let mut assessment_by_receipt: BTreeMap<String, Vec<(usize, &Value)>> = BTreeMap::new();
    for (idx, t) in capture.tuples.iter().enumerate() {
        if is_authoritative_assessment(t) {
            let receipt = t["payload"]["receipt"]
                .as_str()
                .unwrap_or("")
                .to_string();
            assessment_by_receipt.entry(receipt).or_default().push((idx, t));
        }
    }
    let mut prepared: BTreeSet<(String, String)> = BTreeSet::new();
    for t in &capture.tuples {
        if is_event_kind(t, EXPOSURE) {
            let spawn = t["payload"]["spawn"].as_str().unwrap_or("").to_string();
            for e in t["payload"]["entries"].as_array().into_iter().flatten() {
                let src = e["id"].as_str().or_else(|| e["source"].as_str());
                if let Some(src) = src {
                    prepared.insert((src.to_string(), spawn.clone()));
                }
            }
        }
    }
    let mut opened_set: BTreeSet<(String, String)> = BTreeSet::new();
    for t in &capture.tuples {
        if is_event_kind(t, OPEN) {
            let spawn = t["payload"]["spawn"].as_str().unwrap_or("").to_string();
            if let Some(src) = t["payload"]["source"].as_str() {
                opened_set.insert((src.to_string(), spawn));
            }
        }
    }

    let mut sorted_pairs = all_pairs;
    sorted_pairs.sort_by(|a, b| a.id.cmp(&b.id));

    let mut seen_keys: BTreeSet<(String, String, String)> = BTreeSet::new();
    let mut excluded = Vec::new();
    let mut ambiguous_assessments = Vec::new();
    let mut author_exit_unsupported = Vec::new();
    let mut unknown_coverage = Vec::new();
    let mut pairs_out = Vec::new();

    for pair in &sorted_pairs {
        let key = (
            pair.source.clone(),
            pair.consumer_task.clone(),
            pair.consumer_generation.clone(),
        );
        if !seen_keys.insert(key) {
            excluded.push(Excluded {
                pair: pair.id.clone(),
                reason: "duplicate".into(),
                detail: "identical source/consumer_task/consumer_generation already counted"
                    .into(),
            });
            continue;
        }
        let Some(source) = by_id.get(pair.source.as_str()) else {
            excluded.push(Excluded {
                pair: pair.id.clone(),
                reason: "source_not_captured".into(),
                detail: format!("source {} is not present in the tuple capture", pair.source),
            });
            continue;
        };
        let source_scope = source["scope"].as_str().unwrap_or("");
        if source_scope != pair.repo {
            excluded.push(Excluded {
                pair: pair.id.clone(),
                reason: "wrong_repo".into(),
                detail: format!("source scope {source_scope} != pair repo {}", pair.repo),
            });
            continue;
        }
        let source_spawn = source["payload"]["spawn"].as_str().unwrap_or("");
        if !source_spawn.is_empty() && source_spawn == pair.consumer_generation {
            excluded.push(Excluded {
                pair: pair.id.clone(),
                reason: "self".into(),
                detail: "source was authored by the consumer's own generation".into(),
            });
            continue;
        }
        let source_bbs_kind = source["payload"]["bbs_kind"].as_str().unwrap_or("");
        if source_bbs_kind == FINDING
            && !evidence_resolves_in_repo(&source["payload"]["evidence"], &by_id, &pair.repo)
        {
            excluded.push(Excluded {
                pair: pair.id.clone(),
                reason: "missing_evidence".into(),
                detail: "source finding's evidence does not resolve to an artifact captured \
                         in this repo"
                    .into(),
            });
            continue;
        }
        let source_created = source["created_at"].as_str().and_then(parse_rfc3339);

        // Find the reuse claim, validating scope, generation, evidence and
        // temporal order. A candidate failing any of these is not a valid
        // claim for this pair — never silently accepted.
        let mut claim: Option<&Value> = None;
        let mut claim_reject: Option<(&'static str, String)> = None;
        if let Some(candidates) = reuse_by_source_task.get(&(pair.source.clone(), pair.consumer_task.clone())) {
            for c in candidates {
                if c["scope"] != pair.repo {
                    claim_reject.get_or_insert((
                        "wrong_repo",
                        format!(
                            "reuse tuple {} is scoped to a different repo than the pair",
                            c["id"].as_str().unwrap_or("?")
                        ),
                    ));
                    continue;
                }
                let spawn = c["payload"]["spawn"].as_str().unwrap_or("");
                if spawn != pair.consumer_generation {
                    claim_reject.get_or_insert((
                        "wrong_generation",
                        format!(
                            "reuse tuple {} was written by generation {spawn}, not the pair's {}",
                            c["id"].as_str().unwrap_or("?"),
                            pair.consumer_generation
                        ),
                    ));
                    continue;
                }
                if !evidence_resolves_in_repo(&c["payload"]["evidence"], &by_id, &pair.repo) {
                    claim_reject.get_or_insert((
                        "missing_evidence",
                        format!(
                            "reuse tuple {}'s evidence does not resolve to an artifact captured \
                             in this repo",
                            c["id"].as_str().unwrap_or("?")
                        ),
                    ));
                    continue;
                }
                let claim_created = c["created_at"].as_str().and_then(parse_rfc3339);
                if let (Some(sc), Some(cc)) = (source_created, claim_created) {
                    if sc > cc {
                        claim_reject.get_or_insert((
                            "future_source",
                            format!(
                                "source {} was created after the reuse it supposedly informed",
                                pair.source
                            ),
                        ));
                        continue;
                    }
                }
                claim = Some(c);
                break;
            }
        }
        if claim.is_none() {
            if let Some((reason, detail)) = claim_reject {
                excluded.push(Excluded {
                    pair: pair.id.clone(),
                    reason: reason.into(),
                    detail,
                });
                continue;
            }
        }

        let review = review_by_pair.get(pair.id.as_str());
        let claimed_outcome = claim.and_then(|c| c["payload"]["outcome"].as_str()).map(str::to_string);
        let claim_evidence = claim.and_then(|c| c["id"].as_str()).map(str::to_string);
        let claim_created = claim.and_then(|c| c["created_at"].as_str().and_then(parse_rfc3339));

        let mut assessed_verdict: Option<String> = None;
        let mut assessment_evidence: Option<String> = None;
        if let Some(receipt) = &claim_evidence {
            if let Some(all_assessments) = assessment_by_receipt.get(receipt) {
                // Same-repo and resolvable-evidence filter applied here
                // (not at global collection time) since it depends on this
                // pair's repo; a cross-repo or evidence-less row is not a
                // trustworthy verdict for this claim.
                let assessments: Vec<(usize, &Value)> = all_assessments
                    .iter()
                    .filter(|(_, a)| {
                        a["scope"] == pair.repo
                            && evidence_resolves_in_repo(&a["payload"]["evidence"], &by_id, &pair.repo)
                    })
                    .copied()
                    .collect();
                match assessments.len() {
                    0 => {}
                    1 => {
                        let (_, a) = assessments[0];
                        assessed_verdict = a["payload"]["verdict"].as_str().map(str::to_string);
                        assessment_evidence = a["id"].as_str().map(str::to_string);
                    }
                    _ => {
                        if capture.order == Order::PersistenceSequence {
                            let (_, a) = assessments.iter().max_by_key(|(idx, _)| *idx).unwrap();
                            assessed_verdict = a["payload"]["verdict"].as_str().map(str::to_string);
                            assessment_evidence = a["id"].as_str().map(str::to_string);
                        } else {
                            ambiguous_assessments.push(AmbiguousAssessment {
                                pair: pair.id.clone(),
                                receipt: receipt.clone(),
                                reason: format!(
                                    "{} assessments for this receipt but tuple capture order is unknown; \
                                     cannot determine the current one without persistence order",
                                    assessments.len()
                                ),
                            });
                        }
                    }
                }
            }
        }

        let coverage_status = if prepared.contains(&(pair.source.clone(), pair.consumer_generation.clone())) {
            "prepared".to_string()
        } else {
            match review.map(|r| &r.coverage) {
                Some(Coverage::Prepared { .. }) => "prepared".to_string(),
                Some(Coverage::NotPrepared { .. }) => "not_prepared".to_string(),
                _ => {
                    unknown_coverage.push(pair.id.clone());
                    "unknown".to_string()
                }
            }
        };
        let opened = opened_set.contains(&(pair.source.clone(), pair.consumer_generation.clone()));

        let mut author_terminal = false;
        let mut author_terminal_evidence = None;
        if let Some(evidence_id) = review.and_then(|r| r.author_terminal_evidence.as_deref()) {
            match resolve_author_exit(evidence_id, &by_id, source_spawn, claim_created) {
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
        let relayed_by_operator = review.map(|r| r.relayed_by_operator).unwrap_or(false);
        let regression = review.map(|r| r.regression).unwrap_or(false);

        // A verified effect must never be silently certified on incomplete
        // evidence: unknown temporal order (couldn't confirm the source
        // predated the reuse) or unknown coverage (never established the
        // source was ever discoverable to this consumer) each make the
        // stigmergy story unprovable, even if the outcome/verdict fields
        // otherwise look positive.
        let temporal_order_known = source_created.is_some() && claim_created.is_some();
        let changed_work = matches!(claimed_outcome.as_deref(), Some("used") | Some("adapted"));
        let counts_as_effect = changed_work
            && assessed_verdict.as_deref() == Some("verified")
            && !relayed_by_operator
            && temporal_order_known
            && coverage_status != "unknown";

        pairs_out.push(PairResult {
            pair: pair.id.clone(),
            source: pair.source.clone(),
            consumer_task: pair.consumer_task.clone(),
            consumer_generation: pair.consumer_generation.clone(),
            batch: pair.batch.clone(),
            coverage_status,
            opened,
            claimed_outcome,
            claim_evidence,
            assessed_verdict,
            assessment_evidence,
            author_terminal,
            author_terminal_evidence,
            relayed_by_operator,
            regression,
            counts_as_effect,
        });
    }

    let eligible = sorted_pairs.len();
    let presented = pairs_out.iter().filter(|p| p.coverage_status == "prepared").count();
    let opened = pairs_out.iter().filter(|p| p.opened).count();
    let claimed = pairs_out.iter().filter(|p| p.claimed_outcome.is_some()).count();
    let assessed = pairs_out.iter().filter(|p| p.assessed_verdict.is_some()).count();

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

    // Verified reuse rate is per distinct eligible consumer task, not per
    // pair — a task with several eligible pairs is one opportunity to act.
    let mut tasks: BTreeMap<&str, Vec<&PairResult>> = BTreeMap::new();
    for p in &pairs_out {
        tasks.entry(p.consumer_task.as_str()).or_default().push(p);
    }
    let eligible_consumer_tasks = tasks.len();
    let mut verified_used_or_adapted = 0usize;
    let mut confirmed_tasks = 0usize;
    let mut rejected_tasks = 0usize;
    for ps in tasks.values() {
        if ps.iter().any(|p| {
            matches!(p.claimed_outcome.as_deref(), Some("used") | Some("adapted"))
                && p.assessed_verdict.as_deref() == Some("verified")
        }) {
            verified_used_or_adapted += 1;
        } else if ps.iter().any(|p| p.claimed_outcome.as_deref() == Some("confirmed")) {
            confirmed_tasks += 1;
        } else if ps.iter().any(|p| p.claimed_outcome.as_deref() == Some("rejected")) {
            rejected_tasks += 1;
        }
    }
    let verified_reuse = VerifiedReuse {
        eligible_consumer_tasks,
        verified_used_or_adapted_tasks: verified_used_or_adapted,
        rate: if eligible_consumer_tasks == 0 {
            0.0
        } else {
            verified_used_or_adapted as f64 / eligible_consumer_tasks as f64
        },
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
        // A truncated capture cannot certify the goal: an assessment or
        // reuse tuple outside the truncation window could exist and change
        // any of these counts. Ambiguous-order assessments already can't
        // contribute an effect at all (see the ambiguous-assessment branch
        // above), so no separate gate is needed for that case.
        goal_met: !capture.truncated
            && effect_pairs.len() >= MECHANISM_EFFECTS_REQUIRED
            && effect_batches.len() >= MECHANISM_BATCHES_REQUIRED
            && author_exit_effects >= MECHANISM_AUTHOR_EXIT_REQUIRED,
        effect_pairs,
    };

    // Cost/duration per accepted delivery, keyed by distinct consumer task.
    let mut distinct_tasks: BTreeSet<&str> = sorted_pairs.iter().map(|p| p.consumer_task.as_str()).collect();
    distinct_tasks.retain(|t| !t.is_empty());
    let mut deliveries = Vec::new();
    for task in distinct_tasks {
        let task_span_tuples: Vec<Value> = capture
            .tuples
            .iter()
            .filter(|t| is_task_span(t) && t["payload"]["task"] == task)
            .cloned()
            .collect();
        let cp = crate::critical_path::build_critical_path(task, &task_span_tuples);
        let mut active_ms: Option<i64> = None;
        let mut verification_ms: Option<i64> = None;
        let mut queue_ms: Option<i64> = None;
        if let Some(phases) = cp["phases"].as_array() {
            for phase in phases {
                let dur = phase["duration_ms"].as_i64();
                let wait = phase["queue_wait_ms"].as_i64();
                if phase["phase"] == "verification" {
                    // Admission/check queue AND its own run time both count
                    // as verification time, never as active work.
                    if let Some(d) = dur {
                        *verification_ms.get_or_insert(0) += d;
                    }
                    if let Some(w) = wait {
                        *verification_ms.get_or_insert(0) += w;
                    }
                } else if phase["phase"] == "attention_hold" {
                    // A human-attention wait is neither active work nor an
                    // admission queue — its own bucket.
                    if let Some(d) = dur {
                        *queue_ms.get_or_insert(0) += d;
                    }
                    if let Some(w) = wait {
                        *queue_ms.get_or_insert(0) += w;
                    }
                } else {
                    if let Some(d) = dur {
                        *active_ms.get_or_insert(0) += d;
                    }
                    // Generic pre-phase queueing is a wait, not active work,
                    // even for an otherwise-"active" phase.
                    if let Some(w) = wait {
                        *queue_ms.get_or_insert(0) += w;
                    }
                }
            }
        }
        let mut cost_usd_sum = 0.0;
        let mut cost_seen = false;
        let mut cost_fully_known = true;
        let mut attempts = 0usize;
        let mut failed_attempts = 0usize;
        for t in &capture.tuples {
            if is_harness_result(t) && t["payload"]["task"] == task {
                attempts += 1;
                cost_seen = true;
                match t["payload"]["cost_usd"].as_f64() {
                    Some(c) => cost_usd_sum += c,
                    None => cost_fully_known = false,
                }
                let declared_done = t["payload"]["declared_done"].as_bool().unwrap_or(false);
                let is_error = t["payload"]["is_error"].as_bool().unwrap_or(false);
                if is_error || !declared_done {
                    failed_attempts += 1;
                }
            }
        }
        // No captured harness_result, or at least one missing its own
        // cost_usd, both mean "unknown total" — never a silent zero.
        let cost_usd = (cost_seen && cost_fully_known).then_some(cost_usd_sum);
        // A `delivery_closure` span is the design's actual acceptance-
        // criteria phase; a `merge` phase alone is not proof (a merge can
        // still be reverted). Absence is unknown, not a negative.
        let accepted = cp["phases"].as_array().and_then(|phases| {
            phases
                .iter()
                .any(|p| p["phase"] == "delivery_closure")
                .then_some(true)
        });
        deliveries.push(DeliveryCost {
            task: task.to_string(),
            attempts,
            failed_attempts,
            cost_usd,
            active_ms,
            verification_ms,
            queue_ms,
            accepted,
        });
    }

    let interventions: usize = deliveries
        .iter()
        .map(|d| {
            capture
                .tuples
                .iter()
                .filter(|t| {
                    is_task_span(t)
                        && t["payload"]["task"] == d.task
                        && t["payload"]["phase"] == "attention_hold"
                })
                .count()
        })
        .sum();
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
        interventions,
        incorrect_reuse,
        regressions,
    };

    unknown_coverage.sort();
    excluded.sort_by(|a, b| a.pair.cmp(&b.pair));
    ambiguous_assessments.sort_by(|a, b| a.pair.cmp(&b.pair));
    author_exit_unsupported.sort_by(|a, b| a.pair.cmp(&b.pair));

    Ok(Report {
        schema_version: SCHEMA_VERSION,
        evaluator_version: EVALUATOR_VERSION,
        experiment_id: manifest.experiment_id.clone(),
        build: manifest.build.clone(),
        tuples_order: match capture.order {
            Order::PersistenceSequence => "persistence_sequence".into(),
            Order::Unknown => "unknown".into(),
        },
        tuples_truncated: capture.truncated,
        eligible,
        excluded,
        presented,
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
        quality,
        pairs: pairs_out,
    })
}

/// Renders a `Report` as human-readable text. JSON output should use
/// `serde_json::to_string_pretty` directly on the `Report` for a byte-stable
/// machine shape.
pub fn render(report: &Report) -> String {
    let mut out = format!(
        "## Stigmergy evidence report: {}\n\n",
        report.experiment_id
    );
    let _ = writeln!(
        out,
        "eligible={} excluded={} presented={} opened={} claimed={} assessed={}",
        report.eligible,
        report.excluded.len(),
        report.presented,
        report.opened,
        report.claimed,
        report.assessed
    );
    let _ = writeln!(
        out,
        "tuples: order={} truncated={}",
        report.tuples_order, report.tuples_truncated
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
        "verified reuse: {}/{} tasks ({:.1}%); confirmed={} rejected={}",
        report.verified_reuse.verified_used_or_adapted_tasks,
        report.verified_reuse.eligible_consumer_tasks,
        report.verified_reuse.rate * 100.0,
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
    if !report.excluded.is_empty() {
        out.push_str("excluded:\n");
        for e in &report.excluded {
            let _ = writeln!(out, "  {} [{}] {}", e.pair, e.reason, e.detail);
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
        out.push_str("deliveries:\n");
        for d in &report.deliveries {
            let _ = writeln!(
                out,
                "  {} attempts={} failed={} cost_usd={:?} active_ms={:?} verification_ms={:?} queue_ms={:?} accepted={:?}",
                d.task, d.attempts, d.failed_attempts, d.cost_usd, d.active_ms, d.verification_ms, d.queue_ms, d.accepted
            );
        }
    }
    if report.quality.interventions > 0
        || report.quality.incorrect_reuse > 0
        || !report.quality.regressions.is_empty()
    {
        let _ = writeln!(
            out,
            "quality: interventions={} incorrect_reuse={} regressions={}",
            report.quality.interventions,
            report.quality.incorrect_reuse,
            report.quality.regressions.join(", ")
        );
    }
    out
}

pub fn to_json(report: &Report) -> Value {
    serde_json::to_value(report).unwrap_or(json!({}))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(pairs: Vec<EligiblePair>) -> Manifest {
        Manifest {
            schema_version: SCHEMA_VERSION,
            experiment_id: "exp-1".into(),
            repos: vec!["repo".into()],
            window: Window::default(),
            build: BuildIdentity::default(),
            quality_criteria: vec![],
            batches: vec![Batch {
                id: "batch-1".into(),
                arm: "real".into(),
                repo: "repo".into(),
            }],
            consumer_tasks: vec![],
            eligible_pairs: pairs,
        }
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
        json!({
            "id": id,
            "category": "artifact",
            "scope": "repo",
            "identity": format!("finding-{id}"),
            "instance": "author",
            "lifecycle": "furniture",
            "created_at": created,
            "payload": {
                "schema_version": 1,
                "bbs_kind": FINDING,
                "agent": "author",
                "spawn": spawn,
                "task": "TKT-source",
                "text": "a reusable interface constraint",
                "areas": ["src/x.rs"],
                "revision": "abc123",
                "evidence": ["ev-1"],
                "limitations": "none noted"
            }
        })
    }

    fn reuse(id: &str, source: &str, task: &str, spawn: &str, outcome: &str, created: &str) -> Value {
        json!({
            "id": id,
            "category": "artifact",
            "scope": "repo",
            "identity": format!("reuse-{id}"),
            "instance": "consumer",
            "lifecycle": "furniture",
            "created_at": created,
            "payload": {
                "schema_version": 1,
                "bbs_kind": REUSE,
                "agent": "consumer",
                "spawn": spawn,
                "task": task,
                "source": source,
                "outcome": outcome,
                "text": "used the constraint directly",
                "evidence": ["ev-2"]
            }
        })
    }

    fn assessment(id: &str, receipt: &str, verdict: &str, created: &str) -> Value {
        json!({
            "id": id,
            "category": "artifact",
            "scope": "repo",
            "identity": format!("assessment-{id}"),
            "instance": "operator",
            "lifecycle": "furniture",
            "created_at": created,
            "payload": {
                "schema_version": 1,
                "bbs_kind": ASSESSMENT,
                "agent": "operator",
                "spawn": null,
                "task": "TKT-source",
                "receipt": receipt,
                "verdict": verdict,
                "reason": "matches delivered work",
                "evidence": ["ev-3"]
            }
        })
    }

    /// A `harness_result` lifecycle-terminal event for `spawn`, usable as
    /// `author_terminal_evidence`.
    fn lifecycle_terminal(id: &str, spawn: &str, task: &str, created: &str) -> Value {
        json!({
            "id": id,
            "category": "event",
            "scope": "repo",
            "identity": "harness_result",
            "instance": spawn,
            "created_at": created,
            "payload": {
                "agent": spawn,
                "spawn": spawn,
                "task": task,
                "is_error": false,
                "declared_done": true,
                "cost_usd": 0.5
            }
        })
    }

    /// Wraps test tuples into a capture, auto-injecting the generic
    /// evidence artifacts ("ev-1"/"ev-2"/"ev-3") the `finding`/`reuse`/
    /// `assessment` fixtures above reference by default, so an unrelated
    /// test (e.g. one exercising self/wrong_repo/wrong_generation
    /// exclusion) doesn't also have to construct evidence tuples just to
    /// satisfy the evidence-resolves-in-repo check.
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

    fn review_with_exit(pair_id: &str, exit_evidence: &str, relayed: bool) -> Review {
        Review {
            pair: pair_id.into(),
            declares: None,
            coverage: Coverage::Prepared {
                evidence: "exposure-1".into(),
            },
            author_terminal_evidence: Some(exit_evidence.into()),
            relayed_by_operator: relayed,
            regression: false,
            notes: None,
        }
    }

    fn review_no_exit(pair_id: &str) -> Review {
        Review {
            pair: pair_id.into(),
            declares: None,
            coverage: Coverage::Prepared {
                evidence: "exposure-1".into(),
            },
            author_terminal_evidence: None,
            relayed_by_operator: false,
            regression: false,
            notes: None,
        }
    }

    #[test]
    fn verified_used_effect_counts_and_is_deterministic() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            reuse("r1", "src-1", "TKT-1", "gen-1", "used", "2026-01-02T00:00:00Z"),
            assessment("a1", "r1", "verified", "2026-01-03T00:00:00Z"),
            lifecycle_terminal("hr1", "author-gen", "TKT-source", "2026-01-01T12:00:00Z"),
        ];
        let c = capture(tuples, Order::Unknown);
        let reviews = vec![review_with_exit("p1", "hr1", false)];
        let r1 = compute(&m, &c, &reviews).unwrap();
        assert_eq!(r1.claimed, 1);
        assert_eq!(r1.assessed, 1);
        assert_eq!(r1.mechanism.effects, 1);
        assert_eq!(r1.author_exit_reuse, vec!["p1".to_string()]);
        assert!(r1.author_exit_unsupported.is_empty());
        assert_eq!(r1.verified_reuse.verified_used_or_adapted_tasks, 1);
        assert_eq!(r1.verified_reuse.eligible_consumer_tasks, 1);
        assert!(r1.excluded.is_empty());

        let r2 = compute(&m, &c, &reviews).unwrap();
        assert_eq!(
            serde_json::to_string(&r1).unwrap(),
            serde_json::to_string(&r2).unwrap(),
            "same inputs must produce byte-identical output"
        );
    }

    #[test]
    fn author_exit_evidence_missing_from_capture_is_not_credited() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            reuse("r1", "src-1", "TKT-1", "gen-1", "used", "2026-01-02T00:00:00Z"),
            assessment("a1", "r1", "verified", "2026-01-03T00:00:00Z"),
        ];
        let c = capture(tuples, Order::Unknown);
        // Reviewer claims author-exit but points at a tuple that was never
        // captured: a bare assertion, no native evidence.
        let reviews = vec![review_with_exit("p1", "nonexistent-tuple", false)];
        let r = compute(&m, &c, &reviews).unwrap();
        assert_eq!(r.mechanism.effects, 1);
        assert!(r.author_exit_reuse.is_empty());
        assert_eq!(r.author_exit_unsupported.len(), 1);
        assert_eq!(r.author_exit_unsupported[0].pair, "p1");
    }

    #[test]
    fn author_exit_evidence_for_wrong_generation_is_not_credited() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            reuse("r1", "src-1", "TKT-1", "gen-1", "used", "2026-01-02T00:00:00Z"),
            assessment("a1", "r1", "verified", "2026-01-03T00:00:00Z"),
            // Terminal event for a DIFFERENT generation than the source's author.
            lifecycle_terminal("hr1", "someone-else", "TKT-other", "2026-01-01T12:00:00Z"),
        ];
        let c = capture(tuples, Order::Unknown);
        let reviews = vec![review_with_exit("p1", "hr1", false)];
        let r = compute(&m, &c, &reviews).unwrap();
        assert!(r.author_exit_reuse.is_empty());
        assert_eq!(r.author_exit_unsupported.len(), 1);
    }

    #[test]
    fn author_exit_evidence_after_reuse_is_not_credited() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            reuse("r1", "src-1", "TKT-1", "gen-1", "used", "2026-01-02T00:00:00Z"),
            assessment("a1", "r1", "verified", "2026-01-03T00:00:00Z"),
            // Exit happens AFTER the reuse it's supposed to precede.
            lifecycle_terminal("hr1", "author-gen", "TKT-source", "2026-01-05T00:00:00Z"),
        ];
        let c = capture(tuples, Order::Unknown);
        let reviews = vec![review_with_exit("p1", "hr1", false)];
        let r = compute(&m, &c, &reviews).unwrap();
        assert!(r.author_exit_reuse.is_empty());
        assert_eq!(r.author_exit_unsupported.len(), 1);
    }

    #[test]
    fn confirmation_alone_never_counts_as_a_changed_work_effect() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            reuse("r1", "src-1", "TKT-1", "gen-1", "confirmed", "2026-01-02T00:00:00Z"),
            assessment("a1", "r1", "verified", "2026-01-03T00:00:00Z"),
        ];
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.mechanism.effects, 0);
        assert_eq!(r.verified_reuse.verified_used_or_adapted_tasks, 0);
        assert_eq!(r.verified_reuse.confirmed_tasks, 1);
        assert_eq!(r.outcome_classes.confirmed, 1);
    }

    #[test]
    fn duplicate_pair_excluded_and_counted_once() {
        let m = manifest(vec![
            pair("p1", "src-1", "TKT-1", "gen-1"),
            pair("p2", "src-1", "TKT-1", "gen-1"),
        ]);
        let tuples = vec![finding("src-1", "author-gen", "2026-01-01T00:00:00Z")];
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.excluded.len(), 1);
        assert_eq!(r.excluded[0].pair, "p2");
        assert_eq!(r.excluded[0].reason, "duplicate");
    }

    #[test]
    fn self_use_excluded() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "author-gen")]);
        let tuples = vec![finding("src-1", "author-gen", "2026-01-01T00:00:00Z")];
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.excluded[0].reason, "self");
    }

    #[test]
    fn wrong_repo_excluded() {
        let mut m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        m.eligible_pairs[0].repo = "other-repo".into();
        let tuples = vec![finding("src-1", "author-gen", "2026-01-01T00:00:00Z")];
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.excluded[0].reason, "wrong_repo");
    }

    #[test]
    fn missing_evidence_excluded() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let mut f = finding("src-1", "author-gen", "2026-01-01T00:00:00Z");
        f["payload"]["evidence"] = json!([]);
        let c = capture(vec![f], Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.excluded[0].reason, "missing_evidence");
    }

    #[test]
    fn wrong_generation_claim_excluded() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            // Written by a different generation than the frozen pair names.
            reuse("r1", "src-1", "TKT-1", "gen-OTHER", "used", "2026-01-02T00:00:00Z"),
        ];
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.excluded[0].reason, "wrong_generation");
        assert_eq!(r.claimed, 0);
    }

    #[test]
    fn future_source_excluded() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let tuples = vec![
            // Source postdates the reuse it supposedly informed.
            finding("src-1", "author-gen", "2026-02-01T00:00:00Z"),
            reuse("r1", "src-1", "TKT-1", "gen-1", "used", "2026-01-02T00:00:00Z"),
        ];
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.excluded[0].reason, "future_source");
    }

    #[test]
    fn unsupported_assessment_never_counts_toward_verified_reuse() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            reuse("r1", "src-1", "TKT-1", "gen-1", "used", "2026-01-02T00:00:00Z"),
            assessment("a1", "r1", "unsupported", "2026-01-03T00:00:00Z"),
        ];
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.claimed, 1);
        assert_eq!(r.assessed, 1);
        assert_eq!(r.outcome_classes.unsupported, 1);
        assert_eq!(r.mechanism.effects, 0);
        assert_eq!(r.verified_reuse.verified_used_or_adapted_tasks, 0);
    }

    #[test]
    fn operator_relayed_effect_excluded_from_mechanism_goal() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            reuse("r1", "src-1", "TKT-1", "gen-1", "used", "2026-01-02T00:00:00Z"),
            assessment("a1", "r1", "verified", "2026-01-03T00:00:00Z"),
        ];
        let c = capture(tuples, Order::Unknown);
        let reviews = vec![review_with_exit("p1", "nonexistent", true)];
        let r = compute(&m, &c, &reviews).unwrap();
        assert_eq!(r.mechanism.effects, 0);
        assert!(!r.mechanism.goal_met);
    }

    #[test]
    fn mechanism_goal_needs_three_effects_two_batches_one_author_exit() {
        let mut m = manifest(vec![]);
        m.batches.push(Batch {
            id: "batch-2".into(),
            arm: "real".into(),
            repo: "repo".into(),
        });
        m.eligible_pairs = vec![
            EligiblePair {
                id: "p1".into(),
                source: "src-1".into(),
                consumer_task: "TKT-1".into(),
                consumer_generation: "gen-1".into(),
                repo: "repo".into(),
                batch: "batch-1".into(),
            },
            EligiblePair {
                id: "p2".into(),
                source: "src-2".into(),
                consumer_task: "TKT-2".into(),
                consumer_generation: "gen-2".into(),
                repo: "repo".into(),
                batch: "batch-1".into(),
            },
            EligiblePair {
                id: "p3".into(),
                source: "src-3".into(),
                consumer_task: "TKT-3".into(),
                consumer_generation: "gen-3".into(),
                repo: "repo".into(),
                batch: "batch-2".into(),
            },
        ];
        let mut tuples = vec![];
        for (src, task, gen) in [("src-1", "TKT-1", "gen-1"), ("src-2", "TKT-2", "gen-2"), ("src-3", "TKT-3", "gen-3")] {
            tuples.push(finding(src, "author-gen", "2026-01-01T00:00:00Z"));
            tuples.push(reuse(&format!("r-{src}"), src, task, gen, "adapted", "2026-01-02T00:00:00Z"));
            tuples.push(assessment(&format!("a-{src}"), &format!("r-{src}"), "verified", "2026-01-03T00:00:00Z"));
        }
        tuples.push(lifecycle_terminal("hr1", "author-gen", "TKT-source", "2026-01-01T12:00:00Z"));
        let c = capture(tuples, Order::Unknown);
        let reviews = vec![
            review_no_exit("p1"),
            review_no_exit("p2"),
            review_with_exit("p3", "hr1", false),
        ];
        let r = compute(&m, &c, &reviews).unwrap();
        assert_eq!(r.mechanism.effects, 3);
        assert_eq!(r.mechanism.batches, 2);
        assert_eq!(r.mechanism.author_exit_effects, 1);
        assert!(r.mechanism.goal_met);
    }

    #[test]
    fn ambiguous_assessment_order_is_not_silently_resolved() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            reuse("r1", "src-1", "TKT-1", "gen-1", "used", "2026-01-02T00:00:00Z"),
            // Two revisions of the same assessment, order unknown: a naive
            // implementation might pick "last in array" or "highest ULID"
            // and silently manufacture a verdict. Neither is legitimate
            // without real persistence order.
            assessment("a1", "r1", "unsupported", "2026-01-03T00:00:00Z"),
            assessment("a2", "r1", "verified", "2026-01-04T00:00:00Z"),
        ];
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.assessed, 0);
        assert_eq!(r.ambiguous_assessments.len(), 1);
        assert_eq!(r.ambiguous_assessments[0].pair, "p1");
        assert_eq!(r.mechanism.effects, 0);
    }

    #[test]
    fn persistence_sequence_order_resolves_supersession_by_position() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            reuse("r1", "src-1", "TKT-1", "gen-1", "used", "2026-01-02T00:00:00Z"),
            assessment("a1", "r1", "incorrect", "2026-01-03T00:00:00Z"),
            assessment("a2", "r1", "verified", "2026-01-04T00:00:00Z"),
        ];
        let c = capture(tuples, Order::PersistenceSequence);
        let r = compute(&m, &c, &[]).unwrap();
        assert!(r.ambiguous_assessments.is_empty());
        assert_eq!(r.pairs[0].assessed_verdict.as_deref(), Some("verified"));
    }

    #[test]
    fn unknown_coverage_listed_separately_and_excluded_from_presented() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let tuples = vec![finding("src-1", "author-gen", "2026-01-01T00:00:00Z")];
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.presented, 0);
        assert_eq!(r.unknown_coverage, vec!["p1".to_string()]);
    }

    #[test]
    fn wraps_bare_scan_object_and_rejects_missing_tuples_field() {
        let scan_output = json!({"tuples": [finding("src-1", "author-gen", "2026-01-01T00:00:00Z")], "truncated": true});
        let parsed = parse_tuple_capture(&scan_output).unwrap();
        assert_eq!(parsed.order, Order::Unknown);
        assert!(parsed.truncated);
        assert_eq!(parsed.tuples.len(), 1);

        let bad = json!({"not_tuples": []});
        assert!(parse_tuple_capture(&bad).is_err());
    }

    #[test]
    fn envelope_with_persistence_sequence_order_is_honored() {
        let envelope = json!({
            "schema_version": 1,
            "order": "persistence_sequence",
            "tuples": [],
        });
        let parsed = parse_tuple_capture(&envelope).unwrap();
        assert_eq!(parsed.order, Order::PersistenceSequence);
    }

    #[test]
    fn rejects_wrong_schema_version() {
        let mut m = manifest(vec![]);
        m.schema_version = 99;
        let c = capture(vec![], Order::Unknown);
        assert!(compute(&m, &c, &[]).is_err());
    }

    #[test]
    fn rejects_review_for_pair_not_in_manifest_or_declared() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let c = capture(vec![], Order::Unknown);
        let reviews = vec![review_no_exit("not-a-real-pair")];
        assert!(compute(&m, &c, &reviews).is_err());
    }

    #[test]
    fn rejects_review_that_redeclares_a_predeclared_pair() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let c = capture(vec![], Order::Unknown);
        let mut r = review_no_exit("p1");
        r.declares = Some(PairDeclaration {
            source: "src-2".into(),
            consumer_task: "TKT-1".into(),
            consumer_generation: "gen-1".into(),
            repo: "repo".into(),
            batch: "batch-1".into(),
        });
        assert!(compute(&m, &c, &[r]).is_err());
    }

    #[test]
    fn live_enrollment_mints_a_pair_bound_to_frozen_scope() {
        // No predeclared pairs at all — everything discovered live, as a
        // real batch would: only batches + consumer_tasks are frozen ahead
        // of time.
        let mut m = manifest(vec![]);
        m.consumer_tasks.push(ConsumerTaskScope {
            task: "TKT-1".into(),
            repo: "repo".into(),
            batch: "batch-1".into(),
        });
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            reuse("r1", "src-1", "TKT-1", "gen-1", "used", "2026-01-02T00:00:00Z"),
            assessment("a1", "r1", "verified", "2026-01-03T00:00:00Z"),
        ];
        let c = capture(tuples, Order::Unknown);
        let mut r = review_no_exit("p1");
        r.declares = Some(PairDeclaration {
            source: "src-1".into(),
            consumer_task: "TKT-1".into(),
            consumer_generation: "gen-1".into(),
            repo: "repo".into(),
            batch: "batch-1".into(),
        });
        let report = compute(&m, &c, &[r]).unwrap();
        assert_eq!(report.eligible, 1);
        assert_eq!(report.claimed, 1);
        assert_eq!(report.mechanism.effects, 1);
    }

    #[test]
    fn live_enrollment_rejects_task_not_frozen_in_consumer_tasks() {
        let m = manifest(vec![]); // no consumer_tasks frozen
        let c = capture(vec![], Order::Unknown);
        let mut r = review_no_exit("p1");
        r.declares = Some(PairDeclaration {
            source: "src-1".into(),
            consumer_task: "TKT-1".into(),
            consumer_generation: "gen-1".into(),
            repo: "repo".into(),
            batch: "batch-1".into(),
        });
        assert!(compute(&m, &c, &[r]).is_err());
    }

    #[test]
    fn cost_and_duration_aggregate_per_task_including_failures() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let mut tuples = vec![finding("src-1", "author-gen", "2026-01-01T00:00:00Z")];
        tuples.push(json!({
            "id": "hr1", "category": "event", "scope": "repo", "identity": "harness_result",
            "instance": "gen-1", "created_at": "2026-01-01T01:00:00Z",
            "payload": {"task": "TKT-1", "cost_usd": 1.5, "declared_done": false, "is_error": true}
        }));
        tuples.push(json!({
            "id": "hr2", "category": "event", "scope": "repo", "identity": "harness_result",
            "instance": "gen-1b", "created_at": "2026-01-01T02:00:00Z",
            "payload": {"task": "TKT-1", "cost_usd": 2.25, "declared_done": true, "is_error": false}
        }));
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        let d = &r.deliveries[0];
        assert_eq!(d.task, "TKT-1");
        assert_eq!(d.attempts, 2);
        assert_eq!(d.failed_attempts, 1);
        assert!((d.cost_usd.unwrap() - 3.75).abs() < 1e-9);
    }

    #[test]
    fn missing_cost_usd_field_on_any_attempt_is_unknown_not_zero() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let mut tuples = vec![finding("src-1", "author-gen", "2026-01-01T00:00:00Z")];
        tuples.push(json!({
            "id": "hr1", "category": "event", "scope": "repo", "identity": "harness_result",
            "instance": "gen-1", "created_at": "2026-01-01T01:00:00Z",
            "payload": {"task": "TKT-1", "declared_done": true, "is_error": false}
        }));
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.deliveries[0].cost_usd, None);
    }

    #[test]
    fn no_harness_result_leaves_cost_and_accepted_unknown() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let tuples = vec![finding("src-1", "author-gen", "2026-01-01T00:00:00Z")];
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        let d = &r.deliveries[0];
        assert_eq!(d.cost_usd, None);
        assert_eq!(d.accepted, None);
        assert_eq!(d.attempts, 0);
    }

    #[test]
    fn merge_phase_alone_does_not_prove_accepted_delivery() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let mut tuples = vec![finding("src-1", "author-gen", "2026-01-01T00:00:00Z")];
        tuples.push(json!({
            "id": "ts1", "category": "event", "scope": "repo", "identity": "task_span",
            "instance": "castle", "created_at": "2026-01-01T01:00:00Z",
            "payload": {"task": "TKT-1", "phase": "merge", "attempt": 1,
                        "started_at": "2026-01-01T00:00:00Z", "ended_at": "2026-01-01T00:05:00Z",
                        "duration_ms": 300_000}
        }));
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.deliveries[0].accepted, None);
    }

    #[test]
    fn delivery_closure_phase_proves_accepted_delivery() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let mut tuples = vec![finding("src-1", "author-gen", "2026-01-01T00:00:00Z")];
        tuples.push(json!({
            "id": "ts1", "category": "event", "scope": "repo", "identity": "task_span",
            "instance": "castle", "created_at": "2026-01-01T01:00:00Z",
            "payload": {"task": "TKT-1", "phase": "delivery_closure", "attempt": 1,
                        "started_at": "2026-01-01T00:00:00Z", "ended_at": "2026-01-01T00:05:00Z",
                        "duration_ms": 300_000}
        }));
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.deliveries[0].accepted, Some(true));
    }

    #[test]
    fn attention_hold_and_queue_wait_excluded_from_active_time() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let mut tuples = vec![finding("src-1", "author-gen", "2026-01-01T00:00:00Z")];
        tuples.push(json!({
            "id": "ts1", "category": "event", "scope": "repo", "identity": "task_span",
            "instance": "castle", "created_at": "2026-01-01T01:00:00Z",
            "payload": {"task": "TKT-1", "phase": "completed", "attempt": 1,
                        "queued_at": "2026-01-01T00:00:00Z", "started_at": "2026-01-01T00:01:00Z",
                        "ended_at": "2026-01-01T00:02:00Z",
                        "queue_wait_ms": 60_000, "duration_ms": 60_000}
        }));
        tuples.push(json!({
            "id": "ts2", "category": "event", "scope": "repo", "identity": "task_span",
            "instance": "castle", "created_at": "2026-01-01T01:00:00Z",
            "payload": {"task": "TKT-1", "phase": "attention_hold", "attempt": 1,
                        "started_at": "2026-01-01T00:02:00Z", "ended_at": "2026-01-01T01:02:00Z",
                        "duration_ms": 3_600_000}
        }));
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        let d = &r.deliveries[0];
        assert_eq!(d.active_ms, Some(60_000));
        assert_eq!(d.queue_ms, Some(60_000 + 3_600_000));
    }

    #[test]
    fn forged_assessment_not_authored_by_operator_is_not_trusted() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let mut a = assessment("a1", "r1", "verified", "2026-01-03T00:00:00Z");
        a["payload"]["agent"] = json!("some-rat");
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            reuse("r1", "src-1", "TKT-1", "gen-1", "used", "2026-01-02T00:00:00Z"),
            a,
        ];
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.assessed, 0);
        assert_eq!(r.mechanism.effects, 0);
    }

    #[test]
    fn evidence_referencing_a_different_repo_does_not_resolve() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let mut f = finding("src-1", "author-gen", "2026-01-01T00:00:00Z");
        f["payload"]["evidence"] = json!(["ev-foreign"]);
        let foreign = json!({
            "id": "ev-foreign", "category": "artifact", "scope": "other-repo",
            "identity": "x", "instance": "author", "created_at": "2026-01-01T00:00:00Z",
            "payload": {}
        });
        let c = capture(vec![f, foreign], Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.excluded[0].reason, "missing_evidence");
    }

    #[test]
    fn evidence_referencing_a_nonexistent_artifact_does_not_resolve() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let mut f = finding("src-1", "author-gen", "2026-01-01T00:00:00Z");
        f["payload"]["evidence"] = json!(["never-captured"]);
        let c = capture(vec![f], Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.excluded[0].reason, "missing_evidence");
    }

    #[test]
    fn missing_source_timestamp_prevents_counting_a_verified_effect() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let mut f = finding("src-1", "author-gen", "2026-01-01T00:00:00Z");
        f.as_object_mut().unwrap().remove("created_at");
        let tuples = vec![
            f,
            reuse("r1", "src-1", "TKT-1", "gen-1", "used", "2026-01-02T00:00:00Z"),
            assessment("a1", "r1", "verified", "2026-01-03T00:00:00Z"),
        ];
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        // Still legitimately claimed/assessed (transparency), just not
        // credited as a mechanism-goal effect without known temporal order.
        assert_eq!(r.claimed, 1);
        assert_eq!(r.assessed, 1);
        assert_eq!(r.mechanism.effects, 0);
    }

    #[test]
    fn unknown_coverage_prevents_counting_a_verified_effect() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            reuse("r1", "src-1", "TKT-1", "gen-1", "used", "2026-01-02T00:00:00Z"),
            assessment("a1", "r1", "verified", "2026-01-03T00:00:00Z"),
        ];
        let c = capture(tuples, Order::Unknown);
        // No review at all: coverage_status stays "unknown".
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.pairs[0].coverage_status, "unknown");
        assert_eq!(r.mechanism.effects, 0);
    }

    #[test]
    fn truncated_capture_cannot_certify_the_mechanism_goal() {
        let mut m = manifest(vec![]);
        m.batches.push(Batch {
            id: "batch-2".into(),
            arm: "real".into(),
            repo: "repo".into(),
        });
        m.eligible_pairs = vec![
            pair("p1", "src-1", "TKT-1", "gen-1"),
            pair("p2", "src-2", "TKT-2", "gen-2"),
            EligiblePair {
                id: "p3".into(),
                source: "src-3".into(),
                consumer_task: "TKT-3".into(),
                consumer_generation: "gen-3".into(),
                repo: "repo".into(),
                batch: "batch-2".into(),
            },
        ];
        let mut tuples = vec![];
        for (src, task, gen) in [("src-1", "TKT-1", "gen-1"), ("src-2", "TKT-2", "gen-2"), ("src-3", "TKT-3", "gen-3")] {
            tuples.push(finding(src, "author-gen", "2026-01-01T00:00:00Z"));
            tuples.push(reuse(&format!("r-{src}"), src, task, gen, "adapted", "2026-01-02T00:00:00Z"));
            tuples.push(assessment(&format!("a-{src}"), &format!("r-{src}"), "verified", "2026-01-03T00:00:00Z"));
        }
        tuples.push(lifecycle_terminal("hr1", "author-gen", "TKT-source", "2026-01-01T12:00:00Z"));
        let reviews = vec![
            review_no_exit("p1"),
            review_no_exit("p2"),
            review_with_exit("p3", "hr1", false),
        ];
        let mut c = capture(tuples, Order::Unknown);
        c.truncated = true;
        let r = compute(&m, &c, &reviews).unwrap();
        assert_eq!(r.mechanism.effects, 3);
        assert!(
            !r.mechanism.goal_met,
            "a truncated capture must never certify the mechanism goal even when raw counts satisfy it"
        );
    }
}

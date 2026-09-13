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
///
/// * 1 — initial S3 evaluator (`TKT-tavik-kifos-lozuf`).
/// * 2 — `TKT-buruk-parut-zisoh` (slice A): native record identity binding,
///   evidence resolution, repo/window scope, and opportunity-denominator
///   retention. Author-exit and per-delivery cost were reported as UNSUPPORTED.
/// * 3 — `TKT-bonik-vuruv-mivuh` (slice B): author-exit derived from a physical
///   exit observation bound to an exact `(spawn, session)`, and per-delivery
///   cost derived per proven provider segment with explicit finality. The two
///   derivations slice A withheld are now real, so a version-2 and a version-3
///   report of the same inputs are NOT comparable on those fields.
pub const EVALUATOR_VERSION: u32 = 3;

/// Frozen per the design doc: "three verified used/adapted effects across at
/// least two batches, at least one after its author exited". Not
/// manifest-configurable — a manifest cannot raise or lower its own bar.
pub const MECHANISM_EFFECTS_REQUIRED: usize = 3;
pub const MECHANISM_BATCHES_REQUIRED: usize = 2;
pub const MECHANISM_AUTHOR_EXIT_REQUIRED: usize = 1;

// `bbs_kind` literals from the design doc's payload contract table.
const FINDING: &str = "finding";
const ANSWER: &str = "answer";
const REUSE: &str = "reuse";
const ASSESSMENT: &str = "assessment";
const EXPOSURE: &str = "exposure";
const OPEN: &str = "open";
// The two native observation kinds S2's published contract adds
// (docs/2026-09-13-s2-native-observation-and-export-contract.md, consumed via
// BBS artifact 01M2CF3RJHX58HH085WKZBJD9A).
const AGENT_EXIT: &str = "agent_exit";
const AGENT_FINAL_USAGE: &str = "agent_final_usage";

/// The identity each BBS record kind is actually minted with by
/// `crates/rk-daemon/src/bbs.rs`. A record whose identity does not carry its
/// kind's prefix was not written by the BBS write path.
///
/// Read off the producers rather than off
/// `rk_core::bbs::RESERVED_IDENTITY_PREFIXES`: the digest-suffixed kinds match
/// that list, but `record_open` mints the fixed identity `"bbs-open"` with no
/// trailing dash. Acceptance here has to match what is actually emitted.
fn reserved_prefix(bbs_kind: &str) -> Option<&'static str> {
    match bbs_kind {
        FINDING => Some("bbs-finding-"),
        ANSWER => Some("bbs-answer-"),
        REUSE => Some("bbs-reuse-"),
        ASSESSMENT => Some("bbs-assessment-"),
        EXPOSURE => Some("bbs-exposure-"),
        OPEN => Some("bbs-open"),
        AGENT_EXIT => Some("bbs-agent-exit"),
        AGENT_FINAL_USAGE => Some("bbs-agent-final-usage"),
        _ => None,
    }
}

/// Native events that prove an agent generation actually started a process, so
/// a prepared exposure can be joined to a LAUNCHED consumer rather than
/// counted for a spawn that never ran. `spawn` is additive on these events
/// (S2's `48fb2da`); a `harness_result` also proves the generation ran.
const LAUNCH_EVENT_IDENTITIES: [&str; 2] = ["agent_spawned", "agent_respawned"];

/// `AgentState` values that mean this launch produced its *last* provider
/// result. A `paused` result may be followed by more model usage and then a
/// budget kill with no further result, which makes the reported total a partial
/// amount rather than a final cost for the launch.
const TERMINAL_USAGE_STATES: [&str; 3] = ["completed", "failed", "stopped"];

/// The only `cost_basis` a *provider-reported* total may carry. The daemon's
/// own priced-increment fallback is an estimate of an estimate: it is reported
/// in its own field and never pooled with this one.
const PROVIDER_COST_BASIS: &str = "provider_reported_segment_total";
const DAEMON_COST_BASIS: &str = "daemon_priced_increments";

/// Still reported as unknown, and still not this ticket's to derive: an
/// `attention_hold` span count is a LOWER BOUND on operator interventions, not
/// a total. Reviewed task-scoped annotations are the parent's scope.
const INTERVENTIONS_LOWER_BOUND: &str =
    "lower bound: attention_hold spans only cover waits the daemon recorded; an operator \
     intervention that left no span is not counted here. Reviewed annotations are \
     TKT-nonub-pugar-pilid's scope.";

/// No native observation distinguishes model-active time from a process sitting
/// paused awaiting verification or the operator, so active work stays an
/// explicit unknown rather than being aliased to process lifetime or to a
/// phase-duration sum.
const ACTIVE_WORK_UNKNOWN: &str =
    "unknown: launch-to-exit is process lifetime, and no native observation distinguishes \
     model-active time from a process paused awaiting verification or the operator.";

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
    /// Frozen measurement window, ENFORCED against every native observation.
    /// Source findings/artifacts are deliberately NOT window-filtered: an
    /// older source that a batch consumer reused is exactly what the
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
#[serde(deny_unknown_fields)]
pub struct Window {
    #[serde(default)]
    pub since: Option<DateTime<Utc>>,
    #[serde(default)]
    pub until: Option<DateTime<Utc>>,
}

/// Where an observation falls relative to the frozen window. An observation
/// with no parsable timestamp is `Undated` — never silently treated as either
/// inside or outside, because that is how a missing observation becomes a
/// manufactured zero.
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
///
/// Populated one of two ways: predeclared directly in
/// `Manifest.eligible_pairs` (fixture/replay, pair identities already
/// known), or minted by a `Review.declares` entry bound to an
/// already-frozen `ConsumerTaskScope` (a live batch, where the source id
/// and consumer generation only exist once the batch has actually run).
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
    // Batch id -> its declared repo, so a pair can be checked against the repo
    // its batch actually belongs to and not merely against a known batch id.
    let mut batch_repos: BTreeMap<&str, &str> = BTreeMap::new();
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
        if !repo_in_scope(&b.repo) {
            bail!(
                "batch {} declares repo {} which is not in manifest.repos {:?}",
                b.id,
                b.repo,
                m.repos
            );
        }
        batch_repos.insert(b.id.as_str(), b.repo.as_str());
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
        // A fixture pair naming a known batch id is not enough: the pair's
        // repo has to BE that batch's repo, or one batch silently aggregates
        // two repositories' evidence.
        match batch_repos.get(p.batch.as_str()) {
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
#[serde(deny_unknown_fields)]
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

/// One occurrence bound to real evidence already in the capture — never a
/// bare assertion — plus the operator's stated reason it counts.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnnotatedEvidence {
    pub evidence: String,
    pub reason: String,
}

/// A bounded, task-scoped operator judgment of repeated investigation,
/// rework, or a real intervention that daemon telemetry alone does not
/// establish end-to-end (design doc: "repeated investigations and rework,
/// with reviewed evidence" — "do not turn an agent's estimate of time saved
/// into measured savings"). `deny_unknown_fields` on both this and
/// `AnnotatedEvidence` so no invented duration or savings figure can be
/// smuggled in through an unknown key: only a COUNT of evidenced occurrences
/// is ever reported, and each one is bound to a resolvable capture record,
/// never trusted as a bare reviewer claim. Distinct from `Review`, which is
/// scoped to one source/consumer pair rather than one task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewedAnnotation {
    pub task: String,
    pub repo: String,
    #[serde(default)]
    pub repeated_investigations: Vec<AnnotatedEvidence>,
    #[serde(default)]
    pub rework: Vec<AnnotatedEvidence>,
    #[serde(default)]
    pub interventions: Vec<AnnotatedEvidence>,
}

/// A reviewed annotation may only speak about a task already frozen into
/// delivery scope (`frozen_task_scope`) — the same rule a `Review.declares`
/// pair must satisfy — and at most once per (task, repo): a retrospective
/// annotation cannot introduce new scope or be silently duplicated to
/// inflate a count.
fn validate_reviewed_annotations(manifest: &Manifest, annotations: &[ReviewedAnnotation]) -> Result<()> {
    let frozen: BTreeSet<(String, String)> = frozen_task_scope(manifest)
        .into_iter()
        .map(|s| (s.task, s.repo))
        .collect();
    let mut seen: BTreeSet<(String, String)> = BTreeSet::new();
    for a in annotations {
        if !seen.insert((a.task.clone(), a.repo.clone())) {
            bail!(
                "duplicate reviewed annotation for task {} in repo {}",
                a.task,
                a.repo
            );
        }
        if !frozen.contains(&(a.task.clone(), a.repo.clone())) {
            bail!(
                "reviewed annotation for task {} in repo {} is not in the frozen delivery scope \
                 (must already appear in manifest.consumer_tasks or an eligible_pairs \
                 consumer_task)",
                a.task,
                a.repo
            );
        }
    }
    Ok(())
}

/// Parses the `--reviews` file as either a bare `Review` array (the existing,
/// still-supported shape — `reviewed_annotations` defaults to empty) or an
/// object `{"reviews": [...], "reviewed_annotations": [...]}`. Mirrors
/// `parse_tuple_capture`'s bare-array-or-envelope pattern so the file format
/// grows without breaking either an existing caller or the CLI acceptance
/// tests that write a bare array today.
pub fn parse_reviews_file(raw: &Value) -> Result<(Vec<Review>, Vec<ReviewedAnnotation>)> {
    if raw.is_array() {
        let reviews: Vec<Review> = serde_json::from_value(raw.clone())
            .context("reviews file does not match the expected schema")?;
        return Ok((reviews, Vec::new()));
    }
    if raw.is_object() {
        let reviews: Vec<Review> = match raw.get("reviews") {
            Some(v) => serde_json::from_value(v.clone())
                .context("reviews file's `reviews` field does not match the expected schema")?,
            None => Vec::new(),
        };
        let reviewed_annotations: Vec<ReviewedAnnotation> = match raw.get("reviewed_annotations") {
            Some(v) => serde_json::from_value(v.clone()).context(
                "reviews file's `reviewed_annotations` field does not match the expected schema",
            )?,
            None => Vec::new(),
        };
        return Ok((reviews, reviewed_annotations));
    }
    bail!(
        "reviews file must be a JSON array (bare `Review` list) or an object with `reviews`/\
         `reviewed_annotations` fields"
    )
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
/// here any more: an invalid *claim*. A malformed or foreign receipt removes
/// the claim, never the opportunity — see [`RejectedClaim`].
#[derive(Debug, Clone, Serialize)]
pub struct Excluded {
    pub pair: String,
    pub reason: String,
    pub detail: String,
}

/// A receipt refused for this pair. The pair itself stays in the eligible
/// denominator with no claimed outcome, so a bad claim cannot erase a real
/// opportunity from the discovery or verified-reuse rates.
#[derive(Debug, Clone, Serialize)]
pub struct RejectedClaim {
    pub pair: String,
    pub record: String,
    pub reason: String,
    pub detail: String,
}

/// A native record dropped with a reason. Dropping these silently is what let
/// an unversioned artifact, a castle-authored row read as a consumer, or an
/// unattributed legacy record certify a result.
#[derive(Debug, Clone, Serialize)]
pub struct InvalidRecord {
    pub record: String,
    pub kind: String,
    pub reason: String,
}

/// What the capture actually contained, so a scoping or window mistake is
/// visible instead of silently shrinking every metric.
#[derive(Debug, Clone, Default, Serialize)]
pub struct CaptureSummary {
    pub tuples: usize,
    pub observations_in_window: usize,
    pub observations_out_of_window: usize,
    pub observations_undated: usize,
    pub observations_out_of_scope_repo: usize,
}

/// The discovery denominator, reported explicitly rather than left implicit in
/// a single rate. `rate` is `None` — not `0.0` — when nothing has known
/// coverage: an absent denominator is not a zero numerator.
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
    /// consumer generation. A selection prepared for a spawn that never ran is
    /// neither a discovery success nor a discovery failure, so it is kept out
    /// of both sides of the rate and reported here.
    pub prepared_not_launched: Vec<String>,
    pub rate: Option<f64>,
}

/// A derivation this evaluator version deliberately does not perform, named
/// with the reason so its absence cannot be read as a zero.
#[derive(Debug, Clone, Serialize)]
pub struct UnsupportedDerivation {
    pub derivation: String,
    pub status: String,
    pub reason: String,
    pub tracked_by: String,
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
    pub source_kind: String,
    /// `false` for a legacy/ordinary artifact with no author generation. An
    /// unattributed source can neither be excluded as self-use nor support an
    /// author-exit claim; both stay explicitly unknown.
    pub source_attributed: bool,
    pub source_evidence: String,
    pub repo: String,
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
    /// evidence. The SAME gate the per-task verified-reuse rate uses, so the
    /// two can never disagree about what "verified" means.
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
    /// Why the goal cannot pass, when something other than the raw counts
    /// blocks it (a truncated capture, or a derivation this evaluator version
    /// does not perform). Present exactly when `goal_met` is forced `false`.
    pub goal_blocked_reason: Option<String>,
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
    /// Resolved evidence ids from `ReviewedAnnotation` for this task. Operator
    /// judgment, kept distinct from `rework_spans`/`attention_hold_spans`
    /// (daemon-observed lower bounds) rather than summed with them — a
    /// reviewer's count and a span count answer different questions and
    /// silently adding them would misstate both.
    pub reviewed_repeated_investigations: Vec<String>,
    pub reviewed_rework: Vec<String>,
    pub reviewed_interventions: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct QualitySummary {
    /// `attention_hold` phase spans in this repo and window. A LOWER BOUND on
    /// operator interventions: one that left no span is not counted.
    pub attention_hold_spans: usize,
    pub interventions_known: usize,
    pub interventions_coverage: String,
    pub rework_spans: usize,
    pub incorrect_reuse: usize,
    pub regressions: Vec<String>,
    /// Resolved `ReviewedAnnotation` counts across every delivery, kept
    /// SEPARATE from the daemon-observed span counts above: a reviewer's
    /// bounded, evidenced judgment answers a different question than a
    /// telemetry lower bound, and summing them would misstate both.
    pub reviewed_repeated_investigations: usize,
    pub reviewed_rework: usize,
    pub reviewed_interventions: usize,
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
    pub capture: CaptureSummary,
    pub invalid_records: Vec<InvalidRecord>,
    /// Records that are structurally fine but cannot be placed or resolved: an
    /// undated observation, a legacy row with no generation id, an
    /// operator/unbound exposure, evidence simply absent from this capture.
    /// These stay UNKNOWN — they are never read as a negative.
    pub unresolved_records: Vec<InvalidRecord>,
    /// Derivations this evaluator version does not perform, named so their
    /// absence cannot be mistaken for a measured zero.
    pub unsupported: Vec<UnsupportedDerivation>,
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

/// Whether a payload merely CLAIMS a `bbs_kind`. A tuple with no `bbs_kind` is
/// ordinary traffic, not a defective BBS record, so only claimants are
/// validated (and therefore only claimants are ever reported as invalid).
fn claims_kind(t: &Value, bbs_kind: &str) -> bool {
    t["payload"]["bbs_kind"] == bbs_kind
}

/// Structural contract check for an artifact-category BBS record
/// (`finding`/`answer`/`reuse`/`assessment`). Returns the reason it is NOT
/// one, or `None` when it is genuine.
///
/// These are exactly the invariants `crates/rk-daemon/src/bbs.rs` guarantees
/// at write time: immutable `Furniture` lifecycle, `payload.schema_version`
/// for the kinds that carry one, the kind's daemon-minted identity prefix,
/// and `instance == payload.agent`, which
/// `Tuple::new(category, scope, identity, caller, payload)` makes
/// unconditional for every BBS write — each one passes the authenticated
/// caller as both. A row where they disagree was not written by that path.
/// Matching `category` + `bbs_kind` alone, as this module previously did, lets
/// an ordinary unversioned artifact masquerade as a finding.
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
             authors the tuple as its own authenticated caller"
        ));
    }
    None
}

/// Structural contract check for a daemon-authored telemetry Event
/// (`exposure`/`open`).
///
/// Note what is deliberately NOT checked here: `instance == payload.agent`.
/// These records are authored by the CASTLE *about* an agent, so `instance` is
/// the castle and the consumer identity lives only in
/// `payload.agent`/`payload.spawn`/`payload.bound`. Reading `instance` as the
/// consumer generation attributes the record to the wrong party. What is
/// checked instead is that the tuple's scope agrees with the `repo` the daemon
/// wrote into the payload.
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
/// alone let an event, a receipt or a telemetry row stand in for the artifact
/// a finding claims; filtering non-string members silently let `["ev-1", 7]`
/// pass as though it had named one thing.
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

/// Resolves one `ReviewedAnnotation` evidence list (repeated-investigation,
/// rework or intervention) against the capture, reusing `check_evidence`'s
/// artifact/repo/non-string rules so a reviewed annotation is held to the same
/// bar as a finding's own evidence. `Unknown` and `Invalid` are reported, never
/// silently dropped or silently trusted; only `Resolved` items are counted.
fn resolve_annotated_evidence(
    items: &[AnnotatedEvidence],
    by_id: &BTreeMap<&str, &Value>,
    repo: &str,
    kind: &str,
    invalid: &mut Vec<InvalidRecord>,
    unresolved: &mut Vec<InvalidRecord>,
) -> Vec<String> {
    let mut resolved = Vec::new();
    for item in items {
        match check_evidence(&json!([item.evidence.clone()]), by_id, repo) {
            EvidenceCheck::Resolved => resolved.push(item.evidence.clone()),
            EvidenceCheck::Unknown(reason) => unresolved.push(InvalidRecord {
                record: item.evidence.clone(),
                kind: kind.to_string(),
                reason,
            }),
            EvidenceCheck::Invalid(reason) => invalid.push(InvalidRecord {
                record: item.evidence.clone(),
                kind: kind.to_string(),
                reason,
            }),
        }
    }
    resolved
}

/// Whether `source` is something a receipt may legitimately name. Mirrors the
/// daemon's own `bbs.reuse` rule (crates/rk-daemon/src/bbs.rs): an ordinary
/// artifact with NO `bbs_kind` at all — reusable regardless of lifecycle,
/// since a daemon-authored gate result carries reproduction evidence just as
/// much as a session-lifecycle one — or a genuine `finding`/`answer`. A
/// receipt, an assessment or a telemetry record is never a source, and a row
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
        None => None,
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

/// A native observation's OWN timestamp field (`exited_at`, `observed_at`),
/// which is what the window must be applied to — `created_at` is when the
/// tuple was written, not when the thing happened.
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
    /// `true` when this launch had already been superseded by a later one
    /// under the same `spawn` by the time this exit was recorded. When set,
    /// `prior_state`/`crashed` describe the SUCCESSOR, not this launch, and
    /// are nulled out at the source rather than defaulted — so a stale exit
    /// must never be read as "nothing happened after the last result"
    /// (`s2-stale-event-observation-contract`, `TKT-tulir-kotah-gisub`).
    /// Absent on records predating that contract; treated as not-stale.
    stale_session: Option<bool>,
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

fn build_index<'a>(capture: &'a TupleCapture, manifest: &Manifest) -> Index<'a> {
    let repos: BTreeSet<&str> = manifest.repos.iter().map(String::as_str).collect();
    let in_scope = |repo: &str| repos.is_empty() || repos.contains(repo);
    let window = &manifest.window;

    let mut idx = Index {
        by_id: capture
            .tuples
            .iter()
            .filter_map(|t| t.get("id").and_then(Value::as_str).map(|id| (id, t)))
            .collect(),
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

    // An observation is accepted only when it is in a manifest repo AND inside
    // the frozen window. Both rejections are COUNTED, so a scoping or window
    // mistake shows up as visible coverage loss instead of silently shrinking
    // every metric toward zero.
    let admit = |idx: &mut Index<'a>, t: &Value, at: Option<DateTime<Utc>>| -> bool {
        if !in_scope(str_field(t, &["scope"])) {
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
            // Only an exact agent generation counts toward agent exposure.
            // `bound` is the daemon's own statement about whether it could
            // establish one; an operator/unbound row is recorded as unknown
            // rather than attributed to a guess.
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
            idx.opened.insert((scope, source, spawn));
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
                stale_session: t["payload"]["stale_session"].as_bool(),
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


        if claims_kind(t, REUSE) {
            if let Some(reason) = artifact_record_defect(t, REUSE) {
                idx.invalid.push(InvalidRecord {
                    record: record_id(t),
                    kind: REUSE.into(),
                    reason,
                });
                continue;
            }
            // The frozen window is enforced here exactly as it is for every
            // other native observation (exposure/open/launch events): a
            // receipt created outside it is excluded and counted under
            // `observations_out_of_window`, not silently admitted as a valid
            // claim (review 01M2CS8FMFPCPZKFPM65VGV5BH, TKT-figil-fobud-niluk).
            if !admit(&mut idx, t, created_at(t)) {
                continue;
            }
            // A receipt authored by a generation is itself proof the
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

        if claims_kind(t, ASSESSMENT) {
            if let Some(reason) = assessment_defect(t) {
                idx.invalid.push(InvalidRecord {
                    record: record_id(t),
                    kind: ASSESSMENT.into(),
                    reason,
                });
                continue;
            }
            // Same window enforcement as REUSE above: an out-of-window verdict
            // must not be admitted as a valid assessment.
            if !admit(&mut idx, t, created_at(t)) {
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

        // Findings are validated here only so a malformed one is REPORTED;
        // source selection re-checks per pair against that pair's repo.
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
                repo: scope,
                spawn,
                session: None,
                at: created_at(t),
                record: record_id(t),
                kind: "launch_event",
            });
            }
        }
    }

    idx.invalid
        .sort_by(|a, b| (&a.record, &a.kind).cmp(&(&b.record, &b.kind)));
    idx
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
    // `stale_session: true` means a LATER launch of this same generation
    // already existed by the time this exit was recorded: the daemon nulls
    // out prior_state/crashed because they would describe that successor,
    // not this launch. The exit is real, but it is direct proof a relaunch
    // already happened, so it cannot establish the terminal generation state
    // at the time of the reuse (`TKT-tulir-kotah-gisub`).
    if exit.stale_session == Some(true) {
        return Err(format!(
            "{evidence_id} is a stale-session exit: a later launch of generation {source_spawn} \
             already existed when it was recorded, so it cannot establish the terminal state at \
             the time of the reuse"
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
    // A stale-session exit's `prior_state` is nulled at the SOURCE because it
    // would otherwise describe a successor launch, not this one — it is not
    // "nothing happened between", it is "unknown". Only a non-stale absent
    // `prior_state` (legacy shape, or genuinely nothing recorded) is read as a
    // match (`TKT-tulir-kotah-gisub`).
    if exit.prior_state.is_none() && exit.stale_session == Some(true) {
        return (
            false,
            format!(
                "exit {} is a stale-session record: its prior_state is unknown (it would \
                 describe a successor launch, not this one), so this segment cannot be confirmed \
                 final",
                exit.record
            ),
        );
    }
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

/// Computes the deterministic report with no reviewed task-scoped annotations
/// (repeated-investigation/rework/intervention). Equivalent to
/// `compute_full(manifest, capture, reviews, &[])`. Test-only: every real
/// caller (the CLI) goes through `compute_full` directly so it can pass
/// parsed reviewed annotations.
#[cfg(test)]
fn compute(manifest: &Manifest, capture: &TupleCapture, reviews: &[Review]) -> Result<Report> {
    compute_full(manifest, capture, reviews, &[])
}

/// Computes the deterministic report. Same inputs always produce the same
/// output (stable sort keys throughout; no reliance on hash-map iteration
/// order, wall-clock "now", or randomness).
#[allow(clippy::too_many_lines)]
pub fn compute_full(
    manifest: &Manifest,
    capture: &TupleCapture,
    reviews: &[Review],
    reviewed_annotations: &[ReviewedAnnotation],
) -> Result<Report> {
    validate_manifest(manifest)?;
    let all_pairs = validate_and_merge_pairs(manifest, reviews)?;
    validate_reviewed_annotations(manifest, reviewed_annotations)?;

    let idx = build_index(capture, manifest);
    let review_by_pair: BTreeMap<&str, &Review> =
        reviews.iter().map(|r| (r.pair.as_str(), r)).collect();

    let mut sorted_pairs = all_pairs;
    sorted_pairs.sort_by(|a, b| a.id.cmp(&b.id));

    let mut seen_keys: BTreeSet<(String, String, String, String)> = BTreeSet::new();
    let mut excluded: Vec<Excluded> = Vec::new();
    let mut rejected_claims: Vec<RejectedClaim> = Vec::new();
    let mut unresolved = idx.unresolved.clone();
    let mut ambiguous_assessments: Vec<AmbiguousAssessment> = Vec::new();
    let mut author_exit_unsupported: Vec<AuthorExitUnsupported> = Vec::new();
    let mut pairs_out: Vec<PairResult> = Vec::new();

    // Reviewed task-scoped annotations: resolved once, up front, then looked
    // up per task in the delivery loop below.
    let mut invalid_records: Vec<InvalidRecord> = idx.invalid.clone();
    #[allow(clippy::type_complexity)]
    let mut reviewed_by_task: BTreeMap<(String, String), (Vec<String>, Vec<String>, Vec<String>)> =
        BTreeMap::new();
    for a in reviewed_annotations {
        let repeated = resolve_annotated_evidence(
            &a.repeated_investigations,
            &idx.by_id,
            &a.repo,
            "reviewed_repeated_investigation",
            &mut invalid_records,
            &mut unresolved,
        );
        let rework = resolve_annotated_evidence(
            &a.rework,
            &idx.by_id,
            &a.repo,
            "reviewed_rework",
            &mut invalid_records,
            &mut unresolved,
        );
        let interventions = resolve_annotated_evidence(
            &a.interventions,
            &idx.by_id,
            &a.repo,
            "reviewed_intervention",
            &mut invalid_records,
            &mut unresolved,
        );
        reviewed_by_task.insert((a.task.clone(), a.repo.clone()), (repeated, rework, interventions));
    }

    for pair in &sorted_pairs {
        // A duplicate identity is counted once and REPORTED, so a repeated
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
        let Some(source) = idx.by_id.get(pair.source.as_str()) else {
            excluded.push(Excluded {
                pair: pair.id.clone(),
                reason: "source_not_captured".into(),
                detail: format!("source {} is not present in the tuple capture", pair.source),
            });
            continue;
        };
        if let Some(reason) = source_defect(source, &pair.repo) {
            excluded.push(Excluded {
                pair: pair.id.clone(),
                reason: "invalid_source".into(),
                detail: reason,
            });
            continue;
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
            continue;
        }
        // A finding/answer declares its evidence; an ordinary artifact has no
        // evidence contract to check. Unresolvable evidence leaves the
        // opportunity standing but stops it certifying anything.
        let source_evidence = if source_kind == FINDING || source_kind == ANSWER {
            let check = check_evidence(&source["payload"]["evidence"], &idx.by_id, &pair.repo);
            match &check {
                EvidenceCheck::Invalid(reason) => {
                    excluded.push(Excluded {
                        pair: pair.id.clone(),
                        reason: "invalid_source_evidence".into(),
                        detail: reason.clone(),
                    });
                    continue;
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

        // Find the receipt. Every rejection below removes the CLAIM, not the
        // pair: the opportunity stays in the eligible denominator with no
        // claimed outcome, because a malformed or foreign receipt is not
        // evidence that no opportunity existed.
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
                        detail: "receipt has no parsable created_at, so it cannot be ordered \
                                 against the source"
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

        let review = review_by_pair.get(pair.id.as_str()).copied();
        let claimed_outcome = claim
            .and_then(|c| c["payload"]["outcome"].as_str())
            .map(str::to_string);
        let claim_evidence = claim.map(record_id);
        let claim_created = claim.and_then(created_at);

        let mut assessed_verdict: Option<String> = None;
        let mut assessment_evidence: Option<String> = None;
        if let Some(receipt) = &claim_evidence {
            if let Some(all) = idx.assessment_by_receipt.get(receipt) {
                // Same-repo and resolvable-evidence filters are applied here,
                // not at collection time, because both depend on this pair's
                // repo. A verdict on unresolvable evidence is not a verdict.
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
                        if capture.order == Order::PersistenceSequence {
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

        // Coverage. Native telemetry and operator judgment are kept strictly
        // apart — merging them made an operator's note indistinguishable from a
        // daemon observation — and a selection prepared for a generation that
        // never ran is neither.
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
                    Some(Coverage::Prepared { evidence }) => (
                        "prepared".to_string(),
                        "reviewed".to_string(),
                        Some(evidence.clone()),
                        Some(idx.by_id.contains_key(evidence.as_str())),
                    ),
                    Some(Coverage::NotPrepared { evidence }) => (
                        "not_prepared".to_string(),
                        "reviewed".to_string(),
                        Some(evidence.clone()),
                        Some(idx.by_id.contains_key(evidence.as_str())),
                    ),
                    _ => ("unknown".to_string(), "none".to_string(), None, None),
                },
            };
        let opened = idx.opened.contains(&(
            pair.repo.clone(),
            pair.source.clone(),
            pair.consumer_generation.clone(),
        ));

        // Author exit, derived. Only an `agent_exit` observation for the source
        // author's exact `(spawn, session)` can establish it, and only with no
        // relaunch of that generation before the consuming decision — a manual
        // respawn keeps the SpawnId, so an earlier exit is not proof the author
        // was gone when the consumer decided.
        let mut author_terminal = false;
        let mut author_terminal_evidence = None;
        if let Some(evidence_id) = review.and_then(|r| r.author_terminal_evidence.as_deref()) {
            match resolve_author_exit(
                evidence_id,
                &idx,
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

        // A verified effect is never certified on incomplete evidence. Every
        // conjunct is a real observation, not a default: unknown temporal order
        // means the source was not shown to predate the reuse; unknown or
        // not-prepared coverage means the source was never shown to be
        // discoverable to this consumer; an unlaunched consumer never made a
        // decision at all.
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

        pairs_out.push(PairResult {
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
        });
    }

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
    let known_coverage_pairs = prepared_pairs + not_prepared_pairs;
    let discovery = Discovery {
        eligible_pairs: eligible,
        known_coverage_pairs,
        prepared_pairs,
        prepared_native,
        prepared_reviewed,
        not_prepared_pairs,
        unknown_coverage_pairs: unknown_coverage.len(),
        prepared_not_launched,
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

    // Verified reuse is per distinct eligible consumer task, not per pair — a
    // task with several eligible pairs is one opportunity to act. It uses the
    // SAME `verified_effect` gate the mechanism goal counts; previously it
    // applied a weaker test and could report reuse the goal refused.
    // Confirmations and rejections are independent counts, reported alongside
    // rather than instead of a verified effect.
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
    let mut author_exit_reuse: Vec<String> = pairs_out
        .iter()
        .filter(|p| p.counts_as_effect && p.author_terminal)
        .map(|p| p.pair.clone())
        .collect();
    author_exit_reuse.sort();
    let author_exit_effects = author_exit_reuse.len();
    // A truncated capture still cannot certify the goal: a reuse or assessment
    // outside the truncation window could change any of these counts.
    let goal_blocked_reason = capture.truncated.then(|| {
        "the tuple capture is truncated: a reuse or assessment outside the truncation window \
         could change any of these counts"
            .to_string()
    });
    let mechanism = MechanismResult {
        effects: effect_pairs.len(),
        batches: effect_batches.len(),
        author_exit_effects,
        effects_required: MECHANISM_EFFECTS_REQUIRED,
        batches_required: MECHANISM_BATCHES_REQUIRED,
        author_exit_required: MECHANISM_AUTHOR_EXIT_REQUIRED,
        goal_met: goal_blocked_reason.is_none()
            && effect_pairs.len() >= MECHANISM_EFFECTS_REQUIRED
            && effect_batches.len() >= MECHANISM_BATCHES_REQUIRED
            && author_exit_effects >= MECHANISM_AUTHOR_EXIT_REQUIRED,
        goal_blocked_reason,
        effect_pairs,
    };

    // --- deliveries, from frozen scope -------------------------------
    let mut deliveries = Vec::new();
    let mut tasks_without_native_records = Vec::new();
    let mut attention_hold_spans = 0usize;
    let mut rework_spans = 0usize;
    let mut reviewed_repeated_investigations_total = 0usize;
    let mut reviewed_rework_total = 0usize;
    let mut reviewed_interventions_total = 0usize;
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
                    (true, Some(_), other) => {
                        // A final result carrying a figure under neither known
                        // basis (e.g. the producer's own `unknown` basis for a
                        // stale/mixed-basis segment). Never pooled with either
                        // total and never silently dropped.
                        unknown_cost.push(format!(
                            "{spawn}/{session}/{}: reported cost has unrecognized cost_basis={other:?}, \
                             not pooled",
                            provider_session.as_deref().unwrap_or("no-provider-session")
                        ));
                    }
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

        let (reviewed_repeated_investigations, reviewed_rework, reviewed_interventions) =
            reviewed_by_task
                .get(&(scope.task.clone(), scope.repo.clone()))
                .cloned()
                .unwrap_or_default();
        reviewed_repeated_investigations_total += reviewed_repeated_investigations.len();
        reviewed_rework_total += reviewed_rework.len();
        reviewed_interventions_total += reviewed_interventions.len();

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
            reviewed_repeated_investigations,
            reviewed_rework,
            reviewed_interventions,
        });
    }
    deliveries.sort_by(|a, b| (&a.repo, &a.task).cmp(&(&b.repo, &b.task)));
    tasks_without_native_records.sort();

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
        interventions_known: attention_hold_spans,
        interventions_coverage: INTERVENTIONS_LOWER_BOUND.into(),
        rework_spans,
        incorrect_reuse,
        regressions,
        reviewed_repeated_investigations: reviewed_repeated_investigations_total,
        reviewed_rework: reviewed_rework_total,
        reviewed_interventions: reviewed_interventions_total,
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
        capture: idx.capture.clone(),
        invalid_records,
        unresolved_records: unresolved,
        // Author-exit and per-delivery cost are DERIVED as of evaluator version
        // 3. What remains genuinely underived is named here, so its absence
        // still cannot be read as a measured zero.
        unsupported: vec![
            UnsupportedDerivation {
                derivation: "active_work_ms".into(),
                status: "unknown".into(),
                reason: ACTIVE_WORK_UNKNOWN.into(),
                tracked_by: "no native observation exists; not scheduled".into(),
            },
            UnsupportedDerivation {
                derivation: "total_operator_interventions".into(),
                status: "lower_bound".into(),
                reason: INTERVENTIONS_LOWER_BOUND.into(),
                tracked_by: "TKT-nonub-pugar-pilid".into(),
            },
        ],
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

/// Renders a `Report` as human-readable text. JSON output should use
/// `serde_json::to_string_pretty` directly on the `Report` for a byte-stable
/// machine shape.
fn opt_usd(v: Option<f64>) -> String {
    match v {
        Some(v) => format!("${v:.4}"),
        None => "unknown".into(),
    }
}

fn opt_rate(rate: Option<f64>) -> String {
    match rate {
        Some(r) => format!("{:.1}%", r * 100.0),
        None => "unknown (no denominator)".into(),
    }
}

pub fn render(report: &Report) -> String {
    let mut out = format!("## Stigmergy evidence report: {}\n\n", report.experiment_id);
    let _ = writeln!(
        out,
        "evaluator_version={} eligible={} excluded={} rejected_claims={} opened={} claimed={} \
         assessed={}",
        report.evaluator_version,
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
    if let Some(reason) = &report.mechanism.goal_blocked_reason {
        let _ = writeln!(out, "  goal cannot pass: {reason}");
    }
    for u in &report.unsupported {
        let _ = writeln!(
            out,
            "UNSUPPORTED {} [{}]: {}",
            u.derivation, u.status, u.reason
        );
    }
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
                "    cost: reported={} coverage={} partial={} daemon_priced={} \
                 provisional_completion={}",
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
            if !d.reviewed_repeated_investigations.is_empty()
                || !d.reviewed_rework.is_empty()
                || !d.reviewed_interventions.is_empty()
            {
                let _ = writeln!(
                    out,
                    "    reviewed: repeated_investigations={} rework={} interventions={}",
                    d.reviewed_repeated_investigations.len(),
                    d.reviewed_rework.len(),
                    d.reviewed_interventions.len()
                );
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
        "quality: attention_hold_spans={} interventions_known={} rework_spans={} \
         incorrect_reuse={} regressions={}",
        report.quality.attention_hold_spans,
        report.quality.interventions_known,
        report.quality.rework_spans,
        report.quality.incorrect_reuse,
        report.quality.regressions.join(", ")
    );
    let _ = writeln!(
        out,
        "  reviewed (operator judgment, not daemon facts): repeated_investigations={} rework={} \
         interventions={}",
        report.quality.reviewed_repeated_investigations,
        report.quality.reviewed_rework,
        report.quality.reviewed_interventions
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
            "identity": format!("bbs-finding-{id}"),
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
            "identity": format!("bbs-assessment-{id}"),
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

    const CASTLE: &str = "castle-1";

    /// `record_exposure`'s exact payload (crates/rk-daemon/src/bbs.rs on
    /// rat/scurry-15/tkt-tapip-puhot-sitih): castle-authored, so `instance` is
    /// the CASTLE and the consumer lives only in the payload, with the
    /// selected tuple named by `entries[].source`.
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

    /// A `harness_result`: task-completion evidence, and therefore proof the
    /// generation RAN. Never author-exit evidence — the supervisor emits it at
    /// `rk done`, while the OS process is still alive.
    fn harness_result(id: &str, spawn: &str, task: &str, created: &str) -> Value {
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

    /// `agent_exit` (S2 contract, identity `bbs-agent-exit`): castle-authored
    /// physical-exit evidence, never `harness_result`. `stale_session: Some(true)`
    /// models a delayed exit for a launch already superseded by a later one
    /// under the same `spawn`; its `prior_state`/`crashed` are nulled at the
    /// source (`TKT-tulir-kotah-gisub`), not because nothing happened.
    #[allow(clippy::too_many_arguments)]
    fn agent_exit(
        id: &str,
        repo: &str,
        task: &str,
        agent: &str,
        spawn: &str,
        session: &str,
        launched_at: Option<&str>,
        exited_at: Option<&str>,
        prior_state: Option<&str>,
        crashed: bool,
        stale_session: Option<bool>,
        created: &str,
    ) -> Value {
        json!({
            "id": id,
            "category": "event",
            "scope": repo,
            "identity": "bbs-agent-exit",
            "instance": CASTLE,
            "lifecycle": "furniture",
            "created_at": created,
            "payload": {
                "schema_version": 1, "bbs_kind": AGENT_EXIT, "repo": repo, "task": task,
                "agent": agent, "spawn": spawn, "session": session,
                "launched_at": launched_at, "exited_at": exited_at,
                "prior_state": prior_state, "crashed": crashed, "exit_code": 0,
                "stale_session": stale_session
            }
        })
    }

    /// `agent_final_usage` (S2 contract, identity `bbs-agent-final-usage`):
    /// castle-authored provider-cost evidence, keyed with `agent_exit` on
    /// `(spawn, session)`. `cost_basis` is the ONLY field that gates whether a
    /// figure may be pooled as a provider-reported total.
    #[allow(clippy::too_many_arguments)]
    fn agent_final_usage(
        id: &str,
        repo: &str,
        task: &str,
        spawn: &str,
        session: &str,
        provider_session: Option<&str>,
        state: Option<&str>,
        cost_usd: Option<f64>,
        cost_basis: &str,
        observed_at: &str,
        created: &str,
    ) -> Value {
        json!({
            "id": id,
            "category": "event",
            "scope": repo,
            "identity": "bbs-agent-final-usage",
            "instance": CASTLE,
            "lifecycle": "furniture",
            "created_at": created,
            "payload": {
                "schema_version": 1, "bbs_kind": AGENT_FINAL_USAGE, "repo": repo, "task": task,
                "spawn": spawn, "session": session, "provider_session": provider_session,
                "state": state, "cost_usd": cost_usd, "cost_basis": cost_basis,
                "observed_at": observed_at
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

    /// The happy path, shaped natively: finding, receipt, operator verdict, a
    /// castle-authored exposure binding the source to the consumer generation,
    /// and a completion proving that generation actually launched.
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
            harness_result("h1", "gen-1", "TKT-1", "2026-01-02T01:00:00Z"),
        ]
    }

    fn only(report: &Report) -> &PairResult {
        assert_eq!(report.pairs.len(), 1, "expected exactly one surviving pair");
        &report.pairs[0]
    }

    fn exclusion_reasons(report: &Report) -> Vec<&str> {
        report.excluded.iter().map(|e| e.reason.as_str()).collect()
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
            reuse(
                "r1",
                "src-1",
                "TKT-1",
                "gen-1",
                "used",
                "2026-01-02T00:00:00Z",
            ),
            assessment("a1", "r1", "verified", "2026-01-03T00:00:00Z"),
            harness_result("hr1", "author-gen", "TKT-source", "2026-01-01T12:00:00Z"),
        ];
        let c = capture(tuples, Order::Unknown);
        let reviews = vec![review_with_exit("p1", "hr1", false)];
        let r1 = compute(&m, &c, &reviews).unwrap();
        assert_eq!(r1.claimed, 1);
        assert_eq!(r1.assessed, 1);
        assert_eq!(r1.mechanism.effects, 1);
        // The previous evaluator credited author-exit from this very
        // `harness_result`. It is completion evidence, not physical exit, and
        // this version credits nothing at all until the real derivation lands.
        assert!(r1.author_exit_reuse.is_empty());
        assert_eq!(r1.author_exit_unsupported.len(), 1);
        assert_eq!(r1.author_exit_unsupported[0].evidence, "hr1");
        assert!(r1.author_exit_unsupported[0].reason.contains("still alive"));
        assert_eq!(r1.mechanism.author_exit_effects, 0);
        assert!(!r1.mechanism.goal_met);
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
            reuse(
                "r1",
                "src-1",
                "TKT-1",
                "gen-1",
                "used",
                "2026-01-02T00:00:00Z",
            ),
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
            reuse(
                "r1",
                "src-1",
                "TKT-1",
                "gen-1",
                "used",
                "2026-01-02T00:00:00Z",
            ),
            assessment("a1", "r1", "verified", "2026-01-03T00:00:00Z"),
            // Terminal event for a DIFFERENT generation than the source's author.
            harness_result("hr1", "someone-else", "TKT-other", "2026-01-01T12:00:00Z"),
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
            reuse(
                "r1",
                "src-1",
                "TKT-1",
                "gen-1",
                "used",
                "2026-01-02T00:00:00Z",
            ),
            assessment("a1", "r1", "verified", "2026-01-03T00:00:00Z"),
            // Exit happens AFTER the reuse it's supposed to precede.
            harness_result("hr1", "author-gen", "TKT-source", "2026-01-05T00:00:00Z"),
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
            reuse(
                "r1",
                "src-1",
                "TKT-1",
                "gen-1",
                "confirmed",
                "2026-01-02T00:00:00Z",
            ),
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
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let mut f = finding("src-1", "author-gen", "2026-01-01T00:00:00Z");
        f["scope"] = json!("other-repo");
        let c = capture(vec![f], Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.excluded[0].reason, "invalid_source");
        assert!(r.excluded[0].detail.contains("!= pair repo repo"));
    }

    #[test]
    fn a_pair_repo_outside_manifest_repos_is_an_input_error() {
        // `manifest.repos` was enforced nowhere before this correction.
        let mut m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        m.eligible_pairs[0].repo = "other-repo".into();
        let err = validate_manifest(&m).unwrap_err().to_string();
        assert!(err.contains("declared for repo repo"), "{err}");
    }

    #[test]
    fn missing_evidence_excluded() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let mut f = finding("src-1", "author-gen", "2026-01-01T00:00:00Z");
        f["payload"]["evidence"] = json!([]);
        let c = capture(vec![f], Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.excluded[0].reason, "invalid_source_evidence");
        assert!(r.excluded[0].detail.contains("empty"));
    }

    #[test]
    fn wrong_generation_claim_excluded() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            // Written by a different generation than the frozen pair names.
            reuse(
                "r1",
                "src-1",
                "TKT-1",
                "gen-OTHER",
                "used",
                "2026-01-02T00:00:00Z",
            ),
        ];
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        // OMITTED DENOMINATOR before this correction: the pair was `excluded`,
        // deleting a real opportunity from both rates. A bad claim removes the
        // claim.
        assert!(r.excluded.is_empty(), "the opportunity is not erased");
        assert_eq!(r.eligible, 1);
        assert_eq!(r.rejected_claims.len(), 1);
        assert_eq!(r.rejected_claims[0].reason, "wrong_generation");
        assert_eq!(r.claimed, 0);
        assert_eq!(r.verified_reuse.eligible_consumer_tasks, 1);
        assert_eq!(r.verified_reuse.rate, Some(0.0));
    }

    #[test]
    fn future_source_excluded() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let tuples = vec![
            // Source postdates the reuse it supposedly informed.
            finding("src-1", "author-gen", "2026-02-01T00:00:00Z"),
            reuse(
                "r1",
                "src-1",
                "TKT-1",
                "gen-1",
                "used",
                "2026-01-02T00:00:00Z",
            ),
        ];
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert!(r.excluded.is_empty(), "the opportunity is not erased");
        assert_eq!(r.eligible, 1);
        assert_eq!(r.rejected_claims[0].reason, "future_source");
        assert_eq!(r.claimed, 0);
    }

    #[test]
    fn unsupported_assessment_never_counts_toward_verified_reuse() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            reuse(
                "r1",
                "src-1",
                "TKT-1",
                "gen-1",
                "used",
                "2026-01-02T00:00:00Z",
            ),
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
            reuse(
                "r1",
                "src-1",
                "TKT-1",
                "gen-1",
                "used",
                "2026-01-02T00:00:00Z",
            ),
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
        for (src, task, gen) in [
            ("src-1", "TKT-1", "gen-1"),
            ("src-2", "TKT-2", "gen-2"),
            ("src-3", "TKT-3", "gen-3"),
        ] {
            tuples.push(finding(src, "author-gen", "2026-01-01T00:00:00Z"));
            tuples.push(reuse(
                &format!("r-{src}"),
                src,
                task,
                gen,
                "adapted",
                "2026-01-02T00:00:00Z",
            ));
            tuples.push(assessment(
                &format!("a-{src}"),
                &format!("r-{src}"),
                "verified",
                "2026-01-03T00:00:00Z",
            ));
        }
        tuples.push(harness_result(
            "hr1",
            "author-gen",
            "TKT-source",
            "2026-01-01T12:00:00Z",
        ));
        let c = capture(tuples, Order::Unknown);
        let reviews = vec![
            review_no_exit("p1"),
            review_no_exit("p2"),
            review_with_exit("p3", "hr1", false),
        ];
        let r = compute(&m, &c, &reviews).unwrap();
        assert_eq!(r.mechanism.effects, 3);
        assert_eq!(r.mechanism.batches, 2);
        // The effect and batch counts clear the bar, and the goal STILL must
        // not pass: the only cited terminal evidence is a `harness_result`,
        // which this evaluator version explicitly refuses as author-exit
        // proof (task-completion evidence, not a physical exit). Unlike a
        // truncated capture, this is not "cannot be evaluated" — it is simply
        // not met, so `goal_blocked_reason` stays `None`.
        assert_eq!(r.mechanism.author_exit_effects, 0);
        assert!(!r.mechanism.goal_met);
        assert!(r.mechanism.goal_blocked_reason.is_none());
        assert_eq!(r.author_exit_unsupported.len(), 1);
        assert!(r.author_exit_unsupported[0].reason.contains("still alive"));
    }

    #[test]
    fn ambiguous_assessment_order_is_not_silently_resolved() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            reuse(
                "r1",
                "src-1",
                "TKT-1",
                "gen-1",
                "used",
                "2026-01-02T00:00:00Z",
            ),
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
            reuse(
                "r1",
                "src-1",
                "TKT-1",
                "gen-1",
                "used",
                "2026-01-02T00:00:00Z",
            ),
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
        assert_eq!(r.presented_native, 0);
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
            reuse(
                "r1",
                "src-1",
                "TKT-1",
                "gen-1",
                "used",
                "2026-01-02T00:00:00Z",
            ),
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
    fn deliveries_come_from_frozen_scope_with_no_native_records_at_all() {
        // OMITTED DENOMINATOR the previous version had: deliveries were derived
        // from the pairs that survived evaluation, so a frozen task with no
        // eligible source and no receipt vanished along with its accounting.
        let m = Manifest {
            consumer_tasks: vec![ConsumerTaskScope {
                task: "TKT-lonely".into(),
                repo: "repo".into(),
                batch: "batch-1".into(),
            }],
            ..manifest(vec![])
        };
        let report = compute(&m, &capture(vec![], Order::Unknown), &[]).unwrap();
        assert_eq!(report.eligible, 0, "no pairs at all");
        assert_eq!(report.deliveries.len(), 1, "the frozen task still appears");
        let d = &report.deliveries[0];
        assert_eq!(d.task, "TKT-lonely");
        assert_eq!(d.repo, "repo");
        assert_eq!(d.enrollment, "frozen_consumer_task");
        // No native record at all for this task: every figure is an explicit
        // unknown, never a manufactured zero.
        assert_eq!(d.reported_cost_estimate_usd, None);
        assert_eq!(d.cost_coverage, "none_observed");
        assert_eq!(d.active_work_ms, None);
        assert_eq!(d.process_lifetime_ms, None);
        assert_eq!(d.accepted, None);
        assert_eq!(
            report.tasks_without_native_records,
            vec!["repo/TKT-lonely".to_string()]
        );
        assert!(report
            .quality
            .interventions_coverage
            .contains("lower bound"));
    }

    #[test]
    fn author_exit_is_credited_from_a_valid_agent_exit_observation() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let tuples = vec![
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
            agent_exit(
                "ax1",
                "repo",
                "TKT-source",
                "author",
                "author-gen",
                "sess-1",
                Some("2026-01-01T00:00:00Z"),
                Some("2026-01-01T12:00:00Z"),
                None,
                false,
                None,
                "2026-01-01T12:00:00Z",
            ),
        ];
        let c = capture(tuples, Order::Unknown);
        let reviews = vec![review_with_exit("p1", "ax1", false)];
        let r = compute(&m, &c, &reviews).unwrap();
        assert!(r.author_exit_unsupported.is_empty());
        assert_eq!(r.author_exit_reuse, vec!["p1".to_string()]);
        assert_eq!(r.mechanism.author_exit_effects, 1);
        assert!(only(&r).author_terminal);
        assert_eq!(
            only(&r).author_terminal_evidence,
            Some("ax1".to_string())
        );
    }

    #[test]
    fn author_exit_is_not_credited_from_a_stale_session_exit() {
        // stale_session: true means a LATER launch of this same generation
        // already existed when this exit was recorded; prior_state/crashed are
        // nulled at the source rather than describing this launch.
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let tuples = vec![
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
            agent_exit(
                "ax1",
                "repo",
                "TKT-source",
                "author",
                "author-gen",
                "sess-1",
                Some("2026-01-01T00:00:00Z"),
                Some("2026-01-01T12:00:00Z"),
                None,
                false,
                Some(true),
                "2026-01-01T12:00:00Z",
            ),
        ];
        let c = capture(tuples, Order::Unknown);
        let reviews = vec![review_with_exit("p1", "ax1", false)];
        let r = compute(&m, &c, &reviews).unwrap();
        assert!(r.author_exit_reuse.is_empty());
        assert_eq!(r.author_exit_unsupported.len(), 1);
        assert!(r.author_exit_unsupported[0]
            .reason
            .contains("stale-session"));
    }

    #[test]
    fn cost_is_credited_when_a_segment_is_final_and_provider_reported() {
        let m = Manifest {
            consumer_tasks: vec![ConsumerTaskScope {
                task: "TKT-1".into(),
                repo: "repo".into(),
                batch: "batch-1".into(),
            }],
            ..manifest(vec![])
        };
        let tuples = vec![
            agent_final_usage(
                "u1", "repo", "TKT-1", "gen-1", "sess-1", Some("prov-1"), Some("completed"),
                Some(1.5), PROVIDER_COST_BASIS, "2026-01-01T11:00:00Z", "2026-01-01T11:00:00Z",
            ),
            agent_exit(
                "ax1", "repo", "TKT-1", "gen-1", "gen-1", "sess-1",
                Some("2026-01-01T10:00:00Z"), Some("2026-01-01T12:00:00Z"),
                Some("completed"), false, None, "2026-01-01T12:00:00Z",
            ),
        ];
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.deliveries.len(), 1);
        let d = &r.deliveries[0];
        assert_eq!(d.cost_coverage, "complete");
        assert_eq!(d.reported_cost_estimate_usd, Some(1.5));
        assert_eq!(d.process_lifetime_ms, Some(2 * 60 * 60 * 1000));
        assert!(d.unknown_cost.is_empty());
    }

    #[test]
    fn cost_stays_partial_when_more_work_followed_the_last_result() {
        // The last usage result said `paused`, but the exit's prior_state was
        // `running`: work happened after that result, so the reported amount
        // is partial, not final.
        let m = Manifest {
            consumer_tasks: vec![ConsumerTaskScope {
                task: "TKT-1".into(),
                repo: "repo".into(),
                batch: "batch-1".into(),
            }],
            ..manifest(vec![])
        };
        let tuples = vec![
            agent_final_usage(
                "u1", "repo", "TKT-1", "gen-1", "sess-1", Some("prov-1"), Some("paused"),
                Some(3.0), PROVIDER_COST_BASIS, "2026-01-01T11:00:00Z", "2026-01-01T11:00:00Z",
            ),
            agent_exit(
                "ax1", "repo", "TKT-1", "gen-1", "gen-1", "sess-1",
                Some("2026-01-01T10:00:00Z"), Some("2026-01-01T12:00:00Z"),
                Some("running"), false, None, "2026-01-01T12:00:00Z",
            ),
        ];
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        let d = &r.deliveries[0];
        assert_eq!(d.cost_coverage, "partial");
        assert_eq!(d.reported_cost_estimate_usd, None);
        assert_eq!(d.partial_reported_usd, Some(3.0));
        assert_eq!(d.unknown_cost.len(), 1);
    }

    #[test]
    fn cost_is_missing_when_no_final_usage_observation_exists() {
        let m = Manifest {
            consumer_tasks: vec![ConsumerTaskScope {
                task: "TKT-1".into(),
                repo: "repo".into(),
                batch: "batch-1".into(),
            }],
            ..manifest(vec![])
        };
        let tuples = vec![harness_result(
            "h1",
            "gen-1",
            "TKT-1",
            "2026-01-01T12:00:00Z",
        )];
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        let d = &r.deliveries[0];
        assert_eq!(d.cost_coverage, "missing");
        assert_eq!(d.reported_cost_estimate_usd, None);
        assert_eq!(d.completions, 1);
        assert_eq!(d.failed_completions, 0);
    }

    #[test]
    fn reviewed_annotation_counts_resolved_evidence_and_reports_bad_evidence() {
        let mut m = Manifest {
            consumer_tasks: vec![ConsumerTaskScope {
                task: "TKT-1".into(),
                repo: "repo".into(),
                batch: "batch-1".into(),
            }],
            ..manifest(vec![])
        };
        m.eligible_pairs = vec![];
        let c = capture(vec![], Order::Unknown);
        let annotations = vec![ReviewedAnnotation {
            task: "TKT-1".into(),
            repo: "repo".into(),
            repeated_investigations: vec![AnnotatedEvidence {
                evidence: "ev-1".into(),
                reason: "same root cause chased twice".into(),
            }],
            rework: vec![],
            interventions: vec![AnnotatedEvidence {
                evidence: "not-in-capture".into(),
                reason: "operator unblocked a stuck review".into(),
            }],
        }];
        let r = compute_full(&m, &c, &[], &annotations).unwrap();
        let d = &r.deliveries[0];
        assert_eq!(d.reviewed_repeated_investigations, vec!["ev-1".to_string()]);
        assert_eq!(d.reviewed_interventions.len(), 0, "unresolved evidence is not counted");
        assert_eq!(r.quality.reviewed_repeated_investigations, 1);
        assert_eq!(r.quality.reviewed_interventions, 0);
        assert!(r
            .unresolved_records
            .iter()
            .any(|u| u.record == "not-in-capture" && u.kind == "reviewed_intervention"));
    }

    #[test]
    fn reviewed_annotation_for_a_task_outside_frozen_scope_is_an_input_error() {
        let m = manifest(vec![]);
        let c = capture(vec![], Order::Unknown);
        let annotations = vec![ReviewedAnnotation {
            task: "TKT-not-frozen".into(),
            repo: "repo".into(),
            repeated_investigations: vec![],
            rework: vec![],
            interventions: vec![],
        }];
        assert!(compute_full(&m, &c, &[], &annotations).is_err());
    }

    #[test]
    fn a_fixture_pairs_task_is_also_enrolled_for_delivery_accounting() {
        let report = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(verified_tuples(), Order::Unknown),
            &[review_no_exit("p1")],
        )
        .unwrap();
        assert_eq!(report.deliveries.len(), 1);
        assert_eq!(report.deliveries[0].task, "TKT-1");
        assert_eq!(report.deliveries[0].enrollment, "fixture_pair");
    }

    #[test]
    fn unsupported_derivations_are_named_with_their_reason_and_tracking_ticket() {
        // An absent metric has to be legible as absent, in machine output as
        // well as human output, or a reader fills the gap with a zero.
        // Author-exit and delivery cost are DERIVED as of evaluator version 3;
        // what remains genuinely underived is named here instead.
        let report = compute(&manifest(vec![]), &capture(vec![], Order::Unknown), &[]).unwrap();
        let named: Vec<&str> = report
            .unsupported
            .iter()
            .map(|u| u.derivation.as_str())
            .collect();
        assert_eq!(named, vec!["active_work_ms", "total_operator_interventions"]);
        for u in &report.unsupported {
            assert!(!u.reason.is_empty());
            assert!(!u.tracked_by.is_empty());
        }
        assert_eq!(report.unsupported[0].status, "unknown");
        assert_eq!(report.unsupported[1].status, "lower_bound");
        let text = render(&report);
        assert!(text.contains("UNSUPPORTED active_work_ms"), "{text}");
        assert!(
            text.contains("UNSUPPORTED total_operator_interventions"),
            "{text}"
        );
        assert!(report.deliveries.is_empty());
    }

    #[test]
    fn forged_assessment_not_authored_by_operator_is_not_trusted() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let mut a = assessment("a1", "r1", "verified", "2026-01-03T00:00:00Z");
        a["payload"]["agent"] = json!("some-rat");
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            reuse(
                "r1",
                "src-1",
                "TKT-1",
                "gen-1",
                "used",
                "2026-01-02T00:00:00Z",
            ),
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
        assert_eq!(r.excluded[0].reason, "invalid_source_evidence");
        assert!(r.excluded[0].detail.contains("scoped to \"other-repo\""));
    }

    #[test]
    fn evidence_referencing_a_nonexistent_artifact_does_not_resolve() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let mut f = finding("src-1", "author-gen", "2026-01-01T00:00:00Z");
        f["payload"]["evidence"] = json!(["never-captured"]);
        let c = capture(vec![f], Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        // An id absent from the capture is UNKNOWN, not a negative: a partial
        // capture must not delete a real opportunity. The pair survives and
        // simply cannot certify anything.
        assert!(r.excluded.is_empty());
        assert_eq!(r.eligible, 1);
        assert_eq!(only(&r).source_evidence, "unknown");
        assert!(!only(&r).verified_effect);
        assert!(r
            .unresolved_records
            .iter()
            .any(|u| u.reason.contains("never-captured")));
    }

    #[test]
    fn evidence_with_a_non_string_member_is_rejected_explicitly() {
        // The previous `evidence_ids` silently filtered non-strings, so
        // `["ev-1", 7]` looked like a clean one-item list.
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let mut f = finding("src-1", "author-gen", "2026-01-01T00:00:00Z");
        f["payload"]["evidence"] = json!(["ev-1", 7]);
        let r = compute(&m, &capture(vec![f], Order::Unknown), &[]).unwrap();
        assert_eq!(r.excluded[0].reason, "invalid_source_evidence");
        assert!(r.excluded[0].detail.contains("non-string member"));
    }

    #[test]
    fn evidence_naming_a_non_artifact_is_invalid_not_resolved() {
        // Checking only `scope` let an EVENT stand in for the artifact a
        // finding claims as its evidence.
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let f = finding("src-1", "author-gen", "2026-01-01T00:00:00Z");
        let ev = json!({
            "id": "ev-1", "category": "event", "scope": "repo", "identity": "something",
            "instance": "x", "created_at": "2026-01-01T00:00:00Z", "payload": {}
        });
        let r = compute(&m, &capture(vec![f, ev], Order::Unknown), &[]).unwrap();
        assert_eq!(r.excluded[0].reason, "invalid_source_evidence");
        assert!(r.excluded[0].detail.contains("not an artifact"));
    }

    #[test]
    fn missing_source_timestamp_prevents_counting_a_verified_effect() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let mut f = finding("src-1", "author-gen", "2026-01-01T00:00:00Z");
        f.as_object_mut().unwrap().remove("created_at");
        let tuples = vec![
            f,
            reuse(
                "r1",
                "src-1",
                "TKT-1",
                "gen-1",
                "used",
                "2026-01-02T00:00:00Z",
            ),
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
            reuse(
                "r1",
                "src-1",
                "TKT-1",
                "gen-1",
                "used",
                "2026-01-02T00:00:00Z",
            ),
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
        for (src, task, gen) in [
            ("src-1", "TKT-1", "gen-1"),
            ("src-2", "TKT-2", "gen-2"),
            ("src-3", "TKT-3", "gen-3"),
        ] {
            tuples.push(finding(src, "author-gen", "2026-01-01T00:00:00Z"));
            tuples.push(reuse(
                &format!("r-{src}"),
                src,
                task,
                gen,
                "adapted",
                "2026-01-02T00:00:00Z",
            ));
            tuples.push(assessment(
                &format!("a-{src}"),
                &format!("r-{src}"),
                "verified",
                "2026-01-03T00:00:00Z",
            ));
        }
        tuples.push(harness_result(
            "hr1",
            "author-gen",
            "TKT-source",
            "2026-01-01T12:00:00Z",
        ));
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

    // ------------------------------------------------------------------
    // Record identity binding. Each of these passed the previous
    // category+bbs_kind check and could certify a result.
    // ------------------------------------------------------------------

    #[test]
    fn source_whose_instance_disagrees_with_its_author_is_rejected() {
        // `Tuple::new(.., caller, ..)` makes `instance == payload.agent`
        // unconditional for every BBS write, so a row where they disagree was
        // not written by that path.
        let mut tuples = verified_tuples();
        tuples[0]["instance"] = json!("someone-else");
        let r = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &[review_no_exit("p1")],
        )
        .unwrap();
        assert_eq!(exclusion_reasons(&r), vec!["invalid_source"]);
        assert!(r.excluded[0].detail.contains("instance"));
        assert_eq!(r.mechanism.effects, 0);
    }

    #[test]
    fn source_without_the_daemon_minted_identity_prefix_is_rejected() {
        let mut tuples = verified_tuples();
        tuples[0]["identity"] = json!("finding-1");
        let r = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &[review_no_exit("p1")],
        )
        .unwrap();
        assert_eq!(exclusion_reasons(&r), vec!["invalid_source"]);
        assert!(r.excluded[0].detail.contains("bbs-finding-"));
    }

    #[test]
    fn a_receipt_is_not_a_reusable_source() {
        // The daemon refuses `bbs.reuse` on a receipt; the report must refuse
        // the same pairing rather than let a receipt stand in for a finding.
        let source = reuse(
            "rx",
            "src-0",
            "TKT-0",
            "other-gen",
            "used",
            "2026-01-01T00:00:00Z",
        );
        let r = compute(
            &manifest(vec![pair("p1", "rx", "TKT-1", "gen-1")]),
            &capture(vec![source], Order::Unknown),
            &[],
        )
        .unwrap();
        assert_eq!(exclusion_reasons(&r), vec!["invalid_source"]);
        assert!(r.excluded[0].detail.contains("not a reusable source"));
    }

    #[test]
    fn an_ordinary_artifact_is_a_valid_source_and_stays_unattributed() {
        // Mirrors the daemon rule: an artifact with NO bbs_kind is reusable
        // regardless of lifecycle. It carries no authoring generation, so
        // attribution stays explicitly unknown rather than being guessed.
        let plain = json!({
            "id": "src-plain", "category": "artifact", "scope": "repo",
            "identity": "gate-result", "instance": CASTLE, "lifecycle": "furniture",
            "created_at": "2026-01-01T00:00:00Z", "payload": {"note": "repro"}
        });
        let mut tuples = verified_tuples();
        tuples[0] = plain;
        tuples[1] = reuse(
            "r1",
            "src-plain",
            "TKT-1",
            "gen-1",
            "used",
            "2026-01-02T00:00:00Z",
        );
        tuples[3] = exposure("x1", "src-plain", "gen-1", "2026-01-01T12:00:00Z");
        let r = compute(
            &manifest(vec![pair("p1", "src-plain", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &[review_no_exit("p1")],
        )
        .unwrap();
        assert!(r.excluded.is_empty());
        let p = only(&r);
        assert_eq!(p.source_kind, "artifact");
        assert!(!p.source_attributed);
        assert_eq!(p.source_evidence, "not_applicable");
    }

    // ------------------------------------------------------------------
    // Exposure scoping, binding, and the launch join.
    // ------------------------------------------------------------------

    #[test]
    fn exposure_from_another_repo_is_not_native_coverage() {
        // The previous `is_event_kind` ignored tuple scope entirely, so a
        // foreign repo's exposure counted as prepared.
        let mut tuples = verified_tuples();
        tuples[3] = exposure_full(
            "x1",
            "src-1",
            "gen-1",
            "2026-01-01T12:00:00Z",
            "other",
            "agent",
        );
        let r = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &[],
        )
        .unwrap();
        assert_eq!(only(&r).coverage_status, "unknown");
        assert_eq!(r.presented_native, 0);
        assert_eq!(r.capture.observations_out_of_scope_repo, 1);
        assert!(!only(&r).verified_effect, "unknown coverage cannot certify");
    }

    #[test]
    fn exposure_outside_the_frozen_window_is_not_native_coverage() {
        // `Manifest.window` was enforced NOWHERE before this correction.
        let mut m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        m.window = Window {
            since: Some("2026-02-01T00:00:00Z".parse().unwrap()),
            until: None,
        };
        let r = compute(&m, &capture(verified_tuples(), Order::Unknown), &[]).unwrap();
        assert_eq!(only(&r).coverage_status, "unknown");
        assert!(r.capture.observations_out_of_window >= 1);
    }

    /// `build_index` validates a REUSE/ASSESSMENT record's shape but must also
    /// apply the SAME frozen window used for every other native observation
    /// (exposure/open/launch events) — otherwise a receipt or verdict from
    /// outside the batch window is silently admitted as a valid claim
    /// (review 01M2CS8FMFPCPZKFPM65VGV5BH, TKT-figil-fobud-niluk).
    #[test]
    fn reuse_created_before_the_frozen_window_does_not_count_as_a_claim() {
        let mut m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        m.window = Window {
            since: Some("2026-01-02T00:00:00Z".parse().unwrap()),
            until: None,
        };
        // The reuse itself is dated BEFORE the frozen window opened.
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            reuse(
                "r1",
                "src-1",
                "TKT-1",
                "gen-1",
                "used",
                "2026-01-01T12:00:00Z",
            ),
            assessment("a1", "r1", "verified", "2026-01-03T00:00:00Z"),
        ];
        let c = capture(tuples, Order::Unknown);
        let before = c.tuples.len();
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(before, c.tuples.len(), "sanity: capture untouched");
        assert_eq!(r.claimed, 0, "the out-of-window reuse must not be a claim");
        assert_eq!(r.verified_reuse.verified_used_or_adapted_tasks, 0);
        assert_eq!(only(&r).claimed_outcome, None);
        assert!(r.capture.observations_out_of_window >= 1);
        // The eligible opportunity itself is untouched — only the bad claim is
        // dropped, matching the excluded/rejected_claims split elsewhere.
        assert_eq!(r.eligible, 1);
    }

    #[test]
    fn reuse_created_after_the_frozen_window_does_not_count_as_a_claim() {
        let mut m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        m.window = Window {
            since: None,
            until: Some("2026-01-01T18:00:00Z".parse().unwrap()),
        };
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            reuse(
                "r1",
                "src-1",
                "TKT-1",
                "gen-1",
                "used",
                "2026-01-02T00:00:00Z",
            ),
            assessment("a1", "r1", "verified", "2026-01-01T12:00:00Z"),
        ];
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.claimed, 0);
        assert_eq!(only(&r).claimed_outcome, None);
        assert!(r.capture.observations_out_of_window >= 1);
        assert_eq!(r.eligible, 1);
    }

    #[test]
    fn assessment_outside_the_frozen_window_does_not_certify_a_verdict() {
        let mut m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        m.window = Window {
            since: Some("2026-01-02T12:00:00Z".parse().unwrap()),
            until: None,
        };
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            // The reuse is inside the window; only the assessment is not.
            reuse(
                "r1",
                "src-1",
                "TKT-1",
                "gen-1",
                "used",
                "2026-01-02T13:00:00Z",
            ),
            assessment("a1", "r1", "verified", "2026-01-02T00:00:00Z"),
        ];
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.claimed, 1, "the reuse itself is a valid, in-window claim");
        assert_eq!(
            r.assessed, 0,
            "the out-of-window assessment must not certify a verdict"
        );
        assert_eq!(only(&r).assessed_verdict, None);
        assert_eq!(r.verified_reuse.verified_used_or_adapted_tasks, 0);
        assert!(r.capture.observations_out_of_window >= 1);
    }

    #[test]
    fn an_undated_reuse_or_assessment_is_unresolved_not_silently_admitted() {
        // Undated only differs from Inside once a window is actually
        // declared — an undeclared window treats everything as Inside, same
        // as every other native observation.
        let mut m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        m.window = Window {
            since: Some("2026-01-01T00:00:00Z".parse().unwrap()),
            until: None,
        };
        let mut r1 = reuse(
            "r1",
            "src-1",
            "TKT-1",
            "gen-1",
            "used",
            "2026-01-02T00:00:00Z",
        );
        r1.as_object_mut().unwrap().remove("created_at");
        let tuples = vec![finding("src-1", "author-gen", "2026-01-01T00:00:00Z"), r1];
        let c = capture(tuples, Order::Unknown);
        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(r.claimed, 0);
        assert_eq!(r.capture.observations_undated, 1);
    }

    #[test]
    fn an_operator_bound_exposure_is_not_agent_exposure() {
        let mut tuples = verified_tuples();
        tuples[3] = exposure_full(
            "x1",
            "src-1",
            "gen-1",
            "2026-01-01T12:00:00Z",
            "repo",
            "operator",
        );
        let r = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &[],
        )
        .unwrap();
        assert_eq!(only(&r).coverage_status, "unknown");
        assert!(r
            .unresolved_records
            .iter()
            .any(|u| u.kind == EXPOSURE && u.reason.contains("no exact consumer generation")));
    }

    #[test]
    fn an_exposure_entry_without_a_source_id_is_reported_invalid() {
        let mut tuples = verified_tuples();
        tuples[3]["payload"]["entries"] = json!([{"reason": "task or dependency"}]);
        let r = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &[],
        )
        .unwrap();
        assert_eq!(only(&r).coverage_status, "unknown");
        assert!(r
            .invalid_records
            .iter()
            .any(|i| i.kind == "exposure_entry" && i.reason.contains("no `source` id")));
    }

    #[test]
    fn exposure_for_a_generation_that_never_launched_is_not_an_opportunity() {
        // A selection prepared for a spawn that never ran is neither a
        // discovery success nor a discovery failure: there was no decision to
        // inform, so it is kept out of both sides of the rate.
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            exposure("x1", "src-1", "gen-1", "2026-01-01T12:00:00Z"),
        ];
        let r = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &[],
        )
        .unwrap();
        let p = only(&r);
        assert_eq!(p.coverage_status, "prepared_not_launched");
        assert!(!p.consumer_launched);
        assert_eq!(r.discovery.prepared_not_launched, vec!["p1"]);
        assert_eq!(r.discovery.known_coverage_pairs, 0);
        assert_eq!(r.discovery.rate, None, "absent denominator, not a 0% rate");
        assert!(!p.verified_effect);
    }

    #[test]
    fn an_authored_record_is_itself_launch_evidence() {
        // A generation that wrote a receipt demonstrably ran, so a pair with a
        // real claim is never mis-reported as never-launched.
        let tuples = vec![
            finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
            reuse(
                "r1",
                "src-1",
                "TKT-1",
                "gen-1",
                "used",
                "2026-01-02T00:00:00Z",
            ),
            exposure("x1", "src-1", "gen-1", "2026-01-01T12:00:00Z"),
        ];
        let r = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &[],
        )
        .unwrap();
        assert!(only(&r).consumer_launched);
        assert_eq!(only(&r).coverage_status, "prepared");
        assert_eq!(r.discovery.prepared_native, 1);
    }

    #[test]
    fn opens_are_bound_to_the_exact_consumer_generation() {
        let mut tuples = verified_tuples();
        tuples.push(open_record("o1", "src-1", "gen-1", "2026-01-01T13:00:00Z"));
        tuples.push(open_record(
            "o2",
            "src-1",
            "other-gen",
            "2026-01-01T13:00:00Z",
        ));
        let r = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &[],
        )
        .unwrap();
        assert!(only(&r).opened);
        assert_eq!(r.opened, 1, "another generation's open is not this pair's");
    }

    // ------------------------------------------------------------------
    // Denominators and gate parity.
    // ------------------------------------------------------------------

    #[test]
    fn native_and_reviewed_prepared_coverage_stay_distinguishable() {
        // Merging them made an operator's note indistinguishable from a daemon
        // observation.
        let mut tuples = verified_tuples();
        tuples.remove(3); // no native exposure
        let r = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &[review_no_exit("p1")],
        )
        .unwrap();
        let p = only(&r);
        assert_eq!(p.coverage_status, "prepared");
        assert_eq!(p.coverage_provenance, "reviewed");
        assert_eq!(
            p.coverage_reference_resolved,
            Some(false),
            "an operator reference that is not a captured tuple is labelled, not trusted"
        );
        assert_eq!(r.presented_native, 0);
        assert_eq!(r.presented_reviewed, 1);
        assert_eq!(r.discovery.prepared_native, 0);
        assert_eq!(r.discovery.prepared_reviewed, 1);
    }

    #[test]
    fn verified_reuse_uses_the_same_gate_as_the_mechanism_goal() {
        // The previous per-task rate checked only outcome + verdict, so a pair
        // the mechanism goal refused for unknown coverage still counted as
        // verified reuse. The two can no longer disagree.
        let mut tuples = verified_tuples();
        tuples.remove(3); // drop the native exposure -> coverage unknown
        let r = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &[],
        )
        .unwrap();
        assert_eq!(only(&r).coverage_status, "unknown");
        assert_eq!(
            r.outcome_classes.verified, 1,
            "the verdict is still reported"
        );
        assert_eq!(r.verified_reuse.verified_used_or_adapted_tasks, 0);
        assert_eq!(r.mechanism.effects, 0);
        assert_eq!(r.unknown_coverage, vec!["p1"]);
    }

    #[test]
    fn a_not_prepared_pair_is_a_known_denominator_but_never_an_effect() {
        let mut tuples = verified_tuples();
        tuples.remove(3);
        let r = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &[Review {
                coverage: Coverage::NotPrepared {
                    evidence: "telemetry-checked".into(),
                },
                ..review_no_exit("p1")
            }],
        )
        .unwrap();
        assert_eq!(only(&r).coverage_status, "not_prepared");
        assert!(!only(&r).verified_effect);
        assert_eq!(r.discovery.known_coverage_pairs, 1);
        assert_eq!(r.discovery.rate, Some(0.0));
    }

    #[test]
    fn rates_are_unknown_rather_than_zero_when_there_is_no_denominator() {
        let r = compute(&manifest(vec![]), &capture(vec![], Order::Unknown), &[]).unwrap();
        assert_eq!(r.discovery.rate, None);
        assert_eq!(r.verified_reuse.rate, None);
        let text = render(&r);
        assert!(text.contains("unknown (no denominator)"), "{text}");
        assert!(text.contains("evaluator_version=3"), "{text}");
    }

    #[test]
    fn a_misspelled_manifest_field_is_an_input_error_not_a_silent_default() {
        // This is how `window` came to be unenforced in the first place.
        let err = serde_json::from_value::<Manifest>(json!({
            "schema_version": 1, "experiment_id": "e",
            "batches": [{"id": "b", "arm": "real", "repo": "repo"}],
            "windwo": {"since": "2026-01-01T00:00:00Z"}
        }))
        .unwrap_err()
        .to_string();
        assert!(err.contains("windwo"), "{err}");
    }

    #[test]
    fn manifest_rejects_a_backwards_window_and_duplicate_frozen_tasks() {
        let mut m = manifest(vec![]);
        m.window = Window {
            since: Some("2026-02-01T00:00:00Z".parse().unwrap()),
            until: Some("2026-01-01T00:00:00Z".parse().unwrap()),
        };
        assert!(validate_manifest(&m)
            .unwrap_err()
            .to_string()
            .contains("after"));

        let mut m = manifest(vec![]);
        let scope = ConsumerTaskScope {
            task: "TKT-1".into(),
            repo: "repo".into(),
            batch: "batch-1".into(),
        };
        m.consumer_tasks = vec![scope.clone(), scope];
        assert!(validate_manifest(&m)
            .unwrap_err()
            .to_string()
            .contains("duplicate consumer_tasks"));
    }
}

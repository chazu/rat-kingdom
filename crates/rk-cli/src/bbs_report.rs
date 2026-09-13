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

/// `rk_core::action::canonical_digest` is a lowercase hex SHA-256, so every
/// digest-keyed identity suffix is exactly this many lowercase hex characters.
const DIGEST_HEX_LEN: usize = 64;
/// Every `rk_daemon::bbs::ExposureSurface::as_str()` value.
const EXPOSURE_SURFACES: [&str; 4] = ["spawn", "resume", "recovery", "brief"];
/// A castle's wire author id is `castle-<first 16 hex of its Ed25519 key>`
/// (`rk_core::identity::actor_from_pubkey`); a configured `castle_name` is a
/// presentation-only alias `server.rs` never lets become the wire id.
/// `"daemon"` is the literal author every non-supervisor span call site passes.
const DAEMON_AUTHOR: &str = "daemon";
const CASTLE_ACTOR_PREFIX: &str = "castle-";
const CASTLE_ACTOR_HEX_LEN: usize = 16;

fn is_lower_hex(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Whether `instance` has the shape of a daemon-side author. A SHAPE check
/// over an offline export, NOT cryptographic proof — this module verifies no
/// signature and cannot say so. What it does buy: a worker generation
/// (`Scritch-15`) can never be mistaken for the castle that is the only
/// legitimate author, and an unrecognized author stays a reported finding.
fn is_castle_author(instance: &str) -> bool {
    instance == DAEMON_AUTHOR
        || instance
            .strip_prefix(CASTLE_ACTOR_PREFIX)
            .is_some_and(|hex| hex.len() == CASTLE_ACTOR_HEX_LEN && is_lower_hex(hex))
}

/// How a BBS record kind's identity is actually MINTED by
/// `crates/rk-daemon/src/bbs.rs`. Flattening these three contracts into one
/// `starts_with` is what let forged records in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeIdentity {
    /// `format!("{prefix}{key}")` with `key` a `canonical_digest` over the
    /// record's content, so the suffix is exactly [`DIGEST_HEX_LEN`] lowercase
    /// hex characters — never a short readable token.
    Digest(&'static str),
    /// `format!("bbs-exposure-{}", surface.as_str())`: a closed four-value
    /// suffix, and the SAME value the producer writes to `payload.surface`.
    Surface(&'static str),
    /// One FIXED identity, written bare and complete (`bbs-open`,
    /// `bbs-agent-exit`, `bbs-agent-final-usage`). No producer appends
    /// anything, so any longer string is forged authority, not a variant.
    Exact(&'static str),
}

/// Read off the producers, not off `rk_core::bbs::RESERVED_IDENTITY_PREFIXES`:
/// that list is a `starts_with` DENY list for the agent write path, where
/// matching too much is safe. Reusing it as an ACCEPT rule is the defect this
/// fixes — `bbs-agent-exit-forged` is correctly refused a write by the deny
/// list and was then wrongly admitted here as an `agent_exit`.
fn native_identity(bbs_kind: &str) -> Option<NativeIdentity> {
    use NativeIdentity::{Digest, Exact, Surface};
    match bbs_kind {
        FINDING => Some(Digest("bbs-finding-")),
        ANSWER => Some(Digest("bbs-answer-")),
        REUSE => Some(Digest("bbs-reuse-")),
        ASSESSMENT => Some(Digest("bbs-assessment-")),
        EXPOSURE => Some(Surface("bbs-exposure-")),
        OPEN => Some(Exact("bbs-open")),
        AGENT_EXIT => Some(Exact("bbs-agent-exit")),
        AGENT_FINAL_USAGE => Some(Exact("bbs-agent-final-usage")),
        _ => None,
    }
}

/// Why `t`'s identity is not one this `bbs_kind`'s producer could have minted.
/// Each family keeps exactly its own producer's contract, so no real exported
/// record is rejected: `Digest` still accepts ANY digest, but it must actually
/// be one (`bbs-finding-x` is not something `canonical_digest` can return);
/// `Surface` requires agreement between suffix and `payload.surface`, which
/// one `as_str()` call makes unconditional, so it can only ever refuse a
/// forgery; `Exact` is byte equality, having no variants to preserve.
fn identity_defect(t: &Value, bbs_kind: &str) -> Option<String> {
    let identity = str_field(t, &["identity"]);
    let strip = |prefix: &str| -> Result<&str, String> {
        identity.strip_prefix(prefix).ok_or_else(|| {
            format!("identity {identity:?} does not carry the daemon-minted {prefix}* prefix")
        })
    };
    match native_identity(bbs_kind)? {
        NativeIdentity::Exact(exact) => {
            if identity != exact {
                return Some(format!(
                    "identity {identity:?} is not the fixed daemon-minted identity {exact:?}; \
                     this kind is written bare, so a suffixed variant is forged"
                ));
            }
        }
        NativeIdentity::Digest(prefix) => {
            let key = match strip(prefix) {
                Ok(key) => key,
                Err(reason) => return Some(reason),
            };
            if key.len() != DIGEST_HEX_LEN || !is_lower_hex(key) {
                return Some(format!(
                    "identity {identity:?} suffix is not a canonical_digest: a native key is \
                     {DIGEST_HEX_LEN} lowercase hex characters, this one is {} character(s)",
                    key.len()
                ));
            }
        }
        NativeIdentity::Surface(prefix) => {
            let key = match strip(prefix) {
                Ok(key) => key,
                Err(reason) => return Some(reason),
            };
            if !EXPOSURE_SURFACES.contains(&key) {
                return Some(format!(
                    "identity {identity:?} names surface {key:?}, which is not an exposure \
                     surface this daemon prepares"
                ));
            }
            let surface = str_field(t, &["payload", "surface"]);
            if key != surface {
                return Some(format!(
                    "identity {identity:?} names surface {key:?} but payload.surface is \
                     {surface:?}; the producer renders both from one value"
                ));
            }
        }
    }
    None
}

/// `rk_daemon::span::SPAN_IDENTITY`: the identity every phase-span Event is
/// written under.
const SPAN_IDENTITY: &str = "task_span";
/// Every `rk_daemon::span::Phase::as_str()` value, in declaration order. A
/// span naming anything else was not written by that enum.
const SPAN_PHASES: [&str; 12] = [
    "ticket_ready",
    "claimed",
    "agent_launched",
    "first_progress",
    "completed",
    "verification",
    "landing_prep",
    "semantic_review",
    "rework",
    "merge",
    "delivery_closure",
    "attention_hold",
];
/// The only `duration_semantic` any producer stamps
/// (`rk_daemon::span::PhaseSpan::from_durations`): `queue_wait_ms` and
/// `duration_ms` are disjoint, so summing both is sound.
const ADDITIVE_DURATION: &str = "additive";

/// Native events that prove an agent generation actually started a process, so
/// a prepared exposure can be joined to a LAUNCHED consumer rather than
/// counted for a spawn that never ran. `spawn` is additive on these events
/// (S2's `48fb2da`); a `harness_result` also proves the generation ran.
const LAUNCH_EVENT_IDENTITIES: [&str; 2] = ["agent_spawned", "agent_respawned"];
/// `LaunchRow.kind` for a record that observes a physical launch ATTEMPT,
/// as opposed to one that merely proves the generation ran. Only these are
/// counted as attempts a session was never observed for.
const LAUNCH_EVENT_KIND: &str = "launch_event";

/// `AgentState` values that mean this launch produced its *last* provider result.
const TERMINAL_USAGE_STATES: [&str; 3] = ["completed", "failed", "stopped"];

/// Only the producer's `CostCoverage::Final` supports final cost.
const FINAL_COST_COVERAGE: &str = "final";
/// The non-final values the producer emits: `partial_unknown` (more model work ran past the last result), `none` (no result was ever reported), `unknown` (no watch survived).
const NON_FINAL_COST_COVERAGE: [&str; 3] = ["partial_unknown", "none", "unknown"];

/// The only `cost_basis` a *provider-reported* total may carry. The daemon's
/// own priced-increment fallback is an estimate of an estimate: it is reported
/// in its own field and never pooled with this one.
const PROVIDER_COST_BASIS: &str = "provider_reported_segment_total";
const DAEMON_COST_BASIS: &str = "daemon_priced_increments";

/// Keep attention-hold spans separate from reviewed intervention counts.
const INTERVENTIONS_LOWER_BOUND: &str =
    "lower bound: attention_hold spans only cover waits the daemon recorded; an operator \
     intervention that left no span is not counted here. A ReviewedAnnotation can supply \
     evidenced interventions separately (quality.reviewed_interventions).";

/// Active work remains unknown; process lifetime and phase durations cannot establish it.
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
/// Only a `bbs.export` envelope built from `Space::persistence_page` may
/// claim `PersistenceSequence`, and the claim alone is not enough: it is
/// accepted only when the records themselves carry the ascending
/// `commit_sequence` positions that substantiate it (see
/// `persistence_order_defect`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    PersistenceSequence,
    Unknown,
}

/// The `coverage` block of a `bbs.export` envelope: the daemon's own
/// statement about whether the evidence closure it shipped is complete.
/// A bare array or a raw `rk --json scan` object makes no closure claim at
/// all, and must never be read as one.
#[derive(Debug, Clone, Default)]
pub struct CaptureCoverage {
    /// `None` when the capture shape makes no closure claim at all. That is
    /// NOT a claim of completeness.
    pub complete: Option<bool>,
    pub missing_references: Vec<String>,
    pub reference_budget_exhausted: bool,
}

pub struct TupleCapture {
    pub order: Order,
    /// Why a declared `order: "persistence_sequence"` was REFUSED and the
    /// capture downgraded to `Order::Unknown`. A claim is only a claim: it is
    /// accepted solely when the records themselves carry the persistence
    /// positions that back it.
    pub order_claim_rejected: Option<String>,
    /// The export PAGE: the bounded, sequence-ordered window this capture
    /// covers.
    pub tuples: Vec<Value>,
    /// The evidence CLOSURE: tuples referenced by a page record that were not
    /// themselves on the page, resolved AS-OF the page's frozen boundary.
    /// These are real tuples and must be merged, or linked evidence reads as
    /// absent — but they are sorted by id, not by persistence position, so
    /// only their own `commit_sequence` can order them.
    pub references: Vec<Value>,
    pub truncated: bool,
    pub coverage: CaptureCoverage,
    #[allow(dead_code)]
    pub source: Option<String>,
}

/// Page and closure as one deduped record list, in persistence order when the
/// records carry the positions to establish it.
pub struct MergedRecords<'a> {
    pub records: Vec<&'a Value>,
    /// Closure records actually added (i.e. not already on the page).
    pub references_merged: usize,
    /// Closure records dropped because the page already carried that id. The
    /// daemon never emits these; a hand-built capture can, and silently
    /// double-counting the same observation would inflate every metric.
    pub duplicate_references: usize,
}

fn commit_sequence(t: &Value) -> Option<u64> {
    t.get("commit_sequence").and_then(Value::as_u64)
}

impl TupleCapture {
    /// Merges the closure into the page. Ordering: when EVERY merged record
    /// carries a `commit_sequence`, the merged list is sorted by it, so a
    /// closure record takes its true persistence position rather than being
    /// appended after the page. Otherwise the page keeps its array order and
    /// the closure follows — a merged list that `Order::Unknown` already
    /// forbids anyone from reading as supersession order.
    pub fn merged(&self) -> MergedRecords<'_> {
        let page_ids: BTreeSet<&str> = self
            .tuples
            .iter()
            .filter_map(|t| t.get("id").and_then(Value::as_str))
            .collect();
        let mut records: Vec<&Value> = self.tuples.iter().collect();
        let mut references_merged = 0usize;
        let mut duplicate_references = 0usize;
        for r in &self.references {
            match r.get("id").and_then(Value::as_str) {
                Some(id) if page_ids.contains(id) => duplicate_references += 1,
                _ => {
                    records.push(r);
                    references_merged += 1;
                }
            }
        }
        if records.iter().all(|t| commit_sequence(t).is_some()) {
            records.sort_by_key(|t| (commit_sequence(t), t["id"].as_str().unwrap_or("")));
        }
        MergedRecords {
            records,
            references_merged,
            duplicate_references,
        }
    }
}

/// Validates a declared `order: "persistence_sequence"` against the records
/// that are supposed to back it. The claim is a string in an envelope anyone
/// can write; what makes it true is that every record carries its own
/// `commit_sequence` and the page's are strictly ascending in array order.
/// Returns the refusal reason when the claim cannot be substantiated.
fn persistence_order_defect(tuples: &[Value], references: &[Value]) -> Option<String> {
    let mut previous: Option<u64> = None;
    for (i, t) in tuples.iter().enumerate() {
        let Some(seq) = commit_sequence(t) else {
            return Some(format!(
                "the capture declares order=persistence_sequence but page record {} (index {i}) \
                 carries no numeric commit_sequence, so the claim cannot be checked",
                t["id"].as_str().unwrap_or("<no id>")
            ));
        };
        if let Some(prev) = previous {
            if seq <= prev {
                return Some(format!(
                    "the capture declares order=persistence_sequence but page record {} (index \
                     {i}) has commit_sequence {seq} after {prev}: the array is not in ascending \
                     persistence order",
                    t["id"].as_str().unwrap_or("<no id>")
                ));
            }
        }
        previous = Some(seq);
    }
    for r in references {
        if commit_sequence(r).is_none() {
            return Some(format!(
                "the capture declares order=persistence_sequence but reference record {} carries \
                 no numeric commit_sequence, so its position among the page records is unknown",
                r["id"].as_str().unwrap_or("<no id>")
            ));
        }
    }
    None
}

/// Parses either shape: a bare tuple array, the raw `rk --json scan` object
/// (`{"tuples":[...], "truncated":bool, ...}`), or the real `bbs.export`
/// envelope (`{"schema_version":1,"kind":"bbs.export","order":
/// "persistence_sequence","tuples":[...],"references":[...],"coverage":{...}}`).
/// Legacy/raw input is always `Order::Unknown` — never inferred from tuple id
/// (ULID) or `created_at` — and a declared persistence-sequence order is
/// accepted only when `persistence_order_defect` can substantiate it.
pub fn parse_tuple_capture(raw: &Value) -> Result<TupleCapture> {
    if let Some(arr) = raw.as_array() {
        return Ok(TupleCapture {
            order: Order::Unknown,
            order_claim_rejected: None,
            tuples: arr.clone(),
            references: Vec::new(),
            truncated: false,
            coverage: CaptureCoverage::default(),
            source: None,
        });
    }
    let tuples = raw
        .get("tuples")
        .and_then(Value::as_array)
        .context(
            "tuples input must be a JSON array, or an object with a `tuples` array \
             (the shape `rk --json scan` and the bbs.export envelope both produce)",
        )?
        .clone();
    let references = raw
        .get("references")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut order_claim_rejected = None;
    let order = match raw.get("order").and_then(Value::as_str) {
        Some("persistence_sequence") => match persistence_order_defect(&tuples, &references) {
            None => Order::PersistenceSequence,
            Some(reason) => {
                order_claim_rejected = Some(reason);
                Order::Unknown
            }
        },
        _ => Order::Unknown,
    };
    let truncated = raw
        .get("truncated")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let raw_coverage = raw.get("coverage");
    let coverage = CaptureCoverage {
        complete: raw_coverage
            .and_then(|c| c.get("complete"))
            .and_then(Value::as_bool),
        missing_references: raw_coverage
            .and_then(|c| c.get("missing_references"))
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default(),
        reference_budget_exhausted: raw_coverage
            .and_then(|c| c.get("reference_budget_exhausted"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
    };
    let source = raw
        .get("source")
        .and_then(Value::as_str)
        .map(str::to_string);
    Ok(TupleCapture {
        order,
        order_claim_rejected,
        tuples,
        references,
        truncated,
        coverage,
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

/// An evidenced operator judgment, distinct from native telemetry and measured time savings.
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

/// Annotations must belong to frozen task scope and cannot duplicate a (task, repo) entry.
fn validate_reviewed_annotations(
    manifest: &Manifest,
    annotations: &[ReviewedAnnotation],
) -> Result<()> {
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

/// Accept legacy review arrays or an envelope containing reviews and reviewed annotations.
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
    /// Closure records the export shipped alongside the page.
    pub references: usize,
    /// Closure records actually merged into the index (the rest were already
    /// on the page). Evidence lives here as often as on the page; a report
    /// that indexes only `tuples` reads linked evidence as absent.
    pub references_merged: usize,
    /// Closure records dropped as duplicates of a page record.
    pub duplicate_references: usize,
    /// `coverage.missing_references`: referenced tuples the daemon could not
    /// resolve at the frozen boundary. Each is also surfaced as an
    /// unresolved record, never as an absent one.
    pub missing_references: Vec<String>,
    /// `coverage.complete`. `None` when the capture shape makes no closure
    /// claim at all (a bare array or a raw `rk --json scan` object) — which
    /// is NOT the same as a claim of completeness.
    pub closure_complete: Option<bool>,
    pub reference_budget_exhausted: bool,
    /// Set when a declared `order: "persistence_sequence"` was refused and
    /// the capture downgraded to unknown order.
    pub order_claim_rejected: Option<String>,
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
    /// Sum verification duration and queue wait only for native spans explicitly tagged additive.
    pub verification_ms: Option<i64>,
    /// Retain untagged verification spans separately because their durations may overlap queue wait.
    pub verification_ms_legacy_spans: usize,
    /// `attention_hold` — waiting on a human.
    pub attention_hold_ms: Option<i64>,
    /// Pre-phase queue wait on every other phase.
    pub queue_wait_ms: Option<i64>,
}

/// One observed provider-cost segment: a `(spawn, session, provider_session)` triple.
#[derive(Debug, Clone, Serialize)]
pub struct CostSegment {
    pub spawn: String,
    pub session: String,
    pub provider_session: Option<String>,
    pub record: String,
    pub reported_usd: Option<f64>,
    pub cost_basis: String,
    pub state: Option<String>,
    /// `true` only when the last result for this segment was terminal, the launch's exit agrees with it, AND that exit reports `cost_coverage: "final"`.
    pub final_cost: bool,
    pub finality_reason: String,
}

/// One physical process launch of one generation: the `(spawn, session)` pair
/// S2's contract names as the only safe aggregation key.
#[derive(Debug, Clone, Serialize)]
pub struct LaunchObservation {
    /// `None` for an attempt observed only through a launch EVENT
    /// (`agent_spawned`/`agent_respawned`), which names no session.
    pub session: Option<String>,
    /// What observed this attempt: `agent_exit`, `agent_final_usage` (the
    /// process reported usage but no exit was captured — live, or its exit
    /// is outside the capture), or `launch_event`.
    pub observed_via: String,
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
    /// Every observed launch ATTEMPT, not every observed exit: a live
    /// generation, or one whose exit fell outside the capture, still
    /// launched. See `observed_attempts`.
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
    /// Resolved evidence ids from `ReviewedAnnotation` for this task.
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
    if let Some(reason) = identity_defect(t, bbs_kind) {
        return Some(reason);
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
/// wrote into the payload, and that the author has the shape of a castle at
/// all — otherwise a worker-authored row with a correct fixed identity could
/// still manufacture an open, an author exit or a final cost.
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
    if let Some(reason) = identity_defect(t, bbs_kind) {
        return Some(reason);
    }
    let instance = str_field(t, &["instance"]);
    if !is_castle_author(instance) {
        return Some(format!(
            "instance {instance:?} is not a castle author ({DAEMON_AUTHOR} or \
             {CASTLE_ACTOR_PREFIX}<hex>); daemon telemetry is authored by the castle, never by \
             the generation it describes"
        ));
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
    if matches!(bbs_kind, AGENT_EXIT | AGENT_FINAL_USAGE) {
        if str_field(t, &["payload", "agent"]).trim().is_empty() {
            return Some("payload.agent is absent; native usage/exit must name its agent".into());
        }
        if !matches!(&t["payload"]["task"], Value::Null | Value::String(_)) {
            return Some("payload.task is neither a string nor an unassigned null".into());
        }
        let field = if bbs_kind == AGENT_EXIT {
            "exited_at"
        } else {
            "observed_at"
        };
        if payload_time(t, field).is_none() {
            return Some(format!(
                "payload.{field} is absent or not an RFC3339 timestamp"
            ));
        }
        if bbs_kind == AGENT_EXIT
            && !t["payload"]["launched_at"].is_null()
            && payload_time(t, "launched_at").is_none()
        {
            return Some("present payload.launched_at is not an RFC3339 timestamp".into());
        }
    }
    None
}

fn is_task_span(t: &Value) -> bool {
    t["category"] == "event" && t["identity"] == SPAN_IDENTITY
}

/// Validate native phase-span shape before it can affect delivery or timing.
fn task_span_defect(t: &Value) -> Option<String> {
    if t["lifecycle"] != "furniture" {
        return Some(format!(
            "lifecycle is {} not furniture; a phase span is immutable furniture",
            t["lifecycle"].as_str().unwrap_or("absent")
        ));
    }
    // Phase spans are authored by `daemon` or the castle actor, never the described worker.
    let instance = str_field(t, &["instance"]);
    if !is_castle_author(instance) {
        return Some(format!(
            "instance {instance:?} is not a castle author ({DAEMON_AUTHOR} or \
             {CASTLE_ACTOR_PREFIX}<hex>); a phase span is authored by the castle that recorded \
             it, never by the worker it describes"
        ));
    }
    // Repo binding: the span is written into the ticket's own repo scope, and
    // `payload.repo`, where a producer set it, is that same repo.
    let scope = str_field(t, &["scope"]);
    if scope.is_empty() {
        return Some("tuple scope is absent; a phase span is scoped to its repo".into());
    }
    match &t["payload"]["repo"] {
        Value::Null => {}
        Value::String(repo) if repo == scope => {}
        other => {
            return Some(format!(
                "payload.repo {other} disagrees with tuple scope {scope:?}"
            ))
        }
    }
    if str_field(t, &["payload", "task"]).is_empty() {
        return Some("payload.task is absent; a span with no ticket cannot be placed".into());
    }
    let phase = str_field(t, &["payload", "phase"]);
    if !SPAN_PHASES.contains(&phase) {
        return Some(format!(
            "payload.phase {phase:?} is not a phase this daemon records; an unrecognized phase \
             would be pooled into the work-phase total"
        ));
    }
    // `PhaseSpan.attempt` is a `u32` and every producer numbers from 1.
    match t["payload"]["attempt"].as_u64() {
        Some(a) if a >= 1 && a <= u64::from(u32::MAX) => {}
        _ => {
            return Some(format!(
                "payload.attempt {} is not a positive integer occurrence number",
                t["payload"]["attempt"]
            ))
        }
    }
    // Times, where present, must parse and must not run backwards.
    let mut times = Vec::new();
    for field in ["queued_at", "started_at", "ended_at"] {
        match &t["payload"][field] {
            Value::Null => {}
            Value::String(raw) => match parse_rfc3339(raw) {
                Some(at) => times.push((field, at)),
                None => return Some(format!("payload.{field} {raw:?} is not an RFC3339 time")),
            },
            other => return Some(format!("payload.{field} {other} is not a time")),
        }
    }
    if let Some(w) = times.windows(2).find(|w| w[0].1 > w[1].1) {
        return Some(format!(
            "payload.{} is after payload.{}; a span cannot run backwards",
            w[0].0, w[1].0
        ));
    }
    // Durations are derived by the producer as non-negative millisecond
    // differences over exactly those timestamps.
    for field in ["queue_wait_ms", "duration_ms"] {
        match &t["payload"][field] {
            Value::Null => {}
            v => match v.as_i64() {
                Some(ms) if ms >= 0 => {}
                _ => {
                    return Some(format!(
                        "payload.{field} {v} is not a non-negative duration"
                    ))
                }
            },
        }
    }
    // Each native duration requires both endpoints and equals their millisecond difference.
    for (field, from, to) in [
        ("queue_wait_ms", "queued_at", "started_at"),
        ("duration_ms", "started_at", "ended_at"),
    ] {
        let Some(ms) = t["payload"][field].as_i64() else {
            continue;
        };
        let (Some(from_at), Some(to_at)) = (payload_time(t, from), payload_time(t, to)) else {
            return Some(format!(
                "payload.{field} is {ms} but payload.{from}/payload.{to} are not both present; \
                 the producer derives this duration from them and cannot state one without them"
            ));
        };
        let derived = (to_at - from_at).num_milliseconds();
        if ms != derived {
            return Some(format!(
                "payload.{field} is {ms} but {from}..{to} is {derived}ms; the producer derives \
                 one from the other"
            ));
        }
    }
    // `duration_semantic` is stamped by exactly one constructor with exactly
    // one value; anything else is a claim the producer never makes, and
    // `"additive"` is what unlocks summing a wait into `verification_ms`.
    match &t["payload"]["duration_semantic"] {
        Value::Null => {}
        Value::String(v) if v == ADDITIVE_DURATION => {}
        other => {
            return Some(format!(
                "payload.duration_semantic {other} is not {ADDITIVE_DURATION:?}"
            ))
        }
    }
    match &t["payload"]["authority"] {
        Value::Null => {}
        Value::String(v) if v == "human" || v == "llm" => {}
        other => return Some(format!("payload.authority {other} is not human or llm")),
    }
    if !matches!(&t["payload"]["proof_reused"], Value::Null | Value::Bool(_)) {
        return Some(format!(
            "payload.proof_reused {} is not a boolean",
            t["payload"]["proof_reused"]
        ));
    }
    // The occurrence fence. Each is optional, but a present one must be a
    // usable non-empty string: an empty `occurrence_key` claims a fence it
    // cannot provide.
    for field in [
        "terminal_reason",
        "target",
        "candidate",
        "lane",
        "occurrence_key",
        "proof_kind",
    ] {
        match &t["payload"][field] {
            Value::Null => {}
            Value::String(v) if !v.is_empty() => {}
            other => return Some(format!("payload.{field} {other} is not a non-empty string")),
        }
    }
    None
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
fn check_evidence(
    evidence: &Value,
    by_id: &BTreeMap<&str, &Value>,
    rejected: &[InvalidRecord],
    repo: &str,
) -> EvidenceCheck {
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
        if let Some(bad) = rejected.iter().find(|r| r.record == id) {
            return EvidenceCheck::Invalid(format!("evidence {id} was rejected: {}", bad.reason));
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
                let defect = match t["payload"]["bbs_kind"].as_str() {
                    Some(ASSESSMENT) => assessment_defect(t),
                    Some(kind @ (FINDING | ANSWER | REUSE)) => artifact_record_defect(t, kind),
                    Some(kind @ (EXPOSURE | OPEN | AGENT_EXIT | AGENT_FINAL_USAGE)) => {
                        telemetry_record_defect(t, kind)
                    }
                    _ => None,
                };
                if let Some(reason) = defect {
                    return EvidenceCheck::Invalid(format!(
                        "evidence {id} has an invalid BBS contract: {reason}"
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

/// Resolve annotation evidence with the same artifact, repository, and type checks as findings.
fn resolve_annotated_evidence(
    items: &[AnnotatedEvidence],
    by_id: &BTreeMap<&str, &Value>,
    rejected: &[InvalidRecord],
    repo: &str,
    kind: &str,
    invalid: &mut Vec<InvalidRecord>,
    unresolved: &mut Vec<InvalidRecord>,
) -> Vec<String> {
    let mut resolved = Vec::new();
    for item in items {
        match check_evidence(&json!([item.evidence.clone()]), by_id, rejected, repo) {
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
    /// `true` when this launch had already been superseded by a later one under the same `spawn` by the time this exit was recorded.
    stale_session: Option<bool>,
    /// The producer's own measurement of how completely the last reported cost covers this launch (`rk_daemon::bbs::record_exit`).
    cost_coverage: Option<String>,
    record: String,
}

struct UsageRow {
    repo: String,
    task: String,
    agent: String,
    spawn: String,
    session: String,
    provider_session: Option<String>,
    state: Option<String>,
    cost_usd: Option<f64>,
    cost_basis: String,
    observed_at: Option<DateTime<Utc>>,
    /// Position in the merged capture.
    position: usize,
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
    /// The task this launch was observed under, retained so a frozen task
    /// whose ONLY native record is a launch event still owns a delivery.
    /// Empty when the observing record names no task.
    task: String,
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

    /// The earliest observed launch of `spawn` strictly after `after` and no later than `until` — i.e.
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

    // The export page and its evidence closure are ONE record set. Indexing
    // only `tuples` makes a referenced finding/receipt that the daemon
    // resolved into `references` read as though it were never captured.
    let merged = capture.merged();

    let mut idx = Index {
        by_id: merged
            .records
            .iter()
            .filter_map(|t| t.get("id").and_then(Value::as_str).map(|id| (id, *t)))
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
            references: capture.references.len(),
            references_merged: merged.references_merged,
            duplicate_references: merged.duplicate_references,
            missing_references: capture.coverage.missing_references.clone(),
            closure_complete: capture.coverage.complete,
            reference_budget_exhausted: capture.coverage.reference_budget_exhausted,
            order_claim_rejected: capture.order_claim_rejected.clone(),
            ..CaptureSummary::default()
        },
    };

    // A reference the daemon could NOT resolve is a hole in the evidence
    // closure, not an absent tuple: record it so an evidence check that fails
    // against this capture is attributable to the capture, not to the author.
    for id in &capture.coverage.missing_references {
        idx.unresolved.push(InvalidRecord {
            record: id.clone(),
            kind: "missing_reference".into(),
            reason: "named by a captured record but absent from the export's evidence closure \
                     (coverage.missing_references): unresolvable in this capture, not proven \
                     absent"
                .into(),
        });
    }

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

    for (pos, t) in merged.records.iter().copied().enumerate() {
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
            if str_field(t, &["payload", "task"]).trim().is_empty() {
                idx.unresolved.push(InvalidRecord {
                    record: record_id(t),
                    kind: AGENT_EXIT.into(),
                    reason: "native observation has no assigned task; it cannot be attributed"
                        .into(),
                });
                continue;
            }
            let exited_at = payload_time(t, "exited_at");
            if !admit(&mut idx, t, exited_at) {
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
                task: str_field(t, &["payload", "task"]).to_string(),
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
                cost_coverage: t["payload"]["cost_coverage"].as_str().map(str::to_string),
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
            if str_field(t, &["payload", "task"]).trim().is_empty() {
                idx.unresolved.push(InvalidRecord {
                    record: record_id(t),
                    kind: AGENT_FINAL_USAGE.into(),
                    reason: "native observation has no assigned task; it cannot be attributed"
                        .into(),
                });
                continue;
            }
            let observed_at = payload_time(t, "observed_at");
            if !admit(&mut idx, t, observed_at) {
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
                task: str_field(t, &["payload", "task"]).to_string(),
                spawn: spawn.clone(),
                session: Some(session.clone()),
                at: observed_at,
                record: record_id(t),
                kind: AGENT_FINAL_USAGE,
            });
            idx.usage.push(UsageRow {
                repo: scope.clone(),
                task: str_field(t, &["payload", "task"]).to_string(),
                agent: str_field(t, &["payload", "agent"]).to_string(),
                spawn,
                session,
                provider_session: t["payload"]["provider_session"]
                    .as_str()
                    .map(str::to_string),
                state: t["payload"]["state"].as_str().map(str::to_string),
                cost_usd: t["payload"]["cost_usd"].as_f64(),
                cost_basis,
                observed_at,
                position: pos,
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
                task: str_field(t, &["payload", "task"]).to_string(),
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
                task: str_field(t, &["payload", "task"]).to_string(),
                spawn,
                // A launch event names no session; it is still proof of a
                // physical launch attempt, including one that has not exited.
                session: None,
                at: created_at(t),
                record: record_id(t),
                kind: LAUNCH_EVENT_KIND,
            });
            continue;
        }

        // --- task_span ------------------------------------------------
        if is_task_span(t) {
            // Validated BEFORE indexing: an indexed span reaches `build_critical_path`, the phase totals and delivery acceptance, and nothing downstream re-checks it.
            if let Some(reason) = task_span_defect(t) {
                idx.invalid.push(InvalidRecord {
                    record: record_id(t),
                    kind: SPAN_IDENTITY.into(),
                    reason,
                });
                continue;
            }
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
                    task: str_field(t, &["payload", "task"]).to_string(),
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
                    task: str_field(t, &["payload", "task"]).to_string(),
                    spawn,
                    session: None,
                    at: created_at(t),
                    record: record_id(t),
                    // An authored finding proves the generation RAN; it is
                    // not a launch event and must not be counted as a
                    // separate launch attempt.
                    kind: "authored_record",
                });
            }
        }
    }

    idx.invalid
        .sort_by(|a, b| (&a.record, &a.kind).cmp(&(&b.record, &b.kind)));
    idx
}

/// Resolve exact physical exit with launch, repository, and decision-time fences.
fn resolve_author_exit(
    evidence_id: &str,
    idx: &Index<'_>,
    repo: &str,
    source_agent: &str,
    source_task: &str,
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
    if source_task.trim().is_empty() || exit.task != source_task {
        return Err(format!(
            "{evidence_id} names task {:?}, not an established source task {source_task:?}",
            exit.task
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
    // Reject stale-session exits: their lifecycle state describes a successor launch.
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

/// Per-segment cost finality.
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
    // A stale exit's missing prior state denotes uncertainty about the superseded launch.
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
    // Coverage is measured by the producer, not inferred here.
    match exit.cost_coverage.as_deref() {
        Some(FINAL_COST_COVERAGE) => {}
        None => {
            return (
                false,
                format!(
                    "exit {} states no cost_coverage, so it is not evidence that the reported \
                     amount covers the whole launch",
                    exit.record
                ),
            )
        }
        Some(other) if NON_FINAL_COST_COVERAGE.contains(&other) => {
            return (
                false,
                format!(
                    "exit {} reports cost_coverage={other:?}: the launch's own producer states \
                     the reported amount does not cover it",
                    exit.record
                ),
            )
        }
        Some(other) => {
            return (
                false,
                format!(
                    "exit {} reports an unrecognized cost_coverage={other:?}, which cannot be \
                     read as covering the launch",
                    exit.record
                ),
            )
        }
    }
    // Agreement is required, so an absent prior_state fails closed like any unknown.
    match exit.prior_state.as_deref() {
        None => (
            false,
            format!(
                "exit {} records no prior_state, so there is nothing to agree with the last \
                 result (state={state:?}): the launch's position at exit is unknown and the \
                 reported amount cannot be confirmed to cover it",
                exit.record
            ),
        ),
        Some(prior) if prior == state => (
            true,
            format!(
                "terminal result (state={state}) followed by observed exit {} with \
                 cost_coverage={FINAL_COST_COVERAGE}",
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

/// Computes the deterministic report with no reviewed task-scoped annotations (repeated-investigation/rework/intervention).
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
    let mut reviewed_by_task: BTreeMap<
        (String, String),
        (Vec<String>, Vec<String>, Vec<String>),
    > = BTreeMap::new();
    for a in reviewed_annotations {
        let repeated = resolve_annotated_evidence(
            &a.repeated_investigations,
            &idx.by_id,
            &idx.invalid,
            &a.repo,
            "reviewed_repeated_investigation",
            &mut invalid_records,
            &mut unresolved,
        );
        let rework = resolve_annotated_evidence(
            &a.rework,
            &idx.by_id,
            &idx.invalid,
            &a.repo,
            "reviewed_rework",
            &mut invalid_records,
            &mut unresolved,
        );
        let interventions = resolve_annotated_evidence(
            &a.interventions,
            &idx.by_id,
            &idx.invalid,
            &a.repo,
            "reviewed_intervention",
            &mut invalid_records,
            &mut unresolved,
        );
        reviewed_by_task.insert(
            (a.task.clone(), a.repo.clone()),
            (repeated, rework, interventions),
        );
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
            let check = check_evidence(
                &source["payload"]["evidence"],
                &idx.by_id,
                &idx.invalid,
                &pair.repo,
            );
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
                match check_evidence(
                    &c["payload"]["evidence"],
                    &idx.by_id,
                    &idx.invalid,
                    &pair.repo,
                ) {
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
                    match check_evidence(
                        &a["payload"]["evidence"],
                        &idx.by_id,
                        &idx.invalid,
                        &pair.repo,
                    ) {
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

        // Author exit, derived.
        let mut author_terminal = false;
        let mut author_terminal_evidence = None;
        if let Some(evidence_id) = review.and_then(|r| r.author_terminal_evidence.as_deref()) {
            match resolve_author_exit(
                evidence_id,
                &idx,
                &pair.repo,
                &source_agent,
                str_field(source, &["payload", "task"]),
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
    // A capture that does not cover its own evidence still cannot certify the
    // goal. Truncation is one way to fall short; an incomplete reference
    // closure is another, and the export states both. An unresolved reference
    // could be the very receipt or assessment that changes a count.
    let goal_blocked_reason = if capture.truncated {
        Some(
            "the tuple capture is truncated: a reuse or assessment outside the truncation window \
             could change any of these counts"
                .to_string(),
        )
    } else if capture.coverage.complete == Some(false) {
        Some(format!(
            "the capture declares an incomplete evidence closure (coverage.complete=false, \
             {} unresolved reference(s), reference_budget_exhausted={}): a reuse or assessment \
             outside the closure could change any of these counts",
            capture.coverage.missing_references.len(),
            capture.coverage.reference_budget_exhausted
        ))
    } else if !capture.coverage.missing_references.is_empty() {
        Some(format!(
            "the capture reports {} unresolved reference(s) in its evidence closure: a reuse or \
             assessment among them could change any of these counts",
            capture.coverage.missing_references.len()
        ))
    } else {
        None
    };
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
                        // Verification includes admission and execution time only when the producer declares them disjoint.
                        if phase["duration_semantic"] == ADDITIVE_DURATION {
                            opt_sum(&mut phase_ms.verification_ms, dur);
                            opt_sum(&mut phase_ms.verification_ms, wait);
                        } else if dur.is_some() || wait.is_some() {
                            phase_ms.verification_ms_legacy_spans += 1;
                        }
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

        // Every native generation observed for this task, whether or not it ever produced an eligible pair or a receipt.
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
        for l in &idx.launches {
            if l.repo == scope.repo && l.task == scope.task {
                spawns.insert(l.spawn.as_str());
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

            // ATTEMPTS, not exits.
            let mut launches = Vec::new();
            let mut sessioned: BTreeSet<&str> = BTreeSet::new();
            for e in idx
                .exits
                .iter()
                .filter(|e| e.repo == scope.repo && e.task == scope.task && e.spawn == spawn)
            {
                exits_total += 1;
                sessioned.insert(e.session.as_str());
                let ms = match (e.launched_at, e.exited_at) {
                    (Some(l), Some(x)) => Some((x - l).num_milliseconds()),
                    _ => None,
                };
                opt_sum(&mut lifetime, ms);
                launches.push(LaunchObservation {
                    session: Some(e.session.clone()),
                    observed_via: AGENT_EXIT.into(),
                    launched_at: e.launched_at.map(|t| t.to_rfc3339()),
                    exited_at: e.exited_at.map(|t| t.to_rfc3339()),
                    exit_record: Some(e.record.clone()),
                    exit_code: e.exit_code,
                    crashed: e.crashed,
                    prior_state: e.prior_state.clone(),
                    process_lifetime_ms: ms,
                });
            }
            // A session that reported usage but never an exit: a real launch
            // whose termination this capture does not witness.
            let mut usage_only: BTreeSet<&str> = BTreeSet::new();
            for u in idx
                .usage
                .iter()
                .filter(|u| u.repo == scope.repo && u.task == scope.task && u.spawn == spawn)
            {
                if !sessioned.contains(u.session.as_str()) {
                    usage_only.insert(u.session.as_str());
                }
            }
            for session in &usage_only {
                launches.push(LaunchObservation {
                    session: Some((*session).to_string()),
                    observed_via: AGENT_FINAL_USAGE.into(),
                    launched_at: None,
                    exited_at: None,
                    exit_record: None,
                    exit_code: None,
                    crashed: None,
                    prior_state: None,
                    process_lifetime_ms: None,
                });
            }
            // Launch events carry no session, so they cannot be matched to the attempts above one-for-one.
            let mut launch_events: Vec<&LaunchRow> = idx
                .launches
                .iter()
                .filter(|l| {
                    l.repo == scope.repo
                        && l.task == scope.task
                        && l.spawn == spawn
                        && l.kind == LAUNCH_EVENT_KIND
                })
                .collect();
            launch_events.sort_by(|a, b| (a.at, &a.record).cmp(&(b.at, &b.record)));
            let accounted = sessioned.len() + usage_only.len();
            for l in launch_events.iter().skip(accounted) {
                launches.push(LaunchObservation {
                    session: None,
                    observed_via: LAUNCH_EVENT_KIND.into(),
                    launched_at: l.at.map(|t| t.to_rfc3339()),
                    exited_at: None,
                    exit_record: None,
                    exit_code: None,
                    crashed: None,
                    prior_state: None,
                    process_lifetime_ms: None,
                });
            }
            launches_total += launches.len();
            launches.sort_by(|a, b| {
                (&a.session, &a.launched_at, &a.exit_record).cmp(&(
                    &b.session,
                    &b.launched_at,
                    &b.exit_record,
                ))
            });

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
            let ordered = capture.order == Order::PersistenceSequence;
            for ((session, provider_session), mut rows) in by_segment {
                any_segment = true;
                // WHICH record is the segment's last is a persistence-order question, and `observed_at` is a producer-stamped field, not persistence order.
                if ordered {
                    rows.sort_by(|a, b| (a.position, &a.record).cmp(&(b.position, &b.record)));
                } else {
                    rows.sort_by(|a, b| {
                        (a.observed_at, &a.record).cmp(&(b.observed_at, &b.record))
                    });
                }
                let last = rows.last().copied().expect("segment has at least one row");
                let exit = idx.exits.iter().find(|e| {
                    e.repo == scope.repo
                        && e.task == scope.task
                        && e.agent == last.agent
                        && e.spawn == spawn
                        && e.session == session
                });
                // One row IS its own last, whatever the order; only a
                // multi-row segment needs persistence order to resolve.
                let (final_cost, finality_reason) = if rows.len() > 1 && !ordered {
                    (
                        false,
                        format!(
                            "{} cumulative usage results in this segment but the capture declares \
                             no validated persistence order, so the last one — the \
                             segment's actual total — cannot be identified",
                            rows.len()
                        ),
                    )
                } else {
                    segment_finality(last, exit)
                };
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
                tracked_by: "no native observation can close this gap; ReviewedAnnotation \
                             supplies a separate, evidenced figure rather than closing it"
                    .into(),
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
        "closure: complete={} references={} merged={} duplicates={} missing={}",
        match report.capture.closure_complete {
            Some(true) => "yes".to_string(),
            Some(false) => format!(
                "no (budget_exhausted={})",
                report.capture.reference_budget_exhausted
            ),
            None => "undeclared".to_string(),
        },
        report.capture.references,
        report.capture.references_merged,
        report.capture.duplicate_references,
        report.capture.missing_references.len()
    );
    if let Some(reason) = &report.capture.order_claim_rejected {
        let _ = writeln!(out, "order claim REFUSED: {reason}");
    }
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
                 verification_ms={:?} (legacy_spans={}) attention_hold_ms={:?} queue_wait_ms={:?}",
                d.process_lifetime_ms,
                d.phase_ms.work_phases_ms,
                d.phase_ms.verification_ms,
                d.phase_ms.verification_ms_legacy_spans,
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
            "identity": format!("bbs-finding-{}", digest_key(id)),
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
            "identity": format!("bbs-reuse-{}", digest_key(id)),
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
            "identity": format!("bbs-assessment-{}", digest_key(id)),
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

    /// A real castle wire author id: `castle-<16 lowercase hex>`
    /// (`rk_core::identity::actor_from_pubkey`). Taken from the captured
    /// native records rather than shortened, because the author SHAPE is now
    /// part of what a native record has to prove.
    const CASTLE: &str = "castle-48451de05dc5e21a";

    /// A producer-shaped identity key. `canonical_digest` returns a lowercase
    /// hex SHA-256, so every native `bbs-finding-`/`bbs-answer-`/`bbs-reuse-`/
    /// `bbs-assessment-` suffix is exactly 64 lowercase hex characters. Tests
    /// derive a stable one per seed instead of a short readable stand-in the
    /// real producer could never mint.
    fn digest_key(seed: &str) -> String {
        let mut key = String::with_capacity(DIGEST_HEX_LEN);
        for (i, b) in seed.bytes().cycle().take(DIGEST_HEX_LEN / 2).enumerate() {
            key.push_str(&format!("{:02x}", b ^ (i as u8)));
        }
        key
    }

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

    /// `agent_exit` (S2 contract, identity `bbs-agent-exit`): castle-authored physical-exit evidence, never `harness_result`.
    fn exit_rec(
        id: &str,
        task: &str,
        agent: &str,
        spawn: &str,
        launched: &str,
        exited: &str,
    ) -> Value {
        json!({
            "id": id, "category": "event", "scope": "repo",
            "identity": "bbs-agent-exit", "instance": CASTLE,
            "lifecycle": "furniture", "created_at": exited,
            "payload": {
                "schema_version": 1, "bbs_kind": AGENT_EXIT, "repo": "repo", "task": task,
                "agent": agent, "spawn": spawn, "session": "sess-1",
                "launched_at": launched, "exited_at": exited, "prior_state": null,
                "crashed": false, "exit_code": 0, "stale_session": null,
                "cost_coverage": FINAL_COST_COVERAGE
            }
        })
    }

    /// Restate one payload field of a fixture, so a test states only what it
    /// is about.
    fn field(mut t: Value, key: &str, v: Value) -> Value {
        t["payload"][key] = v;
        t
    }

    /// `agent_final_usage` (S2 contract, identity `bbs-agent-final-usage`): castle-authored provider-cost evidence, keyed with `agent_exit` on `(spawn, session)`.
    fn usage_rec(
        id: &str,
        task: &str,
        spawn: &str,
        state: Option<&str>,
        cost_usd: Option<f64>,
        observed: &str,
    ) -> Value {
        json!({
            "id": id, "category": "event", "scope": "repo",
            "identity": "bbs-agent-final-usage", "instance": CASTLE,
            "lifecycle": "furniture", "created_at": observed,
            "payload": {
                "schema_version": 1, "bbs_kind": AGENT_FINAL_USAGE, "repo": "repo",
                "task": task, "agent": spawn, "spawn": spawn, "session": "sess-1",
                "provider_session": "ps-1", "state": state, "cost_usd": cost_usd,
                "cost_basis": PROVIDER_COST_BASIS, "observed_at": observed
            }
        })
    }

    /// `record_phase_span`'s exact envelope (crates/rk-daemon/src/span.rs):
    /// a castle-authored `Furniture` Event under `task_span`, scoped to the
    /// ticket's repo, carrying `PhaseSpan::to_payload()`.
    fn span_full(id: &str, payload: Value) -> Value {
        json!({
            "id": id,
            "category": "event",
            "scope": "repo",
            "identity": SPAN_IDENTITY,
            "instance": "daemon",
            "lifecycle": "furniture",
            "created_at": "2026-01-01T00:00:00Z",
            "payload": payload
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
            order_claim_rejected: None,
            tuples,
            references: Vec::new(),
            truncated: false,
            coverage: CaptureCoverage::default(),
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

    /// Stamps `commit_sequence` the way the daemon does, so an `order`
    /// claim has something to be checked against.
    fn sequenced(records: Vec<Value>, start: u64) -> Vec<Value> {
        records
            .into_iter()
            .enumerate()
            .map(|(i, mut r)| {
                r["commit_sequence"] = json!(start + i as u64);
                r
            })
            .collect()
    }

    /// A real `bbs.export` envelope, shaped as `rk-daemon`'s `bbs.rs`
    /// emits it: the page in `tuples`, the resolved evidence closure in
    /// `references` (id-sorted, each carrying its own AS-OF sequence), and a
    /// `coverage` block stating whether that closure is complete.
    fn export_envelope(page: Vec<Value>, references: Vec<Value>, missing: Vec<&str>) -> Value {
        let complete = missing.is_empty();
        json!({
            "schema_version": 1,
            "kind": "bbs.export",
            "repo": "repo",
            "order": "persistence_sequence",
            "order_provenance": "tuple_persistence_events.commit_sequence ascending",
            "source": "space.persistence_page",
            "truncated": false,
            "tuples": sequenced(page, 1),
            "references": sequenced(references, 1_000),
            "coverage": {
                "missing_references": missing,
                "complete": complete,
                "reference_budget_exhausted": false,
                "scope": "repo",
            },
        })
    }

    /// The evidence a receipt and a verdict name does not have to be on the
    /// page: the daemon resolves it into `references`. A reporter that reads
    /// only `tuples` treats that evidence as ABSENT and throws the verdict
    /// away — the whole point of shipping the closure.
    #[test]
    fn export_references_are_merged_so_linked_evidence_resolves() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let evidence: Vec<Value> = ["ev-1", "ev-2", "ev-3"]
            .iter()
            .map(|id| {
                json!({
                    "id": id, "category": "artifact", "scope": "repo", "identity": id,
                    "instance": "x", "created_at": "2026-01-01T00:00:00Z", "payload": {}
                })
            })
            .collect();
        let envelope = export_envelope(verified_tuples(), evidence, vec![]);
        let c = parse_tuple_capture(&envelope).unwrap();
        assert_eq!(c.order, Order::PersistenceSequence);
        assert_eq!(c.references.len(), 3);

        let r = compute(&m, &c, &[]).unwrap();
        assert_eq!(c.tuples.len(), 5, "the evidence is NOT on the page");
        assert_eq!(r.capture.references_merged, 3);
        assert_eq!(r.capture.duplicate_references, 0);
        assert_eq!(r.capture.closure_complete, Some(true));
        assert_eq!(
            r.pairs[0].assessed_verdict.as_deref(),
            Some("verified"),
            "the verdict's evidence resolved out of the closure, not the page"
        );
        assert_eq!(r.outcome_classes.verified, 1);
        assert!(r.mechanism.goal_blocked_reason.is_none());
    }

    /// The same envelope with the closure UNRESOLVED. An unresolvable
    /// reference is a hole in the capture, not proof the evidence never
    /// existed: the verdict drops to unresolved, the hole is named, and the
    /// mechanism goal is blocked exactly as truncation blocks it.
    #[test]
    fn incomplete_closure_is_reported_and_blocks_the_mechanism_goal() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let envelope = export_envelope(verified_tuples(), vec![], vec!["ev-2"]);
        let c = parse_tuple_capture(&envelope).unwrap();
        let r = compute(&m, &c, &[]).unwrap();

        assert_eq!(r.capture.closure_complete, Some(false));
        assert_eq!(r.capture.missing_references, vec!["ev-2".to_string()]);
        assert!(
            r.unresolved_records
                .iter()
                .any(|u| u.record == "ev-2" && u.kind == "missing_reference"),
            "the hole is named: {:?}",
            r.unresolved_records
        );
        let blocked = r
            .mechanism
            .goal_blocked_reason
            .expect("an incomplete closure cannot certify the goal");
        assert!(blocked.contains("incomplete evidence closure"), "{blocked}");
        assert!(!r.mechanism.goal_met);
    }

    /// `order: "persistence_sequence"` is a string anyone can write into an
    /// envelope. It is accepted only when the records carry the ascending
    /// `commit_sequence` positions that make it true — otherwise the capture
    /// is downgraded to unknown order WITH the refusal reason, rather than
    /// silently resolving supersession off an unproven claim.
    #[test]
    fn unsubstantiated_persistence_order_claim_is_refused() {
        let bare = json!({
            "order": "persistence_sequence",
            "tuples": [finding("src-1", "author-gen", "2026-01-01T00:00:00Z")],
        });
        let c = parse_tuple_capture(&bare).unwrap();
        assert_eq!(c.order, Order::Unknown, "no commit_sequence to check");
        assert!(c
            .order_claim_rejected
            .as_deref()
            .is_some_and(|r| r.contains("no numeric commit_sequence")));

        let mut out_of_order = export_envelope(
            vec![
                finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
                finding("src-2", "author-gen", "2026-01-01T00:00:00Z"),
            ],
            vec![],
            vec![],
        );
        out_of_order["tuples"][1]["commit_sequence"] = json!(1);
        let c = parse_tuple_capture(&out_of_order).unwrap();
        assert_eq!(c.order, Order::Unknown, "not ascending");
        assert!(c
            .order_claim_rejected
            .as_deref()
            .is_some_and(|r| r.contains("not in ascending persistence order")));

        let mut unplaced = export_envelope(
            vec![finding("src-1", "author-gen", "2026-01-01T00:00:00Z")],
            vec![json!({
                "id": "ev-1", "category": "artifact", "scope": "repo", "identity": "ev-1",
                "instance": "x", "created_at": "2026-01-01T00:00:00Z", "payload": {}
            })],
            vec![],
        );
        unplaced["references"][0]
            .as_object_mut()
            .unwrap()
            .remove("commit_sequence");
        let c = parse_tuple_capture(&unplaced).unwrap();
        assert_eq!(c.order, Order::Unknown, "a reference has no position");
        assert!(c
            .order_claim_rejected
            .as_deref()
            .is_some_and(|r| r.contains("reference record")));
    }

    /// A legacy raw `rk --json scan` object still parses, still reports
    /// unknown order, and is NOT refused: compatibility without inventing a
    /// persistence order the shape cannot supply.
    #[test]
    fn legacy_raw_scan_is_unknown_order_without_a_refusal() {
        let scan = json!({"tuples": [], "truncated": false});
        let c = parse_tuple_capture(&scan).unwrap();
        assert_eq!(c.order, Order::Unknown);
        assert_eq!(c.order_claim_rejected, None);
        assert_eq!(c.coverage.complete, None, "a scan makes no closure claim");
    }

    /// A generation observed ONLY through its launch event — spawned and
    /// still running, or crashed before any completion, exit or usage was
    /// recorded — is a real enrolled attempt. Seeding deliveries from
    /// completions/exits/usage alone dropped it entirely.
    #[test]
    fn a_generation_with_only_a_launch_event_is_still_an_enrolled_attempt() {
        let m = task_manifest("TKT-live");
        let spawned = json!({
            "id": "e1", "category": "event", "scope": "repo", "identity": "agent_spawned",
            "instance": CASTLE, "created_at": "2026-01-01T00:00:00Z",
            "payload": {"agent": "Live-1", "spawn": "gen-live", "task": "TKT-live"}
        });
        let r = compute(&m, &capture(vec![spawned], Order::Unknown), &[]).unwrap();
        let d = &r.deliveries[0];
        assert_eq!(d.generations.len(), 1, "the live generation is retained");
        assert_eq!(d.generations[0].spawn, "gen-live");
        assert_eq!(d.launches, 1, "a launch with no exit is still an attempt");
        assert_eq!(d.exits, 0);
        assert_eq!(d.generations[0].launches[0].session, None);
        assert_eq!(d.generations[0].launches[0].observed_via, "launch_event");
        assert!(
            r.tasks_without_native_records.is_empty(),
            "the task DOES have a native record"
        );
    }

    /// Two launches, one exited and one still live. Counting exits reported
    /// one attempt; the second launch event is the excess over the sessions
    /// already accounted for and is counted as the attempt it is.
    #[test]
    fn a_relaunch_with_no_exit_yet_is_counted_as_a_further_attempt() {
        let m = task_manifest("TKT-relaunch");
        let event = |id: &str, identity: &str, at: &str| {
            json!({
                "id": id, "category": "event", "scope": "repo", "identity": identity,
                "instance": CASTLE, "created_at": at,
                "payload": {"agent": "R-1", "spawn": "gen-r", "task": "TKT-relaunch"}
            })
        };
        let tuples = vec![
            event("e1", "agent_spawned", "2026-01-01T00:00:00Z"),
            event("e2", "agent_respawned", "2026-01-01T02:00:00Z"),
            exit_rec(
                "x1",
                "TKT-relaunch",
                "R-1",
                "gen-r",
                "2026-01-01T00:00:00Z",
                "2026-01-01T01:00:00Z",
            ),
        ];
        let r = compute(&m, &capture(tuples, Order::Unknown), &[]).unwrap();
        let d = &r.deliveries[0];
        assert_eq!(d.exits, 1);
        assert_eq!(d.launches, 2, "the un-exited relaunch is not dropped");
        let sessions: Vec<Option<String>> = d.generations[0]
            .launches
            .iter()
            .map(|l| l.session.clone())
            .collect();
        assert_eq!(sessions, vec![None, Some("sess-1".into())]);
    }

    /// Within one provider segment the reported cost is CUMULATIVE, so the
    /// segment's total is whichever record persisted last. `observed_at` is
    /// stamped by the producer and can disagree with persistence order; under
    /// a validated capture the position decides.
    #[test]
    fn cumulative_segment_total_comes_from_persistence_position_not_observed_at() {
        let m = task_manifest("TKT-cost");
        let usage = |id: &str, cost: f64, observed: &str| {
            usage_rec(
                id,
                "TKT-cost",
                "gen-c",
                Some("completed"),
                Some(cost),
                observed,
            )
        };
        let exit = field(
            exit_rec(
                "x1",
                "TKT-cost",
                "C-1",
                "gen-c",
                "2026-01-01T00:00:00Z",
                "2026-01-01T03:00:00Z",
            ),
            "prior_state",
            json!("completed"),
        );
        // Page order (= persistence order) puts the 2.00 total LAST, while
        // its observed_at is EARLIER than the 1.00 row's.
        let page = vec![
            usage("u1", 1.00, "2026-01-01T02:00:00Z"),
            usage("u2", 2.00, "2026-01-01T01:00:00Z"),
            exit,
        ];
        let c = parse_tuple_capture(&export_envelope(page.clone(), vec![], vec![])).unwrap();
        assert_eq!(c.order, Order::PersistenceSequence);
        let r = compute(&m, &c, &[]).unwrap();
        let d = &r.deliveries[0];
        assert_eq!(d.cost_coverage, "complete");
        assert_eq!(
            d.reported_cost_estimate_usd,
            Some(2.00),
            "the last PERSISTED cumulative total, not the latest observed_at"
        );

        // The same records with no order claim: the last row is genuinely
        // unidentifiable, so the segment is refused finality instead of
        // being resolved by a field that cannot answer the question.
        let unordered = compute(&m, &capture(page, Order::Unknown), &[]).unwrap();
        let d = &unordered.deliveries[0];
        assert_eq!(d.cost_coverage, "partial");
        assert_eq!(d.reported_cost_estimate_usd, None);
        assert!(
            d.unknown_cost
                .iter()
                .any(|u| u.contains("no validated persistence order")),
            "{:?}",
            d.unknown_cost
        );
    }

    /// A single-row segment IS its own last record whatever the order, so an
    /// unordered capture must not manufacture a finality gap for it.
    #[test]
    fn a_single_row_segment_stays_final_under_unknown_order() {
        let m = task_manifest("TKT-one");
        let tuples = vec![
            usage_rec(
                "u1",
                "TKT-one",
                "gen-o",
                Some("completed"),
                Some(0.75),
                "2026-01-01T01:00:00Z",
            ),
            field(
                exit_rec(
                    "x1",
                    "TKT-one",
                    "O-1",
                    "gen-o",
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T02:00:00Z",
                ),
                "prior_state",
                json!("completed"),
            ),
        ];
        let r = compute(&m, &capture(tuples, Order::Unknown), &[]).unwrap();
        let d = &r.deliveries[0];
        assert_eq!(d.cost_coverage, "complete");
        assert_eq!(d.reported_cost_estimate_usd, Some(0.75));
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
        let m = task_manifest("TKT-lonely");
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

    /// Only additive-tagged spans contribute durations; retain untagged spans as legacy coverage.
    #[test]
    fn verification_ms_sums_only_additive_tagged_spans_and_counts_the_rest_as_legacy() {
        let m = task_manifest("TKT-verify");
        // `from_durations` anchors `ended_at` at now and derives the two
        // earlier stamps, so a real additive span carries all three.
        let additive_span = span_full(
            "span-additive",
            json!({
                "task": "TKT-verify", "phase": "verification", "attempt": 1,
                "queued_at": "2026-01-01T00:00:00Z",
                "started_at": "2026-01-01T00:00:00.500Z",
                "ended_at": "2026-01-01T00:00:00.550Z",
                "queue_wait_ms": 500, "duration_ms": 50,
                "duration_semantic": ADDITIVE_DURATION, "repo": "repo"
            }),
        );
        // A span recorded before the tag existed. The legacy dimension is the
        // MISSING `duration_semantic`, not missing timestamps: `to_payload`
        // has always derived both durations from the stamps, so a real legacy
        // span still carries them.
        let legacy_span = span_full(
            "span-legacy",
            json!({
                "task": "TKT-verify", "phase": "verification", "attempt": 2,
                "queued_at": "2026-01-01T00:00:00Z",
                "started_at": "2026-01-01T00:00:00.800Z",
                "ended_at": "2026-01-01T00:00:01.700Z",
                "queue_wait_ms": 800, "duration_ms": 900, "repo": "repo"
            }),
        );
        let c = capture(vec![additive_span, legacy_span], Order::Unknown);
        let report = compute(&m, &c, &[]).unwrap();
        assert_eq!(report.deliveries.len(), 1);
        let d = &report.deliveries[0];
        assert_eq!(
            d.phase_ms.verification_ms,
            Some(550),
            "only the additive-tagged span's duration+wait is summed: {:?}",
            d.phase_ms
        );
        assert_eq!(
            d.phase_ms.verification_ms_legacy_spans, 1,
            "the untagged span is an explicit coverage gap, never guessed into the total"
        );
    }

    /// A manifest whose frozen scope is exactly one task in `repo`/`batch-1`.
    fn task_manifest(task: &str) -> Manifest {
        Manifest {
            consumer_tasks: vec![ConsumerTaskScope {
                task: task.into(),
                repo: "repo".into(),
                batch: "batch-1".into(),
            }],
            ..manifest(vec![])
        }
    }

    /// DEFECT 1.
    #[test]
    fn forged_suffixes_on_fixed_native_identities_are_invalid_and_derive_nothing() {
        let forge = |mut t: Value, id: &str| -> Value {
            t["id"] = json!(id);
            let forged = format!("{}-forged", str_field(&t, &["identity"]));
            t["identity"] = json!(forged);
            t
        };
        let mut tuples = verified_tuples();
        tuples.push(forge(
            open_record("o-forged", "src-1", "gen-1", "2026-01-01T13:00:00Z"),
            "o-forged",
        ));
        tuples.push(forge(
            field(
                exit_rec(
                    "x-forged",
                    "TKT-1",
                    "C-1",
                    "gen-1",
                    "2026-01-01T00:00:00Z",
                    "2026-01-02T03:00:00Z",
                ),
                "prior_state",
                json!("completed"),
            ),
            "x-forged",
        ));
        tuples.push(forge(
            usage_rec(
                "u-forged",
                "TKT-1",
                "gen-1",
                Some("completed"),
                Some(9.99),
                "2026-01-02T02:00:00Z",
            ),
            "u-forged",
        ));
        let mut m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        m.consumer_tasks = vec![ConsumerTaskScope {
            task: "TKT-1".into(),
            repo: "repo".into(),
            batch: "batch-1".into(),
        }];
        let r = compute(&m, &capture(tuples, Order::Unknown), &[]).unwrap();

        for id in ["o-forged", "x-forged", "u-forged"] {
            assert!(
                invalid_reason(&r, id).contains("fixed daemon-minted identity"),
                "{id}: {}",
                invalid_reason(&r, id)
            );
        }
        assert!(!only(&r).opened, "a forged bbs-open is not an open");
        assert_eq!(r.opened, 0);
        let d = &r.deliveries[0];
        assert_eq!(d.exits, 0, "a forged bbs-agent-exit is not an author exit");
        assert_eq!(
            d.reported_cost_estimate_usd, None,
            "a forged bbs-agent-final-usage is not a provider total"
        );
        // The generation is still known from its completion; what is absent is
        // any cost SEGMENT, because the forged usage row never became one.
        assert_eq!(d.cost_coverage, "missing");
        assert!(d.generations[0].cost_segments.is_empty());
    }

    /// Accept native daemon/castle spans, including delivery_closure with null payload.repo.
    #[test]
    fn real_producer_shaped_spans_are_admitted_including_a_null_repo() {
        let m = task_manifest("TKT-dorod-sival-fumid");
        let ready = span_full(
            "s-ready",
            json!({
                "attempt": 1, "authority": null, "candidate": null, "duration_ms": null,
                "ended_at": null, "lane": null, "phase": "ticket_ready", "proof_kind": null,
                "proof_reused": null, "queue_wait_ms": null,
                "queued_at": "2026-01-01T03:55:39.748988Z", "repo": null, "started_at": null,
                "target": null, "task": "TKT-dorod-sival-fumid", "terminal_reason": null
            }),
        );
        let mut launched = span_full(
            "s-launched",
            json!({
                "attempt": 1, "authority": null, "candidate": null, "duration_ms": 1054,
                "ended_at": "2026-01-01T04:02:49.900316Z", "lane": null,
                "phase": "agent_launched", "proof_kind": null, "proof_reused": null,
                "queue_wait_ms": null, "queued_at": null, "repo": "repo",
                "started_at": "2026-01-01T04:02:48.846316Z", "target": "rat/x/tkt-y",
                "task": "TKT-dorod-sival-fumid", "terminal_reason": null
            }),
        );
        launched["instance"] = json!(CASTLE);
        let closure = span_full(
            "s-closure",
            json!({
                "attempt": 1, "phase": "delivery_closure", "repo": null,
                "task": "TKT-dorod-sival-fumid", "ended_at": "2026-01-01T05:00:00Z",
                "terminal_reason": "delivered"
            }),
        );
        let r = compute(
            &m,
            &capture(vec![ready, launched, closure], Order::Unknown),
            &[],
        )
        .unwrap();
        assert!(r.invalid_records.is_empty(), "{:?}", r.invalid_records);
        assert!(
            r.tasks_without_native_records.is_empty(),
            "all three spans are usable native evidence"
        );
    }

    /// DEFECT 2.
    #[test]
    fn malformed_or_forged_spans_are_rejected_without_erasing_the_selected_task() {
        let base = json!({
            "task": "TKT-span", "phase": "verification", "attempt": 1,
            "queued_at": "2026-01-01T00:00:00Z", "started_at": "2026-01-01T00:00:00.500Z",
            "ended_at": "2026-01-01T00:00:00.550Z", "queue_wait_ms": 500, "duration_ms": 50,
            "duration_semantic": ADDITIVE_DURATION, "repo": "repo"
        });
        let mutate = |id: &str, f: &dyn Fn(&mut Value)| -> Value {
            let mut payload = base.clone();
            f(&mut payload);
            span_full(id, payload)
        };
        // A worker generation cannot author a span about itself, however well
        // formed the rest of it is: `record_phase_span` names the castle.
        let mut worker_authored = span_full("s-worker", base.clone());
        worker_authored["instance"] = json!("Scritch-15");
        let mut ephemeral = span_full("s-ephemeral", base.clone());
        ephemeral["lifecycle"] = json!("ephemeral");

        let cases: Vec<(Value, &str)> = vec![
            (worker_authored, "is not a castle author"),
            (ephemeral, "not furniture"),
            (
                mutate("s-phase", &|p| p["phase"] = json!("verification_extra")),
                "is not a phase this daemon records",
            ),
            (
                mutate("s-attempt", &|p| p["attempt"] = json!(0)),
                "positive integer occurrence number",
            ),
            (
                mutate("s-repo", &|p| p["repo"] = json!("other-repo")),
                "disagrees with tuple scope",
            ),
            (
                mutate("s-negative", &|p| {
                    p["duration_ms"] = json!(-50);
                }),
                "not a non-negative duration",
            ),
            // A fabricated duration with no endpoints to derive it from. The
            // producer cannot state one without both stamps.
            (
                mutate("s-unanchored", &|p| {
                    p["duration_ms"] = json!(9_000_000);
                    p["started_at"] = Value::Null;
                    p["ended_at"] = Value::Null;
                }),
                "are not both present",
            ),
            (
                mutate("s-disagree", &|p| p["duration_ms"] = json!(9_000_000)),
                "the producer derives one from the other",
            ),
            (
                mutate("s-backwards", &|p| {
                    p["ended_at"] = json!("2025-01-01T00:00:00Z");
                }),
                "cannot run backwards",
            ),
            (
                mutate("s-semantic", &|p| p["duration_semantic"] = json!("total")),
                "duration_semantic",
            ),
            (
                mutate("s-authority", &|p| p["authority"] = json!("king")),
                "not human or llm",
            ),
            (
                mutate("s-fence", &|p| p["occurrence_key"] = json!("")),
                "not a non-empty string",
            ),
        ];

        let m = task_manifest("TKT-span");
        for (span, expected) in cases {
            let id = span["id"].as_str().unwrap().to_string();
            let r = compute(&m, &capture(vec![span], Order::Unknown), &[]).unwrap();
            assert!(
                invalid_reason(&r, &id).contains(expected),
                "{id}: expected {expected:?}, got {:?}",
                invalid_reason(&r, &id)
            );
            assert_eq!(
                r.deliveries.len(),
                1,
                "{id}: a malformed span must not erase the selected task from the denominator"
            );
            assert_eq!(
                r.deliveries[0].phase_ms.verification_ms, None,
                "{id}: a rejected span contributes no verification time"
            );
            assert_eq!(
                r.tasks_without_native_records,
                vec!["repo/TKT-span".to_string()],
                "{id}: the task is reported as having no usable native record, not dropped"
            );
        }
    }

    /// DEFECT 3.
    #[test]
    fn a_matching_exit_state_is_not_a_final_cost_unless_coverage_says_final() {
        let m = task_manifest("TKT-cost");
        let delivery_for = |coverage: &Value| -> DeliveryCost {
            // Exactly the state-only condition the old check accepted:
            // terminal result, observed exit, prior_state agreeing.
            let exit = exit_rec(
                "x1",
                "TKT-cost",
                "C-1",
                "gen-c",
                "2026-01-01T00:00:00Z",
                "2026-01-01T03:00:00Z",
            );
            let exit = field(exit, "prior_state", json!("completed"));
            let exit = field(exit, "cost_coverage", coverage.clone());
            let usage = usage_rec(
                "u1",
                "TKT-cost",
                "gen-c",
                Some("completed"),
                Some(2.00),
                "2026-01-01T02:00:00Z",
            );
            let mut r = compute(&m, &capture(vec![usage, exit], Order::Unknown), &[]).unwrap();
            r.deliveries.remove(0)
        };

        let d = delivery_for(&json!(FINAL_COST_COVERAGE));
        assert_eq!(d.cost_coverage, "complete");
        assert_eq!(d.reported_cost_estimate_usd, Some(2.00));
        assert!(d.generations[0].cost_segments[0].final_cost);

        for coverage in [
            json!("partial_unknown"),
            json!("none"),
            json!("unknown"),
            Value::Null,
            json!("mostly"),
        ] {
            let d = delivery_for(&coverage);
            assert_eq!(d.cost_coverage, "partial", "coverage={coverage}");
            assert_eq!(
                d.reported_cost_estimate_usd, None,
                "coverage={coverage}: must never reach a provider total"
            );
            assert_eq!(
                d.partial_reported_usd,
                Some(2.00),
                "coverage={coverage}: the supported partial amount is preserved"
            );
            assert_eq!(
                d.unknown_cost.len(),
                1,
                "coverage={coverage}: the uncertainty is named, not silent"
            );
            // Cost uncertainty is not exit uncertainty: the same record is
            // still the launch's physical-exit evidence.
            assert_eq!(
                d.exits, 1,
                "coverage={coverage}: a non-final cost does not retract the physical exit"
            );
            assert_eq!(d.launches, 1, "coverage={coverage}");
        }
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
            exit_rec(
                "ax1",
                "TKT-source",
                "author",
                "author-gen",
                "2026-01-01T00:00:00Z",
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
        assert_eq!(only(&r).author_terminal_evidence, Some("ax1".to_string()));
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
            field(
                exit_rec(
                    "ax1",
                    "TKT-source",
                    "author",
                    "author-gen",
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T12:00:00Z",
                ),
                "stale_session",
                json!(true),
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
        let m = task_manifest("TKT-1");
        let tuples = vec![
            field(
                usage_rec(
                    "u1",
                    "TKT-1",
                    "gen-1",
                    Some("completed"),
                    Some(1.5),
                    "2026-01-01T11:00:00Z",
                ),
                "provider_session",
                json!("prov-1"),
            ),
            field(
                exit_rec(
                    "ax1",
                    "TKT-1",
                    "gen-1",
                    "gen-1",
                    "2026-01-01T10:00:00Z",
                    "2026-01-01T12:00:00Z",
                ),
                "prior_state",
                json!("completed"),
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
        let m = task_manifest("TKT-1");
        let tuples = vec![
            field(
                usage_rec(
                    "u1",
                    "TKT-1",
                    "gen-1",
                    Some("paused"),
                    Some(3.0),
                    "2026-01-01T11:00:00Z",
                ),
                "provider_session",
                json!("prov-1"),
            ),
            field(
                exit_rec(
                    "ax1",
                    "TKT-1",
                    "gen-1",
                    "gen-1",
                    "2026-01-01T10:00:00Z",
                    "2026-01-01T12:00:00Z",
                ),
                "prior_state",
                json!("running"),
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
    fn an_exit_without_a_prior_state_cannot_confirm_a_final_cost() {
        // Healthy otherwise: only prior_state is absent, so agreement is unmet.
        let m = task_manifest("TKT-1");
        let tuples = vec![
            usage_rec(
                "u1",
                "TKT-1",
                "gen-1",
                Some("completed"),
                Some(4.25),
                "2026-01-01T11:00:00Z",
            ),
            field(
                exit_rec(
                    "ax1",
                    "TKT-1",
                    "gen-1",
                    "gen-1",
                    "2026-01-01T10:00:00Z",
                    "2026-01-01T12:00:00Z",
                ),
                "prior_state",
                Value::Null,
            ),
        ];
        let r = compute(&m, &capture(tuples, Order::Unknown), &[]).unwrap();
        let d = &r.deliveries[0];
        assert_eq!(d.cost_coverage, "partial");
        assert_eq!(d.reported_cost_estimate_usd, None, "never a provider total");
        assert_eq!(
            d.partial_reported_usd,
            Some(4.25),
            "partial amount preserved"
        );
        assert_eq!(d.unknown_cost.len(), 1, "the uncertainty is named");
        assert!(
            d.unknown_cost[0].contains("no prior_state"),
            "{:?}",
            d.unknown_cost[0]
        );
        // Bounding the cost does not retract the physical exit evidence.
        assert_eq!(d.exits, 1);
        assert_eq!(d.launches, 1);
        assert_eq!(d.process_lifetime_ms, Some(2 * 60 * 60 * 1000));
    }

    #[test]
    fn cost_is_missing_when_no_final_usage_observation_exists() {
        let m = task_manifest("TKT-1");
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
        let mut m = task_manifest("TKT-1");
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
        assert_eq!(
            d.reviewed_interventions.len(),
            0,
            "unresolved evidence is not counted"
        );
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
        assert_eq!(
            named,
            vec!["active_work_ms", "total_operator_interventions"]
        );
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
    fn assessment_window_cases_preserve_the_opportunity_and_only_credit_in_window() {
        let mut m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        m.window = Window {
            since: Some("2026-01-02T00:00:00Z".parse().unwrap()),
            until: Some("2026-01-03T00:00:00Z".parse().unwrap()),
        };
        for (timestamp, expected) in [
            (Some("2026-01-02T14:00:00Z"), 1),
            (Some("2026-01-01T14:00:00Z"), 0),
            (Some("2026-01-04T14:00:00Z"), 0),
            (None, 0),
        ] {
            let mut verdict = assessment(
                "a1",
                "r1",
                "verified",
                timestamp.unwrap_or("2026-01-02T14:00:00Z"),
            );
            if timestamp.is_none() {
                verdict.as_object_mut().unwrap().remove("created_at");
            }
            let c = capture(
                vec![
                    finding("src-1", "author-gen", "2026-01-01T00:00:00Z"),
                    exposure("x1", "src-1", "gen-1", "2026-01-02T12:00:00Z"),
                    reuse(
                        "r1",
                        "src-1",
                        "TKT-1",
                        "gen-1",
                        "used",
                        "2026-01-02T13:00:00Z",
                    ),
                    verdict,
                ],
                Order::Unknown,
            );
            let r = compute(&m, &c, &[]).unwrap();
            assert_eq!(r.eligible, 1, "assessment timestamp {timestamp:?}");
            assert_eq!(r.claimed, 1, "assessment timestamp {timestamp:?}");
            assert_eq!(r.discovery.prepared_pairs, 1);
            assert_eq!(r.assessed, expected, "assessment timestamp {timestamp:?}");
            assert_eq!(r.verified_reuse.verified_used_or_adapted_tasks, expected);
            assert!(!r.mechanism.goal_met);
        }
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

    fn invalid_reason<'a>(r: &'a Report, record: &str) -> &'a str {
        r.invalid_records
            .iter()
            .find(|i| i.record == record)
            .map_or_else(
                || {
                    panic!(
                        "{record} was not retained as invalid: {:?}",
                        r.invalid_records
                    )
                },
                |i| i.reason.as_str(),
            )
    }

    /// DEFECT 1, variable identities. Their contract is preserved — any
    /// genuine key is accepted — but it must actually BE the key the producer
    /// mints. `canonical_digest` returns a 64-character lowercase hex SHA-256,
    /// so a short readable suffix is not something a real export can carry;
    /// and an exposure's suffix is one of four `ExposureSurface` values, so a
    /// forged surface is refused even when the payload agrees with it.
    #[test]
    fn variable_native_identities_must_carry_a_real_digest_or_a_real_surface() {
        let mut short_digest = finding("src-2", "author-gen", "2026-01-01T00:00:00Z");
        short_digest["identity"] = json!("bbs-finding-x");
        let mut uppercase_digest = finding("src-3", "author-gen", "2026-01-01T00:00:00Z");
        uppercase_digest["identity"] =
            json!(format!("bbs-finding-{}", digest_key("s").to_uppercase()));
        let mut forged_surface = exposure("x-forged", "src-1", "gen-1", "2026-01-01T12:00:00Z");
        forged_surface["identity"] = json!("bbs-exposure-forged");
        forged_surface["payload"]["surface"] = json!("forged");
        let mut mismatched = exposure("x-mismatch", "src-1", "gen-1", "2026-01-01T12:00:00Z");
        mismatched["identity"] = json!("bbs-exposure-brief");

        let mut tuples = verified_tuples();
        tuples.extend([short_digest, uppercase_digest, forged_surface, mismatched]);
        let r = compute(
            &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
            &capture(tuples, Order::Unknown),
            &[],
        )
        .unwrap();

        for id in ["src-2", "src-3"] {
            assert!(
                invalid_reason(&r, id).contains("canonical_digest"),
                "{id}: {}",
                invalid_reason(&r, id)
            );
        }
        assert!(invalid_reason(&r, "x-forged").contains("not an exposure surface"));
        assert!(invalid_reason(&r, "x-mismatch").contains("payload.surface"));
        // The real records alongside them are untouched.
        assert_eq!(
            only(&r).coverage_status,
            "prepared",
            "the genuine spawn exposure still establishes native coverage"
        );
    }

    /// DEFECT 1, positive baseline. Every real `ExposureSurface` must stay
    /// admissible: the rule is "the suffix is the surface the payload names",
    /// not a hardcoded preference for `spawn`.
    #[test]
    fn every_real_exposure_surface_is_still_native_coverage() {
        for surface in EXPOSURE_SURFACES {
            let mut x = exposure("x1", "src-1", "gen-1", "2026-01-01T12:00:00Z");
            x["identity"] = json!(format!("bbs-exposure-{surface}"));
            x["payload"]["surface"] = json!(surface);
            let mut tuples = verified_tuples();
            tuples[3] = x;
            let r = compute(
                &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
                &capture(tuples, Order::Unknown),
                &[],
            )
            .unwrap();
            assert!(
                r.invalid_records.is_empty(),
                "surface={surface}: {:?}",
                r.invalid_records
            );
            assert_eq!(only(&r).coverage_status, "prepared", "surface={surface}");
        }
    }

    #[test]
    fn forged_open_and_worker_authored_open_do_not_establish_read_evidence() {
        for worker_author in [false, true] {
            let mut open = open_record("bad-open", "src-1", "gen-1", "2026-01-01T13:00:00Z");
            if worker_author {
                open["instance"] = json!("consumer");
            } else {
                open["identity"] = json!("bbs-open-forged");
            }
            let mut tuples = verified_tuples();
            tuples.push(open);
            let report = compute(
                &manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]),
                &capture(tuples, Order::Unknown),
                &[],
            )
            .unwrap();
            let reason = invalid_reason(&report, "bad-open");
            assert!(reason.contains(if worker_author {
                "castle author"
            } else {
                "fixed daemon-minted identity"
            }));
            assert_eq!(report.opened, 0);
            assert!(!only(&report).opened);
            assert_eq!(report.eligible, 1);
        }
    }
    #[test]
    fn clover_cost_exit_requires_matching_task_and_agent() {
        let m = task_manifest("TKT-1");
        let usage = usage_rec(
            "u",
            "TKT-1",
            "gen-1",
            Some("completed"),
            Some(4.25),
            "2026-01-01T11:00:00Z",
        );
        let exit = field(
            exit_rec(
                "x",
                "TKT-1",
                "gen-1",
                "gen-1",
                "2026-01-01T10:00:00Z",
                "2026-01-01T12:00:00Z",
            ),
            "prior_state",
            json!("completed"),
        );
        let good = compute(
            &m,
            &capture(vec![usage.clone(), exit.clone()], Order::Unknown),
            &[],
        )
        .unwrap();
        assert_eq!(good.deliveries[0].reported_cost_estimate_usd, Some(4.25));
        for (key, value) in [("task", "TKT-other"), ("agent", "other-agent")] {
            let bad = field(exit.clone(), key, json!(value));
            let report =
                compute(&m, &capture(vec![usage.clone(), bad], Order::Unknown), &[]).unwrap();
            assert_eq!(report.deliveries.len(), 1);
            assert_eq!(
                report.deliveries[0].reported_cost_estimate_usd, None,
                "foreign {key} must not finalize cost"
            );
            assert_eq!(report.deliveries[0].partial_reported_usd, Some(4.25));
        }
    }

    #[test]
    fn clover_author_exit_requires_the_source_task() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        for task in ["TKT-source", "TKT-other", ""] {
            let mut tuples = verified_tuples();
            tuples.push(exit_rec(
                "x",
                task,
                "author",
                "author-gen",
                "2026-01-01T00:00:00Z",
                "2026-01-01T12:00:00Z",
            ));
            let report = compute(
                &m,
                &capture(tuples, Order::Unknown),
                &[review_with_exit("p1", "x", false)],
            )
            .unwrap();
            assert_eq!(report.mechanism.effects, 1);
            assert_eq!(
                report.mechanism.author_exit_effects,
                usize::from(task == "TKT-source"),
                "exit task {task:?}"
            );
        }
    }

    #[test]
    fn clover_native_cost_requires_binding_and_producer_time() {
        let m = task_manifest("TKT-1");
        let usage = usage_rec(
            "u",
            "TKT-1",
            "gen-1",
            Some("completed"),
            Some(4.25),
            "2026-01-01T11:00:00Z",
        );
        let exit = field(
            exit_rec(
                "x",
                "TKT-1",
                "gen-1",
                "gen-1",
                "2026-01-01T10:00:00Z",
                "2026-01-01T12:00:00Z",
            ),
            "prior_state",
            json!("completed"),
        );
        for (is_usage, key, value) in [
            (true, "observed_at", Value::Null),
            (true, "observed_at", json!("invalid")),
            (false, "exited_at", Value::Null),
            (false, "exited_at", json!("invalid")),
            (true, "task", Value::Null),
            (false, "task", Value::Null),
            (true, "agent", Value::Null),
            (false, "agent", Value::Null),
        ] {
            let (mut u, mut x) = (usage.clone(), exit.clone());
            if is_usage {
                u["payload"][key] = value;
            } else {
                x["payload"][key] = value;
            }
            let report = compute(&m, &capture(vec![u, x], Order::Unknown), &[]).unwrap();
            assert_eq!(report.deliveries.len(), 1);
            assert_eq!(
                report.deliveries[0].reported_cost_estimate_usd, None,
                "usage={is_usage}, missing/malformed {key}"
            );
            assert!(report
                .invalid_records
                .iter()
                .chain(report.unresolved_records.iter())
                .any(|r| r.record == if is_usage { "u" } else { "x" }));
        }
    }

    #[test]
    fn clover_rejected_artifacts_cannot_certify_effects_or_annotations() {
        let m = manifest(vec![pair("p1", "src-1", "TKT-1", "gen-1")]);
        let mut forged = finding("ev-2", "author-gen", "2026-01-01T00:00:00Z");
        forged["identity"] = json!("forged-finding");
        let missing_receipt = assessment("ev-2", "", "verified", "2026-01-01T00:00:00Z");
        for bad in [forged, missing_receipt] {
            let mut tuples = verified_tuples();
            tuples.push(bad);
            let annotation = ReviewedAnnotation {
                task: "TKT-1".into(),
                repo: "repo".into(),
                repeated_investigations: vec![],
                interventions: vec![],
                rework: vec![AnnotatedEvidence {
                    evidence: "ev-2".into(),
                    reason: "claimed supporting evidence".into(),
                }],
            };
            let report = compute_full(
                &m,
                &capture(tuples, Order::Unknown),
                &[review_no_exit("p1")],
                &[annotation],
            )
            .unwrap();
            assert_eq!(report.eligible, 1);
            assert_eq!(report.mechanism.effects, 0);
            assert_eq!(report.quality.reviewed_rework, 0);
            assert!(report.invalid_records.iter().any(|r| r.record == "ev-2"));
        }
    }
}

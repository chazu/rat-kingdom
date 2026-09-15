//! P9.3: a narrow, native, incremental assessment producer for one enabled
//! feature's bounded native telemetry (TKT-bahov-lakat-darif).
//!
//! This is deliberately NOT a generic metrics/expression engine. It supports
//! exactly one predeclared objective —
//! `rk-retirement-observed-operation-success`, defined by
//! `docs/2026-09-13-continuous-validation-promotion.md` sections 7.2/8 and
//! the operator-supplied
//! `first-production-objective-v1.json` — against exactly one existing
//! source: `crate::landing_need_resolution::telemetry_event`
//! (`Category::Event`, identity [`RETIREMENT_TELEMETRY_IDENTITY`]).
//! Extending to a second objective or source is a new, explicit slice, not a
//! config knob here.
//!
//! # Copy/paste companion
//!
//! ```text
//! rk bbs assessment configure --repo rat-kingdom --objective first-production-objective-v1.json
//! rk bbs assessment activate  --repo rat-kingdom
//! rk bbs assessment status    --repo rat-kingdom   # cursor/counters, read-only
//! rk bbs assessment latest    --repo rat-kingdom   # last published verdict, read-only
//! rk bbs assessment disable   --repo rat-kingdom   # retains all prior evidence
//! ```
//! No `tick` call is required in production: once `activate`d, the daemon's
//! own bounded sweep (see `sweep_due`, wired into `Server::run`) advances the
//! evaluator on the objective's own `evaluation_cadence_seconds`, resuming
//! from the durable checkpoint across a daemon restart. `bbs.assessment.tick`
//! remains available for tests and manual inspection but is never required.
//!
//! Config shape mirrors `crate::landing_need_resolution` and
//! `crate::bbs_discovery`: one JSON-file-backed registry, mutated only
//! through validated operations, resolved fresh on every read. Unlike those
//! modules this registry also carries MUTABLE reducer/checkpoint state and
//! the latest published assessment, because the whole point of this feature
//! is to accumulate state across ticks — but every mutation still goes
//! through the same atomic tmp-then-rename write those modules use, so a
//! crash mid-write never corrupts the file. Every mutating operation
//! (`configure`/`activate`/`disable`/`tick`) additionally holds an
//! in-process, per-registry-file lock ([`with_registry_lock`]) for its whole
//! load-mutate-persist span, so a concurrent RPC and a concurrent scheduler
//! sweep can never race a lost update onto the same `.json.tmp` path.
//!
//! # Incremental evaluation
//!
//! [`tick`] is the one entry point that advances a repo's evaluator. It never
//! rescans the whole persistence journal: it resumes from the durably saved
//! `cursor` (a `Space::persistence_page` commit sequence), reads up to
//! `maximum_pages_per_tick` bounded pages of at most `page_limit` rows each,
//! and persists the new cursor atomically together with the updated reducer
//! totals — so a crash between "read a page" and "persist the checkpoint"
//! reprocesses that same page from the old cursor exactly once, never loses
//! it and never double-counts a page that was already durably checkpointed.
//! The published result tuple is written via `Space::reinforce` under a
//! fixed per-repo identity (never `Space::out`), so a crash between writing
//! that tuple and persisting the checkpoint — which would otherwise recompute
//! and republish the identical assessment on the next tick — upserts the
//! same live tuple in place instead of appending a duplicate.
//!
//! [`sweep_due`] is the autonomous half: called on a bounded internal
//! cadence by `Server::run` (never by an operator), it loads the registry
//! once, selects every repo whose activation is live AND whose own
//! `evaluation_cadence_seconds` has elapsed since its last tick, and ticks
//! each. A disabled repo (`activation: None`) is never selected, so
//! `disable` stops future evaluation immediately — not just future manual
//! ticks.
//!
//! # Verdicts
//!
//! Computed in [`assess`] from the accumulated [`AssessmentState`] and the
//! active [`ObjectiveConfig`], in priority order:
//! 1. `ever_failed` (sticky: any observed `failed > 0`, ever) → always `fail`,
//!    surviving a daemon restart or an unrelated binary release
//!    (`docs/2026-09-13-continuous-validation-promotion.md` 7.2: "a new
//!    release cannot reset an ongoing feature's evaluation clock or discard
//!    its failures"; "known failure cannot become pass through restart or
//!    version churn").
//! 2. Zero valid telemetry rows ever consumed (`events_consumed == 0`) →
//!    `unavailable` — "missing telemetry", never fabricated into a zero. This
//!    is distinct from "zero operations": a repo that has observed valid,
//!    well-formed telemetry where every candidate was legitimately skipped
//!    (`resolved == 0 && failed == 0` but `events_consumed > 0`) is
//!    `inconclusive`, not `unavailable` — the pipe is known-good, there is
//!    just nothing to report yet.
//! 3. The most recent valid telemetry is older than
//!    `objective.source_freshness_seconds` → `unavailable` ("stale
//!    evidence") — a caught-up clean history must not silently conceal a gone
//!    quiet source.
//! 4. `resolved + failed` below `minimum_denominator`, or distinct resolved
//!    Needs below `minimum_distinct_resolved_needs_for_pass` → `inconclusive`.
//!    A truncated distinct set is never used to bypass this check — it is
//!    only ever a true undercount, so if it were sufcient it would already
//!    show as sufficient without the bypass.
//! 5. The evaluator has not fully caught up to the source boundary this tick
//!    (`last_tick_truncated`) → capped at `inconclusive`, never `pass`: an
//!    un-scanned tail could hold a failure this window has not seen yet.
//! 6. Otherwise `pass` iff `resolved / (resolved + failed) >=
//!    required_ratio`, else `inconclusive`.

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rk_core::paths::Layout;
use rk_core::tuple::{Category, Lifecycle, Tuple};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use crate::landing_need_resolution;

/// The only objective this slice understands. A config naming any other
/// value is rejected outright — see [`ObjectiveConfig::validate`].
pub const SUPPORTED_OBJECTIVE_ID: &str = "rk-retirement-observed-operation-success";
pub const SUPPORTED_OBJECTIVE_VERSION: u32 = 1;

/// The one existing telemetry identity this slice consumes, produced by
/// `landing_need_resolution::telemetry_event`. Kept as a distinct constant
/// (rather than importing that module's private literal) so a rename there
/// is a deliberate, visible break here too.
pub const RETIREMENT_TELEMETRY_IDENTITY: &str = "landing-need-retirement-run";

/// Identity this module writes its own published assessments under. Fixed
/// per repo so [`space.reinforce`] upserts one live "current assessment"
/// tuple per `(repo, castle)` rather than appending a growing history.
/// `Category::Event` is never scanned by `bbs::brief` (only
/// `Claim`/`Need`/`Artifact` are), so continuous assessment telemetry can
/// never crowd a real finding or artifact out of a briefing.
pub const ASSESSMENT_RESULT_IDENTITY: &str = "continuous-assessment-result";

const MAX_DISTINCT_RESOLVED_NEEDS: usize = 4096;
const MAX_SEEN_EVENT_IDS: usize = 4096;
const MAX_OBSERVED_CONFIG_REVISIONS: usize = 32;
const MAX_OBSERVED_BUILDS: usize = 16;
const MAX_SOURCE_REFERENCES: usize = 16;
const MAX_HISTORY_ENTRIES: usize = 8;
const MAX_PAGE_LIMIT: usize = 2000;
const MAX_PAGES_PER_TICK: usize = 16;
const MAX_CADENCE_SECONDS: u64 = 3600;
const MAX_FRESHNESS_SECONDS: u64 = 86_400;
const MAX_TICKS_TO_REFLECT: u64 = 100;

/// Process-wide, per-registry-file locks. A `Mutex` (not an flock) is
/// sufficient and correct: this registry is daemon-private
/// (`<home>/continuous-assessment.json`), touched only from within this one
/// process — by RPC handler threads and the scheduler sweep — never shared
/// across processes the way the singleton daemon socket/pid files are.
fn registry_lock(path: &Path) -> Arc<Mutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();
    let locks = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks.lock().unwrap_or_else(|e| e.into_inner());
    locks
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

/// Run `f` while holding the exclusive in-process lock for `path`'s
/// registry. Every mutating entry point (`configure`/`activate`/`disable`/
/// `tick`) wraps its ENTIRE load-mutate-persist span in this, so a
/// concurrent RPC call and a concurrent scheduler sweep can never
/// interleave two independent load/persist cycles against the same file.
fn with_registry_lock<T>(
    path: &Path,
    f: impl FnOnce() -> rk_core::Result<T>,
) -> rk_core::Result<T> {
    let lock = registry_lock(path);
    let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
    f()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Pass,
    Fail,
    Inconclusive,
    Unavailable,
}

/// Operator-declared, validated objective identity, metric semantics and
/// bounded cadence — the "desired semantics" section of
/// `first-production-objective-v1.json`, adapted to this module's concrete
/// schema. Every field is an explicit number or string; there is no
/// expression language.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObjectiveConfig {
    pub objective_id: String,
    pub objective_version: u32,
    /// Must equal `landing_need_resolution::FEATURE_ID`. Named explicitly
    /// (rather than assumed) so a future second source is a visible config
    /// change, not silent scope creep.
    pub feature: String,
    /// `resolved / (resolved + failed)` must reach this to ever pass.
    pub required_ratio: f64,
    /// `resolved + failed` below this is `inconclusive`, never `pass`/`fail`
    /// purely on sample-size grounds — EXCEPT `failed > 0`, which is always
    /// `fail` regardless of sample size (`first-production-objective-v1.json`
    /// `outcomes.fail`).
    pub minimum_denominator: u64,
    /// A `pass` also requires at least this many DISTINCT resolved Need
    /// identities — repeated bindings of the same Need (including a
    /// retried/replayed pass re-resolving it) cannot manufacture distinct
    /// successful work.
    pub minimum_distinct_resolved_needs_for_pass: u64,
    pub evaluation_cadence_seconds: u64,
    pub maximum_source_to_assessment_ticks: u64,
    pub source_freshness_seconds: u64,
    pub page_limit: usize,
    pub maximum_pages_per_tick: usize,
}

impl ObjectiveConfig {
    /// Rejects malformed/unsupported configuration wholesale — never a
    /// partial activation. Called by `configure` before anything is
    /// persisted.
    pub fn validate(&self) -> Result<(), String> {
        if self.objective_id != SUPPORTED_OBJECTIVE_ID {
            return Err(format!(
                "unsupported objective_id '{}'; this build only supports '{SUPPORTED_OBJECTIVE_ID}'",
                self.objective_id
            ));
        }
        if self.objective_version != SUPPORTED_OBJECTIVE_VERSION {
            return Err(format!(
                "unsupported objective_version {}; this build only supports {SUPPORTED_OBJECTIVE_VERSION}",
                self.objective_version
            ));
        }
        if self.feature != landing_need_resolution::FEATURE_ID {
            return Err(format!(
                "unsupported feature '{}'; this build only supports '{}'",
                self.feature,
                landing_need_resolution::FEATURE_ID
            ));
        }
        if !(self.required_ratio > 0.0 && self.required_ratio <= 1.0) {
            return Err("required_ratio must be in (0.0, 1.0]".into());
        }
        if self.minimum_denominator < 1 {
            return Err("minimum_denominator must be >= 1".into());
        }
        if self.minimum_distinct_resolved_needs_for_pass < 1 {
            return Err("minimum_distinct_resolved_needs_for_pass must be >= 1".into());
        }
        if !(1..=MAX_CADENCE_SECONDS).contains(&self.evaluation_cadence_seconds) {
            return Err(format!(
                "evaluation_cadence_seconds must be 1..={MAX_CADENCE_SECONDS}"
            ));
        }
        if !(1..=MAX_TICKS_TO_REFLECT).contains(&self.maximum_source_to_assessment_ticks) {
            return Err(format!(
                "maximum_source_to_assessment_ticks must be 1..={MAX_TICKS_TO_REFLECT}"
            ));
        }
        if self.source_freshness_seconds < self.evaluation_cadence_seconds
            || self.source_freshness_seconds > MAX_FRESHNESS_SECONDS
        {
            return Err(format!(
                "source_freshness_seconds must be >= evaluation_cadence_seconds and <= {MAX_FRESHNESS_SECONDS}"
            ));
        }
        if !(1..=MAX_PAGE_LIMIT).contains(&self.page_limit) {
            return Err(format!("page_limit must be 1..={MAX_PAGE_LIMIT}"));
        }
        if !(1..=MAX_PAGES_PER_TICK).contains(&self.maximum_pages_per_tick) {
            return Err(format!(
                "maximum_pages_per_tick must be 1..={MAX_PAGES_PER_TICK}"
            ));
        }
        Ok(())
    }
}

/// What bound the evaluation window to actual installed identity, captured
/// once at activation and never silently rewritten by a later reconfigure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Activation {
    pub activated_at: DateTime<Utc>,
    /// `Space::persistence_page` boundary at the moment of activation. Events
    /// persisted at or before this sequence are old exposure and can never
    /// retrospectively satisfy this activation.
    pub activation_boundary: u64,
    pub installed_build: String,
    /// The source feature's own config revision
    /// (`landing_need_resolution::RetirementConfig::revision`) read at
    /// activation time. This binds the window to EXACTLY that revision: a
    /// telemetry row observed with a DIFFERENT revision — lower (evidence
    /// from before this window's configuration existed) or higher (the
    /// feature was reconfigured during this window) — is unsupported
    /// provenance and is rejected (see
    /// [`AssessmentState::revision_mismatch_events`]), never silently
    /// folded in either direction. Every observed revision, matched or not,
    /// is still explicitly recorded in
    /// [`AssessmentState::observed_feature_config_revisions`] for audit.
    pub source_feature_revision: u64,
    pub activated_by: String,
}

/// Bounded, durable reducer state for one repo's evaluation window. Every
/// collection here has an explicit finite cap; hitting the cap sets the
/// matching `_truncated` flag rather than silently evicting evidence.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AssessmentState {
    /// Last consumed `Space::persistence_page` commit sequence for this repo.
    pub cursor: u64,
    pub attempted_total: u64,
    pub skipped_total: u64,
    pub failed_total: u64,
    /// The numerator: `sum(resolved)` over every consumed telemetry event,
    /// exactly as the predeclared objective defines it. Two DISTINCT native
    /// operation events may legitimately name the same Need (e.g. a genuine
    /// retry); that is real observed work and both count here. Protection
    /// against a data-pipeline REPLAY of the identical event is instead
    /// [`seen_event_ids`](Self::seen_event_ids) below, keyed on the source
    /// tuple's own native identity, not on which Need it names.
    pub resolved_total: u64,
    /// A SEPARATE minimum-sample signal, never the metric numerator: distinct
    /// resolved Need identities seen across every consumed event. Repeated
    /// bindings of the same Need cannot manufacture distinct successful work
    /// for `minimum_distinct_resolved_needs_for_pass`.
    pub distinct_resolved_needs: BTreeSet<String>,
    /// Set when a producer-truncated `resolved_bindings` list (or this
    /// state's own bounded Need set) could not record complete evidence.
    /// Sticky: once true, `assess` treats the whole window as carrying
    /// unknown evidence.
    pub distinct_resolved_truncated: bool,
    pub observed_feature_config_revisions: BTreeSet<u64>,
    pub observed_builds: BTreeSet<String>,
    /// Telemetry rows that named this feature/identity but carried
    /// missing/non-numeric/inconsistent required counters (including a
    /// failed `attempted == resolved + skipped + failed` cross-check,
    /// numeric overflow, a missing/empty `build` or unrecognized
    /// `config_status`, or an incomplete `resolved_bindings` list for a
    /// non-truncated `resolved > 0` row). Never folded into `failed_total`
    /// (a producer error is not evidence the OPERATION failed) and never
    /// silently dropped either.
    pub malformed_events: u64,
    /// Well-formed rows whose `config_revision` did not EXACTLY equal this
    /// activation's own bound `source_feature_revision` — a later
    /// reconfiguration must not silently widen or change this window's
    /// scope any more than an older one may retroactively narrow it.
    /// Reported rather than counted as success either way.
    pub revision_mismatch_events: u64,
    /// Native source tuple ids already folded into the totals above, for
    /// this activation. Checked before any counters are updated so a
    /// data-pipeline replay of the IDENTICAL source event can never count
    /// operations (or failures) twice. Bounded; see
    /// [`seen_event_ids_truncated`](Self::seen_event_ids_truncated).
    pub seen_event_ids: BTreeSet<String>,
    /// Set once `seen_event_ids` hits its bound: dedup can no longer be
    /// proven for a new incoming id, so the window's evidence is no longer
    /// fully trustworthy. Sticky, like `distinct_resolved_truncated`.
    pub seen_event_ids_truncated: bool,
    /// Sticky: once true, [`assess`] always returns `Fail` for this
    /// activation.
    pub ever_failed: bool,
    /// Count of rows that passed every validation and were actually folded
    /// into the totals above (excludes `malformed_events` and
    /// `revision_mismatch_events`). `0` is the precise signal for "no valid
    /// evidence has been observed for this activation yet".
    pub events_consumed: u64,
    pub last_event_at: Option<DateTime<Utc>>,
    pub last_tick_at: Option<DateTime<Utc>>,
    pub last_tick_pages: usize,
    /// True when the journal held more matching rows beyond this tick's
    /// bounded page budget — reported, never hidden. Reflects only the
    /// MOST RECENT tick (self-resolving once a later tick fully catches
    /// up), unlike the other sticky flags above.
    pub last_tick_truncated: bool,
    /// Most recent contributing telemetry event ids, bounded, for
    /// reproducibility references on the published assessment.
    pub source_references: Vec<String>,
}

impl AssessmentState {
    fn record_config_revision(&mut self, revision: u64) {
        if self.observed_feature_config_revisions.len() < MAX_OBSERVED_CONFIG_REVISIONS {
            self.observed_feature_config_revisions.insert(revision);
        }
    }

    fn record_build(&mut self, build: &str) {
        if self.observed_builds.len() < MAX_OBSERVED_BUILDS && !self.observed_builds.contains(build)
        {
            self.observed_builds.insert(build.to_string());
        }
    }

    fn record_resolved_need(&mut self, need_id: &str) {
        if self.distinct_resolved_needs.len() >= MAX_DISTINCT_RESOLVED_NEEDS
            && !self.distinct_resolved_needs.contains(need_id)
        {
            self.distinct_resolved_truncated = true;
            return;
        }
        self.distinct_resolved_needs.insert(need_id.to_string());
    }

    fn record_source_reference(&mut self, id: String) {
        self.source_references.push(id);
        if self.source_references.len() > MAX_SOURCE_REFERENCES {
            self.source_references.remove(0);
        }
    }

    /// `true` iff `id` is newly recorded and this event should be processed;
    /// `false` iff `id` is a known duplicate and the caller must skip it
    /// entirely. On bound overflow, still returns `true` (dropping known-good
    /// data is its own failure mode) but sets
    /// [`seen_event_ids_truncated`](Self::seen_event_ids_truncated) so
    /// `assess` stops trusting the window's completeness.
    fn record_seen_event(&mut self, id: &str) -> bool {
        if self.seen_event_ids.contains(id) {
            return false;
        }
        if self.seen_event_ids.len() >= MAX_SEEN_EVENT_IDS {
            self.seen_event_ids_truncated = true;
            return true;
        }
        self.seen_event_ids.insert(id.to_string());
        true
    }
}

/// A deterministic, reproducible published verdict — never mutated in place;
/// each meaningful change writes a new one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublishedAssessment {
    pub verdict: Verdict,
    pub reason: String,
    pub objective_id: String,
    pub objective_version: u32,
    /// The [`AssessmentRecord::revision`] in effect when this was computed —
    /// the exact objective/config identity, not just its name.
    pub config_revision: u64,
    pub observed_builds: Vec<String>,
    pub observed_feature_config_revisions: Vec<u64>,
    pub cursor_range: (u64, u64),
    pub numerator: u64,
    pub denominator: u64,
    pub distinct_resolved_needs: u64,
    pub distinct_resolved_truncated: bool,
    pub published_at: DateTime<Utc>,
    pub source_references: Vec<String>,
}

/// One retired activation window, kept for audit when a repo is
/// re-activated. Bounded to [`MAX_HISTORY_ENTRIES`] — the oldest is dropped,
/// never the current record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetiredActivation {
    pub activation: Activation,
    pub deactivated_at: DateTime<Utc>,
    pub final_state: AssessmentState,
    pub final_assessment: Option<PublishedAssessment>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AssessmentRecord {
    pub objective: Option<ObjectiveConfig>,
    /// Bumps on every `configure`/`activate`/`disable`. This is the
    /// "effective objective configuration" identity a published assessment's
    /// `config_revision` names.
    pub revision: u64,
    /// `Some` while enabled/on; `None` while off. Disabling retains `state`
    /// and `latest` — only `activation` clears.
    pub activation: Option<Activation>,
    pub state: AssessmentState,
    pub latest: Option<PublishedAssessment>,
    pub history: Vec<RetiredActivation>,
    pub updated_at: Option<DateTime<Utc>>,
    pub updated_by: Option<String>,
}

/// JSON-file-backed, persisted synchronously on every mutation — the same
/// restart-memory contract as `crate::landing_need_resolution::RetirementRegistry`.
pub struct AssessmentRegistry {
    path: PathBuf,
    repos: BTreeMap<String, AssessmentRecord>,
}

impl AssessmentRegistry {
    pub fn load(path: &Path) -> rk_core::Result<Self> {
        let repos = match std::fs::read_to_string(path) {
            Ok(raw) => serde_json::from_str(&raw)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            path: path.to_path_buf(),
            repos,
        })
    }

    pub fn record(&self, repo: &str) -> Option<&AssessmentRecord> {
        self.repos.get(repo)
    }

    /// Every configured/activated repo, for the scheduler sweep. Read-only.
    pub fn entries(&self) -> impl Iterator<Item = (&str, &AssessmentRecord)> {
        self.repos
            .iter()
            .map(|(repo, record)| (repo.as_str(), record))
    }

    fn persist(&self) -> rk_core::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(&self.repos)?)?;
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }

    /// Validate and store an objective for `repo`. Does not touch an existing
    /// `activation`/`state`/`latest` — a live evaluation window keeps
    /// running under the corrected parameters, per the doc's "a new release
    /// cannot reset an ongoing feature's evaluation clock" rule extended to
    /// an operator correction of the SAME objective.
    pub fn configure(
        &mut self,
        repo: &str,
        objective: ObjectiveConfig,
        by: &str,
    ) -> rk_core::Result<AssessmentRecord> {
        objective.validate().map_err(rk_core::Error::other)?;
        let mut record = self.repos.get(repo).cloned().unwrap_or_default();
        record.objective = Some(objective);
        record.revision += 1;
        record.updated_at = Some(Utc::now());
        record.updated_by = Some(by.to_string());
        self.repos.insert(repo.to_string(), record.clone());
        self.persist()?;
        Ok(record)
    }

    /// Begin (or restart) the evaluation window: captures a fresh
    /// `activation_boundary` so pre-activation events never count, and resets
    /// the reducer state fresh — retiring any prior window into `history`
    /// (bounded) rather than discarding it.
    pub fn activate(
        &mut self,
        repo: &str,
        space: &rk_space::Space,
        installed_build: String,
        source_feature_revision: u64,
        by: &str,
    ) -> rk_core::Result<AssessmentRecord> {
        let mut record = self.repos.get(repo).cloned().ok_or_else(|| {
            rk_core::Error::other(format!(
                "no assessment objective configured for repo '{repo}'; call configure first"
            ))
        })?;
        if record.objective.is_none() {
            return Err(rk_core::Error::other(format!(
                "no assessment objective configured for repo '{repo}'; call configure first"
            )));
        }
        if let Some(previous) = record.activation.take() {
            record.history.push(RetiredActivation {
                activation: previous,
                deactivated_at: Utc::now(),
                final_state: std::mem::take(&mut record.state),
                final_assessment: record.latest.clone(),
            });
            if record.history.len() > MAX_HISTORY_ENTRIES {
                record.history.remove(0);
            }
        }
        let activation_boundary = space.latest_persistence_sequence()?;
        record.state = AssessmentState {
            cursor: activation_boundary,
            ..AssessmentState::default()
        };
        record.latest = None;
        record.activation = Some(Activation {
            activated_at: Utc::now(),
            activation_boundary,
            installed_build,
            source_feature_revision,
            activated_by: by.to_string(),
        });
        record.revision += 1;
        record.updated_at = Some(Utc::now());
        record.updated_by = Some(by.to_string());
        self.repos.insert(repo.to_string(), record.clone());
        self.persist()?;
        Ok(record)
    }

    /// Stop evaluating new events. Retires the ended window into `history`
    /// (bounded, oldest dropped first) while leaving `state`/`latest`
    /// untouched on the record itself — nothing already observed is
    /// destroyed, and it stays readable via `status`/`latest` even while
    /// disabled, exactly as it would if this were never called.
    pub fn disable(&mut self, repo: &str, by: &str) -> rk_core::Result<AssessmentRecord> {
        let mut record = self.repos.get(repo).cloned().unwrap_or_default();
        if let Some(activation) = record.activation.take() {
            record.history.push(RetiredActivation {
                activation,
                deactivated_at: Utc::now(),
                final_state: record.state.clone(),
                final_assessment: record.latest.clone(),
            });
            if record.history.len() > MAX_HISTORY_ENTRIES {
                record.history.remove(0);
            }
        }
        record.revision += 1;
        record.updated_at = Some(Utc::now());
        record.updated_by = Some(by.to_string());
        self.repos.insert(repo.to_string(), record.clone());
        self.persist()?;
        Ok(record)
    }

    /// Persist an updated `state` (and, only when a publish just actually
    /// succeeded, `latest`) for `repo` after a tick, without disturbing
    /// `objective`/`activation`/`revision`. The one mutation path [`tick`]
    /// uses. `latest` is deliberately `Option<Option<..>>`-shaped by the
    /// caller: passing `None` here means "no new publish this tick, leave
    /// the existing `latest` exactly as it is" (including "leave it stale
    /// because the reinforce write just failed, so the next tick retries").
    fn save_progress(
        &mut self,
        repo: &str,
        state: AssessmentState,
        latest: Option<PublishedAssessment>,
    ) -> rk_core::Result<()> {
        let record = self
            .repos
            .get_mut(repo)
            .ok_or_else(|| rk_core::Error::other(format!("repo '{repo}' vanished mid-tick")))?;
        record.state = state;
        if let Some(latest) = latest {
            record.latest = Some(latest);
        }
        self.persist()
    }
}

pub fn registry_path(layout: &Layout) -> PathBuf {
    layout.home().join("continuous-assessment.json")
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShowParams {
    pub repo: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigureParams {
    pub repo: String,
    pub objective: ObjectiveConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivateParams {
    pub repo: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DisableParams {
    pub repo: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TickParams {
    pub repo: String,
}

fn describe(repo: &str, record: &AssessmentRecord) -> serde_json::Value {
    serde_json::json!({
        "feature": "continuous-assessment",
        "repo": repo,
        "enabled": record.activation.is_some(),
        "revision": record.revision,
        "objective": record.objective,
        "activation": record.activation,
        "updated_at": record.updated_at,
        "updated_by": record.updated_by,
    })
}

/// Read-only, side-effect-free effective config + activation identity.
pub fn show(layout: &Layout, params: &ShowParams) -> rk_core::Result<serde_json::Value> {
    let registry = AssessmentRegistry::load(&registry_path(layout))?;
    let record = registry.record(&params.repo).cloned().unwrap_or_default();
    Ok(describe(&params.repo, &record))
}

/// Read-only progress: cursor, cumulative counters, lag/truncation.
pub fn status(layout: &Layout, params: &ShowParams) -> rk_core::Result<serde_json::Value> {
    let registry = AssessmentRegistry::load(&registry_path(layout))?;
    let record = registry.record(&params.repo).cloned().unwrap_or_default();
    Ok(serde_json::json!({
        "repo": params.repo,
        "enabled": record.activation.is_some(),
        "revision": record.revision,
        "state": record.state,
    }))
}

/// Read-only latest published assessment, or `null` if none has been
/// computed yet.
pub fn latest(layout: &Layout, params: &ShowParams) -> rk_core::Result<serde_json::Value> {
    let registry = AssessmentRegistry::load(&registry_path(layout))?;
    let record = registry.record(&params.repo).cloned().unwrap_or_default();
    Ok(serde_json::json!({
        "repo": params.repo,
        "latest": record.latest,
    }))
}

/// Validated config write. Never partially activates: a malformed objective
/// leaves any existing configuration/activation untouched.
pub fn configure(
    layout: &Layout,
    repos: &crate::repos::RepoRegistry,
    caller: &str,
    params: &ConfigureParams,
) -> rk_core::Result<serde_json::Value> {
    if repos.get(&params.repo).is_none() {
        return Err(rk_core::Error::other(format!(
            "repository is not registered: {}",
            params.repo
        )));
    }
    let path = registry_path(layout);
    let record = with_registry_lock(&path, || {
        let mut registry = AssessmentRegistry::load(&path)?;
        registry.configure(&params.repo, params.objective.clone(), caller)
    })?;
    let mut value = describe(&params.repo, &record);
    value["rollover_required"] = serde_json::json!(false);
    value["rollover_note"] = serde_json::json!(
        "applied on the next scheduled or manual tick for this repo; no daemon restart is required."
    );
    Ok(value)
}

/// Bind actual installed build + source feature revision and start a fresh
/// evaluation window from the current persistence boundary.
pub fn activate(
    layout: &Layout,
    space: &rk_space::Space,
    caller: &str,
    params: &ActivateParams,
) -> rk_core::Result<serde_json::Value> {
    let source_feature_revision =
        landing_need_resolution::resolve_for_repo(layout, &params.repo).revision;
    let path = registry_path(layout);
    let record = with_registry_lock(&path, || {
        let mut registry = AssessmentRegistry::load(&path)?;
        registry.activate(
            &params.repo,
            space,
            rk_core::version::build_version().to_string(),
            source_feature_revision,
            caller,
        )
    })?;
    Ok(describe(&params.repo, &record))
}

pub fn disable(
    layout: &Layout,
    caller: &str,
    params: &DisableParams,
) -> rk_core::Result<serde_json::Value> {
    let path = registry_path(layout);
    let record = with_registry_lock(&path, || {
        let mut registry = AssessmentRegistry::load(&path)?;
        registry.disable(&params.repo, caller)
    })?;
    let mut value = describe(&params.repo, &record);
    value["retained_note"] = serde_json::json!(
        "prior evidence, checkpoint and last assessment are retained and still readable"
    );
    Ok(value)
}

/// Compute the deterministic verdict for the current accumulated state under
/// `objective` as of `now`. `numerator = resolved_total`, `denominator =
/// resolved_total + failed_total` — the predeclared `sum(resolved)` /
/// `sum(resolved + failed)` metric exactly, never redefined in terms of
/// distinct Need identities (that stays a SEPARATE minimum-sample check).
/// See the module doc for the full verdict priority order.
pub fn assess(
    objective: &ObjectiveConfig,
    state: &AssessmentState,
    now: DateTime<Utc>,
) -> (Verdict, String) {
    if state.ever_failed {
        return (
            Verdict::Fail,
            "at least one observed failed operation remains a failure for this activation \
             (sticky: cannot become pass through restart or version churn)"
                .to_string(),
        );
    }
    // Any known-incomplete or unknown evidence in the active window makes
    // the window untrustworthy for a confident pass/fail/inconclusive claim
    // — checked BEFORE the metric, and sticky for everything except the
    // per-tick catch-up flag, so a success followed later by a malformed,
    // mismatched, or truncated row still reports `unavailable`, not a stale
    // `pass`.
    if state.malformed_events > 0
        || state.revision_mismatch_events > 0
        || state.distinct_resolved_truncated
        || state.seen_event_ids_truncated
        || state.last_tick_truncated
    {
        let mut reasons = Vec::new();
        if state.malformed_events > 0 {
            reasons.push(format!("{} malformed row(s)", state.malformed_events));
        }
        if state.revision_mismatch_events > 0 {
            reasons.push(format!(
                "{} revision-mismatched row(s)",
                state.revision_mismatch_events
            ));
        }
        if state.distinct_resolved_truncated {
            reasons.push("truncated resolution bindings".to_string());
        }
        if state.seen_event_ids_truncated {
            reasons.push("dedup identity bound exceeded".to_string());
        }
        if state.last_tick_truncated {
            reasons.push("catch-up incomplete this tick".to_string());
        }
        return (
            Verdict::Unavailable,
            format!(
                "incomplete or unknown evidence in the active window: {}",
                reasons.join(", ")
            ),
        );
    }
    if state.events_consumed == 0 {
        return (
            Verdict::Unavailable,
            "no telemetry observed yet for this activation".to_string(),
        );
    }
    if let Some(last_event_at) = state.last_event_at {
        let age = now.signed_duration_since(last_event_at);
        let horizon = ChronoDuration::seconds(objective.source_freshness_seconds as i64);
        if age > horizon {
            return (
                Verdict::Unavailable,
                format!(
                    "most recent valid telemetry is {}s old, exceeding source_freshness_seconds {}",
                    age.num_seconds(),
                    objective.source_freshness_seconds
                ),
            );
        }
    }
    let denominator = state.resolved_total + state.failed_total;
    if denominator == 0 {
        return (
            Verdict::Inconclusive,
            "valid telemetry observed but zero resolved/failed operations so far (all skipped)"
                .to_string(),
        );
    }
    if denominator < objective.minimum_denominator {
        return (
            Verdict::Inconclusive,
            format!(
                "denominator {denominator} below minimum_denominator {}",
                objective.minimum_denominator
            ),
        );
    }
    let distinct = state.distinct_resolved_needs.len() as u64;
    if distinct < objective.minimum_distinct_resolved_needs_for_pass {
        return (
            Verdict::Inconclusive,
            format!(
                "distinct resolved needs {distinct} below minimum {}",
                objective.minimum_distinct_resolved_needs_for_pass
            ),
        );
    }
    let ratio = state.resolved_total as f64 / denominator as f64;
    if ratio < objective.required_ratio {
        return (
            Verdict::Inconclusive,
            format!(
                "ratio {ratio:.4} below required {:.4}",
                objective.required_ratio
            ),
        );
    }
    (
        Verdict::Pass,
        format!(
            "ratio {ratio:.4} >= required {:.4} over {denominator} observed operation(s), \
             {distinct} distinct resolved need(s)",
            objective.required_ratio
        ),
    )
}

#[derive(Debug, Clone, Copy)]
pub struct TickOutcome {
    pub pages_processed: usize,
    pub events_consumed: u64,
    pub truncated: bool,
    pub verdict: Verdict,
    pub published: bool,
}

/// Advance one repo's evaluator by up to `objective.maximum_pages_per_tick`
/// bounded pages, starting from the durably saved cursor. Pure and
/// synchronous — safe to call from a blocking RPC handler, the scheduler
/// sweep, or a test, with an injectable `now` so tests never need a real
/// wall-clock wait. Holds this repo's registry-file lock for its whole
/// duration (see [`with_registry_lock`]).
pub fn tick(
    space: &rk_space::Space,
    layout: &Layout,
    repo: &str,
    castle: &str,
    now: DateTime<Utc>,
) -> rk_core::Result<TickOutcome> {
    let path = registry_path(layout);
    with_registry_lock(&path, || {
        let mut registry = AssessmentRegistry::load(&path)?;
        let record = registry.record(repo).cloned().ok_or_else(|| {
            rk_core::Error::other(format!("repo '{repo}' has no assessment record"))
        })?;
        let objective = record.objective.clone().ok_or_else(|| {
            rk_core::Error::other(format!("repo '{repo}' has no configured objective"))
        })?;
        let Some(activation) = record.activation.clone() else {
            return Err(rk_core::Error::other(format!(
                "assessment is not activated for repo '{repo}'"
            )));
        };
        let mut state = record.state;

        let mut pin: Option<u64> = None;
        let mut pages_processed = 0usize;
        let mut events_consumed_this_tick = 0u64;
        let mut truncated = false;
        for _ in 0..objective.maximum_pages_per_tick {
            let page =
                space.persistence_page(repo, Some(state.cursor), objective.page_limit, pin)?;
            pin = Some(page.boundary);
            pages_processed += 1;
            for (_, tuple) in &page.entries {
                if tuple.category != Category::Event
                    || tuple.identity != RETIREMENT_TELEMETRY_IDENTITY
                {
                    continue;
                }
                ingest_retirement_event(
                    &mut state,
                    tuple,
                    activation.source_feature_revision,
                    &mut events_consumed_this_tick,
                );
            }
            state.cursor = page.next_cursor;
            if !page.more {
                truncated = false;
                break;
            }
            truncated = true;
        }
        state.last_tick_at = Some(now);
        state.last_tick_pages = pages_processed;
        state.last_tick_truncated = truncated;

        let (verdict, reason) = assess(&objective, &state, now);
        let denominator = state.resolved_total + state.failed_total;
        let distinct = state.distinct_resolved_needs.len() as u64;
        let meaningfully_changed = record
            .latest
            .as_ref()
            .map(|prior| {
                prior.verdict != verdict
                    || prior.numerator != state.resolved_total
                    || prior.denominator != denominator
                    || prior.distinct_resolved_needs != distinct
            })
            .unwrap_or(true);

        let mut published = false;
        let mut latest_to_persist = None;
        if meaningfully_changed {
            let assessment = PublishedAssessment {
                verdict,
                reason,
                objective_id: objective.objective_id.clone(),
                objective_version: objective.objective_version,
                config_revision: record.revision,
                observed_builds: state.observed_builds.iter().cloned().collect(),
                observed_feature_config_revisions: state
                    .observed_feature_config_revisions
                    .iter()
                    .copied()
                    .collect(),
                cursor_range: (activation.activation_boundary, state.cursor),
                numerator: state.resolved_total,
                denominator,
                distinct_resolved_needs: distinct,
                distinct_resolved_truncated: state.distinct_resolved_truncated,
                published_at: now,
                source_references: state.source_references.clone(),
            };
            // `reinforce`, not `out`: this upserts the ONE live tuple keyed
            // on (category, scope=repo, identity, instance=castle), so a
            // crash between this write succeeding and `save_progress` below
            // persisting the checkpoint — which would otherwise recompute
            // and re-attempt the IDENTICAL publish on the next tick —
            // reinforces the same tuple in place instead of appending a
            // duplicate published result.
            match space.reinforce(result_tuple(repo, castle, &assessment)) {
                Ok(_) => {
                    published = true;
                    latest_to_persist = Some(assessment);
                }
                Err(error) => {
                    // Deliberately do NOT persist `latest` here: leaving the
                    // OLD `latest` in place means the next tick's
                    // `meaningfully_changed` comparison (against this same
                    // still-stale `record.latest`) is true again, so
                    // publication is retried rather than silently dropped.
                    tracing::warn!(%error, repo, "continuous-assessment: result publish failed; will retry next tick");
                }
            }
        }

        registry.save_progress(repo, state, latest_to_persist)?;
        Ok(TickOutcome {
            pages_processed,
            events_consumed: events_consumed_this_tick,
            truncated,
            verdict,
            published,
        })
    })
}

/// Every repo whose activation is live and whose own
/// `evaluation_cadence_seconds` has elapsed since its last tick (or which has
/// never ticked at all). Read-only; does not itself advance anything.
fn due_repos(layout: &Layout, now: DateTime<Utc>) -> rk_core::Result<Vec<String>> {
    let registry = AssessmentRegistry::load(&registry_path(layout))?;
    let mut due = Vec::new();
    for (repo, record) in registry.entries() {
        let Some(objective) = &record.objective else {
            continue;
        };
        if record.activation.is_none() {
            continue;
        }
        let is_due = match record.state.last_tick_at {
            None => true,
            Some(last) => {
                now.signed_duration_since(last)
                    >= ChronoDuration::seconds(objective.evaluation_cadence_seconds as i64)
            }
        };
        if is_due {
            due.push(repo.to_string());
        }
    }
    Ok(due)
}

/// The autonomous half of evaluation: called on a bounded internal cadence by
/// `Server::run` (see the background sweep loop there), never by an
/// operator. Ticks every currently-due, currently-activated repo and returns
/// how many were actually advanced. A single repo's failure is logged and
/// never blocks the others. No agents, no King wake — this only ever calls
/// the same pure [`tick`] the RPC path calls.
pub fn sweep_due(space: &rk_space::Space, layout: &Layout, now: DateTime<Utc>) -> usize {
    let due = match due_repos(layout, now) {
        Ok(due) => due,
        Err(error) => {
            tracing::warn!(%error, "continuous-assessment: failed to read registry for scheduled sweep");
            return 0;
        }
    };
    let mut ticked = 0;
    for repo in due {
        match tick(space, layout, &repo, "daemon", now) {
            Ok(_) => ticked += 1,
            Err(error) => {
                tracing::warn!(%error, repo, "continuous-assessment: scheduled tick failed")
            }
        }
    }
    ticked
}

/// Recognized `landing_need_resolution::RetirementConfig::status` values
/// (`rk_core::bbs::ConfigStatus::as_str`), duplicated as a literal list
/// rather than importing that type so a payload carrying anything else is
/// visibly rejected here without this module needing to track every variant
/// of a type it never otherwise touches.
const KNOWN_CONFIG_STATUSES: &[&str] = &["explicit", "default_absent", "unreadable_fallback"];

/// Validate and fold one telemetry tuple into `state`. Rejects (counts as
/// `malformed_events`, never `failed_total`) a row with missing/non-numeric
/// counters, a failed `attempted == resolved + skipped + failed` identity
/// (including on overflow), a missing/empty `build`, an unrecognized
/// `config_status`, or an incomplete `resolved_bindings` list for a
/// non-truncated `resolved > 0` row. Rejects (counts as
/// `revision_mismatch_events`) a well-formed row whose `config_revision`
/// does not EXACTLY equal `activation_source_feature_revision`. A row whose
/// native tuple id has already been folded into this state (a
/// data-pipeline replay of the identical source event) is skipped entirely
/// — see [`AssessmentState::record_seen_event`]. Only a row that clears
/// every check increments `events_consumed` and folds into the running
/// totals.
fn ingest_retirement_event(
    state: &mut AssessmentState,
    tuple: &Tuple,
    activation_source_feature_revision: u64,
    consumed: &mut u64,
) {
    let payload = &tuple.payload;
    if payload.get("feature").and_then(|v| v.as_str()) != Some(landing_need_resolution::FEATURE_ID)
    {
        return;
    }
    if !state.record_seen_event(&tuple.id.to_string()) {
        // A known duplicate of an already-folded native event id: never
        // recount its operations, failures, or malformed/mismatch status.
        return;
    }
    let attempted = payload.get("attempted").and_then(|v| v.as_u64());
    let resolved = payload.get("resolved").and_then(|v| v.as_u64());
    let skipped = payload.get("skipped").and_then(|v| v.as_u64());
    let failed = payload.get("failed").and_then(|v| v.as_u64());
    let config_revision = payload.get("config_revision").and_then(|v| v.as_u64());
    let build = payload.get("build").and_then(|v| v.as_str());
    let config_status = payload.get("config_status").and_then(|v| v.as_str());
    let (
        Some(attempted),
        Some(resolved),
        Some(skipped),
        Some(failed),
        Some(config_revision),
        Some(build),
        Some(config_status),
    ) = (
        attempted,
        resolved,
        skipped,
        failed,
        config_revision,
        build,
        config_status,
    )
    else {
        state.malformed_events += 1;
        return;
    };
    if build.is_empty() || !KNOWN_CONFIG_STATUSES.contains(&config_status) {
        state.malformed_events += 1;
        return;
    }
    let Some(expected_attempted) = resolved
        .checked_add(skipped)
        .and_then(|s| s.checked_add(failed))
    else {
        state.malformed_events += 1;
        return;
    };
    if expected_attempted != attempted {
        state.malformed_events += 1;
        return;
    }
    // Explicit visibility for EVERY observed revision, matched or not —
    // recorded before the match decision so a mismatch is never silently
    // invisible.
    state.record_config_revision(config_revision);
    if config_revision != activation_source_feature_revision {
        state.revision_mismatch_events += 1;
        return;
    }
    let bindings = payload.get("resolved_bindings").and_then(|v| v.as_array());
    let bindings_truncated = payload
        .get("resolved_bindings_truncated")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let mut resolved_ids: Vec<&str> = Vec::new();
    if resolved > 0 {
        let named: Vec<&str> = bindings
            .map(|b| {
                b.iter()
                    .filter_map(|entry| entry.get("need_id").and_then(|v| v.as_str()))
                    .collect()
            })
            .unwrap_or_default();
        if !bindings_truncated && (named.len() as u64) < resolved {
            // Incomplete required bindings for a non-truncated resolved
            // count: cannot trust which Needs actually resolved.
            state.malformed_events += 1;
            return;
        }
        resolved_ids = named;
    }
    // Every check has passed: commit this event's effect. `resolved_total`
    // is the predeclared `sum(resolved)` metric itself — two DISTINCT
    // native events may legitimately name the same Need, and both count
    // here; only a replayed IDENTICAL tuple id (already excluded above)
    // must never double count.
    state.attempted_total = state.attempted_total.saturating_add(attempted);
    state.resolved_total = state.resolved_total.saturating_add(resolved);
    state.skipped_total = state.skipped_total.saturating_add(skipped);
    state.failed_total = state.failed_total.saturating_add(failed);
    if failed > 0 {
        state.ever_failed = true;
    }
    state.record_build(build);
    if bindings_truncated {
        state.distinct_resolved_truncated = true;
    }
    for need_id in resolved_ids {
        // A SEPARATE minimum-sample signal only — never the metric itself.
        state.record_resolved_need(need_id);
    }
    state.last_event_at = Some(
        state
            .last_event_at
            .map_or(tuple.created_at, |prior| prior.max(tuple.created_at)),
    );
    state.events_consumed += 1;
    state.record_source_reference(tuple.id.to_string());
    *consumed += 1;
}

fn result_tuple(repo: &str, castle: &str, assessment: &PublishedAssessment) -> Tuple {
    Tuple::new(
        Category::Event,
        repo.to_string(),
        ASSESSMENT_RESULT_IDENTITY,
        castle.to_string(),
        serde_json::json!({
            "bbs_kind": "continuous_assessment_result",
            "objective_id": assessment.objective_id,
            "objective_version": assessment.objective_version,
            "config_revision": assessment.config_revision,
            "verdict": assessment.verdict,
            "reason": assessment.reason,
            "numerator": assessment.numerator,
            "denominator": assessment.denominator,
            "distinct_resolved_needs": assessment.distinct_resolved_needs,
            "distinct_resolved_truncated": assessment.distinct_resolved_truncated,
            "cursor_range": assessment.cursor_range,
            "observed_builds": assessment.observed_builds,
            "observed_feature_config_revisions": assessment.observed_feature_config_revisions,
            "source_references": assessment.source_references,
        }),
    )
    .with_lifecycle(Lifecycle::Furniture)
}

#[cfg(test)]
// Fixtures below build a base `AssessmentState`/`ObjectiveConfig` then
// reassign only the one or two fields each test actually exercises, so the
// field under test reads clearly against the untouched default — clearer
// here than threading every field through a struct literal per test.
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    fn valid_objective() -> ObjectiveConfig {
        ObjectiveConfig {
            objective_id: SUPPORTED_OBJECTIVE_ID.to_string(),
            objective_version: SUPPORTED_OBJECTIVE_VERSION,
            feature: landing_need_resolution::FEATURE_ID.to_string(),
            required_ratio: 1.0,
            minimum_denominator: 1,
            minimum_distinct_resolved_needs_for_pass: 1,
            evaluation_cadence_seconds: 30,
            maximum_source_to_assessment_ticks: 2,
            source_freshness_seconds: 1800,
            page_limit: 128,
            maximum_pages_per_tick: 2,
        }
    }

    #[test]
    fn a_supported_objective_validates() {
        assert!(valid_objective().validate().is_ok());
    }

    #[test]
    fn an_unsupported_objective_id_is_rejected() {
        let mut objective = valid_objective();
        objective.objective_id = "something-else".into();
        assert!(objective.validate().is_err());
    }

    #[test]
    fn an_unsupported_feature_is_rejected() {
        let mut objective = valid_objective();
        objective.feature = "bbs-discovery-ranking".into();
        assert!(objective.validate().is_err());
    }

    #[test]
    fn out_of_range_numeric_fields_are_rejected() {
        for mutate in [
            (|o: &mut ObjectiveConfig| o.required_ratio = 0.0) as fn(&mut ObjectiveConfig),
            |o| o.required_ratio = 1.5,
            |o| o.minimum_denominator = 0,
            |o| o.minimum_distinct_resolved_needs_for_pass = 0,
            |o| o.evaluation_cadence_seconds = 0,
            |o| o.maximum_source_to_assessment_ticks = 0,
            |o| o.source_freshness_seconds = 1, // below cadence
            |o| o.page_limit = 0,
            |o| o.page_limit = MAX_PAGE_LIMIT + 1,
            |o| o.maximum_pages_per_tick = 0,
            |o| o.maximum_pages_per_tick = MAX_PAGES_PER_TICK + 1,
        ] {
            let mut objective = valid_objective();
            mutate(&mut objective);
            assert!(objective.validate().is_err(), "{objective:?}");
        }
    }

    #[test]
    fn configure_rejects_malformed_config_without_partial_activation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.json");
        let mut registry = AssessmentRegistry::load(&path).unwrap();
        let mut bad = valid_objective();
        bad.required_ratio = 2.0;
        assert!(registry.configure("r", bad, "operator").is_err());
        assert!(registry.record("r").is_none());
    }

    #[test]
    fn activate_requires_prior_configure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.json");
        let mut registry = AssessmentRegistry::load(&path).unwrap();
        let space = rk_space::Space::open_in_memory().unwrap();
        assert!(registry
            .activate("r", &space, "build-x".into(), 1, "operator")
            .is_err());
    }

    #[test]
    fn zero_valid_events_is_unavailable_not_inconclusive() {
        let objective = valid_objective();
        let state = AssessmentState::default();
        let (verdict, _) = assess(&objective, &state, Utc::now());
        assert_eq!(verdict, Verdict::Unavailable);
    }

    #[test]
    fn valid_all_skipped_telemetry_is_inconclusive_not_unavailable() {
        let objective = valid_objective();
        let mut state = AssessmentState::default();
        state.events_consumed = 1;
        state.last_event_at = Some(Utc::now());
        // resolved == 0 && failed == 0: legitimately nothing to report yet.
        let (verdict, _) = assess(&objective, &state, Utc::now());
        assert_eq!(verdict, Verdict::Inconclusive);
    }

    #[test]
    fn a_single_resolved_need_is_pass_at_ratio_one() {
        let objective = valid_objective();
        let mut state = AssessmentState::default();
        state.events_consumed = 1;
        state.last_event_at = Some(Utc::now());
        state.resolved_total = 1;
        state.distinct_resolved_needs.insert("01ABC".into());
        let (verdict, _) = assess(&objective, &state, Utc::now());
        assert_eq!(verdict, Verdict::Pass);
    }

    #[test]
    fn any_failed_is_fail_even_under_minimum_denominator() {
        let mut objective = valid_objective();
        objective.minimum_denominator = 10;
        let mut state = AssessmentState::default();
        state.events_consumed = 1;
        state.last_event_at = Some(Utc::now());
        state.failed_total = 1;
        state.ever_failed = true;
        let (verdict, _) = assess(&objective, &state, Utc::now());
        assert_eq!(verdict, Verdict::Fail);
    }

    #[test]
    fn ever_failed_is_sticky_even_after_later_clean_events() {
        let objective = valid_objective();
        let mut state = AssessmentState::default();
        state.events_consumed = 1;
        state.last_event_at = Some(Utc::now());
        state.ever_failed = true;
        state.distinct_resolved_needs.insert("01ABC".into());
        // failed_total could even be back at 0 if a later tick only observed
        // clean events, but `ever_failed` must still win.
        let (verdict, _) = assess(&objective, &state, Utc::now());
        assert_eq!(verdict, Verdict::Fail);
    }

    #[test]
    fn distinct_needs_below_minimum_is_inconclusive_not_pass() {
        let mut objective = valid_objective();
        objective.minimum_distinct_resolved_needs_for_pass = 2;
        let mut state = AssessmentState::default();
        state.events_consumed = 1;
        state.last_event_at = Some(Utc::now());
        // A high operation count, but only one distinct Need — the
        // predeclared sum metric alone is satisfied; the SEPARATE
        // distinct-Need minimum is not.
        state.resolved_total = 5;
        state.distinct_resolved_needs.insert("01ABC".into());
        let (verdict, _) = assess(&objective, &state, Utc::now());
        assert_eq!(verdict, Verdict::Inconclusive);
    }

    #[test]
    fn truncated_bindings_are_unavailable_not_a_silent_minimum_bypass() {
        let mut objective = valid_objective();
        objective.minimum_distinct_resolved_needs_for_pass = 2;
        let mut state = AssessmentState::default();
        state.events_consumed = 1;
        state.last_event_at = Some(Utc::now());
        state.resolved_total = 1;
        state.distinct_resolved_needs.insert("01ABC".into());
        // Truncated bindings are unknown/incomplete evidence: unavailable,
        // never treated as "probably satisfies the minimum".
        state.distinct_resolved_truncated = true;
        let (verdict, reason) = assess(&objective, &state, Utc::now());
        assert_eq!(verdict, Verdict::Unavailable);
        assert!(reason.contains("truncated"), "{reason}");
    }

    #[test]
    fn malformed_only_evidence_is_unavailable_not_a_fabricated_zero() {
        let objective = valid_objective();
        let mut state = AssessmentState::default();
        state.malformed_events = 3;
        let (verdict, reason) = assess(&objective, &state, Utc::now());
        assert_eq!(verdict, Verdict::Unavailable);
        assert!(reason.contains("malformed"), "{reason}");
    }

    #[test]
    fn a_success_followed_by_malformed_telemetry_is_unavailable_not_pass() {
        let objective = valid_objective();
        let mut state = AssessmentState::default();
        state.events_consumed = 1;
        state.last_event_at = Some(Utc::now());
        state.resolved_total = 1;
        state.distinct_resolved_needs.insert("01ABC".into());
        // A LATER malformed row must still make the CURRENT verdict
        // unavailable, not conceal itself behind the earlier success.
        state.malformed_events = 1;
        let (verdict, reason) = assess(&objective, &state, Utc::now());
        assert_eq!(verdict, Verdict::Unavailable);
        assert!(reason.contains("malformed"), "{reason}");
    }

    #[test]
    fn stale_evidence_is_unavailable_even_with_a_clean_history() {
        let objective = valid_objective();
        let mut state = AssessmentState::default();
        state.events_consumed = 1;
        state.resolved_total = 1;
        state.distinct_resolved_needs.insert("01ABC".into());
        state.last_event_at = Some(Utc::now() - ChronoDuration::seconds(3600));
        let (verdict, reason) = assess(&objective, &state, Utc::now());
        assert_eq!(verdict, Verdict::Unavailable);
        assert!(
            reason.contains("stale") || reason.contains("exceeding"),
            "{reason}"
        );
    }

    #[test]
    fn fresh_clean_evidence_within_horizon_still_passes() {
        let objective = valid_objective();
        let mut state = AssessmentState::default();
        state.events_consumed = 1;
        state.resolved_total = 1;
        state.distinct_resolved_needs.insert("01ABC".into());
        state.last_event_at = Some(Utc::now() - ChronoDuration::seconds(10));
        let (verdict, _) = assess(&objective, &state, Utc::now());
        assert_eq!(verdict, Verdict::Pass);
    }

    #[test]
    fn truncated_tick_is_unavailable_not_a_silently_incomplete_pass() {
        let objective = valid_objective();
        let mut state = AssessmentState::default();
        state.events_consumed = 1;
        state.last_event_at = Some(Utc::now());
        state.resolved_total = 1;
        state.distinct_resolved_needs.insert("01ABC".into());
        state.last_tick_truncated = true;
        let (verdict, reason) = assess(&objective, &state, Utc::now());
        assert_eq!(verdict, Verdict::Unavailable);
        assert!(reason.contains("catch-up"), "{reason}");
    }

    #[tokio::test]
    async fn a_revision_mismatch_is_sticky_and_still_blocks_a_later_matched_tick() {
        // `revision_mismatch_events` is sticky (see `assess`'s untrusted-
        // evidence check): once a mismatched-revision row has been observed,
        // a LATER tick that ingests only clean, exactly-matched-revision
        // evidence does NOT clear the window back to trustworthy — the
        // count is never reset, so `assess` keeps reporting `Unavailable`
        // for this activation. (A prior version of this test asserted the
        // opposite — that the mismatch was forgiven — without ever actually
        // injecting one, via direct state construction rather than real
        // event ingestion; this exercises the real boundary through
        // `tick`/`Space` instead.)
        let dir = tempfile::tempdir().unwrap();
        let layout = rk_core::paths::Layout::at(dir.path());
        let space = rk_space::Space::open_in_memory().unwrap();
        let repo = "fixture-repo";
        {
            let mut registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
            registry
                .configure(repo, valid_objective(), "operator")
                .unwrap();
            // Activation binds source_feature_revision = 1 EXACTLY.
            registry
                .activate(repo, &space, "build-1".into(), 1, "operator")
                .unwrap();
        }

        // A mismatched-revision row first.
        space
            .out(well_formed_event_tuple_revision(
                repo,
                1,
                0,
                "01MISMATCH",
                2,
            ))
            .unwrap();
        let outcome1 = tick(&space, &layout, repo, "test-castle", Utc::now()).unwrap();
        assert_eq!(outcome1.events_consumed, 0);
        assert_eq!(outcome1.verdict, Verdict::Unavailable);

        // A later tick observing ONLY matched-revision, complete, clean
        // evidence — no NEW mismatch.
        space
            .out(well_formed_event_tuple_revision(repo, 1, 0, "01MATCHED", 1))
            .unwrap();
        let outcome2 = tick(&space, &layout, repo, "test-castle", Utc::now()).unwrap();
        assert_eq!(outcome2.events_consumed, 1);

        let registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
        let state = &registry.record(repo).unwrap().state;
        assert_eq!(
            state.revision_mismatch_events, 1,
            "the past mismatch is retained, never reset by later clean evidence"
        );
        assert_eq!(state.resolved_total, 1);
        assert_eq!(
            outcome2.verdict,
            Verdict::Unavailable,
            "a past revision mismatch keeps the window untrustworthy even after later clean, \
             exactly-matched-revision evidence"
        );
    }

    fn well_formed_event_tuple(repo: &str, resolved: u64, failed: u64, need_id: &str) -> Tuple {
        well_formed_event_tuple_revision(repo, resolved, failed, need_id, 1)
    }

    fn well_formed_event_tuple_revision(
        repo: &str,
        resolved: u64,
        failed: u64,
        need_id: &str,
        config_revision: u64,
    ) -> Tuple {
        landing_need_resolution::telemetry_event(
            repo,
            "test-castle",
            &landing_need_resolution::RetirementConfig {
                enabled: true,
                revision: config_revision,
                status: rk_core::bbs::ConfigStatus::Explicit,
            },
            &landing_need_resolution::RetirementOutcome {
                attempted: (resolved + failed) as usize,
                resolved: resolved as usize,
                skipped: 0,
                failed: failed as usize,
            },
            &if resolved > 0 {
                vec![serde_json::json!({"need_id": need_id, "branch": "rat/x/tkt-x"})]
            } else {
                vec![]
            },
        )
    }

    #[tokio::test]
    async fn a_real_activate_then_tick_journey_reaches_pass_within_one_tick() {
        let dir = tempfile::tempdir().unwrap();
        let layout = rk_core::paths::Layout::at(dir.path());
        layout.ensure().unwrap();
        let space = rk_space::Space::open_in_memory().unwrap();
        let repo = "fixture-repo";

        let mut registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
        registry
            .configure(repo, valid_objective(), "operator")
            .unwrap();
        registry
            .activate(repo, &space, "build-1".into(), 1, "operator")
            .unwrap();
        drop(registry);

        // A real retirement pass's telemetry event, persisted AFTER
        // activation.
        space
            .out(well_formed_event_tuple(repo, 1, 0, "01NEED"))
            .unwrap();

        let outcome = tick(&space, &layout, repo, "test-castle", Utc::now()).unwrap();
        assert_eq!(outcome.events_consumed, 1, "{}", outcome.events_consumed);
        assert_eq!(outcome.verdict, Verdict::Pass);
        assert!(outcome.published);

        let registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
        let record = registry.record(repo).unwrap();
        assert_eq!(record.state.resolved_total, 1);
        let latest = record.latest.as_ref().unwrap();
        assert_eq!(latest.verdict, Verdict::Pass);
        assert_eq!(latest.numerator, 1);
        assert_eq!(latest.denominator, 1);

        let published = space
            .scan(
                &rk_core::tuple::Pattern::category(Category::Event)
                    .scope(repo)
                    .identity(ASSESSMENT_RESULT_IDENTITY),
            )
            .unwrap();
        assert_eq!(published.len(), 1, "{published:?}");
        assert_eq!(published[0].payload["verdict"], "pass");
    }

    #[tokio::test]
    async fn resume_from_durable_checkpoint_never_double_counts() {
        let dir = tempfile::tempdir().unwrap();
        let layout = rk_core::paths::Layout::at(dir.path());
        let store_path = dir.path().join("space.db");
        let space = rk_space::Space::open(&store_path).unwrap();
        let repo = "fixture-repo";

        {
            let mut registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
            registry
                .configure(repo, valid_objective(), "operator")
                .unwrap();
            registry
                .activate(repo, &space, "build-1".into(), 1, "operator")
                .unwrap();
        }
        space
            .out(well_formed_event_tuple(repo, 1, 0, "01NEED-A"))
            .unwrap();
        let outcome1 = tick(&space, &layout, repo, "test-castle", Utc::now()).unwrap();
        assert_eq!(outcome1.events_consumed, 1);

        // Simulate a crash + restart: drop and reopen the Space from the same
        // file; the assessment registry is already durable on disk.
        drop(space);
        let space = rk_space::Space::open(&store_path).unwrap();

        // A second tick with NO new events must not reprocess the first.
        let outcome2 = tick(&space, &layout, repo, "test-castle", Utc::now()).unwrap();
        assert_eq!(outcome2.events_consumed, 0);
        let registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
        assert_eq!(registry.record(repo).unwrap().state.resolved_total, 1);

        // A late-arriving second event is picked up cumulatively, not
        // replacing the first.
        space
            .out(well_formed_event_tuple(repo, 1, 0, "01NEED-B"))
            .unwrap();
        let outcome3 = tick(&space, &layout, repo, "test-castle", Utc::now()).unwrap();
        assert_eq!(outcome3.events_consumed, 1);
        let registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
        let record = registry.record(repo).unwrap();
        assert_eq!(record.state.resolved_total, 2);
    }

    #[test]
    fn duplicate_native_event_ids_are_deduplicated_not_double_counted() {
        // A data-pipeline replay of the IDENTICAL native tuple (same id)
        // must never count its operation twice.
        let mut state = AssessmentState::default();
        let tuple = well_formed_event_tuple("fixture-repo", 1, 0, "01NEED");
        let mut consumed = 0u64;
        ingest_retirement_event(&mut state, &tuple, 1, &mut consumed);
        ingest_retirement_event(&mut state, &tuple, 1, &mut consumed);
        assert_eq!(
            state.resolved_total, 1,
            "a replayed identical native event id must not double count"
        );
        assert_eq!(consumed, 1);
        assert_eq!(state.events_consumed, 1);
    }

    #[test]
    fn two_distinct_operations_resolving_the_same_need_both_count_toward_the_metric() {
        // Different native operation events may legitimately name the same
        // Need (a genuine retry): both are real observed work and both
        // count toward the predeclared sum(resolved) metric. The SEPARATE
        // distinct-Need minimum is unaffected.
        let mut state = AssessmentState::default();
        let t1 = well_formed_event_tuple("fixture-repo", 1, 0, "01SAME");
        let t2 = well_formed_event_tuple("fixture-repo", 1, 0, "01SAME");
        let mut consumed = 0u64;
        ingest_retirement_event(&mut state, &t1, 1, &mut consumed);
        ingest_retirement_event(&mut state, &t2, 1, &mut consumed);
        assert_eq!(state.resolved_total, 2, "both distinct operations count");
        assert_eq!(
            state.distinct_resolved_needs.len(),
            1,
            "the separate distinct-Need minimum is not inflated"
        );
        assert_eq!(consumed, 2);
    }

    #[test]
    fn seen_event_id_bound_overflow_is_reported_not_silently_dropped() {
        let mut state = AssessmentState::default();
        let mut consumed = 0u64;
        for i in 0..(MAX_SEEN_EVENT_IDS + 5) {
            let tuple = well_formed_event_tuple("fixture-repo", 1, 0, &format!("01N{i}"));
            ingest_retirement_event(&mut state, &tuple, 1, &mut consumed);
        }
        assert!(state.seen_event_ids_truncated);
        let (verdict, reason) = assess(&valid_objective(), &state, Utc::now());
        assert_eq!(verdict, Verdict::Unavailable, "{reason}");
    }

    #[tokio::test]
    async fn a_retried_pass_resolving_the_same_need_twice_via_real_tick() {
        let dir = tempfile::tempdir().unwrap();
        let layout = rk_core::paths::Layout::at(dir.path());
        let space = rk_space::Space::open_in_memory().unwrap();
        let repo = "fixture-repo";
        {
            let mut registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
            registry
                .configure(repo, valid_objective(), "operator")
                .unwrap();
            registry
                .activate(repo, &space, "build-1".into(), 1, "operator")
                .unwrap();
        }
        // Two SEPARATE telemetry events (as a crash-then-retry of the SAME
        // underlying retirement pass would produce, per
        // `landing_need_resolution`'s own crash-recovery doc comment) naming
        // the SAME need_id — both genuinely happened, so both count.
        space
            .out(well_formed_event_tuple(repo, 1, 0, "01SAME"))
            .unwrap();
        space
            .out(well_formed_event_tuple(repo, 1, 0, "01SAME"))
            .unwrap();
        let outcome = tick(&space, &layout, repo, "test-castle", Utc::now()).unwrap();
        assert_eq!(
            outcome.events_consumed, 2,
            "both events are validly consumed"
        );
        let registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
        let record = registry.record(repo).unwrap();
        assert_eq!(
            record.state.resolved_total, 2,
            "two distinct native operations both count toward the predeclared sum metric"
        );
        assert_eq!(
            record.state.distinct_resolved_needs.len(),
            1,
            "the separate distinct-Need minimum is not inflated"
        );
    }

    #[tokio::test]
    async fn attempted_not_matching_resolved_plus_skipped_plus_failed_is_malformed() {
        let dir = tempfile::tempdir().unwrap();
        let layout = rk_core::paths::Layout::at(dir.path());
        let space = rk_space::Space::open_in_memory().unwrap();
        let repo = "fixture-repo";
        {
            let mut registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
            registry
                .configure(repo, valid_objective(), "operator")
                .unwrap();
            registry
                .activate(repo, &space, "build-1".into(), 1, "operator")
                .unwrap();
        }
        space
            .out(
                Tuple::new(
                    Category::Event,
                    repo.to_string(),
                    RETIREMENT_TELEMETRY_IDENTITY,
                    "test-castle".to_string(),
                    serde_json::json!({
                        "feature": landing_need_resolution::FEATURE_ID,
                        "build": "x",
                        "config_revision": 1,
                        "attempted": 5,
                        "resolved": 1,
                        "skipped": 1,
                        "failed": 1,
                    }),
                )
                .with_lifecycle(Lifecycle::Furniture),
            )
            .unwrap();
        let outcome = tick(&space, &layout, repo, "test-castle", Utc::now()).unwrap();
        assert_eq!(outcome.events_consumed, 0);
        let registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
        let record = registry.record(repo).unwrap();
        assert_eq!(record.state.malformed_events, 1);
        assert_eq!(record.state.failed_total, 0);
    }

    #[tokio::test]
    async fn incomplete_bindings_for_a_nontruncated_resolved_row_is_malformed() {
        let dir = tempfile::tempdir().unwrap();
        let layout = rk_core::paths::Layout::at(dir.path());
        let space = rk_space::Space::open_in_memory().unwrap();
        let repo = "fixture-repo";
        {
            let mut registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
            registry
                .configure(repo, valid_objective(), "operator")
                .unwrap();
            registry
                .activate(repo, &space, "build-1".into(), 1, "operator")
                .unwrap();
        }
        space
            .out(
                Tuple::new(
                    Category::Event,
                    repo.to_string(),
                    RETIREMENT_TELEMETRY_IDENTITY,
                    "test-castle".to_string(),
                    serde_json::json!({
                        "feature": landing_need_resolution::FEATURE_ID,
                        "build": "x",
                        "config_revision": 1,
                        "attempted": 2,
                        "resolved": 2,
                        "skipped": 0,
                        "failed": 0,
                        "resolved_bindings": [{"need_id": "01ONLY"}],
                        "resolved_bindings_truncated": false,
                    }),
                )
                .with_lifecycle(Lifecycle::Furniture),
            )
            .unwrap();
        let outcome = tick(&space, &layout, repo, "test-castle", Utc::now()).unwrap();
        assert_eq!(outcome.events_consumed, 0);
        let registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
        assert_eq!(registry.record(repo).unwrap().state.malformed_events, 1);
    }

    #[tokio::test]
    async fn only_the_exact_bound_revision_is_counted_any_mismatch_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let layout = rk_core::paths::Layout::at(dir.path());
        let space = rk_space::Space::open_in_memory().unwrap();
        let repo = "fixture-repo";
        {
            let mut registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
            registry
                .configure(repo, valid_objective(), "operator")
                .unwrap();
            // Activation binds source_feature_revision = 5 EXACTLY.
            registry
                .activate(repo, &space, "build-1".into(), 5, "operator")
                .unwrap();
        }
        // An OLDER revision: rejected.
        space
            .out(well_formed_event_tuple_revision(repo, 1, 0, "01OLD", 3))
            .unwrap();
        let outcome = tick(&space, &layout, repo, "test-castle", Utc::now()).unwrap();
        assert_eq!(outcome.events_consumed, 0);
        assert_eq!(outcome.verdict, Verdict::Unavailable);
        let registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
        let record = registry.record(repo).unwrap();
        assert_eq!(record.state.revision_mismatch_events, 1);
        assert_eq!(record.state.resolved_total, 0);
        // Every observed revision is still explicitly recorded, even a
        // rejected one.
        assert!(record.state.observed_feature_config_revisions.contains(&3));

        // A NEWER revision (the feature was reconfigured mid-window) is
        // ALSO rejected — only the EXACT bound revision counts.
        space
            .out(well_formed_event_tuple_revision(repo, 1, 0, "01NEWER", 6))
            .unwrap();
        let outcome2 = tick(&space, &layout, repo, "test-castle", Utc::now()).unwrap();
        assert_eq!(outcome2.events_consumed, 0);
        let registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
        assert_eq!(
            registry
                .record(repo)
                .unwrap()
                .state
                .revision_mismatch_events,
            2
        );

        // Only the exact bound revision (5) is accepted.
        space
            .out(well_formed_event_tuple_revision(repo, 1, 0, "01EXACT", 5))
            .unwrap();
        let outcome3 = tick(&space, &layout, repo, "test-castle", Utc::now()).unwrap();
        assert_eq!(outcome3.events_consumed, 1);
        let registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
        assert_eq!(registry.record(repo).unwrap().state.resolved_total, 1);
    }

    #[test]
    fn a_missing_source_with_no_events_yet_is_unavailable() {
        let dir = tempfile::tempdir().unwrap();
        let layout = rk_core::paths::Layout::at(dir.path());
        let space = rk_space::Space::open_in_memory().unwrap();
        let repo = "fixture-repo";
        {
            let mut registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
            registry
                .configure(repo, valid_objective(), "operator")
                .unwrap();
            registry
                .activate(repo, &space, "build-1".into(), 1, "operator")
                .unwrap();
        }
        let outcome = tick(&space, &layout, repo, "test-castle", Utc::now()).unwrap();
        assert_eq!(outcome.events_consumed, 0);
        assert_eq!(outcome.verdict, Verdict::Unavailable);
    }

    #[test]
    fn events_persisted_before_activation_never_retroactively_count() {
        let dir = tempfile::tempdir().unwrap();
        let layout = rk_core::paths::Layout::at(dir.path());
        let space = rk_space::Space::open_in_memory().unwrap();
        let repo = "fixture-repo";

        // Old exposure, written BEFORE activation.
        space
            .out(well_formed_event_tuple(repo, 1, 0, "01OLD"))
            .unwrap();

        {
            let mut registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
            registry
                .configure(repo, valid_objective(), "operator")
                .unwrap();
            registry
                .activate(repo, &space, "build-1".into(), 1, "operator")
                .unwrap();
        }
        let outcome = tick(&space, &layout, repo, "test-castle", Utc::now()).unwrap();
        assert_eq!(
            outcome.events_consumed, 0,
            "pre-activation event must not retroactively satisfy new exposure"
        );
        assert_eq!(outcome.verdict, Verdict::Unavailable);
    }

    #[test]
    fn reconfigure_while_active_retains_history_and_does_not_reset_failure() {
        let dir = tempfile::tempdir().unwrap();
        let layout = rk_core::paths::Layout::at(dir.path());
        let space = rk_space::Space::open_in_memory().unwrap();
        let repo = "fixture-repo";
        {
            let mut registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
            registry
                .configure(repo, valid_objective(), "operator")
                .unwrap();
            registry
                .activate(repo, &space, "build-1".into(), 1, "operator")
                .unwrap();
        }
        space
            .out(well_formed_event_tuple(repo, 0, 1, "01NEED"))
            .unwrap();
        tick(&space, &layout, repo, "test-castle", Utc::now()).unwrap();
        {
            let registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
            assert_eq!(
                registry
                    .record(repo)
                    .unwrap()
                    .latest
                    .as_ref()
                    .unwrap()
                    .verdict,
                Verdict::Fail
            );
        }

        // Reconfigure the SAME objective with a different page_limit; must
        // not reset accumulated failure.
        {
            let mut registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
            let mut objective = valid_objective();
            objective.page_limit = 64;
            registry.configure(repo, objective, "operator").unwrap();
        }
        let outcome = tick(&space, &layout, repo, "test-castle", Utc::now()).unwrap();
        assert_eq!(
            outcome.verdict,
            Verdict::Fail,
            "reconfigure must not clear ever_failed"
        );
    }

    #[test]
    fn disable_then_reactivate_starts_a_fresh_window_but_keeps_history() {
        let dir = tempfile::tempdir().unwrap();
        let layout = rk_core::paths::Layout::at(dir.path());
        let space = rk_space::Space::open_in_memory().unwrap();
        let repo = "fixture-repo";
        {
            let mut registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
            registry
                .configure(repo, valid_objective(), "operator")
                .unwrap();
            registry
                .activate(repo, &space, "build-1".into(), 1, "operator")
                .unwrap();
        }
        space
            .out(well_formed_event_tuple(repo, 0, 1, "01NEED"))
            .unwrap();
        tick(&space, &layout, repo, "test-castle", Utc::now()).unwrap();

        {
            let mut registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
            registry.disable(repo, "operator").unwrap();
            let record = registry.record(repo).unwrap();
            assert!(record.activation.is_none());
            assert_eq!(record.state.failed_total, 1, "disable retains prior state");
        }

        // Old failing event still sits before the OLD activation boundary
        // conceptually; re-activate captures a NEW boundary at current
        // sequence, so old events (already consumed) cannot double count and
        // the fresh window starts clean.
        {
            let mut registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
            registry
                .activate(repo, &space, "build-2".into(), 1, "operator")
                .unwrap();
            let record = registry.record(repo).unwrap();
            assert_eq!(record.state.failed_total, 0, "re-activation starts fresh");
            assert_eq!(record.history.len(), 1, "prior window retained in history");
            assert_eq!(record.history[0].final_state.failed_total, 1);
        }
        let outcome = tick(&space, &layout, repo, "test-castle", Utc::now()).unwrap();
        assert_eq!(outcome.events_consumed, 0);
        assert_eq!(outcome.verdict, Verdict::Unavailable);
    }

    #[test]
    fn due_repos_selects_a_never_ticked_active_repo_and_skips_disabled_ones() {
        let dir = tempfile::tempdir().unwrap();
        let layout = rk_core::paths::Layout::at(dir.path());
        let space = rk_space::Space::open_in_memory().unwrap();
        {
            let mut registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
            registry
                .configure("active-repo", valid_objective(), "operator")
                .unwrap();
            registry
                .activate("active-repo", &space, "build-1".into(), 1, "operator")
                .unwrap();
            registry
                .configure("disabled-repo", valid_objective(), "operator")
                .unwrap();
            // Never activated.
        }
        let due = due_repos(&layout, Utc::now()).unwrap();
        assert_eq!(due, vec!["active-repo".to_string()]);
    }

    #[test]
    fn due_repos_waits_out_the_declared_cadence_between_ticks() {
        let dir = tempfile::tempdir().unwrap();
        let layout = rk_core::paths::Layout::at(dir.path());
        let space = rk_space::Space::open_in_memory().unwrap();
        let repo = "fixture-repo";
        let now = Utc::now();
        {
            let mut registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
            registry
                .configure(repo, valid_objective(), "operator")
                .unwrap();
            registry
                .activate(repo, &space, "build-1".into(), 1, "operator")
                .unwrap();
        }
        tick(&space, &layout, repo, "test-castle", now).unwrap();
        // Immediately after a tick, not yet due again (cadence is 30s).
        assert!(due_repos(&layout, now + ChronoDuration::seconds(5))
            .unwrap()
            .is_empty());
        // Past the cadence, due again.
        assert_eq!(
            due_repos(&layout, now + ChronoDuration::seconds(31)).unwrap(),
            vec![repo.to_string()]
        );
    }

    #[test]
    fn sweep_due_autonomously_advances_an_activated_repo_with_no_manual_tick() {
        let dir = tempfile::tempdir().unwrap();
        let layout = rk_core::paths::Layout::at(dir.path());
        let space = rk_space::Space::open_in_memory().unwrap();
        let repo = "fixture-repo";
        {
            let mut registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
            registry
                .configure(repo, valid_objective(), "operator")
                .unwrap();
            registry
                .activate(repo, &space, "build-1".into(), 1, "operator")
                .unwrap();
        }
        space
            .out(well_formed_event_tuple(repo, 1, 0, "01NEED"))
            .unwrap();
        // Note: `tick` is never called directly here.
        let ticked = sweep_due(&space, &layout, Utc::now());
        assert_eq!(ticked, 1);
        let registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
        assert_eq!(
            registry
                .record(repo)
                .unwrap()
                .latest
                .as_ref()
                .unwrap()
                .verdict,
            Verdict::Pass
        );
    }

    #[test]
    fn concurrent_ticks_on_the_same_repo_never_corrupt_the_registry() {
        let dir = tempfile::tempdir().unwrap();
        let layout = rk_core::paths::Layout::at(dir.path());
        let store_path = dir.path().join("space.db");
        let space = rk_space::Space::open(&store_path).unwrap();
        let repo = "fixture-repo";
        {
            let mut registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
            registry
                .configure(repo, valid_objective(), "operator")
                .unwrap();
            registry
                .activate(repo, &space, "build-1".into(), 1, "operator")
                .unwrap();
        }
        for i in 0..20 {
            space
                .out(well_formed_event_tuple(repo, 1, 0, &format!("01N{i}")))
                .unwrap();
        }
        let barrier = Arc::new(std::sync::Barrier::new(4));
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let space = space.clone();
                let layout = layout.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..5 {
                        let _ = tick(&space, &layout, repo, "test-castle", Utc::now());
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        // The registry must still be validly readable (no torn/corrupt
        // write survived), and every one of the 20 genuinely distinct
        // native events must be counted exactly once regardless of which
        // racing tick's page happened to observe it — the per-event native
        // identity dedup, not the registry lock alone, is what prevents a
        // page observed by two racing ticks from double counting.
        let registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
        let record = registry.record(repo).unwrap();
        assert_eq!(record.state.resolved_total, 20, "{record:?}");
        assert_eq!(record.state.distinct_resolved_needs.len(), 20, "{record:?}");
        let published = space
            .scan(
                &rk_core::tuple::Pattern::category(Category::Event)
                    .scope(repo)
                    .identity(ASSESSMENT_RESULT_IDENTITY),
            )
            .unwrap();
        assert_eq!(
            published.len(),
            1,
            "reinforce must upsert one live result tuple even under concurrent republishing: {published:?}"
        );
    }

    #[test]
    fn a_forced_republish_of_the_identical_assessment_upserts_rather_than_duplicates() {
        // Exercises the exact crash window the atomicity finding named:
        // `space.reinforce` for the result succeeds, but the checkpoint
        // persist that would normally prevent a second identical publish
        // never lands (simulated here by clearing `latest` back to `None`
        // after a successful tick, forcing `meaningfully_changed` to be
        // true again on the next tick with byte-identical content).
        let dir = tempfile::tempdir().unwrap();
        let layout = rk_core::paths::Layout::at(dir.path());
        let space = rk_space::Space::open_in_memory().unwrap();
        let repo = "fixture-repo";
        {
            let mut registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
            registry
                .configure(repo, valid_objective(), "operator")
                .unwrap();
            registry
                .activate(repo, &space, "build-1".into(), 1, "operator")
                .unwrap();
        }
        space
            .out(well_formed_event_tuple(repo, 1, 0, "01NEED"))
            .unwrap();
        let now = Utc::now();
        tick(&space, &layout, repo, "test-castle", now).unwrap();

        let path = registry_path(&layout);
        {
            let mut registry = AssessmentRegistry::load(&path).unwrap();
            let record = registry.record(repo).unwrap().clone();
            registry.save_progress(repo, record.state, None).unwrap();
            // Force-clear `latest` directly to simulate "the checkpoint that
            // would have recorded this publish never landed".
            let mut raw: BTreeMap<String, AssessmentRecord> =
                serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
            raw.get_mut(repo).unwrap().latest = None;
            std::fs::write(&path, serde_json::to_vec_pretty(&raw).unwrap()).unwrap();
        }

        // Same cursor, same data: the recomputed assessment is byte-for-byte
        // identical to the one already reinforced above.
        tick(&space, &layout, repo, "test-castle", now).unwrap();

        let published = space
            .scan(
                &rk_core::tuple::Pattern::category(Category::Event)
                    .scope(repo)
                    .identity(ASSESSMENT_RESULT_IDENTITY),
            )
            .unwrap();
        assert_eq!(
            published.len(),
            1,
            "a forced re-publish of identical content must upsert, never duplicate: {published:?}"
        );
    }
}

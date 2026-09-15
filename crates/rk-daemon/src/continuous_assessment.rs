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
//! Config shape mirrors `crate::landing_need_resolution` and
//! `crate::bbs_discovery`: one JSON-file-backed registry, mutated only
//! through validated operations, resolved fresh on every read. Unlike those
//! modules this registry also carries MUTABLE reducer/checkpoint state and
//! the latest published assessment, because the whole point of this feature
//! is to accumulate state across ticks — but every mutation still goes
//! through the same atomic tmp-then-rename write those modules use, so a
//! crash mid-write never corrupts the file.
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
//!
//! # Verdicts
//!
//! Computed in [`assess`] from the accumulated [`AssessmentState`] and the
//! active [`ObjectiveConfig`]: `pass`, `fail`, `inconclusive`, `unavailable`.
//! A single observed `failed > 0` sets a STICKY `ever_failed` flag in the
//! persisted state — once true, the verdict is `fail` forever for this
//! activation, surviving a daemon restart or an unrelated binary release
//! (`docs/2026-09-13-continuous-validation-promotion.md` 7.2: "A new release
//! cannot reset an ongoing feature's evaluation clock or discard its
//! failures"; "known failure cannot become pass through restart or version
//! churn"). A malformed telemetry payload is NOT sticky: it marks that one
//! tick `unavailable` (never fabricates a zero), but a later tick with clean
//! evidence can still reach `pass`/`fail`/`inconclusive` on its own merits.

use crate::landing_need_resolution;
use chrono::{DateTime, Utc};
use rk_core::paths::Layout;
use rk_core::tuple::{Category, Lifecycle, Tuple};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// The only objective this slice understands. A config naming any other
/// value is rejected outright — see [`ObjectiveConfig::validate`].
pub const SUPPORTED_OBJECTIVE_ID: &str = "rk-retirement-observed-operation-success";
pub const SUPPORTED_OBJECTIVE_VERSION: u32 = 1;

/// The one existing telemetry identity this slice consumes, produced by
/// `landing_need_resolution::telemetry_event`. Kept as a distinct constant
/// (rather than importing that module's private literal) so a rename there
/// is a deliberate, visible break here too.
pub const RETIREMENT_TELEMETRY_IDENTITY: &str = "landing-need-retirement-run";

/// Identity this module writes its own published assessments under.
/// `Category::Event` is never scanned by `bbs::brief` (only
/// `Claim`/`Need`/`Artifact` are), so continuous assessment telemetry can
/// never crowd a real finding or artifact out of a briefing.
pub const ASSESSMENT_RESULT_IDENTITY: &str = "continuous-assessment-result";

const MAX_DISTINCT_RESOLVED_NEEDS: usize = 4096;
const MAX_OBSERVED_CONFIG_REVISIONS: usize = 32;
const MAX_OBSERVED_BUILDS: usize = 16;
const MAX_SOURCE_REFERENCES: usize = 16;
const MAX_HISTORY_ENTRIES: usize = 8;
const MAX_PAGE_LIMIT: usize = 2000;
const MAX_PAGES_PER_TICK: usize = 16;
const MAX_CADENCE_SECONDS: u64 = 3600;
const MAX_FRESHNESS_SECONDS: u64 = 86_400;
const MAX_TICKS_TO_REFLECT: u64 = 100;

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
    /// identities — repeated bindings of the same Need cannot manufacture
    /// distinct successful work.
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
    /// activation time, recorded for provenance even though later drift in
    /// that revision does not reset this window.
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
    pub resolved_total: u64,
    pub skipped_total: u64,
    pub failed_total: u64,
    pub distinct_resolved_needs: BTreeSet<String>,
    pub distinct_resolved_truncated: bool,
    pub observed_feature_config_revisions: BTreeSet<u64>,
    pub observed_builds: BTreeSet<String>,
    /// Telemetry rows that named this feature/identity but carried
    /// missing/non-numeric required counters. Never folded into
    /// `failed_total` (a producer error is not evidence the OPERATION
    /// failed) and never silently dropped either.
    pub malformed_events: u64,
    /// Sticky: once true, [`assess`] always returns `Fail` for this
    /// activation.
    pub ever_failed: bool,
    pub events_consumed: u64,
    pub last_event_at: Option<DateTime<Utc>>,
    pub last_tick_at: Option<DateTime<Utc>>,
    pub last_tick_pages: usize,
    /// True when the journal held more matching rows beyond this tick's
    /// bounded page budget — reported, never hidden.
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
        if self.distinct_resolved_needs.len() >= MAX_DISTINCT_RESOLVED_NEEDS {
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
}

/// A deterministic, reproducible published verdict — never mutated in place;
/// each meaningful change writes a new one.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// Persist an updated `state`/`latest` for `repo` after a tick, without
    /// disturbing `objective`/`activation`/`revision`. The one mutation path
    /// [`tick`] uses.
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
        if latest.is_some() {
            record.latest = latest;
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
    let mut registry = AssessmentRegistry::load(&registry_path(layout))?;
    let record = registry.configure(&params.repo, params.objective.clone(), caller)?;
    let mut value = describe(&params.repo, &record);
    value["rollover_required"] = serde_json::json!(false);
    value["rollover_note"] = serde_json::json!(
        "applied on the next assessment.tick for this repo; no daemon restart is required."
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
    let mut registry = AssessmentRegistry::load(&registry_path(layout))?;
    let record = registry.activate(
        &params.repo,
        space,
        rk_core::version::build_version().to_string(),
        source_feature_revision,
        caller,
    )?;
    Ok(describe(&params.repo, &record))
}

pub fn disable(
    layout: &Layout,
    caller: &str,
    params: &DisableParams,
) -> rk_core::Result<serde_json::Value> {
    let mut registry = AssessmentRegistry::load(&registry_path(layout))?;
    let record = registry.disable(&params.repo, caller)?;
    let mut value = describe(&params.repo, &record);
    value["retained_note"] = serde_json::json!(
        "prior evidence, checkpoint and last assessment are retained and still readable"
    );
    Ok(value)
}

/// Compute the deterministic verdict for the current accumulated state under
/// `objective`. `denominator = resolved + failed`, matching the objective's
/// declared metric exactly.
pub fn assess(objective: &ObjectiveConfig, state: &AssessmentState) -> (Verdict, String) {
    if state.malformed_events > 0 && state.resolved_total == 0 && state.failed_total == 0 {
        return (
            Verdict::Unavailable,
            format!(
                "{} telemetry row(s) carried missing/non-numeric required counters and no valid \
                 evidence has been observed yet",
                state.malformed_events
            ),
        );
    }
    if state.ever_failed {
        return (
            Verdict::Fail,
            "at least one observed failed operation remains a failure for this activation \
             (sticky: cannot become pass through restart or version churn)"
                .to_string(),
        );
    }
    let denominator = state.resolved_total + state.failed_total;
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
    // A truncated distinct set is only ever an undercount, so treat it as a
    // known lower bound that can still satisfy the minimum.
    if distinct < objective.minimum_distinct_resolved_needs_for_pass
        && !state.distinct_resolved_truncated
    {
        return (
            Verdict::Inconclusive,
            format!(
                "distinct resolved needs {distinct} below minimum {}",
                objective.minimum_distinct_resolved_needs_for_pass
            ),
        );
    }
    let ratio = state.resolved_total as f64 / denominator as f64;
    if ratio >= objective.required_ratio {
        (
            Verdict::Pass,
            format!(
                "ratio {ratio:.4} >= required {:.4} over {denominator} observed operation(s), \
                 {distinct} distinct resolved need(s)",
                objective.required_ratio
            ),
        )
    } else {
        (
            Verdict::Inconclusive,
            format!(
                "ratio {ratio:.4} below required {:.4}",
                objective.required_ratio
            ),
        )
    }
}

pub struct TickOutcome {
    pub pages_processed: usize,
    pub events_consumed: u64,
    pub truncated: bool,
    pub verdict: Verdict,
    pub published: bool,
}

/// Advance one repo's evaluator by up to `objective.maximum_pages_per_tick`
/// bounded pages, starting from the durably saved cursor. Pure and
/// synchronous — safe to call from a blocking RPC handler or a test, with an
/// injectable `now` so tests never need a real wall-clock wait.
pub fn tick(
    space: &rk_space::Space,
    layout: &Layout,
    repo: &str,
    castle: &str,
    now: DateTime<Utc>,
) -> rk_core::Result<TickOutcome> {
    let mut registry = AssessmentRegistry::load(&registry_path(layout))?;
    let record = registry
        .record(repo)
        .cloned()
        .ok_or_else(|| rk_core::Error::other(format!("repo '{repo}' has no assessment record")))?;
    let objective = record.objective.clone().ok_or_else(|| {
        rk_core::Error::other(format!("repo '{repo}' has no configured objective"))
    })?;
    if record.activation.is_none() {
        return Err(rk_core::Error::other(format!(
            "assessment is not activated for repo '{repo}'"
        )));
    }
    let mut state = record.state;

    let mut pin: Option<u64> = None;
    let mut pages_processed = 0usize;
    let mut events_consumed_this_tick = 0u64;
    let mut truncated = false;
    for _ in 0..objective.maximum_pages_per_tick {
        let page = space.persistence_page(repo, Some(state.cursor), objective.page_limit, pin)?;
        pin = Some(page.boundary);
        pages_processed += 1;
        for (_, tuple) in &page.entries {
            if tuple.category != Category::Event || tuple.identity != RETIREMENT_TELEMETRY_IDENTITY
            {
                continue;
            }
            ingest_retirement_event(&mut state, tuple, &mut events_consumed_this_tick);
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

    let (verdict, reason) = assess(&objective, &state);
    let denominator = state.resolved_total + state.failed_total;
    let meaningfully_changed = record
        .latest
        .as_ref()
        .map(|prior| {
            prior.verdict != verdict
                || prior.numerator != state.resolved_total
                || prior.denominator != denominator
                || prior.distinct_resolved_needs != state.distinct_resolved_needs.len() as u64
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
            cursor_range: (
                record
                    .activation
                    .as_ref()
                    .map(|a| a.activation_boundary)
                    .unwrap_or(0),
                state.cursor,
            ),
            numerator: state.resolved_total,
            denominator,
            distinct_resolved_needs: state.distinct_resolved_needs.len() as u64,
            distinct_resolved_truncated: state.distinct_resolved_truncated,
            published_at: now,
            source_references: state.source_references.clone(),
        };
        if let Err(error) = space.out(result_tuple(repo, castle, &assessment)) {
            tracing::warn!(%error, repo, "continuous-assessment: result telemetry write failed; checkpoint unaffected");
        } else {
            published = true;
        }
        latest_to_persist = Some(assessment);
    }

    registry.save_progress(repo, state, latest_to_persist)?;
    Ok(TickOutcome {
        pages_processed,
        events_consumed: events_consumed_this_tick,
        truncated,
        verdict,
        published,
    })
}

fn ingest_retirement_event(state: &mut AssessmentState, tuple: &Tuple, consumed: &mut u64) {
    let payload = &tuple.payload;
    if payload.get("feature").and_then(|v| v.as_str()) != Some(landing_need_resolution::FEATURE_ID)
    {
        return;
    }
    let counters = [
        payload.get("attempted").and_then(|v| v.as_u64()),
        payload.get("resolved").and_then(|v| v.as_u64()),
        payload.get("skipped").and_then(|v| v.as_u64()),
        payload.get("failed").and_then(|v| v.as_u64()),
    ];
    let config_revision = payload.get("config_revision").and_then(|v| v.as_u64());
    let (Some(attempted), Some(resolved), Some(skipped), Some(failed)) =
        (counters[0], counters[1], counters[2], counters[3])
    else {
        state.malformed_events += 1;
        return;
    };
    let Some(config_revision) = config_revision else {
        state.malformed_events += 1;
        return;
    };
    state.attempted_total += attempted;
    state.resolved_total += resolved;
    state.skipped_total += skipped;
    state.failed_total += failed;
    if failed > 0 {
        state.ever_failed = true;
    }
    state.record_config_revision(config_revision);
    if let Some(build) = payload.get("build").and_then(|v| v.as_str()) {
        state.record_build(build);
    }
    if payload
        .get("resolved_bindings_truncated")
        .and_then(|v| v.as_bool())
        == Some(true)
    {
        state.distinct_resolved_truncated = true;
    }
    if let Some(bindings) = payload.get("resolved_bindings").and_then(|v| v.as_array()) {
        for binding in bindings {
            if let Some(need_id) = binding.get("need_id").and_then(|v| v.as_str()) {
                state.record_resolved_need(need_id);
            }
        }
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
    fn zero_observations_is_inconclusive() {
        let objective = valid_objective();
        let state = AssessmentState::default();
        let (verdict, _) = assess(&objective, &state);
        assert_eq!(verdict, Verdict::Inconclusive);
    }

    #[test]
    fn a_single_resolved_need_is_pass_at_ratio_one() {
        let objective = valid_objective();
        let mut state = AssessmentState::default();
        state.resolved_total = 1;
        state.distinct_resolved_needs.insert("01ABC".into());
        let (verdict, _) = assess(&objective, &state);
        assert_eq!(verdict, Verdict::Pass);
    }

    #[test]
    fn any_failed_is_fail_even_under_minimum_denominator() {
        let mut objective = valid_objective();
        objective.minimum_denominator = 10;
        let mut state = AssessmentState::default();
        state.failed_total = 1;
        state.ever_failed = true;
        let (verdict, _) = assess(&objective, &state);
        assert_eq!(verdict, Verdict::Fail);
    }

    #[test]
    fn ever_failed_is_sticky_even_after_later_clean_events() {
        let objective = valid_objective();
        let mut state = AssessmentState::default();
        state.ever_failed = true;
        state.resolved_total = 100;
        state.distinct_resolved_needs.insert("01ABC".into());
        // failed_total could even be back at 0 if a later tick only observed
        // clean events, but `ever_failed` must still win.
        let (verdict, _) = assess(&objective, &state);
        assert_eq!(verdict, Verdict::Fail);
    }

    #[test]
    fn distinct_needs_below_minimum_is_inconclusive_not_pass() {
        let mut objective = valid_objective();
        objective.minimum_distinct_resolved_needs_for_pass = 2;
        let mut state = AssessmentState::default();
        state.resolved_total = 5; // same Need resolved 5 times somehow
        state.distinct_resolved_needs.insert("01ABC".into());
        let (verdict, _) = assess(&objective, &state);
        assert_eq!(verdict, Verdict::Inconclusive);
    }

    #[test]
    fn malformed_only_evidence_is_unavailable_not_a_fabricated_zero() {
        let objective = valid_objective();
        let mut state = AssessmentState::default();
        state.malformed_events = 3;
        let (verdict, _) = assess(&objective, &state);
        assert_eq!(verdict, Verdict::Unavailable);
    }

    fn well_formed_event_tuple(repo: &str, resolved: u64, failed: u64, need_id: &str) -> Tuple {
        landing_need_resolution::telemetry_event(
            repo,
            "test-castle",
            &landing_need_resolution::RetirementConfig {
                enabled: true,
                revision: 1,
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
        assert_eq!(record.state.distinct_resolved_needs.len(), 1);
        let latest = record.latest.as_ref().unwrap();
        assert_eq!(latest.verdict, Verdict::Pass);
        assert_eq!(latest.numerator, 1);
        assert_eq!(latest.denominator, 1);
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
        assert_eq!(record.state.distinct_resolved_needs.len(), 2);
    }

    #[test]
    fn malformed_counters_are_reported_not_counted_as_failed() {
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
        // A malformed telemetry row: right identity/category, but the
        // counters are not numbers.
        space
            .out(
                Tuple::new(
                    Category::Event,
                    repo.to_string(),
                    RETIREMENT_TELEMETRY_IDENTITY,
                    "test-castle".to_string(),
                    serde_json::json!({
                        "feature": landing_need_resolution::FEATURE_ID,
                        "attempted": "not-a-number",
                    }),
                )
                .with_lifecycle(Lifecycle::Furniture),
            )
            .unwrap();
        let outcome = tick(&space, &layout, repo, "test-castle", Utc::now()).unwrap();
        assert_eq!(outcome.verdict, Verdict::Unavailable);
        let registry = AssessmentRegistry::load(&registry_path(&layout)).unwrap();
        let record = registry.record(repo).unwrap();
        assert_eq!(record.state.malformed_events, 1);
        assert_eq!(record.state.failed_total, 0);
    }

    #[test]
    fn a_missing_source_with_no_events_yet_is_inconclusive() {
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
        assert_eq!(outcome.verdict, Verdict::Inconclusive);
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
        assert_eq!(outcome.verdict, Verdict::Inconclusive);
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
        assert_eq!(outcome.verdict, Verdict::Inconclusive);
    }
}

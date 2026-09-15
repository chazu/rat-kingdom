//! Per-repository, default-off feature: durably retire an obsolete daemon-
//! owned landing Need once the ticket's accepted delivery proves the exact
//! held incident head is included in it (TKT-lalap-zonop-lafoz).
//!
//! Config shape mirrors `crate::bbs_discovery` exactly — JSON-file-backed,
//! mutated only through validated RPC, resolved fresh on every read with no
//! daemon cache — because that module's own doc comment names itself the
//! template for "a second feature reuses the same mechanics under a new
//! file/module". This is a wholly separate flag: enabling or disabling BBS
//! discovery ranking has no effect here, and vice versa.
//!
//! Matching reuses `crate::current_needs::resolution_candidates` for the
//! repo/task/target/delivery-timing proof, but that function is deliberately
//! BROADER than this feature's contract — it accepts a replacement source
//! branch and never itself proves the incident's own head landed (see its
//! doc comment, and BBS finding `01M2HHCBRETF1EBWYHDQYJ8PEY`). This module
//! adds the missing, stricter proof on top: the incident's `head_sha` must
//! itself be an ancestor of (or equal to) the delivered merge commit, via the
//! same batched git ancestry check already used for `rk inbox`. A candidate
//! that fails this stricter check, or whose incident carries no readable
//! `head_sha`, is left standing — unknown or mismatched evidence is skipped,
//! never resolved. `current_needs`'s own broader inbox semantics are
//! untouched by this module.
//!
//! Resolving a Need here is a real, durable state transition — a
//! `Category::Resolution` provenance trail written FIRST, then deletion — not
//! a read-time display filter: once retired, every surface stops seeing it,
//! not just the caller of this pass. Trail-before-delete matters for restart
//! safety: an interruption between the two leaves the Need standing, so the
//! next pass regenerates the same candidate, proves the same ancestry again,
//! and reinforces (never duplicates) the same trail before retrying the
//! delete — provenance is never destroyed by a crash that never reaches the
//! delete.
//!
//! [`run_retirement_pass`] is the one shared, synchronous core, called from
//! two triggers: automatically, right after `LandingPipeline::record_delivery`
//! durably records an accepted delivery (`landing.rs`) — bounded to once per
//! successful land, not the hot spawn path, so this adds no git latency to
//! agent launch — and again defensively on the next `bbs.brief` RPC for the
//! same repo (`Daemon::retire_resolved_landing_needs` in `server.rs`), which
//! catches a Need whose delivery landed before the flag was ever turned on,
//! or whose post-landing pass itself failed. Both triggers converge on the
//! same idempotent state transition; neither is required for correctness on
//! its own. The separate spawn/resume/recovery briefing priming in
//! `supervisor.rs` (`crate::bbs::brief` called directly, with no access to
//! `Tickets` or git) is still not itself a trigger — it simply observes
//! whatever the two triggers above already retired.

use crate::current_needs::{LandingIncident, ResolutionCandidate};
use chrono::{DateTime, Utc};
use rk_core::bbs::ConfigStatus;
use rk_core::id::RecordId;
use rk_core::paths::Layout;
use rk_core::tuple::{Category, Lifecycle, Pattern, Tuple};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub const FEATURE_ID: &str = "landing-need-retirement";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureMode {
    Off,
    On,
}

impl FeatureMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::On => "on",
        }
    }

    /// Parse an operator-supplied mode string. Only `off`/`on` exist for this
    /// feature; anything else is refused rather than silently folded into
    /// `off`.
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "off" => Ok(Self::Off),
            "on" => Ok(Self::On),
            other => Err(format!(
                "unknown mode '{other}' for {FEATURE_ID}; supported: off, on"
            )),
        }
    }
}

/// The resolved setting for one repo. Never fails to resolve — a `bbs.brief`
/// read must always be computable even when this registry cannot be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetirementConfig {
    pub enabled: bool,
    /// `0` when no explicit per-repo record exists (implicit default off).
    /// Otherwise the exact revision of the record that produced `enabled`,
    /// even when that record set the repo back to `off` — provenance of the
    /// actual decision, not just its behavioral equivalence to no record.
    pub revision: u64,
    pub status: ConfigStatus,
}

impl RetirementConfig {
    fn default_absent() -> Self {
        Self {
            enabled: false,
            revision: 0,
            status: ConfigStatus::DefaultAbsent,
        }
    }

    fn unreadable_fallback() -> Self {
        Self {
            enabled: false,
            revision: 0,
            status: ConfigStatus::UnreadableFallback,
        }
    }
}

impl Default for RetirementConfig {
    fn default() -> Self {
        Self::default_absent()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureRecord {
    pub mode: FeatureMode,
    pub revision: u64,
    pub updated_at: DateTime<Utc>,
    pub updated_by: String,
}

/// JSON-file-backed, persisted synchronously on every mutation — the same
/// restart-memory contract as `crate::bbs_discovery::DiscoveryRegistry`.
pub struct RetirementRegistry {
    path: PathBuf,
    repos: HashMap<String, FeatureRecord>,
}

impl RetirementRegistry {
    /// Reads the file directly rather than checking existence first (racy,
    /// and a permission error would otherwise be relabeled as "never
    /// configured"). Only a genuine `NotFound` is absence; every other I/O or
    /// parse error propagates.
    pub fn load(path: &Path) -> rk_core::Result<Self> {
        let repos = match std::fs::read_to_string(path) {
            Ok(raw) => serde_json::from_str(&raw)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            path: path.to_path_buf(),
            repos,
        })
    }

    pub fn resolve(&self, repo: &str) -> RetirementConfig {
        match self.repos.get(repo) {
            Some(record) => RetirementConfig {
                enabled: record.mode == FeatureMode::On,
                revision: record.revision,
                status: ConfigStatus::Explicit,
            },
            None => RetirementConfig::default_absent(),
        }
    }

    pub fn record(&self, repo: &str) -> Option<&FeatureRecord> {
        self.repos.get(repo)
    }

    /// Validated write: `mode` must already be `Ok` from [`FeatureMode::parse`].
    pub fn set(
        &mut self,
        repo: &str,
        mode: FeatureMode,
        by: &str,
    ) -> rk_core::Result<FeatureRecord> {
        let revision = self.repos.get(repo).map_or(1, |r| r.revision + 1);
        let record = FeatureRecord {
            mode,
            revision,
            updated_at: Utc::now(),
            updated_by: by.to_string(),
        };
        self.repos.insert(repo.to_string(), record.clone());
        self.persist()?;
        Ok(record)
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
}

pub fn registry_path(layout: &Layout) -> PathBuf {
    layout.home().join("landing-need-retirement.json")
}

/// Resolve this repo's setting, degrading to disabled (never failing the
/// caller) on a load error.
pub fn resolve_for_repo(layout: &Layout, repo: &str) -> RetirementConfig {
    match RetirementRegistry::load(&registry_path(layout)) {
        Ok(registry) => registry.resolve(repo),
        Err(error) => {
            tracing::warn!(
                %error,
                repo,
                "landing-need-retirement config unreadable; leaving needs standing"
            );
            RetirementConfig::unreadable_fallback()
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShowParams {
    pub repo: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetParams {
    pub repo: String,
    pub mode: String,
}

fn describe(repo: &str, record: Option<&FeatureRecord>) -> serde_json::Value {
    let mode = record.map_or(FeatureMode::Off, |r| r.mode);
    serde_json::json!({
        "feature": FEATURE_ID,
        "repo": repo,
        "mode": mode.as_str(),
        "default": FeatureMode::Off.as_str(),
        "revision": record.map_or(0, |r| r.revision),
        "updated_at": record.map(|r| r.updated_at),
        "updated_by": record.map(|r| r.updated_by.as_str()),
    })
}

pub fn show(layout: &Layout, params: &ShowParams) -> rk_core::Result<serde_json::Value> {
    let registry = RetirementRegistry::load(&registry_path(layout))?;
    Ok(describe(&params.repo, registry.record(&params.repo)))
}

/// `repos` proves `params.repo` is a registered repository before this
/// mutates anything.
pub fn set(
    layout: &Layout,
    repos: &crate::repos::RepoRegistry,
    caller: &str,
    params: &SetParams,
) -> rk_core::Result<serde_json::Value> {
    if repos.get(&params.repo).is_none() {
        return Err(rk_core::Error::other(format!(
            "repository is not registered: {}",
            params.repo
        )));
    }
    let mode = FeatureMode::parse(&params.mode).map_err(rk_core::Error::other)?;
    let mut registry = RetirementRegistry::load(&registry_path(layout))?;
    let record = registry.set(&params.repo, mode, caller)?;
    let mut value = describe(&params.repo, Some(&record));
    value["rollover_required"] = serde_json::json!(false);
    value["rollover_note"] = serde_json::json!(
        "applied on the next bbs.brief for this repo; no daemon restart is required. Set mode back to \"off\" to disable — retained history (Resolution trails and prior run telemetry) is untouched and still readable."
    );
    Ok(value)
}

/// Bounded counters for one retirement pass over one repo's landing Needs.
/// `attempted` is every candidate `current_needs::resolution_candidates`
/// proposed (repo/task/target/delivery-timing already matched there); the
/// other three exhaustively partition it.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct RetirementOutcome {
    pub attempted: usize,
    /// The incident's own `head_sha` was proven an ancestor of (or equal to)
    /// the delivered merge commit, and that commit was proven to reach
    /// `target`: the Need was deleted and a `Resolution` trail recorded.
    pub resolved: usize,
    /// Ancestry could not be proven (absent, unknown, or the incident carries
    /// no readable `head_sha`/the Need row went missing). Left standing —
    /// never guessed at.
    pub skipped: usize,
    /// The batched ancestry check errored outright, or a delete/trail write
    /// itself errored. Recorded separately from `skipped` so a failure never
    /// masquerades as "nothing matched".
    pub failed: usize,
}

/// Durable per-Need provenance: which delivery retired it and on what proof.
/// `Category::Resolution` is the same decaying-trail category the reactor
/// already writes for a manual `--resolves` artifact (`reactor.rs`'s
/// `link_resolution`) — reused here as the "existing resolution/trail
/// mechanism", not a new record type. Written via `Space::reinforce`, so a
/// re-run after a crash between delete and this write simply upserts the same
/// trail rather than piling up duplicates.
pub fn resolution_trail(
    need: &Tuple,
    candidate: &ResolutionCandidate,
    incident: &LandingIncident,
    castle: &str,
) -> Tuple {
    Tuple::new(
        Category::Resolution,
        candidate.repo.clone(),
        format!("landing-need-retired-{}", need.id),
        castle.to_string(),
        serde_json::json!({
            "feature": FEATURE_ID,
            "need_id": need.id.to_string(),
            "task": need.payload.get("task"),
            "branch": incident.branch,
            "target": candidate.target,
            "head_sha": incident.head_sha,
            "merge_commit": candidate.merge_commit,
            "source_spawn": incident.source_spawn,
            "resolved_at": Utc::now(),
        }),
    )
    .with_lifecycle(Lifecycle::Ephemeral)
}

/// Hard cap on how many per-Need bindings one telemetry event embeds. This
/// pass is already bounded by how many landing Needs exist for one repo, but
/// the cap keeps the record itself bounded even against a pathological
/// backlog, consistent with `bound_json`'s bounding discipline used
/// elsewhere in this crate's king-decision payloads.
const MAX_TELEMETRY_BINDINGS: usize = 50;

/// Bounded, non-displayable telemetry for one retirement pass. `Category::
/// Event` is never scanned by `bbs::brief` (it only reads `Claim`/`Need`/
/// `Artifact`), so this can never crowd a real finding or artifact out of a
/// briefing the way an ordinary `--resolves` artifact could (the ticket's
/// "Additional observed relevance constraint") — no new discovery-exclusion
/// rule is needed for it to stay out of that view.
///
/// `bindings` carries the exact delivery identity (need, task, branch,
/// target, merge_commit, head_sha) for each Need actually resolved this
/// pass — the "exact delivery/config identity" the ticket's telemetry
/// contract asks for, alongside the executable `build` that computed it, on
/// top of the aggregate counters.
pub fn telemetry_event(
    repo: &str,
    castle: &str,
    config: &RetirementConfig,
    outcome: &RetirementOutcome,
    bindings: &[serde_json::Value],
) -> Tuple {
    let bounded: Vec<_> = bindings.iter().take(MAX_TELEMETRY_BINDINGS).collect();
    Tuple::new(
        Category::Event,
        repo.to_string(),
        "landing-need-retirement-run",
        castle.to_string(),
        serde_json::json!({
            "bbs_kind": "landing_need_retirement_run",
            "feature": FEATURE_ID,
            "build": rk_core::version::build_version(),
            "config_revision": config.revision,
            "config_status": config.status.as_str(),
            "attempted": outcome.attempted,
            "resolved": outcome.resolved,
            "skipped": outcome.skipped,
            "failed": outcome.failed,
            "resolved_bindings": bounded,
            "resolved_bindings_truncated": bindings.len() > MAX_TELEMETRY_BINDINGS,
        }),
    )
    .with_lifecycle(Lifecycle::Furniture)
}

/// The one shared retirement pass — pure storage plus an already-open git
/// repo, no async. Safe to call either wrapped in `spawn_blocking` (the
/// `bbs.brief` RPC path, where a blocking git subprocess must not stall the
/// async dispatch loop) or directly (the post-landing hook in `landing.rs`,
/// which already performs blocking git operations inline in that file's
/// established style — see e.g. `LandingPipeline::advance_target`).
///
/// Returns instantly with a zeroed outcome, and writes nothing at all, when
/// `config.enabled` is false — a disabled repo must never grow even a
/// telemetry trail. Every other path always writes exactly one telemetry
/// event, including an `attempted: 0` pass, so the record of "this pass ran
/// and found nothing" is as durable as one that resolved something.
pub(crate) fn run_retirement_pass(
    space: &rk_space::Space,
    tickets: &crate::tickets::Tickets,
    repo: &str,
    git_repo: &rk_git::Repo,
    instance: &str,
    config: &RetirementConfig,
) -> rk_core::Result<RetirementOutcome> {
    let mut outcome = RetirementOutcome::default();
    if !config.enabled {
        return Ok(outcome);
    }
    let needs = space.scan(&Pattern::category(Category::Need).scope(repo))?;
    // No `landing_processed` history scan: this feature only ever acts on a
    // Need carrying a structured `landing_incident` (checked below), so
    // `resolution_candidates`'s legacy-compat inference over process history
    // — built for needs that predate that payload — can never produce a
    // candidate this pass would use. `history_truncated: true` skips that
    // inference outright, bounding this pass to one Need scan per repo per
    // trigger instead of an unbounded events scan.
    let candidates = crate::current_needs::resolution_candidates(&needs, &[], tickets, true)?;
    outcome.attempted = candidates.len();
    if candidates.is_empty() {
        space
            .out(telemetry_event(repo, instance, config, &outcome, &[]))
            .ok();
        return Ok(outcome);
    }
    // `resolution_candidates` already proved repo/task/target and delivery
    // timing; it deliberately stops short of proving the held branch/head
    // itself landed (it accepts a replacement source branch — see its doc
    // comment and BBS finding 01M2HHCBRETF1EBWYHDQYJ8PEY). Add the two proofs
    // this feature's contract requires on top: the incident's own `branch`
    // must equal the delivery's recorded branch exactly (never inferred from
    // a differently-named "equivalent" branch), and its `head_sha` must be an
    // ancestor of (or equal to) the delivered merge commit.
    let mut incidents: HashMap<RecordId, LandingIncident> = HashMap::new();
    for candidate in &candidates {
        let Some(need) = needs.iter().find(|n| n.id == candidate.need_id) else {
            outcome.skipped += 1;
            continue;
        };
        let incident = need
            .payload
            .get("landing_incident")
            .and_then(|v| serde_json::from_value::<LandingIncident>(v.clone()).ok());
        let Some(incident) = incident else {
            // No readable head_sha to prove against: unknown evidence, never
            // resolved on the target/timing match alone.
            outcome.skipped += 1;
            continue;
        };
        if incident.branch != candidate.branch {
            // A later, differently-named recovery/replacement branch is not
            // automatically the same source-branch binding, even when its
            // ticket/target/timing otherwise match.
            outcome.skipped += 1;
            continue;
        }
        incidents.insert(candidate.need_id, incident);
    }
    let mut bindings = Vec::new();
    for candidate in &candidates {
        let Some(incident) = incidents.get(&candidate.need_id) else {
            continue; // already counted as skipped above
        };
        let delivered = git_repo.ancestry(&candidate.merge_commit, &candidate.target)
            == rk_git::Ancestry::Present;
        let head_included = git_repo.ancestry(&incident.head_sha, &candidate.merge_commit)
            == rk_git::Ancestry::Present;
        if !(delivered && head_included) {
            outcome.skipped += 1;
            continue;
        }
        let Some(need) = needs.iter().find(|n| n.id == candidate.need_id) else {
            outcome.skipped += 1;
            continue;
        };
        // Trail first: it is the durable proof of *why* this Need is about to
        // be consumed. Only once it is safely written do we consume the Need
        // itself, so an interruption between the two never destroys
        // provenance for a Need that still exists to be retried.
        let trail = resolution_trail(need, candidate, incident, instance);
        if let Err(error) = space.reinforce(trail) {
            tracing::warn!(%error, need = %need.id, "landing-need-retirement: resolution trail write failed; leaving the Need standing for a later retry");
            outcome.failed += 1;
            continue;
        }
        match space.delete(need.id) {
            // Only a call that actually performed the delete counts as a
            // fresh resolution. `Ok(false)` means the Need was already gone —
            // a concurrent pass, or a prior crashed pass that reached the
            // trail write but not the delete, already retired it — so this
            // call did no new work; counting it as `resolved` again would let
            // a race inflate the count past the number of Needs genuinely
            // consumed. It is not evidence of anything wrong either, so it
            // folds into `skipped` (already-settled) rather than `failed`.
            Ok(true) => {
                outcome.resolved += 1;
                bindings.push(serde_json::json!({
                    "need_id": need.id.to_string(),
                    "task": need.payload.get("task"),
                    "branch": incident.branch,
                    "target": candidate.target,
                    "merge_commit": candidate.merge_commit,
                    "head_sha": incident.head_sha,
                }));
            }
            Ok(false) => outcome.skipped += 1,
            Err(error) => {
                // Provenance already durable; the Need stands until a later
                // pass retries the delete.
                tracing::warn!(%error, need = %need.id, "landing-need-retirement: delete failed; leaving the Need standing (trail already recorded)");
                outcome.failed += 1;
            }
        }
    }
    if let Err(error) = space.out(telemetry_event(repo, instance, config, &outcome, &bindings)) {
        tracing::warn!(%error, repo, "landing-need-retirement: telemetry write failed; work is unaffected");
    }
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn default_resolution_is_disabled_with_zero_revision() {
        let dir = tempfile::tempdir().unwrap();
        let registry = RetirementRegistry::load(&dir.path().join("x.json")).unwrap();
        let resolved = registry.resolve("rat-kingdom");
        assert!(!resolved.enabled);
        assert_eq!(resolved.revision, 0);
        assert_eq!(resolved.status, ConfigStatus::DefaultAbsent);
        assert!(registry.record("rat-kingdom").is_none());
    }

    #[test]
    fn a_missing_file_is_absent_but_any_other_read_failure_is_not() {
        let dir = tempfile::tempdir().unwrap();
        let registry = RetirementRegistry::load(&dir.path().join("x.json")).unwrap();
        assert_eq!(registry.resolve("r").status, ConfigStatus::DefaultAbsent);
        let as_dir = dir.path().join("dir.json");
        std::fs::create_dir(&as_dir).unwrap();
        assert!(RetirementRegistry::load(&as_dir).is_err());
    }

    #[test]
    fn enable_then_disable_round_trips_through_persistence_and_increments_revision() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.json");
        {
            let mut registry = RetirementRegistry::load(&path).unwrap();
            let record = registry
                .set("rat-kingdom", FeatureMode::On, "operator")
                .unwrap();
            assert_eq!(record.revision, 1);
        }
        let registry = RetirementRegistry::load(&path).unwrap();
        let resolved = registry.resolve("rat-kingdom");
        assert!(resolved.enabled);
        assert_eq!(resolved.revision, 1);
        assert_eq!(registry.resolve("other-repo"), RetirementConfig::default());

        let mut registry = RetirementRegistry::load(&path).unwrap();
        let record = registry
            .set("rat-kingdom", FeatureMode::Off, "operator")
            .unwrap();
        assert_eq!(record.revision, 2);
        let resolved = registry.resolve("rat-kingdom");
        assert!(!resolved.enabled);
        // Disable returns to disabled BEHAVIOR, but the revision honestly
        // reflects the real record, not a fabricated "never configured" 0.
        assert_eq!(resolved.revision, 2);
    }

    #[test]
    fn unknown_mode_is_rejected() {
        for bad in ["shadow", "bogus", ""] {
            let error = FeatureMode::parse(bad).unwrap_err();
            assert!(error.contains(bad) || bad.is_empty(), "{error}");
        }
        assert!(FeatureMode::parse("off").is_ok());
        assert!(FeatureMode::parse("on").is_ok());
    }

    #[test]
    fn malformed_registry_file_fails_to_load_explicitly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.json");
        std::fs::write(&path, "not json").unwrap();
        assert!(RetirementRegistry::load(&path).is_err());
    }

    #[test]
    fn resolution_trail_carries_exact_identity_for_audit() {
        let need = Tuple::new(
            Category::Need,
            "rat-kingdom",
            "landing",
            "daemon",
            serde_json::json!({"task": "TKT-x"}),
        );
        let candidate = ResolutionCandidate {
            need_id: need.id,
            repo: "rat-kingdom".into(),
            merge_commit: "deadbeef".into(),
            target: "main".into(),
            branch: "rat/x/tkt-x".into(),
        };
        let incident = LandingIncident {
            branch: "rat/x/tkt-x".into(),
            target: "main".into(),
            head_sha: "cafef00d".into(),
            source_spawn: None,
        };
        let trail = resolution_trail(&need, &candidate, &incident, "castle-1");
        assert_eq!(trail.category, Category::Resolution);
        assert_eq!(trail.scope, "rat-kingdom");
        assert_eq!(trail.payload["head_sha"], "cafef00d");
        assert_eq!(trail.payload["merge_commit"], "deadbeef");
        assert_eq!(trail.payload["branch"], "rat/x/tkt-x");
        assert_eq!(trail.payload["need_id"], need.id.to_string());
    }

    #[test]
    fn telemetry_event_is_never_scanned_by_bbs_brief_categories() {
        let config = RetirementConfig {
            enabled: true,
            revision: 3,
            status: ConfigStatus::Explicit,
        };
        let outcome = RetirementOutcome {
            attempted: 2,
            resolved: 1,
            skipped: 1,
            failed: 0,
        };
        let bindings = vec![serde_json::json!({"need_id": "01ABC", "branch": "rat/x/tkt-x"})];
        let event = telemetry_event("rat-kingdom", "castle-1", &config, &outcome, &bindings);
        assert_eq!(event.category, Category::Event);
        assert_eq!(event.payload["resolved"], 1);
        assert_eq!(event.payload["skipped"], 1);
        assert_eq!(event.payload["config_revision"], 3);
        assert!(!event.payload["build"]
            .as_str()
            .unwrap_or_default()
            .is_empty());
        assert_eq!(
            event.payload["resolved_bindings"][0]["branch"],
            "rat/x/tkt-x"
        );
        assert_eq!(event.payload["resolved_bindings_truncated"], false);
    }

    #[test]
    fn telemetry_bindings_are_capped_and_flagged_truncated() {
        let config = RetirementConfig::default();
        let outcome = RetirementOutcome::default();
        let bindings: Vec<_> = (0..(MAX_TELEMETRY_BINDINGS + 5))
            .map(|i| serde_json::json!({"need_id": i.to_string()}))
            .collect();
        let event = telemetry_event("rat-kingdom", "castle-1", &config, &outcome, &bindings);
        assert_eq!(
            event.payload["resolved_bindings"].as_array().unwrap().len(),
            MAX_TELEMETRY_BINDINGS
        );
        assert_eq!(event.payload["resolved_bindings_truncated"], true);
    }

    fn git(dir: &Path, args: &[&str]) {
        let status = std::process::Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?}");
    }

    /// A one-commit repo where the delivered commit is also the incident's
    /// own head — the simplest real ancestry fixture a passing case needs
    /// (`git merge-base --is-ancestor <c> <c>` is trivially true, as is
    /// `<c>` being an ancestor of the `main` branch that IS `<c>`).
    fn one_commit_repo() -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        git(dir.path(), &["init", "-b", "main"]);
        git(dir.path(), &["config", "user.email", "r@x"]);
        git(dir.path(), &["config", "user.name", "R"]);
        std::fs::write(dir.path().join("f"), "x").unwrap();
        git(dir.path(), &["add", "."]);
        git(dir.path(), &["commit", "-m", "c"]);
        let sha = String::from_utf8(
            std::process::Command::new("git")
                .arg("-C")
                .arg(dir.path())
                .args(["rev-parse", "HEAD"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_string();
        (dir, sha)
    }

    async fn create_ticket(space: &rk_space::Space, repo: &str) -> String {
        let tickets = crate::tickets::Tickets::new(space.clone(), "test-castle".into());
        let ticket = tickets
            .create(
                serde_json::from_value(serde_json::json!({"title": "x", "scope": repo})).unwrap(),
            )
            .await
            .unwrap();
        ticket.identity.clone()
    }

    /// Records delivery AFTER the caller's Need already exists — `resolution_
    /// candidates` requires `need.created_at < landed_at`; recording delivery
    /// before the incident it is meant to fix would make every candidate this
    /// module cares about skip on the timing check alone.
    async fn deliver(space: &rk_space::Space, task: &str, branch: &str, merge_commit: &str) {
        let tickets = crate::tickets::Tickets::new(space.clone(), "test-castle".into());
        tickets
            .record_delivery(
                task,
                &crate::tickets::DeliveryRecord {
                    merge_commit: merge_commit.to_string(),
                    branch: branch.to_string(),
                    target: "main".to_string(),
                    landed_at: Utc::now().to_rfc3339(),
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn run_retirement_pass_retires_a_matching_need_and_leaves_a_mismatch() {
        let (repo_dir, head) = one_commit_repo();
        let git_repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let space = rk_space::Space::open_in_memory().unwrap();
        let repo = "fixture-repo";
        let branch = "rat/x/tkt-fix";
        let task = create_ticket(&space, repo).await;
        let tickets = crate::tickets::Tickets::new(space.clone(), "test-castle".into());

        let matching = Tuple::new(
            Category::Need,
            repo,
            "landing",
            "daemon",
            serde_json::json!({
                "agent": "landing", "task": task,
                "landing_incident": {"branch": branch, "target": "main", "head_sha": head, "source_spawn": null},
            }),
        );
        let matching_id = matching.id;
        space.out(matching).unwrap();
        let mismatch = Tuple::new(
            Category::Need,
            repo,
            "steward",
            "daemon",
            serde_json::json!({
                "agent": "landing", "task": task,
                "landing_incident": {"branch": "some-other-branch", "target": "main", "head_sha": head, "source_spawn": null},
            }),
        );
        let mismatch_id = mismatch.id;
        space.out(mismatch).unwrap();
        // `resolution_candidates` requires `need.created_at < landed_at`;
        // guarantee a strictly later wall-clock reading regardless of timer
        // resolution.
        std::thread::sleep(std::time::Duration::from_millis(5));
        deliver(&space, &task, branch, &head).await;

        let config = RetirementConfig {
            enabled: true,
            revision: 1,
            status: ConfigStatus::Explicit,
        };
        let outcome =
            run_retirement_pass(&space, &tickets, repo, &git_repo, "test", &config).unwrap();
        assert_eq!(outcome.attempted, 2);
        assert_eq!(outcome.resolved, 1);
        assert_eq!(outcome.skipped, 1);
        assert_eq!(outcome.failed, 0);

        let remaining = space
            .scan(&Pattern::category(Category::Need).scope(repo))
            .unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].id, mismatch_id);

        let trails = space
            .scan(&Pattern::category(Category::Resolution).scope(repo))
            .unwrap();
        assert_eq!(trails.len(), 1);
        assert_eq!(trails[0].payload["need_id"], matching_id.to_string());

        // A second pass: the matching Need is already gone, so it is never
        // even proposed as a candidate again, and nothing new is written.
        let outcome2 =
            run_retirement_pass(&space, &tickets, repo, &git_repo, "test", &config).unwrap();
        assert_eq!(
            outcome2.attempted, 1,
            "only the mismatch is still a candidate"
        );
        assert_eq!(outcome2.resolved, 0);
        let trails_again = space
            .scan(&Pattern::category(Category::Resolution).scope(repo))
            .unwrap();
        assert_eq!(trails_again.len(), 1, "reinforce must not duplicate");
    }

    /// A real, genuinely concurrent exercise of `space.delete`'s `Ok(false)`
    /// branch (BBS finding: "space.delete returning Ok(false) still
    /// increments resolved"). Two OS threads run the FULL pass — their own
    /// scan, their own ancestry proof, their own trail write — on the same
    /// still-present Need, released together by a barrier so both complete
    /// their scan before either can have reached delete; that is a real
    /// window, not a contrived one, because both a trail write and TWO real
    /// `git merge-base` subprocess calls sit between a thread's own scan and
    /// its own delete. Exactly one of the two may count a fresh resolution;
    /// the other's `Ok(false)` must fold into `skipped`, never `resolved`.
    #[tokio::test]
    async fn concurrent_settlement_never_double_counts_a_resolved_need() {
        let (repo_dir, head) = one_commit_repo();
        let space = rk_space::Space::open_in_memory().unwrap();
        let repo = "fixture-repo";
        let branch = "rat/x/tkt-fix";
        let task = create_ticket(&space, repo).await;
        let tickets = Arc::new(crate::tickets::Tickets::new(
            space.clone(),
            "test-castle".into(),
        ));

        space
            .out(Tuple::new(
                Category::Need,
                repo,
                "landing",
                "daemon",
                serde_json::json!({
                    "agent": "landing", "task": task,
                    "landing_incident": {"branch": branch, "target": "main", "head_sha": head, "source_spawn": null},
                }),
            ))
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        deliver(&space, &task, branch, &head).await;

        let config = RetirementConfig {
            enabled: true,
            revision: 1,
            status: ConfigStatus::Explicit,
        };
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let space = space.clone();
                let tickets = Arc::clone(&tickets);
                let barrier = Arc::clone(&barrier);
                let repo_path = repo_dir.path().to_path_buf();
                std::thread::spawn(move || {
                    let git_repo = rk_git::Repo::discover(&repo_path).unwrap();
                    barrier.wait();
                    run_retirement_pass(&space, &tickets, repo, &git_repo, "test", &config).unwrap()
                })
            })
            .collect();
        let outcomes: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        let total_resolved: usize = outcomes.iter().map(|o| o.resolved).sum();
        assert_eq!(
            total_resolved, 1,
            "exactly one concurrent pass may count a fresh resolution: {outcomes:?}"
        );
        let remaining = space
            .scan(&Pattern::category(Category::Need).scope(repo))
            .unwrap();
        assert!(remaining.is_empty(), "{remaining:?}");
        let trails = space
            .scan(&Pattern::category(Category::Resolution).scope(repo))
            .unwrap();
        assert_eq!(trails.len(), 1, "reinforce must not duplicate: {trails:?}");
    }
    /// The exact interruption state the trail-before-delete ordering in
    /// [`run_retirement_pass`] exists to survive, constructed DIRECTLY rather
    /// than inferred from two full concurrent passes: a `Resolution` trail
    /// already durably written while its matching `Need` is STILL PRESENT —
    /// i.e. a crash landing precisely between the two writes.
    ///
    /// This is the one property the module doc comment claims outright
    /// ("an interruption between the two leaves the Need standing, so the
    /// next pass regenerates the same candidate, proves the same ancestry
    /// again, and reinforces (never duplicates) the same trail before
    /// retrying the delete") that no other test isolates:
    /// `concurrent_settlement_never_double_counts_a_resolved_need` only
    /// proves two COMPLETE passes do not double-count each other, and never
    /// exercises trail-written-but-Need-still-standing on its own.
    ///
    /// The pre-written trail is the real one — produced by `resolution_trail`
    /// from the same candidate `resolution_candidates` hands the pass, under
    /// the same castle instance — so it collides on the exact
    /// `(category, scope, identity, instance)` key `Space::reinforce` dedups
    /// on. A trail merely shaped like it would prove nothing.
    ///
    /// The boundary is PERSISTED, not just in-memory: the store is a real
    /// on-disk `Space`, and the interrupted `Space` handle is dropped and
    /// reopened from that same file before the recovery pass runs. So the
    /// recovery pass reads the half-finished state back off disk — exactly
    /// what a later daemon generation does — rather than inheriting it from
    /// the same live handle that wrote it.
    #[tokio::test]
    async fn a_persisted_trail_written_without_its_delete_is_finished_by_the_next_pass() {
        let (repo_dir, head) = one_commit_repo();
        let git_repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let store_dir = tempfile::tempdir().unwrap();
        let store_path = store_dir.path().join("space.db");
        let space = rk_space::Space::open(&store_path).unwrap();
        let repo = "fixture-repo";
        let branch = "rat/x/tkt-fix";
        let task = create_ticket(&space, repo).await;
        let tickets = crate::tickets::Tickets::new(space.clone(), "test-castle".into());

        let need = Tuple::new(
            Category::Need,
            repo,
            "landing",
            "daemon",
            serde_json::json!({
                "agent": "landing", "task": task,
                "landing_incident": {"branch": branch, "target": "main", "head_sha": head, "source_spawn": null},
            }),
        );
        let need_id = need.id;
        space.out(need).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        deliver(&space, &task, branch, &head).await;

        // Replay only the FIRST half of a pass, then "crash": write the trail
        // the pass would have written, and stop before the delete.
        let needs = space
            .scan(&Pattern::category(Category::Need).scope(repo))
            .unwrap();
        let candidates =
            crate::current_needs::resolution_candidates(&needs, &[], &tickets, true).unwrap();
        assert_eq!(candidates.len(), 1);
        let incident: LandingIncident =
            serde_json::from_value(needs[0].payload.get("landing_incident").unwrap().clone())
                .unwrap();
        let interrupted_trail_id = space
            .reinforce(resolution_trail(
                &needs[0],
                &candidates[0],
                &incident,
                "test",
            ))
            .unwrap()
            .id;

        // The "crash": drop every handle on the interrupted store and reopen
        // it from the same file, so nothing below is inherited from the
        // generation that wrote the trail.
        drop(tickets);
        drop(needs);
        drop(space);
        let space = rk_space::Space::open(&store_path).unwrap();
        let tickets = crate::tickets::Tickets::new(space.clone(), "test-castle".into());

        // Precondition — the interruption state itself, read back off disk and
        // asserted so this test cannot silently degrade into the
        // already-settled case: provenance is durable, and the Need it
        // describes has NOT been consumed.
        let standing = space
            .scan(&Pattern::category(Category::Need).scope(repo))
            .unwrap();
        assert_eq!(
            standing.len(),
            1,
            "the persisted crash state must be trail-written-but-Need-still-standing: {standing:?}"
        );
        assert_eq!(standing[0].id, need_id);
        let trails_before = space
            .scan(&Pattern::category(Category::Resolution).scope(repo))
            .unwrap();
        assert_eq!(trails_before.len(), 1, "{trails_before:?}");

        let config = RetirementConfig {
            enabled: true,
            revision: 1,
            status: ConfigStatus::Explicit,
        };
        let outcome =
            run_retirement_pass(&space, &tickets, repo, &git_repo, "test", &config).unwrap();

        // The recovery pass completes the transition the crash left half-done:
        // it re-derives the same candidate, re-proves ancestry, reinforces the
        // SAME trail, and performs the delete this time — so the delete really
        // did happen here and counts as one fresh resolution.
        assert_eq!(outcome.attempted, 1, "{outcome:?}");
        assert_eq!(
            outcome.resolved, 1,
            "the recovery pass must complete the delete the crash never reached: {outcome:?}"
        );
        assert_eq!(outcome.skipped, 0, "{outcome:?}");
        assert_eq!(outcome.failed, 0, "{outcome:?}");

        let remaining = space
            .scan(&Pattern::category(Category::Need).scope(repo))
            .unwrap();
        assert!(remaining.is_empty(), "{remaining:?}");

        // Reinforce, not append: one trail, and the SAME record — the crash's
        // provenance was refreshed in place, never destroyed and never
        // duplicated into a second audit row for one retirement.
        let trails_after = space
            .scan(&Pattern::category(Category::Resolution).scope(repo))
            .unwrap();
        assert_eq!(
            trails_after.len(),
            1,
            "reinforce must not duplicate the pre-existing trail: {trails_after:?}"
        );
        assert_eq!(
            trails_after[0].id, interrupted_trail_id,
            "the recovery pass must reinforce the crashed pass's own trail record"
        );
        assert_eq!(trails_after[0].payload["need_id"], need_id.to_string());
        assert_eq!(trails_after[0].payload["head_sha"], head);
    }
}

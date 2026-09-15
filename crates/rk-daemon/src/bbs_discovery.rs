//! Per-repository setting for the BBS discovery ranking feature: the P8/P11
//! first slice (design doc `docs/2026-09-13-continuous-validation-
//! promotion.md` sections 7.1 and 11 — "First feature: alternative BBS
//! discovery ranking, with current ranking retained as baseline").
//!
//! JSON-file-backed and mutated only through validated RPC (`bbs.discovery.
//! set`, operator-only), mirroring [`crate::repos::RepoRegistry`]. Applied
//! fresh on every read: there is no in-memory daemon cache, so a change is
//! observed by the very next `bbs.brief` or spawn/resume/recovery briefing
//! for that repo. No daemon restart or rollover is required.
//!
//! Only `off` (the default and the permanent fallback) and `on` are
//! implemented in this slice. `shadow` and `cohort` are named in the design
//! doc as later work and are rejected explicitly, never silently treated as
//! `off`.

use chrono::{DateTime, Utc};
use rk_core::bbs::{ConfigStatus, RankingVariant};
use rk_core::paths::Layout;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// The only feature this slice ships. A second feature reuses the same
/// mechanics under a new file/module, not a generic multi-feature dispatch —
/// building that generalized surface is explicitly later P8/P10 scope.
pub const FEATURE_ID: &str = "bbs-discovery-ranking";

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

    /// Parse an operator-supplied mode string. Only `off`/`on` are
    /// implemented; `shadow`/`cohort` are named in the design doc as later
    /// slices and must be refused with a distinct message, not silently
    /// folded into `off` or accepted as a no-op.
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "off" => Ok(Self::Off),
            "on" => Ok(Self::On),
            "shadow" | "cohort" => Err(format!(
                "mode '{raw}' is not implemented yet for {FEATURE_ID}; only 'off' and 'on' are supported in this slice"
            )),
            other => Err(format!(
                "unknown mode '{other}' for {FEATURE_ID}; supported: off, on"
            )),
        }
    }

    fn variant(self) -> RankingVariant {
        match self {
            Self::Off => RankingVariant::Baseline,
            Self::On => RankingVariant::ObservedGenericWordFilter,
        }
    }
}

/// The resolved setting for one repo: what `bbs::brief` should actually
/// apply, bundled with the config identity recorded on the exposure
/// envelope. Never fails to resolve — a briefing must always be computable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiscoveryConfig {
    pub variant: RankingVariant,
    /// `0` when no explicit per-repo record exists (implicit baseline
    /// default). Otherwise the exact revision of the record that produced
    /// `variant`, even when that record set the repo back to `off`: the
    /// point is provenance of the actual decision, not just its behavioral
    /// equivalence to no record at all.
    pub revision: u64,
    /// Whether this is a confirmed repo setting or a fallback applied
    /// because the registry itself could not be read. See
    /// [`rk_core::bbs::ConfigStatus`] — the two "baseline" cases below are
    /// NOT interchangeable evidence.
    pub status: ConfigStatus,
}

impl DiscoveryConfig {
    /// The registry was read successfully and genuinely has no record for
    /// this repo — a confirmed absence.
    fn default_absent() -> Self {
        Self {
            variant: RankingVariant::Baseline,
            revision: 0,
            status: ConfigStatus::DefaultAbsent,
        }
    }

    /// The registry could not be read at all. Baseline is applied as a safe
    /// fallback, but whether this repo has an explicit setting is UNKNOWN —
    /// never reported as though it were a confirmed absence.
    fn unreadable_fallback() -> Self {
        Self {
            variant: RankingVariant::Baseline,
            revision: 0,
            status: ConfigStatus::UnreadableFallback,
        }
    }
}

impl Default for DiscoveryConfig {
    /// For test scaffolding that constructs a config directly rather than
    /// resolving one: equivalent to a genuinely unconfigured repo.
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
/// restart-memory contract as [`crate::repos::RepoRegistry`].
pub struct DiscoveryRegistry {
    path: PathBuf,
    repos: HashMap<String, FeatureRecord>,
}

impl DiscoveryRegistry {
    /// Reads the file directly rather than checking existence first: a
    /// separate `Path::exists` probe is racy (the file can vanish between
    /// the check and the read) and, on a permission error, `exists` itself
    /// returns `false` — silently relabeling a real read failure as "no
    /// config was ever written". Only an actual `NotFound` means absent;
    /// every other I/O or parse error propagates.
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

    /// The resolved config for `repo`. A missing repo (registry read fine,
    /// no record for it) is a confirmed [`ConfigStatus::DefaultAbsent`],
    /// never an error — a briefing must always be computable.
    pub fn resolve(&self, repo: &str) -> DiscoveryConfig {
        match self.repos.get(repo) {
            Some(record) => DiscoveryConfig {
                variant: record.mode.variant(),
                revision: record.revision,
                status: ConfigStatus::Explicit,
            },
            None => DiscoveryConfig::default_absent(),
        }
    }

    pub fn record(&self, repo: &str) -> Option<&FeatureRecord> {
        self.repos.get(repo)
    }

    /// Validated write: `mode` must already be `Ok` from [`FeatureMode::parse`].
    /// Repo registration/scope is the caller's responsibility — this module
    /// does not own the repo registry.
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
    layout.home().join("bbs-discovery.json")
}

/// Resolve this repo's discovery config for a briefing, degrading to the
/// baseline (never failing the briefing) on a load error. Mirrors the
/// existing "a failed capture never fails the read" discipline already
/// applied to exposure/telemetry capture in `crate::bbs`.
pub fn resolve_for_brief(layout: &Layout, repo: &str) -> DiscoveryConfig {
    match DiscoveryRegistry::load(&registry_path(layout)) {
        Ok(registry) => registry.resolve(repo),
        Err(error) => {
            tracing::warn!(
                %error,
                repo,
                "BBS discovery config unreadable; falling back to baseline ranking"
            );
            DiscoveryConfig::unreadable_fallback()
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
    let registry = DiscoveryRegistry::load(&registry_path(layout))?;
    Ok(describe(&params.repo, registry.record(&params.repo)))
}

/// `repos` proves `params.repo` is a registered repository before this
/// mutates anything — the "malformed or unauthorized scope" rejection.
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
    let mut registry = DiscoveryRegistry::load(&registry_path(layout))?;
    let record = registry.set(&params.repo, mode, caller)?;
    let mut value = describe(&params.repo, Some(&record));
    // No release inventory, cohort assignment, or daemon-wide state changes:
    // the setting is read fresh from disk on the very next briefing, so no
    // rollover is ever required for this slice.
    value["rollover_required"] = serde_json::json!(false);
    value["rollover_note"] = serde_json::json!(
        "applied on the next bbs.brief or spawn/resume/recovery briefing for this repo; no daemon restart is required"
    );
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_resolution_is_baseline_with_zero_revision() {
        let dir = tempfile::tempdir().unwrap();
        let registry = DiscoveryRegistry::load(&dir.path().join("bbs-discovery.json")).unwrap();
        let resolved = registry.resolve("rat-kingdom");
        assert_eq!(resolved.variant, RankingVariant::Baseline);
        assert_eq!(resolved.revision, 0);
        assert_eq!(resolved.status, ConfigStatus::DefaultAbsent);
        assert!(registry.record("rat-kingdom").is_none());
    }

    #[test]
    fn a_missing_file_is_absent_but_any_other_read_failure_is_not() {
        let dir = tempfile::tempdir().unwrap();
        // No file at all: genuinely absent, loads fine.
        let registry = DiscoveryRegistry::load(&dir.path().join("bbs-discovery.json")).unwrap();
        assert_eq!(registry.resolve("r").status, ConfigStatus::DefaultAbsent);
        // A directory where a file was expected is a real I/O error (not
        // `NotFound`) and must propagate, not be silently read as "absent".
        let as_dir = dir.path().join("bbs-discovery-dir.json");
        std::fs::create_dir(&as_dir).unwrap();
        assert!(DiscoveryRegistry::load(&as_dir).is_err());
    }

    #[test]
    fn enable_then_disable_round_trips_through_persistence_and_increments_revision() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bbs-discovery.json");
        {
            let mut registry = DiscoveryRegistry::load(&path).unwrap();
            let record = registry
                .set("rat-kingdom", FeatureMode::On, "operator")
                .unwrap();
            assert_eq!(record.revision, 1);
        }
        // Reload from disk, simulating a fresh daemon process: no in-memory
        // cache survives, only the file does.
        let registry = DiscoveryRegistry::load(&path).unwrap();
        let resolved = registry.resolve("rat-kingdom");
        assert_eq!(resolved.variant, RankingVariant::ObservedGenericWordFilter);
        assert_eq!(resolved.revision, 1);
        // A second, unrelated repo is entirely unaffected.
        assert_eq!(registry.resolve("other-repo"), DiscoveryConfig::default());

        let mut registry = DiscoveryRegistry::load(&path).unwrap();
        let record = registry
            .set("rat-kingdom", FeatureMode::Off, "operator")
            .unwrap();
        assert_eq!(record.revision, 2);
        let resolved = registry.resolve("rat-kingdom");
        // Disable returns to baseline BEHAVIOR, but the revision honestly
        // reflects the real record, not a fabricated "never configured" 0.
        assert_eq!(resolved.variant, RankingVariant::Baseline);
        assert_eq!(resolved.revision, 2);
    }

    #[test]
    fn shadow_and_cohort_and_unknown_modes_are_rejected() {
        for bad in ["shadow", "cohort", "bogus", ""] {
            let error = FeatureMode::parse(bad).unwrap_err();
            assert!(error.contains(bad) || bad.is_empty(), "{error}");
        }
        assert!(FeatureMode::parse("off").is_ok());
        assert!(FeatureMode::parse("on").is_ok());
    }

    #[test]
    fn malformed_registry_file_fails_to_load_explicitly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bbs-discovery.json");
        std::fs::write(&path, "not json").unwrap();
        assert!(DiscoveryRegistry::load(&path).is_err());
    }
}

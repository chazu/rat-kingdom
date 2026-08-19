//! Install/drift tooling for `#Trigger` definitions, mirroring
//! `workflow_cmds.rs`'s source-of-truth checks for `#Workflow` definitions.
//!
//! Global triggers live as many `<name>.cue` files under
//! `~/.rat-kingdom/triggers/` (mirrors the workflows dir exactly). Repo-local
//! triggers are the odd one out: the reactor only ever reads the single fixed
//! path `<repo>/.rk/triggers.cue` (`Reactor::trigger_files`), never a
//! directory of many — so `install`/`drift` target that one filename
//! regardless of the source file's own name.
//!
//! This exists to close a named gap in the daemon-native landing pipeline
//! cutover (docs/proposals/daemon-native-landing-pipeline.md §6.5): swapping
//! `steward-on-completion` for `steward-landing-on-completion` was a manual
//! file copy with no tooling to confirm the old trigger definition was
//! actually removed from a deployment afterward. A stale copy left behind
//! keeps matching the identical `harness_result` predicate the replacement
//! uses, double-dispatching every completion. This module only reports
//! drift — it never deletes a deployed file itself; removal is an operator
//! decision same as `rk workflow drift`.
use anyhow::{Context, Result};
use rk_core::paths::Layout;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};

const REPO_LOCAL_FILENAME: &str = "triggers.cue";

#[derive(Debug, Serialize)]
pub struct DriftRow {
    pub target: String,
    pub source: String,
    pub status: String,
    pub deployed_digest: Option<String>,
    pub source_digest: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DriftReport {
    pub rows: Vec<DriftRow>,
    pub drifted: usize,
}

pub fn install(layout: &Layout, source: &str, repo: Option<&str>) -> Result<PathBuf> {
    let source = PathBuf::from(source)
        .canonicalize()
        .with_context(|| format!("cannot read trigger source {source}"))?;
    if source.extension().and_then(|ext| ext.to_str()) != Some("cue") {
        anyhow::bail!("trigger source must be a .cue file: {}", source.display());
    }
    let target = match repo {
        // Fixed filename: the reactor resolves exactly `.rk/triggers.cue`
        // (crates/rk-daemon/src/reactor.rs `trigger_files`), never scanning
        // the repo `.rk` directory for other names.
        Some(repo) => PathBuf::from(repo)
            .canonicalize()
            .with_context(|| format!("cannot read repository {repo}"))?
            .join(".rk")
            .join(REPO_LOCAL_FILENAME),
        None => {
            fs::create_dir_all(layout.triggers_dir()).with_context(|| {
                format!(
                    "create trigger directory {}",
                    layout.triggers_dir().display()
                )
            })?;
            layout.triggers_dir().join(
                source
                    .file_name()
                    .context("trigger source has no filename")?,
            )
        }
    };
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create trigger directory {}", parent.display()))?;
    }
    fs::copy(&source, &target)
        .with_context(|| format!("install {} to {}", source.display(), target.display()))?;
    Ok(target)
}

pub fn drift(layout: &Layout, repo: &str, source_dir: Option<&str>) -> Result<DriftReport> {
    let repo = PathBuf::from(repo)
        .canonicalize()
        .with_context(|| format!("cannot read repository {repo}"))?;
    // Trigger examples ship loose at the top of `examples/` (`triggers.cue`,
    // `triggers-landing-pipeline.cue`, ...), not in a `triggers/` subdirectory
    // — there is only ever one repo-local deployment target, so there is
    // nothing for a per-file subdirectory to organize.
    let source_dir = source_dir
        .map(PathBuf::from)
        .unwrap_or_else(|| repo.join("examples"))
        .canonicalize()
        .with_context(|| "cannot read trigger source directory")?;
    let sources = cue_files(&source_dir)?;
    let mut rows = Vec::new();

    for target in cue_files_if_present(&layout.triggers_dir())? {
        let name = target
            .file_name()
            .context("trigger target has no filename")?;
        rows.push(drift_row("global", &target, sources.get(name)));
    }

    let repo_local = repo.join(".rk").join(REPO_LOCAL_FILENAME);
    if repo_local.exists() {
        let source = sources.get(std::ffi::OsStr::new(REPO_LOCAL_FILENAME));
        rows.push(drift_row("repo-local", &repo_local, source));
    }

    // Global and repo-local are fallback layers, not two required copies of
    // every source: report one missing row only when a source ships neither
    // a global nor a repo-local deployment sharing its filename.
    for (name, source) in &sources {
        let deployed = layout.triggers_dir().join(name).exists()
            || (name.as_os_str() == REPO_LOCAL_FILENAME && repo_local.exists());
        if !deployed {
            let target = if name.as_os_str() == REPO_LOCAL_FILENAME {
                repo_local.clone()
            } else {
                layout.triggers_dir().join(name)
            };
            let label = if name.as_os_str() == REPO_LOCAL_FILENAME {
                "repo-local"
            } else {
                "global"
            };
            rows.push(DriftRow {
                target: format!("{label}:{}", target.display()),
                source: source.display().to_string(),
                status: "missing".into(),
                deployed_digest: None,
                source_digest: Some(digest(source)?),
            });
        }
    }

    rows.sort_by(|a, b| a.target.cmp(&b.target));
    let drifted = rows
        .iter()
        .filter(|row| row.status == "drifted" || row.status == "missing")
        .count();
    Ok(DriftReport { rows, drifted })
}

#[derive(Debug, Serialize)]
pub struct ConflictEntry {
    pub name: String,
    pub file: String,
}

#[derive(Debug, Serialize)]
pub struct ConflictGroup {
    #[serde(rename = "match")]
    pub matcher: rk_workflow::TriggerMatch,
    pub triggers: Vec<ConflictEntry>,
}

#[derive(Debug, Serialize)]
pub struct ConflictsReport {
    pub groups: Vec<ConflictGroup>,
}

/// Flag every group of >1 active trigger sharing an identical `match`
/// predicate. `drift` only tells you a deployed file matches its source; it
/// says nothing about whether a *sibling* deployed file's predicate collides
/// with it. Two triggers matching the same tuple both fire in full on every
/// match, double-dispatching — the hazard named by
/// docs/proposals/daemon-native-landing-pipeline.md §6.2 step 3 (e.g. the
/// retired `steward-on-completion` left deployed alongside its replacement
/// `steward-landing-on-completion`, both matching the identical
/// `harness_result` predicate).
///
/// Scans the same active set the reactor itself dispatches from
/// (`Reactor::trigger_files`): every global `~/.rat-kingdom/triggers/*.cue`
/// file plus this one repo's `.rk/triggers.cue`, if present.
pub fn conflicts(layout: &Layout, repo: &str) -> Result<ConflictsReport> {
    let repo = PathBuf::from(repo)
        .canonicalize()
        .with_context(|| format!("cannot read repository {repo}"))?;

    let mut files = rk_workflow::definitions(&layout.triggers_dir());
    let repo_local = repo.join(".rk").join(REPO_LOCAL_FILENAME);
    if repo_local.exists() {
        files.push(repo_local);
    }

    let mut groups: Vec<ConflictGroup> = Vec::new();
    for file in &files {
        let triggers = rk_workflow::load_triggers(file)
            .with_context(|| format!("parse {}", file.display()))?;
        for trigger in triggers {
            let entry = ConflictEntry {
                name: trigger.name,
                file: file.display().to_string(),
            };
            match groups
                .iter_mut()
                .find(|group| group.matcher == trigger.matcher)
            {
                Some(group) => group.triggers.push(entry),
                None => groups.push(ConflictGroup {
                    matcher: trigger.matcher,
                    triggers: vec![entry],
                }),
            }
        }
    }

    groups.retain(|group| group.triggers.len() > 1);
    for group in &mut groups {
        group.triggers.sort_by(|a, b| a.name.cmp(&b.name));
    }
    groups.sort_by(|a, b| a.triggers[0].name.cmp(&b.triggers[0].name));
    Ok(ConflictsReport { groups })
}

fn drift_row(label: &str, target: &Path, source: Option<&PathBuf>) -> DriftRow {
    let target_digest = match digest(target) {
        Ok(d) => d,
        Err(e) => {
            return DriftRow {
                target: format!("{label}:{}", target.display()),
                source: source
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "<missing>".into()),
                status: format!("unreadable: {e}"),
                deployed_digest: None,
                source_digest: None,
            }
        }
    };
    let source_digest = source.and_then(|path| digest(path).ok());
    let status = match source_digest.as_deref() {
        None => "unmanaged",
        Some(d) if d == target_digest => "clean",
        Some(_) => "drifted",
    };
    DriftRow {
        target: format!("{label}:{}", target.display()),
        source: source
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "<missing>".into()),
        status: status.into(),
        deployed_digest: Some(target_digest),
        source_digest,
    }
}

/// Trigger sources ship loose alongside `checks.cue`/`schedules.cue` in
/// `examples/`, not in a dedicated subdirectory — so a plain directory scan
/// would also compare every deployed trigger file against those unrelated
/// definitions and misreport a permanently "missing" trigger source for each.
/// A cheap top-level-field sniff (no `cue` shell-out) is enough to tell them
/// apart, since `#Trigger` files always declare a top-level `triggers:` list.
fn cue_files(dir: &Path) -> Result<std::collections::BTreeMap<std::ffi::OsString, PathBuf>> {
    Ok(cue_files_if_present(dir)?
        .into_iter()
        .filter(|path| looks_like_trigger_source(path).unwrap_or(false))
        .filter_map(|path| {
            let name = path.file_name()?.to_os_string();
            Some((name, path))
        })
        .collect())
}

fn looks_like_trigger_source(path: &Path) -> Result<bool> {
    let content = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    Ok(content
        .lines()
        .any(|line| line.trim_start().starts_with("triggers:")))
}

fn cue_files_if_present(dir: &Path) -> Result<Vec<PathBuf>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("cue") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn digest(path: &Path) -> Result<String> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_global_preserves_filename_and_reports_clean() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let source_dir = repo.path().join("examples");
        fs::create_dir_all(&source_dir).unwrap();
        let source = source_dir.join("triggers-landing-pipeline.cue");
        fs::write(&source, "triggers: []\n").unwrap();
        let layout = Layout::at(home.path());

        let installed = install(&layout, source.to_str().unwrap(), None).unwrap();
        assert_eq!(
            installed.file_name().unwrap(),
            "triggers-landing-pipeline.cue"
        );

        let clean = drift(&layout, repo.path().to_str().unwrap(), None).unwrap();
        assert_eq!(clean.drifted, 0);
        assert!(clean.rows.iter().any(|row| row.status == "clean"));
    }

    #[test]
    fn install_repo_local_always_targets_fixed_filename() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let source_dir = repo.path().join("examples");
        fs::create_dir_all(&source_dir).unwrap();
        // The repo-local landing-pipeline install: the source file is not
        // named `triggers.cue`, but the reactor only ever reads that exact
        // repo-local path.
        let source = source_dir.join("triggers-landing-pipeline.cue");
        fs::write(&source, "triggers: []\n").unwrap();
        let layout = Layout::at(home.path());

        let installed = install(
            &layout,
            source.to_str().unwrap(),
            Some(repo.path().to_str().unwrap()),
        )
        .unwrap();
        assert_eq!(installed.file_name().unwrap(), "triggers.cue");
        assert_eq!(installed.parent().unwrap().file_name().unwrap(), ".rk");
    }

    #[test]
    fn drift_flags_stale_copy_after_source_changes() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let source_dir = repo.path().join("examples");
        fs::create_dir_all(&source_dir).unwrap();
        let source = source_dir.join("triggers.cue");
        fs::write(&source, "triggers: [{name: \"steward-on-completion\"}]\n").unwrap();
        let layout = Layout::at(home.path());
        install(&layout, source.to_str().unwrap(), None).unwrap();

        let clean = drift(&layout, repo.path().to_str().unwrap(), None).unwrap();
        assert_eq!(clean.drifted, 0);

        // The CUE cutover removes/replaces the shipped trigger; the deployed
        // global copy is now stale and must be flagged, not silently ignored.
        fs::write(
            &source,
            "triggers: [{name: \"steward-landing-on-completion\", action: \"land\"}]\n",
        )
        .unwrap();
        let drifted = drift(&layout, repo.path().to_str().unwrap(), None).unwrap();
        assert_eq!(drifted.drifted, 1);
        assert!(drifted.rows.iter().any(|row| row.status == "drifted"));
    }

    #[test]
    fn drift_reports_missing_when_neither_layer_deploys_a_source() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let source_dir = repo.path().join("examples");
        fs::create_dir_all(&source_dir).unwrap();
        fs::write(source_dir.join("triggers.cue"), "triggers: []\n").unwrap();
        let layout = Layout::at(home.path());

        let report = drift(&layout, repo.path().to_str().unwrap(), None).unwrap();
        assert_eq!(report.drifted, 1);
        assert!(report.rows.iter().any(|row| row.status == "missing"));
    }

    #[test]
    fn conflicts_flags_global_and_repo_local_triggers_sharing_a_predicate() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        fs::create_dir_all(layout.triggers_dir()).unwrap();
        // The exact double-dispatch hazard this closes: a retired trigger
        // left deployed globally alongside its repo-local replacement,
        // both matching the identical harness_result predicate.
        fs::write(
            layout.triggers_dir().join("steward-on-completion.cue"),
            "triggers: [{name: \"steward-on-completion\", match: {category: \"event\", identity: \"harness_result\"}, run: \"steward\"}]\n",
        )
        .unwrap();
        fs::create_dir_all(repo.path().join(".rk")).unwrap();
        fs::write(
            repo.path().join(".rk").join("triggers.cue"),
            "triggers: [{name: \"steward-landing-on-completion\", match: {category: \"event\", identity: \"harness_result\"}, action: \"land\"}]\n",
        )
        .unwrap();

        let report = conflicts(&layout, repo.path().to_str().unwrap()).unwrap();
        assert_eq!(report.groups.len(), 1);
        let names: Vec<&str> = report.groups[0]
            .triggers
            .iter()
            .map(|t| t.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec!["steward-landing-on-completion", "steward-on-completion"]
        );
    }

    #[test]
    fn conflicts_is_clean_when_predicates_differ() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        fs::create_dir_all(layout.triggers_dir()).unwrap();
        fs::write(
            layout.triggers_dir().join("a.cue"),
            "triggers: [{name: \"a\", match: {category: \"event\", identity: \"one\"}, run: \"a-wf\"}]\n",
        )
        .unwrap();
        fs::create_dir_all(repo.path().join(".rk")).unwrap();
        fs::write(
            repo.path().join(".rk").join("triggers.cue"),
            "triggers: [{name: \"b\", match: {category: \"event\", identity: \"two\"}, run: \"b-wf\"}]\n",
        )
        .unwrap();

        let report = conflicts(&layout, repo.path().to_str().unwrap()).unwrap();
        assert!(report.groups.is_empty());
    }

    #[test]
    fn conflicts_ignores_repo_with_no_local_trigger_file() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        fs::create_dir_all(layout.triggers_dir()).unwrap();
        fs::write(
            layout.triggers_dir().join("a.cue"),
            "triggers: [{name: \"a\", match: {category: \"event\", identity: \"one\"}, run: \"a-wf\"}]\n",
        )
        .unwrap();

        let report = conflicts(&layout, repo.path().to_str().unwrap()).unwrap();
        assert!(report.groups.is_empty());
    }
}

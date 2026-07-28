use anyhow::{Context, Result};
use rk_core::paths::Layout;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};

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
        .with_context(|| format!("cannot read workflow source {source}"))?;
    if source.extension().and_then(|ext| ext.to_str()) != Some("cue") {
        anyhow::bail!("workflow source must be a .cue file: {}", source.display());
    }
    let target_dir = match repo {
        Some(repo) => PathBuf::from(repo)
            .canonicalize()
            .with_context(|| format!("cannot read repository {repo}"))?
            .join(".rk")
            .join("workflows"),
        None => layout.workflows_dir(),
    };
    fs::create_dir_all(&target_dir)
        .with_context(|| format!("create workflow directory {}", target_dir.display()))?;
    let target = target_dir.join(
        source
            .file_name()
            .context("workflow source has no filename")?,
    );
    fs::copy(&source, &target)
        .with_context(|| format!("install {} to {}", source.display(), target.display()))?;
    Ok(target)
}

pub fn drift(layout: &Layout, repo: &str, source_dir: Option<&str>) -> Result<DriftReport> {
    let repo = PathBuf::from(repo)
        .canonicalize()
        .with_context(|| format!("cannot read repository {repo}"))?;
    let source_dir = source_dir
        .map(PathBuf::from)
        .unwrap_or_else(|| repo.join("examples").join("workflows"))
        .canonicalize()
        .with_context(|| "cannot read workflow source directory")?;
    let sources = cue_files(&source_dir)?;
    let roots = [
        ("global", layout.workflows_dir()),
        ("repo-local", repo.join(".rk").join("workflows")),
    ];
    let mut rows = Vec::new();
    for (label, target_dir) in &roots {
        for target in cue_files_if_present(target_dir)? {
            let name = target
                .file_name()
                .context("workflow target has no filename")?;
            let source = sources.get(name).cloned();
            let target_digest = digest(&target)?;
            let source_digest = source.as_ref().map(|path| digest(path)).transpose()?;
            let status = match source_digest.as_deref() {
                None => "unmanaged",
                Some(digest) if digest == target_digest => "clean",
                Some(_) => "drifted",
            };
            rows.push(DriftRow {
                target: format!("{label}:{}", target.display()),
                source: source
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<missing>".into()),
                status: status.into(),
                deployed_digest: Some(target_digest),
                source_digest,
            });
        }
    }
    // Global and repo-local definitions are fallback layers, not two required
    // copies of every workflow. Report one missing row only when neither layer
    // deploys a source definition; otherwise the rows above compare every copy
    // that actually exists.
    for (name, source) in &sources {
        let deployed = roots
            .iter()
            .any(|(_, target_dir)| target_dir.join(name).exists());
        if !deployed {
            let target_dir = repo.join(".rk").join("workflows");
            rows.push(DriftRow {
                target: format!("repo-local:{}", target_dir.join(name).display()),
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

fn cue_files(dir: &Path) -> Result<std::collections::BTreeMap<std::ffi::OsString, PathBuf>> {
    Ok(cue_files_if_present(dir)?
        .into_iter()
        .filter_map(|path| {
            let name = path.file_name()?.to_os_string();
            Some((name, path))
        })
        .collect())
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
    fn install_and_drift_report_content_changes() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let source_dir = repo.path().join("examples/workflows");
        fs::create_dir_all(&source_dir).unwrap();
        let source = source_dir.join("steward.cue");
        fs::write(&source, "workflow: {}\n").unwrap();
        let layout = Layout::at(home.path());
        install(&layout, source.to_str().unwrap(), None).unwrap();

        let clean = drift(&layout, repo.path().to_str().unwrap(), None).unwrap();
        assert_eq!(clean.drifted, 0, "global deployment satisfies the source");
        assert!(clean.rows.iter().any(|row| row.status == "clean"));
        assert!(!clean.rows.iter().any(|row| row.status == "missing"));

        fs::write(
            layout.workflows_dir().join("steward.cue"),
            "workflow: changed\n",
        )
        .unwrap();
        let changed = drift(&layout, repo.path().to_str().unwrap(), None).unwrap();
        assert!(changed.rows.iter().any(|row| row.status == "drifted"));
    }
}

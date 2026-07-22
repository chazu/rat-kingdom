//! Repo registry: the machine-local map of repository names to their paths.
//!
//! Paths are inherently machine-local (a checkout on this castle is not valid
//! on another), so — unlike tickets, which replicate through the tuplespace —
//! this registry is a plain JSON file the daemon owns, mirroring the agent
//! registry in [`crate::agents`].

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoRecord {
    pub name: String,
    pub path: PathBuf,
    pub created_at: DateTime<Utc>,
}

/// JSON-file-backed registry, persisted synchronously on every mutation so the
/// file is the daemon's restart memory.
pub struct RepoRegistry {
    path: PathBuf,
    repos: HashMap<String, RepoRecord>,
}

impl RepoRegistry {
    pub fn load(path: &Path) -> rk_core::Result<Self> {
        let repos = if path.exists() {
            serde_json::from_str(&std::fs::read_to_string(path)?)?
        } else {
            HashMap::new()
        };
        Ok(Self {
            path: path.to_path_buf(),
            repos,
        })
    }

    /// Register (or re-point) a repo by name. Re-adding an existing name updates
    /// its path rather than erroring.
    pub fn add(&mut self, record: RepoRecord) -> rk_core::Result<()> {
        self.repos.insert(record.name.clone(), record);
        self.persist()
    }

    pub fn get(&self, name: &str) -> Option<&RepoRecord> {
        self.repos.get(name)
    }

    pub fn list(&self) -> Vec<RepoRecord> {
        let mut all: Vec<_> = self.repos.values().cloned().collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        all
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

#[cfg(test)]
mod tests {
    use super::*;

    fn record(name: &str, path: &str) -> RepoRecord {
        RepoRecord {
            name: name.into(),
            path: path.into(),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn registry_persists_and_reloads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("repos.json");
        {
            let mut reg = RepoRegistry::load(&path).unwrap();
            reg.add(record("myrepo", "/tmp/myrepo")).unwrap();
        }
        let reg = RepoRegistry::load(&path).unwrap();
        assert_eq!(reg.get("myrepo").unwrap().path, PathBuf::from("/tmp/myrepo"));
    }

    #[test]
    fn re_adding_repoints_the_name() {
        let dir = tempfile::tempdir().unwrap();
        let mut reg = RepoRegistry::load(&dir.path().join("repos.json")).unwrap();
        reg.add(record("r", "/tmp/one")).unwrap();
        reg.add(record("r", "/tmp/two")).unwrap();
        assert_eq!(reg.list().len(), 1);
        assert_eq!(reg.get("r").unwrap().path, PathBuf::from("/tmp/two"));
    }
}

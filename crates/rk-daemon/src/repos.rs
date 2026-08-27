//! Repo registry: the machine-local map of repository names to their paths.
//!
//! Paths are inherently machine-local (a checkout on this castle is not valid
//! on another), so — unlike tickets, which replicate through the tuplespace —
//! this registry is a plain JSON file the daemon owns, mirroring the agent
//! registry in [`crate::agents`].

use chrono::{DateTime, Utc};
use rk_workflow::RepositoryPolicy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoRecord {
    pub name: String,
    pub path: PathBuf,
    pub created_at: DateTime<Utc>,
    /// The git host (e.g. `github.com`, `gitlab.com`) inferred from the remote's
    /// URL at registration time. `None` if the repo had no such remote.
    #[serde(default)]
    pub host: Option<String>,
    /// Exact `.rk/repo.cue` policy approved by an operator, plus the digest of
    /// the activated file. `None` keeps the repo registered and inspectable,
    /// but operational behavior fails closed until onboarding activates it.
    #[serde(default)]
    pub activated_policy: Option<ActivatedRepositoryPolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivatedRepositoryPolicy {
    pub digest: String,
    pub policy: RepositoryPolicy,
}

impl RepoRecord {
    /// The only active repository behavior is the operator-approved CUE
    /// policy. Unknown historical JSON fields are ignored by deserialization
    /// and never synthesize authority.
    pub fn effective_policy(&self) -> rk_core::Result<RepositoryPolicy> {
        self.activated_policy
            .as_ref()
            .map(|approved| approved.policy.clone())
            .ok_or_else(|| {
                rk_core::Error::other(format!(
                    "repository '{}' has no activated .rk/repo.cue policy; run `rk repo onboard start {}`",
                    self.name, self.name
                ))
            })
    }
}

/// Infer the git host (e.g. `github.com`) from a remote URL, handling the three
/// forms git accepts: the scp-like `git@github.com:owner/repo.git`, an
/// `ssh://git@host[:port]/owner/repo.git` URL, and an `https://host/owner/repo`
/// URL. Returns `None` when no host can be read (empty or malformed input).
pub fn infer_host(remote_url: &str) -> Option<String> {
    let url = remote_url.trim();
    if url.is_empty() {
        return None;
    }
    // scheme://[user@]host[:port]/path — matches ssh://, https://, git://, etc.
    let authority = if let Some((_, rest)) = url.split_once("://") {
        rest.split(['/', '?', '#']).next().unwrap_or(rest)
    } else if let Some((prefix, _)) = url.split_once(':') {
        // scp-like: [user@]host:path (no scheme, single colon before the path)
        prefix
    } else {
        return None;
    };
    // Drop any userinfo and port, keeping just the host.
    let host = authority
        .rsplit('@')
        .next()
        .unwrap_or(authority)
        .split(':')
        .next()
        .unwrap_or(authority);
    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
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

    /// Resolve the deterministic first alias registered for a canonical path.
    pub fn get_by_path(&self, path: &Path) -> Option<&RepoRecord> {
        let mut matches = self
            .repos
            .values()
            .filter(|record| record.path == path)
            .collect::<Vec<_>>();
        matches.sort_by(|left, right| left.name.cmp(&right.name));
        matches.into_iter().next()
    }

    pub fn list(&self) -> Vec<RepoRecord> {
        let mut all: Vec<_> = self.repos.values().cloned().collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        all
    }

    /// Activate one content-bound policy for every alias of a canonical repo.
    /// The checkout path is machine-local; aliases must not acquire divergent
    /// delivery authority merely because the same path has multiple names.
    pub fn activate_policy_by_path(
        &mut self,
        repo_path: &Path,
        activated: ActivatedRepositoryPolicy,
    ) -> rk_core::Result<Vec<String>> {
        let mut names = Vec::new();
        for record in self.repos.values_mut() {
            if record.path == repo_path {
                record.activated_policy = Some(activated.clone());
                names.push(record.name.clone());
            }
        }
        if names.is_empty() {
            return Err(rk_core::Error::other(format!(
                "repository is not registered: {}",
                repo_path.display()
            )));
        }
        names.sort();
        self.persist()?;
        Ok(names)
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
            host: None,
            activated_policy: None,
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
        assert_eq!(
            reg.get("myrepo").unwrap().path,
            PathBuf::from("/tmp/myrepo")
        );
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

    #[test]
    fn unknown_historical_registry_fields_grant_no_policy() {
        // Unknown fields are ignored as data, never translated into behavior.
        // Only an activated CUE policy can authorize operations.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("repos.json");
        std::fs::write(
            &path,
            r#"{"old":{"name":"old","path":"/tmp/old","created_at":"2020-01-01T00:00:00Z","merge_mode":"pr","remote":"upstream"}}"#,
        )
        .unwrap();
        let reg = RepoRegistry::load(&path).unwrap();
        let old = reg.get("old").unwrap();
        assert_eq!(old.host, None);
        assert!(old.effective_policy().is_err());
    }

    #[test]
    fn infer_host_reads_every_remote_form() {
        assert_eq!(
            infer_host("git@github.com:owner/repo.git").as_deref(),
            Some("github.com")
        );
        assert_eq!(
            infer_host("https://gitlab.com/owner/repo.git").as_deref(),
            Some("gitlab.com")
        );
        assert_eq!(
            infer_host("ssh://git@gitlab.com:2222/owner/repo.git").as_deref(),
            Some("gitlab.com")
        );
        assert_eq!(
            infer_host("https://user@bitbucket.org/o/r").as_deref(),
            Some("bitbucket.org")
        );
        assert_eq!(
            infer_host("git://example.com/o/r.git").as_deref(),
            Some("example.com")
        );
        assert_eq!(infer_host(""), None);
        assert_eq!(infer_host("   "), None);
        assert_eq!(infer_host("just-a-path"), None);
    }
}

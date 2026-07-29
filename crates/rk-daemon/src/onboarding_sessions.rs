//! Durable guided-onboarding session records.
//!
//! The supervisor remains the authority for harness/process state. This store
//! gives that process a stable repository-scoped identity and preserves the
//! assessment, branch, worktree, and linked agent across CLI disconnects and
//! daemon restarts.

use crate::onboarding::AssessmentReport;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub const ONBOARDER_ROLE: &str = "onboarder";
pub const SESSION_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnboardingSessionState {
    Starting,
    Running,
    Completed,
    Failed,
    Orphaned,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingSession {
    pub schema_version: u32,
    pub id: String,
    pub target: String,
    pub repo_name: String,
    pub repo_path: PathBuf,
    pub base_branch: String,
    pub branch: String,
    pub worktree: PathBuf,
    pub harness: String,
    pub attached: bool,
    pub state: OnboardingSessionState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attach_target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_result: Option<String>,
    pub assessment: AssessmentReport,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingSessionStatus {
    pub schema_version: u32,
    pub id: String,
    pub target: String,
    pub repo_name: String,
    pub repo_path: PathBuf,
    pub base_branch: String,
    pub branch: String,
    pub worktree: PathBuf,
    pub harness: String,
    pub attached: bool,
    pub state: OnboardingSessionState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attach_target: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingReport {
    pub schema_version: u32,
    pub session: OnboardingSessionStatus,
    pub assessment: AssessmentReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_result: Option<String>,
}

impl OnboardingSession {
    #[allow(clippy::too_many_arguments)]
    pub fn starting(
        target: String,
        repo_name: String,
        repo_path: PathBuf,
        base_branch: String,
        harness: String,
        attached: bool,
        assessment: AssessmentReport,
        worktrees_dir: &Path,
    ) -> Self {
        let id = session_id(&repo_path);
        let now = Utc::now();
        Self {
            schema_version: SESSION_SCHEMA_VERSION,
            branch: onboarding_branch(&id),
            worktree: onboarding_worktree(worktrees_dir, &repo_name, &id),
            id,
            target,
            repo_name,
            repo_path,
            base_branch,
            harness,
            attached,
            state: OnboardingSessionState::Starting,
            agent: None,
            attach_target: None,
            agent_result: None,
            assessment,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn status(&self) -> OnboardingSessionStatus {
        OnboardingSessionStatus {
            schema_version: self.schema_version,
            id: self.id.clone(),
            target: self.target.clone(),
            repo_name: self.repo_name.clone(),
            repo_path: self.repo_path.clone(),
            base_branch: self.base_branch.clone(),
            branch: self.branch.clone(),
            worktree: self.worktree.clone(),
            harness: self.harness.clone(),
            attached: self.attached,
            state: self.state,
            agent: self.agent.clone(),
            attach_target: self.attach_target.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }

    pub fn report(&self) -> OnboardingReport {
        OnboardingReport {
            schema_version: SESSION_SCHEMA_VERSION,
            session: self.status(),
            assessment: self.assessment.clone(),
            agent_result: self.agent_result.clone(),
        }
    }
}

/// One stable session per canonical repository path. The path is already the
/// identity resolved by the read-only assessment, so aliases and the caller's
/// current directory cannot mint duplicate sessions.
pub fn session_id(repo_path: &Path) -> String {
    let mut digest = Sha256::new();
    digest.update(b"rat-kingdom-onboarding-session\0");
    digest.update(repo_path.as_os_str().as_encoded_bytes());
    let hex = hex::encode(digest.finalize());
    format!("onb-{}", &hex[..20])
}

pub fn onboarding_branch(id: &str) -> String {
    format!("onboarding/{id}")
}

pub fn onboarding_worktree(worktrees_dir: &Path, repo_name: &str, id: &str) -> PathBuf {
    worktrees_dir.join(repo_name).join("onboarding").join(id)
}

/// Synchronously persisted because each transition is a recovery boundary.
pub struct OnboardingSessions {
    path: PathBuf,
    sessions: BTreeMap<String, OnboardingSession>,
}

impl OnboardingSessions {
    pub fn load(path: &Path) -> rk_core::Result<Self> {
        let sessions = if path.exists() {
            serde_json::from_slice(&std::fs::read(path)?)?
        } else {
            BTreeMap::new()
        };
        Ok(Self {
            path: path.to_path_buf(),
            sessions,
        })
    }

    pub fn get(&self, id: &str) -> Option<OnboardingSession> {
        self.sessions.get(id).cloned()
    }

    /// Insert the durable starting journal unless this repository-derived id
    /// already exists. The bool distinguishes the one caller that owns launch
    /// from idempotent concurrent/repeated starts.
    pub fn insert_if_absent(
        &mut self,
        session: OnboardingSession,
    ) -> rk_core::Result<(OnboardingSession, bool)> {
        if let Some(existing) = self.sessions.get(&session.id) {
            return Ok((existing.clone(), false));
        }
        self.sessions.insert(session.id.clone(), session.clone());
        self.persist()?;
        Ok((session, true))
    }

    pub fn update(
        &mut self,
        id: &str,
        f: impl FnOnce(&mut OnboardingSession),
    ) -> rk_core::Result<Option<OnboardingSession>> {
        let Some(session) = self.sessions.get_mut(id) else {
            return Ok(None);
        };
        f(session);
        session.updated_at = Utc::now();
        let updated = session.clone();
        self.persist()?;
        Ok(Some(updated))
    }

    /// A daemon cannot know whether an in-flight launch crossed its last
    /// persistence boundary. Mark every nonterminal session orphaned on boot;
    /// reconciliation against the durable agent registry may immediately
    /// refine it to completed/failed, and `resume` is safe and idempotent.
    pub fn orphan_nonterminal(&mut self) -> rk_core::Result<()> {
        let now = Utc::now();
        let mut changed = false;
        for session in self.sessions.values_mut() {
            if matches!(
                session.state,
                OnboardingSessionState::Starting | OnboardingSessionState::Running
            ) {
                session.state = OnboardingSessionState::Orphaned;
                session.updated_at = now;
                changed = true;
            }
        }
        if changed {
            self.persist()?;
        }
        Ok(())
    }

    fn persist(&self) -> rk_core::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(&self.sessions)?)?;
        std::fs::rename(tmp, &self.path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::onboarding::RepositoryIdentity;

    fn assessment(path: &Path) -> AssessmentReport {
        AssessmentReport {
            schema_version: 1,
            identity: RepositoryIdentity {
                target: path.display().to_string(),
                canonical_path: Some(path.display().to_string()),
                registered_name: Some("repo".into()),
                registered_aliases: vec!["repo".into()],
            },
            ready: true,
            findings: Vec::new(),
        }
    }

    #[test]
    fn repository_identity_stabilizes_session_branch_and_worktree() {
        let root = PathBuf::from("/tmp/example");
        let worktrees = PathBuf::from("/tmp/rk-worktrees");
        let first = OnboardingSession::starting(
            root.display().to_string(),
            "repo".into(),
            root.clone(),
            "main".into(),
            "codex".into(),
            false,
            assessment(&root),
            &worktrees,
        );
        let second = OnboardingSession::starting(
            "repo".into(),
            "repo".into(),
            root,
            "main".into(),
            "codex".into(),
            true,
            first.assessment.clone(),
            &worktrees,
        );
        assert_eq!(first.id, second.id);
        assert_eq!(first.branch, second.branch);
        assert_eq!(first.worktree, second.worktree);
        assert!(first.branch.starts_with("onboarding/onb-"));
    }

    #[test]
    fn store_reuses_and_recovers_a_starting_session() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("repo");
        let path = dir.path().join("sessions.json");
        let session = OnboardingSession::starting(
            root.display().to_string(),
            "repo".into(),
            root.clone(),
            "main".into(),
            "codex".into(),
            false,
            assessment(&root),
            &dir.path().join("worktrees"),
        );
        let id = session.id.clone();
        {
            let mut store = OnboardingSessions::load(&path).unwrap();
            assert!(store.insert_if_absent(session.clone()).unwrap().1);
            assert!(!store.insert_if_absent(session).unwrap().1);
        }
        let mut store = OnboardingSessions::load(&path).unwrap();
        store.orphan_nonterminal().unwrap();
        assert_eq!(
            store.get(&id).unwrap().state,
            OnboardingSessionState::Orphaned
        );
    }
}

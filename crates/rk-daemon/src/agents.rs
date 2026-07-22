//! Agent registry: the supervision tree as first-class data.
//!
//! Every record carries its `parent` (the spawner) — completion routing walks
//! this structure, never payload fields (the predecessor's foreman-routing lesson).

use chrono::{DateTime, Utc};
use rk_harness::TokenUsage;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentState {
    Spawning,
    Running,
    Completed,
    Failed,
    Dismissed,
    /// The daemon restarted while this agent was running; its process is gone
    /// but worktree/branch/session are preserved for respawn.
    Orphaned,
}

impl AgentState {
    pub fn is_live(self) -> bool {
        matches!(self, AgentState::Spawning | AgentState::Running)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRecord {
    pub name: String,
    pub role: String,
    pub harness: String,
    /// Model requested at spawn (None = harness default; pricing then relies
    /// on harness-reported cost only).
    #[serde(default)]
    pub model: Option<String>,
    pub repo_root: PathBuf,
    pub repo_name: String,
    pub task: Option<String>,
    pub branch: Option<String>,
    pub worktree: Option<PathBuf>,
    /// Merge target on dismissal.
    pub target_branch: String,
    /// Spawning agent's name (None = spawned by a human).
    pub parent: Option<String>,
    pub session_id: Option<String>,
    /// herdr target when running attached in a pane (attach-mode spawn).
    #[serde(default)]
    pub attach_target: Option<String>,
    pub pid: Option<u32>,
    pub state: AgentState,
    pub result: Option<String>,
    pub usage: TokenUsage,
    pub cost_usd: f64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// JSON-file-backed registry. All mutation goes through [`Registry::update`],
/// which persists synchronously — the file is the daemon's restart memory.
pub struct Registry {
    path: PathBuf,
    agents: HashMap<String, AgentRecord>,
}

impl Registry {
    pub fn load(path: &Path) -> rk_core::Result<Self> {
        let agents = if path.exists() {
            let data = std::fs::read_to_string(path)?;
            serde_json::from_str(&data)?
        } else {
            HashMap::new()
        };
        Ok(Self {
            path: path.to_path_buf(),
            agents,
        })
    }

    /// Mark all live agents orphaned (called once at daemon startup).
    pub fn orphan_live_agents(&mut self) -> rk_core::Result<Vec<String>> {
        let mut orphaned = Vec::new();
        for record in self.agents.values_mut() {
            if record.state.is_live() {
                record.state = AgentState::Orphaned;
                record.pid = None;
                record.updated_at = Utc::now();
                orphaned.push(record.name.clone());
            }
        }
        self.persist()?;
        Ok(orphaned)
    }

    pub fn insert(&mut self, record: AgentRecord) -> rk_core::Result<()> {
        self.agents.insert(record.name.clone(), record);
        self.persist()
    }

    pub fn get(&self, name: &str) -> Option<&AgentRecord> {
        self.agents.get(name)
    }

    pub fn names_in_use(&self) -> Vec<&str> {
        self.agents.keys().map(String::as_str).collect()
    }

    pub fn list(&self) -> Vec<&AgentRecord> {
        let mut all: Vec<_> = self.agents.values().collect();
        all.sort_by_key(|a| a.created_at);
        all
    }

    pub fn update<F>(&mut self, name: &str, mutate: F) -> rk_core::Result<Option<AgentRecord>>
    where
        F: FnOnce(&mut AgentRecord),
    {
        let Some(record) = self.agents.get_mut(name) else {
            return Ok(None);
        };
        mutate(record);
        record.updated_at = Utc::now();
        let snapshot = record.clone();
        self.persist()?;
        Ok(Some(snapshot))
    }

    pub fn remove(&mut self, name: &str) -> rk_core::Result<Option<AgentRecord>> {
        let removed = self.agents.remove(name);
        if removed.is_some() {
            self.persist()?;
        }
        Ok(removed)
    }

    fn persist(&self) -> rk_core::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(&self.agents)?)?;
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(name: &str, state: AgentState) -> AgentRecord {
        AgentRecord {
            name: name.into(),
            role: "rat".into(),
            harness: "fake".into(),
            model: None,
            repo_root: "/tmp/repo".into(),
            repo_name: "repo".into(),
            task: Some(".rk-1".into()),
            branch: Some(format!("rat/{name}/rk-1")),
            worktree: Some(format!("/tmp/wt/{name}").into()),
            target_branch: "main".into(),
            parent: None,
            session_id: None,
            attach_target: None,
            pid: Some(1234),
            state,
            result: None,
            usage: TokenUsage::default(),
            cost_usd: 0.0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn registry_persists_and_reloads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agents.json");
        {
            let mut reg = Registry::load(&path).unwrap();
            reg.insert(record("Whisker", AgentState::Running)).unwrap();
            reg.update("Whisker", |r| r.cost_usd = 1.25).unwrap();
        }
        let reg = Registry::load(&path).unwrap();
        assert_eq!(reg.get("Whisker").unwrap().cost_usd, 1.25);
    }

    #[test]
    fn orphaning_marks_only_live_agents() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agents.json");
        let mut reg = Registry::load(&path).unwrap();
        reg.insert(record("Whisker", AgentState::Running)).unwrap();
        reg.insert(record("Nibbles", AgentState::Dismissed))
            .unwrap();

        let orphaned = reg.orphan_live_agents().unwrap();
        assert_eq!(orphaned, vec!["Whisker".to_string()]);
        assert_eq!(reg.get("Whisker").unwrap().state, AgentState::Orphaned);
        assert!(reg.get("Whisker").unwrap().pid.is_none());
        assert_eq!(reg.get("Nibbles").unwrap().state, AgentState::Dismissed);
    }
}

//! Coordinator-facing projection helpers.
//!
//! The tuplespace remains the durable source of truth. This module gives the
//! daemon a compact, filterable view over workflow snapshots, agent records,
//! and replayable lifecycle events so callers do not have to rebuild one from
//! unrelated RPCs.

use crate::agents::{AgentRecord, AgentState};
use crate::workflow_exec::Instance;
use rk_core::tuple::{Category, Tuple};
use rk_space::CoordinatorEvent;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

/// Keep the initial coordinator response below the daemon's 1 MiB frame cap.
/// A truncated response is still useful because it includes a current
/// snapshot and explicitly tells the client to discard incomplete history.
pub const MAX_REPLAY_EVENTS: usize = 256;
const MAX_ERROR_CHARS: usize = 512;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorSession {
    pub coordinator: String,
    pub cursor: u64,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// Small durable session registry for turn-boundary adapters. The adapter may
/// acknowledge only after it accepts a pending block; a disconnected session
/// therefore replays from its last acknowledged cursor.
#[derive(Debug)]
pub struct CoordinatorSessions {
    path: PathBuf,
    sessions: HashMap<String, CoordinatorSession>,
}

impl CoordinatorSessions {
    pub fn load(path: &Path) -> rk_core::Result<Self> {
        let sessions = if path.exists() {
            serde_json::from_str(&fs::read_to_string(path)?)?
        } else {
            HashMap::new()
        };
        Ok(Self {
            path: path.to_path_buf(),
            sessions,
        })
    }

    pub fn register(&mut self, coordinator: &str, after: Option<u64>) -> rk_core::Result<CoordinatorSession> {
        let now = chrono::Utc::now();
        let entry = self.sessions.entry(coordinator.to_string()).or_insert_with(|| CoordinatorSession {
            coordinator: coordinator.to_string(),
            cursor: after.unwrap_or(0),
            updated_at: now,
        });
        if let Some(after) = after {
            entry.cursor = entry.cursor.max(after);
        }
        entry.updated_at = now;
        let snapshot = entry.clone();
        self.persist()?;
        Ok(snapshot)
    }

    pub fn cursor(&self, coordinator: &str) -> Option<u64> {
        self.sessions.get(coordinator).map(|session| session.cursor)
    }

    pub fn acknowledge(&mut self, coordinator: &str, cursor: u64) -> rk_core::Result<CoordinatorSession> {
        let session = self
            .sessions
            .get_mut(coordinator)
            .ok_or_else(|| rk_core::Error::other(format!("unknown coordinator session: {coordinator}")))?;
        session.cursor = session.cursor.max(cursor);
        session.updated_at = chrono::Utc::now();
        let snapshot = session.clone();
        self.persist()?;
        Ok(snapshot)
    }

    fn persist(&self) -> rk_core::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let data = serde_json::to_vec_pretty(&self.sessions)?;
        let tmp = self.path.with_extension("json.tmp");
        fs::write(&tmp, data)?;
        fs::rename(tmp, &self.path)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct CoordinatorFilter {
    pub repo: Option<String>,
    pub instance: Option<String>,
    pub after: Option<u64>,
    /// Owned-workflow scope, identified by a stable coordinator session.
    pub coordinator: Option<String>,
    /// `instance`, `owned`, `subtree`, or diagnostic `repo`.
    pub scope: Option<String>,
    /// Middle-rat root for an explicit subtree view.
    pub subtree: Option<String>,
    /// `middle` (default for owned views), `all`, or `workflow`.
    pub depth: Option<String>,
    /// `attention` and/or `rollup`; omitted means both.
    pub include: Vec<String>,
}

impl CoordinatorFilter {
    pub fn matches_event(&self, tuple: &Tuple) -> bool {
        if tuple.category != Category::Event {
            return false;
        }
        if !matches!(
            tuple.identity.as_str(),
            "workflow_state_changed"
                | "middle_rat_progress"
                | "coordination_attention"
                | "agent_lifecycle"
        ) {
            return false;
        }
        if self.repo.as_deref().is_some_and(|repo| tuple.scope != repo) {
            return false;
        }
        if self
            .coordinator
            .as_deref()
            .is_some_and(|coordinator| {
                tuple.payload.get("coordinator").and_then(Value::as_str)
                    != Some(coordinator)
            })
        {
            return false;
        }
        if !self.include.is_empty() {
            let route = tuple.payload.get("route").and_then(Value::as_str).unwrap_or("rollup");
            let class = if matches!(route, "escalate" | "terminal") {
                "attention"
            } else {
                "rollup"
            };
            if !self.include.iter().any(|requested| requested == class) {
                return false;
            }
        }
        self.instance
            .as_deref()
            .is_none_or(|instance| event_instance(tuple).is_some_and(|value| value == instance))
            && self
                .subtree
                .as_deref()
                .is_none_or(|root| event_agent(tuple).is_some_and(|value| value == root))
    }

    pub fn matches_workflow(&self, instance: &Instance) -> bool {
        self.repo
            .as_deref()
            .is_none_or(|repo| repo_matches(repo, &instance.repo))
            && self
                .instance
                .as_deref()
                .is_none_or(|id| instance.id == id)
            && self
                .coordinator
                .as_deref()
                .is_none_or(|owner| instance.coordinator.as_deref() == Some(owner))
    }

    pub fn matches_agent(&self, agent: &AgentRecord) -> bool {
        self.repo
            .as_deref()
            .is_none_or(|repo| repo == agent.repo_name || repo_matches(repo, &agent.repo_root.to_string_lossy()))
            && self
                .instance
                .as_deref()
                .is_none_or(|id| agent.workflow_instance.as_deref() == Some(id))
            && self
                .coordinator
                .as_deref()
                .is_none_or(|owner| agent.coordinator.as_deref() == Some(owner))
    }
}

fn repo_matches(name: &str, path: &str) -> bool {
    name == path
        || Path::new(path)
            .file_name()
            .and_then(|part| part.to_str())
            .is_some_and(|base| base == name)
}

fn event_instance(tuple: &Tuple) -> Option<&str> {
    tuple
        .payload
        .get("instance")
        .or_else(|| tuple.payload.get("workflow_instance"))
        .and_then(Value::as_str)
}

fn event_agent(tuple: &Tuple) -> Option<&str> {
    tuple
        .payload
        .get("agent")
        .or_else(|| tuple.payload.get("subject").and_then(|s| s.get("agent")))
        .and_then(Value::as_str)
}

#[derive(Debug, Clone, Serialize)]
pub struct CoordinatorSnapshot {
    pub workflows: Vec<WorkflowSummary>,
    pub agents: Vec<AgentSummary>,
    pub middle_rats: Vec<MiddleRatSummary>,
    pub attention: Vec<CoordinationAttention>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkflowSummary {
    pub id: String,
    pub workflow: String,
    pub repo: String,
    pub coordinator: Option<String>,
    pub status: crate::workflow_exec::InstanceStatus,
    pub revision: u64,
    pub current_step: usize,
    pub total_steps: usize,
    pub awaiting: Option<String>,
    pub active_agent: Option<String>,
    pub active_branch: Option<String>,
    pub awaited: Vec<String>,
    pub error: Option<String>,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentSummary {
    pub name: String,
    pub role: String,
    pub repo: String,
    pub task: Option<String>,
    pub workflow_instance: Option<String>,
    pub coordinator: Option<String>,
    pub parent: Option<String>,
    pub state: AgentState,
    pub branch: Option<String>,
    pub crashed: bool,
    pub cost_usd: f64,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub progress: Option<AgentProgressSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentProgressSummary {
    pub summary: String,
    pub next: Option<String>,
    pub status: String,
    pub revision: u64,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct DescendantRollup {
    pub total: usize,
    pub spawning: usize,
    pub running: usize,
    pub completed: usize,
    pub failed: usize,
    pub orphaned: usize,
    pub dismissed: usize,
    pub blocked: usize,
    pub escalated: usize,
    pub oldest_active_age_secs: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MiddleRatSummary {
    pub agent: String,
    pub generation: chrono::DateTime<chrono::Utc>,
    pub role: String,
    pub workflow_instance: Option<String>,
    pub coordinator: Option<String>,
    pub parent: Option<String>,
    pub state: AgentState,
    pub branch: Option<String>,
    pub task: Option<String>,
    pub cost_usd: f64,
    pub rollup: DescendantRollup,
    pub summary: Option<String>,
    pub next: Option<String>,
    pub last_meaningful_update: chrono::DateTime<chrono::Utc>,
    pub stale: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct CoordinationAttention {
    pub cursor: u64,
    pub kind: String,
    pub severity: String,
    pub workflow_instance: Option<String>,
    pub agent: Option<String>,
    pub summary: String,
}

pub fn snapshot(
    workflows: &[Instance],
    agents: &[AgentRecord],
    filter: &CoordinatorFilter,
) -> CoordinatorSnapshot {
    hierarchical_snapshot(workflows, agents, &[], filter)
}

/// Build the coordinator's bounded hierarchical view. The input is deliberately
/// plain data so the supervision-tree rules can be tested without a daemon.
pub fn hierarchical_snapshot(
    workflows: &[Instance],
    agents: &[AgentRecord],
    events: &[CoordinatorEvent],
    filter: &CoordinatorFilter,
) -> CoordinatorSnapshot {
    let workflows: Vec<&Instance> = workflows
        .iter()
        .filter(|instance| filter.matches_workflow(instance))
        .collect();
    let workflow_ids: HashSet<&str> = workflows.iter().map(|instance| instance.id.as_str()).collect();
    let selected: Vec<&AgentRecord> = agents
        .iter()
        .filter(|agent| filter.matches_agent(agent))
        .filter(|agent| {
            filter
                .subtree
                .as_deref()
                .is_none_or(|root| agent.name == root || is_descendant(agents, agent, root))
        })
        .filter(|agent| {
            agent
                .workflow_instance
                .as_deref()
                .is_none_or(|instance| workflow_ids.contains(instance))
        })
        .collect();

    let depth = filter.depth.as_deref().unwrap_or_else(|| {
        if filter.coordinator.is_some() || filter.scope.as_deref() == Some("owned") {
            "middle"
        } else {
            "all"
        }
    });
    let middle_rats: Vec<MiddleRatSummary> = selected
        .iter()
        .filter(|agent| is_reporting_boundary(agent))
        .map(|agent| middle_rat_summary(agent, agents, events))
        .collect();
    let visible_agents = if depth == "middle" {
        selected
            .iter()
            .filter(|agent| is_reporting_boundary(agent))
            .map(|agent| agent_summary(agent))
            .collect()
    } else if depth == "workflow" {
        Vec::new()
    } else {
        selected.iter().map(|agent| agent_summary(agent)).collect()
    };
    let mut attention: Vec<CoordinationAttention> = events
        .iter()
        .filter(|event| filter.matches_event(&event.event))
        .filter(|event| event.event.payload.get("route").and_then(Value::as_str) == Some("escalate")
            || event.event.payload.get("route").and_then(Value::as_str) == Some("terminal"))
        .map(|event| CoordinationAttention {
            cursor: event.cursor,
            kind: event.event.identity.clone(),
            severity: event.event.payload.get("severity").and_then(Value::as_str).unwrap_or("warning").to_string(),
            workflow_instance: event_instance(&event.event).map(str::to_string),
            agent: event_agent(&event.event).map(str::to_string),
            summary: event.event.payload.get("summary")
                .or_else(|| event.event.payload.get("reason"))
                .and_then(Value::as_str)
                .unwrap_or("coordination event")
                .chars().take(512).collect(),
        })
        .collect();
    for middle in &middle_rats {
        if middle.stale {
            attention.push(CoordinationAttention {
                cursor: 0,
                kind: "reporting_boundary_stale".into(),
                severity: "warning".into(),
                workflow_instance: middle.workflow_instance.clone(),
                agent: Some(middle.agent.clone()),
                summary: format!(
                    "{} has live descendants but no meaningful update since {}",
                    middle.agent, middle.last_meaningful_update
                ),
            });
        }
    }
    CoordinatorSnapshot {
        workflows: workflows.into_iter().map(workflow_summary).collect(),
        agents: visible_agents,
        middle_rats,
        attention,
    }
}

fn is_reporting_boundary(agent: &AgentRecord) -> bool {
    agent
        .coordination
        .as_ref()
        .and_then(|coordination| coordination.reports_to.as_deref())
        == Some("coordinator")
        || (agent.workflow_instance.is_some()
            && matches!(agent.role.as_str(), "foreman" | "steward"))
}

fn is_descendant(all: &[AgentRecord], agent: &AgentRecord, root: &str) -> bool {
    let mut parent = agent.parent.as_deref();
    let mut seen = HashSet::new();
    while let Some(name) = parent {
        if name == root {
            return true;
        }
        if !seen.insert(name) {
            return false;
        }
        parent = all.iter().find(|candidate| candidate.name == name).and_then(|candidate| candidate.parent.as_deref());
    }
    false
}

fn middle_rat_summary(
    agent: &AgentRecord,
    all: &[AgentRecord],
    events: &[CoordinatorEvent],
) -> MiddleRatSummary {
    let descendants: Vec<&AgentRecord> = all
        .iter()
        .filter(|candidate| is_descendant(all, candidate, &agent.name))
        .collect();
    let mut rollup = DescendantRollup {
        total: descendants.len(),
        ..Default::default()
    };
    let now = chrono::Utc::now();
    for descendant in descendants {
        match descendant.state {
            AgentState::Spawning => rollup.spawning += 1,
            AgentState::Running => rollup.running += 1,
            AgentState::Completed => rollup.completed += 1,
            AgentState::Failed => rollup.failed += 1,
            AgentState::Orphaned => rollup.orphaned += 1,
            AgentState::Dismissed => rollup.dismissed += 1,
        }
        if descendant.progress.as_ref().is_some_and(|progress| progress.status == "blocked") {
            rollup.blocked += 1;
        }
        if descendant.state.is_live() {
            let age = (now - descendant.created_at).num_seconds().max(0);
            rollup.oldest_active_age_secs = Some(rollup.oldest_active_age_secs.map_or(age, |current| current.max(age)));
        }
    }
    rollup.escalated = events
        .iter()
        .filter(|event| event.event.payload.get("route").and_then(Value::as_str) == Some("escalate"))
        .filter(|event| event.event.payload.get("agent").and_then(Value::as_str).is_some_and(|name| name == agent.name || all.iter().any(|candidate| candidate.name == name && is_descendant(all, candidate, &agent.name))))
        .count();
    let last_meaningful_update = agent
        .progress
        .as_ref()
        .map(|progress| progress.updated_at)
        .unwrap_or(agent.updated_at);
    let stale = rollup.total != 0
        && rollup.running > 0
        && (chrono::Utc::now() - last_meaningful_update).num_minutes() >= 15;
    MiddleRatSummary {
        agent: agent.name.clone(),
        generation: agent.created_at,
        role: agent.role.clone(),
        workflow_instance: agent.workflow_instance.clone(),
        coordinator: agent.coordinator.clone(),
        parent: agent.parent.clone(),
        state: agent.state,
        branch: agent.branch.clone(),
        task: agent.task.clone(),
        cost_usd: agent.cost_usd,
        rollup,
        summary: agent.progress.as_ref().map(|progress| progress.summary.clone()),
        next: agent.progress.as_ref().and_then(|progress| progress.next.clone()),
        last_meaningful_update,
        stale,
    }
}

fn workflow_summary(instance: &Instance) -> WorkflowSummary {
    WorkflowSummary {
        id: instance.id.clone(),
        workflow: instance.workflow.clone(),
        repo: instance.repo.clone(),
        coordinator: instance.coordinator.clone(),
        status: instance.status,
        revision: instance.revision,
        current_step: instance.current_step,
        total_steps: instance.total_steps,
        awaiting: instance.awaiting.clone(),
        active_agent: instance.context.active_agent.clone(),
        active_branch: instance.context.active_branch.clone(),
        awaited: instance.context.awaited.clone(),
        error: instance.error.as_deref().map(bound_error),
        started_at: instance.started_at,
        completed_at: instance.completed_at,
    }
}

fn agent_summary(agent: &AgentRecord) -> AgentSummary {
    AgentSummary {
        name: agent.name.clone(),
        role: agent.role.clone(),
        repo: agent.repo_name.clone(),
        task: agent.task.clone(),
        workflow_instance: agent.workflow_instance.clone(),
        coordinator: agent.coordinator.clone(),
        parent: agent.parent.clone(),
        state: agent.state,
        branch: agent.branch.clone(),
        crashed: agent.crashed,
        cost_usd: agent.cost_usd,
        created_at: agent.created_at,
        updated_at: agent.updated_at,
        progress: agent.progress.as_ref().map(|progress| AgentProgressSummary {
            summary: progress.summary.clone(),
            next: progress.next.clone(),
            status: progress.status.clone(),
            revision: progress.revision,
            updated_at: progress.updated_at,
        }),
    }
}

fn bound_error(error: &str) -> String {
    error.chars().take(MAX_ERROR_CHARS).collect()
}

#[derive(Debug, Clone)]
pub struct Replay {
    pub events: Vec<CoordinatorEvent>,
    /// The newest scanned journal entry, including the extra sentinel row when
    /// the result was truncated. The live stream skips through this boundary.
    pub boundary: Option<u64>,
    pub truncated: bool,
}

pub fn replay(scanned: Vec<CoordinatorEvent>, filter: &CoordinatorFilter) -> Replay {
    let truncated = scanned.len() > MAX_REPLAY_EVENTS;
    let boundary = scanned
        .get(if truncated {
            MAX_REPLAY_EVENTS
        } else {
            scanned.len().saturating_sub(1)
        })
        .map(|event| event.cursor)
        .or(filter.after);
    let mut events = Vec::new();
    let mut progress_positions = HashMap::new();
    for event in scanned
        .into_iter()
        .take(MAX_REPLAY_EVENTS)
        .filter(|event| filter.matches_event(&event.event))
    {
        if event.event.identity == "middle_rat_progress"
            && event.event.payload.get("route").and_then(Value::as_str) == Some("rollup")
        {
            let key = (
                event_instance(&event.event).unwrap_or("").to_string(),
                event_agent(&event.event).unwrap_or("").to_string(),
            );
            if let Some(position) = progress_positions.get(&key).copied() {
                events[position] = event;
                continue;
            }
            progress_positions.insert(key, events.len());
        }
        events.push(event);
    }
    Replay {
        events,
        boundary,
        truncated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agents::AgentProgress;
    use rk_harness::TokenUsage;
    use rk_core::tuple::{Category, Tuple};
    use serde_json::json;
    use std::path::PathBuf;

    #[test]
    fn instance_filter_accepts_own_workflow_transition_only() {
        let filter = CoordinatorFilter {
            repo: Some("repo".into()),
            instance: Some("wf-own".into()),
            ..Default::default()
        };
        let own = Tuple::new(
            Category::Event,
            "repo",
            "workflow_state_changed",
            "castle",
            json!({"instance": "wf-own"}),
        );
        let peer = Tuple::new(
            Category::Event,
            "repo",
            "workflow_state_changed",
            "castle",
            json!({"instance": "wf-peer"}),
        );
        assert!(filter.matches_event(&own));
        assert!(!filter.matches_event(&peer));
    }

    #[test]
    fn replay_uses_a_sentinel_boundary_when_history_is_truncated() {
        let filter = CoordinatorFilter::default();
        let tuples: Vec<_> = (0..=MAX_REPLAY_EVENTS)
            .map(|i| {
                CoordinatorEvent {
                    cursor: i as u64 + 1,
                    event: Tuple::new(
                        Category::Event,
                        "repo",
                        "workflow_state_changed",
                        "castle",
                        json!({"instance": format!("wf-{i}")}),
                    ),
                }
            })
            .collect();
        let sentinel = tuples[MAX_REPLAY_EVENTS].cursor;
        let replay = replay(tuples, &filter);
        assert!(replay.truncated);
        assert_eq!(replay.events.len(), MAX_REPLAY_EVENTS);
        assert_eq!(replay.boundary, Some(sentinel));
    }

    fn agent(
        name: &str,
        role: &str,
        parent: Option<&str>,
        state: AgentState,
        progress: Option<AgentProgress>,
    ) -> AgentRecord {
        let now = chrono::Utc::now();
        AgentRecord {
            name: name.into(),
            spawn: None,
            role: role.into(),
            coordination: (role == "foreman").then(|| rk_workflow::Coordination {
                reports_to: Some("coordinator".into()),
                descendant_policy: "rollup".into(),
            }),
            harness: "fake".into(),
            permission_mode: None,
            model: None,
            repo_root: PathBuf::from("/tmp/repo"),
            repo_name: "repo".into(),
            task: Some(name.into()),
            branch: Some(format!("rat/{name}")),
            worktree: None,
            target_branch: "main".into(),
            parent: parent.map(str::to_string),
            workflow_instance: Some("wf-1".into()),
            coordinator: Some("session-1".into()),
            session_id: None,
            attach_target: None,
            pid: None,
            merge_commit: None,
            state,
            crashed: false,
            stderr_tail: None,
            result: None,
            progress,
            usage: TokenUsage::default(),
            cost_usd: 0.0,
            created_at: now,
            updated_at: now,
            archived_at: None,
        }
    }

    #[test]
    fn hierarchical_snapshot_rolls_up_only_owned_boundary_descendants() {
        let workflow = Instance {
            id: "wf-1".into(),
            workflow: "feature-set".into(),
            repo: "repo".into(),
            coordinator: Some("session-1".into()),
            schedule: None,
            status: crate::workflow_exec::InstanceStatus::Running,
            revision: 1,
            current_step: 1,
            total_steps: 3,
            context: Default::default(),
            error: None,
            awaiting: None,
            instance_max_usd: None,
            definition: "feature-set".into(),
            definition_digest: String::new(),
            automated_landing_authorized: false,
            params: Default::default(),
            depth: 0,
            started_at: chrono::Utc::now(),
            completed_at: None,
            archived_at: None,
            trigger: None,
        };
        let blocked = AgentProgress {
            summary: "waiting on API decision".into(),
            next: Some("escalate to coordinator".into()),
            status: "blocked".into(),
            revision: 1,
            updated_at: chrono::Utc::now(),
        };
        let agents = vec![
            agent("Foreman-1", "foreman", None, AgentState::Running, None),
            agent("Leaf-1", "rat", Some("Foreman-1"), AgentState::Running, Some(blocked)),
            agent("Leaf-2", "rat", Some("Foreman-1"), AgentState::Completed, None),
            agent("Unrelated", "rat", None, AgentState::Running, None),
        ];
        let filter = CoordinatorFilter {
            coordinator: Some("session-1".into()),
            scope: Some("owned".into()),
            ..Default::default()
        };
        let snapshot = hierarchical_snapshot(&[workflow], &agents, &[], &filter);
        assert_eq!(snapshot.workflows.len(), 1);
        assert_eq!(snapshot.middle_rats.len(), 1);
        let rollup = &snapshot.middle_rats[0].rollup;
        assert_eq!(rollup.total, 2);
        assert_eq!(rollup.running, 1);
        assert_eq!(rollup.completed, 1);
        assert_eq!(rollup.blocked, 1);
        assert!(snapshot.agents.iter().all(|agent| agent.name == "Foreman-1"));
    }

    #[test]
    fn coordinator_session_cursor_is_durable_and_monotonic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.json");
        let mut sessions = CoordinatorSessions::load(&path).unwrap();
        assert_eq!(sessions.register("session-1", Some(4)).unwrap().cursor, 4);
        assert_eq!(sessions.acknowledge("session-1", 9).unwrap().cursor, 9);
        assert_eq!(sessions.acknowledge("session-1", 3).unwrap().cursor, 9);
        let restored = CoordinatorSessions::load(&path).unwrap();
        assert_eq!(restored.cursor("session-1"), Some(9));
    }
}

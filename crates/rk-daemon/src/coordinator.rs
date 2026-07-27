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
use std::path::Path;

/// Keep the initial coordinator response below the daemon's 1 MiB frame cap.
/// A truncated response is still useful because it includes a current
/// snapshot and explicitly tells the client to discard incomplete history.
pub const MAX_REPLAY_EVENTS: usize = 256;
const MAX_ERROR_CHARS: usize = 512;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct CoordinatorFilter {
    pub repo: Option<String>,
    pub instance: Option<String>,
    pub after: Option<u64>,
}

impl CoordinatorFilter {
    pub fn matches_event(&self, tuple: &Tuple) -> bool {
        if tuple.category != Category::Event {
            return false;
        }
        if tuple.identity != "workflow_state_changed" {
            return false;
        }
        if self.repo.as_deref().is_some_and(|repo| tuple.scope != repo) {
            return false;
        }
        self.instance
            .as_deref()
            .is_none_or(|instance| event_instance(tuple).is_some_and(|value| value == instance))
    }

    pub fn matches_workflow(&self, instance: &Instance) -> bool {
        self.repo
            .as_deref()
            .is_none_or(|repo| repo_matches(repo, &instance.repo))
            && self
                .instance
                .as_deref()
                .is_none_or(|id| instance.id == id)
    }

    pub fn matches_agent(&self, agent: &AgentRecord) -> bool {
        self.repo
            .as_deref()
            .is_none_or(|repo| repo == agent.repo_name || repo_matches(repo, &agent.repo_root.to_string_lossy()))
            && self
                .instance
                .as_deref()
                .is_none_or(|id| agent.workflow_instance.as_deref() == Some(id))
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

#[derive(Debug, Clone, Serialize)]
pub struct CoordinatorSnapshot {
    pub workflows: Vec<WorkflowSummary>,
    pub agents: Vec<AgentSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkflowSummary {
    pub id: String,
    pub workflow: String,
    pub repo: String,
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
    pub state: AgentState,
    pub branch: Option<String>,
    pub crashed: bool,
    pub cost_usd: f64,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

pub fn snapshot(
    workflows: &[Instance],
    agents: &[AgentRecord],
    filter: &CoordinatorFilter,
) -> CoordinatorSnapshot {
    CoordinatorSnapshot {
        workflows: workflows
            .iter()
            .filter(|instance| filter.matches_workflow(instance))
            .map(workflow_summary)
            .collect(),
        agents: agents
            .iter()
            .filter(|agent| filter.matches_agent(agent))
            .map(agent_summary)
            .collect(),
    }
}

fn workflow_summary(instance: &Instance) -> WorkflowSummary {
    WorkflowSummary {
        id: instance.id.clone(),
        workflow: instance.workflow.clone(),
        repo: instance.repo.clone(),
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
        state: agent.state,
        branch: agent.branch.clone(),
        crashed: agent.crashed,
        cost_usd: agent.cost_usd,
        created_at: agent.created_at,
        updated_at: agent.updated_at,
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
    let events = scanned
        .into_iter()
        .take(MAX_REPLAY_EVENTS)
        .filter(|event| filter.matches_event(&event.event))
        .collect();
    Replay {
        events,
        boundary,
        truncated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rk_core::tuple::{Category, Tuple};
    use serde_json::json;

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
}

//! Unified operator attention queue.
//!
//! `rk inbox` collapses the five surfaces an operator otherwise polls
//! separately — `rk list` (failed/orphaned agents), `rk workflow list`
//! (failed instances and gates awaiting a decision), `rk scan obstacle`, and
//! `rk scan need` — into one ranked triage list. Every row carries the exact
//! `rk` command that resolves it, and its raw source `kind` so the operator can
//! override the ranking. This is pure read-side aggregation over data that
//! already exists: no new storage.

use crate::agents::{AgentRecord, AgentState};
use crate::workflow_exec::{Instance, InstanceStatus};
use rk_core::tuple::Tuple;
use serde::Serialize;
use serde_json::json;

/// Urgency ranks. Higher sorts first. Derived at read time from the source, not
/// stored anywhere. Ordering follows the ticket heuristic:
/// budget_exceeded > failed instance / failed agent > parked gate > obstacle > need.
mod urgency {
    pub const BUDGET_EXCEEDED: u8 = 5;
    pub const FAILED: u8 = 4;
    pub const PARKED_GATE: u8 = 3;
    pub const OBSTACLE: u8 = 2;
    pub const NEED: u8 = 1;
}

/// One row in the operator inbox: something awaiting a human, plus the exact
/// command that resolves it.
#[derive(Debug, Clone, Serialize)]
pub struct InboxItem {
    /// Derived urgency; higher is more urgent. Rows sort by this, descending.
    pub urgency: u8,
    /// Raw source kind, kept visible so the operator can override the ranking.
    pub kind: String,
    /// Subject the row is about (agent name, instance id, or tuple identity).
    pub subject: String,
    /// Isolation scope (usually a repo name).
    pub scope: String,
    /// One-line description of what needs attention.
    pub detail: String,
    /// The exact `rk` command that resolves this row.
    pub action: String,
}

/// Aggregate everything awaiting a human into one ranked list. Pure over its
/// inputs so it can be unit-tested without a running daemon.
pub fn build(
    agents: &[AgentRecord],
    instances: &[Instance],
    obstacles: &[Tuple],
    needs: &[Tuple],
) -> Vec<InboxItem> {
    let mut items = Vec::new();

    // Registry agents that dropped out of their run and need a hand back up.
    for a in agents {
        let (kind, action) = match a.state {
            AgentState::Failed => ("agent-failed", format!("rk respawn {}", a.name)),
            AgentState::Orphaned => ("agent-orphaned", format!("rk respawn {}", a.name)),
            _ => continue,
        };
        let detail = match &a.result {
            Some(r) if !r.is_empty() => format!("{} — {r}", a.task.as_deref().unwrap_or("-")),
            _ => a.task.clone().unwrap_or_else(|| "-".into()),
        };
        items.push(InboxItem {
            urgency: urgency::FAILED,
            kind: kind.into(),
            subject: a.name.clone(),
            scope: a.repo_name.clone(),
            detail,
            action,
        });
    }

    // Workflow instances: failed runs, and runs parked at an approval gate.
    for i in instances {
        if i.status == InstanceStatus::Failed {
            items.push(InboxItem {
                urgency: urgency::FAILED,
                kind: "workflow-failed".into(),
                subject: i.id.clone(),
                scope: repo_name(&i.repo),
                detail: format!(
                    "{} failed: {}",
                    i.workflow,
                    i.error.as_deref().unwrap_or("(no error recorded)")
                ),
                action: format!("rk workflow status {}", i.id),
            });
        } else if i.status == InstanceStatus::Running && i.awaiting.as_deref() == Some("approval") {
            items.push(InboxItem {
                urgency: urgency::PARKED_GATE,
                kind: "workflow-gate".into(),
                subject: i.id.clone(),
                scope: repo_name(&i.repo),
                detail: format!(
                    "{} parked at approval gate (step {})",
                    i.workflow, i.current_step
                ),
                action: format!("rk approve {id}  |  rk reject {id}", id = i.id),
            });
        }
    }

    // Obstacle tuples. Budget-exceeded obstacles jump the queue; every other
    // obstacle rides at the obstacle rank.
    for t in obstacles {
        let obstacle_type = t.payload.get("type").and_then(|v| v.as_str());
        let is_budget_exceeded = obstacle_type == Some("budget_exceeded");
        let urgency = if is_budget_exceeded {
            urgency::BUDGET_EXCEEDED
        } else {
            urgency::OBSTACLE
        };
        let detail = match obstacle_type {
            Some(ty) if ty.starts_with("budget_") => format!(
                "{ty}: ${:.2}, {} tokens",
                t.payload.get("cost_usd").and_then(|v| v.as_f64()).unwrap_or(0.0),
                t.payload.get("tokens").and_then(|v| v.as_u64()).unwrap_or(0),
            ),
            _ => t
                .payload
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or("(obstacle)")
                .to_string(),
        };
        items.push(InboxItem {
            urgency,
            kind: "obstacle".into(),
            subject: t.identity.clone(),
            scope: t.scope.clone(),
            detail,
            action: format!("rk status {}", t.identity),
        });
    }

    // Need tuples: a rat asked the room for help. No single resolving command,
    // so the action inspects the request in context for the operator to route.
    for t in needs {
        let text = t
            .payload
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("(need)");
        items.push(InboxItem {
            urgency: urgency::NEED,
            kind: "need".into(),
            subject: t.identity.clone(),
            scope: t.scope.clone(),
            detail: text.to_string(),
            action: format!("rk scan need {}", t.scope),
        });
    }

    // Most urgent first; a stable sort keeps each source's own order (agents by
    // spawn time, instances by start time, tuples oldest-first) within a rank.
    items.sort_by(|a, b| b.urgency.cmp(&a.urgency));
    items
}

/// Render the inbox as machine-readable JSON.
pub fn to_json(items: &[InboxItem]) -> serde_json::Value {
    json!({ "items": items })
}

/// A repo path's last component — its registered name. Instance `repo` fields
/// hold canonical paths; tuple scopes already hold the bare repo name.
fn repo_name(repo: &str) -> String {
    repo.rsplit(['/', '\\'])
        .find(|s| !s.is_empty())
        .unwrap_or(repo)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rk_core::tuple::{Category, Tuple};
    use rk_harness::TokenUsage;

    fn agent(name: &str, state: AgentState) -> AgentRecord {
        AgentRecord {
            name: name.into(),
            role: "rat".into(),
            harness: "fake".into(),
            model: None,
            repo_root: "/tmp/repo".into(),
            repo_name: "repo".into(),
            task: Some("TKT-9".into()),
            branch: Some(format!("rat/{name}/tkt-9")),
            worktree: Some(format!("/tmp/wt/{name}").into()),
            target_branch: "main".into(),
            parent: None,
            workflow_instance: None,
            session_id: None,
            attach_target: None,
            pid: None,
            state,
            result: None,
            usage: TokenUsage::default(),
            cost_usd: 0.0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn instance(id: &str, status: InstanceStatus, awaiting: Option<&str>) -> Instance {
        Instance {
            id: id.into(),
            workflow: "gated-merge".into(),
            repo: "/home/x/dev/repo".into(),
            status,
            current_step: 2,
            total_steps: 5,
            context: Default::default(),
            error: if status == InstanceStatus::Failed {
                Some("boom".into())
            } else {
                None
            },
            awaiting: awaiting.map(str::to_string),
            instance_max_usd: None,
            definition: "gated-merge".into(),
            params: Default::default(),
            started_at: Utc::now(),
            completed_at: None,
        }
    }

    fn obstacle(identity: &str, payload: serde_json::Value) -> Tuple {
        Tuple::new(Category::Obstacle, "repo", identity, "castle", payload)
    }

    fn need(identity: &str, text: &str) -> Tuple {
        Tuple::new(
            Category::Need,
            "repo",
            identity,
            "castle",
            json!({ "text": text }),
        )
    }

    #[test]
    fn ranks_budget_exceeded_above_everything_else() {
        let agents = vec![agent("Whisker", AgentState::Failed)];
        let instances = vec![
            instance("wf-fail", InstanceStatus::Failed, None),
            instance("wf-gate", InstanceStatus::Running, Some("approval")),
        ];
        let obstacles = vec![obstacle(
            "Nibbles",
            json!({"type": "budget_exceeded", "cost_usd": 4.2, "tokens": 900000}),
        )];
        let needs = vec![need("Scamper", "need a reviewer")];

        let inbox = build(&agents, &instances, &obstacles, &needs);
        let kinds: Vec<&str> = inbox.iter().map(|i| i.kind.as_str()).collect();

        // budget(5) > failed agent/instance(4) > parked gate(3) > need(1).
        assert_eq!(inbox[0].kind, "obstacle");
        assert_eq!(inbox[0].urgency, urgency::BUDGET_EXCEEDED);
        assert_eq!(*kinds.last().unwrap(), "need");
        // Gate row carries both resolving commands.
        let gate = inbox.iter().find(|i| i.kind == "workflow-gate").unwrap();
        assert!(gate.action.contains("rk approve wf-gate"));
        assert!(gate.action.contains("rk reject wf-gate"));
    }

    #[test]
    fn only_actionable_rows_appear() {
        // Running (non-parked) instances, completed instances, and live agents
        // are not awaiting a human and must not show up.
        let agents = vec![
            agent("Live", AgentState::Running),
            agent("Gone", AgentState::Orphaned),
        ];
        let instances = vec![
            instance("wf-run", InstanceStatus::Running, None),
            instance("wf-ok", InstanceStatus::Completed, None),
        ];
        let inbox = build(&agents, &instances, &[], &[]);
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].subject, "Gone");
        assert_eq!(inbox[0].action, "rk respawn Gone");
    }

    #[test]
    fn plain_obstacle_uses_text_and_obstacle_rank() {
        let obstacles = vec![obstacle("Pip", json!({"text": "merge conflict in lib.rs"}))];
        let inbox = build(&[], &[], &obstacles, &[]);
        assert_eq!(inbox[0].urgency, urgency::OBSTACLE);
        assert_eq!(inbox[0].detail, "merge conflict in lib.rs");
        assert_eq!(inbox[0].action, "rk status Pip");
    }
}

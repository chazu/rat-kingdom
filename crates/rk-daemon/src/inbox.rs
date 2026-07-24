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
use std::collections::HashSet;

/// Urgency ranks. Higher sorts first. Derived at read time from the source, not
/// stored anywhere. Ordering follows the ticket heuristic:
/// budget_exceeded > failed instance / failed agent > parked gate > obstacle > need.
mod urgency {
    pub const BUDGET_EXCEEDED: u8 = 5;
    pub const FAILED: u8 = 4;
    pub const PARKED_GATE: u8 = 3;
    /// A pushed branch with an open PR/MR awaiting a human review+merge on the
    /// forge. Co-ranked with a parked gate — both are pushed work blocked on a
    /// human decision — and above passive obstacles.
    pub const AWAITING_REVIEW: u8 = 3;
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
    pull_requests: &[Tuple],
    pull_requests_closed: &[Tuple],
    cleared_prs: &HashSet<(String, String)>,
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

    // Open pull/merge requests: a PR-mode `dismiss`/`land` pushed a branch and
    // opened a PR (a `pull_request_opened` event), then completed — so nothing
    // else in this queue tracks it. Surface each so a pushed branch is visible
    // attention, never silently forgotten, carrying the forge URL to review it.
    // Dedup by (scope, branch): a re-land emits a fresh event for the same
    // branch, and only the newest matters. `pull_requests` arrives oldest-first
    // (scan order), so a later event overwrites an earlier one for its branch.
    // `cleared_prs` names (scope, branch) pairs whose branch has since been
    // merged into its target or deleted (computed against local git by the
    // caller); those rows have auto-cleared and are dropped, so a merged PR
    // stops nagging without waiting for its event to be pruned from the store.
    //
    // `pull_requests_closed` are `pull_request_closed` events emitted by the
    // fetch-driven review sweep (TKT-70): a background pass fetched the forge
    // and saw the branch merged/deleted upstream even though the operator never
    // pulled, so the LOCAL `cleared_prs` check could not see it. Fold their
    // (scope, branch) into the same suppression — a closed event clears the row.
    let mut suppressed: HashSet<(String, String)> = cleared_prs.clone();
    for t in pull_requests_closed {
        if let Some(branch) = t.payload.get("branch").and_then(|v| v.as_str()) {
            suppressed.insert((t.scope.clone(), branch.to_string()));
        }
    }
    let mut latest_pr: std::collections::HashMap<(String, String), &Tuple> =
        std::collections::HashMap::new();
    for t in pull_requests {
        let branch = t
            .payload
            .get("branch")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let key = (t.scope.clone(), branch);
        if suppressed.contains(&key) {
            continue;
        }
        latest_pr.insert(key, t);
    }
    // Deterministic order: newest PR first (event ids are time-sortable).
    let mut prs: Vec<&Tuple> = latest_pr.into_values().collect();
    prs.sort_by(|a, b| b.id.cmp(&a.id));
    for t in prs {
        let branch = t.payload.get("branch").and_then(|v| v.as_str());
        let target = t.payload.get("target").and_then(|v| v.as_str());
        let url = t.payload.get("url").and_then(|v| v.as_str());
        let detail = match (branch, target) {
            (Some(b), Some(tg)) => format!("PR open: {b} → {tg}{}", url_suffix(url)),
            (Some(b), None) => format!("PR open for {b}{}", url_suffix(url)),
            _ => format!("PR open{}", url_suffix(url)),
        };
        // The resolving action is to review + merge on the forge; the URL is the
        // one thing the operator needs. Fall back to the branch when unknown.
        let action = match url {
            Some(u) => format!("review & merge: {u}"),
            None => format!(
                "review & merge branch {} on the forge",
                branch.unwrap_or("(unknown)")
            ),
        };
        items.push(InboxItem {
            urgency: urgency::AWAITING_REVIEW,
            kind: "awaiting-review".into(),
            subject: branch.unwrap_or(&t.identity).to_string(),
            scope: t.scope.clone(),
            detail,
            action,
        });
    }

    // Most urgent first; a stable sort keeps each source's own order (agents by
    // spawn time, instances by start time, tuples oldest-first) within a rank.
    items.sort_by(|a, b| b.urgency.cmp(&a.urgency));
    items
}

/// Render an optional PR URL as a ` (<url>)` suffix, or empty when absent.
fn url_suffix(url: Option<&str>) -> String {
    url.map(|u| format!(" ({u})")).unwrap_or_default()
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
            merge_commit: None,
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

    fn pull_request(branch: &str, target: &str, url: Option<&str>) -> Tuple {
        Tuple::new(
            Category::Event,
            "repo",
            "pull_request_opened",
            "castle",
            json!({ "branch": branch, "target": target, "url": url }),
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

        let inbox = build(&agents, &instances, &obstacles, &needs, &[], &[], &HashSet::new());
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
        let inbox = build(&agents, &instances, &[], &[], &[], &[], &HashSet::new());
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].subject, "Gone");
        assert_eq!(inbox[0].action, "rk respawn Gone");
    }

    #[test]
    fn open_pr_surfaces_as_awaiting_review_with_url() {
        let prs = vec![pull_request(
            "rat/rat-9/tkt-9",
            "main",
            Some("https://forge/x/y/compare/main...rat/rat-9/tkt-9"),
        )];
        let inbox = build(&[], &[], &[], &[], &prs, &[], &HashSet::new());
        assert_eq!(inbox.len(), 1);
        let row = &inbox[0];
        assert_eq!(row.kind, "awaiting-review");
        assert_eq!(row.urgency, urgency::AWAITING_REVIEW);
        assert_eq!(row.subject, "rat/rat-9/tkt-9");
        assert!(row.detail.contains("rat/rat-9/tkt-9 → main"));
        assert!(row.detail.contains("https://forge/x/y/compare"));
        assert!(row.action.contains("review & merge: https://forge/"));
    }

    #[test]
    fn open_pr_dedups_by_branch_keeping_newest() {
        // A re-land emits a second event for the same branch; only the newest
        // should surface, as one row. Events arrive oldest-first (scan order),
        // so the last entry for a branch wins.
        let older = pull_request("rat/rat-9/tkt-9", "main", Some("https://forge/pr/1"));
        let newer = pull_request("rat/rat-9/tkt-9", "main", Some("https://forge/pr/2"));
        let inbox = build(&[], &[], &[], &[], &[older, newer], &[], &HashSet::new());
        let review: Vec<&InboxItem> = inbox
            .iter()
            .filter(|i| i.kind == "awaiting-review")
            .collect();
        assert_eq!(review.len(), 1);
        assert!(review[0].detail.contains("https://forge/pr/2"));
    }

    #[test]
    fn merged_or_gone_pr_auto_clears_from_the_queue() {
        // A branch the caller has resolved as merged/gone (its (scope, branch)
        // is in `cleared_prs`) drops out entirely, even though its
        // `pull_request_opened` event is still in the store. A second, still-open
        // PR on a different branch survives, so clearing is per-branch.
        let merged = pull_request("rat/rat-9/tkt-9", "main", Some("https://forge/pr/1"));
        let still_open = pull_request("rat/rat-8/tkt-8", "main", Some("https://forge/pr/2"));
        let mut cleared = HashSet::new();
        cleared.insert(("repo".to_string(), "rat/rat-9/tkt-9".to_string()));

        let inbox = build(&[], &[], &[], &[], &[merged, still_open], &[], &cleared);
        let review: Vec<&InboxItem> = inbox
            .iter()
            .filter(|i| i.kind == "awaiting-review")
            .collect();
        assert_eq!(review.len(), 1);
        assert_eq!(review[0].subject, "rat/rat-8/tkt-8");
    }

    fn pull_request_closed(branch: &str) -> Tuple {
        Tuple::new(
            Category::Event,
            "repo",
            "pull_request_closed",
            "castle",
            json!({ "branch": branch, "target": "main", "reason": "merged" }),
        )
    }

    #[test]
    fn pull_request_closed_event_clears_the_row_without_a_local_pull() {
        // The fetch-driven sweep (TKT-70) saw a forge merge the operator never
        // pulled and emitted a `pull_request_closed` event. Even with the
        // `pull_request_opened` event still present and NOTHING in `cleared_prs`
        // (the local check cannot see the un-pulled merge), the row is dropped.
        let merged = pull_request("rat/rat-9/tkt-9", "main", Some("https://forge/pr/1"));
        let still_open = pull_request("rat/rat-8/tkt-8", "main", Some("https://forge/pr/2"));
        let closed = vec![pull_request_closed("rat/rat-9/tkt-9")];

        let inbox = build(
            &[],
            &[],
            &[],
            &[],
            &[merged, still_open],
            &closed,
            &HashSet::new(),
        );
        let review: Vec<&InboxItem> = inbox
            .iter()
            .filter(|i| i.kind == "awaiting-review")
            .collect();
        assert_eq!(review.len(), 1);
        assert_eq!(review[0].subject, "rat/rat-8/tkt-8");
    }

    #[test]
    fn plain_obstacle_uses_text_and_obstacle_rank() {
        let obstacles = vec![obstacle("Pip", json!({"text": "merge conflict in lib.rs"}))];
        let inbox = build(&[], &[], &obstacles, &[], &[], &[], &HashSet::new());
        assert_eq!(inbox[0].urgency, urgency::OBSTACLE);
        assert_eq!(inbox[0].detail, "merge conflict in lib.rs");
        assert_eq!(inbox[0].action, "rk status Pip");
    }
}

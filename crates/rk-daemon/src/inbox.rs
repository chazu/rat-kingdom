//! Unified operator attention queue.
//!
//! `rk inbox` collapses the surfaces an operator otherwise polls separately —
//! `rk list` (failed/orphaned agents), `rk workflow list` (failed instances and
//! gates awaiting a decision), `rk scan obstacle`, `rk scan need`, and
//! `rk scan suggestion` — into one ranked triage list. Every row carries the
//! exact `rk` command that resolves it, and its raw source `kind` so the
//! operator can override the ranking. This is pure read-side aggregation over
//! data that already exists: no new storage.
//!
//! Two of the sources are INVARIANT ASSERTIONS rather than reports of something
//! that announced itself — a pushed branch with an open PR nobody merged
//! (`awaiting-review`), and a `land` that reported success-with-`merged: false`
//! and left its branch outside the target (`unlanded-branch`). Both describe
//! finished work that is absent from where it belongs and that no failure
//! anywhere would have named. Both are checked against local git on every read
//! and clear themselves the moment the branch actually lands.
//!
//! One source is a BALLOT rather than a problem: an open `Suggestion` inside its
//! voting window (`open-suggestion`). It is here because a proposal that nobody
//! votes on is indistinguishable from one nobody made — it simply decays — and
//! the operator is the one endorser who is always available.

use crate::agents::{AgentRecord, AgentState};
use crate::workflow_exec::{Instance, InstanceStatus};
use chrono::{DateTime, Utc};
use rk_core::tuple::{Category, Tuple, SYSTEM_SCOPE};
use serde::Serialize;
use serde_json::json;
use std::collections::{HashMap, HashSet};

/// Urgency ranks. Higher sorts first. Derived at read time from the source, not
/// stored anywhere. Ordering follows the ticket heuristic:
/// budget_exceeded > failed instance / failed agent > parked gate > obstacle > need.
mod urgency {
    pub const BUDGET_EXCEEDED: u8 = 5;
    pub const FAILED: u8 = 4;
    /// A `land` that neither merged nor opened a PR, whose branch is still
    /// standing unmerged. Co-ranked with a failure: the work is finished and
    /// reviewed but absent from the target, and the cost of leaving it there
    /// grows with every commit the target advances (TKT-171).
    pub const UNLANDED: u8 = 4;
    pub const PARKED_GATE: u8 = 3;
    /// A pushed branch with an open PR/MR awaiting a human review+merge on the
    /// forge. Co-ranked with a parked gate — both are pushed work blocked on a
    /// human decision — and above passive obstacles.
    pub const AWAITING_REVIEW: u8 = 3;
    pub const OBSTACLE: u8 = 2;
    pub const NEED: u8 = 1;
    /// An open proposal awaiting endorsements. Co-ranked with `need` — both are
    /// a rat asking the room for something rather than reporting a problem, and
    /// neither blocks anything. It earns a row at all because it EXPIRES: unlike
    /// a need, which someone may answer late, a suggestion that misses quorum
    /// inside its voting window is gone and the norm with it (TKT-167).
    pub const OPEN_SUGGESTION: u8 = 1;
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

/// The branch-shaped inputs: events about work that was pushed or landed, plus
/// the git-backed answer to "has it reached its target yet?".
///
/// These travel together because they are one mechanism. Each names a
/// `{branch, target}`, each describes finished work that may or may not have
/// arrived where it belongs, and each row derived from them is retired by the
/// same question asked of local git — which is why one `cleared` set covers all
/// of them.
#[derive(Debug, Default, Clone)]
pub struct BranchEvents<'a> {
    /// `pull_request_opened` — a PR-mode land/dismiss pushed a branch and opened
    /// a request nobody has merged yet (TKT-67).
    pub pull_requests: &'a [Tuple],
    /// `pull_request_closed` — the fetch-driven sweep saw the forge merge or
    /// delete a branch the operator never pulled (TKT-70).
    pub pull_requests_closed: &'a [Tuple],
    /// `branch_landed` — every `land` step's own outcome. The ones reporting
    /// neither a merge nor an opened PR are dropped branches (TKT-171).
    pub lands: &'a [Tuple],
    /// (scope, branch) pairs the caller has resolved against local git as
    /// merged-into-their-target or gone. Suppresses rows from BOTH sources — an
    /// open PR the human merged on the forge, and a dropped `land` whose branch
    /// has since landed by any route.
    pub cleared: HashSet<(String, String)>,
}

/// The suggestion ballots: open proposals, the votes cast on them, and the
/// promotions that have already settled.
///
/// `rk suggest` writes a `Suggestion` on the system scope with a voting window
/// (default 24h). The reactor promotes it to a permanent `Convention` once
/// `quorum` DISTINCT agents have endorsed it; otherwise it decays and the norm
/// is lost. Nothing announces an open ballot, so a proposal only ever promotes
/// if a peer happens to go looking for one it has no reason to suspect exists —
/// measured 2026-07-25, ZERO conventions had ever reached quorum over 277
/// spawns, three separate rats having tried and failed to gather three votes
/// (TKT-167). These rows are the announcement, and they put the one endorser who
/// is always reachable — the operator — in front of every ballot before it
/// decays.
#[derive(Debug, Clone)]
pub struct Ballots<'a> {
    /// `Suggestion` tuples: the open proposals, keyed `identity = <sug-id>`.
    pub suggestions: &'a [Tuple],
    /// `Endorsement` tuples, keyed `(identity = <sug-id>, instance = endorser)`.
    /// Counted DISTINCT by `instance`, exactly as the reactor counts them, so
    /// the tally shown is the tally that promotes.
    pub endorsements: &'a [Tuple],
    /// `Convention` tuples, keyed `identity = <sug-id>`. A promoted proposal is
    /// settled and must never nag; the permanent Convention is the same
    /// already-promoted marker the reactor itself uses.
    pub conventions: &'a [Tuple],
    /// Distinct-endorser count at which the reactor promotes. Zero disables
    /// quorum promotion, and a ballot that can never resolve is not a ballot —
    /// no rows are raised.
    pub quorum: usize,
    /// Read-time clock, injected rather than read here so `build` stays pure
    /// over its inputs and the remaining-window rendering is testable.
    pub now: DateTime<Utc>,
}

impl Default for Ballots<'_> {
    fn default() -> Self {
        Self {
            suggestions: &[],
            endorsements: &[],
            conventions: &[],
            // Matches the reactor's "promotion disabled" reading. Safe as a
            // default because it can only suppress rows, never invent them.
            quorum: 0,
            now: Utc::now(),
        }
    }
}

/// Aggregate everything awaiting a human into one ranked list. Pure over its
/// inputs so it can be unit-tested without a running daemon.
pub fn build(
    agents: &[AgentRecord],
    instances: &[Instance],
    obstacles: &[Tuple],
    needs: &[Tuple],
    branches: &BranchEvents<'_>,
    ballots: &Ballots<'_>,
) -> Vec<InboxItem> {
    let (pull_requests, pull_requests_closed, lands) = (
        branches.pull_requests,
        branches.pull_requests_closed,
        branches.lands,
    );
    let cleared_branches = &branches.cleared;
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
    let mut suppressed: HashSet<(String, String)> = cleared_branches.clone();
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
    prs.sort_by_key(|b| std::cmp::Reverse(b.id));
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

    // Dropped lands (TKT-171). Every `land` step emits a `branch_landed` event
    // carrying its own outcome, and a land that neither merged nor opened a PR
    // left the branch standing OUTSIDE the target — reviewed work that is simply
    // absent. `land` reports that as a clean `{merged: false}` rather than an
    // error (by design: it lets a workflow gate and retry), so nothing else in
    // this queue tracks it. Whether the drop surfaces at all has until now
    // depended on the workflow DEFINITION carrying an
    // `evaluate {expect: {merged: true}}` after its `land` — and a definition is
    // a file that can be stale, forked per repo, or hand-edited. TKT-147 is what
    // that costs: a steward completed cleanly on `{merged: false}` and the fix
    // sat off main for two days. Asserting it here instead makes the invariant
    // hold for every workflow, including the ones that forgot the gate.
    //
    // `cleared_branches` (the same caller-computed set the PR rows use) holds
    // the branches git says have since merged into their target or gone — the
    // `git merge-base --is-ancestor <branch> <target>` assertion. That makes the
    // row SELF-CLEARING: a hand-merge, a cherry-pick that the operator then
    // deletes the branch for, or any later land retires it without anything
    // having to write a "resolved" record.
    for t in dropped_lands(lands) {
        let branch = t
            .payload
            .get("branch")
            .and_then(|v| v.as_str())
            .unwrap_or("(unknown)");
        if cleared_branches.contains(&(t.scope.clone(), branch.to_string())) {
            continue;
        }
        let target = t
            .payload
            .get("target")
            .and_then(|v| v.as_str())
            .unwrap_or("main");
        let why = t
            .payload
            .get("detail")
            .and_then(|v| v.as_str())
            .filter(|d| !d.trim().is_empty())
            .unwrap_or("no detail recorded");
        items.push(InboxItem {
            urgency: urgency::UNLANDED,
            kind: "unlanded-branch".into(),
            subject: branch.to_string(),
            scope: t.scope.clone(),
            detail: format!("land did not merge {branch} → {target}: {why}"),
            // No `rk` verb lands a named branch, so name the git that does. The
            // row also clears if the operator decides the branch is redundant
            // and deletes it.
            action: format!("git checkout {target} && git merge {branch}"),
        });
    }

    items.extend(open_suggestions(ballots));

    // Most urgent first; a stable sort keeps each source's own order (agents by
    // spawn time, instances by start time, tuples oldest-first) within a rank.
    items.sort_by_key(|b| std::cmp::Reverse(b.urgency));
    items
}

/// One row per open ballot: a system-scope `Suggestion` still inside its voting
/// window that has neither promoted nor decayed (TKT-167).
///
/// Three filters, each dropping a ballot the operator cannot usefully act on:
///
/// - **quorum 0** — promotion is disabled, so no endorsement can resolve it.
/// - **already promoted** — a `Convention` carries the suggestion's id, so the
///   norm is permanent and the vote is over.
/// - **decayed** — `expires_at` has passed. Expiry is collected by the GC rather
///   than filtered on read, so a scan returns ballots whose window closed
///   minutes ago; endorsing one is not what the operator meant.
///
/// Scoped to `system`, matching the reactor: `promote_conventions` only ever
/// considers system-scope tuples, so a suggestion written anywhere else could
/// not promote no matter who endorsed it, and showing it would offer a vote that
/// does nothing. `rk suggest` always writes system scope.
fn open_suggestions(ballots: &Ballots<'_>) -> Vec<InboxItem> {
    if ballots.quorum == 0 || ballots.suggestions.is_empty() {
        return Vec::new();
    }
    let mut endorsers: HashMap<&str, HashSet<&str>> = HashMap::new();
    for t in ballots.endorsements {
        if t.scope == SYSTEM_SCOPE && t.category == Category::Endorsement {
            endorsers
                .entry(t.identity.as_str())
                .or_default()
                .insert(t.instance.as_str());
        }
    }
    let promoted: HashSet<&str> = ballots
        .conventions
        .iter()
        .filter(|t| t.scope == SYSTEM_SCOPE && t.category == Category::Convention)
        .map(|t| t.identity.as_str())
        .collect();

    let mut open: Vec<&Tuple> = ballots
        .suggestions
        .iter()
        .filter(|t| t.scope == SYSTEM_SCOPE && !promoted.contains(t.identity.as_str()))
        .filter(|t| t.expires_at.is_none_or(|e| e > ballots.now))
        .collect();
    // Closest to decaying first, so the ballot the operator is about to lose is
    // the one they read. A ballot with no window cannot decay and sorts last;
    // ties fall back to newest-first on the time-sortable id.
    open.sort_by(|a, b| {
        a.expires_at
            .unwrap_or(DateTime::<Utc>::MAX_UTC)
            .cmp(&b.expires_at.unwrap_or(DateTime::<Utc>::MAX_UTC))
            .then_with(|| b.id.cmp(&a.id))
    });

    let mut rows = Vec::new();
    for t in open {
        let count = endorsers
            .get(t.identity.as_str())
            .map(HashSet::len)
            .unwrap_or(0);
        let text = t
            .payload
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("(no text)");
        // The proposer is one of the endorsers it needs, so name them: an
        // operator reading two near-duplicate ballots needs to know who is
        // asking before deciding which one to back.
        let by = t
            .payload
            .get("agent")
            .and_then(|v| v.as_str())
            .unwrap_or(t.instance.as_str());
        rows.push(InboxItem {
            urgency: urgency::OPEN_SUGGESTION,
            kind: "open-suggestion".into(),
            subject: t.identity.clone(),
            scope: t.scope.clone(),
            detail: format!(
                "{count}/{} endorsers{} — {by} proposes: {text}",
                ballots.quorum,
                window_left(t.expires_at, ballots.now),
            ),
            action: format!("rk endorse {}", t.identity),
        });
    }
    rows
}

/// Render how long a ballot has left as a ` (6h12m left)` clause, or empty when
/// it carries no voting window at all. Sub-minute remainders round to `<1m`
/// rather than `0m`, which would read as decayed.
fn window_left(expires_at: Option<DateTime<Utc>>, now: DateTime<Utc>) -> String {
    let Some(expires_at) = expires_at else {
        return String::new();
    };
    let mins = (expires_at - now).num_minutes();
    if mins < 1 {
        return " (<1m left)".into();
    }
    match (mins / 60, mins % 60) {
        (0, m) => format!(" ({m}m left)"),
        (h, m) => format!(" ({h}h{m:02}m left)"),
    }
}

/// The `branch_landed` events that describe a DROPPED branch: the newest event
/// per (scope, branch), keeping only those where the land neither merged nor
/// opened a PR, newest first.
///
/// Newest-wins is what retires a retry: a successful re-land emits a later
/// `{merged: true}` event for the same branch, which replaces the failed one.
/// Events arrive oldest-first (scan order), so the last entry for a branch wins.
///
/// Public because the caller runs a git query per branch to decide which rows
/// have auto-cleared, and that query is a subprocess: it must run over these —
/// a handful, and self-limiting since resolving one removes it — rather than
/// over every land the fleet has ever performed.
pub fn dropped_lands(lands: &[Tuple]) -> Vec<&Tuple> {
    let mut latest: std::collections::HashMap<(String, String), &Tuple> =
        std::collections::HashMap::new();
    for t in lands {
        let Some(branch) = t.payload.get("branch").and_then(|v| v.as_str()) else {
            continue;
        };
        latest.insert((t.scope.clone(), branch.to_string()), t);
    }
    let mut dropped: Vec<&Tuple> = latest
        .into_values()
        // A merge, or a PR-mode land that pushed and opened a request, is a
        // clean hand-off. Only the both-false outcome is a dropped branch.
        // `pr_opened` is absent on events written before PR mode existed
        // (TKT-67), which reads as false — the right default for a Direct-mode
        // land that did not merge.
        .filter(|t| !flag(t, "merged") && !flag(t, "pr_opened"))
        .collect();
    // Deterministic order: newest first (event ids are time-sortable).
    dropped.sort_by_key(|b| std::cmp::Reverse(b.id));
    dropped
}

/// Read a boolean outcome flag off an event payload. A missing or non-boolean
/// field is `false` — events written before a flag existed must not read as if
/// the outcome it names had happened.
fn flag(t: &Tuple, field: &str) -> bool {
    t.payload
        .get(field)
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
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
            crashed: false,
            result: None,
            usage: TokenUsage::default(),
            cost_usd: 0.0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            archived_at: None,
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
            definition_digest: String::new(),
            params: Default::default(),
            depth: 0,
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

        let inbox = build(
            &agents,
            &instances,
            &obstacles,
            &needs,
            &BranchEvents::default(),
            &Ballots::default(),
        );
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
        let inbox = build(
            &agents,
            &instances,
            &[],
            &[],
            &BranchEvents::default(),
            &Ballots::default(),
        );
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
        let inbox = build(
            &[],
            &[],
            &[],
            &[],
            &BranchEvents {
                pull_requests: &prs,
                ..Default::default()
            },
            &Ballots::default(),
        );
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
        let inbox = build(
            &[],
            &[],
            &[],
            &[],
            &BranchEvents {
                pull_requests: &[older, newer],
                ..Default::default()
            },
            &Ballots::default(),
        );
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

        let inbox = build(
            &[],
            &[],
            &[],
            &[],
            &BranchEvents {
                pull_requests: &[merged, still_open],
                cleared,
                ..Default::default()
            },
            &Ballots::default(),
        );
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
            &BranchEvents {
                pull_requests: &[merged, still_open],
                pull_requests_closed: &closed,
                ..Default::default()
            },
            &Ballots::default(),
        );
        let review: Vec<&InboxItem> = inbox
            .iter()
            .filter(|i| i.kind == "awaiting-review")
            .collect();
        assert_eq!(review.len(), 1);
        assert_eq!(review[0].subject, "rat/rat-8/tkt-8");
    }

    fn land(branch: &str, merged: bool, pr_opened: bool, detail: &str) -> Tuple {
        Tuple::new(
            Category::Event,
            "repo",
            "branch_landed",
            "castle",
            json!({
                "branch": branch,
                "target": "main",
                "merged": merged,
                "pr_opened": pr_opened,
                "detail": detail,
            }),
        )
    }

    #[test]
    fn a_land_that_did_not_merge_surfaces_as_a_dropped_branch() {
        // TKT-171: `land` reports a conflict as a clean `{merged: false}`, not
        // an error, so a workflow whose definition lacks the post-land
        // `evaluate` completes as if the work landed. The branch is left
        // outside main with nothing naming it. Assert it here instead.
        let lands = vec![land(
            "rat/dusty-2/steward-review-tkt-147",
            false,
            false,
            "merge conflict or failure: CONFLICT (content): Merge conflict in lib.rs",
        )];
        let inbox = build(
            &[],
            &[],
            &[],
            &[],
            &BranchEvents {
                lands: &lands,
                ..Default::default()
            },
            &Ballots::default(),
        );
        assert_eq!(inbox.len(), 1);
        let row = &inbox[0];
        assert_eq!(row.kind, "unlanded-branch");
        // Ranked with the failures: dropped reviewed work, not a passive note.
        assert_eq!(row.urgency, urgency::UNLANDED);
        assert_eq!(row.subject, "rat/dusty-2/steward-review-tkt-147");
        assert!(row
            .detail
            .contains("rat/dusty-2/steward-review-tkt-147 → main"));
        // The reason git gave must reach the operator, not just "it failed".
        assert!(row.detail.contains("Merge conflict in lib.rs"));
        assert!(row
            .action
            .contains("git merge rat/dusty-2/steward-review-tkt-147"));
    }

    #[test]
    fn a_clean_land_hand_off_raises_no_row() {
        // Both success shapes are clean: a Direct-mode merge, and a PR-mode
        // land that pushed and opened a request (already tracked by its own
        // awaiting-review row — this source must not double-count it).
        let lands = vec![
            land("rat/a/merged", true, false, "merged rat/a/merged into main"),
            land("rat/b/pushed", false, true, "opened MR"),
        ];
        let inbox = build(
            &[],
            &[],
            &[],
            &[],
            &BranchEvents {
                lands: &lands,
                ..Default::default()
            },
            &Ballots::default(),
        );
        assert!(inbox.is_empty(), "clean hand-offs must not nag: {inbox:?}");
    }

    #[test]
    fn a_land_event_predating_pr_mode_still_surfaces() {
        // Events written before `pr_opened` existed (TKT-67) omit the field.
        // A missing flag must read as false, or every historical dropped land
        // would be silently reclassified as a clean PR hand-off.
        let old = Tuple::new(
            Category::Event,
            "repo",
            "branch_landed",
            "castle",
            json!({"branch": "rat/filch/steward-review-tkt-18", "target": "main", "merged": false}),
        );
        let inbox = build(
            &[],
            &[],
            &[],
            &[],
            &BranchEvents {
                lands: &[old],
                ..Default::default()
            },
            &Ballots::default(),
        );
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].kind, "unlanded-branch");
    }

    #[test]
    fn a_successful_re_land_retires_the_dropped_row() {
        // Newest event per branch wins: the retry merged, so the branch is no
        // longer dropped even though the failed event is still in the store.
        // Events arrive oldest-first (scan order).
        let failed = land("rat/a/tkt-9", false, false, "conflict");
        let retried = land("rat/a/tkt-9", true, false, "merged rat/a/tkt-9 into main");
        let inbox = build(
            &[],
            &[],
            &[],
            &[],
            &BranchEvents {
                lands: &[failed, retried],
                ..Default::default()
            },
            &Ballots::default(),
        );
        assert!(
            inbox.is_empty(),
            "a successful re-land must clear it: {inbox:?}"
        );
    }

    #[test]
    fn a_hand_merged_branch_clears_without_any_record_of_the_merge() {
        // The common resolution: a human merges (or cherry-picks and deletes)
        // the branch. Nothing writes a "resolved" record for that, so the row
        // must clear off the caller's git check alone. A second still-dropped
        // branch survives, so clearing is per-branch.
        let stuck = land("rat/a/still-stuck", false, false, "conflict");
        let fixed = land("rat/b/hand-merged", false, false, "conflict");
        let mut cleared = HashSet::new();
        cleared.insert(("repo".to_string(), "rat/b/hand-merged".to_string()));

        let inbox = build(
            &[],
            &[],
            &[],
            &[],
            &BranchEvents {
                lands: &[stuck, fixed],
                cleared,
                ..Default::default()
            },
            &Ballots::default(),
        );
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].subject, "rat/a/still-stuck");
    }

    fn suggestion(sug_id: &str, by: &str, text: &str, expires_in_mins: Option<i64>) -> Tuple {
        let mut t = Tuple::new(
            Category::Suggestion,
            SYSTEM_SCOPE,
            sug_id,
            by,
            json!({ "agent": by, "text": text }),
        );
        t.expires_at = expires_in_mins.map(|m| Utc::now() + chrono::Duration::minutes(m));
        t
    }

    fn endorsement(sug_id: &str, by: &str) -> Tuple {
        Tuple::new(
            Category::Endorsement,
            SYSTEM_SCOPE,
            sug_id,
            by,
            json!({ "agent": by, "suggestion": sug_id }),
        )
    }

    fn ballots<'a>(suggestions: &'a [Tuple], endorsements: &'a [Tuple]) -> Ballots<'a> {
        Ballots {
            suggestions,
            endorsements,
            quorum: 3,
            ..Default::default()
        }
    }

    #[test]
    fn an_open_ballot_surfaces_with_its_tally_window_and_endorse_command() {
        // TKT-167: nothing else announces that a vote is open, so a proposal
        // decays unendorsed. The row must carry enough to decide on: who is
        // asking, what they propose, how far from quorum, and how long is left.
        let suggestions = vec![suggestion(
            "sug-8nsqa4132x",
            "rat-28",
            "a pre-existing failure is a ticket, not an inline fix",
            Some(6 * 60 + 12),
        )];
        let endorsements = vec![endorsement("sug-8nsqa4132x", "rat-36")];
        let inbox = build(
            &[],
            &[],
            &[],
            &[],
            &BranchEvents::default(),
            &ballots(&suggestions, &endorsements),
        );
        assert_eq!(inbox.len(), 1);
        let row = &inbox[0];
        assert_eq!(row.kind, "open-suggestion");
        assert_eq!(row.urgency, urgency::OPEN_SUGGESTION);
        assert_eq!(row.subject, "sug-8nsqa4132x");
        assert_eq!(row.scope, SYSTEM_SCOPE);
        assert!(
            row.detail.starts_with("1/3 endorsers (6h1"),
            "{}",
            row.detail
        );
        assert!(row
            .detail
            .contains("rat-28 proposes: a pre-existing failure"));
        assert_eq!(row.action, "rk endorse sug-8nsqa4132x");
    }

    #[test]
    fn a_settled_or_decayed_ballot_stops_nagging() {
        // Three ballots the operator cannot usefully act on: one already promoted
        // to a permanent Convention (the vote is over), one whose voting window
        // closed before the GC collected it, and one written outside the system
        // scope — which `promote_conventions` never considers, so endorsing it
        // could not promote it. Only the live system-scope ballot survives.
        let mut off_scope = suggestion("sug-elsewhere", "rat-9", "repo-local idea", Some(60));
        off_scope.scope = "repo".into();
        let suggestions = vec![
            suggestion("sug-promoted", "rat-1", "already a norm", Some(60)),
            suggestion("sug-decayed", "rat-2", "missed its window", Some(-1)),
            off_scope,
            suggestion("sug-live", "rat-3", "still open", Some(60)),
        ];
        let conventions = vec![Tuple::new(
            Category::Convention,
            SYSTEM_SCOPE,
            "sug-promoted",
            "reactor",
            json!({"text": "already a norm", "count": 3}),
        )];
        let inbox = build(
            &[],
            &[],
            &[],
            &[],
            &BranchEvents::default(),
            &Ballots {
                suggestions: &suggestions,
                conventions: &conventions,
                quorum: 3,
                ..Default::default()
            },
        );
        assert_eq!(inbox.len(), 1, "{inbox:?}");
        assert_eq!(inbox[0].subject, "sug-live");
        assert!(inbox[0].detail.starts_with("0/3 endorsers"));
    }

    #[test]
    fn endorsers_are_counted_distinct_and_per_ballot() {
        // The tally shown must be the tally that promotes: the reactor counts
        // DISTINCT `instance` per suggestion, so a re-endorsement cannot inflate
        // it and another ballot's votes cannot leak in.
        let suggestions = vec![
            suggestion("sug-a", "rat-1", "proposal a", Some(60)),
            suggestion("sug-b", "rat-2", "proposal b", Some(120)),
        ];
        let endorsements = vec![
            endorsement("sug-a", "rat-7"),
            endorsement("sug-a", "rat-7"),
            endorsement("sug-a", "rat-8"),
            endorsement("sug-b", "rat-9"),
        ];
        let inbox = build(
            &[],
            &[],
            &[],
            &[],
            &BranchEvents::default(),
            &ballots(&suggestions, &endorsements),
        );
        // Closest to decaying first, so the ballot about to be lost reads first.
        assert_eq!(inbox[0].subject, "sug-a");
        assert!(
            inbox[0].detail.starts_with("2/3 endorsers"),
            "{:?}",
            inbox[0]
        );
        assert_eq!(inbox[1].subject, "sug-b");
        assert!(
            inbox[1].detail.starts_with("1/3 endorsers"),
            "{:?}",
            inbox[1]
        );
    }

    #[test]
    fn ballots_never_outrank_a_real_problem() {
        // A proposal is not a failure. It sits at the bottom of the queue with
        // the needs, below every obstacle and every dropped branch.
        let suggestions = vec![suggestion("sug-a", "rat-1", "proposal a", Some(60))];
        let obstacles = vec![obstacle("Pip", json!({"text": "merge conflict"}))];
        let inbox = build(
            &[agent("Whisker", AgentState::Failed)],
            &[],
            &obstacles,
            &[],
            &BranchEvents::default(),
            &ballots(&suggestions, &[]),
        );
        assert_eq!(*inbox.last().unwrap().kind, *"open-suggestion");
        assert!(inbox[0].urgency > inbox.last().unwrap().urgency);
    }

    #[test]
    fn quorum_zero_raises_no_ballots() {
        // Promotion disabled: no number of endorsements can resolve the ballot,
        // so offering the vote would be a lie.
        let suggestions = vec![suggestion("sug-a", "rat-1", "proposal a", Some(60))];
        let inbox = build(
            &[],
            &[],
            &[],
            &[],
            &BranchEvents::default(),
            &Ballots {
                suggestions: &suggestions,
                ..Default::default()
            },
        );
        assert!(inbox.is_empty(), "{inbox:?}");
    }

    #[test]
    fn a_ballot_in_its_last_seconds_reads_as_still_open() {
        // Sub-minute remainders must not render as `0m left`, which reads as
        // decayed — the operator has seconds to save the norm, not none.
        let suggestions = vec![suggestion("sug-a", "rat-1", "proposal a", None)];
        let now = Utc::now();
        let mut expiring = suggestions.clone();
        expiring[0].expires_at = Some(now + chrono::Duration::seconds(20));
        let inbox = build(
            &[],
            &[],
            &[],
            &[],
            &BranchEvents::default(),
            &Ballots {
                suggestions: &expiring,
                quorum: 3,
                now,
                ..Default::default()
            },
        );
        assert!(inbox[0].detail.contains("(<1m left)"), "{:?}", inbox[0]);

        // A ballot with no window at all renders no clause rather than a bogus one.
        let inbox = build(
            &[],
            &[],
            &[],
            &[],
            &BranchEvents::default(),
            &ballots(&suggestions, &[]),
        );
        assert_eq!(
            inbox[0].detail,
            "0/3 endorsers — rat-1 proposes: proposal a"
        );
    }

    #[test]
    fn plain_obstacle_uses_text_and_obstacle_rank() {
        let obstacles = vec![obstacle("Pip", json!({"text": "merge conflict in lib.rs"}))];
        let inbox = build(
            &[],
            &[],
            &obstacles,
            &[],
            &BranchEvents::default(),
            &Ballots::default(),
        );
        assert_eq!(inbox[0].urgency, urgency::OBSTACLE);
        assert_eq!(inbox[0].detail, "merge conflict in lib.rs");
        assert_eq!(inbox[0].action, "rk status Pip");
    }
}

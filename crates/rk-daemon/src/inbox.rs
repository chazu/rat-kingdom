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
//! One source is a BALLOT rather than a problem: an open `Suggestion`
//! (`open-suggestion`). It is here because a proposal nobody sees is
//! indistinguishable from one nobody made, and the operator is the one endorser
//! who is always available. It is also the only row here with no failure behind
//! it, which is why it needs BOTH of its settled states wired up: since TKT-168
//! a ballot no longer decays, so `rk withdraw` (TKT-184) is what retires a row
//! nobody is going to back. Otherwise this rank only grows, and an inbox rank
//! that can never be emptied trains the operator to skip the whole queue.

use crate::agents::{AgentRecord, AgentState};
use crate::landing::LandingQueueSummary;
use crate::workflow_exec::{settled_at, Instance, InstanceStatus};
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
    /// The oldest entry in a `(repo, target)` landing queue has waited past
    /// the configured staleness threshold. Co-ranked with a parked gate and
    /// an open PR — all three are finished work blocked from landing, and a
    /// wedged queue is otherwise indistinguishable from one patiently
    /// draining a deep backlog (probe O18).
    pub const LANDING_QUEUE_STALLED: u8 = 3;
    pub const OBSTACLE: u8 = 2;
    pub const NEED: u8 = 1;
    /// An open proposal awaiting endorsements. Co-ranked with `need` — both are
    /// a rat asking the room for something rather than reporting a problem, and
    /// neither blocks anything. It earns a row at all because nothing else in
    /// the fleet announces a ballot: a suggestion is only ever endorsed by a
    /// peer who goes looking for one it has no reason to suspect exists
    /// (TKT-167). Since TKT-168 a ballot is durable by default and no longer
    /// races a clock, so the row is no longer a deadline — it is the only
    /// announcement. A ballot given an explicit `--ttl` still expires, and that
    /// one IS a deadline; `open_suggestions` sorts it first.
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
/// `rk suggest` writes a durable `Suggestion` on the system scope. The reactor
/// promotes it to a permanent `Convention` once `quorum` DISTINCT agents have
/// endorsed it; until then it stays open. Nothing announces an open ballot, so a
/// proposal only ever promotes if a peer happens to go looking for one it has no
/// reason to suspect exists — measured 2026-07-25, ZERO conventions had ever
/// reached quorum over 277 spawns, three separate rats having tried and failed
/// to gather three votes (TKT-167). These rows are the announcement, and they
/// put the one endorser who is always reachable — the operator — in front of
/// every ballot.
///
/// Ballots carried a 24h voting window until TKT-168, which made both categories
/// durable by default: a vote is a ledger entry, not a pheromone, because no
/// endorser survives to re-cast it and the `Convention` it promotes to is
/// permanent regardless. `--ttl` still time-boxes a vote on request, and legacy
/// Ephemeral ballots already in the space keep the windows they were written
/// with — so `expires_at` remains `Option` here and is still honoured
/// throughout, it is simply no longer the common case.
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
    /// `Withdrawal` tuples, keyed `identity = <sug-id>`: ballots their proposer
    /// or the operator has closed (TKT-184). The other settled state, and the
    /// one this row exists to make reachable — ballots stopped expiring in
    /// TKT-168, so without an explicit close a proposal nobody backs sits in
    /// this queue forever and the operator learns to scroll past the whole
    /// `open-suggestion` rank. A queue that cannot be emptied stops being read.
    pub withdrawals: &'a [Tuple],
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
            withdrawals: &[],
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

    // Workflow instances: failures no later run has made good (TKT-187), and
    // runs parked at an approval gate.
    items.extend(unresolved_workflow_failures(instances));
    for i in instances {
        if i.status == InstanceStatus::Running && i.awaiting.as_deref() == Some("approval") {
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
                t.payload
                    .get("cost_usd")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0),
                t.payload
                    .get("tokens")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0),
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

    // Open pull/merge requests: a PR-mode landing pushed a branch and
    // opened a PR (a `pull_request_opened` event), then completed — so nothing
    // else in this queue tracks it. Surface each so a pushed branch is visible
    // attention, never silently forgotten, carrying the forge URL to review it.
    // Dedup by (scope, branch): a re-land emits a fresh event for the same
    // branch, and only the newest matters. Compare ULIDs explicitly so this
    // reducer remains correct for either storage scan order.
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
        if latest_pr
            .get(&key)
            .is_none_or(|previous| previous.id < t.id)
        {
            latest_pr.insert(key, t);
        }
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

/// One row per open ballot: a system-scope `Suggestion` that has neither
/// promoted nor decayed (TKT-167).
///
/// Four filters, each dropping a ballot the operator cannot usefully act on:
///
/// - **quorum 0** — promotion is disabled, so no endorsement can resolve it.
/// - **already promoted** — a `Convention` carries the suggestion's id, so the
///   norm is permanent and the vote is over.
/// - **withdrawn** — a `Withdrawal` carries the suggestion's id: its proposer or
///   the operator closed it (TKT-184). The other settled state, and the only one
///   that empties this rank now that ballots no longer decay (TKT-168). Note
///   this drops the ROW, not the ballot: the suggestion and its endorsements
///   stay in the space, so `rk scan suggestion system` still shows the full
///   history to anyone who goes looking. What ends is the nagging.
/// - **decayed** — `expires_at` has passed. Expiry is collected by the GC rather
///   than filtered on read, so a scan returns ballots whose window closed
///   minutes ago; endorsing one is not what the operator meant. Only reachable
///   for a `--ttl` ballot or a legacy one written before TKT-168.
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
    // Both settled states retire the row, and they are read the same way: a
    // tuple on the suggestion's own id. Union them so the filter below asks one
    // question — "is this ballot over?" — rather than tracking two outcomes.
    let settled: HashSet<&str> = ballots
        .conventions
        .iter()
        .filter(|t| t.scope == SYSTEM_SCOPE && t.category == Category::Convention)
        .chain(
            ballots
                .withdrawals
                .iter()
                .filter(|t| t.scope == SYSTEM_SCOPE && t.category == Category::Withdrawal),
        )
        .map(|t| t.identity.as_str())
        .collect();

    let mut open: Vec<&Tuple> = ballots
        .suggestions
        .iter()
        .filter(|t| t.scope == SYSTEM_SCOPE && !settled.contains(t.identity.as_str()))
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

/// One row per `(repo, target)` landing-queue key whose OLDEST pending entry
/// has waited at least `stale_after_secs` since it first enqueued (probe
/// O18). Computed live over the current queue summary on every read — no ack,
/// no stored escalation, same shape as `unlanded-branch`/`awaiting-review`:
/// the row exists exactly as long as the condition holds and disappears the
/// moment the oldest entry drains or a fresher entry becomes the oldest.
/// `stale_after_secs == 0` disables the row entirely (depth/age still show on
/// `status`/`rk top`, which read the summary directly).
pub(crate) fn stalled_landing_queue_rows(
    summary: &[LandingQueueSummary],
    stale_after_secs: u64,
) -> Vec<InboxItem> {
    if stale_after_secs == 0 {
        return Vec::new();
    }
    let mut rows: Vec<&LandingQueueSummary> = summary
        .iter()
        .filter(|q| q.oldest_age_secs >= 0 && q.oldest_age_secs as u64 >= stale_after_secs)
        .collect();
    // Longest-waiting first — the entry closest to explaining a stall belongs
    // at the top of this rank.
    rows.sort_by_key(|q| std::cmp::Reverse(q.oldest_age_secs));
    rows.into_iter()
        .map(|q| InboxItem {
            urgency: urgency::LANDING_QUEUE_STALLED,
            kind: "landing-queue-stalled".into(),
            subject: format!("{} → {}", q.repo, q.target),
            scope: q.repo.clone(),
            detail: format!(
                "{} queued, oldest ({}) waiting {}",
                q.depth,
                q.oldest_branch,
                waiting_for(q.oldest_age_secs),
            ),
            action: "rk status --json  (see landing_queue)".into(),
        })
        .collect()
}

/// Render an age in seconds as `3h12m waiting` / `45m waiting` / `12s
/// waiting`. Negative input (a clock skew, never expected in practice) reads
/// as `0s waiting` rather than a nonsensical negative duration.
fn waiting_for(age_secs: i64) -> String {
    let secs = age_secs.max(0);
    let mins = secs / 60;
    match (mins / 60, mins % 60) {
        (0, 0) => format!("{}s waiting", secs),
        (0, m) => format!("{m}m waiting"),
        (h, m) => format!("{h}h{m:02}m waiting"),
    }
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

/// One row per workflow failure that is still outstanding: failures a later run
/// of the SAME WORK has since made good are retired, and repeated attempts at
/// one piece of work collapse into a single row (TKT-187).
///
/// # Why the error text is never read
///
/// The obvious way to stop a transient failure nagging forever is to look at
/// `Instance::error` and decide from its wording whether it was retryable — a
/// timeout, a killed process, a connection refused. Every part of that is wrong
/// here. `error` is `e.to_string()` over an arbitrary `rk_core::Error`
/// (`workflow_exec::finalize`): it is prose, not a contract, produced by code
/// that has no idea it is being parsed, and it changes the moment anyone rewords
/// a message. A classifier over it is a guess that starts silently discarding
/// real failures the first time someone edits a string — and this queue exists
/// precisely because silently-absent work is the failure mode that keeps costing
/// the fleet days (TKT-147, TKT-171).
///
/// # What is asserted instead
///
/// The same shape as the branch-shaped rows above: name a stable identity, then
/// re-ask an independent authority whether it has resolved. For a branch the
/// identity is `{scope, branch}` and the authority is local git. For a workflow
/// the identity is [`Instance::work_key`] — repo, workflow, params — and the
/// authority is the instance store itself:
///
/// > A failure is proven transient exactly when the same work later succeeded.
///
/// You cannot tell a transient failure from a permanent one by looking at it;
/// you can only tell by looking at what happened next. So this takes the latest
/// TERMINAL attempt at each piece of work and reports only that one. Completed
/// means the work is done and no row is owed. Failed means it is still owed.
/// Nothing anywhere inspects *why* anything failed.
///
/// Three properties make an auto-clear defensible without a human in the loop:
///
/// - **It suppresses a row, it does not delete a run.** The failed instance
///   stays in the store and `rk workflow status`/`timeline` still read it in
///   full. The worst case of a wrong suppression is that the operator is not
///   nagged — never that a failure is lost. That is the same bargain
///   `BranchEvents::cleared` already makes for branches.
/// - **It is recomputed on every read.** Prune the successful run and the
///   failure is owed a row again, because the evidence that retired it is gone.
///   Nothing writes a "resolved" marker that could outlive its own truth.
/// - **It fails closed.** An instance whose `work_key` is empty (params that
///   would not serialize) is grouped alone under its own id, so it can neither
///   clear another failure nor be cleared by one, and keeps exactly its
///   pre-TKT-187 behaviour.
///
/// A retry that is still RUNNING clears nothing: trying is not succeeding, and
/// hiding a row because someone is having another go is how TKT-147 lost two
/// days. The row is annotated with the in-flight id instead, which is what lets
/// the operator tell "wait" from "fix" without hiding anything.
fn unresolved_workflow_failures(instances: &[Instance]) -> Vec<InboxItem> {
    let mut groups: HashMap<String, Vec<&Instance>> = HashMap::new();
    for i in instances {
        let work_key = i.work_key();
        let key = if work_key.is_empty() {
            // Unknowable work identity: group with nothing but itself.
            format!("id:{}", i.id)
        } else {
            format!("work:{work_key}")
        };
        groups.entry(key).or_default().push(i);
    }

    // Row order must never depend on `HashMap` iteration order. Emit each row at
    // the position its reported instance occupies in `instances`, which is the
    // caller's order — instances by start time, as the module contract says.
    let position: HashMap<&str, usize> = instances
        .iter()
        .enumerate()
        .map(|(n, i)| (i.id.as_str(), n))
        .collect();

    let mut rows: Vec<(usize, InboxItem)> = Vec::new();
    for attempts in groups.values() {
        // The newest attempt that actually settled. `settled_at` is the shared
        // definition `rk prune` windows on. The id breaks an exact timestamp tie
        // for determinism only — instance ids carry no time component, so it is
        // never a recency signal.
        let Some(newest) = attempts
            .iter()
            .copied()
            .filter(|i| i.status != InstanceStatus::Running)
            .max_by(|a, b| {
                settled_at(a)
                    .cmp(&settled_at(b))
                    .then_with(|| a.id.cmp(&b.id))
            })
        else {
            // Nothing has settled yet, so no failure has been reported at all.
            continue;
        };
        if newest.status != InstanceStatus::Failed {
            // The latest word on this work is a success. The failure it
            // superseded is made good and owes the operator nothing.
            continue;
        }
        let settled = settled_at(newest);

        // Earlier failed attempts at the same work are the same problem, so they
        // are reported once rather than once each.
        let mut earlier: Vec<&Instance> = attempts
            .iter()
            .copied()
            .filter(|i| i.status == InstanceStatus::Failed && i.id != newest.id)
            .collect();
        earlier.sort_by(|a, b| {
            settled_at(b)
                .cmp(&settled_at(a))
                .then_with(|| b.id.cmp(&a.id))
        });

        // A retry launched since this failure settled. Annotates, never clears.
        let in_flight = attempts
            .iter()
            .copied()
            .filter(|i| i.status == InstanceStatus::Running && i.started_at >= settled)
            .max_by(|a, b| {
                a.started_at
                    .cmp(&b.started_at)
                    .then_with(|| a.id.cmp(&b.id))
            });

        let mut detail = format!(
            "{} failed: {}",
            newest.workflow,
            newest.error.as_deref().unwrap_or("(no error recorded)")
        );
        if !earlier.is_empty() {
            detail.push_str(&format!(
                " (+{} earlier failed attempt{} at the same work)",
                earlier.len(),
                if earlier.len() == 1 { "" } else { "s" }
            ));
        }
        if let Some(retry) = in_flight {
            detail.push_str(&format!(" — retry {} in flight", retry.id));
        }

        // Inspect, then clear. Without the second verb this row was
        // unresolvable: nothing retired a failed instance, so the board only
        // ever grew (TKT-177). The prune names EVERY failed attempt in the
        // group, because pruning just the newest would promote the one before it
        // to newest-terminal and raise a fresh row in its place.
        let prune_ids: Vec<&str> = std::iter::once(newest.id.as_str())
            .chain(earlier.iter().map(|i| i.id.as_str()))
            .collect();
        let action = format!(
            "rk workflow status {}  |  rk workflow prune {}",
            newest.id,
            prune_ids.join(" ")
        );

        let at = position
            .get(newest.id.as_str())
            .copied()
            .unwrap_or(usize::MAX);
        rows.push((
            at,
            InboxItem {
                urgency: urgency::FAILED,
                kind: "workflow-failed".into(),
                subject: newest.id.clone(),
                scope: repo_name(&newest.repo),
                detail,
                action,
            },
        ));
    }
    rows.sort_by_key(|(at, _)| *at);
    rows.into_iter().map(|(_, item)| item).collect()
}

/// The `branch_landed` events that describe a DROPPED branch: the newest event
/// per (scope, branch), keeping only those where the land neither merged nor
/// opened a PR, newest first.
///
/// Newest-wins is what retires a retry: a successful re-land emits a later
/// `{merged: true}` event for the same branch, which replaces the failed one.
/// The newest ULID wins regardless of storage scan order.
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
        let key = (t.scope.clone(), branch.to_string());
        if latest.get(&key).is_none_or(|previous| previous.id < t.id) {
            latest.insert(key, t);
        }
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

/// One row per unacked `recovery_action` escalation (strategic review B2):
/// the durable record `recovery::RecoveryAnnouncer::announce` writes for an
/// automated recovery action (auto-respawn, kill-at-`rk done`,
/// stale-instance timeout, orphaned-ticket reopen). Standalone rather than
/// folded into [`build`] so this module never has to depend on
/// `crate::recovery` for anything but the identity strings it already
/// re-derives here — the caller (`server.rs::inbox_value`) merges this
/// output into `build`'s and re-sorts by urgency.
///
/// `rk inbox ack <id>` is the ONLY thing that retires a row: unlike a
/// `need`/`obstacle`, nothing here auto-clears from git or a later event, and
/// the B2 re-notify sweep keeps pushing exactly as long as this row keeps
/// showing up here.
pub fn recovery_action_rows(actions: &[Tuple], acks: &[Tuple]) -> Vec<InboxItem> {
    let acked: HashSet<&str> = acks
        .iter()
        .filter_map(|t| t.payload.get("tuple").and_then(|v| v.as_str()))
        .collect();
    actions
        .iter()
        .filter(|t| !acked.contains(t.id.to_string().as_str()))
        .map(|t| {
            let id = t.id.to_string();
            let notice = t.payload.get("notice");
            let severity = notice
                .and_then(|n| n.get("severity"))
                .and_then(|v| v.as_str())
                .unwrap_or("info");
            let urgency = match severity {
                "critical" => urgency::FAILED,
                "warn" => urgency::OBSTACLE,
                _ => urgency::NEED,
            };
            let subject = notice
                .and_then(|n| n.get("subject"))
                .and_then(|v| v.as_str())
                .unwrap_or(&t.identity)
                .to_string();
            let detail = notice
                .and_then(|n| n.get("text"))
                .and_then(|v| v.as_str())
                .unwrap_or("(recovery action)")
                .to_string();
            InboxItem {
                urgency,
                kind: "recovery-action".into(),
                subject,
                scope: t.scope.clone(),
                detail,
                action: format!("rk inbox ack {id}"),
            }
        })
        .collect()
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
            spawn: None,
            role: "rat".into(),
            coordination: None,
            harness: "fake".into(),
            permission_mode: None,
            model: None,
            repo_root: "/tmp/repo".into(),
            repo_name: "repo".into(),
            task: Some("TKT-9".into()),
            branch: Some(format!("rat/{name}/tkt-9")),
            fork_point: None,
            worktree: Some(format!("/tmp/wt/{name}").into()),
            target_branch: "main".into(),
            parent: None,
            workflow_instance: None,
            review: None,
            coordinator: None,
            session_id: None,
            attach_target: None,
            pid: None,
            merge_commit: None,
            state,
            crashed: false,
            stderr_tail: None,
            result: None,
            progress: None,
            usage: TokenUsage::default(),
            cost_usd: 0.0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            archived_at: None,
            liveness: Default::default(),
        }
    }

    fn instance(id: &str, status: InstanceStatus, awaiting: Option<&str>) -> Instance {
        Instance {
            id: id.into(),
            workflow: "gated-merge".into(),
            repo: "/home/x/dev/repo".into(),
            coordinator: None,
            schedule: None,
            status,
            revision: 0,
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
            automated_landing_authorized: false,
            params: Default::default(),
            depth: 0,
            started_at: Utc::now(),
            completed_at: None,
            archived_at: None,
            trigger: None,
            stale_timeout_secs: None,
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

    fn recovery_action(scope: &str, subject: &str, severity: &str, text: &str) -> Tuple {
        Tuple::new(
            Category::Event,
            scope,
            "recovery_action",
            "reactor",
            json!({
                "action_kind": "respawn",
                "held": false,
                "notice": {
                    "tuple_id": "placeholder",
                    "class": "recovery-test",
                    "severity": severity,
                    "scope": scope,
                    "subject": subject,
                    "text": text,
                    "suggested_action": null,
                    "refs": {},
                },
            }),
        )
    }

    fn ack(tuple_id: &str) -> Tuple {
        Tuple::new(
            Category::Event,
            "repo",
            "inbox_ack",
            "operator",
            json!({ "tuple": tuple_id, "acked_by": "operator" }),
        )
    }

    #[test]
    fn recovery_action_rows_surfaces_unacked_and_drops_acked() {
        let stale = recovery_action("repo", "Whisker", "critical", "respawn held");
        let live = recovery_action("repo", "Nibbles", "warn", "respawn ok");
        let acked_row = ack(&stale.id.to_string());

        let rows = recovery_action_rows(&[stale.clone(), live.clone()], &[acked_row]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].subject, "Nibbles");
        assert_eq!(rows[0].kind, "recovery-action");
        assert_eq!(rows[0].urgency, urgency::OBSTACLE);
        assert_eq!(rows[0].action, format!("rk inbox ack {}", live.id));

        // Nothing acked: both rows stand.
        let rows = recovery_action_rows(&[stale.clone(), live.clone()], &[]);
        assert_eq!(rows.len(), 2);
        let critical_row = rows.iter().find(|r| r.subject == "Whisker").unwrap();
        assert_eq!(critical_row.urgency, urgency::FAILED);
    }

    fn queue_summary(
        repo: &str,
        target: &str,
        depth: usize,
        oldest_age_secs: i64,
    ) -> LandingQueueSummary {
        LandingQueueSummary {
            repo: repo.into(),
            target: target.into(),
            depth,
            oldest_age_secs,
            oldest_branch: "rat/some-branch".into(),
            oldest_task: "TKT-9".into(),
        }
    }

    #[test]
    fn stalled_landing_queue_rows_only_fire_past_the_threshold() {
        let three_hours = 3 * 3600;
        // Actively progressing: well under threshold — no row (probe O18's
        // acceptance bar: patience must not read as a stall).
        let fresh = vec![queue_summary("alpha", "main", 4, 60)];
        assert!(stalled_landing_queue_rows(&fresh, three_hours as u64).is_empty());

        // Genuinely wedged: oldest entry past the threshold — exactly one
        // row, carrying depth and the oldest branch/age in its detail.
        let stalled = vec![queue_summary("alpha", "main", 4, three_hours + 60)];
        let rows = stalled_landing_queue_rows(&stalled, three_hours as u64);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].kind, "landing-queue-stalled");
        assert_eq!(rows[0].urgency, urgency::LANDING_QUEUE_STALLED);
        assert_eq!(rows[0].scope, "alpha");
        assert!(rows[0].detail.contains('4'));
        assert!(rows[0].detail.contains("rat/some-branch"));

        // Disabled threshold: never raises a row regardless of age.
        assert!(stalled_landing_queue_rows(&stalled, 0).is_empty());
    }

    #[test]
    fn stalled_landing_queue_rows_rank_oldest_first_across_keys() {
        let queues = vec![
            queue_summary("alpha", "main", 1, 4 * 3600),
            queue_summary("beta", "main", 1, 10 * 3600),
        ];
        let rows = stalled_landing_queue_rows(&queues, 3600);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].scope, "beta");
        assert_eq!(rows[1].scope, "alpha");
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
        // should surface, as one row. Make the ULID order deterministic because
        // two events can otherwise share a millisecond.
        let mut older = pull_request("rat/rat-9/tkt-9", "main", Some("https://forge/pr/1"));
        let mut newer = pull_request("rat/rat-9/tkt-9", "main", Some("https://forge/pr/2"));
        older.id = rk_core::id::RecordId::floor_at(Utc::now());
        newer.id = rk_core::id::RecordId::floor_at(Utc::now() + chrono::Duration::seconds(1));
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
        let mut failed = land("rat/a/tkt-9", false, false, "conflict");
        let mut retried = land("rat/a/tkt-9", true, false, "merged rat/a/tkt-9 into main");
        failed.id = rk_core::id::RecordId::floor_at(Utc::now());
        retried.id = rk_core::id::RecordId::floor_at(Utc::now() + chrono::Duration::seconds(1));
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
    fn newest_land_wins_even_when_storage_returns_newest_first() {
        let mut failed = land("rat/a/tkt-9", false, false, "conflict");
        let mut retried = land("rat/a/tkt-9", true, false, "merged");
        failed.id = rk_core::id::RecordId::floor_at(Utc::now());
        retried.id = rk_core::id::RecordId::floor_at(Utc::now() + chrono::Duration::seconds(1));

        let inbox = build(
            &[],
            &[],
            &[],
            &[],
            &BranchEvents {
                // This is the order produced by the bounded newest-first scan.
                lands: &[retried, failed],
                ..Default::default()
            },
            &Ballots::default(),
        );
        assert!(
            inbox.is_empty(),
            "newest successful state must win: {inbox:?}"
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

    fn withdrawal(sug_id: &str, by: &str) -> Tuple {
        Tuple::new(
            Category::Withdrawal,
            SYSTEM_SCOPE,
            sug_id,
            by,
            json!({ "suggestion": sug_id, "withdrawn_by": by }),
        )
    }

    /// TKT-184 — the row that could not be retired. A ballot no longer decays
    /// (TKT-168), so before `rk withdraw` existed a proposal nobody intended to
    /// back sat in the inbox forever. Withdrawing it closes the row, and does so
    /// per-ballot: a peer proposal is untouched.
    #[test]
    fn a_withdrawn_ballot_stops_nagging() {
        let suggestions = vec![
            suggestion(
                "sug-pulled",
                "rat-1",
                "the author thought better of it",
                None,
            ),
            suggestion("sug-live", "rat-2", "still worth backing", None),
        ];
        let withdrawals = vec![withdrawal("sug-pulled", "rat-1")];
        let inbox = build(
            &[],
            &[],
            &[],
            &[],
            &BranchEvents::default(),
            &Ballots {
                suggestions: &suggestions,
                withdrawals: &withdrawals,
                quorum: 3,
                ..Default::default()
            },
        );
        assert_eq!(inbox.len(), 1, "{inbox:?}");
        assert_eq!(inbox[0].subject, "sug-live");
    }

    /// The withdrawal closes the BALLOT, not the votes. Endorsements on a
    /// withdrawn proposal stay in the space and stay countable — dropping them
    /// would recreate the orphaned-vote hazard TKT-168 was written to avoid —
    /// so the row must be retired by the withdrawal alone, no matter how healthy
    /// the tally underneath it looks.
    #[test]
    fn a_withdrawn_ballot_stays_closed_even_at_quorum() {
        let suggestions = vec![suggestion(
            "sug-pulled",
            "rat-1",
            "popular but pulled",
            None,
        )];
        let endorsements = vec![
            endorsement("sug-pulled", "rat-7"),
            endorsement("sug-pulled", "rat-8"),
            endorsement("sug-pulled", "rat-9"),
        ];
        let withdrawals = vec![withdrawal("sug-pulled", "operator")];
        let inbox = build(
            &[],
            &[],
            &[],
            &[],
            &BranchEvents::default(),
            &Ballots {
                suggestions: &suggestions,
                endorsements: &endorsements,
                withdrawals: &withdrawals,
                quorum: 3,
                ..Default::default()
            },
        );
        assert!(inbox.is_empty(), "{inbox:?}");
    }

    /// A withdrawal is keyed on the suggestion id like every other ballot tuple,
    /// and must not leak across ids or in from another scope — the same
    /// discipline the endorsement tally is held to.
    #[test]
    fn a_withdrawal_only_closes_its_own_ballot() {
        let suggestions = vec![suggestion("sug-a", "rat-1", "proposal a", None)];
        let mut off_scope = withdrawal("sug-a", "rat-1");
        off_scope.scope = "repo".into();
        let withdrawals = vec![withdrawal("sug-other", "rat-2"), off_scope];
        let inbox = build(
            &[],
            &[],
            &[],
            &[],
            &BranchEvents::default(),
            &Ballots {
                suggestions: &suggestions,
                withdrawals: &withdrawals,
                quorum: 3,
                ..Default::default()
            },
        );
        assert_eq!(inbox.len(), 1, "{inbox:?}");
        assert_eq!(inbox[0].subject, "sug-a");
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

//! Mechanical repair for the two `crate::reconcile` violations durable
//! evidence alone can prove *and* durable evidence alone can fix:
//! `delivered-but-open` (a ticket's own delivery record disagrees with its
//! own status) and `terminal-assignee-active-work` — "stale ownership", a
//! terminal agent still on record as owning open work. Every other
//! violation kind stays report-only: this module never plans an action for
//! `conflict-held-landing`, `tracker-contradicts-git-history`, or
//! `workflow-settled-agent-still-live` — those need orchestrator dispatch
//! judgment or a human, not a script.
//!
//! Deliberately independent of the authority-ladder substrate
//! (`attention.rs`/`authority.rs`/`orchestrator_lease.rs`): a repair here
//! carries its own fixed [`MECHANICAL_AUTHORITY`] tag rather than routing
//! through that policy engine, so this module has no edge into it.
//!
//! Two-phase, always fresh:
//!
//! - [`plan`] is pure over already-scanned data (mirrors `reconcile::build`'s
//!   own shape) and re-derives eligibility from the exact predicates the
//!   report itself uses ([`reconcile::delivered_but_open`],
//!   [`reconcile::terminal_assignee_active_work`]), then layers on a second
//!   pass of durable-evidence checks unique to repair: does git *right now*
//!   confirm the delivery record's own ancestry claim, does the landed
//!   commit touch a protected path, has the branch since diverged from what
//!   was recorded. Anything short of a clean answer on all of those holds —
//!   zero mutation, one of [`HoldReason`]'s named reasons, never a guess.
//! - [`apply`] executes a plan's `Planned` items through `Tickets`' own
//!   compare-and-swap writers ([`crate::tickets::Tickets::repair_close_delivered`],
//!   [`crate::tickets::Tickets::repair_clear_stale_ownership`]), which
//!   re-check the precondition against the ticket's *live* payload
//!   immediately before writing — so drift between `plan` and `apply` (a
//!   human closing the ticket by hand, a second repair racing this one)
//!   fails that one item closed instead of corrupting it. A durable marker
//!   event is checked before acting and written after a real mutation, so
//!   replaying the same apply call is a no-op the second time — and the
//!   marker itself doubles as the journal/announcement, carrying the
//!   violation id, the evidence, and the `mechanical` authority tag.
//! - [`dry_run`] runs the identical plan and reports what *would* happen
//!   without calling [`apply`] — same evidence, same hold reasons, zero
//!   writes.

use crate::agents::AgentRecord;
use crate::reconcile::{self, GitFacts};
use crate::tickets::{CasOutcome, Tickets};
use rk_core::tuple::{Category, Lifecycle, Pattern, Tuple};
use rk_space::Space;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

/// Identity of the durable marker/announcement event a successful apply
/// writes — the journal entry and the replay guard in one tuple.
pub const REPAIR_APPLIED_IDENTITY: &str = "reconcile_repair_applied";

/// Who is answerable for a repair action. Deliberately a fixed constant, not
/// `reconcile::Authority` reused: the *report's* authority for
/// `terminal-assignee-active-work` is `Orchestrator` (redispatch is a
/// judgment call), but the *record fix* this module performs — un-sticking
/// an ownership pointer already proven wrong by durable evidence — is a
/// narrower, mechanical action that leaves the redispatch decision entirely
/// to whatever claims the ticket next.
pub const MECHANICAL_AUTHORITY: &str = "mechanical";

/// Extra durable-evidence checks repair needs beyond `reconcile::GitFacts`,
/// gathered fresh by the caller immediately before planning/applying (never
/// reused across calls, never cached) so a repair never acts on evidence
/// that might be stale by even a few seconds.
#[derive(Debug, Default, Clone)]
pub struct RepairFacts {
    pub git: GitFacts,
    /// `merge_commit -> does its diff touch the repo's configured protected
    /// paths?` Absent means the caller could not check (an unregistered
    /// repo, an unresolvable parent commit) — missing evidence, never read
    /// as "does not touch".
    pub protected_touch: HashMap<String, bool>,
    /// `(scope, branch) -> has the branch ref moved since landing in a way
    /// the recorded merge commit does not capture?` (its current tip is not
    /// an ancestor of `merge_commit`). Absent, or the branch already gone,
    /// reads as "nothing to compare" rather than a conflict.
    pub diverged: HashMap<(String, String), bool>,
}

/// Why a candidate repair was held instead of acted on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HoldReason {
    /// A fresh check this repair needs was not answerable (unregistered
    /// repo, unresolvable commit, a vanished ticket) — absence of evidence,
    /// never license to act.
    MissingEvidence,
    /// Git does not, right now, confirm the delivery record's own claim.
    AmbiguousDelivery,
    /// The landed branch has moved since landing in a way the recorded
    /// merge commit does not capture.
    ConflictingHeads,
    /// The delivered commit touches a path the repo's landing policy marks
    /// protected — needs a human's eyes before the ticket auto-closes.
    ProtectedPathImpact,
    /// The live ticket no longer matches the evidence the plan was built
    /// on — the compare-and-swap precondition failed at apply time.
    EvidenceDrift,
}

/// The action a `Planned` item will take if applied.
#[derive(Debug, Clone)]
pub enum RepairAction {
    CloseDelivered {
        merge_commit: String,
    },
    ClearStaleOwnership {
        status: String,
        assignee: Option<String>,
    },
}

impl RepairAction {
    fn describe(&self) -> String {
        match self {
            RepairAction::CloseDelivered { merge_commit } => format!(
                "close the ticket — its own delivery record (merge {}) already proves it shipped",
                short(merge_commit)
            ),
            RepairAction::ClearStaleOwnership { .. } => "reopen the ticket and clear its \
                assignee — the recorded owner has settled terminal with no hand-off"
                .to_string(),
        }
    }
}

fn short(sha: &str) -> &str {
    sha.get(0..8).unwrap_or(sha)
}

/// What repair decided about one violation: act, or hold with a reason.
#[derive(Debug, Clone)]
pub enum Disposition {
    Planned(RepairAction),
    Held { reason: HoldReason, detail: String },
}

/// One violation carried through repair planning: the same join keys and
/// evidence as the `reconcile::Violation` it was built from, plus what
/// repair decided to do about it.
#[derive(Debug, Clone)]
pub struct RepairItem {
    pub violation_id: String,
    pub kind: String,
    pub scope: String,
    pub subject: String,
    pub detail: String,
    pub evidence: Vec<String>,
    pub disposition: Disposition,
}

impl RepairItem {
    fn from_violation(v: &reconcile::Violation) -> Self {
        RepairItem {
            violation_id: v.id.clone(),
            kind: v.kind.clone(),
            scope: v.scope.clone(),
            subject: v.subject.clone(),
            detail: v.detail.clone(),
            evidence: v.evidence.clone(),
            // Always overwritten by `.held(...)` or `.planned(...)` before
            // the item leaves this module.
            disposition: Disposition::Held {
                reason: HoldReason::MissingEvidence,
                detail: String::new(),
            },
        }
    }

    fn held(mut self, reason: HoldReason, detail: impl Into<String>) -> Self {
        self.disposition = Disposition::Held {
            reason,
            detail: detail.into(),
        };
        self
    }

    fn planned(mut self, action: RepairAction) -> Self {
        self.disposition = Disposition::Planned(action);
        self
    }
}

/// The full repair plan for one repository: every eligible violation, with
/// its disposition already decided. Pure data — nothing here has mutated
/// anything.
#[derive(Debug, Clone)]
pub struct RepairPlan {
    pub scope: String,
    pub items: Vec<RepairItem>,
}

/// Re-derive the two mechanically-repairable violation kinds from the same
/// predicates `reconcile::build` uses, and decide a disposition for each —
/// pure over its inputs, unit-testable without a daemon, exactly like
/// `reconcile::build` itself.
pub fn plan(
    scope: &str,
    tickets: &[Tuple],
    agents: &[AgentRecord],
    landed_tickets: &HashSet<String>,
    queued_tickets: &HashSet<String>,
    facts: &RepairFacts,
) -> RepairPlan {
    let mut items: Vec<RepairItem> = reconcile::delivered_but_open(tickets)
        .into_iter()
        .map(|v| plan_delivered_but_open(v, tickets, facts))
        .collect();
    items.extend(
        reconcile::terminal_assignee_active_work(tickets, agents, landed_tickets, queued_tickets)
            .into_iter()
            .map(|v| plan_stale_ownership(v, tickets)),
    );
    items.sort_by(|a, b| a.violation_id.cmp(&b.violation_id));
    RepairPlan {
        scope: scope.to_string(),
        items,
    }
}

fn plan_delivered_but_open(
    v: reconcile::Violation,
    tickets: &[Tuple],
    facts: &RepairFacts,
) -> RepairItem {
    let base = RepairItem::from_violation(&v);
    let Some(ticket) = tickets.iter().find(|t| t.identity == v.subject) else {
        return base.held(
            HoldReason::MissingEvidence,
            "ticket vanished before repair could re-verify it",
        );
    };
    let Some(record) = crate::tickets::delivery_of(ticket) else {
        return base.held(
            HoldReason::MissingEvidence,
            "delivery record vanished before repair could re-verify it",
        );
    };
    let ancestry_key = (record.merge_commit.clone(), record.target.clone());
    match facts.git.is_ancestor.get(&ancestry_key) {
        None => base.held(
            HoldReason::MissingEvidence,
            "git ancestry for the delivery record was not freshly checked",
        ),
        Some(false) => base.held(
            HoldReason::AmbiguousDelivery,
            "git does not confirm the recorded merge commit reached its target",
        ),
        Some(true) => match facts.protected_touch.get(&record.merge_commit) {
            None => base.held(
                HoldReason::MissingEvidence,
                "protected-path impact of the delivered commit was not checked",
            ),
            Some(true) => base.held(
                HoldReason::ProtectedPathImpact,
                "the delivered commit touches a path this repo's landing policy marks protected",
            ),
            Some(false) => {
                let diverged_key = (v.scope.clone(), record.branch.clone());
                if facts.diverged.get(&diverged_key).copied().unwrap_or(false) {
                    base.held(
                        HoldReason::ConflictingHeads,
                        "the landed branch has since diverged from the recorded merge commit",
                    )
                } else {
                    base.planned(RepairAction::CloseDelivered {
                        merge_commit: record.merge_commit,
                    })
                }
            }
        },
    }
}

fn plan_stale_ownership(v: reconcile::Violation, tickets: &[Tuple]) -> RepairItem {
    let base = RepairItem::from_violation(&v);
    let Some(ticket) = tickets.iter().find(|t| t.identity == v.subject) else {
        return base.held(
            HoldReason::MissingEvidence,
            "ticket vanished before repair could re-verify it",
        );
    };
    let Some(status) = ticket.payload.get("status").and_then(Value::as_str) else {
        return base.held(HoldReason::MissingEvidence, "ticket has no status field");
    };
    let assignee = ticket
        .payload
        .get("assignee")
        .and_then(Value::as_str)
        .map(str::to_string);
    base.planned(RepairAction::ClearStaleOwnership {
        status: status.to_string(),
        assignee,
    })
}

/// What happened to one repair item — a dry-run preview, a real mutation, an
/// idempotent replay, or a held item carried through unchanged from the plan.
#[derive(Debug, Clone)]
pub enum Outcome {
    WouldApply { detail: String },
    Applied { detail: String, journal: String },
    AlreadyApplied { detail: String },
    Held { reason: HoldReason, detail: String },
}

#[derive(Debug, Clone)]
pub struct RepairResult {
    pub violation_id: String,
    pub kind: String,
    pub scope: String,
    pub subject: String,
    pub detail: String,
    pub evidence: Vec<String>,
    pub authority: &'static str,
    pub outcome: Outcome,
}

#[derive(Debug, Clone)]
pub struct RepairReport {
    pub scope: String,
    pub mode: &'static str,
    pub results: Vec<RepairResult>,
}

/// Preview a plan with zero mutation: `Planned` items report what they would
/// do, `Held` items pass through unchanged.
pub fn dry_run(plan: RepairPlan) -> RepairReport {
    let results = plan
        .items
        .into_iter()
        .map(|item| {
            let outcome = match item.disposition {
                Disposition::Planned(action) => Outcome::WouldApply {
                    detail: action.describe(),
                },
                Disposition::Held { reason, detail } => Outcome::Held { reason, detail },
            };
            RepairResult {
                violation_id: item.violation_id,
                kind: item.kind,
                scope: item.scope,
                subject: item.subject,
                detail: item.detail,
                evidence: item.evidence,
                authority: MECHANICAL_AUTHORITY,
                outcome,
            }
        })
        .collect();
    RepairReport {
        scope: plan.scope,
        mode: "dry_run",
        results,
    }
}

/// The live collaborators one [`apply`] run writes through, bundled because
/// they travel together the whole way down (`apply` → `execute` → the
/// journal) and because the freshness contract below is a property of the
/// call as a whole, not of one parameter position.
pub struct ApplyContext<'a> {
    /// The compare-and-swap ticket writer every mutation goes through.
    pub tickets: &'a Tickets,
    /// Where the durable marker/journal event is written.
    pub space: &'a Space,
    /// Castle identity stamped on that event.
    pub castle: &'a str,
    /// Must be fetched fresh by the caller immediately before calling
    /// [`apply`], never the same snapshot [`plan`] was built from: a ticket's
    /// own `status`/`assignee` fields are compare-and-swapped by [`Tickets`],
    /// but the evidence that actually justifies
    /// [`RepairAction::ClearStaleOwnership`] — the assignee's agent having
    /// settled terminal with no hand-off — lives outside the ticket
    /// entirely. Nothing re-checks it once [`plan`] closes over the original
    /// agent slice, so this fresh slice is the only thing standing between a
    /// plan built minutes ago and a ticket ripped out from under an agent
    /// that has since resumed under the same name.
    pub agents: &'a [AgentRecord],
}

/// The identifying facts of one repair — the violation's join keys and the
/// evidence vector it was proven from — borrowed from the plan item so that
/// `execute` and the journal entry it writes agree by construction instead of
/// by four matching parameter positions.
#[derive(Debug, Clone, Copy)]
struct RepairFocus<'a> {
    violation_id: &'a str,
    kind: &'a str,
    scope: &'a str,
    evidence: &'a [String],
}

/// Execute a plan's `Planned` items through `Tickets`' compare-and-swap
/// writers. `Held` items pass through unchanged — this function never widens
/// a hold into an action.
///
/// See [`ApplyContext::agents`] for the freshness contract the caller owes
/// this function.
pub async fn apply(plan: RepairPlan, ctx: &ApplyContext<'_>) -> rk_core::Result<RepairReport> {
    let mut results = Vec::with_capacity(plan.items.len());
    for item in plan.items {
        let RepairItem {
            violation_id,
            kind,
            scope,
            subject,
            detail,
            evidence,
            disposition,
        } = item;
        let outcome = match disposition {
            Disposition::Held { reason, detail } => Outcome::Held { reason, detail },
            Disposition::Planned(action) => {
                let focus = RepairFocus {
                    violation_id: &violation_id,
                    kind: &kind,
                    scope: &scope,
                    evidence: &evidence,
                };
                execute(focus, action, ctx).await?
            }
        };
        results.push(RepairResult {
            violation_id,
            kind,
            scope,
            subject,
            detail,
            evidence,
            authority: MECHANICAL_AUTHORITY,
            outcome,
        });
    }
    Ok(RepairReport {
        scope: plan.scope,
        mode: "apply",
        results,
    })
}

async fn execute(
    focus: RepairFocus<'_>,
    action: RepairAction,
    ctx: &ApplyContext<'_>,
) -> rk_core::Result<Outcome> {
    if already_applied(ctx.space, focus.violation_id)? {
        return Ok(Outcome::AlreadyApplied {
            detail: action.describe(),
        });
    }
    let detail = action.describe();
    let subject_and_cas = match &action {
        RepairAction::CloseDelivered { merge_commit } => {
            // The violation id is `<kind>:<ticket-id>` (`reconcile::Violation::id`);
            // the ticket id is the only variable component.
            let ticket_id = subject_from_violation_id(focus.violation_id, focus.kind);
            (
                ticket_id.clone(),
                ctx.tickets
                    .repair_close_delivered(&ticket_id, merge_commit)
                    .await?,
            )
        }
        RepairAction::ClearStaleOwnership { status, assignee } => {
            let ticket_id = subject_from_violation_id(focus.violation_id, focus.kind);
            // The ticket-side CAS below only re-checks `status`/`assignee`
            // against the ticket's own live payload — it has no way to see
            // that the *reason* those fields were ever eligible to clear
            // (the assignee's agent settled terminal with no hand-off) has
            // itself gone stale, e.g. the same-named agent resumed between
            // plan and apply. Re-resolve ownership against the fresh
            // `agents` slice right here, immediately before the write.
            match reconcile::resolve_owner(&ticket_id, assignee.as_deref(), ctx.agents) {
                Some(owner) if owner.state.is_archivable() => {}
                _ => {
                    return Ok(Outcome::Held {
                        reason: HoldReason::EvidenceDrift,
                        detail: "the assignee's agent no longer confirms a settled terminal \
                            state with no hand-off — re-checked fresh immediately before the \
                            write"
                            .to_string(),
                    });
                }
            }
            (
                ticket_id.clone(),
                ctx.tickets
                    .repair_clear_stale_ownership(&ticket_id, status, assignee.as_deref())
                    .await?,
            )
        }
    };
    let (subject, cas) = subject_and_cas;
    match cas {
        CasOutcome::Applied(_) => {
            let journal = announce(ctx.space, ctx.castle, focus, &subject, &detail)?;
            Ok(Outcome::Applied { detail, journal })
        }
        CasOutcome::Drifted { detail } => Ok(Outcome::Held {
            reason: HoldReason::EvidenceDrift,
            detail,
        }),
        CasOutcome::Gone => Ok(Outcome::Held {
            reason: HoldReason::MissingEvidence,
            detail: "ticket no longer exists".to_string(),
        }),
    }
}

/// `reconcile::Violation::id` is `format!("{kind}:{subject}")` for both
/// repairable kinds (never a compound key), so the subject is everything
/// after the first `kind:` prefix.
fn subject_from_violation_id(violation_id: &str, kind: &str) -> String {
    violation_id
        .strip_prefix(&format!("{kind}:"))
        .unwrap_or(violation_id)
        .to_string()
}

fn already_applied(space: &Space, violation_id: &str) -> rk_core::Result<bool> {
    let mut pattern = Pattern::category(Category::Event).identity(REPAIR_APPLIED_IDENTITY);
    pattern.payload_search = Some(format!("\"violation_id\":\"{violation_id}\""));
    space.has_persistence_event_matching(&pattern)
}

/// Journal and announce a successful repair in one durable, replicated
/// event: this tuple is both the idempotent-replay marker
/// ([`already_applied`]) and the audit trail — evidence, the mechanical
/// authority tag, and the action taken, all in the payload.
fn announce(
    space: &Space,
    castle: &str,
    focus: RepairFocus<'_>,
    subject: &str,
    detail: &str,
) -> rk_core::Result<String> {
    let payload = json!({
        "violation_id": focus.violation_id,
        "kind": focus.kind,
        "subject": subject,
        "authority": MECHANICAL_AUTHORITY,
        "action": detail,
        "evidence": focus.evidence,
        "applied_at": chrono::Utc::now().to_rfc3339(),
    });
    let tuple = Tuple::new(
        Category::Event,
        focus.scope.to_string(),
        REPAIR_APPLIED_IDENTITY,
        castle.to_string(),
        payload,
    )
    .with_lifecycle(Lifecycle::Furniture);
    let id = tuple.id.to_string();
    space.out(tuple)?;
    Ok(id)
}

pub fn to_json(report: &RepairReport) -> Value {
    json!({
        "scope": report.scope,
        "mode": report.mode,
        "results": report.results.iter().map(result_to_json).collect::<Vec<_>>(),
    })
}

fn result_to_json(r: &RepairResult) -> Value {
    let outcome = match &r.outcome {
        Outcome::WouldApply { detail } => json!({"status": "would_apply", "detail": detail}),
        Outcome::Applied { detail, journal } => {
            json!({"status": "applied", "detail": detail, "journal": journal})
        }
        Outcome::AlreadyApplied { detail } => {
            json!({"status": "already_applied", "detail": detail})
        }
        Outcome::Held { reason, detail } => {
            json!({"status": "held", "reason": reason, "detail": detail})
        }
    };
    json!({
        "violation_id": r.violation_id,
        "kind": r.kind,
        "scope": r.scope,
        "subject": r.subject,
        "detail": r.detail,
        "evidence": r.evidence,
        "authority": r.authority,
        "outcome": outcome,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agents::AgentState;
    use crate::reconcile::kind as vkind;
    use chrono::Utc;
    use rk_core::tuple::{Category, Lifecycle};
    use std::path::PathBuf;

    fn ticket(id: &str, scope: &str, status: &str, extra: Value) -> Tuple {
        let mut payload = json!({
            "title": "t",
            "status": status,
            "assignee": Value::Null,
        });
        if let (Some(obj), Some(extra_obj)) = (payload.as_object_mut(), extra.as_object()) {
            for (k, v) in extra_obj {
                obj.insert(k.clone(), v.clone());
            }
        }
        Tuple::new(Category::Task, scope, id, "castle", payload).with_lifecycle(Lifecycle::Session)
    }

    fn delivery_json(commit: &str, target: &str, branch: &str) -> Value {
        json!({
            "delivery": {
                "merge_commit": commit,
                "branch": branch,
                "target": target,
                "landed_at": "2026-08-19T00:00:00Z",
            }
        })
    }

    fn confirmed_facts(commit: &str, target: &str) -> RepairFacts {
        let mut is_ancestor = HashMap::new();
        is_ancestor.insert((commit.to_string(), target.to_string()), true);
        let mut protected_touch = HashMap::new();
        protected_touch.insert(commit.to_string(), false);
        RepairFacts {
            git: GitFacts {
                is_ancestor,
                ..Default::default()
            },
            protected_touch,
            diverged: HashMap::new(),
        }
    }

    /// The apply-side collaborators every `apply` test needs, with the same
    /// castle identity throughout.
    fn ctx<'a>(
        tickets: &'a Tickets,
        space: &'a Space,
        agents: &'a [AgentRecord],
    ) -> ApplyContext<'a> {
        ApplyContext {
            tickets,
            space,
            castle: "castle",
            agents,
        }
    }

    fn agent(name: &str, task: Option<&str>, state: AgentState) -> AgentRecord {
        AgentRecord {
            name: name.into(),
            spawn: None,
            role: "worker".into(),
            coordination: None,
            harness: "claude".into(),
            permission_mode: None,
            model: None,
            repo_root: PathBuf::from("/tmp/repo"),
            repo_name: "myrepo".into(),
            task: task.map(str::to_string),
            branch: None,
            fork_point: None,
            worktree: None,
            target_branch: "main".into(),
            parent: None,
            workflow_instance: None,
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
            usage: Default::default(),
            cost_usd: 0.0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            archived_at: None,
        }
    }

    #[test]
    fn delivered_but_open_with_confirmed_evidence_is_planned() {
        let t = ticket(
            "TKT-1",
            "myrepo",
            "in_progress",
            delivery_json("abc123", "main", "rat/x/tkt-1"),
        );
        let facts = confirmed_facts("abc123", "main");
        let p = plan(
            "myrepo",
            &[t],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &facts,
        );
        assert_eq!(p.items.len(), 1);
        assert_eq!(p.items[0].kind, vkind::DELIVERED_BUT_OPEN);
        assert!(matches!(
            p.items[0].disposition,
            Disposition::Planned(RepairAction::CloseDelivered { .. })
        ));
    }

    #[test]
    fn missing_ancestry_evidence_holds_instead_of_guessing() {
        let t = ticket(
            "TKT-1",
            "myrepo",
            "in_progress",
            delivery_json("abc123", "main", "rat/x/tkt-1"),
        );
        let p = plan(
            "myrepo",
            &[t],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &RepairFacts::default(),
        );
        assert_eq!(p.items.len(), 1);
        assert!(matches!(
            p.items[0].disposition,
            Disposition::Held {
                reason: HoldReason::MissingEvidence,
                ..
            }
        ));
    }

    #[test]
    fn git_disputing_the_delivery_record_holds_ambiguous() {
        let t = ticket(
            "TKT-1",
            "myrepo",
            "in_progress",
            delivery_json("abc123", "main", "rat/x/tkt-1"),
        );
        let mut is_ancestor = HashMap::new();
        is_ancestor.insert(("abc123".to_string(), "main".to_string()), false);
        let facts = RepairFacts {
            git: GitFacts {
                is_ancestor,
                ..Default::default()
            },
            ..Default::default()
        };
        let p = plan(
            "myrepo",
            &[t],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &facts,
        );
        assert!(matches!(
            p.items[0].disposition,
            Disposition::Held {
                reason: HoldReason::AmbiguousDelivery,
                ..
            }
        ));
    }

    #[test]
    fn a_protected_path_touch_holds_for_a_human() {
        let t = ticket(
            "TKT-1",
            "myrepo",
            "in_progress",
            delivery_json("abc123", "main", "rat/x/tkt-1"),
        );
        let mut facts = confirmed_facts("abc123", "main");
        facts.protected_touch.insert("abc123".to_string(), true);
        let p = plan(
            "myrepo",
            &[t],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &facts,
        );
        assert!(matches!(
            p.items[0].disposition,
            Disposition::Held {
                reason: HoldReason::ProtectedPathImpact,
                ..
            }
        ));
    }

    #[test]
    fn a_diverged_branch_holds_conflicting_heads() {
        let t = ticket(
            "TKT-1",
            "myrepo",
            "in_progress",
            delivery_json("abc123", "main", "rat/x/tkt-1"),
        );
        let mut facts = confirmed_facts("abc123", "main");
        facts
            .diverged
            .insert(("myrepo".to_string(), "rat/x/tkt-1".to_string()), true);
        let p = plan(
            "myrepo",
            &[t],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &facts,
        );
        assert!(matches!(
            p.items[0].disposition,
            Disposition::Held {
                reason: HoldReason::ConflictingHeads,
                ..
            }
        ));
    }

    #[test]
    fn stale_ownership_is_planned_with_the_live_status_and_assignee() {
        let t = ticket(
            "TKT-1",
            "myrepo",
            "in_progress",
            json!({"assignee": "Whisker"}),
        );
        let a = agent("Whisker", Some("TKT-1"), AgentState::Dismissed);
        let p = plan(
            "myrepo",
            &[t],
            &[a],
            &HashSet::new(),
            &HashSet::new(),
            &RepairFacts::default(),
        );
        assert_eq!(p.items.len(), 1);
        assert_eq!(p.items[0].kind, vkind::TERMINAL_ASSIGNEE_ACTIVE_WORK);
        match &p.items[0].disposition {
            Disposition::Planned(RepairAction::ClearStaleOwnership { status, assignee }) => {
                assert_eq!(status, "in_progress");
                assert_eq!(assignee.as_deref(), Some("Whisker"));
            }
            other => panic!("expected a planned ownership clear, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn apply_clears_stale_ownership_when_the_agent_is_still_confirmed_terminal() {
        let space = Space::open_in_memory().unwrap();
        let tickets = Tickets::new(space.clone(), "castle".into());
        seed(
            &space,
            "TKT-1",
            "in_progress",
            json!({"assignee": "Whisker"}),
        );

        let live = tickets.list(Some("myrepo".into()), None, None).unwrap();
        let a = agent("Whisker", Some("TKT-1"), AgentState::Dismissed);
        let p = plan(
            "myrepo",
            &live,
            std::slice::from_ref(&a),
            &HashSet::new(),
            &HashSet::new(),
            &RepairFacts::default(),
        );
        assert_eq!(p.items.len(), 1);

        let report = apply(p, &ctx(&tickets, &space, &[a])).await.unwrap();
        assert!(matches!(report.results[0].outcome, Outcome::Applied { .. }));
        let stored = tickets.get("TKT-1").unwrap().unwrap();
        assert_eq!(stored.payload["status"], "open");
        assert_eq!(stored.payload["assignee"], Value::Null);
    }

    #[tokio::test]
    async fn apply_holds_stale_ownership_when_the_agent_resumed_since_planning() {
        // Regression for the gap Triss flagged: the plan is built from one
        // agent snapshot where Whisker has settled terminal, but by the time
        // apply() runs, Whisker has resumed under the same name (a respawn,
        // a retry) and is live again. The ticket's own status/assignee
        // fields never changed, so the ticket-side CAS in
        // `Tickets::repair_clear_stale_ownership` alone sees no drift and
        // would clear ownership out from under a now-live agent. The fresh
        // `agents` re-check in `execute` must catch this and hold with zero
        // mutation instead.
        let space = Space::open_in_memory().unwrap();
        let tickets = Tickets::new(space.clone(), "castle".into());
        seed(
            &space,
            "TKT-1",
            "in_progress",
            json!({"assignee": "Whisker"}),
        );

        let live = tickets.list(Some("myrepo".into()), None, None).unwrap();
        let stale_snapshot = [agent("Whisker", Some("TKT-1"), AgentState::Dismissed)];
        let p = plan(
            "myrepo",
            &live,
            &stale_snapshot,
            &HashSet::new(),
            &HashSet::new(),
            &RepairFacts::default(),
        );
        assert_eq!(p.items.len(), 1);

        let resumed = [agent("Whisker", Some("TKT-1"), AgentState::Running)];
        let report = apply(p, &ctx(&tickets, &space, &resumed)).await.unwrap();
        assert!(matches!(
            report.results[0].outcome,
            Outcome::Held {
                reason: HoldReason::EvidenceDrift,
                ..
            }
        ));
        let stored = tickets.get("TKT-1").unwrap().unwrap();
        assert_eq!(
            stored.payload["status"], "in_progress",
            "no mutation: a live agent's ticket must not be reopened out from under it"
        );
        assert_eq!(stored.payload["assignee"], "Whisker");

        // No journal event was written for a repair that never happened.
        let pattern = Pattern::category(Category::Event).identity(REPAIR_APPLIED_IDENTITY);
        assert!(space.scan(&pattern).unwrap().is_empty());
    }

    #[test]
    fn a_live_owner_is_not_flagged_at_all() {
        let t = ticket(
            "TKT-1",
            "myrepo",
            "in_progress",
            json!({"assignee": "Whisker"}),
        );
        let a = agent("Whisker", Some("TKT-1"), AgentState::Running);
        let p = plan(
            "myrepo",
            &[t],
            &[a],
            &HashSet::new(),
            &HashSet::new(),
            &RepairFacts::default(),
        );
        assert!(p.items.is_empty());
    }

    #[test]
    fn other_violation_kinds_never_enter_the_plan() {
        // conflict-held-landing / tracker-contradicts-git / workflow-settled
        // are never in scope here; a ticket that only trips those kinds must
        // produce an empty plan. A ticket whose git ancestry check disagrees
        // is TRACKER_CONTRADICTS_GIT territory in `reconcile::build`, but
        // `delivered_but_open` never looks at git at all, so it is not
        // re-derived here either — nothing to plan.
        let t = ticket(
            "TKT-1",
            "myrepo",
            "closed",
            delivery_json("abc123", "main", "b"),
        );
        let p = plan(
            "myrepo",
            &[t],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &RepairFacts::default(),
        );
        assert!(p.items.is_empty());
    }

    /// Seed exactly one raw ticket tuple, bypassing `Tickets::create` — a
    /// second, independently-created tuple under the same identity would
    /// make `Tickets::list`/`get` ambiguous about which one is live, which
    /// is not the shape any of these tests want.
    fn seed(space: &Space, id: &str, status: &str, extra: Value) {
        space.out(ticket(id, "myrepo", status, extra)).unwrap();
    }

    #[tokio::test]
    async fn dry_run_never_mutates_and_reports_would_apply() {
        let space = Space::open_in_memory().unwrap();
        let tickets = Tickets::new(space.clone(), "castle".into());
        seed(
            &space,
            "TKT-1",
            "in_progress",
            delivery_json("abc123", "main", "rat/x/tkt-1"),
        );

        let live = tickets.list(Some("myrepo".into()), None, None).unwrap();
        let facts = confirmed_facts("abc123", "main");
        let p = plan(
            "myrepo",
            &live,
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &facts,
        );
        assert_eq!(p.items.len(), 1);
        let report = dry_run(p);
        assert_eq!(report.mode, "dry_run");
        assert!(matches!(
            report.results[0].outcome,
            Outcome::WouldApply { .. }
        ));
        // Zero mutation: the ticket is still open.
        assert_eq!(
            tickets.get("TKT-1").unwrap().unwrap().payload["status"],
            "in_progress"
        );
    }

    #[tokio::test]
    async fn apply_closes_the_ticket_and_journals_the_repair() {
        let space = Space::open_in_memory().unwrap();
        let tickets = Tickets::new(space.clone(), "castle".into());
        seed(
            &space,
            "TKT-1",
            "in_progress",
            delivery_json("abc123", "main", "rat/x/tkt-1"),
        );

        let live = tickets.list(Some("myrepo".into()), None, None).unwrap();
        let facts = confirmed_facts("abc123", "main");
        let p = plan(
            "myrepo",
            &live,
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &facts,
        );
        let report = apply(p, &ctx(&tickets, &space, &[])).await.unwrap();
        assert_eq!(report.mode, "apply");
        let journal_id = match &report.results[0].outcome {
            Outcome::Applied { journal, .. } => journal.clone(),
            other => panic!("expected Applied, got {other:?}"),
        };
        assert!(!journal_id.is_empty());
        assert_eq!(
            tickets.get("TKT-1").unwrap().unwrap().payload["status"],
            "closed"
        );

        // The journal/announcement event is durable and carries the evidence
        // the violation was proven from, not just a human-readable summary —
        // the parent acceptance criteria requires the repair be "announced
        // with its evidence and authority class", and only the journal event
        // is durable (the RepairResult it was built from is a live-request
        // return value, gone once the RPC response is sent).
        let pattern = Pattern::category(Category::Event).identity(REPAIR_APPLIED_IDENTITY);
        let events = space.scan(&pattern).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload["authority"], MECHANICAL_AUTHORITY);
        let journaled_evidence = events[0].payload["evidence"]
            .as_array()
            .expect("journal event must carry the evidence array, not just a description")
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            journaled_evidence,
            vec![
                "ticket:TKT-1".to_string(),
                "delivery.merge_commit:abc123".to_string(),
                "delivery.target:main".to_string(),
                "ticket.status:in_progress".to_string(),
            ],
            "journal must persist the same evidence vector the violation was proven from"
        );

        // Replaying against fresh state is an idempotent no-op: the repaired
        // ticket no longer even trips the violation, so there is nothing
        // left to (re-)plan.
        let live_after = tickets.list(Some("myrepo".into()), None, None).unwrap();
        let p2 = plan(
            "myrepo",
            &live_after,
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &facts,
        );
        assert!(
            p2.items.is_empty(),
            "the repaired ticket no longer trips the violation"
        );
    }

    #[tokio::test]
    async fn a_replayed_marker_makes_a_stale_plan_item_a_no_op() {
        // Simulate a duplicate apply RPC racing itself: the marker for a
        // violation id already exists even though the plan item is (for the
        // sake of this test) constructed by hand rather than re-derived.
        let space = Space::open_in_memory().unwrap();
        let tickets = Tickets::new(space.clone(), "castle".into());
        seed(
            &space,
            "TKT-1",
            "in_progress",
            delivery_json("abc123", "main", "rat/x/tkt-1"),
        );
        let violation_id = format!("{}:TKT-1", vkind::DELIVERED_BUT_OPEN);
        let evidence = ["pre-existing evidence".to_string()];
        announce(
            &space,
            "castle",
            RepairFocus {
                violation_id: &violation_id,
                kind: vkind::DELIVERED_BUT_OPEN,
                scope: "myrepo",
                evidence: &evidence,
            },
            "TKT-1",
            "pre-existing marker",
        )
        .unwrap();

        let plan = RepairPlan {
            scope: "myrepo".into(),
            items: vec![RepairItem {
                violation_id,
                kind: vkind::DELIVERED_BUT_OPEN.to_string(),
                scope: "myrepo".into(),
                subject: "TKT-1".into(),
                detail: "test".into(),
                evidence: vec![],
                disposition: Disposition::Planned(RepairAction::CloseDelivered {
                    merge_commit: "abc123".into(),
                }),
            }],
        };
        let report = apply(plan, &ctx(&tickets, &space, &[])).await.unwrap();
        assert!(matches!(
            report.results[0].outcome,
            Outcome::AlreadyApplied { .. }
        ));
        // Untouched: the ticket the marker referenced never got a real CAS
        // write from this call, so it is still whatever it was.
        assert_eq!(
            tickets.get("TKT-1").unwrap().unwrap().payload["status"],
            "in_progress"
        );
    }

    #[tokio::test]
    async fn evidence_drift_at_apply_time_holds_instead_of_mutating() {
        let space = Space::open_in_memory().unwrap();
        let tickets = Tickets::new(space.clone(), "castle".into());
        seed(
            &space,
            "TKT-1",
            "in_progress",
            delivery_json("abc123", "main", "rat/x/tkt-1"),
        );

        let live = tickets.list(Some("myrepo".into()), None, None).unwrap();
        let facts = confirmed_facts("abc123", "main");
        let p = plan(
            "myrepo",
            &live,
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &facts,
        );
        assert_eq!(p.items.len(), 1);
        // Evidence goes stale between plan and apply: someone else already
        // closed the ticket by hand.
        tickets.set_status("TKT-1", "closed").await.unwrap();

        let report = apply(p, &ctx(&tickets, &space, &[])).await.unwrap();
        assert!(matches!(
            report.results[0].outcome,
            Outcome::Held {
                reason: HoldReason::EvidenceDrift,
                ..
            }
        ));
    }
}

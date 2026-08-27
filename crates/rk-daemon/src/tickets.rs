//! Tickets: durable work items, stored as `task` tuples in the tuplespace.
//!
//! A ticket is a `task`-category tuple whose `identity` is `TKT-<ulid>` and whose
//! payload carries `{title, body, status, parent, ...}`. The ULID is minted
//! locally but globally unique, so two castles can create tickets concurrently
//! without identity collisions. Nothing collects
//! `session`/`furniture` tuples, so a ticket persists as a backlog item until
//! explicitly closed — and because tickets carry a repo *name* (not a path),
//! they replicate across castles through git-notes sync as a shared backlog.
//!
//! All mutations (create and update) serialize through one lock so the
//! take-and-replace of an update cannot interleave with another mutation.

use rk_core::action::canonical_digest;
use rk_core::id::RecordId;
use rk_core::tuple::{Category, Lifecycle, Pattern, Tuple};
use rk_space::Space;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{Mutex, MutexGuard};
use tracing::warn;

/// The status lifecycle a ticket may move through.
pub const STATUSES: &[&str] = &[
    "open",
    "claimed",
    "in_progress",
    "blocked",
    "done",
    "closed",
];

pub(crate) const ID_PREFIX: &str = "TKT-";

/// Payload key holding a ticket's durable delivery record — the answer to
/// "did this ticket ship", written once by the landing pipeline at land time
/// (P1b). Before this existed, delivery was inferred from live branch refs,
/// which landing itself deletes: a landed ticket's branch is gone, so the
/// inference read "not delivered" and the ticket sat `in_progress` forever
/// (probe O14/O16, 14 tickets observed at once). The record is git + rk state
/// only — a merge commit sha and the branch/target it landed on — so it
/// carries no assumption about the repo's language or build system.
pub const DELIVERY_FIELD: &str = "delivery";

/// The durable proof that a ticket's work reached its target branch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeliveryRecord {
    /// The merge commit the land produced. This is the whole point of the
    /// record: it stays true after the branch ref is deleted.
    pub merge_commit: String,
    /// The branch that landed (kept for provenance/debugging; never read as
    /// a liveness signal — the ref is typically gone by the time anyone asks).
    pub branch: String,
    /// The branch it landed ON.
    pub target: String,
    #[serde(default)]
    pub landed_at: String,
}

/// Result of a compare-and-swap repair write (`repair_close_delivered`,
/// `repair_clear_stale_ownership`): either the precondition matched the
/// ticket's live payload and the mutation landed, the ticket had already
/// drifted away from the caller's evidence (fails closed, zero mutation), or
/// the ticket no longer exists at all.
#[derive(Debug)]
pub enum CasOutcome {
    Applied(Tuple),
    Drifted { detail: String },
    Gone,
}

/// Outcome of the content-bound, write-once delivery path used when an
/// operator records work that landed outside the agent landing pipeline.
#[derive(Debug)]
pub enum DeliveryWrite {
    Recorded(Tuple),
    Already(Tuple),
}

/// Read a ticket tuple's delivery record, if it carries one.
pub fn delivery_of(ticket: &Tuple) -> Option<DeliveryRecord> {
    serde_json::from_value(ticket.payload.get(DELIVERY_FIELD)?.clone()).ok()
}

/// THE delivery predicate: has this ticket's work actually landed? Answered
/// from the durable record alone — never from branch existence, and never
/// from a ticket merely being marked `done` (the "approved but never merged"
/// class, TKT-18/46/147).
pub fn is_delivered(ticket: &Tuple) -> bool {
    delivery_of(ticket).is_some_and(|d| !d.merge_commit.is_empty())
}

fn system_scope() -> String {
    rk_core::tuple::SYSTEM_SCOPE.to_string()
}

fn default_priority() -> String {
    "normal".to_string()
}

/// Fields for a new ticket. Deserialized directly from the `ticket.new` RPC.
#[derive(Deserialize)]
pub struct NewTicket {
    pub title: String,
    #[serde(default)]
    pub body: Option<String>,
    /// Explicit scope override. When absent, a sub-ticket (`parent` set)
    /// inherits its parent's scope; a top-level ticket defaults to
    /// [`SYSTEM_SCOPE`].
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub parent: Option<String>,
    #[serde(default = "default_priority")]
    pub priority: String,
    #[serde(default)]
    pub labels: Vec<String>,
    /// Ticket ids this one is blocked by (must be done/closed before it is ready).
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Who filed it (an agent name, or the castle for human-filed tickets).
    #[serde(default)]
    pub created_by: Option<String>,
    /// Dedupe key for reactor-coalesced tickets: a stable topic identifier that
    /// makes a still-open ticket its own "already filed" guard. `None` for
    /// ordinary tickets; written into the payload only when present.
    #[serde(default)]
    pub coalesce_key: Option<String>,
}

/// A partial update to a ticket. Every field is optional; only present fields
/// are written.
#[derive(Deserialize, Default)]
pub struct TicketChanges {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default)]
    pub priority: Option<String>,
    #[serde(default)]
    pub assignee: Option<String>,
    #[serde(default)]
    pub parent: Option<String>,
    /// Labels to add, deduped against the existing set. Additive rather than a
    /// wholesale replace so a backlog groom can tag a ticket (e.g. with a
    /// `frozen:<subsystem>` tag) without clobbering labels another pass set.
    #[serde(default)]
    pub add_labels: Vec<String>,
    /// Labels to remove — how a freeze tag is lifted when a subsystem thaws.
    #[serde(default)]
    pub remove_labels: Vec<String>,
}

pub struct Tickets {
    space: Space,
    castle: String,
    lock: Mutex<()>,
}

pub(crate) struct TicketMutationGuard<'a> {
    _guard: MutexGuard<'a, ()>,
}

impl Tickets {
    pub fn new(space: Space, castle: String) -> Self {
        Self {
            space,
            castle,
            lock: Mutex::new(()),
        }
    }

    /// Mint a globally unique ticket id. RecordId is a ULID, so the identity
    /// remains sortable while avoiding the local-maximum collision that used
    /// to make two castles both create `TKT-1`.
    fn next_id(&self) -> rk_core::Result<String> {
        Ok(format!("{ID_PREFIX}{}", RecordId::new()))
    }

    pub async fn create(&self, t: NewTicket) -> rk_core::Result<Tuple> {
        self.create_idempotent(t)
            .await
            .map(|(ticket, _created)| ticket)
    }

    /// Create once for a stable coalesce key. The lookup and insert share the
    /// ticket mutation lock, so concurrent/restarted factory execution cannot
    /// mint two tickets for the same graph node.
    pub async fn create_idempotent(&self, t: NewTicket) -> rk_core::Result<(Tuple, bool)> {
        let guard = self.mutation_guard().await;
        self.create_idempotent_locked(&guard, t)
    }

    pub(crate) async fn mutation_guard(&self) -> TicketMutationGuard<'_> {
        TicketMutationGuard {
            _guard: self.lock.lock().await,
        }
    }

    pub(crate) fn create_idempotent_locked(
        &self,
        _guard: &TicketMutationGuard<'_>,
        t: NewTicket,
    ) -> rk_core::Result<(Tuple, bool)> {
        // Explicit scope wins. Otherwise a sub-ticket inherits its parent's
        // scope, so decomposing a repo-scoped ticket doesn't silently drop the
        // sub-tickets into "system" (which breaks `rk spawn --ticket` and the
        // steward-on-completion trigger match).
        let scope = match &t.scope {
            Some(s) => s.clone(),
            None => match t.parent.as_deref().map(|p| self.get(p)).transpose()? {
                Some(Some(parent)) => parent.scope,
                _ => system_scope(),
            },
        };
        if let Some(key) = t.coalesce_key.as_deref() {
            if let Some(existing) = self
                .space
                .scan(&Pattern::category(Category::Task).scope(&scope))?
                .into_iter()
                .find(|ticket| {
                    ticket.payload.get("coalesce_key").and_then(Value::as_str) == Some(key)
                })
            {
                return Ok((existing, false));
            }
        }
        // A freeze tag naming an unknown subsystem, or a carve-out naming an
        // unratified reason, is rejected here rather than stored: the dispatch
        // predicate fails closed on both, so an unvalidated typo would freeze a
        // ticket silently and forever.
        rk_core::freeze::validate_labels(&t.labels).map_err(rk_core::Error::other)?;
        // Dependencies must reference tickets that already exist. (A brand-new
        // ticket has no dependents, so it can never close a cycle here.)
        for dep in &t.depends_on {
            if self.get(dep)?.is_none() {
                return Err(rk_core::Error::other(format!(
                    "cannot depend on missing ticket: {dep}"
                )));
            }
        }
        let id = self.next_id()?;
        let now = chrono::Utc::now().to_rfc3339();
        let mut payload = json!({
            "title": t.title,
            "body": t.body.unwrap_or_default(),
            "status": "open",
            "parent": t.parent,
            "priority": t.priority,
            "labels": t.labels,
            "depends_on": t.depends_on,
            "assignee": Value::Null,
            "created_by": t.created_by.unwrap_or_else(|| self.castle.clone()),
            "created_at": now,
            "updated_at": now,
        });
        if let Some(key) = t.coalesce_key {
            payload["coalesce_key"] = json!(key);
        }
        let tuple = Tuple::new(Category::Task, scope, id, self.castle.clone(), payload)
            .with_lifecycle(Lifecycle::Session);
        self.space.out(tuple.clone())?;
        // A brand-new ticket is itself a readiness edge when it has no
        // unresolved dependency (no deps at all, or every named dep already
        // delivered) — it is actionable from the instant it exists, so that
        // is when its `TicketReady` span settles. Best-effort: a failed scan
        // never fails the creation that already landed.
        if let Ok(by_id) = self.all_by_id() {
            self.record_ready_if_unblocked(&tuple, &by_id);
        }
        Ok((tuple, true))
    }

    /// Deterministic CAS digest over the repository's current ticket identities
    /// and payloads. This deliberately excludes tuple timestamps/strength so an
    /// unrelated storage rewrite cannot invalidate an approved graph apply.
    pub fn snapshot_digest(&self, scope: &str) -> rk_core::Result<String> {
        self.snapshot_digest_filtered(scope, None)
    }

    pub(crate) fn snapshot_digest_excluding_created_by(
        &self,
        _guard: &TicketMutationGuard<'_>,
        scope: &str,
        created_by: &str,
    ) -> rk_core::Result<String> {
        self.snapshot_digest_filtered(scope, Some(created_by))
    }

    fn snapshot_digest_filtered(
        &self,
        scope: &str,
        excluded_created_by: Option<&str>,
    ) -> rk_core::Result<String> {
        let mut rows = self
            .list(Some(scope.to_string()), None, None)?
            .into_iter()
            .filter(|ticket| {
                excluded_created_by.is_none_or(|excluded| {
                    ticket.payload.get("created_by").and_then(Value::as_str) != Some(excluded)
                })
            })
            .map(|ticket| (ticket.identity, ticket.payload))
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        canonical_digest(&rows)
    }

    pub fn get(&self, id: &str) -> rk_core::Result<Option<Tuple>> {
        let mut pattern = Pattern::category(Category::Task);
        pattern.identity = Some(id.to_string());
        Ok(self.space.scan(&pattern)?.into_iter().next())
    }

    pub fn list(
        &self,
        scope: Option<String>,
        status: Option<String>,
        parent: Option<String>,
    ) -> rk_core::Result<Vec<Tuple>> {
        let mut pattern = Pattern::category(Category::Task);
        pattern.scope = scope;
        let mut tickets: Vec<Tuple> = self
            .space
            .scan(&pattern)?
            .into_iter()
            .filter(|t| t.identity.starts_with(ID_PREFIX))
            .filter(|t| {
                status
                    .as_deref()
                    .is_none_or(|s| t.payload.get("status").and_then(Value::as_str) == Some(s))
            })
            .filter(|t| {
                parent
                    .as_deref()
                    .is_none_or(|p| t.payload.get("parent").and_then(Value::as_str) == Some(p))
            })
            .collect();
        tickets.sort_by_key(|t| t.created_at);
        Ok(tickets)
    }

    pub async fn update(&self, id: &str, changes: TicketChanges) -> rk_core::Result<Tuple> {
        if let Some(s) = &changes.status {
            if !STATUSES.contains(&s.as_str()) {
                return Err(rk_core::Error::other(format!(
                    "invalid status '{s}' (allowed: {})",
                    STATUSES.join(", ")
                )));
            }
        }
        // Reject a malformed freeze tag at the point it is written, so a typo is
        // a loud error at groom time rather than a ticket that silently never
        // drains (the predicate itself fails closed — see rk_core::freeze).
        rk_core::freeze::validate_labels(&changes.add_labels).map_err(rk_core::Error::other)?;
        let _guard = self.lock.lock().await;
        if let Some(next) = changes.status.as_deref() {
            let current = self
                .get(id)?
                .ok_or_else(|| rk_core::Error::other(format!("no such ticket: {id}")))?;
            let previous = current
                .payload
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("open");
            if !valid_transition(previous, next) {
                return Err(rk_core::Error::other(format!(
                    "invalid ticket status transition: {previous} -> {next}"
                )));
            }
        }
        self.edit(id, |obj| {
            if let Some(v) = changes.status {
                obj.insert("status".into(), json!(v));
            }
            if let Some(v) = changes.title {
                obj.insert("title".into(), json!(v));
            }
            if let Some(v) = changes.body {
                obj.insert("body".into(), json!(v));
            }
            if let Some(v) = changes.priority {
                obj.insert("priority".into(), json!(v));
            }
            if let Some(v) = changes.assignee {
                obj.insert("assignee".into(), json!(v));
            }
            if let Some(v) = changes.parent {
                obj.insert("parent".into(), json!(v));
            }
            if !changes.add_labels.is_empty() || !changes.remove_labels.is_empty() {
                let labels = obj.entry("labels").or_insert_with(|| json!([]));
                if let Some(arr) = labels.as_array_mut() {
                    arr.retain(|l| {
                        l.as_str()
                            .is_none_or(|l| !changes.remove_labels.iter().any(|r| r == l))
                    });
                    for label in &changes.add_labels {
                        if !arr.iter().any(|l| l.as_str() == Some(label.as_str())) {
                            arr.push(json!(label));
                        }
                    }
                }
            }
        })
        .await
    }

    /// Record a successful land on the ticket and close it in the same write
    /// (P1b). This is the post-landing writer the pipeline previously lacked:
    /// the merge commit becomes the durable delivery record, and the ticket
    /// reaches a terminal state without an operator touching it.
    ///
    /// Deliberately bypasses [`valid_transition`]: a land is ground truth
    /// about the world, not a workflow request, so it must be recordable from
    /// ANY prior status — including legacy tickets already marked terminal
    /// without a merge commit. Idempotent by
    /// merge commit: re-recording the same commit rewrites the same record and
    /// does not re-emit `ticket_closed` (the [`edit`] close-edge guard already
    /// fires only on the non-terminal → terminal crossing).
    ///
    /// [`edit`]: Self::edit
    pub async fn record_delivery(
        &self,
        id: &str,
        record: &DeliveryRecord,
    ) -> rk_core::Result<Tuple> {
        let _guard = self.lock.lock().await;
        let value = serde_json::to_value(record)
            .map_err(|e| rk_core::Error::other(format!("delivery record: {e}")))?;
        self.edit(id, move |obj| {
            obj.insert(DELIVERY_FIELD.into(), value);
            obj.insert("status".into(), json!("closed"));
        })
        .await
    }

    /// Record one delivery without ever replacing different provenance.
    /// The lookup and write share the ticket mutation lock, so concurrent
    /// operator retries either replay the same fact or fail closed.
    pub async fn record_delivery_once(
        &self,
        id: &str,
        record: &DeliveryRecord,
    ) -> rk_core::Result<DeliveryWrite> {
        let _guard = self.lock.lock().await;
        let Some(existing) = self.get(id)? else {
            return Err(rk_core::Error::other(format!("no such ticket: {id}")));
        };
        if let Some(prior) = delivery_of(&existing) {
            return if prior.merge_commit == record.merge_commit
                && prior.branch == record.branch
                && prior.target == record.target
            {
                Ok(DeliveryWrite::Already(existing))
            } else {
                Err(rk_core::Error::other(format!(
                    "ticket {id} already has a different delivery record"
                )))
            };
        }
        let value = serde_json::to_value(record)
            .map_err(|e| rk_core::Error::other(format!("delivery record: {e}")))?;
        let ticket = self
            .edit(id, move |obj| {
                obj.insert(DELIVERY_FIELD.into(), value);
                obj.insert("status".into(), json!("closed"));
            })
            .await?;
        Ok(DeliveryWrite::Recorded(ticket))
    }

    /// Drop a ticket's delivery record and reopen it at `status` — the revert
    /// half of [`record_delivery`]. A reverted merge must stop reading as
    /// delivered, otherwise the work is durably lost: the branch is gone AND
    /// the record would still claim it shipped. Returns `false` if the ticket
    /// does not exist or carried no record to clear.
    ///
    /// [`record_delivery`]: Self::record_delivery
    pub async fn clear_delivery(&self, id: &str, status: &str) -> rk_core::Result<bool> {
        let _guard = self.lock.lock().await;
        let Some(existing) = self.get(id)? else {
            return Ok(false);
        };
        if delivery_of(&existing).is_none() {
            return Ok(false);
        }
        let status = status.to_string();
        self.edit(id, move |obj| {
            obj.remove(DELIVERY_FIELD);
            obj.insert("status".into(), json!(status));
        })
        .await?;
        Ok(true)
    }

    /// The delivery record for `id`, if it exists and has landed.
    pub fn delivery(&self, id: &str) -> rk_core::Result<Option<DeliveryRecord>> {
        Ok(self.get(id)?.as_ref().and_then(delivery_of))
    }

    /// Atomically claim an open ticket for a backlog-drain: compare-and-set
    /// `open` → `in_progress`. Returns `true` if this call won the claim (the
    /// ticket existed, was `open`, and is now `in_progress`), `false` if it was
    /// already claimed (any non-`open` status) or no longer exists. Serialized
    /// through the same lock as every other mutation and executed as a single
    /// take-and-replace, so of two concurrent drains racing for one ticket
    /// exactly one wins and the loser leaves the ticket untouched.
    pub async fn claim(&self, id: &str) -> rk_core::Result<bool> {
        let _guard = self.lock.lock().await;
        let Some(existing) = self.take_ticket(id).await? else {
            return Ok(false);
        };
        let open = existing.payload.get("status").and_then(Value::as_str) == Some("open");
        let scope = existing.scope.clone();
        let mut payload = existing.payload.clone();
        if open {
            if let Some(obj) = payload.as_object_mut() {
                obj.insert("status".into(), json!("in_progress"));
                obj.insert("updated_at".into(), json!(chrono::Utc::now().to_rfc3339()));
            }
        }
        // Always write the ticket back — with the new status on a win, unchanged
        // on a loss — so a losing claim never destroys the ticket it took.
        self.space.out(with_payload(existing, payload))?;
        if open {
            // Best-effort span: the claim itself already landed above and must
            // never be undone by a telemetry failure.
            let _ = crate::span::record_phase_span(
                &self.space,
                &scope,
                &self.castle,
                &crate::span::PhaseSpan::new(id, crate::span::Phase::Claimed),
            );
        }
        Ok(open)
    }

    /// Set just the status (used by the supervisor to close a ticket's loop
    /// when its rat completes or is merged). No-op error if the ticket is gone.
    pub async fn set_status(&self, id: &str, status: &str) -> rk_core::Result<Tuple> {
        self.update(
            id,
            TicketChanges {
                status: Some(status.to_string()),
                ..Default::default()
            },
        )
        .await
    }

    /// Atomically reopen an orphaned `in_progress` ticket: compare-and-set
    /// `in_progress` -> `open`. Returns `true` if this call performed the
    /// reopen, `false` if the ticket no longer exists or had already moved on
    /// (a live owner advanced it, or it reached `done`/`closed`) between the
    /// caller's staleness check and this write. The mirror of [`claim`]'s
    /// `open` -> `in_progress` CAS, so the B9 orphaned-ticket sweep can never
    /// clobber a ticket its own rat finished racing the sweep's read.
    ///
    /// [`claim`]: Self::claim
    pub async fn reopen_if_in_progress(&self, id: &str) -> rk_core::Result<bool> {
        let _guard = self.lock.lock().await;
        let Some(existing) = self.take_ticket(id).await? else {
            return Ok(false);
        };
        let in_progress =
            existing.payload.get("status").and_then(Value::as_str) == Some("in_progress");
        let mut payload = existing.payload.clone();
        if in_progress {
            if let Some(obj) = payload.as_object_mut() {
                obj.insert("status".into(), json!("open"));
                obj.insert("updated_at".into(), json!(chrono::Utc::now().to_rfc3339()));
            }
        }
        // Always write the ticket back — with the new status on a win,
        // unchanged on a loss — so a losing reopen never destroys the ticket
        // it took (same reasoning as `claim`).
        self.space.out(with_payload(existing, payload))?;
        Ok(in_progress)
    }

    /// Reopen a `done` or `closed` ticket as an explicit recovery action
    /// (used by `rk revert` and the operator/steward-only `rk ticket
    /// reopen`). Ordinary updates cannot move a ticket backwards out of
    /// either terminal state — [`valid_transition`] only allows `done` ->
    /// `closed` — so this is the sole path back to the backlog once a
    /// ticket has landed there, regardless of which terminal state it is in.
    pub async fn reopen(&self, id: &str, status: &str) -> rk_core::Result<Tuple> {
        if !matches!(status, "open" | "blocked") {
            return Err(rk_core::Error::other(format!(
                "reopen status must be open or blocked, got '{status}'"
            )));
        }
        let _guard = self.lock.lock().await;
        self.edit(id, |obj| {
            obj.insert("status".into(), json!(status));
        })
        .await
    }

    /// Add a `id depends-on dep` edge, rejecting self-loops, missing tickets,
    /// and any edge that would close a cycle.
    pub async fn add_dep(&self, id: &str, dep: &str) -> rk_core::Result<Tuple> {
        let guard = self.mutation_guard().await;
        self.add_dep_locked(&guard, id, dep).await
    }

    pub(crate) async fn add_dep_locked(
        &self,
        _guard: &TicketMutationGuard<'_>,
        id: &str,
        dep: &str,
    ) -> rk_core::Result<Tuple> {
        if id == dep {
            return Err(rk_core::Error::other("a ticket cannot depend on itself"));
        }
        let by_id = self.all_by_id()?;
        if !by_id.contains_key(id) {
            return Err(rk_core::Error::other(format!("no such ticket: {id}")));
        }
        if !by_id.contains_key(dep) {
            return Err(rk_core::Error::other(format!("no such ticket: {dep}")));
        }
        // Adding id -> dep closes a cycle iff id is already reachable from dep.
        if reaches(dep, id, &by_id) {
            return Err(rk_core::Error::other(format!(
                "{id} depends-on {dep} would create a dependency cycle"
            )));
        }
        self.edit(id, |obj| {
            let deps = obj.entry("depends_on").or_insert_with(|| json!([]));
            if let Some(arr) = deps.as_array_mut() {
                if !arr.iter().any(|d| d.as_str() == Some(dep)) {
                    arr.push(json!(dep));
                }
            }
        })
        .await
    }

    pub async fn remove_dep(&self, id: &str, dep: &str) -> rk_core::Result<Tuple> {
        let _guard = self.lock.lock().await;
        if self.get(id)?.is_none() {
            return Err(rk_core::Error::other(format!("no such ticket: {id}")));
        }
        self.edit(id, |obj| {
            if let Some(arr) = obj.get_mut("depends_on").and_then(Value::as_array_mut) {
                arr.retain(|d| d.as_str() != Some(dep));
            }
        })
        .await
    }

    /// Open tickets whose every dependency has a durable delivery record —
    /// actionable right now. Status alone is not proof that work landed.
    pub fn ready(&self, scope: Option<String>) -> rk_core::Result<Vec<Tuple>> {
        let by_id = self.all_by_id()?;
        let mut ready: Vec<Tuple> = by_id
            .values()
            .filter(|t| scope.as_deref().is_none_or(|s| t.scope == s))
            .filter(|t| t.payload.get("status").and_then(Value::as_str) == Some("open"))
            .filter(|t| !is_blocked(t, &by_id))
            .cloned()
            .collect();
        ready.sort_by_key(|t| t.created_at);
        Ok(ready)
    }

    /// The unfinished dependency ids blocking `id` (empty = ready to work), or
    /// `None` if the ticket does not exist.
    pub fn blockers(&self, id: &str) -> rk_core::Result<Option<Vec<String>>> {
        let by_id = self.all_by_id()?;
        let Some(ticket) = by_id.get(id) else {
            return Ok(None);
        };
        Ok(Some(
            deps_of(ticket)
                .into_iter()
                .filter(|d| by_id.get(d).is_some_and(|dep| !is_delivered(dep)))
                .collect(),
        ))
    }

    /// Of the given tickets, which ids are currently blocked (for list display).
    pub fn blocked_ids(&self, tickets: &[Tuple]) -> rk_core::Result<Vec<String>> {
        let by_id = self.all_by_id()?;
        Ok(tickets
            .iter()
            .filter(|t| is_blocked(t, &by_id))
            .map(|t| t.identity.clone())
            .collect())
    }

    fn all_by_id(&self) -> rk_core::Result<HashMap<String, Tuple>> {
        Ok(self
            .space
            .scan(&Pattern::category(Category::Task))?
            .into_iter()
            .filter(|t| t.identity.starts_with(ID_PREFIX))
            .map(|t| (t.identity.clone(), t))
            .collect())
    }

    /// Existence-checked destructive take of a ticket tuple by id. Returns
    /// `None` if no such ticket exists, so callers short-circuit rather than
    /// block a `take` that would wait out its whole timeout. Assumes
    /// `self.lock` is already held by the caller.
    async fn take_ticket(&self, id: &str) -> rk_core::Result<Option<Tuple>> {
        if self.get(id)?.is_none() {
            return Ok(None);
        }
        let mut pattern = Pattern::category(Category::Task);
        pattern.identity = Some(id.to_string());
        self.space.take(&pattern, Duration::from_secs(2)).await
    }

    /// Take the ticket, apply `f` to its payload object, stamp `updated_at`, and
    /// write it back. Assumes `self.lock` is already held by the caller.
    async fn edit(
        &self,
        id: &str,
        f: impl FnOnce(&mut serde_json::Map<String, Value>),
    ) -> rk_core::Result<Tuple> {
        let existing = self
            .take_ticket(id)
            .await?
            .ok_or_else(|| rk_core::Error::other(format!("no such ticket: {id}")))?;

        let was_delivered = is_delivered(&existing);
        let mut payload = existing.payload.clone();
        let obj = payload
            .as_object_mut()
            .ok_or_else(|| rk_core::Error::other("ticket payload is not an object"))?;
        f(obj);
        obj.insert("updated_at".into(), json!(chrono::Utc::now().to_rfc3339()));

        let updated = with_payload(existing, payload);
        self.space.out(updated.clone())?;
        // Emit a `ticket_closed` event on the undelivered → delivered edge —
        // the moment a ticket's dependents can unblock (TKT-56). A reactor trigger
        // matching this event hands the now-ready backlog to a drain workflow,
        // turning the dependency DAG into a self-advancing pipeline instead of
        // one that waits for the next drain sweep. Only the crossing edge fires
        // (a done→closed re-close, or a non-status edit, does not), so a closed
        // ticket's dependents are announced exactly once. Best-effort: a failed
        // emit never fails the status change that already landed.
        if is_delivered(&updated) && !was_delivered {
            self.emit_ticket_closed(&updated);
        }
        Ok(updated)
    }

    /// Mechanically close a ticket whose own delivery record already proves
    /// it shipped (`crate::reconcile::kind::DELIVERED_BUT_OPEN`), but only if
    /// the ticket's live payload still carries exactly the merge commit the
    /// caller verified before deciding to repair it — a compare-and-swap so a
    /// concurrent status change (a human closing it by hand, a second repair
    /// racing this one, a revert) is never clobbered. Takes the ticket
    /// destructively, checks the precondition in memory under the same lock
    /// every other mutation serializes through, and always writes the ticket
    /// back — mutated on a match, byte-identical on a miss — so a losing
    /// repair never destroys the ticket it took (same shape as [`claim`]).
    ///
    /// Deliberately bypasses [`valid_transition`]: like [`record_delivery`],
    /// a durable delivery record is ground truth, not a workflow request, so
    /// the close must be recordable from any prior non-terminal status.
    ///
    /// [`claim`]: Self::claim
    /// [`record_delivery`]: Self::record_delivery
    pub async fn repair_close_delivered(
        &self,
        id: &str,
        expected_merge_commit: &str,
    ) -> rk_core::Result<CasOutcome> {
        let _guard = self.lock.lock().await;
        let Some(existing) = self.take_ticket(id).await? else {
            return Ok(CasOutcome::Gone);
        };
        let status = existing
            .payload
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("open")
            .to_string();
        let current_commit = delivery_of(&existing).map(|d| d.merge_commit);
        let matches = !matches!(status.as_str(), "done" | "closed")
            && current_commit.as_deref() == Some(expected_merge_commit);
        let mut payload = existing.payload.clone();
        if matches {
            if let Some(obj) = payload.as_object_mut() {
                obj.insert("status".into(), json!("closed"));
                obj.insert("updated_at".into(), json!(chrono::Utc::now().to_rfc3339()));
            }
        }
        let updated = with_payload(existing, payload);
        self.space.out(updated.clone())?;
        if matches {
            Ok(CasOutcome::Applied(updated))
        } else {
            Ok(CasOutcome::Drifted {
                detail: format!(
                    "expected status not in (done, closed) and delivery.merge_commit == {expected_merge_commit}, found status={status} delivery.merge_commit={current_commit:?}"
                ),
            })
        }
    }

    /// Mechanically clear a stale ownership record
    /// (`crate::reconcile::kind::TERMINAL_ASSIGNEE_ACTIVE_WORK`): the ticket
    /// still names an owner whose agent record has settled terminal with no
    /// hand-off recorded, so the ownership pointer itself is proven wrong by
    /// durable evidence, independent of whatever redispatch decision comes
    /// next (that decision stays [`crate::reconcile::Authority::Orchestrator`]
    /// — this only un-sticks the record). Resets the ticket to `open` and
    /// clears `assignee` so it becomes claimable again.
    ///
    /// Compare-and-swap on `(status, assignee)` exactly as read by the
    /// caller, with the same take-check-always-write-back shape as
    /// [`repair_close_delivered`] — a live owner reclaiming the ticket, or a
    /// concurrent status change, drifts the precondition and the write is a
    /// no-op.
    ///
    /// [`repair_close_delivered`]: Self::repair_close_delivered
    pub async fn repair_clear_stale_ownership(
        &self,
        id: &str,
        expected_status: &str,
        expected_assignee: Option<&str>,
    ) -> rk_core::Result<CasOutcome> {
        let _guard = self.lock.lock().await;
        let Some(existing) = self.take_ticket(id).await? else {
            return Ok(CasOutcome::Gone);
        };
        let status = existing
            .payload
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("open")
            .to_string();
        let assignee = existing
            .payload
            .get("assignee")
            .and_then(Value::as_str)
            .map(str::to_string);
        let matches = status == expected_status && assignee.as_deref() == expected_assignee;
        let mut payload = existing.payload.clone();
        if matches {
            if let Some(obj) = payload.as_object_mut() {
                obj.insert("status".into(), json!("open"));
                obj.insert("assignee".into(), Value::Null);
                obj.insert("updated_at".into(), json!(chrono::Utc::now().to_rfc3339()));
            }
        }
        let updated = with_payload(existing, payload);
        self.space.out(updated.clone())?;
        if matches {
            Ok(CasOutcome::Applied(updated))
        } else {
            Ok(CasOutcome::Drifted {
                detail: format!(
                    "expected status={expected_status} assignee={expected_assignee:?}, found status={status} assignee={assignee:?}"
                ),
            })
        }
    }

    /// Announce a just-closed ticket as an `Event` tuple the reactor can react
    /// to. Scoped to the ticket's repo so a trigger's repo defaults to that repo
    /// (and its fan-out drains that repo's newly-ready backlog).
    fn emit_ticket_closed(&self, ticket: &Tuple) {
        let status = ticket
            .payload
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("done");
        let event = Tuple::new(
            Category::Event,
            ticket.scope.clone(),
            "ticket_closed",
            self.castle.clone(),
            json!({
                "ticket": ticket.identity,
                "status": status,
                "scope": ticket.scope,
            }),
        );
        if let Err(e) = self.space.out(event) {
            warn!(ticket = %ticket.identity, error = %e, "failed to emit ticket_closed event");
        }
        let mut span =
            crate::span::PhaseSpan::new(&ticket.identity, crate::span::Phase::DeliveryClosure)
                .terminal_reason(status.to_string());
        if let Some(record) = delivery_of(ticket) {
            span = span.candidate(record.merge_commit).target(record.target);
        }
        let _ = crate::span::record_phase_span(&self.space, &ticket.scope, &self.castle, &span);

        // The readiness edge Phase::TicketReady was missing a producer for
        // (TKT-01M0QMT83E7YXH6ZXHMQG0VRS6): `ready()` only ever re-derives
        // the backlog on demand, so there was no discrete moment a producer
        // could hang off. This delivered edge IS that moment for every open
        // dependent of `ticket` — if this was the last blocker standing, the
        // dependent just became actionable, so stamp its span here, exactly
        // once (`record_phase_span` dedupes on (task, phase, attempt)).
        if let Ok(by_id) = self.all_by_id() {
            for dependent in by_id.values() {
                if deps_of(dependent).iter().any(|d| d == &ticket.identity) {
                    self.record_ready_if_unblocked(dependent, &by_id);
                }
            }
        }
    }

    /// Stamp `ticket`'s `TicketReady` span if it is open and every one of its
    /// dependencies is already delivered — the shared readiness check behind
    /// both producers: a brand-new ticket with no unresolved dep (creation),
    /// and an existing dependent whose last blocker just landed (the
    /// undelivered → delivered edge in [`emit_ticket_closed`]). Idempotent
    /// and best-effort like every other span write in this module: a still-
    /// blocked ticket is silently skipped, and a failed write never fails
    /// the mutation that triggered this check.
    fn record_ready_if_unblocked(&self, ticket: &Tuple, by_id: &HashMap<String, Tuple>) {
        if ticket.payload.get("status").and_then(Value::as_str) != Some("open") {
            return;
        }
        if is_blocked(ticket, by_id) {
            return;
        }
        let span = crate::span::PhaseSpan::new(&ticket.identity, crate::span::Phase::TicketReady)
            .queued_at(chrono::Utc::now());
        let _ = crate::span::record_phase_span(&self.space, &ticket.scope, &self.castle, &span);
    }
}

/// Rebuild a ticket tuple with a fresh record id and the given (possibly
/// mutated) payload, preserving every other field of the tuple it replaces.
fn with_payload(existing: Tuple, payload: Value) -> Tuple {
    Tuple {
        id: RecordId::new(),
        category: existing.category,
        scope: existing.scope,
        identity: existing.identity,
        instance: existing.instance,
        lifecycle: existing.lifecycle,
        payload,
        created_at: existing.created_at,
        expires_at: existing.expires_at,
        strength: existing.strength,
    }
}

fn deps_of(ticket: &Tuple) -> Vec<String> {
    ticket
        .payload
        .get("depends_on")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn valid_transition(previous: &str, next: &str) -> bool {
    if previous == next {
        return true;
    }
    match previous {
        "open" => matches!(
            next,
            "claimed" | "in_progress" | "blocked" | "done" | "closed"
        ),
        "claimed" => matches!(next, "in_progress" | "blocked" | "done" | "closed"),
        "in_progress" => matches!(next, "blocked" | "done" | "closed"),
        "blocked" => matches!(next, "open" | "in_progress" | "done" | "closed"),
        "done" => next == "closed",
        "closed" => false,
        _ => false,
    }
}

/// Blocked = has a dependency that exists and has no durable delivery record.
/// A missing
/// dependency (deleted ticket) does not block.
fn is_blocked(ticket: &Tuple, by_id: &HashMap<String, Tuple>) -> bool {
    deps_of(ticket)
        .iter()
        .any(|d| by_id.get(d).is_some_and(|dep| !is_delivered(dep)))
}

/// Can `target` be reached from `start` by following depends_on edges?
fn reaches(start: &str, target: &str, by_id: &HashMap<String, Tuple>) -> bool {
    let mut stack = vec![start.to_string()];
    let mut seen = std::collections::HashSet::new();
    while let Some(node) = stack.pop() {
        if node == target {
            return true;
        }
        if !seen.insert(node.clone()) {
            continue;
        }
        if let Some(t) = by_id.get(&node) {
            stack.extend(deps_of(t));
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tickets() -> Tickets {
        Tickets::new(Space::open_in_memory().unwrap(), "castle".into())
    }

    /// A `Tickets` plus a handle on its space, so a test can inspect the event
    /// tuples the ticket lifecycle emits.
    fn tickets_with_space() -> (Tickets, Space) {
        let space = Space::open_in_memory().unwrap();
        (Tickets::new(space.clone(), "castle".into()), space)
    }

    fn closed_events(space: &Space) -> Vec<Tuple> {
        let mut p = Pattern::category(Category::Event);
        p.identity = Some("ticket_closed".into());
        space.scan(&p).unwrap()
    }

    fn new(title: &str, scope: &str, parent: Option<&str>) -> NewTicket {
        NewTicket {
            title: title.into(),
            body: None,
            scope: Some(scope.into()),
            parent: parent.map(Into::into),
            priority: default_priority(),
            labels: vec![],
            depends_on: vec![],
            created_by: None,
            coalesce_key: None,
        }
    }

    fn record(commit: &str) -> DeliveryRecord {
        DeliveryRecord {
            merge_commit: commit.into(),
            branch: "rat/x/tkt-1".into(),
            target: "main".into(),
            landed_at: "2026-08-19T00:00:00Z".into(),
        }
    }

    /// The headline acceptance: a landed ticket reaches a terminal state with
    /// no operator action, and carries the merge commit that proves it.
    #[tokio::test]
    async fn recording_delivery_closes_the_ticket_and_stores_the_merge_commit() {
        let t = tickets();
        let id = t.create(new("work", "repo", None)).await.unwrap().identity;
        set_status(&t, &id, "in_progress").await;

        t.record_delivery(&id, &record("abc123")).await.unwrap();

        let stored = t.get(&id).unwrap().unwrap();
        assert_eq!(
            stored.payload.get("status").and_then(Value::as_str),
            Some("closed")
        );
        assert!(is_delivered(&stored));
        assert_eq!(t.delivery(&id).unwrap().unwrap().merge_commit, "abc123");
    }

    /// The record is what "is it delivered" reads — NOT branch existence. This
    /// test holds no branch at all: nothing in the predicate may consult one.
    #[tokio::test]
    async fn delivery_reads_from_the_record_not_from_a_branch_ref() {
        let t = tickets();
        let id = t.create(new("work", "repo", None)).await.unwrap().identity;
        assert!(!is_delivered(&t.get(&id).unwrap().unwrap()));

        t.record_delivery(&id, &record("deadbeef")).await.unwrap();

        // Deleting the branch is exactly what landing does next; the record is
        // untouched by it, so the ticket still reads delivered.
        let stored = t.get(&id).unwrap().unwrap();
        assert!(is_delivered(&stored));
        assert_eq!(delivery_of(&stored).unwrap().branch, "rat/x/tkt-1");
    }

    /// A ticket marked `closed` without a land does NOT read as delivered —
    /// the "approved but never merged" class (TKT-18/46/147).
    #[tokio::test]
    async fn a_closed_ticket_without_a_land_is_not_delivered() {
        let t = tickets();
        let id = t.create(new("work", "repo", None)).await.unwrap().identity;
        set_status(&t, &id, "closed").await;
        assert!(!is_delivered(&t.get(&id).unwrap().unwrap()));
        assert!(t.delivery(&id).unwrap().is_none());
    }

    #[tokio::test]
    async fn reverting_clears_the_delivery_record_and_reopens() {
        let t = tickets();
        let id = t.create(new("work", "repo", None)).await.unwrap().identity;
        t.record_delivery(&id, &record("abc123")).await.unwrap();

        assert!(t.clear_delivery(&id, "open").await.unwrap());

        let stored = t.get(&id).unwrap().unwrap();
        assert!(!is_delivered(&stored));
        assert_eq!(
            stored.payload.get("status").and_then(Value::as_str),
            Some("open")
        );
        // Nothing left to clear on a second revert.
        assert!(!t.clear_delivery(&id, "open").await.unwrap());
    }

    /// A land is ground truth, so it must record from any prior status,
    /// including a legacy ticket already closed without a delivery record.
    #[tokio::test]
    async fn recording_delivery_works_from_an_already_closed_ticket() {
        let t = tickets();
        let id = t.create(new("work", "repo", None)).await.unwrap().identity;
        set_status(&t, &id, "closed").await;

        t.record_delivery(&id, &record("abc123")).await.unwrap();

        assert!(is_delivered(&t.get(&id).unwrap().unwrap()));
    }

    /// Re-landing the same commit must not announce a second close, or every
    /// dependent of the ticket re-fires.
    #[tokio::test]
    async fn re_recording_the_same_delivery_does_not_re_emit_a_close() {
        let (t, space) = tickets_with_space();
        let id = t.create(new("work", "repo", None)).await.unwrap().identity;
        t.record_delivery(&id, &record("abc123")).await.unwrap();
        assert_eq!(closed_events(&space).len(), 1);

        t.record_delivery(&id, &record("abc123")).await.unwrap();

        assert_eq!(closed_events(&space).len(), 1);
        assert!(is_delivered(&t.get(&id).unwrap().unwrap()));
    }

    #[tokio::test]
    async fn ids_are_unique_and_prefixed() {
        let t = tickets();
        let a = t.create(new("first", "system", None)).await.unwrap();
        let b = t.create(new("second", "system", None)).await.unwrap();
        assert!(a.identity.starts_with(ID_PREFIX));
        assert!(b.identity.starts_with(ID_PREFIX));
        assert_ne!(a.identity, b.identity);
    }

    #[tokio::test]
    async fn sub_ticket_inherits_parent_scope_when_scope_omitted() {
        let t = tickets();
        let parent = t.create(new("root", "myrepo", None)).await.unwrap();
        let mut sub = new("sub", "myrepo", Some(&parent.identity));
        sub.scope = None; // as if --repo was never passed
        let sub = t.create(sub).await.unwrap();
        assert_eq!(sub.scope, "myrepo");
    }

    #[tokio::test]
    async fn sub_ticket_explicit_scope_overrides_parent() {
        let t = tickets();
        let parent = t.create(new("root", "myrepo", None)).await.unwrap();
        let sub = t
            .create(new("sub", "otherrepo", Some(&parent.identity)))
            .await
            .unwrap();
        assert_eq!(sub.scope, "otherrepo");
    }

    #[tokio::test]
    async fn top_level_ticket_defaults_to_system_scope_when_omitted() {
        let t = tickets();
        let mut top = new("top", "myrepo", None);
        top.scope = None;
        let top = t.create(top).await.unwrap();
        assert_eq!(top.scope, system_scope());
    }

    #[tokio::test]
    async fn sub_ticket_falls_back_to_system_when_parent_missing() {
        let t = tickets();
        let mut sub = new("sub", "myrepo", Some("TKT-does-not-exist"));
        sub.scope = None;
        let sub = t.create(sub).await.unwrap();
        assert_eq!(sub.scope, system_scope());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mutation_guard_serializes_ordinary_ticket_creates() {
        let tickets = std::sync::Arc::new(tickets());
        let guard = tickets.mutation_guard().await;
        let contender = tickets.clone();
        let mut pending = tokio::spawn(async move {
            contender
                .create(new("blocked", "repo", None))
                .await
                .unwrap()
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut pending)
                .await
                .is_err(),
            "ordinary create bypassed the shared mutation lock"
        );
        drop(guard);
        assert!(pending.await.unwrap().identity.starts_with(ID_PREFIX));
    }

    #[tokio::test]
    async fn external_snapshot_digest_excludes_current_factory_execution_only() {
        let tickets = tickets();
        tickets.create(new("external", "repo", None)).await.unwrap();
        let baseline = tickets.snapshot_digest("repo").unwrap();
        let guard = tickets.mutation_guard().await;
        let mut owned = new("owned", "repo", None);
        owned.created_by = Some("factory:exec-1".into());
        owned.coalesce_key = Some("factory:ticket-graph:exec-1:NODE-1".into());
        tickets.create_idempotent_locked(&guard, owned).unwrap();

        assert_eq!(
            tickets
                .snapshot_digest_excluding_created_by(&guard, "repo", "factory:exec-1")
                .unwrap(),
            baseline
        );
        assert_ne!(tickets.snapshot_digest("repo").unwrap(), baseline);
    }

    #[tokio::test]
    async fn update_changes_status_in_place() {
        let t = tickets();
        let a = t.create(new("x", "myrepo", None)).await.unwrap();
        let changes = TicketChanges {
            status: Some("in_progress".into()),
            title: None,
            body: None,
            priority: None,
            assignee: Some("Whisker".into()),
            parent: None,
            ..Default::default()
        };
        let updated = t.update(&a.identity, changes).await.unwrap();
        assert_eq!(updated.payload["status"], "in_progress");
        assert_eq!(updated.payload["assignee"], "Whisker");
        // Still exactly one tuple for this ticket.
        assert_eq!(t.list(None, None, None).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn rejects_unknown_status() {
        let t = tickets();
        let a = t.create(new("x", "system", None)).await.unwrap();
        let changes = TicketChanges {
            status: Some("bogus".into()),
            title: None,
            body: None,
            priority: None,
            assignee: None,
            parent: None,
            ..Default::default()
        };
        assert!(t.update(&a.identity, changes).await.is_err());
    }

    #[tokio::test]
    async fn closed_tickets_require_explicit_reopen() {
        let t = tickets();
        let a = t.create(new("x", "system", None)).await.unwrap();
        set_status(&t, &a.identity, "done").await;
        set_status(&t, &a.identity, "closed").await;
        let ordinary = TicketChanges {
            status: Some("open".into()),
            ..Default::default()
        };
        assert!(t.update(&a.identity, ordinary).await.is_err());
        t.reopen(&a.identity, "open").await.unwrap();
        assert_eq!(
            t.get(&a.identity).unwrap().unwrap().payload["status"],
            "open"
        );
    }

    #[tokio::test]
    async fn done_tickets_require_explicit_reopen() {
        let t = tickets();
        let a = t.create(new("x", "system", None)).await.unwrap();
        set_status(&t, &a.identity, "done").await;
        let ordinary = TicketChanges {
            status: Some("in_progress".into()),
            ..Default::default()
        };
        assert!(
            t.update(&a.identity, ordinary).await.is_err(),
            "done -> in_progress must stay refused via plain update"
        );
        t.reopen(&a.identity, "open").await.unwrap();
        assert_eq!(
            t.get(&a.identity).unwrap().unwrap().payload["status"],
            "open"
        );
    }

    #[tokio::test]
    async fn list_filters_by_scope_status_and_parent() {
        let t = tickets();
        let root = t.create(new("root", "myrepo", None)).await.unwrap();
        t.create(new("child", "myrepo", Some(&root.identity)))
            .await
            .unwrap();
        t.create(new("elsewhere", "system", None)).await.unwrap();

        assert_eq!(t.list(Some("myrepo".into()), None, None).unwrap().len(), 2);
        assert_eq!(
            t.list(None, None, Some(root.identity.clone()))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(t.list(None, Some("open".into()), None).unwrap().len(), 3);
        assert_eq!(t.list(None, Some("done".into()), None).unwrap().len(), 0);
    }

    async fn set_status(t: &Tickets, id: &str, status: &str) {
        let changes = TicketChanges {
            status: Some(status.into()),
            title: None,
            body: None,
            priority: None,
            assignee: None,
            parent: None,
            ..Default::default()
        };
        t.update(id, changes).await.unwrap();
    }

    #[tokio::test]
    async fn ready_reflects_dependency_satisfaction() {
        let t = tickets();
        let a = t.create(new("a", "r", None)).await.unwrap(); // TKT-1
        let b = t.create(new("b", "r", None)).await.unwrap(); // TKT-2
        t.add_dep(&b.identity, &a.identity).await.unwrap(); // b depends on a

        // b is blocked, a is ready.
        let ready: Vec<_> = t
            .ready(None)
            .unwrap()
            .into_iter()
            .map(|x| x.identity)
            .collect();
        assert_eq!(ready, vec![a.identity.clone()]);
        assert_eq!(
            t.blockers(&b.identity).unwrap().unwrap(),
            vec![a.identity.clone()]
        );

        // A status-only finish is not delivery and cannot unblock b.
        set_status(&t, &a.identity, "done").await;
        assert!(t.ready(None).unwrap().is_empty());
        assert_eq!(
            t.blockers(&b.identity).unwrap().unwrap(),
            vec![a.identity.clone()]
        );

        // Recording the land is the one transition that makes a dependency
        // satisfied, so ready and blockers change together.
        t.record_delivery(&a.identity, &record("abc123"))
            .await
            .unwrap();
        let ready: Vec<_> = t
            .ready(None)
            .unwrap()
            .into_iter()
            .map(|x| x.identity)
            .collect();
        assert_eq!(ready, vec![b.identity.clone()]);
        assert!(t.blockers(&b.identity).unwrap().unwrap().is_empty());
    }

    #[tokio::test]
    async fn dependency_cycles_are_rejected() {
        let t = tickets();
        let a = t.create(new("a", "r", None)).await.unwrap();
        let b = t.create(new("b", "r", None)).await.unwrap();
        let c = t.create(new("c", "r", None)).await.unwrap();
        t.add_dep(&b.identity, &a.identity).await.unwrap(); // b -> a
        t.add_dep(&c.identity, &b.identity).await.unwrap(); // c -> b
                                                            // a -> c would close a cycle a -> c -> b -> a.
        assert!(t.add_dep(&a.identity, &c.identity).await.is_err());
        // Self-dependency is rejected too.
        assert!(t.add_dep(&a.identity, &a.identity).await.is_err());
    }

    #[tokio::test]
    async fn claim_is_won_exactly_once() {
        let t = tickets();
        let a = t.create(new("x", "r", None)).await.unwrap();
        assert!(t.claim(&a.identity).await.unwrap(), "first claim wins");
        assert!(
            !t.claim(&a.identity).await.unwrap(),
            "second claim loses — already in_progress"
        );
        // The won claim advanced the ticket, and losing left it untouched.
        let tk = t.get(&a.identity).unwrap().unwrap();
        assert_eq!(tk.payload["status"], "in_progress");
        // Still exactly one tuple — a losing claim must not destroy the ticket.
        assert_eq!(t.list(None, None, None).unwrap().len(), 1);
        // Claiming a ticket that never existed is a clean loss, not an error.
        assert!(!t.claim("TKT-999").await.unwrap());
    }

    /// The claim producer wires into the task-to-main span substrate: a won
    /// claim records exactly one `Claimed` span, a losing replay of the same
    /// claim call records no second one (idempotent on `(task, phase,
    /// attempt)`, `crate::span`). Creation itself already stamped a
    /// `TicketReady` span (`a` has no dependency, so it was actionable from
    /// the start), so the claim adds exactly one span on top of that.
    #[tokio::test]
    async fn claim_records_a_claimed_phase_span_exactly_once() {
        let (t, space) = tickets_with_space();
        let a = t.create(new("x", "r", None)).await.unwrap();
        assert!(t.claim(&a.identity).await.unwrap());
        assert!(!t.claim(&a.identity).await.unwrap(), "already in_progress");

        // Two spans minted in the same millisecond sort randomly through
        // `spans_for_task`'s `RecordId`-based ordering (see `id.rs`'s sub-ms
        // ordering, c74a9b5), so check by phase, not scan position.
        let spans = crate::span::spans_for_task(&space, &a.scope, &a.identity).unwrap();
        assert_eq!(spans.len(), 2);
        assert!(spans.iter().any(|s| s["phase"] == "ticket_ready"));
        assert!(spans.iter().any(|s| s["phase"] == "claimed"));
    }

    // Two drains race to claim a shared backlog; the atomic claim must hand each
    // ticket to exactly one of them (never both), so no ticket is double-grabbed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_drains_never_double_grab() {
        let t = std::sync::Arc::new(tickets());
        let ids: Vec<String> = {
            let mut ids = Vec::new();
            for i in 0..25 {
                let tk = t.create(new(&format!("t{i}"), "r", None)).await.unwrap();
                ids.push(tk.identity);
            }
            ids
        };

        async fn drain(t: std::sync::Arc<Tickets>, ids: Vec<String>) -> Vec<String> {
            let mut won = Vec::new();
            for id in ids {
                if t.claim(&id).await.unwrap() {
                    won.push(id);
                }
            }
            won
        }
        let (a, b) = tokio::join!(drain(t.clone(), ids.clone()), drain(t.clone(), ids.clone()));

        // No ticket won by both drains.
        for id in &a {
            assert!(!b.contains(id), "{id} double-grabbed by both drains");
        }
        // Every ticket claimed exactly once across the two drains combined.
        let mut all: Vec<String> = a.iter().chain(&b).cloned().collect();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), ids.len(), "each ticket claimed exactly once");
        // And every ticket is now in_progress.
        for id in &ids {
            let tk = t.get(id).unwrap().unwrap();
            assert_eq!(tk.payload["status"], "in_progress");
        }
    }

    #[tokio::test]
    async fn only_delivery_emits_a_ticket_closed_event() {
        let (t, space) = tickets_with_space();
        let a = t.create(new("x", "myrepo", None)).await.unwrap();
        assert!(closed_events(&space).is_empty(), "no event before close");

        set_status(&t, &a.identity, "done").await;
        assert!(
            closed_events(&space).is_empty(),
            "status without delivery must not unblock dependents"
        );
        t.record_delivery(&a.identity, &record("abc123"))
            .await
            .unwrap();
        let events = closed_events(&space);
        assert_eq!(events.len(), 1, "delivery emits exactly one event");
        let ev = &events[0];
        assert_eq!(ev.scope, "myrepo", "event is scoped to the ticket's repo");
        assert_eq!(ev.payload["ticket"], json!(a.identity));
        assert_eq!(ev.payload["status"], json!("closed"));

        // Creation already stamped a `TicketReady` span (`a` has no
        // dependency, so it was actionable from the start); delivery adds
        // exactly one more, the delivery-closure span. Two spans minted in
        // the same millisecond sort randomly through `spans_for_task`'s
        // `RecordId`-based ordering (see `id.rs`'s sub-ms ordering, c74a9b5),
        // so find by phase rather than scan position.
        let spans = crate::span::spans_for_task(&space, "myrepo", &a.identity).unwrap();
        assert_eq!(
            spans.len(),
            2,
            "ticket-ready plus one delivery-closure span"
        );
        assert!(spans.iter().any(|s| s["phase"] == "ticket_ready"));
        let closure = spans
            .iter()
            .find(|s| s["phase"] == "delivery_closure")
            .expect("delivery_closure span");
        assert_eq!(closure["candidate"], "abc123");

        // Re-recording the same delivery replays the undelivered->delivered
        // edge guard (see `record_delivery`'s own doc) and must not record a
        // second span on top of it.
        t.record_delivery(&a.identity, &record("abc123"))
            .await
            .unwrap();
        let spans = crate::span::spans_for_task(&space, "myrepo", &a.identity).unwrap();
        assert_eq!(spans.len(), 2, "replayed delivery does not double-count");
    }

    /// The readiness-edge producer wired for `TicketReady`
    /// (TKT-01M0QMT83E7YXH6ZXHMQG0VRS6): a ticket with no dependency is
    /// actionable the instant it exists, so creation itself stamps the span.
    #[tokio::test]
    async fn ticket_ready_span_records_on_creation_when_unblocked() {
        let (t, space) = tickets_with_space();
        let a = t.create(new("x", "myrepo", None)).await.unwrap();
        let spans = crate::span::spans_for_task(&space, "myrepo", &a.identity).unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0]["phase"], "ticket_ready");
    }

    /// A ticket created with an undelivered dependency is blocked, so
    /// creation must not stamp `TicketReady` for it — the span settles later,
    /// on the delivered edge of its last blocker (the dependent scan inside
    /// `emit_ticket_closed`).
    #[tokio::test]
    async fn ticket_ready_span_waits_for_last_blocker_to_deliver() {
        let (t, space) = tickets_with_space();
        let a = t.create(new("a", "r", None)).await.unwrap();
        let mut b_new = new("b", "r", None);
        b_new.depends_on = vec![a.identity.clone()];
        let b = t.create(b_new).await.unwrap();

        // a is unblocked at creation; b is blocked on a and gets no span yet.
        let a_spans = crate::span::spans_for_task(&space, "r", &a.identity).unwrap();
        assert_eq!(a_spans.len(), 1);
        assert_eq!(a_spans[0]["phase"], "ticket_ready");
        assert!(crate::span::spans_for_task(&space, "r", &b.identity)
            .unwrap()
            .is_empty());

        // A status-only finish is not delivery and must not unblock b.
        set_status(&t, &a.identity, "done").await;
        assert!(crate::span::spans_for_task(&space, "r", &b.identity)
            .unwrap()
            .is_empty());

        // Recording the land resolves b's last blocker: exactly one
        // TicketReady span for b, settled at the delivered edge.
        t.record_delivery(&a.identity, &record("abc123"))
            .await
            .unwrap();
        let b_spans = crate::span::spans_for_task(&space, "r", &b.identity).unwrap();
        assert_eq!(b_spans.len(), 1);
        assert_eq!(b_spans[0]["phase"], "ticket_ready");

        // Re-recording the same delivery replays the undelivered->delivered
        // edge guard and must not double-stamp b's span.
        t.record_delivery(&a.identity, &record("abc123"))
            .await
            .unwrap();
        let b_spans = crate::span::spans_for_task(&space, "r", &b.identity).unwrap();
        assert_eq!(b_spans.len(), 1, "replayed delivery does not double-count");
    }

    #[tokio::test]
    async fn non_terminal_edits_do_not_emit_a_close() {
        let (t, space) = tickets_with_space();
        let a = t.create(new("x", "myrepo", None)).await.unwrap();
        // Claim (open → in_progress) and a plain in_progress update are both
        // non-terminal, so neither announces a close.
        assert!(t.claim(&a.identity).await.unwrap());
        set_status(&t, &a.identity, "blocked").await;
        assert!(
            closed_events(&space).is_empty(),
            "only the crossing into done/closed emits"
        );
    }

    #[tokio::test]
    async fn status_changes_before_delivery_do_not_emit() {
        let (t, space) = tickets_with_space();
        let a = t.create(new("x", "myrepo", None)).await.unwrap();
        set_status(&t, &a.identity, "done").await;
        set_status(&t, &a.identity, "closed").await;
        assert_eq!(
            closed_events(&space).len(),
            0,
            "neither done nor closed satisfies a dependency without delivery"
        );
    }

    #[tokio::test]
    async fn create_rejects_dependency_on_missing_ticket() {
        let t = tickets();
        let mut nt = new("x", "r", None);
        nt.depends_on = vec!["TKT-999".into()];
        assert!(t.create(nt).await.is_err());
    }

    /// The delivered-but-open shape (a delivery record present, status still
    /// non-terminal) only arises from a direct payload write or a status
    /// regression after delivery — never from the ordinary `Tickets` API,
    /// which always closes atomically with the record ([`Tickets::record_delivery`]).
    /// Built the same way `reconcile.rs`'s own tests build it: a raw tuple
    /// written straight to the space, bypassing `Tickets` entirely.
    fn seed_delivered_but_open(space: &Space, id: &str, merge_commit: &str) {
        let payload = json!({
            "title": "t",
            "status": "in_progress",
            "assignee": Value::Null,
            "delivery": {
                "merge_commit": merge_commit,
                "branch": "rat/x/tkt-1",
                "target": "main",
                "landed_at": "2026-08-19T00:00:00Z",
            },
        });
        space
            .out(
                Tuple::new(Category::Task, "repo", id, "castle", payload)
                    .with_lifecycle(Lifecycle::Session),
            )
            .unwrap();
    }

    #[tokio::test]
    async fn repair_close_delivered_applies_on_a_matching_precondition() {
        let (t, space) = tickets_with_space();
        seed_delivered_but_open(&space, "TKT-1", "abc123");

        let outcome = t.repair_close_delivered("TKT-1", "abc123").await.unwrap();
        assert!(matches!(outcome, CasOutcome::Applied(_)));
        assert_eq!(t.get("TKT-1").unwrap().unwrap().payload["status"], "closed");
    }

    #[tokio::test]
    async fn repair_close_delivered_drifts_closed_on_a_mismatched_commit() {
        let (t, space) = tickets_with_space();
        seed_delivered_but_open(&space, "TKT-1", "abc123");

        let outcome = t
            .repair_close_delivered("TKT-1", "does-not-match")
            .await
            .unwrap();
        assert!(matches!(outcome, CasOutcome::Drifted { .. }));
        // Zero mutation: still exactly the ticket the caller took, unchanged.
        assert_eq!(
            t.get("TKT-1").unwrap().unwrap().payload["status"],
            "in_progress"
        );
    }

    #[tokio::test]
    async fn repair_close_delivered_drifts_on_an_already_terminal_ticket() {
        let t = tickets();
        let id = t.create(new("work", "repo", None)).await.unwrap().identity;
        t.record_delivery(&id, &record("abc123")).await.unwrap(); // already closes it
        let outcome = t.repair_close_delivered(&id, "abc123").await.unwrap();
        assert!(matches!(outcome, CasOutcome::Drifted { .. }));
    }

    #[tokio::test]
    async fn repair_close_delivered_reports_gone_on_a_missing_ticket() {
        let t = tickets();
        let outcome = t.repair_close_delivered("TKT-999", "abc123").await.unwrap();
        assert!(matches!(outcome, CasOutcome::Gone));
    }

    #[tokio::test]
    async fn repair_clear_stale_ownership_applies_on_a_matching_precondition() {
        let t = tickets();
        let a = t.create(new("x", "r", None)).await.unwrap();
        t.update(
            &a.identity,
            TicketChanges {
                status: Some("in_progress".into()),
                assignee: Some("Whisker".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let outcome = t
            .repair_clear_stale_ownership(&a.identity, "in_progress", Some("Whisker"))
            .await
            .unwrap();
        assert!(matches!(outcome, CasOutcome::Applied(_)));
        let stored = t.get(&a.identity).unwrap().unwrap();
        assert_eq!(stored.payload["status"], "open");
        assert_eq!(stored.payload["assignee"], Value::Null);
    }

    #[tokio::test]
    async fn repair_clear_stale_ownership_drifts_closed_when_the_owner_already_moved_on() {
        let t = tickets();
        let a = t.create(new("x", "r", None)).await.unwrap();
        t.update(
            &a.identity,
            TicketChanges {
                status: Some("in_progress".into()),
                assignee: Some("Whisker".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        // The live owner advanced it between the caller's read and this call.
        set_status(&t, &a.identity, "blocked").await;

        let outcome = t
            .repair_clear_stale_ownership(&a.identity, "in_progress", Some("Whisker"))
            .await
            .unwrap();
        assert!(matches!(outcome, CasOutcome::Drifted { .. }));
        let stored = t.get(&a.identity).unwrap().unwrap();
        assert_eq!(stored.payload["status"], "blocked", "no mutation on drift");
        assert_eq!(stored.payload["assignee"], "Whisker");
    }
}

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
use serde::Deserialize;
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

    /// Reopen a closed ticket as an explicit recovery action (used by
    /// `rk revert`). Ordinary updates cannot move a closed ticket backwards.
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

    /// Open tickets whose every dependency is done/closed — actionable right now.
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
                .filter(|d| by_id.get(d).is_some_and(|dep| !is_done(dep)))
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

        let was_done = is_done(&existing);
        let mut payload = existing.payload.clone();
        let obj = payload
            .as_object_mut()
            .ok_or_else(|| rk_core::Error::other("ticket payload is not an object"))?;
        f(obj);
        obj.insert("updated_at".into(), json!(chrono::Utc::now().to_rfc3339()));

        let updated = with_payload(existing, payload);
        self.space.out(updated.clone())?;
        // Emit a `ticket_closed` event on the non-terminal → terminal edge — the
        // moment a ticket's dependents can unblock (TKT-56). A reactor trigger
        // matching this event hands the now-ready backlog to a drain workflow,
        // turning the dependency DAG into a self-advancing pipeline instead of
        // one that waits for the next drain sweep. Only the crossing edge fires
        // (a done→closed re-close, or a non-status edit, does not), so a closed
        // ticket's dependents are announced exactly once. Best-effort: a failed
        // emit never fails the status change that already landed.
        if is_done(&updated) && !was_done {
            self.emit_ticket_closed(&updated);
        }
        Ok(updated)
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
        "open" => matches!(next, "claimed" | "in_progress" | "blocked" | "done" | "closed"),
        "claimed" => matches!(next, "in_progress" | "blocked" | "done" | "closed"),
        "in_progress" => matches!(next, "blocked" | "done" | "closed"),
        "blocked" => matches!(next, "open" | "in_progress" | "done" | "closed"),
        "done" => next == "closed",
        "closed" => false,
        _ => false,
    }
}

fn is_done(ticket: &Tuple) -> bool {
    matches!(
        ticket.payload.get("status").and_then(Value::as_str),
        Some("done") | Some("closed")
    )
}

/// Blocked = has a dependency that exists and is not yet done/closed. A missing
/// dependency (deleted ticket) does not block.
fn is_blocked(ticket: &Tuple, by_id: &HashMap<String, Tuple>) -> bool {
    deps_of(ticket)
        .iter()
        .any(|d| by_id.get(d).is_some_and(|dep| !is_done(dep)))
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
        tickets
            .create(new("external", "repo", None))
            .await
            .unwrap();
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
        assert_eq!(t.get(&a.identity).unwrap().unwrap().payload["status"], "open");
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
            t.list(None, None, Some(root.identity.clone())).unwrap().len(),
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
        let ready: Vec<_> = t.ready(None).unwrap().into_iter().map(|x| x.identity).collect();
        assert_eq!(ready, vec![a.identity.clone()]);
        assert_eq!(t.blockers(&b.identity).unwrap().unwrap(), vec![a.identity.clone()]);

        // Finish a → b becomes ready.
        set_status(&t, &a.identity, "done").await;
        let ready: Vec<_> = t.ready(None).unwrap().into_iter().map(|x| x.identity).collect();
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
        let (a, b) = tokio::join!(
            drain(t.clone(), ids.clone()),
            drain(t.clone(), ids.clone())
        );

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
    async fn closing_a_ticket_emits_a_ticket_closed_event() {
        let (t, space) = tickets_with_space();
        let a = t.create(new("x", "myrepo", None)).await.unwrap();
        assert!(closed_events(&space).is_empty(), "no event before close");

        set_status(&t, &a.identity, "done").await;
        let events = closed_events(&space);
        assert_eq!(events.len(), 1, "closing emits exactly one event");
        let ev = &events[0];
        assert_eq!(ev.scope, "myrepo", "event is scoped to the ticket's repo");
        assert_eq!(ev.payload["ticket"], json!(a.identity));
        assert_eq!(ev.payload["status"], json!("done"));
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
    async fn re_closing_a_done_ticket_does_not_re_emit() {
        let (t, space) = tickets_with_space();
        let a = t.create(new("x", "myrepo", None)).await.unwrap();
        set_status(&t, &a.identity, "done").await; // non-terminal → done: emits
        set_status(&t, &a.identity, "closed").await; // done → closed: no re-emit
        assert_eq!(
            closed_events(&space).len(),
            1,
            "the terminal edge fires once; a done→closed re-close is silent"
        );
    }

    #[tokio::test]
    async fn create_rejects_dependency_on_missing_ticket() {
        let t = tickets();
        let mut nt = new("x", "r", None);
        nt.depends_on = vec!["TKT-999".into()];
        assert!(t.create(nt).await.is_err());
    }
}

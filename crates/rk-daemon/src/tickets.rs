//! Tickets: durable work items, stored as `task` tuples in the tuplespace.
//!
//! A ticket is a `task`-category tuple whose `identity` is `TKT-<n>` and whose
//! payload carries `{title, body, status, parent, ...}`. Nothing collects
//! `session`/`furniture` tuples, so a ticket persists as a backlog item until
//! explicitly closed — and because tickets carry a repo *name* (not a path),
//! they replicate across castles through git-notes sync as a shared backlog.
//!
//! All mutations (create and update) serialize through one lock so ticket-id
//! allocation and the take-and-replace of an update can never interleave and
//! mint a duplicate id.

use rk_core::id::RecordId;
use rk_core::tuple::{Category, Lifecycle, Pattern, Tuple};
use rk_space::Space;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::Mutex;

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
    #[serde(default = "system_scope")]
    pub scope: String,
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
}

pub struct Tickets {
    space: Space,
    castle: String,
    lock: Mutex<()>,
}

fn id_num(identity: &str) -> u64 {
    identity
        .strip_prefix(ID_PREFIX)
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0)
}

impl Tickets {
    pub fn new(space: Space, castle: String) -> Self {
        Self {
            space,
            castle,
            lock: Mutex::new(()),
        }
    }

    /// Next free ticket id: one past the highest `TKT-<n>` currently in the
    /// space. Called only while `self.lock` is held.
    fn next_id(&self) -> rk_core::Result<String> {
        let existing = self.space.scan(&Pattern::category(Category::Task))?;
        let max = existing.iter().map(|t| id_num(&t.identity)).max().unwrap_or(0);
        Ok(format!("{ID_PREFIX}{}", max + 1))
    }

    pub async fn create(&self, t: NewTicket) -> rk_core::Result<Tuple> {
        let _guard = self.lock.lock().await;
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
        let tuple = Tuple::new(Category::Task, t.scope, id, self.castle.clone(), payload)
            .with_lifecycle(Lifecycle::Session);
        self.space.out(tuple.clone())?;
        Ok(tuple)
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
        tickets.sort_by_key(|t| id_num(&t.identity));
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
        let _guard = self.lock.lock().await;
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

    /// Add a `id depends-on dep` edge, rejecting self-loops, missing tickets,
    /// and any edge that would close a cycle.
    pub async fn add_dep(&self, id: &str, dep: &str) -> rk_core::Result<Tuple> {
        if id == dep {
            return Err(rk_core::Error::other("a ticket cannot depend on itself"));
        }
        let _guard = self.lock.lock().await;
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
        ready.sort_by_key(|t| id_num(&t.identity));
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

        let mut payload = existing.payload.clone();
        let obj = payload
            .as_object_mut()
            .ok_or_else(|| rk_core::Error::other("ticket payload is not an object"))?;
        f(obj);
        obj.insert("updated_at".into(), json!(chrono::Utc::now().to_rfc3339()));

        let updated = with_payload(existing, payload);
        self.space.out(updated.clone())?;
        Ok(updated)
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

    fn new(title: &str, scope: &str, parent: Option<&str>) -> NewTicket {
        NewTicket {
            title: title.into(),
            body: None,
            scope: scope.into(),
            parent: parent.map(Into::into),
            priority: default_priority(),
            labels: vec![],
            depends_on: vec![],
            created_by: None,
            coalesce_key: None,
        }
    }

    #[tokio::test]
    async fn ids_are_monotonic_and_prefixed() {
        let t = tickets();
        let a = t.create(new("first", "system", None)).await.unwrap();
        let b = t.create(new("second", "system", None)).await.unwrap();
        assert_eq!(a.identity, "TKT-1");
        assert_eq!(b.identity, "TKT-2");
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
        };
        assert!(t.update(&a.identity, changes).await.is_err());
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
    async fn create_rejects_dependency_on_missing_ticket() {
        let t = tickets();
        let mut nt = new("x", "r", None);
        nt.depends_on = vec!["TKT-999".into()];
        assert!(t.create(nt).await.is_err());
    }
}

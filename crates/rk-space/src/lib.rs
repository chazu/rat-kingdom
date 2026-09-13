//! The rat-kingdom tuplespace: Linda primitives (`out`, `in`/`take`, `rd`,
//! `scan`) with blocking reads and no lost wakeups.
//!
//! # Concurrency design (the predecessor's lesson, fixed structurally)
//!
//! One mutex guards both the store and the waiter list. `out` inserts the
//! tuple, offers it to waiters, and (if consumed) deletes it — all under one
//! lock acquisition. `take`/`rd` check the store and, on miss, register their
//! waiter under that same lock. Therefore, for any tuple T and reader R:
//! either R's check sees T in the store, or R's waiter is registered before
//! `out(T)` acquires the lock and offers T to waiters. There is no interleaving
//! in which both miss — the lost-wakeup class is impossible by construction,
//! and waiters are matched with the *same* [`Pattern::matches`] predicate the
//! store query mirrors.

mod store;

pub use store::{PersistenceDelta, PersistencePage, SdlcTransitionRecord};

use rk_core::sdlc::{ConfiguredSourceName, SignalEnvelope, SignalReceipt, SignalSourcePrincipal};
use rk_core::tuple::{Lifecycle, Pattern, Tuple};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, oneshot};
use tracing::debug;

use store::Store;

/// Capacity of the watch/event feed. Laggy watchers miss events (they are
/// observers, not participants — waiters never ride this channel).
const EVENT_FEED_CAPACITY: usize = 1024;

struct Waiter {
    id: u64,
    pattern: Pattern,
    destructive: bool,
    tx: oneshot::Sender<Tuple>,
}

struct Inner {
    store: Store,
    waiters: Vec<Waiter>,
    next_waiter_id: u64,
}

impl Inner {
    /// Persist `tuple`, offer it to matching waiters, and (if a destructive
    /// waiter consumed it) delete it again — all under the caller's lock.
    /// Coordinator writes also receive a journal sequence; ordinary writes
    /// return `None`.
    fn insert_and_offer(
        &mut self,
        tuple: &Tuple,
        coordinator: bool,
    ) -> rk_core::Result<Option<u64>> {
        let sequence = if coordinator {
            Some(self.store.insert_coordinator(tuple)?)
        } else {
            self.store.insert(tuple)?;
            None
        };

        self.offer(tuple)?;
        Ok(sequence)
    }

    fn offer(&mut self, tuple: &Tuple) -> rk_core::Result<()> {
        let mut consumed = false;
        let consumable = tuple.lifecycle != Lifecycle::Furniture;
        // Drain-and-retain: offer to every matching rd waiter, and to the first
        // matching take waiter still listening. Dropped receivers (timed out)
        // are pruned as we go.
        let waiters = std::mem::take(&mut self.waiters);
        for waiter in waiters {
            let matches = waiter.pattern.matches(tuple);
            if !matches {
                self.waiters.push(waiter);
                continue;
            }
            if waiter.destructive {
                if consumed || !consumable {
                    self.waiters.push(waiter);
                    continue;
                }
                // On Err the receiver is gone (timed out); drop the waiter.
                if waiter.tx.send(tuple.clone()).is_ok() {
                    consumed = true;
                }
            } else {
                // Non-destructive: deliver and drop the waiter (rd is one-shot).
                let _ = waiter.tx.send(tuple.clone());
            }
        }
        if consumed {
            self.store.delete(tuple.id)?;
        }
        Ok(())
    }
}

/// A durable coordinator event paired with its journal-local cursor.
#[derive(Clone, Debug)]
pub struct CoordinatorEvent {
    pub cursor: u64,
    pub event: Tuple,
}

/// A shared handle to the tuplespace. Cheap to clone.
#[derive(Clone)]
pub struct Space {
    inner: Arc<Mutex<Inner>>,
    events: broadcast::Sender<Tuple>,
    coordinator_events: broadcast::Sender<CoordinatorEvent>,
    sdlc_rollback_injection: Arc<AtomicBool>,
    /// Test-only: refuse BBS telemetry writes so the nonfatal-capture contract
    /// can be exercised against a store that genuinely fails. See
    /// [`Space::fail_bbs_telemetry_writes_for_tests`].
    bbs_telemetry_write_failure: Arc<AtomicBool>,
}

impl Space {
    pub fn open(path: &Path) -> rk_core::Result<Self> {
        Ok(Self::from_store(Store::open(path)?))
    }

    pub fn open_in_memory() -> rk_core::Result<Self> {
        Ok(Self::from_store(Store::open_in_memory()?))
    }

    fn from_store(store: Store) -> Self {
        let (events, _) = broadcast::channel(EVENT_FEED_CAPACITY);
        let (coordinator_events, _) = broadcast::channel(EVENT_FEED_CAPACITY);
        Self {
            inner: Arc::new(Mutex::new(Inner {
                store,
                waiters: Vec::new(),
                next_waiter_id: 0,
            })),
            events,
            coordinator_events,
            sdlc_rollback_injection: Arc::new(AtomicBool::new(false)),
            bbs_telemetry_write_failure: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Accept a hardened SDLC signal, persist its occurrence receipt, update the
    /// current state, and project Event/Fact tuples atomically. Duplicate
    /// `(source, delivery_id)` deliveries return the original receipt without
    /// appending daemon shadow state.
    pub fn accept_sdlc_signal(
        &self,
        envelope: SignalEnvelope,
        principal: SignalSourcePrincipal,
    ) -> rk_core::Result<SignalReceipt> {
        let rollback_injection = self.sdlc_rollback_injection.load(Ordering::SeqCst);
        let mut inner = self.lock();
        let accepted = inner
            .store
            .accept_sdlc_signal(&envelope, &principal, rollback_injection)?;
        drop(inner);

        for tuple in accepted.projected_tuples {
            let _ = self.events.send(tuple);
        }
        Ok(accepted.receipt)
    }

    pub fn get_sdlc_receipt(
        &self,
        source: &ConfiguredSourceName,
        delivery_id: &str,
    ) -> rk_core::Result<Option<SignalReceipt>> {
        self.lock().store.sdlc_receipt(source, delivery_id)
    }

    pub fn get_sdlc_transition(
        &self,
        transition_tuple_id: &str,
    ) -> rk_core::Result<Option<SdlcTransitionRecord>> {
        self.lock().store.sdlc_transition(transition_tuple_id)
    }

    pub fn current_sdlc_facts(
        &self,
        source: Option<&str>,
        scope: Option<&str>,
        subject: Option<&str>,
    ) -> rk_core::Result<Vec<Tuple>> {
        self.lock().store.current_sdlc_facts(source, scope, subject)
    }

    /// The durable SQLite persistence high-water mark for ordinary tuple writes.
    /// Sequence zero means no tuple has been persisted yet.
    pub fn latest_persistence_sequence(&self) -> rk_core::Result<u64> {
        self.lock().store.latest_persistence_sequence()
    }

    /// Return immutable tuple persistence events after `after`, ordered by SQLite
    /// commit sequence, plus the captured durable boundary for at-least-once
    /// consumers. A later take or delete does not remove the event snapshot.
    pub fn persistence_delta(&self, after: Option<u64>) -> rk_core::Result<PersistenceDelta> {
        self.lock().store.persistence_delta(after)
    }

    /// One bounded, scope-filtered, persistence-ordered page of the immutable
    /// journal. Scope, cursor and limit are pushed into SQL before any payload
    /// is deserialized, so the cost is the page, not the journal. Prefer this
    /// over [`Space::persistence_delta`] for any capture surface that must stay
    /// bounded on a production-sized store, and read `more` to report
    /// truncation explicitly.
    /// `pin` freezes the boundary across pages so a concurrent write cannot
    /// slip into a later page of the same snapshot; a pin ahead of the store's
    /// current sequence is refused, never clamped.
    pub fn persistence_page(
        &self,
        scope: &str,
        after: Option<u64>,
        limit: usize,
        pin: Option<u64>,
    ) -> rk_core::Result<PersistencePage> {
        self.lock().store.persistence_page(scope, after, limit, pin)
    }

    /// Resolve one tuple as of a frozen persistence boundary, fenced to
    /// `scope`, so an export's references cannot pull in rows written after
    /// its snapshot or belonging to a different repository. `None` means the
    /// tuple did not exist in that scope at that boundary and must be
    /// reported as missing rather than resolved through the live row.
    ///
    /// The returned sequence is the matched journal row's own
    /// `commit_sequence` — the only value that can honestly be called this
    /// reference's as-of order. It is NOT the live row's `commit_sequence`,
    /// which can differ (deleted, or reinforced after the boundary).
    pub fn get_as_of(
        &self,
        id: rk_core::id::RecordId,
        boundary: u64,
        scope: &str,
    ) -> rk_core::Result<Option<(u64, rk_core::tuple::Tuple)>> {
        self.lock().store.get_as_of(id, boundary, scope)
    }

    /// Whether this local store ever persisted the tuple id, even if the live row
    /// was later consumed or deleted.
    pub fn has_persistence_event(&self, id: rk_core::id::RecordId) -> rk_core::Result<bool> {
        self.lock().store.has_persistence_event(id)
    }

    /// Whether the immutable local persistence journal has ever contained a
    /// tuple matching `pattern`.
    pub fn has_persistence_event_matching(&self, pattern: &Pattern) -> rk_core::Result<bool> {
        self.lock().store.has_persistence_event_matching(pattern)
    }

    /// Convert a legacy ULID cursor to a safe historical replay boundary. ULID
    /// ordering cannot reveal delayed lower-ID rows the old cursor skipped, so
    /// conversion returns sequence zero and relies on consumer idempotency.
    pub fn legacy_persistence_sequence(
        &self,
        id: rk_core::id::RecordId,
    ) -> rk_core::Result<Option<u64>> {
        self.lock().store.legacy_persistence_sequence(id)
    }

    /// Test-only fault injection for the BBS nonfatal-capture contract: refuse
    /// daemon-authored telemetry records while leaving everything else — the
    /// briefing, the launch, the `bbs show` the record describes — working.
    ///
    /// The `telemetry_gap` fallback is deliberately still accepted. A store that
    /// refuses the record but can still take the gap is the case the capture
    /// path documents as worth distinguishing; a store that refuses both leaves
    /// only the in-process return value, which is why that value, not the gap
    /// tuple, is the reliable signal.
    pub fn fail_bbs_telemetry_writes_for_tests(&self, enabled: bool) {
        self.bbs_telemetry_write_failure
            .store(enabled, Ordering::SeqCst);
    }

    fn refuses_telemetry(&self, tuple: &Tuple) -> bool {
        self.bbs_telemetry_write_failure.load(Ordering::SeqCst)
            && tuple.lifecycle == rk_core::tuple::Lifecycle::Furniture
            && tuple
                .payload
                .get("bbs_kind")
                .and_then(|kind| kind.as_str())
                .is_some_and(|kind| kind != "telemetry_gap")
    }

    pub fn enable_sdlc_rollback_injection_for_tests(&self, enabled: bool) {
        self.sdlc_rollback_injection
            .store(enabled, Ordering::SeqCst);
    }

    /// Write a tuple: persist, wake matching waiters, publish to the event
    /// feed. If a destructive waiter consumes the tuple it is removed again
    /// before the lock is released; `rd` waiters observing it still get it.
    pub fn out(&self, tuple: Tuple) -> rk_core::Result<()> {
        if self.refuses_telemetry(&tuple) {
            return Err(rk_core::Error::other(
                "injected BBS telemetry write failure (tests only)",
            ));
        }
        let mut inner = self.lock();
        inner.insert_and_offer(&tuple, false)?;
        drop(inner);

        let _ = self.events.send(tuple);
        Ok(())
    }

    /// Write a pheromone trail with reinforcement: if a trail already exists on
    /// this exact `(category, scope, identity, instance)` key, refresh it in
    /// place (new payload, new TTL, strength back to full) instead of appending
    /// a duplicate — keeping its id and `created_at` stable so sync's
    /// earliest-claim-wins arbitration is undisturbed. On a first write this is
    /// just `out`. Returns the surviving tuple (carrying the id that persisted).
    ///
    /// Only the RPC write path routes evaporating categories here; in-process
    /// writers (the supervisor's budget/liveness obstacles) still use `out` and
    /// remain append-only, so an agent re-stating its own trail never clobbers a
    /// distinct supervisor-authored one on the same identity.
    pub fn reinforce(&self, mut tuple: Tuple) -> rk_core::Result<Tuple> {
        // Reinforcement is authoritative: a written or re-written trail is at
        // full strength regardless of what the caller passed. GC decays it from
        // there until the next reinforcement.
        tuple.strength = Some(rk_core::tuple::FULL_STRENGTH);
        let mut inner = self.lock();
        if let Some(id) = inner.store.newest_trail(
            tuple.category,
            &tuple.scope,
            &tuple.identity,
            &tuple.instance,
        )? {
            inner.store.reinforce(id, &tuple)?;
            drop(inner);
            tuple.id = id;
            let _ = self.events.send(tuple.clone());
            return Ok(tuple);
        }
        inner.insert_and_offer(&tuple, false)?;
        drop(inner);
        let _ = self.events.send(tuple.clone());
        Ok(tuple)
    }

    /// Non-blocking read of all matching tuples, oldest first.
    pub fn scan(&self, pattern: &Pattern) -> rk_core::Result<Vec<Tuple>> {
        self.lock().store.query(pattern, false, None)
    }

    /// Non-blocking read capped at `limit` rows. RPC callers use this bounded
    /// form so a broad operator scan cannot materialize an unbounded SQLite
    /// result before the protocol frame-size guard gets a chance to run.
    pub fn scan_limited(&self, pattern: &Pattern, limit: usize) -> rk_core::Result<Vec<Tuple>> {
        self.lock().store.query(pattern, false, Some(limit))
    }

    /// Non-blocking newest-first read capped at `limit`. Read-side reducers use
    /// this when recent events supersede older ones and the history is bounded.
    pub fn scan_newest_limited(
        &self,
        pattern: &Pattern,
        limit: usize,
    ) -> rk_core::Result<Vec<Tuple>> {
        self.lock().store.query_newest(pattern, false, Some(limit))
    }

    /// Read one tuple by its durable id without consuming it.
    pub fn get(&self, id: rk_core::id::RecordId) -> rk_core::Result<Option<Tuple>> {
        self.lock().store.get(id)
    }

    /// Bounded, indexed lookup of the live commit sequence for a small, known
    /// set of ids — `O(len(ids))` regardless of total store size. Use this to
    /// order a handful of records by actual persistence order instead of by
    /// `RecordId`/ULID mint order, which a delayed writer can invert.
    pub fn commit_sequences(
        &self,
        ids: &[rk_core::id::RecordId],
    ) -> rk_core::Result<std::collections::HashMap<rk_core::id::RecordId, u64>> {
        self.lock().store.commit_sequences(ids)
    }

    /// Delete one tuple by id (archive-on-resolution, targeted GC). Returns
    /// whether a row was removed. Unlike [`Space::take`], this consumes a
    /// *specific* tuple rather than the oldest pattern match — the reactor uses
    /// it to retire the exact obstacle/need an artifact resolved. Deletions are
    /// local (like GC): tuples replicate, their removal does not.
    pub fn delete(&self, id: rk_core::id::RecordId) -> rk_core::Result<bool> {
        self.lock().store.delete(id)
    }

    /// Non-blocking ranked read (the `--hot` gradient, stigmergy P7): matching
    /// tuples scored by `category_weight × recency × strength`, strongest first,
    /// optionally capped to the top `limit`. Read-only sugar over [`Space::scan`]
    /// — the oldest-first path and the waiter-wake predicate are untouched.
    pub fn scan_hot(&self, pattern: &Pattern, limit: Option<usize>) -> rk_core::Result<Vec<Tuple>> {
        self.lock()
            .store
            .query_ranked(pattern, chrono::Utc::now(), limit)
    }

    /// Idempotent write for replication: inserts (and wakes waiters /
    /// publishes) only if this tuple id is not already present. Returns
    /// whether the tuple was new. Remotely-authored tuples arrive here so
    /// repeated sync cycles cannot duplicate them.
    pub fn out_if_new(&self, tuple: Tuple) -> rk_core::Result<bool> {
        let mut inner = self.lock();
        if inner.store.exists(tuple.id)? {
            return Ok(false);
        }
        inner.insert_and_offer(&tuple, false)?;
        drop(inner);
        let _ = self.events.send(tuple);
        Ok(true)
    }

    /// Replace one exact non-furniture revision atomically. A stale revision
    /// returns false; an insert failure rolls back removal of the old tuple.
    /// The replacement retains the same logical key and gets a new record id.
    pub fn replace(&self, expected: rk_core::id::RecordId, tuple: Tuple) -> rk_core::Result<bool> {
        let mut inner = self.lock();
        if !inner.store.replace(expected, &tuple)? {
            return Ok(false);
        }
        inner.offer(&tuple)?;
        drop(inner);
        let _ = self.events.send(tuple);
        Ok(true)
    }

    /// Blocking destructive read: atomically consume the oldest matching
    /// tuple, or wait up to `timeout` for one to arrive. Returns `None` on
    /// timeout. Furniture is never consumable.
    pub async fn take(
        &self,
        pattern: &Pattern,
        timeout: Duration,
    ) -> rk_core::Result<Option<Tuple>> {
        self.blocking_read(pattern, timeout, true).await
    }

    /// Blocking non-destructive read.
    pub async fn rd(&self, pattern: &Pattern, timeout: Duration) -> rk_core::Result<Option<Tuple>> {
        self.blocking_read(pattern, timeout, false).await
    }

    async fn blocking_read(
        &self,
        pattern: &Pattern,
        timeout: Duration,
        destructive: bool,
    ) -> rk_core::Result<Option<Tuple>> {
        // Check-and-register is one critical section; see module docs.
        let (waiter_id, rx) = {
            let mut inner = self.lock();
            let mut found = inner.store.query(pattern, destructive, Some(1))?;
            if let Some(tuple) = found.pop() {
                if destructive {
                    inner.store.delete(tuple.id)?;
                }
                return Ok(Some(tuple));
            }
            let (tx, rx) = oneshot::channel();
            let id = inner.next_waiter_id;
            inner.next_waiter_id = inner.next_waiter_id.wrapping_add(1);
            inner.waiters.push(Waiter {
                id,
                pattern: pattern.clone(),
                destructive,
                tx,
            });
            (id, rx)
        };

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(tuple)) => Ok(Some(tuple)),
            // Remove timed-out waiters immediately. Waiting for a later write
            // to prune them made a long-lived daemon retain every expired
            // reader until the next matching tuple arrived.
            Ok(Err(_)) | Err(_) => {
                let mut inner = self.lock();
                inner.waiters.retain(|waiter| waiter.id != waiter_id);
                debug!(?pattern, destructive, "blocking read timed out");
                Ok(None)
            }
        }
    }

    /// Subscribe to the live feed of every `out` (for `rk watch`, triggers,
    /// and the ledger). Observational only — missing events while lagging is
    /// acceptable here and only here.
    pub fn subscribe(&self) -> broadcast::Receiver<Tuple> {
        self.events.subscribe()
    }

    /// Persist and publish a coordinator event. These events are durable,
    /// non-consumable furniture, so workflow observers can replay them after
    /// disconnecting without competing with daemon participants.
    pub fn out_coordinator(&self, tuple: Tuple) -> rk_core::Result<u64> {
        let tuple = tuple.with_lifecycle(Lifecycle::Furniture);
        let mut inner = self.lock();
        let sequence = inner
            .insert_and_offer(&tuple, true)?
            .expect("coordinator write must allocate a journal sequence");
        drop(inner);

        let _ = self.events.send(tuple.clone());
        let _ = self.coordinator_events.send(CoordinatorEvent {
            cursor: sequence,
            event: tuple,
        });
        Ok(sequence)
    }

    pub fn subscribe_coordinator(&self) -> broadcast::Receiver<CoordinatorEvent> {
        self.coordinator_events.subscribe()
    }

    pub fn coordinator_events_after(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> rk_core::Result<Vec<CoordinatorEvent>> {
        Ok(self
            .lock()
            .store
            .coordinator_events_after(after, limit)?
            .into_iter()
            .map(|(cursor, event)| CoordinatorEvent { cursor, event })
            .collect())
    }

    pub fn coordinator_latest_sequence(&self) -> rk_core::Result<Option<u64>> {
        self.lock().store.coordinator_latest_sequence()
    }

    /// One garbage-collection pass: decay every pheromone trail by `decay_step`
    /// and collect the faded (strength `<= 0`) ones, then sweep any hard-TTL
    /// expiries. Returns the total number collected. The strength decay is the
    /// smooth fade; the TTL sweep is the backstop for tuples that carry an
    /// `expires_at` but no strength (e.g. suggestions, endorsements).
    pub fn gc_expired(&self, decay_step: f64) -> rk_core::Result<usize> {
        let inner = self.lock();
        let faded = inner.store.decay_and_collect(decay_step)?;
        let expired = inner.store.delete_expired(chrono::Utc::now())?;
        Ok(faded + expired)
    }

    pub fn count(&self) -> rk_core::Result<u64> {
        self.lock().store.count()
    }

    /// Count tuples in the given categories without materializing them — a
    /// cheap, order-independent change signal for the reactor's recompute gate.
    pub fn count_in_categories(
        &self,
        categories: &[rk_core::tuple::Category],
    ) -> rk_core::Result<u64> {
        self.lock().store.count_in_categories(categories)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        match self.inner.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rk_core::tuple::Category;
    use serde_json::json;
    use std::time::Duration;

    fn event(identity: &str, payload: serde_json::Value) -> Tuple {
        Tuple::new(Category::Event, "repo", identity, "castle", payload)
    }

    fn claim(area: &str, agent: &str, strength: f64) -> Tuple {
        let mut t = Tuple::new(Category::Claim, "repo", area, agent, json!({}))
            .with_lifecycle(Lifecycle::Ephemeral);
        t.strength = Some(strength);
        t
    }

    const SHORT: Duration = Duration::from_millis(200);
    const LONG: Duration = Duration::from_secs(5);

    #[tokio::test]
    async fn take_returns_existing_tuple_immediately() {
        let space = Space::open_in_memory().unwrap();
        space.out(event("a", json!({}))).unwrap();
        let got = space
            .take(&Pattern::default().identity("a"), SHORT)
            .await
            .unwrap();
        assert!(got.is_some());
        assert_eq!(space.count().unwrap(), 0, "take consumes");
    }

    #[tokio::test]
    async fn rd_does_not_consume() {
        let space = Space::open_in_memory().unwrap();
        space.out(event("a", json!({}))).unwrap();
        let got = space
            .rd(&Pattern::default().identity("a"), SHORT)
            .await
            .unwrap();
        assert!(got.is_some());
        assert_eq!(space.count().unwrap(), 1);
    }

    #[tokio::test]
    async fn blocked_take_wakes_on_out() {
        let space = Space::open_in_memory().unwrap();
        let s2 = space.clone();
        let waiter =
            tokio::spawn(async move { s2.take(&Pattern::default().identity("later"), LONG).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        space.out(event("later", json!({"n": 1}))).unwrap();
        let got = waiter.await.unwrap().unwrap();
        assert_eq!(got.unwrap().identity, "later");
        assert_eq!(space.count().unwrap(), 0, "consumed by waiter");
    }

    #[tokio::test]
    async fn payload_search_waiter_wakes_via_same_predicate() {
        // The predecessor's regression case: a waiter whose pattern includes payload
        // search must be woken by a matching write.
        let space = Space::open_in_memory().unwrap();
        let mut pattern = Pattern::default().identity("task_done");
        pattern.payload_search = Some("\"Whisker\"".into());
        let s2 = space.clone();
        let waiter = tokio::spawn(async move { s2.rd(&pattern, LONG).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Non-matching write must NOT satisfy the waiter...
        space
            .out(event("task_done", json!({"agent": "Nibbles"})))
            .unwrap();
        // ...but the matching one must.
        space
            .out(event("task_done", json!({"agent": "Whisker"})))
            .unwrap();
        let got = waiter.await.unwrap().unwrap().expect("waiter woken");
        assert_eq!(got.payload["agent"], "Whisker");
    }

    #[tokio::test]
    async fn exactly_one_taker_wins_per_tuple() {
        let space = Space::open_in_memory().unwrap();
        let mut handles = Vec::new();
        for _ in 0..8 {
            let s = space.clone();
            handles.push(tokio::spawn(async move {
                s.take(&Pattern::default().identity("contested"), SHORT)
                    .await
            }));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        space.out(event("contested", json!({}))).unwrap();
        let mut winners = 0;
        for h in handles {
            if h.await.unwrap().unwrap().is_some() {
                winners += 1;
            }
        }
        assert_eq!(winners, 1);
        assert_eq!(space.count().unwrap(), 0);
    }

    #[tokio::test]
    async fn rd_waiters_all_observe_a_consumed_tuple() {
        let space = Space::open_in_memory().unwrap();
        let mut rds = Vec::new();
        for _ in 0..4 {
            let s = space.clone();
            rds.push(tokio::spawn(async move {
                s.rd(&Pattern::default().identity("shared"), LONG).await
            }));
        }
        let taker = {
            let s = space.clone();
            tokio::spawn(async move { s.take(&Pattern::default().identity("shared"), LONG).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        space.out(event("shared", json!({}))).unwrap();
        for rd in rds {
            assert!(rd.await.unwrap().unwrap().is_some(), "every rd observes");
        }
        assert!(taker.await.unwrap().unwrap().is_some());
        assert_eq!(space.count().unwrap(), 0);
    }

    #[tokio::test]
    async fn furniture_cannot_be_taken_but_can_be_read() {
        let space = Space::open_in_memory().unwrap();
        space
            .out(event("perm", json!({})).with_lifecycle(Lifecycle::Furniture))
            .unwrap();
        let taken = space
            .take(&Pattern::default().identity("perm"), SHORT)
            .await
            .unwrap();
        assert!(taken.is_none());
        let read = space
            .rd(&Pattern::default().identity("perm"), SHORT)
            .await
            .unwrap();
        assert!(read.is_some());
        assert_eq!(space.count().unwrap(), 1);
    }

    #[tokio::test]
    async fn timed_out_waiters_do_not_steal_later_tuples() {
        let space = Space::open_in_memory().unwrap();
        // This waiter times out before the write arrives.
        let expired = space
            .take(
                &Pattern::default().identity("slow"),
                Duration::from_millis(30),
            )
            .await
            .unwrap();
        assert!(expired.is_none());
        // A dead waiter must not consume the tuple when it finally arrives.
        space.out(event("slow", json!({}))).unwrap();
        let got = space
            .take(&Pattern::default().identity("slow"), SHORT)
            .await
            .unwrap();
        assert!(got.is_some(), "tuple survived the dead waiter");
    }

    #[tokio::test]
    async fn timed_out_waiters_are_removed_without_a_later_write() {
        let space = Space::open_in_memory().unwrap();
        let got = space
            .rd(
                &Pattern::default().identity("never-arrives"),
                Duration::from_millis(5),
            )
            .await
            .unwrap();
        assert!(got.is_none());
        assert_eq!(space.lock().waiters.len(), 0);
    }

    #[test]
    fn out_if_new_is_atomic_for_concurrent_replication() {
        let space = Space::open_in_memory().unwrap();
        let tuple = event("replicated", json!({"source": "peer"}));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let s = space.clone();
            let t = tuple.clone();
            handles.push(std::thread::spawn(move || s.out_if_new(t).unwrap()));
        }
        let inserted = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .filter(|inserted| *inserted)
            .count();
        assert_eq!(inserted, 1, "exactly one replica write wins");
        assert_eq!(space.count().unwrap(), 1);
    }

    #[test]
    fn reinforce_upserts_same_key_and_resets_strength() {
        let space = Space::open_in_memory().unwrap();
        let first = space
            .reinforce(claim("src/lib.rs", "Nibbles", 1.0))
            .unwrap();

        // Same (category, scope, identity, instance): a decayed-then-reinforced
        // trail refreshes in place — one row, same id, strength back to full.
        let mut decayed = claim("src/lib.rs", "Nibbles", 0.2);
        decayed.payload = json!({"note": "refreshed"});
        let second = space.reinforce(decayed).unwrap();

        assert_eq!(second.id, first.id, "reinforcement keeps the original id");
        let rows = space.scan(&Pattern::category(Category::Claim)).unwrap();
        assert_eq!(rows.len(), 1, "no duplicate trail");
        assert_eq!(rows[0].strength, Some(1.0));
        assert_eq!(rows[0].payload, json!({"note": "refreshed"}));
    }

    #[test]
    fn reinforce_distinct_agents_do_not_collide() {
        let space = Space::open_in_memory().unwrap();
        space.reinforce(claim("area", "Nibbles", 1.0)).unwrap();
        space.reinforce(claim("area", "Whisker", 1.0)).unwrap();
        // Different instance (agent) => different key => two separate trails.
        assert_eq!(
            space
                .scan(&Pattern::category(Category::Claim))
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn reinforce_fresh_write_wakes_a_waiter() {
        let space = Space::open_in_memory().unwrap();
        let s2 = space.clone();
        let waiter =
            tokio::spawn(async move { s2.rd(&Pattern::default().identity("area"), LONG).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        space.reinforce(claim("area", "Nibbles", 1.0)).unwrap();
        assert!(
            waiter.await.unwrap().unwrap().is_some(),
            "waiter woken by reinforce"
        );
    }

    #[test]
    fn gc_decays_then_collects_faded_trails() {
        let space = Space::open_in_memory().unwrap();
        // Seed strengths directly via `out` (reinforce would reset to full).
        space.out(claim("bright", "Nibbles", 1.0)).unwrap();
        space.out(claim("faint", "Nibbles", 0.05)).unwrap();

        // One decay step of 0.1: the faint trail crosses zero and is collected;
        // the bright one survives with reduced strength.
        let collected = space.gc_expired(0.1).unwrap();
        assert_eq!(collected, 1);
        let rows = space.scan(&Pattern::category(Category::Claim)).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].identity, "bright");
        assert!((rows[0].strength.unwrap() - 0.9).abs() < 1e-9);
    }

    #[tokio::test]
    async fn watch_feed_sees_all_writes() {
        let space = Space::open_in_memory().unwrap();
        let mut rx = space.subscribe();
        space.out(event("one", json!({}))).unwrap();
        space.out(event("two", json!({}))).unwrap();
        assert_eq!(rx.recv().await.unwrap().identity, "one");
        assert_eq!(rx.recv().await.unwrap().identity, "two");
    }

    #[tokio::test]
    async fn coordinator_events_are_ordered_durable_and_non_consumable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("space.db");
        let space = Space::open(&path).unwrap();
        let first = space
            .out_coordinator(
                event(
                    "workflow_state_changed",
                    json!({"instance": "wf-1", "revision": 1}),
                )
                .with_lifecycle(Lifecycle::Furniture),
            )
            .unwrap();
        let second = space
            .out_coordinator(
                event(
                    "workflow_state_changed",
                    json!({"instance": "wf-1", "revision": 2}),
                )
                .with_lifecycle(Lifecycle::Furniture),
            )
            .unwrap();
        assert!(first < second);
        assert_eq!(space.coordinator_latest_sequence().unwrap(), Some(second));
        assert!(space
            .take(
                &Pattern::default().identity("workflow_state_changed"),
                SHORT,
            )
            .await
            .unwrap()
            .is_none());
        drop(space);

        let reopened = Space::open(&path).unwrap();
        let replay = reopened.coordinator_events_after(Some(first), 10).unwrap();
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].cursor, second);
        assert_eq!(replay[0].event.payload["revision"], 2);
    }

    /// Stress: many concurrent writers and takers with randomized timing;
    /// every tuple is taken exactly once, no lost wakeups, no double-consume.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn stress_no_lost_wakeups_no_double_consume() {
        use rand::Rng;
        const WRITERS: usize = 8;
        const PER_WRITER: usize = 25;

        let space = Space::open_in_memory().unwrap();
        let mut takers = Vec::new();
        for w in 0..WRITERS {
            for i in 0..PER_WRITER {
                let s = space.clone();
                let identity = format!("t-{w}-{i}");
                takers.push(tokio::spawn(async move {
                    // Randomize whether the taker arrives before or after the write.
                    let jitter = rand::thread_rng().gen_range(0..8);
                    tokio::time::sleep(Duration::from_millis(jitter)).await;
                    s.take(
                        &Pattern::default().identity(&identity),
                        Duration::from_secs(10),
                    )
                    .await
                    .unwrap()
                }));
            }
        }

        let mut writers = Vec::new();
        for w in 0..WRITERS {
            let s = space.clone();
            writers.push(tokio::spawn(async move {
                for i in 0..PER_WRITER {
                    let jitter = rand::thread_rng().gen_range(0..8);
                    tokio::time::sleep(Duration::from_millis(jitter)).await;
                    s.out(event(&format!("t-{w}-{i}"), json!({"w": w, "i": i})))
                        .unwrap();
                }
            }));
        }

        for w in writers {
            w.await.unwrap();
        }
        for t in takers {
            let got = t.await.unwrap();
            assert!(got.is_some(), "lost wakeup: a taker timed out");
        }
        assert_eq!(
            space.count().unwrap(),
            0,
            "every tuple consumed exactly once"
        );
    }
}

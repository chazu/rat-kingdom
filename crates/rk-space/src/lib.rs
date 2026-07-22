//! The rat-kingdom tuplespace: Linda primitives (`out`, `in`/`take`, `rd`,
//! `scan`) with blocking reads and no lost wakeups.
//!
//! # Concurrency design (the imp lesson, fixed structurally)
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

use rk_core::tuple::{Lifecycle, Pattern, Tuple};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, oneshot};
use tracing::debug;

use store::Store;

/// Capacity of the watch/event feed. Laggy watchers miss events (they are
/// observers, not participants — waiters never ride this channel).
const EVENT_FEED_CAPACITY: usize = 1024;

struct Waiter {
    pattern: Pattern,
    destructive: bool,
    tx: oneshot::Sender<Tuple>,
}

struct Inner {
    store: Store,
    waiters: Vec<Waiter>,
}

/// A shared handle to the tuplespace. Cheap to clone.
#[derive(Clone)]
pub struct Space {
    inner: Arc<Mutex<Inner>>,
    events: broadcast::Sender<Tuple>,
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
        Self {
            inner: Arc::new(Mutex::new(Inner {
                store,
                waiters: Vec::new(),
            })),
            events,
        }
    }

    /// Write a tuple: persist, wake matching waiters, publish to the event
    /// feed. If a destructive waiter consumes the tuple it is removed again
    /// before the lock is released; `rd` waiters observing it still get it.
    pub fn out(&self, tuple: Tuple) -> rk_core::Result<()> {
        let mut inner = self.lock();
        inner.store.insert(&tuple)?;

        let mut consumed = false;
        let consumable = tuple.lifecycle != Lifecycle::Furniture;
        // Drain-and-retain: offer to every matching rd waiter, and to the first
        // matching take waiter still listening. Dropped receivers (timed out)
        // are pruned as we go.
        let waiters = std::mem::take(&mut inner.waiters);
        for waiter in waiters {
            let matches = waiter.pattern.matches(&tuple);
            if !matches {
                inner.waiters.push(waiter);
                continue;
            }
            if waiter.destructive {
                if consumed || !consumable {
                    inner.waiters.push(waiter);
                    continue;
                }
                match waiter.tx.send(tuple.clone()) {
                    Ok(()) => consumed = true,
                    Err(_) => {} // receiver gone (timeout); drop the waiter
                }
            } else {
                // Non-destructive: deliver and drop the waiter (rd is one-shot).
                let _ = waiter.tx.send(tuple.clone());
            }
        }
        if consumed {
            inner.store.delete(tuple.id)?;
        }
        drop(inner);

        let _ = self.events.send(tuple);
        Ok(())
    }

    /// Non-blocking read of all matching tuples, oldest first.
    pub fn scan(&self, pattern: &Pattern) -> rk_core::Result<Vec<Tuple>> {
        self.lock().store.query(pattern, false, None)
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
        let rx = {
            let mut inner = self.lock();
            let mut found = inner.store.query(pattern, destructive, Some(1))?;
            if let Some(tuple) = found.pop() {
                if destructive {
                    inner.store.delete(tuple.id)?;
                }
                return Ok(Some(tuple));
            }
            let (tx, rx) = oneshot::channel();
            inner.waiters.push(Waiter {
                pattern: pattern.clone(),
                destructive,
                tx,
            });
            rx
        };

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(tuple)) => Ok(Some(tuple)),
            // Sender dropped without sending (should not happen) or timeout:
            // remove our (now dead) waiter entry lazily via out()'s pruning.
            Ok(Err(_)) | Err(_) => {
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

    /// Collect expired ephemeral tuples. Returns the number collected.
    pub fn gc_expired(&self) -> rk_core::Result<usize> {
        self.lock().store.delete_expired(chrono::Utc::now())
    }

    pub fn count(&self) -> rk_core::Result<u64> {
        self.lock().store.count()
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
        // The imp regression case: a waiter whose pattern includes payload
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
    async fn watch_feed_sees_all_writes() {
        let space = Space::open_in_memory().unwrap();
        let mut rx = space.subscribe();
        space.out(event("one", json!({}))).unwrap();
        space.out(event("two", json!({}))).unwrap();
        assert_eq!(rx.recv().await.unwrap().identity, "one");
        assert_eq!(rx.recv().await.unwrap().identity, "two");
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

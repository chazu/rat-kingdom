//! Durable orchestrator lease: the seam a primed external LLM session
//! acquires before it may act on an `Orchestrator`-authority attention item
//! (`crate::attention`). Modeled directly on `action_approval.rs`'s
//! JSON-file-backed store — one file, atomic temp-write-then-rename, reloaded
//! verbatim on daemon restart — so a lease and its cursor survive a
//! disconnect, an orchestrator replacement, or a daemon restart the same way
//! an action-approval grant does.
//!
//! One lease per repository scope. The SAME holder re-`acquire`ing (after a
//! disconnect, or after this daemon restarted and reloaded the file) gets its
//! generation and cursor back untouched, with only the expiry extended — this
//! is the "safely resumes" path the ticket asks for. A DIFFERENT holder may
//! take over only once the existing lease has expired — "replacement" — and
//! that bumps the generation, which fences every call the old holder makes
//! afterward: `renew`/`advance_cursor` check the presented generation against
//! the stored one and refuse once it has moved on, so a stale orchestrator
//! session cannot keep acting after it has been superseded.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Lease {
    pub scope: String,
    pub holder: String,
    pub generation: u64,
    pub acquired_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    /// The resumable cursor: the id of the last attention item this lease
    /// has recorded a decision for and acknowledged. `None` before the first
    /// decision — `crate::attention::next_attention` starts from the
    /// beginning of the sorted violation list in that case.
    pub cursor: Option<String>,
}

impl Lease {
    pub fn is_live(&self, now: DateTime<Utc>) -> bool {
        self.expires_at > now
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct StoreData {
    leases: BTreeMap<String, Lease>,
}

pub struct LeaseStore {
    path: PathBuf,
    data: Mutex<StoreData>,
}

impl LeaseStore {
    pub fn load(path: impl AsRef<Path>) -> rk_core::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let data = if path.exists() {
            serde_json::from_str(&std::fs::read_to_string(&path)?)?
        } else {
            StoreData::default()
        };
        Ok(Self {
            path,
            data: Mutex::new(data),
        })
    }

    fn lock(&self) -> rk_core::Result<std::sync::MutexGuard<'_, StoreData>> {
        self.data
            .lock()
            .map_err(|_| rk_core::Error::other("orchestrator lease store lock poisoned"))
    }

    fn persist(&self, data: &StoreData) -> rk_core::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(data)?)?;
        std::fs::rename(tmp, &self.path)?;
        Ok(())
    }

    pub fn acquire(
        &self,
        scope: &str,
        holder: &str,
        ttl_secs: i64,
        now: DateTime<Utc>,
    ) -> rk_core::Result<Lease> {
        let ttl = Duration::seconds(ttl_secs.clamp(1, 3600));
        let mut data = self.lock()?;
        let lease = match data.leases.get(scope) {
            Some(existing) if existing.holder == holder => Lease {
                expires_at: now + ttl,
                ..existing.clone()
            },
            Some(existing) if existing.is_live(now) => {
                return Err(rk_core::Error::other(format!(
                    "lease for {scope} is held by {} until {}",
                    existing.holder, existing.expires_at
                )));
            }
            Some(existing) => Lease {
                scope: scope.to_string(),
                holder: holder.to_string(),
                generation: existing.generation + 1,
                acquired_at: now,
                expires_at: now + ttl,
                // The cursor is a property of the SCOPE's progress through its
                // attention items, not of who happens to hold the lease — a
                // replacement orchestrator resumes exactly where its
                // predecessor left off rather than re-deciding settled items.
                cursor: existing.cursor.clone(),
            },
            None => Lease {
                scope: scope.to_string(),
                holder: holder.to_string(),
                generation: 1,
                acquired_at: now,
                expires_at: now + ttl,
                cursor: None,
            },
        };
        data.leases.insert(scope.to_string(), lease.clone());
        self.persist(&data)?;
        Ok(lease)
    }

    fn check_fence(
        &self,
        data: &StoreData,
        scope: &str,
        holder: &str,
        generation: u64,
        now: DateTime<Utc>,
    ) -> rk_core::Result<()> {
        let lease = data
            .leases
            .get(scope)
            .ok_or_else(|| rk_core::Error::other(format!("no lease held for {scope}")))?;
        if lease.holder != holder || lease.generation != generation {
            return Err(rk_core::Error::other(format!(
                "lease fencing failed for {scope}: held by {} generation {} (presented {holder} generation {generation})",
                lease.holder, lease.generation
            )));
        }
        if !lease.is_live(now) {
            return Err(rk_core::Error::other(format!(
                "lease for {scope} expired at {}",
                lease.expires_at
            )));
        }
        Ok(())
    }

    pub fn renew(
        &self,
        scope: &str,
        holder: &str,
        generation: u64,
        ttl_secs: i64,
        now: DateTime<Utc>,
    ) -> rk_core::Result<Lease> {
        let ttl = Duration::seconds(ttl_secs.clamp(1, 3600));
        let mut data = self.lock()?;
        self.check_fence(&data, scope, holder, generation, now)?;
        let lease = data
            .leases
            .get_mut(scope)
            .expect("presence checked by check_fence");
        lease.expires_at = now + ttl;
        let out = lease.clone();
        self.persist(&data)?;
        Ok(out)
    }

    /// Advance the resumable cursor. Fenced identically to [`renew`] — a
    /// holder that has been superseded (generation moved on) cannot
    /// acknowledge attention items into a lease it no longer holds.
    pub fn advance_cursor(
        &self,
        scope: &str,
        holder: &str,
        generation: u64,
        cursor: &str,
        now: DateTime<Utc>,
    ) -> rk_core::Result<Lease> {
        let mut data = self.lock()?;
        self.check_fence(&data, scope, holder, generation, now)?;
        let lease = data
            .leases
            .get_mut(scope)
            .expect("presence checked by check_fence");
        lease.cursor = Some(cursor.to_string());
        let out = lease.clone();
        self.persist(&data)?;
        Ok(out)
    }

    pub fn current(&self, scope: &str) -> rk_core::Result<Option<Lease>> {
        Ok(self.lock()?.leases.get(scope).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> DateTime<Utc> {
        Utc::now()
    }

    #[test]
    fn acquiring_a_fresh_scope_starts_at_generation_one_with_no_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let store = LeaseStore::load(dir.path().join("lease.json")).unwrap();
        let lease = store.acquire("myrepo", "orch-a", 300, now()).unwrap();
        assert_eq!(lease.generation, 1);
        assert_eq!(lease.cursor, None);
    }

    #[test]
    fn the_same_holder_reacquiring_keeps_generation_and_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let store = LeaseStore::load(dir.path().join("lease.json")).unwrap();
        let first = store.acquire("myrepo", "orch-a", 300, now()).unwrap();
        store
            .advance_cursor("myrepo", "orch-a", first.generation, "v:1", now())
            .unwrap();
        let second = store.acquire("myrepo", "orch-a", 300, now()).unwrap();
        assert_eq!(second.generation, first.generation);
        assert_eq!(second.cursor.as_deref(), Some("v:1"));
    }

    #[test]
    fn a_different_holder_cannot_preempt_a_live_lease() {
        let dir = tempfile::tempdir().unwrap();
        let store = LeaseStore::load(dir.path().join("lease.json")).unwrap();
        store.acquire("myrepo", "orch-a", 300, now()).unwrap();
        let err = store
            .acquire("myrepo", "orch-b", 300, now())
            .unwrap_err()
            .to_string();
        assert!(err.contains("held by orch-a"), "{err}");
    }

    #[test]
    fn a_different_holder_replaces_an_expired_lease_and_bumps_generation() {
        let dir = tempfile::tempdir().unwrap();
        let store = LeaseStore::load(dir.path().join("lease.json")).unwrap();
        let first = store.acquire("myrepo", "orch-a", 300, now()).unwrap();
        store
            .advance_cursor("myrepo", "orch-a", first.generation, "v:1", now())
            .unwrap();
        let expired_now = now() + Duration::seconds(400);
        let second = store.acquire("myrepo", "orch-b", 300, expired_now).unwrap();
        assert_eq!(second.holder, "orch-b");
        assert_eq!(second.generation, first.generation + 1);
        // Cursor carries over: the replacement resumes, it does not restart.
        assert_eq!(second.cursor.as_deref(), Some("v:1"));
    }

    #[test]
    fn a_stale_generation_is_fenced_out_of_renew_and_advance() {
        let dir = tempfile::tempdir().unwrap();
        let store = LeaseStore::load(dir.path().join("lease.json")).unwrap();
        let first = store.acquire("myrepo", "orch-a", 300, now()).unwrap();
        let expired_now = now() + Duration::seconds(400);
        store.acquire("myrepo", "orch-b", 300, expired_now).unwrap();

        let err = store
            .renew("myrepo", "orch-a", first.generation, 300, expired_now)
            .unwrap_err()
            .to_string();
        assert!(err.contains("fencing failed"), "{err}");

        let err = store
            .advance_cursor("myrepo", "orch-a", first.generation, "v:2", expired_now)
            .unwrap_err()
            .to_string();
        assert!(err.contains("fencing failed"), "{err}");
    }

    #[test]
    fn lease_state_persists_across_a_reload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lease.json");
        let acquired = {
            let store = LeaseStore::load(&path).unwrap();
            let lease = store.acquire("myrepo", "orch-a", 300, now()).unwrap();
            store
                .advance_cursor("myrepo", "orch-a", lease.generation, "v:1", now())
                .unwrap()
        };
        let reloaded = LeaseStore::load(&path).unwrap();
        let current = reloaded.current("myrepo").unwrap().unwrap();
        assert_eq!(current, acquired);
        // The same holder can keep going against the reloaded store exactly
        // as if the daemon never restarted.
        let renewed = reloaded
            .renew("myrepo", "orch-a", current.generation, 300, now())
            .unwrap();
        assert_eq!(renewed.generation, current.generation);
    }

    #[test]
    fn current_on_an_unknown_scope_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = LeaseStore::load(dir.path().join("lease.json")).unwrap();
        assert_eq!(store.current("myrepo").unwrap(), None);
    }
}

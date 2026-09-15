//! P7.1: an operator-only handoff window that stops *new* landing admission
//! for one repository at a safe boundary, without draining or cancelling
//! whatever is already durably queued or actively running.
//!
//! The fence never touches [`LandingPipeline::enqueue_disposition`] — a
//! candidate may still be durably queued while the fence is engaged (`rk
//! land`'s durable acknowledgment is unaffected). It only gates the moment a
//! `(repo, target)` key's exclusive drain lane would next claim NEW work:
//! [`LandingPipeline::drain_key`], [`LandingPipeline::drive_key_as_owner`]
//! and [`LandingPipeline::spawn_background_drain`] each re-check
//! [`LandingPipeline::admission_fenced`] immediately before their next
//! `claim_batch`/`claim_next` call, strictly AFTER already holding that
//! key's exclusive lock — so the check is atomic with the claim it gates,
//! not a separate status-then-claim race. Work already claimed (mid
//! `process_entry`, including a live gate/review) is never interrupted; it
//! finishes exactly as it would without a fence. This makes "ready" a live,
//! computed property rather than stored state: no `(repo, target)` key
//! currently holds its exclusive lock (see [`LandingPipeline::active_keys`]).
//!
//! The fence record itself is a small file-backed store — modeled directly
//! on [`crate::orchestrator_lease::LeaseStore`] — so a request survives a
//! daemon restart, is idempotent for its own holder, is fenced against a
//! stale/superseded holder via a bumped `generation`, and auto-lifts once
//! its bounded `deadline_at` passes without an explicit release: an
//! abandoned request cannot wedge landing admission forever (only the ONE
//! repo it names; every other repo's admission is untouched throughout).

use super::*;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum HandoffFenceState {
    Requested,
    Released,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HandoffFenceRecord {
    pub(crate) repo: String,
    pub(crate) holder: String,
    pub(crate) generation: u64,
    pub(crate) state: HandoffFenceState,
    pub(crate) requested_at: DateTime<Utc>,
    pub(crate) deadline_at: DateTime<Utc>,
}

impl HandoffFenceRecord {
    /// Live blocking iff the record is still `Requested` and its bounded
    /// deadline has not yet passed. A `Released` record, or one whose
    /// deadline has elapsed, never blocks new admission again — it is kept
    /// on disk only so `fence_status` can still report it.
    fn blocks_admission(&self, now: DateTime<Utc>) -> bool {
        self.state == HandoffFenceState::Requested && now <= self.deadline_at
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct HandoffStoreData {
    fences: std::collections::BTreeMap<String, HandoffFenceRecord>,
}

/// One JSON file under the daemon home, one record per repo. Deliberately
/// infallible to load: an unreadable/corrupt file degrades to "no fence
/// engaged" (logged) rather than failing daemon startup or `LandingPipeline`
/// construction over a best-effort operator convenience feature — the same
/// posture [`crate::orchestrator_lease::LeaseStore`] takes toward its own
/// callers propagating `?`, adapted here because [`LandingPipeline::new`]
/// itself has no `Result` to propagate through (it is built inside a
/// `OnceLock::get_or_init` closure — see `Server::landing`).
pub(crate) struct HandoffFenceStore {
    path: PathBuf,
    data: Mutex<HandoffStoreData>,
}

impl HandoffFenceStore {
    pub(crate) fn load(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        let data = if path.exists() {
            std::fs::read_to_string(&path)
                .ok()
                .and_then(|raw| serde_json::from_str(&raw).ok())
                .unwrap_or_else(|| {
                    warn!(path = %path.display(), "landing handoff fence store unreadable, starting empty");
                    HandoffStoreData::default()
                })
        } else {
            HandoffStoreData::default()
        };
        Self {
            path,
            data: Mutex::new(data),
        }
    }

    fn persist(&self, data: &HandoffStoreData) -> rk_core::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(data)?)?;
        std::fs::rename(tmp, &self.path)?;
        Ok(())
    }

    pub(crate) fn current(&self, repo: &str) -> Option<HandoffFenceRecord> {
        self.data.lock().unwrap().fences.get(repo).cloned()
    }

    /// Idempotent for the SAME live holder (renews `deadline_at`, keeps the
    /// generation). A different holder may only take over once the existing
    /// record is `Released` or its deadline has passed — that transition
    /// bumps `generation`, fencing anything the previous holder does next
    /// (mirrors [`crate::orchestrator_lease::LeaseStore::acquire`]).
    pub(crate) fn request(
        &self,
        repo: &str,
        holder: &str,
        ttl_secs: i64,
        now: DateTime<Utc>,
    ) -> rk_core::Result<HandoffFenceRecord> {
        let ttl = chrono::Duration::seconds(ttl_secs.clamp(1, 3600));
        let mut data = self.data.lock().unwrap();
        let record = match data.fences.get(repo) {
            Some(existing) if existing.holder == holder && existing.blocks_admission(now) => {
                HandoffFenceRecord {
                    deadline_at: now + ttl,
                    ..existing.clone()
                }
            }
            Some(existing) if existing.blocks_admission(now) => {
                return Err(rk_core::Error::other(format!(
                    "landing handoff fence for {repo} is held by {} until {}",
                    existing.holder, existing.deadline_at
                )));
            }
            Some(existing) => HandoffFenceRecord {
                repo: repo.to_string(),
                holder: holder.to_string(),
                generation: existing.generation + 1,
                state: HandoffFenceState::Requested,
                requested_at: now,
                deadline_at: now + ttl,
            },
            None => HandoffFenceRecord {
                repo: repo.to_string(),
                holder: holder.to_string(),
                generation: 1,
                state: HandoffFenceState::Requested,
                requested_at: now,
                deadline_at: now + ttl,
            },
        };
        data.fences.insert(repo.to_string(), record.clone());
        self.persist(&data)?;
        Ok(record)
    }

    /// Release is idempotent and safe to repeat: a missing record, an
    /// already-`Released` record, or a record already past its own deadline
    /// all report success rather than an error — none of them are still
    /// blocking anything, so there is nothing left to release. Only a
    /// STILL-LIVE record held by a different `(holder, generation)` is
    /// refused, so a stale or superseded caller cannot release someone
    /// else's active fence out from under them.
    pub(crate) fn release(
        &self,
        repo: &str,
        holder: &str,
        generation: u64,
        now: DateTime<Utc>,
    ) -> rk_core::Result<()> {
        let mut data = self.data.lock().unwrap();
        let Some(existing) = data.fences.get_mut(repo) else {
            return Ok(());
        };
        if !existing.blocks_admission(now) {
            return Ok(());
        }
        if existing.holder != holder || existing.generation != generation {
            return Err(rk_core::Error::other(format!(
                "landing handoff fence for {repo} is held by {} generation {} \
                 (presented {holder} generation {generation})",
                existing.holder, existing.generation
            )));
        }
        existing.state = HandoffFenceState::Released;
        self.persist(&data)?;
        Ok(())
    }
}

impl LandingPipeline {
    /// True iff new admission is currently blocked for `repo_name` — the
    /// single question every claim-gating call site in this file asks
    /// before claiming fresh work. See the module doc for exactly what this
    /// does and does not affect.
    pub(crate) fn admission_fenced(&self, repo_name: &str) -> bool {
        self.handoff
            .current(repo_name)
            .is_some_and(|record| record.blocks_admission(Utc::now()))
    }

    /// Every `(repo_name, *)` key whose exclusive drain lane is held RIGHT
    /// NOW — i.e. genuinely active work, not merely queued work. Used by
    /// `fence_status` to report blockers and compute `ready`. A non-blocking
    /// peek (`try_lock`): it never waits on, and never itself becomes, a
    /// key's owner.
    pub(crate) fn active_keys(&self, repo_name: &str) -> Vec<String> {
        let prefix = format!("{repo_name}\0");
        let locks = self.key_locks.lock().unwrap();
        locks
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .filter_map(|(key, lock)| match lock.try_lock() {
                Ok(_guard) => None,
                Err(_) => Some(key[prefix.len()..].to_string()),
            })
            .collect()
    }

    /// `repo.land.fence_request` — engage the handoff fence for `repo_name`.
    /// See the module doc; durable, idempotent for the same live holder,
    /// bumps `generation` on takeover from an expired/released prior fence.
    pub(crate) fn fence_request(
        &self,
        repo_name: &str,
        holder: &str,
        ttl_secs: i64,
    ) -> rk_core::Result<Value> {
        let record = self
            .handoff
            .request(repo_name, holder, ttl_secs, Utc::now())?;
        Ok(self.fence_status_json(repo_name, &record))
    }

    /// `repo.land.fence_release` — end the fence early (idempotent; see
    /// [`HandoffFenceStore::release`]). Admission resumes for `repo_name` on
    /// the very next claim attempt; nothing is force-cancelled or re-run.
    pub(crate) fn fence_release(
        &self,
        repo_name: &str,
        holder: &str,
        generation: u64,
    ) -> rk_core::Result<Value> {
        self.handoff
            .release(repo_name, holder, generation, Utc::now())?;
        Ok(self.fence_status(repo_name))
    }

    /// `repo.land.fence_status` — read-only report: state, blockers, and
    /// whether it is safe to proceed with a rollover. `state` is one of
    /// `"released"` (no fence, or explicitly released), `"expired"` (its
    /// deadline passed without release — admission has already resumed),
    /// `"draining"` (engaged, at least one key still actively working), or
    /// `"ready"` (engaged, nothing currently holds a key's drain lane —
    /// later candidates may still sit durably queued; see the module doc).
    pub(crate) fn fence_status(&self, repo_name: &str) -> Value {
        match self.handoff.current(repo_name) {
            Some(record) => self.fence_status_json(repo_name, &record),
            None => json!({
                "repo": repo_name,
                "state": "released",
                "fenced": false,
                "blocking_keys": Vec::<String>::new(),
            }),
        }
    }

    fn fence_status_json(&self, repo_name: &str, record: &HandoffFenceRecord) -> Value {
        let now = Utc::now();
        let blockers = self.active_keys(repo_name);
        let state = match record.state {
            HandoffFenceState::Released => "released",
            HandoffFenceState::Requested if now > record.deadline_at => "expired",
            HandoffFenceState::Requested if blockers.is_empty() => "ready",
            HandoffFenceState::Requested => "draining",
        };
        json!({
            "repo": repo_name,
            "state": state,
            "fenced": record.blocks_admission(now),
            "holder": record.holder,
            "generation": record.generation,
            "requested_at": record.requested_at,
            "deadline_at": record.deadline_at,
            "blocking_keys": blockers,
        })
    }
}

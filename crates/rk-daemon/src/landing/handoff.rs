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
//! key's exclusive lock, and under a shared per-repo admission gate
//! ([`LandingPipeline::admission_gate`]) held across check-and-claim — so
//! the check is linearized with the claim it gates, not a separate
//! status-then-claim race. `fence_request` takes that same gate
//! exclusively while it flips the record, so once a request is
//! ACKNOWLEDGED, every claim window that could still have observed the
//! pre-fence answer has already finished: no new queued entry can win the
//! covered drain lane after the declared boundary. Work already claimed (mid
//! `process_entry`, including a live gate/review) is never interrupted; it
//! finishes exactly as it would without a fence.
//!
//! # What `ready` covers, and what it does not
//!
//! `ready` is a live computed property, never stored state, and it is a
//! SAFETY CLAIM: "an ordinary daemon stop for this repo will not hang on
//! work this daemon still owns." Three independent dimensions must all be
//! clear, because each can hang a stop on its own and none can see the
//! others:
//!
//! 1. **Landing drain lanes** — no `(repo, target)` key currently holds its
//!    exclusive lock ([`LandingPipeline::active_keys`]).
//! 2. **Managed verification runs** — no `verify.run` bound to this repo is
//!    executing OR queued behind an admission permit
//!    ([`crate::managed_verification::ManagedVerificationRuns::active_for_repo`]).
//!    A managed check holds NO landing key, so dimension 1 is blind to it,
//!    yet it is exactly the "owned managed work that would make ordinary
//!    shutdown hang" the ticket names.
//! 3. **Release preparation** — `release.prepare`/`release.select` hold a
//!    single DAEMON-WIDE `release_prepare_lock`, so an in-flight prepare is
//!    reported as a daemon-scope blocker (`scope: "daemon"`) even when it
//!    names another repository. It is inside the fence for READINESS
//!    purposes — a rollover replaces the whole daemon — while remaining
//!    outside it for ADMISSION purposes, since the fence gates only this
//!    repo's landing claims.
//!
//! These are REPORTED, never cancelled: the ticket requires reusing the
//! existing managed-run and release status/cancellation contracts rather
//! than silently killing an operator's own jobs. A bounded deadline reports
//! what remains active; it never fabricates readiness and never forces a
//! cancellation.
//!
//! Queued-but-unclaimed landing candidates are deliberately NOT blockers —
//! readiness never requires pending entries to disappear. That is the whole
//! point of the window.
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

/// One JSON file under the daemon home, one record per repo.
///
/// Loading is infallible in the sense that it never fails daemon startup —
/// [`LandingPipeline::new`] is built inside a `OnceLock::get_or_init`
/// closure (see `Server::landing`) and has no `Result` to propagate. But an
/// existing-yet-unreadable file is NOT silently equivalent to "no fence
/// engaged": an operator may have acknowledged a fence before the restart,
/// and quietly reporting `released` would hand them a fabricated
/// safe-to-roll-over answer — the exact failure P7.1 exists to prevent.
///
/// So a failed load is recorded in `load_failure` and the store enters an
/// explicit UNAVAILABLE posture with three deliberate properties:
///
/// 1. **Never ready.** `fence_status` reports `state: "unavailable"` with
///    `ready: false` and a `recovery` action. Readiness is a safety claim;
///    we cannot make it from a store we could not read.
/// 2. **Never a permanent invisible pause.** Admission is NOT blocked while
///    unavailable. The ticket forbids a restart leaving an invisible
///    permanent pause, and an unreadable file carries no deadline we could
///    bound one by. Landing keeps working exactly as it does with no fence;
///    what changes is only that we refuse to *claim* readiness.
/// 3. **Never destructive.** The unreadable bytes are preserved — moved
///    aside to `<path>.corrupt` on first write rather than overwritten — so
///    the prior record stays recoverable by hand.
pub(crate) struct HandoffFenceStore {
    path: PathBuf,
    data: Mutex<HandoffStoreData>,
    /// `Some(reason)` when an existing store file could not be read or
    /// parsed at load. Sticky until a successful write replaces the file.
    load_failure: Mutex<Option<String>>,
}

impl HandoffFenceStore {
    pub(crate) fn load(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        let (data, load_failure) = if path.exists() {
            match std::fs::read_to_string(&path)
                .map_err(|e| e.to_string())
                .and_then(|raw| {
                    serde_json::from_str::<HandoffStoreData>(&raw).map_err(|e| e.to_string())
                }) {
                Ok(data) => (data, None),
                Err(reason) => {
                    // Loud, and NOT silently downgraded to "no fence": see
                    // the struct doc. A fence acknowledged before this
                    // restart may still be genuinely owed to an operator.
                    warn!(
                        path = %path.display(), error = %reason,
                        "landing handoff fence store unreadable; refusing to \
                         report readiness until it is recovered or explicitly \
                         re-requested"
                    );
                    (HandoffStoreData::default(), Some(reason))
                }
            }
        } else {
            (HandoffStoreData::default(), None)
        };
        Self {
            path,
            data: Mutex::new(data),
            load_failure: Mutex::new(load_failure),
        }
    }

    /// The recorded load failure, if this store came up unreadable and has
    /// not been rewritten since.
    pub(crate) fn load_failure(&self) -> Option<String> {
        self.load_failure.lock().unwrap().clone()
    }

    /// Atomic write-then-rename. On the FIRST write after a failed load the
    /// unreadable bytes are moved to `<path>.corrupt` instead of being
    /// destroyed, so whatever an operator had acknowledged stays recoverable
    /// by hand; the load-failure flag clears only once the new file is
    /// durably in place.
    fn persist(&self, data: &HandoffStoreData) -> rk_core::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let had_failure = self.load_failure.lock().unwrap().is_some();
        if had_failure && self.path.exists() {
            let quarantine = self.path.with_extension("json.corrupt");
            if let Err(error) = std::fs::rename(&self.path, &quarantine) {
                warn!(
                    path = %self.path.display(), error = %error,
                    "could not preserve unreadable landing handoff fence store"
                );
            } else {
                warn!(
                    quarantine = %quarantine.display(),
                    "preserved unreadable landing handoff fence store before rewriting"
                );
            }
        }
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(data)?)?;
        std::fs::rename(tmp, &self.path)?;
        *self.load_failure.lock().unwrap() = None;
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
        // Mutate, then persist, then KEEP — never the reverse. If the write
        // fails we restore the exact prior entry before returning the error,
        // so an operator who sees `fence_request` fail can trust that no
        // fence was engaged. Leaving it engaged in memory only would be
        // worse than useless: the next restart would silently drop it, which
        // is precisely the acknowledged-ownership-disappears failure this
        // store must not have.
        let previous = data.fences.insert(repo.to_string(), record.clone());
        if let Err(error) = self.persist(&data) {
            match previous {
                Some(prior) => {
                    data.fences.insert(repo.to_string(), prior);
                }
                None => {
                    data.fences.remove(repo);
                }
            }
            return Err(error);
        }
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
        // Same discipline as `request`, in the opposite direction: if the
        // write fails the fence stays REQUESTED in memory too, so a failed
        // release never silently resumes admission that a restart would then
        // re-block.
        existing.state = HandoffFenceState::Released;
        if let Err(error) = self.persist(&data) {
            if let Some(entry) = data.fences.get_mut(repo) {
                entry.state = HandoffFenceState::Requested;
            }
            return Err(error);
        }
        Ok(())
    }
}

/// Everything outside the landing queue that can still own `repo` when an
/// operator asks whether a rollover is safe. Gathered by `Server`, which is
/// the only place that can see all three contracts at once, and passed in —
/// rather than reaching back into `Server` from the pipeline, which would
/// need a cycle. See the module doc for why each dimension counts.
#[derive(Debug, Default, Clone)]
pub(crate) struct ManagedWorkSnapshot {
    /// Managed `verify.run`/`release.prepare` runs bound to this repo,
    /// executing or queued.
    pub(crate) runs: Vec<crate::managed_verification::ManagedRunBlocker>,
    /// Whether the daemon-wide release-prepare lock is currently held.
    pub(crate) release_prepare_in_flight: bool,
}

impl ManagedWorkSnapshot {
    fn is_clear(&self) -> bool {
        self.runs.is_empty() && !self.release_prepare_in_flight
    }

    /// Operator-facing blocker rows, each carrying the SCOPE it applies at
    /// so a reader can tell a repo-bound check from a daemon-wide release
    /// prepare without guessing from the kind.
    fn blocker_rows(&self) -> Vec<Value> {
        let mut rows: Vec<Value> = self
            .runs
            .iter()
            .map(|run| {
                json!({
                    "scope": "repo",
                    "kind": run.kind,
                    "agent": run.agent,
                    "repo": run.repo,
                })
            })
            .collect();
        if self.release_prepare_in_flight {
            rows.push(json!({
                "scope": "daemon",
                "kind": "release-prepare-lock",
                "detail": "a release prepare/select holds the daemon-wide \
                           release_prepare_lock; a rollover would interrupt it",
            }));
        }
        rows
    }
}

impl LandingPipeline {
    /// The per-repo gate that linearizes "is admission fenced?" against the
    /// claim it guards. Claim sites hold it SHARED across check-and-claim;
    /// `fence_request` holds it EXCLUSIVELY while flipping the record. See
    /// the module doc's linearization note.
    ///
    /// Per-repo rather than global so engaging a fence for one repository
    /// never even momentarily serializes another's claims — the module doc
    /// promises every other repo's admission is untouched throughout.
    pub(crate) fn admission_gate(&self, repo_name: &str) -> Arc<tokio::sync::RwLock<()>> {
        let mut gates = self.admission_gates.lock().unwrap();
        Arc::clone(
            gates
                .entry(repo_name.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::RwLock::new(()))),
        )
    }

    /// True iff new admission is currently blocked for `repo_name` — the
    /// single question every claim-gating call site in this file asks
    /// before claiming fresh work. See the module doc for exactly what this
    /// does and does not affect.
    ///
    /// Callers MUST already hold this repo's [`Self::admission_gate`] read
    /// guard across both this call and the claim it gates; otherwise the
    /// answer can go stale between the two.
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
        let mut keys: Vec<String> = locks
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .filter_map(|(key, lock)| match lock.try_lock() {
                Ok(_guard) => None,
                Err(_) => Some(key[prefix.len()..].to_string()),
            })
            .collect();
        keys.sort();
        keys
    }

    /// `repo.land.fence_request` — engage the handoff fence for `repo_name`.
    /// See the module doc; durable, idempotent for the same live holder,
    /// bumps `generation` on takeover from an expired/released prior fence.
    ///
    /// Async because it takes the repo's admission gate EXCLUSIVELY while it
    /// flips the record: that is what makes the returned acknowledgment mean
    /// "no claim can still be mid-window on the pre-fence answer".
    pub(crate) async fn fence_request(
        &self,
        repo_name: &str,
        holder: &str,
        ttl_secs: i64,
        managed: &ManagedWorkSnapshot,
    ) -> rk_core::Result<Value> {
        let gate = self.admission_gate(repo_name);
        let record = {
            let _exclusive = gate.write().await;
            self.handoff
                .request(repo_name, holder, ttl_secs, Utc::now())?
        };
        Ok(self.fence_status_json(repo_name, Some(&record), managed))
    }

    /// `repo.land.fence_release` — end the fence early (idempotent; see
    /// [`HandoffFenceStore::release`]). Admission resumes for `repo_name` on
    /// the very next claim attempt; nothing is force-cancelled or re-run.
    pub(crate) async fn fence_release(
        &self,
        repo_name: &str,
        holder: &str,
        generation: u64,
        managed: &ManagedWorkSnapshot,
    ) -> rk_core::Result<Value> {
        let gate = self.admission_gate(repo_name);
        {
            let _exclusive = gate.write().await;
            self.handoff
                .release(repo_name, holder, generation, Utc::now())?;
        }
        Ok(self.fence_status(repo_name, managed))
    }

    /// `repo.land.fence_status` — read-only report: state, blockers, and
    /// whether it is safe to proceed with a rollover.
    ///
    /// `state` is one of:
    /// - `"unavailable"` — the durable store could not be read; readiness is
    ///   refused outright (see [`HandoffFenceStore`]'s doc).
    /// - `"released"` — no fence, or explicitly released.
    /// - `"expired"` — its deadline passed without release; admission has
    ///   already resumed on its own.
    /// - `"draining"` — engaged, but at least one blocker is still active.
    /// - `"ready"` — engaged and every dimension in the module doc is clear.
    ///
    /// `ready` is ALSO surfaced as its own boolean, so a caller never has to
    /// infer a safety decision by string-matching `state`.
    pub(crate) fn fence_status(&self, repo_name: &str, managed: &ManagedWorkSnapshot) -> Value {
        let current = self.handoff.current(repo_name);
        self.fence_status_json(repo_name, current.as_ref(), managed)
    }

    fn fence_status_json(
        &self,
        repo_name: &str,
        record: Option<&HandoffFenceRecord>,
        managed: &ManagedWorkSnapshot,
    ) -> Value {
        let now = Utc::now();
        let landing_keys = self.active_keys(repo_name);
        let managed_rows = managed.blocker_rows();
        let clear = landing_keys.is_empty() && managed.is_clear();

        // An unreadable store can never yield a readiness claim, whatever
        // the in-memory view happens to say — it is reported first, above
        // every other state.
        if let Some(reason) = self.handoff.load_failure() {
            return json!({
                "repo": repo_name,
                "state": "unavailable",
                "ready": false,
                "fenced": false,
                "error": reason,
                "recovery": format!(
                    "the durable handoff store could not be read; inspect the preserved \
                     copy alongside it and re-run `rk land fence-request --repo {repo_name}` \
                     to re-establish a fence before rolling over"
                ),
                "blocking_keys": landing_keys,
                "managed_blockers": managed_rows,
            });
        }

        let Some(record) = record else {
            return json!({
                "repo": repo_name,
                "state": "released",
                "ready": false,
                "fenced": false,
                "blocking_keys": landing_keys,
                "managed_blockers": managed_rows,
            });
        };

        let engaged = record.blocks_admission(now);
        let state = match record.state {
            HandoffFenceState::Released => "released",
            HandoffFenceState::Requested if now > record.deadline_at => "expired",
            HandoffFenceState::Requested if clear => "ready",
            HandoffFenceState::Requested => "draining",
        };
        // Readiness is asserted ONLY for a live fence with every dimension
        // clear. A released or expired record is not "safe to roll over" —
        // admission has already resumed under it, so new work can arrive at
        // any moment.
        let ready = engaged && clear;
        json!({
            "repo": repo_name,
            "state": state,
            "ready": ready,
            "fenced": engaged,
            "holder": record.holder,
            "generation": record.generation,
            "requested_at": record.requested_at,
            "deadline_at": record.deadline_at,
            "blocking_keys": landing_keys,
            "managed_blockers": managed_rows,
        })
    }
}

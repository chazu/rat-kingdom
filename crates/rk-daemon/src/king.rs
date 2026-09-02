//! Durable control plane for the dedicated King (operator-delegate) session.
//!
//! Herdr prompt injection is deliberately a dumb wake transport. The daemon
//! persists an opaque wake id first, injects only `RK_WAKE <id>`, and the King
//! claims that id through authenticated RPC to obtain a bounded snapshot. This
//! gives at-least-once delivery without treating terminal text as authority.

use chrono::{DateTime, Duration, Utc};
use rk_core::config::KingConfig;
use rk_core::id::RecordId;
use rk_mux::AgentIdentity;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const HISTORY_LIMIT: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WakeState {
    Pending,
    Injected,
    Claimed,
    Resolved,
    Deferred,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KingWake {
    pub id: String,
    pub digest: String,
    pub summary: String,
    pub snapshot: Value,
    pub state: WakeState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub injection_attempts: u32,
    pub last_injected_at: Option<DateTime<Utc>>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub settled_at: Option<DateTime<Utc>>,
}

impl KingWake {
    fn active(&self) -> bool {
        !matches!(self.state, WakeState::Resolved | WakeState::Deferred)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KingRegistration {
    pub holder: String,
    pub name: String,
    pub identity: AgentIdentity,
    pub generation: u64,
    pub registered_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContextLifecycle {
    #[default]
    Clean,
    Dirty,
    CompactRequested,
    Compacting,
    Compacted,
    HibernateReady,
    Hibernating,
    Hibernated,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KingCheckpoint {
    pub id: String,
    pub created_at: DateTime<Utc>,
    pub registration_generation: u64,
    pub last_snapshot: Value,
    pub active_wake: Option<String>,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct KingState {
    pub registration: Option<KingRegistration>,
    pub wakes: VecDeque<KingWake>,
    pub last_snapshot: Value,
    pub last_resolved_digest: Option<String>,
    pub context: ContextLifecycle,
    pub idle_since: Option<DateTime<Utc>>,
    pub last_activity_at: Option<DateTime<Utc>>,
    pub compact_started_at: Option<DateTime<Utc>>,
    pub compacted_at: Option<DateTime<Utc>>,
    pub wake_batches_since_compaction: u32,
    pub checkpoints: VecDeque<KingCheckpoint>,
    pub pending_restore: Option<String>,
    pub restore_last_injected_at: Option<DateTime<Utc>>,
}

impl Default for KingState {
    fn default() -> Self {
        Self {
            registration: None,
            wakes: VecDeque::new(),
            last_snapshot: Value::Null,
            last_resolved_digest: None,
            context: ContextLifecycle::Clean,
            idle_since: None,
            last_activity_at: None,
            compact_started_at: None,
            compacted_at: None,
            wake_batches_since_compaction: 0,
            checkpoints: VecDeque::new(),
            pending_restore: None,
            restore_last_injected_at: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextAction {
    Compact,
    Hibernate,
}

/// Herdr uses `done` for a completed interactive turn and `idle` before the
/// first turn. Both states are ready to accept the next atomic prompt.
pub(crate) fn is_quiescent(status: &str) -> bool {
    matches!(status, "idle" | "done")
}

pub struct KingStore {
    path: PathBuf,
    state: Mutex<KingState>,
}

impl KingStore {
    pub fn load(path: impl AsRef<Path>) -> rk_core::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let state = if path.exists() {
            serde_json::from_str(&std::fs::read_to_string(&path)?)?
        } else {
            KingState::default()
        };
        Ok(Self {
            path,
            state: Mutex::new(state),
        })
    }

    fn lock(&self) -> rk_core::Result<std::sync::MutexGuard<'_, KingState>> {
        self.state
            .lock()
            .map_err(|_| rk_core::Error::other("King state lock poisoned"))
    }

    fn persist(&self, state: &KingState) -> rk_core::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(state)?)?;
        std::fs::rename(tmp, &self.path)?;
        Ok(())
    }

    pub fn snapshot(&self) -> rk_core::Result<KingState> {
        Ok(self.lock()?.clone())
    }

    pub fn register(
        &self,
        holder: String,
        name: String,
        identity: AgentIdentity,
        initial_wake_batches: u32,
        now: DateTime<Utc>,
    ) -> rk_core::Result<KingRegistration> {
        let mut state = self.lock()?;
        let generation = state
            .registration
            .as_ref()
            .map_or(1, |old| old.generation.saturating_add(1));
        let registration = KingRegistration {
            holder,
            name,
            identity,
            generation,
            registered_at: now,
        };
        state.registration = Some(registration.clone());
        state.context = ContextLifecycle::Dirty;
        state.last_activity_at = Some(now);
        state.idle_since = None;
        // A newly adopted session may already carry a large context. Herdr
        // provides semantic activity but not token usage, and terminal scraping
        // is intentionally outside this protocol, so make the first idle spell
        // eligible for compaction.
        state.wake_batches_since_compaction = initial_wake_batches;
        self.persist(&state)?;
        Ok(registration)
    }

    /// Remove the terminal binding while retaining durable King history.
    ///
    /// Any unsettled envelope may have been interrupted in the dismissed
    /// generation, so make it pending again for at-least-once delivery to the
    /// next explicitly registered generation.
    pub fn unregister(&self, now: DateTime<Utc>) -> rk_core::Result<Option<KingRegistration>> {
        let mut state = self.lock()?;
        let registration = state.registration.take();
        for wake in state.wakes.iter_mut().filter(|wake| wake.active()) {
            wake.state = WakeState::Pending;
            wake.updated_at = now;
            wake.last_injected_at = None;
            wake.claimed_at = None;
        }
        state.context = ContextLifecycle::Clean;
        state.idle_since = None;
        state.last_activity_at = None;
        state.compact_started_at = None;
        state.pending_restore = None;
        state.restore_last_injected_at = None;
        self.persist(&state)?;
        Ok(registration)
    }

    /// Observe a fresh authoritative snapshot and return a wake that should be
    /// injected now. One active wake coalesces all changes until it is settled.
    pub fn observe(
        &self,
        digest: String,
        summary: String,
        snapshot: Value,
        has_work: bool,
        retry_secs: i64,
        now: DateTime<Utc>,
    ) -> rk_core::Result<Option<KingWake>> {
        let mut state = self.lock()?;
        state.last_snapshot = snapshot.clone();

        let active = state.wakes.iter_mut().rev().find(|wake| wake.active());
        if !has_work {
            if let Some(wake) = active {
                wake.state = WakeState::Resolved;
                wake.settled_at = Some(now);
                wake.updated_at = now;
                state.last_resolved_digest = Some(digest);
            }
            self.persist(&state)?;
            return Ok(None);
        }

        if let Some(wake) = active {
            if wake.digest != digest {
                wake.digest = digest;
                wake.summary = summary;
                wake.snapshot = snapshot;
                wake.updated_at = now;
            }
            let due = match wake.state {
                WakeState::Pending => true,
                WakeState::Injected => wake.last_injected_at.is_none_or(|at| {
                    now.signed_duration_since(at) >= Duration::seconds(retry_secs.max(1))
                }),
                WakeState::Claimed | WakeState::Resolved | WakeState::Deferred => false,
            };
            let out = due.then(|| wake.clone());
            self.persist(&state)?;
            return Ok(out);
        }

        if state.last_resolved_digest.as_deref() == Some(&digest) {
            self.persist(&state)?;
            return Ok(None);
        }
        let wake = KingWake {
            id: format!("KWK-{}", RecordId::new()),
            digest,
            summary,
            snapshot,
            state: WakeState::Pending,
            created_at: now,
            updated_at: now,
            injection_attempts: 0,
            last_injected_at: None,
            claimed_at: None,
            settled_at: None,
        };
        state.wakes.push_back(wake.clone());
        trim(&mut state.wakes);
        self.persist(&state)?;
        Ok(Some(wake))
    }

    pub fn mark_injected(&self, id: &str, now: DateTime<Utc>) -> rk_core::Result<KingWake> {
        let mut state = self.lock()?;
        let wake = find_wake_mut(&mut state, id)?;
        if wake.active() {
            wake.state = WakeState::Injected;
            wake.injection_attempts = wake.injection_attempts.saturating_add(1);
            wake.last_injected_at = Some(now);
            wake.updated_at = now;
        }
        let out = wake.clone();
        self.persist(&state)?;
        Ok(out)
    }

    pub fn claim(&self, id: &str, holder: &str, now: DateTime<Utc>) -> rk_core::Result<KingWake> {
        let mut state = self.lock()?;
        require_holder(&state, holder)?;
        let (out, newly_claimed) = {
            let wake = find_wake_mut(&mut state, id)?;
            if matches!(wake.state, WakeState::Resolved | WakeState::Deferred) {
                return Err(rk_core::Error::other(format!(
                    "wake {id} is already settled"
                )));
            }
            let newly_claimed = wake.state != WakeState::Claimed;
            if newly_claimed {
                wake.state = WakeState::Claimed;
                wake.claimed_at = Some(now);
                wake.updated_at = now;
            }
            (wake.clone(), newly_claimed)
        };
        if newly_claimed {
            state.wake_batches_since_compaction =
                state.wake_batches_since_compaction.saturating_add(1);
        }
        state.context = ContextLifecycle::Dirty;
        state.last_activity_at = Some(now);
        state.idle_since = None;
        self.persist(&state)?;
        Ok(out)
    }

    pub fn settle(
        &self,
        id: &str,
        holder: &str,
        deferred: bool,
        now: DateTime<Utc>,
    ) -> rk_core::Result<KingWake> {
        let mut state = self.lock()?;
        require_holder(&state, holder)?;
        let out = {
            let wake = find_wake_mut(&mut state, id)?;
            if wake.state != WakeState::Claimed {
                return Err(rk_core::Error::other(format!("wake {id} is not claimed")));
            }
            wake.state = if deferred {
                WakeState::Deferred
            } else {
                WakeState::Resolved
            };
            wake.settled_at = Some(now);
            wake.updated_at = now;
            wake.clone()
        };
        state.last_resolved_digest = Some(out.digest.clone());
        self.persist(&state)?;
        Ok(out)
    }

    pub fn context_action(
        &self,
        status: &str,
        focused: bool,
        cfg: &KingConfig,
        now: DateTime<Utc>,
    ) -> rk_core::Result<Option<ContextAction>> {
        let mut state = self.lock()?;
        if state.context == ContextLifecycle::Compacting
            && state.compact_started_at.is_some_and(|started| {
                now.signed_duration_since(started)
                    >= Duration::seconds(cfg.compact_timeout_secs.max(1))
            })
        {
            state.context = ContextLifecycle::HibernateReady;
            self.persist(&state)?;
            return Ok(Some(ContextAction::Hibernate));
        }
        if status == "working" || status == "blocked" || status == "unknown" {
            if state.context != ContextLifecycle::Compacting {
                state.context = ContextLifecycle::Dirty;
                state.last_activity_at = Some(now);
                state.idle_since = None;
            }
            self.persist(&state)?;
            return Ok(None);
        }
        if !is_quiescent(status) {
            self.persist(&state)?;
            return Ok(None);
        }

        let idle_since = *state.idle_since.get_or_insert(now);
        let idle_for = now.signed_duration_since(idle_since);
        if state.context == ContextLifecycle::Compacting {
            state.context = ContextLifecycle::Compacted;
            state.compacted_at = Some(now);
            state.compact_started_at = None;
            state.wake_batches_since_compaction = 0;
            self.persist(&state)?;
            return Ok(None);
        }
        if state.context == ContextLifecycle::CompactRequested {
            self.persist(&state)?;
            return Ok(None);
        }

        let active_wake = state.wakes.iter().any(KingWake::active);
        if focused || active_wake || state.pending_restore.is_some() {
            self.persist(&state)?;
            return Ok(None);
        }
        if idle_for >= Duration::seconds(cfg.hibernate_after_idle_secs.max(1)) {
            state.context = ContextLifecycle::HibernateReady;
            self.persist(&state)?;
            return Ok(Some(ContextAction::Hibernate));
        }
        if idle_for >= Duration::seconds(cfg.compact_after_idle_secs.max(1))
            && state.wake_batches_since_compaction >= cfg.compact_min_wake_batches
            && !matches!(
                state.context,
                ContextLifecycle::Compacted | ContextLifecycle::Hibernated
            )
        {
            state.context = ContextLifecycle::CompactRequested;
            self.persist(&state)?;
            return Ok(Some(ContextAction::Compact));
        }
        self.persist(&state)?;
        Ok(None)
    }

    pub fn mark_compacting(&self, now: DateTime<Utc>) -> rk_core::Result<()> {
        let mut state = self.lock()?;
        state.context = ContextLifecycle::Compacting;
        state.compact_started_at = Some(now);
        self.persist(&state)
    }

    pub fn compaction_failed(&self, now: DateTime<Utc>) -> rk_core::Result<()> {
        let mut state = self.lock()?;
        state.context = ContextLifecycle::HibernateReady;
        state.compact_started_at = Some(now);
        self.persist(&state)
    }

    pub fn checkpoint(
        &self,
        notes: Option<String>,
        now: DateTime<Utc>,
    ) -> rk_core::Result<KingCheckpoint> {
        let mut state = self.lock()?;
        let registration_generation = state
            .registration
            .as_ref()
            .ok_or_else(|| rk_core::Error::other("no King registered"))?
            .generation;
        let checkpoint = KingCheckpoint {
            id: format!("KCP-{}", RecordId::new()),
            created_at: now,
            registration_generation,
            last_snapshot: state.last_snapshot.clone(),
            active_wake: state
                .wakes
                .iter()
                .rev()
                .find(|wake| wake.active())
                .map(|wake| wake.id.clone()),
            notes: notes.map(|text| text.chars().take(4_096).collect()),
        };
        state.checkpoints.push_back(checkpoint.clone());
        trim(&mut state.checkpoints);
        state.context = ContextLifecycle::Hibernating;
        self.persist(&state)?;
        Ok(checkpoint)
    }

    pub fn checkpoint_by_id(&self, id: &str) -> rk_core::Result<KingCheckpoint> {
        self.lock()?
            .checkpoints
            .iter()
            .find(|checkpoint| checkpoint.id == id)
            .cloned()
            .ok_or_else(|| rk_core::Error::other(format!("no such King checkpoint: {id}")))
    }

    pub fn complete_hibernation(
        &self,
        identity: AgentIdentity,
        checkpoint: String,
        now: DateTime<Utc>,
    ) -> rk_core::Result<KingRegistration> {
        let mut state = self.lock()?;
        let registration = state
            .registration
            .as_mut()
            .ok_or_else(|| rk_core::Error::other("no King registered"))?;
        registration.identity = identity;
        registration.generation = registration.generation.saturating_add(1);
        registration.registered_at = now;
        let out = registration.clone();
        state.context = ContextLifecycle::Hibernated;
        state.idle_since = None;
        state.last_activity_at = Some(now);
        state.compact_started_at = None;
        state.wake_batches_since_compaction = 0;
        state.pending_restore = Some(checkpoint);
        state.restore_last_injected_at = None;
        self.persist(&state)?;
        Ok(out)
    }

    pub fn pending_restore_due(
        &self,
        retry_secs: i64,
        now: DateTime<Utc>,
    ) -> rk_core::Result<Option<String>> {
        let state = self.lock()?;
        let due = state.pending_restore.as_ref().is_some_and(|_| {
            state.restore_last_injected_at.is_none_or(|last| {
                now.signed_duration_since(last) >= Duration::seconds(retry_secs.max(1))
            })
        });
        Ok(due.then(|| state.pending_restore.clone()).flatten())
    }

    pub fn mark_restore_injected(
        &self,
        checkpoint: &str,
        now: DateTime<Utc>,
    ) -> rk_core::Result<()> {
        let mut state = self.lock()?;
        if state.pending_restore.as_deref() == Some(checkpoint) {
            state.restore_last_injected_at = Some(now);
        }
        self.persist(&state)
    }

    pub fn acknowledge_restore(&self, checkpoint: &str) -> rk_core::Result<()> {
        let mut state = self.lock()?;
        if state.pending_restore.as_deref() == Some(checkpoint) {
            state.pending_restore = None;
            state.restore_last_injected_at = None;
        }
        self.persist(&state)
    }

    pub fn hibernation_failed(&self) -> rk_core::Result<()> {
        let mut state = self.lock()?;
        state.context = ContextLifecycle::HibernateReady;
        self.persist(&state)
    }
}

fn require_holder(state: &KingState, holder: &str) -> rk_core::Result<()> {
    let registration = state
        .registration
        .as_ref()
        .ok_or_else(|| rk_core::Error::other("no King registered"))?;
    if registration.holder != holder {
        return Err(rk_core::Error::other(format!(
            "King holder mismatch: registered {}, presented {holder}",
            registration.holder
        )));
    }
    Ok(())
}

fn find_wake_mut<'a>(state: &'a mut KingState, id: &str) -> rk_core::Result<&'a mut KingWake> {
    state
        .wakes
        .iter_mut()
        .find(|wake| wake.id == id)
        .ok_or_else(|| rk_core::Error::other(format!("no such King wake: {id}")))
}

fn trim<T>(items: &mut VecDeque<T>) {
    while items.len() > HISTORY_LIMIT {
        items.pop_front();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(session: &str) -> AgentIdentity {
        AgentIdentity {
            terminal_id: "term-1".into(),
            pane_id: "w1:p1".into(),
            session_id: session.into(),
            agent: "codex".into(),
            cwd: "/repo".into(),
        }
    }

    fn store() -> (tempfile::TempDir, KingStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = KingStore::load(dir.path().join("king.json")).unwrap();
        store
            .register(
                "king-a".into(),
                "king".into(),
                identity("one"),
                0,
                Utc::now(),
            )
            .unwrap();
        (dir, store)
    }

    #[test]
    fn wake_is_durable_retryable_and_coalesced_until_claimed() {
        let (dir, store) = store();
        let now = Utc::now();
        let first = store
            .observe(
                "a".into(),
                "one".into(),
                serde_json::json!({"n": 1}),
                true,
                60,
                now,
            )
            .unwrap()
            .unwrap();
        store.mark_injected(&first.id, now).unwrap();
        assert!(store
            .observe(
                "b".into(),
                "two".into(),
                serde_json::json!({"n": 2}),
                true,
                60,
                now + Duration::seconds(59),
            )
            .unwrap()
            .is_none());
        let retried = store
            .observe(
                "b".into(),
                "two".into(),
                serde_json::json!({"n": 2}),
                true,
                60,
                now + Duration::seconds(60),
            )
            .unwrap()
            .unwrap();
        assert_eq!(retried.id, first.id);
        assert_eq!(retried.digest, "b");
        drop(store);
        let restored = KingStore::load(dir.path().join("king.json")).unwrap();
        assert_eq!(
            restored.snapshot().unwrap().wakes.back().unwrap().id,
            first.id
        );
    }

    #[test]
    fn settled_digest_does_not_repeat_until_authoritative_state_changes() {
        let (_dir, store) = store();
        let now = Utc::now();
        let wake = store
            .observe("a".into(), "one".into(), Value::Null, true, 10, now)
            .unwrap()
            .unwrap();
        store.claim(&wake.id, "king-a", now).unwrap();
        store.settle(&wake.id, "king-a", false, now).unwrap();
        assert!(store
            .observe("a".into(), "one".into(), Value::Null, true, 10, now)
            .unwrap()
            .is_none());
        assert!(store
            .observe("b".into(), "two".into(), Value::Null, true, 10, now)
            .unwrap()
            .is_some());
    }

    #[test]
    fn compaction_requires_idle_unfocused_no_wake_and_enough_batches() {
        let (_dir, store) = store();
        let now = Utc::now();
        let cfg = KingConfig {
            compact_after_idle_secs: 5,
            compact_min_wake_batches: 1,
            ..KingConfig::default()
        };
        let wake = store
            .observe("a".into(), "one".into(), Value::Null, true, 10, now)
            .unwrap()
            .unwrap();
        store.claim(&wake.id, "king-a", now).unwrap();
        store.settle(&wake.id, "king-a", false, now).unwrap();
        assert_eq!(
            store.context_action("idle", false, &cfg, now).unwrap(),
            None
        );
        assert_eq!(
            store
                .context_action("idle", false, &cfg, now + Duration::seconds(5))
                .unwrap(),
            Some(ContextAction::Compact)
        );
    }

    #[test]
    fn completed_herdr_turn_is_quiescent() {
        assert!(is_quiescent("idle"));
        assert!(is_quiescent("done"));
        assert!(!is_quiescent("working"));
        assert!(!is_quiescent("blocked"));
        assert!(!is_quiescent("unknown"));
    }

    #[test]
    fn compaction_timeout_forces_hibernation_even_if_agent_never_settles() {
        let (_dir, store) = store();
        let now = Utc::now();
        let cfg = KingConfig {
            compact_timeout_secs: 10,
            ..KingConfig::default()
        };
        store.mark_compacting(now).unwrap();
        assert_eq!(
            store
                .context_action("working", false, &cfg, now + Duration::seconds(9))
                .unwrap(),
            None
        );
        assert_eq!(
            store
                .context_action("working", false, &cfg, now + Duration::seconds(10))
                .unwrap(),
            Some(ContextAction::Hibernate)
        );
    }

    #[test]
    fn holder_fences_claim_and_settlement() {
        let (_dir, store) = store();
        let now = Utc::now();
        let wake = store
            .observe("a".into(), "one".into(), Value::Null, true, 10, now)
            .unwrap()
            .unwrap();
        assert!(store.claim(&wake.id, "not-the-king", now).is_err());
        store.claim(&wake.id, "king-a", now).unwrap();
        assert!(store.settle(&wake.id, "not-the-king", false, now).is_err());
    }

    #[test]
    fn fresh_session_restore_retries_until_restore_rpc_acknowledges_it() {
        let (_dir, store) = store();
        let now = Utc::now();
        store
            .complete_hibernation(identity("two"), "KCP-one".into(), now)
            .unwrap();
        assert_eq!(
            store.pending_restore_due(60, now).unwrap().as_deref(),
            Some("KCP-one")
        );
        store.mark_restore_injected("KCP-one", now).unwrap();
        assert!(store
            .pending_restore_due(60, now + Duration::seconds(59))
            .unwrap()
            .is_none());
        assert_eq!(
            store
                .pending_restore_due(60, now + Duration::seconds(60))
                .unwrap()
                .as_deref(),
            Some("KCP-one")
        );
        store.acknowledge_restore("KCP-one").unwrap();
        assert!(store
            .pending_restore_due(60, now + Duration::seconds(120))
            .unwrap()
            .is_none());
    }

    #[test]
    fn unregister_replays_unsettled_work_to_the_next_generation() {
        let (_dir, store) = store();
        let now = Utc::now();
        let wake = store
            .observe("a".into(), "one".into(), Value::Null, true, 10, now)
            .unwrap()
            .unwrap();
        store.claim(&wake.id, "king-a", now).unwrap();

        let removed = store.unregister(now).unwrap().unwrap();
        assert_eq!(removed.identity.session_id, "one");
        let state = store.snapshot().unwrap();
        assert!(state.registration.is_none());
        assert_eq!(state.wakes.back().unwrap().state, WakeState::Pending);
        assert_eq!(state.context, ContextLifecycle::Clean);
    }
}

//! Durable task-to-main phase-span telemetry (parent
//! TKT-01M0P2KNB92EAV2QG9256MY3QV, this substrate TKT-01M0P974EZZTPMGVP4S0E76NXH).
//!
//! One durable correlation identity threads every phase of a ticket's
//! journey from readiness through delivery: the ticket id itself
//! (`TKT-<ulid>`), the same string every existing producer in this daemon
//! already carries as `task`/`ctx.task`/`entry.task`. This module mints no
//! second identity and opens no second ledger — it adds a single new
//! `Furniture` Event kind (`task_span`, [`SPAN_IDENTITY`]) that existing
//! producers write once, alongside the event they already emit, using
//! timing/identity data they already have in hand at that call site.
//!
//! # Idempotency and restart safety
//!
//! A span is a fact keyed on `(task, phase, attempt)`. [`record_phase_span`]
//! scans for an existing span on that exact key before writing a new one, so
//! calling it twice for the same underlying occurrence — a retried caller, a
//! duplicate event replay, or the same durable store reopened after a daemon
//! restart — writes the tuple exactly once. This mirrors the dedup idiom
//! already used by `verification_proof` (`workflow_exec.rs`) and the
//! conflict/rework dispatch markers (`landing.rs`): a composite business key,
//! scanned before a write, rather than a distinct sequence counter to
//! reconcile. No open/close pairing is needed: every producer this module is
//! wired into already knows its full timing (or can derive it from a
//! duration it already tracked) at the single point it settles, so there is
//! no "started but never closed" state for a restart to strand.

use chrono::{DateTime, Utc};
use rk_core::tuple::{Category, Lifecycle, Pattern, Tuple};
use rk_space::Space;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Durable `(Event, <scope>, "task_span")` phase-span record.
pub const SPAN_IDENTITY: &str = "task_span";

/// One phase of a ticket's task-to-main journey. Matches the parent ticket's
/// acceptance criteria one-for-one: "ticket readiness and claim" (2 phases),
/// "agent launch and first progress" (2), "completion" (1), "verification
/// queue/start/end" (1 phase carrying up to three timestamps), "landing
/// preparation" (1), "semantic review and bounded rework rounds" (2),
/// "merge" (1), "delivery closure" (1), "actionable hold" (1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    TicketReady,
    Claimed,
    AgentLaunched,
    FirstProgress,
    Completed,
    VerificationQueued,
    LandingPrep,
    SemanticReview,
    Rework,
    Merge,
    DeliveryClosure,
    AttentionHold,
}

impl Phase {
    pub fn as_str(self) -> &'static str {
        match self {
            Phase::TicketReady => "ticket_ready",
            Phase::Claimed => "claimed",
            Phase::AgentLaunched => "agent_launched",
            Phase::FirstProgress => "first_progress",
            Phase::Completed => "completed",
            Phase::VerificationQueued => "verification",
            Phase::LandingPrep => "landing_prep",
            Phase::SemanticReview => "semantic_review",
            Phase::Rework => "rework",
            Phase::Merge => "merge",
            Phase::DeliveryClosure => "delivery_closure",
            Phase::AttentionHold => "attention_hold",
        }
    }
}

/// Human vs LLM/orchestrator authority over a phase, where applicable
/// (`AttentionHold`, `SemanticReview`/`Rework`). Absent for phases with no
/// notion of authority (a queue wait has no decider).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Authority {
    Human,
    Llm,
}

impl Authority {
    pub fn as_str(self) -> &'static str {
        match self {
            Authority::Human => "human",
            Authority::Llm => "llm",
        }
    }
}

/// One phase-span occurrence, ready to record. Every field but `task`,
/// `phase`, and `attempt` is optional and filled in only where the calling
/// producer already has it — the acceptance criteria's "where applicable".
#[derive(Debug, Clone)]
pub struct PhaseSpan {
    pub task: String,
    pub phase: Phase,
    pub attempt: u32,
    pub queued_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub terminal_reason: Option<String>,
    pub repo: Option<String>,
    pub target: Option<String>,
    pub candidate: Option<String>,
    pub lane: Option<String>,
    pub proof_kind: Option<String>,
    pub proof_reused: Option<bool>,
    pub authority: Option<Authority>,
}

impl PhaseSpan {
    pub fn new(task: impl Into<String>, phase: Phase) -> Self {
        Self {
            task: task.into(),
            phase,
            attempt: 1,
            queued_at: None,
            started_at: None,
            ended_at: None,
            terminal_reason: None,
            repo: None,
            target: None,
            candidate: None,
            lane: None,
            proof_kind: None,
            proof_reused: None,
            authority: None,
        }
    }

    pub fn attempt(mut self, attempt: u32) -> Self {
        self.attempt = attempt;
        self
    }

    pub fn queued_at(mut self, at: DateTime<Utc>) -> Self {
        self.queued_at = Some(at);
        self
    }

    pub fn started_at(mut self, at: DateTime<Utc>) -> Self {
        self.started_at = Some(at);
        self
    }

    pub fn ended_at(mut self, at: DateTime<Utc>) -> Self {
        self.ended_at = Some(at);
        self
    }

    pub fn terminal_reason(mut self, reason: impl Into<String>) -> Self {
        self.terminal_reason = Some(reason.into());
        self
    }

    pub fn repo(mut self, repo: impl Into<String>) -> Self {
        self.repo = Some(repo.into());
        self
    }

    pub fn target(mut self, target: impl Into<String>) -> Self {
        self.target = Some(target.into());
        self
    }

    pub fn candidate(mut self, candidate: impl Into<String>) -> Self {
        self.candidate = Some(candidate.into());
        self
    }

    pub fn lane(mut self, lane: impl Into<String>) -> Self {
        self.lane = Some(lane.into());
        self
    }

    pub fn proof_kind(mut self, kind: impl Into<String>) -> Self {
        self.proof_kind = Some(kind.into());
        self
    }

    pub fn proof_reused(mut self, reused: bool) -> Self {
        self.proof_reused = Some(reused);
        self
    }

    pub fn authority(mut self, authority: Authority) -> Self {
        self.authority = Some(authority);
        self
    }

    /// Duration from queued to started, when both are known.
    pub fn queue_wait_ms(&self) -> Option<i64> {
        Some((self.started_at? - self.queued_at?).num_milliseconds())
    }

    /// Duration from started to ended, when both are known.
    pub fn duration_ms(&self) -> Option<i64> {
        Some((self.ended_at? - self.started_at?).num_milliseconds())
    }

    /// Derive a span's start/end from durations alone, anchored so
    /// `ended_at` is `now` — the shape every existing queue-wait producer in
    /// this daemon already tracks (`RunProgress::queue_wait_ms`, a
    /// `duration_ms` measured off an `Instant`), neither of which survives a
    /// restart to reconstruct a real wall-clock `queued_at`. Good enough for
    /// percentile aggregation without inventing a wall-clock-tracking
    /// replacement for `Instant` inside `run_check_in`.
    pub fn from_durations(
        task: impl Into<String>,
        phase: Phase,
        queue_wait_ms: Option<u64>,
        duration_ms: Option<u64>,
        now: DateTime<Utc>,
    ) -> Self {
        let ended_at = now;
        let started_at =
            duration_ms.map(|d| ended_at - chrono::Duration::milliseconds(d as i64));
        let queued_at = match (started_at, queue_wait_ms) {
            (Some(started), Some(wait)) => {
                Some(started - chrono::Duration::milliseconds(wait as i64))
            }
            _ => None,
        };
        let mut span = Self::new(task, phase).ended_at(ended_at);
        if let Some(s) = started_at {
            span = span.started_at(s);
        }
        if let Some(q) = queued_at {
            span = span.queued_at(q);
        }
        span
    }

    fn to_payload(&self) -> Value {
        json!({
            "task": self.task,
            "phase": self.phase.as_str(),
            "attempt": self.attempt,
            "queued_at": self.queued_at,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "queue_wait_ms": self.queue_wait_ms(),
            "duration_ms": self.duration_ms(),
            "terminal_reason": self.terminal_reason,
            "repo": self.repo,
            "target": self.target,
            "candidate": self.candidate,
            "lane": self.lane,
            "proof_kind": self.proof_kind,
            "proof_reused": self.proof_reused,
            "authority": self.authority.map(Authority::as_str),
        })
    }
}

/// Record `span` once. Idempotent on `(task, phase, attempt)`: if a span
/// already exists on that exact key (a retried caller, a duplicate event
/// replay, or this same durable store reopened after a daemon restart),
/// this is a no-op returning `Ok(false)`; otherwise the span is written as a
/// `Furniture` Event and this returns `Ok(true)`.
pub fn record_phase_span(
    space: &Space,
    scope: &str,
    castle: &str,
    span: &PhaseSpan,
) -> rk_core::Result<bool> {
    if span_exists(space, scope, &span.task, span.phase, span.attempt)? {
        return Ok(false);
    }
    let tuple = Tuple::new(Category::Event, scope, SPAN_IDENTITY, castle, span.to_payload())
        .with_lifecycle(Lifecycle::Furniture);
    space.out(tuple)?;
    Ok(true)
}

fn span_exists(
    space: &Space,
    scope: &str,
    task: &str,
    phase: Phase,
    attempt: u32,
) -> rk_core::Result<bool> {
    let mut pattern = Pattern::category(Category::Event)
        .identity(SPAN_IDENTITY)
        .scope(scope);
    pattern.payload_search = Some(format!("\"task\":\"{task}\""));
    pattern.payload_search_and = Some(format!("\"phase\":\"{}\"", phase.as_str()));
    Ok(space.scan(&pattern)?.into_iter().any(|t| {
        t.payload.get("attempt").and_then(Value::as_u64) == Some(u64::from(attempt))
    }))
}

/// All recorded spans for `task`, oldest first — the read side a later
/// `rk status`/`digest` critical-path renderer builds on
/// (TKT-01M0P974FGSEFSX2KCS93QFPTF).
pub fn spans_for_task(space: &Space, scope: &str, task: &str) -> rk_core::Result<Vec<Value>> {
    let mut pattern = Pattern::category(Category::Event)
        .identity(SPAN_IDENTITY)
        .scope(scope);
    pattern.payload_search = Some(format!("\"task\":\"{task}\""));
    let mut spans: Vec<Tuple> = space.scan(&pattern)?;
    spans.sort_by_key(|t| t.id);
    Ok(spans.into_iter().map(|t| t.payload).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rk_core::tuple::SYSTEM_SCOPE;

    fn space() -> Space {
        Space::open_in_memory().unwrap()
    }

    #[test]
    fn clean_success_records_every_phase_once_in_order() {
        let space = space();
        let task = "TKT-clean";
        for phase in [
            Phase::TicketReady,
            Phase::Claimed,
            Phase::AgentLaunched,
            Phase::FirstProgress,
            Phase::Completed,
            Phase::VerificationQueued,
            Phase::LandingPrep,
            Phase::Merge,
            Phase::DeliveryClosure,
        ] {
            let wrote = record_phase_span(
                &space,
                SYSTEM_SCOPE,
                "daemon",
                &PhaseSpan::new(task, phase),
            )
            .unwrap();
            assert!(wrote, "{phase:?} should write on first record");
        }
        let spans = spans_for_task(&space, SYSTEM_SCOPE, task).unwrap();
        assert_eq!(spans.len(), 9);
        let phases: std::collections::BTreeSet<&str> = spans
            .iter()
            .map(|s| s["phase"].as_str().unwrap())
            .collect();
        assert!(phases.contains("ticket_ready"));
        assert!(phases.contains("delivery_closure"));
        assert_eq!(phases.len(), 9, "every phase recorded exactly once");
    }

    #[test]
    fn focused_inner_then_full_final_proof_are_distinct_attempts() {
        let space = space();
        let task = "TKT-proof";
        let inner = PhaseSpan::new(task, Phase::VerificationQueued)
            .attempt(1)
            .proof_kind("focused-inner")
            .proof_reused(false);
        let full_final = PhaseSpan::new(task, Phase::VerificationQueued)
            .attempt(2)
            .proof_kind("full-final")
            .proof_reused(false);
        assert!(record_phase_span(&space, SYSTEM_SCOPE, "daemon", &inner).unwrap());
        assert!(record_phase_span(&space, SYSTEM_SCOPE, "daemon", &full_final).unwrap());

        let spans = spans_for_task(&space, SYSTEM_SCOPE, task).unwrap();
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0]["proof_kind"], "focused-inner");
        assert_eq!(spans[1]["proof_kind"], "full-final");
    }

    #[test]
    fn verification_contention_carries_queue_wait_and_duration() {
        let span = PhaseSpan::from_durations(
            "TKT-contend",
            Phase::VerificationQueued,
            Some(45_000),
            Some(12_000),
            Utc::now(),
        );
        assert_eq!(span.queue_wait_ms(), Some(45_000));
        assert_eq!(span.duration_ms(), Some(12_000));
    }

    #[test]
    fn one_llm_rework_round_is_attempt_two_under_llm_authority() {
        let space = space();
        let task = "TKT-rework";
        let review = PhaseSpan::new(task, Phase::SemanticReview)
            .attempt(1)
            .authority(Authority::Llm)
            .terminal_reason("rework-requested");
        let rework = PhaseSpan::new(task, Phase::Rework)
            .attempt(1)
            .authority(Authority::Llm)
            .terminal_reason("dispatched");
        assert!(record_phase_span(&space, SYSTEM_SCOPE, "daemon", &review).unwrap());
        assert!(record_phase_span(&space, SYSTEM_SCOPE, "daemon", &rework).unwrap());

        let spans = spans_for_task(&space, SYSTEM_SCOPE, task).unwrap();
        assert_eq!(spans.len(), 2);
        assert!(spans.iter().all(|s| s["authority"] == "llm"));
    }

    #[test]
    fn a_correct_human_gate_is_recorded_with_human_authority() {
        let space = space();
        let task = "TKT-gate";
        let hold = PhaseSpan::new(task, Phase::AttentionHold)
            .authority(Authority::Human)
            .terminal_reason("protected-path-hit");
        assert!(record_phase_span(&space, SYSTEM_SCOPE, "daemon", &hold).unwrap());

        let spans = spans_for_task(&space, SYSTEM_SCOPE, task).unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0]["authority"], "human");
        assert_eq!(spans[0]["terminal_reason"], "protected-path-hit");
    }

    #[test]
    fn a_timed_out_phase_carries_its_terminal_reason() {
        let space = space();
        let span = PhaseSpan::from_durations(
            "TKT-timeout",
            Phase::VerificationQueued,
            Some(0),
            Some(600_000),
            Utc::now(),
        )
        .terminal_reason("timeout");
        assert!(record_phase_span(&space, SYSTEM_SCOPE, "daemon", &span).unwrap());
        let spans = spans_for_task(&space, SYSTEM_SCOPE, "TKT-timeout").unwrap();
        assert_eq!(spans[0]["terminal_reason"], "timeout");
    }

    /// Restart: a fresh `Space` handle reopened over the SAME durable store
    /// must see the span already written by a prior process and refuse to
    /// duplicate it — the daemon-restart half of idempotency, distinct from
    /// [`duplicate_event_replay_within_one_process_does_not_double_count`]
    /// which never tears down the store at all.
    #[test]
    fn restart_over_the_same_durable_store_does_not_double_count() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("space.db");
        let task = "TKT-restart";
        {
            let space = Space::open(&path).unwrap();
            let span = PhaseSpan::new(task, Phase::Completed).terminal_reason("declared-done");
            assert!(record_phase_span(&space, SYSTEM_SCOPE, "daemon", &span).unwrap());
        }
        // Simulated restart: a brand new Space handle over the same file.
        let reopened = Space::open(&path).unwrap();
        let span = PhaseSpan::new(task, Phase::Completed).terminal_reason("declared-done");
        let wrote_again = record_phase_span(&reopened, SYSTEM_SCOPE, "daemon", &span).unwrap();
        assert!(!wrote_again, "restart must not create a second span");

        let spans = spans_for_task(&reopened, SYSTEM_SCOPE, task).unwrap();
        assert_eq!(spans.len(), 1);
    }

    #[test]
    fn duplicate_event_replay_within_one_process_does_not_double_count() {
        let space = space();
        let task = "TKT-replay";
        let span = PhaseSpan::new(task, Phase::AgentLaunched);
        assert!(record_phase_span(&space, SYSTEM_SCOPE, "daemon", &span).unwrap());
        // A retried caller / replayed harness_result re-emits the identical
        // occurrence — same task, phase, attempt.
        assert!(!record_phase_span(&space, SYSTEM_SCOPE, "daemon", &span).unwrap());
        assert!(!record_phase_span(&space, SYSTEM_SCOPE, "daemon", &span).unwrap());

        let spans = spans_for_task(&space, SYSTEM_SCOPE, task).unwrap();
        assert_eq!(spans.len(), 1);
    }

    #[test]
    fn a_distinct_task_or_attempt_is_never_shadowed_by_another_key() {
        let space = space();
        let a = PhaseSpan::new("TKT-a", Phase::Rework).attempt(1);
        let b = PhaseSpan::new("TKT-ab", Phase::Rework).attempt(1);
        let c = PhaseSpan::new("TKT-a", Phase::Rework).attempt(2);
        assert!(record_phase_span(&space, SYSTEM_SCOPE, "daemon", &a).unwrap());
        assert!(record_phase_span(&space, SYSTEM_SCOPE, "daemon", &b).unwrap());
        assert!(record_phase_span(&space, SYSTEM_SCOPE, "daemon", &c).unwrap());

        assert_eq!(spans_for_task(&space, SYSTEM_SCOPE, "TKT-a").unwrap().len(), 2);
        assert_eq!(spans_for_task(&space, SYSTEM_SCOPE, "TKT-ab").unwrap().len(), 1);
    }
}

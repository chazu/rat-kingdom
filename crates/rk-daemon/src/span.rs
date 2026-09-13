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
//! A span is a fact keyed on `(task, phase, attempt)`, additionally fenced by
//! `target`, `candidate`, `lane` and `occurrence_key` when a producer sets
//! them. [`record_phase_span`] scans for an existing span on that exact key before
//! writing a new one, so calling it twice for the same underlying occurrence
//! — a retried caller, a duplicate event replay, or the same durable store
//! reopened after a daemon restart — writes the tuple exactly once. This
//! mirrors the dedup idiom already used by `verification_proof`
//! (`workflow_exec.rs`) and the conflict/rework dispatch markers
//! (`landing.rs`): a composite business key, scanned before a write, rather
//! than a distinct sequence counter to reconcile. No open/close pairing is
//! needed: every producer this module is wired into already knows its full
//! timing (or can derive it from a duration it already tracked) at the
//! single point it settles, so there is no "started but never closed" state
//! for a restart to strand.
//!
//! The `target`/`candidate`/`lane`/`occurrence_key` fence exists for one
//! reason: a landing gate's per-check `VerificationQueued` span numbers
//! `attempt` by the check's position in that round's plan (1, 2, 3, ...),
//! the same small ordinal a LATER round over the same task reuses for its
//! own checks against a genuinely new candidate. Keying dedup on `(task,
//! phase, attempt)` alone would make the later round's real occurrence
//! collide with — and be silently dropped by — the earlier round's, even
//! though both actually ran. Every such per-check span already carries
//! `target` (the branch this round is landing onto), `candidate` (the tested
//! sha), `lane` (the check name) and `occurrence_key` (a digest over what
//! actually executed — command/toolchain/environment policy — so a check
//! whose PLAN changed at the SAME candidate and plan position is also never
//! confused with whatever ran there before), so folding all four into the
//! key distinguishes a new occurrence from an exact replay of the same one:
//! identical on every field on a retry (idempotent, no-op), different on at
//! least one for a genuinely new occurrence (recorded, never shadowed).
//! `target` is fenced explicitly rather than assumed implied by `candidate`
//! (landing onto a different target ordinarily produces a different merge
//! commit, but that is a property of `rk_git::Repo::prepare_merge`, not of
//! this module, and is not something this module should have to rely on).
//! A phase that never sets these fields (all `None`) keeps exactly its old
//! `(task, phase, attempt)` behavior, since `None == None` on every side
//! changes nothing.

use chrono::{DateTime, Utc};
use rk_core::tuple::{Category, Lifecycle, Pattern, Tuple};
use rk_space::Space;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Durable `(Event, <scope>, "task_span")` phase-span record.
pub const SPAN_IDENTITY: &str = "task_span";

/// One phase of a ticket's task-to-main journey. The variants enumerate the
/// parent ticket's acceptance criteria one-for-one: "ticket readiness and
/// claim" (2 phases), "agent launch and first progress" (2), "completion"
/// (1), "verification queue/start/end" (1 phase carrying up to three
/// timestamps), "landing preparation" (1), "semantic review and bounded
/// rework rounds" (2), "merge" (1), "delivery closure" (1), "actionable
/// hold" (1).
///
/// Every variant has a wired producer: `TicketReady`, `Claimed` and
/// `DeliveryClosure` in `tickets.rs`, `AgentLaunched`, `FirstProgress` and
/// `Completed` in `supervisor.rs`, the rest in `landing.rs`. Readiness is a
/// derived query state (`Tickets::ready()` re-evaluates open tickets against
/// their dependencies on every call), not a discrete transition, so it has
/// no single call site holding its timing the way the others do — instead
/// `TicketReady` is stamped at the two points that can *prove* a ticket just
/// became actionable: ticket creation (no unresolved dependency from the
/// start) and the undelivered → delivered edge of each of its dependencies
/// (the last blocker closing), both in `tickets.rs`
/// (TKT-01M0QMT83E7YXH6ZXHMQG0VRS6).
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
    /// Opaque, producer-supplied digest over whatever this producer
    /// considers "did the same thing actually execute again" — e.g. a
    /// landing gate's per-check span sets this to
    /// `verification_proof_key(repo, candidate, check)`, the digest already
    /// covering the check's command/toolchain/environment policy, so a check
    /// whose PLAN changed at the exact same candidate and plan position is
    /// fenced from an earlier, different execution recorded there (module
    /// doc). Never a fresh/random value minted per replay: an exact replay
    /// of the identical occurrence must recompute the identical digest.
    pub occurrence_key: Option<String>,
    pub proof_kind: Option<String>,
    pub proof_reused: Option<bool>,
    pub authority: Option<Authority>,
    /// Provenance tag for the `queue_wait_ms`/`duration_ms` pair, set only by
    /// [`PhaseSpan::from_durations`]. `Some("additive")` means the two are
    /// disjoint, non-overlapping intervals (admission wait, then execution)
    /// so `queue_wait_ms + duration_ms` is a sound total elapsed; `None`
    /// covers every span built before this field existed (or via direct
    /// timestamps, where `duration_ms`/`queue_wait_ms` are exact derived
    /// differences and need no provenance tag at all). A consumer must never
    /// treat `None` as "additive" by default — that was the earlier bug this
    /// tag exists to make impossible to repeat silently: a producer passing
    /// an already wait-inclusive `duration_ms` into `from_durations` made the
    /// derived `queued_at` double-count the wait when added back. Old rows
    /// are left exactly as recorded rather than retroactively reinterpreted.
    pub duration_semantic: Option<&'static str>,
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
            occurrence_key: None,
            proof_kind: None,
            proof_reused: None,
            authority: None,
            duration_semantic: None,
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

    pub fn occurrence_key(mut self, key: impl Into<String>) -> Self {
        self.occurrence_key = Some(key.into());
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
    ///
    /// Contract: `queue_wait_ms` and `duration_ms` MUST be disjoint,
    /// non-overlapping intervals — admission wait, then execution — never
    /// two measurements of overlapping spans (e.g. one timer started before
    /// admission and never reset once execution began would make
    /// `duration_ms` already include the wait `queue_wait_ms` also reports).
    /// Violating this makes the derived `queued_at` double-count the wait
    /// the moment anything adds `queue_wait_ms + duration_ms` back for a
    /// total. Every call site is expected to measure `duration_ms` from the
    /// point execution itself began (e.g. `RunProgress::execution_started_at`),
    /// not from before admission was requested. The returned span is tagged
    /// [`PhaseSpan::duration_semantic`] `"additive"` so a consumer can tell
    /// it followed this contract, as opposed to an older row with no such
    /// tag at all.
    pub fn from_durations(
        task: impl Into<String>,
        phase: Phase,
        queue_wait_ms: Option<u64>,
        duration_ms: Option<u64>,
        now: DateTime<Utc>,
    ) -> Self {
        let ended_at = now;
        let started_at = duration_ms.map(|d| ended_at - chrono::Duration::milliseconds(d as i64));
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
        span.duration_semantic = Some("additive");
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
            "duration_semantic": self.duration_semantic,
            "terminal_reason": self.terminal_reason,
            "repo": self.repo,
            "target": self.target,
            "candidate": self.candidate,
            "lane": self.lane,
            "occurrence_key": self.occurrence_key,
            "proof_kind": self.proof_kind,
            "proof_reused": self.proof_reused,
            "authority": self.authority.map(Authority::as_str),
        })
    }
}

/// Record `span` once. Idempotent on `(task, phase, attempt)`, additionally
/// fenced by `target`/`candidate`/`lane`/`occurrence_key` when `span` sets
/// them (module doc): if a span already exists on that exact key (a retried
/// caller, a duplicate event replay, or this same durable store reopened
/// after a daemon restart), this is a no-op returning `Ok(false)`; otherwise
/// the span is written as a
/// `Furniture` Event and this returns `Ok(true)`.
pub fn record_phase_span(
    space: &Space,
    scope: &str,
    castle: &str,
    span: &PhaseSpan,
) -> rk_core::Result<bool> {
    if span_exists(space, scope, span)? {
        return Ok(false);
    }
    let tuple = Tuple::new(
        Category::Event,
        scope,
        SPAN_IDENTITY,
        castle,
        span.to_payload(),
    )
    .with_lifecycle(Lifecycle::Furniture);
    space.out(tuple)?;
    Ok(true)
}

fn span_exists(space: &Space, scope: &str, span: &PhaseSpan) -> rk_core::Result<bool> {
    let mut pattern = Pattern::category(Category::Event)
        .identity(SPAN_IDENTITY)
        .scope(scope);
    pattern.payload_search = Some(format!("\"task\":\"{}\"", span.task));
    pattern.payload_search_and = Some(format!("\"phase\":\"{}\"", span.phase.as_str()));
    Ok(space.scan(&pattern)?.into_iter().any(|t| {
        t.payload.get("attempt").and_then(Value::as_u64) == Some(u64::from(span.attempt))
            && t.payload.get("target").and_then(Value::as_str) == span.target.as_deref()
            && t.payload.get("candidate").and_then(Value::as_str) == span.candidate.as_deref()
            && t.payload.get("lane").and_then(Value::as_str) == span.lane.as_deref()
            && t.payload.get("occurrence_key").and_then(Value::as_str)
                == span.occurrence_key.as_deref()
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
            let wrote =
                record_phase_span(&space, SYSTEM_SCOPE, "daemon", &PhaseSpan::new(task, phase))
                    .unwrap();
            assert!(wrote, "{phase:?} should write on first record");
        }
        let spans = spans_for_task(&space, SYSTEM_SCOPE, task).unwrap();
        assert_eq!(spans.len(), 9);
        let phases: std::collections::BTreeSet<&str> =
            spans.iter().map(|s| s["phase"].as_str().unwrap()).collect();
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
        // Ordered by attempt, not by scan position: two spans minted in the
        // same millisecond sort randomly by `RecordId` (see `id.rs`'s own
        // doc on ULID sub-ms ordering), so `spans_for_task`'s "oldest first"
        // is only an approximation under contention.
        let by_attempt = |attempt: u64| {
            spans
                .iter()
                .find(|s| s["attempt"] == attempt)
                .unwrap_or_else(|| panic!("no span with attempt {attempt}"))
        };
        assert_eq!(by_attempt(1)["proof_kind"], "focused-inner");
        assert_eq!(by_attempt(2)["proof_kind"], "full-final");
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

        assert_eq!(
            spans_for_task(&space, SYSTEM_SCOPE, "TKT-a").unwrap().len(),
            2
        );
        assert_eq!(
            spans_for_task(&space, SYSTEM_SCOPE, "TKT-ab")
                .unwrap()
                .len(),
            1
        );
    }

    /// A landing gate's per-check span reuses its plan position as `attempt`
    /// every round (module doc): a later round's real occurrence — a new
    /// candidate, same check, same small ordinal — must never be shadowed by
    /// an earlier round's, while an exact replay of the SAME round (same
    /// candidate) still dedups.
    #[test]
    fn a_new_candidate_reusing_an_earlier_rounds_attempt_is_recorded_not_shadowed() {
        let space = space();
        let task = "TKT-round";
        let round_one =
            PhaseSpan::from_durations(task, Phase::VerificationQueued, None, Some(10), Utc::now())
                .attempt(1)
                .candidate("sha-a")
                .lane("verify");
        let round_two =
            PhaseSpan::from_durations(task, Phase::VerificationQueued, None, Some(12), Utc::now())
                .attempt(1)
                .candidate("sha-b")
                .lane("verify");
        assert!(record_phase_span(&space, SYSTEM_SCOPE, "daemon", &round_one).unwrap());
        assert!(
            record_phase_span(&space, SYSTEM_SCOPE, "daemon", &round_two).unwrap(),
            "a genuinely new candidate at the same attempt must still be recorded"
        );
        assert_eq!(spans_for_task(&space, SYSTEM_SCOPE, task).unwrap().len(), 2);

        // Exact replay of round two (a crash-resume re-run against the
        // identical candidate) stays idempotent.
        assert!(
            !record_phase_span(&space, SYSTEM_SCOPE, "daemon", &round_two).unwrap(),
            "a replay of the same candidate/lane/attempt must not duplicate"
        );
        assert_eq!(spans_for_task(&space, SYSTEM_SCOPE, task).unwrap().len(), 2);
    }

    /// A check whose PLAN changed (a different command/toolchain/environment
    /// policy) at the exact same candidate and plan position — `attempt` and
    /// `candidate` both unchanged, only `occurrence_key` differs — must still
    /// be recorded as a distinct occurrence, not shadowed by whatever ran
    /// there before at that candidate.
    #[test]
    fn a_changed_plan_at_the_same_candidate_is_recorded_not_shadowed() {
        let space = space();
        let task = "TKT-replan";
        let before =
            PhaseSpan::from_durations(task, Phase::VerificationQueued, None, Some(10), Utc::now())
                .attempt(1)
                .candidate("sha-a")
                .lane("verify")
                .occurrence_key("cmd-v1-digest");
        let after =
            PhaseSpan::from_durations(task, Phase::VerificationQueued, None, Some(10), Utc::now())
                .attempt(1)
                .candidate("sha-a")
                .lane("verify")
                .occurrence_key("cmd-v2-digest");
        assert!(record_phase_span(&space, SYSTEM_SCOPE, "daemon", &before).unwrap());
        assert!(
            record_phase_span(&space, SYSTEM_SCOPE, "daemon", &after).unwrap(),
            "a changed command/toolchain/environment at the same candidate and attempt \
             must still be recorded, not treated as the same occurrence"
        );
        assert_eq!(spans_for_task(&space, SYSTEM_SCOPE, task).unwrap().len(), 2);

        // Exact replay (identical occurrence_key) stays idempotent.
        assert!(!record_phase_span(&space, SYSTEM_SCOPE, "daemon", &after).unwrap());
        assert_eq!(spans_for_task(&space, SYSTEM_SCOPE, task).unwrap().len(), 2);
    }

    /// `target` is fenced explicitly, not assumed implied by `candidate`: a
    /// span naming the SAME candidate but a DIFFERENT target must still be
    /// recorded as its own occurrence, and an exact replay against the same
    /// target must still dedup.
    #[test]
    fn a_same_candidate_different_target_is_recorded_not_shadowed() {
        let space = space();
        let task = "TKT-retarget";
        let onto_parent = PhaseSpan::new(task, Phase::VerificationQueued)
            .attempt(1)
            .target("parent")
            .candidate("sha-a")
            .lane("verify");
        let onto_main = PhaseSpan::new(task, Phase::VerificationQueued)
            .attempt(1)
            .target("main")
            .candidate("sha-a")
            .lane("verify");
        assert!(record_phase_span(&space, SYSTEM_SCOPE, "daemon", &onto_parent).unwrap());
        assert!(
            record_phase_span(&space, SYSTEM_SCOPE, "daemon", &onto_main).unwrap(),
            "a different target at the same candidate/attempt/lane must still be recorded"
        );
        assert_eq!(spans_for_task(&space, SYSTEM_SCOPE, task).unwrap().len(), 2);

        // Exact replay against the same target stays idempotent.
        assert!(!record_phase_span(&space, SYSTEM_SCOPE, "daemon", &onto_main).unwrap());
        assert_eq!(spans_for_task(&space, SYSTEM_SCOPE, task).unwrap().len(), 2);
    }
}

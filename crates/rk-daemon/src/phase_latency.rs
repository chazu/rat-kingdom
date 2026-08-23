//! Repository-policy phase latency targets over the durable task-to-main
//! phase-span substrate (`crate::span`, TKT-01M0P974EZZTPMGVP4S0E76NXH) —
//! parent TKT-01M0P2KNB92EAV2QG9256MY3QV, this ticket
//! TKT-01M0P974MQK5XE1MR9KQCWT654.
//!
//! A repo's `.rk/repo.cue` may declare a warning and/or intervention
//! wall-clock target per phase (`rk_workflow::PhaseLatencyPolicy`,
//! stack-neutral and empty by default — see its own doc comment). This module
//! classifies one phase occurrence — either a settled `task_span` (terminal,
//! [`evaluate_terminal_spans`]) or a currently in-flight occurrence supplied
//! by a live source ([`LivePhaseProbe`], [`evaluate_live_probes`]) — against
//! those targets, and announces AT MOST ONE attention item per exact breach
//! identity ([`Breach::key`]) through the SAME durable "recovery item"
//! substrate ([`crate::recovery::RecoveryAnnouncer`]) every other automated
//! escalation in this daemon already goes through: `rk inbox` surfaces it,
//! `rk inbox ack` retires it, `[[notify.sinks]]` fans it out. [`announce_breach`]
//! is a thin idempotency guard on top of `announce` — scan for an existing
//! `recovery_action` carrying this exact breach key before writing a new one,
//! mirroring [`crate::span::record_phase_span`]'s own scan-before-write
//! dedup — the only new mechanism this module adds; everything else reuses
//! substrate B2 already built.
//!
//! # Never more than detection
//!
//! This module only ever reads state and writes ONE advisory event per
//! breach identity. It never kills a process, never calls into
//! verification/review, never mutates a repository, and never touches a
//! human gate's own decision state — a breached `Human`-authority phase
//! (e.g. `AttentionHold`) is reported exactly like any other breach,
//! carrying its authority through unchanged in `refs`, and nothing here acts
//! on it.

use crate::recovery::{AnnounceOutcome, RateCap, RecoveryAction, RecoveryAnnouncer};
use crate::span::Phase;
use rk_core::notify::{EscalationNotice, Severity, SinkRegistry};
use rk_core::tuple::{Category, Pattern};
use rk_space::Space;
use rk_workflow::PhaseLatencyPolicy;
use serde_json::Value;
use std::collections::BTreeMap;

/// Machine-readable key every `refs` entry carrying the exact breach identity
/// is stored under, so [`breach_already_announced`] can scan for it.
const BREACH_KEY_REF: &str = "breach_key";

/// Severity tier a breach has escalated to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Warning,
    Intervention,
}

impl Tier {
    fn as_str(self) -> &'static str {
        match self {
            Tier::Warning => "warning",
            Tier::Intervention => "intervention",
        }
    }

    fn severity(self) -> Severity {
        match self {
            Tier::Warning => Severity::Warn,
            Tier::Intervention => Severity::Critical,
        }
    }
}

/// Warning/intervention target for one phase, resolved to milliseconds.
#[derive(Debug, Clone, Copy, Default)]
struct TargetMs {
    warning_ms: Option<u64>,
    intervention_ms: Option<u64>,
}

/// One phase occurrence still in flight, from whatever live source the
/// caller has evidence for (today: the landing queue — see
/// `crate::server`'s sweep wiring). Decoupled from any one runtime source so
/// a future producer (an agent record, a workflow instance) can supply
/// probes the same way without this module knowing about it.
#[derive(Debug, Clone)]
pub struct LivePhaseProbe {
    pub task: String,
    pub phase: Phase,
    pub attempt: u32,
    pub elapsed_ms: u64,
    pub repo: Option<String>,
    pub target: Option<String>,
    pub candidate: Option<String>,
    /// Free-form machine-readable capacity-lane occupancy evidence (e.g.
    /// landing-queue depth) — carried through verbatim into the attention
    /// item's `refs`, never interpreted here.
    pub capacity: BTreeMap<String, String>,
}

/// One classified phase-latency breach, ready to announce.
#[derive(Debug, Clone)]
pub struct Breach {
    pub task: String,
    pub phase: Phase,
    pub attempt: u32,
    pub tier: Tier,
    pub elapsed_ms: u64,
    pub target_ms: u64,
    /// `true` for a settled `task_span` (the phase is over); `false` for a
    /// still-in-flight [`LivePhaseProbe`].
    pub terminal: bool,
    pub repo: Option<String>,
    pub target: Option<String>,
    pub candidate: Option<String>,
    pub terminal_reason: Option<String>,
    pub authority: Option<String>,
    pub capacity: BTreeMap<String, String>,
}

impl Breach {
    /// The exact identity dedup is keyed on. Stable across restarts and
    /// repeated sweeps: the same underlying occurrence crossing the same
    /// tier always renders this identically, and a later, worse tier for the
    /// same occurrence is a DISTINCT key — a fresh escalation, not a
    /// duplicate of the earlier one.
    pub fn key(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            self.task,
            self.phase.as_str(),
            self.attempt,
            self.tier.as_str()
        )
    }
}

fn phase_from_str(s: &str) -> Option<Phase> {
    Some(match s {
        "ticket_ready" => Phase::TicketReady,
        "claimed" => Phase::Claimed,
        "agent_launched" => Phase::AgentLaunched,
        "first_progress" => Phase::FirstProgress,
        "completed" => Phase::Completed,
        "verification" => Phase::VerificationQueued,
        "landing_prep" => Phase::LandingPrep,
        "semantic_review" => Phase::SemanticReview,
        "rework" => Phase::Rework,
        "merge" => Phase::Merge,
        "delivery_closure" => Phase::DeliveryClosure,
        "attention_hold" => Phase::AttentionHold,
        _ => return None,
    })
}

fn parse_ms(value: &str) -> Option<u64> {
    if value.trim().is_empty() {
        return None;
    }
    crate::workflow_exec::parse_duration(value)
        .ok()
        .map(|d| d.as_millis() as u64)
}

fn resolve_target(policy: &PhaseLatencyPolicy, phase: Phase) -> Option<TargetMs> {
    let target = policy.targets.get(phase.as_str())?;
    let warning_ms = parse_ms(&target.warning);
    let intervention_ms = parse_ms(&target.intervention);
    if warning_ms.is_none() && intervention_ms.is_none() {
        return None;
    }
    Some(TargetMs {
        warning_ms,
        intervention_ms,
    })
}

/// Classify `elapsed_ms` against `target`, returning the crossed tier and the
/// exact target (ms) it crossed — the intervention tier takes precedence
/// when both are crossed, since it is the more severe, still-true fact about
/// this occurrence.
fn classify(elapsed_ms: u64, target: TargetMs) -> Option<(Tier, u64)> {
    if let Some(intervention_ms) = target.intervention_ms {
        if elapsed_ms >= intervention_ms {
            return Some((Tier::Intervention, intervention_ms));
        }
    }
    if let Some(warning_ms) = target.warning_ms {
        if elapsed_ms >= warning_ms {
            return Some((Tier::Warning, warning_ms));
        }
    }
    None
}

/// Classify every settled `task_span` payload (as returned by
/// `crate::span::spans_for_task`/a broader scan of the same identity) against
/// the calling repo's policy. A span with no `duration_ms` (never settled —
/// should not occur for a durably recorded span, but the substrate makes no
/// such guarantee at the type level) or an unrecognized `phase` string is
/// skipped rather than guessed at.
pub fn evaluate_terminal_spans(
    spans: &[Value],
    policy_for_repo: &dyn Fn(&str) -> PhaseLatencyPolicy,
) -> Vec<Breach> {
    let mut breaches = Vec::new();
    for span in spans {
        let Some(phase) = span
            .get("phase")
            .and_then(Value::as_str)
            .and_then(phase_from_str)
        else {
            continue;
        };
        let Some(duration_ms) = span.get("duration_ms").and_then(Value::as_i64) else {
            continue;
        };
        if duration_ms < 0 {
            continue;
        }
        let Some(task) = span.get("task").and_then(Value::as_str) else {
            continue;
        };
        let repo = span.get("repo").and_then(Value::as_str).map(String::from);
        let policy = policy_for_repo(repo.as_deref().unwrap_or(""));
        let Some(target) = resolve_target(&policy, phase) else {
            continue;
        };
        let Some((tier, target_ms)) = classify(duration_ms as u64, target) else {
            continue;
        };
        let attempt = span.get("attempt").and_then(Value::as_u64).unwrap_or(1) as u32;
        breaches.push(Breach {
            task: task.to_string(),
            phase,
            attempt,
            tier,
            elapsed_ms: duration_ms as u64,
            target_ms,
            terminal: true,
            repo,
            target: span.get("target").and_then(Value::as_str).map(String::from),
            candidate: span
                .get("candidate")
                .and_then(Value::as_str)
                .map(String::from),
            terminal_reason: span
                .get("terminal_reason")
                .and_then(Value::as_str)
                .map(String::from),
            authority: span
                .get("authority")
                .and_then(Value::as_str)
                .map(String::from),
            capacity: BTreeMap::new(),
        });
    }
    breaches
}

/// Classify every still-in-flight probe against the calling repo's policy.
pub fn evaluate_live_probes(
    probes: &[LivePhaseProbe],
    policy_for_repo: &dyn Fn(&str) -> PhaseLatencyPolicy,
) -> Vec<Breach> {
    let mut breaches = Vec::new();
    for probe in probes {
        let policy = policy_for_repo(probe.repo.as_deref().unwrap_or(""));
        let Some(target) = resolve_target(&policy, probe.phase) else {
            continue;
        };
        let Some((tier, target_ms)) = classify(probe.elapsed_ms, target) else {
            continue;
        };
        breaches.push(Breach {
            task: probe.task.clone(),
            phase: probe.phase,
            attempt: probe.attempt,
            tier,
            elapsed_ms: probe.elapsed_ms,
            target_ms,
            terminal: false,
            repo: probe.repo.clone(),
            target: probe.target.clone(),
            candidate: probe.candidate.clone(),
            terminal_reason: None,
            authority: None,
            capacity: probe.capacity.clone(),
        });
    }
    breaches
}

fn breach_already_announced(space: &Space, key: &str) -> rk_core::Result<bool> {
    let mut pattern =
        Pattern::category(Category::Event).identity(crate::recovery::RECOVERY_ACTION_IDENTITY);
    pattern.payload_search = Some(format!("\"{BREACH_KEY_REF}\":\"{key}\""));
    Ok(!space.scan(&pattern)?.is_empty())
}

/// Announce `breach` at most once: if a `recovery_action` already carries
/// this exact breach key (a prior sweep, a duplicate source event replay, or
/// this same durable store reopened after a daemon restart), this is a no-op
/// returning `Ok(None)`. Otherwise the breach is announced through
/// `RecoveryAnnouncer::announce` (durable `recovery_action` Event +
/// `[[notify.sinks]]` fan-out) exactly like every other automated escalation
/// in this daemon, and this returns the outcome. Uncapped
/// ([`RateCap::unlimited`]): the identity-based dedup above is already the
/// real bound, and rate-capping distinct breach identities would silently
/// drop a genuine, different intervention during a real fleet-wide slowdown.
pub fn announce_breach(
    space: &Space,
    sinks: &SinkRegistry,
    announcer: &RecoveryAnnouncer,
    instance: &str,
    breach: &Breach,
) -> rk_core::Result<Option<AnnounceOutcome>> {
    let key = breach.key();
    if breach_already_announced(space, &key)? {
        return Ok(None);
    }
    let scope = breach.repo.clone().unwrap_or_else(|| "system".to_string());
    let mut refs = BTreeMap::new();
    refs.insert(BREACH_KEY_REF.to_string(), key);
    refs.insert("phase".to_string(), breach.phase.as_str().to_string());
    refs.insert("attempt".to_string(), breach.attempt.to_string());
    refs.insert("tier".to_string(), breach.tier.as_str().to_string());
    refs.insert("elapsed_ms".to_string(), breach.elapsed_ms.to_string());
    refs.insert("target_ms".to_string(), breach.target_ms.to_string());
    refs.insert("terminal".to_string(), breach.terminal.to_string());
    if let Some(target) = &breach.target {
        refs.insert("target_branch".to_string(), target.clone());
    }
    if let Some(candidate) = &breach.candidate {
        refs.insert("candidate".to_string(), candidate.clone());
    }
    if let Some(reason) = &breach.terminal_reason {
        refs.insert("terminal_reason".to_string(), reason.clone());
    }
    if let Some(authority) = &breach.authority {
        refs.insert("authority".to_string(), authority.clone());
    }
    for (k, v) in &breach.capacity {
        refs.insert(format!("capacity_{k}"), v.clone());
    }
    let text = format!(
        "{} phase `{}` (attempt {}) {} its {} target: {}ms elapsed vs {}ms target",
        if breach.terminal {
            "settled"
        } else {
            "in-flight"
        },
        breach.phase.as_str(),
        breach.attempt,
        if matches!(breach.tier, Tier::Intervention) {
            "exceeded"
        } else {
            "is approaching"
        },
        breach.tier.as_str(),
        breach.elapsed_ms,
        breach.target_ms,
    );
    let notice = EscalationNotice {
        tuple_id: String::new(), // overwritten by `announce` itself.
        class: "phase-latency".to_string(),
        severity: breach.tier.severity(),
        scope,
        subject: breach.task.clone(),
        text,
        suggested_action: Some(format!("rk status {}", breach.task)),
        refs,
    };
    let outcome = announcer.announce(
        space,
        sinks,
        RecoveryAction {
            kind: "phase-latency".to_string(),
            instance: instance.to_string(),
            notice,
        },
        RateCap::unlimited(),
    )?;
    Ok(Some(outcome))
}

/// One sweep pass: classify every supplied terminal span and live probe
/// against `policy_for_repo`, and durably announce every NEWLY crossed
/// breach ([`announce_breach`]'s own idempotency guard makes an
/// already-announced identity a no-op). Returns the breaches actually
/// announced this pass — an empty result means either nothing breached or
/// everything that did was already announced by a prior pass.
pub fn sweep_once(
    space: &Space,
    sinks: &SinkRegistry,
    announcer: &RecoveryAnnouncer,
    instance: &str,
    terminal_spans: &[Value],
    live_probes: &[LivePhaseProbe],
    policy_for_repo: &dyn Fn(&str) -> PhaseLatencyPolicy,
) -> rk_core::Result<Vec<Breach>> {
    let mut candidates = evaluate_terminal_spans(terminal_spans, policy_for_repo);
    candidates.extend(evaluate_live_probes(live_probes, policy_for_repo));
    let mut announced = Vec::new();
    for breach in candidates {
        if announce_breach(space, sinks, announcer, instance, &breach)?.is_some() {
            announced.push(breach);
        }
    }
    Ok(announced)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::span::{record_phase_span, PhaseSpan};
    use rk_core::tuple::SYSTEM_SCOPE;
    use std::collections::HashMap;

    fn space() -> Space {
        Space::open_in_memory().unwrap()
    }

    fn sinks() -> SinkRegistry {
        SinkRegistry::new()
    }

    fn policy_with(phase: &str, warning: &str, intervention: &str) -> PhaseLatencyPolicy {
        let mut targets = HashMap::new();
        targets.insert(
            phase.to_string(),
            rk_workflow::PhaseLatencyTarget {
                warning: warning.to_string(),
                intervention: intervention.to_string(),
            },
        );
        PhaseLatencyPolicy { targets }
    }

    fn no_policy() -> PhaseLatencyPolicy {
        PhaseLatencyPolicy::default()
    }

    fn probe(task: &str, phase: Phase, elapsed_ms: u64) -> LivePhaseProbe {
        LivePhaseProbe {
            task: task.to_string(),
            phase,
            attempt: 1,
            elapsed_ms,
            repo: Some("myrepo".to_string()),
            target: Some("main".to_string()),
            candidate: Some("abc123".to_string()),
            capacity: BTreeMap::new(),
        }
    }

    #[test]
    fn below_target_never_breaches() {
        let policy = policy_with("verification", "20m", "45m");
        let breaches = evaluate_live_probes(
            &[probe("TKT-a", Phase::VerificationQueued, 60_000)],
            &|_| policy.clone(),
        );
        assert!(breaches.is_empty());
    }

    #[test]
    fn crossing_warning_but_not_intervention_is_warning_tier() {
        let policy = policy_with("verification", "20m", "45m");
        let breaches = evaluate_live_probes(
            &[probe("TKT-a", Phase::VerificationQueued, 25 * 60_000)],
            &|_| policy.clone(),
        );
        assert_eq!(breaches.len(), 1);
        assert_eq!(breaches[0].tier, Tier::Warning);
        assert_eq!(breaches[0].target_ms, 20 * 60_000);
    }

    #[test]
    fn crossing_intervention_is_intervention_tier_not_warning() {
        let policy = policy_with("verification", "20m", "45m");
        let breaches = evaluate_live_probes(
            &[probe("TKT-a", Phase::VerificationQueued, 50 * 60_000)],
            &|_| policy.clone(),
        );
        assert_eq!(breaches.len(), 1);
        assert_eq!(breaches[0].tier, Tier::Intervention);
        assert_eq!(breaches[0].target_ms, 45 * 60_000);
    }

    #[test]
    fn healthy_long_running_work_with_no_configured_target_never_breaches() {
        // A phase this repo has declared no target for is never flagged no
        // matter how long it runs — stack neutrality means "unconfigured"
        // reads as "no opinion", not "infinite target".
        let breaches = evaluate_live_probes(
            &[probe("TKT-a", Phase::VerificationQueued, 999_999_999)],
            &|_| no_policy(),
        );
        assert!(breaches.is_empty());
    }

    #[test]
    fn a_timed_out_terminal_phase_carries_its_terminal_reason_and_still_breaches() {
        let span = PhaseSpan::from_durations(
            "TKT-timeout",
            Phase::VerificationQueued,
            Some(0),
            Some(50 * 60_000),
            chrono::Utc::now(),
        )
        .terminal_reason("timeout")
        .repo("myrepo");
        let payload = serde_json::to_value(span_payload(&span)).unwrap();
        let policy = policy_with("verification", "20m", "45m");
        let breaches = evaluate_terminal_spans(&[payload], &|_| policy.clone());
        assert_eq!(breaches.len(), 1);
        assert_eq!(breaches[0].tier, Tier::Intervention);
        assert_eq!(breaches[0].terminal_reason.as_deref(), Some("timeout"));
        assert!(breaches[0].terminal);
    }

    #[test]
    fn a_correct_human_gate_breach_carries_human_authority_and_mutates_nothing_else() {
        let space = space();
        let span = PhaseSpan::new("TKT-gate", Phase::AttentionHold)
            .started_at(chrono::Utc::now() - chrono::Duration::minutes(90))
            .ended_at(chrono::Utc::now())
            .authority(crate::span::Authority::Human)
            .terminal_reason("protected-path-hit")
            .repo("myrepo");
        record_phase_span(&space, SYSTEM_SCOPE, "daemon", &span).unwrap();
        let spans = crate::span::spans_for_task(&space, SYSTEM_SCOPE, "TKT-gate").unwrap();
        let policy = policy_with("attention_hold", "20m", "45m");
        let breaches = evaluate_terminal_spans(&spans, &|_| policy.clone());
        assert_eq!(breaches.len(), 1);
        assert_eq!(breaches[0].authority.as_deref(), Some("human"));

        // Announcing it writes exactly one new tuple (the recovery_action
        // event) and touches nothing else — no gate, no ticket, no repo.
        let before = space
            .scan(&Pattern::category(Category::Event))
            .unwrap()
            .len();
        let announcer = RecoveryAnnouncer::new();
        announce_breach(&space, &sinks(), &announcer, "daemon", &breaches[0])
            .unwrap()
            .expect("first announce should write");
        let after = space
            .scan(&Pattern::category(Category::Event))
            .unwrap()
            .len();
        assert_eq!(after, before + 1);
    }

    #[test]
    fn duplicate_source_event_replay_does_not_duplicate_the_alert() {
        let space = space();
        let breach = Breach {
            task: "TKT-replay".to_string(),
            phase: Phase::VerificationQueued,
            attempt: 1,
            tier: Tier::Warning,
            elapsed_ms: 25 * 60_000,
            target_ms: 20 * 60_000,
            terminal: false,
            repo: Some("myrepo".to_string()),
            target: Some("main".to_string()),
            candidate: Some("abc123".to_string()),
            terminal_reason: None,
            authority: None,
            capacity: BTreeMap::new(),
        };
        let announcer = RecoveryAnnouncer::new();
        let sinks = sinks();
        let first = announce_breach(&space, &sinks, &announcer, "daemon", &breach).unwrap();
        assert!(first.is_some());
        // A replayed detection of the identical occurrence.
        let second = announce_breach(&space, &sinks, &announcer, "daemon", &breach).unwrap();
        assert!(second.is_none(), "replay must not duplicate the alert");
        let third = announce_breach(&space, &sinks, &announcer, "daemon", &breach).unwrap();
        assert!(third.is_none());

        let mut pattern =
            Pattern::category(Category::Event).identity(crate::recovery::RECOVERY_ACTION_IDENTITY);
        pattern.payload_search = Some(format!("\"{BREACH_KEY_REF}\":\"{}\"", breach.key()));
        assert_eq!(space.scan(&pattern).unwrap().len(), 1);
    }

    #[test]
    fn restart_over_the_same_durable_store_does_not_duplicate_the_alert() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("space.db");
        let breach = Breach {
            task: "TKT-restart".to_string(),
            phase: Phase::SemanticReview,
            attempt: 1,
            tier: Tier::Intervention,
            elapsed_ms: 50 * 60_000,
            target_ms: 45 * 60_000,
            terminal: false,
            repo: Some("myrepo".to_string()),
            target: None,
            candidate: None,
            terminal_reason: None,
            authority: None,
            capacity: BTreeMap::new(),
        };
        {
            let space = Space::open(&path).unwrap();
            let announcer = RecoveryAnnouncer::new();
            let outcome = announce_breach(&space, &sinks(), &announcer, "daemon", &breach).unwrap();
            assert!(outcome.is_some());
        }
        // Simulated restart: a brand new Space handle AND a brand new
        // RecoveryAnnouncer (its rate-cap state is in-memory and does not
        // survive a restart either) over the same durable file.
        let reopened = Space::open(&path).unwrap();
        let announcer = RecoveryAnnouncer::new();
        let outcome = announce_breach(&reopened, &sinks(), &announcer, "daemon", &breach).unwrap();
        assert!(outcome.is_none(), "restart must not duplicate the alert");
    }

    #[test]
    fn sweep_once_dedups_across_repeated_sweeps_and_returns_only_new_breaches() {
        let space = space();
        let sinks = sinks();
        let announcer = RecoveryAnnouncer::new();
        let policy = policy_with("verification", "20m", "45m");
        let probes = vec![probe("TKT-sweep", Phase::VerificationQueued, 25 * 60_000)];

        let first = sweep_once(&space, &sinks, &announcer, "daemon", &[], &probes, &|_| {
            policy.clone()
        })
        .unwrap();
        assert_eq!(first.len(), 1, "first sweep announces the new breach");

        // A repeated sweep tick observing the SAME still-in-flight occurrence
        // (same elapsed bucket crossing the same tier) must not re-announce.
        let second = sweep_once(&space, &sinks, &announcer, "daemon", &[], &probes, &|_| {
            policy.clone()
        })
        .unwrap();
        assert!(second.is_empty(), "repeated sweep must not duplicate");

        // Escalating further, past intervention, IS a distinct breach identity.
        let escalated = vec![probe("TKT-sweep", Phase::VerificationQueued, 50 * 60_000)];
        let third = sweep_once(
            &space,
            &sinks,
            &announcer,
            "daemon",
            &[],
            &escalated,
            &|_| policy.clone(),
        )
        .unwrap();
        assert_eq!(third.len(), 1);
        assert_eq!(third[0].tier, Tier::Intervention);
    }

    /// Local mirror of `PhaseSpan::to_payload` (private to `span`) built only
    /// from the fields these tests need, so the terminal-span tests can
    /// exercise `evaluate_terminal_spans` without depending on `span`
    /// internals it does not expose.
    fn span_payload(span: &PhaseSpan) -> serde_json::Value {
        serde_json::json!({
            "task": span.task,
            "phase": span.phase.as_str(),
            "attempt": span.attempt,
            "duration_ms": span.duration_ms(),
            "terminal_reason": span.terminal_reason,
            "repo": span.repo,
            "target": span.target,
            "candidate": span.candidate,
            "authority": span.authority.map(|a| a.as_str()),
        })
    }
}

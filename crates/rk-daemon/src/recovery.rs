//! Shared "announce" helper for automated recovery actions (strategic review
//! B2), built on the B1 [`NotificationSink`] fan-out.
//!
//! Before this, an automated recovery action had nowhere durable to say what
//! it did: it either acted silently or improvised its own record. This module
//! is the one seam every future recovery source (auto-respawn, kill-at-`rk
//! done`, stale-instance timeout, orphaned-ticket reopen) goes through:
//!
//! * [`RecoveryAnnouncer::announce`] — durably records the action as a
//!   channel-agnostic [`EscalationNotice`], fans it out through the sinks the
//!   caller already built from `[[notify.sinks]]`, and enforces a per-action
//!   rolling rate cap (jittered ±10% so many independent callers sharing a
//!   nominal window do not all reset it in lockstep). A capped action is
//!   still announced — at raised severity, explaining it was held — because
//!   "silence is earned later, not shipped now" (the strategic review's
//!   words) applies to a rate-capped action exactly as much as a performed
//!   one.
//! * [`renotify_sweep`] — the other half of "announce mode": an escalation
//!   nobody acked (`rk inbox ack <id>`, see `server.rs::handle_inbox_ack`)
//!   re-pushes on a fixed schedule rather than going silent after its first
//!   push, which is what let a finished-but-unmerged branch sit unseen for
//!   two days (TKT-147). An acked escalation never re-notifies.
//!
//! Both are pure over an injected [`Space`]/[`SinkRegistry`], so any daemon
//! subsystem holding those two (the reactor today; a future recovery sweep
//! wherever it lands) can call them without depending on `Reactor` itself.

use chrono::{DateTime, Utc};
use rk_core::notify::{EscalationNotice, Severity, SinkDedup, SinkRegistry};
use rk_core::tuple::{Category, Lifecycle, Pattern, Tuple};
use rk_space::Space;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Identity of the durable escalation record [`RecoveryAnnouncer::announce`]
/// writes. `rk inbox` surfaces one row per unacked instance of this identity
/// (see `inbox::recovery_action_rows`); `rk inbox ack <id>` and
/// [`renotify_sweep`] both key off the tuple's own id.
pub const RECOVERY_ACTION_IDENTITY: &str = "recovery_action";
/// Identity of the durable ack marker `inbox.ack` writes (`server.rs`). A
/// `recovery_action` whose id appears in one of these tuples' `tuple` field
/// is acked: it drops off `rk inbox` and [`renotify_sweep`] skips it forever.
pub const INBOX_ACK_IDENTITY: &str = "inbox_ack";
/// Identity of the durable per-(escalation, sink) delivery marker, mirroring
/// the reactor's own `already_fired`/`mark_notified` pair (see
/// `reactor.rs::EscalationDedup`) but namespaced separately so this module
/// never has to reach into reactor internals.
const DELIVERY_MARKER_IDENTITY: &str = "recovery_delivered";
/// Identity of the durable "this escalation was re-notified for the Nth time"
/// marker [`renotify_sweep`] writes. The count of these per escalation IS the
/// schedule's position — no separate counter is kept.
const RENOTIFY_MARKER_IDENTITY: &str = "recovery_renotify";
/// Author of every marker this module writes. Bookkeeping only — nothing
/// reacts to it — kept distinct from the caller-supplied
/// [`RecoveryAction::instance`] so a marker never reads as if the acting
/// subsystem wrote it.
const RECOVERY_MARKER_INSTANCE: &str = "recovery";

/// One automated recovery action, ready to announce.
pub struct RecoveryAction {
    /// The rate-cap bucket AND the class this action is grouped under for
    /// scheduling purposes (e.g. `"respawn"`, `"kill-process-group"`,
    /// `"instance-timeout"`, `"ticket-reopen"`). Distinct from
    /// [`EscalationNotice::class`], which is the sink-routing key — callers
    /// typically make the two the same string, but nothing here requires it.
    pub kind: String,
    /// Authorship recorded on the durable escalation tuple (e.g. `"reactor"`,
    /// `"supervisor"`) — provenance, not a routing key.
    pub instance: String,
    pub notice: EscalationNotice,
}

/// A rolling-window cap: at most `max` actions of one `kind` within `window`.
/// `max == 0` means uncapped.
#[derive(Debug, Clone, Copy)]
pub struct RateCap {
    pub max: u32,
    pub window: Duration,
}

impl RateCap {
    pub fn per_hour(max: u32) -> Self {
        Self {
            max,
            window: Duration::from_secs(3600),
        }
    }

    pub const fn unlimited() -> Self {
        Self {
            max: 0,
            window: Duration::from_secs(1),
        }
    }
}

/// What [`RecoveryAnnouncer::announce`] did. Either way the escalation was
/// written and fanned out — the variants distinguish whether the caller's
/// underlying action should actually proceed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnnounceOutcome {
    /// Under the rate cap: the caller's action should proceed.
    Announced { event_id: String },
    /// Over the rate cap: the caller's action must be HELD, not performed.
    /// The escalation itself still went out, at raised severity, explaining
    /// why nothing happened.
    RateHeld { event_id: String },
}

impl AnnounceOutcome {
    pub fn event_id(&self) -> &str {
        match self {
            Self::Announced { event_id } | Self::RateHeld { event_id } => event_id,
        }
    }

    pub fn held(&self) -> bool {
        matches!(self, Self::RateHeld { .. })
    }
}

/// Scale `window` by `1.0 + frac`. Pure so the jitter math is directly
/// testable without depending on the RNG.
fn scale_window(window: Duration, frac: f64) -> Duration {
    Duration::from_secs_f64((window.as_secs_f64() * (1.0 + frac)).max(0.0))
}

/// ±10% jitter on a rate-cap window: many independent recovery loops sharing
/// the same nominal window (a fleet-wide respawn storm, a batch of
/// stale-instance timeouts firing on the same reactor tick) must not all
/// reset their rolling cap at exactly the same instant, which is exactly the
/// condition that turns a rate cap into a thundering herd every `window`.
fn jittered_window(window: Duration) -> Duration {
    scale_window(window, rand::random::<f64>() * 0.2 - 0.1)
}

/// Durable "already pushed this notice on this sink" memory, backed directly
/// by `Space` rather than a caller's own marker store — this module has no
/// state of its own to reuse the way the reactor's `EscalationDedup` reuses
/// `already_fired`.
struct SpaceDedup<'a> {
    space: &'a Space,
}

impl SpaceDedup<'_> {
    fn key(notice: &EscalationNotice, sink: &str) -> String {
        format!("{}@{sink}", notice.tuple_id)
    }
}

impl SinkDedup for SpaceDedup<'_> {
    fn already_delivered(&self, notice: &EscalationNotice, sink: &str) -> bool {
        let mut pattern = Pattern::category(Category::Event).identity(DELIVERY_MARKER_IDENTITY);
        pattern.payload_search = Some(format!("\"key\":\"{}\"", Self::key(notice, sink)));
        self.space
            .has_persistence_event_matching(&pattern)
            .unwrap_or(false)
    }

    fn record_delivered(&self, notice: &EscalationNotice, sink: &str) -> rk_core::Result<()> {
        let marker = Tuple::new(
            Category::Event,
            notice.scope.clone(),
            DELIVERY_MARKER_IDENTITY,
            RECOVERY_MARKER_INSTANCE,
            json!({
                "key": Self::key(notice, sink),
                "tuple": notice.tuple_id,
                "sink": sink,
            }),
        )
        .with_lifecycle(Lifecycle::Furniture);
        self.space.out(marker)
    }
}

/// Per-action-kind rolling rate cap state, and the entry point every
/// automated recovery source announces through.
#[derive(Default)]
pub struct RecoveryAnnouncer {
    fires: Mutex<HashMap<String, Vec<Instant>>>,
}

impl RecoveryAnnouncer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Atomically checks the rolling cap for `kind` AND, if under cap,
    /// records this fire — in one critical section. Splitting this into a
    /// separate check-then-record pair (as an earlier version did) lets two
    /// concurrent callers both observe "under cap" before either records its
    /// fire, so both proceed and the cap is silently exceeded by however many
    /// callers race the same window. Returns `true` if the action is held
    /// (over cap); a held action does NOT record a fire, matching the
    /// original record-fire-only-when-not-held behavior.
    fn check_and_record(&self, kind: &str, cap: &RateCap) -> bool {
        if cap.max == 0 {
            return false;
        }
        let window = jittered_window(cap.window);
        let now = Instant::now();
        let mut fires = self.fires.lock().unwrap_or_else(|p| p.into_inner());
        let entry = fires.entry(kind.to_string()).or_default();
        entry.retain(|t| now.duration_since(*t) < window);
        let held = entry.len() as u32 >= cap.max;
        if !held {
            entry.push(now);
        }
        held
    }

    /// THE shared entry point for an automated recovery action: durably
    /// records it, then fans it out through `sinks` — exactly the "announce
    /// mode" acceptance criterion (emit event tuple + inbox row + sink
    /// fan-out, before/as the action happens). Never silent: even a
    /// rate-capped action still writes and announces, escalating INSTEAD of
    /// acting, so a held action is exactly as visible as a performed one.
    ///
    /// `rk inbox` surfaces the written event as a `recovery-action` row via
    /// `inbox::recovery_action_rows` (pure read-side aggregation, same as
    /// every other inbox source — no second write happens here).
    pub fn announce(
        &self,
        space: &Space,
        sinks: &SinkRegistry,
        action: RecoveryAction,
        cap: RateCap,
    ) -> rk_core::Result<AnnounceOutcome> {
        let held = self.check_and_record(&action.kind, &cap);
        let mut notice = action.notice;
        if held {
            notice.severity = Severity::Critical;
            notice.text = format!(
                "{} (rate cap hit: {}/{}s for `{}` — action HELD, not performed)",
                notice.text,
                cap.max,
                cap.window.as_secs(),
                action.kind
            );
        }
        let mut tuple = Tuple::new(
            Category::Event,
            notice.scope.clone(),
            RECOVERY_ACTION_IDENTITY,
            action.instance,
            Value::Null,
        )
        // Permanent ledger entry, not `in`-consumable — the same reasoning
        // `space.withdraw` uses for a closed ballot: this record must not
        // silently evaporate out from under the re-notify sweep or the ack
        // it is waiting for.
        .with_lifecycle(Lifecycle::Furniture);
        notice.tuple_id = tuple.id.to_string();
        tuple.payload = json!({
            "action_kind": action.kind,
            "held": held,
            "notice": notice,
        });
        space.out(tuple)?;
        sinks.fan_out(&notice, &SpaceDedup { space });
        let event_id = notice.tuple_id;
        Ok(if held {
            AnnounceOutcome::RateHeld { event_id }
        } else {
            AnnounceOutcome::Announced { event_id }
        })
    }
}

/// The re-notify cadence: `first` after the escalation is written, then every
/// `repeat`, for up to `max` re-notifies total.
pub struct RenotifySchedule {
    pub first: Duration,
    pub repeat: Duration,
    pub max: u32,
}

impl RenotifySchedule {
    /// When the `attempt`-th re-notify (1-indexed) is due.
    fn due_at(&self, created: DateTime<Utc>, attempt: u32) -> DateTime<Utc> {
        let delay = self.first + self.repeat * attempt.saturating_sub(1);
        created
            + chrono::Duration::from_std(delay)
                .unwrap_or_else(|_| chrono::Duration::seconds(i64::MAX))
    }
}

fn acked_ids(space: &Space) -> rk_core::Result<HashSet<String>> {
    Ok(space
        .scan(&Pattern::category(Category::Event).identity(INBOX_ACK_IDENTITY))?
        .iter()
        .filter_map(|t| t.payload.get("tuple").and_then(Value::as_str))
        .map(String::from)
        .collect())
}

fn renotify_count(space: &Space, event_id: &str) -> rk_core::Result<u32> {
    let mut pattern = Pattern::category(Category::Event).identity(RENOTIFY_MARKER_IDENTITY);
    pattern.payload_search = Some(format!("\"tuple\":\"{event_id}\""));
    Ok(space.scan(&pattern)?.len() as u32)
}

fn extract_notice(action: &Tuple) -> Option<EscalationNotice> {
    serde_json::from_value(action.payload.get("notice")?.clone()).ok()
}

/// Re-push every unacked `recovery_action` escalation whose next scheduled
/// re-notify is due. Acked escalations are skipped outright: acking is what
/// stops the schedule, permanently. An escalation that has used up
/// `schedule.max` re-notifies is left standing as a passive `rk inbox` row —
/// no more pushes, same as a dead sink degrading to the passive queue (B1).
///
/// Idempotent and safe at any sweep cadence: each due attempt is durably
/// marked (`recovery_renotify`) as part of firing it, so a sweep that runs
/// again before the next attempt is due does nothing for this escalation, and
/// a sweep that runs late simply catches up to whichever attempt is now due
/// (it does not replay the ones it missed — the schedule names ATTEMPTS, not
/// wall-clock deadlines to hit exactly).
pub fn renotify_sweep(
    space: &Space,
    sinks: &SinkRegistry,
    schedule: &RenotifySchedule,
    now: DateTime<Utc>,
) -> rk_core::Result<usize> {
    let actions =
        space.scan(&Pattern::category(Category::Event).identity(RECOVERY_ACTION_IDENTITY))?;
    if actions.is_empty() {
        return Ok(0);
    }
    let acked = acked_ids(space)?;
    let dedup = SpaceDedup { space };
    let mut pushed = 0usize;
    for action in &actions {
        let event_id = action.id.to_string();
        if acked.contains(&event_id) {
            continue;
        }
        let Some(mut notice) = extract_notice(action) else {
            continue;
        };
        let attempts = renotify_count(space, &event_id)?;
        if attempts >= schedule.max {
            continue;
        }
        let attempt = attempts + 1;
        if now < schedule.due_at(action.created_at, attempt) {
            continue;
        }
        // A distinct tuple_id per attempt so the per-(notice, sink) delivery
        // marker cannot collide with the original push or an earlier attempt
        // — each attempt gets its own real delivery, not a suppressed repeat.
        notice.tuple_id = format!("{event_id}@renotify{attempt}");
        sinks.fan_out(&notice, &dedup);
        space.out(
            Tuple::new(
                Category::Event,
                action.scope.clone(),
                RENOTIFY_MARKER_IDENTITY,
                RECOVERY_MARKER_INSTANCE,
                json!({"tuple": event_id, "attempt": attempt}),
            )
            .with_lifecycle(Lifecycle::Furniture),
        )?;
        pushed += 1;
    }
    Ok(pushed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rk_core::config::SinkConfig;
    use std::sync::Mutex as StdMutex;

    struct Recorder(std::sync::Arc<StdMutex<Vec<String>>>);

    impl rk_core::notify::NotificationSink for Recorder {
        fn kind(&self) -> &str {
            "recorder"
        }
        fn deliver(&self, notice: &EscalationNotice) -> rk_core::Result<()> {
            self.0.lock().unwrap().push(notice.tuple_id.clone());
            Ok(())
        }
    }

    fn registry() -> (SinkRegistry, std::sync::Arc<StdMutex<Vec<String>>>) {
        let seen = std::sync::Arc::new(StdMutex::new(Vec::new()));
        let mut registry = SinkRegistry::new();
        registry.register(
            SinkConfig::of_kind("recorder"),
            Box::new(Recorder(seen.clone())),
        );
        (registry, seen)
    }

    fn notice(scope: &str, subject: &str) -> EscalationNotice {
        EscalationNotice::new(
            "placeholder",
            "recovery-test",
            Severity::Warn,
            scope,
            subject,
            "something automated happened",
        )
    }

    #[test]
    fn scale_window_applies_the_fraction() {
        let base = Duration::from_secs(3600);
        assert_eq!(scale_window(base, 0.0), base);
        assert_eq!(scale_window(base, 0.1), Duration::from_secs(3960));
        assert_eq!(scale_window(base, -0.1), Duration::from_secs(3240));
    }

    #[test]
    fn jitter_always_stays_within_ten_percent() {
        let base = Duration::from_secs(1000);
        for _ in 0..500 {
            let jittered = jittered_window(base);
            assert!(jittered >= Duration::from_secs(900), "{jittered:?}");
            assert!(jittered <= Duration::from_secs(1100), "{jittered:?}");
        }
    }

    #[test]
    fn announce_writes_a_durable_event_and_fans_out() {
        let space = Space::open_in_memory().unwrap();
        let (sinks, recorder) = registry();
        let announcer = RecoveryAnnouncer::new();
        let outcome = announcer
            .announce(
                &space,
                &sinks,
                RecoveryAction {
                    kind: "respawn".into(),
                    instance: "supervisor".into(),
                    notice: notice("myrepo", "Whisker"),
                },
                RateCap::unlimited(),
            )
            .unwrap();
        assert!(!outcome.held());
        let written = space
            .scan(&Pattern::category(Category::Event).identity(RECOVERY_ACTION_IDENTITY))
            .unwrap();
        assert_eq!(written.len(), 1);
        assert_eq!(written[0].id.to_string(), outcome.event_id());
        assert_eq!(written[0].payload["action_kind"], "respawn");
        assert_eq!(written[0].payload["held"], false);
        assert_eq!(
            *recorder.lock().unwrap(),
            vec![outcome.event_id().to_string()]
        );
    }

    #[test]
    fn a_rate_capped_action_is_held_but_still_announced() {
        let space = Space::open_in_memory().unwrap();
        let (sinks, recorder) = registry();
        let announcer = RecoveryAnnouncer::new();
        let cap = RateCap::per_hour(2);
        for i in 0..2 {
            let outcome = announcer
                .announce(
                    &space,
                    &sinks,
                    RecoveryAction {
                        kind: "respawn".into(),
                        instance: "supervisor".into(),
                        notice: notice("myrepo", &format!("rat-{i}")),
                    },
                    cap,
                )
                .unwrap();
            assert!(!outcome.held(), "the first two must be allowed");
        }
        let held = announcer
            .announce(
                &space,
                &sinks,
                RecoveryAction {
                    kind: "respawn".into(),
                    instance: "supervisor".into(),
                    notice: notice("myrepo", "rat-3"),
                },
                cap,
            )
            .unwrap();
        assert!(held.held(), "the third within the window must be held");
        // A different kind has its own bucket and is unaffected.
        let other_kind = announcer
            .announce(
                &space,
                &sinks,
                RecoveryAction {
                    kind: "kill-process-group".into(),
                    instance: "supervisor".into(),
                    notice: notice("myrepo", "rat-4"),
                },
                cap,
            )
            .unwrap();
        assert!(!other_kind.held());
        // All four were announced (fanned out), including the held one.
        assert_eq!(recorder.lock().unwrap().len(), 4);
        let events = space
            .scan(&Pattern::category(Category::Event).identity(RECOVERY_ACTION_IDENTITY))
            .unwrap();
        let capped: Vec<&Tuple> = events
            .iter()
            .filter(|t| t.payload["held"] == true)
            .collect();
        assert_eq!(capped.len(), 1);
        assert!(capped[0].payload["notice"]["text"]
            .as_str()
            .unwrap()
            .contains("rate cap hit"));
    }

    #[test]
    fn concurrent_announces_cannot_exceed_the_cap() {
        // Regression for a race where `rate_limited` (check) and
        // `record_fire` (record) were separate critical sections: many
        // threads could all observe "under cap" before any of them recorded
        // a fire, letting the cap be exceeded. `check_and_record` closes that
        // window by doing both under one lock. Set up one remaining slot
        // (cap of N, N-1 already spent) and have N threads race it — exactly
        // one must win.
        let space = Space::open_in_memory().unwrap();
        let (sinks, recorder) = registry();
        let sinks = std::sync::Arc::new(sinks);
        let announcer = std::sync::Arc::new(RecoveryAnnouncer::new());
        let cap = RateCap::per_hour(6);

        for i in 0..5 {
            let outcome = announcer
                .announce(
                    &space,
                    &sinks,
                    RecoveryAction {
                        kind: "respawn".into(),
                        instance: "supervisor".into(),
                        notice: notice("myrepo", &format!("warmup-{i}")),
                    },
                    cap,
                )
                .unwrap();
            assert!(!outcome.held(), "warmup fires must not be held");
        }
        recorder.lock().unwrap().clear();

        const RACERS: usize = 16;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(RACERS));
        let handles: Vec<_> = (0..RACERS)
            .map(|i| {
                let announcer = announcer.clone();
                let space = space.clone();
                let sinks = sinks.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    announcer
                        .announce(
                            &space,
                            &sinks,
                            RecoveryAction {
                                kind: "respawn".into(),
                                instance: "supervisor".into(),
                                notice: notice("myrepo", &format!("racer-{i}")),
                            },
                            cap,
                        )
                        .unwrap()
                })
            })
            .collect();
        let outcomes: Vec<AnnounceOutcome> =
            handles.into_iter().map(|h| h.join().unwrap()).collect();

        let winners = outcomes.iter().filter(|o| !o.held()).count();
        assert_eq!(winners, 1, "exactly one racer must win the remaining slot");
        assert_eq!(outcomes.len() - winners, RACERS - 1);
    }

    #[test]
    fn renotify_sweep_follows_the_4h_then_24h_schedule_and_stops_at_max() {
        let space = Space::open_in_memory().unwrap();
        let (sinks, recorder) = registry();
        let announcer = RecoveryAnnouncer::new();
        let outcome = announcer
            .announce(
                &space,
                &sinks,
                RecoveryAction {
                    kind: "instance-timeout".into(),
                    instance: "reactor".into(),
                    notice: notice("myrepo", "wf-stuck"),
                },
                RateCap::unlimited(),
            )
            .unwrap();
        recorder.lock().unwrap().clear();

        let created = space
            .get(outcome.event_id().parse().unwrap())
            .unwrap()
            .unwrap()
            .created_at;
        let schedule = RenotifySchedule {
            first: Duration::from_secs(4 * 3600),
            repeat: Duration::from_secs(24 * 3600),
            max: 3,
        };

        // Before 4h: nothing due.
        let pushed = renotify_sweep(
            &space,
            &sinks,
            &schedule,
            created + chrono::Duration::hours(1),
        )
        .unwrap();
        assert_eq!(pushed, 0);
        assert!(recorder.lock().unwrap().is_empty());

        // At 4h: first re-notify.
        let pushed = renotify_sweep(
            &space,
            &sinks,
            &schedule,
            created + chrono::Duration::hours(4),
        )
        .unwrap();
        assert_eq!(pushed, 1);
        assert_eq!(recorder.lock().unwrap().len(), 1);

        // Re-running the sweep at the same instant does not double-push.
        let pushed = renotify_sweep(
            &space,
            &sinks,
            &schedule,
            created + chrono::Duration::hours(4),
        )
        .unwrap();
        assert_eq!(pushed, 0);
        assert_eq!(recorder.lock().unwrap().len(), 1);

        // At 28h (4h + 24h): second re-notify.
        let pushed = renotify_sweep(
            &space,
            &sinks,
            &schedule,
            created + chrono::Duration::hours(28),
        )
        .unwrap();
        assert_eq!(pushed, 1);
        assert_eq!(recorder.lock().unwrap().len(), 2);

        // At 52h (4h + 2*24h): third and last re-notify (max = 3).
        let pushed = renotify_sweep(
            &space,
            &sinks,
            &schedule,
            created + chrono::Duration::hours(52),
        )
        .unwrap();
        assert_eq!(pushed, 1);
        assert_eq!(recorder.lock().unwrap().len(), 3);

        // At 76h: max already reached, no further push, ever.
        let pushed = renotify_sweep(
            &space,
            &sinks,
            &schedule,
            created + chrono::Duration::hours(76),
        )
        .unwrap();
        assert_eq!(pushed, 0);
        assert_eq!(recorder.lock().unwrap().len(), 3);
    }

    #[test]
    fn an_acked_escalation_never_renotifies() {
        let space = Space::open_in_memory().unwrap();
        let (sinks, recorder) = registry();
        let announcer = RecoveryAnnouncer::new();
        let outcome = announcer
            .announce(
                &space,
                &sinks,
                RecoveryAction {
                    kind: "ticket-reopen".into(),
                    instance: "reactor".into(),
                    notice: notice("myrepo", "TKT-9"),
                },
                RateCap::unlimited(),
            )
            .unwrap();
        recorder.lock().unwrap().clear();

        space
            .out(Tuple::new(
                Category::Event,
                "myrepo",
                INBOX_ACK_IDENTITY,
                "operator",
                json!({"tuple": outcome.event_id(), "acked_by": "operator"}),
            ))
            .unwrap();

        let created = space
            .get(outcome.event_id().parse().unwrap())
            .unwrap()
            .unwrap()
            .created_at;
        let schedule = RenotifySchedule {
            first: Duration::from_secs(1),
            repeat: Duration::from_secs(1),
            max: 3,
        };
        let pushed = renotify_sweep(
            &space,
            &sinks,
            &schedule,
            created + chrono::Duration::hours(200),
        )
        .unwrap();
        assert_eq!(pushed, 0);
        assert!(recorder.lock().unwrap().is_empty());
    }
}

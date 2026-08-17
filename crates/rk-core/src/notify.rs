//! Pluggable escalation notification: one notice type, one sink trait, one
//! fan-out.
//!
//! Before this, "push the operator" meant exactly one hardwired call —
//! `reactor.rs` → `rk_mux::HerdrMux::notify`. That worked, but it made the
//! delivery channel a property of the *call site*: a second channel (a phone
//! push, a chat webhook, a future rat-king that acts on the escalation via
//! ordinary `rk` commands) could only be added by editing every escalation
//! source.
//!
//! The shape here inverts that. An escalation source builds an
//! [`EscalationNotice`] — a channel-agnostic description of *what happened* —
//! and hands it to [`SinkRegistry::fan_out`]. Which channels see it is decided
//! by config ([`crate::config::NotifyConfig`], `[[notify.sinks]]`), not by the
//! caller. Adding a channel is implementing [`NotificationSink`] and naming a
//! `kind` in config.
//!
//! Two invariants make this safe to put on the escalation path:
//!
//! * **Best-effort.** A sink that errors, hangs its shell-out, or is simply not
//!   installed produces a logged [`Delivery`] failure and nothing else. The
//!   escalation is already durable in the tuplespace and already ranked by
//!   `rk inbox` before any sink runs, so a dead sink degrades to the passive
//!   queue — it never blocks recording, and never aborts the reactor cycle.
//! * **Deduped per (notice, sink).** Delivery is at-least-once by nature (the
//!   reactor re-scans, cursors can be lost), so the caller supplies a
//!   [`SinkDedup`] backed by whatever durable marker it already keeps. The
//!   marker is written per ATTEMPT rather than per success — see
//!   [`SinkRegistry::deliver_one`] for why a dead channel must not mean an
//!   unbounded retry.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tracing::{debug, warn};

/// How loud a notice is. Ordered, so a sink can filter with `min_severity`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// Worth seeing, not worth interrupting for.
    #[default]
    Info,
    /// Something is degraded and a human should look soon.
    Warn,
    /// Blocked on a human decision right now.
    Critical,
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Severity::Info => "info",
            Severity::Warn => "warn",
            Severity::Critical => "critical",
        })
    }
}

/// A channel-agnostic escalation, built by the source and rendered by each sink.
///
/// `class` is the routing key config filters on (`classes = [...]`) and doubles
/// as the human label: a kebab-case class renders as spaced words, so
/// `steward-escalation` titles as "steward escalation — <subject>".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EscalationNotice {
    /// The tuple that caused this, as a string id. Also the dedup key.
    pub tuple_id: String,
    /// Routing key + label, kebab-case (e.g. `steward-escalation`).
    pub class: String,
    pub severity: Severity,
    /// Tuple scope the escalation belongs to (repo name, or `system`).
    pub scope: String,
    /// What it is about — a task id, a branch, a subject line.
    pub subject: String,
    /// The human-readable body.
    pub text: String,
    /// What the operator should do about it, if the source knows.
    pub suggested_action: Option<String>,
    /// Machine-readable context (branch, instance, workflow, ticket, …). Sinks
    /// that can carry structure use it; sinks that cannot ignore it.
    pub refs: BTreeMap<String, String>,
}

impl EscalationNotice {
    /// A notice with no action and no refs — the common case.
    pub fn new(
        tuple_id: impl Into<String>,
        class: impl Into<String>,
        severity: Severity,
        scope: impl Into<String>,
        subject: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self {
            tuple_id: tuple_id.into(),
            class: class.into(),
            severity,
            scope: scope.into(),
            subject: subject.into(),
            text: text.into(),
            suggested_action: None,
            refs: BTreeMap::new(),
        }
    }

    pub fn with_action(mut self, action: impl Into<String>) -> Self {
        self.suggested_action = Some(action.into());
        self
    }

    pub fn with_ref(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.refs.insert(key.into(), value.into());
        self
    }

    /// The one-line headline a push channel shows: the class as spaced words,
    /// an em dash, the subject. Kept here rather than in a sink so every
    /// channel titles an escalation the same way.
    pub fn title(&self) -> String {
        format!("{} — {}", self.class.replace('-', " "), self.subject)
    }

    /// The body a push channel shows: the text, plus the suggested action on
    /// its own line when the source supplied one.
    pub fn body(&self) -> String {
        match &self.suggested_action {
            Some(action) => format!("{}\n→ {action}", self.text),
            None => self.text.clone(),
        }
    }
}

/// One delivery channel. Implementations shell out, POST, write a file — the
/// registry only requires that they name themselves and report failure rather
/// than panicking or blocking indefinitely.
pub trait NotificationSink: Send + Sync {
    /// The `kind` this sink implements, used to match `[[notify.sinks]]`.
    fn kind(&self) -> &str;

    /// Deliver, or explain why not. An `Err` is logged and recorded on the
    /// [`Delivery`]; it never propagates to the escalation source.
    fn deliver(&self, notice: &EscalationNotice) -> crate::Result<()>;
}

/// Durable "already pushed this notice on this sink" memory, supplied by the
/// caller so the registry stays free of storage concerns. The reactor backs
/// this with its existing `reactor:marker` tuples.
pub trait SinkDedup {
    fn already_delivered(&self, notice: &EscalationNotice, sink: &str) -> bool;
    /// Called only after a *successful* deliver.
    fn record_delivered(&self, notice: &EscalationNotice, sink: &str) -> crate::Result<()>;
}

/// Dedup that remembers nothing — for one-shot callers and tests.
pub struct NoDedup;

impl SinkDedup for NoDedup {
    fn already_delivered(&self, _notice: &EscalationNotice, _sink: &str) -> bool {
        false
    }
    fn record_delivered(&self, _notice: &EscalationNotice, _sink: &str) -> crate::Result<()> {
        Ok(())
    }
}

/// What one sink did with one notice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    /// The configured sink name (not the kind — several sinks may share a kind).
    pub sink: String,
    pub outcome: Outcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Pushed, and the dedup marker was written.
    Delivered,
    /// Suppressed by the sink's class/severity filter or its `enabled = false`.
    Filtered,
    /// A marker says this sink already pushed this notice.
    AlreadyDelivered,
    /// The sink (or the marker write) failed. Logged, not fatal.
    Failed(String),
}

impl Outcome {
    pub fn delivered(&self) -> bool {
        matches!(self, Outcome::Delivered)
    }
}

/// A sink plus the config that decides which notices reach it.
struct Registered {
    config: crate::config::SinkConfig,
    sink: Box<dyn NotificationSink>,
}

/// The configured set of channels, and the single fan-out every escalation
/// source goes through.
#[derive(Default)]
pub struct SinkRegistry {
    sinks: Vec<Registered>,
}

impl SinkRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `sink` under `config`. The caller is responsible for matching
    /// `config.kind` to the implementation; [`SinkRegistry::build`] does that
    /// from a factory.
    pub fn register(&mut self, config: crate::config::SinkConfig, sink: Box<dyn NotificationSink>) {
        self.sinks.push(Registered { config, sink });
    }

    /// Build a registry from resolved config, asking `factory` for an
    /// implementation per `kind`. An unknown kind is warned about and skipped —
    /// a typo in config must not take the daemon down, and the escalation is
    /// still on the inbox.
    pub fn build(
        configs: impl IntoIterator<Item = crate::config::SinkConfig>,
        mut factory: impl FnMut(&str) -> Option<Box<dyn NotificationSink>>,
    ) -> Self {
        let mut registry = Self::new();
        for config in configs {
            match factory(&config.kind) {
                Some(sink) => registry.register(config, sink),
                None => warn!(
                    sink = %config.name(),
                    kind = %config.kind,
                    "unknown notification sink kind; skipping"
                ),
            }
        }
        registry
    }

    pub fn is_empty(&self) -> bool {
        self.sinks.is_empty()
    }

    pub fn len(&self) -> usize {
        self.sinks.len()
    }

    /// Names of the configured sinks, in registration order.
    pub fn names(&self) -> Vec<&str> {
        self.sinks.iter().map(|s| s.config.name()).collect()
    }

    /// THE fan-out. Every escalation source calls this and nothing else.
    ///
    /// Each accepting sink is tried independently: one sink's failure neither
    /// suppresses another's delivery nor surfaces as an error to the caller.
    /// The returned [`Delivery`] list is for logging and tests — a caller that
    /// does not care can drop it.
    pub fn fan_out(&self, notice: &EscalationNotice, dedup: &dyn SinkDedup) -> Vec<Delivery> {
        self.sinks
            .iter()
            .map(|registered| Delivery {
                sink: registered.config.name().to_string(),
                outcome: self.deliver_one(registered, notice, dedup),
            })
            .collect()
    }

    fn deliver_one(
        &self,
        registered: &Registered,
        notice: &EscalationNotice,
        dedup: &dyn SinkDedup,
    ) -> Outcome {
        let name = registered.config.name();
        if !registered.config.accepts(notice) {
            return Outcome::Filtered;
        }
        if dedup.already_delivered(notice, name) {
            return Outcome::AlreadyDelivered;
        }
        let outcome = match registered.sink.deliver(notice) {
            Ok(()) => {
                debug!(sink = name, tuple = %notice.tuple_id, class = %notice.class, "notification delivered");
                Outcome::Delivered
            }
            Err(e) => {
                // Best-effort: the operator still has the inbox row.
                warn!(sink = name, tuple = %notice.tuple_id, error = %e, "notification sink failed");
                Outcome::Failed(e.to_string())
            }
        };
        // Marked on ATTEMPT, not on success. A castle with no channel installed
        // (the headless case, and what an absent herdr server has always looked
        // like) would otherwise re-attempt this notice on every re-scan forever.
        // The escalation is durable and already ranked by `rk inbox` either way,
        // so one logged failure is the honest cost of a dead channel — not an
        // unbounded retry. A channel meant to survive outages should retry
        // inside its own `deliver`, where it can bound the attempts.
        if let Err(e) = dedup.record_delivered(notice, name) {
            warn!(sink = name, tuple = %notice.tuple_id, error = %e, "notification dedup marker failed");
        }
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SinkConfig;
    use std::sync::Mutex;

    fn notice() -> EscalationNotice {
        EscalationNotice::new(
            "01AAA",
            "steward-escalation",
            Severity::Critical,
            "myrepo",
            "TKT-7",
            "needs a human merge decision",
        )
    }

    #[derive(Default)]
    struct Recorder {
        seen: Mutex<Vec<String>>,
        fail: bool,
    }

    impl NotificationSink for Recorder {
        fn kind(&self) -> &str {
            "recorder"
        }
        fn deliver(&self, notice: &EscalationNotice) -> crate::Result<()> {
            if self.fail {
                return Err(crate::Error::other("channel down"));
            }
            self.seen.lock().unwrap().push(notice.title());
            Ok(())
        }
    }

    #[test]
    fn title_and_body_render_the_class_as_words() {
        let n = notice();
        assert_eq!(n.title(), "steward escalation — TKT-7");
        assert_eq!(n.body(), "needs a human merge decision");
        assert_eq!(
            n.with_action("rk land myrepo").body(),
            "needs a human merge decision\n→ rk land myrepo"
        );
    }

    #[test]
    fn filters_gate_by_class_and_severity() {
        let mut registry = SinkRegistry::new();
        registry.register(
            SinkConfig {
                name: Some("matching".into()),
                classes: vec!["steward-escalation".into()],
                ..SinkConfig::of_kind("recorder")
            },
            Box::new(Recorder::default()),
        );
        registry.register(
            SinkConfig {
                name: Some("wrong-class".into()),
                classes: vec!["ci-failure".into()],
                ..SinkConfig::of_kind("recorder")
            },
            Box::new(Recorder::default()),
        );
        registry.register(
            SinkConfig {
                name: Some("disabled".into()),
                enabled: false,
                ..SinkConfig::of_kind("recorder")
            },
            Box::new(Recorder::default()),
        );

        let deliveries = registry.fan_out(&notice(), &NoDedup);
        let by_name = |n: &str| {
            deliveries
                .iter()
                .find(|d| d.sink == n)
                .unwrap()
                .outcome
                .clone()
        };
        assert_eq!(by_name("matching"), Outcome::Delivered);
        assert_eq!(by_name("wrong-class"), Outcome::Filtered);
        assert_eq!(by_name("disabled"), Outcome::Filtered);

        // An info notice is below a warn-and-up sink's floor.
        let mut quiet = SinkRegistry::new();
        quiet.register(
            SinkConfig {
                min_severity: Severity::Warn,
                ..SinkConfig::of_kind("recorder")
            },
            Box::new(Recorder::default()),
        );
        let mut low = notice();
        low.severity = Severity::Info;
        assert_eq!(quiet.fan_out(&low, &NoDedup)[0].outcome, Outcome::Filtered);
        assert!(quiet.fan_out(&notice(), &NoDedup)[0].outcome.delivered());
    }

    #[test]
    fn a_dead_sink_does_not_block_a_live_one() {
        struct Marks(Mutex<Vec<String>>);
        impl SinkDedup for Marks {
            fn already_delivered(&self, _n: &EscalationNotice, sink: &str) -> bool {
                self.0.lock().unwrap().iter().any(|m| m == sink)
            }
            fn record_delivered(&self, _n: &EscalationNotice, sink: &str) -> crate::Result<()> {
                self.0.lock().unwrap().push(sink.to_string());
                Ok(())
            }
        }

        let mut registry = SinkRegistry::new();
        registry.register(
            SinkConfig {
                name: Some("dead".into()),
                ..SinkConfig::of_kind("recorder")
            },
            Box::new(Recorder {
                fail: true,
                ..Default::default()
            }),
        );
        registry.register(
            SinkConfig {
                name: Some("live".into()),
                ..SinkConfig::of_kind("recorder")
            },
            Box::new(Recorder::default()),
        );

        let marks = Marks(Mutex::new(Vec::new()));
        let deliveries = registry.fan_out(&notice(), &marks);
        assert!(matches!(deliveries[0].outcome, Outcome::Failed(_)));
        assert_eq!(deliveries[1].outcome, Outcome::Delivered);
        assert_eq!(
            *marks.0.lock().unwrap(),
            ["dead", "live"],
            "both are marked on attempt, so a missing channel is not retried forever"
        );

        // Second pass: both are suppressed by their markers.
        let deliveries = registry.fan_out(&notice(), &marks);
        assert_eq!(deliveries[0].outcome, Outcome::AlreadyDelivered);
        assert_eq!(deliveries[1].outcome, Outcome::AlreadyDelivered);
    }

    #[test]
    fn an_unknown_kind_is_skipped_not_fatal() {
        let registry = SinkRegistry::build(
            [SinkConfig::of_kind("recorder"), SinkConfig::of_kind("carrier-pigeon")],
            |kind| match kind {
                "recorder" => Some(Box::new(Recorder::default()) as Box<dyn NotificationSink>),
                _ => None,
            },
        );
        assert_eq!(registry.names(), ["recorder"]);
    }
}

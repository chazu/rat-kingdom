//! The tuple model: rat-kingdom's single coordination substrate.
//!
//! Tuples have a fixed structural prefix `(category, scope, identity, instance)`
//! plus a JSON payload. The category encodes epistemic weight (a daemon-verified
//! `Fact` outranks an agent's `Claim`); the lifecycle class encodes hygiene
//! (who may write it, whether `in` may consume it, and when it is collected).

use crate::id::RecordId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Tuple categories, ordered roughly by epistemic weight.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Category {
    /// Daemon-verified ground truth (repo metadata, CI status, token usage).
    Fact,
    /// Shared norms, promoted from proposals at quorum.
    Convention,
    /// A unit of work.
    Task,
    /// A claimable unit of work; consumed atomically via `in`.
    Available,
    /// An agent's advisory assertion that it is working on something.
    Claim,
    /// Something blocking progress.
    Obstacle,
    /// A request to the room, not directed at anyone.
    Need,
    /// A work product (patch, test results, design decision).
    Artifact,
    /// A record of something that happened.
    Event,
    /// A directed message for a specific agent or human.
    Message,
    /// A proposed improvement to rat-kingdom itself (system scope).
    Suggestion,
    /// A vote of support for a suggestion (one per agent, idempotent).
    Endorsement,
}

impl Category {
    pub fn as_str(&self) -> &'static str {
        match self {
            Category::Fact => "fact",
            Category::Convention => "convention",
            Category::Task => "task",
            Category::Available => "available",
            Category::Claim => "claim",
            Category::Obstacle => "obstacle",
            Category::Need => "need",
            Category::Artifact => "artifact",
            Category::Event => "event",
            Category::Message => "message",
            Category::Suggestion => "suggestion",
            Category::Endorsement => "endorsement",
        }
    }

    pub const ALL: [Category; 12] = [
        Category::Fact,
        Category::Convention,
        Category::Task,
        Category::Available,
        Category::Claim,
        Category::Obstacle,
        Category::Need,
        Category::Artifact,
        Category::Event,
        Category::Message,
        Category::Suggestion,
        Category::Endorsement,
    ];

    /// Pheromone trails: agent assertions that must be *refreshed* to stay
    /// alive. They carry a decaying [`Tuple::strength`] and are collected once it
    /// reaches zero (see [`crate::tuple`] docs and the space GC). A still-active
    /// rat re-issuing the same trail reinforces it back to full strength; an
    /// abandoned one (its author dead) evaporates on its own.
    pub fn evaporates(&self) -> bool {
        matches!(self, Category::Claim | Category::Obstacle | Category::Need)
    }
}

/// Strength a freshly written or reinforced pheromone trail starts at. Each GC
/// cycle decays it; reinforcement resets it to this. Ranking consumers
/// (hot-scans, obstacle-coalesce) read [`Tuple::strength`] as the raw signal.
pub const FULL_STRENGTH: f64 = 1.0;

impl std::str::FromStr for Category {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Category::ALL
            .iter()
            .find(|c| c.as_str() == s)
            .copied()
            .ok_or_else(|| crate::Error::InvalidTuple(format!("unknown category: {s}")))
    }
}

/// Lifecycle classes control hygiene: who writes, whether `in` may consume,
/// and how the tuple is collected.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum Lifecycle {
    /// Daemon-only, permanent, refreshed from reality. `in` is rejected.
    Furniture,
    /// Agent-written, lives for the duration of its parent task, then archived.
    #[default]
    Session,
    /// Consumable via `in`; TTL-collected if unclaimed.
    Ephemeral,
}

/// The well-known scope preloaded into every agent's context.
pub const SYSTEM_SCOPE: &str = "system";

/// A tuple in the space.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Tuple {
    /// Unique, time-sortable record id.
    pub id: RecordId,
    pub category: Category,
    /// Isolation boundary — usually a repo name, or [`SYSTEM_SCOPE`].
    pub scope: String,
    /// What this tuple is about (task id, agent name, event kind, ...).
    pub identity: String,
    /// Originating actor (castle/agent). Defaults to the local castle name.
    pub instance: String,
    #[serde(default)]
    pub lifecycle: Lifecycle,
    /// Free-form JSON payload; validated against per-category schemas at write
    /// time by the sugar layer.
    #[serde(default)]
    pub payload: Value,
    pub created_at: DateTime<Utc>,
    /// Ephemeral tuples only: collected after this instant if unconsumed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
    /// Pheromone strength for [`Category::evaporates`] trails: [`FULL_STRENGTH`]
    /// when fresh, decayed each GC cycle, reset on reinforcement, collected at
    /// zero. `None` for tuples that do not evaporate. Carried through reads so
    /// ranking consumers can weight by how live a trail is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strength: Option<f64>,
}

impl Tuple {
    pub fn new(
        category: Category,
        scope: impl Into<String>,
        identity: impl Into<String>,
        instance: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self {
            id: RecordId::new(),
            category,
            scope: scope.into(),
            identity: identity.into(),
            instance: instance.into(),
            lifecycle: Lifecycle::default(),
            payload,
            created_at: Utc::now(),
            expires_at: None,
            strength: None,
        }
    }

    pub fn with_lifecycle(mut self, lifecycle: Lifecycle) -> Self {
        self.lifecycle = lifecycle;
        self
    }
}

/// A match pattern for `in`/`rd`/`scan`. Empty/None fields match anything.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Pattern {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<Category>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,
    /// Substring/FTS search over the serialized payload.
    ///
    /// INVARIANT (the predecessor's lesson): every code path that decides whether a tuple
    /// matches a waiting reader MUST use [`Pattern::matches`], which includes
    /// this field. There is no "cheap" prefix-only match anywhere.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_search: Option<String>,
}

impl Pattern {
    pub fn category(category: Category) -> Self {
        Self {
            category: Some(category),
            ..Default::default()
        }
    }

    pub fn scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(scope.into());
        self
    }

    pub fn identity(mut self, identity: impl Into<String>) -> Self {
        self.identity = Some(identity.into());
        self
    }

    /// The single authoritative match predicate. Both the storage query and the
    /// waiter wake path must agree with this exactly.
    pub fn matches(&self, tuple: &Tuple) -> bool {
        if let Some(c) = self.category {
            if tuple.category != c {
                return false;
            }
        }
        if let Some(s) = &self.scope {
            if &tuple.scope != s {
                return false;
            }
        }
        if let Some(i) = &self.identity {
            if &tuple.identity != i {
                return false;
            }
        }
        if let Some(inst) = &self.instance {
            if &tuple.instance != inst {
                return false;
            }
        }
        if let Some(search) = &self.payload_search {
            let hay = tuple.payload.to_string();
            if !hay.contains(search.as_str()) {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn t() -> Tuple {
        Tuple::new(
            Category::Event,
            "myrepo",
            "task_done",
            "castle-a",
            json!({"agent": "Whisker", "task": ".rk-1"}),
        )
    }

    #[test]
    fn empty_pattern_matches_everything() {
        assert!(Pattern::default().matches(&t()));
    }

    #[test]
    fn pattern_matches_on_all_axes() {
        let p = Pattern::category(Category::Event)
            .scope("myrepo")
            .identity("task_done");
        assert!(p.matches(&t()));
        assert!(!Pattern::category(Category::Fact).matches(&t()));
        assert!(!Pattern::default().scope("other").matches(&t()));
    }

    fn pattern_scope(scope: &str) -> Pattern {
        Pattern {
            scope: Some(scope.into()),
            ..Default::default()
        }
    }

    #[test]
    fn payload_search_is_part_of_the_predicate() {
        let mut p = pattern_scope("myrepo");
        p.payload_search = Some("Whisker".into());
        assert!(p.matches(&t()));
        p.payload_search = Some("Nibbles".into());
        assert!(!p.matches(&t()));
    }

    #[test]
    fn category_round_trips_through_str() {
        for c in Category::ALL {
            assert_eq!(c.as_str().parse::<Category>().unwrap(), c);
        }
    }

    #[test]
    fn tuple_serde_round_trip() {
        let tuple = t();
        let s = serde_json::to_string(&tuple).unwrap();
        let back: Tuple = serde_json::from_str(&s).unwrap();
        assert_eq!(tuple, back);
    }

    #[test]
    fn strength_round_trips_and_is_omitted_when_absent() {
        let plain = t();
        assert_eq!(plain.strength, None);
        assert!(
            !serde_json::to_string(&plain).unwrap().contains("strength"),
            "absent strength is not serialized"
        );

        let mut trail = t();
        trail.strength = Some(FULL_STRENGTH);
        let s = serde_json::to_string(&trail).unwrap();
        assert!(s.contains("strength"));
        let back: Tuple = serde_json::from_str(&s).unwrap();
        assert_eq!(trail, back);
    }

    #[test]
    fn only_claim_obstacle_need_evaporate() {
        let evaporating = [Category::Claim, Category::Obstacle, Category::Need];
        for c in Category::ALL {
            assert_eq!(
                c.evaporates(),
                evaporating.contains(&c),
                "{c:?} evaporation flag"
            );
        }
    }
}

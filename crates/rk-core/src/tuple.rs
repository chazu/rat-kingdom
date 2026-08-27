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
    /// A decaying backlink from a resolved wall (topic) to the [`Artifact`] that
    /// solved it. Written by the reactor when an artifact `--resolves` an
    /// obstacle/need, reinforced whenever a rat hits the same wall again, and
    /// evaporated once nobody does — institutional memory as a living structure.
    Resolution,
    /// A record of something that happened.
    Event,
    /// A directed message for a specific agent or human.
    Message,
    /// A proposed improvement to rat-kingdom itself (system scope).
    Suggestion,
    /// A vote of support for a suggestion (one per agent, idempotent).
    Endorsement,
    /// An agent's up/down/clear vote on a fact, keyed by the fact id.
    FactVote,
    /// The explicit close of a losing ballot, keyed `identity = <sug-id>` —
    /// the counterpart to [`Category::Convention`] on the other outcome.
    ///
    /// Ballots stopped expiring in TKT-168 (a vote is a ledger entry, not a
    /// pheromone: nobody survives to reinforce it), which removed the silent
    /// clock that used to clear a proposal nobody backed. Something still has to
    /// close one, or `rk inbox` grows an `open-suggestion` row that never
    /// retires. Withdrawal is that act, and it is deliberately explicit: only
    /// the proposer or the operator may cast it, via `rk withdraw` (TKT-184).
    ///
    /// It does NOT delete the ballot. The `Suggestion` and every `Endorsement`
    /// on it stay exactly where they are and stay countable — dropping the votes
    /// would recreate the orphaned-endorsement hazard TKT-168 was written to
    /// avoid, and would discard the record of who backed the idea before it was
    /// pulled. The withdrawal is a separate tuple that renders those votes
    /// *inert*: the reactor refuses to promote a withdrawn ballot however many
    /// endorsers it later accumulates, and `rk inbox` stops raising its row.
    Withdrawal,
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
            Category::Resolution => "resolution",
            Category::Event => "event",
            Category::Message => "message",
            Category::Suggestion => "suggestion",
            Category::Endorsement => "endorsement",
            Category::FactVote => "fact_vote",
            Category::Withdrawal => "withdrawal",
        }
    }

    pub const ALL: [Category; 15] = [
        Category::Fact,
        Category::Convention,
        Category::Task,
        Category::Available,
        Category::Claim,
        Category::Obstacle,
        Category::Need,
        Category::Artifact,
        Category::Resolution,
        Category::Event,
        Category::Message,
        Category::Suggestion,
        Category::Endorsement,
        Category::FactVote,
        // Lowest weight, below the vote it closes: a withdrawal is bookkeeping
        // on a ballot that is over. It must never outrank a live trail in a
        // hot-scan — it is the one ballot tuple that steers nobody.
        Category::Withdrawal,
    ];

    /// Pheromone trails: agent assertions that must be *refreshed* to stay
    /// alive. They carry a decaying [`Tuple::strength`] and are collected once it
    /// reaches zero (see [`crate::tuple`] docs and the space GC). A still-active
    /// rat re-issuing the same trail reinforces it back to full strength; an
    /// abandoned one (its author dead) evaporates on its own.
    pub fn evaporates(&self) -> bool {
        matches!(
            self,
            Category::Claim | Category::Obstacle | Category::Need | Category::Resolution
        )
    }

    /// Epistemic weight for hot-ranking: higher = a stronger trail to follow.
    /// Derived from the declaration order of [`Category::ALL`] (which runs from
    /// `Fact` highest through `Endorsement` lowest), so ranking reuses the same
    /// weighting the enum already encodes rather than a second, drift-prone
    /// table. Read-only sugar for hot-scans (P7).
    pub fn weight(&self) -> f64 {
        let n = Self::ALL.len();
        let pos = Self::ALL.iter().position(|c| c == self).unwrap_or(n);
        (n - pos) as f64
    }
}

/// Strength a freshly written or reinforced pheromone trail starts at. Each GC
/// cycle decays it; reinforcement resets it to this. Ranking consumers
/// (hot-scans, obstacle-coalesce) read [`Tuple::strength`] as the raw signal.
pub const FULL_STRENGTH: f64 = 1.0;

/// Default lifetime backstop for an evaporating pheromone trail (claim /
/// obstacle / need / resolution) written without an explicit TTL: a trail never
/// reinforced is hard-collected after this long even if strength decay hasn't
/// reached zero. Shared by the RPC `out` boundary and daemon-internal writers
/// so every evaporating write ages on one clock (see [`Tuple::into_trail`]).
pub const DEFAULT_TRAIL_TTL: std::time::Duration = std::time::Duration::from_secs(30 * 60);
/// Maximum accepted TTL for a pheromone trail or RPC-authored ephemeral tuple.
/// Keeping this bounded avoids unchecked `u64` to `i64` duration conversions.
pub const MAX_TRAIL_TTL: std::time::Duration = std::time::Duration::from_secs(365 * 24 * 3600);

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

    /// Shape a freshly-built evaporating pheromone into a live trail: full
    /// strength, an Ephemeral lifetime, and a `ttl` backstop from now. This is
    /// the transform the RPC `out` boundary applies to CLI-authored
    /// claim/obstacle/need writes (see the daemon's `handle_out`); daemon-
    /// internal writers — the supervisor's budget/liveness obstacles and
    /// respawn-exhaustion need, the syncer's `sync_failure` obstacle — apply it
    /// too. Without it those writes default to [`Lifecycle::Session`] (durable),
    /// so rk-sync exports each as a durable `Out` that no `Take` ever drains,
    /// piling up in the notes log and re-importing on every peer forever. As an
    /// Ephemeral trail the write stays LOCAL (Ephemeral tuples never replicate)
    /// and evaporates on its own, while remaining visible to `rk inbox`, the
    /// reactor, and hot-scans for as long as the condition it reports persists.
    pub fn into_trail(mut self, ttl: std::time::Duration) -> Self {
        let ttl = ttl.min(MAX_TRAIL_TTL);
        self.strength = Some(FULL_STRENGTH);
        self.lifecycle = Lifecycle::Ephemeral;
        let duration =
            chrono::Duration::from_std(ttl).expect("MAX_TRAIL_TTL must fit chrono::Duration");
        self.expires_at = Utc::now().checked_add_signed(duration);
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
    /// Literal, case-sensitive substring search over the serialized payload.
    ///
    /// INVARIANT (the predecessor's lesson): every code path that decides whether a tuple
    /// matches a waiting reader MUST use [`Pattern::matches`], which includes
    /// this field. There is no "cheap" prefix-only match anywhere.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_search: Option<String>,
    /// A second literal substring, ANDed with `payload_search` rather than
    /// replacing it. Exists because the serialized payload is one JSON
    /// document and `payload_search` is one substring test — binding two
    /// independent fields (e.g. `branch` AND `head_sha`, [`Pattern::for_commit`])
    /// needs two separate `contains` checks, since the fields are not
    /// guaranteed to sit adjacent in the serialized document. `None` when a
    /// predicate only needs to bind one field, which remains the common case.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_search_and: Option<String>,
    /// Exclusive lower bound on `id`: match only tuples with `id > after_id`.
    /// Ids are ULIDs (chronologically sortable), so this is a "newer than"
    /// cursor. The storage query answers it from the `id` PRIMARY KEY index —
    /// the cheap way to scan just the tuples added since a cursor, instead of
    /// reading the whole store and discarding the old ones in Rust.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_id: Option<RecordId>,
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

    /// Restrict to tuples newer than `after` (exclusive). `None` leaves the
    /// pattern unbounded, so `pattern.after(cursor)` is a no-op on a fresh
    /// cursor and a bounded delta scan once one exists.
    pub fn after(mut self, after: Option<RecordId>) -> Self {
        self.after_id = after;
        self
    }

    /// The one predicate for "the tuple `<identity>` that THIS generation of
    /// agent wrote", keyed on [`crate::id::SpawnId`] instead of a name.
    ///
    /// This structurally retires name-and-time joins (TKT-146/TKT-159): a
    /// `SpawnId` is minted once per generation and never reused, so — like
    /// [`Self::for_workflow_instance`] — no `after_id` floor is needed to
    /// exclude a namesake predecessor. The key itself cannot collide.
    ///
    pub fn for_spawn(
        category: Category,
        identity: impl Into<String>,
        spawn: crate::id::SpawnId,
    ) -> Self {
        let mut pattern = Self::category(category).identity(identity);
        pattern.payload_search = Some(format!("\"spawn\":\"{spawn}\""));
        pattern
    }

    /// The one predicate for "the tuple that names workflow instance
    /// `<instance_id>` in its payload" — the per-instance discriminator behind
    /// approval routing.
    ///
    /// Same lesson as [`Pattern::for_spawn`], one key over: `(category,
    /// scope, identity)` is not an identity when two instances of a workflow run
    /// on one repo, and `(event, <repo>, workflow_approval)` is exactly that
    /// shape. An approval GATE already keys its wait on this predicate, but the
    /// `read` that lifts the decision behind the gate historically did not, so
    /// "newest wins" could hand instance A the human's verdict on instance B —
    /// merging on a stranger's approval, or holding on a stranger's rejection
    /// (TKT-172). This constructor exists so both sides derive the predicate the
    /// same way instead of hand-rolling the substring twice.
    ///
    /// No `after_id` floor is needed here: an
    /// instance id is minted once per run and never reused, so it keys a run
    /// rather than a generation and cannot be satisfied by a namesake.
    pub fn for_workflow_instance(
        category: Category,
        identity: impl Into<String>,
        instance_id: &str,
    ) -> Self {
        let mut pattern = Self::category(category).identity(identity);
        // serde_json renders a string field exactly like this regardless of key
        // order, so the substring is a reliable per-instance test.
        pattern.payload_search = Some(format!("\"instance\":\"{instance_id}\""));
        pattern
    }

    /// The one predicate for "the tuple that names commit `<sha>` ON BRANCH
    /// `<branch>` in its payload" — the exact-tip discriminator behind the
    /// steward's commit-keyed verdict cache (Phase 2 of the steward
    /// remediation).
    ///
    /// Unlike [`Pattern::for_spawn`]/[`Pattern::for_workflow_instance`],
    /// this is deliberately unscoped by author or run: ANY prior verdict
    /// artifact for this exact branch tip is a valid cache hit, regardless of
    /// which reviewer or steward instance produced it. A new commit changes
    /// `sha`, which invalidates the cache naturally — there is no separate
    /// eviction to get wrong.
    ///
    /// `branch` is bound too (rework of TKT-01M036NWEG0H019BJ16G59RZVP): a bare
    /// sha is not exclusive to one branch — two branches cut from the same
    /// point, before either gains a new commit, share a tip commit, and a
    /// verdict recorded reviewing branch A's diff-against-target must never be
    /// replayed onto branch B's (different) diff-against-target just because
    /// they happen to have the same HEAD. `scope` (the repo) is bound by the
    /// caller as always, so the full key is `(repo, branch, head_sha)`.
    pub fn for_commit(
        category: Category,
        identity: impl Into<String>,
        branch: &str,
        sha: &str,
    ) -> Self {
        let mut pattern = Self::category(category).identity(identity);
        // serde_json renders a string field exactly like this regardless of key
        // order, so each substring is a reliable exact test independent of the
        // other — see `payload_search_and`'s doc for why this needs two checks
        // rather than one combined string.
        pattern.payload_search = Some(format!("\"head_sha\":\"{sha}\""));
        pattern.payload_search_and = Some(format!("\"branch\":\"{branch}\""));
        pattern
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
        if self.payload_search.is_some() || self.payload_search_and.is_some() {
            let hay = tuple.payload.to_string();
            if let Some(search) = &self.payload_search {
                if !hay.contains(search.as_str()) {
                    return false;
                }
            }
            if let Some(search) = &self.payload_search_and {
                if !hay.contains(search.as_str()) {
                    return false;
                }
            }
        }
        if let Some(after) = &self.after_id {
            if tuple.id <= *after {
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
            "Whisker",
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

    /// `for_spawn`'s whole point: a namesake predecessor's tuple
    /// keyed on the id instead of a name/time heuristic: a namesake predecessor's tuple
    /// must not match, and no floor is needed to make that true.
    #[test]
    fn for_spawn_rejects_a_namesake_predecessors_tuple() {
        use crate::id::SpawnId;

        let predecessor_spawn = SpawnId::new();
        let mine_spawn = SpawnId::new();

        let mut predecessor = t();
        predecessor.id = RecordId::floor_at(Utc::now() - chrono::Duration::days(2));
        predecessor.payload = json!({"agent": "Whisker", "spawn": predecessor_spawn.to_string()});

        let mut mine = t();
        mine.payload = json!({"agent": "Whisker", "spawn": mine_spawn.to_string()});

        let p = Pattern::for_spawn(Category::Event, "task_done", mine_spawn);
        assert!(!p.matches(&predecessor), "matched a predecessor's tuple");
        assert!(p.matches(&mine), "missed this generation's own tuple");
    }

    /// TKT-172: the approval decision for one instance must not satisfy
    /// another's read. Both the gate's wait and the `read` behind it derive
    /// their predicate here, so this pins the shape they agree on.
    fn approval(instance: &str, approved: bool) -> Tuple {
        let mut tuple = t();
        tuple.identity = "workflow_approval".into();
        tuple.payload = json!({"instance": instance, "approved": approved, "by": "operator"});
        tuple
    }

    #[test]
    fn for_workflow_instance_rejects_a_peers_decision() {
        let p = Pattern::for_workflow_instance(Category::Event, "workflow_approval", "wf-aaa");
        assert!(p.matches(&approval("wf-aaa", true)), "missed own decision");
        assert!(
            !p.matches(&approval("wf-bbb", false)),
            "matched a peer's decision"
        );
        // An id that is a prefix of another must not match it — the search is on
        // the rendered `"instance":"<id>"` pair, not the bare id.
        assert!(
            !p.matches(&approval("wf-aaa-2", true)),
            "\"wf-aaa\" matched \"wf-aaa-2\""
        );
        // Right instance, wrong event.
        let p = Pattern::for_workflow_instance(Category::Event, "task_done", "wf-aaa");
        assert!(!p.matches(&approval("wf-aaa", true)));
    }

    #[test]
    fn payload_search_is_part_of_the_predicate() {
        let mut p = pattern_scope("myrepo");
        p.payload_search = Some("Whisker".into());
        assert!(p.matches(&t()));
        p.payload_search = Some("Nibbles".into());
        assert!(!p.matches(&t()));
    }

    fn review(branch: &str, sha: &str) -> Tuple {
        Tuple::new(
            Category::Artifact,
            "myrepo",
            "review",
            "some-reviewer",
            json!({"agent": "some-reviewer", "recommendation": "APPROVE", "branch": branch, "head_sha": sha}),
        )
    }

    /// The rework's whole point (TKT-01M036NWEG0H019BJ16G59RZVP rework): two
    /// branches sharing a tip commit — a fresh branch cut with no new commits
    /// of its own — must not share a cached verdict. `for_commit` binds BOTH
    /// fields, so a verdict recorded for branch A's tip must not satisfy a
    /// probe for branch B at that same sha.
    #[test]
    fn for_commit_binds_branch_as_well_as_sha() {
        let p = Pattern::for_commit(Category::Artifact, "review", "branch-a", "sha-shared")
            .scope("myrepo");
        assert!(
            p.matches(&review("branch-a", "sha-shared")),
            "missed own branch+sha"
        );
        assert!(
            !p.matches(&review("branch-b", "sha-shared")),
            "matched a different branch at the same shared tip commit"
        );
        assert!(
            !p.matches(&review("branch-a", "sha-other")),
            "matched a different commit on the same branch"
        );
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
    fn into_trail_makes_a_fresh_ephemeral_pheromone() {
        // A daemon-internal obstacle defaults to a durable Session tuple with no
        // strength — exactly the shape rk-sync would replicate forever. into_trail
        // gives it the same evaporating shape the RPC boundary applies, so it
        // stays local (Ephemeral never replicates) and evaporates.
        let obstacle = Tuple::new(
            Category::Obstacle,
            SYSTEM_SCOPE,
            "sync_failure",
            "castle-a",
            serde_json::json!({"error": "boom"}),
        );
        assert_eq!(obstacle.lifecycle, Lifecycle::Session);
        assert_eq!(obstacle.strength, None);
        assert_eq!(obstacle.expires_at, None);

        let trail = obstacle.into_trail(std::time::Duration::from_secs(1800));
        assert_eq!(trail.lifecycle, Lifecycle::Ephemeral);
        assert_eq!(trail.strength, Some(FULL_STRENGTH));
        let ttl = trail.expires_at.expect("trail carries a TTL backstop");
        // ~30 min out; generous window keeps this off the wall clock's edge.
        let secs = (ttl - trail.created_at).num_seconds();
        assert!(
            (1795..=1805).contains(&secs),
            "expires_at is ~ttl from now, got {secs}s"
        );
    }

    #[test]
    fn weight_is_strictly_descending_in_declaration_order() {
        // Fact (first) outranks everything; each later category weighs less; all
        // weights are positive so no category vanishes from a hot-scan.
        let mut prev = f64::INFINITY;
        for c in Category::ALL {
            let w = c.weight();
            assert!(w > 0.0, "{c:?} weight must be positive");
            assert!(
                w < prev,
                "{c:?} weight {w} must be below the previous {prev}"
            );
            prev = w;
        }
        assert!(Category::Fact.weight() > Category::Endorsement.weight());
    }

    /// TKT-184: a withdrawal is bookkeeping on a finished ballot. It must sit at
    /// the bottom of the hot-scan ranking (never crowding out a live trail) and
    /// must NOT evaporate — the whole point is that it closes the ballot for
    /// good, so a decaying one would silently reopen it.
    #[test]
    fn withdrawal_is_the_weakest_trail_and_never_evaporates() {
        assert!(!Category::Withdrawal.evaporates());
        for c in Category::ALL {
            if c != Category::Withdrawal {
                assert!(
                    c.weight() > Category::Withdrawal.weight(),
                    "{c:?} must outrank a withdrawal"
                );
            }
        }
    }

    #[test]
    fn only_claim_obstacle_need_evaporate() {
        let evaporating = [
            Category::Claim,
            Category::Obstacle,
            Category::Need,
            Category::Resolution,
        ];
        for c in Category::ALL {
            assert_eq!(
                c.evaporates(),
                evaporating.contains(&c),
                "{c:?} evaporation flag"
            );
        }
    }
}

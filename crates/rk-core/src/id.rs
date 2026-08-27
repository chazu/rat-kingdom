//! Identifiers. ULIDs everywhere: sortable, unique, embeddable in NDJSON lines,
//! and safe under `cat_sort_uniq`-style union merges (Phase 6).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use ulid::Ulid;

/// A unique, lexicographically sortable identifier for a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RecordId(Ulid);

impl RecordId {
    pub fn new() -> Self {
        Self(Ulid::new())
    }

    pub fn timestamp_ms(&self) -> u64 {
        self.0.timestamp_ms()
    }

    /// The smallest possible id at instant `at`: that millisecond's timestamp
    /// with a zero random suffix. Never minted for a real record — it exists to
    /// be an exclusive `after_id` floor, so `id > floor_at(t)` selects exactly
    /// the tuples written at or after `t` (a real ULID minted in that same
    /// millisecond has a nonzero random suffix with overwhelming probability,
    /// and sorts above the floor either way).
    ///
    /// This is how a reader that keys on a reusable identifier (an agent name)
    /// bounds itself to the CURRENT generation: pin the floor to the moment the
    /// generation began and a predecessor's records cannot match.
    pub fn floor_at(at: DateTime<Utc>) -> Self {
        Self(Ulid::from_parts(at.timestamp_millis().max(0) as u64, 0))
    }
}

impl Default for RecordId {
    fn default() -> Self {
        Self::new()
    }
}

impl schemars::JsonSchema for RecordId {
    fn schema_name() -> String {
        "RecordId".into()
    }

    fn json_schema(generator: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        String::json_schema(generator)
    }
}

impl fmt::Display for RecordId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for RecordId {
    type Err = ulid::DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Ulid::from_string(s)?))
    }
}

/// Identity of ONE generation of a rat: minted once at spawn, never reused, and
/// never derived from a name.
///
/// # Why this exists
///
/// A rat's *name* does two incompatible jobs today: it is the operator's handle
/// (`rk log Basil-7`, `rat/basil-7/…`) and it is the key tuples, logs, registry
/// rows and supervision edges are joined on. The second job is unsound. Tuples
/// are durable and immortal; a name's claim on them is not. A consumer that
/// joins on a name alone can match a record written by a *different* rat that
/// carried the same name earlier — TKT-136/146, where a `wait` matched a
/// two-day-old namesake's `harness_result`, the `evaluate` behind it judged a
/// stranger's work, and the `dismiss` SIGTERMed a live rat one second into its
/// task. Fixed three times on three keys (TKT-146/159/160); never retired as a
/// class, because "a name identifies a rat" is still false and still
/// load-bearing.
///
/// `SpawnId` retires it structurally: the key cannot collide, so the failure is
/// unreachable rather than merely prevented by policy
/// (`Registry::reserve_name`'s non-recycling rule).
///
/// # Why a ULID
///
/// - **One opaque string.** Embeds in a filename, a payload field, a
///   `payload_search` substring, an env var and a CLI argument with no escaping
///   and no composite-key reconstruction at each site — unlike the de-facto
///   `(name, created_at)` pair several subsystems build by hand today.
/// - **Carries its own timestamp.** [`Self::floor`] derives the generation's
///   lower bound *from the key itself*, so a reader no longer needs a reachable
///   registry record to bound a legacy name-keyed read (the TKT-159 fallback
///   that degraded to an unbounded read).
/// - **Sortable**, so generations order without parsing and survive a
///   `cat_sort_uniq` union merge — the same argument that made [`RecordId`] a
///   ULID.
///
/// # Display
///
/// Never rendered alone to an operator. A rat reads as `Name@<short>`
/// (`Basil-7@01M08HB5`) via [`Self::short`]; names stay the ergonomic handle and
/// the suffix appears only where disambiguation matters. The full id belongs in
/// payloads, paths and `--json`.
///
/// Design and the full consumer census:
/// `docs/2026-08-17-tkt-c1-generation-identity.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SpawnId(Ulid);

impl SpawnId {
    /// Mint the identity of a fresh generation. Called once, where the registry
    /// row is created.
    pub fn new() -> Self {
        Self(Ulid::new())
    }

    /// The identity a pre-migration record *would* have had, derived from its
    /// `created_at`.
    ///
    /// Records written before this type existed carry no `SpawnId`. Rather than
    /// rewrite `agents.json`, the registry synthesises one on load: this is
    /// deterministic (same record, same id, across restarts), sorts correctly
    /// against minted ids, and collides only if two records share a name *and* a
    /// millisecond — which the non-recycling naming policy already forbids.
    ///
    /// The zero random suffix is an implementation detail of deterministic
    /// migration, not an operational identity mode.
    pub fn synthetic_for(created_at: DateTime<Utc>) -> Self {
        Self(Ulid::from_parts(
            created_at.timestamp_millis().max(0) as u64,
            0,
        ))
    }

    /// The 8-char prefix used in the `Name@<short>` operator rendering. A
    /// display affordance only — never a join key, since a prefix can collide.
    pub fn short(&self) -> String {
        self.0.to_string().chars().take(8).collect()
    }
}

impl Default for SpawnId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SpawnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for SpawnId {
    type Err = ulid::DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Ulid::from_string(s)?))
    }
}

impl schemars::JsonSchema for SpawnId {
    fn schema_name() -> String {
        "SpawnId".into()
    }

    fn json_schema(generator: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        String::json_schema(generator)
    }
}

/// Generate a short workflow/agent-style instance id with a prefix, e.g. `wf-01ARZ3`.
pub fn prefixed_id(prefix: &str) -> String {
    let u = Ulid::new().to_string();
    // Last 10 chars carry the randomness; enough for human-scale uniqueness.
    format!("{prefix}-{}", &u[u.len() - 10..].to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_ids_sort_by_creation_time() {
        let a = RecordId::new();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = RecordId::new();
        assert!(a < b);
    }

    #[test]
    fn floor_at_sorts_below_ids_minted_after_it() {
        let before = RecordId::new();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let floor = RecordId::floor_at(Utc::now());
        std::thread::sleep(std::time::Duration::from_millis(2));
        let after = RecordId::new();
        assert!(before < floor, "an earlier id must sort below the floor");
        assert!(floor < after, "a later id must sort above the floor");
    }

    /// The whole point of the type: two generations minted back-to-back are
    /// distinguishable even when a name is reused between them.
    #[test]
    fn spawn_ids_are_distinct_and_ordered() {
        let first = SpawnId::new();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let second = SpawnId::new();
        assert_ne!(first, second);
        assert!(
            first < second,
            "a later generation sorts above an earlier one"
        );
    }

    /// Two mints in the SAME millisecond must still differ — otherwise the key
    /// is no better than the `(name, created_at)` composite it replaces, which
    /// is unique only because generations happen not to overlap in time.
    #[test]
    fn spawn_ids_minted_in_one_millisecond_still_differ() {
        let a = SpawnId::new();
        let b = SpawnId::new();
        assert_ne!(a, b);
    }

    /// Backfill is deterministic: the same pre-migration record synthesises the
    /// same id on every restart, so a reader never sees a generation change key.
    #[test]
    fn synthetic_ids_are_deterministic() {
        let created_at = Utc::now();
        let once = SpawnId::synthetic_for(created_at);
        let twice = SpawnId::synthetic_for(created_at);
        assert_eq!(once, twice, "backfill must be stable across loads");
    }

    /// A spawn id round-trips through its rendered form — it travels as a string
    /// in payloads, paths and env vars.
    #[test]
    fn spawn_ids_round_trip_through_display() {
        let id = SpawnId::new();
        let parsed: SpawnId = id.to_string().parse().unwrap();
        assert_eq!(id, parsed);
        assert_eq!(id.short().len(), 8);
        assert!(id.to_string().starts_with(&id.short()));
    }

    #[test]
    fn prefixed_id_has_prefix() {
        let id = prefixed_id("wf");
        assert!(id.starts_with("wf-"));
        assert_eq!(id.len(), 3 + 10);
    }
}

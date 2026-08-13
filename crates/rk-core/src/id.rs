//! Identifiers. ULIDs everywhere: sortable, unique, embeddable in NDJSON lines,
//! and safe under `cat_sort_uniq`-style union merges (Phase 6).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Mutex};
use ulid::{Generator, Ulid};

static RECORD_ID_GENERATOR: Mutex<Generator> = Mutex::new(Generator::new());

/// A unique, lexicographically sortable identifier for a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RecordId(Ulid);

impl RecordId {
    pub fn new() -> Self {
        let mut generator = RECORD_ID_GENERATOR
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Self(
            generator
                .generate()
                .expect("RecordId monotonic random component exhausted"),
        )
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
    fn record_ids_are_strictly_monotonic_without_clock_delay() {
        let ids = (0..1_000).map(|_| RecordId::new()).collect::<Vec<_>>();
        assert!(
            ids.windows(2).all(|pair| pair[0] < pair[1]),
            "cursor-safe ids must preserve mint order within one millisecond"
        );
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

    #[test]
    fn prefixed_id_has_prefix() {
        let id = prefixed_id("wf");
        assert!(id.starts_with("wf-"));
        assert_eq!(id.len(), 3 + 10);
    }
}

//! Identifiers. ULIDs everywhere: sortable, unique, embeddable in NDJSON lines,
//! and safe under `cat_sort_uniq`-style union merges (Phase 6).

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
    fn prefixed_id_has_prefix() {
        let id = prefixed_id("wf");
        assert!(id.starts_with("wf-"));
        assert_eq!(id.len(), 3 + 10);
    }
}

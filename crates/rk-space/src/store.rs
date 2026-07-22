//! SQLite persistence for tuples.
//!
//! Payload search note: payloads are stored as their exact
//! `serde_json::Value::to_string()` serialization, and payload search is a
//! substring `LIKE` over that text — byte-for-byte the same haystack that
//! [`rk_core::tuple::Pattern::matches`] uses in memory. Do not "optimize" this
//! into FTS tokenization: divergent predicates between the storage query and
//! the waiter wake path are how the predecessor lost wakeups.

use chrono::{DateTime, Utc};
use rk_core::id::RecordId;
use rk_core::tuple::{Category, Lifecycle, Pattern, Tuple};
use rk_core::Error;
use rusqlite::{params_from_iter, Connection, Row};
use std::path::Path;

pub(crate) struct Store {
    conn: Connection,
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS tuples (
    id         TEXT PRIMARY KEY,
    category   TEXT NOT NULL,
    scope      TEXT NOT NULL,
    identity   TEXT NOT NULL,
    instance   TEXT NOT NULL,
    lifecycle  TEXT NOT NULL,
    payload    TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_tuples_prefix
    ON tuples (category, scope, identity, instance);
CREATE INDEX IF NOT EXISTS idx_tuples_expiry
    ON tuples (expires_at) WHERE expires_at IS NOT NULL;
";

impl Store {
    pub fn open(path: &Path) -> rk_core::Result<Self> {
        let conn = Connection::open(path).map_err(sql_err)?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(sql_err)?;
        conn.pragma_update(None, "busy_timeout", 5000)
            .map_err(sql_err)?;
        conn.execute_batch(SCHEMA).map_err(sql_err)?;
        Ok(Self { conn })
    }

    pub fn open_in_memory() -> rk_core::Result<Self> {
        let conn = Connection::open_in_memory().map_err(sql_err)?;
        conn.execute_batch(SCHEMA).map_err(sql_err)?;
        Ok(Self { conn })
    }

    pub fn insert(&self, tuple: &Tuple) -> rk_core::Result<()> {
        self.conn
            .execute(
                "INSERT INTO tuples
                 (id, category, scope, identity, instance, lifecycle, payload, created_at, expires_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                rusqlite::params![
                    tuple.id.to_string(),
                    tuple.category.as_str(),
                    tuple.scope,
                    tuple.identity,
                    tuple.instance,
                    lifecycle_str(tuple.lifecycle),
                    tuple.payload.to_string(),
                    tuple.created_at.to_rfc3339(),
                    tuple.expires_at.map(|t| t.to_rfc3339()),
                ],
            )
            .map_err(sql_err)?;
        Ok(())
    }

    pub fn exists(&self, id: RecordId) -> rk_core::Result<bool> {
        let n: u64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM tuples WHERE id = ?1",
                [id.to_string()],
                |r| r.get(0),
            )
            .map_err(sql_err)?;
        Ok(n > 0)
    }

    pub fn delete(&self, id: RecordId) -> rk_core::Result<bool> {
        let n = self
            .conn
            .execute("DELETE FROM tuples WHERE id = ?1", [id.to_string()])
            .map_err(sql_err)?;
        Ok(n > 0)
    }

    /// Find tuples matching `pattern`, oldest first.
    ///
    /// The WHERE clause mirrors [`Pattern::matches`] exactly; `consumable_only`
    /// additionally excludes furniture (for destructive reads).
    pub fn query(
        &self,
        pattern: &Pattern,
        consumable_only: bool,
        limit: Option<usize>,
    ) -> rk_core::Result<Vec<Tuple>> {
        let mut sql = String::from(
            "SELECT id, category, scope, identity, instance, lifecycle, payload, created_at, expires_at
             FROM tuples WHERE 1=1",
        );
        let mut args: Vec<String> = Vec::new();

        if let Some(c) = pattern.category {
            sql.push_str(" AND category = ?");
            args.push(c.as_str().to_string());
        }
        if let Some(s) = &pattern.scope {
            sql.push_str(" AND scope = ?");
            args.push(s.clone());
        }
        if let Some(i) = &pattern.identity {
            sql.push_str(" AND identity = ?");
            args.push(i.clone());
        }
        if let Some(inst) = &pattern.instance {
            sql.push_str(" AND instance = ?");
            args.push(inst.clone());
        }
        if let Some(search) = &pattern.payload_search {
            sql.push_str(" AND payload LIKE ? ESCAPE '\\'");
            args.push(format!("%{}%", escape_like(search)));
        }
        if consumable_only {
            sql.push_str(" AND lifecycle != 'furniture'");
        }
        sql.push_str(" ORDER BY id ASC");
        if let Some(n) = limit {
            sql.push_str(&format!(" LIMIT {n}"));
        }

        let mut stmt = self.conn.prepare(&sql).map_err(sql_err)?;
        let rows = stmt
            .query_map(params_from_iter(args.iter()), row_to_tuple)
            .map_err(sql_err)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(sql_err)?);
        }
        Ok(out)
    }

    /// Delete expired ephemeral tuples; returns how many were collected.
    pub fn delete_expired(&self, now: DateTime<Utc>) -> rk_core::Result<usize> {
        let n = self
            .conn
            .execute(
                "DELETE FROM tuples WHERE expires_at IS NOT NULL AND expires_at < ?1",
                [now.to_rfc3339()],
            )
            .map_err(sql_err)?;
        Ok(n)
    }

    pub fn count(&self) -> rk_core::Result<u64> {
        self.conn
            .query_row("SELECT COUNT(*) FROM tuples", [], |r| r.get(0))
            .map_err(sql_err)
    }
}

fn escape_like(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn lifecycle_str(l: Lifecycle) -> &'static str {
    match l {
        Lifecycle::Furniture => "furniture",
        Lifecycle::Session => "session",
        Lifecycle::Ephemeral => "ephemeral",
    }
}

fn parse_lifecycle(s: &str) -> Lifecycle {
    match s {
        "furniture" => Lifecycle::Furniture,
        "ephemeral" => Lifecycle::Ephemeral,
        _ => Lifecycle::Session,
    }
}

fn row_to_tuple(row: &Row<'_>) -> rusqlite::Result<Tuple> {
    let id: String = row.get(0)?;
    let category: String = row.get(1)?;
    let lifecycle: String = row.get(5)?;
    let payload: String = row.get(6)?;
    let created_at: String = row.get(7)?;
    let expires_at: Option<String> = row.get(8)?;

    Ok(Tuple {
        id: id.parse().unwrap_or_default(),
        category: category.parse::<Category>().unwrap_or(Category::Event),
        scope: row.get(2)?,
        identity: row.get(3)?,
        instance: row.get(4)?,
        lifecycle: parse_lifecycle(&lifecycle),
        payload: serde_json::from_str(&payload).unwrap_or(serde_json::Value::Null),
        created_at: DateTime::parse_from_rfc3339(&created_at)
            .map(|t| t.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
        expires_at: expires_at
            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
            .map(|t| t.with_timezone(&Utc)),
    })
}

fn sql_err(e: rusqlite::Error) -> Error {
    Error::Other(format!("sqlite: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn tuple(identity: &str, payload: serde_json::Value) -> Tuple {
        Tuple::new(Category::Event, "repo", identity, "castle", payload)
    }

    #[test]
    fn insert_query_delete_round_trip() {
        let store = Store::open_in_memory().unwrap();
        let t = tuple("task_done", json!({"agent": "Whisker"}));
        store.insert(&t).unwrap();

        let found = store
            .query(&Pattern::category(Category::Event), false, None)
            .unwrap();
        assert_eq!(found, vec![t.clone()]);

        assert!(store.delete(t.id).unwrap());
        assert_eq!(store.count().unwrap(), 0);
    }

    #[test]
    fn query_where_clause_agrees_with_pattern_matches() {
        // The core invariant: for a corpus of tuples and a set of patterns,
        // SQL query results must be identical to in-memory Pattern::matches.
        let store = Store::open_in_memory().unwrap();
        let tuples = vec![
            tuple("a", json!({"agent": "Whisker", "n": 1})),
            tuple("b", json!({"agent": "Nibbles", "note": "50% done"})),
            Tuple::new(
                Category::Fact,
                "other",
                "c",
                "castle-b",
                json!({"x": "under_score"}),
            ),
            Tuple::new(Category::Claim, "repo", "a", "castle-b", json!(null)),
        ];
        for t in &tuples {
            store.insert(t).unwrap();
        }

        let patterns = vec![
            Pattern::default(),
            Pattern::category(Category::Event),
            Pattern::default().scope("repo"),
            Pattern::default().identity("a"),
            Pattern {
                payload_search: Some("Whisker".into()),
                ..Default::default()
            },
            // LIKE metacharacters must be treated literally.
            Pattern {
                payload_search: Some("50%".into()),
                ..Default::default()
            },
            Pattern {
                payload_search: Some("under_score".into()),
                ..Default::default()
            },
            Pattern {
                category: Some(Category::Claim),
                scope: Some("repo".into()),
                instance: Some("castle-b".into()),
                ..Default::default()
            },
        ];

        for p in &patterns {
            // Compare as id-sorted sets: ULIDs minted in the same millisecond
            // have no defined relative order, so insertion order != ORDER BY id.
            let mut from_sql = store.query(p, false, None).unwrap();
            from_sql.sort_by_key(|t| t.id);
            let mut from_mem: Vec<Tuple> =
                tuples.iter().filter(|t| p.matches(t)).cloned().collect();
            from_mem.sort_by_key(|t| t.id);
            assert_eq!(from_sql, from_mem, "pattern diverged: {p:?}");
        }
    }

    #[test]
    fn consumable_only_excludes_furniture() {
        let store = Store::open_in_memory().unwrap();
        let furniture = tuple("f", json!({})).with_lifecycle(Lifecycle::Furniture);
        store.insert(&furniture).unwrap();
        assert_eq!(
            store.query(&Pattern::default(), true, None).unwrap(),
            vec![]
        );
        assert_eq!(
            store.query(&Pattern::default(), false, None).unwrap().len(),
            1
        );
    }

    #[test]
    fn delete_expired_collects_only_past_ttls() {
        let store = Store::open_in_memory().unwrap();
        let mut expired = tuple("old", json!({})).with_lifecycle(Lifecycle::Ephemeral);
        expired.expires_at = Some(Utc::now() - chrono::Duration::seconds(5));
        let mut alive = tuple("new", json!({})).with_lifecycle(Lifecycle::Ephemeral);
        alive.expires_at = Some(Utc::now() + chrono::Duration::seconds(60));
        store.insert(&expired).unwrap();
        store.insert(&alive).unwrap();

        assert_eq!(store.delete_expired(Utc::now()).unwrap(), 1);
        let left = store.query(&Pattern::default(), false, None).unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].identity, "new");
    }
}

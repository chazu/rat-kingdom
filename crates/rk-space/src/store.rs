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
use rusqlite::{params_from_iter, Connection, OptionalExtension, Row};
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
    expires_at TEXT,
    strength   REAL
);
CREATE INDEX IF NOT EXISTS idx_tuples_prefix
    ON tuples (category, scope, identity, instance);
CREATE INDEX IF NOT EXISTS idx_tuples_expiry
    ON tuples (expires_at) WHERE expires_at IS NOT NULL;
";

/// Bring an older DB up to the current schema. `CREATE TABLE IF NOT EXISTS`
/// leaves an already-created table untouched, so a DB from before `strength`
/// existed needs an explicit `ALTER` (the duplicate-column error on an
/// up-to-date DB is expected and swallowed). The strength index is created here,
/// after the column is guaranteed to exist, so it cannot fail the initial batch
/// on a pre-`strength` database.
fn migrate(conn: &Connection) {
    let _ = conn.execute("ALTER TABLE tuples ADD COLUMN strength REAL", []);
    let _ = conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_tuples_strength
             ON tuples (strength) WHERE strength IS NOT NULL;",
    );
}

impl Store {
    pub fn open(path: &Path) -> rk_core::Result<Self> {
        let conn = Connection::open(path).map_err(sql_err)?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(sql_err)?;
        conn.pragma_update(None, "busy_timeout", 5000)
            .map_err(sql_err)?;
        conn.execute_batch(SCHEMA).map_err(sql_err)?;
        migrate(&conn);
        Ok(Self { conn })
    }

    pub fn open_in_memory() -> rk_core::Result<Self> {
        let conn = Connection::open_in_memory().map_err(sql_err)?;
        conn.execute_batch(SCHEMA).map_err(sql_err)?;
        migrate(&conn);
        Ok(Self { conn })
    }

    pub fn insert(&self, tuple: &Tuple) -> rk_core::Result<()> {
        self.conn
            .execute(
                "INSERT INTO tuples
                 (id, category, scope, identity, instance, lifecycle, payload, created_at, expires_at, strength)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
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
                    tuple.strength,
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
            "SELECT id, category, scope, identity, instance, lifecycle, payload, created_at, expires_at, strength
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
        if let Some(after) = &pattern.after_id {
            // id is the TEXT PRIMARY KEY and ULIDs sort lexicographically by
            // creation time, so this "newer than cursor" bound is answered from
            // the PK index — a bounded delta scan, not a full-table read.
            sql.push_str(" AND id > ?");
            args.push(after.to_string());
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

    /// Find tuples matching `pattern`, strongest trail first (the `--hot`
    /// gradient, stigmergy P7). Each tuple is scored by
    /// `category_weight × recency × strength` — see [`hot_score`] — and returned
    /// highest-first, optionally capped to the top `limit`.
    ///
    /// Read-only sugar layered over [`Store::query`]: it reuses the exact same
    /// WHERE clause (so the [`Pattern::matches`] mirror invariant is untouched)
    /// and merely reorders in memory. The default oldest-first `query` path and
    /// the waiter-wake predicate are deliberately left alone.
    pub fn query_ranked(
        &self,
        pattern: &Pattern,
        now: DateTime<Utc>,
        limit: Option<usize>,
    ) -> rk_core::Result<Vec<Tuple>> {
        let tuples = self.query(pattern, false, None)?;
        let mut scored: Vec<(f64, Tuple)> =
            tuples.into_iter().map(|t| (hot_score(&t, now), t)).collect();
        // Strongest score first; ties broken newest-id first so a hot-scan is
        // deterministic. `total_cmp` keeps NaN-free scores well-ordered.
        scored.sort_by(|a, b| {
            b.0.total_cmp(&a.0).then_with(|| b.1.id.cmp(&a.1.id))
        });
        let mut out: Vec<Tuple> = scored.into_iter().map(|(_, t)| t).collect();
        if let Some(n) = limit {
            out.truncate(n);
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

    /// The id of the newest existing pheromone trail on this exact
    /// `(category, scope, identity, instance)` key, if any. Reinforcement
    /// refreshes that row in place rather than appending a duplicate; keeping
    /// the id stable also preserves earliest-claim-wins arbitration in sync.
    pub fn newest_trail(
        &self,
        category: Category,
        scope: &str,
        identity: &str,
        instance: &str,
    ) -> rk_core::Result<Option<RecordId>> {
        let id: Option<String> = self
            .conn
            .query_row(
                "SELECT id FROM tuples
                 WHERE category = ?1 AND scope = ?2 AND identity = ?3 AND instance = ?4
                 ORDER BY id DESC LIMIT 1",
                rusqlite::params![category.as_str(), scope, identity, instance],
                |r| r.get(0),
            )
            .optional()
            .map_err(sql_err)?;
        Ok(id.and_then(|s| s.parse().ok()))
    }

    /// Reinforce an existing trail in place: refresh its payload, TTL, and
    /// lifecycle, and reset strength to `strength`. The id and `created_at`
    /// stay put so downstream arbitration still sees the original claim.
    pub fn reinforce(&self, id: RecordId, tuple: &Tuple) -> rk_core::Result<()> {
        self.conn
            .execute(
                "UPDATE tuples SET lifecycle = ?2, payload = ?3, expires_at = ?4, strength = ?5
                 WHERE id = ?1",
                rusqlite::params![
                    id.to_string(),
                    lifecycle_str(tuple.lifecycle),
                    tuple.payload.to_string(),
                    tuple.expires_at.map(|t| t.to_rfc3339()),
                    tuple.strength,
                ],
            )
            .map_err(sql_err)?;
        Ok(())
    }

    /// Decay every pheromone trail by `step`, then collect the ones that have
    /// faded to nothing (strength `<= 0`). Returns how many were collected.
    /// This is the smooth-fade half of GC: a trail its author stopped
    /// refreshing loses strength each cycle until it evaporates.
    pub fn decay_and_collect(&self, step: f64) -> rk_core::Result<usize> {
        self.conn
            .execute(
                "UPDATE tuples SET strength = strength - ?1 WHERE strength IS NOT NULL",
                [step],
            )
            .map_err(sql_err)?;
        let n = self
            .conn
            .execute(
                "DELETE FROM tuples WHERE strength IS NOT NULL AND strength <= 0",
                [],
            )
            .map_err(sql_err)?;
        Ok(n)
    }

    pub fn count(&self) -> rk_core::Result<u64> {
        self.conn
            .query_row("SELECT COUNT(*) FROM tuples", [], |r| r.get(0))
            .map_err(sql_err)
    }

    /// Number of tuples whose category is in `categories`, counted in SQL
    /// without materializing any rows. The reactor uses this as a cheap,
    /// order-independent "did this category's population change?" gate before
    /// deciding whether a full whole-store recompute scan is warranted — an
    /// exact count is immune to the same-millisecond ULID ordering that makes a
    /// cursor delta an unreliable change signal.
    pub fn count_in_categories(&self, categories: &[Category]) -> rk_core::Result<u64> {
        if categories.is_empty() {
            return Ok(0);
        }
        let placeholders = std::iter::repeat("?")
            .take(categories.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT COUNT(*) FROM tuples WHERE category IN ({placeholders})");
        let args: Vec<String> = categories.iter().map(|c| c.as_str().to_string()).collect();
        self.conn
            .query_row(&sql, params_from_iter(args.iter()), |r| r.get(0))
            .map_err(sql_err)
    }
}

/// Recency half-life for hot-ranking, in seconds: a tuple's recency factor
/// halves every this-many seconds of age. Set near the ~30-min unreinforced
/// pheromone lifetime (TKT-14) so a fresh trail clearly outshines a stale one
/// without an old-but-strong Fact being buried instantly.
const HOT_HALF_LIFE_SECS: f64 = 1800.0;

/// The hot-scan gradient score for one tuple: `category_weight × recency ×
/// strength`, all read-only signals.
///
/// * `category_weight` — [`Category::weight`], `Fact` heaviest.
/// * `recency` — exponential decay `0.5^(age / half_life)`, so a just-written
///   tuple scores `1.0` and older ones fade smoothly. Future/clock-skewed
///   `created_at` is clamped to age `0` (recency `1.0`), never above.
/// * `strength` — the evaporating-trail pheromone strength (TKT-14); tuples
///   that do not carry one (facts, artifacts, …) count as [`FULL_STRENGTH`],
///   so their weight and recency alone rank them.
fn hot_score(tuple: &Tuple, now: DateTime<Utc>) -> f64 {
    let age_secs = (now - tuple.created_at).num_milliseconds().max(0) as f64 / 1000.0;
    let recency = 0.5f64.powf(age_secs / HOT_HALF_LIFE_SECS);
    let strength = tuple.strength.unwrap_or(rk_core::tuple::FULL_STRENGTH);
    tuple.category.weight() * recency * strength
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
    let strength: Option<f64> = row.get(9)?;

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
        strength,
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
    fn count_in_categories_sums_the_requested_categories_only() {
        let store = Store::open_in_memory().unwrap();
        store.insert(&tuple("a", json!({}))).unwrap(); // Event
        store.insert(&tuple("b", json!({}))).unwrap(); // Event
        store
            .insert(&Tuple::new(Category::Obstacle, "repo", "c", "rat", json!({})))
            .unwrap();
        store
            .insert(&Tuple::new(Category::Need, "repo", "d", "rat", json!({})))
            .unwrap();

        assert_eq!(store.count_in_categories(&[]).unwrap(), 0, "empty set is zero");
        assert_eq!(store.count_in_categories(&[Category::Event]).unwrap(), 2);
        assert_eq!(
            store
                .count_in_categories(&[Category::Obstacle, Category::Need])
                .unwrap(),
            2,
            "sums across the requested categories, ignores the rest"
        );
        assert_eq!(
            store.count_in_categories(&[Category::Convention]).unwrap(),
            0,
            "absent category counts zero"
        );
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
            // Exclusive id lower bound: SQL `id > ?` must agree with the
            // in-memory `tuple.id <= after` cut over the same ULID total order.
            Pattern::default().after(Some(tuples[1].id)),
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

    fn trail(identity: &str, instance: &str, strength: f64) -> Tuple {
        let mut t = Tuple::new(Category::Claim, "repo", identity, instance, json!({}))
            .with_lifecycle(Lifecycle::Ephemeral);
        t.strength = Some(strength);
        t
    }

    #[test]
    fn strength_persists_through_insert_and_query() {
        let store = Store::open_in_memory().unwrap();
        let t = trail("area", "rat", 1.0);
        store.insert(&t).unwrap();
        let got = store.query(&Pattern::default(), false, None).unwrap();
        assert_eq!(got[0].strength, Some(1.0));
    }

    #[test]
    fn reinforce_refreshes_in_place_keeping_id() {
        let store = Store::open_in_memory().unwrap();
        let mut original = trail("area", "rat", 0.3);
        original.expires_at = Some(Utc::now() + chrono::Duration::seconds(10));
        store.insert(&original).unwrap();

        // Same key: newest_trail finds it, reinforce refreshes strength/payload/TTL.
        let found = store
            .newest_trail(Category::Claim, "repo", "area", "rat")
            .unwrap();
        assert_eq!(found, Some(original.id));

        let mut refreshed = trail("area", "rat", 1.0);
        refreshed.payload = json!({"note": "still working"});
        refreshed.expires_at = Some(Utc::now() + chrono::Duration::seconds(600));
        store.reinforce(original.id, &refreshed).unwrap();

        let rows = store.query(&Pattern::default(), false, None).unwrap();
        assert_eq!(rows.len(), 1, "reinforcement does not duplicate");
        assert_eq!(rows[0].id, original.id, "id is preserved for arbitration");
        assert_eq!(rows[0].strength, Some(1.0));
        assert_eq!(rows[0].payload, json!({"note": "still working"}));
    }

    #[test]
    fn newest_trail_none_for_unknown_key() {
        let store = Store::open_in_memory().unwrap();
        assert_eq!(
            store
                .newest_trail(Category::Claim, "repo", "nope", "rat")
                .unwrap(),
            None
        );
    }

    #[test]
    fn opens_and_upgrades_a_pre_strength_database() {
        // Simulate a DB created before the `strength` column existed: the old
        // table shape and the two original indexes, no strength column/index.
        let dir = std::env::temp_dir().join(format!("rk-migrate-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("space.db");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE tuples (
                     id TEXT PRIMARY KEY, category TEXT NOT NULL, scope TEXT NOT NULL,
                     identity TEXT NOT NULL, instance TEXT NOT NULL, lifecycle TEXT NOT NULL,
                     payload TEXT NOT NULL, created_at TEXT NOT NULL, expires_at TEXT);
                 INSERT INTO tuples VALUES
                     ('01', 'fact', 'repo', 'old', 'castle', 'session', '{}', '2020-01-01T00:00:00Z', NULL);",
            )
            .unwrap();
        }

        // Opening runs migrate(): the ALTER + strength index must succeed, the
        // pre-existing row survives with a NULL strength, and new writes work.
        let store = Store::open(&path).unwrap();
        let rows = store.query(&Pattern::default(), false, None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].strength, None);
        store.insert(&trail("area", "rat", 1.0)).unwrap();
        assert_eq!(store.decay_and_collect(0.5).unwrap(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn hot_ranks_by_category_recency_and_strength() {
        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();

        // A fresh strong claim, a fresh fact (heaviest category), and a stale
        // faint claim. Backdate created_at directly so recency is exercised.
        let mut fact = Tuple::new(Category::Fact, "repo", "hot-fact", "castle", json!({}));
        fact.created_at = now;
        let mut fresh = trail("fresh", "rat", 1.0);
        fresh.created_at = now;
        let mut stale = trail("stale", "rat", 0.1);
        stale.created_at = now - chrono::Duration::hours(6);
        for t in [&fact, &fresh, &stale] {
            store.insert(t).unwrap();
        }

        let ranked = store
            .query_ranked(&Pattern::default(), now, None)
            .unwrap();
        let order: Vec<&str> = ranked.iter().map(|t| t.identity.as_str()).collect();
        // Fact outweighs the fresh claim; the stale faint claim sinks last.
        assert_eq!(order, vec!["hot-fact", "fresh", "stale"]);

        // --top N caps to the strongest.
        let top1 = store
            .query_ranked(&Pattern::default(), now, Some(1))
            .unwrap();
        assert_eq!(top1.len(), 1);
        assert_eq!(top1[0].identity, "hot-fact");
    }

    #[test]
    fn hot_scan_leaves_default_order_untouched() {
        // The ranked path is additive: the plain oldest-first query still sorts
        // by id ASC regardless of score.
        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();
        let weak = tuple("weak", json!({}));
        let strong = Tuple::new(Category::Fact, "repo", "strong", "castle", json!({}));
        store.insert(&weak).unwrap();
        store.insert(&strong).unwrap();

        let plain = store.query(&Pattern::default(), false, None).unwrap();
        let mut expected = vec![weak.clone(), strong.clone()];
        expected.sort_by_key(|t| t.id);
        assert_eq!(
            plain.iter().map(|t| t.id).collect::<Vec<_>>(),
            expected.iter().map(|t| t.id).collect::<Vec<_>>()
        );
        // And the ranked path does not mutate stored rows.
        let _ = store.query_ranked(&Pattern::default(), now, None).unwrap();
        assert_eq!(store.count().unwrap(), 2);
    }

    #[test]
    fn hot_score_clamps_future_created_at() {
        // A clock-skewed future timestamp must not score above a fresh one.
        let mut future = trail("future", "rat", 1.0);
        let now = Utc::now();
        future.created_at = now + chrono::Duration::hours(1);
        let mut fresh = trail("fresh", "rat", 1.0);
        fresh.created_at = now;
        assert!((hot_score(&future, now) - hot_score(&fresh, now)).abs() < 1e-9);
    }

    #[test]
    fn decay_reduces_strength_and_collects_at_zero() {
        let store = Store::open_in_memory().unwrap();
        store.insert(&trail("faint", "rat", 0.05)).unwrap();
        store.insert(&trail("bright", "rat", 1.0)).unwrap();
        // A non-evaporating tuple (strength NULL) must be untouched by decay.
        store
            .insert(&Tuple::new(
                Category::Fact,
                "repo",
                "f",
                "castle",
                json!({}),
            ))
            .unwrap();

        let collected = store.decay_and_collect(0.1).unwrap();
        assert_eq!(
            collected, 1,
            "the faint trail faded to <= 0 and was collected"
        );

        let rows = store.query(&Pattern::default(), false, None).unwrap();
        assert_eq!(rows.len(), 2);
        let bright = rows.iter().find(|t| t.identity == "bright").unwrap();
        assert!((bright.strength.unwrap() - 0.9).abs() < 1e-9);
        let fact = rows.iter().find(|t| t.identity == "f").unwrap();
        assert_eq!(fact.strength, None, "non-trail strength stays NULL");
    }
}

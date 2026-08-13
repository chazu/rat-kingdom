use chrono::{Duration, Utc};
use rk_core::id::RecordId;
use rk_core::tuple::{Category, Tuple};
use rk_space::Space;
use serde_json::json;

fn fact(id: RecordId, identity: &str) -> Tuple {
    let mut tuple = Tuple::new(Category::Fact, "repo", identity, "castle-a", json!({}));
    tuple.id = id;
    tuple
}

#[test]
fn delayed_lower_record_id_replays_after_persistence_boundary() {
    let space = Space::open_in_memory().unwrap();
    let now = Utc::now();
    let delayed = fact(RecordId::floor_at(now), "delayed");
    let boundary = fact(RecordId::floor_at(now + Duration::days(1)), "boundary");

    space.out(boundary).unwrap();
    let first_boundary = space.latest_persistence_sequence().unwrap();

    space.out(delayed.clone()).unwrap();
    let delta = space.persistence_delta(Some(first_boundary)).unwrap();

    assert!(delta.boundary > first_boundary);
    assert_eq!(delta.tuples, vec![delayed]);
}

#[test]
fn persistence_boundary_survives_reopen_and_clock_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("space.db");
    let now = Utc::now();
    let future = fact(RecordId::floor_at(now + Duration::days(1)), "future");

    let first_boundary = {
        let space = Space::open(&db).unwrap();
        space.out(future).unwrap();
        space.latest_persistence_sequence().unwrap()
    };

    let restarted = Space::open(&db).unwrap();
    let rolled_back = fact(RecordId::floor_at(now), "rolled-back");
    restarted.out(rolled_back.clone()).unwrap();
    let delta = restarted
        .persistence_delta(Some(first_boundary))
        .unwrap();

    assert!(delta.boundary > first_boundary);
    assert_eq!(delta.tuples, vec![rolled_back]);
}

#[test]
fn independent_connections_share_one_persistence_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("shared.db");
    let first = Space::open(&db).unwrap();
    let second = Space::open(&db).unwrap();
    let now = Utc::now();
    let delayed = fact(RecordId::floor_at(now), "delayed-independent");
    let boundary = fact(
        RecordId::floor_at(now + Duration::days(1)),
        "boundary-independent",
    );

    first.out(boundary).unwrap();
    let first_boundary = first.latest_persistence_sequence().unwrap();
    second.out(delayed.clone()).unwrap();

    assert_eq!(
        first
            .persistence_delta(Some(first_boundary))
            .unwrap()
            .tuples,
        vec![delayed]
    );
    assert_eq!(second.latest_persistence_sequence().unwrap(), first_boundary + 1);
}

#[test]
fn deleted_rows_still_advance_the_captured_boundary() {
    let space = Space::open_in_memory().unwrap();
    let tuple = fact(RecordId::floor_at(Utc::now()), "short-lived");
    let id = tuple.id;
    space.out(tuple).unwrap();
    let boundary = space.latest_persistence_sequence().unwrap();
    assert!(space.delete(id).unwrap());

    let delta = space.persistence_delta(None).unwrap();

    assert_eq!(delta.boundary, boundary);
    assert!(delta.tuples.is_empty());
}

#[test]
fn legacy_database_backfills_deterministically_without_hiding_new_low_ids() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("legacy.db");
    let now = Utc::now();
    let low = fact(RecordId::floor_at(now), "legacy-low");
    let high = fact(
        RecordId::floor_at(now + Duration::days(1)),
        "legacy-high",
    );
    {
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch(
            "CREATE TABLE tuples (
                id TEXT PRIMARY KEY,
                category TEXT NOT NULL,
                scope TEXT NOT NULL,
                identity TEXT NOT NULL,
                instance TEXT NOT NULL,
                lifecycle TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT,
                strength REAL
            );",
        )
        .unwrap();
        for tuple in [&high, &low] {
            conn.execute(
                "INSERT INTO tuples
                 (id, category, scope, identity, instance, lifecycle, payload,
                  created_at, expires_at, strength)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'session', ?6, ?7, NULL, NULL)",
                rusqlite::params![
                    tuple.id.to_string(),
                    tuple.category.as_str(),
                    tuple.scope,
                    tuple.identity,
                    tuple.instance,
                    tuple.payload.to_string(),
                    tuple.created_at.to_rfc3339(),
                ],
            )
            .unwrap();
        }
    }

    let space = Space::open(&db).unwrap();
    let migrated = space.persistence_delta(None).unwrap();
    assert_eq!(migrated.boundary, 2);
    assert_eq!(migrated.tuples, vec![low.clone(), high.clone()]);
    assert_eq!(space.legacy_persistence_sequence(low.id).unwrap(), Some(1));
    assert_eq!(space.legacy_persistence_sequence(high.id).unwrap(), Some(2));

    let post_migration_low = fact(
        RecordId::floor_at(now - Duration::days(1)),
        "post-migration-low",
    );
    space.out(post_migration_low.clone()).unwrap();
    assert_eq!(space.latest_persistence_sequence().unwrap(), 3);
    assert_eq!(
        space.legacy_persistence_sequence(high.id).unwrap(),
        Some(2),
        "legacy conversion must never consume a post-migration low ULID"
    );
    assert_eq!(
        space.persistence_delta(Some(2)).unwrap().tuples,
        vec![post_migration_low]
    );
}

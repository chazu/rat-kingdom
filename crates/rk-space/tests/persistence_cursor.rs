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

fn create_legacy_database(path: &std::path::Path, tuples: &[&Tuple]) {
    let conn = rusqlite::Connection::open(path).unwrap();
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
    for tuple in tuples {
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

fn create_sequence_only_database_after_deletion(path: &std::path::Path, surviving: &Tuple) {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch(
        "CREATE TABLE tuples (
            id TEXT PRIMARY KEY,
            commit_sequence INTEGER,
            category TEXT NOT NULL,
            scope TEXT NOT NULL,
            identity TEXT NOT NULL,
            instance TEXT NOT NULL,
            lifecycle TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT,
            strength REAL
        );
        CREATE TABLE tuple_sequence_state (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            last_sequence INTEGER NOT NULL,
            legacy_backfill_sequence INTEGER NOT NULL
        );
        INSERT INTO tuple_sequence_state
            (singleton, last_sequence, legacy_backfill_sequence)
        VALUES (1, 2, 2);",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO tuples
         (id, commit_sequence, category, scope, identity, instance, lifecycle,
          payload, created_at, expires_at, strength)
         VALUES (?1, 2, ?2, ?3, ?4, ?5, 'session', ?6, ?7, NULL, NULL)",
        rusqlite::params![
            surviving.id.to_string(),
            surviving.category.as_str(),
            surviving.scope,
            surviving.identity,
            surviving.instance,
            surviving.payload.to_string(),
            surviving.created_at.to_rfc3339(),
        ],
    )
    .unwrap();
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
fn deleted_rows_remain_replayable_in_the_persistence_journal() {
    let space = Space::open_in_memory().unwrap();
    let tuple = fact(RecordId::floor_at(Utc::now()), "short-lived");
    let id = tuple.id;
    space.out(tuple.clone()).unwrap();
    let boundary = space.latest_persistence_sequence().unwrap();
    assert!(space.delete(id).unwrap());

    let delta = space.persistence_delta(None).unwrap();

    assert_eq!(delta.boundary, boundary);
    assert_eq!(delta.tuples, vec![tuple]);
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
    create_legacy_database(&db, &[&high, &low]);

    let space = Space::open(&db).unwrap();
    let migrated = space.persistence_delta(None).unwrap();
    assert_eq!(migrated.boundary, 2);
    assert_eq!(migrated.tuples, vec![low.clone(), high.clone()]);
    assert_eq!(space.legacy_persistence_sequence(low.id).unwrap(), Some(0));
    assert_eq!(space.legacy_persistence_sequence(high.id).unwrap(), Some(0));

    let post_migration_low = fact(
        RecordId::floor_at(now - Duration::days(1)),
        "post-migration-low",
    );
    space.out(post_migration_low.clone()).unwrap();
    assert_eq!(space.latest_persistence_sequence().unwrap(), 3);
    assert_eq!(
        space.legacy_persistence_sequence(high.id).unwrap(),
        Some(0),
        "legacy conversion must replay the historical baseline so a tuple the old ULID cursor skipped is recovered"
    );
    assert_eq!(
        space.persistence_delta(Some(2)).unwrap().tuples,
        vec![post_migration_low]
    );
}

#[test]
fn sequence_only_database_with_deleted_history_upgrades_without_rewinding() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("sequence-only-deletion.db");
    let surviving = fact(RecordId::floor_at(Utc::now()), "surviving-sequence-two");
    create_sequence_only_database_after_deletion(&db, &surviving);

    let space = Space::open(&db).unwrap();
    assert_eq!(space.latest_persistence_sequence().unwrap(), 2);
    assert_eq!(space.persistence_delta(None).unwrap().tuples, vec![surviving]);

    let future = fact(
        RecordId::floor_at(Utc::now() + Duration::days(1)),
        "post-upgrade-sequence-three",
    );
    space.out(future.clone()).unwrap();
    let delta = space.persistence_delta(Some(2)).unwrap();
    assert_eq!(delta.boundary, 3);
    assert_eq!(delta.tuples, vec![future]);
}

#[test]
fn sequence_only_database_recovers_from_failed_pre_journal_upgrade() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("partial-journal-upgrade.db");
    let surviving = fact(RecordId::floor_at(Utc::now()), "surviving-partial-upgrade");
    create_sequence_only_database_after_deletion(&db, &surviving);
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch(
        "CREATE TABLE tuple_persistence_events (
            commit_sequence INTEGER PRIMARY KEY,
            id TEXT NOT NULL,
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
    drop(conn);

    let space = Space::open(&db).unwrap();
    assert_eq!(space.latest_persistence_sequence().unwrap(), 2);
    assert_eq!(space.persistence_delta(None).unwrap().tuples, vec![surviving]);
}

#[test]
fn reopening_fails_closed_when_migrated_sequence_state_is_missing() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("missing-state.db");
    let tuple = fact(RecordId::floor_at(Utc::now()), "deleted-before-corruption");
    {
        let space = Space::open(&db).unwrap();
        space.out(tuple.clone()).unwrap();
        assert!(space.delete(tuple.id).unwrap());
    }
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute("DELETE FROM tuple_sequence_state", []).unwrap();
    drop(conn);

    assert!(
        Space::open(&db).is_err(),
        "an already-migrated database must not reconstruct a lower high-water mark from live rows"
    );
}

#[test]
fn insert_aborts_when_sequence_state_is_missing() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("missing-live-state.db");
    let space = Space::open(&db).unwrap();
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute("DELETE FROM tuple_sequence_state", []).unwrap();
    drop(conn);
    let tuple = fact(RecordId::floor_at(Utc::now()), "must-not-persist");

    assert!(space.out(tuple.clone()).is_err());
    let conn = rusqlite::Connection::open(&db).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM tuples WHERE id = ?1",
            [tuple.id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0, "the tuple insert must roll back with the trigger");
}

#[test]
fn sequence_overflow_aborts_without_persisting_the_tuple() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("overflow.db");
    let space = Space::open(&db).unwrap();
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute(
        "UPDATE tuple_sequence_state SET last_sequence = ?1 WHERE singleton = 1",
        [i64::MAX],
    )
    .unwrap();
    drop(conn);
    let tuple = fact(RecordId::floor_at(Utc::now()), "overflow");

    assert!(space.out(tuple.clone()).is_err());
    let conn = rusqlite::Connection::open(&db).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM tuples WHERE id = ?1",
            [tuple.id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn persistence_delta_rejects_cursor_above_sqlite_integer_range() {
    let space = Space::open_in_memory().unwrap();
    let invalid = (i64::MAX as u64) + 1;

    assert!(space.persistence_delta(Some(invalid)).is_err());
}

#[test]
fn persistence_delta_rejects_cursor_above_captured_boundary() {
    let space = Space::open_in_memory().unwrap();
    space
        .out(fact(RecordId::floor_at(Utc::now()), "one-event"))
        .unwrap();
    let boundary = space.latest_persistence_sequence().unwrap();

    assert!(space.persistence_delta(Some(boundary + 1)).is_err());
}

#[test]
fn persistence_delta_rejects_state_below_retained_journal() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("state-behind-journal.db");
    let space = Space::open(&db).unwrap();
    let tuple = fact(RecordId::floor_at(Utc::now()), "journal-high-water");
    space.out(tuple.clone()).unwrap();
    assert!(space.delete(tuple.id).unwrap());
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute(
        "UPDATE tuple_sequence_state SET last_sequence = 0 WHERE singleton = 1",
        [],
    )
    .unwrap();
    drop(conn);

    assert!(space.persistence_delta(None).is_err());
}

#[test]
fn persistence_delta_rejects_a_missing_retained_journal_event() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("journal-gap.db");
    let space = Space::open(&db).unwrap();
    space
        .out(fact(RecordId::floor_at(Utc::now()), "must-remain"))
        .unwrap();
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch(
        "DROP TRIGGER IF EXISTS tuple_persistence_events_reject_delete;
         DELETE FROM tuple_persistence_events;",
    )
    .unwrap();
    drop(conn);

    assert!(space.persistence_delta(None).is_err());
}

#[test]
fn reopening_does_not_heal_a_missing_trusted_journal_event_from_live_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("trusted-journal-gap.db");
    {
        let space = Space::open(&db).unwrap();
        space
            .out(fact(RecordId::floor_at(Utc::now()), "trusted-event"))
            .unwrap();
    }
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch(
        "DROP TRIGGER tuple_persistence_events_reject_delete;
         DELETE FROM tuple_persistence_events;",
    )
    .unwrap();
    drop(conn);

    assert!(Space::open(&db).is_err(), "trusted journal gaps must fail closed");
}

#[test]
fn failed_migration_does_not_leave_journal_schema_residue() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("atomic-migration.db");
    let surviving = fact(RecordId::floor_at(Utc::now()), "missing-state");
    create_sequence_only_database_after_deletion(&db, &surviving);
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute("DELETE FROM tuple_sequence_state", []).unwrap();
    drop(conn);

    assert!(Space::open(&db).is_err());
    let conn = rusqlite::Connection::open(&db).unwrap();
    let journal_exists: bool = conn
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM sqlite_master
                  WHERE type = 'table' AND name = 'tuple_persistence_events'
             )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!journal_exists, "failed migration DDL must roll back atomically");
}

#[test]
fn persistence_journal_rejects_insert_or_replace_of_an_existing_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("journal-replace.db");
    let space = Space::open(&db).unwrap();
    let original = fact(RecordId::floor_at(Utc::now()), "original");
    space.out(original.clone()).unwrap();
    let conn = rusqlite::Connection::open(&db).unwrap();

    let replacement = conn.execute(
        "INSERT OR REPLACE INTO tuple_persistence_events
         (commit_sequence, id, category, scope, identity, instance, lifecycle,
          payload, created_at, expires_at, strength)
         VALUES (1, 'replacement', 'fact', 'repo', 'replacement', 'castle-a',
                 'session', '{}', ?1, NULL, NULL)",
        [Utc::now().to_rfc3339()],
    );

    assert!(replacement.is_err(), "REPLACE must not mutate the immutable journal");
    assert_eq!(space.persistence_delta(None).unwrap().tuples, vec![original]);
}

#[test]
fn negative_sequence_state_is_reported_as_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("negative-state.db");
    let space = Space::open(&db).unwrap();
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch(
        "PRAGMA ignore_check_constraints = ON;
         UPDATE tuple_sequence_state SET last_sequence = -1 WHERE singleton = 1;",
    )
    .unwrap();
    drop(conn);

    assert!(space.latest_persistence_sequence().is_err());
}

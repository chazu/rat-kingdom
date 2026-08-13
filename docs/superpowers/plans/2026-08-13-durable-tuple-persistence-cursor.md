# Durable Tuple Persistence Cursor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace ULID ordering as the reactor and multiplayer sync durable cursor with a SQLite-assigned persistence sequence that is safe across delayed writers, independent database connections, process restarts, and wall-clock rollback.

**Architecture:** Add a nullable `commit_sequence` column to the local `tuples` table plus a singleton sequence state row and an `AFTER INSERT` trigger. SQLite serializes writers, so the trigger assigns sequence numbers in persistence order inside the tuple's transaction. Existing rows receive a deterministic ULID-order baseline during migration. `Pattern.after_id` remains unchanged because it is a public tuple-id/time-floor predicate, not a persistence cursor.

**Tech Stack:** Rust, SQLite/rusqlite, rk-space, rk-daemon reactor, rk-daemon multiplayer sync.

## Global Constraints

- Use test-driven development. Each production change follows a regression that fails for the expected cursor-loss reason.
- Do not change `RecordId` generation or tuple wire formats.
- Do not remove or reinterpret `Pattern.after_id`; raw clients use it as an ID/time-floor filter.
- Sequence assignment must occur inside the same SQLite transaction as tuple persistence.
- Existing databases must migrate automatically. Historical rows use deterministic `id ASC` order because original commit order cannot be reconstructed.
- Existing ULID cursor files must be read once and mapped only against the migrated historical baseline. New cursor files contain decimal sequence numbers.
- Cursor advancement remains at-least-once: save only after the full reactor or sync batch succeeds.
- Do not run `cargo fmt` and do not touch `.git-issue/`.
- Serialize Cargo with `CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1` and `-j1`.

---

### Task 1: Persistence-order storage contract

**Files:**
- Create: `crates/rk-space/tests/persistence_cursor.rs`
- Modify: `crates/rk-space/src/store.rs`
- Modify: `crates/rk-space/src/lib.rs`

**Interfaces:**
- Produces: `PersistenceDelta { boundary: u64, tuples: Vec<Tuple> }`.
- Produces: `Space::persistence_delta(after: Option<u64>) -> rk_core::Result<PersistenceDelta>`.
- Produces: `Space::latest_persistence_sequence() -> rk_core::Result<u64>`.
- Produces: `Space::legacy_persistence_sequence(id: RecordId) -> rk_core::Result<Option<u64>>`.

- [ ] **Step 1: Write the delayed-writer storage regression**

Create a lower-ID tuple, then a higher-ID tuple. Persist the higher ID first, capture the boundary, persist the delayed lower ID, and assert `persistence_delta(Some(boundary))` returns the lower-ID tuple.

- [ ] **Step 2: Write the restart storage regression**

Open a file-backed space, persist a future/high-ID tuple, capture the boundary, reopen the same database, persist a normal lower-ID tuple, and assert its persistence boundary is higher and it is replayed after the saved boundary.

- [ ] **Step 3: Run the tests and verify RED**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 cargo test -j1 -p rk-space --test persistence_cursor -- --test-threads=1
```

Expected: compilation fails because the persistence cursor API does not exist.

- [ ] **Step 4: Add the SQLite sequence migration**

Extend `tuples` with nullable `commit_sequence INTEGER`. Add `tuple_sequence_state(singleton, last_sequence, legacy_backfill_sequence)`. Under `BEGIN IMMEDIATE`, assign missing historical sequences in `id ASC` order, store the historical high-water mark, create a unique sequence index, and create an `AFTER INSERT` trigger that increments the singleton and writes the new sequence onto the inserted tuple.

- [ ] **Step 5: Add the bounded delta API**

Read the singleton boundary first, then select current tuples with `commit_sequence > after AND commit_sequence <= boundary ORDER BY commit_sequence ASC`. Return the captured boundary even when rows were deleted, so consumers can advance past vanished tuples without rescanning forever.

- [ ] **Step 6: Add legacy cursor mapping**

Map an old ULID cursor to `MAX(commit_sequence)` only among rows at or below `legacy_backfill_sequence` whose tuple ID is at or below the old cursor. Never include post-migration rows in this conversion, because a delayed low-ID write must remain replayable.

- [ ] **Step 7: Run storage tests and existing migration tests**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 cargo test -j1 -p rk-space --test persistence_cursor --test sdlc_storage --test sdlc_deployment -- --test-threads=1
```

Expected: all tests pass.

- [ ] **Step 8: Commit**

```bash
git add crates/rk-space/src/store.rs crates/rk-space/src/lib.rs crates/rk-space/tests/persistence_cursor.rs
git commit -m "feat(space): add persistence-order cursor"
```

---

### Task 2: Reactor sequence cursor

**Files:**
- Modify: `crates/rk-daemon/src/reactor.rs`
- Modify: `crates/rk-daemon/tests/reactor.rs`

**Interfaces:**
- Consumes: `Space::persistence_delta`, `Space::latest_persistence_sequence`, and `Space::legacy_persistence_sequence`.
- Produces: decimal sequence content in `$RK_HOME/reactor-cursor`.

- [ ] **Step 1: Write deterministic delayed-writer reactor regression**

Persist a nonmatching future/high-ID boundary tuple and run a cycle. Then persist a matching lower-ID ping and assert the next cycle fires exactly once. This must use explicit IDs and no sleeps.

- [ ] **Step 2: Write restart/high-cursor reactor regression**

Use a file-backed space. Persist a future/high-ID tuple, initialize the reactor cursor, reopen the database, persist a normal matching tuple, and assert it fires after restart.

- [ ] **Step 3: Verify RED**

Run the two exact tests. Expected: the delayed/restarted lower-ID tuple is skipped by the current ULID cursor.

- [ ] **Step 4: Replace reactor ULID cursor use**

Baseline with `latest_persistence_sequence`. Load decimal cursor files directly. For legacy ULID files, call `legacy_persistence_sequence`. Process `persistence_delta(cursor).tuples`, and save the returned boundary only when no retryable failure occurred.

- [ ] **Step 5: Remove the temporary millisecond sleep**

Delete the 2ms isolation sleep from `fresh_obstacle_on_resolved_topic_steers_and_reinforces`; persistence-order sequencing now makes same-millisecond ULID suffixes irrelevant.

- [ ] **Step 6: Verify GREEN**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 cargo test -j1 -p rk-daemon --test reactor -- --test-threads=1
```

Expected: all reactor tests pass.

- [ ] **Step 7: Commit**

```bash
git add crates/rk-daemon/src/reactor.rs crates/rk-daemon/tests/reactor.rs
git commit -m "fix(reactor): cursor by persistence sequence"
```

---

### Task 3: Multiplayer sync sequence cursor

**Files:**
- Modify: `crates/rk-daemon/src/sync.rs`

**Interfaces:**
- Consumes: the same rk-space persistence cursor APIs.
- Produces: decimal sequence content in `$RK_HOME/sync-cursor`.

- [ ] **Step 1: Write deterministic delayed-writer sync regression**

Run one local cycle after a higher-ID locally authored durable tuple to advance the cursor. Persist a lower-ID locally authored durable tuple afterward, run another cycle, and assert one new `SyncOp::Out` is exported.

- [ ] **Step 2: Verify RED**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 cargo test -j1 -p rk-daemon sync::tests::delayed_lower_id_local_tuple_is_exported -- --test-threads=1
```

Expected: exported count is zero under the old ULID cursor.

- [ ] **Step 3: Replace sync ULID cursor use**

Capture the persistence delta at cycle start. Keep the whole-space snapshot for take detection and current liveness, but select export candidates by delta membership, local castle ownership, durable lifecycle, and presence in the current live-ID set. After local notes append succeeds, save the delta boundary even when the batch contained only remote, ephemeral, or deleted events.

- [ ] **Step 4: Add legacy cursor conversion**

Read decimal sequence files directly. Map legacy ULID files through `Space::legacy_persistence_sequence`. Keep the existing atomic temp-file rename for writes.

- [ ] **Step 5: Verify GREEN**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 cargo test -j1 -p rk-daemon sync::tests -- --test-threads=1
```

Expected: all sync tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/rk-daemon/src/sync.rs
git commit -m "fix(sync): cursor by persistence sequence"
```

---

### Task 4: Compatibility, stress, review, and full verification

**Files:**
- Modify if needed: `docs/factory-foreman.md`
- Test: `crates/rk-space/tests/persistence_cursor.rs`
- Test: `crates/rk-daemon/tests/reactor.rs`
- Test: `crates/rk-daemon/src/sync.rs`

**Interfaces:**
- Verifies all public and persisted cursor contracts.

- [ ] **Step 1: Add legacy database migration acceptance**

Create an old `tuples` schema without `commit_sequence`, insert rows out of construction order, open through `Space`, and assert deterministic historical backfill plus a newer sequence for the first post-migration insert.

- [ ] **Step 2: Add legacy cursor-file acceptance**

Write ULID-form reactor and sync cursor files over a migrated database and verify each consumer resumes from the historical floor, then rewrites its cursor as decimal sequence text.

- [ ] **Step 3: Stress focused regressions**

Run the storage delayed-writer/restart tests, reactor delayed-writer/restart tests, and sync delayed-writer test repeatedly under serialized Cargo.

- [ ] **Step 4: Run independent read-only review**

Review migration atomicity, trigger behavior under independent opens, rollback behavior, deleted-row boundary advancement, legacy cursor conversion, reactor at-least-once semantics, and sync non-resurrection semantics.

- [ ] **Step 5: Run full fail-fast verification**

```bash
set -euo pipefail
export CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1
cargo build -j1 --workspace
env -u RK_AGENT cargo test -j1 --workspace -- --test-threads=1
cargo clippy -j1 --workspace --all-targets -- -D warnings -A clippy::unnecessary_get_then_check
git diff --check
```

Expected: exit code 0, with only the intentionally untouched `.git-issue/` untracked.

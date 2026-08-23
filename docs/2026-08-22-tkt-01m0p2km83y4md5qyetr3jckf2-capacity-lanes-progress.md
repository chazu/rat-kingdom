# TKT-01M0P2KM83Y4MD5QYETR3JCKF2: implementation/verification/review capacity lanes

## Status: complete

Generalizes the existing fleet-wide `[drain] max_wip` ceiling
(`Registry::try_reserve_wip`, no repo dimension, exploitable by one repo) into
a **repo-scoped, per-lane, durably-FIFO-ordered** admission model, additive to
the existing ceiling rather than replacing it — every pre-existing
fleet-wide-ceiling test still passes unchanged. Merged forward twice against a
moving `main` (through the managed-verification cancellation/restart-reap
rework and the landing-dedup fix) with no semantic conflicts remaining.

## What landed

- **`crates/rk-core/src/config.rs`**: `PolicyConfig` gains
  `implementation_admission_limit`/`_by_repo` and
  `review_admission_limit`/`_by_repo`, matching the existing
  `verification_admission_limit`/`_by_repo` convention (`0` = disabled
  fleet-wide default, per-repo `BTreeMap` override). `rat-kingdom: 2` is
  seeded into `implementation_admission_limit_by_repo`'s default per this
  ticket's explicit throughput-program requirement.

- **`crates/rk-daemon/src/agents.rs`**: `Lane` enum (`Implementation`/
  `Review`, `Lane::for_role` splits on `role == "reviewer"`) and
  `Registry::{live_or_reserved_lane_wip, try_reserve_lane_wip,
  release_lane_wip}`, keyed by `(repo_name, lane)`. Occupancy is recomputed
  from `AgentRecord.state.is_live()` (already durable in `agents.json`), so
  restart durability/idempotence for the reservation counters themselves is
  inherited for free — the same mechanism the pre-existing fleet-wide
  ceiling already relies on.

- **Durable FIFO wait-record** (`crates/rk-daemon/src/agents.rs`): a
  `LaneWaiter { repo, lane_tag, key, requested_at, last_seen }` durably
  persisted to `lane_waiters.json` (same atomic-write discipline as
  `agents.json`), one entry per distinct `(repo, lane, key)` currently
  refused. `try_reserve_lane_wip` enforces REAL FIFO admission order, not
  just FIFO reporting: a freed slot admits the queue's head first, refusing
  even a technically-fits new arrival that hasn't waited as long — enforced
  inside the exact same registry-lock critical section every other admission
  decision already serializes on, so this added zero new locking risk.
  - **Idempotent**: a repeat refusal for an already-queued key updates
    `last_seen` (a liveness heartbeat) instead of growing the queue; the
    heartbeat itself is throttled to at most once per
    `LANE_WAIT_HEARTBEAT_PERSIST_SECS` (60s) so a caller retrying every
    ~250ms does not turn into a disk write several times a second.
  - **Durable across restart**: the wait-queue survives a daemon restart
    (`lane_waiters.json`), including which entry is at the head — a
    newcomer that only shows up after the restart still loses to a waiter
    the predecessor process recorded before it died.
  - **Stale-eviction**: an entry that hasn't been retried (heartbeat via
    `last_seen`, deliberately NOT `requested_at`) in `LANE_WAIT_STALE_SECS`
    (10 minutes) is evicted, so an abandoned caller (crashed, cancelled
    ticket, dismissed workflow instance) cannot permanently jam the lane for
    everyone behind it. Staleness is judged on liveness, not total wait
    time, so an actively-retrying waiter never loses its place no matter how
    long the overall wait has been.
  - **Fails closed on a persistence error**: if the durable clear-on-admit
    write fails, the admission attempt is refused (not silently allowed to
    proceed on an undurable record) and the failure is logged — an admitted
    spawn whose queue-clear couldn't be persisted would otherwise leave a
    phantom head-of-queue entry that outlives its own admission, wrongly
    blocking every real waiter behind it until the 10-minute stale-eviction
    caught it.
  - `Registry::lane_wait_stats(repo, lane) -> (count, oldest_wait_secs)` for
    observability.

- **`crates/rk-daemon/src/supervisor.rs`**: `LaneLimits` (config-side
  default+override storage, mirrors `VerificationAdmission`'s own) backs two
  new `Supervisor` fields (`implementation_admission_limits`,
  `review_admission_limits`) with `set_*`/`lane_admission_limit_for`
  accessors. `Supervisor::spawn` acquires the lane reservation (with a
  caller-stable waiter key — `workflow:<instance>:<task>` or `<role>:<task>`
  — so a caller's own retries hold their queue place instead of re-queuing)
  in the SAME atomic critical section as the existing fleet-wide
  `try_reserve_wip` call, and releases it on every existing release path
  (onboarder-validation failure, insert failure, and the success path once
  the row goes live) — zero new failure paths were introduced; the existing
  ones already cover every terminal/failed-launch case for the fleet-wide
  ceiling. New `IMPLEMENTATION_LANE_REFUSED`/`REVIEW_LANE_REFUSED` error
  strings; `Supervisor::capacity_summary()` for observability (below).

- **`crates/rk-daemon/src/workflow_exec.rs`**: `is_fleet_wip_refusal` now also
  matches the two new refusal strings, so drain's ticket-reopen handling and
  workflow `Step::Spawn`/`for_each` fan-out's retry-poll loop both handle a
  lane refusal identically to a fleet-wide refusal — **zero call-site
  changes** needed in `drain.rs` or `workflow_exec.rs`'s spawn/retry logic
  itself.

- **`crates/rk-daemon/src/server.rs`**: `Daemon::new` wires the four new
  config limits into the supervisor (mirrors the existing
  `set_verification_admission_limits` call); `#[doc(hidden)]` test hooks
  (`set_implementation_admission_limits`/`set_review_admission_limits`)
  mirror `set_min_free_disk_gb` for daemons built via
  `new_in_memory`/`with_space_for_tests`, which bypass `Daemon::new`'s config
  wiring. `Server::status()` gains a `capacity` field: per-repo, per-lane
  `{limit, occupied/in_flight, waiting_count, oldest_wait_secs,
  waiting_reason}` for every repo with either an explicit override or a live
  agent. `waiting_reason` fires on raw occupancy OR a non-empty durable
  queue — the brief window right after a slot frees but before its head has
  retried to claim it is still logically full, not idle.

- **`crates/rk-cli/src/{top.rs, main.rs, observe.rs}`**: `rk top`'s header,
  `rk daemon status`, and `rk digest`'s printed report all surface saturated
  lanes with queue depth and oldest-wait age (silent when nothing is
  saturated — most fleets run with lanes disabled).

- **Operator ticket dispatch is now ALSO capped** (a real, deliberate
  behavior change): `Server::handle_spawn` (`agent.spawn` RPC) previously
  passed `fleet_wip_cap = 0` — deliberately exempt from the fleet-wide
  ceiling. The new per-repo lane check lives inside `Supervisor::spawn`
  itself, unconditional on that pre-existing exemption, so operator dispatch
  is now bound by `implementation_admission_limit_by_repo` too. This closes a
  real pre-existing gap (one repo's operator-driven spawns could never be
  scoped away from another's) and satisfies the ticket's explicit "operator
  ticket dispatch acquire one atomic implementation-lane reservation"
  requirement. Unlike drain and workflow `spawn`/`for_each`, a direct
  `agent.spawn` RPC call has NO built-in retry loop of its own — a refusal is
  a plain terminal error to whatever called it, by design (it is a
  synchronous request/response API, not a queue a caller hands work to). A
  caller that wants "wait for a slot" behavior retries on the
  `"... lane at capacity ..."` error text itself, the same way drain and the
  workflow engine do internally — proven directly in the load test below.

## Tests

`crates/rk-daemon/src/supervisor.rs` (`respawn_tests` module):
- `implementation_lane_is_scoped_per_repo` — atomic-concurrency admission
  (multi-thread runtime, real lock contention) plus cross-repo independence.
- `implementation_lane_saturation_does_not_starve_the_review_lane`.
- `implementation_lane_occupancy_survives_a_restart`.
- `implementation_lane_admits_the_longest_waiting_request_first` — real FIFO
  admission order: a second waiter that retries FIRST once capacity frees
  must still lose to a first waiter that was refused earlier.
- `implementation_lane_wait_order_survives_a_restart` — the durable queue's
  ordering, not just occupancy, survives a daemon restart: a brand-new
  post-restart request still loses to a waiter the predecessor process
  recorded before it died.
- `implementation_lane_refuses_admission_rather_than_silently_lose_durable_queue_order`
  — forces `lane_waiters.json` to be unwritable (chmod) and proves admission
  fails closed rather than silently proceeding on an undurable clear.

`crates/rk-daemon/tests/capacity_lanes_dispatch_load.rs` (real daemon over a
socket): drives a genuine implementation burst against ONE repository through
all three real dispatch callers simultaneously — drain's own refill loop, a
workflow `for_each` fan-out, and direct operator `agent.spawn` calls, all
competing for the same six-ticket pool / lane cap of 2 — while a concurrent
reviewer spawn proves the review lane (cap 1) is never starved, AND a
concurrent operator `verify.run` call proves the wholly independent
verification lane (cap 1) is never starved either. Asserts: peak live
non-reviewer agents for the repo never exceeds 2 across all three
implementation paths at once; the reviewer starts within 10s despite the
burst; the `verify.run` call — spawned as its own task the moment the burst
begins, racing the drain/fan-out/operator saturation for real, not run after
it settles — starts and completes within 10s, returns `verdict: "pass"`
/`exit: 0`, and its check script (which appends to a log file on every
execution) proves it ran EXACTLY once; the fan-out still completes; every
ticket is claimed exactly once (no stranding, no double-claim between drain
and the fan-out sharing one pool); and the final agent count is exactly 9 (6
tickets + 2 operator + 1 reviewer) — no duplicate launches anywhere,
including from the concurrent verification run, which is a managed check
execution and must never itself produce an agent record. That final count is
read only after the poll loop's break condition requires the fan-out, the
reviewer, AND the verify.run call to have ALL settled, so the assertion is
race-robust rather than a lucky snapshot taken while one of the three
concurrent proofs might still be mid-flight. Stable across repeated runs.

Closing this gap needed one small addition beyond the test itself:
`Daemon::set_verification_admission_limits` (`crates/rk-daemon/src/server.rs`),
a `#[doc(hidden)]` test-only hook mirroring the pre-existing
`set_implementation_admission_limits`/`set_review_admission_limits` ones —
`Daemon::with_space_for_tests` bypasses `Daemon::new`'s
`config.policy.verification_admission_limit*` wiring, and no such hook
previously existed for an integration test (as opposed to an in-module unit
test with direct field access) to configure the verification lane's cap
without going through a full `config.cue`.

All of the above pass, plus the full pre-existing `respawn_tests` /
`verification_admission_tests` / managed-verification-cancellation /
restart-reap suites (`workflow_exec::tests::{cancelling_a_managed_*,
reap_stale_managed_children_*, timeout_kills_*, process_signature_survives_*}`)
with zero regressions, and the full `crates/rk-daemon/tests/
managed_verification_cancel_e2e.rs` (10/10, real daemon + real spawned `rk`
CLI child processes) — proving the new lanes and the managed-verification
cancellation/reap machinery landed on `main` concurrently with this branch
compose correctly, not just compile together.

## Known scope notes (not blocking, informational)

- The verification lane's own admission (`VerificationAdmission`, landed by
  the prerequisite ticket) is a `tokio::sync::Semaphore`, independently
  proven FIFO. It does not track queue depth/age the way the new durable
  `Registry` wait-queue does for the implementation/review lanes, so
  `capacity_summary`'s `verification` entry exposes `waiting_reason` but not
  `waiting_count`/`oldest_wait_secs`. Not changed here — it already has a
  stronger fairness guarantee (provable FIFO) than the check-then-refuse
  implementation/review lanes, just a different observability shape.
- Onboarder-role spawns count against the implementation lane like any other
  non-reviewer role; not explicitly named in the acceptance criteria, noted
  here in case an operator finds that surprising.

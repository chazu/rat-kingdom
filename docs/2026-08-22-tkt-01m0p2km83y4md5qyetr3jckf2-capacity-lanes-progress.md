# TKT-01M0P2KM83Y4MD5QYETR3JCKF2: implementation/verification/review capacity lanes — checkpoint

## Status: paused for operator handoff, NOT yet verified/landed

A blocking defect in a prerequisite (interrupting Gorgonzola-11 did not cancel
its daemon-owned managed `rk verify` process) was reproduced live during this
dispatch. Per operator instruction, this branch is committed but deliberately
**not** run through `rk verify`/`mise run verify` and **not** `rk done`d —
that repair must land first. The full `cargo test --workspace` (spawn env
stripped) suite was run directly (not through the daemon's managed verify
path) and passed clean: exit 0, 0 failures, across the whole workspace.

## What landed on this branch

Generalizes the existing fleet-wide `[drain] max_wip` ceiling
(`Registry::try_reserve_wip`, no repo dimension, exploitable by one repo) into
a **repo-scoped, per-lane** admission model, additive to it rather than
replacing it — every existing fleet-wide-ceiling test still passes unchanged.

- **`crates/rk-core/src/config.rs`**: `PolicyConfig` gains
  `implementation_admission_limit`/`_by_repo` and
  `review_admission_limit`/`_by_repo`, matching the existing
  `verification_admission_limit`/`_by_repo` convention (`0` = disabled
  fleet-wide default, per-repo `BTreeMap` override). `rat-kingdom: 2` is
  seeded into `implementation_admission_limit_by_repo`'s default per this
  ticket's explicit throughput-program requirement.
- **`crates/rk-daemon/src/agents.rs`**: new `Lane` enum
  (`Implementation`/`Review`, `Lane::for_role` splits on `role == "reviewer"`)
  and `Registry::{live_or_reserved_lane_wip, try_reserve_lane_wip,
  release_lane_wip}`, keyed by `(repo_name, lane)` — mirrors
  `try_reserve_wip`/`release_wip` exactly, but scoped. Occupancy is
  recomputed from `AgentRecord.state.is_live()` (already durable in
  `agents.json`), so restart durability/idempotence is inherited for free,
  same mechanism the pre-existing fleet-wide ceiling already relies on.
- **`crates/rk-daemon/src/supervisor.rs`**: `LaneLimits` (config-side
  default+override storage, mirrors `VerificationAdmission`'s own) backs two
  new `Supervisor` fields (`implementation_admission_limits`,
  `review_admission_limits`) with `set_*`/`lane_admission_limit_for`
  accessors. `Supervisor::spawn` acquires the lane reservation in the SAME
  atomic critical section as the existing fleet-wide `try_reserve_wip` call,
  and releases it on every existing release path (onboarder-validation
  failure, insert failure, and the success path once the row goes live) —
  zero new failure paths were introduced; the existing ones already cover
  every terminal/failed-launch case for the fleet-wide ceiling. New
  `IMPLEMENTATION_LANE_REFUSED`/`REVIEW_LANE_REFUSED` error strings; new
  `Supervisor::capacity_summary()` for observability (below).
- **`crates/rk-daemon/src/workflow_exec.rs`**: `is_fleet_wip_refusal` now also
  matches the two new refusal strings, so drain's ticket-reopen handling and
  workflow `Step::Spawn`/`for_each` fan-out's retry-poll loop both handle a
  lane refusal identically to a fleet-wide refusal — **zero call-site changes**
  needed in `drain.rs` or `workflow_exec.rs`'s spawn/retry logic itself.
- **`crates/rk-daemon/src/server.rs`**: `Daemon::new` wires the two new config
  limits into the supervisor (mirrors the existing
  `set_verification_admission_limits` call). `Server::status()` gains a
  `capacity` field: per-repo, per-lane `{limit, occupied/in_flight,
  waiting_reason}` for every repo with either an explicit override or a live
  agent.
- **`crates/rk-cli/src/{top.rs, main.rs, observe.rs}`**: `rk top`'s header,
  `rk daemon status`, and `rk digest`'s printed report all surface saturated
  lanes (silent when nothing is saturated — most fleets run with lanes
  disabled).
- **Tests** (`crates/rk-daemon/src/supervisor.rs`, `respawn_tests` module):
  `implementation_lane_is_scoped_per_repo` (atomic-concurrency + cross-repo
  independence, same shape as the existing
  `fleet_wip_admission_is_atomic_under_concurrent_spawns`),
  `implementation_lane_saturation_does_not_starve_the_review_lane`,
  `implementation_lane_occupancy_survives_a_restart`. All pass; full
  `cargo test --workspace` (env-stripped) passes with 0 failures.

## Why "operator ticket dispatch" is now ALSO capped (a real behavior change)

`Server::handle_spawn` (`agent.spawn` RPC, backing manual/foreman dispatch)
previously passed `fleet_wip_cap = 0` — deliberately exempt from the
fleet-wide ceiling. The new per-repo lane check lives inside
`Supervisor::spawn` itself, unconditional on that pre-existing exemption, so
operator dispatch is now bound by `implementation_admission_limit_by_repo`
too (default `0` = unaffected unless configured, but `rat-kingdom: 2` IS
configured by default). This was necessary to satisfy the ticket's explicit
"operator ticket dispatch acquire one atomic implementation-lane reservation"
requirement, and closes a real pre-existing gap (one repo's operator-driven
spawns could never be scoped away from another's).

## Known limitations / deferred (not blocking, but should become follow-up tickets)

1. **No literal FIFO queue for the implementation/review lanes.** Unlike
   `VerificationAdmission`'s `tokio::sync::Semaphore` (provably FIFO), a lane
   refusal here is a plain check-then-refuse; a refused drain/workflow caller
   retries via its own existing poll loop, which is not strictly
   first-refused-first-admitted under contention. This is the SAME level of
   ordering guarantee the pre-existing fleet-wide ceiling already has —
   not a regression, but also not literally the "queue order is durable" the
   ticket's wording might imply if read strictly.
2. **`rk digest`/`rk top`/`rk status` surface capacity but not "queue age".**
   There is no durable record of *how long* a specific ticket/instance has
   been waiting on a lane refusal (drain just reopens the ticket; workflow
   sets `awaiting` but that's not timestamped as a distinct wait episode).
   `waiting_reason` is exposed; wait duration is not.
3. **No exhaustive end-to-end load test** driving drain + workflow fan-out +
   operator dispatch simultaneously against one repo's lane and asserting the
   verification/review lane still starts within a bound under that burst.
   What's proven instead: the authoritative admission primitive
   (`Supervisor::spawn`/`Registry`) is correct in isolation (all three
   callers funnel through it unmodified), lane independence, and restart
   durability — not a full daemon-integration burst test exercising all
   three real dispatch code paths at once.
4. Onboarder-role spawns count against the implementation lane like any other
   non-reviewer role; not explicitly called out in the acceptance criteria,
   noted here in case an operator finds that surprising.

Filed forward as a follow-up ticket (see `rk ticket new` at handoff) rather
than pursued further in this dispatch, per operator's stop instruction.

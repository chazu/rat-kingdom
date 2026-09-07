# TKT-kujaj-libar-mopud: `implementation_lane_admits_the_longest_waiting_request_first` flake under host load

## Symptom

While landing the doc-only branch
`rat/cornflower-13/research-maki-agent-harness-chatgpt`, the protected-main
steward gate ran `mise run verify-full` and failed only on
`supervisor::respawn_tests::implementation_lane_admits_the_longest_waiting_request_first`.
The candidate had no Rust changes and `verify-changed` had already passed;
the branch was held unmerged until a human forced an audited bypass.

## Reproduction

The failure reproduces directly by adding concurrent host CPU/process load
(several `yes > /dev/null` busy-loops, plus 6-8 copies of the same test
binary running the affected test repeatedly and concurrently) and looping
the compiled test binary. Baseline (no induced load): 0 failures in ~300
runs. Under induced load: reproduced reliably, e.g. 2 failures in 320 runs
and 5 failures in 480 runs across two separate stress sessions, always at
the same assertion:

```
thread '...' panicked at crates/rk-daemon/src/supervisor.rs:10022:9:
assertion failed: matches!(&first_refusal, Err(e) if e.to_string() ==
    IMPLEMENTATION_LANE_REFUSED)
```

Instrumenting the test to print the `occupying` record's state at the
moment of failure showed `occupying_state=Some(Failed)` — the record had
already terminalized before the test's own very next `spawn_async` call.

## Root cause: test-fixture race, not a bug in the admission logic

This is **not** a race in the implementation-lane FIFO admission code under
test (`Registry::try_reserve_lane_wip`, `agents.rs`). That logic is a pure,
synchronous, mutex-guarded decision over an in-memory `Vec<LaneWaiter>` plus
live-agent counts — nothing in it depends on wall-clock timing for
correctness, and it behaved exactly as designed given the actual (buggy)
state it was asked to admit against.

The bug is in the test fixture. `spawn_async(..., "occupying", ...)` uses
the `"fake"` harness (`crates/rk-harness/src/fake.rs`), which launches a
**real `bash -c` subprocess** running `FakeHarness::DEFAULT_SCRIPT`: it
prints a `Started` line, blocks on `read -r` until steered, then prints its
`Completed` result and exits. `Supervisor::spawn` (supervisor.rs:1842-1850)
hands that session's event stream to a `tokio::spawn`ed background task
that calls `handle_event` on every event — including `Completed`/`Exited`,
which terminalize the record (`Completed` on a clean run, or `Failed` if
the child exits without ever reporting a `Completed` event, e.g. a
resource-starved fork/exec or pipe hiccup under heavy host load).

The test assumes `occupying` stays live (`Running`) from the moment it is
spawned until the test's own explicit
`sup.lock_registry().update(&occupying.name, |r| r.state =
AgentState::Completed)` call several `.await` points later — but nothing
enforces that. The fake harness's background completion is a real,
independently-scheduled subprocess lifecycle racing the test's own async
task on a `current_thread` Tokio runtime: under host contention the test
task can be scheduled less favorably than the child process + IO reactor,
letting the background event task terminalize `occupying` (to `Failed`,
observed under fork/process-table pressure) before the test's next
`spawn_async` call. Once `occupying` is no longer live, the lane's only
slot is free, and `try_reserve_lane_wip` correctly *admits* the very
request the test expected it to refuse — hence the assertion failure.

The same pattern (spawn a fake-harness-backed "occupying" agent, then
depend on it staying live across one or more further `.await` points
before manually terminalizing it) existed in four tests in
`crates/rk-daemon/src/supervisor.rs`:

- `implementation_lane_admits_the_longest_waiting_request_first` (two
  windows: before `first_refusal` and before `second_refusal`, **plus** a
  third window after `first_retry` is admitted and before `second_again`
  — the newly-admitted `first_retry` record has the exact same fake-harness
  race against its own background completion)
- `implementation_lane_wait_order_survives_a_restart`
- `implementation_lane_refuses_admission_rather_than_silently_lose_durable_queue_order`
- `implementation_lane_saturation_does_not_starve_the_review_lane`

The last one was not part of the originally observed failure but reproduces
under the same synthetic load (confirmed directly) and shares the identical
mechanism, so it was fixed alongside the reported test rather than left for
a separate ticket.

## Fix

Each affected test now pins the "occupying" record's state back to
`Running` (via the same `sup.lock_registry().update(...)` call the test
already uses to *free* the slot later) immediately after spawning it and
again after any later admission the test expects to keep occupying the
lane, closing every window where the fake harness's own background
completion could race ahead of the test. This is a test-only change — no
production admission-control code was touched. Verified clean: 2400+
iterations of the affected tests under sustained synthetic host load (6
CPU-saturating busy loops + 6-8 concurrent copies of the test binary) with
zero failures post-fix, versus a reproducible failure rate of roughly
1-2% under the same load before the fix.

## Policy for unrelated pre-existing flaky gate failures

This instance turned out to be a fixable test bug, not a genuine gate
failure or a fundamental host-contention limitation — so no change to the
daemon-native landing gate (`crates/rk-daemon/src/landing.rs`) or its
retry logic was needed here. The existing fleet convention
(`preexisting-failure-is-a-ticket-not-an-inline-fix`, TKT-43) already
covers the *reporting* half correctly. What it does not yet cover is the
steward gate's own behavior when a `verify`/`verify-full` check hold fires
for a failure with **no plausible relationship to the candidate's diff**
(here: a doc-only branch, `verify-changed` green, failure confined to an
unrelated pre-existing test): today that still requires an operator's
audited bypass to unstick, same as a genuine regression.

`managed_verification.rs` already has precedent for a narrowly-scoped,
signature-matched automatic retry that does not weaken genuine-failure
detection: `is_cargo_target_contention_signature` retries a check exactly
once when stdout/stderr match an exact, unambiguous ENOENT signature
specific to the shared `CARGO_TARGET_DIR` race (see
`docs/2026-08-19-tkt-hot-scan-target-dir-contention.md`), and the
`"infra"` verdict (child died to a signal/runner-loss, not a real exit
code) already gets one bounded automatic retry in
`LandingPipeline::run_gates_at` before a hold. Extending a similarly
narrow, signature-scoped retry to specific, fleet-known-flaky **ordinary
"fail" verdicts** (not just "infra" ones) is a real design decision with
fleet-wide blast radius — it changes what "the gate said no" means for
every repo using the named `verify`/`verify-full` checks — and needs
explicit operator sign-off rather than a unilateral change inside one
task's diff. Filed as a follow-up decision rather than implemented here;
see the linked ticket for options.

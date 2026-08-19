# TKT-01M0CK4Z019SMBN9CTCZBYCTKX: invocation-source tagging is blocked, and its own premise needs revisiting

## Status: blocked, not implemented

This ticket asked, once TestExecLock ships and is live, to tag ENOENT-signature
check failures (`could not execute process ... (never executed) ... No such
file or directory`) with their invocation source — `run_check_in`-mediated
(covered by TestExecLock) vs. "external/unknown" (plausibly a rat's own
self-driven `mise run verify`) — then use a deploy cycle of that data to decide
whether the fleet-wide PATH-wrapper design (TKT-01M0CJY73NHNXNE3PTAY86033B) is
worth building.

Verified against the live repo state at merge-base `34ffda5` (2026-08-19):

- `crates/rk-daemon/src/supervisor.rs` has no `TestExecLock` (or any lock name
  matching `ExecLock`) — `grep -rln "TestExecLock\|test_exec_lock\|ExecLock"
  crates/` is empty. TKT-01M0CFA1RX36SJ7DV4YWGHQ9BT (TestExecLock) is still
  `in_progress`, not merged.
- `grep -rn CARGO_TARGET_DIR crates/` is also empty. `shared_cargo_target`
  is not live on `main` (matches the standing finding in
  TKT-01M0CHX9N81PMD2T6ES9YGZXD6), and per that ticket its adoption is an open
  operator decision, not a foregone conclusion — gate-worktree
  retention/pruning already fixed the disk-exhaustion symptom a different way.

So the ticket's own stated precondition ("once TestExecLock ships") is unmet,
and there is nothing to instrument yet. That much was already known when this
ticket was filed.

## New finding: the "external/unknown" bucket has no data source, before or after TestExecLock

Traced every call site of `record_gate_failure`, the only place a check's
non-pass verdict is durably recorded (`crates/rk-daemon/src/workflow_exec.rs`,
`grep -rn record_gate_failure crates/rk-daemon/src`): both calls are inside
`WorkflowEngine::run_check_in` itself (workflow_exec.rs:2967, :3032). Nothing
else in the daemon calls it. `run_check_in` in turn is only reached from a
workflow `run` step and the daemon-native landing-pipeline gate
(`landing.rs`) — i.e. every structured check-failure record the daemon can
ever produce is *already* `run_check_in`-mediated, today and after
TestExecLock lands.

A rat's own self-driven `mise run verify` (completion-protocol step 3) runs as
a descendant of the harness process, inside the rat's own bash tool session.
Its failures are not reported to the daemon as a check result at all — there
is no wrapper, hook, or event that turns a self-driven test failure into
anything the daemon can record. TKT-01M0CJY73NHNXNE3PTAY86033B already says
this in different words ("the daemon has no subprocess-level hook into it
beyond the env/PATH it injected at spawn time"), but the implication for
*this* ticket specifically wasn't spelled out: the "external/unknown
invocation" bucket this ticket wants to tag and count will read zero forever,
by construction, regardless of how much real contention exists — not because
self-driven contention doesn't happen, but because the daemon has no channel
to see it. A deploy cycle of TestExecLock would only ever show `run_check_in`
recurrences trending down (the part TestExecLock already covers) with no
signal at all about the self-driven part it doesn't.

## Implication

Tagging ENOENT failures by invocation source, as scoped, cannot produce the
comparison this ticket was meant to enable. Either:

1. The self-driven bucket needs its own reporting channel first (e.g. a rat
   that hits the ENOENT signature during its own `mise run verify` files an
   `rk obstacle`/artifact with the signature text, so it becomes countable
   without building the full PATH-wrapper) — a much smaller, non-fleet-wide
   piece of instrumentation than the deferred wrapper design, or
2. The decision in TKT-01M0CJY73NHNXNE3PTAY86033B gets made without this data:
   reason from the structural argument already in that ticket (every rat's
   own verify and the automated steward gate both hit the same shared target
   dir, clustered in time; self-driven is very plausibly not rare) rather than
   waiting on telemetry that can't arrive.

Not picking either option here — that's a design call for whoever owns
TKT-01M0CJY73NHNXNE3PTAY86033B, not something to fold into this already-narrow
ticket. This doc exists so the next agent to pick this up does not have to
re-derive the `record_gate_failure` call-site trace from scratch.

## What's still true from the original ticket

TestExecLock and shared_cargo_target still need to land before there is even
a `run_check_in`-mediated baseline to compare against. That half of the
original blocker stands unchanged.

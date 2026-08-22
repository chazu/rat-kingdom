# TKT-01M0CK4Z019SMBN9CTCZBYCTKX: revisited post-TestExecLock — still nothing to tag, and now structurally so

## What's changed since the last pass

`docs/2026-08-19-tkt-ck4z-invocation-source-tagging-blocked.md` (Nubbin-9,
merge-base `34ffda5`) found this ticket's own precondition unmet: neither
`TestExecLock` (TKT-01M0CFA1RX36SJ7DV4YWGHQ9BT) nor `shared_cargo_target`
existed on `main`.

Re-verified against current `main` (`2d438bb`):

- `TestExecLock` **has** landed (`246967a`, `crates/rk-daemon/src/supervisor.rs`
  — `struct TestExecLock`, `acquire_test_exec_lock`, wired into
  `WorkflowEngine::run_check_in` at `crates/rk-daemon/src/workflow_exec.rs:3114`).
- `[disk] shared_cargo_target` also landed, but **defaults to `false`**
  (`f4df009`, `TKT-01M0EXYHV1GR9Z75QSS42HXBVK`) and nothing on `main` — no
  config file, no call site — flips it to `true`. `Supervisor::new` sets
  `shared_cargo_target: AtomicBool::new(false)` (`supervisor.rs:961`); the only
  way it becomes `true` at runtime is `Server`'s `set_shared_cargo_target`
  reading it from `config.disk.shared_cargo_target`
  (`server.rs:310`), and that config value itself defaults off.

So "once both land" is now true; "**and are enabled in the live daemon**" is
not. `run_check_in` already has its own free-retry for exactly this
ENOENT/os-error-2 signature
(`is_cargo_target_contention_signature`, `workflow_exec.rs:298`) gated on
`resolved.shared_cargo_target && self.supervisor.shared_cargo_target_enabled()`
— with sharing off by default, that branch is dead in the default
configuration, and there is no cross-process `CARGO_TARGET_DIR` contention
left to produce the signature at all, gated or self-driven. This matches
the parent ticket's own branch-level conclusion
(`rat/pretzel-11/tkt-01m0cjy73nhnxne3ptay86033b`, commit `d80cc77`, not yet
merged to `main` as of this writing): sharing was found to *corrupt builds
outright, with zero concurrency involved* (`TKT-01M0EXYHV1GR9Z75QSS42HXBVK`),
so keeping it off by default is deliberate, not a gap waiting to close.

## The deeper structural finding still holds

Nubbin-9's other finding — the one that matters regardless of
`shared_cargo_target`'s default — was that `record_gate_failure`, the only
place a check's non-"pass" verdict is durably recorded, is reachable from
exactly one function. Re-traced on current `main`:

```
$ grep -n 'record_gate_failure\|fn run_check_in' crates/rk-daemon/src/workflow_exec.rs
3090: pub(crate) async fn run_check_in(
3125:     self.record_gate_failure(   # lock-timeout early return
3240:     self.record_gate_failure(   # onTimeout: fail
3319:     self.record_gate_failure(   # verdict != "pass" after retries
3459: fn record_gate_failure(
```

All three call sites sit inside `run_check_in`'s own body (`3090`–`3459`);
nothing outside it calls `record_gate_failure`. A rat's own direct
`mise run verify`, run in its bash tool session per completion-protocol
step 3, is not a descendant of `run_check_in` and produces no
daemon-recorded verdict at all — passing or failing. That's still true
after `TestExecLock` shipped, because `TestExecLock` only wraps
`run_check_in`'s own execution; it added no new reporting path.

**Consequence, unchanged from the prior pass:** even if an operator flipped
`shared_cargo_target` on today, tagging ENOENT failures by invocation source
as this ticket asks would show `run_check_in`-mediated recurrences (now
actively suppressed by `TestExecLock`'s serialization plus the free retry)
trending toward zero, and the "external/unknown" bucket reading zero
forever — not because self-driven contention doesn't happen, but because
the daemon has no channel to see it. A deploy cycle can't produce the
comparison the ticket wants without a new reporting path (e.g. a rat that
hits this signature during its own verify filing an `rk obstacle`/artifact
with the signature text) — a smaller, separate piece of instrumentation
this ticket did not scope and should not silently absorb.

## Decision: leave this ticket as deferred instrumentation, do not implement now

Two independent reasons converge, matching the parent ticket's own stance
(`d80cc77`: "`TKT-01M0CK4Z019SMBN9CTCZBYCTKX`... stays open and correctly
scoped as-is... it just stays low-priority relative to default-configuration
work until [an operator re-enables sharing at scale]"):

1. **No precondition to observe.** `shared_cargo_target` defaults off and
   is expected to stay off by default going forward (it corrupts builds, not
   just races) — there is no live cross-process contention for a tagging
   mechanism to classify, gated or self-driven.
2. **No channel to classify with, even if there were.** The "external/unknown"
   bucket has no daemon-visible data source today and none was added by
   `TestExecLock` — tagging as scoped can only ever produce a one-sided
   comparison.

Building the tagging now would instrument a signal that cannot occur under
the default configuration, using a mechanism that structurally cannot see
half of what it's meant to compare. Not implementing it is the correct
outcome of the ticket's own stated conditional, not a shortfall against it:
the "once both land and are enabled in the live daemon" gate was written
precisely to prevent building this before there's something real to
observe, and there still isn't.

**If this is ever revisited:** it needs (a) an operator decision to
re-enable `shared_cargo_target` at a scale where contention is plausible
again, and (b) the small separate reporting-channel piece above (a rat
self-reporting the ENOENT signature via `rk obstacle`/artifact) before the
"external/unknown" bucket can hold any data. Neither is in scope here.

No code changes accompany this doc — same as the prior two passes on this
ticket chain, there is nothing safe or useful to build against the current,
deliberately-off-by-default configuration.

# TKT-vugar-dirag-sumup: `long_silent_but_live_verifier_descendant_prevents_kill` flake under host load

## Symptom

Observed FAILED once during `rk verify --check verify-changed` on
`rat/cheesethief-13/tkt-kujaj-libar-mopud` (a branch that never touches
`stuck_liveness_tests` or anything in its dependency path). Reran the exact
same test in isolation immediately after: passed cleanly in 0.12s. Filed per
`preexisting-failure-is-a-ticket-not-an-inline-fix` (TKT-43) for a future
picker to reproduce under concurrent host load before deciding whether this
is a similar test-fixture race to TKT-kujaj-libar-mopud's
(`docs/2026-09-07-tkt-implementation-lane-fifo-test-flake.md`) or something
else.

## Reproduction

Same methodology as the referenced doc: 6 `yes > /dev/null` CPU busy-loops
plus 8 concurrent copies of the compiled `rk-daemon` lib test binary looping
`supervisor::stuck_liveness_tests::long_silent_but_live_verifier_descendant_prevents_kill`
repeatedly (an 8-core host). Baseline in isolation: passes in ~0.12-0.26s
every time. Under induced load: reproduced reliably at a high rate — one
3-minute run produced 598 failures out of 3539 total runs (~17%); a second,
shorter run produced 152+162 failures out of a few thousand. Two distinct
panic sites, both inside the test itself:

```
thread '...' panicked at crates/rk-daemon/src/supervisor.rs:10658:9:
the backgrounded fake cargo must be recognized before this test proceeds
```

```
thread '...' panicked at crates/rk-daemon/src/supervisor.rs:10668:9:
a live verifier descendant must excuse silence, however long
```

The second is the one that matches the original ticket's description (an
assertion that a live verifier descendant excuses a `decide_sweep` call).

## Root cause: intermittent `ps` snapshot incompleteness, NOT a test-fixture race

This is a **different failure class** from TKT-kujaj-libar-mopud. That ticket's
tests raced their own manual state-pinning against an independently-scheduled
background async task completing a fake harness early — a bug entirely inside
the test fixture, with no implication for production code. This one does not
fit that pattern: the test has no manual state to get out of sync, and it
confirms the descendant is live (via the exact same production call the
assertion later depends on) immediately before proceeding.

Instrumented `crate::managed_verification::live_process_table()` (the `ps -Ao
pid=,ppid=,pgid=,stat=,comm=` wrapper) to log whenever `ps` itself failed to
spawn or exited non-success, and instrumented the test to dump a **second,
independent re-scan** of the process table whenever the main assertion failed.
Result over 162 captured failures of the `supervisor.rs:10668` assertion:

- **Zero** occurrences of `ps` failing to spawn or exiting non-zero. The
  `live_process_table()` fallback-to-empty-on-spawn-failure path (documented
  in that function as "best-effort... an empty result... simply means a
  caller falls back") was never exercised.
- **144/162 (89%)**: an immediate re-scan, taken microseconds later, found the
  exact same descendant alive and correctly classified as a live verifier
  (`cargo`), in a large (~850-860 row), otherwise-healthy table.
- **18/162 (11%)**: even the re-scan still missed it, though the descendant
  (a copy of `/bin/sleep` running for 300s, held alive by the test for the
  entire assertion) cannot plausibly have actually exited in that window —
  and a *third*, separately-captured table dump taken moments after the
  re-scan, in the same failing run, still showed the row present with the
  right `comm`.

In other words: the descendant process never dies. `ps`(1)'s own snapshot of
the live kernel process table is intermittently missing a row for a process
that is unambiguously alive one instant before and after, when the host is
under heavy concurrent fork/exec/exit churn (many short-lived `sh` + copied
`/bin/sleep` process trees being created and torn down across 8 concurrent
copies of this same test, on top of CPU-saturating busy loops). This is not
something `crate::managed_verification::live_process_table()`'s callers can
distinguish from "genuinely no live descendant" today — an empty-of-this-row
snapshot and a genuinely-dead descendant produce the identical
`live_verifier_descendants == 0` result.

**This implicates production code, not just the test fixture.**
`live_process_table()`/`process_liveness()` are called directly by
`Supervisor::gather_liveness_evidence`, which `Supervisor::decide_sweep` uses
in real daemon operation — not just in this test's setup. A transient,
single-snapshot miss under genuine host contention (exactly the condition
this feature exists to tolerate: a rat's own `cargo test`/`rk verify` running
quietly under load) can cause one sweep to see zero live verifier descendants
for a generation that is, in fact, still building. By itself this only
produces a `SweepAction::Soft` (a flag), not a kill — an actual `Hard` (kill)
requires the miss to recur on every sweep for the entire `kill_grace_secs`
window, which is much less likely but was not ruled out here (sustained,
severe host contention for the whole grace window is plausible on a busy
build host, which is the scenario this feature was built to handle).

## Decision

**Not a duplicate of TKT-kujaj-libar-mopud's pattern.** No test-only fix
(e.g. re-pinning manual state) applies here, because there is no manual state
being raced — the bug is a single un-retried, non-atomic `ps` snapshot being
trusted as ground truth for "is any live descendant here." Patching the test
to retry its own assertion would only mask a real, if narrow and
load-dependent, false-negative risk in the production liveness-evidence path.

Per `preexisting-failure-is-a-ticket-not-an-inline-fix` (TKT-43) and the
precedent in `docs/2026-09-07-tkt-implementation-lane-fifo-test-flake.md`
for design decisions with fleet-wide blast radius (there: whether to add a
new class of automatic gate retry), a production-code fix — most plausibly a
single bounded retry of the `ps` scan before `gather_liveness_evidence`
concludes "no live verifier descendant," mirroring the existing narrowly-scoped
retry precedent (`is_cargo_target_contention_signature`) already in this same
file — is filed as a follow-up decision ticket rather than applied
unilaterally inside this investigation. No production or test code was
changed by this investigation.

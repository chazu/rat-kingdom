# TKT-01M0CJY73NHNXNE3PTAY86033B: does the self-driven `mise run verify` gap actually matter?

## Question

`TestExecLock` (TKT-01M0CFA1RX36SJ7DV4YWGHQ9BT, branch `rat/filch-9/...`, not yet
merged to main) serializes the shared-`CARGO_TARGET_DIR` test-execution phase for
every check that routes through `WorkflowEngine::run_check_in` — workflow `run`
steps and the daemon-native landing-pipeline gate (`landing.rs`), including
steward's automated verify gate that fires on every completed rat. It does not,
and structurally cannot, cover a rat's own direct `mise run verify` invocation in
its bash tool session (completion protocol step 3): that process is a
grandchild of the harness process the daemon spawned, with no daemon
subprocess-level hook beyond the env/PATH injected at spawn time.

This ticket's parent recommended instrumenting/observing before investing in a
harness-level wrapper mechanism (PATH-routed `cargo`/`mise` calls acquiring a
cross-process lock) to close that gap. This doc is that observation pass, done
by reading the actual state of the code and ticket trail rather than live
telemetry — see "Why not live telemetry" below.

## Finding 1: the precondition for this gap isn't live yet

`shared_cargo_target` — the config flag that points every spawned agent's
`CARGO_TARGET_DIR` at one shared per-repo cache instead of its own worktree's
`target/` — does not exist on `main` today:

```
$ grep -rn "shared_cargo_target\|CARGO_TARGET_DIR" crates/ --include=*.rs
(no output)
```

It only exists on the still-unmerged `TestExecLock` branch, which cherry-picked
it forward from `rat/tunnel-8/tkt-01m04d1qdbncf0t0d0ehrvnjv5` (TKT-01M0CHX9N81PMD2T6ES9YGZXD6:
that ticket was marked closed/done but its commit, 52f1c5840, was never merged —
a "done tickets not in main" recurrence).

That same ticket (TKT-01M0CHX9N81PMD2T6ES9YGZXD6) also surfaced that the
original disk-exhaustion problem `shared_cargo_target` was meant to fix
(3-7GB × 60+ concurrent worktrees blowing `cargo test --workspace` out with
`ENOSPC`) already has a *different*, already-live fix on main: gate-worktree
retention/pruning (commits 78642db/1bc2efb, "O12"). It explicitly flagged an
open operator/architect question — adopt `shared_cargo_target` anyway (option a)
or treat it as moot now that retention/pruning covers the original symptom
(option b) — and left that undecided.

**Consequence:** today, on main, there is no shared target dir, so there is no
cross-process ENOENT contention at all — self-driven or otherwise. The entire
premise of this ticket is conditional on `shared_cargo_target` actually landing
*and* being turned on, which has not happened yet.

## Finding 2: once it does land, the self-driven case is not a rare corner

Assuming `shared_cargo_target` does ship and get enabled (the direction the
fleet currently seems to be moving in, given `TestExecLock` is actively being
implemented against it), the self-driven case is not an edge case relative to
the now-covered path — it's roughly its mirror image:

- Every dispatched rat runs `mise run verify` itself, directly, as mandatory
  completion-protocol step 3. That happens on *every* task.
- The daemon-native landing-pipeline gate *also* runs verify automatically on
  *every completed rat* (`TestExecLock`'s own commit message: "fires on every
  completed rat and is likely the dominant source of concurrent
  shared-target-dir contention").

So for a single rat's task, there are typically two verify invocations
clustered close in time: the rat's own (uncovered) and the automated gate's
(covered). They aren't just occasionally simultaneous — the gate fires
*because* the rat just finished, i.e. right after the rat's own verify. Two
consequences:

1. The self-driven invocation isn't rare; it's on the same order of frequency
   as the now-covered one.
2. Because `TestExecLock` is an in-process `tokio::sync::Mutex` inside the
   daemon (deliberately not a cross-process `flock`, per its own doc comment:
   "no cross-process flock is needed... because it is only ever touched by
   checks this daemon spawns") — that assumption is exactly what's false here.
   A self-driven verify can run *concurrently with* a locked automated-gate
   verify for a *different* rat on the same repo, defeating the serialization
   guarantee for that overlap window even after `TestExecLock` ships.

This matches the original bug: `docs/...hot-scan-target-dir-contention.md`'s
own Symptom section describes the failure as happening under "a full
`mise run verify`" — language that reads as a rat's own invocation, not a
workflow-driven check.

## Why not live telemetry

The natural next step would be to instrument and observe real occurrence
rates. That can't be done yet: the feature this gap is *about*
(`shared_cargo_target` + `TestExecLock`) isn't merged or deployed, so there's
no live daemon config to observe contention against, and no historical
ENOENT-signature data tagged with invocation source (self-driven vs.
daemon-driven) to mine — nothing has ever distinguished the two.

## Recommendation

Don't build the harness-level PATH-wrapper mechanism now. Two reasons:
it's a fleet-wide harness-launch change (the parent ticket already flagged
this as needing its own design pass and explicit sign-off), and there's
currently nothing to observe to justify that investment — the precondition
feature hasn't shipped.

Concrete next step, sequenced after `TestExecLock`/`shared_cargo_target`
actually merge and deploy: add cheap tagging, not a new mechanism — when a
check fails on the `could not execute process ... (never executed) ...
No such file or directory` signature, record which path produced it
(`run_check_in`-mediated vs. unknown/external) in the failure artifact/ticket.
After a deploy cycle, if self-driven-shaped recurrences (an ENOENT hit
correlated with a *rat's own* verify run rather than a workflow/gate run)
show up despite `TestExecLock` covering the gated path, that's the concrete
evidence to greenlight the wrapper design. If they don't recur — e.g. because
gate-worktree retention/pruning already keeps same-repo concurrency low
enough in practice, or because `shared_cargo_target` ends up not adopted per
the still-open Finding 1 question — the wrapper is unnecessary and this
ticket closes as "gap real in theory, not worth fixing in practice."

Filed as TKT (see artifact) rather than implemented inline: `TestExecLock`'s
own files (`supervisor.rs`, `workflow_exec.rs`, `landing.rs`, `config.rs`) are
actively claimed and in-progress by other rats (Filch-9, Dart-9, Scrounge-9)
implementing the very feature this gap depends on; there is nothing safe to
build against on `main` yet.

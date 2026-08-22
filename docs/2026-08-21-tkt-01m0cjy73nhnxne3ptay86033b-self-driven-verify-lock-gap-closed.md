# TKT-01M0CJY73NHNXNE3PTAY86033B: self-driven `mise run verify` TestExecLock gap — closed, no wrapper needed

## The question this ticket asked

`TestExecLock` (TKT-01M0CFA1RX36SJ7DV4YWGHQ9BT) serializes the test-execution
phase of every check that routes through `WorkflowEngine::run_check_in`
against shared-`CARGO_TARGET_DIR` contention. It structurally cannot cover a
rat's own direct `mise run verify` invocation in its bash tool session
(completion protocol step 3) — that process is a grandchild of the harness
process the daemon spawned, with no daemon subprocess-level hook into it.

The ticket's own recommendation was to decide whether that gap matters in
practice — is the self-driven case the dominant contention source, or is the
now-covered automated steward gate sufficient — before investing in a
fleet-wide harness-launch wrapper (PATH-routing `cargo`/`mise` through an
IPC- or flock-mediated acquire/release the daemon would inject at spawn
time, same as it injects `CARGO_TARGET_DIR` today).

## What was already established

`docs/2026-08-19-tkt-self-driven-verify-lock-gap-analysis.md` did the first
pass: at that point `shared_cargo_target` (the precondition for any
cross-process contention, gated or self-driven, to exist at all) hadn't
merged to main, so there was nothing live to observe. It reasoned that once
the flag *did* ship, the self-driven case would not be a rare corner —
every rat runs verify itself on every task, clustered in time with the
automated gate's own verify on the same repo — and recommended deferring
the wrapper in favor of cheap tagging (filed as
`TKT-01M0CK4Z019SMBN9CTCZBYCTKX`: record whether an ENOENT-signature failure
came from a `run_check_in`-mediated path or an external/unknown one) to get
real data before committing to the wrapper design.

## What changed since: the premise itself is gone by default

`TKT-01M0EXYHV1GR9Z75QSS42HXBVK` (commit `f4df009`,
`docs/2026-08-20-tkt-01m0exyhv1-shared-cargo-target-followup.md`) found that
sharing one `CARGO_TARGET_DIR` across worktrees doesn't just race — it
silently corrupts builds with **zero concurrency involved** (cargo doesn't
fully key a workspace-member unit's fingerprint by the checkout's absolute
path, so two worktrees of the same repo can collide onto the same cached
artifact regardless of timing). `TestExecLock` cannot fix that: there is no
race to serialize, the wrong answer is cached. `[disk] shared_cargo_target`
now **defaults to `false`**, and the original ENOSPC problem it traded
against is covered independently by `WorktreeSweepConfig`'s hourly
terminal-worktree reap plus `min_free_gb`'s spawn guard.

That doc already drew the direct consequence for this ticket: with sharing
off by default, each spawned agent is back on cargo's own per-worktree
`target/`, which cannot collide with any other worktree's by construction —
gated or self-driven. There is no shared target dir left for a self-driven
`mise run verify` to contend over in the default configuration. The
originally-reported `hot_scan.rs` ENOENT flake this whole line of work traces
back to (`docs/2026-08-19-tkt-hot-scan-target-dir-contention.md`) cannot
recur under the default either, regardless of which process (a rat's own
shell, a workflow `run` step, or the automated landing gate) happens to run
`mise run verify`.

## Decision

Do not build the harness-level PATH-wrapper mechanism. The two things that
would have justified it — real contention data proving the self-driven path
is a material source, weighed against the fleet-wide harness-launch change
it requires — no longer apply: the contention itself only exists when an
operator explicitly opts back into `shared_cargo_target: true` despite the
now-documented correctness cost, which is expected to be rare and is the
operator's own informed call, not something every rat's completion protocol
needs to be defended against by default.

This ticket closes as: gap real only under a non-default, correctness-risky
opt-in; not worth a fleet-wide wrapper given that scope.

`TestExecLock`, the `run_check_in` contention-retry, and `shared_cargo_target`
itself all remain in place and untouched — they still do their job correctly
for an operator who opts back in. `TKT-01M0CK4Z019SMBN9CTCZBYCTKX` (tag
ENOENT-signature failures by invocation source) also stays open and correctly
scoped as-is: it is the right fallback instrumentation *if* an operator ever
re-enables sharing at a scale where this analysis should be revisited, and
nothing about it needs to change now — it just stays low-priority relative
to default-configuration work until that happens.

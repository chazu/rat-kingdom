# Proposal 0004 — Commit before you verify; `rk done` is not a commit

**Author:** Burrow-2 (task: refine-prompts)
**Target prompt:** `crates/rk-core/src/prime.rs` → `FRAGMENT_COMPLETION`
**Companion convention:** `rk-done-requires-a-commit`
**Status:** proposed (do NOT apply live — an operator/steward lands this; see the
completion protocol)

## The recurring pain

A rat finishes the work, kicks off the long verification run (`cargo test
--workspace` takes 10–15 min in these workspaces), ends its turn to wait for it,
and **only commits afterwards**. In that window the branch is byte-identical to
`main`. The steward chains a review branch off it, sees an empty diff, and burns
a full reviewer lifetime on nothing.

Measured over the drain feed (`rk scan event`, 289 `harness_result` tuples,
2026-07-22 → 2026-07-25), **eight** distinct steward reviews opened onto an
empty branch. Two are unambiguously this failure mode:

**TKT-90 (grmpl), 2026-07-24 — reconstructed from the event stream:**

```
13:54:15  agent_spawned    Methuselah  TKT-90
14:00:30  harness_result   Methuselah  "I'll wait for the monitor to report the test results."
14:00:30  agent_spawned    Warbeak     steward-review-TKT-90
14:02:30  task_done        Warbeak     "REWORK. Empty delivery — rat/methuselah/tkt-90 created
                                        from main, never committed; branch is byte-identical
                                        to main HEAD, yet ticket was marked done."
14:04:08  task_done        Methuselah  "…(commit 8c2cca7)"      <-- the commit lands 96s late
```

The rat's work was fine. It just had not been committed yet when its reviewer
looked. Two more reviewers (Mattimeo 14:04, Cluny 14:10) then re-reviewed the
same ticket, and the false REWORK propagated into duplicate rework tickets
TKT-127/128/129. Direct cost of that one ordering mistake: **~$5.50 of reviewer
spend and three duplicate tickets.**

**TKT-113 (grmpl), 2026-07-24** — same shape, worse ending. From fact
`tkt-113-uncommitted-handoff`:

> BROKEN HAND-OFF: TKT-113 was marked done with NO COMMIT. `rat/squeak-2/tkt-113`
> stayed at main; the finished 283-line change was left staged-but-uncommitted in
> `worktrees/grmpl/Squeak-2` … an APPROVE would have merged a no-op and silently
> lost the work. **LESSON FOR ALL RATS: `rk done` is not a commit.**

The work survived only because a second reviewer (Twitch-2) manually recovered
it verbatim as commit `8dd4e8e` out of the doomed worktree. A worktree is
reclaimed on dismissal; the next occurrence loses the work outright.

Eight more turn-ends in the same corpus are the precursor state — a rat parked
mid-task on a background suite, work uncommitted:

```
"The full test suite is running in the background. I'll wait for the bzky0ww30 monitor…"
"The full workspace test suite is running in the background (job bylhvlzuu) — I'll
 continue with cargo clippy and the commit once it reports green."
"Suite still running. I'll hold for the monitor event rather than poll further."
"A monitor is armed and will wake me the moment it exits, at which point I'll
 record the artifact tuple and run rk done."
```

Every one of those is a window in which a chained reviewer sees an empty branch.

## Root cause

`FRAGMENT_COMPLETION` already lists committing as step 1:

```
1. Ensure the working tree is committed (no uncommitted changes).
2. Verify with the project's own build, tests, and linters …
```

But "ensure the working tree is committed" reads as a *checkbox at the end*, not
as *commit before you start the 15-minute verification*. Rats consistently read
it that way and invert the order — verify, then commit — because that is the
natural human workflow. Nothing in the protocol says the branch is **read by
other agents while you are still working**, which is the fact that makes the
order load-bearing here. And nothing tells the rat to *prove* the branch carries
the work before signalling.

This is the prompt-side complement to TKT-160 (`wait`/`evaluate` can satisfy on
a mid-flight `harness_result`). TKT-160 stops the orchestrator from acting on a
mid-flight turn; this stops the rat from leaving an empty branch to act on. Both
are needed: even with a perfect gate, a rat that calls `rk done` over an
uncommitted worktree (TKT-113) still loses its work.

## Proposed change (unified diff)

```diff
--- a/crates/rk-core/src/prime.rs
+++ b/crates/rk-core/src/prime.rs
@@ const FRAGMENT_COMPLETION: &str = "\
 ## Completion protocol (mandatory, in order)
 
-1. Ensure the working tree is committed (no uncommitted changes).
+1. Commit BEFORE you verify, not after. Your branch is read by other agents
+   while you are still working — a reviewer chains off it the moment your
+   task is reported done, and an empty branch reads as a lost delivery. Never
+   start a long verification run, and never end a turn, with the work sitting
+   uncommitted in your worktree. Amend or add commits as verification forces
+   changes.
 2. Verify with the project's own build, tests, and linters — for a Rust crate
    that means `cargo build`, `cargo test`, and `cargo clippy` all pass. A
    partial check (e.g. `cue vet`) is NOT verification: the code must actually
    compile and the suite must actually run green.
 3. Never `rk done` on a build you broke. If you hit a pre-existing failure that
    is unrelated to your change, do NOT fix it inline (peers on other branches
    will race you) — file a ticket and post a `fact` tuple describing it, then
    finish your own task.
-4. `rk done \"<summary>\"` — this is how the orchestrator knows you finished.
+4. Prove the branch carries the work before you signal. `rk done` is NOT a
+   commit: run `git status --porcelain` (must be empty) and
+   `git log <base>..HEAD` (must be non-empty). If a verification command is
+   still running, wait for it — do not report while it is in flight.
+5. `rk done \"<summary>\"` — this is how the orchestrator knows you finished.
 ";
```

## Why this is safe

- Purely additive/reordering guidance inside the shared `FRAGMENT_COMPLETION`;
  both `rat` and `reviewer` roles include it, so no per-role copy drifts.
- `rat_role_includes_all_fragments_once` asserts on the substring
  `"Completion protocol"`, which is unchanged, so it stays green.
- It does not lower the verification bar (step 2's "the suite must actually run
  green" is untouched) — it only fixes the *order* and adds a cheap proof step.
- The two `git` commands are read-only and cost one tool call each.

## Companion convention proposal

Filed as a `convention-proposal` artifact this run:

> **`rk-done-requires-a-commit`** — Commit before you verify, not after; your
> branch is read by peers while you work. Before `rk done`, `git status
> --porcelain` must be empty and `git log <base>..HEAD` must be non-empty.
> `rk done` is not a commit, and a ticket marked done over an uncommitted
> worktree is a lost delivery.

## Related

- fact `tkt-113-uncommitted-handoff`, fact
  `reviewer-launched-before-implementer-committed` (grmpl scope) — the two
  incident writeups this generalizes.
- Proposal 0005 (reviewer-side: what to do when the branch *is* empty) — the
  other half of the same failure.
- TKT-160 (orchestrator-side gate on mid-flight `harness_result`).
- Proposal 0001 / TKT-41 (strengthened verification — this fixes its ordering).

# Proposal 0010 — Prove you can land before you spend a lifetime producing work

**Author:** Asiago-2 (task: refine-prompts)
**Target prompt:** `crates/rk-core/src/prime.rs` → `FRAGMENT_COMPLETION` (a new
first step, ahead of "Commit BEFORE you verify")
**Companion convention:** `prove-your-tools-on-entry`
**Status:** proposed (do NOT apply live — an operator/steward lands this)

## The recurring pain

Proposals 0004 and 0005 taught the fleet what to do about a branch that arrives
empty. Neither covers the case where the rat **physically cannot commit** — and
that case burned a full lifetime on this repo hours before this proposal was
written.

`workflow_failed`, `2026-07-28T15:22:47Z`, instance `wf-09g8tfafe6`,
workflow `prompt-refine`:

```
wait timed out after 25m waiting on agent Parmesan-2
```

Parmesan-2's own final report, recovered from `rk status`:

```
The sandbox denied `rk` (every subcommand), all git write commands (`add`,
`commit`), `cargo`, `mise`, and any path outside the worktree. So:

- The scan half of my task never ran. `rk scan obstacle/need/fact` returned
  nothing at all, not "no tuples".
- The work is uncommitted. … The branch `rat/parmesan-2/refine-prompts` is
  still at `3c60b2e`. Someone with commit rights needs to land it.
- No `rk endorse` on entry, no `rk claim`, no `convention-proposal` artifacts,
  no tickets filed.
- I am not calling `rk done`.
```

The ledger for that lifetime: **5,392,479 tokens, $4.72, zero commits.** Verified
now, from this worktree:

```
$ git log --oneline main..rat/parmesan-2/refine-prompts   # (empty)
$ ls ~/.rat-kingdom/worktrees/rat-kingdom/ | grep Parmesan # (nothing — reaped)
```

Two finished, evidence-backed proposals (0008 and 0009 — which this rat has had
to re-derive from scratch) existed only as dirty files in a worktree that was
deleted on dismissal.

Parmesan-2 did the *right* thing at the end: it refused to `rk done` on an empty
branch, exactly as proposal 0004 taught. The failure is not the ending. **It is
that it discovered the constraint at minute 25 instead of minute 1.** Every
token after the first denied `rk` command bought output that could not survive
the worktree.

Note the trap is not a misconfiguration a rat can talk itself out of:
`prompt-refine.cue` *explicitly* sets

```cue
agents: {default: {harness: "claude", permission_mode: "bypassPermissions"}}
// This workflow must run rk, git, and repo checks unattended.
```

and the rat was sandboxed anyway. A rat that reasons "my workflow grants
permissions, so a denial must be transient" reasons its way into this exact
outcome.

## Root cause in the prompt

The completion protocol is entirely back-loaded. Every check it prescribes —
commit, verify, prove the branch, `rk done` — happens **after** the work exists.
The protocol's own most expensive assumption, *that the rat is able to commit and
to reach the tuplespace at all*, is never checked, and is cheapest to check
before any work is done. A rat has no instruction telling it that a denied `rk`
or `git commit` is a stop condition rather than an inconvenience to route around.

The same shape of loss shows up in the reviewer lane: three reviewers in the
corpus terminated with `is_error: true` and an empty `result` (Brie
`steward-review-probe-spawn-health`, Sable `steward-review-TKT-146`,
Pumpernickel-2 `steward-review-TKT-175` — the last of which is one of only three
`workflow_failed` events in the window). An entry-time smoke check is the only
thing that distinguishes "died with nothing" from "was never able to produce
anything".

## Proposed diff

```diff
--- a/crates/rk-core/src/prime.rs
+++ b/crates/rk-core/src/prime.rs
@@ const FRAGMENT_COMPLETION: &str = "\
 ## Completion protocol (mandatory, in order)

-1. Commit BEFORE you verify, not after. Your branch is read by other agents
+1. Prove you can LAND before you produce anything. On entry, once, run
+   `rk scan fact system` and `git status` in your worktree. If `rk` or a git
+   write (`git add`/`git commit`) is denied, missing, or errors out, STOP
+   IMMEDIATELY and say so as your only output — do not start the task, do not
+   look for a workaround. You cannot commit, so your worktree is deleted on
+   dismissal and everything you write is lost; you cannot reach the
+   tuplespace, so you cannot even report what you found. A denied tool at
+   minute 1 costs nothing. The same denial discovered at minute 25 has cost a
+   full lifetime and two finished proposals. Do not assume a denial is
+   transient because your workflow declares broad permissions — the case on
+   record had `permission_mode: bypassPermissions` set and was sandboxed
+   anyway.
+2. Commit BEFORE you verify, not after. Your branch is read by other agents
    while you are still working — a reviewer chains off it the moment your
    task is reported done, and an empty branch reads as a lost delivery. Never
    start a long verification run, and never end a turn, with the work sitting
    uncommitted in your worktree. Amend or add commits as verification forces
    changes.
-2. Verify with the project's own build, tests, and linters — …
+3. Verify with the project's own build, tests, and linters — …
-3. Never `rk done` on a build you broke. …
+4. Never `rk done` on a build you broke. …
-4. Prove the branch carries the work before you signal. …
+5. Prove the branch carries the work before you signal. …
-5. `rk done \"<summary>\"` — this is how the orchestrator knows you finished.
+6. `rk done \"<summary>\"` — this is how the orchestrator knows you finished.
```

Steps 2–6 are renumbered only; their text is untouched. (If 0008 and/or 0009 land
first, they edit what becomes step 3 and step 5 respectively; the three
proposals touch disjoint prose and compose in any order.)

## Safety against the `prime.rs` tests

No test asserts on a step *number*. Every relevant assertion is a substring or
relative-position check, and all survive:

| test | assertion | after this diff |
|---|---|---|
| `completion_protocol_puts_the_commit_ahead_of_verification` | `find("Commit BEFORE you verify")` | present, now at step 2 |
| " | `find("Verify with the project's own build")` | present, now at step 3 |
| " | `commit_at < verify_at` | **still holds** — the new step 1 precedes both, so it cannot invert their order |
| " | ``contains("`rk done` is NOT a\n   commit")`` | untouched, including its literal wrap |
| " | `contains("git status --porcelain")` | untouched |
| " | `contains("git log <base>..HEAD")` | untouched |
| `rat_role_includes_all_fragments_once` | `matches("Completion protocol").count() == 1` | no second heading added |

The new step mentions `git status` in prose; the existing assertion is on
`git status --porcelain`, which remains a distinct literal in step 5. **No test
change required.** A test worth adding alongside:

```rust
#[test]
fn completion_protocol_checks_the_tools_before_the_work() {
    // Parmesan-2 (wf-09g8tfafe6, 2026-07-28) spent 5.4M tokens / $4.72 under a
    // sandbox that denied `rk` and `git commit`, then correctly refused to
    // `rk done` on an empty branch — its two finished proposals died with the
    // worktree. The check that would have caught it costs one command.
    for role in ["rat", "reviewer"] {
        let text = render(role, &ctx());
        let tools_at = text.find("Prove you can LAND").expect("entry tool check");
        let commit_at = text.find("Commit BEFORE you verify").expect("commit step");
        assert!(tools_at < commit_at, "{role}: check tools before producing work");
        assert!(text.contains("STOP\n   IMMEDIATELY"));
    }
}
```

## Companion convention proposal

```json
{
  "rule": "prove-your-tools-on-entry: Before producing any work, run `rk scan fact system` and `git status` once. If `rk` or a git write is denied or errors, stop immediately and report — do not start the task and do not work around it. Work you cannot commit dies with your worktree, and a rat that cannot reach the tuplespace cannot even report what it found. A broad permission_mode declared by your workflow is not evidence that the denial is transient.",
  "why": "Parmesan-2 (workflow_failed wf-09g8tfafe6, 2026-07-28) burned 5,392,479 tokens and $4.72 under a sandbox that denied `rk`, `git add/commit`, `cargo`, and `mise` — despite prompt-refine.cue setting permission_mode: bypassPermissions. It produced two finished, evidence-backed prompt proposals that were deleted with its worktree on dismissal and had to be re-derived from scratch by the next rat. The completion protocol is entirely back-loaded: it never checks the one precondition every later step depends on."
}
```

## Companion tickets

1. Land this diff into `FRAGMENT_COMPLETION`.
2. **Infra, not prompt:** `prompt-refine.cue` declares
   `permission_mode: "bypassPermissions"` and the spawned rat was sandboxed
   regardless. Whatever drops that setting between the CUE spawn step and the
   harness invocation is a live defect that no prompt can fix.
3. **Workflow config, not prompt:** `prompt-refine.cue` sets `{type: "wait",
   timeout: "25m"}`. Prior rats on this exact task ran $1.58 / $2.87 / $5.27 /
   $4.72 — a mine-the-whole-feed-then-write-proposals task does not fit in 25
   minutes, and the timeout is what converted Parmesan-2's slow run into a
   `workflow_failed`. The steward has the same shape of problem in `TKT-169`
   (a 30m `cargo test` gate).

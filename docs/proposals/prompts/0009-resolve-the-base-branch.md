# Proposal 0009 — `<base>` is a placeholder no rat can resolve, and the reviewer's answer is the opposite of the rat's

**Author:** Asiago-2 (task: refine-prompts)
**Target prompt:** `crates/rk-core/src/prime.rs` → `FRAGMENT_COMPLETION` step 4,
and the `"reviewer"` arm of `render()`
**Companion convention:** `resolve-your-base-before-you-prove-your-branch`
**Reconstructs:** Parmesan-2's lost 0009 (see 0010), re-derived independently and
re-checked against `supervisor.rs` / `workflow_exec.rs` / `steward.cue`
**Requires a test change** (unlike 0008) — see the safety section
**Status:** landed

## The gap

Proposal 0004 landed the empty-delivery proof, and it is the strongest rule in
the completion protocol:

```
4. Prove the branch carries the work before you signal. `rk done` is NOT a
   commit: run `git status --porcelain` (must be empty) and
   `git log <base>..HEAD` (must be non-empty).
```

The reviewer arm opens on the same token:

```
FIRST establish there are changes: run `git log <base>..HEAD`.
```

**Nothing in a rat's world defines `<base>`.** Verified at the tree:

- `Supervisor::agent_env` (`crates/rk-daemon/src/supervisor.rs:2531-2554`) exports
  exactly `RK_HOME`, `RK_AGENT`, `RK_AUTH_TOKEN`, `RK_ROLE`, `RK_REPO`,
  `RK_TASK`, `RK_BRANCH`, `RK_WORKTREE`, `RK_WORKFLOW_INSTANCE`, `PATH`. **No
  base.** (Confirmed live: this rat's own environment has no such variable.)
- `PrimeContext` (`prime.rs:12-23`) has `agent`, `repo`, `task`, `branch`,
  `parent`, `conventions`. **No base field**, so `render()` could not
  substitute one even if it wanted to.
- The daemon *does* know it — `SpawnParams::base` (`supervisor.rs:101`), resolved
  to `target_branch` at `supervisor.rs:456`. The value exists and is simply never
  handed to the agent that is told to use it.

So every rat guesses, and the guess is load-bearing in two places that need
**opposite answers**.

## Consequence 1 — a chained rat's proof is a no-op

Workflow spawns do not fork from `main`. `workflow_exec.rs:965`:

```rust
base: spawn.branch.clone().or(ctx.active_branch.clone()),
```

A step's rat is cut from *the previous step's branch*. A rat that guesses `main`
then runs `git log main..HEAD` and sees its predecessor's commits — non-empty,
proof passes — **even if it committed nothing itself.** Step 4 exists precisely
to catch the zero-commit case, and it is disarmed exactly in the chained case
that produced the eight empty review branches 0004/0005 were written for.

## Consequence 2 — a reviewer that resolves `<base>` correctly REWORKs finished work

A reviewer is chained onto the branch it reviews. `steward.cue:104-108`:

```cue
{ type: "spawn", role: "reviewer", agent: "reviewer", branch: _input.branch, … }
```

So the reviewer's true fork point **is the tip of the work under review**, and
`git log <true-base>..HEAD` is empty **by construction on every healthy review**.
That empty result then feeds straight into the disambiguation the reviewer arm
teaches:

```
Find the implementer's commit … `git merge-base --is-ancestor <sha> main`
- NOT an ancestor ⇒ the work was never committed … REWORK
```

For committed-but-not-yet-merged work — i.e. the normal state of every branch
awaiting review — `<sha>` is *not* an ancestor of `main`. Verdict: **REWORK.**

> A reviewer that resolves its base correctly REWORKs finished work. One that
> guesses `main` behaves correctly.

That the steward already works around this is the tell: `steward.cue:115` injects
the answer into the reviewer's task description by hand —

```
Compare with: git log \(_input.target)..HEAD and git diff \(_input.target)...HEAD
```

— naming `_input.target` (the integration branch), not the reviewer's base. The
fix has been living in one CUE file. Reviewers spawned outside the steward
(`code-review.cue`, a bare `rk spawn --role reviewer`) get no injection and are
left with the unresolvable placeholder.

## Root cause

One token, `<base>`, is asked to mean two different things:

| role | what `<base>` must mean | why |
|---|---|---|
| rat | **my fork point** | proves *I* committed something, not my predecessor |
| reviewer | **the integration branch** (`main`) | I am chained *onto* the work; my fork point makes every healthy review look empty |

## Proposed diff

```diff
--- a/crates/rk-core/src/prime.rs
+++ b/crates/rk-core/src/prime.rs
@@ const FRAGMENT_COMPLETION: &str = "\
 4. Prove the branch carries the work before you signal. `rk done` is NOT a
    commit: run `git status --porcelain` (must be empty) and
-   `git log <base>..HEAD` (must be non-empty). If a verification command is
-   still running, wait for it — do not report while it is in flight.
+   `git log <base>..HEAD` (must be non-empty). Resolve `<base>` — do not assume
+   `main`. Your worktree is NOT always cut from the integration branch: a
+   workflow chains each step's rat onto the previous step's branch, so
+   `git log main..HEAD` can be non-empty because of a PREDECESSOR's commits
+   while you have committed nothing. Get your own fork point with
+   `git merge-base HEAD main` and count from there:
+   `git log $(git merge-base HEAD main)..HEAD` — and confirm at least one of
+   those commits is yours (`git log --format='%an %s' $(git merge-base HEAD \
+   main)..HEAD`). If a verification command is still running, wait for it — do
+   not report while it is in flight.
```

```diff
@@ fn render(role: &str, ctx: &PrimeContext) -> String {
             out.push_str(
                 "Review the changes on your branch against the task requirements. \
-                 FIRST establish there are changes: run `git log <base>..HEAD`. An \
+                 FIRST establish there are changes: run `git log <base>..HEAD`, where \
+                 `<base>` is the repo's INTEGRATION branch (`main`) — NOT your own \
+                 fork point. You are chained onto the branch you are reviewing, so \
+                 your fork point is the tip of that work and `git log` from it is \
+                 empty on every healthy review. Counting from your fork point would \
+                 make you REWORK finished work. An \
                  EMPTY branch is not a verdict — it has two causes needing OPPOSITE \
```

## Safety against the `prime.rs` tests

**This proposal requires one test edit** — flagged loudly, because 0008 does not.

`completion_protocol_puts_the_commit_ahead_of_verification` (`prime.rs:448`)
asserts:

```rust
assert!(text.contains("git log <base>..HEAD"));
```

The diff **preserves that literal** in both step 4 and the reviewer arm (the new
text is appended after it, never replacing it), so **the assertion still passes
unmodified**. The stronger move is to tighten it alongside the change:

```rust
assert!(text.contains("git log <base>..HEAD"));
// <base> is not resolvable from a rat's env (supervisor.rs agent_env exports
// no base), and a workflow-chained rat is cut from its predecessor's branch,
// not main — so the placeholder has to come with the resolution.
assert!(text.contains("git merge-base HEAD main"));
assert!(text.contains("do not assume"));
```

and, for the reviewer asymmetry, a new focused test beside
`reviewer_disambiguates_an_empty_branch_before_reaching_a_verdict`:

```rust
#[test]
fn reviewer_counts_from_the_integration_branch_not_its_fork_point() {
    // steward.cue spawns the reviewer with `branch: _input.branch`, so the
    // reviewer's fork point IS the work under review and `git log <fork>..HEAD`
    // is empty on every healthy review. Resolving <base> "correctly" therefore
    // routes finished work to REWORK.
    let text = render("reviewer", &ctx());
    assert!(text.contains("NOT your own"));
    assert!(text.contains("chained onto the branch you are reviewing"));
    // The rat's opposite instruction must not leak into the reviewer arm.
    assert!(!render("reviewer", &ctx()).contains("do not assume"));
}
```

Other tests are unaffected: `rat_role_includes_all_fragments_once` counts
headings (none added); `reviewer_disambiguates_an_empty_branch_before_reaching_a_verdict`
asserts `check_at < verdicts_at` on `"EMPTY branch is not a verdict"` and
`"Produce exactly one recommendation"` — the diff inserts *before* the first, so
the ordering strengthens rather than breaks.

## The durable fix, and why it is a separate ticket

The prompt-side change above makes rats resolve the base themselves. The real
fix is to stop making them guess:

1. `Supervisor::agent_env` exports `RK_BASE` from the already-resolved
   `SpawnParams::base` / `target_branch`.
2. `PrimeContext` gains a `base: Option<String>` field, and `render()`
   substitutes it for the `<base>` placeholder — so the prompt says
   `git log rat/foo/tkt-1..HEAD`, not a token.
3. `rk status <name>` surfaces the base alongside the branch, so a reviewer or
   operator can read it without archaeology.

That spans the daemon, core, and CLI and would race peers, so it is filed as a
ticket rather than started here.

## Companion convention proposal

```json
{
  "rule": "resolve-your-base-before-you-prove-your-branch: <base> is not main by default. A workflow chains each step's rat onto the previous step's branch, so an implementer must count commits from its own fork point (git merge-base HEAD main) and confirm one is its own; a reviewer must count from the INTEGRATION branch, because it is chained onto the work under review and its own fork point makes every healthy review look empty.",
  "why": "The spawn env (supervisor.rs agent_env) exports no base and PrimeContext has no base field, so the <base> in completion step 4 and the reviewer arm is an unresolvable placeholder. Guessing main disarms the empty-delivery proof for chained rats; resolving it correctly makes a reviewer REWORK finished work. steward.cue:115 already hand-injects the answer for the one path it controls."
}
```

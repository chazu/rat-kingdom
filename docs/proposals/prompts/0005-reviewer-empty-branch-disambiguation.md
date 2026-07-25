# Proposal 0005 — Reviewer: an empty review branch has two causes, disambiguate before the verdict

**Author:** Burrow-2 (task: refine-prompts)
**Target prompt:** `crates/rk-core/src/prime.rs` → the `"reviewer"` arm of `render()`
**Companion convention:** `empty-review-branch-must-be-disambiguated`
**Status:** proposed (do NOT apply live — an operator/steward lands this; see the
completion protocol)

## The recurring pain

Reviewers keep opening onto a branch with **zero commits against main**, and the
correct verdict depends on *why* — but nothing in the reviewer prompt says so,
so each reviewer re-derives it from scratch, and some get it wrong in the
expensive direction.

Eight occurrences in one drain (`rk scan event`, 2026-07-22 → 2026-07-25):

```
"The branch under review is empty … both point at the exact same commit as main
 (79d2ee5) — zero commits, git diff main...HEAD produces nothing."      (TKT-82)
"Both rat/asiago/tkt-84 and my review branch point at exactly fd3a270, which IS
 main … There are no commits to review and nothing to auto-merge."      (TKT-84)
"My review branch has an empty diff against main … approving it would merge
 nothing and P12 would never reach main."                              (TKT-121)
"The rat delivered nothing … byte-identical to main HEAD … yet the ticket was
 marked done."                                                          (TKT-90)
"Empty diff … Zero commits, zero changes."                             (TKT-107)
"rat/squeak-2/tkt-113 == main == ca688d7 — zero commits"               (TKT-113)
"My review branch has zero commits vs main — but this is NOT the lost-work case
 the fleet has been burned by. TKT-127's work is already IN main."      (TKT-127)
"A duplicate ticket. My branch has zero commits vs main …"             (TKT-137)
```

The last two are the point. The fleet learned the hard way that the naive rule
"empty ⇒ REWORK" is wrong half the time. Fact `empty-review-branch-has-two-causes`
(grmpl scope, TKT-127) records it:

> An empty review branch (`git log main..HEAD == 0`) has TWO distinct causes and
> they need OPPOSITE verdicts.
> **case A (lost):** work never committed … APPROVE would merge a no-op and
> silently lose work ⇒ REWORK.
> **case B (already merged):** the ticket was already delivered, reviewed and
> auto-merged, and the orchestrator re-spawned a duplicate reviewer. REWORK here
> is WRONG: it manufactures a rework loop for work that is already done and
> green (**TKT-90 already spawned duplicates TKT-127/128/129 for the same
> rework**).
> **how to tell:** find the implementer commits (`rk scan artifact <repo>` gives
> the sha) and run `git merge-base --is-ancestor <sha> main`.

Both wrong answers are costly and neither is self-correcting:

- **Wrong REWORK on case B** manufactures duplicate tickets and duplicate rat
  lifetimes. TKT-90 measurably produced three (TKT-127/128/129).
- **Wrong APPROVE on case A** auto-merges a no-op *and closes the ticket*, so
  the work is silently lost when the worktree is reclaimed. TKT-113 came within
  one verdict of that; the work survived only because a second reviewer manually
  recovered 283 uncommitted lines out of the doomed worktree.

## Root cause

The reviewer prompt says only "Review the changes on your branch against the
task requirements" and then lists the three verdicts. It never says:

1. **establish that there are changes at all** before judging them, or
2. what an empty diff means, or
3. that the disambiguation is a two-command mechanical check.

So the knowledge lives in a repo-scoped `fact` a reviewer must think to go read
— and the reviewers who did (TKT-127, TKT-137) got it right while the ones who
did not (TKT-90) did not. That is exactly the kind of norm the shared prompt
exists to carry.

## Proposed change (unified diff)

```diff
--- a/crates/rk-core/src/prime.rs
+++ b/crates/rk-core/src/prime.rs
@@ pub fn render(role: &str, ctx: &PrimeContext) -> String {
         "reviewer" => {
             out.push_str(
                 "Review the changes on your branch against the task requirements. \
+                 FIRST establish there are changes: run `git log <base>..HEAD`. An \
+                 EMPTY branch is not a verdict — it has two causes needing OPPOSITE \
+                 verdicts, so disambiguate before you judge. Find the implementer's \
+                 commit (`rk scan artifact <repo>` records the sha) and run \
+                 `git merge-base --is-ancestor <sha> main`:\n\
+                 - NOT an ancestor ⇒ the work was never committed (check the \
+                 implementer's branch and worktree — it may still be live with the \
+                 work staged). APPROVE would merge a no-op and lose the work: \
+                 REWORK, naming exactly what is missing.\n\
+                 - IS an ancestor ⇒ the work already landed and you are a duplicate \
+                 reviewer. REWORK here manufactures a rework loop for finished work: \
+                 verify the LANDED code against the task, then APPROVE.\n\
+                 Never APPROVE an empty branch you have not disambiguated.\n\
                  Produce exactly one recommendation, choosing by what should happen next:\n\
                  - APPROVE — clean and safe to auto-merge as-is.\n\
```

## Why this is safe

- Confined to the `"reviewer"` arm; the `rat` role and every shared fragment are
  untouched.
- `reviewer_role_has_no_single_task_banner` asserts `contains("APPROVE")` and
  `!contains("only your task")` — both still hold (the added text contains
  "APPROVE"/"REWORK" but not the single-task banner).
- It does not change the meaning of any verdict; proposal 0003's
  APPROVE/REWORK/STOP cost criteria are untouched. It adds a precondition on
  *reading the branch*, which is upstream of choosing a verdict.
- The check is two read-only commands, and it only fires on the empty-branch
  path — a normal review pays one `git log`.

## Companion convention proposal

Filed as a `convention-proposal` artifact this run:

> **`empty-review-branch-must-be-disambiguated`** — A review branch with zero
> commits against main is not automatically a REWORK. Find the implementer's
> commit and run `git merge-base --is-ancestor <sha> main`: not an ancestor ⇒
> the work is uncommitted/lost ⇒ REWORK; is an ancestor ⇒ the work already
> landed and you are a duplicate reviewer ⇒ verify the landed code and APPROVE.
> Never APPROVE an empty branch you have not disambiguated.

## Related

- fact `empty-review-branch-has-two-causes` (grmpl, TKT-127) — the source rule.
- fact `reviewer-launched-before-implementer-committed` (grmpl, TKT-113).
- Proposal 0004 (implementer-side: stop producing empty branches) — the other
  half of the same failure.
- Proposal 0003 / the verdict-cost criteria this sits in front of.

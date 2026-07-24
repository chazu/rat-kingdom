# Proposal 0003 — Give the generic reviewer role verdict criteria and their cost

**Author:** rat-114 (task: refine-prompts)
**Target prompt:** `crates/rk-core/src/prime.rs` → `reviewer` arm of `render`
**Status:** proposed (do NOT apply live — an operator/steward lands this)
**Confidence:** medium (structural prompt gap + one live escalation; see caveat)

## The gap

Two prompts tell a reviewer how to decide a verdict, and they disagree in
richness:

**`steward.cue` review step (lines 96–99)** — the path most reviews take —
gives real criteria:

```
Decide APPROVE (clean, safe to auto-merge), REWORK (fixable issues
remain), or STOP (fundamentally wrong / needs a human call).
```

**The generic `reviewer` role in `prime.rs` (lines 204–210)** — what a reviewer
spawned outside the steward workflow gets — is much thinner:

```
Review the changes on your branch against the task requirements. Produce a
recommendation: APPROVE, REWORK (with specific feedback), or STOP (serious
problems). ...
```

"STOP (serious problems)" tells the reviewer nothing about what STOP *does*.
In the steward workflow the routing is asymmetric and expensive:

- **APPROVE** → auto-lands on main (no human).
- **REWORK** → files a follow-up ticket, holds the branch — a *free, durable
  auto-handoff* (no human).
- **STOP** → emits a `need` tuple that **parks the work for a human merge
  decision** and shows up in `rk inbox`.

STOP is the only verdict that pages a person. A reviewer that reads "serious
problems" with no cost signal will reach for STOP on anything alarming —
including fixable things REWORK was built to absorb without a human.

## Evidence and honest caveat

There is a live instance in the feed:

```
need [rat-kingdom]: steward: reviewer returned STOP for groom-backlog on
  rat/rat-108/steward-review-groom-backlog — needs a human merge decision;
  branch held unmerged
```

**Caveat:** that particular reviewer went through the *steward* step (the richer
prompt), and a grooming decision may legitimately need a human — so this one
STOP was plausibly correct. This proposal is therefore not "the generic prompt
caused that failure." It is: the generic reviewer prompt is measurably weaker
than the one steward.cue proved out, and the weakness (no verdict cost) is
exactly the kind that turns into avoidable human-in-the-loop toil the moment a
reviewer is spawned off the steward path. Aligning the two is low-risk and
closes the gap before it bites.

## Proposed change (unified diff)

```diff
--- a/crates/rk-core/src/prime.rs
+++ b/crates/rk-core/src/prime.rs
@@         "reviewer" => {
             out.push_str(
-                "Review the changes on your branch against the task requirements. \
-                 Produce a recommendation: APPROVE, REWORK (with specific feedback), \
-                 or STOP (serious problems). Record it with \
-                 `rk out artifact <repo> review --payload '{\"recommendation\": ...}'` \
-                 before `rk done`.\n\n",
+                "Review the changes on your branch against the task requirements. \
+                 Produce exactly one recommendation, choosing by what should happen next:\n\
+                 - APPROVE — clean and safe to auto-merge as-is.\n\
+                 - REWORK — fixable issues remain. Give specific, actionable feedback; \
+                 this is auto-handed-off as a follow-up ticket, no human needed. Prefer \
+                 REWORK for anything a rat could fix.\n\
+                 - STOP — reserve for genuine dead-ends: fundamentally wrong, unsafe, or \
+                 needing a human judgment call. STOP parks the work for a human and pages \
+                 the operator, so do NOT use it for anything REWORK can carry.\n\
+                 Record it with \
+                 `rk out artifact <repo> review --payload '{\"recommendation\": ...}'` \
+                 before `rk done`.\n\n",
             );
```

## Why this is safe

- Confined to the `reviewer` arm; the `rat` role is untouched.
- No test asserts on the reviewer blurb's wording (the tests check fragment
  inclusion and the "APPROVE/REWORK/STOP" vocabulary is preserved), so the
  suite stays green.
- It mirrors criteria already validated in production by `steward.cue`; it does
  not invent new policy, it propagates the working one to the generic path.

## Companion convention proposal

Filed as a `convention-proposal` artifact this run:

> **`reviewer-stop-is-for-human-dead-ends`** — A reviewer's STOP is reserved for
> genuine dead-ends (fundamentally wrong, unsafe, or needing a human judgment
> call); it pages the operator and parks the branch. Anything a rat can fix is
> REWORK, which auto-hands-off as a ticket with no human in the loop.

## Related

- `examples/workflows/steward.cue` (source of the proven criteria + the routing
  that gives each verdict its cost).
- TKT-1 (reviewer-drives-rework: the read/when routing this rides on).

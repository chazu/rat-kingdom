# Proposal 0014 — Define the stale-ticket grooming handoff

**Author:** Pretzel-3 (task: refine-prompts)
**Target prompt:** `groom-backlog` descriptions in
`examples/workflows/backlog-groom.cue`, `examples/workflows/nightly-self-improve.cue`,
and `examples/workflows/decompose-then-drain.cue`
**Companion convention:** `stale-ticket-findings-use-an-artifact-handoff`
**Ticket:** TKT-01KZFNGG0E4G90GEZMXTHNACMW
**Status:** proposed (do NOT apply live — this rat only writes proposals)

## The recurring pain

The 2026-08-07 drain produced a prompt-caused failure in the backlog-groom
phase. Pumpernickel-3's `groom-backlog` harness result for workflow
`wf-pgcmb9qszz` reported:

```text
The seven child tickets were created successfully. The daemon rejected the
stale-ticket status update as agent-unauthorized, so I will record that item as
stale in the grooming artifact/report rather than retrying the forbidden
mutation.
```

That same nightly run later failed its `all_ok` gate on a separate empty
`Peanut-3` result, and companion reviewer workflow `wf-7ddrzwb0hv` failed with
an empty `Marbles-3` result. Those are additional drain/harness failures, not
evidence that the stale finding caused the gate failure. The groom instruction
that drove Pumpernickel-3's failure was only:

```text
Merge duplicates (note the survivor in each), and flag stale items.
```

That wording does not say whether “flag” means a ticket status, an artifact, or
a follow-up ticket. The documented ticket lifecycle is
`open → claimed → in_progress → blocked → done → closed`; `stale` is not a
status. Agents therefore have no safe, explicit operation for this required
part of grooming and can spend the run attempting an unauthorized mutation.

The same instruction is copied into three shipped workflow definitions, so one
ambiguous sentence can repeat on standalone grooming, nightly self-improvement,
and the composed decompose-then-drain path.

## Root cause in the prompt

The prompt names a desired classification (“stale”) without defining its
durable handoff. It also asks the rat to record only `decomposed` and `deduped`
counts, so a rat that discovers a stale item has no prescribed payload shape
for the evidence or the operator action.

This is distinct from a generic daemon authorization failure: the prompt itself
asks the rat to perform an undefined mutation. The fix belongs in the workflow
task description. It should not broaden agent authority or add a new ticket
state.

## Proposed diff

```diff
--- a/examples/workflows/backlog-groom.cue
+++ b/examples/workflows/backlog-groom.cue
@@
-                    Merge duplicates (note the survivor in each), and flag stale items.
-                    Do NOT start any ticket. Record what you changed:
-                      rk out artifact $RK_REPO backlog-groom --payload '{"decomposed": N, "deduped": M}'
+                    Merge duplicates (note the survivor in each). For a stale item, do
+                    not invent a `stale` ticket status or call an unauthorized ticket
+                    state mutation: the supported lifecycle is open, claimed,
+                    in_progress, blocked, done, closed. Record each stale finding in
+                    the grooming artifact with its ticket id, evidence, and recommended
+                    operator action. File a follow-up ticket if durable work is needed.
+                    Do NOT start any ticket. Record what you changed:
+                      rk out artifact $RK_REPO backlog-groom --payload '{"decomposed": N, "deduped": M, "stale": [{"ticket": "TKT-...", "evidence": "...", "action": "..."}]}'
```

Apply the same semantic change to the copied `groom-backlog` descriptions in
`examples/workflows/nightly-self-improve.cue` and
`examples/workflows/decompose-then-drain.cue`. The latter currently has no
grooming artifact command, so add the same command before “Report done”; the
nightly copy must preserve its existing `Report done` wording and counts.

## Why this is safe

This is a prompt-only handoff clarification. It does not add a ticket status,
change authorization, or mutate the live workflow in this proposal. Existing
decomposition, duplicate-merging, no-start, and no-merge behavior remain
unchanged. The artifact payload is additive; consumers that read only the two
existing counts continue to work.

When landed, add a focused definition test or text assertion covering all three
copies. It should assert that they name the supported lifecycle, prohibit an
invented `stale` status, and prescribe the `stale` artifact field. It should not
assert a daemon authorization implementation detail.

## Durable convention proposal

```json
{
  "rule": "stale-ticket-findings-use-an-artifact-handoff: A grooming rat must not invent a stale ticket status or attempt an unauthorized ticket-state mutation. Record each stale finding in the backlog-groom artifact with ticket id, evidence, and recommended operator action; file a follow-up ticket when durable work is needed. The supported ticket lifecycle remains open, claimed, in_progress, blocked, done, closed.",
  "why": "In nightly-self-improve workflow wf-pgcmb9qszz on 2026-08-07, Pumpernickel-3 followed the vague instruction 'flag stale items', attempted a stale-ticket status update, and reported an agent-authorization rejection. The same ambiguity is copied into three shipped groom prompts; the run also exposed separate empty-result failures at its all_ok and reviewer gates."
}
```

The convention complements the existing artifact handoff and failure-boundary
rules. It keeps ticket authority explicit while ensuring stale findings are
durable and actionable.

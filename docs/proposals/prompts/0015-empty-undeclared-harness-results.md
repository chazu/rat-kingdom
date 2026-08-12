# Proposal 0015 — Classify empty undeclared harness results before prompt edits

**Author:** Warbeak-3 (task: refine-prompts)
**Target prompt:** `prompt-refine` task descriptions in
`examples/workflows/prompt-refine.cue` and
`examples/workflows/nightly-self-improve.cue`
**Companion convention:** `empty-undeclared-harness-results-are-infrastructure`
**Ticket:** TKT-01KZSZ5Z3HWZ54B3DEJ1SGZ95X
**Status:** proposed (do NOT apply live — this rat only writes proposals)

## The recurring pain

Recent nightly and steward failures repeatedly contain a harness result that
never declared completion and produced no usable work product. The signature is
`declared_done: false`, `is_error: true`, usually `tokens: 0`, and an empty
`result` (or a short interrupted turn):

| Run | Date | Evidence |
| --- | --- | --- |
| `wf-90k4t36k67` | 2026-08-04 | `Dart-3`: undeclared error, empty result, zero cost/tokens |
| `wf-pgcmb9qszz` / `wf-7ddrzwb0hv` | 2026-08-07 | `Peanut-3` / `Marbles-3`: undeclared error, empty result |
| `wf-b9k3zdat5s` / `wf-dhp1wta5gk` | 2026-08-10 | `Emmental-3` / `Jarlsberg-3`: undeclared error, empty result |
| `wf-zw9jgbz6gx` / `wf-616vca7nfw` | 2026-08-11 | `Martin-3` / `Basil-3`: undeclared error, empty result |

The same feed contains an interrupted reviewer turn (`wf-bnznmvx70b`) whose
last text stops before its final proof. These records establish a harness or
workflow boundary, not a causal role-prompt omission. A prompt-refinement rat
that treats an empty result as evidence about the task wording can create a
speculative patch while the real owner is the harness provisioning, process
termination, or workflow timeout path.

This proposal complements 0013's general failure-boundary rule. 0013 names
missing tools, unavailable models, policy refusals, authorization failures,
merge collisions, and repository-gate failures; it does not name the durable
`declared_done`/`is_error`/empty-result signature that is now recurring in the
nightly feed.

## Root cause in the prompt

The two refine task descriptions say to cross-reference `workflow_failed`
events but do not say how to interpret a failed `harness_result` with no
declared completion or usable output. The only causal path they offer is to
look for a weak role prompt or missing convention. That makes an empty harness
result look like a prompt failure by omission, even though the record contains
no agent behavior to analyze.

## Proposed diff

```diff
--- a/examples/workflows/prompt-refine.cue
+++ b/examples/workflows/prompt-refine.cue
@@
    Cross-reference with recent workflow_failed events.
    +For each failure, inspect its matching harness_result before inferring
    +prompt causality. If declared_done is false and is_error is true, with
    +an empty or interrupted result (especially zero tokens), classify it as
    +a harness/process/workflow boundary unless independent evidence shows
    +that the role instructions caused the termination. Record the run,
    +agent, step, declared_done, is_error, tokens, and result evidence and
    +file a ticket for the owning boundary; do not write a speculative
    +prompt patch from an empty result.
    Where a recurring failure traces to a weak role prompt or a missing
```

Apply the same clarification to the identical refine description in
`examples/workflows/nightly-self-improve.cue`. Keep the existing 0013
classification rule when it is present in the live prompt; this proposal is an
additive, more specific check for empty or interrupted harness records.

## Why this is safe

This is a documentation-only classification guard. It does not alter harness
selection, timeouts, workflow execution, completion semantics, or ticket
authorization. It preserves the existing prompt-edit path when an agent's
actual behavior provides causal evidence, while making the absence of such
behavior a fail-closed handoff to the owning infrastructure boundary.

When landed, add a focused workflow-definition assertion for both copies. It
should check that the descriptions mention `declared_done`, `is_error`, empty
or interrupted results, the required evidence fields, and the prohibition on a
speculative patch. It should not assert a particular harness implementation or
ticket title.

## Durable convention proposal

```json
{
  "rule": "empty-undeclared-harness-results-are-infrastructure: A failed harness_result with declared_done=false and is_error=true, especially zero tokens and an empty or interrupted result, is not evidence of a role-prompt defect. Record the run, agent, step, declared_done, is_error, tokens, and result, then file a ticket for the owning harness/process/workflow boundary unless independent evidence establishes prompt causality.",
  "why": "The rat-kingdom feed repeats this signature in wf-90k4t36k67 (2026-08-04), wf-pgcmb9qszz and wf-7ddrzwb0hv (2026-08-07), wf-b9k3zdat5s and wf-dhp1wta5gk (2026-08-10), and wf-zw9jgbz6gx and wf-616vca7nfw (2026-08-11). The current refine task asks for workflow_failed cross-reference but does not define this boundary, so an empty result can invite speculative prompt edits instead of durable infrastructure handoff."
}
```

The convention complements proposal 0013's broader failure-boundary rule and
the landed `declared_done` completion telemetry; it does not weaken the
requirement to investigate a real, non-empty role failure.

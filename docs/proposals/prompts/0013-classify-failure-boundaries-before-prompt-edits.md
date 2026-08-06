# Proposal 0013 — Classify failure boundaries before proposing prompt edits

**Author:** Clover-3 (task: refine-prompts)
**Target prompt:** `prompt-refine` workflow task description in
`examples/workflows/prompt-refine.cue` (standalone and nightly phases)
**Companion convention:** `prompt-refinement-evidence-boundary`
**Ticket:** TKT-01KZAGJDDV056ZJC0WGA7JZ5JZ
**Status:** proposed (do NOT apply live — this rat only writes proposals)

## The recurring pain

The recent failed-run feed contains several different failure boundaries that
the current prompt-refine task describes only as “recurring pain”:

- `wf-1jmpq62hm4` (`nightly-self-improve`, 2026-08-05) failed at the drain
  gate because `Tails-3` reported `` `rk scan fact system` failed: `rk: command
  not found` ``. The chained reviewer instances `wf-fc7984ew7d` and
  `wf-9sxjphm766` stopped for the same missing executable.
- `wf-qjbxfe68ef` (`nightly-self-improve`, 2026-08-03) failed because three
  workers could not use the configured `gpt-5.6-luna` model.
- `wf-njpsbmr2jf` (`steward`, 2026-07-28) failed its repository check in
  `continuous_drain`; the suite output showed a test failure, not a role-prompt
  omission.
- Eight steward runs on 2026-07-29 through 2026-08-03 failed before review
  because a raw workflow command was refused by the repository's
  `require_named_checks` policy.

These are respectively harness provisioning, model/workflow configuration,
repository-gate, and workflow-policy failures. None is evidence that a rat's
role prompt gave the wrong instruction. The same feed also contains the real
prompt/coordination defect in `wf-fp0gwx21zw`: four clean workers collided at
`dismiss_all`, which is the evidence already captured by proposal 0012.

## Root cause in the prompt

Both `prompt-refine` task descriptions currently say:

```text
Where a recurring failure traces to a weak role prompt or a missing
convention, propose a concrete edit ...
```

That condition is correct but underspecified. It does not require the rat to
prove that the failing command, model, workflow policy, or repository gate was
available and that the existing prompt was the causal missing instruction.
Without that classification boundary, an overnight refine pass can spend its
budget re-describing infrastructure failures as prompt work, or create a
speculative prompt patch for a failure that belongs to a workflow or harness
ticket.

## Proposed diff

```diff
--- a/examples/workflows/prompt-refine.cue
+++ b/examples/workflows/prompt-refine.cue
@@
 					Cross-reference with recent workflow_failed events.
+					Classify each failure before proposing a prompt change. A prompt
+					candidate requires evidence that the relevant tool/model/workflow
+					gate was available and that the current role instructions caused
+					the observed behavior. Missing executables, inaccessible models,
+					workflow-policy refusals, daemon authorization errors, merge
+					collisions, and repository-check failures are infrastructure,
+					workflow, or gate findings unless evidence proves otherwise.
+					For those boundaries, do not write a speculative prompt proposal:
+					file one ticket with the exact run id, step, error, and owning
+					boundary, then continue mining other evidence.
 					Where a recurring failure traces to a weak role prompt or a missing
 					convention, propose a concrete edit ...
```

Apply the same clarification to the identical task description in
`examples/workflows/nightly-self-improve.cue`. The wording deliberately keeps
the existing prompt/convention path intact for causal role failures such as
proposal 0012; it only adds a fail-closed evidence threshold for unrelated
boundaries.

## Why this is safe

This is an instruction-only classification guard. It does not change workflow
execution, model selection, check policy, tuple authorization, merge behavior,
or repository tests. It reduces speculative edits and makes each non-prompt
finding durable through a ticket, while preserving the existing requirement
to propose prompt and convention changes when the evidence actually points to
the role instructions.

When landed, add a focused prompt-rendering or workflow-definition assertion
that both refine descriptions contain the classification boundary and the
“do not write a speculative prompt proposal” rule. The assertion should check
the wording, not assume a particular ticket title or workflow implementation.

## Durable convention proposal

```json
{
  "rule": "prompt-refinement-evidence-boundary: Before proposing a role-prompt or convention change, classify the failure boundary and record evidence that the relevant tool, model, workflow policy, and repository gate were available. Missing executables, inaccessible models, policy refusals, authorization failures, merge collisions, and red repository checks are tickets for their owning boundary unless the evidence shows a causal prompt omission; do not write speculative prompt patches.",
  "why": "Recent failures span missing rk in wf-1jmpq62hm4, unavailable gpt-5.6-luna in wf-qjbxfe68ef, a red continuous_drain gate in wf-njpsbmr2jf, and require_named_checks refusals in eight steward runs. The current refine task does not require boundary classification, while wf-fp0gwx21zw demonstrates that a true coordination prompt defect has a distinct causal signature. A durable classification rule prevents infrastructure and gate failures from becoming speculative prompt edits."
}
```

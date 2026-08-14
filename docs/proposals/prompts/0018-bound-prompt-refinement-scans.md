# Proposal 0018 — Bound prompt-refinement evidence scans

**Author:** Pretzel-4 (task: refine-prompts)
**Target prompt:** `prompt-refine` task descriptions in
`examples/workflows/prompt-refine.cue` and
`examples/workflows/nightly-self-improve.cue`
**Companion convention:** `prompt-refinement-uses-bounded-evidence-scans`
**Ticket:** TKT-01KZZ4C9HFWT1ZRVBJYZQ1PP64
**Status:** proposed (do NOT apply live — this rat only writes proposals)

## The recurring pain

The current task asks a rat to cross-reference the event feed but gives no
bounded query plan. On the live rat-kingdom store, an unbounded
`rk scan event rat-kingdom` closed the daemon connection during this run. A
larger hot scan also reported `scan truncated at 10,000 tuples`; without an
explicit limit and follow-up key, the rat cannot distinguish a complete census
from an incomplete read. The workflow definitions themselves acknowledge that
full event-feed mining exceeded the original 25-minute wait and raised the
refine wait to 45 minutes.

This is an evidence-collection boundary, not proof that a prompt failure exists.
The prompt currently gives no instruction to narrow the scope, follow a run id,
or record that a scan was incomplete. That makes exhaustive-sounding claims
unreproducible and encourages a rat to spend its budget repeatedly reading the
same large feed.

## Root cause in the prompt

The task names `workflow_failed` but omits the CLI's bounded search controls
(`--search`, `--top`, and `--hot`) and an escalation path for truncation or a
connection failure. It therefore conflates “the command returned some tuples”
with “the relevant failure corpus was read.”

## Proposed diff

```diff
--- a/examples/workflows/prompt-refine.cue
+++ b/examples/workflows/prompt-refine.cue
@@
                    Cross-reference with recent workflow_failed events.
+                   Use bounded, reproducible queries: start with the repo scope and
+                   `rk scan event $RK_REPO --search workflow_failed --hot --top 50`,
+                   then follow the returned instance/run ids with narrower
+                   `--search` queries. Use `--top` for artifact and fact lookups too;
+                   do not treat an unbounded scan, a truncation notice, or a closed
+                   connection as an exhaustive read. Record the query scope and
+                   limits in the handoff artifact; if a required scan remains
+                   incomplete, file a ticket/obstacle for the read boundary and do
+                   not claim that the feed was fully mined.
                    Where a recurring failure traces to a weak role prompt or a missing
```

Apply the same clarification to the identical task description in
`examples/workflows/nightly-self-improve.cue`. The query example is guidance,
not a claim that 50 is a universal corpus size; the rat may choose a smaller
or larger bound and must record it.

## Why this is safe

This is a documentation-only evidence-quality guard. It does not alter tuple
retention, daemon scan semantics, workflow timeouts, or which failures can
qualify as prompt defects. It makes incomplete reads fail closed and preserves
the existing causal-classification and proposal path when the relevant evidence
has actually been retrieved.

When landed, add a focused definition assertion for both copies. It should
check for bounded event queries, `--search`, `--top`, truncation/connection
handling, and the requirement to record incomplete evidence. It should not pin
the exact limit or require a daemon implementation change.

## Durable convention proposal

```json
{
  "rule": "prompt-refinement-uses-bounded-evidence-scans: Prompt-refinement evidence mining must use scoped, bounded queries with explicit search terms and limits, follow returned run identifiers, and record the query scope/limits. A truncation notice or closed connection is an incomplete read, not an exhaustive corpus; hand it off as a read-boundary ticket or obstacle and do not claim full-feed coverage.",
  "why": "An unbounded live `rk scan event rat-kingdom` closed the daemon connection during the 2026-08-13 refine run, while a larger hot scan reported truncation at 10,000 tuples. The workflow already raised the refine wait from 25m to 45m after full-feed mining exceeded the original budget, but the task prompt still gives no bounded query or incomplete-read rule."
}
```

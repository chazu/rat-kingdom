# Proposal 0017 — Deduplicate prompt-refinement proposals before drafting

**Author:** Pretzel-4 (task: refine-prompts)
**Target prompt:** `prompt-refine` task descriptions in
`examples/workflows/prompt-refine.cue` and
`examples/workflows/nightly-self-improve.cue`
**Companion convention:** `prompt-refinement-deduplicates-before-drafting`
**Ticket:** TKT-01KZZ4DEG5948CT2AXBGN7BDBD
**Status:** proposed (do NOT apply live — this rat only writes proposals)

## The recurring pain

Recent refine runs repeatedly spent a full rat lifetime rediscovering work
already represented in the repository's proposal ledger:

- `01KYMR3WASV4KKTSYVK82V3YQM` is the latest prior `refine-prompts`
  `harness_result`. Asiago-2 reported four proposals, 0008–0011, even though
  those proposal files and their landed/implemented entries already existed.
  The result records 6,652,666 tokens and `$6.05` cost.
- `01KZWHQRD1EQB44J47V7PJRAEY` records Triss-3 rerunning the work for proposal
  0015, then discovering that `cb53dd4` had already landed it. The rat created
  no duplicate commit, but still spent 7,276,404 tokens on a no-op discovery.
- The durable ledger in `docs/proposals/prompts/README.md`, the proposal files,
  tickets, and convention-proposal artifacts already provide the identifiers
  needed to avoid both repeats. The refine task description never tells the rat
  to consult any of them before writing a proposal or ticket.

These are not merely duplicate filenames: repeating an already-landed finding
can mint duplicate tickets or convention ballots, consume a multi-million-token
run, and make the operator decide whether two documents differ when they do
not.

## Root cause in the prompt

The task says to scan recurring pain and file a ticket for each proposed
change, but it does not define a pre-draft deduplication check. It exposes the
proposal directory and artifact command without saying that an existing
proposal, ticket, convention, or delivery artifact is authoritative evidence
of prior coverage. A rat can therefore treat a known finding as new simply
because the current run did not author it.

## Proposed diff

```diff
--- a/examples/workflows/prompt-refine.cue
+++ b/examples/workflows/prompt-refine.cue
@@
                    Cross-reference with recent workflow_failed events.
+                   Before writing a proposal or ticket, inspect
+                   docs/proposals/prompts/README.md and matching proposal files,
+                   then check `rk ticket list --repo $RK_REPO --status open` and
+                   `rk scan artifact $RK_REPO --search prompt --top 50`. Match by
+                   root cause and target, not title alone. If an existing proposal,
+                   ticket, convention, or artifact already covers the finding, do
+                   not create a duplicate file, ticket, or ballot: record the
+                   existing identifiers in a handoff artifact and continue mining.
+                   If only a materially new delta remains, link the prior item and
+                   state exactly what is new.
                    Where a recurring failure traces to a weak role prompt or a missing
```

Apply the same clarification to the identical task description in
`examples/workflows/nightly-self-improve.cue`. A landing change should leave
the existing proposal, ticket, and convention commands intact; this is a
preflight ordering and deduplication guard, not a new authority or lifecycle.

## Why this is safe

This is an additive documentation-only guard. It does not change workflow
execution, ticket state, tuple authorization, or convention promotion. It
prevents duplicate durable work while preserving a path for a genuinely new
root cause or a materially new delta to extend an earlier proposal.

When landed, add a focused definition assertion for both copies. It should
check that they name the proposal ledger, open-ticket lookup, artifact lookup,
root-cause matching, and the no-duplicate rule. It should not require a
particular ticket title or artifact payload schema.

## Durable convention proposal

```json
{
  "rule": "prompt-refinement-deduplicates-before-drafting: Before creating a prompt proposal, ticket, or convention ballot, compare the finding by root cause and target against the proposal ledger, existing proposal files, tickets, conventions, and artifacts. If it is already covered, do not create duplicate durable work; record the existing identifiers and continue. If a materially new delta remains, link the prior item and state the delta.",
  "why": "The latest refine run 01KYMR3WASV4KKTSYVK82V3YQM re-derived proposals 0008-0011 already represented in the repository after spending 6,652,666 tokens and $6.05. Run 01KZWHQRD1EQB44J47V7PJRAEY similarly rediscovered already-landed proposal 0015 and spent 7,276,404 tokens. The task prompt has no pre-draft deduplication step, so repeated rats can mint duplicate tickets or ballots for known findings."
}
```

# Proposal 0016 — Reassert artifact handoff after live daemon drift

**Author:** Django-3 (task: refine-prompts)
**Target prompt:** `crates/rk-core/src/prime.rs` → `FRAGMENT_SINGLE_TASK` and
`FRAGMENT_COMPLETION` step 4
**Companion convention:** `hand-off-through-artifact-and-ticket-not-fact`
**Follow-up:** proposal 0011 / TKT-01KYMQWRSD70DTQCXDYST0Q4FK
**Status:** proposed (do NOT apply live — this rat only writes proposals)

## The recurring pain

The shared prompt still instructs a rat to write a `fact` tuple in two places:

```text
post a `fact` or `need` tuple instead
file a ticket and post a `fact` tuple describing it
```

The deployed fleet convention says the opposite: agent callers must not write
`fact`, `convention`, `task`, `available`, or furniture tuples. Findings must
be handed off with `rk ticket new` and `rk out artifact`; `need` and `obstacle`
remain the live signals. The convention is already promoted, so this proposal
does not mint a duplicate ballot.

The mismatch is not theoretical. The durable convention artifact
`convention-proposal-hand-off-through-artifact-not-fact` records the observed
`forbidden` response in both `rat-kingdom` and `system` scope. The current
`prime.rs` text remains unchanged, and proposal 0011's “implemented” status
describes a source-side daemon authorization experiment, not a repair of the
deployed prompt/runtime contract. A rat following the current completion
protocol can still spend its final turn retrying a forbidden handoff or lose a
pre-existing failure it was required to preserve.

## Root cause in the prompt

Proposal 0011 identified the stale instruction, but its prompt-side diff was
left historical after the daemon-side authorization path was described as the
resolution. The live fleet convention now explicitly requires the artifact
route, so the shared prompt must be made fail-closed against `rk out fact`
regardless of source/deployment drift.

## Proposed diff

```diff
--- a/crates/rk-core/src/prime.rs
+++ b/crates/rk-core/src/prime.rs
@@ FRAGMENT_SINGLE_TASK
-other work, even if you notice claimable tasks or open needs — post a `fact`
-or `need` tuple instead and let the orchestrator route it.
+other work, even if you notice claimable tasks or open needs — file a ticket
+(`rk ticket new`) or post a `need` tuple instead and let the orchestrator route
+it. Do not write a `fact` tuple: agent callers are forbidden from writing it.
+Use `rk out artifact <repo> <name> --payload '{...}'` for a durable finding.
@@ FRAGMENT_COMPLETION step 4
-will race you) — file a ticket and post a `fact` tuple describing it, then
-finish your own task.
+will race you) — file a ticket and record it as an artifact
+(`rk out artifact <repo> preexisting-failure --payload '{...}'`), then finish
+your own task. Do not retry `rk out fact`: an agent caller receives `forbidden`.
```

The wording should preserve the existing ticket/need route while making the
artifact handoff explicit and non-retriable. It should be applied only to the
shared prompt source; live prompts remain untouched by this proposal.

## Why this is safe

This is a documentation-only correction to a command the deployed convention
already forbids. It does not broaden tuple authorization, change fact ranking,
or add a ticket status. A focused prompt test should assert that both rat and
reviewer renderings contain `rk out artifact`, do not contain the stale phrase
`post a fact tuple`, and name the non-retriable `forbidden` path. No workflow execution
semantics or repository files are changed.

## Durable convention handoff

`hand-off-through-artifact-and-ticket-not-fact` is already a promoted system
convention. Reuse it; do not create a near-duplicate suggestion. The proposal
artifact and ticket for this follow-up should point back to that convention and
to proposal 0011's stale status so an implementer can land the prompt-side
repair without reopening the authorization design.

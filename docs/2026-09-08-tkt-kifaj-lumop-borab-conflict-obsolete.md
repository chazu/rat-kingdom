# TKT-kifaj-lumop-borab: conflict-rework ticket is obsolete

## What this ticket asked

The landing pipeline could not merge `rat/rizzo-14/tkt-humih-nusok-lozus` into
`main` — a genuine merge conflict in `crates/rk-cli/src/observation_cmds.rs`,
not a review verdict (see the `landing_conflict_rework_dispatch` event at
2026-09-08T12:23:49Z, `dispatch_key` ending
`...TKT-humih-nusok-lozus\0TKT-kifaj-lumop-borab`). This ticket was filed as
the bounded orchestrator-authority correction dispatch for that conflict,
suggesting either `attention.decide` or a respawn based on
`rat/rizzo-14/tkt-humih-nusok-lozus`.

## Why resolving it now would be wrong

Between the conflict (12:23) and this dispatch actually spawning (16:54, per
`agent_spawned` for Scritch-14), the underlying ticket `TKT-humih-nusok-lozus`
(D1: independent progress evaluator) was reassigned to Tunnel-14, who forked
fresh off current `main` (not off Rizzo-14's branch) and independently
reimplemented D1 from scratch:

- `rat/tunnel-14/tkt-humih-nusok-lozus`, commits `bb264a7` (feat) and
  `2b55d6a` (rustfmt), 632 lines across the same two files
  (`observation_cmds.rs`, `observation_store.rs`).
- Declared done twice (`task_done` 15:01:45, 15:35:20); a full
  `mise run verify` (not just verify-changed) passed at commit `2b55d6a`,
  recorded as `verification_proof` 01M20Z0VRFSAR719J5NJ8J1A95, exit 0.
- Artifacts `d1-progress-evaluator` and `d1-progress-evaluator-verified`
  (Tunnel-14) describe a materially more complete implementation than
  Rizzo-14's (typed bounded-wait exemptions, generation-bound progress
  signatures, `derive_qualification` rewired off the interim proxy, two
  deliberately-deferred follow-ups already ticketed).
- A landing attempt at 15:23 (`landing_processed`, outcome `gate-held`) failed
  on a verification-admission WIP-limit-1 timeout — infrastructure, not a
  content conflict. No landing attempt has been retried since the 15:35
  re-verification as of this writing.

Meanwhile `rat/rizzo-14/tkt-humih-nusok-lozus` (`d3485ec`, `b8a5a1f`) is now
29 commits behind `main`. Diffing it against current `main` on the same two
files shows 985 deleted / 634 inserted lines — almost entirely staleness, not
intentional change (`git diff --stat main rat/rizzo-14/tkt-humih-nusok-lozus`).
Resolving its conflict and landing it now would either fail again against a
`main` that has moved further, or — if forced through — silently regress or
duplicate Tunnel-14's already-verified, more complete D1 implementation in the
exact same files. Six other rats independently reached the same "wait for D1
to land, don't reimplement" conclusion while blocked on this ticket
(Rummage-14, Cheddar-14, Cinder-14, Shrew-14, Crumb-14, Gouda-14/Burrow-14 on
the parent coordination ticket) — this is that same pattern applied to the
conflict-rework side of the fork.

## Disposition

No source change made. `rat/rizzo-14/tkt-humih-nusok-lozus` should be treated
as abandoned; Tunnel-14's branch is the authoritative D1 delivery pending a
landing retry (tracked separately — not this ticket's scope). Filed a ticket
recommending an operator close TKT-kifaj-lumop-borab and delete the stale
branch, since `ticket.update`/branch deletion of another agent's ref are both
outside an agent caller's authority.

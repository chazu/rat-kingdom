# Backlog groom — 2026-08-08

This pass reviewed `rk ticket list --status open` for `rat-kingdom` on
2026-08-08. No ticket was started or state-mutated.

Counts: **0 decomposed**, **0 deduped**, **0 stale flagged**.

## Existing decomposition decisions

The oversized items already have durable child tickets, so this pass did not
create a second layer of speculative tickets:

| Parent | Existing shape | Decision |
| --- | --- | --- |
| TKT-85 | TKT-98 (done), TKT-101 (open) | Keep open as the roadmap gate for P14/P15; do not close the umbrella. |
| TKT-86 | TKT-149, TKT-150, TKT-151 | Keep the three gated implementation slices. |
| TKT-87 | TKT-152 through TKT-157 | Keep the six slices; the ticket correctly distinguishes startable from gated work. |
| TKT-101 | TKT-01KZD3JW4TVYV3NPJGJ9MS2KV1, TKT-01KZD3JW4X6CAG93GMNHWKF778, TKT-01KZD3JW51R8CY6P6GRFD6VTRC, TKT-01KZD3JW548Q2MWDV49VREK1WG | Keep benchmark-driven statefulness split by proof obligation. |
| TKT-141 | TKT-01KZFN4DGWTC9DQF711R7B32YP, TKT-01KZFN4DJA9MJ3FJ2WBXV0Y9DY | Keep the file-disjoint law-oracle sweep. |
| TKT-142 | TKT-01KZ3YVWY39TT8ZEP8G00HXC7P, TKT-01KZ3YW2DWVQEN46YF3WK2CRBZ | Keep focused link repair separate from repo-wide inventory/gate work. |
| TKT-152 | TKT-01KZD3JW57JK817175P6EV5AP3, TKT-01KZD3JW5A6HPY3TQ0ZE15CPJZ, TKT-01KZD3JW5DCAF6F6784PHFMS4B | Keep representation, API threading, and compatibility verification separate. |
| TKT-155 | TKT-01KZ3YV1MHZTE6G3NQ2RDK06RC, TKT-01KZ3YV62A02XSXS8H33FG17A6 | Keep mechanical dependency retest separate from gossip-topic design. |
| TKT-179 | TKT-01KZFN4DKT94DSJ4RF03YTXPD3, TKT-01KZFN4DNAPQDG8HRD6RHT40M4 | Keep diff-scope and land-result gate restoration separate. |

## Duplicate and stale review

The apparent performance cluster TKT-125, TKT-139, TKT-144, and TKT-145 is
not a duplicate set: each names a distinct call site and retains an
independent cheaper fix. TKT-130 and TKT-140 are separate semantic decisions,
not duplicates of that cluster. The open rat-kingdom follow-ups for workflow
checks and stale-ticket prompt handling are also distinct ownership items.

No open ticket had evidence of being obsolete, already landed, or superseded
without a durable replacement. In particular, TKT-141 and TKT-179 remain
active parents because their children are still open; TKT-85 remains open as a
deliberate dependency gate. Any future stale finding should be handed off in
the groom artifact with evidence and an operator action rather than by
inventing a ticket-state transition.

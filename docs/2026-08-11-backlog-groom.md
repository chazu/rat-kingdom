# Backlog groom — 2026-08-11

This pass reviewed the live global result of `rk ticket list --status open`.
The repo-scoped `rat-kingdom` backlog is empty; the open tickets are in the
`grmpl` scope. No ticket was started or state-mutated.

Counts: **0 decomposed**, **0 deduped**, **0 stale flagged**.

## Existing decomposition decisions

Every oversized open umbrella already has durable child tickets, so this pass
did not create speculative second-level work:

| Parent | Existing shape | Decision |
| --- | --- | --- |
| TKT-85 | TKT-98 (done), TKT-101 (open) | Keep open as the load-bearing roadmap gate for P14/P15. |
| TKT-86 | TKT-149, TKT-150, TKT-151 | Keep the three gated P14 slices. |
| TKT-87 | TKT-152 through TKT-157 | Keep the six P15 slices and their startable/gated distinction. |
| TKT-101 | Four P13b children | Keep statefulness split by proof obligation and benchmark trigger. |
| TKT-141 | Two law-oracle sweep children | Keep the file-disjoint split. |
| TKT-142 | Focused repair plus repo-wide inventory children | Keep repair separate from adding the doc gate. |
| TKT-152 | Three cross-domain read children | Keep representation, API threading, and compatibility verification separate. |
| TKT-155 | Unpin retest plus gossip-topic design children | Keep the mechanical check separate from the design decision. |
| TKT-179 | Diff-scope plus land-result gate children | Keep the two steward gates independently verifiable. |

TKT-85 remains intentionally open even though its direct work is split: it is
the dependency gate for the P14/P15 tier, not a work item to close casually.

## Duplicate and stale review

TKT-125, TKT-139, TKT-144, and TKT-145 are distinct performance call sites,
not duplicates; their shared missing-index theme is already recorded in the
ticket bodies. TKT-130 and TKT-140 are separate aggregate and event-order
decisions. No duplicate survivor was therefore selected.

No open ticket had evidence of being obsolete, already landed, or superseded
without a durable replacement. Findings were handed off through the
`backlog-groom` artifact (tuple `01KZSYNYZFH73MMP8TCTJH7Q2P`); future stale
findings should likewise record ticket, evidence, and operator action rather
than inventing a ticket-state transition.

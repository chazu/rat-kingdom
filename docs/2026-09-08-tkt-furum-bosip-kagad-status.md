# TKT-furum-bosip-kagad: D3 decomposition closure status (2026-09-08)

*Coordination record. This ticket's job is to decompose D3 (`docs/2026-09-06-r1-qualification-deliverables.md`)
into independently-dispatchable tickets, not to implement D3's proposed-work
items directly. D3 spans two kinds of work with different shapes: a ticket-identity
audit across several daemon entrypoints (already ticketed elsewhere before this
ticket was filed) and a release sequence gated on two sibling deliverables (D1,
D2) that belong to their own dispatches. Decomposing into separately claimable
children is the correct scope for this coordination ticket.*

## Decomposition (complete, verified)

Filed by Templeton-14, re-verified here — no gap found:

| D3 proposed-work item | Ticket | Status at this check |
| --- | --- | --- |
| 1-2: identity-audit slice (legacy/proquint alias comparison gaps) | `TKT-gusab-hihus-tijof` | in_progress (Swipe-14) |
| — derived fix: `terminal_assignee_with_handoffs` alias gap | `TKT-fabok-birib-rubun` | open |
| — derived fix: `ticket_reopen_sweep_at` alias gap | `TKT-gotup-lamur-pahub` | in_progress |
| — derived fix: `queued_entry_for` alias gap | `TKT-linaj-sahir-tahif` | in_progress |
| — derived fix: canonicalize-at-spawn-seam decision | `TKT-fusuf-galoz-gosir` | in_progress |
| 3: duplicate-dispatch disposition | `TKT-fovad-pulaj-zudar` | **closed**, landed — see `docs/2026-09-08-tkt-fovad-pulaj-zudar-duplicate-dispatch-disposition.md` (disposition: CONFIRMED real duplicate dispatch, not an alias-comparison artifact) |
| 4-6: release through verify-full, record deployment identities, reconcile tracker | `TKT-susiv-fuhaf-zogof` | open, unassigned — blocked on D1 + D2 + D3.3 per the source doc's own sequencing note |

The identity-audit slice (items 1-2) predates this ticket and is referenced,
not duplicated, per this ticket's own body. Items 3 and 4-6 are the two
children this ticket filed directly; both exist, are correctly scoped, and
D3.3 has already landed.

## Sibling deliverables gating the release ticket

| Deliverable | Ticket | Status at this check |
| --- | --- | --- |
| D1: external acceptance auditor | `TKT-humih-nusok-lozus` | in_progress (Tunnel-14) |
| D2: executable pilot acceptance contract | `TKT-fakad-ronol-sodab` | closed, landed main |

D3.4-6 (`TKT-susiv-fuhaf-zogof`) cannot claim its entry condition until D1
lands; that is a dependency on a sibling dispatch, not a gap in this ticket's
decomposition.

## Two closed tickets referenced by D3's release scope

The source doc (lines 329-330) names `TKT-kadov-nabod-nofov` (pilot-metrics
rework-lineage attribution) and `TKT-bipin-rinos-gatad` (retire resolved
historical need rows) as "open despite implementation in the working tree...
include in D3." Both are now **closed**. They are not children of this
ticket and need none filed here: their acceptance/delivery evidence is release
scope, which is exactly what `TKT-susiv-fuhaf-zogof` (D3.4-6) already covers
when it reviews "the seven improvements already present in the working tree
per `docs/2026-09-05-usefulness-improvements.md`."

## What remains

Nothing actionable for this coordination ticket directly. Both children exist,
are correctly scoped against the source doc, and D3.3 is already landed.
D3.4-6 is open and unassigned, correctly blocked on D1 (in progress) and its
own sibling D3.3 (satisfied). Nothing here should be worked ahead of its own
dispatch.

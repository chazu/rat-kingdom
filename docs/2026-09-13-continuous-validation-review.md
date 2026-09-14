# Adversarial plan review

Review baseline: source `3cfabbe90337b8d7214fe4d5791a16dd7ce44b03`.
Round 1 draft: `466d5b321e0a8428d62318e0051de1dc6f5573e2`.
Requirements: `2026-09-13-continuous-validation-requirements.md`.
Two independent reviewers inspected requirements and standards/operational
failure modes. Neither changed source or RK state. Findings below retain their
separate axes; a correction is not closed until the revised draft is reviewed.

## Standards and operational failure modes

1. Gate worktree reset before admission permits candidate B to change A's source
   while A is running. Correction: workspace lease across reset/cleanup or an
   isolated candidate tree, with an actual bytes-after-overlap regression.
2. Reviewer cache miss while gate runs queues a duplicate expensive check.
   Correction: new prerequisite P0 for exact in-flight coalescing, independent
   caller ownership, and settlement before permit release.
3. Current rollover retains its worker recovery set only in CLI memory.
   Correction: persist generation/launch/prior-dispatch identities before stop
   and per-worker recovery receipts; test interruption and partial recovery.
4. Boot success does not cover a responsive successor whose operational progress
   fails. Correction: bounded independent launcher supervision and local rollback
   control, with eligible-work-aware progress tests and explicit recovery budget.
5. State schema alone omits effective config overrides, activated policies and
   surviving clients. Correction: validate all compatibility dimensions before
   activation/rollback and require supported client protocol or reconnection.

Nonblocking: add symmetric cancellation when review rejects first; preserve exact
cleanup and evidence. Authority boundaries and external unknown outcomes were
found appropriately scoped.

## Requirements and completeness

1. Full validation of B against T predictably repeats after A advances T.
   Correction: P2 looks ahead only for reusable source review and cheap work;
   expensive merged-candidate checking stays at the FIFO head. P5 no longer
   depends on P2. Success measures latency and compute, not simultaneous activity.
2. Combined releases can exceed the 3000-line final landing budget and stall.
   Correction: immutable admissible prefix selection, overflow retention, explicit
   separately bounded policy for exceptional constituents, and edge-specific
   protected-path authority. Regression covers individually valid/aggregate-invalid
   inputs without blanket scope-gate removal.

Nonblocking: ticket contracts must name complete CLI/RPC journeys and reusable
policy authority rather than add per-transition human approvals. Existing
analytics reuse, the independent local-service adapter, and honest null benefit
results were found sound.

## Status

Round 1: request changes, 5 standards blockers and 2 requirements blockers.
Round 2 reviewed commit: `66aa837e47f2a06bd4fa9143b74913655ad714ae`.

Standards/operational review: approved; all five original blockers resolved in
the plan, no new concrete blocker. Explicitly requires implementation tests to
exercise the real production entry points and failure sequences.

Requirements review: approved; both original blockers resolved, original goals
preserved, no dependency cycle or new standing agent. The public journeys and
policy authority distinction are concrete enough to execute.

All seven round-1 blockers are closed at the plan level. This approval does not
claim source implementation, installed behavior, or operational proof. Ticket
acceptance retains the failure sequences. The status edit and review disposition
at that revision were administrative; reviewed design content was unchanged at
that point.

## 2026-09-14 amendment: independently shippable vertical slices

The operator made thin vertical slices, shipped independently and promptly, a
design pillar. The previous dependency graph made coalescing a prerequisite for
unrelated capabilities and bundled too much acceptance into whole workstreams.
The amendment changes that sequencing while preserving R1-R9 and their final
capability/failure-path requirements.

- Section 1 now defines a usable slice and its ticket contract: current-system
  compatibility, explicit hard dependencies, bounded evidence/cost, operational
  delivery and recovery, and a published BBS contract.
- Section 11 retains P0-P14 as coverage tracks and identifies first usable
  deliveries. It replaces blanket track dependencies with specific capability
  prerequisites. Existing ticket dependencies must be reconciled before dispatch;
  editing this plan does not itself change live RK state.
- The earlier standards finding about duplicate checks remains valid. Its
  resolution is now separate acceptance for exact coalescing and explicit
  duplicate-cost accounting for overlap, rather than mandatory P0 completion
  before unrelated shipping. Workspace, cancellation and proof correctness
  remain mandatory for every slice that exposes those paths.
- Initial inventory, flags, scorecards and manual operations can use existing
  bounded mechanisms. Later automation retains its stronger acceptance; manual
  capability does not count as automatic promotion or recovery.
- Independent-delivery metrics retain root lineage and deferred scope; more
  tickets or integrated disabled code alone do not prove delivered benefit.

The amendment initially had an author consistency check only. The earlier
approval applies only to its recorded revision. The independent amendment review
below covers the changed contracts and dependency removals; unrelated accepted
deliveries do not repeat their program-level review.

### Independent amendment review

Reviewed source: `6e486af643482c56eaa8dfa749437be027d7b776`, compared with
`108692631b6da730f81c7f8b91424049523ece01`. Both reviewers checked existing
capabilities against deployed baseline `3b12e720d21b0cf3cdb6aa9a5c5512644e2a9b2b`.

- Requirements reviewer (`promotion_plan_requirements`): **approve**, no blocking
  findings. R1-R9 and the full capability/failure-path requirements remain intact.
  Existing execution, installation, configuration and analytics paths can support
  independently useful deliveries without unfinished check sharing.
- Standards reviewer (`promotion_plan_standards`): **approve**, no blocking
  findings. Capability-specific correctness, authority, resource and recovery
  prerequisites remain mandatory. Existing full checks, native review and exact
  source/policy binding still govern each delivery.

Implementation constraints retained from review: tickets must name actual
required capabilities; flags must control real behavior; inventory and reporting
must provide their stated operator journeys. Initial inventory recipes must
explicitly enforce build parallelism limits because the existing installer does
not provide them itself. Overlap must account for duplicate check cost until
coalescing is accepted. Manual recovery does not establish automatic supervision.

This approval covers the design amendment only. It does not accept the earlier
P0 source, reset spent authority, or establish implementation, delivery or
deployment evidence. The subsequent review-record edit is administrative;
reviewed design and requirements are unchanged.

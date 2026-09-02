# Durable delivery finalization

*Status: stabilized after adversarial review. 2026-09-01. Review dispositions:
`docs/2026-09-01-three-enhancements-adversarial-review.md` D1-D4.*

## Decision

A successful target advance is not terminal until its delivery facts have
settled. The existing durable landing queue entry is the finalization receipt:
the pipeline persists `Landing` together with the exact candidate before it
calls the sole target-advance seam, then retains that entry until ticket
delivery and the exact agent generation's merge pointer are both settled.

`finalize_delivery` failures must propagate. They must not be logged and
converted into `LandingOutcome::Landed`, because `mark_processed` plus queue
removal would then make incomplete finalization look terminal.

## Problem

`LandingPipeline::finalize_landed` currently calls `record_delivery`, which
logs and swallows any `Supervisor::finalize_delivery` error. Every caller then
marks the candidate processed; the drain removes its queue entry. Git is
correct, but the ticket and agent registries may remain stale.

The pipeline already has the recovery information it needs:

- `LandingQueueEntry.status == Landing` is persisted before target advance.
- `candidate_sha`, `candidate_base`, branch, target, task, and `source_spawn`
  are durable on the same entry.
- `recover_completed_land` recognizes a target at the exact candidate and
  re-enters finalization. The stabilized design extends this to a target that
  contains the exact candidate as an ancestor, because a later target advance
  must not erase evidence that this prepared merge already landed.
- ticket delivery and generation merge-pointer writes are idempotent for the
  same commit and fail closed for a conflicting commit.

The missing property is error propagation and queue retention.

## Invariants

1. Git target advance remains the sole irreversible effect.
2. A queue entry is persisted in `Landing` before target advance.
3. `landing_processed(outcome = landed)` is written only after delivery
   finalization succeeds.
4. A landed entry whose finalization fails remains discoverable after restart.
5. Replaying the same exact delivery is idempotent.
6. A conflicting recorded merge pointer remains a visible failure; it is never
   overwritten.
7. Content-free and non-merged outcomes do not manufacture delivery records.
8. Batch landing retains every member whose individual finalization has not
   settled.

## Design

Change the finalization call chain to return `Result`:

1. `record_delivery` returns `Result<()>` and propagates
   `Supervisor::finalize_delivery` errors.
2. `finalize_landed` returns `Result<LandingOutcome>`.
3. Single-entry land paths use `?`; they do not call `mark_processed` after a
   failed finalization.
4. Before preparing or gating a batch, detect a shared `Landing` candidate
   already contained in the target and finalize every member directly. No
   second gate or target advance runs for an already-landed batch.
5. Batch landing finalizes and marks each member independently when practical.
   Whole-batch retention is an acceptable first stabilization because replay
   is idempotent, but the failing task must be visible.
6. On retry, `recover_completed_land` reconstructs the landed result when the
   exact candidate equals or is an ancestor of the target, then repeats
   finalization.
7. A finalization failure emits a deduplicated
   `landing_finalization_failed` event. It is visibility only; the queue entry
   remains the recovery authority.

The durable receipt remains the queue entry. No second journal, tuple kind, or
reconciliation heuristic is introduced.

## Batch semantics

The current batch drain returns an error for the whole batch, which would leave
already-finalized members queued. That replay is safe, but it produces noisy
duplicate work and prevents per-member removal. Batch processing should instead
return a result per entry: successes are marked/removed; finalization failures
remain in `Landing` and are reported as retryable processing failures.

If adapting the batch return shape would spread beyond the landing module, the
stabilized first implementation may retain the whole batch. That is safe because
successful delivery finalization is idempotent, but a regression test must prove
the replay and the limitation must remain documented.

## Failure and recovery matrix

| Failure point | Durable evidence | Recovery |
| --- | --- | --- |
| Before `Landing` persist | ordinary queued entry | prepare/gate again |
| After `Landing` persist, before Git advance | exact candidate entry | CAS reports stale or performs advance |
| After Git advance, before ticket write | `Landing` plus target containing candidate | single or batch recovery finalizes |
| After ticket write, before generation write | ticket delivery plus queue entry | idempotent ticket write, then generation write |
| After both writes, before processed marker | both ledgers plus queue entry | idempotent finalization, then mark processed |
| After processed marker, before removal | processed marker plus queue entry | existing reconciliation removes entry |

## Verification

- Unit test: force ticket finalization failure after target advance; assert no
  landed processed marker and the queue entry remains `Landing`.
- Restart test: move the target beyond the candidate, remove the fault, and
  process the same entry; assert ticket
  delivery, exact generation merge pointer, one terminal marker, and queue
  removal.
- Idempotence test: fail between ticket and generation writes, then replay.
- Conflict test: a different generation merge pointer remains unchanged and the
  queue remains visible.
- Batch test: target already contains the shared candidate; assert no gate or
  second advance and eventual finalization for every member.
- Visibility test: repeated failure produces one durable event for the same
  task/candidate/error identity.

## Implementation status

Implemented in `crates/rk-daemon/src/landing.rs`. Finalization errors now retain
the `Landing` receipt, recovery uses candidate ancestry, landed batches bypass
gating and target advance, and repeated failures emit one keyed
`landing_finalization_failed` event. The first implementation deliberately
keeps the safe whole-batch retention behavior described above.

The focused failure/restart and batch-recovery regressions pass. On 2026-09-01,
`mise run verify` passed all 1,755 affected tests and `mise run verify-full`
passed the protected workspace suite and doc tests.

## Non-goals

- Replacing the tuplespace-backed landing queue.
- Treating reconciliation as the ordinary completion path.
- Weakening exact-generation fencing.
- Changing gate, review, or target-advance semantics.

## Rollback

The change adds no durable schema. Rolling back restores best-effort behavior;
any retained `Landing` entry remains readable by the current queue schema and
can be drained after the fault is corrected.

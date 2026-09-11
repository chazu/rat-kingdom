# Collaboration reliability batch

## Problem and scope

The operator approved the ten highest-ranked backlog tickets, in usefulness
order, after the BBS briefing/help-thread changes. The goal is to reduce duplicate
effort, false failures and silent stalls so agents can use the shared BBS productively.
Existing unmerged implementations are reviewed and adapted to the current source;
unrelated private and untracked files remain outside delivery.

## Implementation order

1. `TKT-harid-nakam-rozuh`: admission expiry before a check starts reports an
   infrastructure verdict and consumes the existing durable one-retry budget.
   Execution timeouts retain their current fail-closed behavior.
2. `TKT-gotup-lamur-pahub`: recognize legacy aliases in both reopen-sweep guards.
3. `TKT-futaz-gudiv-sujok`: canonicalize resolvable ticket tasks at spawn, retaining
   historical comparison-side compatibility and free-text tasks.
4. `TKT-nifor-lojoj-lodof`: corroborate negative process-liveness snapshots with
   one bounded retry. The user's approval of this batch supplies the requested
   policy decision; no gate is weakened or disabled.
5. `TKT-humih-nusok-lozus`: finish D1's independently maintained progress clocks,
   evidence gaps, bounded waits and replay behavior using the existing branch.
6. `TKT-susiv-fuhaf-zogof`: verify the complete release candidate, confirm D2 and
   duplicate-dispatch prerequisites, record release/rollback identities, activate
   it and reconcile the tracker. No foreign pilot or milestone acceptance is
   implied by source validation or deployment.
7. `TKT-fumak-lihuv-sabas`: diagnose and correct the restart/budget-stop flake.
8. `TKT-furuk-bajad-rakov`: diagnose and correct the review-ceiling crash flake.
9. `TKT-pahip-ligaj-divok`: measure contention before/after bounded admission and
   verify queue fairness, recovery and diagnostics using isolated workloads.
10. `TKT-susog-bovot-huzit`: expose per-ticket landing wait state through status.

The release step is prepared at its position in the sequence. Any failure that
blocks its acceptance is repaired before release; final delivery includes the
remaining validated slices rather than leaving them only in the working tree.

## Validation and delivery record

Focused regressions cover each changed behavior, followed by the repository's
`mise run verify-full` gate. Load measurements retain failed-run evidence and
report residual uncertainty; retrying until a test passes is not qualification.
Source implementation, committed delivery, installed/runtime identity, and live
acceptance are recorded separately as the work completes.

## Findings and completed validation

The restart/budget-stop race (#7) was already fixed in `f70ac31`: completion
state and its `harness_result` event are separate writes. The existing regression
passed 20 runs at concurrency two. This batch reconciles that stale ticket rather
than duplicating the fix.

The review-ceiling failure (#8) was reproducible through real managed checks.
Recovery could replay the old workflow's spawn step after the attempt had settled,
and the supervisor could later respawn its original record. A durable dispatch
fence now precedes dismissal; fresh spawn, respawn and recovery continuation read
it under the same launch lock. The final settlement marker retains its original
meaning and crash barriers. A fresh explicit re-enqueue uses a different attempt.
The regression rewinds an on-disk workflow to its spawn step after a real daemon
SIGKILL: it failed before the repair and passes afterward. A separate injected
setup panic proved the fixture leaked its daemon; setup and CLI child guards now
cover that path too. Temporary trace instrumentation was removed.

For #9, `scripts/verification-load.py` creates isolated daemon homes and repositories,
executes distinct named checks (so proof-cache hits cannot count as load samples),
and retains each result, on-disk database, gate-failure artifact and admission event.
The workload was the real cross-process review-ceiling crash test, with four
simultaneous requests in each of three rounds per configuration:

| Source | Admission limit | Failed / executed | Peak check processes |
|---|---:|---:|---:|
| Before dispatch fence | 0 | 4 / 12 | 4 |
| Before dispatch fence | 1 | 0 / 12 | 1 |
| After dispatch fence | 0 | 0 / 12 | 4 |
| After dispatch fence | 1 | 0 / 12 | 1 |

Every configuration also ran a deliberate exit-7 diagnostic control. All failures,
including the four real test failures, persisted with exact exits and diagnostics.
The initial probe accidentally reused cached proofs and was rejected; the harness
now asserts that every requested execution wrote an occupancy record. These are
bounded workload measurements, not a claim that every workspace flake is eliminated.
The uncapped repaired workload passed too: admission reduces resource pressure,
while the dispatch fence corrects the lifecycle defect.

Admission regressions cover eight queued checks at WIP four, exact failure
attribution read through a new database handle, cross-repository independence,
and restart during a queued landing without duplicated ownership or budget.
The five saturation integration tests passed. D1's 41 observer unit tests and its
actual silent-worker CLI/restart test passed; queue projection and identity tests
cover #10's additional daemon/observer seam.

The complete source gate is required again on the final combined candidate.
Release identities, the final gate result, binary hashes, rollback copies and live
policy/state parity are recorded in the durable `collaboration-reliability-release`
artifact after delivery. The held post-slimming pilot remains a separate acceptance
step and is not started by this batch.

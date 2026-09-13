# Native verification span contract

Date: 2026-09-13. Delivery slice: TKT-zugut-zodut-bofuh.

A native `task_span` records an observation, not approval authority. Existing
repo/task scoping and missing-value semantics remain in force.

## Duration

`PhaseSpan::from_durations` marks new records with
`duration_semantic: "additive"`: `queue_wait_ms` is admission wait and
`duration_ms` is execution after admission. The landing producer measures
execution from `RunProgress::execution_started_at`, the same boundary used by
managed verification. Their sum represents disjoint durations for this span.

Legacy records without this tag can include wait inside duration. Consumers
must preserve them and mark ambiguous totals unknown instead of assuming the
fields are additive. The BBS evaluator correction is a separate required
reporter delivery; this producer slice does not complete that evaluator.

Wall-clock fields reconstructed from durations are not independent observations
of host sleep or active model work. The existing host-sleep follow-up remains
separate; no historical observation is rewritten by this change.

## Occurrence identity

Within a repo/task, durable insertion and the critical-path reader use the same
`phase`, `attempt`, `target`, `candidate`, `lane`, `occurrence_key` identity.
Optional fields retain absent values for older producers. Landing check spans
carry the existing verification-proof digest as their occurrence key; this
includes candidate, check name/command and configured proof context. Exact
replay remains idempotent, while different candidates, targets or check
contexts at the same plan ordinal remain distinct. Missing historical spans
cannot be reconstructed as zero time.

## Acceptance

The slice retains executing admission-wait and gate replay regressions, writer
and reader identity cases, cross-repo isolation and legacy coverage. The merged
landing code also retains Sable's independently delivered rule that target-
dependent policy checks must run again; ordinary verification proof reuse is
preserved. Native protected-main full verification and semantic review are
required before delivery. This document describes the contract, not a test result.

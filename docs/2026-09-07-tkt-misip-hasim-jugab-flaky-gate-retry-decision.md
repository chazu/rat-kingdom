# TKT-misip-hasim-jugab: bounded automatic retry for unrelated ordinary-fail gate verdicts

## Decision needed

`crates/rk-daemon/src/landing.rs`'s `LandingPipeline::run_gates_at` grants
exactly one automatic retry to an **infra** verdict (child died to a
signal/runner-loss, tracked via `gate_infra_retry_used` /
`gate_infra_retry_check`) before holding a branch. An ordinary **fail**
verdict — a check that ran to completion and exited non-zero — gets no such
grace, even when it is plainly unrelated to the candidate's diff. That gap
held a doc-only, `verify-changed`-green branch
(`rat/cornflower-13/research-maki-agent-harness-chatgpt`) on a
now-fixed-but-then-still-flaky implementation-lane test until an operator
forced an audited bypass (`docs/2026-09-07-tkt-implementation-lane-fifo-test-flake.md`).
This doc records the options and a recommendation; per that doc and
`docs/2026-08-19-tkt-hot-scan-target-dir-contention.md`, actually changing
gate semantics needs explicit operator sign-off before implementation, so no
code changes ship with it.

## Options (as filed)

1. **Per-signature retry list**, mirroring
   `managed_verification.rs::is_cargo_target_contention_signature`: a small,
   explicit set of known-flaky test-name+message signatures get one free
   retry on an ordinary fail verdict, with the same durable-evidence
   discipline the infra retry already uses (a `gate_ordinary_retry_used` /
   `gate_ordinary_retry_check` pair, or reuse of the existing infra fields
   keyed by verdict kind).
2. **Diff-scope-gated generic retry**: any ordinary fail verdict whose
   failing check's working directory is provably excluded by the
   candidate's diff scope (`landing-diff-scope`'s own file-set check) gets
   one bounded retry, no signature curation required.
3. **Status quo**: ordinary fail verdicts stay immediately blocking; rely on
   `preexisting-failure-is-a-ticket-not-an-inline-fix` (TKT-43) plus a human
   bypass.

## Analysis

The infra-retry precedent this would extend is deliberately narrow: it
never asks "is this failure related to the diff," only "did the check even
finish." Both option 1 and option 2 have to answer that harder question,
and they answer it very differently:

- **Option 1** answers it by curation: a signature only enters the list
  after a human has already diagnosed it as a fixable or environmental
  flake (exactly the workflow that produced `is_cargo_target_contention_signature`
  and would have produced an analogous entry for the implementation-lane
  fixture race, had it not turned out to be a one-line test fix landed the
  same day). It changes gate semantics only for the exact failures someone
  has already vetted, so genuine-regression detection is untouched for
  everything else. Its cost is exactly the cost the existing cargo-contention
  entry already pays: someone has to notice a new flake, diagnose it, and
  land a signature match for it. That is real toil, but it is bounded,
  auditable (each entry traces to a diagnosis doc, same as this one and the
  hot-scan one), and it never retries a failure whose relationship to the
  diff was never established.

- **Option 2** answers it structurally: "the diff didn't touch it, so it
  can't be this diff's fault" — but that inference is weaker than it looks.
  A failing test's working directory can be untouched while its behavior
  still depends on something the diff *did* touch (a shared fixture, a
  crate-wide invariant, load induced by the diff's own build). Diff-scope
  exclusion is a good heuristic for "probably unrelated," not a proof of
  it, and turning a heuristic into an automatic retry-and-proceed changes
  what "the gate said no" means for every repo using the named
  `verify`/`verify-full` checks — not just the specific flakes someone has
  already diagnosed. It also couples two checks (`landing-diff-scope` and
  the verify gate) that are currently independent, which is exactly the
  kind of cross-cutting semantics change the hot-scan doc deferred rather
  than land unilaterally.

- **Option 3** has zero implementation risk but a known, recurring cost:
  this is now the second documented incident
  (TKT-01M0BVWJ4JJE8EQATANMWZNTC2 hot-scan, TKT-kujaj-libar-mopud
  implementation-lane) where a genuinely unrelated failure cost a full
  operator interrupt to unstick a clean branch. The fleet already has a
  ticket-not-inline-fix convention for *reporting* the failure; it has no
  convention yet for *unblocking* a branch a diagnosed-safe flake is
  holding.

## Recommendation

**Option 1**, for the same reason the hot-scan doc picked the narrowest
available mitigation over the broader structural fix: it has the lowest
blast radius, it extends a pattern that is already landed and reviewed
(`is_cargo_target_contention_signature`), and it never retries a failure
that hasn't been individually vetted as safe to retry. Concretely, if
approved:

- Add a `gate_ordinary_retry_used` / `gate_ordinary_retry_check` pair to
  `LandingQueueEntry` (or generalize the existing infra fields to carry a
  verdict-kind tag) with identical durable-evidence and crash-resume
  discipline to `gate_infra_retry_used`/`gate_infra_retry_check` —
  `run_gates_at`'s infra-retry branch is the direct template.
  `GateRunOutcome` gains an `OrdinaryRetryExhausted` case mirroring
  `InfraRetryExhausted`, and the landing-hold message names it explicitly
  so an operator reading a hold artifact can tell "retried and still
  failed" from "never eligible to retry."
- Maintain the signature list next to
  `is_cargo_target_contention_signature` (or as a sibling list consumed at
  the same call site), with each entry required to cite a diagnosis doc —
  the same discipline this doc and the hot-scan doc already follow.
- Explicitly do NOT fold this into `is_cargo_target_contention_signature`
  itself: that function is scoped to one exact ENOENT shape from the shared
  `CARGO_TARGET_DIR` race, an infra-adjacent signature already handled
  before a verdict is even classified as "fail." An ordinary-fail signature
  list is a different mechanism gated on the verdict already being
  ordinal-1 "fail," not a variant of the existing one.

Option 2 is worth revisiting later if the signature list grows large enough
that curation toil dominates, but that is not the situation today (the
precedent list has exactly one entry).

## Disposition

Filed as a decision for operator sign-off, not implemented here — see the
linked ticket. No production code changes accompany this doc.

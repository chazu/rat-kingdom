# Adversarial review: three high-leverage enhancements

*2026-09-01. Review target: the three same-date design drafts. Complete: every
blocker and high finding below has an inline disposition, the dispositions are
implemented, and both repository verification profiles pass.*

## Lenses

1. **State-machine soundness:** crash points, concurrency, replay, and partial
   success against the actual landing queue.
2. **Operator truth and compatibility:** emitted inbox shapes, CLI rendering,
   lifecycle language, and JSON consumers.
3. **Authority:** fail-open paths, conditional grants, authentication seams,
   and role/harness mismatches.
4. **Implementation pressure:** whether the proposed seam is the smallest deep
   module that can carry the invariant without creating another ledger.

## Findings and dispositions

### Delivery finalization

**D1 — Blocker: equality-only recovery loses a landed candidate after later
target movement.** `recover_completed_land` requires `target == candidate`.
After a restart or external target advance, the candidate can be a proper
ancestor and still be the exact delivered merge. Rebuilding it as fresh work
is wrong. **Disposition:** recovery accepts exact equality or ancestry of the
persisted prepared candidate in the target. A persisted exact candidate commit
is stronger evidence than branch refs, which may already be deleted.

**D2 — Blocker: batch processing does not call the single-entry recovery
path.** A batch that advanced Git and then failed finalization would be gated
and advanced again on retry, likely becoming stale instead of settling its
members. **Disposition:** batch processing detects a `Landing` batch whose
shared candidate is contained in the target and enters per-member finalization
directly, without gates or a second target advance.

**D3 — High: whole-batch error propagation retains already-settled members.**
This is safe because both ticket and generation writes are idempotent, but it
causes repeated finalization and can obscure which member is blocking removal.
**Disposition:** stabilize the first change with whole-batch retention only if
the implementation records the failing task and a test proves eventual replay.
Per-member removal is preferred when it does not force a cross-module return
shape.

**D4 — High: a retained receipt without operator evidence can become an opaque
retry loop.** **Disposition:** emit a durable, deduplicated
`landing_finalization_failed` event carrying task, branch, target, candidate,
and error. The landing queue remains the authority; the event is visibility,
not a second state machine.

### Canonical operator work

**W1 — Blocker: an allowlist classifier can repeat the original omission for
the next inbox kind.** **Disposition:** classification is exhaustive over the
broad inbox result: known singular resolutions become actionable, known
choices become decision-required, and every remaining row defaults to stalled.
Nothing is silently dropped.

**W2 — High: compatibility aliases can drift if separately constructed.**
**Disposition:** build `actionable` once, clone it into `attention`, and derive
both counts from the same length. Tests assert structural equality.

**W3 — High: “no current work” could include control-loop work not intended for
the human.** Reconciliation rows under orchestrator authority belong to the
King rather than human current work. **Disposition:** keep that documented
operator-invisible class. Human/mechanical contradictions remain projected;
orchestrator-authority contradictions remain in `rk attention next` and King
wakes.

**W4 — High: lifecycle drift is broader than the ticket paragraph.** Operator
daily steps, dispatch help, the dismiss command, and the foreman fragment all
still teach merge-on-dismiss. **Disposition:** update every current prime
fragment. Do not rewrite historical design/research documents; they remain
dated evidence.

**W5 — Medium: stalled rows do not share one display shape.** **Disposition:**
the CLI renderer uses kind/scope plus detail fallbacks (`detail`, `text`,
`action`) and prints a command only when present.

### Capability registry

**C1 — Blocker: placing the full registry in the harness crate or daemon can
create dependency inversion or duplicate tool policy.** **Disposition:** the
daemon owns callable-operation and role/harness policy. Harness adapters remain
thin executors of the resolved permission mode and core-owned narrow tool set.
No harness-to-daemon dependency is introduced.

**C2 — Blocker: changing only terminal-completion acceptance leaves prompt and
resume paths inconsistent.** Diagnostician rendering ignores the existing
terminal-completion flag, and resume/attach errors say “onboarding” for every
terminal-completion pairing. **Disposition:** one role/harness profile selects
permission and completion channel; rendering and resume text branch on that
profile, not on onboarder-specific prose.

**C3 — High: “every dispatched method” is unnecessarily coupled to operator
dispatch.** Operators already pass a stronger observed-origin gate and may call
new methods. The security property is explicit non-operator grants.
**Disposition:** enumerate every intended non-operator callable method in the
registry; unknown/unclassified methods deny non-operators. Operator dispatch
remains independently extensible.

**C4 — High: parameter-sensitive authority cannot be flattened into a table.**
Self `task_done`, foreman child control, and evidence-bearing groomer closure
need live request checks. **Disposition:** registry entries select named
conditional policies; existing predicates remain the final gate.

**C5 — High: Jcode groomer rejection must happen before durable spawn side
effects.** **Disposition:** validate the resolved role/harness profile before
name reservation, registry insertion, worktree creation, or harness launch.

**C6 — Medium, rejected after executable review: a valid derived token without
a current agent record falls into ordinary-agent policy.** The first draft
proposed denying it. Existing end-to-end contracts prove that explicit
authenticated agent callers are intentionally usable without a live row; the
same-user root credential is the trust boundary, while supervised child
processes remain kernel-bound to their caller. **Disposition:** preserve that
contract, but apply the same default-deny method registry to recordless callers.

## Stabilized implementation order

1. Delivery error propagation and ancestry/batch recovery.
2. Exhaustive operator projection and lifecycle text correction.
3. Capability registry, then role/harness completion fixes.
4. Focused tests after each slice; full repository verification after all
   three compose.

## Acceptance

The review is settled when the three design docs say “stabilized after
adversarial review,” contain the dispositions above, and implementation tests
exercise every blocker. Passing existing tests without those new cases is not
acceptance.

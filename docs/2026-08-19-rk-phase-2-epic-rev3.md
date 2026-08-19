# Phase 2 epic, rev 3 — post-drain-probe

*2026-08-19. Supersedes docs/2026-08-18-rk-phase-2-orchestration-program.md
(rev 2). Rev 3 is written against 36 hours of live drain-probe evidence
(docs/2026-08-18-drain-probe-log.md, 17 observations) rather than
speculation. Roughly a third of rev 2 evaporated — the probe's own fleet
delivered it. mu/pudl integration is POSTPONED indefinitely (operator,
2026-08-19); the batching answer to gate economics removed its
justification.*

## Binding cross-cutting requirements

1. **Stack neutrality is a hard requirement, not an aspiration.** rk
   orchestrates repos of any language. No ticket in this epic may put a
   language-, toolchain-, or build-system-specific assumption into daemon
   code. Concretely: checks are *named checks resolved from repo policy*
   (never a hardcoded command); reclaimable build-artifact paths are a
   per-repo glob list (`target/` is rat-kingdom's *data*, not rk's
   knowledge); "did this deliver" is answered from git and rk records,
   never from a language's tooling. rat-kingdom is the first tenant, not
   the template. **Every ticket below carries a stack-neutrality
   acceptance criterion, and a reviewer must reject a diff that hardcodes
   one stack's conventions.**
2. **Announce + rate-cap + jitter** on every automated action.
3. **No new background loop without naming the loop it absorbs.**
4. **No operator bypass of the gate** (see P2 — the operator caused the
   probe's single largest false-red incident by hand-merging past it).

---

## P1. Agent lifecycle state + delivery correctness (the big one)

The probe's dominant intervention class by a wide margin. One root
cluster, one program — reviewers included, per operator direction.

**Observed failures this fixes** (probe O6, O7, O8, O11, O14, O16, O17,
plus the reviewer-death family):

- A rat that *pauses* mid-task (waiting on a slow background check) ends
  its turn and is recorded `completed`. Drain frees the slot, the reopen
  sweep reclaims its ticket, a duplicate rat is dispatched. Same shape
  kills reviews outright: the review workflow sees `is_error: true` for a
  reviewer that was doing exactly the right thing.
- The reopen sweep and the legacy dismiss-time ticket-closer are **landing
  blind**: they read agent liveness, not landing-queue membership, so
  every ticket whose branch queues longer than the window recycles.
- An **empty branch reads as "merged"** — surfacing four separate times
  (empty landings sail through unreviewed; auto-respawn suppressed for a
  crashed rat; done-gate refusals; duplicate no-op merges closing real
  tickets).
- **No post-landing writer** closes a ticket, so delivered work sits
  `in_progress` indefinitely (14 tickets at once, observed).
- Operator `interrupt` leaves an agent in a respawn-eligible state, so
  the machinery resurrects deliberately-stopped agents.

**Work:**

- A distinct **paused/awaiting-resume** agent state, separate from
  completed and from failed; drain WIP accounting, the reopen sweep, and
  review-workflow evaluation all treat it as *live*.
- **Durable delivery record**: the landing pipeline writes the merge
  commit onto the ticket at land time and closes it there. `merged` is
  answered from that record, not from live branch refs (branches are
  deleted on land) — and never from branch *existence*.
- **Commit-count awareness** in every delivery predicate: an empty
  branch is not a delivery, for landing, respawn suppression, or done-gating.
- **Landing-aware sweep and lifecycle split**: reopen consults the landing
  queue, while dismissal never closes tickets or delivers branches; a ticket
  with queued work is never reopened or auto-closed by a duplicate.
- **Reviewer liveness**: a reviewer that pauses is not a failure; a
  reviewer that exits without a verdict escalates as a distinct,
  retryable outcome rather than a dead-end hold.
- **Deliberate-stop terminal state** for `interrupt`, excluded from
  respawn.
- Dependency readiness (`rk ticket ready`, drain claim) reads the durable
  delivery record — ONE predicate, replacing `is_done` in every
  dependency consumer.

*Stack neutrality: all of the above is git + rk state only. No build
tooling touched.*

## P2. Merge queue: test the merge, land the tested tree

Unchanged in design from rev 2, strengthened in evidence: the probe
produced ~15 false-red gate holds (stale-base lints, fmt windows,
rollover-killed gates) and one operator-caused lint window that falsely
held thirteen branches.

- New CAS primitive advancing a target to a *pre-tested* commit; hard
  invariant that landed SHA == tested SHA.
- All merge-mode landings (automatic, workflow, and operator submissions) go
  through the one queue. Operators get a fast lane, never a
  bypass.
- Batch several queued branches into one gate run; bisect on failure.
  This is the throughput answer (mu is postponed).
- Retest exhaustion requeues to tail with an announcement; never a
  terminal hold on a green branch.
- In-flight gate runs survive daemon rollover (or are re-enqueued on
  startup) instead of being recorded as failures.

Implemented 2026-08-19: merge candidates are parked durably with their tested
SHA/base/ref; compatible bursts batch up to eight branches and recursively
bisect on failure; stale CAS results are announced and requeued at the tail;
startup sweeps orphaned candidate refs. `dismiss` is now lifecycle-only and
preserves its branch. Workflow `dismiss`/`dismiss_all` retain their explicit
delivery intent by composing cleanup with a separate queue submission. The
operator command is `rk land`; its `--force --reason` escape hatch is audited
and inbox-visible. Missing named checks produce a durable `no-gate` hold.

*Stack neutrality: the gate runs the repo's **named** canonical check
list from its policy. rat-kingdom's list (build/test/clippy/fmt) is
config. A repo with no named checks must degrade to an explicit,
visible "no gate configured" state — never a hardcoded default command.*

## P3. Machine-aware admission

Slimmed from rev 2 (contention improved materially during the probe).

- The scarce-resource signal must include **disk headroom**, not just
  CPU: the probe's worst outage was 231 GB of build artifacts tripping
  the spawn floor, which *silently* stopped dispatch.
- A disk-floor or resource refusal must escalate through the
  notification sinks. Silent refusal was the probe's only true
  silent-stall class.
- Admission reads queue-wait by class; two priority classes (gate vs
  everything else).
- Terminal agents' reclaimable build artifacts are reaped by the daemon
  sweep, replacing the interim launchd script.

*Stack neutrality: reclaimable-artifact paths come from a per-repo glob
list in policy (default empty ⇒ reap nothing). The daemon must not know
what `target/` is.*

## P4. Reviewer economics (after P1's liveness fixes)

Reviewers get a budget cap (~$30, above the observed cost of a
legitimate deep review) and tier routing, but **only after** P1 makes
reviewer liveness sound — and sonnet reviewers ship in shadow mode
(both models, verdicts compared) before becoming default. The review
layer caught 8 real defects with 0 spurious rejections during the
probe; it is the last component to degrade casually.

## P5. Unattended drain week (replaces rev 2's supervised experiment)

The supervised experiment already ran — this document is its output.
The successor test is **unattended**: drain on, operator away, with an
external liveness audit (an independent sweep asserting from durable
state that every non-terminal ticket changes state or heartbeats within
T). Pre-registered thresholds set from probe data. The classified
intervention count is the rk-king decision input.

Probe prior: of ~30 interventions in 36 hours, nearly all were landing
or state recoveries that P1+P2 eliminate by construction; roughly five
needed genuine judgment (program-tree reconciliation, security
sign-off, priority calls). If that ratio holds, notifications + inbox
suffice and rk-king stays unbuilt.

## Sequencing

P1 and P2 are the program; they are independent of each other and can
run in parallel. P3 rides alongside. P4 waits on P1. P5 waits on
P1+P2+P3. mu/pudl: postponed, no milestone.

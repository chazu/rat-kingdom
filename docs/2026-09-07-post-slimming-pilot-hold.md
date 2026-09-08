# Post-slimming foreign-tenant pilot: hold, do not dispatch

*Status: dispatch assessment, 2026-09-07, for `TKT-01M0FQ94FSY0VB4ZP60DK4Q8PJ`
("Run the 48-72-hour post-slimming foreign-tenant pilot", M3 in
[ROADMAP.md](ROADMAP.md)). This is a finding, not a design change.*

## Outcome

No new 48-72-hour pilot was dispatched. Entry criteria are not met and the
most recent fleet assessment already names this exact ticket as needing
reconciliation before any new run.

## Why

1. **The referenced M3 entry criteria are not all satisfied.** The control-plane
   slimming ticket (`TKT-01M0FQ8BV7S558ZWZ99DBWA45E`) is closed, but "numeric
   thresholds are pre-registered" and "rollback is rehearsed" have no recorded
   evidence for a *new* run, and "the foreign tracer scenarios remain green" is
   unverified post-slimming.
2. **[docs/2026-09-06-r1-qualification-deliverables.md](2026-09-06-r1-qualification-deliverables.md)**
   (dated the day before this assessment, still "proposed for later review")
   explicitly lists this ticket in its tracker table: *"Open post-slimming
   pilot; D4's operational anchor. Its `pilot-running` label needs
   reconciliation with the stopped, failed run."* That document's own sequencing
   states D4 (a fresh pilot) should begin "only after D1-D3 have their
   evidence." None of D1 (external acceptance auditor completeness), D2
   (executable pilot acceptance contract), or D3 (dispatch-risk closure) have
   landed — confirmed against `git log` (nothing under
   `crates/rk-cli/src/observation_cmds.rs` or `observation_store.rs` since that
   doc's commit `056ebe6`) and against the open ticket backlog (no ticket
   titled for D1 or D2; D3's identity-audit slice, `TKT-gusab-hihus-tijof`, is
   still `in_progress`).
3. **The most recent attempt at this same goal already failed.** The
   Glossolalia repeat report (`01M1HS3MBHVNN87XKVKC71JZ5D`, evidence under
   `~/.rat-kingdom-observations/glossolalia-pilot-repeat-2026-09-02/`) covers
   53.5 hours and 5,690 samples and recorded `passed: false`: 5,254 build
   mismatch samples, a 1,956-second sampling gap, one duplicate dispatch,
   maximum landing age 1,561s against a 600s threshold, two reconciliation
   violations, and one stale ticket. None of the causes behind those violations
   are established as fixed (D3's own acceptance criteria still require an
   evidence-backed disposition for the duplicate-dispatch finding).
4. **The observer itself has a known qualification gap.** D1's problem
   statement records an executable counterexample: a fixture with a stale
   `running` agent produced zero stale tickets and a passing derived report
   even with an ad-hoc intervention recorded. Dispatching a new pilot before
   that gap closes risks producing a *report that reads healthy while missing
   the exact failure class M3 exists to catch* — worse than no report at all,
   since a passing-looking result could be read as license to proceed to M4.

Given the roadmap's own language — "a hard failure sends the program to
repair and repeat or stop... later convergence cannot erase it" — spending
another 48-72-hour irreducible elapsed window on a run that inherits the same
unfixed causes as the last failed attempt, audited by a collector with a known
detection gap, is not a responsible use of that irreducible time. This is also
a large, hard-to-reverse operational commitment (continuous live dispatch
against a trusted foreign repository for multiple days); starting it without
resolving the above is not a call a single dispatch should make unilaterally.

## Recommendation

Repair and repeat, not stop: the M3 milestone and this ticket's intent remain
valid, but the correct next actions are D1-D3 from the qualification-deliverables
doc, plus reconciling this ticket's `pilot-running` label against the actual
stopped/failed run — not a new live dispatch. See the filed follow-up ticket
and tuplespace artifact for the durable handoff.

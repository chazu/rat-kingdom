# Progressive delivery priorities

The operator's September 15 direction is to remove redundant validation, make
the important policy decisions configurable per repository, and favor staged
promotion and deployment while independent integration continues. This refines
the order of the [continuous delivery plan](2026-09-13-continuous-validation-promotion.md);
it does not discard its recovery, provenance, or scoped failure requirements.

## Execution order

1. **Separate useful integration from final release acceptance.** Finish and
   adopt TKT-dijid-noruj-pirab's check routing, then TKT-hunim-zovuz-rurim's cache
   isolation. Require coverage of changed inputs on integration, freeze release
   candidates, and let later independent integration continue. Keep required
   checks on their relevant stage instead of repeating final acceptance on every
   worker and inner edge. The existing owners retain their current work.
2. **Remove redundant execution.** Keep the delivered worker verification
   handoff enabled where automatic landing exists. Complete exact in-flight
   check sharing (P0) and review/check overlap (P1), retaining each caller's
   cancellation and candidate binding. Previously exhausted attempts retain
   their history and recovery limits. Do not claim different check recipes or
   different candidates share proof merely because they have similar names.
3. **Make stage policy explicit per repository.** TKT-buhuk-gunud-vuzit adds
   `landing.finalCheck`, defaulting to `verify`, through the existing activated
   policy and named-check registry. This removes a hard-coded name; it does not
   itself make a check faster. Document existing controls and remaining gaps
   below, and add further settings only with a working operational consumer.
4. **Close the production feedback loop.** Finish the bounded automatic feature
   disable action (TKT-piluf-jutop-sibur), then compatible release activation and
   rollback. Ship independently useful features behind per-repo configuration;
   promote exposure when their predeclared objectives support it. Hold the
   affected feature or candidate when evidence fails or is unavailable while
   unrelated work continues.

## Policy inventory

| Decision | Existing per-repository control | Remaining work |
| --- | --- | --- |
| Worker versus landing acceptance | `landing.verificationHandoff` | Measure actual redundant runs and retain unsupported-route fallback. |
| Integration check selection | `landing.focusedChecks` | Complete configured integration routing and conservative coverage enforcement. |
| Final release acceptance | `landing.finalCheck`, default `verify` | Deploy this slice before activating a custom check. |
| Stage destinations | `delivery.target`, `landing.protectedTargets`, `release.integrationBranch`, `release.releaseTarget` | Complete and adopt the integration-during-release journey. |
| Executable check contract | `.rk/checks.cue`: command, cwd, timeout, expected exit, environment, toolchain, shared target | Keep CI and local recipes aligned; expose gaps rather than silently substitute. |
| Review and recovery bounds | `landing.reviewTimeout`, `reviewMaxWait`, rework/conflict/reviewer-death switches, attempt and cost ceilings | Complete overlapping execution without widening authority. |
| Host competition | Host-wide ceiling and check classes; per-repo admission-limit override | Per-repo weight/class profiles and cache isolation remain incomplete. A repo cannot exceed the host ceiling. |
| Feature exposure and objective | Per-repo BBS discovery/retirement switches and assessment configuration | Stable cohorts, shadow execution, and broader feature coverage remain. |
| Promotion, health, rollback | Existing operator installation plus immutable prepared bundles | A reusable per-repo action policy and durable native activation/rollback remain incomplete. |

## Acceptance and production feedback

Use focused executable fixtures for each changed contract and one authoritative
acceptance run per eligible exact candidate/check identity. Report reuse, wait,
execution, cancellation, and invalidation separately. Keep required checks;
move them to the appropriate stage and reuse only valid evidence.

Measure ordinary root-change dispatch-to-deployment and accepted-to-deployed
time; checks executed/reused per stage; check wait/execution time; concurrent
integration progress; model cost; operator interventions; and rework/rollback.
For each enabled feature, record the config revision, objective, observation
window, denominator, failures, and disable events. A successful operational
probe is not proof of improved throughput or collaboration.

Useful contracts and verified findings belong on the BBS so other work can use
them without waiting for whole tracks. Comparative BBS ranking experiments and
broad compatibility work should not hold an independent throughput improvement
unless they supply a specific capability required for correctness. Integration,
installation, enablement, and demonstrated benefit remain separate outcomes.

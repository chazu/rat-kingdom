# Rat Kingdom roadmap

*Status snapshot: 2026-08-20. The native Rat Kingdom ticket graph is the
execution source of truth; this document defines release scope, milestone
ordering, and exit criteria.*

## Objective

Ship Rat Kingdom as a hands-off swarm orchestrator for a trusted,
direct-merge repository other than Rat Kingdom itself. "Hands-off" includes a
primed LLM orchestrator acting within durable, budgeted authority. Product,
security, credential, destructive-impact, and genuinely ambiguous decisions
remain explicit human gates.

This roadmap consolidates the sequencing in:

- `docs/2026-08-16-rk-strategic-review.md`;
- `docs/2026-08-18-rk-phase-2-orchestration-program.md`;
- `docs/2026-08-19-rk-phase-2-epic-rev3.md`; and
- `docs/2026-08-19-multi-project-hands-off-readiness.md`.

Those documents remain the rationale and evidence record. This file
supersedes their sequencing summaries when they disagree.

## Release boundaries

### R1: trusted direct-merge tenant

R1 is the active release. Workers and the orchestrating LLM run on a trusted
personal machine against repositories the operator trusts. Repository-owned
policy defines named checks, protected paths, delivery mode, reclaimable
artifacts, WIP, budget, notifications, and human gates.

### R2: protected-branch delivery

Creating pull requests, treating external CI and forge merge state as delivery
predicates, and reconciling protected-branch delivery are a separate release.
They must not silently expand R1.

### R3: arbitrary or untrusted tenants

Per-repository process, secret, configuration, and network isolation is a
larger product boundary. R1 does not claim it.

## Milestone sequence

```text
M0 convergence complete
  -> M1 foreign tracer
  -> M2 control-plane freeze and slimming
  -> M3 48-72-hour supervised pilot
  -> M4 seven-day unattended acceptance
  -> R1 release decision
```

Durations are planning ranges, not deadlines. A failed exit criterion sends
the programme back to the smallest milestone that owns the failure.

| Milestone | Working duration | Current state |
| --- | ---: | --- |
| M0. Convergence complete | 2-4 focused engineering days | In progress |
| M1. Foreign tracer | 1-2 focused days | Blocked by M0 and tenant approval |
| M2. Feature freeze and slimming | 3-5 focused days | Blocked by M1 |
| M3. Supervised foreign pilot | 48-72 elapsed hours | Blocked by M2 |
| M4. Unattended acceptance | 7 elapsed days | Blocked by M3 |

Assuming no milestone-resetting discovery, R1 is approximately 2.5-3 calendar
weeks from the 2026-08-20 snapshot. The pilot and acceptance periods are
irreducible elapsed time.

## M0: convergence complete

### Purpose

Finish the control-plane invariants required to begin a safe foreign tracer.
Every implementation, review, recovery, landing, ticket, and process state
must converge without an ad-hoc human rescue when the correct action is
mechanical or delegated to the orchestrating LLM.

### Exit criteria

- The bounded reviewer-REWORK chain lands through every preserved integration
  branch and reaches `main`.
- Single and batch landing persist equivalent crash-recovery state before any
  delivery side effect.
- Identical pending landing submissions deduplicate atomically.
- Reviewer infrastructure death is retried within a bounded policy.
- Merge conflicts become a bounded LLM rework task or an evidence-bearing
  human gate; no inbox row recommends an unsafe manual merge.
- Non-main landing does not leave a checked-out target worktree apparently
  dirty against its advanced branch.
- Reclaimable artifacts are repository-owned and default empty; terminal
  cleanup cannot delete stack-specific paths by convention.
- Harness subprocesses and shared verification execution are bounded.
- Reviewer caps and tier routing prevent review retries from becoming
  unbounded spend.
- `mise run verify` passes, the live daemon runs the landed build, and daemon
  rollover plus sync complete without replay or size-limit failures.

### Ticket rollup

Already delivered foundations:

- `TKT-01M0E8PMWCRKE6WZ4ZNYB6Y6YS` - cross-ledger convergence report.
- `TKT-01M0E8PN2SJEH9GNCYKYYDQEHM` - idempotent contradiction repair.
- `TKT-01M0E8PN9C41BWECGNW0990R3J` - durable orchestrator authority ladder.
- `TKT-01M0CTHNHRSVRV9NWFYSBK98J6` - landing queue depth and age.
- `TKT-01M0F8AF5MG4B3RX6N573RZP64` - size-safe git-notes sync.
- `TKT-01M0F9CJG1V94DQZVPSBA10Y4G` - exact reviewer verdict binding.

Active anchors and remaining blockers:

- `TKT-01M0EEAGS6RFJT8PS44KYVYXQJ` - bounded reviewer REWORK automation.
- `TKT-01M0F3C449TNKRA371HB2BJNVC` - preserved REWORK implementation chain.
- `TKT-01M0F3V9VSASX9B44A6Y314SQ3` - finish and verify the chain.
- `TKT-01M0E8PNFQZ70F3ZFG3KCS39ZG` - merge-conflict playbook.
- `TKT-01M0EHB5BRJSAC5ZR9W45R1BQ4` - reviewer infrastructure retry.
- `TKT-01M0EWRJFWXA41FKWX1H0DZQ6E` - atomic landing deduplication.
- `TKT-01M0EHFDGZQDZM0CF4E04G6JKA` - non-main target worktree state.
- `TKT-01M0CTC4VETPP26KZ74QRZ9ZDD` - repository-owned artifact cleanup.
- `TKT-01M0CTC4VXVB33YP4YQZ8S5WJ3` - harness subprocess containment.
- `TKT-01M0CTC4WCVPPAWRF4CSKG6S04` - reviewer budget and tier routing.

Incident and flake tickets discovered while proving these anchors remain real
work, but they do not become roadmap milestones unless they invalidate an exit
criterion above.

## M1: foreign tracer

### Purpose

Prove the complete control-plane vocabulary on a materially different
repository before simplifying around RK-only traffic.

### Entry criteria

- M0 is complete.
- A human selects the tenant and approves delivery mode, checks, protected
  paths, artifact policy, WIP, budget, notifications, credentials, and human
  gates.

### Exit criteria

- An activated, content-bound repository policy has no drift.
- Five to ten curated tickets run at WIP 1 under a pre-registered budget.
- The run injects worker death, daemon rollover, gate failure, and merge
  conflict.
- Every branch is durably delivered or in a classified actionable hold.
- Every intervention is classified as daemon-mechanical, LLM-orchestrated,
  correct human gate, or ad-hoc rescue.
- There are no silent stalls, gate bypasses, duplicate dispatches, lost source,
  or unjournaled mutations.
- The outcome says proceed, repair and repeat, or stop. Proceed requires that
  no missing lifecycle state or authority class was discovered.

### Ticket rollup

- `TKT-01M0E8PNP8BJRE65Q8SBQDSGHZ` - activate the first foreign tenant.
- `TKT-01M0E8PNWNG1GPS5EJA7AGVTRC` - fault-injected foreign tracer batch.

## M2: control-plane freeze and semantic slimming

### Purpose

Freeze new lifecycle features after the foreign tracer stabilizes the domain
vocabulary. Reduce semantic surface before the longer pilot rather than
carrying self-hosting duplication into the acceptance run.

This is not a line-count contest. It reduces independent owners, paths, loops,
and representations while preserving externally observable behavior.

### Exit criteria

- Single, batch, automatic, and operator landing share transition primitives
  and cannot drift on crash markers, deduplication, gates, or delivery.
- Every lifecycle transition has one named owner.
- Every durable delivery or recovery fact has one source of truth.
- A documented loop-absorption map removes or delegates overlapping recovery
  and cleanup loops; a new reconciler responsibility must retire an old owner.
- Compatibility paths are removed only after persisted-state migration and
  rollback are proved.
- Black-box tracer scenarios, repository checks, and daemon rollover tests all
  pass unchanged.
- No M0/M1 readiness criterion is weakened to obtain the simplification.

### Ticket rollup

- `TKT-01M0FQ8BV7S558ZWZ99DBWA45E` - control-plane feature freeze and
  semantic slimming pass.

## M3: 48-72-hour supervised foreign pilot

### Purpose

Validate the simplified control plane continuously before spending the
irreducible unattended week.

### Exit criteria

- Numeric throughput, queue-age, spend, liveness, and intervention thresholds
  are recorded before dispatch.
- The foreign tenant runs continuously at WIP 1 for 48-72 hours with the
  external liveness audit and primed LLM orchestrator active.
- Daemon rollover and orchestrator replacement resume from durable state
  without repeated side effects.
- There are zero silent stalls, gate bypasses, duplicate dispatches or
  landings, lost source, and unclassified holds.
- Every human interaction is a pre-classified human gate.
- The outcome says proceed, repair and repeat, or stop.

Capture these criteria with a repository-scoped external observation run
([observation runbook](2026-09-02-observation-runs.md)). The manifest freezes
thresholds before dispatch; append-only samples retain transient maxima and
daemon outages; typed intervention records and the derived report provide the
pilot evidence. The same mechanism is intentionally reusable for release
soaks, benchmarks, and incident windows.

### Ticket rollup

- `TKT-01M0FQ94FSY0VB4ZP60DK4Q8PJ` - post-slimming foreign-tenant pilot.

## M4: seven-day unattended acceptance

### Purpose

Make an evidence-backed R1 release decision. Daemon recovery and the primed
LLM orchestrator may act unattended within their authority. Human interaction
is allowed only through explicit human gates; any other rescue is an
intervention.

### Entry criteria

- M3 closed with a proceed recommendation.
- The `roadmap-gate:post-slimming-pilot` label has been reviewed on both
  unattended-run tickets. It is an advisory scheduling gate, not an automatic
  dependency, because the current ticket CLI cannot append dependencies.
- The external liveness auditor detects an injected silent stall.
- Acceptance thresholds and stop conditions are pre-registered.

### Release bar

- Zero silent stalls.
- Zero gate bypasses.
- Zero duplicate redispatches or landings.
- Zero lost source or delivery state.
- Every completed branch is delivered or in a classified actionable hold.
- No ad-hoc human intervention handles mechanical or delegated-LLM recovery.
- Every human gate carries evidence, the requested decision, blast radius, and
  a safe paused state.
- Spend and resources are bounded; no harness process or build artifact grows
  without an owner and cap.
- Daemon and orchestrator restarts do not repeat side effects.
- All of the above is demonstrated on the foreign tenant.

### Ticket rollup

- `TKT-01M0CTC52EBF8KSCK1CTFRCFYD` - external liveness audit and unattended
  drain-week thresholds.
- `TKT-01M0E8PP3B8YX3S0D5C2RKMGTF` - LLM-orchestrated foreign-tenant
  unattended week and release report.

## Human gates

The roadmap deliberately requires a human for:

- selecting the foreign tenant;
- approving credentials, repository policy, delivery mode, destructive scope,
  and security boundaries;
- resolving ambiguous product intent or an unsafe conflict;
- changing the R1 product boundary; and
- accepting or rejecting the final release evidence.

Everything else must be owned by deterministic recovery or a durably primed,
budgeted, journalled LLM orchestrator. Repeated ad-hoc human rescue is a failed
milestone, not successful unattended operation.

## Status protocol

The tracker is authoritative for ticket state and dependency readiness:

```sh
rk ticket list --repo rat-kingdom --status open
rk ticket show <TKT-id>
```

Update this roadmap when release scope, milestone ordering, exit criteria, or
duration estimates change. Do not edit it for every ticket transition. A
milestone closes only when its exit evidence is recorded in the tracker or a
durable report; a collection of closed implementation tickets is not enough.

At each milestone boundary, record:

1. the exact build and repository policy tested;
2. the tickets and fault scenarios exercised;
3. threshold results and intervention classifications;
4. unresolved holds and their authority class; and
5. the proceed, repair-and-repeat, or stop decision.

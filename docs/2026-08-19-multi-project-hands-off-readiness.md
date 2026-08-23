# Multi-project hands-off readiness

*2026-08-19. Current-state assessment and execution programme for using Rat
Kingdom as a hands-off swarm orchestrator on projects other than Rat Kingdom
itself.*

## Executive assessment

Rat Kingdom is roughly 60% of the way to a reliable hands-off orchestrator for
trusted, direct-merge repositories, and roughly 35-40% of the way to a general
multi-project system supporting protected branches, external CI, and stronger
isolation.

It is usable now for supervised or deliberately low-WIP experiments. It is not
yet ready to be left unattended on an important foreign repository.

The system already has durable tickets, workflows, tuples, budgets, isolated
worktrees, generation identity, crash respawn, runaway detection, reviewers,
named repository checks, content-bound repository policy, and an exact gated
landing path. The remaining gap is not basic agent dispatch. It is reliable
convergence when the distributed records disagree, portable safety defaults,
and evidence from an unattended non-self-hosted tenant.

## What "unattended" means

Unattended does **not** mean that every decision must be made by deterministic
daemon code. An orchestrating LLM agent counts as part of the unattended
system when it is:

- continuously or repeatedly primed with the operator contract;
- fed durable, resumable attention state rather than relying on chat memory;
- authorized to diagnose and repair a defined subset of conditions;
- budgeted, rate-capped, and required to journal its evidence and actions; and
- able to distinguish delegated judgment from decisions that require human
  feedback.

The useful boundary is therefore not "daemon versus human." It is an authority
ladder:

1. **Mechanical recovery**: deterministic code repairs a condition whose safe
   action is invariant-driven and idempotent.
2. **LLM-orchestrated recovery**: a primed coordinator investigates evidence,
   chooses among bounded reversible actions, and records why.
3. **Human gate**: the system deliberately pauses when product intent,
   security authority, destructive impact, external credentials, or an
   ambiguous conflict requires human judgment.

An unattended run may use levels 1 and 2. Level 3 does not make the run a
failure when the gate was correctly classified, durably surfaced, and did not
silently strand unrelated work. Ad-hoc human rescue of a condition that should
have belonged to levels 1 or 2 **is** an unattended-run intervention.

This reframes the proposed `rk-king` question. Rat Kingdom does not necessarily
need a monolithic permanent King process. It does need a durable contract by
which an external Codex, Claude, or other LLM session can hold an orchestrator
lease, consume attention, act within delegated authority, and hand unresolved
judgment to a human without losing state.

## Current strengths

- Agents work in isolated Git worktrees and communicate through durable,
  authenticated state rather than terminal scraping.
- Repository-owned named checks keep worker prompts and landing gates
  stack-neutral.
- Repository policy is versioned but becomes live only through exact,
  content-bound activation.
- The merge-mode landing queue tests an exact prepared merge candidate and
  advances the target using compare-and-swap semantics.
- Delivery records, commit-count awareness, landing-aware lifecycle handling,
  and deliberate-stop semantics have removed major sources of false delivery.
- Supervisor recovery includes crash respawn, stuck and runaway detection,
  post-completion process cleanup, rate caps, and announcements.
- The coordinator monitor, inbox, notification sinks, reactor, scheduler, and
  external signal ingestion provide most of the observation substrate an LLM
  orchestrator needs.

## Readiness gaps

### 1. Unsafe cross-repository artifact default

The worktree sweep currently defaults `artifact_paths` to `["target"]` at the
castle level. That is Rust-specific knowledge in a supposedly stack-neutral
daemon. A foreign repository may have a legitimate source or data directory
named `target`; a terminal-agent sweep could remove it from the worker
worktree.

The Phase 2 contract is the right one: reclaimable artifact paths are
repository-owned, activated policy, and default empty. This is a hard blocker
for foreign-repository autonomy.

### 2. Durable state does not reliably converge by itself

Current live evidence includes completed work whose tickets were later
reopened, conflict-held unlanded branches, recovery rows that prescribe a
manual merge, and stale tickets whose claims no longer match the verified
target branch.

The exact merge queue fixes tested-tree correctness, but Rat Kingdom still
needs an invariant-driven reconciler over agents, tickets, delivery records,
landing entries, workflow instances, Git refs, and target history. Mechanical
contradictions should self-repair. Ambiguous merge conflicts should become a
bounded LLM rework task or an explicit human gate, not a passive row with an
unsafe manual-merge suggestion.

### 3. The unattended claim has not passed its own acceptance test

The drain is currently disabled. The completed experiment was a supervised
36-hour probe, not the planned unattended week. It demonstrated real
throughput, but roughly 30 interventions were recorded. Approximately five
required genuine judgment; most were landing or state recovery that the
system should own mechanically or through the orchestrating LLM.

The successor test needs an external liveness auditor, pre-registered
thresholds, and intervention accounting split into mechanical, LLM-resolved,
correct human gates, and ad-hoc rescue.

### 4. Portability is implemented but not operationally proven

Several non-RK repositories are registered, but they still use legacy registry
policy rather than an activated `.rk/repo.cue`. There is no clean foreign-tenant
proof of:

```text
onboard -> activate checks and policy -> drain work -> review -> gate
        -> publish -> reconcile
```

The next tenant should differ materially from Rat Kingdom in stack or delivery
shape. A self-hosted fixture inside RK would not prove this boundary.

### 5. Direct merge is stronger than protected-branch delivery

GitHub PR mode currently pushes a branch and surfaces a compare URL; it does not
create the pull request. Forge-side merge detection is fetch-based and opt-in,
and external CI/merge state is not yet a complete delivery predicate. This
makes trusted direct-merge repositories much closer to hands-off readiness than
ordinary protected-branch projects.

### 6. Queue and convergence observability are incomplete

Landing-queue depth and per-entry age are not operator-visible. A slow queue and
a dead queue look alike. The system needs a concise per-repository answer to
"why is no work progressing?", plus a stale-queue escalation and an external
liveness audit that does not trust the subsystem being checked.

### 7. Resource isolation and economics need hardening

Known remaining work includes harness subprocess leakage, shared verification
directory contention, safe terminal artifact cleanup, reviewer caps and tier
routing, and full-workspace test flakes. In an unattended fleet, each leak or
false-red retry becomes repeated spend and possible duplicate work.

### 8. The current trust boundary is a trusted personal machine

Ordinary implementation workers run with full host access. Read-only
diagnostician and groomer roles exist, account-linked connectors are narrowed,
and external prompt text is fenced, but ordinary workers can still reach more
than one repository worktree. Rat Kingdom should either explicitly declare a
trusted-repository-only product boundary or add per-repository process, secret,
configuration, and network isolation before accepting arbitrary repositories.

## Recommended execution sequence

### Gate A: make a foreign-repository pilot safe

1. Make reclaimable artifact paths repository-owned and default empty.
2. Fix the harness subprocess leak and shared-check execution contention.
3. Expose landing queue depth, age, and active gate state.
4. Reconcile the current contradictory tickets, inbox rows, and branches.

### Gate B: give a primed LLM orchestrator bounded authority

1. Define the durable orchestrator lease, cursor, budget, and action journal.
2. Define the authority matrix for mechanical, LLM-delegated, and human-gated
   conditions.
3. Implement the first two LLM-owned recovery playbooks: stale delivery state
   and merge-conflict rework.
4. Ensure coordinator death, restart, or replacement resumes from durable
   attention state without replaying side effects.

### Gate C: prove one foreign tenant

1. Select a materially different registered repository.
2. Activate repository policy and named checks with WIP 1, a strict budget, and
   a curated five-to-ten-ticket backlog.
3. Run a 48-72-hour supervised pilot, including injected worker failure,
   daemon rollover, gate failure, and conflict recovery.

### Gate D: run the unattended week

Run the foreign tenant for seven days with daemon recovery and the primed LLM
orchestrator active. Human interaction is allowed only through pre-classified
human gates. Classify every intervention and publish the outcome.

## Readiness bar

The first defensible hands-off release requires:

- zero silent stalls;
- zero gate bypasses;
- zero duplicate redispatches;
- zero lost source or delivery state;
- every completed branch durably delivered or in a classified actionable hold;
- no ad-hoc human intervention for mechanical or delegated-LLM recovery;
- every human gate carrying evidence, requested decision, blast radius, and a
  safe paused state;
- bounded spend and no leaked harness processes or unbounded build artifacts;
- daemon and orchestrator restart tests that do not duplicate side effects; and
- the full result demonstrated on a non-RK repository.

## Published execution tickets

The approved tracer-bullet programme was published to the native Rat Kingdom
tracker on 2026-08-19:

1. `TKT-01M0E8PMWCRKE6WZ4ZNYB6Y6YS` — report cross-ledger convergence
   violations.
2. `TKT-01M0E8PN2SJEH9GNCYKYYDQEHM` — repair exact delivery-state
   contradictions idempotently.
3. `TKT-01M0E8PN9C41BWECGNW0990R3J` — route one recovery through the
   unattended authority ladder.
4. `TKT-01M0E8PNFQZ70F3ZFG3KCS39ZG` — rework merge-conflicted deliveries
   through a bounded orchestrator playbook.
5. `TKT-01M0E8PNP8BJRE65Q8SBQDSGHZ` — activate the first foreign tenant
   under explicit policy.
6. `TKT-01M0E8PNWNG1GPS5EJA7AGVTRC` — run a fault-injected foreign-tenant
   tracer batch.
7. `TKT-01M0E8PP3B8YX3S0D5C2RKMGTF` — run the LLM-orchestrated
   foreign-tenant unattended week.

These reuse rather than duplicate the existing Phase 2 tickets for safe
artifact cleanup, harness-process cleanup, shared-check arbitration,
landing-queue visibility, reviewer economics, and the external liveness audit.

## Estimate

A trusted direct-merge tenant is approximately two focused engineering weeks
plus an irreducible one-week soak away. Protected-branch GitHub operation adds
roughly two to four weeks. Strong isolation for arbitrary or untrusted projects
is a larger product boundary and should not be hidden inside Phase 2 cleanup.

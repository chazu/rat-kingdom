# Throughput convergence and the foreign tracer

**Status:** assessment and operating recommendation, 2026-08-21

## Executive assessment

Rat Kingdom is producing substantial activity, but product progress is not
keeping pace with the number of agents, tickets, reviews, and merges. The
factory currently behaves more like a reliability-maximizing maintenance
swarm than a milestone-delivery swarm.

This is not a claim that the recent work is useless. Bounded landing-gate
retry, shadow review, reviewer tier routing, durable recovery, identity
stripping, and runtime path resolution all improve the orchestrator. The
problem is portfolio selection: automatically discovered defects and review
findings are more precise, more actionable, and often higher priority than
feature work, so they continuously refill the front of the queue.

The result is a local attractor:

1. Running work exposes a failure.
2. The failure creates a precise corrective ticket.
3. The corrective ticket is easier to dispatch than a larger feature.
4. Its gate or review exposes another failure.
5. The new failure creates another correction.
6. Ticket and merge throughput rises while the product milestone advances
   slowly.

The foreign-repository tracer should interrupt this loop. It should be used
to discover which weaknesses matter on a real tenant, not treated as a reward
that becomes available only after every RK self-hosting weakness has been
eliminated.

## Evidence from the overnight run

For the period beginning at approximately 18:00 ET on 2026-08-20:

- 29 tickets or rework tickets closed;
- 14 gated merges reached `main`, representing 13 distinct delivery streams;
- 9 of those 13 streams were primarily corrective;
- 28 of the 29 closed-ticket titles described a bug, flake, audit,
  continuation, stabilization, or rework;
- 66 agents started: 34 implementers and 32 reviewers;
- reviewers consumed approximately 45% of agent spend; and
- foreign-tracer readiness advanced, but much less than raw activity implied.

The code-volume view is less pessimistic: the large landing-retry and shadow-
review capabilities accounted for most landed churn. That means real feature
work occurred, but it paid a high reliability and review tax. Delivery-stream
count and operator attention remain dominated by corrections.

## Root cause

The drain has no strong concept of an active product campaign. It sees READY
work, priority, dependencies, and capacity, but it does not sufficiently value
milestone impact over local actionability. Automatic ticket creation worsens
this imbalance because failures produce small, well-specified tickets much
faster than product intent produces equally dispatchable vertical slices.

Several additional effects reinforce the loop:

- feature parents remain open while numerous corrective children close;
- reliability and `unattended` labels legitimately raise defect priority;
- full-repository gates are expensive and create more opportunities for
  unrelated flakes;
- review and rework attempts consume nearly as much fleet capacity as
  implementation; and
- success is easy to observe as tickets closed or branches landed, while
  milestone movement is not the scheduler's primary score.

Increasing WIP does not solve this. It lets the same selection policy process
the corrective queue faster.

## Decision: use the tracer to set priorities

The next product campaign should be a bounded tracer in a repository other
than Rat Kingdom. The tracer is an experiment, not an R1 release claim and not
the longer unattended acceptance run described in [ROADMAP.md](ROADMAP.md).

The current M0 list remains useful as a readiness and risk register, but full
M0 closure should not be a prerequisite for this experiment. The tracer may
start once a minimum reversible safety envelope exists:

- the tenant is trusted and explicitly selected by a human;
- work runs at WIP 1 under a strict pre-registered budget;
- repository-owned named checks and protected paths are active;
- reclaimable artifact paths default to empty;
- branches and source evidence are preserved;
- no forced landing or gate bypass is permitted;
- destructive, credential, product, and ambiguous decisions remain human
  gates; and
- a primed LLM orchestrator may resolve bounded mechanical and delegated
  issues without that counting as human intervention.

Begin with three curated, low-risk tickets. A five-to-ten-ticket fault-
injection batch can follow if those three prove the basic lifecycle.

## The anti-recursion rule

The recommendations in this document are **not a new prerequisite backlog**.
Creating a collection of scheduler, ticket-schema, scoring, quota, and drain
features before running the tracer would reproduce the failure this document
describes.

For the first tracer, the orchestrating LLM should enforce the operating
policy manually using existing controls. No implementation ticket should be
created merely to automate these rules before the experiment.

Only three classes of newly discovered problem may interrupt the tracer
campaign:

1. credible risk of destructive action or lost source/delivery state;
2. a defect that directly prevents the current tracer ticket from advancing;
3. a required human decision for credentials, product intent, security, or
   genuinely ambiguous behavior.

Everything else is recorded for post-tracer triage and is ineligible for
automatic drain during the campaign.

## Operating policy for the tracer campaign

The following are orchestration rules, not pre-tracer software deliverables:

### Campaign-scoped admission

Dispatch only the selected tracer tickets and corrections directly required
to finish them. Do not drain the global RK backlog alongside the campaign.

### Finite correction budget

Give each tracer ticket at most two corrective attempts or a pre-registered
spend ceiling. At exhaustion, the orchestrating LLM must choose among:

- simplify the implementation while preserving the ticket outcome;
- accept a bounded and documented risk for the experiment;
- defer the issue and hold the ticket safely; or
- request a precise human decision.

Exhaustion must not recursively create and dispatch another correction.

### WIP allocation

Run one foreign implementation at a time. Review is allowed to occupy a
separate gate slot. RK self-improvement work remains frozen unless it satisfies
one of the three interruption classes above.

### Automatic-ticket quarantine

Automatically generated findings remain evidence, not funded work. They may
be attached to the active ticket, but they do not become READY unless the
orchestrating LLM classifies them as a direct campaign blocker.

### Outcome-based accounting

The primary measure is curated tracer tickets durably delivered or placed in
a correctly classified actionable hold. Ticket count, agent count, review
count, code churn, and merge count are diagnostic measures only.

## Immediate sequence

These are operational steps, not additional product stories:

1. Allow the already-active landing/recovery chains to settle; do not admit
   another RK improvement wave.
2. Verify `main`, deploy the landed daemon, and activate the current RK policy
   so the experiment starts from the source actually reviewed.
3. Have the human select the foreign tenant and approve delivery mode, checks,
   protected paths, credentials, notifications, budget, WIP, artifact policy,
   and human gates.
4. Run the existing read-only onboarding and preflight checks.
5. Curate three low-risk vertical tickets and dispatch them at WIP 1 under the
   campaign rules above.
6. Publish an intervention log classifying every issue as daemon-mechanical,
   LLM-orchestrated, correct human gate, or ad-hoc rescue.
7. Use the result to decide which RK work is actually next.

## Success and stop conditions

The tracer succeeds if all three tickets are durably delivered or held with a
precise actionable classification, with no silent stall, gate bypass,
duplicate dispatch or landing, lost source, or unjournaled mutation.

Stop immediately for destructive-risk ambiguity, lost state, an unbounded
retry loop, a gate bypass, or credentials crossing the approved tenant
boundary. Preserve all branches and evidence, then decide whether the result
requires one bounded repair-and-repeat cycle or invalidates the current
approach.

The purpose is not to prove Rat Kingdom complete. It is to replace RK-only
speculation with evidence about whether Rat Kingdom can move another project
forward.

# External SDLC signals and reactions

Date: 2026-08-04

Status: exploratory design; no implementation commitment

## Problem

Rat Kingdom coordinates local development well: it dispatches isolated agents,
runs repository-owned verification, reacts to tuples, schedules workflows, and
tracks the resulting work. Its view becomes incomplete once work leaves the
local checkout.

Important software-delivery facts currently live elsewhere:

- CI knows whether a revision built and passed its required checks;
- a forge knows whether a pull request merged;
- a build system knows which artifact was produced from which revision;
- a deployment system knows which artifact is running in each environment;
- telemetry and alerting systems know whether that deployment is healthy;
- an incident system knows what production problem is being investigated.

Without those signals, RK cannot connect a local ticket or agent branch to the
later build, deployment, alert, incident, and remediation. An operator has to
notice the external event, reconstruct the context, and initiate the next RK
action manually.

The desired system is cognizant of the full software-development lifecycle:

```text
ticket
  -> agent/workflow
  -> branch
  -> commit
  -> CI run
  -> build artifact
  -> deployment
  -> running service revision
  -> alert/incident
  -> diagnosis
  -> remediation ticket or pull request
```

"Cognizant" does not mean that RK becomes a CI server, telemetry database, or
deployment platform. It means RK can observe significant state and transitions,
correlate them with its own work, and run an explicitly authorized reaction.

## Current foundation

RK already has the two axes needed after an external signal has entered the
system.

The tuple reactor is the event axis:

```text
tuple lands -> durable reactor scan -> trigger -> workflow
```

It uses a persisted cursor and durable idempotency markers, so the live feed is
only a wake-up hint and not the source of truth. Dispatch is at-least-once with
deduplication for a particular `(trigger, tuple-id)`.

The scheduler is the time axis:

```text
clock fires -> schedule -> workflow
```

It provides a durable minute cursor, bounded catch-up, and single-flight
workflow dispatch.

The tuple model already distinguishes two useful meanings:

- `Event`: a record that something happened;
- `Fact`: current ground truth such as repository metadata or CI status.

The missing capability is a trustworthy ingress and correlation boundary in
front of those mechanisms. Plain `rk out` can demonstrate the reaction path,
but it does not supply an external-source identity, upstream delivery
deduplication, state-transition detection, remote webhook authentication, or
payload hygiene.

The existing workflow `run` step is also the wrong polling primitive. It runs a
command in an active agent's worktree and therefore requires an agent to have
been spawned. Polling a CI or telemetry API should neither spend model tokens
nor put service credentials in an agent worktree.

## Goals

1. Accept significant events from CI, forge, build, deployment, alerting, and
   incident systems.
2. Poll systems that cannot or should not push events and feed the observations
   through the same ingestion path.
3. Authenticate each integration as a narrowly authorized source rather than as
   an agent or operator.
4. Deduplicate retries using the external system's delivery identity.
5. Detect state transitions and avoid reacting repeatedly to an unchanged
   observation.
6. Correlate external signals with repositories, revisions, RK workflows,
   tickets, builds, deployments, services, environments, and incidents.
7. Reuse the current tuple reactor, scheduler concepts, workflows, budgets,
   approval gates, and repository policy wherever their contracts already fit.
8. Keep production credentials and arbitrary telemetry outside agent context and
   worktrees.
9. Make every automated reaction bounded, visible, and recoverable by an
   operator.

## Non-goals

- Storing raw metrics, logs, traces, or complete CI archives in RK.
- Replacing Prometheus, Datadog, Sentry, a forge, a CI server, or an incident
  manager.
- Exposing the daemon's Unix socket or operator token directly to the internet.
- Giving an agent CI-administration, cloud, deployment, or production
  credentials.
- Automatically deploying, rolling back, restarting production, or merging a
  remediation without an explicit policy and, initially, human approval.
- Implementing every vendor in the RK core.
- Claiming exactly-once external side effects. Ingestion can be idempotent;
  reactions still need action-specific idempotency and policy.

## Design principle: ingest signals, not telemetry streams

RK should consume decisions and meaningful transitions produced by the systems
that already aggregate raw data.

Good inputs include:

- a CI run completed with `success`, `failure`, or `cancelled`;
- the required-check state for `main` changed from passing to failing;
- deployment `dep-42` promoted artifact `build-9182` to production;
- production now runs revision `abc123`;
- an alert entered firing state, changed severity, or resolved;
- an SLO entered or left violation;
- an incident was opened, mitigated, or closed.

Bad inputs include every request span, log line, CPU sample, or intermediate CI
job update. Those belong in their native data systems. An RK event should carry
a compact summary and a link or external reference from which authorized tools
can retrieve detail when needed.

This boundary is both an operating-cost control and a context-hygiene control:
an LLM should not be dispatched for every sample in a burst.

## Proposed architecture

```text
CI / forge / deploy system / alert manager / incident system
                              |
                     source-specific adapter
                              |
                 authenticated `rk ingest`
                              |
          +-------------------+-------------------+
          |                                       |
 append-only Event occurrence             current Fact snapshot
          |                                       |
          +-------------------+-------------------+
                              |
                      tuple reactor
                              |
                  trigger and workflow policy
                              |
       diagnose / create work / propose PR / request approval
```

Vendor adapters normalize their input. RK owns the canonical signal envelope,
source authorization, deduplication, transition detection, correlation, and
reaction policy.

Push and pull converge at `rk ingest`; they must not create separate event
semantics.

## Canonical signal envelope

A proposed version-one envelope is:

```json
{
  "schema": 1,
  "source": "github",
  "delivery_id": "check-run:9182:attempt:2",
  "kind": "ci.run.completed",
  "subject": "repo:rat-kingdom/ref:main",
  "occurred_at": "2026-08-04T17:42:00Z",
  "repo": "rat-kingdom",
  "revision": "abc123",
  "service": null,
  "environment": "ci",
  "state": "failure",
  "severity": "error",
  "summary": "verify failed in clippy",
  "url": "https://example.invalid/runs/9182",
  "correlation": {
    "ticket": null,
    "workflow": null,
    "build": "9182",
    "deployment": null,
    "incident": null
  },
  "attributes": {
    "check": "verify",
    "branch": "main"
  }
}
```

The fields have different responsibilities:

- `schema` versions the normalized contract independently of vendor payloads.
- `source` is the authenticated integration identity.
- `delivery_id` is the source's idempotency key for one occurrence.
- `kind` is a stable semantic event type, not a vendor event name unless the
  vendor name is already appropriate.
- `subject` is the stable entity or condition whose current state may change.
- `occurred_at` is the source timestamp; RK separately records receive time.
- `repo` tells RK which registered checkout owns a repository-scoped reaction.
- `revision`, `service`, and `environment` connect development to runtime.
- `state`, `severity`, and `summary` support routing and bounded operator views.
- `url` points to detail without copying the entire external record.
- `correlation` carries known RK and SDLC identifiers.
- `attributes` holds bounded, non-secret, source-specific dimensions.

Required fields should depend on the operation. An occurrence requires a
`delivery_id`; a state observation requires a `subject` and `state`. Repository
resolution must fail closed when a reaction needs a checkout and the signal
does not identify one unambiguously.

## Event occurrences and Fact snapshots

Two ingestion modes cover distinct semantics.

### Occurrence

An occurrence is append-only:

```bash
rk ingest event \
  --source github \
  --delivery-id check-run:9182:attempt:2 \
  --scope rat-kingdom \
  --kind ci.run.completed \
  --payload-file event.json
```

It writes an `Event` tuple after authenticating the source and atomically
recording the delivery receipt.

Examples are a deployment attempt, pull-request merge, or completed CI run.

### State observation

A state observation describes what is currently true:

```bash
rk ingest state \
  --source alertmanager \
  --scope rat-kingdom \
  --subject service:api/environment:production \
  --state firing \
  --payload-file alert.json
```

RK compares the normalized observation with the last state for
`(source, scope, subject)`:

- `resolved -> firing` writes or refreshes the current Fact and emits a transition
  Event;
- `firing -> firing` refreshes `last_seen` but emits no transition;
- `firing -> resolved` updates the Fact and emits a recovery Event.

The comparison should use a deliberate semantic state digest, not the entire
payload. A changing request count or source timestamp must not turn one
continuing incident into a stream of state changes.

State transition detection is the primary defense against poll and alert
storms. Trigger rate caps remain a final safety bound, not the normal dedupe
mechanism.

## Source authentication and authority

The current operator and agent identities should remain unchanged. External
integrations should authenticate as a new constrained principal such as:

```text
source:github-ci
source:alertmanager-production
source:sentry
```

A source principal may:

- submit canonical occurrences and state observations for configured scopes;
- reuse its own delivery IDs idempotently;
- optionally query only its own ingestion receipts or cursor state.

A source principal may not:

- spawn, steer, dismiss, or respawn agents;
- run or approve workflows directly;
- create arbitrary operator-authored tuples;
- write conventions or repository policy;
- land branches or open pull requests;
- read unrelated tuples or agent transcripts.

Source credentials should be distinct from the daemon's root operator token.
For local adapters the daemon may spawn and communicate with the adapter
directly. A separate gateway or long-running sidecar needs a derived,
source-scoped credential.

The authenticated source name must be authoritative. A request may not claim a
different `source` in its payload.

## Delivery deduplication

The reactor's existing marker prevents a trigger from firing twice for the same
tuple ID. It does not prevent an external retry from creating two tuple IDs.

Ingress therefore needs a persistent receipt keyed by:

```text
(source, delivery_id)
```

The receipt and tuple insertion should share one transaction where practical.
On a duplicate, `rk ingest` returns the original result and writes no new tuple.

Receipts need a documented retention policy. Retention must exceed the retry
window promised by the source. A permanent receipt may be appropriate for
strongly identified build and deployment records; high-volume webhook receipts
may be compacted after a configured interval.

Pull adapters should also retain their opaque source cursor. The cursor reduces
work, while delivery receipts protect correctness if a cursor is replayed,
reset, or interpreted differently after an adapter upgrade.

## Push adapters

Push systems send a vendor webhook to a small external gateway:

```text
vendor webhook
      |
validate signature, timestamp, and size
      |
normalize vendor payload
      |
submit canonical signal through source-scoped ingress
```

The public HTTP receiver should not be added directly to the daemon in the
first design. Keeping it separate preserves the daemon's local Unix-socket
boundary and lets the gateway own internet-facing concerns:

- TLS and routing;
- vendor HMAC or signature validation;
- request size and rate limits;
- source-specific replay protection;
- network exposure and deployment;
- rejection logging without exposing the tuplespace.

An `rk-gateway` binary could eventually host several adapters, but the protocol
must permit independently deployed adapters as well.

## Pull adapters

Some systems are easier or safer to poll. A source manager periodically invokes
an installed adapter with its last cursor:

```text
source cadence
      |
adapter polls external API with machine-local credentials
      |
adapter emits canonical NDJSON and a new opaque cursor
      |
each signal passes through the normal ingestion boundary
```

A simple adapter protocol could use a subprocess whose stdout is canonical
NDJSON. Example shapes, not committed command names:

```text
rk-source-github ci --cursor <opaque>
rk-source-prometheus alerts --cursor <opaque>
rk-source-sentry issues --cursor <opaque>
```

The source manager needs:

- independent source cadences;
- a hard timeout for every poll;
- single-flight per source;
- bounded catch-up after daemon downtime;
- durable opaque cursors;
- exponential failure backoff;
- a visible obstacle or need after sustained source failure;
- output and payload size bounds;
- explicit environment and secret references.

Source executable and credential configuration should be machine-local or pass
through the same explicit repository activation process as other executable
policy. A repository must not gain arbitrary daemon-host command execution merely
by committing a source definition.

## Secrets and payload hygiene

The adapter, not an agent, talks to the external API. Credentials remain in the
daemon/gateway environment, operating-system keychain, or a named secret
provider.

The normalized envelope should be an allowlist. Ingestion must support:

- maximum envelope and attribute sizes;
- configured redaction of keys and values;
- rejection or hashing of known credential patterns;
- bounded summaries;
- URLs or opaque artifact references in place of raw logs;
- an option to mark an observation local-only when it must not replicate across
  castles.

Agents should retrieve additional evidence through a read-only, audited adapter
operation or a sanitized artifact. They should not receive the integration's
write credential merely because a workflow was triggered by it.

## Trigger matching and storm control

The current trigger can prove the concept by matching normalized identities such
as:

- `ci_failed`;
- `ci_recovered`;
- `deployment_succeeded`;
- `production_alert_firing`;
- `production_alert_resolved`.

That avoids a core matcher change for an initial experiment. The canonical
envelope should nevertheless retain a stable `kind` and structured fields so
identities do not become an ever-growing encoding of every predicate.

A later trigger schema should support structured matching:

```cue
match: {
	category:    "event"
	source:      "github"
	kind:        "ci.run.completed"
	state:       "failure"
	environment: "ci"
	branch:      "main"
}
```

Useful reaction controls include:

- `dedupeBy`: a subject, revision, incident, or other envelope path;
- `singleFlightBy`: do not diagnose the same incident concurrently;
- `debounce`: wait briefly for a burst of related signals to settle;
- `cooldown`: bound repeated attempts while a condition remains unresolved;
- `edge`: react only to selected state transitions;
- `maxFires`: preserve the existing hard per-window cap.

Structured matching should replace serialized-payload substring matching for
external signals. Substring search is useful for ad hoc tuples but too fragile
for security- or production-relevant reaction policy.

## Correlation and provenance

The largest value is not webhook dispatch; it is preserving the causal chain
across system boundaries.

At minimum, build and deployment sources should carry:

- registered RK repository;
- full git revision;
- branch and pull-request identifiers when known;
- CI run and required-check identifiers;
- build and artifact identifiers;
- service and environment;
- deployment identifier and outcome;
- originating RK workflow and ticket when available.

RK should maintain a current deployment Fact equivalent to:

```text
service api in production runs artifact build-9182 from revision abc123
```

Then a production alert can be resolved mechanically:

1. The alert identifies a service and environment.
2. The deployment Fact identifies the running artifact and revision.
3. The deployment event identifies the build.
4. The build identifies its CI run, pull request, RK workflow, and ticket.
5. A diagnostic workflow starts from the exact revision and receives compact
   links to the relevant external evidence.

Unknown links should remain explicitly unknown. An agent may investigate and
propose a correlation, but RK must not silently turn an inferred association
into daemon-verified provenance.

## Reaction safety ladder

External observation does not imply authority to mutate the external system.
The initial policy should separate reactions into two classes.

Automatically allowed reactions:

- ingest and correlate a signal;
- refresh current Fact state;
- make the condition visible to an operator;
- spawn read-only diagnosis;
- create a ticket, need, obstacle, or sanitized artifact;
- reproduce a failure locally;
- prepare a remediation branch or pull request;
- run repository-owned named checks.

Approval-gated reactions by default:

- rerun or cancel a CI job;
- change an incident's state;
- mutate infrastructure or runtime configuration;
- restart, deploy, promote, or roll back a service;
- merge or directly land a remediation.

If RK later performs external mutations, they should use named actions analogous
to repository-owned named checks. An action definition identifies a bounded
operation and its credential reference; a workflow may request that action but
may not inject arbitrary shell or environment. Each action needs:

- an explicit authorization and approval policy;
- an idempotency key;
- a hard timeout;
- a recorded request and result;
- a dry-run or describe mode where the external system supports one;
- no delivery of the underlying secret to an agent.

## Example reactions

### CI failure

```text
ci.run.completed(state=failure, revision=abc123)
      |
record event and current CI Fact
      |
single-flight by repository + revision + check
      |
spawn diagnostic rat from revision abc123
      |
retrieve bounded logs / reproduce with named checks
      |
write diagnosis artifact
      |
create or update remediation ticket / propose pull request
```

The workflow should not merge merely because it repaired a local reproduction.
The later CI result is a new authoritative signal.

### Production alert

```text
alert transition resolved -> firing
      |
record alert Fact and transition Event
      |
resolve service/environment to deployed revision
      |
surface operator need immediately
      |
spawn read-only diagnosis with sanitized telemetry references
      |
correlate recent deployment and originating work
      |
propose remediation, rollback request, or escalation
      |
human approval before production mutation
```

The alert must remain visible even if budget enforcement prevents an agent from
spawning. Ingestion and operator attention are deterministic control-plane work;
LLM diagnosis is optional follow-on work.

### Recovery

```text
alert transition firing -> resolved
      |
refresh current Fact
      |
emit recovery Event
      |
resolve or annotate the matching RK need/incident
      |
record whether recovery followed a deployment, rollback, or no known action
```

Recovery is first-class evidence. RK must not infer that its remediation caused
the recovery unless the provenance supports that conclusion.

## Operator visibility and recovery

An external signal should have a useful deterministic outcome before any model
is called:

- receipt accepted or rejected with a reason;
- source health and last successful poll visible;
- current state represented as a Fact;
- important firing conditions visible in `rk inbox` or coordinator attention;
- triggered workflow ID linked from the event/reaction record;
- duplicate, coalesced, rate-capped, and failed reactions inspectable;
- durable cursor or receipt information available for recovery.

Operator documentation must explain how to determine whether a signal was:

1. received and authenticated;
2. deduplicated or accepted;
3. normalized into the expected Event/Fact;
4. matched by a trigger;
5. dispatched to a workflow;
6. blocked by single-flight, rate, budget, policy, or approval;
7. completed, failed, or awaiting human action.

An accepted webhook is not proof that the intended reaction happened.

## Configuration boundaries

The likely configuration split is:

Machine-local source configuration:

- adapter executable or gateway binding;
- source-scoped credential or secret reference;
- allowed scopes/repositories;
- poll cadence, timeout, backoff, and cursor;
- payload limits and redaction policy.

Repository-owned reaction configuration:

- trigger predicates;
- workflow definitions;
- named verification checks;
- repository correlation metadata such as service ownership;
- permitted reaction and delivery policy.

This preserves RK's existing repository-owned policy direction without letting
a repository definition silently acquire daemon-host secrets or arbitrary
command execution.

## Delivery options considered

### Option A: external scripts call `rk out`

This is the fastest proof and requires no core changes.

Advantages:

- validates whether the reactions are useful;
- reuses the current reactor and workflows immediately;
- suitable for a controlled same-machine experiment.

Limitations:

- requires operator-equivalent local access;
- has no upstream delivery deduplication;
- has no source identity or state-transition contract;
- does not solve internet-facing webhooks;
- encourages every script to invent its own payload.

Use only as a short-lived experiment.

### Option B: embed vendor integrations in `rk-daemon`

Advantages:

- one process and one configuration surface;
- direct access to the tuplespace and workflow engine.

Limitations:

- couples daemon releases to vendor churn;
- expands the daemon's network and secret-handling attack surface;
- risks rebuilding an unused telemetry subsystem;
- makes independent adapter deployment difficult.

Do not choose this as the general architecture.

### Option C: canonical ingress plus external adapters

Advantages:

- keeps vendor code outside the core;
- gives push and pull one semantic boundary;
- preserves the daemon's local authority boundary;
- permits source-scoped authentication and transactional dedupe;
- lets integrations evolve independently.

Limitations:

- introduces an adapter protocol and another deployable process for push;
- requires careful source configuration and operational health reporting.

This is the recommended target.

## Proposed vertical slices

### Slice 0: reaction proof without core changes

Use a controlled local script to write normalized `ci_failed` and
`ci_recovered` Event tuples. Install a trigger and a read-only CI triage workflow.

Exit evidence:

- one synthetic failure launches exactly one diagnostic workflow;
- a repeated unchanged observation does not launch another workflow, even if
  the experiment script must provide that dedupe initially;
- the workflow starts from the reported revision and creates a bounded diagnosis
  artifact;
- recovery is recorded independently;
- no branch lands automatically.

This slice validates product usefulness, not the production ingress contract.

### Slice 1: typed ingress

Add the canonical envelope, `rk ingest event`, source-scoped authorization, and
transactional `(source, delivery_id)` receipts.

Exit evidence:

- a source can ingest an allowed event but cannot invoke operator or agent
  methods;
- identical deliveries return the original receipt and create one tuple;
- spoofing another source is rejected;
- malformed, oversized, or secret-bearing test payloads fail closed;
- reactor restart/replay still fires the matched workflow once.

### Slice 2: state observations

Add `rk ingest state`, current Fact projection, semantic state digests, and
transition Events.

Exit evidence:

- `passing -> failing -> failing -> passing` creates two transitions, not four;
- current Fact reads report the latest state and last-seen time;
- transition handling remains correct across daemon restart;
- state-key and retention semantics are documented.

### Slice 3: one pull source for CI

Add the source-manager lifecycle and one GitHub CI adapter using machine-local
credentials. Polling is recommended before a public webhook gateway because it
does not require opening an inbound network surface.

Exit evidence:

- the source cursor resumes after daemon restart;
- source polling is single-flight and times out safely;
- replayed source results are deduplicated;
- source failure becomes visible without spawning an LLM;
- a real failed CI run produces the canonical signal and launches the read-only
  triage workflow.

### Slice 4: deployment provenance

Ingest build and deployment signals and maintain the current
`(service, environment) -> artifact -> revision` Fact.

Exit evidence:

- the operator can query which revision is running for a service/environment;
- a deployment links back to its build and repository revision;
- missing or ambiguous repository ownership fails visibly rather than guessing.

### Slice 5: production alerting

Add one alert adapter and correlate firing/resolved transitions with deployment
provenance. Keep all production operations read-only.

Exit evidence:

- one alert transition surfaces immediate operator attention;
- repeated firing polls do not spawn repeated diagnoses;
- diagnosis receives a deployed revision and sanitized evidence references;
- resolution updates the current Fact and closes or annotates the RK attention
  record;
- no production credential enters an agent environment.

### Slice 6: push gateway

Add a separately deployable HTTP gateway with one vendor signature verifier and
source-scoped submission to RK.

Exit evidence:

- invalid signatures, stale requests, duplicates, oversized payloads, and rate
  violations are rejected;
- the gateway possesses no operator authority;
- accepted delivery, normalization, trigger match, and workflow dispatch are
  traceable end to end.

### Slice 7: named external actions

Only after observation and correlation are reliable, add an approval-gated
named action such as rerunning a CI check. Production mutation remains a later,
separately approved capability.

## Recommended starting point

Start with CI, then deployment provenance, then production alerts.

CI is the safest place to validate the core model:

- it already has repository and revision identity;
- failures are straightforward to reproduce with repository-owned checks;
- the reaction can remain read-only and propose a pull request;
- polling can use existing machine-local forge authentication;
- it exercises delivery dedupe, state transitions, trigger routing, and workflow
  correlation without production credentials.

Production alerts should follow deployment provenance. An alert that RK cannot
connect to the revision actually running in the affected environment provides
attention but not full lifecycle awareness.

## Open questions

1. What should the first real source be: GitHub CI, another CI provider, or a
   local synthetic adapter?
2. Is `repo` always the workflow-routing scope, or do multi-repository services
   require a first-class service ownership registry from the beginning?
3. Which external state should replicate across castles, and which production
   observations must remain local?
4. How long should delivery receipts, occurrence Events, and historical Fact
   snapshots be retained?
5. Should important external conditions become `Need` tuples directly, or
   should reaction policy explicitly emit the operator-attention tuple?
6. Does the first CI workflow only diagnose, file a native RK ticket, create a
   remediation branch, or open a pull request?
7. How should an adapter expose additional read-only evidence to a diagnostic
   rat without giving it the adapter's credentials?
8. Should structured trigger matching be implemented with fixed envelope fields,
   JSON paths, or a bounded CUE predicate?
9. Should source definitions be global machine-local configuration, explicitly
   activated repository proposals, or a split where the machine owns execution
   and the repository owns only reaction policy?
10. Which external action, if any, is safe enough to prove the named-action
    contract before considering deployment or rollback?

These questions can be resolved incrementally. The recommended first decision
is the source and exact CI reaction to demonstrate; it determines the smallest
useful vertical slice without committing RK to a broad integration framework.

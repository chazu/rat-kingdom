# Hierarchical coordinator monitoring

Date: 2026-07-29

Status: implemented for the daemon/CLI boundary; host adapters are optional

Related: [`2026-07-27-coordinator-view-plan.md`](2026-07-27-coordinator-view-plan.md)

Implementation checkpoint: the daemon now persists explicit coordinator
ownership on workflow/agent records, computes bounded middle-rat rollups,
journals normalized lifecycle/progress/attention events, exposes hierarchical
snapshot/replay plus durable session `register`/`pending`/`ack` calls, and ships
`rk monitor`/`rk progress`. The portable contract ends at that read/ack
boundary: a coordinator session may consume it with `rk monitor --once` or the
`coordinator.pending` RPC. Automatic insertion into a model turn is a
host-specific adapter concern because the coordinator may be an arbitrary
Codex, Claude Code, or other harness session.

## Problem

Rat Kingdom already records most of the information needed to understand live
work:

- the supervisor owns agent lifecycle state and structural `parent` lineage;
- workflow instances persist their current step and terminal state;
- `harness_result`, obstacle, need, and workflow events are durable tuples;
- `coordinator.watch` provides a durable snapshot/replay seam for one workflow
  instance;
- `rk inbox`, `rk digest`, `rk top`, and `rk log --follow` expose useful
  operator views.

The missing behavior is an attention contract for the coordinating agent. The
current surfaces are pull-based and fragmented. A coordinator must remember to
poll several commands, and the existing coordinator stream is instance-scoped.
The daemon now provides one hierarchy-aware pending read, but it cannot assume
that it can inject into or interrupt the host session that happens to be
driving it.

The coordinator is also a user-facing interaction plane. It must remain
available for user instructions and general system interaction; subscribing it
to every rat in the castle would turn useful telemetry into context noise.

The desired topology is therefore:

```text
coordinator session
        |
        +-- owned workflow A
        |      +-- foreman A (reporting boundary)
        |             +-- leaf rats
        |
        +-- owned workflow B
               +-- steward B (reporting boundary)
                      +-- leaf rats
```

The coordinator monitors workflows and their middle-rats. Middle-rats own the
detail of their descendants and report a compact rollup. Leaf-rat events reach
the coordinator only when they become an escalation or when the coordinator
explicitly drills into that subtree.

## Goals

1. Keep the coordinator's default attention surface small enough for an
   interactive user-facing session.
2. Make workflow and middle-rat completion, failure, blocking, and escalation
   visible through one bounded pending read; host adapters may call that read
   automatically at turn boundaries.
3. Give the coordinator a current snapshot plus durable replay, so reconnects
   and daemon restarts do not silently lose important events.
4. Represent descendant work as bounded summaries at the middle-rat boundary.
5. Preserve on-demand drill-down into a middle-rat's children and transcripts.
6. Let the coordinator decide what action to take; monitoring must not itself
   steer, dismiss, merge, or retry work.
7. Keep existing raw tuple, inbox, digest, top, and per-agent log surfaces
   working for operators and diagnostics.

## Non-goals

- Streaming every leaf-rat transcript line, tool call, token update, or budget
  tick into the coordinator context.
- Making the coordinator responsible for supervising every rat in the castle.
- Replacing the tuplespace or the existing protected coordinator journal.
- Making a desktop notification or a long-running shell process the only way a
  model session can receive events.
- Automatically resolving failures or making workflow policy decisions on the
  coordinator's behalf.
- Claiming exactly-once execution of workflow side effects. Event delivery is
  replayable and idempotent; execution semantics remain the workflow engine's
  responsibility.

## Terminology and ownership model

### Coordinator session

The user-facing agent session that accepts instructions and starts or steers
workflows. A session needs a stable opaque identity for the lifetime of its
conversation. This identity is not the same thing as a rat name or a daemon
process ID.

### Owned workflow

A workflow instance launched by a coordinator session. The instance is the
primary scope for monitoring. Existing workflow instance identity and repo
scope remain authoritative for workflow state.

### Middle-rat

A rat that acts as a reporting boundary for a workflow or a subtree: normally a
foreman or steward, but not necessarily limited to those role strings. The
boundary is determined by dispatch metadata and structural lineage, with role
used for presentation and policy defaults.

### Leaf rat

A descendant whose ordinary lifecycle and progress are owned by a middle-rat.
Leaf rats remain queryable and may still emit escalations, but routine events do
not enter the coordinator's default stream.

### Structural versus workflow lineage

Both existing relationships matter:

- `AgentRecord.parent` describes who spawned an agent and is the authoritative
  supervision tree for child routing.
- `workflow_instance` describes which workflow owns the dispatch and is the
  authoritative workflow scope.

The coordinator projection must correlate both instead of inferring ownership
from tuple payload text or from role names alone.

## Design

### 1. Establish the coordinator scope at workflow launch

Each coordinator-owned workflow instance carries an `owner` or
`coordinator_session` identifier. Agents dispatched by that instance inherit
the association through their workflow metadata. Direct ad-hoc spawns retain
their existing `parent` behavior and may opt into a coordinator scope when
they are intentionally part of an interactive workflow.

For compatibility, instances and agent records written before this field
exists load without an owner and remain visible through explicit repo or
instance filters. They are not silently attributed to a coordinator session.

The daemon should expose ownership in snapshots and event envelopes:

```json
{
  "coordinator": "session-01...",
  "workflow_instance": "wf-01...",
  "agent": "Emmental-2",
  "parent": "Foreman-1",
  "role": "rat"
}
```

The coordinator session owns the workflow scope. It does not automatically own
every agent that happens to share the repo.

### 2. Make the reporting boundary explicit

The system should not rely on the literal role names `foreman` and `steward` to
decide what reaches the coordinator. A workflow dispatch should be able to mark
an agent as a reporting boundary, for example with a normalized coordination
metadata value:

```json
{
  "coordination": {
    "reports_to": "coordinator",
    "descendant_policy": "rollup"
  }
}
```

The initial defaults can treat workflow-owned foremen and stewards as visible,
while preserving an explicit override for new middle-rat roles. Lineage remains
the safety boundary; role is only a default policy and display label.

The projection should compute, for each owned workflow:

- visible middle-rats;
- descendants below each middle-rat;
- counts by `spawning`, `running`, `completed`, `failed`, `orphaned`, and
  `dismissed`;
- blocked or escalated descendants;
- last meaningful update and age of the oldest active descendant;
- the middle-rat's own state, branch, ticket, cost, and current summary.

This computation belongs in a pure coordinator projection module. It should be
testable from `AgentRecord`, workflow, and event fixtures without a running
daemon.

### 3. Use event routing policies instead of one flat stream

Every candidate coordination event is classified before it enters the
coordinator projection:

| Route | Meaning | Default coordinator behavior |
| --- | --- | --- |
| `local` | Detail owned by a middle-rat | Do not deliver directly |
| `rollup` | Changes aggregate subtree state | Include in middle-rat summary |
| `escalate` | Requires a decision or attention above the boundary | Deliver promptly |
| `terminal` | Workflow or visible middle-rat reached a terminal state | Deliver promptly |
| `drilldown` | Explicit temporary subscription | Deliver while the subscription is active |

Routine leaf-rat completion is `rollup`. A leaf-rat failure is normally part of
the middle-rat's failure count, but becomes `escalate` if the middle-rat reports
it cannot recover, the subtree loses its reporting boundary, or workflow policy
requires coordinator input. A failed or orphaned middle-rat is always visible
to the coordinator.

Safety and ownership failures may bypass the normal boundary. For example, if a
foreman dies before reporting a child failure, the daemon must still surface
that the reporting boundary is unhealthy; otherwise hierarchy would hide the
very failures it exists to contain.

### 4. Extend the protected coordinator journal

The existing coordinator journal and cursor/replay contract should remain the
durability mechanism. Do not create a second notification database or expose
raw `agent_spawned` and `harness_result` tuples as the coordinator API.

Add a bounded normalized coordination event envelope. The exact wire name can
follow the existing `CoordinatorEvent` types, but the payload should contain at
least:

```json
{
  "kind": "middle_rat_progress",
  "route": "rollup",
  "severity": "info",
  "coordinator": "session-01...",
  "workflow_instance": "wf-01...",
  "subject": {
    "agent": "Foreman-1",
    "generation": 1,
    "role": "foreman"
  },
  "change": "checkpoint",
  "summary": "4/7 child tickets complete; reviewing the remaining three",
  "rollup": {
    "total": 7,
    "running": 2,
    "completed": 4,
    "failed": 0,
    "blocked": 1,
    "orphaned": 0
  },
  "revision": 12
}
```

Invariants:

- journal cursors are monotonic and assigned by the durable journal, not by
  ULID sort order;
- every subject has a monotonic revision so duplicate or stale summaries can
  be discarded;
- strings and result summaries are size-bounded;
- full transcripts, workflow context, and detailed errors remain behind
  existing status/log commands;
- replay is safe after reconnect, and a lagged stream produces an explicit
  resync signal;
- terminal events are idempotent and generation-qualified;
- journal rows are protected from ordinary tuple `take` or targeted deletion.

Existing `workflow_state_changed` events remain the authoritative workflow
transition records. The new projection adds middle-rat and routing information
around them rather than changing the meaning of existing reactor events.

### 5. Provide a hierarchical snapshot and watch API

Extend the existing coordinator filter, preserving the current instance-scoped
behavior as the narrowest and safest mode:

```json
{
  "method": "coordinator.watch",
  "params": {
    "coordinator": "session-01...",
    "repo": "rat-kingdom",
    "after": 42,
    "depth": "middle",
    "include": ["attention", "rollup"]
  }
}
```

The initial response contains:

- owned workflow summaries;
- visible middle-rat summaries;
- bounded replay after the requested cursor;
- the snapshot cursor boundary;
- `truncated` and `resync_required` indicators.

Live notifications contain only events matching the same ownership, route, and
depth policy. Subscribe-before-scan ordering and cursor deduplication should
match the implemented coordinator view plan.

Supported scopes should be explicit:

- `instance`: one workflow, existing behavior;
- `owned`: all workflows owned by one coordinator session;
- `subtree`: one middle-rat and its descendants, summarized by default;
- `repo`: an operator diagnostic view, not the coordinator's default.

The default coordinator mode is `owned + depth=middle`; there is no implicit
castle-wide subscription.

### 6. Separate attention from progress

The coordinator receives two logical queues from the same journal:

#### Attention queue

Delivered at the next available coordinator turn, or through an active live
stream:

- visible middle-rat failed, exited, or became orphaned;
- workflow failed, completed, or parked awaiting a decision;
- middle-rat escalated a child obstacle or need;
- reporting boundary stopped producing updates while descendants remain live;
- budget, timeout, merge, or approval action requires coordinator input.

#### Progress rollup

Coalesced by workflow and middle-rat:

- started/running state;
- meaningful checkpoint;
- descendant counts;
- last update age;
- current next action.

Only the newest unconsumed progress summary for a subject needs to be retained
in the pending view. Attention events remain individually replayable until the
cursor advances. This keeps the coordinator informed without turning each leaf
completion into a separate context item.

### 7. Add explicit middle-rat progress reporting

Supervisor lifecycle is sufficient for start and terminal state, but it cannot
infer useful semantic progress from process liveness alone. Middle-rats should
have a small progress/checkpoint contract, exposed through a command such as:

```text
rk progress --summary "4/7 child tickets complete" \
  --next "reviewing the remaining three" \
  --status working
```

The command should write a bounded progress record associated with the current
agent generation and workflow instance. The daemon should rate-limit and
coalesce progress updates. The middle-rat prompt should require updates at
meaningful milestones and before reporting an escalation or completion.

Automatic supervisor heartbeats remain useful for liveness and stale-boundary
detection, but should not be presented as semantic progress.

### 8. Define the host delivery boundary

`rk monitor --follow` is valuable for humans and debugging, but a long-running
CLI process alone does not notify a model that is waiting for its next turn.
Rat Kingdom cannot provide a universal delivery adapter: the coordinator may be
an arbitrary external harness with no callback, prompt builder, or stable
process boundary that the daemon can control. The daemon therefore owns the
durable protocol and the host owns when to read and inject it.

The portable coordinator contract is:

1. register or reuse a stable session ID;
2. call `coordinator.pending` or `rk monitor --coordinator <id> --once` before
   a meaningful decision;
3. consume the bounded attention queue and current rollup snapshot;
4. acknowledge the returned cursor after the host has accepted the block;
5. keep user instructions as the primary interactive input.

A harness that exposes a turn-boundary hook may wrap this contract and inject a
compact block into the next model turn. Such adapters belong outside the core
daemon because their APIs and lifecycle vary by harness. A host without that
hook can still use the exact same durable read/ack protocol manually or from a
wrapper command.

The injected block should be compact and non-authoritative prose. It should
include stable IDs and commands for drill-down, for example:

```text
[RAT KINGDOM ATTENTION]
- Foreman-1 completed 4/7 child tickets; 1 child is blocked.
  Inspect: rk status Foreman-1
- Steward-2 failed during review of TKT-...
  Inspect: rk log Steward-2 --generation 1
- Workflow wf-01... is awaiting approval.
  Inspect: rk workflow status wf-01...
[ACTIVE ROLLUP]
- 2 workflows running; 3 middle-rats active; 5 leaf rats below them.
[END RAT KINGDOM ATTENTION]
```

Delivery is at-least-once. A failed or disconnected session can replay from
its cursor, and consumers deduplicate by journal sequence and subject revision.
The daemon must not silently delete an event merely because a notification
attempt was made.

`rk monitor --once` provides the portable pending read contract and
`rk monitor --follow` provides a live NDJSON stream for humans or wrappers. The
operator prime should describe both. Automatic injection is a convenience for
hosts that can support it, not a Rat Kingdom completion requirement.

## Implementation slices

### Slice 0 — contract and projection fixtures

- Document coordinator session, owned workflow, middle-rat, and leaf-rat
  terminology in the operator prompt and relevant domain docs.
- Add fixtures for a coordinator-owned workflow with a foreman, steward, and
  multiple leaf rats.
- Implement a pure supervision-tree projection that produces middle-rat
  summaries and descendant counts.
- Define the explicit reporting-boundary metadata with backward-compatible
  defaults.

Exit criterion: projection tests prove that unrelated repo work and leaf-rat
routine events are absent from the default coordinator view.

### Slice 1 — coordinator ownership and routing

- Persist the coordinator-session association on workflow instances.
- Propagate workflow ownership through agent dispatch and respawn.
- Normalize lifecycle, completion, obstacle, need, budget, and workflow events
  into routeable coordination candidates.
- Implement `local`, `rollup`, `escalate`, `terminal`, and `drilldown` policy
  decisions.
- Preserve existing directed completion messages to structural parents.

Exit criterion: a child completion reaches its middle-rat rollup, while a
middle-rat failure or explicit escalation reaches the coordinator scope.

### Slice 2 — hierarchical coordinator snapshot/replay

- Extend `CoordinatorFilter` with coordinator scope, depth, and event-class
  filters.
- Extend the protected coordinator journal with normalized middle-rat events.
- Add hierarchical snapshot DTOs with bounded summaries and ownership paths.
- Preserve subscribe-before-scan, cursor deduplication, bounded replay, and
  explicit lag/resync behavior.
- Add daemon protocol tests for owned-workflow filtering, subtree rollup,
  unrelated-work exclusion, reconnect, and generation-qualified terminal
  events.

Exit criterion: a client can reconnect with a cursor and reconstruct the
complete coordinator-owned view without polling unrelated surfaces.

### Slice 3 — coordinator CLI and pending attention

- Add `rk monitor --once` for a bounded pending read.
- Add `rk monitor --follow` for live human/diagnostic consumption.
- Add JSON output with cursor, snapshot, attention events, and rollups.
- Add drill-down flags for a workflow or middle-rat subtree.
- Keep `rk top`, `rk digest`, `rk inbox`, and `rk log` as existing operator
  surfaces.

Exit criterion: the coordinator can consume one compact command result before
making a decision and can save/resume its cursor without losing events.

### Slice 4 — semantic middle-rat progress

- Add `rk progress` or equivalent agent-facing checkpoint operation.
- Store the latest bounded progress summary per agent generation.
- Rate-limit and coalesce progress events.
- Add prompt guidance requiring middle-rats to report meaningful milestones,
  next actions, and escalations.
- Add stale-reporting-boundary detection based on liveness plus last meaningful
  progress, without confusing either with task completion.

Exit criterion: a coordinator sees useful progress for active middle-rats and
does not receive a heartbeat storm or raw transcript stream.

### Slice 5 — portable session boundary and optional adapters

- Register the coordinator session and its last acknowledged cursor.
- Add the turn-boundary pending/acknowledge protocol as a daemon RPC and CLI
  contract.
- Keep delivery failure replayable and idempotent.
- Document that harness-specific wrappers may call the contract before a model
  turn and inject the result, but are not part of the daemon.
- Update `rk prime --role operator` with the monitoring contract and drill-down
  commands.

Exit criterion: a coordinator can obtain a failed middle-rat and a completed
workflow from one bounded read, resume from its durable cursor, and leave
ordinary user interaction uninterrupted. An external harness adapter may add
automatic turn-boundary injection, but the core system does not depend on one.

### Slice 6 — rollout and operational verification

- Run the existing coordinator workflow tests alongside hierarchical routing
  tests.
- Exercise daemon restart, coordinator reconnect, journal lag, middle-rat
  crash, leaf-rat failure, rapid child completion, and multiple simultaneous
  owned workflows.
- Verify that unrelated castle activity does not enter the coordinator prompt.
- Verify that `rk inbox` and existing steward escalation notification behavior
  remain intact.
- Add operator documentation and changelog entries with cursor/recovery
  examples.

## Event and delivery invariants

1. A coordinator receives only events matching its explicit ownership scope,
   unless it requests a diagnostic repo/all-fleet view.
2. Routine descendant events terminate at the nearest reporting boundary.
3. A failed reporting boundary is itself escalated; hierarchy must not hide
   unavailable supervision.
4. Workflow and middle-rat terminal events are generation-qualified and
   idempotent.
5. Progress is bounded, rate-limited, and coalesced; attention is replayable.
6. A snapshot is authoritative for current state; an event is a state change or
   attention record, not an unbounded replacement for the snapshot.
7. Cursor advancement never makes an undelivered event unrecoverable.
8. User instructions remain separate from daemon-generated attention context.
9. Monitoring never implicitly steers, dismisses, merges, retries, or approves.
10. Full diagnostic detail remains available through existing targeted commands.

## Adversarial review checklist

The implementation should be challenged against:

1. A workflow launches two middle-rats and one unrelated rat in the same repo.
2. A leaf rat completes while its middle-rat is disconnected.
3. A middle-rat crashes after a child fails but before publishing a rollup.
4. A middle-rat respawns under the same name with a new generation.
5. Fifty leaf rats complete in a short burst.
6. A middle-rat emits progress on every tool call instead of at milestones.
7. Two owned workflows update concurrently and share the same repo.
8. The coordinator disconnects between event delivery and cursor
   acknowledgement.
9. The journal lags while a terminal event is emitted.
10. A workflow or agent predates the coordinator ownership field.
11. A user instruction arrives while urgent attention is pending.
12. A coordinator requests deep inspection of one subtree and then returns to
   the summarized default view.
13. A leaf-rat obstacle is repeatedly re-emitted by its middle-rat.
14. A completed middle-rat remains in the default view without creating an
   unbounded historical context payload.

## Recommended defaults

- Default scope: coordinator-owned workflows.
- Default depth: visible middle-rats plus aggregate descendant counts.
- Default leaf behavior: roll up; escalate only on policy or supervision
  failure.
- Default progress cadence: explicit middle-rat milestones, with daemon-side
  rate limiting and coalescing.
- Default notification priority: urgent attention first, then terminal events,
  then compact progress rollups.
- Default delivery: `rk monitor --once` or `coordinator.pending` at the host's
  discretion; `rk monitor --follow` for human observation and diagnostics.
- Default recovery: durable cursor plus fresh snapshot on lag or reconnect.

## Open decisions before implementation

1. Should reporting boundaries be declared only by workflow definitions, or may
   a coordinator promote an ad-hoc rat into a middle-rat at spawn time?
2. Which middle-rat progress fields are mandatory: summary and next action are
   the proposed minimum; a percentage is intentionally not required.
3. Should urgent attention interrupt an active coordinator turn, or always wait
   for the next turn boundary? The safer default is next boundary, with an
   explicit live-watch mode for interactive debugging.
4. How long should protected coordination history remain replayable before
   compaction, given that snapshots are the recovery path?

## Verification evidence required for completion

- Pure projection tests cover ownership, boundary selection, rollup counts,
  escalation, and unrelated-work exclusion.
- Daemon protocol tests cover snapshot/replay, live delivery, cursor recovery,
  lag/resync, deduplication, and generation changes.
- Supervisor tests cover middle-rat failure, child failure before boundary
  failure, respawn, and terminal routing.
- CLI tests cover compact human output, JSON cursor handling, `--once`,
  `--follow`, and subtree drill-down.
- The daemon/CLI boundary tests prove pending reads and acknowledgement are
  replay-safe.
- An optional host-adapter test may demonstrate: a coordinator starts a
  workflow, a middle-rat delegates leaf work, progress is summarized, a child
  problem is escalated, and completion is injected before the next model turn.
- The portable end-to-end run demonstrates the same flow with one explicit
  `rk monitor --once` read when no host adapter is available.

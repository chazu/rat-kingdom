# Coordinator view: durable snapshot and replay

Date: 2026-07-27

Status: implemented and verified

## Problem

The daemon has the state needed to answer whether work is finished, but the
coordinating session has no single, reliable read model for that answer.

- `space.watch` is a lossy broadcast. It has no initial snapshot or replay
  cursor; a lagged client receives only a missed-count warning.
- Agent lifecycle events live in the durable `Event` tuplespace, while agent
  state lives in the supervisor registry.
- Workflow instance mutations are persisted to JSON, but only terminal
  `workflow_complete` / `workflow_failed` events are emitted. Step advancement,
  approval parking, resume, and other meaningful state changes are invisible to
  a live coordinator unless it polls `workflow.status`.
- `rk top` polls several unrelated RPCs, and `rk digest` is retrospective. Both
  are useful operator views, but neither is a correctness contract for a
  coordinating session.

The result is a race-prone operator loop: a coordinator starts work, loses the
session's observation window, and must reconstruct what happened from agent
lists, workflow snapshots, raw events, the inbox, and sometimes Git state.

## Goal

Provide one coordinator-facing read seam with these guarantees:

1. A snapshot reports the current workflow and agent state immediately.
2. Each meaningful workflow state mutation emits a durable, replayable event.
3. Events have a journal-local monotonic sequence assigned at SQLite insertion;
   tuple IDs remain tuple identity, not commit order.
4. A reconnect can request events after a cursor and then continue live.
5. A lagged or overlong history produces an explicit resync signal; the client
   never has to guess whether its view is complete.
6. The first vertical slice proves one workflow end to end, including start,
   agent progress, step advancement, approval parking/resume, and terminal
   completion/failure.

## Non-goals for this slice

- Replacing raw tuples, the tuplespace reactor, `rk top`, or `rk digest`.
- Building a general event-sourcing rewrite of the daemon.
- Adding a second database or making the coordinator journal authoritative over
  workflow snapshots.
- Streaming transcript text, tool calls, or every budget tick through the
  coordinator view.
- Solving cross-castle event ordering beyond the existing tuple ID and sync
  semantics.

## Design

### Source of truth and projection

The existing SQLite-backed `Event` category remains the durable coordination
surface, but coordinator transitions also receive a protected journal row in the
same SQLite insertion transaction. The workflow snapshot remains the
authoritative current state. The coordinator view is a read projection over both:

```text
workflow mutation
      |
      +--> atomic workflow snapshot persistence
      |
      +--> protected Furniture Event/workflow_state_changed
             +--> coordinator journal row and sequence in the same SQLite transaction

coordinator.snapshot/watch
      |
      +--> current workflow and agent snapshot
      +--> protected coordinator journal after requested cursor
      +--> live coordinator feed notifications, deduplicated by sequence
```

The event carries a compact state summary rather than the entire workflow
context. The snapshot carries a compact coordinator DTO; full context and
transcripts remain behind existing status/log RPCs. An event is an
invalidation/update record, not a replacement for the snapshot.

### Event identity and payload

Workflow state changes use the `workflow_state_changed` event identity. The
payload contains:

- `instance`: workflow instance ID;
- `workflow` and `repo`;
- `revision`: a per-instance monotonic state revision;
- `reason`: the daemon mutation site (`started`, `step_updated`, `approval`,
  `terminal`, `resume`, or `state_changed`);
- `status`, `current_step`, `total_steps`, and `awaiting`;
- `active_agent`, `active_branch`, and `error` when present.

The journal's `sequence` is the cross-instance cursor. The instance's `revision`
is the causal order for one workflow. Consumers must use the sequence for
replay and the revision to discard duplicate or out-of-order state summaries.

### Coordinator RPC

Add a `coordinator.watch` request that upgrades a connection to a stream:

```json
{"method":"coordinator.watch","params":{
  "repo":"rat-kingdom",
  "instance":"wf-...",
  "after":42
}}
```

The initial response contains:

- `snapshot`: current matching workflows and agents;
- `cursor`: newest journal sequence included in the snapshot/replay boundary;
- `events`: bounded durable `workflow_state_changed` events after `after`;
- `truncated`: whether the requested history exceeded the replay cap;
- `resync_required`: whether the client must treat the snapshot as the only
  complete state and discard assumptions about omitted history.

After the response, notifications carry one event and its sequence. The server
subscribes to the live coordinator feed before reading the durable backlog, then
filters and deduplicates by sequence. This closes the scan-to-subscribe race
without making the lossy generic feed authoritative. A lagged coordinator feed
closes with `resync_required`; the client reconnects from the snapshot cursor.

This first slice requires `instance`. Repository-wide and cross-castle views are
follow-up work once the instance contract is proven.

The first client adapter will expose this as a workflow-aware watch/await path;
existing raw `rk watch` remains unchanged.

## Invariants

- Every persisted workflow mutation that changes the observable instance state
  increments `revision` and emits one bounded state event after the snapshot
  write; no-op mutations do not create a revision.
- A state event includes enough summary state for a coordinator to know whether
  the workflow is running, parked, completed, or failed without a follow-up
  poll.
- Event notification order follows journal sequence within one connection.
- Replaying an event range and then applying the current snapshot is safe and
  idempotent.
- A dropped or failed event write cannot make the snapshot report a false state;
  the snapshot endpoint remains the recovery path. Snapshot persistence and
  journal publication are deliberately ordered and failure-visible, rather than
  pretending a file write and SQLite insert are one transaction.
- Workflow instances created before the revision field existed load with
  revision zero and continue emitting valid revisions.
- Agent details are represented only by the instance's compact workflow summary
  in this slice; full agent lifecycle correlation is follow-up work.

## Adversarial review checklist

The design must be challenged against:

1. An event written between the live subscription and durable backlog scan.
2. A client reconnecting with a cursor older than the bounded replay window.
3. A daemon restart between snapshot persistence and journal publication.
4. Two rapid workflow updates producing duplicate or reordered notifications.
5. A workflow whose context contains a large result or secret-like payload.
6. A client filtering by instance while unrelated events are arriving.
7. Existing reactor triggers observing the new event identity.
8. Old persisted workflow snapshots with no `revision` field.
9. A live feed lag or connection drop while a terminal event is emitted.
10. A normal `space.take` or targeted delete attempting to remove coordinator
    history.

## Adversarial review and dispositions

Review date: 2026-07-27. The adversarial pass found the initial design unsafe to
land as written. The following changes are load-bearing:

- **ULID cursor was rejected.** ULID random suffixes do not establish SQLite
  commit order under concurrent writes. The coordinator journal now assigns an
  `AUTOINCREMENT` sequence at insertion and replays by that sequence.
- **Ordinary Event tuples were rejected as a journal.** Coordinator events are
  written as `Furniture` and copied into a protected journal table; `take` and
  targeted tuple deletion cannot erase replay history.
- **Broad event replay was rejected.** The first contract is instance-required
  and journals only `workflow_state_changed`; generic agent/ticket/PR events
  remain outside this projection until they carry stable workflow correlation.
- **Unbounded DTOs were rejected.** Snapshot and event summaries omit full agent
  results and workflow context; detailed diagnostics stay on `workflow.status`
  and `rk log`.
- **Concurrent publication was tightened.** Workflow instance mutation,
  snapshot persistence, and coordinator event publication are serialized per
  engine mutex. Revision increments occur only when the observable snapshot
  actually changes, and clients reject stale revisions.
- **Terminal duplication was rejected.** `workflow_state_changed` with
  `status=completed|failed` is the coordinator terminal transition. Legacy
  `workflow_complete`/`workflow_failed` events remain for existing reactor and
  digest consumers but are not part of this stream.
- **Lag handling was tightened.** A lagged coordinator stream closes with an
  explicit resync signal; the CLI obtains a fresh snapshot and reconnects from
  its journal cursor.
- **Restart idempotence remains out of scope.** The projection reports the
  daemon's persisted state; it does not claim to make interrupted workflow side
  effects exactly-once. That needs a separate execution journal.

Review findings and dispositions are recorded below before implementation is
considered complete.

## Implementation slices

### Slice 1 — observable workflow state (implemented)

- Add backward-compatible `revision` to `Instance`.
- Centralize state-event emission behind `WorkflowEngine::store/update`.
- Emit compact workflow state summaries, including start and terminal events.
- Add unit tests for revision monotonicity and summary payload shape.

### Slice 2 — durable snapshot/replay seam (implemented)

- Add a journal-local SQLite sequence and protected coordinator event rows.
- Add instance-scoped coordinator snapshot filtering and bounded event replay in
  the daemon.
- Add `coordinator.watch` stream setup with subscribe-before-scan ordering and
  journal-sequence deduplication.
- Add protocol/client types only where they reduce caller knowledge; retain raw
  JSON compatibility at the daemon seam.
- Add integration tests for backlog replay, live delivery, filters, truncation,
  and reconnect behavior.

### Slice 3 — coordinator-facing command (implemented)

- Add a workflow-aware watch/await CLI path that prints state changes and exits
  on completed/failed terminal state.
- Document the operator/coordinator flow in `rk prime` and the changelog.
- Keep `rk top`, `rk digest`, and `rk watch` as existing views over the same
  daemon state until follow-up work can migrate them.

## Verification evidence

- Focused daemon protocol and workflow tests pass.
- `cargo clippy -p rk-space -p rk-daemon -p rk-cli --all-targets -- -D warnings`
  passes.
- The daemon protocol test covers durable replay plus live delivery from a
  journal cursor; the space test covers restart persistence and non-consumption.
- Full workspace verification remains the final handoff gate for this change.

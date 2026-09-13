# S2 contract: native finalized-cost / physical-exit observations and the
# corrected bounded export envelope

Date: 2026-09-13. Ticket: `TKT-dorod-sival-fumid` (continuation of S2,
`TKT-tapip-puhot-sitih`). Program: `TKT-kuzud-gatag-dipom`.

This document is the **contract only**. It is published so the reporter
correction `TKT-nonub-pugar-pilid` can consume exact field names and, more
importantly, exact *semantics*, without waiting on the producer code. The
producer implementation described in sections 2 and 3 is **NOT IMPLEMENTED** on
this branch — see "Delivery status" at the end. Do not treat any field below as
observable until a producer commit lands.

Source of truth read while writing this: `crates/rk-daemon/src/supervisor.rs`
(`Supervisor::handle_event`), `crates/rk-harness/src/lib.rs`
(`HarnessEvent`), `crates/rk-daemon/src/bbs.rs` (`export`, `record_exposure`,
`record_open`), `crates/rk-space/src/store.rs` (`persistence_page`,
`commit_sequences`), and
`/Users/chazu/.codex/artifacts/rk-stigmergy-20260913T022353Z/claude-cost-semantics.md`.

## 1. Why two new observations are needed at all

Directly observed on the live castle and recorded in the S2 ticket: Gusteau-14's
completion tuple `01M2CBAAR1JMRGBV5Z7ES013M3` carries `cost_usd=14.802756`,
while that same generation's later native status carries `cost_usd=7.3380432`;
its PID 84822 was still alive after the record read `completed`.

Two distinct facts follow, and the report must not conflate them:

1. **`harness_result` is task-completion evidence, not final-cost evidence.**
   `rk done` routes a completion while the provider may still report a
   different, later total for the same query. A report that sums or trusts the
   `harness_result` cost is reading a provisional number.
2. **`harness_result` is not proof of physical exit.** The `Completed` branch of
   `handle_event` updates the registry and returns; the OS process is still
   alive until `HarnessEvent::Exited` arrives. Active-execution duration cannot
   be derived from completion time.

Neither fact can be repaired by reading exposure records: exposure means
*prepared*, and says nothing about launch, cost or exit. It must come from the
lifecycle producers themselves.

## 2. Identity binding (applies to both new records)

The supervisor's per-event identity arguments are already exactly the right
ones and must be used verbatim rather than re-derived:

| field | source | changes on respawn? |
| --- | --- | --- |
| `agent` | `name` | no |
| `spawn` | `spawn: SpawnId` (`AgentRecord::spawn_id`) | **no** — a manual respawn CONTINUES the same generation |
| `session` | `session: SpawnId` (`Supervisor::session_tokens`) | **yes** — one per physical process launch |
| `repo` | `record.repo_name` | no |
| `task` | `record.task` | no |
| `provider_session` | `HarnessEvent::Completed.session_id` / `Started.session_id` | yes, and also on a provider-side reset |

`spawn` alone therefore **cannot** disambiguate two attempts of one generation.
Any join, dedup or aggregation key must be the pair `(spawn, session)`. This is
the single most important line in this document: aggregating by agent name, or
by `SpawnId` alone, double-counts a respawned generation.

`session` (native launch token) and `provider_session` (the harness's own
session id) are **different identities with different lifetimes** and are
recorded as separate fields. A provider reset mints a new `provider_session`
under an unchanged `session`; a respawn mints a new `session` and normally a new
`provider_session`. Neither may be substituted for the other.

Stale-session fencing: the `Exited` handler already fences recovery state on
`self.lock_session_tokens().get(name) == Some(&session)`. The new observations
are **deliberately not** fenced that way — a late event from a superseded
session is still a true fact about *that* session, and the record carries the
`session` it belongs to, so the report can order and attribute it. What must
never happen is a stale event writing over the *active* generation's registry
state; that existing fence stays exactly as it is.

## 3. The two record kinds

Both are daemon-authored, immutable `Furniture` `Event` tuples authored by the
castle, in the agent's repo scope — the same shape and authorization seam as
the existing `exposure`/`open` records, and covered by the same
`RESERVED_IDENTITY_PREFIXES` refusal so an agent caller cannot mint one.
Both join `rk_core::bbs::is_telemetry`, so neither can ever surface in a peer
briefing or a `bbs show` thread.

### 3.1 `bbs_kind: "agent_final_usage"` — identity `bbs-agent-final-usage`

Authored from the **`HarnessEvent::Completed`** arm of `handle_event`, at every
result path that arm can take (the `Stopped`/`Completed`-via-`reconcile_task_done`
early-return merge path, the `Paused` withheld-turn path, and the published
completion path). Emitting at all three is what makes a done-before-final-result
sequence observable: the `rk done` completion and the later provider total are
two separate records for one `(spawn, session)`.

```
schema_version: 1
bbs_kind:       "agent_final_usage"
repo, task, agent, spawn, session, provider_session
observed_at:    RFC3339, when the daemon saw the event
state:          the AgentState this event resolved to (completed|paused|failed|stopped)
declared_done:  bool — whether this generation wrote its own `rk done`
cost_usd:       number | null
cost_basis:     "provider_reported_segment_total" | "daemon_priced_increments" | "unknown"
cost_provenance: free text naming the field/path the number came from
usage:          the TokenUsage as reported, or null
```

**Cost rules, taken from the checked provider documentation** (see the artifact
above; primary source https://code.claude.com/docs/en/agent-sdk/cost-tracking):

- `total_cost_usd` on a streaming result is **cumulative within one query
  call**. Repeated results for one query must **not** be summed. Evaluate the
  **last reported total per proven segment**.
- A reset starts a **new segment with a new `session_id`**. Separate resumed
  query calls report independently. The trial issues no reset commands, but the
  report must still key segments on `provider_session` rather than assume one.
- These are **client-side estimates, not billed charges**. Every surfaced
  number is labelled a *reported cost estimate*. The report must not present it
  as spend.
- `usage` and `total_cost_usd` have different scopes; do not derive one from the
  other.
- When the provider supplies no final usage, `cost_usd` is `null` and
  `cost_basis` is `"unknown"`. **Do not invent a total.** A null here is the
  honest answer and the report lists it under unknown coverage, not as zero.
- `cost_basis: "daemon_priced_increments"` marks the fallback the supervisor
  already applies for harnesses that do not self-report USD (the
  `HarnessEvent::Usage` arm multiplies `TokenUsage` by `self.pricing`). It is a
  daemon estimate of an estimate and must be reported separately from a
  provider-reported total, never pooled with it.

### 3.2 `bbs_kind: "agent_exit"` — identity `bbs-agent-exit`

Authored from the **`HarnessEvent::Exited`** arm, which the harness contract
guarantees is the final event for a launch.

```
schema_version: 1
bbs_kind:      "agent_exit"
repo, task, agent, spawn, session, provider_session
exited_at:     RFC3339, when the daemon saw the exit
exit_code:     int | null   (null = signal-terminated)
crashed:       bool — the record's `crashed` marker after this event
prior_state:   the AgentState the record held before this event
launched_at:   RFC3339 | null — echoed from the launch event for this session
```

`launched_at` is already emitted additively on `agent_spawned`/`agent_respawned`
by commit `48fb2da`. Echoing it here lets the report compute **active execution
duration = `exited_at` - `launched_at`** for one `(spawn, session)` without
joining across event kinds, and — critically — keeps that duration separate
from queueing and verification-admission time, which the required results list
demands be reported separately.

Completion time is **not** exit time. `exited_at` is the only physical-exit
fact; a report claiming author-terminal reuse must join on `agent_exit`, never
on a `harness_result`.

### 3.3 Failure behavior

Identical to the existing exposure/open capture and non-negotiable: a capture
failure is written as a `telemetry_gap` and **returns**, never raises. No
observation may change completion, delivery, routing, budget or state
behavior. If `write_telemetry` fails, the generation still completes, still
routes, still exits. The gap makes the absence known-missing rather than
known-negative.

## 4. Corrections to the existing bounded export

These are operator-review findings against `crates/rk-daemon/src/bbs.rs:627`
and `crates/rk-space/src/store.rs:1063` as they stand on `48fb2da`. All five
are **open**; none is implemented on this branch.

1. **Order enum mismatch.** The exporter emits
   `order: "tuple_persistence_events.commit_sequence ascending"`. The accepted
   S3 capture contract uses **`"persistence_sequence"`**. The wire value must
   become the contract's enum; the SQL-level provenance
   (`tuple_persistence_events.commit_sequence ascending`) is retained in a
   separate `order_provenance` field. A consumer keys on `order`; implementation
   detail must not be load-bearing on the wire.
2. **No caller-pinned snapshot boundary.** `ExportParams` accepts only
   `after`/`limit` and captures a **new** boundary on every call
   (`persistence_page` re-reads `latest_persistence_sequence`). Paging is
   therefore not a snapshot: rows written between page 1 and page 2 appear.
   `ExportParams` must accept an optional caller-supplied `boundary`, page
   against exactly that value, and **reject** a boundary that is invalid or
   ahead of the store's current sequence. Required tests: a write between two
   pages does not appear in page 2; a future boundary is refused.
3. **References are not fenced to the boundary.** References resolve through
   `Space::get`, which reads the *current* row, so a record persisted **after**
   the frozen boundary can leak into an earlier capture. References must be
   resolved as of the frozen boundary, or reported as missing/unknown at that
   boundary. A capture that silently includes post-boundary state is not a
   snapshot.
4. **Reference closure is one hop only.** A receipt's referenced finding may
   itself reference evidence outside the root page. `coverage.complete`
   currently ignores that unresolved second hop and can assert `true` while
   nested evidence is missing. Traverse `source`/`evidence` links under an
   **explicit finite budget**, or enumerate the unresolved references. Never
   assert `complete` while silently missing nested evidence. `complete` is a
   claim the report relies on; an over-broad `true` is worse than a `false`.
5. **Bounding discipline stands.** Scope, cursor and limit stay pushed into SQL
   before payload deserialization, and the export stays scope-fenced (a
   reference into another repo is reported, never exported). The existing
   unit tests do **not** substitute for the required real-CLI multi-page
   fixed-boundary test, nor for a native-record report replay.

## 5. What the report may and may not conclude

- An `exposure` is *prepared*, an `open` is *requested*. Neither is delivery to
  a model, reading, comprehension or benefit.
- A prepared exposure whose spawn later failed to launch is **not** an active
  consumer opportunity. Join `agent_spawned`/`agent_exit` before counting it.
- Author-terminal reuse requires an `agent_exit` record for the source author's
  `(spawn, session)`. A completion is not an exit.
- Cost is a **reported estimate**, per segment, last-total-wins, never summed
  across results of one query, never pooled across `cost_basis` values.
- Missing evidence is listed as unknown coverage with its record IDs. It is
  never rendered as a zero.

## Delivery status (honest)

Implemented and committed previously, on `rat/scurry-15/tkt-tapip-puhot-sitih`
(`48fb2da`, `b16925b`, `1e35d20`), with 14 passing daemon unit tests:
exposure/open capture at all four surfaces, nonfatal capture status,
`telemetry_gap`, the reserved-identity refusal, bounded `bbs.export` /
`rk bbs export`, and the additive `spawn`/`task`/`role`/`launched_at` fields on
the spawn/respawn/lifecycle events.

**Not implemented — this document is contract only:**

- Sections 3.1 and 3.2: neither `agent_final_usage` nor `agent_exit` exists in
  code. No producer was added.
- Section 4, items 1-4: all four export corrections are open.
- The real-CLI / fake-harness integration tests required by S2 acceptance —
  all four surfaces with exact IDs/reasons/omissions/consumer generations,
  empty-versus-missing capture, nonfatal capture failure, wrong-repo and forged
  telemetry refusal, no metadata in peer briefings, and bounded multi-page
  fixed-boundary export — are **not written**. The existing 3 unit tests from
  `b16925b` are a starting point, not acceptance.
- No native cost/exit record is in the export envelope yet, because no such
  record exists.

S2 is therefore **not deliverable** on the strength of this commit. A further
continuation is required.

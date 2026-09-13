# S2 native observation and bounded export contract

Ticket `TKT-dorod-sival-fumid` (S2 continuation of `TKT-tapip-puhot-sitih`).
Consumer: the evaluator correction `TKT-nonub-pugar-pilid`, which owns
evaluation — these are the producers only.

## Identity: join on `(spawn, session)`

| field | source | changes on respawn? |
| --- | --- | --- |
| `spawn` | `AgentRecord::spawn_id` | **no** — a respawn CONTINUES the generation |
| `session` | the supervisor's per-launch token | **yes** — one per physical launch |
| `provider_session` | the harness's own session id | yes, and on a provider reset |

`spawn` alone **cannot** identify an attempt: two launches of one generation
share it. Every join, dedup and aggregation key is the pair `(spawn, session)`.
`provider_session` is a third identity with its own lifetime and is never a
substitute for either. All three are emitted on `agent_spawned`/
`agent_respawned` as well as on the observations, so a launch joins its own
records; a path that registers no session emits `session: null` rather than
inventing one. `provider_session` is learned only at `Started`, which has not
fired yet at spawn/respawn time — `agent_spawned`/`agent_respawned` therefore
always emit `provider_session: null` at launch, an explicit "not yet known"
rather than an omitted field or a value borrowed from a previous launch.

Every launch path publishes `agent_respawned` (or `agent_spawned`) with this
identity, INCLUDING `continue_recovery`'s managed continuation of a
post-commit `RecoveryRecord` — previously the one launch path that tracked a
session (so cost/exit observations attributed correctly) without ever
publishing it, leaving a managed recovery launch joinless from lifecycle
evidence alone.

Attribution is frozen at launch, keyed by session token. A delayed event from a
superseded launch is attributed to the launch it came from, or skipped — never
to whichever record holds the name now.

Identity is not the only thing the name-keyed `AgentRecord` would leak.
Lifecycle state lives there too, so anything read from it is **omitted** on a
superseded launch's late event rather than borrowed from the successor: both
kinds carry `stale_session: true`, and `state`/`declared_done` (result) and
`prior_state`/`crashed` (exit) are `null`. `null` means unknown, never a value
of its own. The event's own provider total is still that launch's and survives;
the daemon's running priced total is the successor's and does not, so a stale
result with no provider figure is `cost_basis: unknown`, not a daemon-priced
final cost.

The same fence also gates `handle_event`'s effects on the LIVE `AgentRecord`
and adjacent supervisor state, not just what these two observations report.
Session-fenced telemetry had previously shipped decoupled from unfenced
lifecycle mutation: a stale predecessor's `Started`/`Usage`/`Completed`/
`Exited` could still resume a paused successor, overwrite its
`session_id`/`transport_outage`/`recovery`, add to its `usage`/`cost_usd` and
consume its budget floor, claim or route a completion on its behalf, fail it
outright, cancel its in-flight managed verification (the generation-stable
`spawn` id a respawn keeps cannot tell the two launches apart; only the
session token can), or falsely flush/route its withheld turn. The chattier
events (`AssistantText`/`ToolUse`/`Retry`/`Stderr`) and the two side-channel
ones (`TransportFailure`/`ControlDelivered`) had the same gap against the
successor's liveness fingerprint, `stderr_tail`, `transport_outage`, and
durable control acknowledgement. All of the above are now gated on the same
per-call `live` check (current session token for `name` == this event's own
token; an unclaimed name with no registered token yet is treated as live, not
stale, so this never engages before a first launch's `track_session` call)
while per-launch telemetry and the generation-bound transcript log — both
already keyed on the event's own launch — are unaffected.

## `agent_final_usage` (identity `bbs-agent-final-usage`)

Authored from the fenced `Completed` handler at **every** result path,
including the early-return one taken when the disposition was already decided.
That path is the done-before-final-result case: without it the only surviving
cost record is the provisional one routed at `rk done`.

Fields: `repo`, `task`, `agent`, `spawn`, `session`, `provider_session`,
`observed_at`, `state`, `declared_done`, `stale_session`, `cost_usd`,
`cost_basis`, `cost_provenance`, `usage`.

`cost_basis` distinguishes three genuinely different situations:

- `provider_reported_segment_total` — this result carried a provider total.
  Cumulative **within its query**: take the last per segment, never the sum.
- `unknown` — no total on this result, but an earlier one in the same launch
  had one. The running figure mixes bases, so no final launch cost is provable
  and `cost_usd` is null.
- `daemon_priced_increments` — the provider never reported USD for this launch
  and the daemon priced `TokenUsage` itself. A weaker estimate; never pooled
  with a provider total. `AgentRecord.cost_usd` is generation-cumulative (a
  same-generation respawn inherits it), so this is never the raw cumulative
  figure: each launch freezes its own `cost_usd` baseline at `begin_launch`,
  and only the delta accrued since that baseline is reported — otherwise a
  provider total from an earlier launch of the same generation would leak
  into a later, unrelated launch's daemon-priced estimate. A non-positive
  delta is `unknown`, not a manufactured zero-or-negative final cost.

All values are **client-side estimates, not billed charges**. An unprovable
total stays null; it is never manufactured as zero. The same provider session
alone does not prove two queries are one cumulative total.

## `agent_exit` (identity `bbs-agent-exit`)

Authored from the fenced `Exited` handler. Completion is **not** physical exit:
the `Completed` handler returns while the process is still alive.

Fields: the identity block plus `exited_at`, `exit_code` (null = signal, which
is not exit 0), `crashed`, `prior_state`, `launched_at`, `cost_coverage`,
`stale_session`, `duration_semantics`.

`cost_coverage` is `final` (a result, nothing after it), `partial_unknown` (a
result, then more model usage, then an end with no later result — the known
total is partial and the rest stays **unknown**), `none` (no result was ever
reported) or `unknown` (no attribution survives; "we know there was none" and
"we cannot say" are different findings). Finality is never inferred from merely
finding some result before an exit.

`launched_at`→`exited_at` is **process lifetime, not active model work**: a
harness can sit paused on a verification run or on the operator. No claim is
made about how much of the span was productive.

## Failure behaviour

Both kinds are daemon-authored immutable `Furniture` Events joining
`rk_core::bbs::is_telemetry`, so neither reaches a briefing or a `bbs show`
thread, and an agent caller cannot mint one. A capture failure is logged and
recorded as a `telemetry_gap`; it never raises and never touches completion,
delivery, state or budget behaviour.

## Bounded export

`bbs.export` / `rk bbs export --repo` emits `order: "persistence_sequence"` —
the S3 capture contract's enum. The SQL detail lives in `order_provenance` so
the wire value survives a column rename. A consumer that does not see exactly
that `order` must treat ordering as unknown.

- `boundary` pins one snapshot across pages. Without it each page captures a
  fresh boundary and a concurrent write appears mid-paging. A boundary ahead of
  the store is **refused, not clamped**.
- References resolve at the frozen boundary via `Store::get_as_of`, not through
  the live row, so a post-boundary tuple cannot leak into an earlier snapshot;
  absent-at-boundary is reported under `missing_references`.
- `get_as_of(id, boundary, scope)` fences `scope` into the same SQL predicate
  as `id` and the sequence bound, driven by `idx_tuple_persistence_scope_id
  (scope, id, commit_sequence)`: a bounded index seek regardless of journal
  size, and a foreign-scope row is never read off disk to be checked in Rust
  afterward, let alone deserialized. It returns the matched journal row's own
  `commit_sequence` alongside the tuple; the export attaches THAT sequence to
  the reference, never the live `tuples` row's — which can be absent (a
  deletion made after the boundary leaves nothing for a live-row lookup to
  find) even though the immutable journal still proves the reference's exact
  historical order.
- Reference closure traverses nested `source`/`evidence` to a finite depth.
  `coverage.complete` is false whenever **any** hop is unresolved, the page
  truncated, or the budget was exhausted.
- Scope, cursor and limit are pushed into SQL before deserialization, and a
  foreign-scope reference is reported, never exported.

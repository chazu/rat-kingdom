# S2 native observation and bounded export contract

Ticket `TKT-dorod-sival-fumid` continues `TKT-tapip-puhot-sitih`. These producers supply
the evaluator owned by `TKT-nonub-pugar-pilid`.

## Identity and lifecycle ownership

Join attempts on `(spawn, session)`. `spawn` is `AgentRecord::spawn_id` and survives a
respawn; `session` is a fresh supervisor token for each physical launch.
`provider_session` is the harness identity, which may also change on provider reset. It
cannot substitute for either native identity or prove two queries share one cumulative
cost total.

Initial spawn, ordinary respawn and managed recovery continuation publish their launch
identity in `agent_spawned` or `agent_respawned`. Provider identity is learned at
`Started`, so the launch event explicitly carries `provider_session: null`. An
unobservable attach launch publishes null session and launch time; it never borrows a
predecessor watch.

Attribution is frozen per launch. Delayed predecessor events retain their own identity
and provider totals. Record-derived fields describe the successor and therefore become
null: result `state`/`declared_done`, exit `prior_state`/`crashed`, and unprovable
daemon-priced cost. Both observations mark `stale_session: true`. Null always means
unknown.

Ownership checking and mutation share a held session-token guard. Launch publication
changes the record, control, token and completion bookkeeping under that same guard.
Late predecessor events cannot resume, fail or overwrite the successor, charge its
budget, change liveness/stderr/recovery/transport state, cancel its verification,
acknowledge its control message, claim completion or flush/route a withheld turn.
Per-launch telemetry and the generation-bound transcript remain attributed to their
originating event.

Attach takeover replaces the old token with a watch-less token rather than deleting it.
An absent entry is treated as owned for an unregistered event context; it must never let
a superseded headless launch regain ownership. Lock order and callback constraints are
documented at `Supervisor::own` and `publish_launch`.

## Final usage: `bbs-agent-final-usage`

The fenced `Completed` handler emits `agent_final_usage` on every result path, including
a result arriving after `rk done` has already settled disposition. Fields: `repo`,
`task`, `agent`, `spawn`, `session`, `provider_session`, `observed_at`, `state`,
`declared_done`, `stale_session`, `cost_usd`, `cost_basis`, `cost_provenance`, `usage`.

`provider_reported_segment_total` means this result carried a provider total. It is
cumulative within its query: use the last total for each segment rather than summing
repeated totals. A later result without USD after a provider-priced result in that
launch has `cost_basis: unknown` and null cost; mixed bases do not establish final cost.

`daemon_priced_increments` applies only when that launch never reported USD. The
generation-wide record accumulates across respawns, so `begin_launch` freezes a baseline
and only a provable positive delta belongs to this launch. A non-positive delta is
unknown, not an invented zero or negative total. Stale events cannot borrow the
successor's running total. All costs are client estimates, not billed charges.

## Physical exit: `bbs-agent-exit`

The fenced `Exited` handler emits `agent_exit`. Completion is separate from physical
process exit. Fields include the identity block, `exited_at`, `exit_code`, `crashed`,
`prior_state`, `launched_at`, `cost_coverage`, `stale_session`, `duration_semantics`. A
null exit code denotes a signal, not success.

Cost coverage is `final` when a result has no later work; `partial_unknown` when more
model usage follows the last result before exit; `none` when no result was reported; or
`unknown` when attribution is missing. Finding some earlier result alone does not
establish finality. Launch-to-exit duration is process lifetime, never measured active
work: the harness can be paused for checks or the operator.

## Capture failure and authorization

Both kinds are daemon-authored immutable Furniture Events registered by
`rk_core::bbs::is_telemetry`. They cannot be minted by workers and never enter briefings
or `bbs show` threads. Failed observation capture is logged and recorded as
`telemetry_gap`; it cannot change completion, delivery, state or budget behavior.

## Bounded historical export

`bbs.export` / `rk bbs export --repo` emits `order: "persistence_sequence"`; SQL
implementation detail belongs in `order_provenance`. Other order values establish no
known ordering.

`boundary` pins one snapshot across pages. A future boundary is refused, never clamped.
References use `Store::get_as_of(id, boundary, scope)` so post-boundary changes cannot
leak into the snapshot; absent references appear in `missing_references`.

The SQL predicate fences scope, ID and sequence using `idx_tuple_persistence_scope_id
(scope, id, commit_sequence)`. Resolution is a bounded index seek before
deserialization, including for foreign references. Each exported reference carries its
matched journal row's commit sequence, not the current live row's. Historical references
remain resolvable after a later deletion.

Nested source/evidence traversal has finite depth and budget. `coverage.complete` is
false for any unresolved hop, truncation or exhausted budget. Scope, cursor and limits
apply in SQL before deserialization; foreign-scope references are reported but never
exported.

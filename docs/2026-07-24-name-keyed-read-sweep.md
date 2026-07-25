# Sweep: unbounded reads keyed on an agent name over a durable category

**Ticket:** TKT-159 (sub of TKT-148) · **Date:** 2026-07-24 · **Rat:** Cinder-2

## Why

TKT-146 (`afaa8a0`) established that an **unbounded blocking read keyed on an
agent NAME over a DURABLE tuple category** is a bug class, not a one-off.
`Store::query` orders `id ASC` and `Space::blocking_read` passes `LIMIT 1`, so
such a read returns the **oldest** namesake tuple and can silently satisfy on a
stranger's record. It killed live rats one second into their tasks and made
`nightly-self-improve` a silent no-op.

TKT-146 fixed three sites. TKT-148 flagged that a fourth might exist. This is
that hunt.

Exposure is reduced but not eliminated: `reserve_name` no longer recycles
archived names, so no NEW collisions arise, but **24 agent names already carry
two generations each** (measured 2026-07-24, groom Colby-2) — exactly the input
that makes such a read return the wrong record today.

## What counts as a site

1. filters on an agent name (payload `"agent":"<name>"`, or `identity` /
   `instance` set to a name), AND
2. targets a durable category (`harness_result` and friends — not an evaporating
   trail: `claim` / `obstacle` / `need` / `resolution`), AND
3. carries no `after_id` lower bound.

## Method

Enumerated every read against the space in production code (tests excluded) and
classified each by what it keys on and whether it is bounded:

- every `Pattern` construction: `Pattern::category` / `::default` / `.identity()`
  / `.identity =` / `.instance` / `payload_search`
- every read: `Space::scan`, `scan_hot`, `rd`, `take`, and `Store::query` /
  `query_ranked` / `newest_trail`
- every `.pop()` / `.into_iter().next()` / `.first()` single-pick over a scan
  (an oldest-first pick has the same hazard as `LIMIT 1`)
- the CUE side: every `read` step and reactor trigger `match.search` in
  `examples/workflows/*.cue` and `examples/triggers.cue`
- the non-tuple durable stores keyed by name: `Registry` (`agents.json` +
  `agents-archive.json`) and `AgentLog` (`agent-logs/<name>.jsonl`)

## Result: no fourth independent call site

Exactly three reads key on an agent name, all already bounded by TKT-146:

| Read | Site | Bound |
| --- | --- | --- |
| workflow `wait` | `workflow_exec.rs` `result_pattern` | agent record `created_at` |
| workflow `wait_all` | same seam (`join` → `result_pattern`) | same |
| attach completion watcher | `supervisor.rs` `task_done` | agent record `created_at` |

Everything else that touches an agent name is out of the class, for a reason
worth recording so a future sweep does not re-derive it:

- **`Supervisor` has only two space reads at all** — the `task_done` watcher
  above, and a `Convention` scan keyed on scope. No rollup, `dismiss`, `land`,
  budget, or liveness path reads a tuple by name.
- **`reactor.rs` steer guard** (`Pattern::category(Message).identity(&wall.instance)`)
  keys `identity` on an agent name over a durable category, but its
  `payload_search` pins the unique obstacle tuple id, and it is an emptiness
  check rather than a single-pick. Not satisfiable by a namesake.
- **`rk endorse` idempotency check** keys `instance` on the agent name over the
  durable `Endorsement` category. Bounded in effect: `identity` is a unique
  `sug-…` id, and quorum counts DISTINCT `instance`, so a namesake's endorsement
  of the same suggestion is correctly a no-op rather than a wrong match. Noted
  as a naming-policy consequence (one vote per name), not a stale-read bug.
- **Evaporating trails** (`claim` / `obstacle` / `need` / `resolution`) do key
  `instance` on an agent name, but they are out of the class by definition, and
  `Store::newest_trail` (the reinforcement lookup) is `ORDER BY id DESC` —
  newest-wins, not oldest.
- **`Step::Read`** takes the NEWEST match (`scan` is oldest-first, the step
  `.pop()`s the tail) and its blocking fallback only runs when the scan found
  nothing. Its `search` is workflow-authored, and no shipped workflow keys one
  on an agent name.
- **Ticket, workflow-instance, PR and reactor-marker reads** key on ids that are
  unique and never recycled (`TKT-n`, `wf-…`, `sug-…`, a branch name, a tuple
  id), so no generation ambiguity exists.
- **`Registry::get_any`** resolves a name to the live record, else
  `max_by_key(created_at)` over the generation-keyed archive — newest-wins,
  correctly bounded.
- **`rk rd` / `rk in`** pass an operator-supplied pattern through the RPC. No
  bound can be inferred there; it is a human at a prompt, not a machine wait.

## What the sweep DID find: an unbounded path inside the fixed seam

`result_pattern` applied its bound only when the agent's registry record was
reachable, and **degraded to an unbounded read otherwise**:

```rust
match self.supervisor.status(agent) {
    Some(record) => pattern.after(Some(RecordId::floor_at(record.created_at))),
    None => {
        warn!(agent, "no record for waited-on agent; wait is unbounded");
        pattern            // <-- the TKT-146 defect, still live on this path
    }
}
```

The comment called it unreachable, and it is hard to reach (no production caller
of `Registry::remove`; archiving is covered by `get_any`'s archive fallback).
But "hard to reach" is not "bounded", and the reachable-in-principle paths are
real: a resumed instance (TKT-52) whose registry file was replaced under it, or
a hand-edited `agents.json`. A `wait` that takes that branch is the exact defect
TKT-146 fixed, on a run with the duplicated generations already in the space.

### Fix

The bound is now unconditional, ordered by how tight a bound each source gives
and with **no representable unbounded result** (`generation_floor_of`):

1. the agent record's `created_at` — exact, the normal case;
2. else the **workflow instance's `started_at`** — every agent a `wait` /
   `wait_all` blocks on was spawned by that instance (`ctx.active_agent` is only
   set by `spawn`, `ctx.fanout` only by `for_each`), so its `harness_result`
   cannot predate the run. Looser than the record, still sound;
3. else `now` — admits no older namesake. Cost is a wait that times out if the
   result already landed. Fail toward waiting, never toward a stranger's record.

### Structural guard

Both surviving name-keyed reads now go through one constructor,
`rk_core::tuple::Pattern::for_agent_since(category, identity, agent, since)`,
which takes `since` as a required argument and applies `RecordId::floor_at`
itself. The unbounded form is unrepresentable through it, and the lesson lives
in one doc comment. This is the answer to the ticket's "a new site should reuse
the helper, not invent a second idiom": there is now a single idiom, and it is
the safe one.

## Tests

- `rk-core` `tuple.rs`: `for_agent_since` rejects a namesake predecessor's
  tuple, still discriminates by agent and identity, and does not match a name
  prefix (`Whisker` vs `Whisker-2` — the TKT-102 generation suffixes make this
  a live concern).
- `rk-daemon` `workflow_exec.rs`: `generation_floor_is_never_unbounded` asserts
  each arm returns a floor that excludes a two-day-old namesake tuple —
  reverting the fallback to unbounded reinstates the TKT-146 defect and fails
  this test; `instance_start_fallback_still_admits_this_generations_result`
  proves the looser bound is not so tight it misses the tuple being waited for.
- The TKT-146 e2e (`crates/rk-daemon/tests/workflow_stale_result.rs`) continues
  to cover the record-present path end to end.

## Handed off, not fixed

- **`AgentLog` is not generation-aware.** `crates/rk-daemon/src/agent_log.rs`
  `path_for` keys `<agent>.jsonl` on the name alone, so `rk log` for any of the
  24 duplicated names interleaves two unrelated rats' transcripts. Same class,
  durable store, no bound — but it is a file path rather than a tuple query and
  is already assigned to **TKT-158**. Confirmed still unfixed; not touched here.
- **The steward's verdict read is unbounded across instances.** `steward.cue`
  step 4 reads `category: artifact, identity: "review"` scoped to the repo with
  no per-instance discriminator. Newest-wins mitigates staleness but does not
  make it correct: with two stewards in flight, the newest `review` artifact in
  the repo may belong to the other one's reviewer, and the verdict routes a
  merge. Not agent-name-keyed, so outside this ticket's class — filed as
  **TKT-161**. `reviewer-drives-rework.cue:91` has the identical read.

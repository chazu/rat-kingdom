# TKT-01M03ZXR2X84H9JZH505W6H97Z: steward-on-completion "inconsistent fires" — diagnosis

## The report

Across six base-chained rework completions on 2026-08-15, the reporting rat
observed the `steward-on-completion` trigger firing an auto-steward review for
three (Gadget-5, Noodle-5, Whisker-7) and not for the other three (Wriggle-5,
Peanut-5, Django-6), despite all six being repo-scoped `rat-kingdom` rats with
role `"rat"` and real branches — a set that should uniformly match the
trigger's predicate (`category: event, identity: harness_result, search:
"role":"rat"`).

## What the tuplespace actually shows

For each of the six named agents, `rk scan event rat-kingdom --search
"\"agent\":\"<name>\""` returns exactly one `harness_result` tuple, and for
each of those six tuple ids there is exactly one `reactor_fired` marker
(`system` scope, `key: steward-on-completion@<tuple id>`) carrying a real
workflow instance id — never the `"queued"` placeholder `enqueue_fire` writes,
so every one of these six fired via the **direct, immediate** dispatch path
in `try_fire`, not via the `maxInFlight` queue. Decoding the ULID timestamps
embedded in the harness tuple id and its paired `reactor_fired` marker id
shows dispatch landing within 10s of the completion in every case (several
under 200ms):

| agent | harness_result landed | fired | delta |
|---|---|---|---|
| Wriggle-5 | 13:20:50.768 | 13:21:05.722 | 15.0s |
| Gadget-5 | 18:23:27.637 | 18:23:38.243 | 10.6s |
| Peanut-5 | 19:18:59.196 | 19:19:10.301 | 11.1s |
| Noodle-5 | 19:46:16.459 | 19:46:27.904 | 11.4s |
| Django-6 | 23:10:12.807 | 23:10:12.907 | 0.1s |
| Whisker-7 | 2026-08-16 00:08:12.309 | 00:08:12.415 | 0.1s |

None of the six left a `reactor_fire_attempt` record (no retries were
needed) and none left a `reactor_fire_gave_up` obstacle. Each of the six
spawned `steward` workflow instances ran all 12 steps to `status: completed`:

- All six → gate passed
- Gadget-5, Whisker-7, Wriggle-5, Peanut-5 → verdict `APPROVE`, merged `true`
  (Wriggle-5 and Peanut-5, two of the three reported "missed", show real
  merge commits: `9e2c9ee4...` and `a3d4d643...`)
- Noodle-5, Django-6 → verdict `REWORK`, `merged: false` (branch correctly
  held unmerged, not a miss — that is the steward doing its job)

So per the live tuplespace, **all six triggers fired, exactly once, via the
direct dispatch path, within single-digit-to-low-teens seconds of the
completion landing** — there is no reactor-side non-fire to explain for this
specific batch. Two of the three reported "missed" completions (Wriggle-5,
Peanut-5) in fact auto-merged successfully; the third (Django-6) correctly
got routed to REWORK.

## Candidate causes ruled out

- **Dedup key collision** (`already_fired` keyed `trigger@tuple.id`): ruled
  out — `RecordId`s are ULIDs, `already_fired`'s payload search is an exact
  `instr()` substring match on the full quoted `"key":"<key>"` string
  (`crates/rk-space/src/store.rs`), and each of the six harness tuples has
  its own single fire marker citing its own id.
- **Cursor pinning at emission time**: ruled out for this batch — no
  retryable failure appears in the delta window around any of the six (no
  `reactor_fire_attempt` records), so nothing pinned the cursor past them.
- **Rate cap / `maxInFlight` window**: ruled out for this batch — all six
  dispatched via the direct path, not the queue, so neither cap held any of
  them back.
- **Param templating failure (null-template)**: not applicable — four of the
  six completions (Gadget-5, Noodle-5, Wriggle-5, Peanut-5) predate the
  `diffClass`/`headSha` fields entirely (no such keys in their
  `harness_result` payload) and still fired correctly, since the reactor
  omits a null-templated param rather than passing it through (the trigger
  file's own comment on this, `examples/triggers.cue`).
- **Scope/payload shape differences on chained completions**: none observed
  — all six payloads carry `role: "rat"`, a real `branch`, and a `target`
  chained onto the predecessor's branch as expected for a rework spawned with
  `--base`.

## Most likely explanation for the original report

The steward pipeline (reviewer spawn → gates → verdict → land) takes roughly
12–15 minutes end to end for all six of these completions — there is no
timing difference between the "fired" and "not fired" reported sets. The most
parsimonious read is that the original characterization checked for evidence
(`rk inbox`, `git log`, or a merged branch) before each rework's steward
pipeline had finished, not that the trigger failed to fire. This is
consistent with the report's own framing ("took three days to characterize")
describing an investigation across many completions in the fleet, not
necessarily a defect reproducible from this specific named batch.

## Disposition

No reactor defect is reproducible against this batch's tuplespace evidence,
so this ticket is closed with this diagnosis rather than a speculative fix.
The one unconditional ask in the ticket — a durable trace whenever a matched
trigger's fire is suppressed, deferred, or retried, not just when it finally
gives up — is real and worth having regardless of root cause, and is added in
this same change:

- `Reactor::trace_fire_deferred` (`crates/rk-daemon/src/reactor.rs`) writes a
  durable `reactor_fire_deferred` obstacle (system scope, same durability
  class as the existing `reactor_rate_capped`/`reactor_fire_gave_up`
  obstacles) naming the trigger, the tuple, and the reason, wired into three
  previously-silent-or-log-only paths:
  - `give_up_or_retry`'s retry branch (every attempt before the final
    give-up used to be a `warn!` log line only)
  - `drain_queued_fires`'s rate-cap `break` (previously silent — no log, no
    tuple)
  - `dispatch_queued`'s retry branch (previously a `warn!` log line only)
- Regression tests: `permanently_retrying_fire_writes_a_durable_deferred_trace`
  and `drain_rate_cap_writes_a_durable_deferred_trace`
  (`crates/rk-daemon/tests/reactor.rs`) prove both paths now leave a durable,
  scannable trace instead of only a log line.

A future investigation into a *reproduced* miss should start by scanning
`rk scan obstacle system --search reactor_fire_deferred` for the affected
tuple id/trigger before assuming a silent drop — if no deferred trace and no
`reactor_fired` marker exist for a matched tuple, that now narrows the
search to the `try_fire`/`enqueue_fire` write path itself (e.g. a
`space.out` failure) rather than the retry/queue machinery this change
instruments.

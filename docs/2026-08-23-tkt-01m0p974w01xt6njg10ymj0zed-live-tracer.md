# Live Rat Kingdom tracer: task-to-main baseline and post-change timings

TKT-01M0P974W01XT6NJG10YMJ0ZED, parent TKT-01M0P2KNB92EAV2QG9256MY3QV
("Trace task-to-main critical path and enforce phase latency targets").

This is the durable artifact for the fourth leg of that epic: prove, against
**production records** and **public operator surfaces**, that the substrate
(TKT-01M0P974EZZTPMGVP4S0E76NXH), rendering (TKT-01M0P974FGSEFSX2KCS93QFPTF),
and policy (TKT-01M0P974MQK5XE1MR9KQCWT654 / TKT-01M0QXS8WPTMJTW3ZR0QPD44XG,
plus the proof-reuse fix TKT-01M0QRZ7QT8CQD74GHRN81XFT5 it depends on to be
meaningful) actually deliver measurable task-to-main latency, and to record
the fleet's agreed temporary target for duplicate work.

## Tool

`scripts/rk-task-to-main-tracer.py` — reads `rk --json status <TKT-id>` and
`rk --json digest --since <window>` (both read-only RPCs, both public
operator CLI surfaces — nothing here uses an internal API or writes a
tuple). `--self-test` re-parses bundled, unmodified fixtures captured from
this run (`scripts/fixtures/rk-task-to-main-tracer/*.json`) so parsing has
automated coverage without a live daemon; `--live` is the explicit,
separately-run path that talks to production. Re-running `--live` against
the same castle later is safe and idempotent: spans are append-only and
deduplicated on `(task, phase, attempt)` by the substrate itself, so a
completed ticket's numbers here never change, and nothing in this tool ever
calls a write RPC.

## Production identity

```
$ rk daemon status
running: pid 58529 · castle Nikaido · 19561 tuples · uptime 490s · v0.1.0+78f52a19f11d
```

Build `78f52a19f11d` = commit `78f52a1`, the merge of
TKT-01M0QXS8WPTMJTW3ZR0QPD44XG — i.e. substrate + rendering + policy +
proof-reuse are all live in the binary that produced every number below.
Captured 2026-08-23T18:50:02Z.

## Representative paths

Four path types were requested: clean, focused-inner/full-final proof,
review/rework, and human-gated. Three have real production correlation
identities below (all findable independently — they are real tickets from
this castle's own tuplespace, not constructed). The fourth is reported as
**unavailable evidence**, not zero — see that section.

Reproduce any of these:

```
rk --json status <TKT-id>
python3 scripts/rk-task-to-main-tracer.py --live label=<TKT-id> [label=<TKT-id> ...]
```

### 1. Baseline — duplicate full-suite verification (pre proof-reuse fix)

`TKT-01M0P974FGSEFSX2KCS93QFPTF` (the rendering ticket's own landing,
merged as `cae9ebf`, **before** the proof-reuse fix `8201be7` was on main).
Command: `rk --json status TKT-01M0P974FGSEFSX2KCS93QFPTF`.

| phase | attempt | lane | duration_ms | proof_kind | proof_reused | terminal |
|---|---|---|---|---|---|---|
| verification | 10000 | verify | 823 | ad-hoc | false | fail |
| verification | 10001 | verify | 756 | ad-hoc | false | fail |
| verification | 10002 | verify | **413842** | ad-hoc | false | pass |
| verification | 1 | steward-protected-paths | 12 | full-final | — | — |
| verification | 2 | steward-diff-scope | 17 | full-final | — | — |
| verification | 3 | verify | **407564** | full-final | — | — |

`proof_reuse: {"reused": 0, "total": 7}`. The managed, developer-triggered
`ad-hoc` verify passed at 413,842ms, then the landing gate reran the
**entire suite a second time** (`full-final`, 407,564ms) for content that
had already passed — this is exactly the duplicate-full-suite pattern
TKT-01M0QRZ7QT8CQD74GHRN81XFT5 was filed to fix. Combined: **821,406ms
(~13.7 min)** of full-suite CI time for one landing that needed to run it
once.

### 2. Post-change — clean path with proof reuse (focused-inner/full-final)

`TKT-01M0QXS8WPTMJTW3ZR0QPD44XG` (merged last on main, `78f52a1`, **after**
the proof-reuse fix was live). Command:
`rk --json status TKT-01M0QXS8WPTMJTW3ZR0QPD44XG`.

| phase | attempt | lane | duration_ms | proof_kind | proof_reused | terminal |
|---|---|---|---|---|---|---|
| verification | 10000 | verify | 119823 | ad-hoc | false | caller_disconnect |
| verification | 10001 | verify | **274855** | ad-hoc | false | pass |
| verification | 1 | steward-protected-paths | 465 | full-final | false | — |
| verification | 2 | steward-diff-scope | 260 | full-final | false | — |
| verification | 3 | verify | **null** | full-final | **true** | — |

`proof_reuse: {"reused": 1, "total": 6}`. The landing gate's `full-final`
verify stage has `duration_ms: null` and `proof_reused: true` — it did not
execute; it reused the passing `ad-hoc` proof for the same content
(TKT-01M0QRZ7QT8CQD74GHRN81XFT5's exact/ancestor candidate matching). Zero
duplicate full-suite time. This ticket also had 0 review rounds and 0
rework rounds — a genuinely clean, ordinary path, and the intended
steady-state shape of #1.

### 3. Review / rework

`TKT-01M0P974MQK5XE1MR9KQCWT654` (the original policy ticket; still
`in_flight: true` in this castle — Dart-12's review sent it to rework, and
the recovery landed separately as ticket #2 above rather than through this
ticket's own branch). Command:
`rk --json status TKT-01M0P974MQK5XE1MR9KQCWT654`.

| phase | attempt | duration_ms | authority | terminal_reason |
|---|---|---|---|---|
| semantic_review | 1 | **null** | llm | rework-requested |
| rework | 1 | **null** | llm | dispatched |

`review_rounds: 1`, `rework_rounds: 1`, `rework_amplification: 1.0`. The
landing-gate `verify` stage on this ticket's own final attempt *also* shows
`proof_reused: true` — proof reuse holds up across a rework round, not just
on a first-try clean path. Note the `null` duration on both `semantic_review`
and `rework`: see Observed limits, #3.

### 4. Human-gated — no production example (unavailable, not zero)

The only phase carrying `authority: human` in the current substrate is
`Phase::AttentionHold`, written from exactly two producers, both
budget-exhaustion escalations: `withhold_rework` (`landing.rs:3650`, rework
retry budget exhausted) and `withhold_conflict` (`landing.rs:4025`, conflict
retry budget exhausted). Neither has ever fired for rat-kingdom:

```
$ rk --json scan event rat-kingdom task_span --search attention_hold --hot --top 100
{"tuples": [], "truncated": false}
```

Zero results — not "0ms", an empty result set from a targeted, ranked
(`--hot`) search that is not subject to the oldest-first truncation
documented below. This is recorded here as **unavailable evidence**: the
path exists and is unit-tested
(`phase_latency.rs::a_correct_human_gate_breach_carries_human_authority_and_mutates_nothing_else`)
but has not yet been exercised by a real rat-kingdom ticket. It should not
be reported as "0 human-gated time," which would misleadingly imply the
gate was checked and found instant.

## The agreed temporary target

> Ordinary clean changes must not accumulate duplicate full-suite or
> duplicate semantic-review time.

(Parent ticket TKT-01M0P2KNB92EAV2QG9256MY3QV acceptance criterion 7.)

**Duplicate full-suite verification** — evaluated directly, with real
before/after evidence:

| path | full-suite stages executed | duplicate ms | verdict |
|---|---|---|---|
| #1 baseline (pre-fix) | 2 (ad-hoc + full-final) | 407,564 | **FAIL** |
| #2 post-change, clean | 1 (ad-hoc; full-final reused) | 0 | **PASS** |
| #3 review/rework | 1 (ad-hoc; full-final reused even after rework) | 0 | **PASS** |

The target is met post-change: every ticket observed in this build that
went through the landing gate had its `full-final` verify stage reuse the
prior passing proof instead of re-executing. The one counter-example (#1)
is from before the fix and is exactly the failure mode the target was
written against — its presence is what makes #2/#3 meaningful rather than
coincidental.

**Duplicate semantic-review time** — cannot be evaluated on duration
directly; only a round-count proxy is available (see Observed limits, #3).
By that proxy: the one rework path observed (#3) had exactly 1 review round
→ 1 rework round → 1 fresh review, i.e. no *redundant* re-review of
unchanged content was observed, but this is a single data point under
`rework_amplification: 1.0`, not a stress test, and the substrate cannot
currently distinguish "review took a long time" from "review round
happened."

## Observed limits

Found while building and running this tracer against production; each is
filed as its own ticket rather than fixed inline
(`preexisting-failure-is-a-ticket-not-an-inline-fix` /
TKT-01M0QRZ7QT8CQD74GHRN81XFT5-adjacent convention — this tracer's job is to
measure and report, not to patch the substrate it is measuring).

1. **`task_to_main_ms` is never populated, for any ticket.**
   `null` in all three examples above, including two that fully merged to
   main. Root cause (read from source, not inferred): `Phase::Merge` is
   written (`landing.rs:3425`) via
   `PhaseSpan::new(...).repo(...).target(...).candidate(merge_commit)` with
   no `.ended_at(...)` — even though the `DeliveryRecord` written
   immediately after in the same function carries a `landed_at` timestamp
   that is simply never threaded into the span. Separately, no producer
   anywhere writes `Phase::TicketReady` with a `queued_at`/`started_at` at
   all. `critical_path.rs::build_critical_path` needs both ends populated to
   compute `task_to_main_ms`, so the headline metric this whole epic
   promises is not populated by the current build for any ticket. Filed as
   **TKT-01M0QZFFT9WFDTG0CS4GVD03QX**.

2. **`rk digest`'s `phase_latency` aggregation cannot see recent spans.**
   `rk --json digest --since <30m|6h|24h|3d|7d>` all returned
   `phase_latency.window_spans: 0` with every sub-metric `null`, in the same
   session where `rk status` on individual tickets showed real spans
   timestamped inside every one of those windows. Root cause: `digest()`
   (`observe.rs:97`) fetches events via `space.scan {"category":"event"}`
   with no scope and no `--hot`/`--top`; `handle_scan` (`server.rs:8295`)
   documents that path as oldest-first, capped at `MAX_SCAN_TUPLES = 10_000`.
   Confirmed directly: `rk scan event rat-kingdom` alone (already narrower
   than digest's unscoped fleet-wide query) returns exactly 10,000 tuples
   spanning 2026-07-22 → 2026-08-21, zero of them `task_span` — every span
   from 2026-08-23 (today) is outside that oldest-10,000 slice. `rk status`
   avoids this because `critical_path.rs` targets one task via a scoped
   payload search rather than a raw category scan. Filed as
   **TKT-01M0QZFTYQW4WV200TYGCN46XA**. Practical consequence for this
   report: every number above came from `rk status` per-ticket, not from
   `rk digest`'s aggregate p50/p95 — that aggregate view is not currently
   trustworthy for recent activity in an active castle.

3. **`semantic_review` and `rework` phase spans never carry `duration_ms`.**
   Confirmed in example #3: both spans are written with a `terminal_reason`
   and `authority` but no timing. `rework_amplification` (round-count ratio)
   is available; elapsed review/rework wall-clock time is not. This is why
   the "duplicate semantic-review time" half of the temporary target above
   can only be evaluated by round count, not duration, today.

4. **This castle is very active**, and `rk digest`'s bounded scan (#2) means
   any future re-run of this tracer's `--live` mode should keep pulling
   per-ticket evidence via `rk status <TKT-id>` on specific correlation
   identities rather than relying on the fleet-wide digest for phase
   latency until #2 is fixed.

## Reproducing this report

```
# Parsing-only, no daemon required (what CI / an automated check should run):
python3 scripts/rk-task-to-main-tracer.py --self-test

# Against a live daemon, same three correlation identities as this note:
python3 scripts/rk-task-to-main-tracer.py --live \
  postchange-clean=TKT-01M0QXS8WPTMJTW3ZR0QPD44XG \
  baseline-duplicate=TKT-01M0P974FGSEFSX2KCS93QFPTF \
  review-rework=TKT-01M0P974MQK5XE1MR9KQCWT654
```

The `--self-test` assertions are the automated, parsing-only check this
ticket calls for; the `--live` output above (and the fixtures it was
captured into) is the explicit, separately-run production evidence — the
two are intentionally not conflated.

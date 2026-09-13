# Stigmergy evidence report: capture recipe and input contracts

Companion to `docs/2026-09-12-stigmergy-evidence-and-trial.md` (S3:
`TKT-tavik-kifos-lozuf`). Describes how to produce the three JSON files
`rk bbs report --manifest FILE --tuples FILE --reviews FILE [--output FILE]`
consumes, and the exact schema each one follows. The report itself is pure,
offline aggregation (`crates/rk-cli/src/bbs_report.rs`) — no daemon
connection, worker credentials, model call or network. It has no dispatch,
landing, repair or approval authority; it only renders evidence that
already exists.

## Evaluator version 3: author-exit and cost are real; two figures remain unsupported

`evaluator_version` is `3` (`TKT-bonik-vuruv-mivuh`, slice B of
`TKT-nonub-pugar-pilid`). Compare it, not just `schema_version`, when diffing
two reports: a version-2 report of the same inputs is not comparable on the
fields below.

- **Version 1** credited author-exit from a `harness_result` or an
  `agent_lifecycle` event. Neither can establish a physical exit: the
  supervisor emits `harness_result` when the agent routes `rk done`, while the
  OS process is still alive and the provider may still report a later total,
  and `agent_lifecycle` carries no `spawn` binding at all and its `change` may
  be `started`. It also summed a provisional `harness_result` cost as a
  measured total, read a `merge` span as an accepted delivery, and labelled a
  phase-duration sum as active work.
- **Version 2** (`TKT-buruk-parut-zisoh`) tightened record acceptance and
  denominator handling, but rather than ship those two known false positives it
  reported `author_exit` and `delivery_cost_and_duration` as UNSUPPORTED, with
  `mechanism.goal_met` forced `false`.
- **Version 3** restores both, from the S2 native observation contract
  (`bbs-agent-exit`/`bbs-agent-final-usage`, castle-authored — `instance` is
  the castle and the observed generation lives only in the payload):
  - **`author_exit`** is credited ONLY from an `agent_exit` record bound to the
    source author's exact `(spawn, session)` in the pair's repo, with
    `exited_at` no later than the reuse and no relaunch of that generation
    observed in between (a manual respawn keeps the same `spawn`, so an earlier
    exit alone is not proof). `harness_result` and `agent_lifecycle` are still
    explicitly refused, with the same reasons as version 2.
    `stale_session: true` on the cited `agent_exit` is also refused: it means a
    later launch of that generation already existed when the exit was
    recorded, and `prior_state`/`crashed` describe that successor, not this
    launch (`TKT-tulir-kotah-gisub`).
  - **Per-delivery cost** is derived per `(spawn, session, provider_session)`
    segment from `agent_final_usage`, taking the LAST reported total per
    segment (a provider total is cumulative within its query) and requiring
    both a terminal last state (`completed`/`failed`/`stopped`) and an
    `agent_exit` whose `prior_state` agrees with it. A `paused` result
    followed by more work and a kill leaves the amount `partial_reported_usd`,
    not a final cost; a segment reporting a cost under neither
    `provider_reported_segment_total` nor `daemon_priced_increments` (e.g. the
    producer's own `unknown` basis for a stale/mixed-basis segment) is listed
    in `unknown_cost`, never pooled and never silently dropped.
    `harness_result.cost_usd` stays `provisional_completion_cost_usd`, distinct
    from the reported total. `process_lifetime_ms` (`exited_at - launched_at`)
    is labelled process lifetime, never active work.
  - Deliveries are still derived from the frozen consumer-task scope (not the
    pairs that survived evaluation), now carrying every observed generation,
    completion (including failed/undeclared attempts) and cost segment for
    that task; a frozen task with zero native records at all is listed under
    `tasks_without_native_records` rather than silently omitted.

What remains genuinely unsupported at version 3, under `report.unsupported`:

- **`active_work_ms`** — no native observation distinguishes model-active time
  from a process paused awaiting verification or the operator, so it stays an
  explicit `unknown` rather than being aliased to process lifetime.
- **`total_operator_interventions`** — `attention_hold` span count
  (`quality.attention_hold_spans`/`interventions_known`) is a LOWER BOUND: an
  intervention that left no span is not counted. `ReviewedAnnotation`
  (below) can supply operator-judged interventions/rework/repeated
  investigations, reported SEPARATELY (`quality.reviewed_*`,
  `DeliveryCost.reviewed_*`) rather than summed with the span-derived lower
  bound — the two answer different questions.

## Phase-span duration contract and occurrence identity (`task_span`)

Every `task_span` (`rk_daemon::span::PhaseSpan`) carries `queue_wait_ms` and
`duration_ms`, and — only when built via `PhaseSpan::from_durations` (the
shape a `VerificationQueued` producer uses, since it has no real wall-clock
`queued_at`/`started_at` to record directly) — a `duration_semantic` tag.
`duration_semantic: "additive"` is a CONTRACT, not a description: it asserts
`queue_wait_ms` (admission wait) and `duration_ms` (execution) are disjoint,
non-overlapping intervals, so `queue_wait_ms + duration_ms` is a sound total
elapsed for that span. A row with no `duration_semantic` at all predates the
tag and must NOT be assumed additive — its `duration_ms` may already include
the wait `queue_wait_ms` also reports (exactly the bug this tag exists to
make impossible to repeat: a landing-gate producer once measured
`duration_ms` from before admission was requested, so summing the two for a
total double-counted the wait). This report therefore sums `duration_ms +
queue_wait_ms` into `DeliveryCost.phase_ms.verification_ms` ONLY for a
`verification`-phase span tagged `additive`; every other `verification` span
is excluded from that sum and counted instead under
`phase_ms.verification_ms_legacy_spans` — an explicit coverage gap, never a
guess in either direction, never zeroed to hide it and never silently summed
on an assumption that could be wrong.

Idempotency for a `VerificationQueued` span is keyed on `(task, phase,
attempt)`, additionally fenced by `target`, `candidate`, `lane` and
`occurrence_key` whenever the producer sets them (`span.rs` module doc) — and
`crates/rk-cli/src/critical_path.rs::build_critical_path` dedups incoming
rows on the IDENTICAL key when rendering, so a row the daemon kept as a
distinct occurrence is never re-collapsed by this report or `rk status`. A
landing gate's per-check span numbers `attempt` by the check's plan position
(1, 2, 3, ...) every round, so `target`/`candidate`/`lane`/`occurrence_key`
are what distinguish a later round's real occurrence, or a check whose
command/toolchain/environment changed at the same candidate
(`occurrence_key` — the same digest `verification_proof_key` computes over
repo/candidate/check-name/command/toolchain/environment), from an earlier
one recorded at the same small ordinal. Missing historical spans are never
backfilled as zero time: a task with no recorded span for a phase simply has
no entry for it, exactly like every other "not invented" field in this
report.

## Reviewed annotations: operator judgment, not daemon facts

`ReviewedAnnotation` is a bounded, task-scoped input distinct from `Review`
(which is scoped to one source/consumer pair): it records repeated
investigation, rework, or a real intervention that daemon telemetry alone does
not establish end-to-end, each occurrence bound to a resolvable evidence id
already in the capture — never a bare reviewer assertion, and never a duration
or time-saved figure (`deny_unknown_fields` on both `ReviewedAnnotation` and
its `AnnotatedEvidence` entries blocks smuggling one through an unknown key).
An annotation may only speak about a task already frozen into delivery scope
(`manifest.consumer_tasks` or an `eligible_pairs` consumer task), at most once
per `(task, repo)`.

```json
{
  "task": "TKT-...",
  "repo": "rat-kingdom",
  "repeated_investigations": [{"evidence": "<artifact-tuple-id>", "reason": "..."}],
  "rework": [],
  "interventions": [{"evidence": "<artifact-tuple-id>", "reason": "..."}]
}
```

Passed via the `--reviews` file, which now accepts either its original bare
`Review` array (`reviewed_annotations` defaults to empty) or an object:

```json
{"reviews": [ /* Review array, as before */ ], "reviewed_annotations": [ /* ReviewedAnnotation array */ ]}
```

Resolved counts land on the matching `DeliveryCost.reviewed_repeated_investigations`/
`reviewed_rework`/`reviewed_interventions` (evidence ids) and are totalled
under `quality.reviewed_repeated_investigations`/`reviewed_rework`/
`reviewed_interventions`. An annotation whose evidence id does not resolve (not
in this capture, not an artifact, wrong repo) is reported under
`unresolved_records`/`invalid_records` with `kind`
`reviewed_repeated_investigation`/`reviewed_rework`/`reviewed_intervention`,
never silently trusted or silently dropped.

## Native record contract the evaluator enforces

Read off the producers, not off the design doc. A record that does not match is
rejected with a reason in `invalid_records`.

The invariant that catches forgery: `Tuple::new(category, scope, identity,
caller, payload)` sets `instance = caller`, and every S1 BBS write passes the
authenticated caller as both `instance` and `payload.agent`. For a
`finding`/`answer`/`reuse`/`assessment`, `instance == payload.agent` is
unconditional.

Telemetry is the opposite case, and the one most easily misread:
`exposure`/`open` are authored by the **castle**, so `instance` is the castle
and the consumer identity lives only in
`payload.agent`/`payload.spawn`/`payload.bound`. Telemetry is checked with
`tuple.scope == payload.repo` instead.

| kind | category | lifecycle | identity | schema_version |
| --- | --- | --- | --- | --- |
| `finding` | artifact | furniture | `bbs-finding-<digest>` | 1 |
| `answer` | artifact | furniture | `bbs-answer-<digest>` | absent (predates it) |
| `reuse` | artifact | furniture | `bbs-reuse-<digest>` | 1 |
| `assessment` | artifact | furniture | `bbs-assessment-<digest>` | 1, author `operator` |
| `exposure` | event | furniture | `bbs-exposure-<surface>` | 1 |
| `open` | event | furniture | `bbs-open` (no trailing dash) | 1 |
| `agent_exit` | event | furniture | `bbs-agent-exit` | 1, castle-authored |
| `agent_final_usage` | event | furniture | `bbs-agent-final-usage` | 1, castle-authored |

`agent_exit`/`agent_final_usage` are checked like `exposure`/`open`
(`tuple.scope == payload.repo`, not `instance == payload.agent` — they are
castle-authored telemetry). Both additionally require `payload.spawn` AND
`payload.session` (the join key every cost/exit derivation uses); a record
missing either is invalid, not silently attributed to just the spawn.
`agent_final_usage` also requires a non-empty `payload.cost_basis` — an
unstated basis cannot be aggregated.

A pair's `source` must be what the daemon lets `bbs.reuse` name: an ordinary
artifact with **no** `bbs_kind` at all (reusable regardless of lifecycle), or a
genuine `finding`/`answer`. A receipt, assessment or telemetry row is never a
source. An ordinary artifact has no authoring generation, recorded as
`source_attributed: false`: it can neither be excluded as self-use nor support an
author-exit claim.

Each id in an `evidence` array must name an **artifact** in the pair's repo. A
non-string member (`["ev-1", 7]`) is a defect, not something to filter out
silently. An id absent from the capture is UNKNOWN, not a negative: the record
survives, `source_evidence` reads `unknown`, and it can no longer certify an
effect.

`repos` and `window` are **enforced**. Every native observation outside them is
excluded and counted under `capture` (`observations_out_of_scope_repo`,
`observations_out_of_window`, `observations_undated`), so a mis-set window shows
up as visible coverage loss rather than silently shrinking every metric. Source
findings are deliberately *not* window-filtered: an older source a batch consumer
reused is exactly what the experiment is looking for. Unknown manifest fields are
rejected, so a misspelled `"windwo"` is an error rather than an ignored key, and
a fixture `eligible_pairs` entry must declare the **same repo as its batch**.

This applies to **every** claim/verdict, including `reuse` and `assessment`
records — an out-of-window receipt or verdict is excluded and counted under
`observations_out_of_window`, not silently admitted as a valid claim
(`TKT-figil-fobud-niluk`, fixed alongside slice B: earlier versions validated
their shape but never window-checked them at all).

Only `bound == "agent"` exposures count; `operator`/`unbound` rows go to
`unresolved_records`. An exposure whose consumer generation has **no** native
launch evidence is reported as `prepared_not_launched` and kept out of BOTH sides
of the discovery rate: a selection prepared for a spawn that never ran is neither
a discovery success nor a failure. Launch evidence is an
`agent_spawned`/`agent_respawned` carrying `spawn`, a `harness_result`, or a
record the generation authored itself.

## A bad claim removes the claim, not the opportunity

Two separate lists, and the distinction is the point:

- `excluded` — never a distinct valid opportunity: `duplicate`,
  `source_not_captured`, `invalid_source`, `invalid_source_evidence`, `self`.
- `rejected_claims` — the opportunity stands, the receipt does not:
  `wrong_repo`, `wrong_generation`, `invalid_evidence`, `unresolved_evidence`,
  `undated`, `future_source`. The pair stays in `eligible` with no claimed
  outcome, so a malformed or foreign receipt cannot erase a real opportunity from
  a denominator.

`discovery` reports the denominator explicitly, and `discovery.rate` /
`verified_reuse.rate` are `null` — not `0.0` — when there is no denominator.
`verified_reuse` uses the same `verified_effect` gate the mechanism goal counts,
so the two cannot disagree: outcome `used`/`adapted`, an operator verdict of
`verified`, known temporal order, `coverage_status == "prepared"`, a launched
consumer, resolved source evidence and a surviving receipt. `counts_as_effect`
additionally requires that the operator did not relay the pointer.

Operator-supplied coverage is reported as `prepared_reviewed` with
`coverage_provenance: "reviewed"` and a `coverage_reference_resolved` flag. It is
never merged into native prepared coverage.

`quality.attention_hold_spans`/`interventions_known` is a LOWER BOUND on
operator interventions, not a total: an intervention that left no
`attention_hold` span is not counted. `ReviewedAnnotation` (see above) can add
operator-judged interventions/rework/repeated investigations, reported
separately under `quality.reviewed_*` rather than summed with the span count.

## What the manifest can and cannot freeze

A source tuple id and a consumer's exact agent generation do not exist
before a live batch runs, so a manifest written ahead of time cannot name
them. What must be frozen ahead of time is *scope*: which batches exist and
which consumer tasks are in play for each. Concrete source/consumer pairs
are enrolled later, in the reviews file, each bound to an already-frozen
batch/task — never by mutating the manifest after the fact. A manifest may
still predeclare full pairs directly (`eligible_pairs`) for deterministic
fixture/replay cases where the pair identities are already known (this is
how the module's own unit tests work).

## The tuple capture and its ordering contract

`--tuples FILE` accepts any of three shapes:

1. A bare JSON array of tuples.
2. The raw object `rk --json scan <category> <scope>` already produces:
   `{"tuples": [...], "truncated": bool, ...}`.
3. The capture envelope below, which additionally declares `order`.

Shapes 1 and 2 are always treated as `order: "unknown"` — **never** inferred
from a tuple's id (ULID) or `created_at`. `rk --json scan` returns a query
result, not a persistence-order export; its array position carries no
ordering guarantee, and neither does sorting by ULID or wall-clock time.
This matters for one thing specifically: when a `receipt` (a `reuse` tuple)
has been assessed more than once, the *current* verdict is whichever
assessment came last in real SQLite persistence order. Without that order,
the report cannot silently guess — it reports the pair under
`ambiguous_assessments` instead of manufacturing a verdict.

### Capture envelope (forward-looking contract)

```json
{
  "schema_version": 1,
  "order": "persistence_sequence",
  "cursor": 12345,
  "since": 12000,
  "captured_at": "2026-09-13T00:00:00Z",
  "source": "space.persistence_delta",
  "truncated": false,
  "tuples": [ /* native wire tuples, in true persistence order */ ]
}
```

Only a capture built from `Space::persistence_delta` (a bounded,
sequence-ordered read — see `crates/rk-space/src/store.rs`,
`latest_persistence_sequence`/`persistence_delta`) may claim
`"order": "persistence_sequence"`. That RPC is daemon-internal today
(used by `bbs.brief`, the reactor and cross-castle sync); a bounded,
CLI-exposed `rk bbs export` built on the same path is S2's to supply
(`TKT-tapip-puhot-sitih`), not this ticket's. Until it lands, capture with
`rk --json scan` (below) and expect `order: "unknown"` — the report still
computes everything it safely can; it only refuses to resolve
assessment supersession without real order.

### Capturing today, with what already exists

```bash
# One JSON object per category, per repo scope, merged by hand or with jq -s:
rk --json scan artifact <repo> > artifacts.json
rk --json scan event <repo>    > events.json    # task_span, harness_result, exposure, open
jq -s '{tuples: (map(.tuples) | add), truncated: (map(.truncated) | any)}' \
  artifacts.json events.json > tuples.json
```

Each `rk --json scan` call already reveals its own truncation
(`"truncated": true` past 10,000 tuples per call) — preserve that flag
rather than dropping it when merging captures; the report refuses to
certify the mechanism goal over a capture it knows is incomplete
(`report.mechanism.goal_met` is forced `false` when `tuples_truncated` is
`true`, even if the raw counts otherwise clear the bar).

## Manifest template

```json
{
  "schema_version": 1,
  "experiment_id": "rk-stigmergy-<date>",
  "repos": ["rat-kingdom"],
  "window": {"since": "2026-09-13T00:00:00Z", "until": null},
  "build": {
    "source_commit": "8e0cccf",
    "installed_rk_version": "0.1.0+...",
    "installed_rk_mcp_version": "0.1.0+...",
    "daemon_identity": "72911dd1d382",
    "model": "claude-sonnet-5",
    "harness": "...",
    "check": "verify",
    "wip": "..."
  },
  "quality_criteria": ["..."],
  "batches": [{"id": "batch-1", "arm": "real", "repo": "rat-kingdom"}],
  "consumer_tasks": [
    {"task": "TKT-...", "repo": "rat-kingdom", "batch": "batch-1"}
  ],
  "eligible_pairs": []
}
```

`repos` and `window` are enforced (see above). `eligible_pairs` stays empty for
a live batch — pairs arrive via reviews
(next section). Fill it directly only for a fixture/replay run where the
pairs are already known.

## Reviews template

One entry per pair, referencing either a predeclared `eligible_pairs[].id`
or minting a new one bound to frozen `consumer_tasks` scope via `declares`:

```json
[
  {
    "pair": "p1",
    "declares": {
      "source": "<finding-or-artifact-tuple-id>",
      "consumer_task": "TKT-...",
      "consumer_generation": "<spawn-id-from-the-reuse-tuple>",
      "repo": "rat-kingdom",
      "batch": "batch-1"
    },
    "coverage": {"status": "prepared", "evidence": "<tuple-id or operator reference>"},
    "author_terminal_evidence": "<a bbs-agent-exit tuple id for the source author's own (spawn, session), or omit>",
    "relayed_by_operator": false,
    "regression": false,
    "notes": "..."
  }
]
```

Or, to also supply `ReviewedAnnotation`s, the object shape (see above):
`{"reviews": [...], "reviewed_annotations": [...]}`.

Two invariants the reviewer must respect (both enforced, not just
documented):

- **A review never mutates a predeclared pair's identity.** If `pair`
  already names a manifest `eligible_pairs[].id`, omit `declares`; if it
  does not, `declares` is required and its `batch`/`consumer_task`/`repo`
  must already appear in `manifest.consumer_tasks`.
- **`author_terminal_evidence` is evaluated as of `evaluator_version` 3.** It
  must resolve to a `bbs-agent-exit` record bound to the source author's exact
  `(spawn, session)` in the pair's repo, timestamped no later than the reuse,
  with no relaunch of that generation observed in between and
  `stale_session` not `true`. Anything that fails one of those checks is
  listed under `author_exit_unsupported` with the specific reason and credits
  nothing — `harness_result` and `agent_lifecycle` are still refused outright,
  same as version 2, since neither can establish a physical exit.

  The observation it consumes is contracted in
  `docs/2026-09-13-s2-native-observation-and-export-contract.md`
  (`bbs-agent-exit`/`bbs-agent-final-usage`). A real capture requires those
  producers, which land with S2's continuation
  (`TKT-dorod-sival-fumid`/`TKT-tulir-kotah-gisub`); until that lands on
  `main`, only a fixture/replay capture can supply them.

## Reading the report

Every count the design doc requires is a top-level field: `eligible`,
`excluded` (with `pair`/`reason`/`detail` — duplicate, self, future_source,
wrong_generation, wrong_repo, missing_evidence, source_not_captured),
`rejected_claims`, `discovery`, `presented_native`, `presented_reviewed`,
`opened`, `claimed`, `assessed`, `outcome_classes`, `unknown_coverage`,
`ambiguous_assessments`, `author_exit_unsupported`, `verified_reuse`,
`author_exit_reuse`, `mechanism` (the frozen 3-effects/2-batches/1-author-exit
threshold, plus `goal_blocked_reason`), `deliveries` (real generations,
completions, cost segments and coverage as of `evaluator_version` 3 — see
above), `tasks_without_native_records`, `invalid_records`,
`unresolved_records`, `unsupported`, `capture`, and `quality` (including
`reviewed_repeated_investigations`/`reviewed_rework`/`reviewed_interventions`).

Run it twice over the same three files and expect byte-identical JSON —
that determinism is covered by
`bbs_report::tests::verified_used_effect_counts_and_is_deterministic` and
the CLI-level `same_inputs_produce_deterministic_repeated_output`.

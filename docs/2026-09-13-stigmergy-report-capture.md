# Stigmergy evidence report: capture recipe and input contracts

Companion to `docs/2026-09-12-stigmergy-evidence-and-trial.md` (S3:
`TKT-tavik-kifos-lozuf`). Describes how to produce the three JSON files
`rk bbs report --manifest FILE --tuples FILE --reviews FILE [--output FILE]`
consumes, and the exact schema each one follows. The report itself is pure,
offline aggregation (`crates/rk-cli/src/bbs_report.rs`) — no daemon
connection, worker credentials, model call or network. It has no dispatch,
landing, repair or approval authority; it only renders evidence that
already exists.

## Evaluator version 2: what it validates, and what it refuses to estimate

`evaluator_version` is `2` (`TKT-buruk-parut-zisoh`). Compare it, not just
`schema_version`, when diffing two reports: a version-1 report of the same
inputs is not comparable.

Version 2 tightened record acceptance and denominator handling, and it
deliberately **stopped** producing two numbers version 1 produced from evidence
that could not support them. Those are reported as unsupported, with a reason,
under `report.unsupported` and in the human render — never as a zero:

- **`author_exit`.** Version 1 credited it from a `harness_result` or an
  `agent_lifecycle` event. Neither can establish a physical exit: the supervisor
  emits `harness_result` when the agent routes `rk done`, while the OS process is
  still alive and the provider may still report a later total, and
  `agent_lifecycle` carries no `spawn` binding at all and its `change` may be
  `started`. Version 2 credits nothing, `author_exit_effects` is `0`, and
  `mechanism.goal_met` is forced `false` with `goal_blocked_reason` set.
- **`delivery_cost_and_duration`.** Version 1 summed a provisional
  `harness_result` cost as a measured total, read a `merge` span as an accepted
  delivery, and labelled a phase-duration sum as active work. Version 2 reports
  the frozen delivery SCOPE with every figure `null`.

Both return with the real derivation in `TKT-bonik-vuruv-mivuh`. A version-2
report is therefore usable for discovery and reuse evidence, and **not** usable
for cost, duration or author-exit claims.

## Native record contract version 2 enforces

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

`quality.interventions` is `null`: an `attention_hold` span count is a lower
bound on operator interventions, not a total.

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
3. The real `bbs.export` envelope below, which additionally ships the
   evidence closure, a coverage statement and an `order` claim.

Shapes 1 and 2 are always treated as `order: "unknown"` — **never** inferred
from a tuple's id (ULID) or `created_at`. `rk --json scan` returns a query
result, not a persistence-order export; its array position carries no
ordering guarantee, and neither does sorting by ULID or wall-clock time.
This matters for one thing specifically: when a `receipt` (a `reuse` tuple)
has been assessed more than once, the *current* verdict is whichever
assessment came last in real SQLite persistence order. Without that order,
the report cannot silently guess — it reports the pair under
`ambiguous_assessments` instead of manufacturing a verdict.

### The `bbs.export` envelope

```json
{
  "schema_version": 1,
  "kind": "bbs.export",
  "order": "persistence_sequence",
  "order_provenance": "tuple_persistence_events.commit_sequence ascending",
  "source": "space.persistence_page",
  "cursor": 12345,
  "since": 12000,
  "captured_at": "2026-09-13T00:00:00Z",
  "truncated": false,
  "tuples":     [ /* the PAGE: wire tuples in persistence order, each with commit_sequence */ ],
  "references": [ /* the CLOSURE: tuples named by a page record, resolved AS-OF the
                     page boundary, id-sorted, each with its own AS-OF commit_sequence */ ],
  "coverage": {
    "missing_references": ["<tuple-id>"],
    "complete": false,
    "reference_budget_exhausted": false,
    "scope": "rat-kingdom"
  }
}
```

Three rules the reporter enforces on this shape:

**References are merged, not ignored.** Evidence a finding, receipt or
assessment names is frequently *not* on the page — the daemon resolves it
into `references` instead. The reporter indexes page and closure as one
record set, so linked evidence resolves; reading only `tuples` would treat
that evidence as absent and silently discard the verdict that depends on it.
A closure record whose id is already on the page is dropped as a duplicate
and counted, never double-counted as a second observation.

**Coverage is propagated, never assumed.** Each `missing_references` id is
reported as an `unresolved_records` entry of kind `missing_reference` — a
hole in the capture, not proof the tuple never existed. `coverage.complete:
false` blocks the mechanism goal exactly as `truncated` does, because the
unresolved reference could be the very receipt or assessment that changes a
count. A bare array or a raw `rk --json scan` object declares no coverage at
all, which is reported as `undeclared` and is **not** read as complete.

**The order claim is validated, not believed.** `"order":
"persistence_sequence"` is a string any file can contain. It is accepted
only when every page record carries a numeric `commit_sequence`, those are
strictly ascending in array order, and every reference carries one too
(otherwise the closure cannot be placed among the page records). A claim
that fails any of these is refused: the capture is downgraded to
`order: "unknown"` and the refusal reason is reported under
`capture.order_claim_rejected`. Shapes 1 and 2 are unknown-order without a
refusal — that is compatibility, not failure.

Cost aggregation and author-exit evaluation remain explicitly unsupported in
this intermediate evaluator. The complete reporter continuation supplies those
derivations using the same validated persistence positions.

### Capturing

The envelope above is what `rk bbs export` produces — use it:

```bash
# Pin ONE snapshot across pages: take `boundary` from page 1 and echo it back,
# and resume from the previous page's `next_cursor`.
rk --json bbs export --repo <repo> --limit 2000 > page1.json
rk --json bbs export --repo <repo> --limit 2000 \
  --boundary "$(jq -r .boundary page1.json)" \
  --after    "$(jq -r .next_cursor page1.json)" > page2.json
```

Without `--boundary`, each page takes a fresh snapshot and a concurrent write
can appear mid-paging. Pages are merged by concatenating `tuples` in order and
unioning `references`; `truncated` and `coverage` must be OR-ed and unioned
across pages, never dropped.

### Legacy fallback: raw scans

Still accepted, still correct, but `order: "unknown"` — no assessment
supersession and no multi-row cost-segment finality (see above).

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
    "author_terminal_evidence": "<omit: not evaluated in evaluator_version 2>",
    "relayed_by_operator": false,
    "regression": false,
    "notes": "..."
  }
]
```

Two invariants the reviewer must respect (both enforced, not just
documented):

- **A review never mutates a predeclared pair's identity.** If `pair`
  already names a manifest `eligible_pairs[].id`, omit `declares`; if it
  does not, `declares` is required and its `batch`/`consumer_task`/`repo`
  must already appear in `manifest.consumer_tasks`.
- **`author_terminal_evidence` is not evaluated in `evaluator_version` 2.**
  Anything supplied is listed under `author_exit_unsupported` with the reason and
  credits nothing — see the version-2 section above for why a `harness_result`
  and an `agent_lifecycle` both fail to establish a physical exit. Crediting it
  needs an exit observation bound to the source author's exact generation and
  launch, with no intervening resume before the consuming decision; that
  derivation is `TKT-bonik-vuruv-mivuh`.

  The observation it will consume is contracted in
  `docs/2026-09-13-s2-native-observation-and-export-contract.md`. Producers for
  it exist on S2's continuation branch but are **not delivered or verified**, so
  no capture can supply one yet.

## Reading the report

Every count the design doc requires is a top-level field: `eligible`,
`excluded` (with `pair`/`reason`/`detail` — duplicate, self, future_source,
wrong_generation, wrong_repo, missing_evidence, source_not_captured),
`rejected_claims`, `discovery`, `presented_native`, `presented_reviewed`,
`opened`, `claimed`, `assessed`, `outcome_classes`, `unknown_coverage`,
`ambiguous_assessments`, `author_exit_unsupported`, `verified_reuse`,
`author_exit_reuse`, `mechanism` (the frozen 3-effects/2-batches/1-author-exit
threshold, plus `goal_blocked_reason`), `deliveries` (scope only in
`evaluator_version` 2), `invalid_records`, `unresolved_records`, `unsupported`,
`capture`, and `quality`.

Run it twice over the same three files and expect byte-identical JSON —
that determinism is covered by
`bbs_report::tests::verified_used_effect_counts_and_is_deterministic` and
the CLI-level `same_inputs_produce_deterministic_repeated_output`.

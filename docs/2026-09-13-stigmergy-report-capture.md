# Stigmergy evidence report: capture recipe and input contracts

Companion to `docs/2026-09-12-stigmergy-evidence-and-trial.md` (S3:
`TKT-tavik-kifos-lozuf`). Describes how to produce the three JSON files
`rk bbs report --manifest FILE --tuples FILE --reviews FILE [--output FILE]`
consumes, and the exact schema each one follows. The report itself is pure,
offline aggregation (`crates/rk-cli/src/bbs_report.rs`) — no daemon
connection, worker credentials, model call or network. It has no dispatch,
landing, repair or approval authority; it only renders evidence that
already exists.

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

`eligible_pairs` stays empty for a live batch — pairs arrive via reviews
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
    "coverage": {"status": "prepared", "evidence": "<exposure-event-id-or-note>"},
    "author_terminal_evidence": "<harness_result-or-agent_lifecycle-tuple-id, or omit>",
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
- **`author_terminal_evidence` must be a real tuple, not an assertion.** A
  reviewer boolean alone cannot establish that a source's author had
  already exited before the reuse. The id must resolve to an actual
  `harness_result`/`agent_lifecycle` event for the source's own authoring
  generation (`payload.spawn`), timestamped no later than the reuse. If it
  doesn't resolve that way, the report lists it under
  `author_exit_unsupported` with the reason and does not credit it.

## Reading the report

Every count the design doc requires is a top-level field: `eligible`,
`excluded` (with `pair`/`reason`/`detail` — duplicate, self, future_source,
wrong_generation, wrong_repo, missing_evidence, source_not_captured),
`presented`, `opened`, `claimed`, `assessed`, `outcome_classes`,
`unknown_coverage`, `ambiguous_assessments`, `author_exit_unsupported`,
`verified_reuse`, `author_exit_reuse`, `mechanism` (the frozen
3-effects/2-batches/1-author-exit threshold), `deliveries` (cost/duration
per task, each field an explicit `null` rather than a manufactured zero
when the underlying tuple wasn't captured), and `quality`.

Run it twice over the same three files and expect byte-identical JSON —
that determinism is covered by
`bbs_report::tests::verified_used_effect_counts_and_is_deterministic` and
the CLI-level `same_inputs_produce_deterministic_repeated_output`.

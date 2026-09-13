# Stigmergy evidence report: capture recipe and input contracts

Companion to `docs/2026-09-12-stigmergy-evidence-and-trial.md` (S3:
`TKT-tavik-kifos-lozuf`; corrected by `TKT-nonub-pugar-pilid`, which is the
version described here — `evaluator_version: 2`). Describes how to produce the
three JSON files
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
rk --json scan event <repo>    > events.json    # task_span, harness_result,
                                                # agent_spawned/respawned, exposure,
                                                # open, agent_exit, agent_final_usage
jq -s '{tuples: (map(.tuples) | add), truncated: (map(.truncated) | any)}' \
  artifacts.json events.json > tuples.json
```

Each `rk --json scan` call already reveals its own truncation
(`"truncated": true` past 10,000 tuples per call) — preserve that flag
rather than dropping it when merging captures; the report refuses to
certify the mechanism goal over a capture it knows is incomplete
(`report.mechanism.goal_met` is forced `false` when `tuples_truncated` is
`true`, even if the raw counts otherwise clear the bar).


## Native record contract the evaluator enforces

Every field below was read from the producer, not from a design document. A
record that does not match is REJECTED with a reason in `invalid_records`
rather than trusted or silently dropped. Nothing here is configurable: it is
what the daemon writes.

### The invariant that catches forgery

`rk_core::tuple::Tuple::new(category, scope, identity, caller, payload)` sets
`instance = caller`, and every S1 BBS write passes the authenticated caller as
both `instance` and `payload.agent`. So for a `finding`/`answer`/`reuse`/
`assessment`, `instance == payload.agent` is unconditional, and a row where
they disagree was not written by that path.

Telemetry is the opposite case and the one most likely to be misread:
`exposure`/`open`/`agent_exit`/`agent_final_usage` are authored by the
**castle**, so `instance` is the castle and the consumer identity lives only in
`payload.agent`/`payload.spawn`/`payload.bound`. Reading `instance` as the
consumer generation attributes a record to the wrong party. Telemetry is
instead checked with `tuple.scope == payload.repo`.

| kind | category | lifecycle | identity | schema_version |
| --- | --- | --- | --- | --- |
| `finding` | artifact | furniture | `bbs-finding-<digest>` | 1 |
| `answer` | artifact | furniture | `bbs-answer-<digest>` | absent (predates it) |
| `reuse` | artifact | furniture | `bbs-reuse-<digest>` | 1 |
| `assessment` | artifact | furniture | `bbs-assessment-<digest>` | 1, author `operator` |
| `exposure` | event | furniture | `bbs-exposure-<surface>` | 1 |
| `open` | event | furniture | `bbs-open` (no trailing dash) | 1 |
| `agent_exit` | event | furniture | `bbs-agent-exit` | 1 |
| `agent_final_usage` | event | furniture | `bbs-agent-final-usage` | 1 |

### Reusable sources

A pair's `source` must be what the daemon itself allows `bbs.reuse` to name:
an ordinary artifact with **no** `bbs_kind` at all (reusable regardless of
lifecycle — a daemon-authored gate result carries reproduction evidence too),
or a genuine `finding`/`answer`. A receipt, an assessment or a telemetry row is
never a source, and a row merely carrying `"bbs_kind":"finding"` must satisfy
the full predicate above.

An ordinary artifact has no authoring generation. That is recorded as
`source_attributed: false`: such a source can neither be excluded as self-use
nor support an author-exit claim, and both stay explicitly unknown.

### Evidence links

Each id in an `evidence` array must name an **artifact** in the pair's repo. A
non-string member (`["ev-1", 7]`) is a defect, not something to filter out
silently. An id simply absent from the capture is UNKNOWN, not a negative: the
record survives, `source_evidence` reads `unknown`, and it can no longer
certify an effect.

### Exposure means prepared, for a launched consumer

Only `bound == "agent"` exposures inside the frozen repo set and window count;
`operator`/`unbound` rows are listed under `unresolved_records` rather than
attributed to a guess. Entries are keyed on the producer's real
`entries[].source` field.

An exposure whose consumer generation has **no** native launch evidence is
reported as `prepared_not_launched` and kept out of BOTH sides of the discovery
rate: a selection prepared for a spawn that never ran is neither a discovery
success nor a discovery failure. Launch evidence is any of `agent_spawned`/
`agent_respawned` carrying `spawn`, a `harness_result`, an `agent_exit`, an
`agent_final_usage`, or a record the generation authored itself.

## Author exit: `agent_exit` only

`author_terminal_evidence` must resolve to an `agent_exit` observation for the
source author's exact `(spawn, session)` in the pair's repo, with
`exited_at` no later than the reuse and **no observed relaunch of that
generation in between** — a manual respawn continues the same `SpawnId`, so an
earlier exit is not proof the author was gone when the consumer decided.

Two records are refused with an explicit reason, because both were accepted
before this correction and both produce a false positive:

- **`harness_result`.** `Supervisor::route_completion` emits it when the agent
  routes `rk done`. The OS process is still alive until `HarnessEvent::Exited`,
  and the provider may still report a different, later total for the same
  query. Observed live: completion tuple `01M2CBAAR1JMRGBV5Z7ES013M3` recorded
  `cost_usd=14.802756` for Gusteau-14 while that generation's later status
  carried `7.3380432` and its process was still running.
- **`agent_lifecycle`.** `emit_coordinator_event` writes `agent` and
  `generation` (`record.created_at`) and **no `spawn` field at all**, is not
  repo-bound, and its `change` may be `started`. It cannot identify a terminal
  generation even in principle.

## Cost: reported estimates, per proven segment

Source of truth: `https://code.claude.com/docs/en/agent-sdk/cost-tracking`
(checked 2026-09-13; notes in the run artifact directory).

- A streaming result's total is **cumulative within one query**. The last
  reported total per segment is that segment's amount; repeated results are
  never summed.
- A segment is the triple `(spawn, session, provider_session)`. `spawn` alone
  aliases two attempts of one generation, because a manual respawn keeps the
  `SpawnId` and only `session` changes. The native launch token (`session`) and
  the provider's own session id (`provider_session`) are separate identities
  with separate lifetimes.
- Amounts are **client-side estimates, not billed charges**, and the report
  labels them that way.
- A segment's amount is **final** only when its last result's `state` is
  terminal (`completed`/`failed`/`stopped`) AND an `agent_exit` for the same
  `(spawn, session)` was observed AND that exit's `prior_state` agrees with
  that state. Finality is never inferred from merely finding a result before an
  exit: a `paused` result can be followed by more usage and then a budget kill
  with no further result, and the earlier cumulative total is then a partial
  amount. That case reports `partial_reported_usd` and leaves
  `reported_cost_estimate_usd` null.
- `cost_basis` values are never pooled. A `daemon_priced_increments` segment (the
  supervisor's fallback for harnesses that do not self-report USD — an estimate
  of an estimate) goes to `daemon_priced_estimate_usd` on its own, and the task
  total stays unknown.
- `harness_result.cost_usd` is reported only as `provisional_completion_cost_usd`.
  It is never final spend and never merged into the provider total.

## Duration: process lifetime is not active work

`exited_at - launched_at` per `(spawn, session)` is reported as
`process_lifetime_ms` and labelled process lifetime. A Claude process can sit
paused awaiting verification or the operator for most of it, so
`active_work_ms` is **always null** with `active_work_coverage` stating why. No
observation available today distinguishes model-active time from paused time,
and the report will not manufacture one.

Phase spans are bucketed separately: `work_phases_ms`, `verification_ms`
(the admission/check span plus its own queue wait), `attention_hold_ms` (a
human wait), and `queue_wait_ms` (every other phase's pre-phase wait).

Spans are pre-filtered by repo scope and window before aggregation, because
`critical_path::build_critical_path` dedups on `(phase, attempt)` and does not
filter by scope: two repositories with an identically named task id would
otherwise merge silently.

## Deliveries come from frozen scope, not from surviving pairs

`deliveries` is derived from `manifest.consumer_tasks` plus the consumer tasks
of predeclared `eligible_pairs` (`enrollment: frozen_consumer_task` or
`fixture_pair`). A frozen task that produced no eligible source and no receipt
therefore still reports its generations, attempts, failures, costs and
acceptance instead of disappearing. A frozen task with no native record at all
is named in `tasks_without_native_records`.

Every attempt is retained: `generations[].completions[]` keeps each
`harness_result` with its `declared_done`/`is_error`/`failed` flags, and
`generations[].launches[]` keeps each physical launch with its exit code,
`crashed` marker and lifetime.

`accepted` is `true` only on an actual `delivery_closure` span in this repo and
window. A `merge` span alone is not proof — a merge can be reverted — and
absence is unknown, not a negative.

## A bad claim removes the claim, not the opportunity

Two separate lists, and the distinction is the point:

- `excluded` — this was never a distinct valid opportunity: `duplicate`,
  `source_not_captured`, `invalid_source`, `invalid_source_evidence`, `self`.
- `rejected_claims` — the opportunity stands, the receipt does not:
  `wrong_repo`, `wrong_generation`, `invalid_evidence`, `unresolved_evidence`,
  `undated`, `future_source`. The pair stays in `eligible` with no claimed
  outcome, so a malformed or foreign receipt cannot erase a real opportunity
  from the discovery or verified-reuse denominators.

`discovery` reports the denominator explicitly rather than hiding it inside a
rate, and `discovery.rate`/`verified_reuse.rate` are `null` — not `0.0` — when
there is no denominator. An absent denominator is not a zero numerator.

`verified_reuse` uses the same `verified_effect` gate the mechanism goal
counts, so the two can never disagree about what "verified" means. That gate
requires: outcome `used`/`adapted`, an operator verdict of `verified`, a known
temporal order, `coverage_status == "prepared"`, a launched consumer, resolved
source evidence and a surviving receipt. `counts_as_effect` additionally
requires that the operator did not relay the pointer.

## Reviewed annotations are operator judgment, labelled as such

Operator-supplied `coverage: {"status": "prepared", ...}` is reported as
`prepared_reviewed` with `coverage_provenance: "reviewed"` and a
`coverage_reference_resolved` flag; it is never merged into native prepared
coverage. Its reference is checked: non-blank and bounded, never an arbitrary
unchecked string.

The reviews file may also be an object carrying task-scoped annotations for the
observations telemetry cannot supply:

```json
{
  "pairs": [ /* as below */ ],
  "tasks": [
    {
      "task": "TKT-...",
      "repo": "rat-kingdom",
      "interventions": [
        {"kind": "operator-steer", "reference": "<tuple-id-or-artifact>",
         "note": "re-scoped by hand; no attention_hold span exists"}
      ],
      "repeated_investigations": [{"kind": "repeat-investigation", "reference": "..."}],
      "rework": [{"kind": "manual-rework", "reference": "..."}]
    }
  ]
}
```

Each annotation needs a `kind` and a `reference`, and its task must already be
frozen in the manifest — a retrospective annotation cannot widen scope. There
is deliberately **no time-saving field**, and `deny_unknown_fields` makes adding
one a hard input error: an agent's estimate of time saved is not a measured
saving.

`quality.attention_hold_spans` is reported as an explicit **lower bound** on
operator interventions, with `interventions_coverage` saying so. An
intervention that left no span is present only as a reviewed annotation.

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

`repos` and `window` are now ENFORCED, not decorative. Every native
observation outside them is excluded and counted under `capture`
(`observations_out_of_scope_repo`, `observations_out_of_window`,
`observations_undated`), so a mis-set window shows up as visible coverage loss
instead of silently shrinking every metric. Source findings are deliberately
*not* window-filtered: an older source that a batch consumer reused is exactly
what the experiment is looking for, so it is retained as linked context.

Unknown manifest fields are rejected, so a misspelled `"windwo"` is an error
rather than a silently ignored key. A fixture `eligible_pairs` entry must also
declare the **same repo as its batch**; naming a known batch id is not enough.

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
    "coverage": {"status": "prepared", "evidence": "<tuple-id or operator reference>"},
    "author_terminal_evidence": "<agent_exit-tuple-id, or omit>",
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
- **`author_terminal_evidence` must be an `agent_exit`, not an assertion and
  not a completion.** See "Author exit" above: a reviewer boolean cannot
  establish it, and neither can a `harness_result` (emitted at `rk done`, while
  the process is still alive) or an `agent_lifecycle` (no `spawn` binding at
  all). If it does not resolve, the report lists it under
  `author_exit_unsupported` with the reason and does not credit it.

  **This is a known live gap, stated rather than worked around:** no producer
  for `agent_exit` exists in code yet — it is S2's, contracted in
  `docs/2026-09-13-s2-native-observation-and-export-contract.md` and not
  implemented. Until it lands, `author_exit_effects` will correctly read 0 for
  every batch and the mechanism goal cannot be met. That is the honest reading,
  not a reason to relax the rule.

## Reading the report

Every count the design doc requires is a top-level field: `eligible`,
`excluded` (with `pair`/`reason`/`detail` — duplicate, self, future_source,
wrong_generation, wrong_repo, missing_evidence, source_not_captured),
`rejected_claims`, `discovery`, `presented_native`, `presented_reviewed`,
`opened`, `claimed`, `assessed`, `outcome_classes`, `unknown_coverage`,
`ambiguous_assessments`, `author_exit_unsupported`, `verified_reuse`,
`author_exit_reuse`, `mechanism` (the frozen 3-effects/2-batches/1-author-exit
threshold), `deliveries` (cost/duration per task, each field an explicit `null`
rather than a manufactured zero when the underlying tuple wasn't captured),
`invalid_records`, `unresolved_records`, `capture`, and `quality`.

Compare `evaluator_version`, not just `schema_version`, when diffing two
reports: the rules above are version 2, and a version-1 report of the same
inputs is not comparable.

Run it twice over the same three files and expect byte-identical JSON —
that determinism is covered by
`bbs_report::tests::verified_used_effect_counts_and_is_deterministic` and
the CLI-level `same_inputs_produce_deterministic_repeated_output`.

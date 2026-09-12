# King/BBS collaboration repeat trial: pre-registered manifest and operator hand-off

Date: 2026-09-12
Ticket: TKT-jafah-sinod-matum
Result: **repair and repeat / operator hand-off** — not qualified. No implementer,
consumer, or observation window was started against the live fleet.

## What this ticket asked for

Per the ticket body, all ten repair dependencies are closed and the operator
preflight recorded in artifact `01M2B0AVTCMVGA2DJP3W2DMP6P` /
`/Users/chazu/.codex/artifacts/rk-batch-continuation-20260912T123110Z` is
complete (main/origin/installed `rk`+`rk-mcp`, running daemon `72911dd1d3825e2eea198dbd515907887f0566c1`,
both repository landing queues empty, trigger audit clean, King registered by
stable session identity). The ticket asks this dispatch to pre-register an
exact fresh bounded workload and dependency graph, then run the full trial:
two independent implementers with a real BBS interface question/answer/
acceptance, a dependent consumer that only starts once both land, controlled
verification contention and held-delivery recovery, at least two repository
scopes to prove landing-queue isolation, King-conversation availability
checks, complete reconciliation against authoritative RK state, and a
published pass/fail report — using `max_landing_age_secs=1200` per the
revised landing allowance.

## Why this dispatch cannot run the trial itself

The trial's core mechanics require RPCs this dispatch is not authorized to
call, verified directly against the daemon's own authorization table rather
than inferred:

`crates/rk-daemon/src/capabilities.rs:69-113` (`method_policy`) is an
explicit allow-list that **fails closed for every method it does not name**.
Checking the methods the trial needs:

- `agent.spawn` (and `agent.respawn`/`dismiss`/`interrupt`/`steer`) resolve to
  `FOREMAN_CHILD` — granted only to a caller the supervisor recognizes as
  foreman of that specific child, never to an ordinary agent (line 76-78).
- `workflow.run` and `repo.add` are **not listed anywhere** in the table, so
  `method_policy` returns `None` for them — `authorize_reasoned`
  (`crates/rk-daemon/src/server.rs:2581-2583`) then rejects with
  `operator_only_method` for any non-operator caller, unconditionally.
- `rk observe start` is external tooling that drives its own bounded
  observation window over repository state; it is not something an
  in-worktree agent dispatch owns or is positioned to launch and babysit for
  the trial's required duration.

Basil-14 is an ordinary agent caller (`RK_AGENT` set, no foreman grant over
any other rat, no operator token). It cannot spawn the two independent
implementers, cannot spawn the dependent consumer, cannot register or spawn
into the second repository scope, and cannot start or hold open the
observation window. This is a hard capability boundary, not a missing tool
to route around.

This matches every historical run of this exact trial shape: the original
run (`01M28PQ3Z86GZTVFJKVQKFSS5H`,
`/Users/chazu/.codex/artifacts/rk-flow-trial-20260911T170220Z`) and the
subsequent activation/continuation batches
(`rk-batch-continuation-20260912T123110Z`, `rk-trial-fixes-dispatch-20260912T005244Z`,
`rk-direct-recovery-20260912T020207Z`) were all conducted by the operator's
own `castle-48451de05dc5e21a` session outside any git worktree, never by a
ticket-dispatched rat. The code-level restriction and the historical
execution pattern agree.

The ticket's own prerequisites also caution that filing/holding this ticket
"does not authorize redeployment, additional workers, external credentials,
or a wider backlog drain." Given this dispatch also cannot honor the trial's
own contention-control, reconciliation, and observation obligations, seeding
the implementer/consumer sub-tickets from here — even though `ticket.new`
with `--depends-on` is within an ordinary agent's grant — would risk
triggering exactly that uncontrolled, unobserved fan-out without the ability
to close the loop. It is left to the operator or a foreman-authorized
session that can also run the observation window.

## Pre-registered immutable manifest

This is the exact bounded workload/graph the next execution must use,
carried forward from the original manifest schema
(`observation/manifest.json` in the original run) with the deltas the ticket
requires. The original failed run and its manifest are left untouched.

```json
{
  "schema_version": 1,
  "name": "king-conversation-bbs-repeat-trial",
  "supersedes_run": "01M28PQ3Z86GZTVFJKVQKFSS5H",
  "repos": ["rat-kingdom", "grmpl"],
  "workload": {
    "primary_scope": "rat-kingdom",
    "implementer_a": {
      "role": "producer",
      "deliverable": "a small, real interface contract (e.g. a struct/trait or wire field) that implementer_b's consumer will need",
      "must": "post the contract on BBS as a bbs.answer to implementer_b's bbs.ask, then receive requester bbs.accept"
    },
    "implementer_b": {
      "role": "producer",
      "deliverable": "an independent unit of work that does not depend on implementer_a, dispatched concurrently",
      "must": "open the bbs.ask that names the exact interface question implementer_a answers"
    },
    "consumer": {
      "depends_on": ["implementer_a", "implementer_b"],
      "must_not_start_before": "both dependencies report delivered",
      "deliverable": "code that actually calls/uses the accepted contract, not a no-op stub"
    },
    "isolation_probe": {
      "scope": "grmpl",
      "deliverable": "one independent, concurrently-dispatched ticket unrelated to the rat-kingdom workload",
      "proves": "a grmpl completion cannot enter rat-kingdom's landing queue and vice versa; deployed triggers stay scoped; any invalid old queue entries are dispositioned with preserved evidence before isolation is claimed"
    }
  },
  "max_lineage_depth": 8,
  "max_lineage_tickets": 256,
  "interval_secs": 30,
  "rpc_timeout_secs": 5,
  "sample_timeout_secs": 20,
  "planned_duration_secs": 5400,
  "thresholds": {
    "stale_after_secs": 900,
    "max_landing_age_secs": 1200,
    "max_ready_age_secs": 900,
    "max_cost_usd": null,
    "max_unavailable_samples": 0,
    "max_reconcile_violations": 0,
    "max_forced_landings": 0,
    "max_duplicate_dispatches": 0,
    "max_duplicate_landings": 0,
    "max_unclassified_holds": 0,
    "progress_stall_after_secs": 900,
    "max_wait_secs": 1800
  },
  "baseline_at_registration": {
    "daemon_commit": "72911dd1d3825e2eea198dbd515907887f0566c1",
    "landing_queues": "both repos empty per operator preflight",
    "king_session": "registered by stable session identity, 6 live revision samples preserved registration with no injected wake",
    "authority": ["01M2B0AVTCMVGA2DJP3W2DMP6P", "/Users/chazu/.codex/artifacts/rk-batch-continuation-20260912T123110Z/repeat-trial-policy-amendment.json"]
  },
  "acceptance_criteria": [
    "real BBS ask/answer/accept exchange between implementer_a and implementer_b, not a rephrased duplicate of the original run's exchange",
    "consumer ticket does not start dispatch until both implementer_a and implementer_b report delivered",
    "at least one controlled verification-contention or held-delivery-recovery event is exercised and recovered without duplicate dispatch or a manufactured commit/target change to escape the hold",
    "grmpl isolation_probe completion never appears in rat-kingdom's landing queue or vice versa; trigger audit and invalid-queue-entry disposition are re-verified, not assumed from the preflight",
    "King conversation stays available throughout; historical delivered needs are not re-surfaced as new; one unchanged held incident produces one decision, not repeated ones; at least one real new operational/human decision is surfaced and recorded with any operator intervention",
    "complete bounded source coverage (0 max_unavailable_samples, 0 max_reconcile_violations) with delivery/hold/worker-generation/cost reconciled against authoritative RK state — missing evidence is recorded as failed/incomplete, not zero incidents",
    "published report states an exact pass/fail per criterion above, including limitations"
  ]
}
```

## What would unblock execution

An operator, or a foreman-authorized session with `agent.spawn`/`workflow.run`/
`repo.add` grants and the ability to run `rk observe start --max-landing-age 20m`
end to end, executes this manifest directly. This dispatch's contribution is
the frozen manifest above plus the capability-boundary finding; it does not
spawn the workload itself.

## Evidence retained

- Original failed run preserved untouched: `01M28PQ3Z86GZTVFJKVQKFSS5H`,
  `/Users/chazu/.codex/artifacts/rk-flow-trial-20260911T170220Z`.
- Activation/preflight evidence: `01M2B0AVTCMVGA2DJP3W2DMP6P`,
  `/Users/chazu/.codex/artifacts/rk-batch-continuation-20260912T123110Z`.
- Daemon authorization source read directly:
  `crates/rk-daemon/src/capabilities.rs`, `crates/rk-daemon/src/server.rs`
  (`authorize_reasoned`).
- No spawn, workflow, repo-registration, or observation call was attempted
  from this dispatch; no credentials or live fleet state were altered.

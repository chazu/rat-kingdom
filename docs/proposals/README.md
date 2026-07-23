# Workflow-library review proposals (rat-29)

Revised `.cue` definitions proposed by the `workflow-review` pass over
`crates/rk-workflow/src/schema.cue` + every file in `examples/workflows/`.
These are **proposals** — the shipped files under `examples/workflows/` are left
untouched. Each revised file carries a `PROPOSAL (rat-29 …)` header block
explaining the delta and its rationale. All four validate against the embedded
schema (same `cue export -e workflow` path `rk_workflow::load` uses).

## Findings acted on

1. **`land` result is ungated** in `steward` and `land-on-approve` (real defect).
   A `land` merge conflict / moved target is a clean `{merged: false}` in
   `ctx.previousResult`, **not** an error — so the shipped APPROVE paths complete
   the instance as if the work merged, silently leaving the branch unmerged with
   no operator signal. `schema.cue` #LandStep explicitly prescribes gating with a
   following `evaluate {expect: {merged: true}}`. Both proposals add it (fail
   closed → surfaces the stuck auto-merge in `rk inbox`).
   → `docs/proposals/steward.cue`, `docs/proposals/land-on-approve.cue`

2. **No per-instance `budget` cap** on the fan-out / unattended workflows
   (missing guardrail). The schema's `#WorkflowBudget` (`budget: {max_usd}`) is
   enforced end-to-end (TKT-32: `workflow.budget → instance_max_usd →
   check_dispatch_budget`, re-checked in `fan_out`), yet no shipped fan-out sets
   one. Added to `backlog-drain` and `nightly-self-improve` (the parallel and the
   scheduled-overnight cases most able to run up spend). Same change applies to
   `cost-tiered-drain`.
   → `docs/proposals/backlog-drain.cue`, `docs/proposals/nightly-self-improve.cue`

3. **Dead `repo` param** in the for_each workflows (correctness / clarity). The
   for_each scope is always the workflow's own repo (schema #TicketQuery) and the
   instance repo comes from the run context (`execute(&id, workflow,
   &snapshot.repo)`), never from a `repo` param — so `--param repo=…` in the
   shipped docs is inert and misleads. Removed from the two proposals above; also
   applies to `cost-tiered-drain` and `backlog-groom`.

## Also flagged (ticket only, no proposal — judgment call)

- **`backlog-drain` all-or-nothing merge**: the `evaluate {all_ok: true}` before
  `dismiss_all` means a single failed rat parks *every* sibling branch unmerged.
  `dismiss_all` already treats a per-branch conflict as a clean `merged: false`
  (not an error), so it could run unconditionally and merge the good branches,
  leaving only failures parked. This trades atomic-batch safety for throughput —
  filed for a decision rather than forced into a proposal.

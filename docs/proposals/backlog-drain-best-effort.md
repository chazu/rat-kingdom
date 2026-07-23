# Proposal: best-effort merge for the drain fan-outs (TKT-47)

Resolves the item the rat-29 workflow-review left as *"Also flagged (ticket
only, no proposal — judgment call)"* in `docs/proposals/README.md`: the
all-or-nothing merge in `backlog-drain` / `cost-tiered-drain` /
`nightly-self-improve`. This is the design writeup the ticket asked for
(`TKT-47`, *"filed for a decision rather than forced into a proposal"*). No
shipped file is changed by this doc; it exists so the maintainer can pick a
direction. The end-state `.cue` and code deltas are spelled out below so that,
once a direction is chosen, applying it is mechanical.

## The problem, as filed

All three drains end their fan-out phase with the same three steps:

```cue
{type: "wait_all", timeout: _input.timeout},   // → {count, ok, errors, all_ok, results}
{type: "evaluate", expect: {all_ok: true}},    // aborts the instance if ANY rat errored
{type: "dismiss_all"},                          // merge every parked branch
```

`evaluate {all_ok: true}` returns an error when any single rat reports
`is_error: true`, which aborts the run **before** `dismiss_all`. So one failed
rat parks *every* sibling branch — including the rats that finished cleanly —
and (for `nightly-self-improve`) also skips the downstream REFINE phase. The
batch is atomic: all merge or none do.

## The subtlety the original framing missed

The rat-29 note reasoned:

> `dismiss_all` already treats a per-branch conflict as a clean `merged: false`
> (not an error), so it could run unconditionally and merge the good branches,
> leaving only failures parked.

That is **not** what a bare reorder produces, and the distinction is the whole
decision. `dismiss_fanout` (`crates/rk-daemon/src/workflow_exec.rs`) dismisses
*every* agent in the fan-out set, and `Supervisor::dismiss(name, no_merge=false)`
(`supervisor.rs:1163`) merges that agent's branch **unconditionally** — it never
consults the rat's `harness_result` or `is_error`. `dismiss_all` has no notion
of a "good" versus a "failed" rat; the two kinds of failure are also distinct:

- **rat errored** (`is_error: true` in `wait_all`'s aggregate) — the *task*
  failed. This is what the `all_ok` gate keys on.
- **merge conflict** (`merged: false` from `dismiss`) — the *branch* would not
  fast-forward/3-way cleanly. This is what `dismiss_all` tolerates.

So the `all_ok` gate is the *only* thing today that stops a broken rat's branch
from being auto-merged alongside the good ones. A bare reorder —

```cue
{type: "wait_all"},
{type: "dismiss_all"},                          // merges the FAILED rat's branch too
{type: "evaluate", expect: {all_merged: true}},
```

— would merge the failed rat's (possibly broken) branch, which is **strictly
worse** than today. Genuine "merge the clean ones, park the failures" therefore
cannot be expressed by reordering steps: `dismiss_fanout` has no per-agent
merge/park signal to act on. It needs new machinery.

## Options

- **A — Keep atomic-batch (status quo).** One failure parks the whole batch.
  Safe; a partial-broken batch never half-merges. Low throughput; a single flaky
  rat wastes every sibling's work until a human re-runs.
- **B — Bare reorder.** Rejected: merges failed rats' branches (see above).
  Worse than A on the exact axis (bad merges) the gate exists to protect.
- **C — Opt-in best-effort merge (recommended).** Teach `dismiss_all` to merge
  only the branches of rats that finished clean and park the rest, gated behind a
  new `onlyClean` flag so the default stays A. Then `evaluate {all_merged: true}`
  surfaces any parked failure in `rk inbox` without discarding the clean merges.
- **D — Escalation.** On `all_ok: false`, re-dispatch only the failed tickets
  (a second fan-out) before merging. Higher spend, out of scope here; C is the
  prerequisite primitive for it anyway.

## Recommended design (Option C)

Make best-effort **opt-in**, so nobody's current behavior changes silently and
the maintainer chooses per-workflow which default the shipped examples ship with.

### 1. Schema — `crates/rk-workflow/src/schema.cue`, `#DismissAllStep`

```cue
#DismissAllStep: {
	type: "dismiss_all"
	noMerge?: bool
	// When true, merge only the branches of rats that finished clean
	// (is_error:false in the preceding wait_all) and park the rest with
	// noMerge, instead of failing the batch on the first error. Requires a
	// preceding wait_all in the same instance (its per-agent results supply
	// the clean/failed signal). Default false = atomic-batch (today).
	onlyClean?: bool
}
```

### 2. Executor — `dismiss_fanout` in `workflow_exec.rs`

`wait_all`'s aggregate already lands in `ctx.previous_result` as
`{count, ok, errors, all_ok, results}`, and each element of `results` is a
`harness_result` payload carrying its own `agent` and `is_error`. When
`onlyClean` is set, build the set of clean agents from that aggregate and pass a
per-agent `no_merge` into each `dismiss`:

```rust
// clean = { agent : results[i].is_error == false }
let no_merge = dismiss_all.no_merge || (dismiss_all.only_clean && !clean.contains(&fa.agent));
```

A parked (failed) rat is dismissed with `no_merge=true`: its child is killed and
its worktree removed exactly as today, but its branch is preserved for review
rather than merged. Extend the aggregate with a `parked` count so a following
`evaluate`/report can distinguish "conflicted" from "held back because the rat
failed":

```
{count, merged, parked, errors, all_merged, results}
```

`all_merged` stays `merged == count`, so `evaluate {all_merged: true}` still
fails the instance (→ `rk inbox`) whenever anything was parked or conflicted —
the clean branches are already merged by then, so the surfaced failure no longer
costs the batch.

If `onlyClean` is set with no preceding `wait_all` (no aggregate in
`ctx.previous_result`), fail the step with a clear error rather than silently
merging everything — the flag is meaningless without the join's per-agent signal.

### 3. Workflow step lists (all three drains)

```cue
{type: "wait_all", timeout: _input.timeout},
// Best-effort: merge every clean rat's branch now, park the failures for review.
{type: "dismiss_all", onlyClean: true},
// Report/surface: fails the instance (→ rk inbox) if anything was parked, but
// only AFTER the clean branches have already merged.
{type: "evaluate", expect: {all_merged: true}},
```

For `nightly-self-improve` this also lets the REFINE phase run on nights where
some drain rats failed — it now sees a freshly-merged set of clean work plus the
parked failures, which is exactly the pain REFINE is meant to mine.

## Tradeoff for the maintainer to settle

The one decision left is **the default for the shipped examples**: keep
`onlyClean` unset (atomic-batch — a conservative, surprise-free default; opt into
throughput per run) versus ship the drains with `onlyClean: true` (throughput by
default — a flaky rat never strands the batch, at the cost that a half-broken
night still lands its clean half). The primitive is the same either way; only the
example defaults differ. Recommendation: land the opt-in primitive first with the
default **off**, then flip the shipped drains to `onlyClean: true` once the
best-effort path has a green e2e (extend `crates/rk-daemon/tests/backlog_drain.rs`
with a mixed clean/failed fan-out asserting the clean branches merged and the
failed one parked).

## Why this is a proposal, not a live edit

Changing the auto-merge semantics of three workflows plus `dismiss_fanout` alters
what lands on `main` unattended — hard to reverse and outward-facing. Consistent
with the rest of `docs/proposals/` (shipped files untouched; maintainer applies),
implementation is deferred pending the default-choice above.

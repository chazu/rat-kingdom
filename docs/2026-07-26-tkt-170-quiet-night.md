# TKT-170 — a quiet night is a completion, not a failure

**Status**: fixed in `crates/rk-daemon/src/workflow_exec.rs`. Regression tests
`crates/rk-daemon/tests/quiet_night.rs`.

## What was asked

```
workflow_failed 2026-07-25T00:06:18 wf-d26pcspewf:
  'wait_all step with no fan-out agents (missing or empty for_each)'
```

`nightly-self-improve` fired on a night whose ready queue was already drained.
Nothing went wrong — there was simply no work — and the run was recorded as an
instance failure, which put an operator-attention item in `rk inbox` with
nothing to attend to.

## What actually happened

`for_each` runs `query_tickets`, gets an empty list, logs "matched no tickets",
and stores an empty fan-out set. The `wait_all` that follows refused it:

```rust
if fanout.is_empty() {
    return Err(rk_core::Error::other(
        "wait_all step with no fan-out agents (missing or empty for_each)",
    ));
}
```

`dismiss_all` carried the identical guard. The parenthetical is the whole bug —
the guard conflated **missing** with **empty**, two states with opposite
meanings:

- *missing* — no `for_each` ran before this step. The workflow is malformed and
  the instance should fail, loudly, so the author fixes the definition.
- *empty* — a `for_each` ran and its query matched nothing. That is a quiet
  night: the correct outcome is to join nothing and merge nothing.

Because a bare `Vec` cannot tell those apart, the second was punished with the
first's error. The cost was not only the spurious inbox item: `nightly-self-improve`
runs GROOM → DRAIN → REFINE in one instance, so failing at the drain's `wait_all`
**skipped the refine phase entirely**. Every quiet night silently dropped the
phase that mines the fleet's obstacles and proposes prompt/convention edits.

## The fix

`WorkflowContext.fanout` becomes an `Option<Vec<FannedAgent>>`, so the type
carries the distinction the guard needed:

| state | meaning | `wait_all` / `dismiss_all` |
| --- | --- | --- |
| `None` | no `for_each` ran | fail: *"no preceding for_each"* |
| `Some([])` | `for_each` matched nothing | no-op |
| `Some([a, b])` | fan-out of 2 | join / merge both |

`for_each` writes `Some(fanned)` even when the query matched nothing — an empty
set is still a set, and recording it is what tells the following step that a
fan-out ran. `dismiss_all` clears back to `None` (not `Some([])`) when it spends
the set, so a later `wait_all` with no `for_each` of its own is an authoring
error again rather than a second quiet night.

Neither aggregate needed special-casing: `wait_all` over zero agents already
falls out as `{count: 0, ok: 0, errors: 0, all_ok: true, results: []}` and
`dismiss_all` as `{count: 0, merged: 0, parked: 0, errors: 0, all_merged: true}`.
Both are vacuously true, which is what a following `evaluate {all_ok: true}` or
`evaluate {all_merged: true}` should see on a night with nothing to judge. So
the nightly chain now runs GROOM → (empty DRAIN) → REFINE and completes.

Two things were deliberately *not* relaxed:

- `dismiss_all onlyClean` still requires a preceding `wait_all`. The check runs
  before the empty short-circuit, so an author who wires `onlyClean` without a
  join is caught on a quiet night too, instead of the mistake lying dormant
  until a night that actually has tickets in it.
- `for_each` matching nothing was downgraded from `warn!` to `info!`. It is a
  normal outcome now, and a warning for a normal outcome is the log-level
  version of the same bug.

## Regression tests

`crates/rk-daemon/tests/quiet_night.rs` runs the full nightly shape (single-spawn
groom → fan-out drain → single-spawn refine) against an **empty** ready queue and
asserts the instance completes *and* that the refine phase's branch landed on
main — i.e. the steps after the empty fan-out actually ran. The second test pins
the other half: a workflow whose only step is a `wait_all` still fails, with an
error naming the missing `for_each`.

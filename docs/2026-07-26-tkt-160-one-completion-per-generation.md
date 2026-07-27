# TKT-160 — one `harness_result` per agent generation, not one per turn

**Status**: fixed (`crates/rk-daemon/src/supervisor.rs`), regression test
`crates/rk-daemon/tests/mid_flight_result.rs`.

## The defect

A harness returns control once per **turn**, not once per task. A background
test suite finishing, a re-armed monitor, or a task notification each end a turn
and produce a `HarnessEvent::Completed`, and `Supervisor::route_completion`
published every one of them as a durable `Event/harness_result`.

Nothing in the payload distinguished a mid-flight turn from a real finish — keys
were `agent, branch, cost_usd, is_error, parent, result, role, task, tokens`,
and `is_error` was `false` on both.

Measured by Peppercorn-2 against the live fleet
(`scripts/rk-name-generations-audit.py`, section 4;
`docs/2026-07-24-tkt-148-name-generation-audit.md`): **12 `(identity, agent)`
groups hold more than one `harness_result` within a single generation** — Rizzo
×5 for TKT-116; Methuselah, Rizzo-2, Squeak-2 ×3; Asiago, Bristle, Bristle-2,
Django, Marbles, Pretzel, Remy, Twitch-2 ×2.

Because `store.query` resolves `ORDER BY id ASC LIMIT 1`, every reader keyed on
the agent name took the **oldest** match, which for 9 of the 12 is a mid-flight
message:

- Rizzo / TKT-116 — "the full cargo test --workspace pass is still running"
- Methuselah / TKT-90 — "I will wait for the monitor to report the test results"
- Squeak-2 / TKT-113 — "Tests still compiling/running"

Worst case, 3 of those 9 are **reviewers whose verdict lands in a later tuple**
— Remy/steward-review-TKT-116 (APPROVE in tuple 2), Bristle-2 and
Twitch-2/steward-review-TKT-113 (REWORK in tuple 2) — so a `wait` handed
`evaluate` a result with no verdict in it at all.

This is the TKT-146 signature (wait satisfied by the wrong tuple → evaluate
judges the wrong text → dismiss fires behind it) surviving TKT-146 by a
different mechanism. `Pattern::after(RecordId::floor_at(created_at))` separates
**generations** (measured min gap 44.5h); it does not separate **turns**, which
are milliseconds apart.

## Why the fix is in the supervisor, not in `wait`

`wait` is not the only consumer of `harness_result`. The same event drives:

- the reactor's `steward-on-completion` trigger (`"role":"rat"`), which would
  spawn a reviewer and run the auto-merge gates against a branch whose rat is
  still writing to it;
- `route_completion`'s ticket auto-close, which marked a ticket `done` on a
  mid-flight turn;
- the parent's `child_completed` message and `rk observe`'s finished count.

Fixing only `wait` would leave all of those reading a turn. So the gate is at
the producer: **a generation publishes exactly one `harness_result`, the one it
finished on.**

## The rule

A turn result is published only when something proves no further turn can
follow. There are exactly three such proofs, and they are complete:

1. **The agent said so.** `rk done` writes exactly one `task_done` per
   generation — the one signal a harness cannot duplicate, because the rat
   writes it, not the harness. Every spawned role (`rat`, `reviewer`, and the
   `_` fallback in `rk_core::prime::render`) is primed with `rk done` as its
   mandatory final step. Checked with `Pattern::for_agent_since`, so the lookup
   is generation-bounded like every other name-keyed read (TKT-159).
2. **The process is gone.** Handled at `HarnessEvent::Exited`, which the runner
   guarantees is the final event. Harnesses that end with the run (codex, axe,
   the test fake) take this path for every agent. A Claude session does **not**:
   `claude -p --input-format stream-json` stays alive between turns to receive
   steers, which is why process exit alone could not have been the signal.
3. **The turn failed.** `is_error: true` ended the session, so there is no later
   turn to prefer, and withholding it would turn a fast, legible failure into a
   `wait` timeout.

Only proof 1 is also a proof the rat *finished* — a distinction this document
originally elided, and which TKT-175 later made load-bearing: see the closing
section.

Bookkeeping lives in `Supervisor::completions` (`CompletionState`: generation,
`routed`, `withheld`). `claim_completion` applies proofs 1 and 3 at
`Completed`; `flush_withheld_completion` applies proof 2 at `Exited`;
`forget_completion` drops the state on `dismiss` (a deliberate teardown must not
emit a late completion that re-fires the steward on a just-merged branch) and on
`respawn` (which continues the *same* generation in a fresh process, so the
crashed run's `routed` flag would otherwise gag the resumed run).

The `task_done` lookup **fails open**: an unreadable space publishes, which is
the pre-fix behaviour. Withholding on a storage error would strand every
workflow waiting on that agent until its step timeout.

## Operator-visible changes

- A rat that finishes a turn without `rk done` and keeps working no longer
  reports; its workflow `wait` stays blocked until it really finishes. That is
  the point, but it means **a rat that never runs `rk done` and never exits now
  reaches its `wait` timeout instead of silently passing an `evaluate`**. A
  timeout is a loud failure in `rk inbox`; the old behaviour was a silent wrong
  answer. TKT-147 (unmerged at time of writing, `64de183`) fails such a wait
  faster when the rat is crashed or abandoned.
- The steward now fires once per rat, on its real completion, instead of once
  per turn.
- A ticket is marked `done` on the finishing turn, not on the first turn that
  happened to end mid-work.
- Latency for the normal path is unchanged: a rat's `rk done` precedes the turn
  end, so the `task_done` is already in the space when its `Completed` arrives.

## Known residue (not fixed here) — closed by TKT-173 and TKT-175

> An agent killed *after* a clean mid-flight turn (budget hard-stop) still
> publishes that turn's text with `is_error: false` when its process exits,
> because the record's state is `Completed` from that turn. This is unchanged
> from the pre-fix behaviour (the turn was published even earlier before), and it
> is the same seam TKT-147 addresses with a liveness gate — worth revisiting once
> `64de183` lands.

Resolved in two steps, and the second reframed the first.

**TKT-173** took the residue at face value and fixed the killed case, reading
"killed" off the exit status (`status.code()` is `None` for a signal-terminated
child; the budget hard-stop and the sweep's hard escalation both SIGTERM the
process group). It also added `declared_done` to every `harness_result`, so a
reader could ask whether the rat said it was finished rather than infer it.

**TKT-175** found the framing wrong. The question was never how the process
ended. Reaching the exit-flush at all is the proof: a turn result is withheld for
exactly one reason — proof 1 above did not arrive — so anything flushed there
belongs to a generation that never wrote a `task_done`, and by the fleet's own
completion protocol that generation did not finish its task. A rat that exited 0
mid-task was telling the same lie as the killed one, just more quietly, and every
workflow gating on `expect {is_error: false}` believed it. So the flush now
publishes `is_error: true` unconditionally. The turn's *text* is untouched, so
`rk inbox` still shows what the rat had got to.

This is a fleet-wide policy change, and its cost landed on the test surface: 29
tests across 17 binaries were modelling rats that skip `rk done` and asserting
that was a clean finish. They now declare done as a real primed rat does —
`crates/rk-daemon/tests/fixture/mod.rs` and the `rk-fixture-done` helper binary
exist for that, because a fixture cannot write the tuple from the test process
(`Pattern::for_agent_since` bounds it to a generation whose name does not exist
until after the spawn, and a `for_each` fan-out names its rats itself). Which of
the two things a fixture means — a rat that finished, or one cut off mid-task —
used to be invisible in these scripts; it is now one line of each.

`declared_done` survives the flip. On the exit path it no longer carries a fact
`is_error` lacks, but it still discriminates on the other: a turn that errored
out is undeclared too, so a reader that wants "the rat said it was done" rather
than "nothing went wrong" reads it instead of parsing prose.

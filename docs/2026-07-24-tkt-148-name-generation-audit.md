# TKT-148 — audit of the agent-name generations duplicated by TKT-136

**Date:** 2026-07-24 · **Rat:** Peppercorn-2 · **Branch:** `rat/peppercorn-2/tkt-148`
**Tool:** [`scripts/rk-name-generations-audit.py`](../scripts/rk-name-generations-audit.py) (read-only, re-runnable)
**Measured against:** the live fleet `RK_HOME` (`~/.rat-kingdom`) at 2026-07-25T02:30Z —
272 agent records over 248 distinct names.

## Why this ticket existed

TKT-136 narrowed `Registry::reserve_name` to the LIVE map, so archiving a rat
returned its name to the pool. TKT-146 (`afaa8a0`) restored the invariant that a
name is an identity key and is never recycled, but it could not retract the
records the recycled names had already been stamped into. TKT-148 was filed to
decide what to do about that residue: **leave it, rename the archived
generations, or make `AgentLog` generation-aware retroactively.**

The ticket asked for a decision. It also stated two consequences as fact. Both
turned out to be worth measuring rather than assuming, and one of them is false.

## Decision

**Leave the historical data in place, documented. Do not rename the archived
generations. No migration is required.**

The three findings below are the basis. The forward-looking code fix
(generation-aware `AgentLog` keying) is TKT-158 and remains worth doing on its
own merits — but it has **nothing historical to migrate**, which is the main
input this audit hands to it.

## Finding 1 — the residue is 24 names, and it is inert

24 names carry exactly two generations each; no name carries three. Every first
generation is `dismissed` and archived; every second generation is the live
record. The full set:

> Brie, Bristle, Cheddar, Colby, Fidget, Gnaw, Gouda, Munch, Nezumi, Nibbles,
> Peppercorn, Pip, Ratatosk, Remy, Rizzo, Sable, Scamper, Scurry, Splinter,
> Squeak, Stilton, Templeton, Twitch, Whisker

This confirms the scope correction Colby-2 measured (24, not the 4 the parent
ticket named) and adds the distribution: the two generations are separated by
**44.5–50.1 hours** — first generations run 2026-07-22T20:46Z → 2026-07-23T04:16Z,
second generations 2026-07-24T19:16Z → 2026-07-25T02:09Z.

That gap matters. TKT-146 bounds the three known blocking reads with
`Pattern::after(RecordId::floor_at(record.created_at))`. A ≥44-hour separation is
far larger than any workflow `wait` timeout, so **that bound fully disambiguates
every one of the 24 cross-generation pairs.** The residue cannot cause a
stale-match today through any of the fixed reads.

## Finding 2 — the stated interleaving consequence does not exist

The parent ticket says `rk log <name>` "interleaves two unrelated rats'
transcripts" for these names, and TKT-158 restates it for all 24. **Measured: 0
of 24 are interleaved.** 20 have a transcript containing only the second
generation's entries; 4 (Cheddar, Fidget, Munch, Scamper) have no transcript file
at all.

The cause is clean and checkable:

| | |
|---|---|
| `AgentLog` (`rk log`, TKT-25) landed in `main` | `da46c34`, **2026-07-23T12:13:50Z** |
| Oldest log entry anywhere in `agent-logs/` (243 files) | **2026-07-23T12:18:30Z** — 5 min after deploy |
| Latest of the 24 *first* generations | **2026-07-23T04:15:59Z** — ~8 h *before* the feature existed |

Every first generation of every duplicated name ran before per-agent transcripts
were being written at all. There is no first-generation transcript anywhere in
the fleet, so there is nothing for the second generation to interleave with.

The premise behind the finding is still true — `AgentLog::path_for` keys the file
on the name alone (`crates/rk-daemon/src/agent_log.rs`), exactly as Colby-2
found, and TKT-138 was closed because TKT-146 removed the *cause* rather than
because the keying was fixed. But the harm it would have produced never
materialised for this cohort, purely because of deploy ordering.

**Consequence for TKT-158:** generation-aware keying is a forward-looking
correctness fix, not a data-repair job. It needs no backfill and no migration of
existing `<name>.jsonl` files.

## Finding 3 — renaming would create inconsistency, not remove it

Option (b) from the parent ticket — rename the archived generations — is the one
option that makes things worse, because a rat's name is stamped into four
independent places and the registry is only one of them:

- durable tuple payloads (`harness_result`, `task_done`: `{"agent":"<name>"}`)
- git branches (`rat/<name>/<task>`)
- worktree paths (`worktrees/<repo>/<Name>`)
- `agent-logs/<name>.jsonl`

Renaming the registry record alone would leave the record disagreeing with the
tuples, branches and paths that still carry the old name — i.e. it would mutate
one side of a name binding without retracting the other, which is precisely the
mistake TKT-136 made. The residue is currently *consistent and stale*; renaming
would make it *inconsistent*. Doing it properly means rewriting durable tuple
payloads, which is not worth it for a set that is provably inert (Finding 1).

## Finding 4 — a live hazard the audit surfaced, NOT TKT-136 residue

This one is not historical and is handed off rather than fixed here.

29 `(identity, agent)` groups hold duplicate name-keyed durable tuples across
generations (expected — that is the residue). But **12 more hold duplicates
*within a single generation***, and those are not a naming problem at all:

```
harness_result  Rizzo       x5 within one generation
harness_result  Methuselah  x3      Rizzo-2  x3     Squeak-2  x3
harness_result  Asiago  Bristle  Bristle-2  Django  Marbles
                Pretzel  Remy  Twitch-2     x2 each
```

A harness that returns control more than once (a re-armed monitor, a background
test suite, a task notification) emits **one `harness_result` per turn**, not one
per task. `WorkflowEngine::result_pattern` builds a
`Pattern::category(Event).identity("harness_result")` with a payload search on
the agent name, and `store.query` resolves `ORDER BY id ASC LIMIT 1` — so it
selects the **oldest** result after the agent's `created_at` floor.

For **9 of those 12**, that oldest tuple is a mid-flight message, not a
completion. Verbatim first-results:

- `Rizzo`/TKT-116 — *"the full `cargo test --workspace` pass is still running"*
- `Methuselah`/TKT-90 — *"I'll wait for the monitor to report the test results."*
- `Squeak-2`/TKT-113 — *"Tests still compiling/running."*
- `Rizzo-2`/TKT-107, `Pretzel`/TKT-82, `Asiago`/TKT-84 — same shape

Three of the nine are **reviewer** agents whose actual verdict lands in a *later*
tuple, so a `wait` would hand `evaluate` a result containing no verdict at all:

- `Remy`/steward-review-TKT-116 — first: *"holding for the full-workspace `cargo test`"*; **APPROVE** is in the second
- `Bristle-2`/steward-review-TKT-113 — first: *"Interim state while I wait"*; **REWORK** is in the second
- `Twitch-2`/steward-review-TKT-113 — first: *"Suite still running."*; **REWORK** is in the second

This is the TKT-146 failure signature — `wait` satisfied by the wrong tuple,
`evaluate` judging the wrong text, `dismiss` firing behind it — surviving
TKT-146's fix through a different mechanism. **`floor_at(created_at)` is
necessary but not sufficient:** it separates generations, but not turns within
one generation.

**Scope of the claim.** This is demonstrated in the stored data — the tuple
`result_pattern` selects is a non-verdict interim message in these cases — but I
did not find a workflow instance that provably mis-evaluated because of it. The
one steward instance waiting on `Remy` (`wf-x1q9ypp7zp`) failed earlier at step 5
on an unrelated 30-minute `cargo test` timeout, before reaching the wait. So:
demonstrated selection hazard, not a reproduced production failure.

Note `task_done` never duplicates within a generation (it is written once, by
`rk done`), which makes it the obvious candidate signal for a completion wait.
That is a design call for whoever picks up the follow-up, not a decision this
audit makes.

Filed as **TKT-160**. Not fixed here: `crates/rk-daemon/src/workflow_exec.rs` is
claimed by Nezumi-2 (TKT-147) and read-swept by Cinder-2 (TKT-159), and it is
adjacent to TKT-147's evaluate-gate work.

## Re-running this

```sh
./scripts/rk-name-generations-audit.py              # live RK_HOME, human-readable
./scripts/rk-name-generations-audit.py --json       # machine-readable
./scripts/rk-name-generations-audit.py /path/to/rk-home
```

Read-only — it opens `space.db` with `immutable=1` and never writes, so it is
safe against a running fleet. Sections 1–3 should stay stable now that TKT-146
prevents new collisions; a growing section 1 means `reserve_name` has regressed.

## Handoffs

| Ticket | Owner | What this audit gives it |
|---|---|---|
| TKT-158 | Sable-2 | Generation-aware `AgentLog` needs **no historical migration** — 0 of 24 logs are interleaved (Finding 2) |
| TKT-159 | Cinder-2 | The residue cannot trigger a stale cross-generation match through the three fixed reads (Finding 1); the remaining exposure is intra-generation (Finding 4) |
| TKT-160 | unassigned | New: `result_pattern` selects the oldest result within a generation, which is mid-flight for 9 of 12 measured multi-result agents |

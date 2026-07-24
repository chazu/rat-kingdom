# rk-sync live multi-machine validation (TKT-71)

Closes the last open item in P6 (`docs/2026-07-22-implementation-plan.md`):
> REMAINING from P6: live multi-machine validation over a real remote.

All rk-sync logic (union merge, deterministic claim arbitration, take-op
replication + per-actor compaction from TKT-58, Ed25519 identity from TKT-59)
was covered by *in-process* two-castle tests against an in-memory space. This
task validated the same behaviours **end to end**: two independent daemons, each
in its own `RK_HOME` with its own `castle.key`, driven by the real `rk` CLI,
replicating through a **real git remote** out of process.

## How it was validated

`scripts/rk-sync-live-validation.sh` — a self-contained harness. It stands up a
bare git remote (`file://`, a real remote to git: real fetch/push, real
fast-forward enforcement, one `refs/notes/rk/<actor>` per castle), two isolated
castles pointed at it, and two `rk daemon run` processes. Every sync cycle is
driven explicitly with `rk sync now` so the run is deterministic and never waits
on the interval timer.

"Two machines" here is two `RK_HOME`s sharing one bare remote — exactly the "two
containers with a shared bare remote" the ticket permits. rk-sync's remote path
is `git fetch`/`git push` of a notes refspec; nothing behaves differently over
`ssh://` or `https://`, so pointing both `config.toml`s at a shared remote runs
the identical harness across two physical hosts.

Run it:

```
./scripts/rk-sync-live-validation.sh    # exit 0 = all five scenarios passed
```

## What was checked (all PASS)

| # | Ticket requirement | Live result |
|---|--------------------|-------------|
| 1 | durable tuples + claims converge | Facts replicate both ways; two conflicting durable claims (`--lifecycle furniture`) converge to the identical 2-record union on both castles; earliest-ULID arbitration picks the same winner on each; each castle sees the other as a distinct signed actor in `rk peers`. |
| 2 | a consume on one castle drops the tuple on the other, never resurrects | A consumes its own replicated tuple → next cycle exports a `Take` → B drops it (`removed=1`); gone on both castles; **no resurrection across 6 further convergence rounds**. |
| 3 | partition / rejoin convergence | Remote parked mid-run: pushes fail (`pushed=false`), local writes survive, a `sync_failure` obstacle surfaces (not a silent stall), neither castle sees the other's partition-era writes. On rejoin both sides' accumulated writes converge and the catch-up push stays fast-forward. |
| 4 | compaction drains consumed Out+Take on both, no resurrection window | The two-phase drain runs live: the Out drops once the tuple is taken, the Take drops once the Out has left the whole union; both records are physically gone from the on-disk notes log, and the tuple never reappears through the drain. |
| 5 | push stays fast-forward after compaction rewrites | git-notes edits advance the notes ref via new commits, so content-rewriting compaction leaves the ref a fast-forward of the remote. Across 10 post-compaction cycles with live churn, **every push succeeded with no `--force`** and no new `sync_failure` obstacle appeared. |

## Finding: local obstacles replicate as never-drained Out records

Observed while inspecting the notes log during validation, filed as **TKT-92**
(not fixed here — the code lives in `sync.rs`/`supervisor.rs`, which peers were
actively editing).

`rk_core::tuple::Tuple::new` defaults to `Lifecycle::Session`, which is
**durable and replicable**. Obstacles authored *inside the daemon* via
`space.out(Tuple::new(Category::Obstacle, …))` bypass the RPC `handle_out`
boundary that would have made them Ephemeral+strength (per TKT-14), so they land
as durable `Session` tuples with `instance == castle`. `Syncer::run_cycle` step
(1) therefore exports each one as a `SyncOp::Out`. Because nothing ever *takes*
an obstacle, no `Take` is ever emitted, so:

- the `sync_failure` obstacle raised by `run_cycle` itself,
- the budget / stuck / runaway obstacles raised by the supervisor sweep,

accumulate in the actor's notes ref forever and re-import onto every peer. This
is not a correctness bug (no resurrection, arbitration unaffected) but it is
unbounded log growth plus cross-castle noise — a scaling concern for a
long-lived multi-machine fleet, and semantically odd for `sync_failure` (a
purely local "I can't reach the remote" signal that only replicates once the
remote is reachable again). See TKT-92 for options (mark daemon-authored
obstacles Ephemeral, or exclude the obstacle category from export).

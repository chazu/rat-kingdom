# Proposal 0006 — Make the convention loop actually run: read and endorse open suggestions on entry

**Author:** Burrow-2 (task: refine-prompts)
**Target prompt:** `crates/rk-core/src/prime.rs` → `FRAGMENT_SPACE`
**Companion convention:** *(none — this proposal is the precondition for any
convention ever existing)*
**Status:** proposed (do NOT apply live — an operator/steward lands this; see the
completion protocol)

## The recurring pain

**The fleet has promoted zero conventions in its entire life.** Measured against
the live space on 2026-07-25:

```
$ rk scan convention          -> (no tuples)
$ rk scan suggestion          -> (no tuples)
$ rk scan endorsement --json  -> 0
```

over a fleet with **277 `agent_spawned` and 289 `harness_result` events** across
2026-07-22 → 2026-07-25. A `Convention` is written with
`Lifecycle::Furniture` (`reactor.rs:333` — "a promoted norm is permanent, never
`in`-consumable"), so this is not decay: **nothing has ever reached quorum.**

That is the entire stigmergy-norms program — `rk suggest`/`rk endorse` +
quorum promotion (TKT-12), spawn injection (TKT-18), live steer of running rats
(TKT-34), cross-castle replication — sitting at zero output. The evidence that
the machinery itself works is in the corpus: the TKT-12 rat shipped a live-daemon
end-to-end test proving propose → endorse → promote works over the wire. The
loop is not broken. **It is never driven.**

The one real attempt is preserved in the drain feed. rat-28 minted two
suggestions; rat-32 found one and did the right thing:

```
1. Investigated first — found rat-28 had already minted the matching suggestion
   sug-3tchrh27hz … No endorsements or convention existed yet.
2. Endorsed the existing suggestion rather than creating a duplicate — landing
   me (rat-32) as the first distinct endorser toward the quorum of 3.
3. Routed the remaining work stigmergically — a single rat can't reach quorum
   alone, and the 2 further distinct endorsements are peers' work, so I posted
   a need asking the room to endorse rather than doing it myself.
```

Nobody answered the `need`. It evaporated on its TTL, the suggestion evaporated
on its 24h voting window, and a third rat (Nikaido) later re-posted a `fact`
begging peers to endorse `sug-8nsqa4132x`. That fact is still in the space and
still unanswered — the suggestion it names is long gone. Three rats spent real
tokens trying to promote **one** norm and it never crossed a quorum of 3.

The cost is not the lost norms themselves; it is that every durable rule the
fleet learns has to be landed by hand-editing `prime.rs` through a proposal like
this one, which needs a human in the loop — precisely the loop the norms
machinery was built to close.

## Root cause

`FRAGMENT_SPACE` teaches `suggest`/`endorse` as *capabilities* in a list, and
its one directive instruction names only two categories:

```
- `rk scan <category> [scope]` — read tuples. Before starting, read `fact` and
  `convention` tuples for your repo scope and the `system` scope.
...
- `rk suggest "<text>"` — propose a fleet norm; prints a `sug-…` id for peers to endorse.
- `rk endorse <sug-id>` — back a suggestion (idempotent). At quorum the daemon
  promotes it to a `convention` automatically — no operator in the loop.
```

So: rats are told to *read* `fact` and `convention`, and that they *may* endorse
— but `suggestion` is not in the read list, so a rat never encounters an open
proposal unless it goes looking for one it has no reason to suspect exists.
Endorsing is framed as an optional favour with an unstated deadline, and the
quorum arithmetic (3 distinct rats *inside one 24h voting window*) is invisible.
A proposer therefore has no way to succeed: it cannot reach quorum alone, and
the peers who could are never pointed at the ballot.

The asymmetry is the bug. Proposing is cheap and taught; endorsing is cheap and
untaught. The mechanism needs the second one to be the default habit.

## Proposed change (unified diff)

```diff
--- a/crates/rk-core/src/prime.rs
+++ b/crates/rk-core/src/prime.rs
@@ const FRAGMENT_SPACE: &str = "\
 - `rk scan <category> [scope]` — read tuples. Before starting, read `fact` and
   `convention` tuples for your repo scope and the `system` scope.
+- On entry, also `rk scan suggestion system` and endorse every open proposal you
+  agree with: `rk endorse <sug-id>`. A suggestion needs 3 DISTINCT endorsers
+  inside its 24h voting window or it decays and the norm is lost — so a proposal
+  only ever becomes binding if passing rats spend the one command on it. This is
+  not extra work: it is a single cheap call, and it is the only way the fleet
+  turns a lesson into a rule without a human. Endorse the existing suggestion
+  rather than minting a near-duplicate.
 - Before editing an area, `rk scan claim <repo>` and `rk scan artifact <repo>`
   to see what peers are touching, and steer clear of their files. On entry,
   mark your area with `rk claim <area>` (a path or glob) so peers avoid it.
   Claims evaporate on a TTL, so re-run it if you are still working there.
```

## Why this is safe

- Purely additive inside the shared `FRAGMENT_SPACE`; both roles include it, so
  no per-role copy drifts.
- `rat_role_includes_all_fragments_once` asserts on
  `"Coordination: the tuplespace"`, unchanged; the claim-trail test asserts on
  `"rk claim <area>"` and `"rk scan claim"`, both unchanged.
- It cannot conflict with the single-task banner: endorsing is not claiming or
  starting work, it is a read plus a vote — the same class of act as posting a
  `fact`, which the banner already prescribes.
- Cost is bounded and tiny: one `rk scan suggestion system` (usually returning
  nothing) plus at most one `rk endorse` per open proposal.
- Failure mode is benign. The worst case is a rat endorsing a norm it only
  loosely agrees with; quorum is 3 distinct rats, and a promoted convention is
  visible in every subsequent prompt, so a bad norm is loud rather than silent.

## A note on what this does NOT fix

Even with this change a suggestion still needs three rats to pass through inside
24h. If the fleet is quiet, a good proposal still decays. Two follow-ups worth
separate tickets, filed rather than bundled here:

- surface open suggestions in `rk inbox` (the operator can then endorse or
  extend), and
- reconsider the 24h default voting window for a fleet whose rats live minutes
  (`SuggestArgs::ttl`, `space_cmds.rs:107`).

Neither is a prompt change, so neither belongs in this proposal.

## Related

- TKT-12 (suggest/endorse + quorum promotion), TKT-18 (spawn injection),
  TKT-34 (live steer on promotion) — the machinery this proposal feeds.
- fact `preexisting-failure-convention-adoption` (system scope) — the standing,
  unanswered plea for endorsers that motivated this.
- Proposals 0004 / 0005 file convention proposals that will hit exactly this
  wall until this lands.

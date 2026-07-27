# TKT-167 — an open ballot the fleet cannot see is a norm the fleet cannot have

**Status**: fixed. Rows in `crates/rk-daemon/src/inbox.rs` (`Ballots`,
`open_suggestions`, `urgency::OPEN_SUGGESTION`), wiring in
`server.rs::handle_inbox`, operator-side vote in
`crates/rk-cli/src/space_cmds.rs::endorse`. Tests: `inbox.rs` unit tests +
`lib.rs::open_suggestion_surfaces_in_the_inbox_over_the_wire`.

## What was asked

A follow-up named in `docs/proposals/prompts/0006-endorse-on-entry.md` and
deliberately **not** bundled into that prompt change: `rk inbox` unions
everything awaiting a human (TKT-24), and an open system-scope `Suggestion`
inside its voting window belongs there, with `rk endorse <sug-id>` as its
resolving command, so an operator can back a proposal before it decays.

## The measurement

Proposal 0006 measured the live space on 2026-07-25:

```
rk scan convention   -> (no tuples)
rk scan suggestion   -> (no tuples)
rk scan endorsement  -> 0
```

over **277 `agent_spawned` events** since 2026-07-22. A `Convention` is written
with `Lifecycle::Furniture` (`reactor.rs`: "a promoted norm is permanent, never
`in`-consumable"), so zero conventions is not decay — **nothing has ever reached
quorum.** The whole norms program (TKT-12 suggest/endorse + promotion, TKT-18
spawn injection, TKT-34 live steer of running rats, cross-castle replication)
has produced zero output.

The machinery works: TKT-12 shipped a live-daemon end-to-end test proving
propose → endorse → promote over the wire, and `convention_quorum.rs` still pins
it. What fails is upstream of the machinery. Three separate rats each proposed a
norm, could not reach a quorum of three alone, asked the room to endorse — and
nobody answered. The `need` evaporated on its TTL, the suggestion evaporated on
its 24h window, and the last of the three left a `fact` still standing in the
space today begging peers to endorse `sug-8nsqa4132x`, a ballot that has been
gone for days.

## Why nobody answered

Not indifference. **No rat is ever told a vote is open.** `FRAGMENT_SPACE` tells
a rat to read `fact` and `convention`; `suggestion` is not in the read list, so
a peer only ever encounters a ballot by going looking for one it has no reason
to suspect exists. Proposing is cheap and taught; endorsing is cheap and
untaught. Proposal 0006 fixes the prompt half of that asymmetry.

This ticket fixes the other half, and it is the half that does not depend on
model behaviour. Even with 0006 landed, a proposal still needs three rats to
pass through inside 24h; on a quiet fleet a good norm still decays with two
votes. The operator is the one endorser who is **always reachable** — and the
operator's whole interface to "what needs me?" is `rk inbox`.

## What was built

`rk inbox` gains an **`open-suggestion`** row per live ballot:

```
open-suggestion  sug-8nsqa4132x system   1/3 endorsers (6h12m left) — rat-28 proposes: a pre-existing failure is a ticket, not an inline fix
  → rk endorse sug-8nsqa4132x
```

The row carries what a vote actually needs: who is asking, what they propose,
how far from quorum, and how long is left. Three filters drop ballots the
operator cannot usefully act on:

- **already promoted** — a `Convention` carries the suggestion's id, so the vote
  is over. This is the same promote-once marker the reactor itself uses, so the
  two can never disagree.
- **decayed** — `expires_at` has passed. Expiry is collected by the GC rather
  than filtered on read, so a scan does return ballots whose window closed
  minutes ago; endorsing one is not what the operator meant.
- **`quorum = 0`** — promotion is disabled in config, so no endorsement can
  resolve the ballot. Offering the vote would be a lie.

Scoped to `system`, matching `promote_conventions`, which only ever considers
system-scope tuples — a suggestion written anywhere else could not promote no
matter who endorsed it. `rk suggest` always writes system scope.

Endorsers are counted **distinct by `instance`, per suggestion** — the same
count the reactor promotes on, so the tally shown is the tally that decides.
Rows sort closest-to-decaying first: the ballot the operator is about to lose is
the one they read.

## Ranking: why a ballot sits at the bottom

`urgency::OPEN_SUGGESTION` is co-ranked with `need` (1), below every obstacle,
every dropped branch and every failure. A proposal is not a problem; nothing is
blocked on it. It earns a row at all only because it **expires** — a need may be
answered late, a suggestion that misses its window is gone and the norm with it.

## The resolving command had to work

Every other row in the inbox names a command an operator can run. `rk endorse`
could not be one: like every sugar command it began with
`env_required("RK_AGENT")`, which is set only inside a spawned rat. A human
running the action off an inbox row would have got

```
RK_AGENT is not set — sugar commands need the spawn environment
```

So `endorse` (and only `endorse`) now falls back to a fixed `operator` identity
when `RK_AGENT` is unset. Fixed rather than per-shell for two reasons: quorum
counts distinct `instance` values, so the operator is exactly **one** more
endorser however many terminals they use, and the existing idempotency check —
one endorsement per `(suggestion, agent)` — keeps working, so a repeated vote
stays a no-op.

This is deliberately not extended to `rk suggest`. Proposing is a rat's act
arising from its work; voting is the act a human is well placed to perform on a
proposal already made.

## What this does not fix

- **The 24h voting window** is still the default, on a fleet whose rats live
  minutes. TKT-168 is filed for that and is untouched here.
- **Rats still are not told to look.** That is proposal 0006 / TKT-165, a prompt
  change an operator lands. This ticket makes the operator's half work
  regardless of whether the prompt half ever does; the two are complementary,
  not alternatives.
- **Nothing pushes.** The row is passive, like the rest of `rk inbox` — it is
  seen when the operator polls. The steward escalation push (herdr) is the
  precedent for making a row active, and no ballot is urgent enough to warrant
  it.

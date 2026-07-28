# TKT-168 — the 24h voting window, reconsidered: a ballot is a ledger entry

**Decision: `rk suggest` and `rk endorse` no longer default to a voting window.**
Both write durable (`Session`) tuples. `--ttl` stays for a deliberately
time-boxed vote. Quorum still means three distinct endorsers; it no longer means
three distinct endorsers *inside one overlapping 24h window*.

Filed as the follow-up named in `docs/proposals/prompts/0006-endorse-on-entry.md`
and in `docs/2026-07-26-tkt-167-open-suggestion-inbox.md` ("what this does not
fix"). It is the third of the three things keeping the norms program at zero
output, after TKT-167 (the operator cannot see a ballot) and proposal 0006 /
TKT-165 (rats are not told to look).

## The question

`SuggestArgs::ttl` and `EndorseArgs::ttl` both defaulted to `24h`. Because
`ttl_secs` is what forces `Lifecycle::Ephemeral` at the RPC `out` boundary
(`server.rs::handle_out`), every ballot was Ephemeral, and the GC collects on
`expires_at`. So a suggestion needed 3 distinct rats to vote *while their three
endorsement tuples and the suggestion all coexisted*. Empirically that never
happened once: zero conventions over 277 spawns.

The ticket named three candidate answers — a longer/absent default window,
Furniture endorsements, or a decaying-but-reinforced tally. The right one falls
out of asking what kind of object a vote actually is.

## Why a window is the wrong instrument (not merely too short)

Three arguments, none of which a longer window addresses:

**1. A vote cannot be reinforced.** Every other TTL'd tuple in this space is a
pheromone whose author refreshes it while the condition holds — a rat re-`claim`s
the area it is still editing, a rat restates the obstacle it is still stuck
behind. That is why `Category::evaporates()` covers `Claim | Obstacle | Need |
Resolution` and nothing else. An endorser is dead minutes after voting; nobody
can ever re-cast its vote. Decay therefore destroys information that cannot be
regenerated. Reinforcement is not merely unused for a ballot, it is impossible in
principle — which is what rules out the ticket's third option: there is no one
left to do the reinforcing. (Reinforcing the *suggestion* on each endorsement was
considered and dropped: it keeps popular ballots alive but still silently kills
the two-endorsement ballot that a quiet week strands, which is precisely the
observed failure.)

**2. Decay buys no freshness.** The usual defence of an expiring vote is that
norms should reflect who currently agrees. But promotion mints a `Convention`
with `Lifecycle::Furniture` — permanent, never `in`-consumable, replicated. The
*output* is permanent no matter what the ballot does, so expiring the ballot can
never make the fleet's norms more current. It can only make promotion harder. A
cost with no matching benefit.

**3. Ephemeral tuples do not replicate.** `rk-sync` exports durable lifecycles
only (`SyncOp::Out` — "durable lifecycles only, ephemeral tuples never touch
git"; the filters are `sync.rs:134,243`). So under the old default, a ballot was
invisible to every other castle while the Convention it would promote to
replicates for free. Two castles could never pool votes into one quorum — the
ballot was local, the norm global. This was not previously written down anywhere
and is a defect the durable default fixes for nothing.

So: **the ballot is a ledger entry, not a pheromone.** It closes on its outcome —
promotion — not on a clock. Three rats reach quorum by *ever* agreeing rather
than by overlapping.

## Why the suggestion goes durable too, not just the endorsement

Making votes durable while ballots still expire is the worst of both: the
suggestion decays out from under its two endorsements, and those votes are then
orphaned forever — unreachable (nobody can see the ballot to cast a third),
un-garbage-collectable, and if a third vote ever did land on that id the reactor
would mint a **permanent** `Convention` citing `text: null`. Whatever the
suggestion's lifetime is, the endorsements must not outlive it. Equalising them
at "durable" is the only assignment with no orphan window.

`rk inbox` needs no change to match: TKT-167's `open_suggestions` already filters
with `expires_at.is_none_or(|e| e > now)`, sorts a windowless ballot last, and
`window_left` renders an empty clause for it. The row was written to accept a
ballot with no window.

## What this does not fix

- **Nothing closes a losing ballot.** A proposal that never reaches quorum now
  stays in the inbox forever. The right closing act is explicit (`rk withdraw
  <sug-id>`, or an operator dismiss) rather than a silent clock, and the pile is
  tiny — the fleet has minted about three suggestions in its life. Filed as a
  ticket rather than bundled here.
- **Legacy Ephemeral ballots.** The ones already in the live space keep their
  windows and will still decay; only new writes are durable.
- **The null-text promotion hazard.** `promote_conventions` still mints a
  permanent Convention citing `text: null` when quorum lands on a suggestion
  whose text is gone (`reactor.rs`, defended by
  `quorum_promotes_even_after_suggestion_decays`). Durable ballots make this
  nearly unreachable, but "nearly" plus "permanent and unretractable" is worth a
  ticket. Deliberately not changed here: it is a defended decision of its own,
  not fallout of the window.
- **`inbox.rs` still says "a voting window (default 24h)"** in a doc comment
  (~line 106). Left stale on purpose — `inbox.rs` is under another rat's claim.
  Ticketed.

## Changes

- `crates/rk-cli/src/space_cmds.rs` — `SuggestArgs::ttl` / `EndorseArgs::ttl`
  become `Option<String>` with no default; new `ballot_ttl_secs` carries the
  rationale and sends `ttl_secs` only when asked. Unit test
  `ballots_carry_no_voting_window_by_default` asserts through clap, so a
  re-added `default_value` fails in CI rather than silently in the fleet.
- `crates/rk-daemon/tests/convention_quorum.rs` — new
  `a_ballot_written_without_a_window_is_durable` pins `Session` + no `expires_at`
  over the wire for both categories.
- `docs/reactor.md`, `crates/rk-daemon/src/server.rs` — the norms-loop prose and
  the inbox-ballot comment no longer describe ballots as Ephemeral.

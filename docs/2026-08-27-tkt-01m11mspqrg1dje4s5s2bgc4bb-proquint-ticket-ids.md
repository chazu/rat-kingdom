# Pronounceable ticket ids (TKT-01M11MSPQRG1DJE4S5S2BGC4BB)

Tickets now have a human-facing identifier you can actually say out loud.

## What changed

A ticket created **before** this landed carries the identity it always
had: `TKT-` followed by a 26-character ULID, e.g.

```
TKT-01M0P96ZSQAJGRE7WTGDBWAXJ9
```

A ticket created **after** this landed carries a pronounceable identity
instead: `TKT-` followed by three dash-joined five-letter syllables, e.g.

```
TKT-babad-bisub-lodob
```

Nothing about an old ticket's durable identity changes — it is never
renamed. Every workflow binding, delivery record, and agent task
assignment that already points at a `TKT-<ULID>` keeps pointing at exactly
that ticket. What's new is that an old ticket also has a **deterministic
alias**: a proquint spelling computed from its ULID identity, shown
alongside it wherever a human reads the ticket (`rk ticket show`, `rk
ticket list`). The alias is never stored — it's recomputed the same way
every time — so it costs nothing to keep in sync and there is no migration
step to run.

## Reading and dictating a ticket id

- **A new ticket**: read the identity straight off `rk ticket new` /
  `rk ticket show` — it's already the pronounceable form. Dictate it as
  three words separated by "dash": "babad dash bisub dash lodob".
- **An old ticket**: `rk ticket show <id>` leads with the alias and shows
  the durable ULID identity right below it:

  ```
  $ rk ticket show TKT-01M0P96ZSQAJGRE7WTGDBWAXJ9
  TKT-nudil-fabov-humig: fix the thing
    id        TKT-01M0P96ZSQAJGRE7WTGDBWAXJ9
    status    open
    ...
  ```

  `rk ticket list` shows the alias in the ID column for any legacy ticket
  in the result set. Either spelling is safe to copy — see "resolving both
  forms" below.

## Resolving both forms

Every command that accepts a ticket id — `show`, `update`, `dep`, `reopen`,
`deliver`, `--parent`, `--depends-on`, `rk spawn --ticket`, and the
matching RPCs — accepts **either** spelling for **any** ticket:

- a ticket's own durable identity (ULID or proquint), or
- a legacy ticket's deterministic alias.

Resolution is exact-match only. There is no prefix matching and no fuzzy
correction: a spelling either names exactly one ticket or it doesn't
resolve. When you set `--parent` or `--depends-on` using an alias, the
ticket's real (canonical) identity is what gets stored in the payload —
the alias is purely a human-facing convenience, not a second identity that
graph algorithms (blockers, readiness, cycle detection) have to know
about.

## Collisions

New ticket ids draw 48 bits of randomness (three proquint words). At that
width the birthday-bound 50% collision probability sits around 2^24 (~16.8
million) tickets minted fleet-wide over the identifier's whole lifetime —
far past any realistic volume. Ticket creation still checks each freshly
drawn id against every existing ticket (including legacy aliases) and
redraws on an actual collision before failing outright, so the guarantee
does not rest on the birthday bound alone.

A legacy ticket's alias is a *hash* of its ULID, not a checked draw, so two
legacy tickets could in principle alias to the same spelling. This is
exercised in `crates/rk-daemon/src/tickets.rs`
(`resolve_refuses_an_ambiguous_alias_collision`) with a verified real
collision pair: resolving a spelling that matches more than one ticket is
refused with an "ambiguous" error rather than silently picking one.

## For client authors

`ticket.get` and `ticket.list` responses carry an `alias` field alongside
the ticket tuple when (and only when) the ticket's durable identity is a
legacy ULID. A client migrating to prefer proquint spellings can display
`alias ?? identity` and fall back to `identity` for anything not carrying
one (every proquint-native ticket, and any ticket predating this doc if the
alias field itself is absent from an older daemon response).

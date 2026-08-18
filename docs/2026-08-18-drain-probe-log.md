# E0 drain probe log

*Started 2026-08-18. 2-day supervised mini-drain per the Phase 2 epic's E0.
Configuration: `drain.enabled = true`, `max_wip = 2`, repo rat-kingdom,
sonnet tier default, all phase-1 recovery machinery live (respawn, done-kill,
burn detection, stale-instance timeout, ticket reopen, announce sinks).*

## Pre-probe grooming (operator, 2026-08-18)

- Closed 11 tickets: 4 duplicates of the already-fixed client_version seam,
  2 of the fixed freeze.rs clippy error, 5 duplicate agent_archive flake
  reports (canonical: TKT-…F214KDE7). All were self-improve-generated
  duplicates of resolved work — drain would have dispatched rats onto
  already-fixed bugs. **Grooming debt is itself a probe finding: the
  backlog accumulates duplicates faster than anything dedupes them.**
- Blocked 16 tickets: 3 operator-only, 2 decision tickets (+2 implementations
  gated on those decisions), 4 epic-wip (gate concurrency = Phase-2 E4
  territory), 4 building on the dead steward CUE path slated for removal,
  1 landing-policy (E2 territory).
- Filed 3 fresh probe tickets: verify/CI profile alignment (high),
  terminal-cost rollup fix (high), `rk ticket reopen` (normal).

## Success criteria (pre-registered)

- Tickets landed autonomously per day: **≥ 3** (yesterday's manual rate was
  ~15 with an operator driving; 2-WIP autonomous at ≥3 is the floor of
  useful).
- Interventions/day: recorded and **classified** below — the classification
  is the deliverable that orders the Phase 2 epic.
- Silent-stall check: at probe end, audit from the durable ledger that every
  ticket that left `open` either reached a terminal/landed state or has a
  live agent or an inbox row. Any ticket that stalled with none of those =
  criterion failure.

## Observations

- **O1** (T+0): drain's first two claims were normal-priority tickets
  (…8VMZANPD, …GA7JZ5JZ) while `high` tickets (…F214KDE7, both probe
  tickets) sat ready — claim order appears not to be priority-first,
  contradicting the drain config's own doc comment. Verify in drain.rs;
  candidate epic finding.
- **O2** (T+0): both drain-spawned rats came up on sonnet via the tier
  catch-all — tier routing confirmed working under drain dispatch (first
  time exercised).

## Interventions

*(operator/steward actions that autonomous machinery should eventually own —
classified as: dispatch / recovery / review / landing / grooming / other)*

| When | Class | What | Should-be-owner |
|---|---|---|---|
| pre-probe | grooming | 27-ticket backlog groom (dupes of fixed bugs, decision tickets, epic-wip) | future: dedup-on-file + decision-ticket type |

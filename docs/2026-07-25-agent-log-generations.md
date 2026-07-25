# Duplicated name generations: policy, and generation-aware `AgentLog`

TKT-158 (sub of TKT-148). Decided and implemented 2026-07-25.

## The question

24 agent names each name two unrelated rats. `AgentLog` keyed its transcript file
on the name alone (`agent_log.rs` `path_for` → `<name>.jsonl`), so a name was an
ambiguous read key. Three options were on the table:

- **(a)** leave it, document the 24 as known-ambiguous
- **(b)** rename the archived generations
- **(c)** make `AgentLog` generation-aware, keyed on the `(name, created_at)`
  generation the TKT-136 archive already uses

**Chosen: (c).** It is the only option that fixes reads for these names rather
than annotating or rewriting history, it reuses a key that already exists, and it
makes the file layout correct by construction — no future naming regression can
put two rats' lines in one file again. (b) was rejected outright: renaming an
archived generation would falsify the name stamped into that rat's durable
records (`harness_result` / `task_done` tuples, branches, worktree paths), i.e.
it would trade a read ambiguity for a set of records that disagree with each
other.

## What was measured first

Everything below is from the live registry and `~/.rat-kingdom/agent-logs/` on
2026-07-25, not from the ticket text.

**The 24 is right; the set is frozen.** 272 records over 248 distinct names; 24
names carry exactly two generations. All 24 older generations were archived in a
single `rk prune` at `2026-07-24T19:07:49Z`, and the pre-TKT-146 `reserve_name`
then handed those freed names out again. The last recycled name is `Sable`
(`2026-07-25T02:09:32Z`); the fixed daemon took the socket at
`2026-07-25T02:13:23Z` and the very next spawn, 9 seconds later, was `Colby-2` —
suffixed, not recycled. Every spawn since has been suffixed. Replaying current
`main`'s `reserve_name` against the real `agents.json` + `agents-archive.json`
yields `Ash-2`, `Sooty-2`, `Dusty-2`… so TKT-146's claim holds and the set cannot
grow.

(The redeploy looks two days late in `ps` output, which prints local time while
records are UTC. `Fri Jul 24 22:13:23 EDT` *is* `2026-07-25T02:13:23Z`.)

**No file interleaves today — but only by accident.** Of the 24 names, 20 have a
log file and 4 have none, and every one of those 20 files contains entries from
the *newer* generation only. The reason is a coincidence of dates: `rk log`
(TKT-25) landed `2026-07-23T12:13Z`, and all 24 older generations were spawned
between `2026-07-22T20:46Z` and `2026-07-23T04:15Z` — every one of them ran
before the transcript feature existed, so none ever wrote a line.

So the realized harm was smaller than the ticket assumed, and the remaining harm
is different in kind: `rk log Sable` did not mix two transcripts, it showed one
rat's transcript **with no indication that another rat had carried the name**. A
silent partial answer to an ambiguous key. That is what (c) fixes, alongside
closing the interleaving hazard the code still had.

## What changed

- **Files are keyed on the generation.** `<home>/agent-logs/<name>.<stamp>.jsonl`,
  where the stamp is the record's `created_at` (`%Y%m%dT%H%M%S%3fZ` — sortable,
  and alphanumeric so `sanitize` leaves it alone; `sanitize` never emits a dot,
  so the dot before the stamp is an unambiguous separator).
- **Writes carry the generation.** `AgentLog::append` takes it, and
  `Supervisor::handle_event` receives it from the record captured when the event
  loop is wired up. A respawn continues the same generation, because it reuses
  the record — the second run appends to the transcript the first started.
- **Reads are fixed retroactively.** `AgentLog::read` takes a `Generation
  { agent, start, end }` and reads *both* that generation's file and the legacy
  `<name>.jsonl`, windowing the legacy half to `[start, end)`. That is sound
  because generations of a name never overlap in time: the predecessor was
  terminal and archived before the successor was spawned, so a timestamp alone
  attributes a line. Entries predating every known generation belong to nobody
  and are dropped rather than credited to the oldest rat. The two files also
  merge cleanly for the one run that straddled the upgrade.
- **Ambiguity is disclosed, not resolved silently.** `Registry::generations_of`
  enumerates every generation of a name (live + archived, deduped by
  `created_at`, so the archive-then-crash window is one rat and not two);
  `Supervisor::log_generations` adds the window bounds. `agent.log` defaults to
  the newest generation and returns `{generations, generation, created_at}`, so
  `rk log <name>` prints a stderr note when a name carried more than one rat.
  `rk log <name> --generation N` (1 = oldest) reads an earlier one; an
  out-of-range N is a parameter error, never a quiet fallback to another rat.
- **`--follow` filters on the generation too**, so following an older generation
  streams nothing rather than leaking a live namesake's output.

Nothing on disk was rewritten and no record was renamed. The 24 legacy files stay
exactly as they are and are now read correctly.

## Known limits

- The 4 names with no log file (`Cheddar`, `Fidget`, `Munch`, `Scamper`) have no
  transcript for either generation. Nothing to recover.
- Older generations of the 24 have no transcript at all (they predate TKT-25).
  `rk log <name> --generation 1` correctly returns an empty transcript for them —
  that is the true answer, not a gap in this fix.
- Pruning a record by hand out of `agents.json` still leaves a transcript with no
  generation to key it to; `Generation::unrecorded` reads the legacy file whole so
  such a transcript stays legible rather than vanishing.

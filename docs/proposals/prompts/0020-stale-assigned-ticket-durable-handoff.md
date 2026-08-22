# Proposal 0020 — Hand off a stale assigned ticket durably, not just as an obstacle

**Author:** Havarti-11 (task: refine-prompts)
**Target prompt:** `FRAGMENT_COMPLETION` step 4, `crates/rk-core/src/prime.rs`
**Companion convention:** `stale-assigned-ticket-needs-a-durable-handoff`
**Status:** proposed (do NOT apply live — this rat only writes proposals)

## The recurring pain

Tonight's cohort (2026-08-21) produced two independent, fresh instances of the
same failure boundary:

- Noodle-11, dispatched onto TKT-01M0CFA1RX36SJ7DV4YWGHQ9BT, found that
  Filch-9's commit `246967a` (an ancestor of Noodle-11's own fork point) had
  already implemented the ticket's substance. Noodle-11 correctly wrote no
  code, ran the mandatory full verify anyway (required by step 3, unavoidable),
  and recorded an `obstacle` tuple with the evidence. That ticket has since
  been closed.
- Mochi-11, dispatched onto TKT-01M0CHJWTECPXJ3J8VY8PB6MG9, found it an exact
  duplicate of TKT-01M0C8PJ7AQ7TQ4WV7SCYJ9Y7F (already `done`, fix already on
  main). Mochi-11 also wrote no code, ran the mandatory full verify (600s+
  stuck, ~$0.77, ~1.09M tokens — pure re-verification cost with zero code
  delta), and recorded only an `obstacle`. As of this scan,
  **TKT-01M0CHJWTECPXJ3J8VY8PB6MG9 is still `in_progress`**, still assigned to
  Dart-9, and remains dispatchable — the exact shape of failure that produced
  the standalone ticket TKT-01M0KPRCC4JBRHFJTQ3ZQPKW79 ("Close stale ticket
  TKT-01M0J7W3DM36SW9XXPR0NR0J73 (rework already merged to main)").

The daemon does not let an ordinary rat close its own ticket:
`ticket.update`'s closing path is gated to the groomer role
(`server.rs:1596-1598`, `is_groomer` + `groomer_can_close_ticket`), and the
groomer's own evidence sweep (`FRAGMENT_GROOMER`) reads ticket bodies/status,
not `obstacle` tuples, which are a live, ephemeral coordination signal (per
the standing tuplespace conventions) — not the durable handoff a later
groomer pass will discover. So the correct-but-incomplete behavior both rats
followed (skip the redundant work, still verify, record an obstacle) leaves
the ticket exactly as dispatchable as it was before, and a future drain cycle
can burn the identical verify cost on a third rat.

## Root cause in the prompt

`FRAGMENT_COMPLETION` step 4 covers the mirror case — a **pre-existing
failure** unrelated to your change — and correctly routes it to a durable
ticket + artifact, not an inline fix:

```
4. Never `rk done` on a build you broke. If you hit a pre-existing failure that
   is unrelated to your change, do NOT fix it inline (peers on other branches
   will race you) — file a ticket and record it as an artifact
   (`rk out artifact <repo> preexisting-failure --payload '{...}'`), then finish
   your own task. Do not retry `rk out fact`: an agent caller receives `forbidden`.
```

There is no equivalent instruction for **pre-existing success** — discovering
your assigned ticket's fix is already an ancestor of your fork point. Nothing
in the ordinary rat's role text says an `obstacle` is insufficient for this
case or that a follow-up `rk ticket new` is required so a groomer can act.
`FRAGMENT_GROOMER`'s `stale-rework` evidence path exists for exactly this kind
of closure but is scoped to tickets literally titled `rework: TKT-...`
referencing a target; a plain duplicate ticket (Mochi-11's case) or a parent
ticket whose substance shipped under an unrelated commit (Noodle-11's case)
falls outside that literal match unless something files a ticket the groomer
can read as evidence.

## Proposed diff

```diff
--- a/crates/rk-core/src/prime.rs
+++ b/crates/rk-core/src/prime.rs
@@ FRAGMENT_COMPLETION step 4
 4. Never `rk done` on a build you broke. If you hit a pre-existing failure that
    is unrelated to your change, do NOT fix it inline (peers on other branches
    will race you) — file a ticket and record it as an artifact
    (`rk out artifact <repo> preexisting-failure --payload '{...}'`), then finish
    your own task. Do not retry `rk out fact`: an agent caller receives `forbidden`.
+   The mirror case is pre-existing SUCCESS: if `git merge-base --is-ancestor`
+   (or equivalent ticket/commit evidence) shows your assigned ticket's fix is
+   already on your fork point, you have no code to write — but you also
+   cannot close your own ticket (`ticket.update --status closed` is refused
+   for the ordinary rat role; only a groomer with recorded evidence may close
+   it). An `obstacle` alone does not fix this: it is a live, ephemeral signal
+   the groomer's evidence sweep does not read, so the ticket stays open for
+   redispatch. File `rk ticket new` naming the landing commit and the
+   duplicate/target ticket id so a groomer can close it with evidence, then
+   still run the mandatory verify (step 3) and `rk done` your own ticket.
```

## Why this is safe

This is an additive instruction appended to an existing step; it does not
renumber steps 1-6 (no test in `prime.rs` pins step-4 text as a substring, so
this is a low-risk append, matching the pattern used by proposal 0019). It
does not grant ordinary rats any new daemon capability, does not change the
groomer's closing authority or evidence categories, and does not weaken the
mandatory verify requirement (step 3 still runs regardless — the fix is only
that the finding becomes durable rather than being reported once as an
obstacle and lost). It composes with proposals 0011/0019 (tickets are the
correct durable handoff; obstacles are for live, non-durable signaling).

When landed, add a focused text assertion that step 4 both retains the
"preexisting failure" ticket+artifact sentence and adds the "pre-existing
SUCCESS" sentence naming `rk ticket new`, the landing commit, and the
duplicate/target ticket id. It should not assert a particular ticket ID or
require a specific `stale-rework` evidence format — that remains the
groomer's own contract.

## Durable convention proposal

```json
{
  "rule": "stale-assigned-ticket-needs-a-durable-handoff: If your assigned ticket's fix is already an ancestor of your fork point (verified via git merge-base --is-ancestor or equivalent ticket/commit evidence), you have no code change to make, but you cannot close your own ticket — ordinary rats are refused ticket.update --status closed. File `rk ticket new` naming the landing commit and the duplicate/target ticket id as a durable, groomer-visible handoff. An `obstacle` alone is insufficient: it is a live signal the groomer's evidence sweep does not read, so the ticket stays dispatchable. Still run the mandatory verify and `rk done` your own ticket.",
  "why": "Noodle-11 and Mochi-11 (2026-08-21 cohort) both independently discovered their assigned ticket's substance was already landed via a different ticket's commit and each recorded only an obstacle. Noodle-11's ticket was eventually closed by other means, but Mochi-11's duplicate (TKT-01M0CHJWTECPXJ3J8VY8PB6MG9) is still in_progress and dispatchable as of this scan -- the same failure shape that produced the standalone cleanup ticket TKT-01M0KPRCC4JBRHFJTQ3ZQPKW79. Each redispatch onto a stale ticket burns a full mandatory verify run (600-3400s, ~$0.77-$1.96, 1-3.4M tokens observed here) for zero code delta."
}
```

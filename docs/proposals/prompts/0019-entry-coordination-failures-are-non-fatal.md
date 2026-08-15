# Proposal 0019 — Entry-time coordination failures must not abort the dispatch

**Author:** Shrew-5 (task: TKT-01M01NZC49QVQHM7P4ZMAEKYMA)
**Target prompt:** `crates/rk-core/src/prime.rs` → `FRAGMENT_SPACE` (new bullet)
and `FRAGMENT_COMPLETION` step 1 (scoping clarification)
**Companion convention:** `prove-your-tools-on-entry` (`sug-8772a9x575`,
promoted convention `01KYQRN6MWP7Q5CP7CD4891PYB`) — see "Convention drift"
below
**Status:** landed

## The recurring pain

Django-4 (codex harness) hit `forbidden: ... space.scan` on its entry-time
`rk endorse` call — the on-entry ballot step taught by proposal 0006
(`FRAGMENT_SPACE`: "On entry, also `rk scan suggestion system` and endorse
every open proposal you agree with"). The underlying cause was a codex-harness
regression that dropped rat credential env in sandboxed shell commands
(fixed separately: `crates/rk-harness/src/codex.rs`,
`shell_environment_policy.inherit=all`, with its own regression test). But
Django-4's *response* to the forbidden error was the real loss: it treated the
failure as covered by the entry-safety STOP taught in `FRAGMENT_COMPLETION`
step 1 ("Prove you can LAND... If `rk`... is denied... STOP IMMEDIATELY...")
and stopped without committing anything. A transient, single-call
authentication hiccup on a coordination vote became a fully wasted dispatch —
no ticketed work was attempted at all.

## Root cause in the prompt

Step 1's STOP condition names its trigger as "`rk`... is denied" — unscoped.
Read literally, *any* `rk` subcommand failing at any point satisfies "`rk` is
denied", not just the two specific entry-check calls (`rk scan fact system`,
`git status`) the step actually runs. `rk endorse` also happens at entry (per
0006), so a rat that hits `forbidden` there has every reason to believe it
just found the condition step 1 warned about. Nothing in the prompt
distinguished "I cannot commit or reach the tuplespace at all" (a genuine,
lifetime-ending precondition failure) from "one coordination call bounced"
(a recoverable, low-cost hiccup — a missed vote, not a lost dispatch).

## Proposed diff

`FRAGMENT_SPACE` gains a new bullet, directly after the endorse-on-entry
bullet it qualifies:

```diff
   ever becomes a rule if passing rats spend the one command on it. This is not
   extra work: it is a single cheap call, and it is the only way the fleet turns a
   lesson into a rule without a human. Endorse the existing suggestion rather than
   minting a near-duplicate.
+- A coordination call failing at entry — `rk scan`, `rk endorse`, `rk suggest`,
+  or `rk fact vote` returning `forbidden` or another error — is a soft
+  failure, not a stop condition: it costs you a vote or a read, not your
+  ability to land. Report it with `rk obstacle "<text>"` if that call itself
+  succeeds; if `rk obstacle` also fails, just note the failure in your final
+  summary. Either way, proceed with your ticketed work — do not abort the
+  dispatch over it. This is separate from the LAND-proving check in the
+  completion protocol (can you commit, can you reach the tuplespace at all),
+  which is still a genuine stop.
 - Before editing an area, `rk scan claim <repo>` and `rk scan artifact <repo>`
```

`FRAGMENT_COMPLETION` step 1 gains a trailing scoping sentence — the STOP
condition itself is untouched (same "STOP\n   IMMEDIATELY" wrap, still fires
on the same two entry calls and git writes) but now says explicitly what it
does *not* cover:

```diff
    full lifetime and two finished proposals. Do not assume a denial is
-   transient because your workflow declares broad permissions.
+   transient because your workflow declares broad permissions. This STOP is
+   scoped to the two entry calls above and to git writes; a coordination call
+   failing later on its own (`rk endorse`, `rk scan`, `rk suggest`, `rk fact
+   vote`) is a soft failure, not this stop condition — see Coordination: the
+   tuplespace for how to handle that case.
 2. Commit BEFORE you verify, not after. Your branch is read by other agents
```

## What stays fatal

Unchanged: a rat that cannot commit, is missing its worktree, is in the wrong
repo, or cannot read its own ticket still STOPs immediately per step 1 and per
its role instructions — those are preconditions for doing the actual work
safely, not coordination courtesy calls. Only the specific
scan/endorse/suggest/fact-vote family is reclassified as non-fatal, and only
when it fails on its own (not as a symptom of the daemon or `rk` being
unreachable at all — that is still covered by step 1).

## Safety against the `prime.rs` tests

No existing test asserts step 1's STOP is unscoped, and the literal
`"STOP\n   IMMEDIATELY"` substring (asserted by
`completion_protocol_checks_landability_before_work`) is untouched — the new
sentence is appended after it. A new test,
`entry_coordination_failures_are_non_fatal`, asserts both fragments carry the
proceed-and-report instruction and that the STOP's scoping note follows the
STOP it qualifies, for both `rat` and `reviewer` roles. Full `prime` module
suite: 24/24 passing after this change.

## Convention drift

The `prove-your-tools-on-entry` convention (`sug-8772a9x575`, promoted
`01KYQRN6MWP7Q5CP7CD4891PYB`, landed by proposal 0010) is a near-verbatim copy
of the *pre-diff* `FRAGMENT_COMPLETION` step 1 text, including the same
unscoped "If rk or a git write is denied or errors, STOP immediately" phrasing
that caused this ticket. `crates/rk-core/src/prime.rs` is the system's actual
source of truth — it is rendered into every spawn and is the thing under test;
the convention tuple is an injected reinforcement of the same rule, not an
independent instruction. This proposal edits `prime.rs` directly (the shipped
fragment), per the standard proposal-then-land process for prompt fragments.

The convention tuple itself cannot be edited by a rat (agent callers cannot
write `convention`/`fact` tuples — see proposal 0011) and conventions have no
retract path. It is now stale relative to the landed prompt text in the same
way past superseded conventions have gone stale after their seeding proposal
was refined further. Filed as a ticket rather than fixed inline, per
`preexisting-failure-is-a-ticket-not-an-inline-fix` — refreshing a live
convention's text requires a new `rk suggest` + fleet quorum, which is
future work for whichever rat picks up that ticket, not a blocking part of
this change.

# The daemon tuple-reactor

The reactor is a background component of the daemon that turns the tuplespace
into an event bus: registered `#Trigger` reactions fire a workflow whenever a
matching tuple lands. Dispatch is zero-token and zero-model — a pure predicate
match followed by a `workflow.run`. It is the foundational enabler both research
reports single out (`docs/research/stigmergy.md` P4,
`docs/research/leverage-features.md` #1): steward loops, continuous drain,
obstacle coalescence, and quorum promotion all ride on it instead of each
growing its own bespoke feed consumer.

## Anatomy of a trigger

Triggers are CUE, validated against `crates/rk-workflow/src/triggers-schema.cue`
exactly as workflows are validated against their schema. They live in
`~/.rat-kingdom/triggers/*.cue` (global) or `<repo>/.rk/triggers.cue`
(repo-local); both are discovered every cycle, so edits take effect without a
daemon restart. (The parse is cached and refreshed on file change — see
[Per-cycle cost](#per-cycle-cost).)

```cue
triggers: [
    {
        name:  "drain-on-unblock"              // lowercase-hyphen, unique per file
        match: {                                // predicate over the landing tuple
            category: "event"                   //   (all set fields AND; unset = any)
            identity: "ticket_closed"
            scope:    "myrepo"
            search:   "done"                    //   substring of the payload
        }
        run:   "backlog-drain"                  // a workflow definition name
        repo:  "myrepo"                         // target repo (see resolution below)
        params: {taskId: "{{tuple.payload.ticket}}"}
        exclude: ["daemon"]                     // authors never reacted to
        maxFires: 10                            // per-window fire cap (<=100)
    },
]
```

The `match` block is turned into the same `rk_core::tuple::Pattern` that every
reader in the system uses, so a trigger matches a tuple exactly when a
`scan`/`rd`/`watch` with the same pattern would.

### `action`: workflow vs. daemon-native land

`action?: "workflow" | "land"` (schema: `crates/rk-workflow/src/triggers-schema.cue`)
picks what a match does. Every trigger above is the default, `"workflow"`: spawn
`run`'s named workflow with `params` templated in. `action: "land"` is the one
built-in alternative — it does not spawn a workflow instance at all. Instead the
reactor hands the matched tuple straight to the daemon-native `LandingPipeline`
(`crates/rk-daemon/src/landing.rs`, `Reactor::fire_land_action`), which reads
`branch`/`head_sha`/`target`/`diff_class`/`task` directly off the tuple's own
payload — so `run` is not read for this action and need not be set, and `params`
templating does not apply either. This is what the shipped landing pipeline
example (`examples/triggers-landing-pipeline.cue`) uses; see
[Shipped reaction: the steward and the landing pipeline](#shipped-reaction-the-steward-and-the-landing-pipeline)
below.

### Param templating

Each `params` value is templated from the matched tuple:

| Placeholder | Substitutes |
|---|---|
| `{{tuple.category}}` `{{tuple.scope}}` `{{tuple.identity}}` `{{tuple.instance}}` `{{tuple.id}}` | the tuple's structural fields |
| `{{tuple.payload.<key>}}` | a top-level payload field |

A param whose **whole value** is a single `{{tuple.payload.<key>}}` placeholder
passes the raw JSON value through, preserving its type (so a workflow's `int` or
`bool` param stays typed). Any other value is string-interpolated.

#### Payload hygiene for ingest-sourced tuples

Anyone who can shape the text of an alert or webhook — an annotation, a
`summary` field — can shape part of a rat's prompt once a trigger templates
that field into a spawn step's task title or description. That is a
prompt-injection channel, so the templater does not trust it: a
`{{tuple.payload.<key>}}` substitution whose tuple is **ingest-sourced**
(`instance` starts with `source:` — see `rk_core::sdlc::is_ingest_sourced`,
set by the `ingest.event` RPC for a configured `[[ingest.sources]]` entry) is
rendered through `rk_core::prompt_hygiene::fence_external_text` instead of
spliced in raw. The result is:

- **length-capped** — truncated so unbounded input cannot grow a prompt without
  bound.
- **fenced** — wrapped in a delimited `[EXTERNAL TEXT ...] ``` ... ``` [END
  EXTERNAL TEXT]` block, with backticks in the body neutralized so hostile
  content cannot close the fence early and forge trailing text.
- **provenance-marked** — tagged with the payload key and the ingest source's
  `instance`, and told explicitly not to be followed as an instruction.

This applies uniformly to every string payload field from an ingest-sourced
tuple, including a lone whole-value `{{tuple.payload.<key>}}` placeholder — the
templater cannot tell a free-text annotation apart from a short typed
identifier at this point, so it treats both the same way a hostile annotation
would need to be treated. **If you are writing a trigger that consumes an
ingest-sourced tuple** (an SDLC signal event, or any future ingest source),
expect payload text to arrive fenced in the spawned prompt — do not rely on it
being a bare value usable as, say, a branch name or ticket title; prefer the
tuple's structural fields (`category`/`scope`/`identity`/`id`) or the
envelope's typed/allowlisted fields (`kind`, `environment`, `service`, ...)
for anything that needs to stay a plain, short identifier. Non-ingest tuples
(rat- or daemon-authored, e.g. an `obstacle`'s `text` field) are unaffected —
this hygiene is scoped to the one channel that carries genuinely external
text.

### Target repo resolution

A fired workflow needs a real checkout to run in. The target repo name is, in
order: the trigger's explicit `repo`, else the trigger file's own repo (for a
repo-local `triggers.cue`), else the matched tuple's `scope`. That name is
resolved to a path through the machine-local repo registry (`rk repo add`). A
name that resolves to no registered repo is logged and skipped.

### Ticket lifecycle events: the self-advancing pipeline

The ticket store announces a status transition the reactor can react to. When a
ticket crosses the **non-terminal → terminal** edge (into `done` or `closed`),
`Tickets::edit` emits a `ticket_closed` `Event`, scoped to the ticket's repo,
with `{ticket, status, scope}`. Only the crossing edge fires — a `done → closed`
re-close, or any non-status edit, is silent — so each closed ticket announces
itself exactly once.

That edge is the exact moment a ticket's **dependents can unblock**: a dependent
is ready only once its *last* blocker reaches done/closed. Wiring the shipped
`drain-on-unblock` trigger (`examples/triggers.cue`) turns that announcement into
dispatch:

```cue
{
    name:  "drain-on-unblock"
    match: {category: "event", identity: "ticket_closed", scope: "myrepo"}
    run:   "backlog-drain"
    repo:  "myrepo"
    maxFires: 3
}
```

`backlog-drain`'s `for_each` recomputes the *dependency-aware* ready set (open
tickets with every dependency satisfied — `Tickets::ready`) and atomically claims
each before spawning (`Tickets::claim`, TKT-6). So a just-unblocked dependent is
picked up the instant its blocker closes, instead of waiting for the next
continuous-drain sweep or an operator — the dependency DAG advances itself. The
atomic claim dedups against the continuous drain and any concurrent
fan-out, so running both is safe; `maxFires` caps a close storm; and because the
event rides the durable scan cursor (below), a dropped feed event cannot lose a
close.

## Why dispatch is scan-driven, not feed-driven

The live feed (`Space::subscribe`) is a **lossy** broadcast — capacity 1024, and
it drops events for any consumer that lags. A trigger must never miss an event,
so the feed is used **only as a wake signal**. The source of truth is a durable
cursor over the store:

1. Each cycle captures the store's current persistence-sequence boundary, then
   scans the append-only tuple persistence journal for events whose
   `commit_sequence` is greater than the saved cursor and no greater than that
   boundary, in persistence order. The journal keeps the tuple snapshot even if
   a take, delete, or expiry removes the live row before the reactor scans it.
   SQLite assigns the sequence and journal row inside the tuple's write
   transaction, so delayed writers, concurrent connections, daemon restarts, and
   wall-clock rollback cannot place a committed tuple behind the cursor.
2. Every new tuple is matched against all loaded triggers and any matches are
   dispatched.
3. The cursor advances to the captured boundary and is persisted as a decimal
   sequence in `~/.rat-kingdom/reactor-cursor`. A legacy ULID cursor cannot prove
   which delayed lower-ID writes the old ordering skipped, so migration safely
   replays the deterministic historical baseline from sequence zero. Durable
   reactor markers make that one-time at-least-once replay idempotent.

For an existing pre-journal database, migration preserves the sequence
high-water mark even when older live rows have already been deleted. Surviving
rows receive a deterministic `id ASC` baseline. The unrecoverable deleted prefix
is recorded by `journal_floor_sequence`, and every event after that floor must
form a complete, immutable, contiguous journal suffix. A durable migration
marker distinguishes a legitimate legacy upgrade from a damaged current
journal, so reopening never repairs trusted history from mutable live rows or
silently rewinds the cursor.

This is the same cursor discipline the multiplayer sync loop uses. A dropped
feed event changes nothing: the next scan — woken by the interval tick if
nothing else — still sees the tuple, because the cursor has not passed it. **The
feed is the trigger; the scan is the truth.**

The registry and triggers are loaded *after* the scan snapshot each cycle, so a
repo or trigger registered just before a tuple landed is visible when that tuple
is processed, rather than being dropped as the cursor advances past it.

## At-least-once, made idempotent

Dispatch is at-least-once: a crash between firing and persisting the cursor
re-runs from the last saved cursor. To make that safe, every fired
`(trigger, tuple)` writes a durable **idempotency marker** (a system-scoped
ephemeral event keyed on `<trigger>@<tuple-id>`). `already_fired` short-circuits
a repeat by consulting the immutable persistence journal, so a redelivery — a
crash, marker expiry, or even a full cursor loss — never double-fires. The live
marker still carries a TTL (`[reactor].marker_ttl_secs`, default one week) and
self-collects, while its journal snapshot remains the permanent local dispatch
ledger. Workflow launches also use a deterministic instance ID derived from the
same `(trigger, tuple)` key, closing the crash window between instance persistence
and marker persistence. Initial instance state is written through a synced
temporary file, atomically renamed, and followed by a parent-directory sync on
Unix before execution starts. On daemon restart, all live and archived workflow
IDs are loaded before reactor or scheduler dispatch tasks start. Only after
those consumers are listening are persisted `Running` instances resumed. An
archived stable ID is therefore still occupied and cannot be relaunched by a
replayed tuple.

Nested `sub_workflow` steps persist the active child ID in the parent before the
child snapshot is created. A restart therefore recreates a not-yet-installed
child or rejoins the exact persisted child and its step cursor, rather than
minting a replacement. A direct child result, cleared child link, and parent
resume cursor are committed in one parent snapshot. Inside `when` or `repeat`,
the joined result may be needed by later nested steps, so it is persisted first
while the child link remains occupied; the link is cleared only when the
enclosing top-level cursor commits. A crash therefore cannot acknowledge a child
and then rerun it under a new ID. More than one nested child execution inside one
top-level step is refused fail-closed because there is no independently durable
nested-step cursor. A parent accepts a joined child only after the child's
terminal snapshot is durable; if that write fails, the parent fails with the
child link still occupied. Legacy `Running` children without that parent link
fail closed with their matching parent instead of risking duplicate side effects.
If recording that recovery state fails, the in-memory instance is still marked
non-resumable and reports that its fail-closed status was not durably recorded.
Archive and unarchive transitions reserve stable IDs atomically across the live
and archived registries. Archive snapshot writes, live removals, and map movement
are serialized under that reservation, so overlapping prune requests cannot
roll back one another's committed copy. Snapshot removals are followed by a
parent-directory sync, and a failed transition restores a durable recovery copy
before returning an error.

## Re-entrancy and storm control

A workflow whose action writes a tuple that re-fires its own trigger would loop
forever. Three guards, defence-in-depth:

1. **Self-output tagging.** Every tuple the reactor writes (markers, obstacles)
   is authored by the reserved `reactor` instance, which no trigger ever reacts
   to. A reaction can never fire on its own bookkeeping.
2. **Author exclusion.** A trigger's `exclude` list and the global
   `[reactor].exclude_instances` name authors the reactor skips — the seam for
   excluding workflow-spawned agents that would otherwise feed their own trigger.
3. **Per-trigger rate cap.** Each trigger fires at most `maxFires` times per
   `[reactor].window_secs` (default cap 20, hard ceiling 100 — mirroring the
   `repeat` `max<=100` discipline). Over the cap, the reactor records a
   `reactor_rate_capped` obstacle and skips, so a storm is bounded and visible.
4. **Match scoping.** A trigger can narrow its `match` (a `search` substring, an
   `identity`, a `scope`) so the tuples its own workflow emits fall outside the
   predicate entirely. The steward does exactly this — it matches only
   `"role":"rat"` completions, so the `"reviewer"` completions it spawns never
   match (see the steward section below).

## Per-cycle cost

A wake must stay cheap even under a sustained write burst, when the feed wakes
the reactor on nearly every tuple. Three things keep a cycle bounded:

1. **Bounded firing scan.** The delta scan is
   `commit_sequence > cursor AND commit_sequence <= boundary`, resolved from the
   journal's persistence-sequence primary key rather than a full-table read
   filtered down in Rust. A wake materialises only the tuple events committed
   since the cursor, however large the live store or historical journal.
2. **Cached trigger parse.** Trigger files are parsed with `cue` (a subprocess
   per file). The parse is cached and reused until a file's `(mtime, len)` stamp
   changes, so a steady-state burst reparses nothing — the `cue` shell-outs, the
   reactor's dominant per-wake cost, run only on an actual edit. (Change
   detection is `(mtime, len)`: a same-second, same-length content edit is the
   one case it can miss; trigger files are hand-edited rarely enough that this is
   the intended "reload on change" tradeoff.)
3. **Change-gated recomputes.** Quorum promotion and obstacle coalescence still
   recompute over the **whole store** (so a suggestion / wall that reached quorum
   while the reactor was down is not missed — their guard is the durable
   Convention / open ticket, not the cursor), but only when their relevant
   category population changed since the previous cycle. The gate is an exact SQL
   `COUNT` over `Endorsement`/`Suggestion` (promotion) and `Obstacle`/`Need`
   (coalescence) — cheap (no row materialisation) and independent of tuple ID or
   cursor ordering. A wake carrying only unrelated writes (claims, facts, harness
   results) does no whole-store scan at all. The first cycle after start always
   recomputes, to catch up on any pre-existing backlog.

## First-boot backlog

On a fresh daemon (no cursor file yet) the reactor baselines its cursor to the
newest existing tuple, so it does **not** react to the entire pre-existing
backlog at startup. Only tuples that arrive after boot are dispatched. A restart
resumes from the persisted cursor.

## Shipped reaction: the steward and the landing pipeline

The **steward** is the reactor's flagship autonomy loop and the biggest single
reduction in per-task operator attention: it automates the most-repeated
operator decision, *"is this branch good to merge?"*, reactively triaging
every rat completion (`Event/harness_result`, emitted by `route_completion`).
It ships in two forms, and as of **2026-08-16 the daemon-native form is the one
actually live on this fleet** (`docs/proposals/daemon-native-landing-pipeline.md`
§6, Phase 3/4 of the steward remediation, `memory/steward-investigation`):

- **`action: "land"` — the daemon-native landing pipeline (live).** A trigger
  (shipped as `steward-landing-on-completion` in
  `examples/triggers-landing-pipeline.cue`) hands each matching completion
  straight to `LandingPipeline` (`crates/rk-daemon/src/landing.rs`) — no
  workflow instance spawned to carry it. See
  [The daemon-native landing pipeline](#the-daemon-native-landing-pipeline)
  below.
- **`run: "steward"` — the workflow-driven mega-workflow (pre-cutover
  reference).** The original design: a trigger (`steward-on-completion` in
  `examples/triggers.cue`) spawns `examples/workflows/steward.cue`, which
  hosts the gates, the verdict read, and the routing itself as CUE steps.
  Nothing in a default installation fires it anymore — the operator's
  `~/.rat-kingdom/triggers/` copy was swapped to the landing-pipeline trigger
  in the same cutover — but the file remains in the tree as the reference
  implementation and for its dedicated schema/routing test coverage
  (`crates/rk-workflow/tests/examples.rs`,
  `crates/rk-daemon/tests/workflow_verdict_cache.rs`). It is still the
  behavior you get if you install `examples/triggers.cue`'s copy instead of
  the landing-pipeline one — both remain valid, mutually exclusive choices;
  see [Cutover and rollback](#cutover-and-rollback). Its removal is tracked
  separately (TKT-01M048ASYM00N37EBK1VM7FH5H) and not yet done as of this
  writing.

Both forms triage a completed branch through the same five decisions — a
policy gate, a diff-scope gate, the repo's real test/lint gate, a review
verdict, and a routed outcome — described once below for the live form; the
mega-workflow's steps are the identical logic expressed as CUE (see the
extensive comments at the top of `examples/workflows/steward.cue` if you need
the pre-cutover shape specifically).

### The daemon-native landing pipeline

`steward-landing-on-completion` matches the identical `harness_result`
predicate the old trigger did (`"role":"rat"`, fail-closed on `is_error`/
`declared_done`, §1.5 of the design doc), but its `action: "land"` reads
`branch`/`head_sha`/`target`/`diff_class`/`task` directly off the tuple
payload in Rust and enqueues a `LandingQueueEntry` — a durable, per-`(repo,
target)` FIFO, restart-safe by construction (queue entries are `Furniture`
tuples, not in-process state). `LandingPipeline::run_cycle` drains each
`(repo, target)` key concurrently (different targets in one repo land in
parallel; within one key, candidates still gate-run one at a time).

For each dequeued candidate, in order:

1. **Policy gate** (`steward-protected-paths` named check) — refuses to
   auto-merge a diff touching protected paths
   (`git diff --name-only <target>...HEAD` matched against an ERE).
2. **Diff-scope gate** (`steward-diff-scope` named check) — refuses to
   auto-merge a diff over a per-repo size budget (`maxDiffFiles` /
   `maxDiffLines`, `0` = off), so a runaway rat that dodges protected paths
   but rewrites half the repo is held for a human rather than auto-merged.
3. **Real test/lint gate** — the repo's own named check (`verify` by
   default), run for real; teeth the harness cannot forge.

All three run in a **persistent, daemon-owned detached worktree** —
`<home>/gate-worktrees/<repo>/<target>`, reset to the candidate's tip each
time — instead of a throwaway agent worktree. That is the structural change
from the mega-workflow: no agent spawn is needed just to have somewhere to run
three deterministic checks.

4. **Review** (only if `diff_class` is not `doc-only`/`trivial`): a
   commit-keyed verdict-cache probe (`Pattern::for_commit`, scoped to
   `(repo, branch, head_sha)`) runs first, directly against the tuplespace —
   no CUE read step. A **hit** (any prior `APPROVE`/`REWORK`/`STOP` for this
   exact branch tip) is reused without spawning a second opinion. A **miss**
   spawns the shrunk `examples/workflows/steward-review.cue` — a reviewer
   chained onto the candidate branch, its *only* job — and parks on the
   verdict tuple itself (`space.rd`, not the workflow instance's completion
   state), which is what makes review survive a daemon restart mid-wait (see
   [Restart safety](#restart-safety)). `doc-only`/`trivial` diffs and a cache
   hit both reach step 5 with **zero agent spawns**.
5. **Route** the verdict (fresh or cached), or the unconditional pass for a
   diff that skipped review entirely:
   - `APPROVE` (or no review needed) → `Supervisor::land` the branch onto its
     **land target** directly (see
     [Land target inheritance](#land-target-inheritance) below);
   - `REWORK` → `Tickets::create` a follow-up ticket directly, hold the
     branch;
   - `STOP` / unrecognized verdict → `Space::out` a `need` tuple directly
     (ranked into `rk inbox` *and* pushed through every configured
     [notification sink](#notification-sinks)), hold the branch;
   - a failing or timed-out gate → a durable `gate-failure` artifact + a
     `need`, hold the branch.

   None of these five outcomes shells out or spawns an agent — every one is a
   direct async call from `LandingPipeline` into `Supervisor`, `Tickets`, or
   `Space` (design doc §2.4). Every gate still **fails closed**: a
   protected-path hit, an over-budget diff, or a red suite holds the branch,
   surfaced in `rk inbox`. Auto-merge is only ever reached through a clean
   policy gate, a within-budget diff, a green suite, and (when review wasn't
   skipped) an explicit `APPROVE`.

**Operator-facing landing authority has moved.** The mega-workflow's `land`
step was gated by two daemon config knobs — `policy.automated_landing_workflows`
(only a workflow named in this list may `land` unattended) and
`policy.require_approval_for_landing` — both enforced in
`crates/rk-daemon/src/workflow_exec.rs`. `LandingPipeline` calls
`Supervisor::land` directly and never passes through that code path, so
**neither knob governs the daemon-native pipeline**: for a repo whose triggers
include an `action: "land"` entry, the trigger's own existence and match
predicate *is* the unattended-landing authorization. This is the intended end
state (steward remediation Phase 4, item 4: "landing authority becomes the
daemon pipeline's own, not a string match on a workflow filename"), but the
two config fields are not yet narrowed or removed from `rk-core`'s
`PolicyConfig` (still `automated_landing_workflows: ["steward"]` by default) —
they remain load-bearing only for the pre-cutover mega-workflow path. Tracked:
TKT-01M048ASY8MDB5DVV5VG3WRM47.

### Land target inheritance

`fire_land_action` reads `target` straight off the completed rat's own
`harness_result` payload (default `"main"` if absent) — the identical field
(`record.target_branch`, set in `supervisor.rs`: the completed rat's own
`--base` when one was given at spawn, else the repo's configured delivery
target) the mega-workflow's `target: "{{tuple.payload.target}}"` trigger param
used. So the landing pipeline's land target silently **inherits the completed
rat's base branch**, not a fixed `main` — unchanged behavior from before the
cutover.

This is deliberate for chained work: a rework rat spawned with
`--base rat/feature/original-branch` (or a workflow step chained onto a prior
step's branch) wants its own landing pass to review-and-land onto that same
feature branch, not skip past it to `main` — the feature branch is then landed
to `main` as a whole once complete.

**Visibility parity with the reactor path.** Before the cutover,
`note_non_main_land_target` (`crates/rk-daemon/src/reactor.rs`) fired a
repo-scoped `reactor_non_main_land_target` event whenever a workflow-firing
trigger's interpolated `params.target` was not `"main"`, and `rk workflow
list` appended `target=<branch>` to that instance — both ways an operator
could see a completed steward had landed somewhere other than `main`.
`fire_land_action` never called `note_non_main_land_target` (that call site
is specific to the workflow-firing path), and the zero-agent-spawn fast paths
(step 4 above) never had a workflow instance for `rk workflow list` to
annotate either — so a non-`main` land through the daemon-native pipeline was
invisible by both of the old mechanisms
(TKT-01M0B71D9B51SV5AG95VR1A4ST). This is now fixed:
`LandingPipeline::note_non_main_land_target` (`crates/rk-daemon/src/landing.rs`)
mirrors the reactor helper's shape and is called from every
`Supervisor::land` call site in the pipeline — the doc-only/trivial fast path
and the `APPROVE` verdict-routing arm — emitting a repo-scoped
`landing_non_main_land_target` event whenever the resolved `entry.target` is
not `"main"`. `rk scan event <repo>` for `branch_landed` tuples
(`Supervisor::land`'s own event, which always carries `target`) remains a
valid way to notice too, but is no longer the only one.

If you want base-chained completions reviewed but held for an explicit
decision instead of auto-landed, override `target` in a repo-local trigger
copy (pin it to `"main"` or the repo's configured delivery target) rather than
editing the shared global one — that changes the tradeoff for every
chained/rework rat in the repo, including the ones the ergonomics exists for.

**Re-entrancy — match scoping.** The steward is the worked example of the fourth
re-entrancy technique: its trigger's `match.search` is `"role":"rat"`, so it
fires only on plain-rat completions. The reviewer `steward-review.cue` spawns
completes as a `"reviewer"` (a field carried on every `harness_result`), whose
payload the search does not contain — so the pipeline never re-triggers itself
on the branch it just reviewed. A reworked ticket, once drained, completes as
a `"rat"` and re-enters the pipeline: a closed loop, not a runaway.

> Installing either steward trigger makes **all** matching rat completions
> auto-merge on a clean verdict. Do not run both `steward-on-completion` and
> `steward-landing-on-completion` in the same triggers directory at once —
> they match the identical predicate and would double-dispatch every
> completion — and do not also run an approval-gated workflow
> (`land-on-approve`) over the same completions, or they race for the branch.

### Restart safety

Three independent pieces of state, each durable rather than held only in
process memory (design doc §2.6):

- **The queue itself** — `Furniture` tuples plus a persisted per-repo seq
  counter file; a restart simply resumes scanning. A crash between a queue
  entry's status-transition write and its predecessor's delete is closed too
  (write-then-delete with a `rev` counter, not delete-then-write) — a crash in
  that gap leaves two tuples sharing one `seq` instead of losing the entry
  outright, self-healed by the next read.
- **In-flight candidate state** — a status field (`queued` / `running_gates` /
  `awaiting_review`) lives on the durable queue entry itself. A restart's
  `run_cycle` treats every status as eligible and reprocesses it: gates are
  idempotent (a warm-worktree reset + shell command has no side effect unsafe
  to redo), and a candidate found `awaiting_review` just re-issues the same
  `space.rd` wait — if the reviewer already wrote its verdict while the daemon
  was down, the durable tuple is already there and the wait resolves
  immediately without spawning a second reviewer.
- **Never double-land / never orphan a reviewer** — `Supervisor::land`'s CAS
  (`Repo::advance_target`'s `update-ref <new> <old>`) makes a duplicate `land`
  call on an already-merged branch a clean no-op; a `work_key = (repo, branch,
  head_sha)` dedup marker (`landing_processed`) drops a redelivered completion
  for an already-fully-processed candidate before it is even re-enqueued. And
  because the pipeline parks on the verdict *tuple*, not the reviewer's
  *workflow instance*, a daemon restart doesn't lose track of an in-flight
  reviewer — the reviewer keeps working independently and the restarted
  pipeline's `space.rd` on the same durable pattern picks up its verdict
  whenever it lands.

Covered end to end by `crates/rk-daemon/src/landing.rs`'s own test module —
`restart_mid_gate_run_resumes_and_lands`,
`park_and_resume_survives_space_level_restart_with_late_verdict`,
`crash_between_write_and_delete_survives_the_entry`,
`burst_of_completions_on_one_key_never_runs_gates_concurrently`, and
`distinct_keys_drain_concurrently_within_one_run_cycle` — plus
`crates/rk-daemon/tests/{land_on_approve,automated_landing,dropped_land}.rs`
for the surrounding merge/delivery/inbox behavior.

### Cutover and rollback

Full runbook: `docs/proposals/daemon-native-landing-pipeline.md` §6. Summary:

1. **Preconditions** — T1–T4 merged and the daemon rebuilt/restarted (a merged
   Rust change is not live until redeployed); the target repo's
   `.rk/checks.cue` registers `steward-protected-paths`, `steward-diff-scope`,
   and its real `verify` check; `examples/workflows/steward-review.cue` is
   installed wherever `steward.cue` was.
2. **Swap the trigger** — copy `examples/triggers-landing-pipeline.cue`
   alongside the existing trigger file under a *new* filename, then remove or
   rename the old `steward-on-completion` entry in the same change (never run
   both at once — see the warning above). Restart the daemon, or wait for the
   trigger file's mtime-based reparse.
3. **Verify parity by hand** before trusting it unattended (no automated
   `rk workflow drift`-equivalent exists yet, §6.5): land one doc-only/trivial
   change and confirm zero agent spawns; land one change needing real review
   and confirm exactly one reviewer spawn; force a REWORK and a STOP and
   confirm they surface in `rk inbox` in the same shape the workflow-driven
   steward's did; force a failing gate and confirm a `gate-failure` artifact
   plus a held branch.
4. **Rollback** — restore the original `steward-on-completion` trigger file
   and remove/rename the `action: "land"` one. Nothing about the swap is
   destructive to in-flight state: a fully-processed candidate has no live
   queue entry to roll back, and one still mid-flight when the trigger set is
   swapped back simply stops being drained (it stays inert in the durable
   queue until the landing trigger is restored or an operator manually
   inspects/clears it — there is no automatic queue-to-workflow migration).
5. **Known gaps**, tracked rather than fixed inline: no automated drift/parity
   check; no attempt-counter backstop on `LandingQueue` analogous to the
   reactor's `MAX_FIRE_ATTEMPTS` (a candidate whose processing keeps erroring —
   not gate-failing, an actual `Err` — retries every poll cycle indefinitely);
   gate-worktree disk/lifetime management is unbounded; and the land-target
   visibility gap noted above.

## Built-in reaction: quorum promotion

Beyond firing `#Trigger` workflows, every cycle the reactor runs one built-in
reaction: promoting fleet **suggestions** into **conventions** at quorum. This is
the flagship stigmergy loop — proposals become shared norms with no operator in
the path.

- `rk suggest '<text>'` writes a `Suggestion` (system scope, authored by
  `RK_AGENT`) and prints a `sug-…` id. It is **durable** (`Session`): the ballot
  closes on its outcome — promotion — not on a clock (TKT-168).
- `rk endorse <sug-id>` writes an `Endorsement` keyed by
  `(identity = suggestion, instance = RK_AGENT)`. Re-endorsing is idempotent —
  the CLI skips a duplicate, and the reactor counts **distinct** endorsers
  regardless, so a double vote can never inflate the tally. Endorsements are
  durable too, so the three endorsers reaching quorum never have to overlap.
- The reactor recomputes, **by full scan** (never off the lossy feed), the
  distinct-endorser count per suggestion. At `quorum` it emits a `Convention`
  (system scope, **Furniture** — permanent, never `in`-consumable) citing the
  suggestion text and the sorted endorser set. The Convention is its own
  promote-once guard: a suggestion that already has one is skipped. System-scope
  Conventions replicate across castles via `rk sync` for free.

Because the count is recomputed by scan at fire time, promotion is robust to
missed feed events and to endorsements arriving across many cycles. The
recompute is **change-gated** (see [Per-cycle cost](#per-cycle-cost)): it runs
only when the `Endorsement`/`Suggestion` population changed since the previous
cycle, so its input is still the whole store but it no longer re-scans on a wake
that carried no relevant tuple.

### The composed convention-quorum loop

Quorum promotion is only the middle hop of a three-hop loop that turns a stray
proposal into behaviour every future rat follows — with **no human and no model
in the path**:

1. **Propose + endorse** (rats). A rat runs `rk suggest '<norm>'` during its
   work; peers who agree run `rk endorse <sug-id>`. Both tuples are system-scope
   and durable — the tally accumulates until the norm passes.
2. **Promote at quorum** (reactor). The built-in `promote_conventions` above
   mints a Furniture `Convention` once `[reactor] quorum` distinct endorsers back
   one suggestion.
3. **Inject at spawn** (supervisor, TKT-18). At spawn the supervisor scans active
   conventions for the rat's repo scope + `system` and composes their text into
   the rendered prompt as a binding **"Standing conventions"** section — so a
   promoted norm changes what the next rat *does*, not just what it *could read*.

Hop 1 has a failure mode the other two do not: it needs three *separate* rats to
back one proposal, and nothing tells a rat a ballot is open. Measured against the
live space on 2026-07-25 — `rk scan convention` = 0, `rk scan endorsement` = 0,
`rk scan suggestion` = 0, over **277 spawns**. A `Convention` is Furniture
(permanent), so zero conventions means nothing had ever reached quorum. Not a
broken mechanism: an undriven one. Three separate rats had each proposed a norm,
asked the room to endorse it, and watched the ballot decay unanswered.

Two things drive it. First, `rk inbox` surfaces every open ballot as an
**`open-suggestion`** row (TKT-167) — the id, the proposer, the text and the live
`n/quorum` tally, with `rk endorse <sug-id>` as the resolving command. Rows drop
out the moment the proposal promotes (a `Convention` carries its id) or if
`quorum = 0` disables promotion entirely. This does not replace hop 1 — peers
endorsing peers is still how a norm should pass — it adds the **one endorser who
is always reachable**. `rk endorse` therefore does not require the spawn
environment: run outside a rat (no `RK_AGENT`) it votes as the single distinct
endorser `operator`.

Second, ballots no longer expire (TKT-168). They used to carry a 24h voting
window, which made quorum mean *three distinct rats inside one overlapping 24h
window* — a wall-clock bound on a fleet whose activity is bursty and whose rats
live minutes. Decay was the wrong instrument for three reasons a longer window
would not have fixed: a vote **cannot be reinforced** (its author is dead minutes
after casting it, so decay destroys information nobody can regenerate, unlike a
`claim` its holder re-runs while still working); decay **buys no freshness**
(promotion mints a permanent Furniture `Convention` regardless, so expiring the
ballot only ever makes promotion harder); and an Ephemeral tuple **does not
replicate** (rk-sync exports durable lifecycles only, so a windowed ballot was
invisible to peer castles while the Convention it promotes to replicates —
castles could never pool votes). A ballot is a ledger entry, not a pheromone.
`rk suggest --ttl` / `rk endorse --ttl` still time-box one deliberately.

The seam between hops 2 and 3 is a real contract: the convention must carry the
suggestion's **non-blank** `text`, because the injection step drops a blank-text
convention (a norm whose source text decayed to empty would reach quorum yet
never bind). `crates/rk-daemon/tests/convention_quorum.rs` pins that contract
end to end over the wire; `scripts/convention-quorum-demo.sh` runs all three
hops against a throwaway daemon with the real `rk` CLI.

> **No trigger closes this loop — and you must not try to add one.** All three
> hops are built-ins; a promoted `Convention` is authored by the reserved
> `reactor` instance, which the dispatcher skips *before* matching triggers (the
> re-entrancy break). A `#Trigger` on `category: convention` would type-check and
> then silently never fire. If you need a hook, react to the rat-authored
> `suggestion`/`endorsement` tuples upstream, never to the `convention`
> downstream. See `examples/triggers.cue`.

## Built-in reaction: obstacle coalescence

The second built-in closes the flat **obstacle** pile into the durable backlog.
Ten rats hitting one wall used to produce ten equal, signal-less obstacles; now
a wall many rats converge on files exactly one ticket.

- Each cycle the reactor buckets every `Obstacle`/`Need` tuple by a **normalised
  topic key** — the tuple `scope` plus a case- and punctuation-folded, length-
  bounded form of `payload.text`. "Cargo build FAILS!!" and "cargo build fails"
  land in the same bucket; the same wall in two repos does not.
- It counts **distinct reporters** (`instance`) per topic, recomputed by full
  scan — a rat re-stating its own obstacle can never inflate the tally (and, as
  each rat's obstacle is keyed `identity = instance = agent`, it holds one trail
  per topic anyway). Like quorum promotion this recompute is **change-gated** on
  the `Obstacle`/`Need` population (see [Per-cycle cost](#per-cycle-cost)).
- At `coalesce_quorum` distinct reporters it files **one** `task` ticket (labelled
  `obstacle-coalesce`, authored by `reactor`) whose body links the contributing
  tuples and reporters. Filing is idempotent two ways: a synchronous durable
  "already filed" marker written **before** the create bridges the create
  latency, and the still-open ticket — which carries the topic's `coalesce_key`
  in its payload — suppresses re-filing until it is closed. So a topic files once
  until closed, then may re-file only if fresh obstacles re-accumulate.

Coalescence never injects synthetic obstacles into the pile — the sub-quorum
"how hot is this wall" gradient already lives in the raw obstacles' own decaying
strength, which a strength-sorted scan ranks. This built-in only escalates a
converged-on wall into durable, closable work.

## Built-in reaction: escalation notification

The third built-in turns a steward escalation into an **active** operator push.
The steward already surfaces a `STOP`/unknown verdict as a `need` (identity
`steward`) that `rk inbox` ranks — a *passive* queue the operator polls. This
built-in builds a channel-agnostic `EscalationNotice` the moment such a `need`
lands and fans it out through the [`SinkRegistry`](#notification-sinks), so a
human is pushed at instead of only finding it on their next inbox check.

- The discriminator is `category = need` **and** `identity = "steward"`. A rat's
  own `rk need` keys on its agent name, so an ordinary help request is left on
  the inbox queue and never pops a notification.
- Delivery is per `(need tuple, sink)`, each guarded by its own durable
  idempotency marker (`notify-escalation@<tuple>@<sink>`, `Event/reactor_fired`
  underneath) — an at-least-once re-scan never double-pops any one channel, and
  a channel added later starts fresh rather than inheriting another channel's
  "already delivered" state. The `herdr` sink additionally honours the
  pre-registry marker key (`notify-escalation@<tuple>`), so a daemon upgraded
  mid-flight does not re-pop a notice it already showed. A reinforced
  escalation keeps its id below the cursor and is never re-seen, so a repeat
  push only happens after the old `need` evaporates and a fresh one is written
  (the intended de-spam).
- Each sink is independently best-effort: a dead or unreachable channel (no
  herdr server running, a `command` sink's program missing) produces a logged
  failure and nothing else. The escalation is already durable and ranked on
  `rk inbox` regardless, so one sink's outage never suppresses another's
  delivery or stalls the reactor cycle. Set `notify_escalations = false` to
  keep escalations purely on the passive `rk inbox` queue — a hard kill switch
  that empties the sink list outright, independent of `[[notify.sinks]]`.

### Notification sinks

Which channels an escalation reaches is `[[notify.sinks]]` config, not a
property of the call site (`crates/rk-core/src/notify.rs`). An escalation
source builds one channel-agnostic `EscalationNotice` and hands it to
`SinkRegistry::fan_out`; the registry — built once at reactor construction from
resolved config — decides which configured sinks accept it and delivers to
each independently.

```toml
[[notify.sinks]]
kind = "herdr"                          # desktop push (rk-mux, the historical default)

[[notify.sinks]]
name = "ops-chat"                       # dedup/registry key; defaults to kind if unset
kind = "command"                        # shell out to an operator program
classes = ["steward-escalation"]        # notice classes this sink accepts; empty = all
min_severity = "warn"                   # info (default) | warn | critical

[notify.sinks.options]
command = "/usr/local/bin/rk-notify-chat"
timeout_secs = "30"
```

- **Kinds.** `herdr` (desktop push via `HerdrMux::notify`, registered in
  `rk-daemon`'s `sink_factory` since `rk-mux` cannot be reached from
  `rk-core`), `log` (emits through `tracing` at the notice's severity — no
  options, cannot fail to install, the useful second sink on a headless
  castle), and `command` (execs `options.command` directly — not a shell line
  — with the notice as `<title> <body>` on argv, `RK_NOTICE_TITLE`/`_BODY`/
  `_TEXT`/`_CLASS`/`_SEVERITY`/`_SCOPE`/`_SUBJECT`/`_TUPLE`/`_ACTION` plus
  `RK_NOTICE_REF_<KEY>` per ref in the environment, and the full notice as JSON
  on stdin; bounded by `options.timeout_secs`, default 10s, past which the
  child is killed and the delivery reported failed). Registering a new kind is
  one `SinkFactory::with_kind` call — no change to the reactor or any
  escalation source. An unknown `kind` in a table is skipped and logged
  (`error!`, since a channel the operator believes in but which never built is
  a silent loss of every future escalation on it), never fatal to the daemon.
- **Back-compat mapping (`NotifyConfig::resolved`).** `notify_escalations =
  false` ⇒ zero sinks, full stop — that bool predates this section and stays a
  hard kill switch rather than silently losing its meaning to config the
  operator never wrote. An empty `[[notify.sinks]]` (the default — unset is
  not the same as "no notifications") ⇒ exactly the historical behaviour,
  expressed as one default `herdr` sink. Any non-empty `[[notify.sinks]]` ⇒ the
  operator's list, verbatim: a second channel is a table, not a patch.
- **Filtering.** A sink accepts a notice when it is `enabled` (default true),
  its `classes` list is empty or contains the notice's `class`, and the notice's
  `severity` is at or above `min_severity`.
- **Markers are per-`(tuple, sink)`.** See the discriminator note above — this
  is what lets a second sink be added without inheriting the first sink's
  delivery history, and what lets the `herdr` sink alone honour the pre-sink-
  registry marker key for upgrade continuity.

## Lifecycle hooks

Where the escalation notification above is one hardwired reaction to one
tuple shape, lifecycle hooks are the general, operator-configured form: run a
program whenever a tuple satisfies one of a fixed vocabulary of lifecycle
events, at castle or repo scope. Loaded and dispatched exactly like
[triggers](#anatomy-of-a-trigger) — same fan-in, same `cue` shell-out
discipline, same file-stamp cache — but a hook's program is a side effect of
the event, never a state change the reactor is answerable for, so a failing
hook degrades exactly like the escalation push above: logged, rate-capped
announced, and never able to stall the cycle or fail the triggering
operation.

- **Vocabulary.** `agent_spawned`, `agent_completed`, `agent_failed`,
  `agent_dismissed`, `branch_landed`, `gate_failed`, `escalation_raised` (see
  `hook_event_for_tuple` in `crates/rk-daemon/src/reactor.rs` for the exact
  tuple each maps from — `agent_completed`/`agent_failed` are both the one
  `harness_result` event, split on its `is_error` field).
- **Scopes.** Castle-level hooks live at `<home>/hooks/*.cue` — a directory no
  rat has filesystem or RPC access to, the same absence-of-capability that
  already keeps `<home>/triggers/*.cue` operator-only. Repo-level hooks live
  at `<repo>/.rk/hooks.cue`, read from the *registered* checkout the daemon
  only advances on a landed (reviewed, merged) branch, mirroring
  `.rk/triggers.cue`'s trust boundary. Neither has an RPC method that writes
  it, so a rat cannot register a hook by any path.
- **Repo scoping is additive, not overriding.** An explicit `hook.repo` field
  always wins; otherwise a repo-local hook file scopes to the repo it was
  discovered in; a castle-level hook with neither fires for every repo's
  matching event. A castle hook and a same-event repo hook both fire for that
  repo — "repo extends castle," the same fan-in relationship triggers already
  have, not a same-name override.
- **Payload.** The event tuple as JSON on stdin, plus `RK_HOOK_NAME`,
  `_EVENT`, `_TUPLE`, `_SCOPE`, `_IDENTITY`, `_INSTANCE`, and — when the tuple
  carries an `agent` field and the event is one of the three agent-terminal
  ones (`agent_completed`/`_failed`/`_dismissed`) — `RK_HOOK_AGENT` and
  `RK_HOOK_TRANSCRIPT_PATH` (that generation's own `agent-logs/*.jsonl` file,
  via `Supervisor::latest_transcript_path`), so an archive hook can ship the
  transcript deliberately rather than a personal Claude hook egressing it
  unconditionally (see `TKT-01M0B8H18Z7FC5CB906AGC6KNF`, the incident this
  feature answers).
- **Execution.** `rk_core::exec::run_piped` — the same spawn/stdin/bounded-
  wait primitive `notify::sinks::CommandSink` uses, extracted so there is one
  out-of-process execution path in the daemon, not two. `hook.timeoutSecs`
  bounds the child (default 10s); a program is exec'd directly, not a shell
  line, for the same injection-avoidance reason `[[notify.sinks]]`'s command
  sink is.
- **Idempotency and failure.** One dispatch per `(hook, tuple)`, sharing the
  triggers' `already_fired`/`reactor_fired` marker ledger under a `hook:`-
  prefixed key. A failing or wedged hook is always logged
  (`tracing::warn!`); a `hook_command_failed` obstacle is additionally written
  at most once per ten minutes *per hook name*, so a hook that matches every
  completion in a busy repo cannot flood `rk inbox` with one obstacle per
  tuple.
- **Not (yet) onboarding-gated.** Unlike a brand-new repo's first
  `.rk/triggers.cue`, landing a change to `.rk/hooks.cue` on an
  *already-registered* repo takes effect the moment the daemon's registered
  checkout advances (ordinary review + merge), the same as any other trigger
  file edit — there is no separate onboarding-proposal step specific to
  hooks. See `TKT-01M0BV4Z1Z48ENFE37PWWP846P`'s follow-up ticket if stricter,
  proposal-gated activation for hook files specifically is ever wanted.

## Built-in reaction: resolution backlinks

The third built-in turns solved walls into **institutional memory as a living,
decaying structure** (stigmergy P8): the next rat hitting a wall someone already
fixed is steered to the prior artifact instead of redoing the work.

A rat records the fix with a backlink:

```bash
rk out artifact <scope> <name> --payload '{...}' --resolves <obstacle-or-need-id>
```

`--resolves` rides in the artifact payload as `resolves: <id>`. The reactor then
reacts to the artifact per-tuple, in the same cursor delta loop as trigger firing
(not a change-gated whole-store recompute):

- **On a resolving `Artifact`** it looks up the exact `Obstacle`/`Need` named by
  `payload.resolves`, **retires** that wall (a targeted delete — "solved"), and
  lays a `Resolution` trail keyed on `(scope, normalised-topic)` pointing at the
  artifact (`artifact_id`, `text`, `resolved`). The trail is written through
  `reinforce`, so re-resolving a topic refreshes the single trail in place at
  full strength rather than piling up duplicates.
- **On a fresh `Obstacle`/`Need`** whose topic already has a `Resolution` trail,
  it **reinforces** that trail (a rat hit this wall again, so it is still live)
  and **steers** the reporting rat with a directed `Message`
  (`type: resolution_steer`, `identity = the rat`) carrying the artifact backlink.
  One steer per obstacle tuple: a durable guard keyed on the obstacle id
  suppresses a crash-replay from re-messaging.

The `Resolution` trail is a pheromone like `claim`/`obstacle`/`need` — it carries
a decaying `strength` and is collected by GC once nobody re-needs it (TKT-14). A
wall many rats keep hitting keeps its resolution hot; a one-off fix fades. Read
the live map with `rk scan resolution <scope>` (or `--hot`). The whole reaction
is naturally idempotent — the wall delete is a no-op once gone, the trail write
is an upsert, and the steer is guarded — so at-least-once redelivery is safe.

A promoted norm only matters if it changes behaviour (stigmergy P6). So at spawn
the supervisor scans active `Convention` tuples for the rat's repo scope and
`system`, and composes their text directly into the rat's system prompt as a
**Standing conventions** section (`prime.rs::render`). This makes a
quorum-promoted convention binding on every rat spawned afterward, instead of
relying on the rat choosing to `rk scan convention`. A rat already running when a
convention crosses quorum is unaffected until it is respawned; steering live rats
on promotion is a possible follow-up.

## Configuration

```toml
[reactor]
enabled = true            # master switch; false = the reactor loop never starts
interval_secs = 30        # fallback scan cadence (the feed also wakes a cycle)
window_secs = 60          # rolling window for the per-trigger rate cap
max_fires = 20            # default per-trigger cap; a #Trigger may lower it
marker_ttl_secs = 604800  # idempotency-marker lifetime (one week)
exclude_instances = []    # authors never reacted to, besides "reactor"
quorum = 3                # distinct endorsers that promote a suggestion; 0 = off
coalesce_quorum = 3       # distinct reporters that coalesce a wall into a ticket; 0 = off
notify_escalations = true # false = hard kill switch, zero notification sinks, inbox-only
```

See [Notification sinks](#notification-sinks) for `[[notify.sinks]]`, the
per-channel table that decides *which* channels a `true` here actually reaches.

## Where it lives

- Schema: `crates/rk-workflow/src/triggers-schema.cue`; loader
  `rk_workflow::load_triggers`.
- Reactor: `crates/rk-daemon/src/reactor.rs` (`Reactor::run_cycle`, plus the
  built-ins `Reactor::promote_conventions`, `Reactor::coalesce_obstacles`,
  `Reactor::notify_escalation`, `Reactor::note_non_main_land_target` (the
  [land target inheritance](#land-target-inheritance) visibility event), and
  the resolution backlinks `Reactor::link_resolution` /
  `Reactor::steer_from_resolution`), spawned as a loop next to the GC and sync
  loops in `crates/rk-daemon/src/server.rs`.
- Suggest/endorse + `--resolves` sugar: `crates/rk-cli/src/space_cmds.rs`
  (`suggest`, `endorse`, `out`).
- Config: `rk_core::config::ReactorConfig`.
- Tests: `crates/rk-daemon/tests/reactor.rs` (live-daemon fire, idempotency under
  feed loss + cursor reset, re-entrancy/exclusion, rate cap, quorum promotion,
  obstacle coalescence — quorum, per-scope/topic separation, idempotent
  re-filing; steward escalation notify — fires once, steward-only, disable
  switch; and resolution backlinks — retire-and-lay-trail plus
  steer-and-reinforce with replay idempotency) plus unit tests in the reactor and
  workflow modules.
- Composed convention-quorum loop: self-test
  `crates/rk-daemon/tests/convention_quorum.rs` (suggestion → quorum →
  injectable convention, over the wire), runnable demo
  `scripts/convention-quorum-demo.sh`.
- Example: `examples/triggers.cue`.

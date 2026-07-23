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
        name:  "drain-on-new-ticket"           // lowercase-hyphen, unique per file
        match: {                                // predicate over the landing tuple
            category: "event"                   //   (all set fields AND; unset = any)
            identity: "ticket_created"
            scope:    "myrepo"
            search:   "priority"                //   substring of the payload
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

### Param templating

Each `params` value is templated from the matched tuple:

| Placeholder | Substitutes |
|---|---|
| `{{tuple.category}}` `{{tuple.scope}}` `{{tuple.identity}}` `{{tuple.instance}}` `{{tuple.id}}` | the tuple's structural fields |
| `{{tuple.payload.<key>}}` | a top-level payload field |

A param whose **whole value** is a single `{{tuple.payload.<key>}}` placeholder
passes the raw JSON value through, preserving its type (so a workflow's `int` or
`bool` param stays typed). Any other value is string-interpolated.

### Target repo resolution

A fired workflow needs a real checkout to run in. The target repo name is, in
order: the trigger's explicit `repo`, else the trigger file's own repo (for a
repo-local `triggers.cue`), else the matched tuple's `scope`. That name is
resolved to a path through the machine-local repo registry (`rk repo add`). A
name that resolves to no registered repo is logged and skipped.

## Why dispatch is scan-driven, not feed-driven

The live feed (`Space::subscribe`) is a **lossy** broadcast — capacity 1024, and
it drops events for any consumer that lags. A trigger must never miss an event,
so the feed is used **only as a wake signal**. The source of truth is a durable
cursor over the store:

1. Each cycle scans the store for tuples with `id` greater than the saved cursor
   (ULIDs sort by creation time), in order.
2. Every new tuple is matched against all loaded triggers and any matches are
   dispatched.
3. The cursor advances to the newest scanned id and is persisted to
   `~/.rat-kingdom/reactor-cursor`.

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
a repeat, so a redelivery — a crash, or even a full cursor loss — never
double-fires. Markers carry a TTL (`[reactor].marker_ttl_secs`, default one
week) and self-collect; they only need to outlast any plausible redelivery.

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

1. **Bounded firing scan.** The delta scan is `id > cursor` resolved from the
   `id` PRIMARY KEY index (`Pattern::after`), not a full-table read filtered down
   in Rust. A wake materialises only the tuples added since the cursor, however
   large the store.
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
   (coalescence) — cheap (no row materialisation) and, unlike a cursor delta,
   immune to the same-millisecond ULID ordering that could otherwise drop a
   just-added tuple from the change signal. A wake carrying only unrelated writes
   (claims, facts, harness results) does no whole-store scan at all. The first
   cycle after start always recomputes, to catch up on any pre-existing backlog.

## First-boot backlog

On a fresh daemon (no cursor file yet) the reactor baselines its cursor to the
newest existing tuple, so it does **not** react to the entire pre-existing
backlog at startup. Only tuples that arrive after boot are dispatched. A restart
resumes from the persisted cursor.

## Shipped reaction: the steward

The **steward** (`examples/workflows/steward.cue` + the `steward-on-completion`
trigger in `examples/triggers.cue`) is the reactor's flagship autonomy loop and
the biggest single reduction in per-task operator attention: it automates the
most-repeated operator decision, *"is this branch good to merge?"*. It is not a
Rust built-in — it is a plain trigger + workflow you opt into by copying both
into place, composing primitives that already exist.

On **every rat completion** (`Event/harness_result`, emitted by
`route_completion`), the steward reactively triages that rat's branch:

1. spawns a cheap reviewer chained onto the completed branch;
2. runs a **policy gate** — refuses to auto-merge a diff touching protected
   paths (`git diff --name-only <target>...HEAD` matched against an ERE);
3. runs the repo's **real test/lint gate** (`run` step — teeth the harness
   cannot forge);
4. `read`s the reviewer's `APPROVE`/`REWORK`/`STOP` verdict artifact and routes:
   - `APPROVE` → `land` the branch straight onto `main` (auto-merge);
   - `REWORK` → file a follow-up ticket, hold the branch;
   - `STOP` / unknown → escalate via a `need` tuple (ranked into `rk inbox`
     *and* pushed to the operator's desktop by the [escalation-notify
     built-in](#built-in-reaction-escalation-notification)), hold the branch.

Both gates **fail closed**: a protected-path hit or a red suite fails the
instance so the branch is never merged and the failure surfaces in `rk inbox`.
Auto-merge is only ever reached through a clean policy gate, a green suite, *and*
an explicit `APPROVE`.

**Re-entrancy — match scoping.** The steward is the worked example of the fourth
re-entrancy technique: its trigger's `match.search` is `"role":"rat"`, so it
fires only on plain-rat completions. The reviewer it spawns completes as a
`"reviewer"` (a field now carried on every `harness_result`), whose payload the
search does not contain — so the steward never re-triggers itself on the branch
it just reviewed. A reworked ticket, once drained, completes as a `"rat"` and
re-enters the steward: a closed loop, not a runaway.

> Installing the steward makes **all** rat completions auto-merge on a clean
> verdict. Do not also run an approval-gated workflow (`land-on-approve`) over
> the same completions, or the two race for the branch.

## Built-in reaction: quorum promotion

Beyond firing `#Trigger` workflows, every cycle the reactor runs one built-in
reaction: promoting fleet **suggestions** into **conventions** at quorum. This is
the flagship stigmergy loop — proposals become shared norms with no operator in
the path.

- `rk suggest '<text>'` writes a `Suggestion` (system scope, authored by
  `RK_AGENT`) and prints a `sug-…` id. It is **Ephemeral** with a voting-window
  TTL: a proposal that never reaches quorum simply decays.
- `rk endorse <sug-id>` writes an `Endorsement` keyed by
  `(identity = suggestion, instance = RK_AGENT)`. Re-endorsing is idempotent —
  the CLI skips a duplicate, and the reactor counts **distinct** endorsers
  regardless, so a double vote can never inflate the tally. Endorsements are
  Ephemeral too and decay with the voting window.
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
   and Ephemeral — a proposal that never gathers support simply decays.
2. **Promote at quorum** (reactor). The built-in `promote_conventions` above
   mints a Furniture `Convention` once `[reactor] quorum` distinct endorsers back
   one suggestion.
3. **Inject at spawn** (supervisor, TKT-18). At spawn the supervisor scans active
   conventions for the rat's repo scope + `system` and composes their text into
   the rendered prompt as a binding **"Standing conventions"** section — so a
   promoted norm changes what the next rat *does*, not just what it *could read*.

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
built-in adds a desktop notification (via `HerdrMux::notify`, herdr's
`notification show`) the moment such a `need` lands, so a human is pinged when a
branch needs a merge decision instead of only on their next inbox check.

- The discriminator is `category = need` **and** `identity = "steward"`. A rat's
  own `rk need` keys on its agent name, so an ordinary help request is left on
  the inbox queue and never pops a notification.
- It fires **at most once per need tuple**, guarded by the same durable
  idempotency marker (`Event/reactor_fired`) the trigger path uses — an
  at-least-once re-scan never double-pops. A reinforced escalation keeps its id
  below the cursor and is never re-seen, so a repeat push only happens after the
  old `need` evaporates and a fresh one is written (the intended de-spam).
- It **degrades to a no-op** when no herdr server is reachable, so a headless
  castle is unaffected. Set `notify_escalations = false` to keep escalations
  purely on the passive `rk inbox` queue.

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
notify_escalations = true # desktop-push a steward escalation via herdr; false = inbox-only
```

## Where it lives

- Schema: `crates/rk-workflow/src/triggers-schema.cue`; loader
  `rk_workflow::load_triggers`.
- Reactor: `crates/rk-daemon/src/reactor.rs` (`Reactor::run_cycle`, plus the
  built-ins `Reactor::promote_conventions`, `Reactor::coalesce_obstacles`, and
  `Reactor::notify_escalation`),
  spawned as a loop next to the GC and sync loops in
  `crates/rk-daemon/src/server.rs`.
- Suggest/endorse sugar: `crates/rk-cli/src/space_cmds.rs` (`suggest`, `endorse`).
- Config: `rk_core::config::ReactorConfig`.
- Tests: `crates/rk-daemon/tests/reactor.rs` (live-daemon fire, idempotency under
  feed loss + cursor reset, re-entrancy/exclusion, rate cap, quorum promotion,
  and obstacle coalescence — quorum, per-scope/topic separation, idempotent
  re-filing; steward escalation notify — fires once, steward-only, disable
  switch) plus unit tests in the reactor and workflow modules.
- Composed convention-quorum loop: self-test
  `crates/rk-daemon/tests/convention_quorum.rs` (suggestion → quorum →
  injectable convention, over the wire), runnable demo
  `scripts/convention-quorum-demo.sh`.
- Example: `examples/triggers.cue`.

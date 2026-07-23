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
daemon restart.

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

## First-boot backlog

On a fresh daemon (no cursor file yet) the reactor baselines its cursor to the
newest existing tuple, so it does **not** react to the entire pre-existing
backlog at startup. Only tuples that arrive after boot are dispatched. A restart
resumes from the persisted cursor.

## Configuration

```toml
[reactor]
enabled = true            # master switch; false = the reactor loop never starts
interval_secs = 30        # fallback scan cadence (the feed also wakes a cycle)
window_secs = 60          # rolling window for the per-trigger rate cap
max_fires = 20            # default per-trigger cap; a #Trigger may lower it
marker_ttl_secs = 604800  # idempotency-marker lifetime (one week)
exclude_instances = []    # authors never reacted to, besides "reactor"
```

## Where it lives

- Schema: `crates/rk-workflow/src/triggers-schema.cue`; loader
  `rk_workflow::load_triggers`.
- Reactor: `crates/rk-daemon/src/reactor.rs` (`Reactor::run_cycle`), spawned as a
  loop next to the GC and sync loops in `crates/rk-daemon/src/server.rs`.
- Config: `rk_core::config::ReactorConfig`.
- Tests: `crates/rk-daemon/tests/reactor.rs` (live-daemon fire, idempotency under
  feed loss + cursor reset, re-entrancy/exclusion, rate cap) plus unit tests in
  the reactor and workflow modules.
- Example: `examples/triggers.cue`.

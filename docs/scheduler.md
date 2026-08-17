# The scheduler: cron-driven workflows

The scheduler is the **TIME axis** of autonomy. Where the [reactor](reactor.md)
fires a workflow when a matching *tuple* lands, the scheduler fires one when a
*clock* strikes — groom, drain, and prompt-refine on a cadence with zero
operator initiation. A scheduled fire is a time-sourced trigger: it resolves a
target repo and calls `engine.run`, the very same dispatch path the reactor
uses.

## Defining a schedule

A schedule file is a CUE package with a top-level `schedules:` list, validated
against `crates/rk-workflow/src/schedules-schema.cue` exactly as workflows and
triggers are. Put them in either place (both are loaded every cycle):

- **Global:** `~/.rat-kingdom/schedules/*.cue`
- **Repo-local:** `<repo>/.rk/schedules.cue`

```cue
schedules: [
    {
        name: "nightly-drain"   // lowercase-hyphen; also the single-flight key
        cron: "0 3 * * *"       // 03:00 UTC daily
        run:  "backlog-drain"   // a workflow definition name
        repo: "rat-kingdom"     // registered repo to run in
    },
    {
        name: "hourly-groom"
        cron: "@hourly"
        run:  "backlog-groom"
        repo: "rat-kingdom"
    },
]
```

`repo` defaults to the repo a repo-local file was discovered in, so a
`<repo>/.rk/schedules.cue` entry can omit it. A **global** schedule MUST set
`repo` — unlike a trigger there is no matched tuple whose scope could stand in,
so a global schedule with no repo is logged and skipped. Static `params` (all
string values) are passed to the workflow verbatim.

## Cron syntax

Standard 5-field cron — `minute hour day-of-month month day-of-week` — evaluated
in **UTC** at minute granularity. Per field:

| form | meaning |
|------|---------|
| `*` | any value |
| `a,b,c` | a list |
| `a-b` | a range |
| `*/n`, `a-b/n` | a step |

Day-of-week is `0..=6` with `0` = Sunday (`7` also accepted as Sunday). Names
(`MON`, `JAN`) are intentionally not supported — numeric only.

The **Vixie day rule** is preserved: when *both* day-of-month and day-of-week
are restricted (neither is a bare `*`), a day matches if *either* field matches
(a logical OR). A field counts as restricted iff its literal text is not exactly
`*`.

Macros: `@yearly`/`@annually`, `@monthly`, `@weekly`, `@daily`/`@midnight`,
`@hourly`.

## Cursor, catch-up, and single-flight

- **Durable minute-cursor.** The scheduler records the last UTC minute it
  evaluated in `~/.rat-kingdom/scheduler-cursor`. Each cycle it evaluates every
  minute in `(cursor, now]` — normally just the one new minute — firing each
  schedule at most once per cycle.
- **First boot** baselines the cursor to the current minute, so no backlog
  fires when the daemon starts.
- **Catch-up after downtime** is bounded by `catchup_minutes` (default one day):
  a daemon down overnight runs each missed daily/hourly schedule *once* on the
  next boot, not a replay of every minute in the gap.
- **Single-flight.** Each schedule is guarded by a lock keyed on its `name`: if
  its previous run's workflow instance is still `Running`, the next fire is
  skipped. Scheduled workflow snapshots persist that schedule name, so after a
  restart the scheduler can rebuild the exact per-schedule guard from rehydrated
  `Running` instances. Legacy snapshots without a schedule name use exact
  repository, invoked workflow definition, and parameter identity as a
  conservative fallback. Nested child instances are excluded from that fallback.
  A slow nightly drain therefore never stacks a second copy on itself, even
  across a daemon restart.
- **Stale-running bypass.** A `Running` instance older than
  `stale_running_hours` (default 6h — above rat p99 runtime, well below the
  typical 24h nightly cadence) no longer counts as a single-flight block, so a
  wedged instance can't make its schedule skip forever. The bypass is
  escalated via a `need` tuple rather than silently ignored, and is idempotent
  per instance: the same wedged instance emits exactly one escalation `need`,
  no matter how many matching minutes or code paths see it before it's
  cleared (e.g. by a replacement dispatch finally succeeding).

Overnight cost is otherwise bounded by the fleet/repo budget caps
(`rk_ledger::FleetBudget`), which refuse new dispatch once a cap is hit — the
same pre-dispatch guard every spawn passes through.

## The nightly self-improvement chain

The headline use of the scheduler is `nightly-self-improve`
(`examples/workflows/nightly-self-improve.cue`): one workflow that runs the three
self-improvement loops back to back — **groom** the backlog, **drain** it in
parallel, then **refine** prompts/conventions from the night's pain. Welding all
three into a single workflow (rather than three separate schedules) means the
whole night is one instance behind **one** single-flight lock, so a slow drain
can never let the next night's groom stack on top of it.

```cue
schedules: [{
    name: "nightly-self-improve"   // the single-flight key for the whole chain
    cron: "0 3 * * *"
    run:  "nightly-self-improve"
    repo: "rat-kingdom"
    params: {limit: "5", timeout: "45m"}
}]
```

Phase semantics are deliberate: the groom phase has no evaluate gate (a grooming
hiccup must not abort the night), the drain phase gates its batch merge on every
rat finishing cleanly (a broken batch parks rather than auto-merges, which also
skips that night's refine — the failed instance then surfaces in `rk inbox`), and
the refine phase only ever *proposes* edits. If you'd rather each phase be
independently retryable, schedule `backlog-groom` / `backlog-drain` /
`prompt-refine` as separate entries instead — `examples/schedules.cue` shows both
shapes.

## Configuration

```toml
[scheduler]
enabled = true             # master switch; false = the scheduler loop never starts
interval_secs = 30         # how often to check for a new cron minute; clamped [1,60]
catchup_minutes = 1440     # bound on look-back after downtime; 0 = current minute only
stale_running_hours = 6    # age past which a wedged Running instance stops blocking its schedule
```

`interval_secs` is clamped to `[1, 60]`: the loop must tick at least once a
minute or a matching minute would be skipped.

`stale_running_hours` bounds how long a single-flight `Running` instance can
block its own schedule before the scheduler bypasses it (see "Stale-running
bypass" above). Set it higher than your slowest legitimate run and comfortably
below the schedule's own cadence.

## Where it lives

- Schema: `crates/rk-workflow/src/schedules-schema.cue`; loader
  `rk_workflow::load_schedules`.
- Cron evaluator: `crates/rk-daemon/src/cron.rs` (`Cron::parse` / `Cron::matches`).
- Scheduler: `crates/rk-daemon/src/scheduler.rs` (`Scheduler::run_cycle`),
  spawned as a loop next to the GC, sync, and reactor loops in
  `crates/rk-daemon/src/server.rs`.
- Config: `rk_core::config::SchedulerConfig`; paths `Layout::schedules_dir`.
- Tests: `crates/rk-daemon/tests/scheduler.rs` (matching-minute fire,
  catch-up-once, single-flight, unresolvable-repo/bad-cron skip, repo-local
  default) plus `cron` and `load_schedules` unit tests.
- Example: `examples/schedules.cue`.

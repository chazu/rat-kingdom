# rat-kingdom

A multi-agent orchestration harness for AI coding agents, in Rust. Rats
(agents) work in isolated git worktrees, coordinate stigmergically through a
shared tuplespace, and are driven over their harnesses' structured protocols —
no terminal scraping, no keystroke injection, no sleeps.

## Requirements

| What | Why | Required? |
|---|---|---|
| Rust stable, git | build + agent isolation | yes |
| [`cue`](https://cuelang.org) CLI | workflow definitions | for workflows |
| `claude` (Claude Code) | default harness | at least one harness |
| `codex` (Codex CLI) | second harness | optional |
| `axe` | budget-capped one-shot harness | optional |
| [`herdr`](https://herdr.dev) | attachable interactive rats | optional |

## Install

```bash
cargo build --release
export PATH="$PWD/target/release:$PATH"   # or copy target/release/rk onto PATH
rk ping                                    # auto-starts the daemon → "pong"
```

Everything lives under `~/.rat-kingdom/` (override with `RK_HOME`): config,
tuplespace db, worktrees, logs, workflow definitions, sync state.

## Five-minute tour

```bash
cd ~/some-git-repo

# Spawn a rat on a task (isolated worktree, branch rat/<name>/<task>)
rk spawn --task fix-readme --prompt "Fix the typos in README.md, commit, then run: rk done"

rk list                      # fleet at a glance (state, tokens, cost)
rk status Whisker            # one rat in detail
rk log Whisker               # its transcript (prose, tool calls, retries); -f to follow
rk watch                     # live tuple stream — the system's inner monologue
rk steer Whisker "also check CONTRIBUTING.md"   # mid-session guidance
rk dismiss Whisker           # stop + merge its branch + clean up
rk cost                      # per-agent token/cost rollup
rk cost --fleet              # fleet/repo spend vs configured budget caps
```

Spawn options: `--harness claude|codex|axe|fake`, `--model`, `--role
rat|reviewer`, `--base <branch>`, `--parent <agent>` (completion routing),
`--no-merge` on dismiss, `--attach` (below).

### How a rat signals

Rats are primed with a composed role prompt and use sugar commands
(auto-filled from their spawn environment):

```bash
rk done "one-line summary"     # completion — mandatory final step
rk obstacle "what's blocking"  # blocked but continuing/winding down
rk need "what would help"      # ask the room
rk out artifact $RK_REPO name --payload '{"...": "..."}'   # work products
rk out artifact $RK_REPO fix --resolves <obstacle-id>      # backlink a solved wall
```

`--resolves <obstacle/need-id>` retires that wall and lays a decaying
`topic -> artifact` trail, so the next rat hitting the same wall is steered to the
prior fix (`rk scan resolution $RK_REPO`) instead of redoing it. See
[docs/reactor.md](docs/reactor.md#built-in-reaction-resolution-backlinks).

The daemon independently records `harness_result` events from the harness
protocol, so even a rat that forgets `rk done` is tracked.

## Priming a session

`rk prime` prints the instructions for driving the system — point any LLM
session (or your own `CLAUDE.md`) at it to learn the commands:

```bash
rk prime                 # operator: how to run the fleet (the default)
rk prime --role rat      # what a worker rat is told
rk prime --role reviewer # what a reviewer is told
```

With no `--role`, it renders the `operator` role — a session driving the fleet
from the outside (repos, tickets, spawn, watch, steer, dismiss). Spawned rats
carry `RK_ROLE`, so `rk prime` inside a rat automatically renders that rat's
own role instead.

## The tuplespace

Coordination substrate and audit log in one. Tuples are
`(category, scope, identity, instance)` + JSON payload; categories carry
epistemic weight (`fact` > `convention` > `artifact` > `claim` >
`obstacle`/`need` > `event`).

```bash
rk out fact myrepo rate-limit --payload '{"discovered": "API caps at 100/s"}'
rk scan fact myrepo            # non-blocking read (oldest first)
rk scan obstacle myrepo --hot  # ranked: strongest trail first (weight × recency × strength)
rk scan '' myrepo --top 5      # the 5 hottest tuples in a scope (implies --hot)
rk rd event '' task_done --search Whisker --timeout 5m   # blocking read
rk in available myrepo --timeout 30s                     # destructive take
```

Lifecycle classes: `furniture` (daemon-owned, unconsumable), `session`
(default), `ephemeral` (`--ttl 5m`, GC'd).

## Repos

So the system knows where your repositories live, register them by name. The
registry is machine-local (paths differ per castle) and is what lets you refer
to a repo by name instead of a path elsewhere.

```bash
rk repo add ~/dev/rat-kingdom          # name defaults to the directory ("rat-kingdom")
rk repo add ~/dev/other --name svc     # or name it explicitly
rk repo list                           # NAME → PATH
rk repo show rat-kingdom               # details + its open tickets
```

A registered name works anywhere a repo is expected, e.g. `rk spawn --repo rat-kingdom`.

By default a finished rat's branch is merged directly into its base. A repo can
instead be put in **PR mode** — `rk repo add <path> --merge-mode pr` — so the
daemon pushes the branch and opens a pull/merge request for human/CI review
instead of merging. See [docs/pr-merge-mode.md](docs/pr-merge-mode.md) for the
credential prerequisites and the GitHub/GitLab flows.

## Tickets

Durable work items — a backlog you and the rats can create, read, and
decompose. A ticket is a `task` tuple (`TKT-<n>`) that persists until closed,
and — because it carries a repo *name*, not a path — it replicates across
castles as a shared backlog through git-notes sync.

```bash
rk ticket new "Fix the login redirect loop" --repo svc --priority high
rk ticket new "Add SSO" --body "SAML + OIDC" --parent TKT-1   # decompose into sub-tickets
rk ticket list --repo svc --status open
rk ticket show TKT-1                                          # details + sub-tickets
rk ticket update TKT-3 --status in_progress --assignee Whisker
```

Statuses: `open → claimed → in_progress → blocked → done → closed`. Rats are
primed to *file* or *decompose* tickets for follow-up work rather than starting
it themselves — the orchestrator routes them.

**Dependencies.** A ticket can be blocked by others (distinct from parent/child
decomposition — this is a DAG of "must finish first" edges). Cycles are
rejected.

```bash
rk ticket new "Build API" --depends-on TKT-1            # blocked-by at creation
rk ticket dep TKT-3 TKT-2                               # TKT-3 is blocked by TKT-2
rk ticket undep TKT-3 TKT-2                             # drop the edge
rk ticket ready --repo svc                              # open tickets with all deps satisfied
```

`rk ticket list` marks blocked tickets with 🔒; `rk ticket show` annotates each
dependency as satisfied (✓) or `blocking`. `rk spawn --ticket TKT-2` refuses a
blocked ticket unless you pass `--force`, so `rk ticket ready` is the list of
what you can actually dispatch right now.

Dispatch a ticket straight to a rat — it fills the task and prompt from the
ticket, resolves the repo from the ticket's scope, and flips the ticket to
`in_progress`:

```bash
rk spawn --ticket TKT-3            # no hand-written --task/--prompt/--repo needed
```

The ticket's lifecycle then closes itself: when the rat finishes (its `rk done`,
or the harness's own completion for a rat that forgets), the ticket moves to
`done` — which **automatically unblocks any dependents** — and merging it on
`rk dismiss` moves it to `closed`. A rat that errors leaves its ticket
`in_progress` for inspection.

## Configuration (`~/.rat-kingdom/config.toml`)

```toml
castle_name = "my-laptop"        # this machine's identity (default: hostname)

[harness]
default = "claude"               # harness when nothing else specifies

[agents.default]                 # global default agent profile
harness = "claude"
model = "sonnet"

[agents.cheap]                   # named profiles, referenced by spawns/workflows
harness = "codex"
model = "gpt-5.5-codex"

[[tiers.rules]]                  # cost-tier routing: ticket labels/priority ->
label = "mechanical"             # a tier (an [agents.<tier>] profile name).
tier = "cheap"                   # First matching rule wins; a fan-out spawn
[[tiers.rules]]                  # resolves its tier just below inline overrides.
priority = "high"                # A workflow's own `tiers:` field shadows these.
tier = "default"                 # A rule with no label/priority is the fallback.

[budget]                         # 0 = unlimited on any cap
max_usd = 5.0                    # per-agent: warn→steer→kill mid-run
max_tokens = 0
warn_at = 0.8                    # warn (obstacle tuple + steer) at 80%, kill at cap
fleet_max_usd = 0.0              # fleet-wide wallet kill-switch: once the SUM of
                                 # all agents' cost hits this, new spawns are
                                 # REFUSED (dispatch stops) — safe for autoscaler/
                                 # nightly runs. See `rk cost --fleet`.
repo_max_usd = 0.0               # same guard, scoped per-repo
                                 # (a workflow's own `budget: {max_usd}` field
                                 # caps one instance's spend, layered below these)

[supervisor]                     # liveness/burn sweep (budget only sees Usage
                                 # events; this catches rats hung emitting nothing)
enabled = true
interval_secs = 60
stuck_after_secs = 900           # silence past this => STUCK (0 = off)
burn_usd_per_min = 0.0           # sustained USD/min => RUNNING AWAY (0 = off)
kill_grace_secs = 600            # obstacle+steer first, kill only if still flagged

[drain]                          # continuous-drain: WIP-limited fleet autoscaler
enabled = false                  # off by default — turning it on hands the
                                 # dispatch loop to the daemon (opt-in)
max_wip = 0                      # target concurrency W: keep up to this many rats
                                 # live, spawning the highest-priority ready ticket
                                 # whenever a slot frees (0 also disables the loop)
interval_secs = 30               # fallback refill cadence; a freed slot also wakes
                                 # a refill via the tuple feed
# repo = "myrepo"                # restrict to one repo scope (unset = every
                                 # registered repo; system-scope tickets never run;
                                 # ignored when [drain.repos] below is set)
aging_secs = 3600               # seconds of waiting that buy one priority level,
                                 # so low-priority tickets can't starve (0 = strict)

# Cross-repo WIP partitioning: subdivide the fleet-wide max_wip per repo so one
# busy repo cannot monopolize the fleet. When any [drain.repos.*] table exists it
# becomes an ALLOWLIST — only listed, enabled repos drain (repo pin above is
# ignored) — and each cap subdivides max_wip. Below, max_wip=4 fleet-wide with
# two repos capped at 2 each means neither starves however deep its backlog.
# [drain.repos.frontend]
# enabled = true                 # per-repo switch (default true; false pauses it)
# max_wip = 2                    # per-repo cap (0 = unlimited within max_wip)
# [drain.repos.backend]
# max_wip = 2

[sync]                           # multiplayer (git-notes replication)
enabled = false
remote_url = "git@github.com:you/rk-sync-state.git"
interval_secs = 30

[policy]                         # workflow-execution policy
require_named_checks = false     # true => a workflow `run` step may ONLY invoke
                                 # a repo-registered named check (see below); a
                                 # raw inline `command` is refused fail-closed, so
                                 # a compromised/untrusted workflow def cannot run
                                 # arbitrary shell in a rat's worktree.
default_merge_mode = "direct"    # fleet-wide fallback for repos registered
                                 # without --merge-mode: "direct" merges the
                                 # branch, "pr" pushes it and opens a pull/merge
                                 # request for review (see docs/pr-merge-mode.md).
                                 # A repo's own --merge-mode always overrides this.
```

Env: `RK_HOME` (state dir), `RK_LOG` (tracing filter), `RK_CONFIG_*`
(config overrides, e.g. `RK_CONFIG_BUDGET_MAX_USD=2`). Plain `RK_*` names
(`RK_AGENT`, `RK_TASK`, ...) are reserved for agent identity — set at spawn,
never read as config.

### `[drain]` — continuous-drain autoscaler

Turning on `[drain]` hands the dispatch loop to the daemon: it keeps up to
`max_wip` rats live, spawning the highest-priority ready ticket whenever a slot
frees. Keys:

- `enabled` — opt in (default `false`). While off, the daemon never
  auto-spawns; you dispatch by hand.
- `max_wip` — target concurrency (the WIP limit). **`0` is inert even when
  `enabled = true`** — the loop needs a non-zero cap to spawn anything.
- `interval_secs` — fallback refill cadence; a freed slot also wakes a refill
  immediately via the tuple feed, so this is just a backstop.
- `repo` — restrict draining to one repo scope. Unset drains every registered
  repo; system-scope tickets never drain. Ignored when `[drain.repos]` is set.
- `repos` — per-repo partition map (`[drain.repos.<name>]` with `enabled` and
  `max_wip`). When any entry exists it becomes an **allowlist** (the single
  `repo` pin is ignored) and each cap subdivides the fleet-wide `max_wip`, so
  one busy repo cannot monopolize the fleet.
- `aging_secs` — seconds of waiting that buy one priority level, so
  low-priority tickets can't starve behind a deep high-priority backlog
  (`0` = strict priority).

**Pair continuous-drain with guardrails for unattended running.** Because the
loop spawns rats with no operator in the seat, set `[budget].fleet_max_usd` (a
fleet-wide wallet kill-switch that refuses new spawns once total spend hits the
cap) and `[supervisor].burn_usd_per_min` (flags a runaway rat by sustained
spend) so a stuck or looping fleet stops itself rather than draining the wallet
overnight.

## Workflows

CUE-defined, validated by unification against the schema in
`crates/rk-workflow/src/schema.cue`. Definitions go in
`~/.rat-kingdom/workflows/` (global) or `<repo>/.rk/workflows/` (repo-local,
wins). Two shipped examples in `examples/workflows/`:

- **solo-task** — spawn → wait → verify success → auto-merge.
- **code-review** — rat implements on the strong model, a cheaper reviewer
  examines the branch and records an `artifact` verdict; a human merges.

```bash
cp examples/workflows/*.cue ~/.rat-kingdom/workflows/
rk workflow defs
rk workflow run solo-task --param taskId=fix-login \
  --param description="Fix the login redirect loop"
rk workflow list
rk workflow status wf-abc123
```

Anatomy:

```cue
workflow: {
    name: "my-flow"
    params: { taskId: {type: "string", required: true} }
    agents: {
        default: {harness: "claude", model: "sonnet"}   // workflow-wide defaults
        cheap:   {harness: "codex"}
    }
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId}},
        {type: "wait", timeout: "30m"},
        {type: "evaluate", expect: {is_error: false}},   // full CUE unification
        {type: "spawn", role: "reviewer", agent: "cheap", model: "o4-mini",
         task: {title: "review", description: "Branch: {{ctx.activeBranch}}"}},
        {type: "wait"},
        {type: "dismiss"},                                // merges active agent
    ]
    aspects: [   // cross-cutting: splice steps around matches at load time
        {match: {type: "spawn", role: "rat"},
         after: [{type: "gate", gateType: "timer", duration: "5s"}]},
    ]
}
```

- `_input.*` resolves through CUE at load time (defaults included);
  `{{ctx.activeAgent}}`, `{{ctx.activeBranch}}`, `{{ctx.previousResult}}`
  resolve at execution time.
- **Model/harness resolution**, most specific wins per field: inline step
  overrides → the tier a routing rule picked from the ticket's labels/priority
  (`[tiers]` / workflow `tiers:`) → step's named profile (workflow `agents`, then
  global `[agents.<name>]`) → workflow `agents.default` → global `[agents.default]`
  → `[harness] default`. Unknown profile/tier names are errors.
- Spawn steps inside a workflow base their worktrees on the previous agent's
  branch (`ctx.activeBranch`), which is how a reviewer sees the rat's work.
- **Per-instance budget**: add `budget: {max_usd: 2.5}` to a workflow to cap the
  summed cost of every agent one run spawns. Enforced as a dispatch preflight
  (same machinery as the global fleet/repo caps): once this run's spend reaches
  the cap, its next spawn — single or fan-out — is refused and the instance
  fails, surfacing a `budget_instance_exceeded` obstacle in `rk inbox`. Layered
  below `fleet_max_usd`/`repo_max_usd`; per-run spend shows in `rk cost --fleet`.
- **Named checks (run-step allowlist)**: a `run` step runs a command in the rat's
  worktree to gate the merge on a verdict the runner cannot forge — but a raw
  `command` is only as trusted as the workflow def that carries it. Register the
  checks a repo trusts in `<repo>/.rk/checks.cue` (see `examples/checks.cue`) and
  reference one by name: `{type: "run", check: "test"}`. Set `[policy]
  require_named_checks = true` to refuse raw `command` run steps fail-closed, so a
  compromised or untrusted workflow definition can invoke only the repo owner's
  registered checks — never arbitrary shell. A named check supplies its own
  command/cwd/`expectExit`/timeout; the step may override cwd/`expectExit`/timeout.
  See `examples/workflows/named-check-merge.cue`.

## Reactor (triggers)

The daemon runs a background **tuple-reactor**: registered `#Trigger` reactions
that fire a workflow whenever a matching tuple lands in the space — zero-token,
zero-model dispatch. This is the keystone the stigmergy proposals (quorum
promotion, obstacle coalescence, convention injection) build on. Triggers go in
`~/.rat-kingdom/triggers/*.cue` (global) or `<repo>/.rk/triggers.cue`
(repo-local), validated against `crates/rk-workflow/src/triggers-schema.cue`.

```cue
triggers: [
    {
        name:  "drain-on-new-ticket"
        match: {category: "event", identity: "ticket_created", scope: "myrepo"}
        run:   "backlog-drain"                 // a workflow definition name
        params: {taskId: "{{tuple.payload.ticket}}"}
        exclude: ["daemon"]                     // never react to these authors
        maxFires: 10                            // per-window storm cap (<=100)
    },
]
```

- **Never misses events.** The live feed is only a wake signal; dispatch is
  driven by a durable cursor scan (`~/.rat-kingdom/reactor-cursor`), so a dropped
  feed event is still picked up by the next scan.
- **Idempotent.** Each fired `(trigger, tuple)` writes a durable marker, so an
  at-least-once redelivery (crash, cursor loss) never double-fires.
- **No storms.** The reactor tags its own output (`reactor` instance, never
  reacted to), honours `exclude`/`[reactor].exclude_instances`, and caps each
  trigger at `maxFires` per `[reactor].window_secs`.
- **Params** template from the matched tuple: `{{tuple.category|scope|identity|
  instance|id}}` and `{{tuple.payload.<field>}}` (a lone payload placeholder
  passes the raw JSON value through; otherwise it is string-interpolated).

Configure in `config.toml`:

```toml
[reactor]
enabled = true
interval_secs = 30       # fallback scan cadence (feed also wakes it)
window_secs = 60         # rolling rate-cap window
max_fires = 20           # default per-trigger cap; a #Trigger may lower it
marker_ttl_secs = 604800  # idempotency marker lifetime
exclude_instances = []    # authors never reacted to (besides "reactor")
notify_escalations = true # desktop-push a steward escalation via herdr; false = inbox-only
```

- **Active escalation push.** When the steward escalates a `STOP`/unknown
  verdict as a `need` (identity `steward`), a built-in reaction fires a desktop
  notification via herdr so the operator is pushed at, not only queued in
  `rk inbox`. A no-op when no herdr server is running.

See `docs/reactor.md` for the full design (why scan-is-truth, the three
re-entrancy guards, first-boot backlog skipping).

## Scheduler (cron)

The daemon also runs a background **scheduler** — the TIME axis of the reactor.
Where a trigger fires on a matching tuple, a schedule fires on a clock: groom,
drain, and prompt-refine on a cadence with zero operator initiation. Schedules
go in `~/.rat-kingdom/schedules/*.cue` (global) or `<repo>/.rk/schedules.cue`
(repo-local), validated against `crates/rk-workflow/src/schedules-schema.cue`.

```cue
schedules: [
    {
        name: "nightly-drain"   // also the single-flight key
        cron: "0 3 * * *"       // 5-field cron or @hourly/@daily/@weekly/... (UTC)
        run:  "backlog-drain"   // a workflow definition name
        repo: "myrepo"          // required for a global schedule
    },
]
```

- **A scheduled fire is a time-sourced trigger** — it reuses the reactor's
  `engine.run` dispatch path, just clock-sourced instead of tuple-sourced.
- **Never double-fires, catches up once.** A durable minute-cursor
  (`~/.rat-kingdom/scheduler-cursor`) baselines to now on first boot (no backlog
  storm) and, after downtime, fires each missed schedule at most once (bounded by
  `[scheduler].catchup_minutes`).
- **Single-flight.** Each schedule is guarded by its `name`: while its previous
  run is still `Running`, the next fire is skipped — a slow drain never stacks.
- **The `nightly-self-improve` chain** is the headline schedule: one workflow
  that grooms the backlog, drains it in parallel, then proposes prompt/convention
  refinements — the three self-improvement loops welded into a single instance so
  the whole night runs behind one single-flight lock. See `docs/scheduler.md` and
  `examples/workflows/nightly-self-improve.cue`.

```toml
[scheduler]
enabled = true
interval_secs = 30      # cron-minute check cadence; clamped [1,60]
catchup_minutes = 1440  # look-back bound after downtime; 0 = current minute only
```

See `docs/scheduler.md` for the full design (cursor/catch-up semantics, the
Vixie day-of-month/day-of-week rule, single-flight).

## Attach mode (herdr)

With a running [herdr](https://herdr.dev) server, rats can run interactively
in panes you can watch and take over:

```bash
herdr integration install claude   # once — lets herdr report TUI readiness
rk spawn --task tricky-1 --attach --prompt "..."
rk attach Whisker                  # drop into the live session
rk steer Whisker "try the other approach"   # works from outside too
```

The daemon still owns the worktree/branch/registry; completion is the rat's
own `rk done`, and dismissal closes the pane and merges as usual. Without
herdr, everything runs headless — but you are not blind to a headless rat:
`rk log <name>` replays its transcript (assistant prose, tool calls, retries)
and `rk log <name> --follow` streams it live, persisted as a bounded per-agent
ring under `~/.rat-kingdom/agent-logs/` (local only; never synced).

## Multiplayer (git-notes sync)

Multiple machines (castles) share one tuplespace through a git remote. Each
castle writes only its own `refs/notes/rk/<castle>` ref — pushes never
conflict — and readers union-merge all refs locally. Concurrent claims on one
task resolve to the same earliest winner on every machine.

```bash
# in config.toml: [sync] enabled = true, remote_url = <shared repo>
rk sync now     # cycle immediately (also runs on the interval)
rk peers        # castles seen in the shared space
```

Facts, tasks, obstacles, and events replicate; ephemeral tuples never leave
the local daemon. A blocked `rk rd` wakes when a peer's tuple arrives.

## Development

```bash
cargo test --workspace     # ~75 tests; integration tests use a scripted fake harness
cargo clippy --workspace --all-targets
```

Crate map: `rk-core` (tuple model, config, priming), `rk-space` (tuplespace),
`rk-git` (worktrees/merges), `rk-harness` (claude/codex/axe/fake adapters),
`rk-ledger` (pricing/budgets), `rk-workflow` (CUE definitions), `rk-sync`
(git-notes replication), `rk-mux` (herdr), `rk-daemon` (supervisor, executor,
server), `rk-cli` (`rk`).

# rat-kingdom

A multi-agent orchestration harness for AI coding agents, in Rust. Rats
(agents) work in isolated git worktrees, coordinate stigmergically through a
shared tuplespace, and are driven over their harnesses' structured protocols —
no terminal scraping, no keystroke injection, no sleeps.

Successor to [imp](https://github.com/chazu/imp)'s ideas with its failure
modes fixed structurally; design rationale in
`docs/2026-07-22-imp-analysis-and-rat-kingdom-design.md`, build status in
`docs/2026-07-22-implementation-plan.md`.

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
rk watch                     # live tuple stream — the system's inner monologue
rk steer Whisker "also check CONTRIBUTING.md"   # mid-session guidance
rk dismiss Whisker           # stop + merge its branch + clean up
rk cost                      # per-agent and fleet token/cost rollup
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
```

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
rk scan fact myrepo            # non-blocking read
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

[budget]                         # per-agent caps; 0 = unlimited
max_usd = 5.0
max_tokens = 0
warn_at = 0.8                    # warn (obstacle tuple + steer) at 80%, kill at cap

[sync]                           # multiplayer (git-notes replication)
enabled = false
remote_url = "git@github.com:you/rk-sync-state.git"
interval_secs = 30
```

Env: `RK_HOME` (state dir), `RK_LOG` (tracing filter), `RK_CONFIG_*`
(config overrides, e.g. `RK_CONFIG_BUDGET_MAX_USD=2`). Plain `RK_*` names
(`RK_AGENT`, `RK_TASK`, ...) are reserved for agent identity — set at spawn,
never read as config.

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
  overrides → step's named profile (workflow `agents`, then global
  `[agents.<name>]`) → workflow `agents.default` → global `[agents.default]`
  → `[harness] default`. Unknown profile names are errors.
- Spawn steps inside a workflow base their worktrees on the previous agent's
  branch (`ctx.activeBranch`), which is how a reviewer sees the rat's work.

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
herdr, everything runs headless.

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

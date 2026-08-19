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
| [`jcode`](https://jcode.sh/docs) | multi-provider NDJSON harness | optional |
| [`herdr`](https://herdr.dev) | attachable interactive rats | optional |

## Install

```bash
cargo build --release
export PATH="$PWD/target/release:$PATH"   # or copy target/release/rk onto PATH
rk ping                                    # auto-starts the daemon → "pong"
```

Everything lives under `~/.rat-kingdom/` (override with `RK_HOME`): config,
tuplespace db, worktrees, logs, workflow definitions, sync state.

## Factory Foreman

Factory Foreman provides a Rust-native read-only dashboard plus daemon snapshots/events, structured-source-only self-optimization scorecards and advisory recommendations, five local stdio MCP tools, and digest-bound typed `workflow.run` proposals whose execution authority remains in the daemon. Open the operator dashboard with:

```bash
rk factory install-skill
rk factory dashboard --repo rat-kingdom
```

`install-skill` embeds this RK release's Factory Foreman package into `~/.jcode/skills/factory-foreman/`, making it discoverable by Jcode from any repository. It is idempotent and refuses to replace customized files unless `--force` is explicit. The skill directs Jcode to collect native typed snapshots, replay recent events, triage fleet and workflow failures, inspect scorecards, deduplicate tickets, recommend existing workflows, and prepare exact approval-gated proposals. The daemon, not the skill, retains execution authority.

The dashboard auto-starts the daemon and opens a live interactive Rust terminal UI with overview, agents, workflows, tickets, inbox, approvals, and events panels. Use `tab` or the arrow keys to switch panels, `j`/`k` to scroll, `r` to refresh, and `q` to quit. `--plain` emits one bounded Markdown snapshot for pipes, while `rk --json factory dashboard ...` emits the typed machine-readable envelope. The bundled Python helper remains only as a Jcode compatibility/legacy triage surface; it is not the primary human interface or execution authority. Native typed automation uses `rk --json factory ...` or `rk-mcp`; exact digest approval is daemon-enforced for that factory path, but legacy `workflow.run` is not globally gated. Scorecards and recommendations are read-only and advisory: they use structured sources, treat missing families as unobserved, keep static routing unchanged, and cannot mutate policy, config, workflows, tickets, approvals, or dispatch. See [docs/factory-foreman.md](docs/factory-foreman.md) for authority, CLI/MCP schemas, scorecard schema, recommendation thresholds, snapshot/replay/watch semantics, recovery, and limitations.

The **product-to-code lifecycle** (`rk product-to-code ...`) turns a product initiative into implemented, independently verified code through offline, contract-validated artifacts and two canonical typed factory actions: `ticket_graph.apply` (mint tickets from a validated graph) and `product_to_code.dispatch` (launch `implement-featureset` for unblocked minted tickets). Save the proposal JSON and inspect it. Then use `rk --json factory approve --proposal-file ...` and `rk --json factory execute-action --proposal-file ...`. The daemon alone applies mutations after authenticated operator approval with status, digest, and CAS checks. See [docs/product-to-code.md](docs/product-to-code.md).

## Five-minute tour

```bash
cd ~/some-git-repo

# Spawn a rat on a task (isolated worktree; names come from `.rk/repo.cue`)
rk spawn --task fix-readme --prompt "Fix the typos in README.md, commit, then run: rk done"

rk list                      # fleet at a glance (state, tokens, cost)
rk status Whisker            # one rat in detail
rk log Whisker               # its transcript (prose, tool calls, retries); -f to follow
rk watch                     # live tuple stream — the system's inner monologue
rk steer Whisker "also check CONTRIBUTING.md"   # mid-session guidance
rk dismiss Whisker           # stop + merge its branch + clean up
rk revert Whisker            # undo a bad auto-merge: revert the landed commit, reopen the ticket
rk cost                      # per-agent token/cost rollup (lifetime, archived included)
rk cost --fleet              # fleet/repo spend vs configured budget caps
rk prune --dry-run           # preview which dead records would be archived
rk prune                     # archive terminal records older than 7d out of the default views
```

### Keeping the fleet views readable

The agent registry never dropped a record, so after a busy session `rk list`
and `rk top` filled with dozens of dead rows. `rk prune` archives settled
terminal records — `completed`, `failed`, `dismissed` — into
`agents-archive.json` beside `agents.json`. Nothing is deleted: cost, usage,
and lineage (`parent`, `workflow_instance`) survive, so `rk cost` and the
budget rollups are unaffected. Archiving does **not** free the rat's name: a
name is an identity key stamped into durable tuples, logs and branches that
outlive the record, so it stays spent forever (the pool is unbounded anyway —
it grows `Whisker-2`, `Whisker-3`, … as needed).

The same pass clears the other half of the board: settled workflow instances
(`completed`, `failed`) move from `workflow-instances/` to
`workflow-instances-archive/` on the same window. A **running** instance is
never archived — including a targeted `rk workflow prune <id>`, which refuses
an in-flight or unknown id rather than silently doing nothing.

```bash
rk prune --before 24h        # duration (30m/24h/7d/2w) or a date (2026-07-24)
rk prune --all               # every eligible record + settled instance, regardless of age
rk prune --all --reap-git    # also reclaim worktrees + branches that already landed
rk prune --all --reap-logs   # also delete the archived rats' `rk log` transcripts
rk list --archived           # what's been archived
rk list --all                # live fleet + archive (archived rows marked with *)
rk top --all                 # same, in the dashboard
rk unarchive Whisker         # put one back
```

Instance-side, with the same shape:

```bash
rk workflow prune wf-abc123          # clear one failed run — the `rk inbox` row action
rk workflow prune --all --dry-run    # preview the sweep
rk workflow list --archived          # what's been pruned (still fully readable)
rk workflow status wf-abc123         # ...including its error and step trace
rk workflow unarchive wf-abc123      # put one back
```

A `spawning`/`running` rat is never archived, and neither is an `orphaned` one
— its worktree and branch are preserved so `rk respawn` can pick it back up.
`--reap-git` only touches a branch that has already merged into its target (or
is already gone); an unmerged branch is left standing and reported as skipped.

`--reap-logs` is the same idea for the other leftover. Each `rk log` transcript
is a bounded ring, but the *count* grows once per rat forever, so the flag
deletes the transcript of each record it archives — and only those, keyed on
the exact rat that wrote it. Unlike `--reap-git` it is one-way: `rk unarchive`
brings the record back but not the transcript. The two are separate switches
because they answer different questions — a branch may hold the only copy of a
rat's work, a transcript only narrates work that lives elsewhere — so combine
them (`--reap-git --reap-logs`) when you want everything reclaimed.

Spawn options: `--harness claude|codex|jcode|fake`, `--model`, `--role
rat|reviewer`, `--base <branch>`, `--parent <agent>` (completion routing),
`--permission-mode`, `--no-merge` on dismiss, `--attach` (below). `rk status`
shows the effective harness, model, and permission mode recorded for the
generation.

Ordinary workers are unattended, so Claude defaults to `bypassPermissions` and
Codex/jcode default to `danger-full-access`. The adapters make the Claude and
Codex modes explicit: Claude receives `--dangerously-skip-permissions`; Codex
receives `--dangerously-bypass-approvals-and-sandbox`. A command that waits for
human approval cannot complete in a headless worker, and Codex also needs access
to the Rat Kingdom socket under `RK_HOME`, outside the agent worktree. Codex
`read-only`/`workspace-write` worker overrides are therefore rejected before
launch. jcode also runs with full host access: its adapter consumes
`run --ndjson`, disables jcode-native swarm/auto-poke ownership, and rejects
permission modes it cannot enforce. Configure its provider with `jcode login`
before spawning. Onboarders are the exception: the daemon always forces Claude
`plan` or Codex/jcode `read-only`, regardless of worker defaults. Jcode v0.65+
onboarders are headless-only and receive an explicit `read,ls,agentgrep` tool
allow-list; Bash and every mutating tool remain unavailable. Their one-shot
native `done` event completes the assessment, so no shell is exposed solely to
run `rk done`.

```bash
jcode auth-test --all-configured
rk spawn --harness jcode --task fix-login --prompt "Fix the login bug, verify, commit, and run rk done"
rk repo onboard start . --harness jcode --model gpt-5.6-luna
```

### Jcode configuration and precedence

Use a global default when most workers should use jcode, and named profiles for
workflow-specific choices:

```toml
# ~/.rat-kingdom/config.toml
[agents.default]
harness = "jcode"
model = "gpt-5.6-luna"
permission_mode = "danger-full-access"

[agents.nightly]
harness = "jcode"
model = "gpt-5.6-luna"
permission_mode = "danger-full-access"
```

A workflow can select and refine the same named profile. Inline fields are the
last override:

```cue
workflow: {
    name: "jcode-example"
    agents: {
        nightly: {model: "gpt-5.6-luna"}
    }
    steps: [{
        type: "spawn"
        role: "rat"
        agent: "nightly"
        model: "gpt-5.6-luna" // optional inline override
        task: {title: "example"}
    }]
}
```

Resolution is field-by-field, from most to least specific: direct/inline spawn
override; routed tier; workflow named profile; global profile with the same
name; workflow default; global default; `[harness].default`. A direct
`rk spawn --harness jcode --model ... --permission-mode ...` therefore wins
over global defaults. `rk respawn` deliberately reuses the model and permission
mode recorded for that generation; editing config does not silently change a
failed worker. Inspect the effective values with `rk status <agent>` (or
`rk --json status <agent>`).

Ordinary jcode workers accept `danger-full-access`; `bypassPermissions` is a
compatibility spelling with the same full-access contract. Restricted ordinary
worker values are rejected because they cannot support the daemon coordination
socket. Onboarding ignores worker permissions and forces the adapter's
`read-only` tool allow-list. The repeatable invocation proof lives in
[`crates/rk-harness/src/jcode.rs`](crates/rk-harness/src/jcode.rs), while profile,
direct-spawn, respawn, and status coverage lives in the workflow, daemon, and
CLI test suites.

Workflow runs can opt into a stable coordinator ownership scope with
`rk workflow run <name> --coordinator <session-id>`. The coordinator can then
consume bounded attention and middle-rat rollups with:

```bash
rk monitor --coordinator <session-id> --once
rk monitor --coordinator <session-id> --follow
rk monitor --coordinator <session-id> --subtree <middle-rat> --once
```

`rk monitor --once` registers/reuses a durable cursor and acknowledges the
rendered block after it is accepted. `--json` exposes the cursor, snapshot,
attention records, and replay envelopes for a host session adapter. Rat
Kingdom cannot universally inject this block into the next turn because the
coordinator may be any external Codex, Claude Code, or other harness. Hosts
with turn-boundary hooks can wrap this command; otherwise the coordinator
should run it before meaningful decisions.

### How a rat signals

Rats are primed with a composed role prompt and use sugar commands
(auto-filled from their spawn environment):

```bash
rk done "one-line summary"     # completion — mandatory final step
rk obstacle "what's blocking"  # blocked but continuing/winding down
rk need "what would help"      # ask the room
rk progress --summary "4/7 child tickets complete" \
  --next "reviewing the remaining three"   # middle-rat milestone
rk out artifact $RK_REPO name --payload '{"...": "..."}'   # work products
rk out artifact $RK_REPO fix --resolves <obstacle-id>      # backlink a solved wall
```

`--resolves <obstacle/need-id>` retires that wall and lays a decaying
`topic -> artifact` trail, so the next rat hitting the same wall is steered to the
prior fix (`rk scan resolution $RK_REPO`) instead of redoing it. See
[docs/reactor.md](docs/reactor.md#built-in-reaction-resolution-backlinks).

`rk out` stamps `"agent": "$RK_AGENT"` into any object payload that does not
already name one, so a tuple written from a spawn session says who wrote it
without the rat having to remember. That stamp is what a workflow `read` with
`fromAgent: true` keys on to lift the verdict ITS reviewer wrote rather than a
concurrent instance's. An explicit `agent` in the payload is never overwritten.

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

Worker prompts stay project-agnostic by default. When a repository contains a
valid `.rk/checks.cue`, the daemon adds its named checks to the spawned and
resumed worker's prompt as **Repository verification checks**. The section
shows each check's name, command, working directory, expected exit, timeout,
environment policy, and declared toolchain; the command is repository-owned
guidance, not extra prompt instructions. A
worker should prefer the `verify` check when present, otherwise choose the
relevant declared check for its task. Workflow `run` steps remain the
authoritative, fail-closed gate. Missing or malformed check registries do not
prevent priming; the worker receives the generic instruction to report a
missing gate instead of inventing a project-specific command.

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
to a repo by name instead of a path elsewhere. If `.rk/repo.cue` exists,
registration validates it and activates its exact digest.

```bash
rk repo add ~/dev/rat-kingdom          # name defaults to the directory ("rat-kingdom")
rk repo add ~/dev/other --name svc     # or name it explicitly
rk repo list                           # NAME → PATH
rk repo show rat-kingdom               # details + its open tickets
rk repo onboard inspect ~/dev/other    # deterministic read-only readiness report
rk --json repo onboard inspect svc     # the same stable report shape as JSON
rk repo onboard start svc              # durable headless assessment session
rk repo onboard start svc --attach     # same session/report in a herdr pane
rk repo onboard status onb-...         # stable state after disconnect or restart
rk repo onboard propose onb-... --kind repo_file --title "Add verify" \
  --evidence "README documents mise run verify" --target .rk/checks.cue \
  --action write_repo_file --diff "$DIFF" --risk low --verification "check:verify" \
  --check-name verify --check-command "mise run verify" --check-cwd . \
  --check-expect-exit 0 --check-timeout 20m \
  --check-environment-policy strip_rk_spawn \
  --check-toolchain "mise rust@1.95.0"
rk repo onboard approve onb-... onb-prop-... --digest <sha256>
rk repo onboard apply onb-... onb-prop-... --digest <sha256>
rk repo onboard decline onb-... onb-prop-... --digest <sha256>
rk repo onboard propose onb-... --kind workflow_activation \
  --title "Add guarded maintenance workflow" --evidence "reviewed workflow design" \
  --target .rk/workflows/maintenance.cue --action activate_workflow \
  --diff "$DIFF" --risk high --verification "CUE workflow schema"
rk repo onboard approve onb-... onb-prop-... --digest <sha256>
rk repo onboard apply onb-... onb-prop-... --digest <sha256> # stage + validate only
rk repo onboard activate onb-... onb-prop-... --digest <sha256>
# or explicitly refuse the validated automation:
rk repo onboard decline-activation onb-... onb-prop-... --digest <sha256>
rk repo onboard report onb-...         # assessment plus terminal agent result
rk repo onboard resume onb-...         # recover an orphaned/failed headless run
rk repo onboard resume onb-... --attach
rk repo onboard cleanup onb-...        # remove terminal clean worktree; retain branch/report
```

A registered name works anywhere a repo is expected, e.g. `rk spawn --repo rat-kingdom`.

Version per-repository work and delivery behavior in `.rk/repo.cue`: branch
and worktree templates, dynamic or fixed target branch, local merge, merge and
push, branch push, or PR/MR delivery, remote mapping, and source-branch cleanup.
The checked-in file is inert until its exact digest is registered or activated,
and `rk repo show` reports drift. See [Repository work and delivery
policy](docs/repository-policy.md) for the schema and trust boundary.

For the operator walkthrough, see [Repository onboarding](docs/repo-onboarding.md).
Run `rk onboard` in the main operator agent to load that guided, gate-first
walkthrough context. It is exact sugar for `rk prime --role onboarding`: it
prints instructions only and does not launch an assessor, create a durable
session, or mutate repository state. The existing `rk repo onboard start`
command remains the legacy spawned-assessor path.

`repo onboard inspect` resolves either a path or registered name, then reports
canonical identity, git/remote/base state, repository instructions, documented
toolchain entrypoints, named checks, repo-local workflows/triggers/schedules,
and configured harness/`rk` readiness. Findings carry stable `kind`, `severity`,
observed `evidence`, inferred `recommendation`, and
`unresolved_ambiguity` fields. Error findings make the command exit non-zero:
dirty or unborn repositories, ambiguous bases, missing remotes/tools, malformed
CUE, submodules, and Git LFS therefore fail closed.

Inspection never registers the repository, launches an agent, runs a project
check, or edits repository/castle state. It also does not auto-start the daemon;
if the daemon is down, start it separately with `rk ping` and then inspect.

`repo onboard start` journals one stable session per canonical repository and
creates `onboarding/onb-...` in a Rat Kingdom-owned worktree. Repeating start
reuses that session, branch, and worktree rather than touching the human
checkout or launching a duplicate. Headless and `--attach` runs write the same
`onboarding-sessions.json` record and expose the same status/report RPCs.
Daemon restart marks an in-flight session orphaned; `resume` reuses the
preserved worktree and, for attached runs, reattaches to a surviving herdr pane
or recreates it.

The spawned role is always `onboarder`; the RPC does not accept a role override.
The daemon rejects unknown roles, forces onboarders into the harness's
read-only/plan mode, and gives them only inspection reads, self progress, and
proposal submission plus their final `rk done` event. Proposal submission only
journals immutable advice: it does not edit the worktree or castle. Onboarders
cannot approve or decline proposals, spawn agents, mutate tickets/repos,
approve workflows, use ordinary rat tuple writes, or gain operator authority by
clearing `RK_AGENT`/`RK_AUTH_TOKEN`.

Each proposal records its evidence, exact diff, risk, target/action, verification
plan, stable repository identity, and the onboarding Git tree revision. Its
canonical SHA-256 digest covers all of that immutable content. Copy the digest
shown by `status` or `report` into `approve`/`decline`; the daemon rejects a
stale tree, edited persisted proposal, different digest, caller-supplied actor,
or opposite second decision. Same-decision retries are idempotent, and the
server records the authenticated castle-qualified operator plus decision time.

A `.rk/checks.cue` proposal additionally binds the check name, exact command,
cwd, expected exit, timeout, environment policy, and toolchain description into
that digest. After approval, `repo onboard apply` applies the exact patch only
inside the Rat Kingdom-owned onboarding worktree, validates the resulting file
through the existing CUE checks schema, commits it on the onboarding branch,
and executes that named check. The durable report records the application
commit and tree plus every verification attempt's command, toolchain,
environment policy, exit/timing result, bounded output summary, and unresolved
risks. `strip_rk_spawn` removes `RK_AGENT`, `RK_TASK`, `RK_REPO`, `RK_ROLE`,
`RK_HOME`, `RK_BRANCH`, `RK_WORKTREE`, and `RK_AUTH_TOKEN`; `inherit` preserves
the daemon environment.

Application retries are fail-closed and recoverable. A clean replay neither
recommits nor reruns a verified check. After a failed check, retry reuses the
recorded commit and executes a new attempt. An interrupted exact patch or
trailer-bearing commit is recovered; unrelated dirt, an edited applied file,
or branch movement is recorded as failure and never swept into the proposal.

Repository policy, workflow, trigger, and schedule proposals use distinct
activation kinds and actions. A policy is a `repo_file` proposal targeting
exactly `.rk/repo.cue`; `apply` validates its schema but does not change live
execution. `workflow_activation`/`activate_workflow` targets exactly
`.rk/workflows/<name>.cue`, `trigger_activation`/`activate_trigger` targets
`.rk/triggers.cue`, and `schedule_activation`/`activate_schedule` targets
`.rk/schedules.cue`. `apply` patches and commits only the onboarding worktree,
then validates through the workflow/trigger/schedule CUE schema (including
schedule cron parsing). This is inert: the daemon does not discover automation
from onboarding worktrees.

`activate` is the separate human decision that crosses the activation boundary.
It journals an operation id before changing Git, then fast-forwards the clean,
registered base checkout only when it is still the exact parent of the
approved application commit. The onboarding branch head, committed tree,
target-file digest, repository identity, and live target digest must all still
match. A moved branch or changed file fails closed. Restart and duplicate
delivery are safe: if the exact application commit is already present with the
approved live digest, the daemon records recovery without landing it again.
`decline-activation` permanently records refusal while retaining the staged
branch.

The report's summary has separate `staged`, `verified`, `activated`, `declined`,
`failed`, and `unresolved` proposal lists. `cleanup` is allowed only for a
terminal session without staged or unresolved proposals; it removes a clean
onboarding worktree but retains both its Git branch and durable report.
Running, orphaned, and long-lived attached sessions are never cleaned.

By default a finished rat's branch is merged directly into its base. Set
`repo.delivery.mode` in `.rk/repo.cue` to `merge-push`, `push-branch`, or `pr`
when that repository needs a remote handoff. See
[docs/repository-policy.md](docs/repository-policy.md) for configuration and
[docs/pr-merge-mode.md](docs/pr-merge-mode.md) for forge credential details.

## Tickets

Durable work items — a backlog you and the rats can create, read, and
decompose. A ticket is a `task` tuple (`TKT-<ulid>`) that persists until closed,
and — because it carries a repo *name*, not a path — it replicates across
castles as a shared backlog through git-notes sync.

```bash
rk ticket new "Fix the login redirect loop" --repo svc --priority high
rk ticket new "Add SSO" --body "SAML + OIDC" --parent TKT-<id>   # use the id returned above
rk ticket list --repo svc --status open
rk ticket show TKT-<id>                                       # details + sub-tickets
rk ticket update TKT-<id> --status in_progress --assignee Whisker
```

Statuses: `open → claimed → in_progress → blocked → done → closed`. Rats are
primed to *file* or *decompose* tickets for follow-up work rather than starting
it themselves — the orchestrator routes them.

**Dependencies.** A ticket can be blocked by others (distinct from parent/child
decomposition — this is a DAG of "must finish first" edges). Cycles are
rejected.

```bash
rk ticket new "Build API" --depends-on TKT-<dependency-id>       # blocked-by at creation
rk ticket dep TKT-<ticket-id> TKT-<dependency-id>                # first is blocked by second
rk ticket undep TKT-<ticket-id> TKT-<dependency-id>              # drop the edge
rk ticket ready --repo svc                              # open tickets with all deps satisfied
```

`rk ticket list` marks blocked tickets with 🔒; `rk ticket show` annotates each
dependency as satisfied (✓) or `blocking`. `rk spawn --ticket TKT-<id>` refuses a
blocked ticket unless you pass `--force`, so `rk ticket ready` is the list of
what you can actually dispatch right now.

Dispatch a ticket straight to a rat — it fills the task and prompt from the
ticket, resolves the repo from the ticket's scope, and flips the ticket to
`in_progress`:

```bash
rk spawn --ticket TKT-<id>         # no hand-written --task/--prompt/--repo needed
```

The ticket's lifecycle then closes itself: when the rat finishes (its `rk done`,
or the harness's own completion for a rat that forgets), the ticket moves to
`done` — which **automatically unblocks any dependents** — and merging it on
`rk dismiss` moves it to `closed`. A rat that errors leaves its ticket
`in_progress` for inspection.

If an unattended auto-merge turns out bad, `rk revert <agent>` is the undo:
it revert-merges the commit that dismissal landed (recorded on the agent's
record), reopens the ticket to `open` (`--block` for `blocked`, holding it
out of the auto-dispatch backlog), and leaves a `fact` tuple recording what
was undone. History stays intact — the revert is a new commit, not a rewrite.

## Configuration (`~/.rat-kingdom/config.toml`)

```toml
castle_name = "my-laptop"        # author label override (default: Ed25519-key-derived actor id)

[harness]
default = "claude"               # harness when nothing else specifies

[agents.default]                 # global default agent profile
harness = "claude"
model = "sonnet"
permission_mode = "bypassPermissions" # optional; applies to every ordinary
                                      # direct/workflow/nested/drain spawn

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
stuck_after_secs = 600           # silence past this => STUCK (0 = off); keep
                                 # comfortably below any workflow `wait` that
                                 # blocks on this rat (e.g. steward's
                                 # reviewTimeout, default 15m) or the soft
                                 # steer below never gets a chance to help
burn_usd_per_min = 4.0           # sustained USD/min => RUNNING AWAY (0 = off;
                                 # 4.0 catches a runaway with ~3x margin both
                                 # ways vs normal rats' p99 $1.24/min)
kill_grace_secs = 600            # obstacle+steer first, kill only if still flagged
respawn_enabled = true           # self-heal: auto-respawn crashed/orphaned rats
                                 # (an agent whose branch already merged is
                                 # never auto-respawned)
respawn_max_attempts = 3         # crash-loop cap: give up after N, escalate a need
respawn_backoff_secs = 300       # base backoff, doubled per attempt (never merged)
                                 # (~15min across 3 attempts, so a systemic
                                 # failure doesn't exhaust them in ~3min)
respawn_rate_cap_per_hour = 10   # castle-wide: at most this many auto-respawns
                                 # (any agent) per rolling hour (0 = uncapped);
                                 # the one past the cap is HELD and escalated,
                                 # not fired — catches a fleet-wide storm a
                                 # per-agent cap alone would miss

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
require_approval_for_landing = true # land/open_pr normally needs a human gate
automated_landing_workflows = ["steward"] # land-only exception for managed global
                                          # definitions; local shadows stay untrusted
default_merge_mode = "direct"    # fleet-wide fallback for repos registered
                                 # without an activated `.rk/repo.cue`: "direct" merges the
                                 # branch, "pr" pushes it and opens a pull/merge
                                 # request for review (see docs/pr-merge-mode.md).
                                 # Versioned repository policy takes precedence.
```

Configuration is loaded when the daemon starts. After editing
`~/.rat-kingdom/config.toml`, restart the daemon (or run `mise run deploy` when
installing new source) before expecting new spawns to use it.

Env: `RK_HOME` (state dir), `RK_LOG` (tracing filter), `RK_CONFIG_*`
(config overrides, e.g. `RK_CONFIG_BUDGET_MAX_USD=2`). Plain `RK_*` names
(`RK_AGENT`, `RK_TASK`, ...) are reserved for agent identity — set at spawn,
never read as config.

Local RPC authorization does not trust those environment variables or the
bearer token alone. The daemon also binds each Unix-socket connection to its
kernel-reported process origin. A process launched in a supervised agent tree
or live agent worktree may claim only that agent, even if it clears
`RK_AGENT`/`RK_AUTH_TOKEN` and can read the same-user `RK_HOME/auth.token`.
Operator commands therefore fail closed when run from inside an agent
worktree; run them from an operator checkout or another non-agent directory.

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
wins). Shipped examples in `examples/workflows/` include:

- **solo-task** — spawn → wait → verify success → auto-merge.
- **code-review** — rat implements on the strong model, a cheaper reviewer
  examines the branch and records an `artifact` verdict; a human merges.
- **implement-featureset** — spawn a `foreman` middle-rat that delegates child
  tickets to workers, reviews and merges them into its feature branch, then
  lands the integrated branch.

```bash
for workflow in examples/workflows/*.cue; do
  rk workflow install "$workflow"
done
rk workflow drift --repo .
rk workflow defs
rk workflow run solo-task --param taskId=fix-login \
  --param description="Fix the login redirect loop"
rk workflow run implement-featureset --coordinator session-01 --param taskId=TKT-... \
  --param taskDescription="Implement the feature set"
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
  → `[harness] default`. The global default profile is also resolved centrally
  for direct and nested `rk spawn` calls and continuous-drain dispatches, so
  those paths cannot silently fall back to different permissions. Unknown
  profile/tier names are errors.
- Spawn steps inside a workflow base their worktrees on the previous agent's
  branch (`ctx.activeBranch`), which is how a reviewer sees the rat's work.
- **Per-instance budget**: add `budget: {max_usd: 2.5}` to a workflow to cap the
  summed cost of every agent one run spawns. Enforced as a dispatch preflight
  (same machinery as the global fleet/repo caps): once this run's spend reaches
  the cap, its next spawn — single or fan-out — is refused and the instance
  fails, surfacing a `budget_instance_exceeded` obstacle in `rk inbox`. Layered
  below `fleet_max_usd`/`repo_max_usd`; per-run spend shows in `rk cost --fleet`.
- **Liveness gate on results**: `wait`/`wait_all` block on *that generation* of
  the agent's own `harness_result`, and `evaluate` refuses to judge a result
  whose rat never reported one. A rat that was killed or crashed out of its run
  produces nothing, so a chain that unified against whatever happened to be in
  `ctx.previousResult` could report `Completed` having done nothing at all — the
  worst failure mode there is for an unattended loop. Such a run now fails and
  lands in `rk inbox` instead, and a `wait` gives up as soon as its rat leaves
  the fleet for good rather than blocking out its whole timeout (an `Orphaned`
  agent is still waited on: `rk respawn` heals the run).
- **Dropped lands surface even if the workflow forgot to gate them**: `land`
  reports a merge conflict as a clean `{merged: false}`, not an error, so a
  workflow can gate on the outcome and retry — but that gate
  (`{type: "evaluate", expect: {merged: true}, anyOf: [{pr_opened: true}]}`)
  lives in the definition, and a definition can be stale or forked per repo. One
  that was cost TKT-147 two days off main on a run that completed clean. So the
  invariant is asserted at read time instead: `rk inbox` shows an
  **`unlanded-branch`** row for any land that neither merged nor opened a PR,
  and clears it once git says the branch reached its target or is gone — a
  hand-merge, a re-land, or a cherry-pick plus a branch delete all retire it.
  Keep the `evaluate` in your workflows; you are no longer relying on it. See
  `docs/2026-07-26-tkt-171-dropped-land.md`.
- **Named checks (run-step allowlist)**: a `run` step runs a command in the rat's
  worktree to gate the merge on a verdict the runner cannot forge — but a raw
  `command` is only as trusted as the workflow def that carries it. Register the
  checks a repo trusts in `<repo>/.rk/checks.cue` (see `examples/checks.cue`) and
  reference one by name: `{type: "run", check: "test"}`. Set `[policy]
  require_named_checks = true` to refuse raw `command` run steps fail-closed, so a
  compromised or untrusted workflow definition can invoke only the repo owner's
  registered checks — never arbitrary shell. A named check supplies its own
  command/cwd/`expectExit`/timeout; the step may override cwd/`expectExit`/timeout.
  It may also declare `environmentPolicy: "inherit" | "strip_rk_spawn"`; the
  latter removes ambient supervised-agent identity before execution. Optional
  `toolchain` text records the repository-owned runner/toolchain for onboarding
  evidence. A workflow may pass data to that fixed command through an `env`
  map, but only with `RK_CHECK_*` names; attempts to replace `PATH`, loader
  hooks, or `RK_AGENT` fail closed. The shipped steward uses this mechanism for
  its repo-owned diff guards and escalation actions, so repositories enabling
  it must register the matching `steward-*` checks shown in
  `examples/checks.cue` as well as their `verify` check.
  The same registry is surfaced as optional guidance in spawned worker prompts;
  it does not replace the workflow gate. See `examples/workflows/named-check-merge.cue`.
- **Run-step evidence and retries (TKT-01M02AMKD24WZVVMARJPXKYKSW)**: a `run`
  step whose verdict is not `"pass"` (a red check or a timeout) writes a
  durable, bounded `(artifact, <repo>, gate-failure)` tuple — `{instance,
  agent, command, exit, verdict, timed_out, stdout_tail, stderr_tail,
  failing_tests, retries}` — before the next step can overwrite
  `ctx.previousResult`. `failing_tests` is parsed from `test <name> ...
  FAILED` lines, so `rk scan artifact <repo>` (or `rk inbox`) names what broke
  instead of only recording that the gate said no. A step may also set
  `retryOnFail: <n>` (default 0) for a check already characterized as flaky
  for reasons outside the code under test — machine load from several
  fleet-wide builds running at once is the shipped steward's case. A retry
  does not weaken the gate: a genuinely red suite fails every attempt and
  still holds the branch, just a few seconds later, and every attempt is
  recorded (`retries` on the result, or in the gate-failure artifact on the
  final non-`"pass"` verdict) so a recovered flake stays visible.

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
notify_escalations = true # false = hard kill switch, zero notification sinks, inbox-only
```

- **Active escalation push.** When the steward escalates a `STOP`/unknown
  verdict as a `need` (identity `steward`), a built-in reaction fans it out
  through every configured **notification sink**, so the operator is pushed
  at, not only queued in `rk inbox`. Which channels see it is config, not code
  — see [Notification sinks](#notification-sinks) below.
- **Open ballots reach the operator.** `rk suggest` proposes a norm and the
  reactor promotes it to a permanent `convention` at `quorum` distinct
  endorsers — but nothing announced that a vote was open, so proposals decayed
  on their voting window and the fleet promoted **zero** conventions in 277
  spawns. `rk inbox` now shows each live proposal as an **`open-suggestion`**
  row: proposer, text, `n/quorum` tally, time left, and `rk endorse <sug-id>` to
  back it. Closest-to-decaying sorts first; the row disappears once the norm
  promotes or the window closes. `rk endorse` works outside a rat too (it votes
  as `operator`), so the queue's resolving command is one a human can run.

See `docs/reactor.md` for the full design (why scan-is-truth, the three
re-entrancy guards, first-boot backlog skipping).

### Notification sinks

Escalation delivery is a config table, not a hardwired call. An escalation
source builds a channel-agnostic notice and hands it to the sink registry,
which fans it out to every `[[notify.sinks]]` entry that accepts it:

```toml
[[notify.sinks]]
kind = "herdr"                          # desktop push (rk-mux)

[[notify.sinks]]
name = "ops-chat"                       # defaults to the kind if unset
kind = "command"                        # shell out to an operator script
classes = ["steward-escalation"]        # empty = every class
min_severity = "warn"                   # info (default) | warn | critical

[notify.sinks.options]
command = "/usr/local/bin/rk-notify-chat"
timeout_secs = "30"
```

- **Built-in kinds:** `herdr` (the historical desktop push), `log` (writes the
  notice through `tracing` at its severity — zero options, cannot fail to be
  installed, the honest default on a headless castle), and `command` (execs an
  operator program with the notice on argv, as `RK_NOTICE_*` env vars, and as
  JSON on stdin — the escape hatch for a chat webhook, phone push, or a script
  that drives `rk`). A repo/embedder can register further kinds; an unknown
  `kind` in a table is skipped and logged, never fatal.
- **Back-compat.** `[[notify.sinks]]` is empty by default, which is *not* "no
  notifications" — it means "use the built-in default", so a castle that never
  heard of this section keeps the one herdr sink it always had.
  `[reactor].notify_escalations = false` predates sinks entirely and stays a
  hard kill switch: it drops to zero sinks regardless of what
  `[[notify.sinks]]` says. Any non-empty `[[notify.sinks]]` list is the
  operator's list, verbatim — adding a second channel is one more table, no
  code change anywhere.
- **Dedup.** Markers are per-`(tuple, sink)`, so adding a channel does not
  inherit another channel's "already pushed" state. The `herdr` sink also
  honours the pre-sink-registry marker key, so upgrading mid-flight does not
  re-pop a notification that already fired.
- **Best-effort.** A sink that errors, hangs, or is not installed produces a
  logged delivery failure and nothing else — the escalation is already durable
  in the tuplespace and ranked by `rk inbox`, so a dead channel degrades to the
  passive queue rather than blocking the reactor cycle.

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
  A `Running` instance older than `stale_running_hours` (default 6h) no longer
  blocks: the bypass is escalated via a `need` tuple (once per wedged
  instance) rather than silently routing around it forever.
- **The `nightly-self-improve` chain** is the headline schedule: one workflow
  that grooms the backlog, drains it in parallel, then proposes prompt/convention
  refinements — the three self-improvement loops welded into a single instance so
  the whole night runs behind one single-flight lock. See `docs/scheduler.md` and
  `examples/workflows/nightly-self-improve.cue`.

```toml
[scheduler]
enabled = true
interval_secs = 30         # cron-minute check cadence; clamped [1,60]
catchup_minutes = 1440     # look-back bound after downtime; 0 = current minute only
stale_running_hours = 6    # age past which a wedged Running instance stops blocking its schedule
```

See `docs/scheduler.md` for the full design (cursor/catch-up semantics, the
Vixie day-of-month/day-of-week rule, single-flight).

## Attach mode (herdr)

With a running [herdr](https://herdr.dev) server, rats can run interactively
in panes you can watch and take over:

```bash
herdr integration install claude   # once — lets herdr report TUI readiness
rk spawn --task tricky-1 --attach --prompt "..."
rk spawn --harness jcode --task tricky-2 --attach --prompt "..."
rk attach Whisker                  # drop into the live session
rk steer Whisker "try the other approach"   # works from outside too
```

The daemon still owns the worktree/branch/registry; completion is the rat's
own `rk done`, and dismissal closes the pane and merges as usual. Without
herdr, everything runs headless — but you are not blind to a headless rat:
`rk log <name>` replays its transcript (assistant prose, tool calls, retries)
and `rk log <name> --follow` streams it live, persisted as a bounded per-agent
ring under `~/.rat-kingdom/agent-logs/` (local only; never synced).

Transcripts are filed per *generation* — `<name>.<spawn-instant>.jsonl` — not
per name. A name normally names one rat for good, but 24 names briefly named two
(see `docs/2026-07-25-agent-log-generations.md`), so `rk log <name>` shows the
newest by default and says so when there are others; `--generation N` (1 =
oldest) reads an earlier one.

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
mise run verify            # build + test + clippy, the full pre-`rk done` check
mise run lint              # clippy alone, warnings as errors
```

The toolchain is pinned to Rust 1.95.0 in `mise.toml`, so run cargo through
mise — a bare `cargo` picks up whatever is on `PATH` and an older one fails the
MSRV check before it compiles anything:

```bash
mise exec -- cargo test --workspace       # integration tests use a scripted fake harness
mise exec -- cargo clippy --workspace --all-targets
```

Run the suite with `RK_AGENT` unset — `mise run test` does this for you. The
daemon client sends `$RK_AGENT` as the RPC caller and the daemon refuses
operator-only methods (`workflow.run`, `agent.spawn`, …) from an agent, so
inside a rat, where that variable is set, tests fail with `forbidden` for
reasons that have nothing to do with your change (TKT-182).

The committed tree is clippy-clean under the pinned toolchain, which is why
`lint` can deny warnings: anything it prints belongs to your change, not to the
baseline. A toolchain bump may add lints over code that was clean when written
— sweep those deliberately in one commit rather than folding them into an
unrelated change.

Crate map: `rk-core` (tuple model, config, priming), `rk-space` (tuplespace),
`rk-git` (worktrees/merges), `rk-harness` (claude/codex/jcode/fake adapters),
`rk-ledger` (pricing/budgets), `rk-workflow` (CUE definitions), `rk-sync`
(git-notes replication), `rk-mux` (herdr), `rk-daemon` (supervisor, executor,
server), `rk-cli` (`rk`).

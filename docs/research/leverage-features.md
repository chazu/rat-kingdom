# Top leverage features for rat-kingdom

Research task: brainstorm the highest-leverage features to add to rat-kingdom —
features that most increase the operator's ability to get more/better work out of
the fleet **per unit of their attention**. Read-only analysis; grounded in the
actual code, not generic advice.

## Method & framing

"Leverage" here is throughput-and-quality-per-operator-attention. Three shapes of
feature dominate the ranking:

1. **Loop removal** — take the operator out of a loop they currently sit in
   (dispatching, reviewing, merging). Pure multiplier on unattended work.
2. **Attention compression** — collapse N surfaces the operator must poll into one
   (or push the important thing to them). Multiplier on each glance.
3. **Safety that unlocks autonomy** — you can only *leave* the fleet running if it
   can't silently hang, run away, or merge garbage. Guardrails are what convert
   "watch it closely" into "check it twice a day."

### What already exists (so we don't re-propose it)

Read of the workspace establishes the real primitive set:

- **Tuplespace** (`rk-space`): single-critical-section `out`/`take`/`rd` with one
  match predicate everywhere (`Pattern::matches`, `tuple.rs:199`; SQL mirror
  `store.rs:102`; waiter-wake `lib.rs:82`), a live broadcast feed
  (`Space::subscribe`, `rk-space/src/lib.rs:186`, capacity 1024, lossy),
  TTL/lifecycle GC. 12 categories incl. `Suggestion`/`Endorsement` (defined but no
  promotion logic) (`tuple.rs:16-41`).
- **Supervisor** (`rk-daemon/src/supervisor.rs`): spawn → event-pump → structural
  completion routing (`route_completion`, `:602`) → dismiss+merge. Registry is
  restart-durable (`agents.rs`), orphans+respawns. **No idle/heartbeat/liveness
  probe on a running rat** — failure is only `Exited`-without-`Completed`
  (`:517-528`); budget checks fire *only* on `Usage` events (`:537`).
- **Ledger/budget** (`rk-ledger`): vendored pricing, per-agent graduated
  warn→steer→kill enforced on every `Usage` (`supervisor.rs:537-571`). Budgets are
  **per-agent config only** (`BudgetConfig`, `config.rs:53`) — no fleet/workflow/
  repo caps, no preflight estimate.
- **Workflow engine** (`rk-workflow` + `workflow_exec.rs`): CUE-defined linear step
  machine with control flow — `spawn/wait/evaluate/dismiss/gate/read/when/repeat/
  break/stop/for_each/wait_all/dismiss_all` (`schema.cue:71`). Approval gates,
  fan-out with **atomic ticket claim** (TKT-6, `workflow_exec.rs:495`), parallel
  join and merge. **Missing: reactive triggers, sub-workflows, instance restart
  recovery, a "run a command" step, a "merge a named branch" step** (Phase 5
  REMAINING, impl-plan `:49`).
- **Harnesses** (`rk-harness`): claude (steer/resume/self-cost), codex, axe
  (one-shot, native `--max-tokens`, exit-4). `HarnessEvent::{AssistantText,ToolUse}`
  are **captured then dropped** (`supervisor.rs:529`) — no transcript persistence.
- **Multiplayer** (`rk-sync`): per-actor git-notes refs, deterministic claim
  arbitration, cross-castle ticket/tuple replication.
- **Tickets** (`tickets.rs`): status/priority/labels/deps/parent-child, atomic CAS
  `claim` (`:218`), dependency-aware `ready` (`:295`).
- **herdr mux** (`rk-mux`): panes for attach, `HerdrMux::notify` desktop alerts
  (`:84`, currently fired only on completion, `supervisor.rs:369`).

The existing `docs/research/high-leverage-workflows.md` covers the workflow
*library* (which `.cue` files to ship). This report is about *capabilities/
primitives* — a different axis — and deliberately avoids re-listing those workflows.

---

## The 30 candidates

Grouped by leverage dimension. ★ = selected for the deep-dive top 10.

### Autonomy — take the operator out of the driving loop

1. **★ Reactive trigger engine** — tuple write → workflow dispatch, zero-token,
   zero-latency. *Leverage: the substrate that turns the whole system from
   operator-pull to self-driving; every other autonomy feature composes on it.*
2. **★ Steward: autonomous triage & auto-merge** — on completion, a cheap rat runs
   the repo's checks and auto-merges clean work, escalating only the rest.
   *Leverage: removes the human from the single most-repeated decision — "is this
   branch good to merge?"*
3. **★ Continuous WIP-limited dispatcher (fleet autoscaler)** — keep W rats fed
   from the ready backlog automatically. *Leverage: "work the backlog" becomes a
   set-and-forget dial instead of one spawn per ticket.*
4. **★ Scheduled / cron workflows** — fire groom/drain/self-improve on a cadence.
   *Leverage: the fleet improves its own backlog, prompts, and tests overnight with
   zero operator initiation — compounding.*
5. **Auto-decompose→drain macro** — one command: groom oversized tickets, then fan
   out the newly-ready ones. *Leverage: chains two existing workflows into a
   backlog-to-zero pipeline the operator triggers once.*
6. **Dependency-unblock auto-dispatch** — when a ticket closes, auto-spawn the
   dependents it just unblocked (`tickets.set_status` already unblocks; trigger the
   spawn). *Leverage: dependency chains flow themselves; no re-checking `rk ready`.*
7. **Self-healing respawn with crash-loop backoff** — auto-`respawn` orphaned rats
   after a daemon restart, bounded by an attempt cap. *Leverage: transient crashes
   stop being a manual `rk respawn` chore.*

### Coordination — make many agents/repos cohere

8. **Sub-workflow step** — call one workflow from another (Phase 5 REMAINING).
   *Leverage: reuse and compose control flow instead of copy-pasting step lists.*
9. **Suggestion/endorsement quorum promotion** — `Suggestion`+`Endorsement`
   categories exist but nothing promotes them; add quorum → `Convention`.
   *Leverage: the fleet's own improvements compound into shared priors across repos.*
10. **Cross-repo backlog orchestration** — drain ready tickets across *all*
    registered repos, not one. *Leverage: one operator drives a multi-repo fleet.*
11. **Merge queue / serialized land** — order concurrent merges to main. *Leverage:
    parallel drains stop stepping on each other (the CAS at `git/lib.rs:147` already
    fails-safe, but a queue avoids the wasted re-work).*

### Observability — see the fleet without staring at it

12. **★ `rk inbox`: unified attention queue** — one ranked list of everything
    awaiting a human (parked gates, obstacles, needs, failed instances, orphans).
    *Leverage: collapses 4–5 polling surfaces into a single triage view — the
    biggest per-glance win.*
13. **`rk top`: live fleet dashboard** — ratatui view of agents/states/burn/
    workflows (Phase 7). *Leverage: situational awareness at a glance; overlaps
    inbox but continuous.*
14. **★ `rk log`: agent transcript/timeline** — surface the `AssistantText`/
    `ToolUse` stream that is currently dropped. *Leverage: turns a surprising run
    from "re-run and hope" into "read what it did" — the trust that unlocks
    delegation.*
15. **Fleet digest ("what happened while you were away")** — LLM-summarized
    interval report over the tuple feed. *Leverage: async catch-up instead of
    scrubbing `rk watch`.*
16. **Workflow instance timeline** — visualize the step trace / where an instance is
    parked. *Leverage: debugging stuck workflows without reading JSON.*

### Safety / guardrails — make unattended running trustworthy

17. **★ Burn-rate & stuck detection** — no-progress + high-burn → obstacle →
    steer → kill (Phase 4 REMAINING; the explicit "no heartbeat" gap). *Leverage:
    the precondition for ever leaving the fleet alone.*
18. **★ Pre-merge verification `run` step** — run the repo's tests/lint in the
    worktree and fail-closed before merge. *Leverage: the teeth that make
    auto-merge (steward, dispatcher) trustworthy.*
19. **Policy engine (per-repo guardrails)** — protected paths, max cost, required
    review, allowed tools, enforced at spawn/merge. *Leverage: one config makes a
    whole repo safe for broad autonomy.*
20. **Diff-scope guardrail** — flag/block merges touching sensitive paths or
    exceeding a size budget. *Leverage: catches the runaway refactor before it
    lands.*
21. **Rollback / revert-merge** — `rk revert <agent>` undoes a bad dismissal.
    *Leverage: cheap recovery lowers the cost of trusting auto-merge.*
22. **Workflow instance restart recovery** — resume running instances across daemon
    restart (Phase 5 REMAINING). *Leverage: long unattended runs survive a restart
    instead of dying silently.*

### Quality — raise the bar per merge

23. **★ Merge-to-main / "land" step** — a step that merges a *named* branch, so a
    reviewer's APPROVE lands work directly (today it can only *complete*; see
    high-leverage-workflows.md §5). *Leverage: closes the last manual hop in
    autonomous review→merge.*
24. **Multi-reviewer adversarial verify panel** — N independent reviewers,
    majority-refute kills the merge. *Leverage: fewer bad merges slip through than
    a single reviewer.*
25. **Auto-learnings capture** — completed work writes `fact`/solution tuples
    (recurring gotchas, fixes). *Leverage: the fleet stops re-learning the same
    lesson; knowledge compounds in-context.*

### Cost control — more work per dollar

26. **★ Model & harness cost-aware tiering** — cheap axe/haiku for bounded jobs,
    premium claude/opus for hard ones, driven by ticket labels/priority. *Leverage:
    the same backlog at a fraction of the spend → the fleet scales wider under a
    fixed budget.*
27. **Hierarchical budgets** — workflow/repo/fleet caps (today only per-agent) with
    `rk cost --fleet` enforcement. *Leverage: a fleet-wide kill-switch is what makes
    a big autoscaler safe to run.*
28. **Preflight cost estimate** — estimate a workflow/ticket run before launching.
    *Leverage: the operator sizes the bet before placing it.*
29. **Pricing refresh + offline session-JSONL backfill** (Phase 4 REMAINING).
    *Leverage: accurate cost is the input every budget/tiering decision depends on.*

### Scaling / expressiveness

30. **Richly-typed workflow params + `--param-file`** — params are string-only
    today (`main.rs:365` wraps every value as a string). *Leverage: workflows take
    structured inputs (lists, numbers, objects), unlocking data-driven fan-out.*

---

## The top 10 by leverage (ranked)

Ranking rationale: #1 is the substrate the other autonomy features ride on; #2–#4
are direct loop-removal/attention-compression with the largest attention payoff;
#5–#6 are the safety and quality guardrails that *unlock* trusting #2–#4; #7–#10
are strong multipliers that depend on or extend the above.

---

### 1. Reactive trigger engine

**What it does.** A daemon component that subscribes to the tuplespace's live feed
and, when a written tuple matches a declared `#Trigger` (`{category, identity}` +
agent/scope excludes), fires a workflow with params templated from the tuple's
payload — zero tokens, zero model latency, cannot be broken by an agent deviating
from protocol. E.g. `harness_result → steward`, `task_ready → drain-one`,
`obstacle:budget_exceeded → notify-operator`.

**Why it's high leverage.** This is the single feature that converts rat-kingdom
from *operator-pull* (every workflow is `rk workflow run`) to *self-driving*
(events dispatch the next action). The design doc calls reactive triggers "the
reliability win" and the predecessor's biggest correct idea (§3, design doc), and
Phase 5 lists it as the headline REMAINING item (impl-plan `:49`). Features #2, #3,
#4, #6-steward, and #9 all become trivial once triggers exist; without it each is a
bespoke daemon loop. It is the highest-leverage feature precisely because it is
*leverage-on-leverage*.

**Implementation sketch (grounded).**
- The feed already exists: `Space::subscribe()` (`rk-space/src/lib.rs:186`) hands
  out a `broadcast::Receiver<Tuple>`; `stream_watch` (`server.rs:313`) already
  consumes it for `rk watch`. Add a `TriggerEngine` background loop next to the GC
  and sync loops (`server.rs:184-228`) that consumes the same feed.
- Reuse the workflow load path: `#Trigger` defs live in `<repo>/.rk/triggers.cue`
  and the global dir, discovered exactly like workflow defs
  (`WorkflowEngine::find_definition`, `workflow_exec.rs:118`), evaluated via the
  same `cue export` pipeline (`rk_workflow::load`, `lib.rs:278`).
- On a matching tuple, template params from `tuple.payload` and call the existing
  `engine.run(name, repo, params)` (`workflow_exec.rs:156`) — which already
  background-spawns the instance.
- Match with `Pattern::matches` (`tuple.rs:199`) to keep one predicate everywhere.

**Dependencies / risks.**
- **Re-entrancy / trigger storms.** A workflow whose action writes a tuple that
  re-fires it loops forever. Mitigate with a re-entrancy guard: exclude tuples
  authored by workflow-spawned agents (agent/instance excludes are in the
  predecessor's `#Trigger` shape) and/or a per-trigger rate cap. This is the one
  place a hard bound is mandatory (mirrors the `repeat` `max<=100` discipline,
  `schema.cue`).
- **Lost events.** The broadcast feed is lossy (capacity 1024, "laggy watchers miss
  events", `lib.rs:186`). `rk watch` tolerates gaps; a trigger must not. Either
  drive dispatch from a durable cursor over the store (`store.query` ordered by
  `id`, `store.rs:137`) with at-least-once + idempotent dispatch, or accept the
  broadcast with a reconciliation sweep. Recommend cursor-based, reusing the sync
  loop's cursor pattern (`sync.rs:175`).
- No new storage; additive.

---

### 2. Steward: autonomous triage & auto-merge

**What it does.** On every rat completion, a lightweight steward automatically
fetches the branch, runs the repo's checks (via #6), and: merges clean work,
routes fixable work back to a rework rat, and escalates only genuine judgment calls
to the operator (via #12/`HerdrMux::notify`). The operator reviews exceptions, not
every branch.

**Why it's high leverage.** "Is this branch good to merge?" is the most frequent
decision the operator makes — once per completed task. Automating the *clean* case
(the majority) is the biggest single reduction in per-task attention. The
predecessor's steward pattern is explicitly flagged as a keep (design doc §3,
Phase 7), and reviewer-drives-rework already ships the routing primitives — the
steward is their reactive, unattended application.

**Implementation sketch (grounded).**
- Register a trigger (#1) on `Event/harness_result` (emitted by
  `route_completion`, `supervisor.rs:606`) that fires a `steward` workflow scoped to
  the completed rat's branch (`ctx.active_branch` chains onto it, exactly as
  `code-review.cue` does).
- The steward workflow: `spawn` a cheap reviewer/axe rat on the branch → `run`
  the repo's test/lint gate (#6) → `read` the reviewer's verdict artifact
  (`read` step already lifts `rk out artifact … review`, `workflow_exec.rs:385`) →
  `when` on verdict: APPROVE → `dismiss` (merge via `merge_branch`,
  `supervisor.rs:709`, CAS-safe `git/lib.rs:118`); REWORK → file ticket + spawn
  rework; STOP/unknown → `need` tuple + `HerdrMux::notify` (`rk-mux/src/lib.rs:84`).
  All these steps already exist.

**Dependencies / risks.**
- Depends on **#1 (triggers)** to be reactive, and on **#6 (run gate)** for real
  teeth — an auto-merge behind a weak check is worse than manual review.
- **False confidence**: pair with **#19 (policy)** so the steward refuses to
  auto-merge diffs touching protected paths, forcing those to the operator.
- Merge-to-main nuance: a reviewer chained off the work branch merges into its
  *base*, not main directly (documented limitation, high-leverage-workflows.md §5);
  #23 removes that hop.

---

### 3. Continuous WIP-limited dispatcher (fleet autoscaler)

**What it does.** A persistent controller that maintains a target concurrency W:
whenever live rats drop below W and the ready backlog is non-empty, it spawns the
highest-priority ready ticket. `backlog-drain` fans out once; this *refills*
continuously. "Keep the fleet busy" becomes a config dial (`max_wip` per repo /
per fleet).

**Why it's high leverage.** It removes the operator from the dispatch loop
entirely: instead of one `rk spawn --ticket` per item (or one `backlog-drain` per
batch), a well-groomed backlog turns itself into a steady stream of parallel work at
a controlled burn. Combined with #2 (steward closes each item) it's a closed
loop — the operator grooms/prioritizes, the fleet executes.

**Implementation sketch (grounded).**
- A daemon loop modeled on the sync loop (`server.rs:203-228`), or a self-re-arming
  trigger on `harness_result` (#1). On each tick: count live agents
  (`registry.list`, filter `state.is_live()`, `agents.rs:26`); if `< W`, pull
  `tickets.ready(scope)` (`tickets.rs:295`, dependency-aware), pick by `priority`,
  and `supervisor.spawn` it.
- Double-grab safety is already solved: `Tickets::claim` is an atomic CAS
  open→in_progress (`tickets.rs:218`, proven by `concurrent_drains_never_double_grab`,
  `:591`) — the fan-out path already uses it (`workflow_exec.rs:495`).
- Merge contention across parallel branches is already safe: `merge_branch` uses a
  temp detached worktree + CAS on the target ref (`git/lib.rs:147`), marking a
  losing race `merged:false` rather than corrupting.

**Dependencies / risks.**
- **Needs #17 (stuck detection) and #27 (fleet budget)** or it will happily spawn
  into a hang or drain the wallet — this is the clearest case where an autonomy
  feature is unsafe without its guardrails.
- Priority starvation: low-priority tickets never picked — add aging.
- Cross-repo variant is #10.

---

### 4. `rk inbox`: unified operator attention queue

**What it does.** One command that scans the whole system for everything awaiting a
human decision and renders a single ranked list, each row carrying the exact command
to resolve it: workflow instances parked at an approval gate (`rk approve/reject`),
`Obstacle` tuples (`budget_exceeded`, `stuck`, `sync_failure`), `Need` tuples,
`Failed`/`Orphaned` agents (`rk respawn`), failed workflow instances.

**Why it's high leverage.** Today that information is spread across `rk list`,
`rk workflow list`, `rk scan obstacle`, `rk scan need`, and `rk watch` — the
operator must poll five surfaces to know what needs them. Collapsing them into one
prioritized triage list is the largest *per-glance* attention win in the report, and
it is almost pure read-side aggregation over data that already exists.

**Implementation sketch (grounded).**
- New `rk inbox` + daemon method that unions:
  - Registry agents in `Failed`/`Orphaned` (`agents.rs:14`, already listable).
  - `Obstacle` and `Need` tuples via `space.scan` (`Pattern::category(Obstacle)`,
    `tuple.rs:180`) — budget obstacles already emitted (`supervisor.rs:581`).
  - Workflow instances with `status==Running` sitting at a `gate` and `Failed`
    instances (persisted at `workflow-instances/*.json`, `workflow_exec.rs:737`).
- Rank by a simple urgency heuristic (budget_exceeded > failed instance > parked
  gate > need) and print the resolving command per row.

**Dependencies / risks.**
- No new storage; the only work is aggregation + ranking. Lowest-risk item in the
  top 10, which is why it ranks so high on leverage-per-effort.
- Ranking is heuristic; keep it transparent (show the raw category) so the operator
  can override.
- Complements #13 (`rk top`, continuous) and #15 (digest, async); inbox is the
  on-demand "what needs me *now*."

---

### 5. Burn-rate & stuck detection

**What it does.** A periodic supervisor sweep that flags rats that are (a) *stuck* —
no events for N minutes while still `Running` — or (b) *running away* — sustained
high token burn with no completion. Graduated response reuses the existing budget
machinery: obstacle tuple → steer ("still working? wrap up") → kill.

**Why it's high leverage.** It is the precondition for every autonomy feature above.
The code today has **no idle/heartbeat/liveness probe**: a rat only "fails" if its
process `Exited` without `Completed` (`supervisor.rs:517-528`), and budget checks
fire *only* on `Usage` events (`:537`) — so a rat that hangs mid-tool-call, emitting
nothing, is invisible forever (its only wall-clock bound is the 24h attach-mode `rd`
timeout). You cannot safely run #2/#3 unattended until a hang or runaway is
detected and contained. This is Phase 4's explicit REMAINING item (impl-plan `:28`).

**Implementation sketch (grounded).**
- Add a timer loop like the GC loop (`server.rs:184`, `GC_INTERVAL`), or piggyback
  on it. For each live agent, compare `updated_at` (bumped on every `Usage`,
  `agents.rs` via `Registry::update`) to now: exceed `stuck_after` → emit obstacle
  via the existing `emit_obstacle_for_budget`-style path (`supervisor.rs:581`) and
  `steer` (`:653`); escalate to `control.kill()` (`:560`) after a grace period.
- Burn-rate = Δ`cost_usd` / Δtime across sweeps (both fields on `AgentRecord`);
  sustained rate above threshold with no `Completed` → runaway → same graduated
  action as `enforce_budget` (`:537-571`), which is already written and tested.
- Corroborate with herdr agent-status (`idle/working/blocked`, available via
  `rk-mux`) where present to cut false positives.

**Dependencies / risks.**
- **False positives** on legitimately long silent work (a rat compiling or running a
  slow test). Tune thresholds per harness; prefer steer-then-wait over immediate
  kill; use herdr status as a second signal.
- Feeds #12 (`stuck` obstacles surface in the inbox) and #17-emitted tuples are what
  #1 can trigger on.

---

### 6. Pre-merge verification `run` step

**What it does.** A new workflow step `{type:"run", command, cwd?, expect_exit?,
timeout}` that executes a command (the repo's test/lint suite) in the active agent's
worktree, capturing exit code and output into `ctx.previous_result` for a following
`evaluate`/`when`. Fail-closed: non-zero exit blocks the merge.

**Why it's high leverage.** It is the missing quality primitive that makes automated
merging trustworthy. Today `evaluate` only CUE-unifies against the *harness's own
reported output* (`unify_concrete`, `workflow_exec.rs:305`) — it takes the rat's
word. Nothing runs the repository's own checks deterministically before a merge. A
`run` gate converts "the rat says it passed" into "the suite is green or it does not
land." Every auto-merge path (#2 steward, #3 dispatcher, #23 land) is only as safe
as this gate.

**Implementation sketch (grounded).**
- Add a `Run(RunStep)` variant to the `Step` enum (`rk-workflow/src/lib.rs:54`) and
  `#RunStep` to `schema.cue` (alongside the existing 13 step types, `:71`).
- Handler in `run_step` (`workflow_exec.rs:250`): the active agent's worktree path
  is on its `AgentRecord`; run the command with `tokio::process` in that cwd, with a
  `parse_duration` timeout (`:829`), store `{exit, stdout, stderr}` in
  `ctx.previous_result`. A following `evaluate {expect:{exit:0}}` fails the instance
  on red (evaluate already fails-closed).

**Dependencies / risks.**
- **Arbitrary command execution.** ✅ ADDRESSED (TKT-30). A run step may reference a
  repo-registered named `check` (`<repo>/.rk/checks.cue`) instead of a raw `command`,
  and `[policy] require_named_checks = true` refuses raw commands fail-closed — so a
  compromised workflow def can invoke only the repo owner's registered checks, never
  arbitrary shell.
- Long suites: bound by timeout; consider running under axe's budget for a hard cap.
- Enables #2 and strengthens #23.

---

### 7. `rk log`: agent transcript / timeline

**What it does.** Persist a bounded per-agent event log (assistant text, tool calls,
retries) and expose `rk log <agent> [--follow]` to render what a rat actually did —
its reasoning and actions, not just its final one-line result.

**Why it's high leverage.** The operator is currently *blind* to a rat's work unless
they `--attach`: `HarnessEvent::AssistantText` and `ToolUse` are received and then
**explicitly ignored** (`supervisor.rs:529-531`); `rk status` shows only state +
final `result`. When a run surprises you (wrong approach, subtle bug, silent
loop), the only recourse is re-run and hope. A readable transcript turns that into a
diagnosis, and — more importantly — the *visibility* is what lets an operator trust
the fleet with larger, less-supervised tasks. Trust is the real bottleneck on
delegation.

**Implementation sketch (grounded).**
- In `handle_event` (`supervisor.rs:466`), instead of dropping `AssistantText`/
  `ToolUse`/`Retry`, append them to a bounded per-agent JSONL under `RK_HOME`
  (mirrors how workflow instances persist to `workflow-instances/*.json`,
  `workflow_exec.rs:737`). Cap the ring to bound disk.
- Alternatively/additionally, read the harness's own session JSONL: the design notes
  herdr reports `agent-session-path` and Claude writes `~/.claude/projects/**/*.jsonl`
  (design doc §5) — `rk log` can tail that directly for full fidelity.
- `rk log --follow` subscribes the same way `rk watch` streams (`space_cmds.rs:219`).

**Dependencies / risks.**
- **Volume**: transcripts are large — bound the persisted ring; prefer on-demand
  read of the harness JSONL for full history.
- Privacy: transcripts may contain sensitive repo content; keep them local (they
  already never touch git sync — ephemeral/non-tuple).

---

### 8. Model & harness cost-aware tiering

**Landed (TKT-26).** A cost-tier routing table maps a ticket's `labels`/`priority`
to a *tier* — the name of an agent profile (`[agents.<tier>]` global, or a
workflow `agents:` entry). `resolve_fields` (`resolve.rs`) gained a tier layer
just below inline overrides, so a routing rule beats the static profile defaults
yet an inline `harness:`/`model:` still wins. Global rules live in `[tiers]`
(`[[tiers.rules]]`, `config.rs`); a workflow's own `tiers:` field
(`schema.cue #TierRouting`) shadows them. The fan-out (`for_each`) routes each
ready ticket by its labels/priority (`workflow_exec.rs fan_out`); single spawns
carry no ticket and route as before. First matching rule wins; a rule with
neither predicate is an unconditional fallback. Escalation-on-failure needs no
new machinery — drain cheap, `wait_all`, and on an `evaluate` failure re-run the
hard tickets on the premium tier. Types + routing (`rk_workflow::TierRouting`),
example `examples/workflows/cost-tiered-drain.cue`, e2e
`crates/rk-daemon/tests/tier_routing.rs`.

**What it does.** Route each job to the cheapest harness/model that can do it: a
one-shot `axe` or a small model (haiku) for bounded/mechanical work (grooming,
verify, lint, doc fixes), premium claude/opus for hard implementation — driven by
ticket `labels`/`priority` or a workflow field, with escalation on failure.

**Why it's high leverage.** Cost is the ceiling on how *wide* the fleet can run. The
agent-resolution layering already exists (`resolve.rs:24`, step > profile > global),
the harness abstraction already spans claude/codex/axe with axe's native budget cap
(`axe.rs`, exit-4), and `rk cost` already prices everything
(`agent_cmds.rs:331`) — so the ROI is directly measurable. Tiering means the same
backlog costs a fraction, which means the autoscaler (#3) can run more rats under a
fixed budget. It's the economic multiplier on scale.

**Implementation sketch (grounded).**
- Extend agent resolution / `SpawnParams` to accept a *tier hint*, and add a
  routing rule (config or per-workflow) mapping ticket `labels`/`priority`
  (`tickets.rs` payload) → `{harness, model}`. `resolve_fields` (`resolve.rs:45`)
  already does field-wise layering; add a tier layer beneath inline overrides.
- Escalation-on-failure is already expressible: a weak-model `spawn` → `evaluate`
  fail → `when`/`repeat` re-spawn on the premium tier (control-flow steps all exist,
  `schema.cue:71`).

**Dependencies / risks.**
- **Mis-routing** hard work to a weak model wastes a round-trip; make escalation
  automatic (the eval-fail → re-spawn loop) so the cost is one cheap attempt, not a
  bad merge.
- Needs #29 (accurate pricing) to make the tradeoff on real numbers.

---

### 9. Scheduled / cron workflows

**What it does.** A daemon scheduler that fires workflows on a cadence from
`#Schedule {cron, workflow, repo, params}` definitions — nightly `backlog-groom`,
nightly `backlog-drain`, weekly `prompt-refine` / `workflow-review` (the
self-improvement loops already designed in high-leverage-workflows.md).

**Why it's high leverage.** It adds the *time* axis to autonomy: the fleet grooms
its own backlog, refines its own prompts, and re-runs its own test sweeps while the
operator sleeps — compounding improvements that need zero initiation. It's the
difference between "the operator remembers to run grooming" and "grooming happens."

**Implementation sketch (grounded).**
- A scheduler loop next to the sync loop (`server.rs:203`), reading `#Schedule`
  defs via the same load path as workflows/triggers (`find_definition`,
  `workflow_exec.rs:118`) and calling `engine.run` on cadence.
- Reuses the #1 dispatch path; a scheduled fire is just a time-sourced trigger.

**Dependencies / risks.**
- **Overlapping runs**: guard each scheduled workflow with a single-flight lock (a
  furniture tuple or an instance-exists check) so a slow nightly drain doesn't stack.
- **Overnight cost**: bound by #27 (fleet budget) — an unattended nightly drain is
  exactly where a runaway hurts most.
- Depends on #1's infrastructure; low marginal cost once triggers exist.

---

### 10. Merge-to-main / "land" step

**What it does.** A workflow step (or a `dismiss` that names a non-active agent's
branch) that merges a *named* branch into a target, so a reviewer's APPROVE verdict
can land the reviewed work directly to main instead of only *completing* the
instance and leaving the merge to a later manual dismissal.

**Why it's high leverage.** It closes the last manual hop in autonomous review. As
documented in high-leverage-workflows.md §5 ("Merge semantics note"), a reviewer
chained off the work branch dismisses into its *base*, not main — so today APPROVE
routes *completion-vs-failure*, and a human still lands the branch. A `land` step
makes review→rework→**merge-to-main** a fully unattended loop, which is what makes
#2 (steward) able to actually ship, not just stage.

**Implementation sketch (grounded).**
- The capability already exists at the git layer: `Repo::merge_branch(branch,
  target)` (`git/lib.rs:118`) is CAS-safe and disturbs no live checkout. Expose it
  as a step/RPC that names `{branch, target}` rather than being implicit in
  `dismiss` over `active_agent` (`supervisor.rs:683`).
- Add a `Land(LandStep)` variant (`lib.rs:54` + `schema.cue`) handled in
  `run_step` (`workflow_exec.rs:250`) calling the new supervisor method.

**Dependencies / risks.**
- **Unreviewed landing**: a misconfigured `land` merges to main without a gate —
  restrict via #19 (policy: which branches may land where) and/or require it be
  reached only through an APPROVE `when` branch or an approval gate.
- Interacts with #11 (merge queue) if many workflows land concurrently — the CAS
  fails-safe, but a queue avoids wasted re-work.

---

## Summary ranking

| # | Feature | Dimension | Primary leverage |
|---|---------|-----------|------------------|
| 1 | Reactive trigger engine | Autonomy | Substrate: pull → self-driving |
| 2 | Steward: autonomous triage & merge | Autonomy | Removes the per-task merge decision |
| 3 | Continuous WIP dispatcher | Scaling | Backlog → self-refilling parallel work |
| 4 | `rk inbox` attention queue | Observability | Collapses 5 poll surfaces into 1 |
| 5 | Burn-rate & stuck detection | Safety | Unlocks *trusting* unattended runs |
| 6 | Pre-merge `run` verification step | Quality | Teeth that make auto-merge safe |
| 7 | `rk log` transcript | Observability | Visibility → trust → delegation |
| 8 | Model/harness cost tiering | Cost | Same backlog, fraction of the spend |
| 9 | Scheduled / cron workflows | Autonomy | Fleet self-improves on a cadence |
| 10 | Merge-to-main "land" step | Quality/Autonomy | Closes the review→ship loop |

**Critical path.** #1 (triggers) is the keystone — #2, #3, #9 all ride on it, and
#5/#6/#27 are the guardrails that make #2/#3 safe to leave running. If only three
features were built, build **#1 + #5 + #6**: a reactive substrate plus the two
guardrails that make reactive autonomy trustworthy. The steward (#2) and dispatcher
(#3) then follow almost for free from primitives that already exist.

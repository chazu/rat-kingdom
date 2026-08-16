# Proposal: daemon-native landing pipeline (Phase 3)

**Status:** design proposal — no code changed
**Ticket:** TKT-01M036PSEHTMD3S5D2JFAG7XVY (Phase 3 of the steward remediation)
**Depends on:** Phase 1 admission control (TKT-01M036NWE1EW5B1PWSHK0MKX8E, done) and Phase 2
commit-keyed verdict cache (TKT-01M036NWEG0H019BJ16G59RZVP, done)

---

## 0. Problem restated

The 2026-08-15 steward investigation (`memory/steward-investigation`) found a 35% steward
machinery failure rate, all mechanical, zero judgment failures, with one load-bearing root
cause: **a workflow `run` step can only execute inside an already-spawned agent's worktree**
(`crates/rk-daemon/src/workflow_exec.rs:2559-2582` — `ctx.active_agent` is set only by `spawn`,
`workflow_exec.rs:2219-2221`). Every completion therefore pays for a full agent spawn — a
brand-new `git worktree add -b`, harness launch, registry row — just to get *somewhere* to run
three deterministic checks. Phase 0 (tiering) and Phase 2 (the verdict cache) already cut the
number of times that spawn needs an LLM behind it, but even a cache **hit** or a **doc-only**
diff still spawns a "gate-holder" agent whose entire job is to host gates
(`examples/workflows/steward.cue:34-48`, `:563-566`). The gate itself — and the merge
serialization after it — are daemon primitives (`MergeQueue`, named-check execution) invoked
through the longest possible path: CUE workflow → spawn → wait → run.

This document proposes removing that coupling: gates and the merge decision move into a
daemon-native `LandingPipeline` component that runs checks in a **persistent, daemon-owned
worktree** instead of a throwaway agent one, consults the Phase 2 verdict cache **directly**
(no CUE read step), and only ever spawns an agent for the one thing that genuinely needs an
LLM — the review judgment itself, when nothing is cached and the diff isn't trivial. `steward.cue`
shrinks to exactly that one job.

---

## 1. Building blocks already in the codebase

### 1.1 `MergeQueue` + the CAS merge primitive (reuse as-is)

`MergeQueue` (`crates/rk-daemon/src/supervisor.rs:520-523`) — note: the ticket's "supervisor.rs
~441-490" line range is stale in the current tree; the struct now lives at `supervisor.rs:506-553`.

```rust
#[derive(Default)]
struct MergeQueue {
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}
```

- One `tokio::sync::Mutex` per `(repo_root, target)` key (`MergeQueue::key`,
  `supervisor.rs:526-529`) — different target branches in the same repo merge concurrently;
  same-target merges serialize FIFO (`tokio::sync::Mutex` is FIFO by construction, doc comment
  `supervisor.rs:531-534`).
- Purely in-memory (`Supervisor.merge_queue: MergeQueue` field, `supervisor.rs:421`, rebuilt
  empty via `MergeQueue::default()` on every daemon start, `supervisor.rs:632`) — this is fine:
  there is no "pending merge request" state to lose, only a serialization lock, and a
  process-death mid-merge just leaves the branch unmerged (discoverable, see §1.1's restart note
  below).
- The actual compare-and-swap is one layer down, in git itself:
  `Repo::advance_target` (`crates/rk-git/src/lib.rs:582-595`) runs
  `git update-ref refs/heads/<target> <merged> <expected>` — only moves the ref if it still
  equals the sha captured before the merge began. `MergeQueue` prevents two daemon-issued merges
  from racing each other at all; the CAS is a second, git-native safety net for any external
  movement of `target` (e.g. an operator's own `git commit`).
- **Public entrypoint a new caller should use directly:**
  `Supervisor::land(&self, repo_root: &Path, branch: &str, target: &str, keep_branch: bool) -> rk_core::Result<serde_json::Value>`
  (`supervisor.rs:3108-3162`, async). It delegates to the private `deliver_branch`
  (`supervisor.rs:2710-2770`), which does `self.merge_queue.acquire(repo.root(), &target).await`
  (`:2731`) then `repo.merge_branch(&branch, &target)` (`:2736`). **This is the exact function
  the landing pipeline should call on an APPROVE/gates-passed decision** — it already handles
  queueing, CAS, branch deletion, and event emission; nothing new is needed here.
- A merge conflict or a target that moved concurrently is a clean
  `MergeOutcome { merged: false, detail }` (`crates/rk-git/src/lib.rs:428-433`), never an `Err` —
  the caller gates on the returned JSON's `merged`/`delivered` fields exactly as `steward.cue`
  does today (`evaluate {expect: {delivered: true}}`, `examples/workflows/steward.cue:424`).
- Outcomes are restart-durable even though the lock isn't: every `land` emits a `branch_landed`
  event tuple (`supervisor.rs:3135`) into the durable, disk-backed tuplespace
  (`rk_space::Space::open`, not `open_in_memory`), and `inbox.rs:722-739`
  (`dropped_lands`) reads these back after restart to surface any land that neither merged nor
  opened a PR. The landing pipeline's own restart-safety (§3.5) piggybacks on this same event.

### 1.2 Phase 1 admission control — the queue pattern to model `LandingQueue` on

Two independent primitives, both worth reusing verbatim rather than reinventing:

**Durable FIFO queue of deferred work**, entirely made of tuples, no bespoke struct:
- Write side: `Reactor::enqueue_fire` (`crates/rk-daemon/src/reactor.rs:931-960`) writes one
  `Tuple` — `category: Event`, `scope: SYSTEM_SCOPE`, `identity: "reactor_queued_fire"`
  (`QUEUE_IDENTITY`, `reactor.rs:65-69`), `lifecycle: Furniture` — carrying everything dispatch
  needs in its payload (`key`, `trigger`, `run`, `repo_name`, `repo_path`, `params`, `seq`).
  `Furniture` lifecycle means "daemon-only, permanent, nothing TTLs it out from under a
  slow-draining trigger" (comment, `reactor.rs:926-930`).
- Ordering: a durable monotonic sequence number, **not** tuple id (same-millisecond tuple ids
  aren't ordering-safe) — `Reactor::next_queue_seq` (`reactor.rs:1985-1998`) persists a counter
  to `<home>/reactor-queue-seq` (`reactor.rs:165`).
- Read/dequeue: `Reactor::drain_queued_fires` (`reactor.rs:968-1028`) scans
  `Pattern::category(Event).scope(SYSTEM_SCOPE).identity(QUEUE_IDENTITY)`, filters to one
  trigger, sorts by `(seq, id)`, pops oldest-first while capacity remains.
- Consumption is **pure polling**, every reactor cycle (`reactor.rs:269-273`) — deliberately not
  event-driven, because "an instance completing frees a slot without necessarily writing a tuple
  this trigger's own pattern matches" (comment at that call site).
- Restart-safety: nothing is held in process memory between cycles — every drain re-scans the
  durable space, and the seq counter file resumes above the highest value ever assigned
  (`reactor.rs:1985-1998`).
- Non-retryable-failure handling (also worth reusing): `Reactor::give_up_or_retry`
  (`reactor.rs:1127-1159`) + a durable per-`(trigger, tuple)` attempt counter, capped at
  `MAX_FIRE_ATTEMPTS = 5` (`reactor.rs:75-80`); on exhaustion it writes an `Obstacle` tuple
  (`reactor_fire_gave_up`) and advances past rather than pinning forever.

**`LandingQueue` should copy this shape exactly**: a `Furniture` tuple per queued landing
candidate, under a new reserved identity (e.g. `landing_queue_entry`), ordered by its own
persisted seq counter, drained by polling — not a new in-memory `VecDeque` and not a new
restart-recovery mechanism. This gets restart-safety for free (§3.5) instead of designing it
from scratch.

**Fleet-wide WIP admission** (reusable if the pipeline ever needs to spawn agents under the same
ceiling as drain/workflow spawns): `Registry::try_reserve_wip(cap)`
(`crates/rk-daemon/src/agents.rs:337-346`) is the single atomic check-and-reserve both `drain.rs`
and workflow fan-out already share, called through `Supervisor::spawn`/`spawn_async`
(`supervisor.rs:687-766`). Any reviewer spawn the landing pipeline still issues (§2.3) should go
through this exact same `spawn_async` call, not a new spawn path — that is what keeps the WIP
ceiling bidirectional.

### 1.3 Phase 2 commit-keyed verdict cache — call the primitive directly, skip the CUE step

The cache key is **(repo/scope, branch, head_sha)** — confirmed load-bearing by the Phase 2
rework (a sha-only key let two branches sharing a tip commit exchange verdicts).

- `Pattern::for_commit(category, identity, branch, sha) -> Pattern`
  (`crates/rk-core/src/tuple.rs:408-417`) builds the two-substring payload match
  (`"head_sha":"<sha>"` AND `"branch":"<branch>"`); a caller adds `.scope(repo)` to bind the repo.
  `Pattern::matches` (`tuple.rs:421-461`) is the single predicate both storage queries and
  waiter wake-ups use.
- The reviewer's verdict artifact: `category: artifact`, `scope: <repo>`, `identity: "review"`,
  payload `{task, recommendation, notes, head_sha, branch}`, written by the reviewer itself via
  `rk out artifact <repo> review --payload '{...}'` (`examples/workflows/steward.cue:365`).
  `rk out` auto-stamps the writer's agent name into the payload if absent
  (`crates/rk-cli/src/space_cmds.rs:302-308`).
- **There is no dedicated Rust wrapper function today** — but the primitive it would wrap is
  already fully general-purpose and callable outside the workflow engine:
  `Space::scan(&pattern) -> Result<Vec<Tuple>>` (immediate) and
  `Space::rd(&pattern, timeout) -> Result<Option<Tuple>>` (bounded blocking wait)
  (`crates/rk-space/src/lib.rs:272`, `:344`). `Space` is `Clone` and already shared with
  `Supervisor`, `Reactor`, and `Tickets` independent of the workflow engine
  (`supervisor.rs:393`, `reactor.rs:123`, `tickets.rs:94`). **The landing pipeline should call
  `Pattern::for_commit(Category::Artifact, "review", branch, sha).scope(repo)` +
  `space.scan(...)` directly** — this is the whole point of "daemon-native": it is a Rust
  function call, not a workflow `read` step, and skips CUE entirely for this lookup. One thing
  the pipeline must replicate itself: `workflow_exec.rs`'s guard that `forCommit` requires a
  non-empty `forBranch` (`workflow_exec.rs:1616-1632`) — that invariant lives only in the CUE
  engine's `ReadStep` handling today, not in `Pattern::for_commit` itself.
- A cache hit (any of APPROVE/REWORK/STOP) is honored identically to a fresh verdict — no
  re-review to shop for a better opinion (`examples/workflows/steward.cue:71-72`, proven by
  `crates/rk-daemon/tests/workflow_verdict_cache.rs:382-433`). The landing pipeline's routing
  (§2.4) must preserve this: a cached REWORK/STOP takes the same path a fresh one would.

### 1.4 Named-check / gate execution — reusable logic, one missing primitive

The `run` step's binding to `ctx.active_agent` is not incidental plumbing; it is the actual gap
this proposal closes. Concretely, `WorkflowExecutor::run_command`
(`crates/rk-daemon/src/workflow_exec.rs:2559-2582`) requires:
1. `ctx.active_agent` (set only by a `spawn` step, `workflow_exec.rs:2219-2221`, `:1424`), then
2. `self.supervisor.status(&agent).worktree` (`AgentRecord.worktree: Option<PathBuf>`,
   `crates/rk-daemon/src/agents.rs:64`) — a path that exists **only** because
   `Supervisor::spawn` called `repo.create_worktree(&worktree, &branch, &target_branch)`
   (`supervisor.rs:821`), which is a **brand-new `git worktree add -b <branch>`**
   (`crates/rk-git/src/lib.rs:251-278`) — always a new branch, refuses protected branches.

**There is no existing "daemon-owned, persistent, reusable checkout" primitive anywhere in
`rk-git`.** The two closest things are both wrong for this purpose:
- `Repo::create_worktree` — mints a *new branch* every call; can't check out `main`/protected
  branches; 1:1 with an agent spawn.
- `Repo::advance_via_worktree` (private, `crates/rk-git/src/lib.rs:383-441`, used by
  `merge_branch`/`revert_merge`) — a **detached** temp worktree at a pid+seq-numbered throwaway
  path, created and `git worktree remove --force`d within the same function call. Exactly the
  right *kind* of worktree (detached, no branch-checkout conflict with the agent's own worktree
  of the same branch) but explicitly non-persistent.

**This is the one genuinely new primitive this proposal needs**: a detached worktree, like
`advance_via_worktree`'s, but created **once** and reused across many gate runs by repeatedly
`git checkout --detach <sha>` (or `git reset --hard`) inside it, rather than being torn down
after each call. Detached avoids git's "a branch can't be checked out in two worktrees at once"
restriction, so the gate worktree can check out a candidate branch's tip while that branch is
*also* still checked out (non-detached) in the completed rat's own worktree, if that worktree
hasn't been dismissed yet.

Everything **downstream** of getting a working directory is reusable as-is, and should be
factored out from `ctx.active_agent`-specific code rather than reimplemented:
- Named-check parsing/resolution: `Check` type (`crates/rk-workflow/src/lib.rs:748-772`),
  `load_checks`/`load_checks_str` (`lib.rs:775-788`), `WorkflowExecutor::find_check`
  (`workflow_exec.rs:2990-3007`), `resolve_run` (merges step overrides onto a named check,
  `workflow_exec.rs:2922-2985`).
- Subprocess execution: `spawn_check_child` (`workflow_exec.rs:2750-2825`) — `sh -c`, 
  `process_group(0)` + `ProcessGroupGuard` group-kill on timeout (`workflow_exec.rs:462-481`),
  `RK_CHECK_*` env allowlist (`valid_check_env_name`, `:3774`), daemon-PATH pinning for child
  `rk` calls (`check_child_path`, `:3787`).
- Output handling: 256 KiB truncate-not-kill cap (`MAX_RUN_OUTPUT_BYTES`, `workflow_exec.rs:44`,
  `read_capped` at `:424-446`), `retryOnFail` with backoff (`MAX_RETRY_ON_FAIL=20`,
  `workflow_exec.rs:150`, retry loop `:2601-2690`), three-way pass/fail/timeout verdict
  (`:2645-2666`).
- Durable gate-failure artifact: `record_gate_failure` (`workflow_exec.rs:2836-2873`) writes
  `category: Artifact, scope: <repo>, identity: "gate-failure", instance: "daemon"` with
  `{instance, agent, command, exit, verdict, stdout_tail, stderr_tail, failing_tests, retries}`
  — already `instance: "daemon"`-shaped, i.e. already written as if from daemon code, not an
  agent. Reuse verbatim.
- `steward-protected-paths` / `steward-diff-scope` are **fully generic** named checks in
  `.rk/checks.cue` — plain shell (`.rk/checks.cue:16-25`) parameterized only via `RK_CHECK_*`
  env. No Rust code is steward-specific; the landing pipeline supplies the same env vars a
  workflow `run` step does today.

**Concretely, this proposal needs one new function on `Repo`** (name illustrative, decided at
implementation time):
```rust
impl Repo {
    /// Idempotent: creates a detached daemon-owned worktree at `path` if absent.
    fn ensure_gate_worktree(&self, path: &Path) -> Result<()>;
    /// Resets an existing gate worktree to `sha`'s tree, discarding prior state.
    fn reset_gate_worktree(&self, path: &Path, sha: &str) -> Result<()>;
}
```
and one refactor: extract the "run a resolved check in `dir`" body of `run_command`/
`spawn_check_child` into a function taking a `&Path` directly, instead of resolving that path
through `ctx.active_agent`.

### 1.5 Direct daemon-side Tickets/Space calls — no shell, no RPC, no agent auth

Both halves of "escalation and rework routing become direct daemon calls" already have a
first-class, unauthenticated (because non-RPC) Rust entrypoint, and the pattern is already in
production use by `Reactor` and `Supervisor` — not hypothetical:

- **Tickets**: `Tickets::create`/`create_idempotent` (`crates/rk-daemon/src/tickets.rs:119,128`)
  are plain async methods on `Arc<Tickets>`. `Reactor::file_coalesced_ticket` already calls
  `tickets.create(new).await` directly (`reactor.rs:677`), specifically so ticket-id allocation
  stays serialized through one path (comment, `reactor.rs:671-674`). The landing pipeline's
  REWORK routing should call this directly instead of shelling out `rk ticket new` the way
  `steward-file-rework-ticket` does today.
- **Space writes**: `Space::out(tuple) -> Result<()>` (`crates/rk-space/src/lib.rs:227`) has
  **no auth or category check at all** — the "agents cannot write furniture/convention/task/
  available tuples" restriction lives *only* inside the RPC handler `handle_out`
  (`crates/rk-daemon/src/server.rs:4833-4880`), gated on `req.caller` (`is_agent = caller !=
  "operator" && !caller.is_empty()`, `:4838-4839`). A daemon-internal caller invoking
  `space.out(...)` directly never passes through `handle_out` and has no `req.caller` at all —
  confirmed empirically: `Reactor::promote_conventions` already writes `Category::Convention`
  tuples with `Lifecycle::Furniture` directly (`reactor.rs:465`), which is exactly the category
  an agent RPC caller is forbidden from writing. **This resolves ticket item 3 outright**: the
  landing pipeline's STOP-escalation `need` tuple and its own `landing_queue_entry`/gate-failure
  tuples are ordinary direct `space.out` calls, with no shell, no `PATH`, and no agent auth token
  involved — the same mechanism `Reactor` already relies on.

  Note: this is a *different* thing from the fleet-wide convention
  (`hand-off-through-artifact-and-ticket-not-fact`) that forbids **agent** rats from writing
  `fact`/`convention`/`task` tuples — that convention is about RPC-authenticated agent callers.
  A daemon-internal component (the reactor, and the proposed landing pipeline) was never subject
  to that restriction; it's a distinct code path, not an exemption from the same one.

- **`harness_result` payload** (what a completion listener has to work with) is built in
  `Supervisor::route_completion` (`supervisor.rs:2556-2605`):
  `{agent, role, task, branch, target, parent, is_error, head_sha, diff_files, diff_lines,
  diff_class, declared_done, result, cost_usd, tokens}`. `head_sha`/`diff_files`/`diff_lines`/
  `diff_class` are computed by `diff_summary`/`classify_diff` (`supervisor.rs:2509-2547`,
  `:75-85`) **before** the registry state flips to a terminal state — a documented,
  test-verified ordering invariant (`supervisor.rs:2534-2540`, regression coverage
  `crates/rk-daemon/tests/agent_lifecycle.rs:133-136`) — so the payload is trustworthy the
  instant a completion listener sees it; no re-derivation needed.
- **`declared_done`** (`supervisor.rs:2454-2463`) distinguishes "the rat itself wrote `rk done`
  for this generation" from "merely stopped" (budget kill, sweep, mid-task exit) — the landing
  pipeline should require `declared_done: true` (in addition to `is_error: false`) before
  enqueuing a candidate, the same fail-closed posture `_reviewArm`'s
  `evaluate {expect: {is_error: false}}` has today, tightened to the stronger signal now
  available (`memory` note: harness-result-declared-done).
- **Park-and-resume precedent for "wait on the review verdict tuple, not the workflow
  instance"** (ticket item 4's exact phrasing): `Supervisor::watch_attached_completion`
  (`supervisor.rs:1076-1099`) already does precisely this in pure daemon Rust, no workflow
  engine involved — it spawns a task that calls
  `space.rd(&pattern, Duration::from_secs(24*3600)).await` on a `task_done` pattern and routes
  on the result once it resolves. The landing pipeline's "park the candidate until the verdict
  tuple appears" step should be built the same way: an `.await` on `Space::rd` bound to
  `Pattern::for_commit(..., branch, head_sha)`, not a subscription to the reviewer's workflow
  instance's own completion state.

### 1.6 Current `steward.cue` shape (what shrinks)

`examples/workflows/steward.cue` (615 lines; the `docs/proposals/steward.cue` copy is stale —
it predates review tiering and the verdict cache entirely, so treat the `examples/` copy as
ground truth) is a single mega-workflow with three CUE-load-time-selected arms
(`_reducedArm`, `_cachedReviewArm`, `_reviewArm`, selected at `:610-614`), all funneling through
shared `_gates` (`:203-339`) and `_routeVerdict` (`:407-481`). All three arms spawn an agent
*purely to host gates* — `_reducedArm` and the hit-path of `_cachedReviewArm` spawn a
`gateholder` whose task is "run `rk done` immediately" (`:532-551`, `:570-587`) for no reason
other than that `run` needs a worktree. Post-Phase-3, none of `_gates`, `_routeVerdict`, the
reduced tier, or the cached-tier gate-holder spawn belong in CUE at all — they move into the
`LandingPipeline`. What's left of `steward.cue` is exactly `_reviewArm`'s first three steps:
spawn `reviewer` chained on the branch, wait, and record the verdict artifact. Nothing else.

---

## 2. Design

### 2.1 `LandingQueue`

A durable, per-`(repo, target)` FIFO of landing candidates, modeled directly on §1.2's Phase 1
trigger queue rather than invented fresh:

- One `Furniture`-lifecycle `Tuple` per candidate — `category: Event` (or a new reserved
  category if `Event` volume becomes a scan-cost concern; default to reusing `Event` since
  Phase 1's queue already does), `scope: <repo>`, `identity: "landing_queue_entry"`,
  payload `{branch, target, head_sha, diff_class, task, seq, enqueued_at}`.
- Ordered by a persisted monotonic `seq` counter, one file per repo (mirrors
  `<home>/reactor-queue-seq`), not tuple id.
- **Admission**: single-consumer per `(repo, target)` key by default (matches the ticket's
  "single-consumer (or bounded k)" and matches `MergeQueue`'s own per-key granularity — no point
  admitting k candidates for the same target when the merge step at the end still serializes on
  one `MergeQueue` lock anyway). A bounded-k variant is a config knob, not a structural change:
  the consumer loop below just runs k tasks pulling from the same per-key queue instead of one.
- **Consumer**: a daemon-native polling loop (own tokio task or folded into an existing cycle),
  draining oldest-first per key while under its concurrency cap — same shape as
  `drain_queued_fires`, not event-driven, for the same reason (a slot freeing doesn't reliably
  produce a tuple this exact scan would match).
- **Feed**: a completion listener watches `harness_result` tuples with `role: "rat"`,
  `is_error: false`, `declared_done: true` (§1.5) and enqueues. Two implementation options,
  flagged as an open question rather than decided here (§4):
  - (a) **Reuse the reactor's trigger/`maxInFlight` machinery**: register a trigger whose *action*
    is "enqueue onto `LandingQueue`" instead of "spawn a workflow instance." This inherits Phase
    1's dedup markers, rate cap, and restart-safety for free, at the cost of adding a new trigger
    action kind to `crates/rk-workflow`'s schema.
  - (b) **A bespoke daemon-native subscriber**, structured like `Reactor`/`Supervisor` (own
    `run_cycle`, own cursor), decoupled entirely from the CUE trigger system. More code, but
    keeps "daemon-native" literal — no CUE involved anywhere upstream of the reviewer spawn.

  Recommend (a) for the first cut: it is strictly less new code, and Phase 1's queue semantics
  (FIFO, restart-safe, rate-capped) are exactly what's wanted here too — the dedup marker keyed
  on `(trigger, tuple)` also solves "don't enqueue the same completion twice" for free.

### 2.2 Persistent per-repo gate worktree

Per §1.4: one detached, daemon-owned worktree per `(repo, target)` (matching `LandingQueue`'s
key granularity, and `MergeQueue`'s), at a fixed path under
`<home>/gate-worktrees/<repo>/<target>`, created once via a new `ensure_gate_worktree` and reset
per candidate via `reset_gate_worktree(path, branch_tip_sha)`. Because the queue is
single-consumer per key, the worktree is never touched concurrently — no new locking needed
beyond what `LandingQueue`'s per-key serialization already provides.

Gate execution against this path reuses §1.4's downstream machinery (named-check resolution,
`spawn_check_child`, retry/timeout/truncation, `record_gate_failure`) with the "resolve a
directory from `ctx.active_agent`" step replaced by "use the fixed gate-worktree path for this
`(repo, target)`." Concretely: `steward-protected-paths`, `steward-diff-scope`, and the repo's
named `verify` check run **exactly as they do today**, with the same `RK_CHECK_*` env
parameterization — nothing about the checks themselves changes, only where they run.

This is the structural win: for the reduced tier (doc-only/trivial diffs) and for a verdict-cache
hit, **zero agents are spawned** — the pipeline dequeues, resets the warm worktree, runs gates,
and (on pass) calls `Supervisor::land` directly. That is strictly fewer moving parts than
today's gate-holder spawn, and removes the entire class of gate-holder-worktree-leak/stuck-sweep
failure modes the investigation catalogued.

### 2.3 Review integration

On dequeue, after gates *would* run (see ordering note below), the pipeline decides whether an
LLM judgment is needed:

1. `diff_class` from the payload is `doc-only`/`trivial` → skip review entirely (no cache probe
   even attempted) — same semantics as today's reduced tier.
2. Otherwise, probe the cache **directly**: `Pattern::for_commit(Category::Artifact, "review",
   branch, head_sha).scope(repo)` + `space.scan(...)` (§1.3). A hit (any recommendation) is
   used without spawning a reviewer — never shop for a second opinion.
3. On a miss, request a review: spawn the shrunk `steward.cue` (§2.5) — or call
   `Supervisor::spawn_async` directly and skip the workflow engine for this too, another (a)/(b)
   choice like §2.1's, deferred to the same open-questions section — chained onto the candidate
   branch, then **park on the tuple, not the instance**: `space.rd(&pattern, review_timeout)`
   bound to that exact `(repo, branch, head_sha)` (§1.5's `watch_attached_completion` pattern).
   A timeout here is a clean STOP-equivalent hold, not an instance failure — mirroring the
   fail-closed posture `steward.cue`'s own `reviewTimeout` wait has today.

**Gate/review ordering**: keep gates-then-verdict, same as today (`_gates` always precedes
`_routeVerdict` in every arm of the current `steward.cue`) — a red suite should hold the branch
regardless of what any reviewer or cache says, and running gates first means a reviewer is never
spawned for a branch that was going to be held anyway. The one behavior to preserve exactly:
gates run on **every** landing attempt, cached verdict or not (§1.6's documented Phase 2
decision) — a suite green yesterday can be red today.

### 2.4 Routing (direct daemon calls, no shell)

Once gates pass and a verdict (fresh or cached) is in hand:

- **APPROVE** → `Supervisor::land(repo_root, branch, target, keep_branch)` (§1.1) directly.
- **REWORK** → `Tickets::create(...)` (§1.5) directly, hold the branch (do nothing further to
  it — no `dismiss` is even needed if no agent worktree was ever spawned for this candidate).
- **STOP** → `Space::out(Tuple::new(Category::Need, repo, "steward", "daemon", payload))` (§1.5)
  directly, hold the branch.
- **Gate failure / timeout** → `record_gate_failure` (§1.4, unchanged) +
  `Space::out` a `need` (mirrors `steward-report-gate-failure`/`steward-report-timeout`) directly.
- **Unrecognized verdict** → same STOP-shaped escalation, treated as a bug per today's
  `default` arm (`examples/workflows/steward.cue:465-478`).

None of these five outcomes require a shell subprocess, `rk` on `PATH`, or an agent auth token —
every one is a direct async Rust call from the `LandingPipeline` component into `Supervisor`,
`Tickets`, or `Space`.

### 2.5 `steward.cue` shrinks to review-only

The only workflow-shaped piece of work left that genuinely needs an agent is the LLM judgment
call. `steward.cue` (or its replacement, name TBD — `steward-review.cue`) becomes:

```
spawn reviewer chained onto the candidate branch
wait (reviewTimeout)
evaluate {is_error: false}
```

— i.e. exactly the first three steps of today's `_reviewArm` (`examples/workflows/steward.cue:
343-371`), nothing else. It is invoked by the `LandingPipeline` on a cache miss (§2.3), not
fired by the reactor per completion — **the trigger fires the pipeline, not the mega-workflow**,
per the ticket's item 5. If §2.1 goes with option (a) (reuse the trigger engine), the
`steward-on-completion` trigger's target changes from `workflow: "steward"` to the
`LandingQueue`-enqueue action; the review-only workflow is instead invoked programmatically by
the pipeline itself when it decides a review is needed, not reactor-fired at all.

### 2.6 Restart-safety

Three independent pieces of state, each durable via a mechanism already used elsewhere in this
codebase (nothing new to invent):

- **The queue itself** — durable `Furniture` tuples + a persisted seq file, exactly like
  Phase 1's (§1.2). A restart simply resumes scanning.
- **In-flight candidate state** ("gates are running" / "parked awaiting review") — needs to be
  recorded so a restart doesn't silently drop or double-process a candidate mid-flight. Model
  this the same way the workflow engine already parks an approval gate: set a status field on
  the queue entry itself (`awaiting_review`, `running_gates`) rather than inventing a separate
  state store — a restart re-scans the queue, sees an entry marked `running_gates` with no
  corresponding live process, and simply restarts that candidate's gate run (gates are
  idempotent — a warm-worktree checkout + shell command has no side effect that isn't safe to
  redo). A candidate marked `awaiting_review` on restart just re-issues the same `space.rd`
  wait — if the reviewer already wrote its verdict while the daemon was down, the durable tuple
  is still there and the wait resolves immediately.
- **Never double-land**: `Supervisor::land`'s CAS (§1.1, `advance_target`'s `update-ref
  <new> <old>`) already makes a duplicate `land` call on an already-merged branch a no-op clean
  failure (the target moved, so the CAS loses) — this is "free" idempotency, not something the
  pipeline needs to build. The queue-entry-level idempotency needed on top of that is a
  `work_key` equal to `(repo, branch, head_sha)`, so a redelivered completion tuple (reactor
  redelivering after a crash, an operator manually re-triggering) that matches an already-fully
  processed entry is recognized and dropped rather than re-enqueued — same shape as the Phase 1
  trigger dedup marker keyed on `(trigger, tuple)`.
- **Never orphan a reviewer**: because the pipeline parks on the verdict *tuple* (§2.3), not
  the reviewer's *workflow instance*, a daemon restart doesn't lose track of an in-flight
  reviewer the way an in-memory-only wait would — the reviewer keeps working, writes its verdict
  tuple whenever it finishes, and the restarted pipeline's `space.rd` on the same durable pattern
  picks it up. This is precisely the failure mode the Phase 2 investigation's "motivating
  specimen" (Templeton-7, `memory/steward-investigation`) hit under the *old* wait-on-instance
  design — the new design structurally can't reproduce it.

---

## 3. Interfaces between the staged tickets

Named concretely so each ticket below can be implemented and tested independently:

- **T1 → T2**: `Repo::ensure_gate_worktree(path: &Path) -> Result<()>` and
  `Repo::reset_gate_worktree(path: &Path, sha: &str) -> Result<()>` on `rk-git`'s `Repo`; and a
  `run_check_in(dir: &Path, resolved: &ResolvedCheck) -> Result<CheckOutcome>` function extracted
  from `workflow_exec.rs`'s `resolve_run`/`spawn_check_child`/`record_gate_failure`, taking a
  directory directly instead of resolving one through `ctx.active_agent`.
- **T2 → T3**: a `LandingQueueEntry { repo, branch, target, head_sha, diff_class, task, status }`
  type and the point in `LandingPipeline`'s consumer loop where "gates passed" hands off to "now
  decide APPROVE/REWORK/STOP" — T3 plugs review-integration and routing in at that hand-off
  without touching T2's queue/gate-runner internals.
- **T3 → T4**: `LandingPipeline::enqueue(completion: &HarnessResultPayload)` (or equivalent) as
  the one function T4's trigger-action-or-subscriber calls to hand off a completion — T4 owns
  *only* getting a completion into that function and the operator-facing cutover, not anything
  downstream of it.

## 4. Staged ticket decomposition

**T1 — Persistent warm gate worktree + check-runner extraction.**
New `Repo::ensure_gate_worktree`/`reset_gate_worktree` (detached checkout at a fixed daemon-owned
path, modeled on `advance_via_worktree` but non-ephemeral). Extract `run_check_in` from
`workflow_exec.rs` so named-check execution no longer requires `ctx.active_agent`. Unit tests:
worktree creation is idempotent, reset discards prior state cleanly, a check runs identically
via the new path vs. the old agent-worktree path (same `RK_CHECK_*` env, same retry/timeout/
truncation/`record_gate_failure` behavior). *Depends on nothing.*

**T2 — `LandingQueue` + daemon-native gate runner.**
New `LandingPipeline` component (durable per-`(repo,target)` queue modeled on §1.2, single
consumer per key by default). Dequeues a candidate, runs gates via T1's warm worktree, and for
the no-review-needed path (doc-only/trivial `diff_class`) routes straight to `Supervisor::land`
on a pass. Integration tests: queue ordering (FIFO within a key, independent across keys), a
doc-only completion lands with zero agent spawns, a failing gate produces a `gate-failure`
artifact and holds the branch. *Depends on T1.*

**T3 — Review integration (verdict cache) + direct-call routing + shrunk `steward.cue`.**
Wire `LandingPipeline` to probe `Pattern::for_commit` directly before requesting a review; on
miss, invoke the shrunk review-only workflow (§2.5) and park on `Space::rd` for the verdict
tuple; route APPROVE/REWORK/STOP via direct `Supervisor::land`/`Tickets::create`/`Space::out`
calls. Ship the shrunk `steward.cue`/`steward-review.cue`. Integration tests: cache hit skips
spawn entirely, cache miss spawns exactly one reviewer, a cached REWORK routes to a ticket
without spawning, park-and-resume survives the reviewer finishing after a simulated daemon
restart (space-level, not process-level, restart in test). *Depends on T2.*

**T4 — Completion feed, restart-safety proof, and cutover.**
Decide and implement the `LandingQueue` feed mechanism (§2.1's option (a) vs (b) — recommend
(a): a new trigger action kind reusing Phase 1's `maxInFlight`/dedup/queue machinery). Implement
the `work_key = (repo, branch, head_sha)` dedup guarding double-land on a redelivered
completion. Full end-to-end integration tests: a burst of completions queues instead of
thundering; a daemon restart mid-gate-run resumes correctly; a daemon restart mid-review-wait
still picks up the verdict once written; escalation (`need`/rework ticket) surfaces in `rk
inbox` identically to today. Operator cutover runbook: disable the `steward-on-completion`
trigger's workflow-spawn behavior (or retire it entirely if (a) was chosen), enable the landing
pipeline, verify `rk workflow drift`-equivalent parity. *Depends on T2, T3.*

**T4 rework — crash-safe transitions, admission proofs, and the cross-key concurrency
contract.** A review of the first T4 landing caught three gaps, closed as follows:

- `LandingQueue::claim_next`/`set_status` used delete-then-write for their durable status
  transition, so a daemon crash landing in that gap lost the queue entry outright. Flipped to
  write-then-delete: the successor tuple is written durably before the predecessor is deleted,
  so a crash in the gap leaves two tuples sharing one `seq` instead of zero. A new `rev` counter
  on `LandingQueueEntry`, bumped on every transition, lets `LandingQueue::scan_current` tell the
  fresh successor from the stale predecessor and self-heal the duplicate on the next read rather
  than exposing (or losing) the entry. Regression:
  `crash_between_write_and_delete_survives_the_entry` drives the write and delete halves
  separately and asserts the entry survives with no orphan left behind.
- Two proofs promised above were missing. `burst_of_completions_on_one_key_never_runs_gates_
  concurrently` enqueues several candidates onto the same `(repo, target)` key before draining
  starts and proves (via a marker file a concurrent run would trip) that they still gate-run one
  at a time. `escalation_row_matches_the_workflow_driven_steward_shape` proves
  `LandingPipeline::escalate`'s direct `Space::out` write produces the identical `rk inbox` row
  `inbox::build` renders from the historical workflow-driven `steward-report-stop`/`-gate-
  failure`/`-timeout`/`-unknown-verdict` `rk out need` shape.
- **Cross-key concurrency contract.** `run_cycle` drained every `(repo, target)` key in one
  sequential `for` loop, awaiting each key's full `drain_key` before starting the next. That
  contradicted both this doc's own §1.1 (`MergeQueue`'s "different target branches in the same
  repo merge concurrently") and `run_cycle`'s own doc comment, which already claimed "nothing
  here serializes two DIFFERENT keys against each other" — and it was a real correctness gap, not
  just a stale comment: a slow `verify` run on one key (up to `GateConfig::gate_timeout`, 60
  minutes by default) silently stalled every other repo's/target's landing traffic for the rest
  of the cycle. Fixed by implementing the promised behavior rather than narrowing the doc to
  match the bug (the smaller change, since it required no new dependency — `tokio::task::JoinSet`
  was enough): `run_cycle` now spawns one task per pending key via `Arc<LandingPipeline>` and
  drains them concurrently, fanning out unboundedly across keys (each key is already a small,
  naturally-bounded admission unit — there is one only if something is genuinely queued for it).
  WITHIN a key, admission is unchanged: `drain_key` still claims and finishes one candidate at a
  time, so a burst on one key still gate-runs serially even though many keys now run side by
  side (§2.1, §5 open question 3 — still open, and orthogonal to this fix). This changed
  `run_cycle`'s receiver from `&self` to `self: &Arc<Self>`; the one live call site
  (`server.rs`'s landing loop) already held an `Arc<LandingPipeline>`, so no behavior changed
  there. Proof: `distinct_keys_drain_concurrently_within_one_run_cycle` enqueues candidates on
  two different targets with a `verify` check that blocks on a shared release flag, and asserts
  both are observed genuinely in flight at once before either is released — verified to fail
  (timeout) against the prior serial implementation before being confirmed to pass against the
  fix, so it is a real discriminator and not a tautology.

Natural sequencing: **T1 → T2 → T3 → T4**, each independently landable and testable; T1 has no
dependency on anything else in this program and could start immediately.

---

## 5. Open questions for the operator

1. **Completion feed mechanism (§2.1)**: reuse the reactor/trigger engine (less new code,
   inherits Phase 1's restart-safety and rate cap) vs. a bespoke daemon-native subscriber
   (keeps "daemon-native" literal all the way up, more code). Recommend reusing the trigger
   engine unless there's a reason CUE-level trigger config is unwanted for this specific path.
2. **Review-request spawn mechanism (§2.3 step 3)**: keep the shrunk review-only workflow
   spawned through the existing workflow engine (`execute()`), or have `LandingPipeline` call
   `Supervisor::spawn_async` directly and manage the wait itself, skipping the workflow engine
   for the reviewer too? The latter is more "daemon-native" but means writing a new
   spawn-and-track path outside `workflow_exec.rs`; the former reuses proven spawn machinery at
   the cost of one CUE hop.
3. **Bounded-k vs strictly single-consumer** per `(repo, target)`: single-consumer is
   recommended above since the final merge step re-serializes on `MergeQueue` regardless, but if
   gate-run wall-clock (not merge) turns out to be the bottleneck under load, a small k (e.g. 2,
   matching the current `maxInFlight: 2` steward trigger default) may be worth it from day one
   instead of as a later tuning pass.
4. **Gate-worktree disk/lifetime management**: one persistent worktree per `(repo, target)`
   accumulates on disk indefinitely (unlike agent worktrees, which get cleaned up on dismiss).
   Needs a retention/pruning story if the repo count or target-branch count grows — out of scope
   for T1-T4 but should be filed as a follow-up rather than silently deferred.
5. **Reviewer worktree**: this proposal leaves the reviewer's own worktree exactly as it is
   today (an ordinary agent spawn via `create_worktree`) — it is cheap, short-lived, and
   read-mostly. A further optimization (give the reviewer `git show`/`git diff` output directly
   instead of a worktree at all) is possible but explicitly out of scope here; flag it as a
   possible Phase 4+ item rather than pulling it into this design.

---

## 6. Operator cutover runbook (T4)

**Status as implemented:** §2.1 option (a) was adopted. `Trigger` gained an `action` field
(`"workflow"` default, or `"land"` — `crates/rk-workflow/src/lib.rs`, schema in
`crates/rk-workflow/src/triggers-schema.cue`). An `action: "land"` trigger match is dispatched
by `Reactor::fire_land_action` (`crates/rk-daemon/src/reactor.rs`) straight into
`LandingPipeline::enqueue` (`crates/rk-daemon/src/landing.rs`) — no workflow instance, no CUE
hop — while still reusing the reactor's `(trigger, tuple)` dedup marker, `maxFires` rate cap, and
cursor-based restart-safety (NOT `maxInFlight`: that admission model is superseded by
`LandingQueue`'s own single-consumer-per-`(repo,target)` queue downstream). The shipped example
is `examples/triggers-landing-pipeline.cue` (`steward-landing-on-completion`), a like-for-like
match-predicate copy of `steward-on-completion` (`examples/triggers.cue`) with
`action: "land"` instead of `run: "steward"`.

`work_key = (repo, branch, head_sha)` dedup (§2.6): `LandingPipeline::enqueue` probes a durable
`landing_processed` marker (written by `LandingPipeline::process_entry` on every terminal
outcome) before writing a new queue tuple, dropping (`Ok(None)`) a redelivered completion for an
already-fully-processed candidate. Restart-safety for an IN-FLIGHT candidate is a queue-entry
status field (`queued` / `running_gates` / `awaiting_review`, `LandingEntryStatus`) that survives
in the durable tuple rather than being deleted at claim time; `LandingQueue::claim_next` treats
every status as eligible, so a daemon restart's next poll cycle re-discovers and reprocesses
anything a crashed prior process left mid-flight. A restart-driven re-request for a review in
flight resolves to the SAME workflow instance (`review_instance_id`, a stable id derived from the
work key) rather than spawning a second reviewer.

The cutover itself is **NOT automatic** — this section is a manual runbook. The operator performs
every step below; nothing in this codebase flips `steward-on-completion` off or the landing
pipeline on by itself.

### 6.1 Preconditions

- T1–T4 are on `main` and the daemon has been rebuilt and restarted (a merged Rust change is not
  live in a running fleet until `mise run deploy` or equivalent — same caveat as every other
  daemon-native landing change in this program's history, see `workflow-instance-archive` and
  `dropped-land-inbox-guard` in fleet memory).
- The target repo has `.rk/checks.cue` registering `steward-protected-paths`, `steward-diff-scope`,
  and its real `verify` check — identical to what the workflow-driven steward already requires.
  `examples/workflows/steward-review.cue` (the shrunk review-only workflow, T3) is installed
  wherever `steward.cue` is today.

### 6.2 Swap the trigger (per repo, or globally)

1. Locate the live trigger file installing `steward-on-completion` (global
   `~/.rat-kingdom/triggers/`, or a repo's `.rk/triggers.cue`).
2. Copy `examples/triggers-landing-pipeline.cue` alongside it under a NEW filename first (do not
   overwrite yet) — e.g. `steward-landing.cue`.
3. **Do not run both at once.** `steward-on-completion` and `steward-landing-on-completion` match
   the identical `harness_result` predicate; loading both trigger files into the same triggers
   directory double-dispatches every completion (one full workflow spawn, one `LandingQueue`
   enqueue), racing each other for the same branch. Remove or rename the old trigger file's
   `steward-on-completion` entry (or the whole file, if it defines nothing else) in the SAME
   change that adds the new one.
4. Restart the daemon (or wait for the trigger file's mtime-based reparse, `Reactor`'s
   `TriggerCache` — see `edited_trigger_file_is_reparsed_not_stale` for the mechanism) so the new
   trigger set is loaded.

### 6.3 Parity verification before trusting it unattended

There is no automated `rk workflow drift` command as of T4; verify parity by hand:

1. Land ONE low-risk doc-only/trivial change through the new trigger. Confirm via `rk scan event
   <repo>` (or daemon logs at `debug`) that: a `landing_queue_entry` tuple was written, then
   removed, and a `landing_processed` marker exists for that `(branch, head_sha)` — and that
   `git log` on the target shows the merge, with **zero** agent spawns
   (`rk scan event <repo>` for `agent_spawned` in the relevant window).
2. Land one change that needs a real review (non-trivial diff, no cache hit). Confirm exactly one
   `reviewer` spawn occurs, the branch lands on APPROVE, and `rk inbox` shows nothing new (a clean
   run is invisible, same as today).
3. Force a REWORK and a STOP verdict (or reuse existing fixtures) and confirm: REWORK files a
   ticket titled `rework: <task>` and leaves the branch unmerged; STOP writes a `need` tuple with
   identity `steward` and the branch stays unmerged — both should appear in `rk inbox` in the
   SAME shape a workflow-driven steward's escalation does today (this was a T4 design constraint,
   not just a happy accident: `LandingPipeline::escalate`/`file_rework_ticket` reuse the exact
   tuple shapes `steward-report-stop`/`steward-file-rework-ticket` wrote).
4. Force a failing gate (e.g. a branch that fails `verify`) and confirm a `gate-failure` artifact
   is recorded and the branch is held unmerged, matching `record_gate_failure`'s existing shape.
5. Only once all four are confirmed working on a scratch/low-traffic repo should the swap be
   repeated for higher-traffic repos.

### 6.4 Rollback

Reverse §6.2: restore the original `steward-on-completion` trigger file and remove/rename the
`action: "land"` one. Nothing about the swap is destructive to in-flight state — a candidate
already fully processed (landed/rework-filed/escalated) has no live queue entry to roll back;
one still mid-flight when the trigger set is swapped back simply stops being drained by the
landing pipeline's consumer loop (it stays in the durable queue, inert, until either the landing
trigger is restored or an operator manually inspects/clears it — there is no automatic
queue-to-workflow migration).

### 6.5 Known gaps (file as follow-up tickets, do not fix inline here)

- No automated drift/parity check (`rk workflow drift`-equivalent) exists yet — §6.3 is manual.
- `LandingQueue` has no attempt-counter backstop analogous to the reactor's `MAX_FIRE_ATTEMPTS`:
  a candidate whose processing keeps erroring (not gate-failing — an actual `Err`, e.g. a corrupt
  gate worktree) is retried every poll cycle indefinitely rather than escalating after N attempts.
- Gate-worktree disk/lifetime management (§5 item 4) is still open, unchanged by T4.

The operator executed this §6 cutover on 2026-08-16: the `steward-landing-on-completion` trigger
(`action: land`) replaced `steward-on-completion` in the global triggers directory, scope
`rat-kingdom`.

---

*Grounding note:* every file:line citation above was verified by direct agent reads against the
tree at this branch's base (see this ticket's dispatch for the parallel research pass); nothing
here was reconstructed from documentation or memory alone. Line numbers for `MergeQueue`
specifically differ from the ticket's own text (`supervisor.rs ~441-490`) — the current location
is `supervisor.rs:506-553`, called out explicitly in §1.1 so implementers don't go looking in
the wrong place.

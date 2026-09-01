# Changelog

All notable changes to rat-kingdom are documented here.

## [Unreleased] — 2026-07-23 · The Self-Driving Fleet

A single release that turns rat-kingdom from an **operator-pull** system (every
action is an `rk` command you type) into a **self-driving** one: a daemon reactor
watches the tuplespace and dispatches work, a steward reviews and merges
completed branches unattended, and an autoscaler keeps the backlog draining
itself — all behind guardrails that make leaving the fleet running safe.

Derived from two research reports (`docs/research/stigmergy.md`,
`docs/research/leverage-features.md`), both of which independently identified the
daemon **reactor** as the keystone. Most of the work below was delivered by the
fleet reviewing and merging *itself* through the steward loop (see _How this was
built_).

### Build, CI, and test hygiene (2026-09-01)

- **Committed `Cargo.lock`; CI builds `--locked`** — the workspace ships three
  binaries but never pinned its dependency graph, so every fresh checkout and
  CI runner re-resolved transitive crates on its own. The lockfile is now
  versioned, `tempfile`/`rusqlite` moved into `[workspace.dependencies]` so a
  version lives in one place, and CI fails a manifest edit that arrives
  without its lockfile change.
- **`main` CI green again** — every push since 2026-08-19 had failed on four
  tests: the conflict-correction restart test built a real `Daemon::new` with
  `Config::default()`'s `claude` harness (ENOENT on any runner without it);
  the implementation-lane tests occupied a lane with a fake spawn that exits
  almost immediately, freeing the slot before the next admission check on a
  loaded runner (now a durable synthetic occupant, matching the restart test);
  and two restart/sweep tests one-shot-read `agent.list`/`harness_result`
  right after `agent.status` flipped, ahead of the write that follows it (now
  polled). The ubuntu leg, advisory since it was added, blocks like macOS.
- **`rk_git::plumbing`** — one success-checked `git_output`/`git_text`/
  `git_ok`/`git_succeeds`/`git_with_stdin` for callers that need a raw git
  command in a directory, replacing the three private near-copies the
  onboarding apply/activation modules carried with different error text.

### Removed

- **`axe` harness adapter** — deleted (`rk-harness/src/axe.rs`, the `axe` arm of
  `make_harness`, and its config/CUE/CLI/README surface). A usage audit on
  2026-08-17 found zero lifetime `axe` spawns across 801 agents, and every
  adapter is maintenance on the critical path the moment its harness's protocol
  shifts. `claude`, `codex`, `jcode`, and `fake` remain; `--harness axe`,
  `harness = "axe"` in config, and `harness: "axe"` in a workflow's agent
  profile are now errors.

### System hardening and remediation (2026-07-26–27)

- **Operator-led onboarding prime** — `rk onboard` is exact sugar for
  `rk prime --role onboarding`, giving the main operator agent a guided,
  gate-first walkthrough without launching a special assessor or mutating
  repository state. The prime makes the repository-owned `verify` contract,
  `.rk/checks.cue`, workflow consumers, and explicit human decisions the
  onboarding priority.
- **Restart-safe guided onboarding** — repository assessment and content-bound
  proposals now stage named checks and repo-local automation only in an isolated
  onboarding branch. Workflow, trigger, and schedule validation stays inert
  until a separate human activation advances the unchanged registered checkout
  to the approved commit; durable intent, digest/tree checks, replay recovery,
  refusal, outcome summaries, and terminal worktree cleanup make the boundary
  at-least-once safe.
- **Authenticated daemon IPC** — require per-layout tokens, restrict agent tuple
  writes and event identities to the authenticated agent, protect the socket
  with mode `0600`, and keep sync provenance separate from local authorization.
- **Role-appropriate execution** — unattended workers receive explicit
  non-interactive Claude/Codex permissions so Git and Rat Kingdom coordination
  cannot stall on an approval nobody can answer; onboarding remains forcibly
  read-only/plan-mode. Workflow approval, named-check, target-allowlist, and
  definition digest policies remain enforced fail-closed.
- **Durable recovery** — workflow snapshots are atomic and corruption becomes a
  visible recovery failure; agent allocation is journaled before side effects;
  changed workflow definitions cannot be resumed silently.
- **Reliable coordination** — sync cycles are single-flight with durable cursor
  and presence updates, reactor dispatch failures retry instead of being
  acknowledged, and tuplespace waiter/replication races are closed.
- **Bounded service paths** — request frames, SQLite scans, ranked scans, inbox
  histories, TTLs, scheduler catch-up, and workflow command output are capped;
  blocking Git/filesystem work is isolated from Tokio workers.
- **Git and ticket safety** — checked-out targets cannot be advanced by a stale
  ref, Git refs and land targets are validated, ticket IDs are globally unique,
  and ticket state transitions are enforced at the daemon boundary.
- **Strict data semantics** — migrations and persisted-row decoding fail closed,
  payload matching is literal and case-sensitive, workflow commands stay inside
  their worktree, and stale numeric ticket guidance was removed from `rk prime`
  and examples.

### The reactive substrate (keystone)

- **Reactive trigger engine** — a daemon tuple-reactor that consumes the live
  feed and fires workflows on matching `#Trigger` defs (`~/.rat-kingdom/triggers/`,
  `<repo>/.rk/triggers.cue`). Cursor-driven for at-least-once delivery under the
  lossy feed, idempotent per `(trigger, tuple)`, with re-entrancy guards and a
  per-trigger rate cap. Turns tuple writes into zero-token, zero-latency dispatch.
- **Reactor performance** — trigger defs cached; per-cycle scan cost bounded.

### Autonomy — the fleet drives itself

- **Autonomous worker permissions** — global `[agents.default]` harness, model,
  and permission settings now apply consistently to direct, nested, workflow,
  and drain spawns. Effective permissions persist across respawn and appear in
  `rk status`; Claude bypasses permission prompts and Codex bypasses both
  approvals and the sandbox for ordinary unattended workers.

- **Steward** — on every rat completion the reactor fires a workflow that spawns
  a cheap reviewer on the branch, runs a protected-path **policy gate** and the
  repo's real **test gate** (both fail-closed), then routes on the verdict:
  `APPROVE` → land to main, `REWORK` → file a follow-up ticket + hold the branch,
  `STOP`/unknown → escalate a `need`. Re-entrancy is broken by match-scoping to
  `role:rat` completions. Removes the most-repeated operator decision — "is this
  branch good to merge?"
- **Continuous-drain** — a WIP-limited fleet autoscaler that keeps `max_wip` rats
  live, continuously claiming the highest-priority ready ticket (aged so
  low-priority work can't starve) whenever a slot frees. Off by default.
  - **Cross-repo WIP partitioning** — drain many registered repos with per-repo
    caps instead of one.
  - **Tier-aware dispatch** — route drained tickets to cost tiers by label/priority.
- **Scheduled / cron workflows** — a daemon scheduler fires `#Schedule` defs on a
  cadence, plus a **nightly-self-improve** chain (groom → drain → prompt-refine).

### Coordination & stigmergy

- **Suggestion → Endorsement → Convention quorum** — rats deposit `suggest`/
  `endorse` tuples; the reactor promotes a suggestion to a durable `Convention`
  at quorum. Conventions are **injected into every spawn prompt**, and a new
  convention **steers already-running rats**. Composed end-to-end with a
  self-test and demo script.
- **Obstacle coalescence** — repeated obstacles/needs on the same normalised
  topic are counted by distinct reporter and filed as one durable ticket at
  quorum, turning a flat obstacle pile into actionable backlog automatically.
- **Read-before-work claim trails** — rats now scan peers' `claim`/`artifact`
  trails before editing and drop an ephemeral area-claim on entry, so parallel
  rats self-organise around each other instead of colliding at merge.
- **Pheromone evaporation + reinforcement** — `claim`/`obstacle`/`need` carry a
  decaying strength refreshed by re-writing; abandoned trails fade via GC.
- **Ranked "hot" scans** — `rk scan <cat> <scope> --hot | --top N` ranks by
  category-weight × recency × strength, turning the space from a log into a
  navigable gradient.
- **Resolution backlinks** — `rk out artifact … --resolves <id>` retires the
  solved wall and lays a decaying trail; the next rat to hit that wall is steered
  to the prior fix instead of redoing the work.

### Quality & merge safety

- **`run` step** — execute the repo's real test/lint suite in the agent's
  worktree and fail-closed into `evaluate`/`when` — a verdict the harness can't
  forge. The teeth behind every automated merge.
- **`land` step** — merge a *named* branch into a target (CAS-safe), so an
  `APPROVE` verdict lands work directly on main instead of only completing.
- **Named-check allowlist** — restrict `run` steps to per-repo registered checks
  (`[policy] require_named_checks`) rather than arbitrary inline commands.
- **Fetch-driven awaiting-review clear** — an opt-in background review sweep
  (`[review_sweep]`) `git fetch --prune`es each repo with an open PR and clears
  the `rk inbox` awaiting-review row once the forge has merged or deleted the
  branch — even when you never pulled the merge locally. Emits a
  `pull_request_closed` event the inbox consults; the fetch is bounded by a hard
  timeout and stays off the hot inbox read path.

### Safety & guardrails

- **Burn-rate & stuck/liveness detection** — a supervisor sweep flags a rat that
  goes silent (stuck) or sustains high spend (running away) and responds
  graduated: obstacle → steer → kill-after-grace. Closes the gap where a rat
  hung or burning without emitting was previously invisible.
- **Hierarchical budgets** — fleet-wide and per-repo USD caps (the wallet
  kill-switch for unattended runs) above the existing per-agent caps, plus
  **per-workflow-instance** caps. `rk cost --fleet` rollup.

### Observability

- **`rk inbox`** — one ranked queue of everything awaiting a human (parked gates,
  obstacles, needs, failed/orphaned agents, failed instances), each row carrying
  the command that resolves it. Collapses five polling surfaces into one.
- **Prunable workflow instances** — a failed instance's inbox row was
  inspect-only, and nothing else retired one, so the board only ever grew (the
  clear was: stop the daemon, move the JSON aside). Settled instances now
  archive exactly as agent records do: `rk workflow prune <id>` clears one,
  `rk prune` sweeps both halves of the board on one window, and
  `rk workflow list --archived` / `rk workflow unarchive` keep the run readable
  and restorable. A running instance is never archived.
- **`rk log`** — a bounded per-agent transcript (assistant text, tool calls,
  retries) with `--follow`; the events the supervisor previously dropped.
- **Escalation push** — a steward escalation fires a desktop notification via
  herdr, so a branch that needs a human decision pings you instead of waiting to
  be noticed.
- **`rk top`** — live ratatui fleet dashboard: agents (state/task/cost),
  workflow instances (step cursor, where parked, per-instance spend), fleet
  budget, and the inbox, refreshed on an interval. Thin by design — per-agent
  detail stays with `rk log`/herdr.
- **`rk digest`** — "what happened while you were away": an interval catch-up
  grouped from the event feed (completions, dismissals, merges, PRs, workflow
  outcomes), live friction (obstacles/needs), spend, and the inbox.
  `--llm` pipes the report through a one-shot `claude -p` for prose, degrading
  to the deterministic report when the binary is absent.
- **`rk workflow timeline`** — an instance's step trace rendered from its
  definition: every step labelled and marked done/current/pending against the
  persisted cursor, with `when`/`repeat` bodies nested and the parked-gate
  status in the headline. Debug a stuck workflow without reading JSON.
- **`rk workflow watch`** — replay a durable workflow snapshot and its
  coordinator state transitions, follow live changes, reconnect after feed lag,
  and exit when the instance completes or fails. Journal cursors are SQLite
  sequences; coordinator events are protected furniture and bounded summaries.

### Cost

- **Model & harness cost-aware tiering** — route jobs to the cheapest capable
  harness/model (cheap for mechanical work, premium for hard implementation) by
  ticket label/priority, with escalation-on-failure.

### Fixes

- Deterministic `hot_scan` test (same-millisecond ULIDs have undefined order).

### Config

Two operator switches are now set in `~/.rat-kingdom/config.toml`:

- `[supervisor] burn_usd_per_min = 4.0` — burn-rate runaway detection on.
- `[drain] enabled = true, max_wip = 2` (scoped to `rat-kingdom`) with
  `[budget] fleet_max_usd = 50` and `max_usd = 20` as wallet/agent backstops —
  the autoscaler is armed and stays inert until ready tickets exist.

### How this was built

The reactor + steward loop delivered most of this release itself: a rat finishes
a ticket → the reactor fires the steward → a cheap reviewer runs the policy +
test gates → clean work auto-lands, real defects are routed to REWORK. The
steward caught genuine issues on its own (a flaky test, a refactor that would
have reverted another ticket, several stale-base build breaks) and never merged a
red branch. The operator's role was reduced to grooming the backlog, resolving a
handful of stale-base merge conflicts, and salvaging three post-completion token
runaways — the last of which is now guarded automatically by burn detection.

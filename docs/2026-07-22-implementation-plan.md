# rat-kingdom Implementation Plan

Companion to `2026-07-22-imp-analysis-and-rat-kingdom-design.md`. Phases are
tracer-bullet vertical slices: each ends with something runnable and useful on its
own, and each de-risks the next. Crate names refer to the workspace layout in §9 of
the design doc.

Ordering rationale: the tuplespace is the substrate everything writes to, so it goes
first and gets the concurrency treatment imp never had. One harness end-to-end beats
three harnesses half-wired, so Claude Code alone carries Phase 2. Cost tracking lands
before workflows because the ledger only needs harness events, while workflows need
everything. Multiplayer sync is deliberately late — it's the most novel work and
nothing else depends on it.

---

## Phase 0 — Skeleton & foundations

**Top-line features**
- Cargo workspace that builds, tests, lints, and releases from day one.
- `rk` binary with config loading and a working daemon socket (echo-level).

**Steps**
1. Init git repo + Cargo workspace: `rk-core`, `rk-cli`, `rk-daemon` (others added
   per phase). Rust stable, edition 2024. `just`/`cargo xtask` for dev tasks.
2. CI: fmt, clippy (deny warnings), test, and a macOS+Linux build matrix.
3. `rk-core`: ULID ids, timestamp/lamport types, error type (thiserror), tuple
   model structs (category enum, scope, identity, instance, lifecycle class,
   payload as `serde_json::Value`) + schemars derivation.
4. Config: figment (toml file `~/.config/rat-kingdom/config.toml` + `RK_*` env +
   CLI overrides). Path layout module (state dir, worktrees dir, socket path).
5. `rk-daemon`: tokio, NDJSON-over-UDS listener, request/response envelope,
   `rk ping` round-trip. tmux-style lazy spawn (client starts detached server on
   connect-fail) + `rk daemon run/status/stop`.
6. tracing setup with env-filter; `--log-format json|pretty`.

**Exit criteria**: `rk ping` auto-starts the daemon and round-trips on a fresh
machine; CI green.

---

## Phase 1 — Tuplespace (rk-space): the substrate, done right

**Top-line features**
- Full Linda primitives over the daemon socket: `rk out/in/rd/scan` with blocking
  in/rd, TTLs, lifecycle classes, and payload search.
- Typed sugar commands: `rk done`, `rk obstacle`, `rk need`, `rk claim` —
  env-autofilled, schema-validated at write time.
- `rk watch` — live tuple stream for debugging (the "free debugger" property).

**Steps**
1. rusqlite store (WAL, bundled): `tuples` table indexed on (category, scope,
   identity, instance), JSON payload column, FTS5 index, lifecycle + TTL columns.
2. Atomic primitives: `out` = insert + FTS + waiter-wake **in one transaction /
   critical section**; `in` via `DELETE ... RETURNING`; furniture rejects `in`.
3. Waiter registry: blocked `in`/`rd` register a predicate; **the wake path
   evaluates the exact same predicate the reader blocks on** (including payload
   search). Re-check after registration. This kills imp's lost-wakeup class.
4. Property tests (proptest/loom-style): N writers × M blocked readers, assert no
   lost wakeups, no double-consume of `in`, under randomized interleavings.
5. Wire protocol: NDJSON ops on the daemon socket; `rk-cli` subcommands; `--json`
   output everywhere.
6. Sugar commands with payload schemas (schemars-validated) and `RK_AGENT`,
   `RK_TASK`, `RK_REPO`, `RK_PARENT` env autofill.
7. GC — actual GC only: TTL expiry, session-lifecycle cleanup on task completion,
   stale-claim flagging. (Escalation/promotion/analytics are NOT in this loop.)
8. Event feed: daemon-internal broadcast of every `out` (feeds `rk watch` now,
   triggers in Phase 5, ledger facts in Phase 4).

**Exit criteria**: property tests pass under stress; two terminal sessions
coordinate through blocking `in`/`rd` with no polling; sugar commands reject
malformed payloads.

---

## Phase 2 — One agent, end to end (rk-harness: Claude Code)

**Top-line features**
- `rk spawn --task <id>` launches a Claude Code agent in an isolated worktree,
  primes it, streams its events, detects completion structurally, and merges on
  dismiss.
- `rk list` / `rk status <agent>` with real state (working/awaiting-input/done)
  from the event stream — no terminal scraping, no sleeps.
- `rk steer <agent> "message"` — mid-session guidance over stream-json stdin.

**Steps**
1. `Harness` trait in `rk-harness` (launch/events/steer/interrupt/resume/
   capabilities) + normalized `HarnessEvent` enum (design doc §4).
2. Claude adapter: spawn `claude -p --output-format stream-json --input-format
   stream-json --verbose` as a child process; parse `system/init` (session_id,
   capabilities), assistant/user messages with usage, `api_retry`, terminal
   `result`. Steering = user-message frames on stdin; interrupt = SIGINT;
   SIGTERM semantics honored on kill.
3. Git lifecycle in `rk-core` (via git2): worktree create at
   `~/.rat-kingdom/worktrees/<repo>/<agent>`, branch `agent/{name}/{task}`,
   protected-branch guards, dismiss = remove-worktree → merge (rebase-first,
   merge fallback) → cleanup; `--no-merge` path; prune.
4. Priming: role templates as **composed fragments** (single source for command
   syntax / completion protocol / git safety), rendered and passed via
   `--append-system-prompt-file`; task context injected as the initial prompt —
   no "please run rk prime" keystroke dance.
5. Supervision registry in the daemon: agent record (harness, session_id, pid,
   worktree, branch, task, **parent**), spawn tree as first-class data.
   Completion events route to the spawner by structure.
6. Session resume: persist session_id; `rk respawn <agent>` uses `--resume` with
   a continuation message (actually delivered, unlike imp).
7. Directed-agent capability gating: a spawned-for-task agent's sugar toolset
   excludes claim; single-task discipline is structural.

**Exit criteria**: spawn → agent does real work in a worktree → `rk done` fires a
completion event → dismiss merges the branch — fully headless, zero sleeps; a
killed daemon restart re-attaches to the registry without losing agents.

---

## Phase 3 — Multi-harness + herdr (rk-mux)

**Top-line features**
- Same `rk spawn` drives **Claude Code, Codex, or axe** via `--harness` (or
  per-role defaults in config): mix models across one fleet.
- `rk attach <agent>` — human takeover through herdr panes; agent status visible
  in herdr's sidebar; desktop notifications on blocked/done.
- Graceful degradation: headless-only mode when herdr isn't running.

**Steps**
1. Codex adapter A (`codex exec --json` + `exec resume`) for fire-and-forget;
   adapter B (`codex app-server` JSON-RPC: thread/start|resume|fork, turn/start,
   **turn/steer**, turn/interrupt, approval routing) for supervised sessions.
   Generate serde types from `codex app-server generate-json-schema` in CI.
2. axe adapter: TOML agent defs, subprocess + `--json` result, `--max-tokens`
   pass-through, exit-code-4 → BudgetExceeded event. Map to steward/reviewer-
   style bounded jobs.
3. Capability negotiation: `HarnessCaps` drives orchestrator behavior (no steer →
   respawn-with-context; no resume → fresh session + tuplespace catch-up).
4. `rk-mux`: herdr socket client (NDJSON, protocol-pinned; types generated from
   `herdr api schema --json` in CI). Trait `Multiplexer` with herdr impl + null
   (headless) impl; tmux fallback impl only if ever needed.
5. Attach mode: spawn interactive harnesses inside herdr panes
   (`agent start ... -- <argv>`), subscribe to `pane.agent_status_changed` as a
   secondary liveness signal, surface `herdr notification show` for human alerts.
   Install herdr integrations for claude/codex (`herdr integration install`).
6. Conformance suite: one integration-test harness scenario (spawn → steer →
   complete → usage report) run against all three adapters; capability matrix
   asserted in tests.

**Exit criteria**: the same task runs to completion on all three harnesses with
identical orchestration code; human can attach to a live Claude pane mid-task and
hand back control.

---

## Phase 4 — Cost ledger & token budgets (rk-ledger)

**Top-line features**
- `rk cost` — live per-agent/per-task/per-day tokens and USD; `rk cost --fleet`
  rollup.
- Budgets in config (per task / agent / castle): warn → steer → interrupt,
  enforced automatically.
- Runaway/stuck detection from burn-rate + harness state (better than any
  terminal-activity heuristic).

**Steps**
1. Ledger tables in the daemon store keyed (agent, task, session): token classes
   (input/output/cache-read/cache-write/reasoning), cost USD, updated from live
   `Usage` events on the harness stream. Codex: diff cumulative turn totals.
2. Pricing: vendor LiteLLM `model_prices_and_context_window.json` snapshot +
   `rk pricing refresh`; model-alias resolution; cost formula per design doc §5.
3. Budget engine: thresholds in config; graduated actions (warn tuple → steer
   message → interrupt/kill) with hysteresis; per-harness native enforcement
   where available (axe `--max-tokens`).
4. Ledger → tuplespace `fact` tuples (`token-usage:<agent>`) so coordinators see
   burn in-context; `obstacle` tuples on runaway/stuck (high burn + no completion;
   active claim + zero events for N min).
5. Offline backfill parsers: `~/.claude/projects/**/*.jsonl` and
   `$CODEX_HOME/sessions/**.jsonl` (`rk cost import`) for sessions run outside
   rat-kingdom.
6. Attribute-key schema tests (imp's telemetry died of key drift — pin and test).

**Exit criteria**: a deliberately runaway prompt gets warned, steered, then killed
at the configured cap; `rk cost` matches the harness's own self-reported totals
within rounding.

---

## Phase 5 — Workflows & reactive triggers (rk-workflow)

**Top-line features**
- CUE-defined workflows in imp's form factor: `#Workflow` schema, `_input`/`_ctx`,
  spawn/wait/evaluate/dismiss/gate/sub-workflow steps, **aspects** (before/after
  weaving by type/name/role).
- Reactive triggers: tuple write → workflow dispatch, zero tokens, zero latency.
- `rk workflow run/list/status/cancel/approve/reject`; recovery across daemon
  restarts.

**Steps**
1. cuengine integration: evaluate the workflow package (bundled `schema.cue` +
   global dir + per-repo dir, repo wins) → JSON → serde step configs. Always
   re-read from disk. Load-time errors are CUE unification errors, verbatim.
2. Aspect weaver: pure Rust `expand_aspects` (match {type, name-glob, spawn-role},
   AND semantics; declaration order, first innermost) + unit tests mirroring
   imp's semantics.
3. Interpolation: generate `context.cue` carrying `_input`/`_ctx` into the eval
   package so CUE resolves references, defaults, and constraints itself.
4. Step handlers over Phase-2/3 machinery: spawn, wait (subscribes to the Phase-1
   event feed — a blocking rd with the *unified* predicate; no TOCTOU by
   construction), evaluate (temp-package unification: `result: expect & actual`,
   concreteness check; `cue` CLI fallback behind a feature flag), dismiss, gate
   (human via approval tuples + herdr notification; timer), workflow (sub-workflow,
   depth cap, output → parent `_ctx.previousOutput`).
5. Instance state machine persisted in the daemon store; recovery on restart:
   liveness-check active agents, resume from current step or fail cleanly.
6. Trigger engine: `triggers.cue` (`#Trigger` schema), exact category+identity
   match + excludes, params templated from payload; dispatch off the Phase-1
   event feed; `rk daemon reload` re-reads definitions without restart.
7. Foreman-pattern integration test: foreman workflow fans out N workers;
   completion events route to the foreman (never bypass to a global coordinator);
   dismissal requires foreman ack.

**Exit criteria**: a code-review-style workflow (implement → review → gate →
merge) runs unattended across two different harnesses; an aspect adds a
build-check after every spawn without touching the workflow file; daemon restart
mid-workflow resumes correctly.

---

## Phase 6 — Multiplayer via git notes (rk-sync)

**Top-line features**
- `rk sync` — asynchronous coordination between machines and humans through the
  repo's git remote: shared tasks, claims, facts, obstacles across castles.
- Zero-contention by construction: per-actor refs, every push a fast-forward.
- Deterministic cross-castle claim arbitration — no revocation-message channel.
- `rk peers` — who's active, what they've claimed, last-seen.

**Steps**
1. Castle identity: Ed25519 keypair + proquint name (imp's scheme), stored under
   the config dir; actor-id = castle + instance.
2. Record format: append-only NDJSON lines (ULID + hybrid timestamp + actor +
   tuple op), one line per record — `cat_sort_uniq`-safe by construction.
3. Write path: local durable tuples journal to the actor's own ref
   (`refs/notes/rk/<actor>` on a well-known anchor per scope; commit-anchored
   facts annotate real commits). git2 for CRUD. Ephemeral tuples never touch git.
4. Sync loop: fetch `refs/notes/rk/*` into non-mirroring remote-tracking
   namespaces (never mirror+prune); union-merge into the local materialized view
   via system-git `notes merge --strategy=cat_sort_uniq` (or in-memory union);
   push own ref (fast-forward always). Wake local waiters for remotely-authored
   tuples through the normal Phase-1 out() path.
5. Arbitration: claims conflict-resolve deterministically — (timestamp, actor-id)
   ordering computed identically by every reader; losing side's daemon steers its
   agent to wind down.
6. Compaction: periodic per-actor history squash; dead-actor pruning; `rk sync
   status` with divergence warnings (imp's silent-stall lesson: sync failure is an
   `obstacle` tuple, not a debug log).
7. Two-castle integration test (two temp clones + bare remote): concurrent claims,
   partition/rejoin, convergence assertions; property test on merge idempotence.

**Exit criteria**: two machines (or two checkouts simulating them) run agents
against a shared backlog with no duplicate completed work, converge after
partition, and a human on machine B sees machine A's obstacles in `rk watch`.

---

## Phase 7 — Fleet quality-of-life

**Top-line features**
- Steward: reactive triage workflow — fetch completed branch, run checks,
  auto-dismiss clean work, escalate the rest (trigger-driven, not a polling
  agent).
- `rk top` — ratatui fleet dashboard: agents, states, burn rates, budgets,
  workflow instances (thin — herdr carries per-agent detail).
- Knowledge maturation: `suggestion`/`endorsement` categories in the system scope
  with quorum promotion (imp's cross-repo insight pattern, generalized).
- Operational polish: `rk doctor`, shell completions, launchd/systemd units,
  crash-loop backoff on respawn, docs site.

**Steps**
1. Steward as a Phase-5 trigger + workflow (no bespoke role machinery) with an
   axe- or Claude-backed check step; escalation via need tuples + herdr
   notifications.
2. `rk top` over the daemon's event feed + ledger queries.
3. system-scope suggestion/endorsement schemas + `rk suggestions` ranking by
   distinct-scope endorsements; quorum auto-promotion in a dedicated (non-GC)
   maintenance job.
4. Template lint in CI (fragment composition rules, no cross-mode leakage —
   imp's priming-drift lesson).
5. `rk doctor` (harness versions, herdr protocol, git remote refspecs, pricing
   staleness), packaging, install docs.

**Exit criteria**: a week of real use on rat-kingdom's own development with the
steward dismissing clean work unattended; onboarding a second repo takes one
command.

---

## Cross-phase invariants

- Every phase ships behind `rk --json` machine-readable output — agents are
  first-class CLI consumers.
- No sleeps as synchronization anywhere; every wait is an event subscription or a
  blocking primitive with a timeout.
- Prose never enforces what structure can: capability gating, typed payloads,
  composed templates, CI lint.
- Each phase's integration tests run in CI against pinned harness/herdr versions;
  version-drift caught by schema-generation diffs, not runtime surprises.

## Dependency graph

```
P0 ─▶ P1 ─▶ P2 ─▶ P3 ─▶ P5 ─▶ P7
             │      └▶ P4 ──┘▲
             └──────────────▶ P6 (needs P1; benefits from P2+ for arbitration steering)
```

P4 (ledger) can start as soon as P2's event stream exists and proceed in parallel
with P3. P6 depends only on P1's substrate and can be prototyped early if the
git-notes mechanics feel like the riskiest bet — recommended de-risk: build step
P6.2–P6.4 as a spike during P3.

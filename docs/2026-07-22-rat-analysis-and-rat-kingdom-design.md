# Predecessor Analysis & rat-kingdom Design Research

Date: 2026-07-22. Sources: full code walk of the predecessor harness (Go, ~44.7k LOC), its `.ai/research` +
`docs/` corpus, local ground truth on installed CLIs (claude 2.1.217, codex 0.144.6,
herdr 0.7.4), and web research on harness automation surfaces, git-notes coordination,
and the mid-2026 Rust ecosystem. Citations inline.

---

## 1. What the predecessor is and how it actually works

The predecessor is a multi-agent orchestration harness: a single daemon + CLI that spawns AI coding
agents into tmux sessions, isolates each in a git worktree, and coordinates them
stigmergically through a Linda-style tuplespace ("BBS").

**Mechanism summary (verified in code):**

- **Roles**: king, worker, reviewer, merge-handler, foreman, steward. King/steward are
  global singletons (no worktree); everything else gets branch `agent/{name}/{taskID}`
  and a per-agent worktree under the castle root (`internal/agent/agent.go:169`).
- **Spawn**: create worktree → install anti-main-commit hook → `tmux new-session -e`
  with the predecessor's env vars + OTLP env → *type* the harness command into the shell
  (`tmux send-keys`) → poll pane text for Claude's `❯` prompt → type
  the prime command → type "Begin working now".
- **Harness invocation**: a single config string, default
  `auggie -w {{workspace}} --allow-indexing` (`internal/config/config.go:16`). No
  process abstraction, no argv, no readiness abstraction — the readiness probe is
  hardcoded to Claude Code's TUI markers while the default command is a different tool.
- **Tuplespace**: tuples `(category, scope, identity, instance)` + JSON payload;
  `out/in/rd/scan` over JSON-RPC multiplexed on the daemon's unix socket. Two backends:
  SQLite, or git-native NDJSON files exposed through SQLite virtual tables with FTS5
  (`internal/bbs/`, ~7.4k LOC + equal test LOC).
- **Notifications**: daemon polls every 10s and *types messages into agents' terminals*
  (`tmux send-keys -l` + 500ms sleep + Enter), after `C-a C-k`-ing any half-typed human
  input into the kill ring (`internal/tmux/tmux.go:42`, `internal/daemon/poller.go:301`).
- **Workflows**: CUE-defined, linear step machine (spawn/wait/evaluate/dismiss/gate/
  sub-workflow) executed by the daemon, with reactive triggers that fire workflows on
  tuple writes (exact category+identity match — zero tokens, zero latency).
- **Multiplayer**: `.bbs/data/*.ndjson` synced through a hidden worktree on an orphan
  `bbs-sync` branch every 15s, merged row-wise by a custom three-way NDJSON merge driver
  keyed on `_pk`; duplicate cross-castle task claims resolved after the fact by
  earliest-`claimed_at`-wins with a revocation message to the loser.
- **Telemetry**: the daemon embeds an OTLP receiver (4317/4318) → DuckDB/Parquet. Spawn
  sets `OTEL_EXPORTER_OTLP_ENDPOINT` etc. so an OTel-aware harness reports tokens/cost.
  The predecessor itself computes no cost and enforces no budgets.

**Scope by package (non-test LOC)**: cli 7.4k, bbs 7.4k, workflow 3.1k, daemon 3.0k,
telemetry/otel 2.0k, onboard 1.6k, agent 1.4k, hub 1.0k, remainder ~3k.

## 2. Where comms fall over (the loose ends)

From code inspection and the predecessor's own post-mortem research docs:

1. **Keystroke injection is the only channel into a live agent.** No ack; `delivered=true`
   means "we typed it," not "the agent saw it." Blind to TUI state (permission dialogs,
   sub-prompts, mid-render). Single biggest fragility.
2. **Timing hacks as synchronization**: 500ms text→Enter sleep, 5s post-ready settle,
   60s startup-delay fallback, 10s poll floor on every message.
3. **Claude-specific readiness probe vs. configurable harness** — priming mis-times on
   any non-Claude CLI (falls back to the 60s sleep and can inject before the agent
   listens).
4. **Tuplespace lost-wakeup (TOCTOU)**: `Out` inserts the row, then inserts the FTS row,
   then wakes waiters — but the wake matcher (`matchesTuple`, category/scope/identity
   only) is a *different predicate* than blocked readers use (`readOne` with
   PayloadSearch via FTS). Insert+index+wake are not one critical section. Result: a
   `task_done` can slip through and a workflow wait blocks for the full timeout
   (documented incident: 30-min hang; `.ai/research/2026-03-09-wait-step-toctou-race`).
   The Phase-1 pre-scan, `seen` maps, and 500ms re-scan loops in `steps/wait.go` are
   mitigation, not cure.
5. **Payload-blind event routing**: worker `task_done`/`dismiss_request` events route to
   steward/king unconditionally; the spawning foreman is bypassed because parent/child
   lineage is a hand-copied payload field the sugar command doesn't even emit
   (`.ai/research/2026-04-06-foreman-worker-notification-routing`). Race: king dismisses
   a worker before its foreman integrates the branch.
6. **Prompt-discipline failures**: workers claim second tasks because the priming template
   both forbids it and teaches the claim loop; 8 drifted templates with inconsistent
   command syntax, invalid tracker statuses, and a regex-based "communication section
   replacer" that can mangle a reviewer's structured output.
7. **Cross-castle claim conflicts resolved after work has started** (earliest-wins +
   revocation message delivered via... keystroke injection, see #1).
8. **Dedup keyed by GC-able tuple IDs** → re-notification after collection. **Respawn
   continuation message is never actually sent** (dead code path). **Sync push backoff
   can stall silently**, letting castles diverge with only a debug log.

**Root-cause pattern**: every failure is either (a) an unreliable transport (terminal
keystrokes) where a protocol should be, (b) a hand-rolled concurrency primitive over a
database, or (c) discipline encoded in prose instead of structure.

## 3. What the predecessor got right (keep these)

- **The tuplespace model itself**: fixed `(category, scope, identity)` prefix, small
  category vocabulary with an epistemic hierarchy (fact > convention > artifact > claim >
  obstacle/need > event), three lifecycle classes (furniture/session/ephemeral), scope as
  isolation boundary, always-loaded `system` scope, "prime the space, not the agent."
- **Reactive daemon triggers over polling agents**: deterministic dispatch table fires
  workflows on tuple writes — zero tokens, zero latency, can't be broken by an agent
  deviating from protocol. The predecessor's own docs call this the reliability win.
- **Schema-enforcing sugar commands** (`task-done`, `obstacle`, `escalate`) that
  auto-fill identity from env — "the strongest defense against BBS noise."
- **Steward pattern**: offload routine triage (fetch branch, test, auto-dismiss or
  escalate) from the human-facing coordinator.
- **Worktree-per-agent, never-touch-main, target-branch-then-merge lifecycle.**
- **A2A client contract** (agent-card discovery, 4 endpoints) — designed, tested, but the
  server was never built. The contract is reusable; the lesson is don't ship a client
  without a server.

**What was overbuilt (don't rebuild):** DuckDB/Parquet warm analytics (~1.9k LOC + CGo,
never produced one Parquet file in production — the agents weren't leaving enough trails
to analyze); a GC doing 9–13 jobs in one 838-line file; 8 separate per-agent state
stores; dual mail systems mid-migration; 1.7k LOC of hub client for a server that
doesn't exist.

## 4. Multi-harness design (claude, codex, axe, +)

**Key finding: keystroke injection is obsolete.** Every relevant harness now has a
machine-parseable supervision surface. Two patterns cover everything:

**Pattern A — NDJSON stream over stdio** (spawn child, read/write line-JSON):
- **Claude Code**: `claude -p --output-format stream-json --input-format stream-json
  --verbose`. Events: `system/init` (capabilities array, session_id), `assistant`/`user`
  messages (with `usage` per API call), `system/api_retry`, terminal `result` message
  (final text, `total_cost_usd`, session metadata). Mid-session steering = write user
  messages to stdin while a turn runs. Resume: `--resume <session_id>` / `--fork-session`.
  `--bare` for deterministic scripted runs. SIGTERM → clean exit 143 + SessionEnd hooks.
- **Amp**: `--stream-json`/`--stream-json-input` deliberately Claude-compatible — one
  adapter covers both.
- **Codex fire-and-forget**: `codex exec --json` JSONL (`thread.started`,
  `turn.completed` with cumulative usage, `item.*`), `--output-schema`,
  `--output-last-message`, `codex exec resume <id>|--last`.

**Pattern B — JSON-RPC server** (long-lived supervised sessions):
- **Codex app-server**: `codex app-server` — JSON-RPC 2.0 over stdio/WebSocket/unix
  control socket. `thread/start|resume|fork`, `turn/start`, **`turn/steer`** (inject into
  an active turn), `turn/interrupt`, approval requests routed to the orchestrator,
  `turn/completed` notifications with token usage. `codex app-server
  generate-json-schema` emits schemas pinned to the installed binary → generate serde
  types.
- **ACP (Agent Client Protocol, Zed)** is the emerging cross-harness seam: Gemini CLI,
  opencode, and Claude adapters speak it. Worth an `AcpHarness` adapter later.

**axe** (jrswab/axe, Apache-2.0, Go, v1.10.x): one-shot Unix-philosophy executor —
TOML-defined agents, `axe run <agent> -p ... --json`, native `--max-tokens` budget caps
with **exit code 4 on budget exceeded**, token counts in JSON metadata, MCP support,
`call_agent` delegation. No resume/steer — supervise as a plain subprocess. Easiest to
integrate; good fit for steward/reviewer-style bounded jobs.

**Proposed abstraction** (informed by vibe-kanban's Rust executor trait, the closest
prior art — see `crates/executors` in BloopAI/vibe-kanban):

```rust
trait Harness {
    fn launch(&self, spec: &TaskSpec) -> Result<HarnessSession>;   // argv, env, cwd
    fn events(&mut self) -> impl Stream<Item = HarnessEvent>;      // normalized
    fn steer(&mut self, msg: UserMessage) -> Result<()>;           // may be Unsupported
    fn interrupt(&mut self) -> Result<()>;
    fn resume(spec: &TaskSpec, session: &SessionRef) -> Result<HarnessSession>;
    fn capabilities(&self) -> HarnessCaps;                          // steer/resume/budget...
}
// HarnessEvent::{Started{session_id}, AssistantText, ToolUse, Usage(TokenUsage),
//                NeedsApproval(..), Retry(..), Completed{result, usage}, Exited(code)}
```

Normalized capability matrix:

| | launch | events | steer | interrupt | resume | usage | native budget |
|---|---|---|---|---|---|---|---|
| Claude Code | stream-json | stream-json | stdin msg | SIGINT/control | `--resume`/fork | per-msg + result USD | no (hooks can) |
| Codex | exec --json / app-server | JSONL / JSON-RPC | `turn/steer` | `turn/interrupt` | `resume`/`thread/fork` | cumulative tokens (diff turns) | no |
| axe | subprocess | `--json` result | — | SIGTERM | — | tokens in result | yes (`--max-tokens`, exit 4) |
| Amp | stream-json | stream-json | stdin msg | signal | `threads continue` | per-msg | no |

Completion detection becomes structural (a `result`/`turn.completed` event or process
exit), replacing the predecessor's pane-scraping and `session_activity` heuristics entirely.
Human attach remains available by running these processes under herdr (§7).

## 5. Cost tracking and token budgets

**Capture (per harness):**
- Claude Code: authoritative `total_cost_usd` + per-model usage in the `result` event;
  per-API-call `usage` on each assistant message; OTLP metrics
  (`claude_code.cost.usage`, `claude_code.token.usage` with session.id attribute) if you
  want fleet-level export; offline backfill from `~/.claude/projects/**/*.jsonl`.
- Codex: `turn.completed.usage` is **cumulative per session** — diff successive turns
  for per-turn cost (ccusage documents the algorithm); offline from
  `$CODEX_HOME/sessions/**.jsonl` (`token_count` events). Codex never reports USD.
- axe: token counts in `--json` result; enforces its own budget (exit 4).

**Tokens→USD**: vendor a snapshot of LiteLLM's `model_prices_and_context_window.json`
(de facto standard; ccusage and OpenCode both use it) with optional runtime refresh;
`llm-pricing` and `ccost` crates exist as references. Formula:
`(input − cached) × in_price + cached × cache_read_price + output × out_price`.

**Budget enforcement design** (the predecessor designed this but never wired it — two config bugs
left it inert; see `.ai/plans/otlp-token-metrics-feedback-loop.md`):
- Ledger table keyed by (agent, task, session): tokens by class, computed USD, updated
  from live Usage events — not from a 5-min poll.
- Budgets at task/agent/castle level in config; on threshold: warn tuple → steer message
  → interrupt/kill (graduated). Runaway detection: high burn + no completion event.
- Burn-rate is a better stuck/spinning signal than terminal activity; combine with
  harness state (herdr's idle/working/blocked) instead of tmux `session_activity`.
- Write the ledger summary back into the tuplespace as `fact` tuples so coordinators
  see per-agent burn in-context (the predecessor's good idea, kept).

## 6. Git notes for asynchronous multi-castle coordination

This replaces the predecessor's bbs-sync orphan branch + custom NDJSON merge driver + after-the-fact
claim resolution. The prior art (git-appraise, git-bug, Gerrit NoteDb, Radicle COBs)
converges on one pattern:

**Per-actor, single-writer refs; append-only NDJSON; merge at read time.**

1. Each actor (human or agent instance, keyed by castle/actor id) writes only to its own
   ref: `refs/notes/rk/<actor-id>` (or plain refs `refs/rk/<actor>/…` git-bug-style).
   Every push is a fast-forward — **zero contention by construction**, no server-side
   merge, no claim-revocation-after-the-fact.
2. Records are one self-contained JSON object per line with a ULID + lamport/hybrid
   timestamp, so `git notes merge --strategy=cat_sort_uniq` is a safe grow-only-set
   union (dedupe-tolerant, order-independent).
3. Readers fetch all `refs/notes/rk/*` into remote-tracking namespaces (never
   mirror+prune — `fetch --prune` with a mirroring notes refspec deletes local notes)
   and materialize a local view: union all actors' records, resolve conflicts at the
   application layer (LWW by timestamp, or claim-arbitration deterministically by
   (timestamp, actor-id) — same rule the predecessor used, but now computed identically by every
   reader with no revocation messages needed for the common case).
4. Anchoring: notes naturally annotate commits — perfect for "review of commit C,"
   "task X completed at commit C," CI verdicts. Free-standing tuples (tasks, claims,
   obstacles) can annotate a well-known anchor object per scope, or use git-bug-style
   per-entity operation chains under `refs/rk/` instead of notes proper.
5. Ephemeral tuples (heartbeats, signals) stay **out of git** entirely (the predecessor's NDJSON
   design learned this — write amplification); they live only in the local daemon.
6. Compaction: periodic squash of old note history per actor ref; prune dead actors.

**Library reality check**: git2 (libgit2) has full notes CRUD (`Repository::note`,
`find_note`, `notes` iterator); **gix has neither notes nor push yet** (crate-status
confirms both unchecked). Neither library implements notes *merge strategies* — shell
out to system `git notes merge --strategy=cat_sort_uniq` and `git push`, which is what
git-appraise effectively does. Pragmatic split: git2 for read/write, system git for
merge/push/fetch, gix optional later for fast read paths.

Semantics consequence: the tuplespace becomes **local-first with eventual convergence**.
Local operations (in/rd/out/scan, blocking waiters) run against the local store at full
speed; the git layer is a replication transport. Destructive `in` across castles is
modeled as a claim record + deterministic arbitration (a true distributed atomic take is
impossible over async git sync anyway — the predecessor's earliest-wins policy was correct, just
delivered over the wrong channel).

## 7. herdr vs tmux — verdict: use herdr

herdr (herdr.dev, ~19.4k★, Rust, v0.7.x, AGPL-3.0 + commercial) is a terminal workspace
manager built *specifically* for AI coding agents. Verified locally (0.7.4 installed,
server running, protocol 16):

| Need (the predecessor's tmux pain) | tmux | herdr |
|---|---|---|
| Inject input | `send-keys` + sleeps | `pane.send_text/send_keys`, atomic `pane run`, `agent prompt --wait` |
| Read output | `capture-pane` scraping | `pane.read` (visible/recent/unwrapped), read-only stream attach |
| Agent state | none — regex the scrollback | first-class `idle/working/blocked/done` via per-harness hook integrations (claude, codex, opencode, +11) |
| Events | coarse `hooks`, manual `wait-for` | `events.subscribe` NDJSON stream, `events.wait`, `agent wait --status`, `wait output --match --regex` |
| API | control mode `-CC` (arcane) | NDJSON socket API with published JSON schema (`herdr api schema --json`) |
| Worktrees | none | `worktree create --branch --base` built in |
| Session file discovery | none | integrations report `agent-session-id`/`agent-session-path` → direct handle to the harness JSONL for cost backfill |
| Remote | ssh+tmux | `--remote <ssh-target>`, named sessions |

The `pane report-agent` / session-path mechanism directly solves the predecessor's two hardest
problems (activity detection and notification timing), and the socket API replaces every
`send-keys` sleep with a request/response + event subscription.

**Recommended architecture**: herdr is the *presentation and PTY layer* — where agents
run so humans can attach, observe, and take over. The **control plane runs through the
harness protocols (§4), not through the terminal**: rat-kingdom spawns
`herdr agent start <name> -- claude ...` (or codex app-server headless with no pane at
all for non-interactive jobs), steers via stream-json/JSON-RPC, and uses herdr's
agent-status events as a *secondary* liveness signal plus human-visibility surface.
Message delivery to an interactive agent stops being keystrokes and becomes either
(a) protocol-level steering, or (b) tuplespace data the agent reads at its own
breakpoints — with `herdr notification show` for human alerts.

**Risks**: pre-1.0 API churn (pin protocol version; `herdr api schema --json` enables
generated types), AGPL (irrelevant over the socket boundary; don't vendor code), young
project. **Hedge**: keep the multiplexer behind a thin trait with a tmux fallback
implementation; or degrade to headless-only (no attach surface) via portable-pty.

## 8. Rust stack recommendation

| Concern | Pick | Notes |
|---|---|---|
| Runtime | tokio 1.x | uncontested; ecosystem assumes it |
| CLI | clap 4 (derive) | uncontested |
| TUI (dashboard) | ratatui | + `tui-term` if embedding panes; largely obviated by herdr |
| Git | git2 + system-git for notes-merge/push; gix later | gix lacks notes & push (verified in crate-status) |
| Storage | rusqlite (bundled) | one store; WAL; `DELETE..RETURNING` for atomic `in()`. Skip DuckDB/Parquet tier (the predecessor's own audit: never used). redb only if SQL proves unnecessary |
| Serialization | serde + serde_json, NDJSON everywhere | same record shape for socket framing and git-notes lines |
| PTY (fallback path) | portable-pty | wezterm's, battle-tested, pre-1.0 — pin |
| IPC | tokio UnixListener (or `interprocess` if Windows matters) | NDJSON-over-UDS, same shape as herdr's protocol |
| Config | figment + toml | layered file/env/CLI |
| Telemetry | tracing + tracing-opentelemetry; optional OTLP receiver via opentelemetry-rust/tonic | isolate behind a facade — OTel-rust metrics still churn |
| Workflow definitions | **CUE via cuengine** (decided 2026-07-22) | keep the predecessor's CUE form factor; see §9a. cuengine = FFI over the Go evaluator (v0.40.x, active, AGPL-3.0, Go toolchain in build). Fallback for unification-style checks: temp-package trick or `cue` CLI shell-out |
| Schema/validation (internal) | serde types + schemars/jsonschema | for tuple payloads and IPC, not workflows |
| Pricing | vendored LiteLLM model-prices JSON + runtime refresh | `llm-pricing`/`ccost` as references |
| Daemon | tmux-style lazy spawn (client starts detached server on connect-fail) + shipped launchd/systemd units | don't double-fork daemonize |
| Codex types | generate from `codex app-server generate-json-schema` | pinned to installed binary |

## 9. Proposed shape of rat-kingdom

```
rat-kingdom (workspace)
├── rk-core        tuple model, schemas, ids (ULID), epistemic/lifecycle rules
├── rk-space       local tuplespace: rusqlite, atomic out+index+wake in one
│                  transaction/critical section, waiters matched on the SAME
│                  predicate readers block on (kills the TOCTOU class)
├── rk-sync        git-notes replication: per-actor refs, cat_sort_uniq via
│                  system git, materialized local view, claim arbitration
├── rk-harness     Harness trait + adapters: claude (stream-json), codex
│                  (app-server + exec), axe (subprocess), amp (stream-json),
│                  later acp
├── rk-mux         herdr socket client (+ trait w/ tmux fallback): panes for
│                  human attach, agent-status events, notifications
├── rk-ledger      usage events → token/cost ledger → budgets → graduated
│                  enforcement (warn tuple / steer / interrupt)
├── rk-workflow    CUE workflow engine (cuengine): loader, aspect weaver,
│                  step handlers, trigger dispatch table, instance state
├── rk-daemon      supervisor: spawn tree (parent/child lineage FIRST-CLASS,
│                  fixing foreman routing), reactive trigger dispatch,
│                  unix-socket IPC
└── rk-cli         clap front end + sugar commands (typed, env-autofilled)
```

### 9a. Workflow engine: the predecessor's CUE form factor, kept (decided 2026-07-22)

rat-kingdom mimics the predecessor's workflow layout — CUE definitions, the aspect system, and
reactive triggers — using **cuengine** for evaluation. The predecessor's workflows need not port
over verbatim; it's the form factor we're keeping.

The layout being mimicked (from the predecessor repo's `internal/workflow/schema.cue`, `types.go`,
`loader.go:590`):

- **One workflow per `.cue` file**, validated by unification against a `#Workflow`
  schema in the same CUE package. Discovery: global dir (`~/.rat-kingdom/workflows/`)
  overridden by per-repo dir. Always re-read from disk (the predecessor's no-cache choice — keep).
- **`_input` / `_ctx` context model**: `_input.*` = declared params (`#Param`:
  type/required/default); `_ctx.*` = implicit execution context threaded through steps
  (`taskId`, `activeAgent`, `activeBranch`, `activeRepo`, `previousOutput`) so steps
  reference results by name, never by index.
- **Step vocabulary**: `spawn` (role + `#TaskDef`), `wait` (timeout), `evaluate`
  (expect unified against previous output), `dismiss` (± noMerge), `gate`
  (human‑approval or timer variants), `workflow` (sub-workflow with param passing,
  recursion cap 5, child output → parent `_ctx.previousOutput`).
- **Aspects — load-time AOP weaving** (`#Aspect`): `match` {step type, name glob,
  spawn role — AND semantics} + `before`/`after` step lists. Applied as a pure
  transformation of the step list at load time, in declaration order, first aspect
  innermost (the predecessor's `expandAspects`). This stays a plain Rust function over the parsed
  step list — CUE defines aspects; Rust weaves them. Injected steps are ordinary steps
  afterward (visible in `workflow status`, timed, recoverable).
- **Triggers in CUE** (`#Trigger`): exact `{category, identity}` tuple match +
  agent/scope excludes → workflow name + params templated from payload fields.
  Same reactive dispatch table as the predecessor, fed by rk-space's out() hook.

**Evaluation pipeline**: cuengine evaluates the workflow *package* (schema + user
files) → JSON export → serde into typed step configs. Schema violations surface as
CUE unification errors at load, exactly like the predecessor.

**Known constraint**: cuengine exposes package/module evaluation only — no
string-eval or `unify` primitive. Two places need unification at *runtime*:

1. **`evaluate` step** (`expect` ⊓ previous output, then concreteness check):
   materialize a temp package — `expect.cue` (literal from the definition),
   `actual.cue` (JSON output embedded as a value), `result: expect & actual` — and
   evaluate it with cuengine; error or non-concrete result = step failure.
2. **Param interpolation** (`_input`/`_ctx` references inside step configs): resolve
   the same way — inject a generated `context.cue` carrying `_input`/`_ctx` values
   into the evaluation package, so CUE itself performs interpolation (the predecessor resolves
   `_ctx` in Go with string templates; letting CUE do it is cleaner and gets
   defaults/constraints for free).

If the temp-package dance proves awkward, fallbacks in order: shell out to `cue vet`
/ `cue export` (zero linkage, full semantics), or Mr-Leshiy/cue-rs (libcue bindings,
Apache-2.0) which does expose compile/unify primitives but is younger.

License note: cuengine is AGPL-3.0-or-later and statically links; fine for a
personal/internal tool, revisit if rat-kingdom is ever distributed.

Design commitments distilled from the predecessor's lessons:
1. **Protocol, not keystrokes** — all agent I/O via harness event streams; terminal is
   for humans.
2. **One critical section for out+index+wake; one predicate for match** — eliminate the
   lost-wakeup class structurally, and property-test it.
3. **Supervision tree is first-class data** — completion events route to the spawner by
   structure, not by payload fields.
4. **Structure over prose** — directed workers *cannot* claim second tasks because the
   claim tool isn't in their capability set; templates are composed from single-source
   fragments and linted in CI.
5. **One store** (SQLite) until data demands more; no analytics tier before agents
   reliably produce trails.
6. **Local-first, eventually-convergent multiplayer** via per-actor git refs; ephemeral
   data never touches git.
7. **Budgets are enforced, not observed** — ledger wired to graduated intervention from
   day one; telemetry attribute keys pinned and schema-tested.

## 10. Open questions

- Notes-on-anchor-objects vs. git-bug-style per-entity refs for free-standing tuples
  (leaning: per-entity refs under `refs/rk/`; notes proper for commit-anchored facts).
- ~~How much of the predecessor's workflow engine to carry~~ **Decided**: keep the predecessor's CUE form
  factor (schema, aspects, triggers) via cuengine — see §9a. Remaining sub-question:
  temp-package unification vs. `cue` CLI shell-out for the evaluate step (prototype
  both, pick by ergonomics/latency).
- ACP adapter timing — watch whether Codex/Claude converge on it.
- herdr protocol pinning strategy across its pre-1.0 releases (generate types from
  `herdr api schema --json` in CI and diff).

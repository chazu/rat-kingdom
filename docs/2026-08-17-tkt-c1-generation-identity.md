# Generation identity: a spawn ULID as the join key, names as display labels

**Ticket:** TKT-01M08HB55EJF86WPP57F2M6MJS (strategic-review program C1 / S3a)
**Author:** Basil-7, 2026-08-17
**Status:** design — reviewed, not implemented. Migration is a separate ticket per consumer.
**Program context:** `docs/2026-08-17-rk-ticket-program.md`

---

## 1. The defect class

A rat's **name** ("Basil-7", "Whisker") is currently doing two incompatible jobs
at once:

- it is a **display label** — what an operator types at `rk log`, what a branch
  is called, what a log line reads as;
- it is a **durable identity key** — the thing tuples, events, logs, registry
  rows, branches and supervision edges are joined on.

Names are drawn from a finite generator (`rk_core::names::next_name`) over a
mutable taken-set. Tuples and logs are **durable and immortal**; a name's claim
on them is not. Any consumer that joins on a name alone can therefore
**time-travel**: match a record written by a different rat that carried the same
name at a different time.

This has fired twice, on two different keys, and been fixed twice locally:

- **TKT-136** made archiving return names to the pool. 24 names ended up naming
  two unrelated rats.
- **TKT-146** was the consequence: a `wait` on a fresh rat matched a
  **two-day-old namesake's** `harness_result` and returned in milliseconds. The
  following `evaluate` judged a stranger's work; the `dismiss` behind it
  SIGTERMed the live rat one second into its task (`code: None`, no session,
  zero tokens). Whole workflows reported success having done nothing.
- **TKT-159** found the same read degrading to *unbounded* when the registry
  record was unreachable — the exact defect, still live on a fallback path.
- **TKT-160** found the floor necessary but not sufficient: it separates
  generations, not the *turns* within one, so a mid-flight "tests still running"
  turn satisfied the wait.

Each fix was correct and each was **local to one reader**. The class was never
retired, because the underlying statement — *"a name identifies a rat"* — is
still false and still load-bearing in ~20 places.

### 1.1 What actually holds the system up today

Two props, both fragile:

1. **`Registry::reserve_name` no longer recycles** (`agents.rs:297`). The
   taken-set unions live agents, reservations **and the archive**, so a name is
   never reissued. This is a *policy*, enforced in one function, with no type
   preventing a future change (a prune that drops archived rows, a
   cross-castle merge, a hand-edited `agents.json`) from silently re-opening the
   hole.
2. **`Pattern::for_agent_since`** (`tuple.rs:345`) — a `payload_search` on
   `"agent":"<name>"` plus an `after_id` floor at the record's `created_at`. A
   *time-window* disambiguator bolted onto a non-unique key.

Prop 2 is the honest signal that prop 1 is not trusted. And prop 2 has real
costs: it needs a reachable registry record to compute the floor (TKT-159's
fallback), it needs every reader to remember to apply it, and it is invisible to
the type system — a plain `Pattern::category(...).identity(...)` compiles fine
and is wrong.

### 1.2 The de-facto generation key already in the tree

Three subsystems have independently converged on `(name, created_at)`:

| Site | Shape |
|---|---|
| `agents.rs:170` `AgentRecord::generation()` | `(&str, DateTime<Utc>)` |
| `agent_log.rs:89` `Generation { agent, start, end }` | name + spawn instant window |
| `factory_analytics.rs:183` run id | "name + creation instant" |
| `coordinator.rs:286` | `generation: DateTime<Utc>` = `agent.created_at` |

This is the right *idea* with the wrong *representation*. It is a composite key
that every consumer must reconstruct by hand; it is only unique because
`created_at` has millisecond resolution and generations "never overlap in time";
it cannot be embedded in a branch name or a filename without escaping; and it
degrades to a **range predicate** rather than an equality predicate, which is
why `agent_log::read` has to *window* a legacy file instead of *selecting* from
it.

---

## 2. The design

### 2.1 The type

Mint one **spawn ULID** per generation, at the moment the registry row is
created, and treat it as the sole identity of that rat-generation.

```rust
/// Identity of ONE generation of a rat: minted once at spawn, never reused,
/// never recycled, and never derived from a name.
pub struct SpawnId(Ulid);
```

Placed in `rk-core::id` alongside `RecordId`, which it deliberately mirrors.

**Why a ULID and not a `(name, created_at)` tuple, a UUID, or a counter:**

- **It is a single opaque string.** It embeds in a filename, a git branch, a
  JSON payload field, a `payload_search` substring, an env var, and a CLI
  argument with no escaping and no composite-key reconstruction at each site.
- **It carries its own timestamp.** `SpawnId::timestamp_ms()` is the spawn
  instant, so `RecordId::floor_at(spawn.timestamp())` is derivable *from the key
  itself* — the TKT-159 "no reachable registry record" fallback disappears,
  because the floor no longer requires a registry lookup. This is the property a
  UUIDv4 would not give us and is the reason to reuse the existing ULID
  dependency rather than introduce another.
- **It is lexicographically sortable**, so `generations_of` sorts without
  parsing and the archive's ordering survives a text merge (`cat_sort_uniq`,
  Phase 6) — same argument that made `RecordId` a ULID.
- **It converts an in-range predicate into an equality predicate.** Every
  time-window disambiguator in §1.1 becomes `spawn == <id>`.

**Display.** A `SpawnId` is *never* shown alone to an operator. The rendered
form of a rat is `Name@<short>`, e.g. `Basil-7@01M08HB5`, where short is the
first 8 ULID chars. Names stay the ergonomic handle; the suffix appears only
where disambiguation matters (`rk list`, `rk log` generation picker, error
text). The full id appears in payloads, paths and `--json`.

### 2.2 The rule

> **Names are display labels. `SpawnId` is the join key.**
> Any read that selects records *belonging to a rat* keys on `SpawnId`.
> Any read that resolves *operator input* keys on name, and resolves it to a
> `SpawnId` exactly once, at the edge.

That second clause is what makes this tractable: name→`SpawnId` resolution is
the **only** place ambiguity is allowed, it is a CLI/RPC-boundary concern, and
when a name is ambiguous the resolver can say so instead of silently picking.

### 2.3 The invariant we get for free

`reserve_name`'s non-recycling policy stops being load-bearing. It remains
desirable (an operator should not see two live `Basil-7`s), but a regression in
it becomes a **cosmetic** bug rather than a workflow-corrupting one. That is the
structural retirement of the TKT-136/146 class: the failure mode is no longer
reachable from a naming-policy change, because no correctness-critical read
consults a name.

### 2.4 Compatibility posture

Every record written before the migration has **no** `SpawnId`. The design must
not orphan them.

- `AgentRecord.spawn: Option<SpawnId>` with `#[serde(default)]`. `None` = a
  pre-migration generation.
- **Backfill on load, not by rewrite.** `Registry` load derives a *synthetic*
  `SpawnId` for a `None` record from `RecordId::floor_at(created_at)`'s ULID —
  deterministic, stable across restarts, sorts correctly, and collides only if
  two records share a name *and* a millisecond (which the current invariant
  already forbids). No migration script, no `agents.json` rewrite, idempotent.
- **Readers stay dual-key through one release.** Each migrated predicate becomes
  "match on `spawn` if the tuple carries one, else fall back to the existing
  `for_agent_since` name+floor test". Old tuples keep matching; new tuples match
  exactly. The fallback arm is deleted in a follow-up once the oldest live tuple
  postdates the cutover.

This dual-key window is the single largest risk in the program and the reason
the migration is per-consumer tickets rather than one sweep: a half-migrated
*pair* (producer stamps `spawn`, consumer still name-only, or the reverse) is
fine, but a consumer migrated to `spawn`-**only** ahead of its producer silently
matches nothing — which reads as a hung `wait`, not as an error. **Every
consumer ticket below must land its reader's fallback arm before, or in the same
commit as, its producer's stamp.**

---

## 3. Consumer census

Every site that joins on an agent name. Grouped by seam, each with a migration
note. `⚠` marks a consumer where a name join is currently *correctness-critical*
(a wrong match causes wrong work, not a wrong display).

### A. Tuple predicates — `rk-core`

| # | Consumer | File | Joins on | Migration note |
|---|---|---|---|---|
| A1 ⚠ | `Pattern::for_agent_since` | `tuple.rs:345` | `payload_search "\"agent\":\"<name>\""` + `after_id` floor | **The keystone.** Add `Pattern::for_spawn(category, identity, SpawnId)` searching `"spawn":"<id>"`, no floor needed. Keep `for_agent_since` during the dual-key window; mark it `#[deprecated]` and delete when the last caller moves. Its doc comment is the canonical account of TKT-146 — move that prose to `for_spawn`, do not lose it. |
| A2 | `Pattern::for_workflow_instance` | `tuple.rs:377` | `"instance":"<wf-id>"` | **No change.** Already the correct shape — an instance id is minted once and never reused. This is the precedent `SpawnId` generalises; its doc comment explicitly notes it needs no floor *because* its key is unique. Cite it in review. |
| A3 ⚠ | `Tuple.instance` (structural prefix field) | `tuple.rs:192` | the writing agent's **name** | This is the tuplespace's per-writer field and it is a *name* today. Widen to accept a `SpawnId` string; render as the name in `rk scan` output. See D2 for the authorization half. **Highest-blast-radius item in the census** — every tuple ever written carries it. Recommend: keep `instance` as the display name, add a `spawn` payload field, rather than changing the structural prefix. Cheaper, and the prefix is replicated cross-castle. |

### B. Workflow execution — `rk-daemon/workflow_exec.rs`

| # | Consumer | File | Joins on | Migration note |
|---|---|---|---|---|
| B1 ⚠ | `result_pattern` (`wait` / `wait_all` predicate) | `:2251` | name + `generation_floor` | **The TKT-146 site.** → `Pattern::for_spawn(Event, "harness_result", spawn)`. Preserve the TKT-160 producer-side rule verbatim: one `harness_result` per generation, not per turn. Migrating the key does **not** relax that. |
| B2 | `generation_floor` | `:2274` | `supervisor.status(agent).created_at`, falling back to instance `started_at` | **Deleted outright.** Its entire job — synthesise a floor for a non-unique key — evaporates. Deleting it also retires the TKT-159 fallback path, which is the one arm that could still degrade to an unbounded read. |
| B3 ⚠ | `FannedAgent { agent: String, branch, ticket }` | `:2210` | name | Add `spawn: SpawnId`. `for_each`/`wait_all`/`dismiss_all` then join on it. A fan-out is the highest-risk shape: N same-shaped rats, one join, and `dismiss_all` acts on the result. |
| B4 ⚠ | `join` / `dismiss_fanout` | `:2430`, `:2490` | fan-out entries by name | Follows B3. Note `dismiss_all onlyClean` reads `ctx.previous_result.results[]` and pairs each entry to an agent — pair on `spawn`, not on `agent`. |
| B5 ⚠ | spawn-side wait floor | `:1603`, `:1641` | name + floor | Same rewrite as B1; the comment at `:1603` ("a name keys a…") is the acknowledgement of the defect and should be replaced, not amended. |
| B6 | `Step::Dismiss` → `supervisor.dismiss(&agent, …)` | `:1510` | name | Pass `SpawnId`. See C4. |
| B7 | `ctx.active_agent` | `:376` | name string | Widen to carry `(name, SpawnId)`; it is the source B1/B3 read from. |
| B8 | `dismiss_orphaned_instance_agents(instance)` | `supervisor.rs:3185` | selects by `workflow_instance`, acts by name | Selection is already instance-keyed (correct); the *action* is name-keyed. Pass `SpawnId` through. |

### C. Supervisor & completion — `rk-daemon/supervisor.rs`

| # | Consumer | File | Joins on | Migration note |
|---|---|---|---|---|
| C1 ⚠ | `declared_done` (`task_done` lookup) | `:2526` | `for_agent_since(Event,"task_done",name,generation)` | → `for_spawn`. This decides `declared_done`, which TKT-173 made a gate-visible field — a wrong match here flips a workflow verdict. |
| C2 ⚠ | `claim_completion` | `:2451`–`:2512` | name + generation | Stamp `spawn` into the published claim. The TKT-160 one-result-per-generation rule is expressed here; restate it in terms of `SpawnId` (it becomes *literally* per-`SpawnId`, which is clearer than "per generation"). |
| C3 ⚠ | `route_completion` → published `harness_result` payload | `:2643`–`:2706` | writes `"agent": <name>` | **Producer side of B1/C1.** Add `"spawn": "<id>"` to the payload; keep `"agent"` for display and for the dual-key fallback. Must land **before or with** B1/C1 (see §2.4). |
| C4 | `Supervisor::dismiss(name, no_merge)` | `:2945` | name → registry `get` | Accept a resolved target. Dismiss merges a branch and deletes a worktree — a wrong target is destructive and irreversible-ish (`rk revert` exists but needs `merge_commit`). |
| C5 | `task_done` drain loop | `:1088`–`:1108` | `for_agent_since` | → `for_spawn`. |
| C6 | Agent env `RK_AGENT` | `:3952` | name | Add `RK_SPAWN=<id>`. **Do not remove `RK_AGENT`** — the prime prompt, the standing conventions, and every `rk` CLI call read it. `RK_SPAWN` becomes what `rk out`/`rk done` stamp; `RK_AGENT` stays the label. |
| C7 | `instruction_base` / branch name `rat/<name>/<task>` | `:4376` | name | **Leave as-is.** A branch name is a display artifact and must stay human-typable. Non-recycling names keep it unique in practice; a collision here is cosmetic once B/C are migrated. Record this as a deliberate non-goal. |
| C8 | `AgentRecord.parent: Option<String>` | `agents.rs:70` | parent **name** | ⚠-adjacent: the module doc says "completion routing walks this structure, never payload fields". That structure is name-edged, so a recycled name reparents a rat. → `Option<SpawnId>`, with the name kept for display. Backfill: `None` stays `None`; a `Some(name)` resolves against the archive at load. |

### D. Registry & authorization — `rk-daemon/agents.rs`, `server.rs`

| # | Consumer | File | Joins on | Migration note |
|---|---|---|---|---|
| D1 ⚠ | `Registry.agents: HashMap<String, AgentRecord>` | `agents.rs` | name is the **map key** | Rekey to `SpawnId`; keep a `HashMap<String, SpawnId>` name index for the *live* set only. This makes "two live rats with one name" representable-but-rejected rather than data-loss-on-insert. Largest mechanical diff in the program; do it in its own ticket with no behaviour change. |
| D2 ⚠ | `space.out` caller check — "agents may only write tuples for their own instance" | `server.rs:5047` | `params.instance != caller` where caller = `RK_AGENT` | Compare `SpawnId`s. Until then the check is only as strong as name uniqueness. Interacts with A3 — decide A3's "keep `instance` as name, add `spawn` payload" recommendation **first**, since this check reads that field. |
| D3 | `Registry::get(name)` | `agents.rs:379` | name | Becomes the *resolver* (§2.2): name → `SpawnId`, live-set-first, then newest archived, and **error on ambiguity** instead of silently taking one. |
| D4 | `AgentRecord::generation() -> (&str, DateTime)` | `agents.rs:170` | composite | → `SpawnId`. Its doc comment already narrates why the composite exists ("the archive/persist crash window"); that window is keyed correctly by a `SpawnId` with no prose needed. |
| D5 | `generations_of(name)` | `agents.rs:410` | name → `Vec<DateTime>` | → `Vec<SpawnId>`, sorted (free: ULIDs sort by mint time). |
| D6 | `insert` idempotency / archive dedupe | `agents.rs:490`–`:546` | `a.generation() == record.generation()` | Direct substitution; the crash-window reconciliation rule is unchanged, just typed. |
| D7 | `reserve_name` / `release_name` | `agents.rs:297` | name pool | **Unchanged in behaviour, downgraded in criticality.** Add a doc note that non-recycling is now a UX guarantee, not a correctness one, so a future prune that drops archived rows is no longer a latent TKT-146. |
| D8 | `agent.list` name filter | `server.rs:2269` | `by_name` | Operator-input resolution — stays name-keyed by design (§2.2). Render `Name@short` when the filter is ambiguous. |

### E. Agent logs — `rk-daemon/agent_log.rs`, `server.rs`

| # | Consumer | File | Joins on | Migration note |
|---|---|---|---|---|
| E1 | Log path `<agent>.<generation>.jsonl` | `agent_log.rs` module doc | name + spawn instant | → `<agent>.<spawn>.jsonl`. Name stays in the filename for `ls`-legibility; the `SpawnId` is what makes it unique. Legacy `<agent>.jsonl` and `<agent>.<rfc3339>.jsonl` paths must still be *readable*. |
| E2 | `Generation { agent, start, end }` | `agent_log.rs:89` | name + time window | The window exists **only** because a legacy file interleaves two rats' lines. Keep `Generation` for reading legacy files; new files need no window at all — the path is the key. Do not delete the windowing code; it is the compatibility arm. |
| E3 | `log_generations(name)` | `supervisor.rs:663` | name → ordered windows | → ordered `SpawnId`s. |
| E4 | `rk log --generation N` ordinal picker | `server.rs:1914`–`:1944` | 1-based index into `generations` | Accept `rk log <name>` (newest), `rk log <name> --generation N` (ordinal, kept), **and** `rk log <spawn-id>` (exact). The ordinal is a positional index into a mutable list — it silently re-points when a generation is pruned. Adding the exact form is the actual fix; keep the ordinal for ergonomics. |
| E5 | `stream_log` match | `server.rs:1804`–`:1809` | `rec.agent == agent && rec.generation == generation` | Direct substitution to `rec.spawn == spawn`. |
| E6 | `AgentLog::delete_for` (`rk prune --reap-logs`) | `agent_log.rs` | generation key | Follows E1. A destructive path — verify it cannot reap a live generation's file when the key changes representation. |

### F. Reactor, landing, analytics, inbox

| # | Consumer | File | Joins on | Migration note |
|---|---|---|---|---|
| F1 ⚠ | Reactor steward trigger | `reactor.rs:969`–`:1030` | `harness_result` payload; templates `{{tuple.payload.branch}}`, `{{tuple.payload.target}}` | Reads C3's payload. Expose `{{tuple.payload.spawn}}` so a trigger can pin a spawn. Also: `.rk`/`~/.rat-kingdom` trigger `.cue` files are **deployed copies that drift from `examples/`** (TKT-176) — a payload-shape change must be announced, not assumed to propagate. |
| F2 ⚠ | Reactor `declared_done` / `is_error` gate | `reactor.rs:1003`–`:1007` | payload fields | No key change, but it acts on whichever `harness_result` matched. Correct only once F1's match is spawn-keyed. |
| F3 | Landing pipeline enqueue | `landing.rs:61` | fed a `harness_result` | Carry `SpawnId` on the landing request so a queued land is attributable after the rat is archived. |
| F4 | `factory_analytics` run id | `factory_analytics.rs:183`–`:227` | "name + creation instant" | Replace the hand-built composite with `SpawnId`. Note `:227`'s existing caveat that agent timestamps are a *generation* id, not a workflow-instance id — that distinction survives and gets clearer. |
| F5 | `coordinator.rs` `generation: DateTime` | `:286`, `:485` | `agent.created_at` | Direct substitution to `SpawnId`. |
| F6 | `inbox` rows (`agent-failed`, `agent-orphaned` → `rk respawn <name>`) | `inbox.rs:194` | name in the **action string** | Stays name-keyed: the action is a command a human types. Render `Name@short` when ambiguous. Non-goal for migration. |
| F7 | `inbox.rs:500` `.get("agent")` payload read | `inbox.rs:500` | payload name | Prefer `spawn` when present; fall back to `agent`. |

### G. Test & fixture surface

| # | Consumer | File | Migration note |
|---|---|---|---|
| G1 | `rk-fixture-done` | `bin/rk-fixture-done.rs:47` | Writes "the field `Pattern::for_agent_since` searches for". Must learn to stamp `spawn`, or every migrated wait test hangs. **Migrate this first** — it gates the whole test surface. |
| G2 | `workflow_stale_result.rs` | tests | The regression test for this exact class. It must keep passing *unchanged* through the dual-key window (that is the compat proof), then gain a spawn-keyed twin. |
| G3 | `tuple.rs:504` `for_agent_since_rejects_a_namesake_predecessors_tuple` | unit test | Keep. Add the `for_spawn` equivalent. Delete only with `for_agent_since`. |
| G4 | `space_cmds.rs:878` | rk-cli test | Same substring dependency as G1. |
| G5 | `mid_flight_result.rs`, `workflow_crash_gate.rs`, `reactor.rs` tests, `agent_lifecycle.rs`, `repository_policy.rs` | tests | All construct `harness_result` tuples by hand. Each needs a `spawn` field once C3 lands. Mechanical but broad — **~6 test binaries**, budget for it. |

**Not in scope (deliberate non-goals), recorded so a reviewer does not re-raise them:** C7 branch naming, D8 / E4-ordinal / F6 operator-input paths, and the `RK_AGENT` env var (C6). All three are name-keyed *by design* under §2.2's second clause.

---

## 4. Migration sequence

Ordering is forced by §2.4's producer-before-consumer rule.

1. **Type + backfill.** `SpawnId` in `rk-core::id`; `AgentRecord.spawn: Option<SpawnId>` with load-time synthesis (D4/D6). No reader changes. Inert.
2. **Producers stamp.** C3 (`harness_result`), C2 (claim), `rk done`/`task_done` (C5), G1 fixture. Everything still name-keyed; new tuples merely carry an extra field.
3. **Registry rekey.** D1/D3/D5. Pure mechanical; no behaviour change.
4. **Critical readers, dual-key.** B1/B2/B5 (wait), C1 (declared_done), B3/B4 (fan-out), D2 (authorization). Each ships its fallback arm.
5. **Logs.** E1–E6.
6. **Periphery.** F1–F7, C8 (parent edges).
7. **Delete the fallbacks.** `for_agent_since`, `generation_floor`, the name-only arms — once the oldest live tuple provably postdates step 2.

Steps 1–3 are non-breaking and can land in any order relative to each other.
Step 7 is the one that actually retires the class; until it lands, the fallback
arms mean a naming regression is still *reachable* — just no longer *silent*.

---

## 5. What this retires, and what it does not

**Retires structurally:** TKT-136/146/159 — a durable record matching a rat that
did not write it. Not by policy (names don't recycle) but by construction (the
key cannot collide).

**Does not retire:** TKT-160 — turn-vs-generation. A generation legitimately
produces many turns, and `SpawnId` is per-*generation*, not per-turn. The
producer-side rule (one `harness_result` per generation, `Supervisor::claim_completion`)
remains the only thing preventing a mid-flight turn from satisfying a `wait`.
**Any implementer who reads "the key is now unique" as licence to reintroduce a
per-turn `harness_result` will re-open TKT-160.** Stated here because that is
the most likely misreading of this document.

**Does not address:** cross-castle name collisions under `rk-sync`. Two castles
independently minting "Basil-7" is a real and separate shape. `SpawnId` happens
to fix it (ULID randomness makes independent mints disjoint), but no consumer in
§3 is a replication path, so it is untested by this design. Worth its own
ticket.

---

## 6. Acceptance

- [x] Design listing every name-joining consumer with a migration note — §3, 38 entries across 7 seams.
- [x] Type specified with rationale for ULID over the alternatives — §2.1.
- [x] Structural retirement of the TKT-136/146 class argued — §2.3, §5.
- [x] Compatibility posture for pre-migration records — §2.4.
- [x] Ordering constraint that makes the migration safe — §2.4, §4.
- [ ] Implementation — **out of scope**, one ticket per §4 step.

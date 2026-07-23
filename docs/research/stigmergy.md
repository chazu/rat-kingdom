# Encouraging stigmergy in the rat kingdom

*Research task `research-stigmergy` (read-only analysis). Author: Scamper.*

## Question

How can rat-kingdom encourage **stigmergy** — indirect coordination where rats
act on shared traces in the environment (tuplespace, tickets, worktrees,
markers) rather than through direct messages or the operator? This report maps
the current coordination substrate, finds where stigmergic mechanisms fit the
system's *actual* primitives, and proposes concrete, ranked changes with the
tuplespace/ticket/CLI edits each requires.

---

## 1. How coordination works today

### 1.1 The tuplespace is the substrate (and the audit log)

All coordination flows through a single Linda-style tuplespace: `out` (write),
`in`/`take` (destructive read), `rd` (blocking non-destructive read), `scan`
(non-blocking read). It is implemented over SQLite in
`crates/rk-space/src/lib.rs` and `crates/rk-space/src/store.rs`, with a
lost-wakeup-free design where one mutex guards the store and the waiter list
(`lib.rs:1-14`, `out` at `lib.rs:72`).

A tuple is `(category, scope, identity, instance)` + JSON payload
(`crates/rk-core/src/tuple.rs:108-130`). Two axes carry coordination meaning:

- **Category** encodes *epistemic weight*, roughly ordered
  (`tuple.rs:13-41`): `Fact` > `Convention` > `Task` > `Available` > `Claim` >
  `Obstacle`/`Need` > `Artifact` > `Event` > `Message`, plus `Suggestion` and
  `Endorsement`. Note that `Convention` is documented as "Shared norms,
  **promoted from proposals at quorum**" and `Endorsement` as "A vote of support
  for a suggestion (one per agent, idempotent)" — a quorum mechanism is
  *anticipated in the type model but not built* (see §1.5).
- **Lifecycle** encodes hygiene (`tuple.rs:89-103`): `Furniture` (daemon-only,
  permanent, never consumable), `Session` (agent-written, lives for the parent
  task), `Ephemeral` (consumable via `in`, TTL-collected if unclaimed).

### 1.2 Reads are flat and oldest-first — no ranking or aggregation

`store.query` always sorts `ORDER BY id ASC` (`store.rs:137`), i.e. oldest ULID
first. There is no recency weighting, no relevance score, no count/aggregation
across matching tuples, and no notion of a tuple's "strength". `scan` returns
the whole flat list (`lib.rs:112`). Payload search is a literal substring `LIKE`
(`store.rs:130-133`) — the same predicate as `Pattern::matches`
(`tuple.rs:199-227`), deliberately kept identical to avoid lost wakeups.

**Consequence for stigmergy:** a rat reading `scan obstacle myrepo` sees an
undifferentiated pile. Ten rats hitting the same wall produce ten equal
obstacle tuples with no signal that this is a *hot* trail worth ten times the
attention.

### 1.3 The only "reaction to a trace" pathways are narrow and hard-coded

The broadcast event feed (`Space::subscribe`, `lib.rs:186`) has exactly **one**
consumer: `stream_watch` in `crates/rk-daemon/src/server.rs:318`, which just
relays tuples to `rk watch`. **There is no general daemon-side reactor** that
watches the space and acts on thresholds. The GC loop is explicitly TTL-only:
its comment reads "escalation/analytics live elsewhere" (`server.rs:183`) —
but nowhere currently.

The few reactive behaviours are point solutions:

- **Completion watcher** — per agent, the supervisor spawns a blocking `rd` on
  `event/task_done` filtered by agent name (`supervisor.rs:347-356`); when the
  rat's `rk done` tuple lands it flips state, emits a `harness_result` event,
  sends the structural parent a `child_completed` `Message`, and (for a
  ticket-rat) sets the ticket `done` (`route_completion`,
  `supervisor.rs:600-660`).
- **Budget enforcement** — on cost update, at the warn threshold it emits an
  `Obstacle` tuple *and* steers the rat; at the cap it kills it
  (`supervisor.rs:537-596`).
- **Ticket atomic claim** — compare-and-set `open → in_progress`, serialized so
  a fan-out of backlog-drains hands each ticket to exactly one drainer
  (`tickets.rs:211-232`; tested at `tickets.rs:572-629`).
- **Ticket dependency DAG** — `depends_on` edges, `rk ticket ready` = deps
  satisfied, completion unblocks dependents (`tickets.rs`, README "Tickets").

### 1.4 Sugar commands are the schema-enforcement + identity layer

`rk done|obstacle|need|claim|out artifact` build their payloads from `RK_*`
spawn env so rats cannot write malformed coordination tuples
(`crates/rk-cli/src/space_cmds.rs:1-6`, `report`/`done`/`claim` at `:233-299`).
Notably: **`rk claim` exists and is wired** (`main.rs:55-56,332`; sugar at
`space_cmds.rs:263-271`) **but is deliberately never taught in priming** — the
rat and reviewer templates are unit-tested to contain no `rk claim`
(`prime.rs:213-230`). Rats are told to *file* facts/needs/tickets and let the
orchestrator route, not to self-assign (`prime.rs`, FRAGMENT_SINGLE_TASK
`:109-116`).

### 1.5 What rats are actually told (the behavioural contract)

`FRAGMENT_SPACE` (`prime.rs:20-33`) is the whole stigmergy instruction set:

> "You coordinate with other agents stigmergically through a shared tuplespace…
> Before starting, read `fact` and `convention` tuples for your repo scope and
> the `system` scope."

So the *only* trace-reading a rat is currently primed to do is: read `fact` and
`convention` before starting. It is **not** told to read `claim`, `obstacle`,
`need`, or `artifact` tuples — so it cannot de-conflict against, or build on,
what peers are doing. Writing is covered (`obstacle`/`need`/`artifact`/`done`);
*reading peers' live traces is not*.

### 1.6 Multiplayer replication already assumes shared traces

`rk-sync` replicates facts, tasks, obstacles, and events across castles via
per-castle git-notes refs with union merge and earliest-claim-wins arbitration
(README "Multiplayer"; `crates/rk-sync`). Any stigmergic marker built on
`fact`/`task`/`obstacle`/`event` categories replicates for free; ephemeral
tuples never leave the local daemon.

### Summary of the gap

The system has an excellent **write-side** stigmergy story (typed traces, a
shared durable space, replication) and a nearly absent **read-side / feedback**
story: no aggregation, no ranking, no decay beyond crude ephemeral TTL, no
quorum, and no daemon reactor to turn accumulated traces into action. The type
model (`Suggestion`/`Endorsement`/`Convention`) even *names* a quorum mechanism
that was never implemented.

---

## 2. Where stigmergic mechanisms fit

| Seam in the code | What it enables |
|---|---|
| `Category::{Suggestion,Endorsement,Convention}` unused (`tuple.rs`) | Pheromone-vote → quorum → promoted norm (the flagship, already designed) |
| `store.query` flat `ORDER BY id ASC` (`store.rs:137`) | Gradient scans: rank by recency × weight × duplicate-count |
| Single feed consumer `stream_watch` (`server.rs:318`); GC comment "escalation lives elsewhere" (`server.rs:183`) | A daemon **tuple reactor**: pattern → threshold → action (the enabler) |
| Ephemeral TTL GC only (`gc_expired`, `store.rs:153`) | **Evaporation** + **reinforcement** of claims/obstacles (ant-trail decay) |
| Priming reads only `fact`/`convention` (`prime.rs:27-28`) | Read-before-work: de-conflict off peers' `claim`/`artifact` trails |
| `rk claim` wired but untaught (`main.rs:55`, `prime.rs:213`) | Fine-grained claim markers for indirect file/area de-confliction |
| Ticket DAG + atomic claim (`tickets.rs`) | Ticket "heat" gradient; obstacle-quorum → auto-file ticket |
| `rk out artifact` payloads (`space_cmds.rs`) | Artifact↔need backlinks: an evaporating memory of solved problems |

---

## 3. Proposals (ranked)

Ranked by **coordination benefit per unit of change**. Dependencies are called
out; Proposal 4 (the reactor) is the foundational enabler for 3, 6, and 8.

---

### P1 — Read-before-work claim trails (behavioural; near-zero code) ⭐ start here

**Mechanism.** Ant-style trail-following for de-confliction. Before a rat
touches a file/area, it reads the space for peers' claims and recent artifacts;
on entry it drops a fine-grained `claim` marker scoped to what it is about to
edit. Peers reading the trail steer around occupied ground. This is pure
stigmergy: no rat messages another; they read and extend a shared trace.

**Changes.**
- *Priming* (`prime.rs` `FRAGMENT_SPACE`): add two lines — "Before editing an
  area, `rk scan claim <repo>` and `rk scan artifact <repo>` to see what peers
  are already doing; avoid their files. On entry, mark your area with
  `rk claim <area>`." This flips `rk claim` from wired-but-untaught
  (`main.rs:55`, currently asserted absent in `prime.rs:213-230`) to a taught
  worker primitive. Update those tests accordingly.
- *Sugar* (`space_cmds.rs:263-271`): let `claim` carry an optional `--area`
  (path/glob) and write it `Ephemeral` with a TTL so an abandoned claim
  evaporates (composes with P5).
- No engine change.

**Benefit.** Directly attacks the concurrency pain already in project memory
(worktree/merge collisions from concurrent spawns). Parallel rats on one repo
self-organise off each other's traces instead of colliding at merge time — the
purest, cheapest stigmergy win available.

**Risk to watch.** Over-claiming / starvation (P-risks §4): TTL evaporation and
area-scoping keep claims from becoming permanent no-go zones.

---

### P2 — Suggestion → Endorsement → Convention quorum promotion ⭐ flagship

**Mechanism.** The pheromone-deposit-and-threshold loop the type model already
anticipates (`tuple.rs:19-40`). Any rat deposits a `Suggestion` (system scope).
Other rats reinforce it by depositing an idempotent `Endorsement` (one per
`instance`/agent). When distinct endorsements cross a quorum, the trace
"crystallises": the daemon promotes it to a `Convention` tuple — which every rat
already reads before starting (`prime.rs:27-28`). Collective norm-formation with
no operator and no direct messaging.

**Changes.**
- *CLI*: add `rk suggest "<text>"` and `rk endorse <suggestion-id>` sugar
  (mirroring `report`/`claim` in `space_cmds.rs`; wire in `main.rs` beside
  `Obstacle`/`Need`/`Claim` at `:52-56,330-332`). `endorse` writes
  `Endorsement` with `identity = <suggestion-id>`, `instance = RK_AGENT`;
  re-endorsing is idempotent (dedupe on `(identity, instance)`).
- *Reactor* (needs P4, or a scoped feed consumer): count **distinct-instance**
  endorsements per suggestion; at `quorum` (config, e.g. 3 or `ceil(fleet/2)`)
  `out` a `Convention` (system scope, `Furniture` so it persists and is never
  consumed) citing the suggestion and endorsers.
- *Decay*: write `Suggestion`/`Endorsement` `Ephemeral` with a voting-window
  TTL, so a proposal that fails to reach quorum in the window evaporates
  (`gc_expired`, `store.rs:153`) instead of lingering forever.
- *Replication*: conventions/suggestions on system scope replicate across
  castles via `rk-sync` for free.

**Benefit.** Turns scattered individual insight into durable, fleet-wide policy
without a human bottleneck — the highest-ceiling mechanism, and the one the
codebase was clearly designed to grow into. Pairs with P6 to actually change
behaviour.

---

### P3 — Obstacle/need coalescence → pheromone strength → auto-ticket at quorum

**Mechanism.** Repeated pain is a pheromone gradient. Instead of N equal
obstacle tuples (§1.2), a reactor buckets obstacles/needs by a normalised topic
key and maintains a single aggregate marker whose `strength` increments with
each new report (reinforcement). When `strength` crosses a threshold, the
reactor auto-files a `Task`/ticket in the repo scope — the fleet's shared
frustration becomes actionable backlog on its own.

**Changes.**
- *Reactor* (needs P4): on each `Obstacle`/`Need`, compute a topic key
  (identity + normalised text / payload_search bucket); upsert an aggregate
  `Obstacle` (identity = topic key) with `payload.strength += 1` and a refreshed
  TTL. At `strength >= threshold`, call the existing ticket path
  (`tickets.rs` `new`) with a body that links the contributing tuples, then
  reset/evaporate the aggregate.
- *Scan surfacing*: `rk scan obstacle` sorts by `strength` desc (composes with
  P7) so the operator and rats see the hottest wall first.

**Benefit.** Converts the flat obstacle pile into a demand gradient and closes
the loop to the durable backlog automatically. The operator stops polling
`rk scan obstacle` to notice patterns; the pattern files itself.

---

### P4 — Daemon tuple-reactor framework (foundational enabler)

**Mechanism.** The keystone primitive P3/P6/P8 depend on: a single feed
consumer in the daemon that runs registered *reactions* — `(pattern, threshold,
debounce) → action` — over the live space. Today only `stream_watch` consumes
the feed (`server.rs:318`) and GC explicitly defers escalation elsewhere
(`server.rs:183`); this gives "elsewhere" a home.

**Changes.**
- *Daemon*: add a `reactor` task next to the GC/sync loops in `server.rs`
  (`:183-215` pattern) that subscribes (`space.subscribe()`), matches each
  tuple against registered reactions, maintains per-key counters/debounce, and
  fires actions (`out` a tuple, file a ticket, steer). Reactions are the
  concrete rules from P2/P3/P6/P8.
- *Idempotency/lag*: reactions must tolerate the broadcast feed's lossy lag
  (`server.rs:326`), so counters should be recomputed by `scan` at fire time,
  not trusted incrementally — the feed is the *trigger*, `scan` is the *truth*.
- *Config*: thresholds/windows in `config.toml` (`[reactor]`).

**Benefit.** One well-tested mechanism unlocks quorum promotion, obstacle
coalescence, convention injection, and resolution backlinks. Without it each of
those needs its own bespoke feed consumer. Rank it high because of leverage, not
because it ships user-visible value alone.

---

### P5 — Evaporation + reinforcement for claims/obstacles/needs (ant-trail decay)

**Mechanism.** Real pheromone trails fade unless refreshed. Today only
`Ephemeral` tuples are collected, and only by hard TTL (`gc_expired`,
`store.rs:153-163`) — a claim by a dead rat lingers forever (project memory
already notes stale-worktree hazards). Give `claim`/`obstacle`/`need` a decaying
`strength` (or a short TTL) that a *still-active* rat refreshes by re-writing
(reinforcement), and that GC decays otherwise. Active trails stay bright;
abandoned ones evaporate.

**Changes.**
- *Sugar/schema*: these categories default to `Ephemeral` with a modest TTL;
  re-issuing the same `(category, identity, instance)` bumps `expires_at`
  (upsert-refresh rather than duplicate).
- *GC*: extend the GC loop (`server.rs:183`) from "delete past-TTL" to also
  *decrement* a `strength` field and only delete at zero — a smooth fade rather
  than a cliff.
- *Optional*: a periodic "reinforce your claims" nudge for long-running rats.

**Benefit.** Stale claims stop blocking peers (fixes the starvation risk P1
introduces); the space self-cleans; `strength` becomes the raw signal P3/P7
rank on. Directly mitigates the daemon-concurrency stale-state gotchas.

---

### P6 — Convention injection into the spawn prompt (closes the quorum loop)

**Mechanism.** A promoted convention (P2) only matters if it changes behaviour.
Rats are *told* to read conventions (`prime.rs:27-28`) but nothing guarantees
they do, and a mid-flight convention never reaches an already-running rat. At
spawn, compose active system/repo `Convention` tuples directly into the rendered
role prompt.

**Changes.**
- *Priming* (`prime.rs` `render`): before returning, `scan` `Convention` tuples
  for the repo scope + `system` and append a "Standing conventions" section.
  `PrimeContext` (`prime.rs:11-18`) gains access to the space (or the supervisor
  passes pre-scanned conventions in).
- *Optional*: `rk steer` all live rats when a new convention crosses quorum
  (reuse the steer path in `supervisor.rs:653`).

**Benefit.** Makes promoted norms *binding* — the deposited pheromone actually
redirects the colony. Without this, P2 produces conventions no one enforces.

---

### P7 — Gradient / "hot" scans (rank by recency × weight × strength)

**Mechanism.** Let rats follow the strongest trail instead of the oldest one.
Add an optional ranked read that scores tuples by
`category_weight × recency_decay × strength(or duplicate_count)` rather than the
flat `ORDER BY id ASC` (`store.rs:137`).

**Changes.**
- *Query*: `store.query` gains an optional ranking mode; `rk scan --hot` (and a
  `--top N`) computes the score in SQL/Rust and returns highest-first. Category
  weight comes straight from the existing `Category` ordering (`tuple.rs:13-41`);
  strength from P5; recency from `created_at`.
- Keep the default `ASC` path untouched so the waiter-wake predicate invariant
  (`store.rs:1-8`) is unaffected — ranking is read-only sugar.

**Benefit.** Rats and the operator see the salient trace first — the hottest
obstacle, the most-endorsed suggestion, the freshest fact — turning the space
from a log into a navigable gradient. Cheap once P5 supplies `strength`.

---

### P8 — Artifact ↔ need resolution backlinks (evaporating solution memory)

**Mechanism.** When an artifact resolves an obstacle/need, record the link so
the next rat hitting the same wall is pointed at the prior solution — a trail
from *problem* to *known good path*, reinforced each time it is reused.

**Changes.**
- *Sugar*: `rk out artifact … --resolves <obstacle/need-id>` (extend
  `space_cmds.rs`); the payload carries the backlink.
- *Reactor* (needs P4): on a resolving artifact, `take`/archive the matching
  `Obstacle`/`Need` (it is solved) and bump a `trail_strength` on the
  (topic → artifact) pair. A rat that later files the same obstacle gets steered
  to the artifact instead of redoing the work; unused trails decay via P5.
- *Priming*: rats reading obstacles (P1) also see linked resolutions.

**Benefit.** Builds institutional memory as a living, decaying structure rather
than a static doc — repeated problems converge on their known fix without the
operator relaying it.

---

### Ranking rationale

| # | Proposal | Leverage | Effort | Depends on |
|---|---|---|---|---|
| P1 | Read-before-work claim trails | High | Very low | — (P5 hardens it) |
| P2 | Suggestion→Endorsement→Convention quorum | Very high | Medium | P4 (or scoped consumer) |
| P3 | Obstacle/need coalescence → auto-ticket | High | Medium | P4 |
| P4 | Daemon tuple-reactor framework | Enabler | Medium | — |
| P5 | Evaporation + reinforcement | High | Low–Med | — |
| P6 | Convention injection at spawn | High | Low | P2 |
| P7 | Gradient / hot scans | Medium | Low | P5 |
| P8 | Artifact↔need resolution backlinks | Medium | Medium | P4 |

**Suggested build order:** P1 + P5 first (behavioural + hygiene, tiny code,
immediate de-confliction), then P4 (enabler), then P2 + P6 (the flagship norm
loop), then P3, P7, P8.

---

## 4. Risks

**Signal pollution.** More writable trace types (suggestions, endorsements,
fine-grained claims, resolution links) means more noise. A rat that spams
suggestions or over-claims degrades everyone's reads.
*Mitigations:* idempotent endorsements keyed on `(identity, instance)`
(P2, already implied by `tuple.rs:39`); ephemeral TTLs so junk evaporates (P5);
schema-enforcing sugar so payloads stay well-formed (`space_cmds.rs:1-6`);
ranked reads (P7) that let real signal outrank noise; per-agent rate awareness
in the reactor.

**Feedback loops / runaway reinforcement.** A reactor that `out`s tuples in
response to tuples can self-amplify (an auto-filed ticket spawns a rat whose
obstacle re-triggers the same auto-ticket). Quorum + reinforcement could also
create a rich-get-richer lock-in on an early-but-wrong convention.
*Mitigations:* the reactor must recompute counts by `scan` at fire time, not
trust the lossy incremental feed (P4), and debounce/cooldown per key; auto-tickets
carry a dedupe key so the same topic files once until closed; conventions decay
or require re-endorsement to persist, so a bad norm can be un-reinforced;
reactor-emitted tuples are tagged so reactions never trigger on their own output.

**Starvation.** Claim trails (P1) can wall off files no one is actually working;
a hot obstacle (P3) can monopolise the fleet while quiet-but-important needs go
untouched; quorum (P2) can let a majority permanently suppress a minority
proposal.
*Mitigations:* claims are ephemeral and evaporate (P5) so abandoned ground
reopens; the operator still sees everything via `rk watch`/`rk scan` and can
`steer`; ranking (P7) should include an anti-starvation age boost so old unmet
needs eventually rise; quorum thresholds stay low enough that suppression is
hard and suggestions that miss the window simply evaporate rather than being
"rejected".

**Loss of operator legibility.** Stigmergic self-organisation can make it harder
for the human king to understand *why* the fleet did something.
*Mitigations:* every promotion/coalescence/resolution is itself a tuple citing
its inputs (audit-log-as-substrate, §1.1), so `rk watch` and `rk scan` remain a
complete, replayable explanation; reactions log like the budget path already
does (`supervisor.rs:544`).

---

## Appendix — key citations

- Tuplespace primitives & lost-wakeup design: `crates/rk-space/src/lib.rs`
- Storage, flat `ORDER BY id ASC`, TTL GC: `crates/rk-space/src/store.rs:137,153`
- Categories (incl. unused Suggestion/Endorsement/Convention quorum) & lifecycle:
  `crates/rk-core/src/tuple.rs:13-103`
- Priming / behavioural contract (reads only fact+convention): `crates/rk-core/src/prime.rs:20-33`
- Sugar commands; `rk claim` wired-but-untaught: `crates/rk-cli/src/space_cmds.rs`,
  `crates/rk-cli/src/main.rs:52-56,330-332`, `crates/rk-core/src/prime.rs:213-230`
- Only feed consumer + GC "escalation lives elsewhere": `crates/rk-daemon/src/server.rs:183,318`
- Reactive point-solutions (completion, budget, ticket claim/DAG):
  `crates/rk-daemon/src/supervisor.rs:347,537,600` · `crates/rk-daemon/src/tickets.rs:211`

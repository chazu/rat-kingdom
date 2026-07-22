# High-leverage workflows for the rat-kingdom library

## Question

What high-leverage workflows should rat-kingdom add to its workflow library?
First study the existing workflow system — the CUE schema at
`crates/rk-workflow/src/schema.cue` (step types spawn / wait / evaluate /
dismiss / gate, agent profiles, aspects, and `ctx` / `_input` templating) and
the shipped examples in `examples/workflows/` (solo-task, code-review,
research). Then propose a set of new CUE-defined workflows that give the
operator strong leverage over the rat fleet, prioritising **self-improvement
loops**: workflows where the fleet improves its own code, workflows, prompts,
or backlog. For each proposal give a name, one-line purpose, why it is
high-leverage, and the concrete step sequence expressed with the existing
primitives — noting any primitive that would have to be added.

## Direct answer

Add seven workflows, in two tiers:

**Tier 1 — expressible today with zero engine changes** (ship these first):

1. **`workflow-review`** — a rat audits the workflow library and proposes
   refined CUE + tickets.
2. **`backlog-groom`** — a rat decomposes, dedupes, and tags the ticket backlog.
3. **`fix-then-verify`** — a fixer rat implements, an independent verifier rat
   writes a regression test and confirms red→green before merge.
4. **`prompt-refine`** — a rat mines obstacle/need tuples and failed runs to
   propose edits to role prompts and convention tuples.

**Tier 2 — high-value, but each needs one new primitive** (build the primitive,
then ship):

5. **`reviewer-drives-rework`** — a reviewer's APPROVE / REWORK / STOP verdict
   *routes* the next action (merge, loop back to a rework rat, or abort).
   Needs: **conditional branch** + **bounded loop** + a way for `evaluate` to
   read the verdict.
6. **`gated-merge`** — a rat implements, then work parks behind a **human
   approval gate** before auto-merge. Needs: **approval gate** (already
   anticipated in the schema).
7. **`backlog-drain`** — fan out one solo-task rat per ready ticket, in
   parallel, then join. Needs: **dynamic fan-out over a tuplespace query** +
   **parallel join**.

The four Tier-1 workflows are the highest-ROI thing to do *right now* because
each closes a self-improvement loop (the fleet grooms its own backlog, refines
its own prompts, audits its own workflows, and self-verifies its own fixes)
using only `spawn / wait / evaluate / dismiss` — no engine work. Tier 2 is where
the biggest operator leverage lives (routing, approval, fan-out), but each is
gated on one specific, well-scoped primitive.

## Evidence and reasoning

### What the schema and runner actually support today

Read of `crates/rk-workflow/src/schema.cue` and the executor at
`crates/rk-daemon/src/workflow_exec.rs` (`execute`, lines 171–258) establishes
the real semantics — not just the declared shape:

- **`spawn`** launches an agent and sets `ctx.active_agent` and
  `ctx.active_branch` (workflow_exec.rs:201–204). A later spawn bases its
  worktree on `spawn.branch` **or** `ctx.active_branch` (line 196), so
  successive spawns *chain onto the previous rat's branch*. This is exactly how
  `code-review.cue` puts the reviewer on the implementer's branch.
- **`wait`** blocks until a `harness_result` Event tuple for the active agent
  appears, then stores that tuple's payload into `ctx.previousResult`
  (workflow_exec.rs:206–231). It matches by `"agent":"<name>"` in the payload.
- **`evaluate`** CUE-unifies `expect` against `ctx.previousResult` and requires
  the result to be *concrete and valid*; **on failure it returns `Err`, which
  fails the entire instance** (workflow_exec.rs:232–241, `unify_concrete`). It
  is a hard gate, **not** a router — there is no "else" branch.
- **`dismiss`** merges (or, with `noMerge`, keeps) the active agent's branch and
  clears `active_agent`, but **leaves `active_branch` set** (workflow_exec.rs:
  242–251). That residual branch is what lets the next spawn chain on.
- **`gate`** only supports `gateType: "timer"` and simply sleeps
  (workflow_exec.rs:252–254). The schema comment is explicit: *"Timer gate only
  in v1 (human gates arrive with approval tuples)"* (schema.cue:101).
- **`aspects`** splice `before` / `after` steps around matching steps at load
  time (schema.cue:49–61) — a static, compile-time weave, not runtime control
  flow.
- **Templating**: `{{ctx.activeAgent}}`, `{{ctx.activeBranch}}`,
  `{{ctx.previousResult}}` interpolate into step strings; `_input.<name>`
  resolves parameters at load (schema.cue:5–9).

Three consequences drive the tiering below:

1. **There is no control flow.** Steps are a fixed linear list executed once
   (`for (index, step) in workflow.steps` — workflow_exec.rs:172). No loops, no
   conditionals, no jumps. `evaluate` can only *stop*, never *branch*.
2. **`evaluate` can only see `previousResult`.** It cannot read arbitrary
   tuples. Critically, `code-review.cue`'s reviewer records its verdict as an
   **artifact tuple** (`rk out artifact … review`), which `evaluate` cannot
   observe. So today a workflow *cannot branch on a review verdict* — it can
   only park the branch for a human.
3. **One active agent at a time.** `spawn` overwrites `active_agent`; `wait`
   waits for whichever is current. There is no fan-out to N rats and no join.

Everything expressible with a *fixed, linear chain of spawn/wait/evaluate/
dismiss over chained branches* is Tier 1. Everything needing routing, looping,
approval, or fan-out is Tier 2 and names its missing primitive.

### Why self-improvement loops are the highest-leverage target

The operator's leverage is a multiplier on fleet throughput and quality. A
workflow that produces one feature is linear leverage. A workflow that improves
the *backlog*, the *prompts*, the *workflows*, or the *verification bar*
compounds: every future rat run benefits. That is why the four Tier-1 picks are
all meta-workflows over the kingdom's own artifacts (workflows, tickets,
prompts, fixes) rather than over product features.

---

## Tier 1 — ship today (no engine changes)

### 1. `workflow-review`

**Purpose:** a rat audits the workflow library and proposes refined CUE plus
follow-up tickets.

**Why high-leverage:** the workflows *are* the operator's control surface. A rat
that reviews and sharpens them improves every future run routed through them —
the definition of compounding leverage. It is also the fleet reasoning about its
own coordination substrate.

**Steps (all supported today):**

```cue
workflow: {
	name:        "workflow-review"
	description: "a rat audits the workflow library and proposes refined CUE + tickets"
	params: {
		focus: {type: "string", required: false, default: "all shipped workflows"}
	}
	agents: {default: {harness: "claude"}}
	steps: [
		{
			type: "spawn"
			role: "rat"
			task: {
				title: "review-workflows"
				description: """
					Read crates/rk-workflow/src/schema.cue and every file under
					examples/workflows/. Focus: \(_input.focus).

					For each workflow: assess correctness against the schema, missing
					guardrails (evaluate/dismiss), and leverage. Where you can improve a
					definition, write the revised .cue to docs/proposals/ (do NOT edit the
					shipped files). File a ticket per substantive change:
					  rk ticket new "<title>" --body "<rationale>"
					Record a summary artifact:
					  rk out artifact $RK_REPO workflow-review --payload '{"reviewed": N, "tickets": M}'
					Commit your proposals, then report done.
					"""
			}
		},
		{type: "wait", timeout: "30m"},
		{type: "evaluate", expect: {is_error: false}},
		{type: "dismiss"},          // merge the proposals doc
	]
}
```

An optional second stage (spawn a cheap reviewer on the same branch to run
`cue vet` / `rk workflow validate` on the proposed CUE, exactly as
`code-review.cue` chains a reviewer) fits with no new primitives.

### 2. `backlog-groom`

**Purpose:** a rat decomposes oversized tickets, dedupes, tags, and closes stale
items — improving the queue that feeds every other workflow.

**Why high-leverage:** the backlog is the fleet's work-distribution medium. Task
instructions already tell every rat to *file* tickets and *decompose* via
`rk ticket new --parent`, but nothing grooms the result. A grooming loop keeps
the queue decomposed into grabbable, dependency-ordered units, which raises the
hit rate of *every* dispatch.

**Steps (all supported today):**

```cue
workflow: {
	name:        "backlog-groom"
	description: "one rat decomposes, dedupes, and tags the ticket backlog"
	params: {repo: {type: "string", required: false, default: ""}}
	agents: {default: {harness: "claude"}}
	steps: [
		{
			type: "spawn"
			role: "rat"
			task: {
				title: "groom-backlog"
				description: """
					Read the open backlog:  rk ticket list --status open
					For each oversized ticket, decompose it:
					  rk ticket new "<sub>" --parent <TKT-id>
					Merge duplicates (note the survivor in each), and flag stale items.
					Do NOT start any ticket. Record what you changed:
					  rk out artifact $RK_REPO backlog-groom --payload '{"decomposed": N, "deduped": M}'
					Report done.
					"""
			}
		},
		{type: "wait", timeout: "20m"},
		{type: "evaluate", expect: {is_error: false}},
		{type: "dismiss", noMerge: true},   // no code change; ticket store mutated directly
	]
}
```

Grooming mutates the ticket store through the `rk ticket` CLI, not the
worktree, so `dismiss noMerge` is correct — there is nothing to merge.

### 3. `fix-then-verify`

**Purpose:** a fixer rat implements a change; an independent verifier rat writes
a regression test and confirms it goes red-before, green-after, then merges.

**Why high-leverage:** it institutionalises the "prove the fix" discipline
without a human in the loop, and does so with a *fresh* agent so the verification
is independent of the fixer's assumptions. This is the single-pass, non-looping
core of a fix/verify cycle — and it is fully expressible today because the
verifier chains onto the fixer's branch.

**Steps (all supported today):**

```cue
workflow: {
	name:        "fix-then-verify"
	description: "fixer implements; independent verifier writes a regression test and confirms red→green"
	params: {
		taskId: {type: "string", required: true}
		description: {type: "string", required: false, default: ""}
	}
	agents: {
		default:  {harness: "claude"}
		verifier: {harness: "claude"}
	}
	steps: [
		{
			type: "spawn"
			role: "rat"
			task: {title: _input.taskId, description: _input.description}
		},
		{type: "wait", timeout: "30m"},
		{type: "evaluate", expect: {is_error: false}},
		{type: "dismiss", noMerge: true},          // keep branch for the verifier
		{
			type:  "spawn"
			role:  "verifier"
			agent: "verifier"
			task: {
				title: "verify-" + _input.taskId
				description: """
					You are on branch {{ctx.activeBranch}}. The task was: \(_input.description)
					Independently verify the fix:
					  1. Write or identify a regression test that FAILS on main.
					  2. Confirm it PASSES on this branch.
					  3. Run the full suite.
					Commit the test. If verification fails, exit non-zero (is_error).
					Report done.
					"""
			}
		},
		{type: "wait", timeout: "20m"},
		{type: "evaluate", expect: {is_error: false}},   // hard gate: bad verify fails the run
		{type: "dismiss"},                                // merge fix + regression test
	]
}
```

Note the leverage of the existing `evaluate` semantics: because a failed
`evaluate` fails the whole instance, an unverifiable fix *does not merge*. The
looping variant ("verifier fails → send it back to a rework rat") is **Tier 2**
(`reviewer-drives-rework`), because the loop-back needs branching + iteration.

### 4. `prompt-refine`

**Purpose:** a rat mines `obstacle` / `need` tuples and failed workflow instances
to propose edits to role prompts and `convention` tuples.

**Why high-leverage:** prompts and conventions are the fleet's shared priors.
Recurring obstacles are a direct signal that a prompt or convention is missing or
wrong; folding those lessons back in reduces the *same* failure across all future
rats. This is stigmergic self-improvement: the tuplespace already records the
pain, so a rat can read it and close the loop.

**Steps (all supported today):**

```cue
workflow: {
	name:        "prompt-refine"
	description: "mine obstacle/need tuples and failed runs; propose prompt + convention edits"
	agents: {default: {harness: "claude"}}
	steps: [
		{
			type: "spawn"
			role: "rat"
			task: {
				title: "refine-prompts"
				description: """
					Scan recurring pain:
					  rk scan obstacle ; rk scan need ; rk scan fact system
					Cross-reference with recent workflow_failed events.
					Where a recurring failure traces to a weak role prompt or a missing
					convention, propose a concrete edit (write a diff/patch under
					docs/proposals/prompts/) and, for durable rules, propose a convention:
					  rk out artifact $RK_REPO convention-proposal --payload '{"rule": "...", "why": "..."}'
					File a ticket for each proposed change. Do NOT edit live prompts.
					Commit proposals, report done.
					"""
			}
		},
		{type: "wait", timeout: "25m"},
		{type: "evaluate", expect: {is_error: false}},
		{type: "dismiss"},
	]
}
```

---

## Tier 2 — high leverage, each gated on one new primitive

### 5. `reviewer-drives-rework`

**Purpose:** a reviewer's APPROVE / REWORK / STOP verdict routes the next action
— merge on APPROVE, loop back to a rework rat on REWORK (up to N rounds), abort
on STOP.

**Why high-leverage:** this is the canonical operator lever — a
review→rework→re-review cycle that runs to a clean verdict without a human
babysitting each round. It converts review from advisory (today's
`code-review.cue` merely parks the branch) into *driving*.

**Why it is not expressible today (three gaps):**

- `evaluate` can only see `previousResult`, but the reviewer records its verdict
  as an **artifact tuple** (`code-review.cue:52–54`). The workflow can't read it.
- `evaluate` is a hard gate, not a router — it can't send APPROVE one way and
  REWORK another.
- There is no loop, so "re-review after rework, up to N times" can't be
  expressed.

**Primitives to add:**

- **(a) verdict-visible evaluate/read** — either let the reviewer emit its
  verdict in the `harness_result` payload (so `evaluate` sees it), or add a
  `read` step that pulls a named tuple into `ctx` (e.g. `ctx.verdict`).
- **(b) conditional branch** — a `when` / `switch` step (or `next:` targets on
  `evaluate`) that routes on a `ctx` value instead of aborting.
- **(c) bounded loop** — a `repeat: {max: N}` block (or labelled step + `goto
  label maxIterations: N`) so REWORK re-enters the rework→review sub-sequence.

**Sketch once those exist (illustrative syntax, not current schema):**

```
spawn rat → wait → evaluate is_error:false → dismiss noMerge
repeat max=3:
    spawn reviewer (emits verdict in result) → wait → read verdict → ctx.verdict
    when ctx.verdict:
        "APPROVE": dismiss (merge) ; break
        "STOP":    dismiss noMerge  ; fail
        "REWORK":  spawn rework-rat on {{ctx.activeBranch}} → wait → evaluate is_error:false → dismiss noMerge ; continue
```

The primitives are small and composable — (a) is a payload/read plumbing change,
(b) is a step that consults `ctx`, (c) is a bounded re-entry over a slice of the
step list. Together they unlock *every* looping self-improvement workflow, so
this is the highest-value engine investment.

### 6. `gated-merge`

**Purpose:** a rat implements a change, then the branch parks behind a **human
approval gate**; on approval it auto-merges, otherwise it is dismissed unmerged.

**Why high-leverage:** it lets the operator run the fleet unattended on risky
changes while keeping a human veto exactly at the merge boundary — the safety
valve that makes broad autonomy palatable.

**Why not today:** `gate` is timer-only; `evaluate` can't observe an external
approval. The schema already names the intended mechanism: *"human gates arrive
with approval tuples"* (schema.cue:101).

**Primitive to add:** an **approval gate** —
`{type: "gate", gateType: "approval", timeout: "24h"}` that blocks until an
`approval` tuple for this instance appears in the space (e.g. via
`rk approve <instance>` / `rk reject <instance>`), capturing the decision into
`ctx.previousResult` so a following `evaluate`/branch can act on it.

**Sketch:**

```cue
steps: [
	{type: "spawn", role: "rat", task: {title: _input.taskId, description: _input.description}},
	{type: "wait", timeout: "30m"},
	{type: "evaluate", expect: {is_error: false}},
	{type: "dismiss", noMerge: true},                          // hold branch for review
	{type: "gate", gateType: "approval", timeout: "24h"},      // NEW: block on approval tuple
	{type: "evaluate", expect: {approved: true}},              // proceed only if approved
	{type: "dismiss"},                                          // merge on approval
]
```

Until the primitive lands, a timer gate is a poor stand-in (it merges on a
schedule, not on consent) and should not be used for genuinely risky merges.

### 7. `backlog-drain`

**Purpose:** fan out one solo-task rat per ready ticket, run them in parallel,
then join and report.

**Why high-leverage:** this is the operator's throughput dial — "work the ready
backlog" as one command instead of hand-launching a run per ticket. Combined
with `backlog-groom` (which keeps the queue decomposed) it turns a well-groomed
backlog directly into parallel fleet work.

**Why not today:** the step list is static and single-active-agent. There is no
way to (a) enumerate tickets at runtime and spawn one rat each, or (b) wait for
several agents to finish (join). `wait` only tracks the most-recent spawn.

**Primitives to add:**

- **dynamic fan-out** — a `spawn` variant driven by a tuplespace/ticket query,
  e.g. `forEach: {query: "ticket status=ready", limit: N}` spawning one agent
  per match with the ticket bound into the task template.
- **parallel join** — a `waitAll` step that blocks until every agent spawned in
  the current fan-out has emitted its `harness_result`, aggregating results into
  `ctx` for a final `evaluate`.

**Sketch (illustrative):**

```
forEach ticket in (rk ticket list --status ready --limit 5):
    spawn rat  { title: ticket.id, description: ticket.body }   // parallel
waitAll timeout=45m
evaluate { all: {is_error: false} }
# each rat's own dismiss/merge is governed by solo-task semantics per branch
```

Fan-out is the largest engine change of the three (it touches the executor's
one-active-agent assumption and the `wait` matcher), so it is the lowest-priority
Tier-2 item despite being the flashiest operator lever.

---

## Recommended build order

1. Ship Tier 1 (`workflow-review`, `backlog-groom`, `fix-then-verify`,
   `prompt-refine`) as-is — pure CUE, no engine risk, immediate self-improvement.
2. Add the **verdict-read + conditional branch + bounded loop** primitives and
   ship `reviewer-drives-rework`. Highest leverage-per-primitive; the same three
   additions retrofit a looping variant onto `fix-then-verify`.
3. Add the **approval gate** and ship `gated-merge` — small, self-contained, and
   the schema already anticipates it.
4. Add **fan-out + join** and ship `backlog-drain` — biggest change, do last.

## Open questions

- **Verdict channel for routing.** Should a reviewer's verdict travel in the
  `harness_result` payload (so `evaluate` sees it directly) or via a new `read`
  step that lifts a named artifact tuple into `ctx`? The artifact route is more
  general (any tuple becomes branchable) but adds a step type; the payload route
  is cheaper but couples verdicts to harness output shape.
- **Loop representation.** `repeat {max}` block vs. labelled steps with a bounded
  `goto`. A block is easier to validate statically in CUE; labels are more
  flexible but invite unbounded loops. A hard iteration cap should be mandatory
  either way, to preserve the "steps run once" safety property the executor
  currently guarantees for free.
- **Fan-out and the single-active-agent model.** Parallel spawn breaks the
  `ctx.active_agent` / `ctx.active_branch` invariant the whole executor is built
  on (workflow_exec.rs:201–231). Does fan-out get a separate agent-set context,
  or does the model generalise `active_agent` to a list? This is the deepest
  design decision of the four.
- **Approval-tuple plumbing.** What CLI/UX emits the approval tuple
  (`rk approve <instance>`?), how is it scoped to an instance+step, and what is
  the timeout/expiry policy when no human responds?
- **Grooming authority.** Should `backlog-groom` be allowed to *close* stale
  tickets autonomously, or only propose closure? Autonomy is higher-leverage but
  risks silent loss of real work items.

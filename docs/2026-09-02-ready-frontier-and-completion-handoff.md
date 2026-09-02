# Ready frontier and completion handoff

*Status: implemented and activated from the Glossolalia foreign-tenant pilot;
the post-rollover canary added the explicit ready-for-agent dispatch contract.*

## Evidence

The pilot exposed two independent control-plane gaps:

1. `TKT-domum-jipiz-lojuv` became ready when its predecessor landed, but the
   King pull contained the first 20 ready tickets globally, all from another
   repository. The King did receive and resolve a wake, but the bounded payload
   omitted the Glossolalia successor. The successor remained ready for 1,385
   seconds against the preregistered 900-second ceiling.
2. Noodle-13 durably published a clean, declared `harness_result` at
   15:48:22Z. Before the reactor admitted that completion to the durable landing
   queue, reconciliation saw `AgentState::Completed` beside an `in_progress`
   ticket and reported `terminal-assignee-active-work`. The ordinary landing
   path later reviewed, merged, and closed the ticket without repair.

These are not the same failure. The first is ready-work visibility. The second
is lifecycle interpretation during an asynchronous durable handoff.

## Decision 1: deepen the King ready frontier

The King remains the LLM operator delegate. This change does **not** grant the
daemon new dispatch authority. Continuous drain remains the existing explicit
opt-in for automatic WIP-targeted dispatch; a King wake remains notification
that causes the King to re-read and act through ordinary RK commands.

The post-rollover canary made the authority boundary more precise. `open` plus
dependency satisfaction means a ticket is technically ready, but does not by
itself authorize unattended spend across every registered repository. The
`ready-for-agent` label is the explicit King dispatch grant. A King must re-read
that labeled ticket and the ordinary dependency, repository, budget, resource,
and WIP gates, then dispatch it or defer at a named concrete gate. Unlabeled
ready work remains visible for interactive operator selection. This preserves
the distinction between a registered operator delegate and continuous drain.

Replace the shallow global `tickets.ready(None)?.take(20)` projection with one
ready-frontier module whose interface returns:

- the exact total number of ready tickets;
- a digest over every ready `(repo, ticket identity)` pair;
- exact counts and the oldest representative per ready repository;
- at most 20 fair ticket representatives, selected round-robin across
  repositories while preserving FIFO within each repository; and
- explicit truncation metadata plus the native re-read command.

The King snapshot keeps `ready_tickets` for wire compatibility and adds the
frontier metadata. The snapshot digest includes the full ready digest and exact
counts, so a change outside the representative cap still creates a durable
wake. With at most 20 ready repositories, every repository contributes at least
one exact ticket to `ready_tickets`; with more, every bounded repository summary
still tells the King to re-read the native ticket/work projection.

### Invariants

1. A deep backlog in one repository cannot hide a ready ticket in another.
2. The representative list is deterministic for unchanged durable state.
3. FIFO order within one repository is preserved.
4. Any ready identity or repository-count change changes the full digest.
5. The payload stays bounded; native ticket/work RPCs remain authoritative.
6. No ready transition directly spawns a rat unless continuous drain is
   explicitly enabled.
7. A King dispatch requires `ready-for-agent`; the label never bypasses an
   ordinary admission or delivery gate.

### Rejected alternatives

- **Raise the global cap.** This postpones starvation; it does not remove it.
- **Auto-enable continuous drain.** That widens authority and contradicts its
  explicit opt-in contract.
- **Put complete ticket bodies in the wake.** Repository-authored text does not
  belong in the terminal transport, and the pull must stay bounded.
- **Trust only the capped payload digest.** Hidden ready changes can remain
  invisible when the cap is full.

## Decision 2: recognize a bounded completion handoff

`AgentState::Completed` continues to mean execution completed. It must not be
renamed to `Landing` or kept live: execution WIP and delivery progress are
separate ledgers.

The clean, declared, generation-fenced `Event/harness_result` is the durable
receipt that execution handed its candidate to the reactor. Reconciliation
will recognize two non-orphan phases for an active ticket whose exact owner is
terminal:

1. **completion pending admission** — an exact-spawn clean rat
   `harness_result` exists and is still inside the bounded admission grace;
2. **landing** — a durable landing-queue entry exists for the same task and
   exact source spawn.

Neither phase is a contradiction. If admission does not produce an exact
landing entry before the grace expires, the same
`terminal-assignee-active-work` violation appears with the stale handoff
receipt and age in its evidence. A clean completion therefore cannot suppress
a real orphan forever.

The admission grace is `max(300 seconds, 2 * reactor.interval_secs)`. Five
minutes covers ordinary feed wake, durable scan, and admission under the
default 30-second reactor cadence while remaining much shorter than the
15-minute stale-ticket recovery threshold. An operator who deliberately slows
the reactor gets a proportionate grace rather than false failures by design.

Landing-queue evidence is generation-fenced. A queue entry for an older
namesake or a different spawn does not excuse the current ticket owner.

### Invariants

1. A clean completion is durable before it can suppress a violation.
2. Task, agent, and spawn must match the active ticket's resolved owner.
3. Failed, undeclared, reviewer, or malformed results never count as handoff.
4. Completion-pending-admission suppression expires.
5. Exact landing-queue evidence suppresses while the candidate is in flight.
6. Landed-but-open remains a separate mechanical contradiction.
7. Reconciliation stays read-only and adds no lifecycle store.

### Rejected alternatives

- **Suppress every `Completed` owner.** A genuinely dropped completion would
  disappear forever.
- **Add `AgentState::AwaitingLanding`.** It conflates execution liveness with
  delivery progress and requires a durable registry migration even though the
  handoff receipt and landing queue already exist.
- **Delay all reconciliation.** That weakens unrelated contradiction families.
- **Teach the observer to ignore the violation.** The lifecycle meaning belongs
  in reconciliation so every caller gets the same answer.

## Failure matrix

| State | Reconciliation result | Recovery authority |
| --- | --- | --- |
| Live owner | no contradiction | supervisor |
| Exact clean completion inside grace | completion handoff | reactor |
| Exact landing entry | landing handoff | landing pipeline |
| Completion grace expired, no landing entry | terminal owner violation | orchestrator |
| Failed/undeclared completion, no live owner | terminal owner violation | orchestrator |
| Delivery recorded, ticket active | delivered-but-open violation | mechanical |

## Verification

- Ready frontier: 25 ready tickets in repository A plus one in repository B
  includes B, preserves FIFO, reports exact counts, and changes its full digest
  when a non-represented ticket changes.
- King integration: bounded snapshot reports exact total and fair
  representatives rather than the cap as the total.
- Reconciliation: exact clean completion inside grace is not a violation.
- Reconciliation: the same completion after grace is a violation.
- Reconciliation: wrong spawn, failed, undeclared, and reviewer results remain
  violations.
- Reconciliation: an exact-spawn landing entry suppresses; a different spawn
  does not.
- Existing convergence, King lifecycle, observation, and full protected gates
  remain green.

The implementation lives in one deep ready-frontier module, the existing King
snapshot composition seam, the reconciliation read model, and the existing
landing-queue snapshot. It adds no lifecycle store and grants no new dispatch
authority. Focused unit and live-assembly regressions cover both pilot
failures. The protected repository gate passes with the implemented query
shape.

## Rollout

The wire changes are additive. The implementation can be built and tested
without touching the running pilot observer. Activation requires a later
binary install and daemon rollover; that operational step must be recorded
separately because installation alone does not change the running daemon.

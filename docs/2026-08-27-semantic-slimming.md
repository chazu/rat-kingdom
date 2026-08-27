# Rat Kingdom semantic slimming map

Status: implementation companion for `TKT-01M0FQ8BV7S558ZWZ99DBWA45E`.

This pass removes duplicate control-plane owners. It does not reduce the
repository contract: `.rk/repo.cue`, `.rk/checks.cue`, triggers, and schedules
remain the authority for deterministic repository policy. Rust resolves and
executes that policy; rats are reserved for semantic judgment.

## Lifecycle owners and sources of truth

| Concern | Durable source of truth | Sole mutation owner |
| --- | --- | --- |
| Ticket delivery | `Ticket.payload.delivery` | `Supervisor::finalize_delivery` through `Tickets::record_delivery` |
| Agent delivery pointer | `AgentRecord.merge_commit`, derived from ticket delivery | `Supervisor::finalize_delivery` under the agent registry lock |
| Gated merge candidate | `LandingQueueEntry` plus its prepared candidate ref | `LandingPipeline` |
| Target advancement | Exact `PreparedMerge.commit` and compare-and-swap base | `LandingPipeline::advance_target` through `Supervisor::land_prepared` |
| Successful landing finalization | Landing result plus source `SpawnId` | `LandingPipeline::finalize_landed` |
| Mechanical ticket repair | `ConvergenceReport` plus fresh git and ticket facts | `reconcile_repair::plan` and `reconcile_repair::apply` |
| Repository validation | Activated `.rk/repo.cue` and repo-owned `.rk/checks.cue` | Non-agentic named-check runner |
| Human judgment | Durable attention item | Explicit operator or King decision; never a mechanical sweep |

## Background-loop inventory

The pre-absorption daemon starts at most 16 recurring tasks. A loop is counted
once per `background_tasks.spawn` call in `Daemon::run`, even when one task
already performs several related actions.

| Before | Loop | Wake and fact inputs | Action and idempotency | Authority | Destination |
| ---: | --- | --- | --- | --- | --- |
| 1 | Tuple GC | fixed timer; tuple TTL and strength | decay or collect by tuple identity | mechanical | remain distinct: tuple lifetime is its contract |
| 2 | Supervisor liveness | timer; live process, usage, heartbeat, transport facts | steer, stop, respawn, or retry using generation and recovery keys | mechanical | remain distinct: live process time is not durable convergence |
| 3 | Task-done reconciliation | tuple feed plus timer; `task_done`, agent generation, terminal record | settle the exact generation; replay is generation-fenced | mechanical | shared convergence scheduler |
| 4 | Forge review sweep | timer; network fetch and remote PR state | emit `pull_request_closed` once per remote state | mechanical | remain distinct: optional network transport |
| 5 | Agent-worktree cleanup | timer; terminal agents, clean worktrees, merged-or-gone branches | safe reap; repeated reap is a no-op | mechanical | shared convergence scheduler |
| 6 | Gate-worktree cleanup | timer; LRU markers and live landing keys | safe reap keyed by repo and target | mechanical | shared convergence scheduler |
| 7 | Recovery and phase-latency sweep | timer; unacked recovery rows, phase spans, activated repo policy | bounded re-notify and deduplicated breach events | mechanical | shared convergence scheduler |
| 8 | Stale-instance timeout | timer; durable workflow state and effective timeout | fail and finalize one instance using its instance id | mechanical | shared convergence scheduler |
| 9 | Orphan-ticket reopen | timer; ticket ownership, agent state, landing carve-outs | guarded ticket reopen; ticket state machine makes replay safe | mechanical | shared convergence scheduler |
| 10 | Multiplayer sync | timer; signed replication journal | import/export at durable cursor | mechanical | remain distinct: peer transport and cursor |
| 11 | Reactor | tuple feed plus timer; activated CUE triggers and durable cursor | fire matching action once per trigger key | CUE-declared mechanical | remain distinct: event trigger axis |
| 12 | Landing consumer | tuple feed plus timer; durable landing queue | gate, review when needed, and advance exact candidate | CUE gate plus semantic reviewer | remain distinct: may await a reviewer for the full ceiling |
| 13 | Late-review reconciliation | tuple feed plus timer; settled review attempts and late verdicts | retain evidence once per attempt and verdict | mechanical | shared convergence scheduler |
| 14 | Cron scheduler | wall clock; activated CUE schedules and minute cursor | single-flight catch-up fire | CUE-declared mechanical | remain distinct: time is the trigger contract |
| 15 | Continuous drain | tuple feed plus timer; ready tickets and WIP capacity | claim and dispatch through ticket and fleet CAS | mechanical dispatch | remain distinct: capacity controller |
| 16 | King | timer; freshly rebuilt authoritative RK state | emit or settle generation-fenced wake envelopes | orchestrator | remain distinct: human-facing notification transport |

After absorption, rows 3, 5, 6, 7, 8, 9, and 13 share one feed-and-deadline
convergence scheduler. The maximum recurring-task count is therefore 10, not
16. Each action keeps its existing durable idempotency key and configuration;
only duplicated timer/feed ownership is removed.

## Reconciler contract

- The tuple feed is a wake signal. Durable stores are always rescanned.
- Each timed action retains its own configured cadence and initial grace
  period. Sharing a scheduler does not make cadences equal.
- A failed action is logged independently and cannot suppress the other due
  actions.
- Blocking git and notification work stays on the blocking pool. Async
  generation and workflow finalization remains async.
- Restart may repeat a plan, so every apply path must be idempotent or
  compare-and-swap guarded. No scheduler-local success bit is authoritative.
- Human-authority violations are reported with one resolving command and are
  never auto-applied.
- Git ancestry is `present`, `absent`, or `unknown`. A deleted historical
  intermediate target is `unknown`, not proof that a final delivery is false.

## Landing-path reduction

The preceding landing slice reduced prepared-target advancement call sites
from 3 to 1, successful delivery-finalization call sites from 4 to 1, and
production CUE-plan resolution call sites from 2 to 1. Operator, workflow,
automatic, dismiss, dismiss-all, and batch merge delivery now converge on the
same durable candidate and finalizer.

## Current-work operator surface

`rk work [repo]` is the daily read model. It composes the existing daemon
status, live-agent registry, ready-ticket query, inbox, reconciliation report,
and decision journal; it owns no lifecycle state of its own.

- Text and JSON both report installed/daemon build parity and exact counts for
  the rows they return.
- `attention` admits only bounded rows with one supported, idempotent command:
  failed/orphaned or retry-exhausted rats (`rk respawn`), durable recovery
  notices (`rk inbox ack`), mechanical convergence repair (`rk attention
  decide`), and an explicit Human-authority invalidation (`rk attention
  invalidate`).
- Advice, open-ended needs, two-way workflow approvals, forge review, unlanded
  branches, queue-stall diagnostics, and Orchestrator-authority work remain in
  `rk inbox`, `rk reconcile`, or `rk attention`; they are not mislabeled as a
  one-command human resolution.
- `rk attention invalidate` records one terminal, exact-violation human
  decision. It changes neither ticket/repository facts nor CUE policy. Replays
  return the same durable decision, and the settled item leaves current work.
- Empty output says `no current work` and points to `rk digest --since 1d` and
  `rk top`. Settled workflow failures and settled wakes remain history.
- King wakes remain durable at-least-once notification transport. Ordinary
  operation requires no wake id or phase, and settling a wake never implies
  this independently rebuilt current-work view is empty.

Compatibility mapping: `rk list` remains the detailed agent registry, `rk
ticket ready` remains the detailed ready query, `rk inbox` remains broad human
triage, `rk reconcile`/`rk attention` retain authority and cursor diagnostics,
and `rk king status` retains wake lifecycle diagnostics. Their JSON contracts
are unchanged; `rk --json work` is an additive composed view.

The short manual delivery journey is `rk work` -> `rk spawn --ticket` -> `rk
land` (when the selected workflow does not land automatically) -> `rk work`.
Landing still resolves the repository's activated CUE plan and runs its named
checks before target advancement.

Work already landed outside that journey can be bound to a ticket with `rk
ticket deliver`. The operator supplies the registered repository, commit,
target, and verification evidence; Rat Kingdom proves local git reachability,
writes the existing delivery record, and closes the ticket in one mutation.
This command deliberately does not execute or translate checks. It is a
content-bound recovery seam, while repository CUE remains the authority for
automated validation and landing.

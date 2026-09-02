# Canonical operator work projection

*Status: stabilized after adversarial review. 2026-09-01. Review dispositions:
`docs/2026-09-01-three-enhancements-adversarial-review.md` W1-W5.*

## Decision

`rk work` is the canonical current operator projection. It composes existing
authoritative stores without becoming a store itself, and it distinguishes:

- **actionable**: one bounded, replay-safe command can advance the item;
- **decision required**: the operator must choose among bounded alternatives;
- **stalled**: work exists but needs diagnosis or an external action;
- **live** and **ready**: active generations and dispatchable tickets.

`no_current_work` is true only when all five sections are empty. Existing
`attention` output remains as a compatibility alias for `actionable` during one
release cycle.

## Problem

The current projection deliberately drops workflow failures, workflow gates,
awaiting-review rows, and landing stalls because they are not single-command
attention. That is a sound classification decision but an unsound completeness
decision: the omitted items disappear from the daily operator surface, and
`no_current_work` can claim idleness while work is blocked.

The generated prime text also describes an obsolete lifecycle in which
dismissal merges and closes a ticket. Current behavior requires explicit
landing; dismissal is cleanup, not delivery.

## Source-of-truth rule

The projection may join and classify authoritative facts, but it never owns
lifecycle state:

- tickets own ready work;
- the registry owns live generations;
- inbox tuples own workflow, review, landing, and recovery signals;
- reconciliation reports own cross-ledger contradictions;
- repository policy owns validation and automation rules.

Every projected row carries its source, repository, stable identity where one
exists, and the command or bounded alternatives that justify its class.

## Classification

### Actionable

Rows already admitted by `current_inbox_resolution`, plus mechanical and human
reconciliation actions that have exactly one supported command.

### Decision required

- workflow approval gates with explicit approve/reject alternatives;
- other rows whose payload contains a bounded set of supported choices.

This section must never collapse a choice into a recommended command without a
policy decision.

### Stalled

- workflow failures;
- awaiting-review work whose next action is review or forge work;
- landing queue stalls;
- transport outages with only diagnostic advice;
- open-ended needs and malformed rows that cannot be safely classified.

Each stalled row retains its existing action/advice text and gains a stable
diagnostic command when one is known. The projection must not invent a command
that is not accepted by the CLI.

Classification is exhaustive over the broad inbox rows. Known singular
resolutions are actionable, known bounded choices are decision-required, and
every remaining row defaults to stalled. A new inbox kind therefore becomes
visible before it receives bespoke rendering.

## Wire format

The response keeps `live_agents`, `ready_tickets`, `daemon`, and diagnostic
pointers. It adds:

- `actionable`, `decision_required`, and `stalled` arrays;
- matching counts;
- `attention` as an exact alias of `actionable` for compatibility;
- `counts.attention` as an alias of `counts.actionable`.

Plain text renders non-empty sections in that order after live and ready work.
Rows use `detail`, then `text`, then `action` as display fallbacks and omit the
command line when no singular command exists.
JSON is additive except for the corrected meaning of `no_current_work`.

## Lifecycle language

README and every current prime fragment must state one contract:

1. completion records the rat's result and makes delivery eligible;
2. `rk land` runs the repository landing path;
3. successful landing records delivery and closes the ticket;
4. dismissal cleans up the exact generation and never implies delivery.

This includes operator daily steps, ticket dispatch, the dismiss command, and
foreman child integration guidance. Dated historical and research documents
are not rewritten.

Tests should assert these phrases semantically rather than duplicate whole
documents as snapshots.

## Invariants

1. Every broad inbox row appears in exactly one of actionable, decision
   required, or stalled. Orchestrator-authority reconciliation is the sole
   explicitly operator-invisible class and remains available to King control.
2. A row is actionable only when its command is singular and supported.
3. Decision rows preserve all alternatives.
4. `no_current_work` means no live, ready, actionable, decision-required, or
   stalled work.
5. Repository filtering applies identically to every section.
6. JSON compatibility aliases remain internally equal.
7. `rk work` never mutates the state it projects.

## Verification

- Table-driven classification tests for every inbox kind currently emitted.
- Empty, actionable-only, decision-only, stalled-only, live-only, and
  ready-only projection tests.
- CLI rendering tests for each section and compatibility alias tests.
- Repository-filter tests with rows from two repositories.
- Prime/README contract tests: land is delivery; dismiss is cleanup.

## Implementation status

Implemented across `crates/rk-daemon/src/server.rs`,
`crates/rk-cli/src/work_cmds.rs`, `crates/rk-core/src/prime.rs`, and
`README.md`. The daemon classifies every broad inbox row, constructs
`attention` from `actionable`, and includes all five work classes in the idle
decision. The CLI renders all three operator-inbox classes with shape-tolerant
fallbacks. Current lifecycle guidance now consistently separates completion,
landing, and dismissal.

Classification, compatibility, empty-state, rendering, and prime regressions
pass. On 2026-09-01, `mise run verify` passed all 1,755 affected tests and
`mise run verify-full` passed the protected workspace suite and doc tests.

## Non-goals

- Replacing `rk inbox`, `rk top`, workflow status, or reconciliation detail.
- Automatically deciding approvals or resolving stalls.
- Adding another lifecycle database.
- Folding historical digest information into current work.

## Rollback

The new fields are additive. A rollback client ignores them. A rollback daemon
restores the narrower idle meaning, so release notes must call out that semantic
regression if rollback is required.

---
name: factory-foreman
description: Triage Rat Kingdom factory health, inspect fleets and workflows, classify failures, analyze scorecards, and prepare exact approval-gated dispatch proposals. Use when a user mentions RK, Rat Kingdom, fleet health, factory triage, workflow failures, inbox work, dispatch, or product-to-code delivery.
triggers:
  - Rat Kingdom factory
  - fleet health
  - RK inbox
  - workflow failures
  - factory triage
  - dispatch work
  - software factory
---

# Factory Foreman

Use this globally installed skill from any repository when the user asks about the Rat Kingdom factory, fleet health, RK inbox, workflow failures, factory triage, dispatch work, product-to-code delivery, or factory optimization. The target repository must be registered with the local RK daemon.

Start with native, typed, read-only RK data. Do not open the interactive dashboard from an agent session:

```bash
rk --json daemon status
rk --json factory snapshot --repo <repo>
rk --json factory events replay --repo <repo> --limit 256
rk --json factory scorecards --repo <repo>
rk --json factory recommend --repo <repo>
```

The snapshot supplies agents, workflows, tickets, inbox, approvals, budget, and repository health. Replay supplies ordered recent changes and resync state. Scorecards and recommendations are advisory and preserve unavailable evidence as unobserved rather than zero.

When the `rk` MCP server is available to Jcode, prefer its typed tools for the
proposal and approval-gated mutation leg: `propose_workflow_run`, then, only
after a later human turn approves the exact proposal, `approve_action` and
`execute_approved_workflow_run`. Fall back to the equivalent `rk --json
factory propose-workflow`, `factory approve`, and `factory execute-action`
shell-outs when the server is unavailable. The MCP tools and CLI commands use
the same daemon-owned canonical digest and approval boundary.

Never execute a mutating Rat Kingdom command unless a later user message explicitly approves the exact proposal rendered in this conversation. Require daemon-verifiable approval of the exact canonical digest before dispatch.

See [REFERENCE.md](REFERENCE.md) for native evidence, saved rendering, approval examples, and recovery behavior.

## What this skill does

1. Resolve the requested registered repository without guessing.
2. Read the native factory snapshot and recent event replay.
3. Report degraded or unavailable sources before drawing conclusions.
4. Read `rk --json work <repo>` for current work and triage stalled or failed workflows, inbox pressure, pending approvals, budget signals, ticket duplication, event lag, and resync requirements.
5. Read scorecards and recommendations when the user asks about recurring performance or routing problems.
6. Separate observed evidence from hypotheses and recommend an existing workflow where possible.
7. Prepare an exact typed proposal when mutation is warranted, using the typed MCP proposal tool when available or the CLI fallback below, then stop for human approval.
8. After a later approval, forward the saved proposal through the daemon's digest-bound approval and execution path and monitor the resulting workflow.
9. For product work, use RK's product-to-code contracts to validate initiative, research, ticket graph, impact evidence, dispatch, and independent verification artifacts.

## Saved evidence rendering

Capture the native responses through the read commands above, then render them offline:

```bash
rk factory render --snapshot <factory-snapshot.json> \
  --events <factory-events-replay.json> --output <factory-dashboard.md>
```

This command reads saved JSON and writes Markdown without contacting or starting
a daemon. The view labels its source SAVED and the connection NOT CONNECTED;
it cannot certify current daemon health or current approval state. Numeric cursors,
replay truncation and boundary, resync, degraded sources, and proposal digests
remain visible. Obsolete flattened helper snapshots must be recaptured through
`factory snapshot`; they are not native evidence.

A Jcode side panel can display the Markdown. Displaying approval data confers no
execution authority. The native daemon still verifies the exact typed proposal.

## Workflow

1. Resolve the repository name without guessing when ambiguous. If the requested repo is unclear, ask for the repo before running repo-scoped commands.
2. Run the native read-only snapshot, replay, and relevant analytics commands first. This is the only default action.
3. Report snapshot degradation before conclusions. If any observation failed, state which command failed and how that limits confidence.
4. Separate observed evidence from hypotheses. Label command output, JSON fields, inbox rows, workflow states, and ticket matches as evidence. Label inferred causes or suggested next actions as hypotheses.
5. Deduplicate existing tickets before proposing new work. Inspect native ticket identities and coalesce keys before recommending another ticket; title similarity alone is not a duplicate proof.
6. Recommend an existing workflow definition where possible. Prefer a workflow already listed by `workflow defs --repo <repo>` over inventing a new shape.
7. Render and save the exact typed dispatch proposal using `propose_workflow_run` when the `rk` MCP server is available; otherwise use `rk --json factory propose-workflow`. Include its `proposal_id`, digest, and execution action, and preserve the proposal file or tool result exactly.
8. Stop and request approval for that exact proposal and digest. Do not continue to approval or dispatch in the same turn.
9. After a later user message explicitly approves it, use `approve_action` and `execute_approved_workflow_run` through MCP when available; otherwise use `rk --json factory approve --proposal-file <file>` and `rk --json factory execute-action --proposal-file <file>`. A changed workflow, repo, parameter, coordinator, proposal, or digest requires a new proposal and approval.
10. Monitor the returned workflow ID with `rk --json workflow status <id>` or `rk --json workflow watch <id>` through completion, failure, or approval wait. workflow watch --json is NDJSON and must not be parsed as one JSON document. Document notable NDJSON events as they arrive.

## Proposal handling

Render native typed proposals with:

```bash
rk --json factory propose-workflow <workflow> --repo <repo> [--param KEY=VALUE] [--coordinator <agent>] > factory-proposal.json
```

Save the exact JSON proposal somewhere durable in the conversation or a task-specific report. The saved data must include:

- `proposal_id`
- canonical lowercase-hex `digest`
- typed `execution_action`
- workflow, daemon-resolved repository scope, parameters, and coordinator when present

Approval is valid only when it arrives in a later user message and names the exact proposal or digest already rendered. The daemon reloads its persisted proposal, recomputes the digest, revalidates scope and caller, and rejects edited, stale, expired, consumed, or mismatched envelopes.

## Output discipline

Use this structure when reporting triage:

- Snapshot health and degradation.
- Observed evidence.
- Hypotheses.
- Existing ticket matches and dedupe result.
- Existing workflow definition recommendation.
- Proposed command and `proposal_id`, if dispatch is appropriate.
- Explicit approval request, if and only if mutation is needed.

## Authority boundary

The skill may run read-only Rat Kingdom inspection commands by default and may prepare dispatch proposals. It has no authority to mutate Rat Kingdom state until a later user message approves the exact proposal and the daemon verifies the canonical digest. RK, not Jcode or the skill, owns repository resolution, authenticated identity, approval lifecycle, compare-and-swap checks, idempotency, and execution.

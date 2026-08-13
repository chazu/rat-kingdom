---
name: factory-foreman
description: Triage Rat Kingdom software factory health, classify failures, and prepare approval-gated dispatch proposals.
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

Use this repository-local skill when the user asks about the Rat Kingdom factory, fleet health, RK inbox, workflow failures, factory triage, dispatch work, or the software factory.

Default command, read-only by default:

```bash
python3 .jcode/skills/factory-foreman/scripts/factory_foreman.py triage --repo <repo> --format markdown
```

Never execute a mutating Rat Kingdom command unless a later user message explicitly approves the exact command rendered in this conversation. For typed execution paths, require daemon-verifiable approval of the exact canonical digest before dispatch. The Phase 1 helper proposal validation is only a fallback guard for legacy/manual flows, not the authority for typed execution.

See [REFERENCE.md](REFERENCE.md) for categories, schemas, approval examples, and recovery behavior.

## Dashboard rendering

When typed factory snapshot and event replay JSON are available, prefer them as the dashboard inputs and render the repository-owned Markdown view with:

```bash
python3 .jcode/skills/factory-foreman/dashboard/render_factory_dashboard.py \
  --snapshot <factory-snapshot.json> \
  --events <factory-events-replay.json> \
  --output <factory-dashboard.md>
```

The renderer is a pure Python, standard-library presentation step. It reads the two existing JSON files and writes deterministic Markdown to `--output`. It does not start or contact the Rat Kingdom daemon, invoke `rk`, invoke `rk-mcp`, expose an MCP tool, or dispatch any action. Obtain snapshot and replay data separately through an authorized typed MCP or CLI read path, then pass the saved files to the renderer. The view displays whether inputs are live or replayed, consumes typed replay `boundary` metadata and event `kind` fields, and tolerates legacy aliases only for backwards-compatible display.

Opening the generated Markdown in a Jcode side panel is optional. The file is a status view, not a control plane. Approval labels, proposal digests, replay boundaries, degraded sources, and resync state are display data only and confer no execution authority.

## Workflow

1. Resolve the repository name without guessing when ambiguous. If the requested repo is unclear, ask for the repo before running repo-scoped commands.
2. Run read-only triage first with the default command. This is the only default action.
3. Report snapshot degradation before conclusions. If any observation failed, state which command failed and how that limits confidence.
4. Separate observed evidence from hypotheses. Label command output, JSON fields, inbox rows, workflow states, and ticket matches as evidence. Label inferred causes or suggested next actions as hypotheses.
5. Deduplicate existing tickets before proposing new work. Search or inspect ticket data from triage before recommending another ticket.
6. Recommend an existing workflow definition where possible. Prefer a workflow already listed by `workflow defs --repo <repo>` over inventing a new shape.
7. Render and save the exact dispatch proposal using `propose-workflow`, including its `proposal_id` and `argv`. Preserve the command exactly as displayed in the conversation. Use this Phase 1 helper path only when a daemon-verifiable typed approval flow is unavailable.
8. Stop and request approval for that exact `proposal_id` and displayed command. For typed execution, require daemon-verifiable approval of the exact canonical digest before dispatch. Do not continue to a mutating dispatch step in the same turn.
9. After a later user message explicitly approves that proposal ID or exact command, run `validate-proposal` and execute only the validated argv for the fallback helper flow. A changed workflow, repo, parameter, coordinator, or argv order requires a new proposal and new approval.
10. Monitor the returned workflow ID with `rk --json workflow status <id>` or `rk --json workflow watch <id>` through completion, failure, or approval wait. workflow watch --json is NDJSON and must not be parsed as one JSON document. Document notable NDJSON events as they arrive.

## Proposal handling

Render proposals with:

```bash
python3 .jcode/skills/factory-foreman/scripts/factory_foreman.py propose-workflow <workflow> --repo <repo> [--param KEY=VALUE] [--coordinator <agent>]
```

Save the exact JSON proposal somewhere durable in the conversation or a task-specific report. The saved data must include:

- `proposal_id`
- `argv`
- rendered shell `command`
- workflow, repo, parameters, and coordinator when present

Approval is valid only when it arrives in a later user message and names the exact `proposal_id` or exact command already rendered. If validation returns different `argv`, stop and request reapproval.

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

The skill may run read-only Rat Kingdom inspection commands by default. It may prepare dispatch proposals. It has no authority to mutate Rat Kingdom state until a later user message approves the exact proposal or command and the execution path verifies the exact digest. Typed execution requires daemon-verifiable exact digest approval. The Phase 1 helper `validate-proposal` path remains a fallback for legacy/manual dispatch preparation only.

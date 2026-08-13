# Factory Foreman

Factory Foreman is a repository-local Jcode skill for read-only Rat Kingdom factory triage. It collects deterministic snapshots from existing `rk --json` commands, classifies known reliability symptoms, and can prepare workflow dispatch proposals that require explicit human approval before any mutation.

Phase 1 intentionally changes no Rat Kingdom daemon, workflow, ticket, repository-policy, or harness semantics.

## Installation and discovery

The skill lives in this repository at `.jcode/skills/factory-foreman/`. Jcode discovers repository-local skills from that directory when working in this checkout. Use it for Rat Kingdom factory, fleet health, RK inbox, workflow failures, factory triage, dispatch work, and software factory requests.

The helper uses Python 3 standard library only:

```bash
python3 .jcode/skills/factory-foreman/scripts/factory_foreman.py triage --repo rat-kingdom --format markdown
```

The `rk` executable must already be on `PATH`, and the Rat Kingdom daemon must already be running. The helper first runs strict preflight:

```bash
rk --json daemon status
```

If preflight fails, the helper stops and does not run observation commands that could auto-start the daemon.

## Commands

### Snapshot

```bash
python3 .jcode/skills/factory-foreman/scripts/factory_foreman.py snapshot \
  --repo rat-kingdom \
  --format json
```

Outputs the read-only snapshot only.

### Triage

```bash
python3 .jcode/skills/factory-foreman/scripts/factory_foreman.py triage \
  --repo rat-kingdom \
  --format json
```

Outputs findings plus the snapshot. Markdown output is available with `--format markdown`.

### Propose workflow

```bash
python3 .jcode/skills/factory-foreman/scripts/factory_foreman.py propose-workflow <workflow> \
  --repo rat-kingdom \
  --param KEY=VALUE \
  --coordinator <session-id>
```

`--param` is repeatable and must be exactly `KEY=VALUE`. The command renders a proposal only. It never executes `rk workflow run`.

### Validate proposal

```bash
python3 .jcode/skills/factory-foreman/scripts/factory_foreman.py validate-proposal \
  --proposal-file <proposal.json> \
  --approved-id <proposal_id>
```

Validation reloads the saved proposal, recomputes the SHA-256 proposal ID from canonical compact JSON for the `argv` list, and emits the exact `argv` only when it matches the approved ID. It never executes the `argv`.

## Read-only observation set

After successful strict preflight, the helper runs only these observation commands:

```text
rk --json list
rk --json inbox
rk --json workflow list
rk --json cost --fleet
rk --json repo show <repo>
rk --json workflow defs --repo <repo filesystem path>
rk --json ticket list --repo <repo>
```

`workflow defs` resolves the registered repository filesystem path. The helper first reads `rk --json repo show <repo>` and, when that response contains a non-empty `path`, passes that filesystem path to `rk --json workflow defs --repo`. This matches workflow definition lookup to the repository registration instead of relying on the repository name string.

A failed observation is preserved in the snapshot and does not discard successful observations.

## JSON schema

### Triage JSON

```json
{
  "schema": 1,
  "repo": "rat-kingdom",
  "findings": [
    {
      "category": "named-check-failure",
      "severity": "medium",
      "subject": "cargo test",
      "summary": "Repository check failed: cargo test.",
      "evidence": "source JSON or command evidence, truncated at 12000 characters",
      "recommended_next_step": "Open the named check output and fix the first failing assertion or command.",
      "workflow_instance": "workflow id when known, otherwise null",
      "agent": "agent id when known, otherwise null"
    }
  ],
  "snapshot": {
    "schema": "factory-foreman.snapshot.v1",
    "generated_at": "1970-01-01T00:00:00Z",
    "repo": "rat-kingdom",
    "healthy": true,
    "observations": {
      "agents": {"ok": true, "command": "rk --json list", "data": []},
      "inbox": {"ok": true, "command": "rk --json inbox", "data": []},
      "workflows": {"ok": true, "command": "rk --json workflow list", "data": []},
      "cost": {"ok": true, "command": "rk --json cost --fleet", "data": {}},
      "repository": {"ok": true, "command": "rk --json repo show rat-kingdom", "data": {}},
      "definitions": {"ok": true, "command": "rk --json workflow defs --repo <repo filesystem path>", "data": {}},
      "tickets": {"ok": true, "command": "rk --json ticket list --repo rat-kingdom", "data": []}
    },
    "errors": []
  },
  "snapshot_health": {
    "healthy": true,
    "errors": []
  },
  "observations": {
    "agents": {"ok": true, "command": "rk --json list", "data": []}
  }
}
```

When an observation fails, its object is:

```json
{"ok": false, "command": "rk --json inbox", "error": "exit 1: observed error text"}
```

### Proposal JSON

```json
{
  "proposal_id": "sha256 hex digest of canonical compact JSON argv",
  "argv": ["rk", "--json", "workflow", "run", "repair", "--repo", "rat-kingdom", "--param", "ticket=42"],
  "command": "rk --json workflow run repair --repo rat-kingdom --param ticket=42"
}
```

The rendered shell command is for human review. The `argv` list is the execution identity.

## Triage categories

The Phase 1 helper emits these categories:

- `missing-rk-executable`: `rk: command not found` appears in failed workflow or observation evidence. Severity: critical. Recommended next step: install `rk` or fix `PATH` before rerunning the workflow.
- `empty-harness-result`: a failed workflow has `declared_done:false`, blank result, blank error, zero usage, and no actionable harness output. Severity: high. Recommended next step: treat the worker result as non-actionable and rerun or inspect worker startup logs.
- `named-check-failure`: a named run step, check, command, or row has a non-zero exit code. Severity: medium. Recommended next step: open the named check output and fix the first failing assertion or command.
- `workflow-timeout`: `timed_out:true`, exit code `124`, or the explicit phrase `timed out` is present. Severity: high. Recommended next step: inspect the timed-out step and rerun with narrower scope or a longer timeout.
- `orphaned-agent`: an inbox row has kind `agent-orphaned`. Severity: high. Recommended next step: reconcile the orphaned agent with the workflow registry before reassigning work.
- `budget-pressure`: an instance has spent at least 80 percent of its own `instance_max_usd` or equivalent instance budget. Severity: medium. Recommended next step: reduce scope, raise the instance budget, or stop the workflow before continuing.
- `permission-or-authority`: failed evidence contains permission denied, unauthorized, or forbidden. Severity: high. Recommended next step: grant the required permission or change the task to an allowed action.
- `stale-or-moved-base`: failed evidence contains stale base, base moved, branch moved, or non-fast-forward. Severity: medium. Recommended next step: refresh from the current base and rerun the affected workflow.
- `unknown`: a failed row did not match a known deterministic classifier. Severity: low. Recommended next step: preserve the evidence and add a classifier once the failure pattern is understood.

Unknown findings are intentionally preserved. They need future classifiers when recurring patterns become understood.

## Evidence versus hypothesis discipline

Treat command output, parsed JSON fields, inbox rows, workflow states, costs, and ticket matches as evidence. Treat inferred causes, likely fixes, and dispatch recommendations as hypotheses.

Factory Foreman classifications are deterministic triage hints, not proven root causes. Do not claim causality from a category alone. Report snapshot degradation before conclusions, and lower confidence when observations are missing or failed.

## Approval boundary and proposal validation

Read-only inspection is allowed by default. Mutations are not.

The skill may prepare dispatch proposals, but it must not execute mutating Rat Kingdom commands until a later user message explicitly approves the exact rendered proposal ID or exact command. An initial request to inspect, triage, fix, or improve the factory is not dispatch approval.

Exact boundary:

1. Run read-only triage.
2. Choose an existing workflow definition when dispatch is appropriate.
3. Render and save a `propose-workflow` JSON proposal with `proposal_id`, `argv`, and `command`.
4. Stop and ask for approval of that exact `proposal_id` or exact command.
5. Only after a later user message approves it, run `validate-proposal`.
6. Execute only the validated `argv` returned by validation.
7. If workflow, repo, parameter, coordinator, or argv order changed, render a new proposal and require new approval.

Approval identity is proposal-digest checked in the helper, while the fact that a human approved remains enforced by the Jcode skill rather than cryptographically attested by the daemon.

The helper never executes `workflow run`, `spawn`, `dismiss`, `approve`, `reject`, `revert`, ticket mutations, or tuple writes during snapshot, triage, proposal rendering, or proposal validation.

## Pure Python dashboard renderer

The repository-owned dashboard is a deterministic Markdown renderer over saved typed factory data. First acquire a `factory.snapshot` response and a `factory.events.replay` response through an authorized typed CLI or MCP read path and save each response as JSON. Then run:

```bash
python3 .jcode/skills/factory-foreman/dashboard/render_factory_dashboard.py \
  --snapshot "$JCODE_SCRATCH_DIR/factory-snapshot.json" \
  --events "$JCODE_SCRATCH_DIR/factory-events.json" \
  --output "$JCODE_SCRATCH_DIR/factory-dashboard.md"
```

Inputs and output:

- `--snapshot PATH` reads one snapshot JSON document. The renderer presents connection state, approvals, workflow runs, agents, tickets, inbox, budget, degraded sources, and `repo_resync` fields when supplied.
- `--events PATH` reads one event replay JSON document. The renderer presents recent events together with replay cursor, truncation, and boundary information when supplied.
- `--output PATH` writes the rendered Markdown file. Parent directories and input artifacts are prepared by the caller.

The Markdown view includes `Factory Dashboard`, `Connection State`, `Resync State`, `Approvals`, `Workflow Runs`, `Agents`, `Tickets`, `Inbox`, `Budget`, `Recent Events`, and `Degraded Data`. It keeps partial-history and health conditions visible: a truncated replay shows its boundary, a running repository resync is labelled `RESYNCING`, and stale or failed sources are labelled `DEGRADED`.

### Renderer boundary

The renderer uses the Python standard library only and is deliberately not an integration client:

- It does not connect to or start the Rat Kingdom daemon.
- It does not invoke the `rk` CLI.
- It does not invoke `rk-mcp`, host an MCP server, or expose MCP tools.
- It does not fetch live state, approve proposals, execute commands, or mutate Rat Kingdom state.

CLI and MCP are upstream options for producing the input files. The renderer only reads those files and writes Markdown. The output can be displayed in a Jcode side panel, but the renderer and side panel are not a control plane or source of truth. Proposal rows remain proposals until authoritative daemon snapshot data says they are approved, and display of an approval or digest never grants execution authority.

## Monitoring after approved dispatch

After an approved and validated workflow dispatch, monitor the returned workflow ID with:

```bash
rk --json workflow status <id>
rk --json workflow watch <id>
```

`workflow watch --json` emits NDJSON. Process one line at a time, document meaningful events as they arrive, and do not parse the whole stream as one JSON document.

Monitor until completion, failure, or an approval wait. When approval is needed, report the waiting state and the actor or gate when present.

## Recovery behavior

- Ambiguous repository: stop and ask for the repository name.
- Daemon unavailable: stop after strict `rk --json daemon status`; do not auto-start the daemon.
- Degraded snapshot: preserve successes, name failed observations, and lower confidence.
- Duplicate ticket evidence: recommend updating the existing ticket instead of creating another.
- No suitable workflow definition: state the gap and avoid dispatch until a human chooses or defines a workflow.
- Validation mismatch: stop, render a new proposal, and request new approval.
- Unknown category: preserve the original evidence and treat it as a candidate for future classifiers.

## Live read-only acceptance evidence

The 2026-08-13 local acceptance run wrote its JSON artifact under `$JCODE_SCRATCH_DIR` and recorded:

- schema `1` and repo `rat-kingdom`.
- 7/7 healthy observations: `agents`, `cost`, `definitions`, `inbox`, `repository`, `tickets`, and `workflows`.
- no snapshot errors.
- 24 findings.
- observed categories: `budget-pressure`, `empty-harness-result`, `missing-rk-executable`, `orphaned-agent`, and `unknown`.
- `named-check-failure` absent because no current row carried matching named-check evidence.

Unknown findings in that report need future classifiers once the failure patterns are understood.

## Phase 1 limitations

Phase 1 limitations are deliberate:

- no typed MCP transport.
- no daemon subscription.
- no cryptographic or daemon-enforced approval token.
- no automatic dispatch.
- read commands are preceded by strict `rk --json daemon status`, but once connected they may still cause ordinary daemon logging or access-time state.
- approval identity is proposal-digest checked in the helper, while the fact that a human approved remains enforced by the Jcode skill rather than cryptographically attested by the daemon.
- no CI, deployment, or production signal ingestion.
- deterministic hints are not root causes.

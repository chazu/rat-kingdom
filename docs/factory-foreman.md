# Factory Foreman

Factory Foreman combines a repository-local Jcode triage skill with native typed Rat Kingdom factory interfaces. The Python helper collects deterministic read-only observations and classifies known reliability symptoms. The native CLI and MCP paths expose daemon snapshots and events plus an approval-gated `workflow.run` path whose authority remains in the daemon.

The Python helper remains a compatibility and triage surface. It is not the source of typed approval or execution authority.

## Native typed factory interface

The global JSON flag comes before the subcommand:

```bash
rk factory dashboard --repo <registered-name-or-path>
rk --json factory dashboard --repo <registered-name-or-path>
rk --json factory snapshot --repo <registered-name-or-path>
rk --json factory events replay --repo <registered-name-or-path> --after <cursor> --limit 256
rk --json factory events watch --repo <registered-name-or-path> --after <cursor>
```

`dashboard` is the primary human entry point. It auto-starts the daemon when needed, fetches the native snapshot and recent replay, and renders bounded Markdown tables directly in the terminal. `--row-limit` and `--event-limit` control display size. With global `--json`, it emits a `factory.dashboard.v1` envelope containing the native snapshot and replay responses.

`snapshot` and `events replay` are finite reads and do not auto-start the daemon. `events watch` first emits the replay page as NDJSON event rows, then continues with live projected events. A missing daemon is an error for these lower-level read commands. Proposal, approval, and execution commands may connect or start the daemon through the ordinary CLI client behavior.

The typed mutation sequence is:

```bash
rk --json factory propose-workflow <workflow> \
  --repo <registered-name-or-path> \
  --param KEY=VALUE \
  --coordinator <session-id>

rk --json factory approve <proposal-id> <digest>

rk --json factory execute-workflow <proposal-id> <digest> \
  --workflow <workflow> \
  --repo <same-registered-name-or-path> \
  --param KEY=VALUE \
  --coordinator <same-session-id>
```

`--param` is repeatable and CLI values are strings. Execution must repeat the exact typed action. A changed workflow, repository, parameter, or coordinator does not reuse the approval.

Proposal-producing CLI commands also emit a structured JSON envelope that can be
saved and forwarded through the generic public factory path:

```bash
rk --json <proposal-producing-command> ... > proposal.json
rk --json factory approve --proposal-file proposal.json
rk --json factory execute-action --proposal-file proposal.json
```

The envelope carries `proposal_id`, `digest`, `kind`, and the original typed
`execution_action`. The daemon remains authoritative: it compares the forwarded
envelope with its persisted canonical proposal and rejects edits, stale scope,
expired or consumed approvals, and digest or caller mismatches. The positional
`factory approve <proposal-id> <digest>` and workflow-specific
`execute-workflow` commands remain available.

### Daemon digest authority

Human-readable commands, dashboard text, MCP tool text, and the Phase 1 helper digest are not execution authority. For the native typed path, the daemon:

1. resolves the supplied repository name or path against the registered repository record;
2. replaces caller-supplied repository identity and path with the registered identity and canonical filesystem path;
3. takes the requester and approving operator from authenticated request identity rather than action parameters;
4. creates a versioned proposal and computes lowercase SHA-256 over recursively key-sorted compact JSON;
5. persists the proposal and exact approval grant;
6. immediately before execution, resolves the repository again and recomputes the action digest;
7. rejects missing, unknown, expired, consumed, caller-mismatched, kind-mismatched, scope-mismatched, or tampered approvals.

The immutable digest payload contains `schema`, `kind`, `risk`, daemon-resolved repository `scope` (`identity` and canonical `path`), authenticated `requester`, typed `action`, random `nonce`, and `expires_at`. Proposal lifecycle fields such as `id`, `digest`, `created_at`, and `status` are not digest input. For `workflow.run`, the action covers workflow name, repository reference, daemon-resolved identity/path, sorted parameters, and optional coordinator.

Approval grants persist `proposal_id`, digest, kind, scope, requester, authenticated `approved_by`, timestamps, and status. Status advances through `approved`, `executing`, and `consumed`, or `failed`. Persisted execution and instance IDs make retry/restart handling idempotent enough to return or resume the recorded execution instead of starting a second workflow. A consumed approval cannot execute twice.

This does **not** mean legacy `workflow.run` is globally digest-gated. Existing direct operator CLI/RPC workflows remain available. Jcode and MCP automation that needs this approval boundary must use the typed `factory.*` proposal path.

### Five MCP tools

`rk-mcp` is a local stdio JSON-RPC/MCP facade. `tools/list` publishes exactly these schema-versioned tools:

| Tool | Purpose | Required arguments |
| --- | --- | --- |
| `factory_snapshot` | Read one finite daemon snapshot. | `schema: 1`, `repo` |
| `factory_events_replay` | Read one bounded event page. | `schema: 1`, `repo`, `limit`, optional `kinds`, optional `after` |
| `propose_workflow_run` | Create, but do not execute, a typed proposal. | `schema: 1`, `workflow`, `repo`, optional `params`, optional `coordinator`, optional `ttl` |
| `approve_action` | Approve one proposal by exact digest. | `schema: 1`, `proposal_id`, `digest` |
| `execute_approved_workflow_run` | Execute the exact approved action. | `schema: 1`, `proposal_id`, `digest`, nested `action` matching the workflow request schema |

Unknown fields are rejected. The MCP process connects to the local daemon without spawning it, forwards daemon RPC error codes in structured tool errors, and inherits daemon authentication and caller semantics. It offers bounded replay, not a streaming watch tool.

### Snapshot, replay, and watch semantics

`factory.snapshot` returns schema `1`, the latest durable cursor, and a snapshot containing filtered agents, workflows, tickets, approvals (`proposals` and `grants`), budget, inbox, and repository resync state. Repository filters accept a registered repository name or path. `--include-archived` includes archived agent and workflow records.

Factory events are a projection of existing durable coordinator events whose tuple identity is `factory_event`. Each projected event has schema, monotonic cursor, occurrence time, kind, repo, authenticated caller, source, subject, summary, and payload. This is not a second journal.

Replay scans events strictly after `after`, returns at most 256 events, and includes `boundary` plus `truncated`. Filters may restrict repository and repeat `--kind`. Because projection and filtering happen after the bounded durable scan, a page may contain fewer matching projected events than the scan limit.

Watch subscribes before replay, returns the finite replay response, then streams `factory.event` notifications after the replay boundary while suppressing duplicate cursors. If durable catch-up is truncated it emits `factory.resync` with `resync_required: true` and a boundary. If the live broadcast receiver lags, the CLI reports a `lagged` notification and the daemon performs durable catch-up before continuing. Consumers must treat the cursor as the ordering/resume token and refresh the snapshot when resync is required.

## Installation and discovery

The human-facing dashboard is part of the Rust `rk` binary. From a registered repository, run:

```bash
rk factory dashboard --repo rat-kingdom
```

No separate daemon-start command is required. The dashboard connects to the daemon or starts it through the same guarded Rust client path used by other operator commands.

The repository-local Jcode compatibility skill lives at `.jcode/skills/factory-foreman/`. Jcode discovers repository-local skills from that directory when working in this checkout. Its Python helper remains available for legacy deterministic triage and fixture-driven skill tests:

```bash
python3 .jcode/skills/factory-foreman/scripts/factory_foreman.py triage --repo rat-kingdom --format markdown
```

The compatibility helper uses only the Python 3 standard library. Unlike the Rust dashboard, it deliberately never starts the daemon. It first runs strict preflight:

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

Validation reloads the saved proposal, recomputes the SHA-256 proposal ID from canonical compact JSON for the `argv` list, and emits the exact `argv` only when it matches the approved ID. It never executes the `argv`. This is the Phase 1 fallback guard. Typed execution must verify daemon-recorded approval of the exact canonical digest instead of trusting the helper alone.

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

## Read-only self-optimization scorecards and recommendations

Phase 5 Factory Foreman analytics add two read-only daemon RPCs and matching CLI commands:

```bash
rk --json factory scorecards --repo <registered-name-or-path> --group-by all --include-archived
rk --json factory recommend --repo <registered-name-or-path> --group-by all --include-archived
```

The daemon methods are `factory.scorecards` and `factory.recommend`. They are advisory observation APIs only. They do not rewrite policy, config, workflow definitions, workflow instances, tickets, approval grants, queues, routing tables, agent records, or dispatch state. They do not spawn agents, enqueue workflows, approve gates, close tickets, land changes, revert changes, run shell commands, or apply recommendations. Existing static routing remains authoritative and unchanged.

### Durable structured outcome source semantics

Scorecards are derived only from durable structured source families that the daemon can read today. Phase 5 currently reconstructs analytic events from typed daemon records, snapshots, and structured SDLC CI event history. It does not have a separate durable analytics journal, and it does not infer outcomes from text. Each derived event carries schema version, deterministic event id, repository, source family, source id/version when available, archive marker and reason when available, observed timestamp when available, explicit dimensions, linked ids, recurrence/coalescing keys when available, and one structured metric payload.

The normal event shape is:

```json
{
  "schema_version": 1,
  "event_id": "stable sha256 over canonical structured fields",
  "repo": "rat-kingdom",
  "source_family": "AgentRecord",
  "source_id": "run-or-source-id",
  "source_version": "optional version",
  "archived": false,
  "archive_reason": null,
  "observed_at_ms": 0,
  "task_class": "explicit-task-class-or-null",
  "workflow": "workflow-id-or-null",
  "harness": "harness-id-or-null",
  "model": "model-id-or-null",
  "agent_id": "agent-id-or-null",
  "workflow_instance_id": "instance-id-or-null",
  "ticket_id": "ticket-id-or-null",
  "phase3_outcome_id": "outcome-id-or-null",
  "phase4_signal_id": "signal-id-or-null",
  "recurrence_key": "recurrence-key-or-null",
  "coalesce_key": "coalesce-key-or-null",
  "metric_payload": {"kind": "run", "count": 1}
}
```

The event id is deterministic over canonical structured fields. It must not include ingestion time, current clock, vector order, map iteration order, transcript text, terminal text, or other unstable inputs.

### Structured source-to-metric matrix

Factory analytics are structured-source-only. They never parse raw logs, prose, issue comments, Markdown, prompt text, transcript text, terminal output, console output, or unstructured agent chatter. Missing fields become `unknown` or `unobserved`, not synthetic success or failure.

The currently available structured families are:

- `AgentRecord`: durable agent/run records with explicit ids and available dimensions such as model, harness, timestamps, status, and linked workflow data when present.
- `WorkflowInstance`: durable workflow instances with explicit ids, workflow names, lifecycle timestamps, status, and linked agent data when present.
- `Phase4CiSignal`: structured SDLC CI event history for a key. Explicit `ci_failed` and `ci_recovered` events are normalized when their tuple identity, scope, family, kind, delivery id, subject, and commit correlation are structured.
- `HumanGateDecision`: structured approval or gate decision facts for human intervention counting.
- `RecurrenceKey`: structured recurrence or ticket coalescing keys for recurrence counting.

The following structured families are not available to Phase 5 today and must remain `unobserved` until explicit read seams exist:

- Phase 3 contract outcome and verified delivery enumerators for acceptance or landed delivery.
- Structured reviewer rework transitions.
- Typed revert history linked to runs, tickets, or landed outcomes.
- `PricingSnapshot` records for durable model pricing provenance.

Agent cost fields are not sufficient cost provenance by themselves. `AgentRecord` may carry usage or cost-like values, but Phase 5 must not report `cost_micro_usd` unless a durable pricing snapshot or an equally explicit structured cost provenance record is available for that run. No such source is available today, so cost metrics remain unavailable.

Structured SDLC CI history can report explicit CI failures and explicit recoveries. A recovery counts only when the recovered event references a matching prior explicit failure for the same repository, structured subject, workflow, and commit correlation, with the failure observed earlier. Current green CI alone is not recovery evidence.

| Metric or dimension | Structured source only | Forbidden inference | Missing behavior |
| --- | --- | --- | --- |
| `runs` | Explicit `AgentRecord` or workflow `Instance` ids. | Counting log lines, prompt files, status prose, or terminal output. | Unobserved unless explicit run dimensions exist. |
| `task_class` | Explicit Phase 3 contract, ticket field, or structured outcome field. | Titles, labels, prose, filenames, workflow names, model names, prompts, or summaries. | Dimension is `unknown`; recommendations that require task class are suppressed. |
| `workflow` | Workflow `Instance` id/name. | Command text, transcript text, or terminal output. | Dimension is `unknown`; same workflow comparisons are suppressed. |
| `harness` | Explicit harness field on agent/run/instance data. | Binary path, CLI output, labels, or model route. | Dimension is `unknown` and can still be aggregated. |
| `model` | Explicit model field on agent/run data. | Prompt text, transcript text, agent label, or inferred provider. | Dimension is `unknown` and can still be aggregated. |
| `accepted` | Phase 3 verified delivery or land structured outcome enumerator. This source is not available today. | Absence of rework, green CI, closing prose, or issue comments. | Metric unavailable for that run unless explicit outcome exists. |
| `reworked` | Structured reviewer rework transition. This source is not available today. | Review comment prose, TODO text, or labels. | Metric unavailable for that run. |
| `ci_failed` | Explicit structured SDLC CI event with kind `ci_failed` for the same structured subject and commit correlation. | Build-log text, terminal text, or later acceptance. | Metric unavailable unless an explicit structured failure event exists. |
| `ci_recovered` | Explicit structured SDLC CI event with kind `ci_recovered`, counted only when a matching prior explicit `ci_failed` event exists for the same structured subject and commit correlation. | Current green CI alone, build-log text, or later acceptance. | Metric unavailable unless the explicit recovery has matching prior failure evidence. |
| `reverted` | Typed revert history linked to run, ticket, or landed outcome. This source is not available today. | Commit-message search or prose. | Metric unavailable for that run. |
| `human_interventions` | Explicit `HumanGateDecision`, approval, or gate decision event. | Mentions, comments, delay, or approval prose. | Metric unavailable for that run. |
| `recurrence` | Explicit non-empty `recurrence_key` or `RecurrenceKey` ticket coalesce key. | Similarity over titles, stack traces, logs, or prose. | Metric unavailable without explicit key. |
| `cost_micro_usd` | Durable `PricingSnapshot`-backed cost provenance or another explicit durable cost provenance record. No such source is available today. | Estimating from model name alone, current pricing tables, or unproven `AgentRecord` cost-like fields. | Metric unavailable for that run. |
| `lead_time_ms` | Structured lifecycle timestamps for the same run. | File mtimes, commit times, transcript timestamps, or terminal timestamps. | Metric unavailable on missing or invalid timestamps. |
| `archive_state` | Explicit source archive marker or archived-history read API. | Treating absence from an active query as archived. | Unknown archive state is `unobserved`. |

`task_class` provenance is explicit by design. A task class must originate from a Phase 3 contract, ticket field, or structured outcome field. It is never inferred from logs, prose, labels, titles, filenames, workflow names, model names, harness names, or prompt text.

### Archive semantics and missing sources

Archived history is counted separately from active history. `include_archived=false` excludes archived events from metric numerators and denominators while still reporting archived availability and source counts. `include_archived=true` includes archived events in metrics and also exposes active/archived splits.

A source is archived only when that source family carries an archive marker or is returned by an explicit archived-history API. Completed workflow instances are not archived merely because they are completed. Closed tickets are archived only when the ticket store defines or exposes that state as archived. Old CI signals, approvals, and outcomes are not archived by age alone.

Missing source families are reported as unobserved, not as zero. Responses preserve source health with availability and source counts, including `available`, `active_source_count`, `archived_source_count`, and `event_count`. An unavailable metric must not look healthy, and recommendations must not emit action text for unavailable metrics.

### Scorecard response schema and metrics

`factory.scorecards` returns schema `1`, `repo`, `include_archived`, `scorecards`, top-level `source_counts`, and top-level `availability`. Rows are deterministic: sorted by composite group key, projection, metric key, and stable identifiers.

Each scorecard row contains:

- `group_key`: `task_class`, `workflow`, `harness`, and `model`, using `unknown` when a structured dimension is absent.
- `projection`: `composite`, `task_class`, `workflow`, `harness`, `model`, `task_class_workflow`, or `all`.
- `projected`: whether the row is a projection rather than the primary composite row.
- `metrics`: `runs`, `accepted`, `reworked`, `ci_failed`, `ci_recovered`, `reverted`, `unknown`, `unobserved`, `active_runs`, `archived_runs`, `total_cost_micro_usd`, `average_cost_micro_usd`, `cost_sample_size`, `median_lead_time_ms`, `p95_lead_time_ms`, `lead_time_sample_size`, `human_interventions`, `intervention_sample_size`, `recurrence_count`, `distinct_recurrence_keys`, and `recurrence_sample_size`.
- `status_counts`: explicit outcome status totals.
- `evidence_counts`: available evidence counts by structured source family.
- `source_counts`: active, archived, and event counts by source family.
- `availability`: source-family availability, with missing families represented as unobserved.
- `sample_size`: explicit run sample size used for row-level recommendations.

Costs are integer micro-USD. Lead time uses explicit start and completion timestamps for the same run. Median and p95 use deterministic nearest-rank selection over available lead-time values. Recurrence counts only explicit recurrence/coalesce keys that repeat within the same composite group.

### Deterministic advisory recommendation rules

`factory.recommend` returns schema `1`, `repo`, `include_archived`, the scorecards evidence, top-level `source_counts`, top-level `availability`, and an advisory recommendation report. The report includes `nature: "advisory"`, `recommendations`, `suppressions`, and `warnings`.

Recommendation rules are deterministic and thresholded:

| Rule | Minimum sample | Trigger |
| --- | ---: | --- |
| `low_acceptance` | 10 | Acceptance below 60% when a comparable peer in the same task class and workflow is at least 80%. |
| `high_rework` | 10 | Rework rate at least 25%. |
| `ci_instability` | 8 | CI failure or instability rate at least 15%. |
| `reverts` | 5 | Revert rate at least 10%. |
| `high_cost` | 8 | Average cost at least 1.5x the comparable median and above the absolute configured floor. |
| `slow_lead_time` | 8 | p95 lead time at least 1.5x the comparable median. |
| `human_intervention` | 8 | Human intervention rate at least 30%. |
| `recurrence` | 5 | Recurrence count at least 3. |

Low-sample suppression hides recommendation action text while still reporting observed metrics, sample size, availability, source counts, and exact suppression reason. Suppression reasons include `below_threshold`, `low_sample`, `metric_unavailable`, and `no_comparable_peer`.

Peer comparisons are valid only between comparable profiles with the same `task_class` and `workflow`. Rules may compare different harness and/or model profiles inside that boundary, but they must not compare across task classes or workflows. If a metric family is unavailable, recommendations may warn that it is unavailable or unobserved, but they must not advise an action from that metric.

### Display and safety boundary

Factory Foreman displays may render scorecards and recommendations after strict daemon preflight, but they remain display-only. They must not add apply buttons, approval shortcuts, dispatch buttons, mutation links, policy/config editors, workflow editors, ticket mutation controls, queue mutation controls, or approval/gate decision controls.

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
4. Stop and ask for approval of that exact `proposal_id` or exact command. Typed execution must use daemon-verifiable approval of the exact canonical digest.
5. Only after a later user message approves it, run `validate-proposal` for the Phase 1 fallback helper flow.
6. Execute only the validated `argv` returned by validation for that fallback flow, or only the daemon-verified digest-approved typed action for typed execution.
7. If workflow, repo, parameter, coordinator, or argv order changed, render a new proposal and require new approval.

Approval identity is proposal-digest checked in the helper for the Phase 1 fallback flow. Typed execution requires daemon-verifiable exact digest approval before dispatch; human approval text and helper validation alone are not sufficient for typed execution.

The helper never executes `workflow run`, `spawn`, `dismiss`, `approve`, `reject`, `revert`, ticket mutations, or tuple writes during snapshot, triage, proposal rendering, or proposal validation.

## Offline Python compatibility renderer

The old repository-owned Python renderer remains available for offline fixtures, Jcode side-panel integration, and compatibility tests. It is not the normal operator launch path. First acquire a `factory.snapshot` response and a `factory.events.replay` response through an authorized typed CLI or MCP read path and save each response as JSON. Then run:

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

The Markdown view includes `Factory Dashboard`, `Data Source`, `Connection State`, `Resync State`, `Approvals`, `Workflow Runs`, `Agents`, `Tickets`, `Inbox`, `Budget`, `Recent Events`, and `Degraded Data`. It keeps partial-history and health conditions visible: a truncated replay shows its typed `boundary` when present, event rows use typed `kind` while tolerating legacy `type`, a running repository resync is labelled `RESYNCING`, and stale or failed sources are labelled `DEGRADED`. Malformed `connection` values are rendered as malformed input rather than raising a traceback.

### Renderer boundary

The renderer uses the Python standard library only and is deliberately not an integration client:

- It does not connect to or start the Rat Kingdom daemon.
- It does not invoke the `rk` CLI.
- It does not invoke `rk-mcp`, host an MCP server, or expose MCP tools.
- It does not fetch live state, approve proposals, execute commands, or mutate Rat Kingdom state.

CLI and MCP are upstream options for producing the input files. The renderer only reads those files and writes Markdown. The output can be displayed in a Jcode side panel, but the renderer and side panel are not a control plane or source of truth. Proposal rows remain proposals until authoritative daemon snapshot data says they are approved, and display of an approval or digest never grants execution authority. Typed execution still requires daemon-verifiable exact digest approval; the Phase 1 helper remains a fallback for legacy/manual dispatch preparation.

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

## Product-to-code lifecycle

The product-to-code lifecycle turns a product initiative into implemented,
independently verified code through offline, contract-validated artifacts and
daemon-executed, operator-approved actions. It reuses the same Phase 2 canonical
approval boundary documented above: the CLI prepares and forwards exact typed
envelopes, while the daemon alone applies them after an authenticated operator
approval with status, digest, and CAS checks. See
[product-to-code.md](./product-to-code.md) for the
full lifecycle, commands, contracts, and safety boundaries.

The two mutating steps are both canonical typed factory actions:

- `ticket_graph.apply` mints `TKT-...` tickets and dependency edges from a
  validated ticket graph, recording the graph-node-id to minted-ticket-id
  mapping under the consumed approval.
- `product_to_code.dispatch` launches `implement-featureset` workflow runs for
  the unblocked minted tickets of a previously approved graph apply. Graph nodes
  without current generic impact evidence are reported as blocked, never
  dispatched, and never leak a minted ticket id.

Both actions are validated, approved, and executed exclusively through
`factory.propose_action`, `factory.approve_action`, and
`factory.execute_action`. The public CLI exposes that boundary through saved
proposal files:

```bash
rk --json product-to-code graph propose-apply ... > graph-proposal.json
rk --json factory approve --proposal-file graph-proposal.json
rk --json factory execute-action --proposal-file graph-proposal.json

rk --json product-to-code workflow propose ... > dispatch-proposal.json
rk --json factory approve --proposal-file dispatch-proposal.json
rk --json factory execute-action --proposal-file dispatch-proposal.json
```

There is no local apply or dispatch shortcut, and RK performs no runtime call to
Jcode, browser automation, or GitNexus during the lifecycle.

## Phase 2 limitations

- The five-tool MCP surface supports only the initial `workflow.run` proposal, approval, and execution flow. The native CLI additionally forwards saved `ticket_graph.apply` and `product_to_code.dispatch` envelopes through the generic daemon action boundary. This is not a claim that every mutation in Rat Kingdom is typed or gated this way.
- Legacy direct mutation RPCs and operator CLI flows may remain. In particular, legacy `workflow.run` is not globally gated. Jcode/MCP callers that require digest binding must use `factory.propose_action`, `factory.approve_action`, and `factory.execute_action` through the typed surfaces.
- The factory event feed projects existing coordinator events. It is not external CI, deployment, production, or general SDLC ingestion, and it is not a separate durable journal.
- `rk-mcp` uses local stdio and local daemon connectivity. It has no remote transport and no MCP streaming watch tool.
- The dashboard is a pure renderer over saved snapshot and replay files. It is not live, authoritative, or a control plane.
- The Python triage helper provides deterministic hints, not proven root causes or daemon authority. Its legacy `argv` digest is not interchangeable with the daemon canonical typed-action digest.
- A canonical digest proves that approval and execution refer to the same exact typed action, scope, requester, nonce, and expiry. It does not prove the workflow is safe, useful, or a good human decision.
- Snapshot and replay are bounded views. Truncation or resync signals require cursor-aware recovery and a fresh snapshot.
- Disposable acceptance must use an isolated `RK_HOME` plus a disposable registered Git repository and workflow path. Do not run mutating acceptance against an ambient Rat Kingdom daemon or the working repository.

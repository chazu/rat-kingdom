# Factory Foreman Reference

This reference supports the `factory-foreman` skill for Rat Kingdom factory, fleet health, RK inbox, workflow failures, factory triage, dispatch work, and software factory requests.

## Categories

Triage categories emitted by the helper include:

- `missing-rk-executable`: `rk` is unavailable to a worker or environment.
- `empty-harness-result`: a workflow returned no declared harness result.
- `named-check-failure`: a named check such as `cargo test` or `cargo clippy` failed.
- `workflow-timeout`: a workflow instance exceeded its timeout.
- `orphaned-agent`: an agent appears detached from an active workflow.
- `budget-pressure`: a workflow instance is close to its configured budget.
- `permission-or-authority`: a worker lacks permission or authority for the requested action.
- `stale-or-moved-base`: a workflow appears based on stale or moved repository state.
- `unknown`: evidence did not match a known category and must be preserved.

## JSON schema

`triage --format json` returns a top-level object shaped like:

```json
{
  "schema": 1,
  "repo": "repository name",
  "findings": [
    {
      "category": "named-check-failure",
      "severity": "high",
      "subject": "cargo test",
      "summary": "Named check failed.",
      "evidence": "observed output or JSON field",
      "recommended_next_step": "Inspect and rerun the named check before landing.",
      "workflow_instance": "workflow id when known",
      "agent": "agent id when known"
    }
  ],
  "snapshot": {
    "schema": "factory-foreman.snapshot.v1",
    "generated_at": "1970-01-01T00:00:00Z",
    "repo": "repository name",
    "healthy": false,
    "observations": {
      "agents": {"ok": true, "command": "rk --json list", "data": {}},
      "inbox": {"ok": true, "command": "rk --json inbox", "data": {}},
      "workflows": {"ok": true, "command": "rk --json workflow list", "data": {}},
      "definitions": {"ok": true, "command": "rk --json workflow defs --repo <repo>", "data": {}},
      "cost": {"ok": true, "command": "rk --json cost --fleet", "data": {}},
      "repository": {"ok": true, "command": "rk --json repo show <repo>", "data": {}},
      "tickets": {"ok": true, "command": "rk --json ticket list --repo <repo>", "data": {}}
    },
    "errors": []
  },
  "snapshot_health": {
    "healthy": false,
    "errors": []
  },
  "observations": {
    "definitions": {"ok": true, "command": "rk --json workflow defs --repo <repo>", "data": {}}
  }
}
```

A failed observation remains in top-level `observations` and nested `snapshot.observations` with `ok: false` and an `error`. Report that degradation before drawing conclusions.

## Dashboard renderer input and output

The bundled legacy dashboard renderer consumes JSON artifacts that have already been obtained from typed factory read interfaces:

- `--snapshot PATH`: one JSON document from `factory.snapshot`, containing the current connection state and factory projection such as approvals, workflow runs, agents, tickets, inbox, budget, degraded sources, and repository resync state when present. The renderer displays the snapshot source so readers can distinguish live data from saved or replayed data.
- `--events PATH`: one JSON document from `factory.events.replay`, containing replay metadata such as cursor, typed replay `boundary`, `truncated`, and the recent event list. Event rows use typed event `kind`; legacy `boundary_cursor` and `type` aliases are tolerated for old artifacts.
- `--output PATH`: the Markdown file to create or replace.

Run the globally installed copy from any repository:

```bash
python3 ~/.jcode/skills/factory-foreman/dashboard/render_factory_dashboard.py \
  --snapshot "$JCODE_SCRATCH_DIR/factory-snapshot.json" \
  --events "$JCODE_SCRATCH_DIR/factory-events.json" \
  --output "$JCODE_SCRATCH_DIR/factory-dashboard.md"
```

The output is deterministic Markdown with sections for data source, connection and resync state, approvals, workflow runs, agents, tickets, inbox, budget, recent events, and degraded data. Replay truncation and its boundary must remain visible so a reader does not mistake a partial replay for complete history. A running resync is rendered as resyncing, while stale or failed inputs remain visibly degraded. Malformed `connection` values are rendered as malformed input instead of raising a traceback.

The renderer uses only the Python standard library. Its boundary is intentionally strict:

- no daemon connection or daemon startup;
- no `rk` CLI subprocesses;
- no `rk-mcp` subprocesses or MCP server/tool behavior;
- no proposal approval, workflow execution, or other mutation;
- no authority beyond the contents of the supplied files.

MCP and CLI are upstream ways to acquire typed JSON, not dependencies called by the renderer. A Jcode side panel may display the generated Markdown, but neither the renderer nor the side panel is a source of truth or an execution boundary. Daemon state remains authoritative, including whether a proposal is actually approved. Typed execution requires daemon-verifiable exact digest approval; the Phase 1 helper remains a fallback for legacy/manual proposal validation only.

## Approval examples

Valid later-user approvals include:

- `Approve proposal_id abc123 and its displayed digest exactly as shown.`
- `Approve the exact saved factory proposal in factory-proposal.json.`

Invalid approvals include:

- Approval in the same assistant turn as proposal rendering.
- `Looks good` without the exact proposal ID and digest.
- Approval after any workflow, repo, parameter, coordinator, proposal field, or digest changed.

For the legacy Phase 1 fallback, run:

```bash
python3 ~/.jcode/skills/factory-foreman/scripts/factory_foreman.py validate-proposal --proposal-file <file> --approved-id <proposal_id>
```

The helper output is inspection-only legacy data. Do not execute its returned `argv`. Native execution must use a saved typed factory proposal and daemon-recorded approval of the exact canonical digest. Require a new proposal and approval for any change.

## Recovery behavior

- Ambiguous repo: stop and ask for the repository name.
- Degraded snapshot: preserve successful observations, name failed observations, and lower confidence.
- Duplicate ticket: recommend updating the existing ticket instead of creating another.
- No suitable workflow definition: state the gap and avoid dispatch until the user chooses or defines a workflow.
- Validation mismatch: stop, render a new proposal, and request new approval.
- Workflow execution accepted: monitor `rk --json workflow status <id>` or `rk --json workflow watch <id>`.
- `workflow watch --json` emits NDJSON. Process one line at a time, document each meaningful event, and do not parse the whole stream as one JSON document.
- Approval wait: report the waiting state and the actor or gate needed when present.

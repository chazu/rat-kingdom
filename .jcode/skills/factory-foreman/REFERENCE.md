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

## Approval examples

Valid later-user approvals include:

- `Approve proposal_id abc123 exactly as shown.`
- `Approve: rk --json workflow run repair --repo rat-kingdom --param ticket=42`

Invalid approvals include:

- Approval in the same assistant turn as proposal rendering.
- `Looks good` without the exact proposal ID or exact command.
- Approval after any workflow, repo, parameter, coordinator, or argv order changed.

Before execution, run:

```bash
python3 .jcode/skills/factory-foreman/scripts/factory_foreman.py validate-proposal --proposal-file <file> --approved-id <proposal_id>
```

Then execute only the returned validated `argv`. Require reapproval for any changed argv.

## Recovery behavior

- Ambiguous repo: stop and ask for the repository name.
- Degraded snapshot: preserve successful observations, name failed observations, and lower confidence.
- Duplicate ticket: recommend updating the existing ticket instead of creating another.
- No suitable workflow definition: state the gap and avoid dispatch until the user chooses or defines a workflow.
- Validation mismatch: stop, render a new proposal, and request new approval.
- Workflow execution accepted: monitor `rk --json workflow status <id>` or `rk --json workflow watch <id>`.
- `workflow watch --json` emits NDJSON. Process one line at a time, document each meaningful event, and do not parse the whole stream as one JSON document.
- Approval wait: report the waiting state and the actor or gate needed when present.

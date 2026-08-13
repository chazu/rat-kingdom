# Factory Foreman Reference

This reference supports the `factory-foreman` skill for Rat Kingdom factory, fleet health, RK inbox, workflow failures, factory triage, dispatch work, and software factory requests.

## Categories

Common triage categories emitted by the helper include:

- `missing-rk-executable`: `rk` is unavailable to a worker or environment.
- `empty-harness-result`: a workflow returned no declared harness result.
- `named-check-failure`: a named check such as `cargo test` or `cargo clippy` failed.
- `workflow-timeout`: a workflow instance exceeded its timeout.
- `orphaned-agent`: an agent appears detached from an active workflow.
- `budget-pressure`: a workflow instance is close to its configured budget.
- `unknown`: evidence did not match a known category and must be preserved.

## JSON schema

`triage --format json` returns a top-level object shaped like:

```json
{
  "schema": 1,
  "generated_at": "ISO-8601 timestamp",
  "repo": "repository name",
  "healthy": false,
  "snapshot": {
    "observations": {
      "agents": {"ok": true, "command": "rk --json list", "data": {}},
      "inbox": {"ok": true, "command": "rk --json inbox", "data": {}},
      "workflows": {"ok": true, "command": "rk --json workflow list", "data": {}},
      "workflow_defs": {"ok": true, "command": "rk --json workflow defs --repo <repo>", "data": {}},
      "cost": {"ok": true, "command": "rk --json cost --fleet", "data": {}},
      "repository": {"ok": true, "command": "rk --json repo show <repo>", "data": {}},
      "tickets": {"ok": true, "command": "rk --json ticket list --repo <repo>", "data": {}}
    },
    "errors": []
  },
  "findings": [
    {
      "category": "named-check-failure",
      "severity": "high",
      "subject": "cargo test",
      "evidence": "observed output or JSON field",
      "workflow_instance": "workflow id when known",
      "agent": "agent id when known"
    }
  ]
}
```

A failed observation remains in `observations` with `ok: false` and an `error`. Report that degradation before drawing conclusions.

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

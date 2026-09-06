# Factory Foreman Reference

## Native evidence

Use `rk --json factory snapshot --repo <repo>` and
`rk --json factory events replay --repo <repo> --limit 256` for bounded,
read-only inspection. These commands fail if the daemon is unavailable. The
interactive `factory dashboard` can start the daemon and is for human operators.

The snapshot response has numeric `schema: 1`, numeric `cursor`, and a nested
`snapshot` object containing agents, workflows, tickets, inbox, budget, approvals,
and repository resync state. Replay has numeric `schema: 1`, an `events` array,
boolean `truncated`, and nullable numeric `boundary`. Events have numeric `cursor`
and string `kind`; process watch output as NDJSON, one event per line.

Preserve missing sources as unobserved. Inspect native workflow states, inbox
rows, current work, and check evidence before proposing a cause. Use
`rk --json work <repo>` for current decisions, actionable work, ready tickets,
and stalls; use snapshot/replay for context and history. The retired Python
classifier's prose-derived categories are not an additional source of truth.
`factory scorecards` and `factory recommend` are structured, advisory analytics;
they do not change routing or authorize dispatch.

## Saved rendering

```bash
rk factory render --snapshot factory-snapshot.json \
  --events factory-events.json --output factory-dashboard.md \
  --row-limit 20 --event-limit 20
```

This finite offline command reads the two files and writes Markdown (stdout
when `--output` is omitted). It never contacts or starts the daemon or initializes
RK state. Its SAVED / NOT CONNECTED labels distinguish historical input from
current observations. Missing source families are DEGRADED, resync and replay
truncation remain visible, and approval labels and digests are display data only.
The command rejects obsolete flattened snapshots and string event cursors; capture
new native responses instead. Global `--json` emits a `factory.dashboard.v1`
envelope with `source: "saved"` and the two original responses.

## Typed proposals and approval

```bash
rk --json factory propose-workflow <workflow> --repo <repo> > proposal.json
# After a later user message approves this exact saved proposal:
rk --json factory approve --proposal-file proposal.json
rk --json factory execute-action --proposal-file proposal.json
```

Prefer the equivalent typed MCP tools when available. Preserve the proposal ID,
canonical digest, and typed execution action. A later user approval must identify
the exact previously rendered proposal. A changed action, scope, parameter,
coordinator, nonce, or expiry requires a new proposal and approval. The daemon
reloads the durable proposal, verifies identity, digest and lifecycle, and owns
execution. Retired argv hashes cannot be converted into native approvals.

## Recovery

- Ambiguous repository: resolve the registered name before scoped reads.
- Unavailable daemon: report the failed read; do not turn inspection into startup.
- Degraded snapshot: preserve successful evidence and name the missing sources.
- Truncated replay or resync: acquire a fresh snapshot and resume from its cursor.
- Unknown failure: preserve the original structured evidence and label hypotheses.
- Existing ticket: inspect its identity and status before proposing duplicate work.
- No suitable workflow: report the gap before proposing dispatch.
- Validation mismatch or expiry: render a new proposal for later approval.
- Accepted dispatch: monitor `rk --json workflow status <id>` or
  `rk --json workflow watch <id>` through completion, failure, or a gate wait.

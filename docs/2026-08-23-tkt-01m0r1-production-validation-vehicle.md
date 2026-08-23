# TKT-01M0R1PBWCFRHQ2G6CPSJMCKKT: production validation vehicle for task-to-main and digest telemetry

Fresh post-deployment validation delivery for telemetry build `38b83ff66555`.

That build carries two fixes from the task-to-main tracer program
(TKT-01M0P2KNB92EAV2QG9256MY3QV):

- TKT-01M0QZFFT9WFDTG0CS4GVD03QX — `task_to_main_ms` is populated by threading
  `Merge` and `TicketReady` span timestamps.
- TKT-01M0QZFTYQW4WV200TYGCN46XA — `rk digest` phase-latency aggregation
  reaches recent `task_span` events instead of the oldest 10k.

This ticket is the vehicle for proving both fixes live once this branch lands
and the daemon is redeployed. It intentionally lands with no post-merge
numbers recorded — this commit predates its own merge, so there is nothing
real to report yet.

## Post-deployment operator commands

After this branch merges to `main` and the daemon is redeployed, the operator
runs:

```
rk status TKT-01M0R1PBWCFRHQ2G6CPSJMCKKT --json
```

Expected: `task_to_main_ms` is non-null, backed by this ticket's own
`TicketReady` and `Merge` span timestamps.

```
rk digest --since 30m --json
```

Expected: `phase_latency.window_spans` is greater than zero, computed from
`task_span` events recorded in the last 30 minutes (which includes this
ticket's own delivery).

## Recording results

Do not fabricate these values here. Once the operator has run the two
commands above against the live post-merge state, the actual output is
recorded as a durable artifact (`rk out artifact rat-kingdom
tkt-01m0r1-validation-result --payload '{...}'`), not edited into this file.

# External observation runs

`rk observe` captures repository-scoped operational evidence for pilots, soak
tests, releases, benchmarks, and incident windows. It is an external process:
sampling uses a read-only connection and never starts, rolls over, or repairs
the daemon it audits. A missing daemon therefore becomes an outage sample
instead of disappearing behind auto-recovery.

## Start a run

Choose numeric thresholds before dispatch. The output directory must not
already exist.

```sh
rk observe start \
  --repo Voxel \
  --name foreign-tenant \
  --ticket TKT-... \
  --interval 30s \
  --stale-after 15m \
  --max-landing-age 10m \
  --max-ready-age 15m \
  --max-cost-usd 100 \
  --output "$HOME/.rat-kingdom-observations/voxel-foreign-tenant"
```

Without `--output`, RK creates a ULID-named directory under
`$HOME/.rat-kingdom-observations`. Without `--duration`, the command samples
until Ctrl-C and then writes `report.json`. Evidence remains usable if the
observer is terminated abruptly:

```sh
rk observe sample "$RUN"
rk observe report "$RUN" --finalize
```

`sample` and `report` return nonzero when collection or thresholds fail. The
report treats partial RPC samples as a failure, so an unsupported or broken
read surface cannot silently turn into zero metrics.

## Record interventions

Every intervention has one structural class; arbitrary free-form class names
are rejected.

```sh
rk observe record "$RUN" \
  --class human-gate \
  --ticket TKT-... \
  --summary "approved the protected-path change after reviewing the diff" \
  --evidence event:01... \
  --evidence commit:abc123
```

Classes are:

- `mechanical`: a deterministic automated recovery or correction.
- `llm`: an LLM-orchestrator judgment within delegated authority.
- `human-gate`: an interaction required by predeclared policy.
- `ad-hoc`: an unplanned rescue or judgment. This must stay visible rather
  than being relabeled as a gate after the fact.

Each record is a separately created JSON file. Concurrent writers therefore
cannot corrupt a shared journal line.

## Evidence and metrics

An observation directory contains:

- `manifest.json`: immutable scope, ticket set, build, interval, and thresholds.
- `samples.jsonl`: append-only external samples with raw bounded read models,
  event deltas, and per-sample metrics.
- `interventions/*.json`: atomic typed intervention records.
- `report.json`: reproducible derived result.

The report covers:

- delivered-ticket throughput and repository-attributed token/cost deltas;
- maximum ready-ticket and landing-queue age, including transient spikes;
- daemon outages/restarts and King generation replacements;
- observer/daemon build parity and partial read-surface failures;
- observer sample cadence and planned-duration coverage;
- convergence violations, stale tickets, and unclassified work holds;
- overlapping live generations for one task and repeated landed side effects;
- forced ungated landings;
- intervention counts by class.

Ready-queue age is observation-window time, not ticket lifetime. It starts when
a selected ticket first appears ready in a sample, accumulates while the ticket
remains continuously ready, and resets if the ticket leaves and later re-enters
the ready queue. Its resolution is therefore the observer interval. This keeps
pre-registered or dependency-blocked work from inheriting preflight wall time
while still failing a run that leaves actionable work undispatched.

A stale ticket is ownerless work already in `claimed`, `in_progress`, or
`blocked` state whose ticket record has not changed within `--stale-after`.
Open dependency-blocked tickets are not stale, and work with a live agent is
covered by liveness and phase telemetry instead. Ready open work is measured by
the ready-queue-age check rather than counted a second time as stale.

Spend is derived from matching agent generations active or updated during the
run, including archived records, as a run-window delta. It is not the live-fleet snapshot shown by
`rk cost --fleet`. Agent results, transcripts, and historical King checkpoints
are excluded from samples; only fields needed to join, attribute, and audit the
run are retained.

For the supervised foreign-tenant pilot, a passing report requires zero daemon
outage samples, convergence violations, forced landings, duplicate dispatches,
duplicate landings, stale tickets, and unclassified holds. Any failed check
means repair and repeat or stop; it is not a passing pilot with a footnote.

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
  --rpc-timeout 5s \
  --sample-timeout 20s \
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
rk observe resume "$RUN"
rk observe sample "$RUN"
rk observe report "$RUN" --finalize
```

`sample` and `report` return nonzero when collection or thresholds fail. The
report treats partial RPC samples as a failure, so an unsupported or broken
read surface cannot silently turn into zero metrics.

`resume` continues the original immutable manifest and duration window. Time
offline remains a gap. Only one collector can own a run. Each RPC and the whole
sequence have deadlines; a timeout closes that connection, skips remaining reads
and records a partial sample with the failed method and elapsed time. Cadence
skips missed ticks rather than issuing catch-up bursts.

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
- `collector.json`: replaceable checkpoint of the event cursor, sequence and
  current ready streaks. Normal collection reads only new evidence. Missing or
  invalid checkpoints rebuild by streaming `samples.jsonl`.
- `recovery/*.partial`: original bytes of interrupted appends. Recovery preserves
  complete rows and quarantines an incomplete final row before resuming. This is
  explicit failure evidence for the report's interrupted-append check.
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

For explicit `--ticket` roots, the cohort includes daemon-created landing and
conflict corrections linked by their durable coalescing identity. Generic parent
links and matching prose do not establish membership. Expansion is bounded by
`--max-lineage-depth` (8) and `--max-lineage-tickets` (256 descendants); exceeding
either records incomplete coverage and fails the run. Correction usage counts
toward the roots, and a live descendant keeps its ancestors from appearing
ownerless. Unrelated agents are excluded. Requested-ticket throughput and
`correction_deliveries` remain separate. Interventions may reference a root or a
correction already present in saved samples.

Reports replay the manifest, samples, interventions and recovery evidence; the
checkpoint is unnecessary. Older evidence cannot gain omitted correction data
retroactively. Collector fixes require a fresh pre-registered pilot.

For the supervised foreign-tenant pilot, a passing report requires zero daemon
outage samples, convergence violations, forced landings, duplicate dispatches,
duplicate landings, stale tickets, and unclassified holds. Any failed check
means repair and repeat or stop; it is not a passing pilot with a footnote.

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
  --progress-stall-after 15m \
  --max-wait 30m \
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

### Declared human-gate waits

A `human-gate` record can additionally bind a bounded progress-clock
exemption (not just a report tally) by supplying `--owner <rat-name>` and
`--spawn <spawn-id>`:

```sh
rk observe record "$RUN" \
  --class human-gate \
  --ticket TKT-... \
  --owner Gruyere-14 \
  --spawn S123 \
  --summary "waiting on operator sign-off before continuing" \
  --evidence bbs:need-01
```

Both fields are optional and default to absent on old records; a record
missing either one is counted in `interventions` but never exempts a stall
-- absent or ambiguous evidence is never a trusted exemption. When both are
present, the collector's independent progress evaluator (below) treats it
exactly like the existing self-declared/queued bounded waits: it exempts the
matching ticket+owner+generation from `progress-stalled-tickets` starting at
the record's own `observed_at`, for the run's frozen `--max-wait` allowance,
and no longer. A duplicate or later re-declaration of the same gate cannot
push that deadline out, a declaration cannot retroactively excuse a sample
observed before it was written, and a generation replacement (new `spawn`)
never inherits a predecessor's declaration.

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
- intervention counts by class;
- independently derived progress stalls and progress-evidence gaps for live
  generations (D1).

Ready-queue age is observation-window time, not ticket lifetime. It starts when
a selected ticket first appears ready in a sample, accumulates while the ticket
remains continuously ready, and resets if the ticket leaves and later re-enters
the ready queue. Its resolution is therefore the observer interval. This keeps
pre-registered or dependency-blocked work from inheriting preflight wall time
while still failing a run that leaves actionable work undispatched.

A stale ticket is ownerless work already in `claimed`, `in_progress`, or
`blocked` state whose ticket record has not changed within `--stale-after`.
Open dependency-blocked tickets are not stale, and work with a live agent is
covered by the progress evaluator below instead. Ready open work is measured
by the ready-queue-age check rather than counted a second time as stale.

### Independent progress evaluation (D1)

A ticket with a `spawning`/`running` agent is excluded from ownerless-ticket
staleness above, so a failed daemon supervisor sweep could otherwise leave a
genuinely wedged generation looking healthy. The collector independently
proves forward progress instead of trusting the daemon's own stuck-sweep
classification (`work.current`'s `stalled` bucket):

- Evidence is bound to canonical ticket identity, generation (`spawn`),
  provider session (`session_id`) and RK execution attempt (`liveness.session`).
  A resume can reuse the provider session while starting a new attempt; neither
  it nor a concurrent generation inherits another attempt's progress clock.
- A signature combines structured checkpoint **content** (summary, next step,
  status), changed-output fingerprint, lifecycle state and reported result.
  Repeating the same checkpoint with a new revision/timestamp is not progress.
  Output churn during a transport retry does not advance the clock either.
- Unchanged evidence past `--progress-stall-after` fails
  `progress-stalled-tickets`. A self-declared verification, queue, review,
  human-gate or recovery-backoff phase receives a fixed `--max-wait` deadline;
  checkpoint chatter cannot renew a continuing wait. The agent's own
  self-reported status string is advisory evidence, not a permission grant or
  independent proof that an actual workflow approval exists.
- A typed `human-gate` intervention record (see "Declared human-gate waits"
  above) supplies an independently authoritative bounded wait instead: it
  requires an explicit `--owner`/`--spawn` match against the exact live
  generation, not just a self-reported status string, and shares the same
  fixed `--max-wait` deadline anchored to the record's own `observed_at`.
  Missing or mismatched ticket/owner/generation evidence never grants this
  exemption. The daemon-side workflow-gate producer and the integration that
  joins it to this observer are tracked separately (TKT-rahit-hihud-vusuv,
  TKT-lokoj-zidup-fujih).
- `status.landing_queue_tasks` supplies authoritative queued-admission evidence
  by repository, ticket and recorded source generation. Legacy aliases resolve
  through the observed ticket set; an older unbound entry cannot excuse a worker
  created after that queue phase began. The durable phase age consumes the same
  fixed `--max-wait` allowance, including time before observation started.
- Missing identities or usable evidence, and backwards observation clocks,
  fail `progress-evidence-gaps`. Sampling gaps and unavailable RPCs also remain
  explicit failures under the run's frozen coverage requirements.
- Reports retain each stall's ticket/attempt, start, resolution and recurrence,
  including work already delivered or replaced. Counts accumulate across the
  entire observed cohort instead of dropping with the live-agent count.
- Live collection, checkpoint reconstruction and offline reports use the same
  transition function over raw samples. Cached metric fields cannot hide an
  observed stall. Qualification evaluator version 2 consumes these incidents;
  checkpoint format/evaluator changes force replay rather than fresh grace.

The bound is observation-window time: the first sample establishes an attempt's
baseline. Detection occurs after its frozen bound, plus the sampling interval
and the RPC/sample deadline allowance. This does not infer unobserved progress
history before collection began. Authoritative queue phase ages are the exception:
they describe a persisted wait that can already be expired at the first sample.

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
duplicate landings, stale tickets, unclassified holds, independently derived
progress stalls, and progress-evidence gaps. Any failed check means repair and
repeat or stop; it is not a passing pilot with a footnote.


Declared wait evidence is frozen into each new sample's
`declared_interventions` field. Live evaluation, checkpoint reconstruction and
report replay use that same evidence; a later declaration cannot rewrite an
earlier sample even if the wall clock moves backward. Historical samples with
no frozen declarations grant no declared-wait exemption. Collector caches are
versioned and rebuilt when evaluator semantics change.

Ticket aliases are resolved against the sample's canonical ticket rows. A
productive, non-wait checkpoint retires the generation's declared allowance;
output chatter and repeated declarations cannot restart it. Subsequent silence
uses the ordinary progress bound, and the earlier stall episode remains in the
report. This slice provides one declared allowance per observed generation;
multiple distinct gate episodes within a generation need a future explicit
episode identity rather than renewing the same allowance implicitly.

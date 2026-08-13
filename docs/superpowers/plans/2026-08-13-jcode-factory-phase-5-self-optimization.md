# Jcode Factory Foreman Phase 5 Self-Optimization Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add deterministic read-only Factory Foreman self-optimization scorecards and advisory recommendations from normalized structured outcomes, so operators can see which task classes, workflows, harnesses, models, and recurrences are reliable or costly without changing existing static routing, policy, workflows, tickets, or dispatch behavior.

**Architecture:** After Phases 3 and 4 are implemented, Rat Kingdom records a durable structured outcome ledger from explicit structured seams only. The ledger is built from immutable snapshots or narrow read APIs over `AgentRecord`, workflow `Instance`, Phase 3 verified delivery/land outcomes, Phase 3 explicit task contract/ticket/outcome metadata, structured reviewer rework transitions, Phase 4 CI signals, structured revert handler events, explicit human gate/approval/decision events, and explicit recurrence or coalescing keys. A read-only daemon service in the actual daemon protocol and handler files (`crates/rk-daemon/src/proto.rs` and `crates/rk-daemon/src/server.rs`, or their current equivalents after inspection) aggregates those facts into deterministic scorecards grouped by the composite key `(task_class, workflow, harness, model)`, with optional projections by one or more dimensions for display. CLI commands expose the same read-only data through global JSON mode, for example `rk --json factory scorecards` and `rk --json factory recommend`. Recommendation rules are deterministic advisory functions with explicit thresholds, comparison evidence, evidence counts, archived-history counts, source availability, and low-sample suppression. Optional Factory Foreman display consumes these read-only calls only after strict daemon preflight. Existing static routing remains unchanged, and recommendations cannot rewrite policy, config, workflows, tickets, queues, approvals, or dispatch.

**Tech Stack:** Rust Rat Kingdom daemon and CLI, existing storage abstractions, deterministic clock injection, serde JSON, fixed-point integer micro-USD accounting, existing test harnesses, Markdown documentation.

## Global Constraints

- Write behavior in small test-driven increments. Each behavior gets a failing test before implementation.
- Edit implementation files only when executing this plan. This plan itself is design guidance.
- Derive outcome facts only from structured seams listed in the source-to-metric matrix below. Never parse raw logs, transcript text, terminal output logs, Markdown prose, issue comments, or unstructured agent chatter.
- `task_class` must come from an explicit Phase 3 contract, ticket field, or structured outcome field. Never infer it from prose, labels, titles, filenames, model names, workflow names, or prompt text.
- Recommendations are advisory and read-only. They must not mutate repository policy, config, workflow definitions, workflow instances, tickets, queues, agent records, routing tables, approvals, or dispatch state.
- Existing static routing remains authoritative and unchanged. Phase 5 may compare against routing choices but must not replace or auto-edit them.
- Count archived history separately and include archived counts per source family and per scorecard row. Do not silently mix active and archived rows.
- Missing source families or fields are `unobserved`, not zero. Report availability counts and source counts so unavailable metrics do not look healthy.
- Scorecards must include runs, accepted, reworked, CI failed, CI recovered, reverted, cost, lead time, human interventions, recurrence, evidence counts, availability counts, and source counts when source data exists.
- Missing structured fields must produce explicit `unknown` or `unobserved` facts instead of synthetic success or failure.
- Deterministic ordering is required for all maps, rows, and JSON arrays: sort by composite group key, optional projection key, metric key, then stable identifier.
- Low-sample suppression must hide recommendation action text while still reporting observed metrics, source availability, sample size, and exact suppression reason.
- Recommendation comparisons are valid only among comparable profiles with the same `task_class` and `workflow`. Do not compare across task classes or workflows.
- Recommendations must never emit advice for unavailable metrics. They may emit warnings that a metric family is unavailable or unobserved.
- Daemon RPCs and CLI commands added in this phase are read-only. They must be safe to call from Factory Foreman displays.
- Optional Factory Foreman display work may render scorecards and recommendations only. It must not add approval shortcuts, apply buttons, dispatch buttons, or mutation links.
- Follow existing repository style and verification commands. Commit each independently testable task when implementing this plan, unless the operator explicitly says not to commit.

## Canonical Metric Semantics

### Durable structured outcome ledger

Add a durable append-only or snapshot-derived structured ledger event, not a heuristic metric cache:

```rust
FactoryOutcomeEvent {
    schema_version: u32,
    event_id: StableHash,
    repo: RepoId,
    source_family: SourceFamily,
    source_id: String,
    source_version: Option<String>,
    archived: bool,
    archive_reason: Option<String>,
    observed_at_ms: i64,
    task_class: Option<ExplicitTaskClass>,
    workflow: Option<WorkflowId>,
    harness: Option<HarnessId>,
    model: Option<ModelId>,
    agent_id: Option<AgentId>,
    workflow_instance_id: Option<InstanceId>,
    ticket_id: Option<TicketId>,
    phase3_outcome_id: Option<String>,
    phase4_signal_id: Option<String>,
    recurrence_key: Option<String>,
    coalesce_key: Option<String>,
    metric_payload: FactoryMetricPayload,
}
```

`event_id` is a deterministic hash over `schema_version`, `repo`, `source_family`, `source_id`, `source_version`, canonical dimensions, canonical metric payload, `archived`, and `observed_at_ms` when the source itself carries a stable event timestamp. It must not include ingestion time, current clock, vector order, or map iteration order. The ledger may be materialized in storage or reconstructed from immutable snapshots, but daemon and CLI responses must expose ledger semantics with source counts and availability.

### Source-to-metric matrix

Only the matrix below may populate metrics. Any unavailable source family is reported as `unobserved` with `available=false`, `active_source_count`, `archived_source_count`, and `event_count`.

| Metric or dimension | Required structured source | Accepted event/value | Forbidden inference | Missing behavior |
| --- | --- | --- | --- | --- |
| `runs` | `AgentRecord` and workflow `Instance` joined by explicit ids | One run per explicit agent/workflow execution record, deduped by stable run id | Counting log lines, prompt files, status prose, or terminal output | Row exists only if dimensions are available, otherwise source family is `unobserved` |
| `task_class` | Phase 3 contract, ticket field, or structured outcome field | Exact explicit enum/string from contract/ticket/outcome | Inferring from prose, title, labels, model, workflow, file path, or summary | Dimension is `unknown`; recommendations requiring task class are suppressed |
| `workflow` | Workflow `Instance` | Exact workflow id/name in instance | Inferring from command text or transcript | Dimension is `unknown`; comparable-profile recommendations suppressed |
| `harness` | `AgentRecord`/Instance explicit harness field | Exact harness id/name | Inferring from binary path, CLI output, or model route | Dimension is `unknown`; still aggregate under unknown projection |
| `model` | `AgentRecord` explicit model field | Exact model route/name recorded for the run | Inferring from prompt, transcript, or agent label | Dimension is `unknown`; still aggregate under unknown projection |
| `accepted` | Phase 3 verified delivery/land structured outcome | `verified_delivery=true` or `landed=true` with outcome id | Treating absence of rework, green CI, or closing text as accepted | `accepted` unavailable for that run; not counted as false unless explicit negative exists |
| `reworked` | Structured reviewer rework transition | Transition such as `review_state: accepted -> rework_requested` or explicit `rework_requested` event | Parsing review comments or TODO prose | Rework metric unavailable for that run |
| `ci_failed` | Phase 4 CI signal | Explicit failed conclusion for associated run/commit | Parsing build logs or console text | CI metric unavailable for that run |
| `ci_recovered` | Phase 4 CI signal | Explicit prior failed signal followed by explicit recovered/pass signal for same run/commit key | Assuming later acceptance means recovery | Recovery metric unavailable unless both signals exist |
| `reverted` | Structured revert handler | Explicit revert event referencing run/ticket/landed outcome | Searching commit messages for "revert" | Revert metric unavailable for that run |
| `human_interventions` | Explicit gate, approval, or decision event | Count of structured human decision events linked to run | Inferring from comments, mentions, delay, or approval prose | Intervention metric unavailable for that run |
| `recurrence` | Explicit `recurrence_key` or coalesce key | Count repeated non-empty recurrence/coalesce keys within the same composite group | Similarity over issue titles, prose, stack traces, or logs | Recurrence unavailable for rows without explicit key |
| `cost_micro_usd` | `AgentRecord` structured cost or token usage plus explicit pricing snapshot | Integer micro-USD after defined conversion | Estimating from model name without pricing snapshot | Cost metric unavailable for that run |
| `lead_time_ms` | Structured lifecycle timestamps from `AgentRecord`, workflow `Instance`, or Phase 3 outcome | `completed_at_ms - started_at_ms` using explicit timestamps for the same run | Inferring from file mtimes, commit times, or transcript timestamps | Lead-time metric unavailable for that run |
| `archive_state` | Source storage archive marker per source family | Active or archived with source family and reason if present | Treating missing from active query as archived | Unknown archive state is `unobserved` and excluded unless explicitly requested |

### Archive semantics per source

- `AgentRecord`: archived means the agent/run record is marked archived or returned only by an explicit archived-history read API. Active and archived records are both counted in `source_counts` when available.
- Workflow `Instance`: archived means the instance is in completed/archived history or returned by an archived instance query. Completed-but-active is not archived unless the source says so.
- Ticket data: archived means the ticket is closed/archived only when the ticket store exposes that state as structured metadata. Closed alone is not enough unless the store defines closed as archived.
- Phase 3 delivery/land outcome: archived means the delivery outcome snapshot is in an archived outcome store or carries an archived marker.
- Phase 4 signal: archived means the CI/revert/signal snapshot is in archived history or carries an archived marker. Old signals are not archived by age alone.
- Gate/approval/decision events: archived means the decision ledger marks the event archived. Resolved approvals remain active unless archived explicitly.
- `include_archived=false`: exclude archived events from metric numerators and denominators, but still report archived availability/source counts.
- `include_archived=true`: include archived events in metrics and also expose active/archived splits.

### Numeric formulas

- Store cost as signed or unsigned integer `micro_usd` where `1 USD = 1_000_000 micro_usd`. Prefer unsigned for non-negative cost.
- If source reports decimal USD, convert with round-half-away-from-zero to nearest integer micro-USD at ingestion: `micro_usd = round_half_away_from_zero(decimal_usd * 1_000_000)`. Reject or mark unavailable on overflow, NaN, infinity, or negative values unless the source explicitly supports credits.
- If source reports tokens plus price, use integer arithmetic when possible: `micro_usd = round_half_away_from_zero(input_tokens * input_micro_usd_per_token + output_tokens * output_micro_usd_per_token + other_components)`. Pricing snapshot id must be recorded as evidence.
- `total_cost_micro_usd = sum(cost_micro_usd for available cost events)`.
- `average_cost_micro_usd = total_cost_micro_usd / cost_sample_size` using integer division rounded half away from zero. Also expose `cost_sample_size`.
- `lead_time_ms = completed_at_ms - started_at_ms`; invalid negative or missing timestamps make lead time unavailable.
- Percentiles use nearest-rank on sorted available values: for percentile `p` in `(0, 1]`, `rank = ceil(p * n)`, 1-based, value at `rank - 1`. Median is nearest-rank `p=0.50`; p95 is nearest-rank `p=0.95`. Expose `lead_time_sample_size`.
- Rates use explicit numerator and denominator fields, not pre-rounded floats. JSON may include rational `{numerator, denominator}` plus display decimal rounded to four places.
- `recurrence_count` is the number of events whose explicit recurrence/coalesce key appears at least twice in the same composite group. Also expose `distinct_recurrence_keys` and `recurrence_sample_size`.

### Grouping and comparable profiles

- Primary aggregation key is the composite `(task_class, workflow, harness, model)`.
- Optional projections may be produced for display by task class, workflow, harness, model, or `(task_class, workflow)`, but recommendations must retain the originating composite evidence.
- Recommendation peer comparison is allowed only within the same `task_class` and `workflow`, comparing different `harness` and/or `model` profiles.
- Suppress recommendation action text below each rule's sample size threshold.
- Never emit advice for metrics with `availability=false` or `sample_size=0`. Emit a warning row instead.

---

### Task 1: Normalize Durable Outcome Events and Facts

**Files:**
- Modify: `crates/rk-core/src/factory/mod.rs` or create the existing module-equivalent for factory analytics domain types.
- Create: `crates/rk-core/src/factory/outcome_events.rs`
- Create: `crates/rk-core/src/factory/outcome_facts.rs`
- Modify: `crates/rk-core/src/lib.rs`
- Add tests near existing core tests, for example: `crates/rk-core/tests/factory_outcome_facts.rs`

**Interfaces:**
- Produces: `FactoryOutcomeEvent`, `FactoryMetricPayload`, `OutcomeFact`, `OutcomeFactSource`, `OutcomeStatus`, `OutcomeEvidenceKind`, `OutcomeFactGroupKey`, `OutcomeFactBuilder`, `SourceAvailability`, `SourceCounts`.
- `OutcomeFactGroupKey` is the composite `(task_class, workflow, harness, model)` with explicit `unknown` dimension values when structured fields are missing.
- `OutcomeStatus` variants: `Accepted`, `Reworked`, `CiFailed`, `CiRecovered`, `Reverted`, `Unknown`, `Unobserved`.
- `OutcomeEvidenceKind` variants include at least `AgentRecord`, `WorkflowInstance`, `Phase3Contract`, `Phase3VerifiedDelivery`, `StructuredReviewerRework`, `Phase4CiSignal`, `StructuredRevert`, `HumanGateDecision`, `RecurrenceKey`, `PricingSnapshot`.
- `fact_id` and `event_id` are stable deterministic hashes over canonical structured fields, not current time or insertion order.

- [ ] **Step 1: Write failing normalization tests**

Implement tests for these behaviors:

- `normalizes_run_dimensions_from_agent_record_and_instance`: structured `AgentRecord` and workflow `Instance` produce run dimensions for `workflow`, `harness`, and `model` without reading logs.
- `task_class_requires_phase3_explicit_contract_ticket_or_outcome`: explicit Phase 3 task class is preserved, while prose-only fixtures produce `unknown` and a source warning.
- `normalizes_accepted_only_from_phase3_verified_delivery_or_land`: Phase 3 verified delivery/land event produces `OutcomeStatus::Accepted`; green CI alone does not.
- `normalizes_reworked_only_from_structured_reviewer_transition`: structured reviewer rework transition produces `OutcomeStatus::Reworked`; review-comment prose is ignored.
- `normalizes_ci_failed_and_recovered_from_phase4_signals`: explicit Phase 4 failed and recovered signals produce `CiFailed` and `CiRecovered` facts.
- `normalizes_revert_only_from_structured_revert_handler`: structured revert handler event produces `OutcomeStatus::Reverted`; commit-message text is ignored.
- `counts_human_intervention_only_from_gate_approval_decision_events`: explicit gate/approval/decision events increment intervention count; mentions or comments do not.
- `uses_only_explicit_recurrence_or_coalesce_key`: recurrence facts require explicit `recurrence_key` or coalesce key; text similarity fixtures are ignored.
- `missing_source_family_is_unobserved_with_availability_counts`: unavailable Phase 3/4 families are reported as `unobserved` with source counts.
- `fact_ids_are_deterministic_across_input_order`: same records in different order produce the same sorted fact IDs.
- `archived_source_marks_fact_archived_with_source_family`: archived records set `archived: true`, keep archive source family, and remain countable separately.

Use synthetic structured fixtures. Include decoy log strings, prose fields, commit messages, and comments that imply different outcomes and assert they are ignored.

- [ ] **Step 2: Run tests and verify RED**

```bash
cargo test -p rk-core --test factory_outcome_facts -- --nocapture
```

Expected: failure because outcome event/fact types and normalization do not exist.

- [ ] **Step 3: Implement minimal normalized facts**

Implement pure normalization functions from existing structured domain objects or thin fixture-compatible adapters. Keep the module independent from daemon state and clocks. Use deterministic canonical serialization for `event_id`/`fact_id` and stable sort outputs by `(repo, task_class, workflow, harness, model, fact_id)`.

Do not add log readers, regex parsing over text logs, shell commands, transcript storage access, or prose classifiers.

- [ ] **Step 4: Run tests and verify GREEN**

Run the `cargo test -p rk-core --test factory_outcome_facts -- --nocapture` command. Expected: all outcome fact tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/rk-core/src crates/rk-core/tests
git commit -m "feat: normalize factory outcome facts"
```

### Task 2: Aggregate Composite Scorecards

**Files:**
- Modify: `crates/rk-core/src/factory/outcome_facts.rs`
- Create: `crates/rk-core/src/factory/scorecards.rs`
- Modify: `crates/rk-core/src/factory/mod.rs`
- Add tests near existing core tests, for example: `crates/rk-core/tests/factory_scorecards.rs`

**Interfaces:**
- Produces: `FactoryScorecard`, `ScorecardGroupKey`, `ScorecardProjection`, `ScorecardMetrics`, `ScorecardEvidenceCounts`, `ScorecardSourceCounts`, `MetricAvailability`, `ScorecardQuery`.
- Primary grouping: composite `(task_class, workflow, harness, model)`.
- Optional projections: `task_class`, `workflow`, `harness`, `model`, `(task_class, workflow)`, and `all`, clearly marked as projections.
- Each scorecard row includes: `group_key`, `projection`, `runs`, `accepted`, `reworked`, `ci_failed`, `ci_recovered`, `reverted`, `unknown`, `unobserved`, `active_runs`, `archived_runs`, `total_cost_micro_usd`, `average_cost_micro_usd`, `cost_sample_size`, `median_lead_time_ms`, `p95_lead_time_ms`, `lead_time_sample_size`, `human_interventions`, `intervention_sample_size`, `recurrence_count`, `distinct_recurrence_keys`, `recurrence_sample_size`, `evidence_counts`, `source_counts`, `availability`, `sample_size`. `runs` counts only explicit structured run facts, not every terminal metric fact. Standalone `PricingSnapshot` facts contribute evidence/source metadata only; cost samples come from structured run/agent cost facts with pricing evidence.
- `evidence_counts` is keyed by structured evidence kind. `source_counts` is keyed by source family and includes active, archived, unavailable, and event counts.

- [ ] **Step 1: Write failing scorecard tests**

Implement tests for these behaviors:

- `groups_metrics_by_composite_task_class_workflow_harness_model`: the same fact set produces deterministic composite rows.
- `can_project_composite_rows_without_losing_source_counts`: optional projections are marked as projections and retain source availability metadata.
- `counts_runs_accepted_reworked_ci_failed_ci_recovered_reverted_unknown_and_unobserved`: status counts equal fixture facts exactly.
- `separates_active_and_archived_history_counts`: archived rows increment `archived_runs` and total `runs` only when included, while metadata always reports archived availability.
- `aggregates_micro_usd_with_integer_rounding`: cost totals and averages are deterministic across reversed input order and use round-half-away-from-zero conversion.
- `computes_lead_time_median_and_p95_nearest_rank`: percentile fixtures document exact nearest-rank outputs.
- `counts_human_interventions_from_explicit_events_only`: structured intervention counts sum; comments and prose are ignored.
- `counts_recurrence_only_from_repeated_explicit_keys`: repeated recurrence/coalesce keys increment recurrence count; singletons do not.
- `includes_evidence_and_source_counts_by_family`: counts reflect AgentRecord, Instance, Phase 3, Phase 4, reviewer transition, revert handler, gate decision, and pricing snapshot sources.
- `sorts_scorecard_rows_by_composite_key_projection_and_metric`: output row order is stable.

- [ ] **Step 2: Run tests and verify RED**

```bash
cargo test -p rk-core factory_scorecards -- --nocapture
```

Expected: failure because scorecard aggregation does not exist.

- [ ] **Step 3: Implement scorecard aggregation**

Implement aggregation as pure functions over `OutcomeFact` slices. Use integer micro-USD for cost. Round only at ingestion boundaries with the documented rule. Keep separate denominator/sample-size fields for cost, lead time, interventions, recurrence, and each status family.

Recurrence count must count events whose repeated non-empty `recurrence_key` or `coalesce_key` appears within the same composite group. A single occurrence is not a recurrence.

- [ ] **Step 4: Run tests and verify GREEN**

Run the `cargo test -p rk-core factory_scorecards -- --nocapture` command. Expected: all scorecard tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/rk-core/src crates/rk-core/tests
git commit -m "feat: aggregate factory scorecards"
```

### Task 3: Deterministic Advisory Recommendation Rules

**Files:**
- Create: `crates/rk-core/src/factory/recommendations.rs`
- Modify: `crates/rk-core/src/factory/mod.rs`
- Add tests near existing core tests, for example: `crates/rk-core/tests/factory_recommendations.rs`

**Interfaces:**
- Produces: `FactoryRecommendation`, `RecommendationRule`, `RecommendationSeverity`, `RecommendationSuppression`, `ComparisonEvidence`, `RecommendationQuery`.
- Recommendation fields: `id`, `severity`, `rule`, `subject_group_key`, `summary`, `advice`, `thresholds`, `metric_availability`, `comparison_evidence`, `evidence_counts`, `source_counts`, `sample_size`, `suppressed`, `suppression_reason`.
- Initial rules:
  - `low_acceptance_rate`: suppress below 10 accepted-availability runs. Compare only profiles with same `task_class` and `workflow`. Recommend review when accepted / availability denominator is below 0.60 and at least one comparable peer is at or above 0.80 with at least 10 accepted-availability runs.
  - `high_rework_rate`: suppress below 10 rework-availability runs. Recommend investigation when reworked / denominator is at or above 0.25.
  - `ci_failure_regression`: suppress below 8 CI-availability runs. Recommend CI-focused review when CI failed / denominator is at or above 0.15 and above the comparable same-task-class/workflow median.
  - `revert_risk`: suppress below 5 revert-availability runs. Recommend stricter review when reverted / denominator is at or above 0.10.
  - `cost_outlier`: suppress below 8 cost-availability runs. Recommend cost review when average cost is at least 1.5x the comparable same-task-class/workflow median and absolute average cost is at least the configured minimum.
  - `lead_time_outlier`: suppress below 8 lead-time-availability runs. Recommend latency review when p95 lead time is at least 1.5x the comparable same-task-class/workflow median p95.
  - `human_intervention_hotspot`: suppress below 8 intervention-availability runs. Recommend workflow ergonomics review when interventions per available run are at or above 0.30.
  - `recurrence_hotspot`: suppress below 5 recurrence-availability runs. Recommend root-cause analysis when recurrence count is at or above 3.

- [ ] **Step 1: Write failing recommendation tests**

Implement tests for these behaviors:

- `suppresses_low_sample_recommendations_but_keeps_metrics`: below-threshold scorecards produce suppressed rows with no action advice and exact suppression reason.
- `does_not_emit_advice_for_unavailable_metrics`: unavailable metrics produce warning/suppression metadata and no advice.
- `emits_low_acceptance_with_same_task_class_workflow_peer_comparison`: low acceptance emits comparison evidence naming the better comparable peer and both sample sizes.
- `does_not_compare_across_task_class_or_workflow`: attractive peers in other task classes/workflows are ignored.
- `does_not_emit_low_acceptance_without_comparable_peer`: no peer evidence means no recommendation, not a guess.
- `emits_high_rework_ci_revert_cost_lead_time_intervention_and_recurrence_rules`: fixture scorecards trigger one deterministic recommendation per rule.
- `recommendation_ids_are_deterministic`: reversed input produces identical IDs and ordering.
- `recommendations_are_advisory_read_only`: recommendation payload contains no command, patch, policy mutation, workflow mutation, ticket mutation, approval mutation, or dispatch instruction fields.
- `thresholds_and_denominators_are_serialized_with_each_recommendation`: every recommendation records the thresholds and sample denominator used.

- [ ] **Step 2: Run tests and verify RED**

```bash
cargo test -p rk-core factory_recommendations -- --nocapture
```

Expected: failure because recommendation rules do not exist.

- [ ] **Step 3: Implement deterministic advisory rules**

Implement pure rule evaluation over composite scorecards. Use explicit threshold constants in one module. Sort by `(severity, rule, task_class, workflow, harness, model, id)` with a documented severity order.

Suppressed recommendations must include metrics, thresholds, source counts, evidence counts, and `suppressed: true`, but `advice` must be empty or `None`. Non-suppressed recommendations may use advisory wording such as "review", "investigate", or "compare", but must not say "change routing", "rewrite policy", "dispatch", "approve", "land", or "update config".

- [ ] **Step 4: Run tests and verify GREEN**

Run the `cargo test -p rk-core factory_recommendations -- --nocapture` command. Expected: all recommendation tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/rk-core/src crates/rk-core/tests
git commit -m "feat: recommend factory self-optimization hints"
```

### Task 4: Read-Only Daemon RPCs

**Files:**
- Modify actual daemon RPC protocol definitions after inspecting the repository, expected current file: `crates/rk-daemon/src/proto.rs`.
- Modify actual daemon service handlers after inspecting the repository, expected current file: `crates/rk-daemon/src/server.rs`.
- Modify storage/query modules that already expose immutable snapshots or narrow read APIs for `AgentRecord`, workflow `Instance`, tickets/contracts/outcomes, Phase 3 evidence, Phase 4 signals, reviewer transitions, revert events, and gate decisions.
- Add integration tests near existing daemon RPC tests.

**Interfaces:**
- Adds read-only RPCs:
  - `factory.scorecards` with request `{repo, group_by?, include_archived?, since?, until?, min_sample?}`.
  - `factory.recommend` with request `{repo, group_by?, include_archived?, since?, until?, min_sample?}`.
- Response envelopes include `schema_version`, `repo`, `generated_at`, `source_counts`, `availability`, `scorecards`, `recommendations`, and `warnings` as applicable.
- RPC handlers must use immutable snapshots or narrow read traits only. Do not pass broad mutation-capable repositories when a read trait can be introduced.
- RPCs must not call mutation repositories, workflow enqueue APIs, policy writers, config writers, ticket writers, approval writers, queue writers, dispatch APIs, or agent-record mutators.

- [ ] **Step 1: Write failing daemon RPC tests**

Implement tests for these behaviors:

- `factory_scorecards_rpc_returns_scorecards_from_structured_sources`: seeded structured records produce composite scorecard rows.
- `factory_recommend_rpc_returns_advisory_recommendations`: seeded scorecards that cross thresholds produce advisory recommendations.
- `factory_rpcs_include_archived_history_counts`: include archived facts and separate active/archived counts when requested.
- `factory_rpcs_exclude_archived_when_requested`: `include_archived:false` excludes archived facts from metrics while reporting archived source availability in metadata.
- `factory_rpcs_report_missing_source_families_as_unobserved`: unavailable Phase 3/4 or gate sources appear in `availability` and `warnings`, not as zero failures.
- `factory_rpcs_are_read_only_against_all_known_mutating_rpcs`: mutation-spy repositories assert no known mutating RPC path is called, including workflow enqueue/run/cancel, ticket create/update/close/archive, policy/config write, approval/gate decision write, dispatch, queue mutation, agent record mutation, and revert mutation.
- `factory_rpcs_do_not_parse_logs_or_prose`: seeded log/prose-only evidence produces no outcome fact unless structured fields also exist.
- `factory_rpcs_are_deterministic_across_repeated_calls`: same seeded state produces byte-equivalent JSON aside from injected `generated_at` when the test clock is fixed.

- [ ] **Step 2: Run tests and verify RED**

Run the daemon test command used by the repository, for example:

```bash
cargo test -p rk-daemon factory_ -- --nocapture
```

If the repository uses a different daemon package name or test filter, adjust only the package/filter, not the behavior.

- [ ] **Step 3: Implement read-only handlers**

Wire the RPC handlers in `server.rs` to structured source read APIs and pure normalization, aggregation, and recommendation evaluation. Add or update request/response types in `proto.rs`. Inject the daemon clock for `generated_at`. Add warning entries for missing source families, unavailable Phase 3/Phase 4 signals, filtered archived history, unavailable task class, and metric families with zero availability.

Keep all handler dependencies read-only. If existing repository interfaces do not separate read and write traits, add narrow read traits for this feature rather than passing broad mutation-capable services into the aggregator.

- [ ] **Step 4: Run tests and verify GREEN**

Run the daemon RPC test command. Expected: all factory RPC tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/rk-daemon crates/rk-core
git commit -m "feat: expose factory scorecard RPCs"
```

### Task 5: CLI Commands for Scorecards and Recommendations

**Files:**
- Modify CLI command definitions, for example: `crates/rk-cli/src/main.rs`, `crates/rk-cli/src/commands/factory.rs`, or the existing CLI module layout.
- Add CLI tests near existing CLI snapshot or command tests.
- Update shell completion snapshots if this repository keeps generated completions under version control.

**Interfaces:**
- Adds read-only commands using global JSON mode:
  - `rk --json factory scorecards --repo <repo> [--group-by task_class|workflow|harness|model|task_class_workflow|composite|all] [--include-archived]`
  - `rk --json factory recommend --repo <repo> [--group-by task_class|workflow|harness|model|task_class_workflow|composite|all] [--include-archived]`
- If a local `--format markdown` already exists in CLI style, Markdown may be supported as a separate explicit renderer, but JSON must be through global `--json`.
- JSON output is a stable serialization of the daemon RPC response.
- Markdown output, if supported, includes sections: `# Factory Scorecards`, `## Source Counts`, `## Scorecards`, `## Recommendations`, `## Suppressed`, and `## Warnings` when applicable.
- Commands are read-only and connect to the daemon. They must not dispatch workflows, mutate tickets, write approvals, or update policy/config.

- [ ] **Step 1: Write failing CLI tests**

Implement tests for these behaviors:

- `factory_scorecards_cli_uses_global_json_and_read_only_rpc`: command calls `factory.scorecards` with exact request fields.
- `factory_recommend_cli_uses_global_json_and_read_only_rpc`: command calls `factory.recommend` with exact request fields.
- `factory_scorecards_json_is_stable`: fixture response serializes deterministically.
- `factory_recommend_markdown_contains_recommendations_and_suppressed_sections`: Markdown includes active recommendations and low-sample suppressions if Markdown is supported.
- `factory_commands_reject_mutating_flags`: unknown flags such as `--apply`, `--dispatch`, `--rewrite-policy`, `--update-workflow`, `--approve`, and `--ticket-update` fail validation.
- `factory_commands_preserve_static_routing`: no routing config writer, workflow dispatcher, approval writer, or ticket mutation mock is invoked.
- `factory_commands_show_archived_history_and_unobserved_counts`: Markdown and JSON include active, archived, unavailable, and event counts.

- [ ] **Step 2: Run tests and verify RED**

```bash
cargo test -p rk-cli factory_ -- --nocapture
```

Expected: failure because CLI commands are absent.

- [ ] **Step 3: Implement CLI and renderers**

Add the `factory` command group or extend it if it already exists. Keep argument parsing explicit. Pass requests to daemon RPCs without local mutation side effects. Render JSON from the response only when global `--json` is set, following existing CLI conventions.

For `--group-by all`, either omit the request grouping or send the documented all-grouping value consistently with daemon tests. Sort Markdown tables the same way as JSON rows.

- [ ] **Step 4: Run tests and verify GREEN**

Run the CLI test command. Expected: all factory CLI tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/rk-cli
git commit -m "feat: add factory scorecard CLI"
```

### Task 6: Optional Factory Foreman Display

**Files:**
- Modify: `.jcode/skills/factory-foreman/scripts/factory_foreman.py`
- Modify: `.jcode/skills/factory-foreman/tests/test_factory_foreman.py`
- Modify: `.jcode/skills/factory-foreman/REFERENCE.md`
- Modify: `.jcode/skills/factory-foreman/SKILL.md` only if necessary and keep it under 500 lines.

**Interfaces:**
- Adds display-only use of:
  - `rk --json factory scorecards --repo <repo> --group-by all --include-archived`
  - `rk --json factory recommend --repo <repo> --group-by all --include-archived`
- Before calling, run strict daemon preflight using existing read-only health/status checks only. Do not start a daemon, dispatch work, approve proposals, or mutate tickets during preflight.
- Factory Foreman report may show scorecard summaries, source availability, suppressed rows, and advisory recommendations.
- No Factory Foreman command may execute `factory recommend --apply`, workflow dispatch, policy rewrite, config rewrite, ticket mutation, approval bypass, or gate decision mutation.

- [ ] **Step 1: Write failing display tests**

Implement tests for these behaviors:

- `factory_foreman_preflights_daemon_with_read_only_call`: fake runner sees only the approved daemon status/read command before scorecard calls.
- `factory_foreman_collects_scorecards_with_read_only_command`: fake runner sees the exact `rk --json factory scorecards` argv.
- `factory_foreman_collects_recommendations_with_read_only_command`: fake runner sees the exact `rk --json factory recommend` argv.
- `factory_foreman_markdown_labels_recommendations_advisory`: rendered report uses advisory language and separates suppressed low-sample rows.
- `factory_foreman_renders_unavailable_metrics_as_unobserved`: unavailable source families appear as unavailable/unobserved, not as zero failures.
- `factory_foreman_does_not_render_dispatch_apply_or_approval_controls`: report contains no apply, dispatch, rewrite-policy, update-workflow, ticket mutation, approval, or gate decision command.
- `factory_foreman_preserves_phase1_approval_boundary`: existing proposal approval tests still pass unchanged.

- [ ] **Step 2: Run tests and verify RED**

```bash
python3 -m unittest discover -s .jcode/skills/factory-foreman/tests -v
```

Expected: new display tests fail because Factory Foreman does not collect Phase 5 scorecards yet.

- [ ] **Step 3: Implement display-only integration**

Extend the existing read-only snapshot or triage report to include optional scorecard and recommendation observations. Treat failures as degraded observations. Do not change existing static workflow proposal behavior.

If the `rk factory` commands are unavailable or daemon preflight fails, render a warning that Phase 5 scorecards are unavailable rather than failing the whole Phase 1 triage report.

- [ ] **Step 4: Run tests and verify GREEN**

Run the Python unittest command. Expected: all Factory Foreman tests pass.

- [ ] **Step 5: Commit**

```bash
git add .jcode/skills/factory-foreman
git commit -m "feat: display factory scorecard advice"
```

### Task 7: Documentation, Acceptance, and Safety Review

**Files:**
- Modify: `docs/factory-foreman.md`
- Modify: `README.md` only if existing Factory Foreman documentation is already linked there.
- Create or modify architecture docs only if this repository already has a factory analytics documentation location.

**Interfaces:**
- Documents scorecard schema, durable outcome event/ledger semantics, source-to-metric matrix, recommendation rules, thresholds, low-sample suppression, comparable-profile constraints, archived-history counts, read-only RPCs, CLI commands, and safety boundaries.
- Documents that existing static routing remains unchanged and recommendations cannot rewrite policy/config/workflows/tickets/approvals or dispatch.

- [ ] **Step 1: Add documentation tests if the repository uses doc contract tests**

If documentation contract tests exist, add assertions that docs mention:

- structured sources only;
- durable outcome ledger/events;
- source-to-metric matrix;
- never logs, prose, transcript text, terminal output, or inferred task class;
- archived history counts and archive semantics;
- missing families as unobserved with availability/source counts;
- low-sample suppression;
- comparable profiles constrained to same task class/workflow;
- advisory-only recommendations;
- static routing unchanged;
- no policy/config/workflow/ticket/approval mutation;
- no dispatch.

- [ ] **Step 2: Run focused verification**

```bash
cargo test -p rk-core factory_ -- --nocapture
cargo test -p rk-daemon factory_ -- --nocapture
cargo test -p rk-cli factory_ -- --nocapture
python3 -m unittest discover -s .jcode/skills/factory-foreman/tests -v
```

Expected: all focused tests pass.

- [ ] **Step 3: Run full repository verification**

```bash
MISE_TRUSTED_CONFIG_PATHS="$PWD" mise run verify
```

Expected: repository verification passes. If this command is too broad or environment-blocked, record the exact failure and run the closest documented non-mutating checks.

- [ ] **Step 4: Run live read-only acceptance**

With a daemon already running, save outputs under `$JCODE_SCRATCH_DIR`, not the repository:

```bash
rk --json factory scorecards --repo rat-kingdom --group-by all --include-archived > "$JCODE_SCRATCH_DIR/factory-scorecards.json"
rk --json factory recommend --repo rat-kingdom --group-by all --include-archived > "$JCODE_SCRATCH_DIR/factory-recommend.json"
```

Validate with standard tools or Rust/CLI snapshot assertions:

```bash
python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); assert d["repo"] == "rat-kingdom" and "scorecards" in d and "source_counts" in d and "availability" in d' "$JCODE_SCRATCH_DIR/factory-scorecards.json"
python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); assert d["repo"] == "rat-kingdom" and "recommendations" in d and "scorecards" in d and "source_counts" in d' "$JCODE_SCRATCH_DIR/factory-recommend.json"
```

Expected: valid read-only JSON. If no daemon is running, commands may fail to connect but must not start a daemon or mutate state.

- [ ] **Step 5: Review mutation safety manually**

Run:

```bash
git grep -n "factory\.recommend\|factory\.scorecards\|FactoryRecommendation\|FactoryOutcomeEvent\|OutcomeFact" -- crates .jcode docs
git grep -n "rewrite-policy\|update-workflow\|dispatch\|workflow run\|workflow enqueue\|ticket .*mut\|ticket .*update\|policy.*write\|config.*write\|approval.*write\|gate.*decision\|queue.*push\|agent.*mut" -- crates .jcode docs
```

Confirm any matches are documentation warnings, tests asserting absence, read-only request wiring, or pre-existing unrelated code. Factory scorecard and recommendation paths must not call mutating APIs.

- [ ] **Step 6: Run no-mutation audits against all known mutating RPCs**

Inventory current mutating RPCs from `crates/rk-daemon/src/proto.rs` and handlers in `crates/rk-daemon/src/server.rs`. Add/update an allowlist test that fails if `factory.scorecards` or `factory.recommend` calls or depends on any mutating handler category, including:

- workflow create/update/enqueue/run/cancel/archive;
- ticket create/update/close/archive/reopen;
- policy/config write or reload with write side effects;
- approval/gate decision write or bypass;
- dispatch/queue mutation;
- agent record create/update/archive;
- revert/land/delivery mutation;
- any shell command execution path.

The audit must use code-level spies, narrow trait bounds, or static handler dependency checks. Grep is a supplemental review only, not the sole proof.

- [ ] **Step 7: Review the diff**

Check:

```bash
git diff --check
git status --short
git diff --stat origin/main..HEAD
```

An independent reviewer must verify structured-source-only normalization, durable outcome event semantics, exact metric formulas, deterministic aggregation, comparable-profile threshold behavior, low-sample suppression, archived-history counts, missing-family unobserved handling, read-only daemon RPCs, global `--json` CLI behavior, Factory Foreman display-only behavior, no-mutation audits, and unchanged static routing.

- [ ] **Step 8: Commit documentation and final integration**

```bash
git add docs/factory-foreman.md README.md crates .jcode/skills/factory-foreman
git commit -m "docs: document factory self-optimization"
```

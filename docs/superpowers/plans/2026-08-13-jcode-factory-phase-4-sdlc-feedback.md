# Jcode Factory Foreman Phase 4 SDLC Feedback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Implement task-by-task in order. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add vendor-neutral SDLC feedback ingestion to Rat Kingdom so local CI, deployment, and production-alert signals become durable canonical tuples that the daemon can reason about safely, while preserving Phase 2 proposals as the only path for any future mutation.

**Architecture:** Add canonical SDLC signal and source-auth types to `rk-core`, persist accepted signals and projected tuples in one SQLite transaction in `rk-space`, then wire machine-local daemon ingest through `rk-daemon` and CLI commands through `rk-cli`. Accepted input is constrained to configured local source names and derived source tokens. Duplicate occurrences are recognized by `(source, delivery_id)`. State transitions are deduplicated by current row for `(source, scope, subject)` plus semantic state digest. CI and alert reactions use existing reactor tuples/triggers. Production diagnosis is read-only and uses only structured sanitized references.

**Actual implementation files only:**

- `crates/rk-core/src/config.rs`
- `crates/rk-core/src/lib.rs`
- `crates/rk-core/src/sdlc.rs`
- `crates/rk-space/src/lib.rs`
- `crates/rk-space/src/store.rs`
- `crates/rk-daemon/src/ingest.rs`
- `crates/rk-daemon/src/server.rs`
- `crates/rk-daemon/src/lib.rs`
- `crates/rk-daemon/src/reactor.rs`
- `crates/rk-cli/src/ingest_cmds.rs`
- `crates/rk-cli/src/main.rs`

Do not reference or create nonexistent `state`, `tuples`, `rpc`, `daemon`, `reactions`, `workflows`, or `commands` files.

## Global Constraints

- Edit only files named by the task being implemented.
- Keep tasks sequential: model, config/auth, storage, ingest RPC/CLI, CI, deployment, alert diagnosis.
- Preserve unrelated working tree changes, including `mise.toml` and other plan files.
- Keep SDLC ingestion vendor-neutral. Do not add GitHub, GitLab, CircleCI, Buildkite, Datadog, PagerDuty, Kubernetes, cloud, or observability SDK clients in v1.
- Do not expose public HTTP ingestion. Ingestion is daemon-local through the existing daemon/client boundary only.
- Source authentication is machine-local only:
  - configured source names are local operator handles, not user principals;
  - preferred v1 mode is `source:<name>` principals proven by derived source tokens;
  - if source tokens cannot be completed in the same slice, fallback v1 is operator-only local ingest with no non-operator source impersonation;
  - the server allowlist applies to ingest methods only;
  - reject inline principals and every authentication method except the configured local source-token or explicit operator-only fallback.
- CLI follows the existing global `--json` convention. Do not add per-subcommand JSON semantics that conflict with `rk --json ...`.
- Do not persist raw telemetry blobs, logs, stack traces, secrets, environment dumps, HTTP headers, executable snippets, action fields, or production credentials.
- Production diagnosis accepts only structured sanitized references. It forbids executable, action, command, credential, token, password, authorization, cookie, shell, deploy, rollback, restart, scale, SSH, kubectl, and Terraform fields.
- Production diagnosis never mutates production.
- CI and alert reactions use existing reactor tuples/triggers and may only create diagnostic/proposal tuples. Any mutation must go through the Phase 2 proposal path and approval boundary.
- Every accepted ingest write is transactional in `rk-space`: receipt, state, and projected tuples are committed together or rolled back together.
- Every accepted ingest write returns a durable receipt that can be queried after daemon restart.
- Follow test-driven development. Each behavior gets a failing test before implementation.
- Commit each independently testable task when implementing this plan, but do not commit while only writing this plan.

---

## Shared Domain Contract

### Source and occurrence identity

- Configured source name: machine-local string from config, for example `local-ci`, `deploy-agent`, or `alerts`.
- Source principal: `source:<name>` after token verification, or operator principal only in the explicit fallback mode.
- Delivery ID: caller-provided stable occurrence ID within a source. Required for all ingested events.
- Occurrence dedupe key: `(source, delivery_id)`. Replaying the same delivery returns the original receipt and does not create another occurrence.
- Occurrence dedupe is not semantic-content dedupe.

### State transition identity

- State row key: `(source, scope, subject)`.
- `scope` values:
  - CI: `ci`
  - deployment: `deployment`
  - production alert: `production_alert`
- `subject` values:
  - CI: `repo=<repo>|branch=<branch>|workflow=<workflow>|job=<job>|commit=<commit_sha>`
  - deployment: `environment=<environment>|service=<service>` with optional `repo=<repo>` stored as metadata, not part of current deployment identity unless implementation already requires it consistently.
  - alert: `environment=<environment>|service=<service>|alert_key=<alert_key>`.
- Semantic state digest: canonical digest over the state-bearing fields after validation and sanitization.
- State transition dedupe: compare the current row for `(source, scope, subject)` with the new semantic state digest. Repeated same state records receipt/last_seen but emits no transition.

### Projected tuple identities

- Current `Event` identity: `(source, delivery_id)`.
- Current `Fact` identity:
  - CI status fact: `(source, "ci", subject)`.
  - Deployment provenance fact: `(source, "deployment", environment, service)`.
  - Production alert fact: `(source, "production_alert", subject)`.
- Deployment current identity is exactly `(environment, service)` within a source. A newer deployment for the same source/environment/service replaces the current deployment provenance fact.

---

## Task 1: Canonical SDLC Signal Model in `rk-core`

**Files:**

- Modify: `crates/rk-core/src/lib.rs`
- Modify: `crates/rk-core/src/config.rs`
- Create: `crates/rk-core/src/sdlc.rs`
- Create or modify tests under the existing `rk-core` test layout.

**Interfaces:**

- Produces module: `rk_core::sdlc`.
- Produces: `SignalEnvelope`, `SignalKind`, `Correlation`, `SignalLimits`, `SignalReceipt`, `ConfiguredSourceName`, `SignalSourcePrincipal`, `SourceToken`, `SemanticStateDigest`, `OccurrenceId`, and typed payload structs for CI, deployment, and production alert signals.
- Stable signal kinds: `ci_failed`, `ci_recovered`, `deployment_succeeded`, `production_alert_firing`, `production_alert_resolved`.
- Required envelope fields: `kind`, `source`, `delivery_id`, `occurred_at`, `observed_at`, `correlation`, `summary`, `refs`, `attributes`.
- `delivery_id` is the occurrence identity component for every new API.
- Required correlation fields are optional in the generic type but validated per kind.
- Receipt includes: `receipt_id`, `source`, `delivery_id`, `accepted_at`, `semantic_state_digest`, `projected_event_id`, `projected_fact_ids`, and `transition_emitted`.

- [ ] **Step 1: Write failing model contract tests**

Implement these exact tests:

- `test_signal_envelope_round_trips_known_kinds`.
- `test_correlation_rejects_empty_identity_for_transition_signals`.
- `test_signal_limits_reject_raw_telemetry_shape`.
- `test_source_principal_is_configured_source_not_inline_text`.
- `test_source_token_derives_source_principal_name`.
- `test_occurrence_identity_is_source_and_delivery_id`.
- `test_semantic_state_digest_ignores_attribute_order`.
- `test_semantic_state_digest_changes_when_state_identity_changes`.
- `test_signal_receipt_contains_digest_principal_delivery_and_tuple_ids`.

- [ ] **Step 2: Run tests and verify RED**

```bash
cargo test -p rk-core --test sdlc -- --nocapture
```

Expected: failure because `rk_core::sdlc` does not exist or is incomplete.

- [ ] **Step 3: Implement minimal canonical model**

Implement serde-compatible structs and validation methods. Keep the model independent of daemon transport and vendor-specific APIs.

Canonicalization requirements:

- sort attributes and refs before hashing;
- exclude transport bytes and receipt metadata from semantic state digest;
- include stable state identity fields needed by each signal kind;
- treat absent optional fields distinctly from empty strings;
- reject empty strings after trimming for identity-bearing fields;
- reject secret-like refs and attributes case-insensitively.

- [ ] **Step 4: Run tests and verify GREEN**

```bash
cargo test -p rk-core --test sdlc -- --nocapture
```

- [ ] **Step 5: Commit**

```bash
git add crates/rk-core/src/lib.rs crates/rk-core/src/config.rs crates/rk-core/src/sdlc.rs crates/rk-core/tests
git commit -m "feat: add canonical SDLC signal model"
```

## Task 2: Configured Source Auth and Ingest Allowlist

**Files:**

- Modify: `crates/rk-core/src/config.rs`
- Modify: `crates/rk-core/src/sdlc.rs`
- Modify: `crates/rk-daemon/src/server.rs`
- Modify: `crates/rk-daemon/src/lib.rs`
- Create or modify: `crates/rk-daemon/src/ingest.rs`
- Create or modify tests under the existing `rk-daemon` test layout.

**Interfaces:**

- Produces config section for SDLC ingest sources in the existing config model.
- Source fields: `name`, `enabled`, `allowed_kinds`, `token_derivation`, `max_summary_len`, `max_refs`, `max_attributes`.
- Produces resolver: configured source name plus verified local source token returns `SignalSourcePrincipal::Source("source:<name>")`.
- If real source-token support is not implementable in this task, implement operator-only v1 explicitly and document it in code comments and tests. Do not silently accept caller-supplied principal strings.

- [ ] **Step 1: Write failing config/auth tests**

Implement these exact tests:

- `test_default_config_has_no_public_ingest_listener`.
- `test_ingest_source_must_be_enabled_to_resolve`.
- `test_requested_source_name_resolves_to_configured_source_principal`.
- `test_source_token_required_for_non_operator_source_mode`.
- `test_inline_principal_is_rejected`.
- `test_server_allowlist_accepts_only_ingest_methods_for_source_tokens`.
- `test_allowed_kinds_are_enforced_by_source`.
- `test_source_limits_cannot_exceed_daemon_maximums`.

- [ ] **Step 2: Run tests and verify RED**

```bash
cargo test -p rk-daemon sdlc_ingest_auth -- --nocapture
```

- [ ] **Step 3: Implement source config and auth resolution**

Rules:

- no default public sources;
- no dynamic source creation in ingest requests;
- source names are local operator handles;
- source tokens derive only `source:<name>` principals;
- inline principal strings are always rejected;
- source-token principals can call only `ingest.event` and read-only ingest state methods;
- requested signal kind must be in `allowed_kinds`;
- source limits are clamped or rejected when looser than daemon maximums.

- [ ] **Step 4: Run tests and verify GREEN**

```bash
cargo test -p rk-daemon sdlc_ingest_auth -- --nocapture
```

- [ ] **Step 5: Commit**

```bash
git add crates/rk-core/src/config.rs crates/rk-core/src/sdlc.rs crates/rk-daemon/src/server.rs crates/rk-daemon/src/lib.rs crates/rk-daemon/src/ingest.rs crates/rk-daemon/tests
git commit -m "feat: authorize local SDLC ingest sources"
```

## Task 3: Transactional Storage in `rk-space`

**Files:**

- Modify: `crates/rk-space/src/lib.rs`
- Modify: `crates/rk-space/src/store.rs`
- Modify: `crates/rk-core/src/sdlc.rs`
- Create or modify tests under the existing `rk-space` test layout.

**Interfaces:**

- Produces storage operations in `rk-space`:
  - `accept_sdlc_signal(envelope, principal) -> SignalReceipt`.
  - `get_sdlc_receipt(source, delivery_id) -> Option<SignalReceipt>`.
  - `current_sdlc_facts(selector) -> Vec<Fact>` or existing tuple equivalent.
- Uses one SQLite transaction for occurrence receipt, current-state row, transition record, and projected tuples.
- Does not introduce daemon-local shadow state outside `rk-space`.

- [ ] **Step 1: Write failing storage tests**

Implement these exact tests:

- `test_accept_signal_persists_receipt_after_store_reopen`.
- `test_duplicate_occurrence_source_delivery_id_returns_existing_receipt`.
- `test_same_semantic_state_new_delivery_updates_last_seen_without_transition`.
- `test_transaction_rolls_back_when_tuple_projection_fails`.
- `test_current_state_snapshot_tracks_latest_ci_transition`.
- `test_current_state_snapshot_tracks_latest_alert_transition`.
- `test_deployment_provenance_current_fact_is_replaced_by_newer_deployment`.
- `test_receipt_lists_projected_event_and_fact_tuple_ids`.

- [ ] **Step 2: Run tests and verify RED**

```bash
cargo test -p rk-space sdlc -- --nocapture
```

- [ ] **Step 3: Implement transactional ingest storage**

Projection requirements:

- accepted occurrence creates or reuses one `Event` tuple keyed by `(source, delivery_id)`;
- current CI and alert status are `Fact` projections keyed by `(source, scope, subject)`;
- deployment provenance is the current `Fact` for `(source, environment, service)`;
- repeated `(source, delivery_id)` returns existing receipt;
- repeated same state digest for the same `(source, scope, subject)` records receipt/last_seen but emits no transition;
- a changed state digest emits one transition record for reactor processing.

- [ ] **Step 4: Run tests and verify GREEN**

```bash
cargo test -p rk-space sdlc -- --nocapture
```

- [ ] **Step 5: Commit**

```bash
git add crates/rk-space/src/lib.rs crates/rk-space/src/store.rs crates/rk-core/src/sdlc.rs crates/rk-space/tests
git commit -m "feat: persist SDLC signals transactionally"
```

## Task 4: Daemon Ingest and CLI Commands

**Files:**

- Modify: `crates/rk-daemon/src/ingest.rs`
- Modify: `crates/rk-daemon/src/server.rs`
- Modify: `crates/rk-daemon/src/lib.rs`
- Modify: `crates/rk-cli/src/ingest_cmds.rs`
- Modify: `crates/rk-cli/src/main.rs`
- Create or modify tests under existing `rk-daemon` and `rk-cli` test layouts.

**Interfaces:**

- Daemon ingest methods:
  - `ingest.event`: accepts one canonical `SignalEnvelope` and configured source name/token.
  - `ingest.state`: returns current SDLC facts filtered by repo, service, environment, or alert key.
- CLI commands use global JSON convention:
  - `rk --json ingest event --source SOURCE --kind KIND --delivery-id ID --summary TEXT ...`
  - `rk --json ingest event --source SOURCE --file PATH`
  - `rk --json ingest state [--repo REPO] [--environment ENV] [--service SERVICE] [--alert-key KEY]`
- CLI constructs canonical envelopes only. It never accepts credentials, raw telemetry files, vendor webhook JSON, executable fields, or action fields.

- [ ] **Step 1: Write failing daemon and CLI tests**

Implement these exact tests:

- `test_ingest_event_requires_local_authenticated_client`.
- `test_ingest_event_rejects_unknown_source`.
- `test_ingest_event_validates_before_persisting`.
- `test_ingest_event_returns_receipt_with_semantic_state_digest`.
- `test_ingest_event_projects_event_tuple`.
- `test_ingest_event_projects_current_fact_tuple`.
- `test_ingest_state_returns_current_facts_without_raw_payload`.
- `test_ingest_event_cli_builds_canonical_ci_failed_envelope`.
- `test_ingest_event_cli_rejects_raw_telemetry_file_flag`.
- `test_ingest_event_cli_rejects_secret_like_attr_keys`.
- `test_ingest_event_cli_file_accepts_canonical_envelope_only`.
- `test_ingest_state_cli_calls_daemon_read_only_handler`.
- `test_ingest_event_cli_prints_receipt_with_global_json`.

- [ ] **Step 2: Run tests and verify RED**

```bash
cargo test -p rk-daemon sdlc_ingest -- --nocapture
cargo test -p rk-cli sdlc_ingest -- --nocapture
```

- [ ] **Step 3: Implement daemon and CLI ingest path**

Validation order:

1. local client authentication;
2. source-token or operator-only authorization;
3. configured source name resolution;
4. allowed-kind check;
5. envelope validation under source limits;
6. semantic state digest computation;
7. transactional persistence in `rk-space`;
8. receipt response.

- [ ] **Step 4: Run tests and verify GREEN**

```bash
cargo test -p rk-daemon sdlc_ingest -- --nocapture
cargo test -p rk-cli sdlc_ingest -- --nocapture
```

- [ ] **Step 5: Commit**

```bash
git add crates/rk-daemon/src/ingest.rs crates/rk-daemon/src/server.rs crates/rk-daemon/src/lib.rs crates/rk-cli/src/ingest_cmds.rs crates/rk-cli/src/main.rs crates/rk-daemon/tests crates/rk-cli/tests
git commit -m "feat: add local SDLC ingest commands"
```

## Task 5: CI Feedback Reactions Through Existing Reactor

**Files:**

- Modify: `crates/rk-daemon/src/reactor.rs`
- Modify: `crates/rk-daemon/src/ingest.rs`
- Create or modify tests under the existing `rk-daemon` test layout.

**Interfaces:**

- Uses existing reactor tuples/triggers.
- Produces one diagnostic/proposal tuple when CI current state transitions from unknown/passing/recovered to failed.
- Produces one recovery acknowledgement fact or tuple when CI current state transitions from failed to recovered.
- Does not directly mutate repositories, CI systems, or production.
- Any fix/rerun/mutation is represented only as a Phase 2 proposal requiring approval.

- [ ] **Step 1: Write failing CI reaction tests**

Implement these exact tests:

- `test_ci_failed_transition_enqueues_one_diagnostic_reactor_tuple`.
- `test_duplicate_ci_occurrence_does_not_enqueue_second_reaction`.
- `test_ci_failed_to_failed_new_delivery_same_state_does_not_enqueue_second_reaction`.
- `test_ci_recovered_resets_failure_state_for_future_failures`.
- `test_ci_recovered_without_prior_failure_does_not_enqueue_diagnostic`.
- `test_ci_diagnostic_reaction_uses_phase2_proposal_path_for_mutation`.
- `test_ci_diagnostic_reaction_has_no_mutating_action_fields`.

- [ ] **Step 2: Run tests and verify RED**

```bash
cargo test -p rk-daemon sdlc_ci -- --nocapture
```

- [ ] **Step 3: Implement CI transition reaction logic**

Base reactions on transition records emitted by `rk-space`, not individual event counts. Current-state rows survive daemon restarts and prevent duplicate reactions.

- [ ] **Step 4: Run tests and verify GREEN**

```bash
cargo test -p rk-daemon sdlc_ci -- --nocapture
```

- [ ] **Step 5: Commit**

```bash
git add crates/rk-daemon/src/reactor.rs crates/rk-daemon/src/ingest.rs crates/rk-daemon/tests
git commit -m "feat: react once to CI feedback transitions"
```

## Task 6: Deployment Provenance Current Fact

**Files:**

- Modify: `crates/rk-space/src/store.rs`
- Modify: `crates/rk-daemon/src/ingest.rs`
- Create or modify tests under existing `rk-space` and `rk-daemon` test layouts.

**Interfaces:**

- Stable current identity: `(source, environment, service)`.
- Fact content includes sanitized commit SHA, deployment source, receipt ID, occurred time, observed time, and optional branch/repo references.
- A newer `deployment_succeeded` for the same `(source, environment, service)` replaces the current deployment provenance fact.
- Deployment provenance is observational only and cannot trigger rollback, deploy, restart, scale, or external lookup.

- [ ] **Step 1: Write failing deployment provenance tests**

Implement these exact tests:

- `test_deployment_succeeded_projects_current_provenance_fact`.
- `test_newer_deployment_replaces_current_fact_for_same_service_environment`.
- `test_deployment_for_different_environment_keeps_separate_fact`.
- `test_deployment_fact_contains_receipt_and_sanitized_refs`.
- `test_deployment_fact_rejects_credential_attributes`.
- `test_deployment_projection_does_not_enqueue_mutation`.

- [ ] **Step 2: Run tests and verify RED**

```bash
cargo test -p rk-space sdlc_deployment -- --nocapture
cargo test -p rk-daemon sdlc_deployment -- --nocapture
```

- [ ] **Step 3: Implement deployment provenance projection**

Reuse the transactional projection path from Task 3. Ensure the current fact is replaced atomically when a newer deployment signal is accepted.

- [ ] **Step 4: Run tests and verify GREEN**

```bash
cargo test -p rk-space sdlc_deployment -- --nocapture
cargo test -p rk-daemon sdlc_deployment -- --nocapture
```

- [ ] **Step 5: Commit**

```bash
git add crates/rk-space/src/store.rs crates/rk-daemon/src/ingest.rs crates/rk-space/tests crates/rk-daemon/tests
git commit -m "feat: track deployment provenance facts"
```

## Task 7: Production Alert Read-Only Diagnosis

**Files:**

- Modify: `crates/rk-daemon/src/reactor.rs`
- Modify: `crates/rk-daemon/src/ingest.rs`
- Modify: `crates/rk-space/src/store.rs`
- Create or modify tests under existing `rk-daemon` and `rk-space` test layouts.

**Interfaces:**

- Stable alert identity: `(source, environment, service, alert_key)`.
- `production_alert_firing` may enqueue or expose one read-only diagnosis request per firing transition.
- `production_alert_resolved` updates current state and prevents new diagnosis while resolved.
- Diagnosis context contains only sanitized refs, deployment provenance facts, current alert fact, receipt IDs, and non-secret attributes.
- Diagnosis payload schema rejects executable, action, command, mutation, and credential fields.

- [ ] **Step 1: Write failing alert diagnosis tests**

Implement these exact tests:

- `test_alert_firing_creates_read_only_diagnosis_context`.
- `test_alert_diagnosis_accepts_only_structured_sanitized_references`.
- `test_alert_diagnosis_context_excludes_credentials`.
- `test_alert_diagnosis_rejects_executable_action_and_command_fields`.
- `test_alert_diagnosis_has_no_production_mutation_action`.
- `test_alert_resolved_updates_current_state`.
- `test_duplicate_alert_firing_is_idempotent`.
- `test_alert_re_firing_after_resolved_can_create_new_diagnosis`.

- [ ] **Step 2: Run tests and verify RED**

```bash
cargo test -p rk-daemon sdlc_alert -- --nocapture
cargo test -p rk-space sdlc_alert -- --nocapture
```

- [ ] **Step 3: Implement read-only alert diagnosis**

Build diagnosis context from already-ingested facts and sanitized refs only. Do not fetch external production systems, call vendor APIs, pass credentials to agents, or create production mutation proposals automatically.

Forbidden fields and values include: `action`, `command`, `executable`, `credential`, `token`, `password`, `authorization`, `cookie`, `rollback`, `restart`, `scale`, `deploy`, `kubectl`, `terraform apply`, `ssh`, `delete`, and `patch`.

- [ ] **Step 4: Run tests and verify GREEN**

```bash
cargo test -p rk-daemon sdlc_alert -- --nocapture
cargo test -p rk-space sdlc_alert -- --nocapture
```

- [ ] **Step 5: Run final verification**

```bash
cargo test -p rk-core sdlc -- --nocapture
cargo test -p rk-space sdlc -- --nocapture
cargo test -p rk-daemon sdlc_ingest -- --nocapture
cargo test -p rk-cli sdlc_ingest -- --nocapture
cargo test -p rk-daemon sdlc_ci -- --nocapture
cargo test -p rk-space sdlc_deployment -- --nocapture
cargo test -p rk-daemon sdlc_deployment -- --nocapture
cargo test -p rk-daemon sdlc_alert -- --nocapture
cargo test -p rk-space sdlc_alert -- --nocapture
cargo test --workspace
MISE_TRUSTED_CONFIG_PATHS="$PWD" mise run verify
```

If `mise run verify` is unavailable or requires local services, record the exact failure and the replacement verification command in the PR notes.

- [ ] **Step 6: Review diff and guardrails**

```bash
git diff --check
git status --short
git diff --stat origin/main..HEAD
rg -n "public HTTP|webhook|github|gitlab|datadog|pagerduty|token|password|authorization|cookie|kubectl|terraform apply|rollback|restart|scale|ssh|action|command|executable" crates/rk-core crates/rk-space crates/rk-daemon crates/rk-cli
```

Manually verify:

- no public HTTP ingest route;
- no vendor SDK dependency;
- no raw telemetry persistence;
- no credential fields passed to agents;
- no production mutation action;
- CI reactions use existing reactor tuples/triggers;
- Phase 2 proposal path is the only documented future mutation path;
- unrelated `mise.toml` and other plan changes are preserved and not included unless intentionally changed by their own tasks.

- [ ] **Step 7: Commit**

```bash
git add crates/rk-daemon/src/reactor.rs crates/rk-daemon/src/ingest.rs crates/rk-space/src/store.rs crates/rk-daemon/tests crates/rk-space/tests
git commit -m "feat: diagnose production alerts read-only"
```

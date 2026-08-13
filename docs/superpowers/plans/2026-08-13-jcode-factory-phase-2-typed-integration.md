# Jcode Factory Foreman Phase 2 Typed Integration Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the Phase 1 repository-local factory foreman from a Python-only read/propose helper into typed Rat Kingdom integration points: shared deterministic action risk and canonical proposal digests in `rk-core`, daemon-enforced digest-bound operator approval and restart-safe execution for the initial `workflow.run` mutation, raw and typed client calls that preserve daemon `RpcError.code`, a minimal stdio `rk-mcp` facade with stable typed schemas, factory projection over existing `rk-space` durable coordinator events and replay/watch semantics, and a repository-owned Jcode dashboard renderer that consumes snapshots/events without becoming a control plane.

**Architecture:** Keep authority in the daemon. Reads may execute directly through typed daemon/MCP methods. Factory mutation RPCs are operator-only in `Server::authorized`; proposal requester and approval operator identities come from the authenticated request caller, never from user-supplied params. Mutations return typed proposals unless the authenticated operator presents an exact daemon-verifiable approval bound to digest, daemon-resolved registered repository identity/path, caller, action kind, and expiry. The daemon recomputes canonical scope and digest immediately before execution and rejects tamper, stale, scope, or caller mismatch. Put reusable canonicalization and risk classification in `rk-core`; reuse the existing coordinator `rk-space` durable event replay/watch cursor semantics instead of inventing another event store. Add only the first typed mutating path, `workflow.run`. Do not port all Phase 1 Python triage to Rust unless a typed schema, risk boundary, event projection, or dashboard snapshot strictly needs that behavior.

**Tech Stack:** Rust workspace, `rk-core`, `rk-daemon`, `rk-cli`, new `rk-mcp` stdio binary crate, `serde`, `serde_json`, `schemars`, `sha2`, existing daemon NDJSON socket protocol, `rk-daemon/src/client.rs` raw/typed client, existing workflow/coordinator `rk-space` durable event semantics, repository-local Jcode Markdown/JSON dashboard assets, existing `mise run verify`.

## Global Constraints

- Write minimal, YAGNI code. Phase 2 adds one typed mutating workflow action: initial `workflow.run`. Do not generalize to spawn, dismiss, archive, tickets, tuple writes, onboarding, or every Python triage rule.
- Preserve Phase 1 skill behavior until the typed path is complete. The Python helper may remain as a compatibility/read-only renderer; do not rewrite it in Rust wholesale.
- Shared canonical digests live in `rk-core` and are the only approval identity. Human-readable shell strings, MCP tool text, dashboard buttons, and CLI displays are not authority.
- Canonical digest input must be deterministic JSON over typed action data, not ad-hoc command strings. Stable field order, explicit schema version, action kind, daemon-resolved registered repo identity/path, params, coordinator, authenticated requester, risk, and expiry/nonce fields must be covered.
- Reads may execute. `factory.propose_action`, `factory.approve_action`, and `factory.execute_action` must be operator-only in `Server::authorized`. Mutations must return proposals unless a daemon-verifiable exact digest approval is presented.
- The daemon must reject execution when the presented approval digest is missing, unknown, expired, already consumed, bound to a different authenticated caller, bound to a different daemon-resolved registered repo identity/path, bound to a different action kind, or no longer matches the recomputed action.
- Approval persistence and execution must be crash-safe enough for one daemon restart: approved-but-not-executing can execute if still valid; executing approvals resume or return the persisted execution id/instance id; consumed approvals cannot execute twice.
- Reuse coordinator `rk-space` durable event semantics: bounded replay, cursor boundary, truncation sentinel, then live stream after the replay boundary. Do not introduce offset-by-timestamp polling or another durable event store.
- The factory event surface must be a projection over existing durable coordinator events and shared replay semantics. Extend projection/envelope data only where needed for agent, workflow, ticket, inbox, budget, approval, and repository resync views, with source metadata and monotonic cursors.
- `rk-mcp` must speak stdio and publish stable typed schemas. Tool names and JSON shapes must be versioned and tested as contract fixtures.
- Repository-owned Jcode dashboard rendering belongs in this repository and consumes typed snapshots/events. It must show whether state is live, replayed, truncated, degraded, or resyncing. The dashboard is a pure renderer; an optional Jcode side-panel may display its output, but neither dashboard nor side-panel is a live control plane.
- Do not edit unrelated files, especially existing `mise.toml` changes owned by another actor.
- Follow test-driven development. Each behavior gets a failing test before implementation.
- Commit each independently testable task during implementation. This planning revision request says do not commit.

---

### Task 1: Shared Canonical Action Risk and Digest in `rk-core`

**Files:**
- Modify: `crates/rk-core/src/lib.rs`
- Create: `crates/rk-core/src/action.rs`
- Modify: `crates/rk-core/Cargo.toml`
- Create: `crates/rk-core/tests/action_digest.rs`

**Interfaces:**
- Produces: `ActionKind`, `ActionRisk`, `ActionScope`, `RepoScope`, `WorkflowRunAction`, `FactoryAction`, `ActionProposal`, `ApprovalGrant`, `ApprovalStatus`, `canonical_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>>`, and `canonical_digest<T: Serialize>(value: &T) -> Result<String>`.
- `ActionKind` initial value: `workflow.run`.
- `ActionRisk` values: `read`, `mutation`, `dangerous`.
- `FactoryAction::WorkflowRun(WorkflowRunAction)` fields: `name`, `repo`, `repo_identity`, `repo_path`, `params`, `coordinator`; the daemon fills `repo_identity` and canonical `repo_path` from the registered repo record before digesting.
- `ActionProposal` schema: `{schema, id, digest, kind, risk, scope, requester, action, created_at, expires_at, status}`. `requester` is copied from `Request.caller` by the daemon.
- `ApprovalGrant` schema: `{schema, proposal_id, digest, kind, scope, requester, approved_by, status, approved_at, expires_at, execution_id, instance_id, failure, consumed_at}` with `status` values `approved`, `executing`, `consumed`, and `failed`. `approved_by` is copied from the authenticated approve caller by the daemon, never accepted from params.

- [ ] **Step 1: Write failing canonical digest tests**

Implement these exact tests:

- `test_workflow_run_digest_is_stable_across_map_insertion_order`: build the same `WorkflowRunAction` with params inserted in different orders and assert equal digests.
- `test_digest_changes_when_repo_identity_or_path_changes`: change only daemon-resolved repo identity/path and assert different digests.
- `test_digest_covers_authenticated_requester`: change only `requester` and assert different digests.
- `test_digest_covers_action_kind_and_schema`: change `kind` or schema version and assert different digests.
- `test_workflow_run_is_mutation_risk`: assert `FactoryAction::WorkflowRun(...).risk() == ActionRisk::Mutation`.
- `test_canonical_json_has_sorted_object_keys`: assert serialized JSON for params orders keys lexicographically and contains no insignificant whitespace.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cargo test -p rk-core --test action_digest -- --nocapture
```

Expected: failure because `rk_core::action` does not exist.

- [ ] **Step 3: Implement minimal canonicalization and risk types**

Add `serde`, `serde_json`, `schemars`, `sha2`, and `chrono` usage from workspace dependencies only where needed. Implement canonical JSON by recursively converting `serde_json::Value::Object` maps into sorted `BTreeMap` order before `serde_json::to_vec`. Reject floats that are not finite. Return lowercase hex SHA-256 digest. Keep approval statuses as string-stable serde values: `approved`, `executing`, `consumed`, `failed`.

Keep domain types narrow. Do not add command-string digests or non-workflow actions.

- [ ] **Step 4: Run tests and verify GREEN**

Run the core test command above. Expected: all action digest tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/rk-core/Cargo.toml crates/rk-core/src/lib.rs crates/rk-core/src/action.rs crates/rk-core/tests/action_digest.rs
git commit -m "feat: add canonical action digests"
```

### Task 2: Daemon Proposal Store for Digest-Bound Workflow Run Approval and Client Errors

**Files:**
- Modify: `crates/rk-daemon/src/lib.rs`
- Create: `crates/rk-daemon/src/action_approval.rs`
- Modify: `crates/rk-daemon/src/server.rs`
- Modify: `crates/rk-daemon/src/client.rs`
- Modify: `crates/rk-daemon/src/proto.rs` only if new error codes are needed
- Create: `crates/rk-daemon/tests/workflow_run_approval.rs`
- Create or modify: `crates/rk-daemon/tests/client_rpc_error.rs`

**Interfaces:**
- Produces RPC methods:
  - `factory.propose_action` with params `{kind:"workflow.run", action:{name, repo, params, coordinator}, ttl_seconds?}`. The daemon resolves `repo` through registered repository state into canonical identity/path, copies `requester` from `Request.caller`, and persists that scope before digesting.
  - `factory.approve_action` with params `{proposal_id, digest}`. The daemon copies `approved_by` from authenticated `Request.caller`; no `approved_by`, `actor`, or `requester` param is accepted.
  - `factory.execute_action` with params `{proposal_id, digest, action:{...}}`. The daemon copies the executing caller from `Request.caller` and compares it with the proposal requester.
- `factory.propose_action`, `factory.approve_action`, and `factory.execute_action` must be added to the operator-only branch in `Server::authorized` next to existing `workflow.run`; supervised agents and onboarders cannot invoke them directly.
- `factory.execute_action` is the only Phase 2 path that calls existing `engine.run_owned` through an approval gate.
- Proposal storage file: under the daemon-owned layout, `factory-actions.json` or a subdirectory adjacent to current daemon durable JSON stores. Persist proposal, grant status, execution id, and workflow instance id so restart retries are idempotent.
- Error codes: existing `bad_params`, `forbidden`, and `internal` are enough unless a typed code materially improves clients. `Client::call_raw` returns the full `Response`/`RpcError`; `Client::call_typed<T>` decodes success but preserves `RpcError.code` in failures before MCP translates it.

- [ ] **Step 1: Write failing daemon approval tests**

Use existing daemon fixture style from `crates/rk-daemon/tests/workflow_approval_binding.rs` and other daemon tests. Implement these exact tests:

- `test_workflow_run_without_approval_returns_proposal_not_instance`: calling the typed proposal path as authenticated operator returns `proposal.digest` and does not create a workflow instance.
- `test_execute_workflow_run_with_matching_approval_starts_instance`: propose, approve exact digest, execute exact action, then assert the returned instance has the requested workflow name and repo.
- `test_execute_rejects_tampered_params`: change one workflow param between approval and execute, then assert no instance is created.
- `test_propose_uses_registered_repo_identity_not_param_path`: pass an alias or relative repo param and assert the stored scope/digest uses daemon-resolved registered identity and canonical path.
- `test_execute_rejects_stale_digest_after_repo_change`: approve one daemon-resolved repo scope, execute with another registered repo, then assert `bad_params` or `forbidden` and no instance.
- `test_execute_rejects_caller_mismatch`: proposal requester copied from caller A cannot be executed by caller B, even if params attempt to name A.
- `test_execute_rejects_scope_mismatch`: approval for daemon-resolved repo A cannot run in repo B even when the digest string is supplied.
- `test_execute_persists_executing_consumed_failed_states`: assert status transitions `approved -> executing -> consumed` with execution id and instance id on success, and `failed` with failure text when `engine.run_owned` rejects after the approval enters execution.
- `test_execute_consumes_approval_once`: second execute with the same grant is rejected and returns the persisted consumed instance only through the idempotency path, not by starting a duplicate.
- `test_approved_unexecuted_grant_survives_daemon_restart`: restart daemon between approval and execution and assert execute still works once.
- `test_executing_grant_restart_is_idempotent`: simulate restart after persisting `executing` with execution id/instance id and assert retry returns the persisted instance instead of starting a duplicate.
- `test_client_raw_and_typed_preserve_rpc_error_code`: fake a daemon `RpcError { code:"forbidden", ... }` and assert raw and typed client APIs expose `forbidden` rather than only formatted protocol text.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cargo test -p rk-daemon --test workflow_run_approval -- --nocapture
```

Expected: failure because factory action RPCs and storage do not exist.

- [ ] **Step 3: Implement proposal, approval, and execute flow**

Implement a small durable store protected by the daemon, similar in spirit to existing onboarding proposal digest checks in `crates/rk-daemon/src/onboarding_proposals.rs` and the digest comparison in onboarding apply/activate handlers. On propose, reject non-operator callers through `Server::authorized`, resolve the submitted repo through registered daemon repo state, copy `requester` from `Request.caller`, compute the `rk-core` digest, and persist proposal status. On approve, re-read the proposal, compare the supplied digest, copy `approved_by` from authenticated `Request.caller`, bind requester, kind, canonical scope, and expiry, then persist an approval grant with `status:"approved"`. On execute, recompute scope and digest from the supplied typed action and request caller, verify the stored grant, persist `status:"executing"` with a stable execution id before calling `engine.run_owned`, then persist `status:"consumed"` with the returned instance id or `status:"failed"` with failure text. Retries after restart must return the persisted executing/consumed instance when present and must not create a second workflow instance.

Add `Client::call_raw(&mut self, method: &str, params: Value) -> rk_core::Result<Response>` and `Client::call_typed<T: DeserializeOwned>(&mut self, method: &str, params: Value) -> Result<T, ClientRpcError>` or the smallest equivalent typed wrapper. Existing `Client::call` may keep returning `Value`, but MCP must use the error-preserving API so daemon `RpcError.code` survives stale/tamper/forbidden rejections.

Do not change the legacy `workflow.run` RPC yet; keep current CLI behavior untouched until the CLI task intentionally routes through typed proposals.

- [ ] **Step 4: Run tests and verify GREEN**

Run the daemon approval test command. Expected: all new tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/rk-daemon/src/lib.rs crates/rk-daemon/src/action_approval.rs crates/rk-daemon/src/server.rs crates/rk-daemon/src/client.rs crates/rk-daemon/src/proto.rs crates/rk-daemon/tests/workflow_run_approval.rs crates/rk-daemon/tests/client_rpc_error.rs
git commit -m "feat: enforce workflow action approvals"
```

### Task 3: CLI Surface for Typed Proposal, Approval, and Execution

**Files:**
- Modify: `crates/rk-cli/src/main.rs`
- Create: `crates/rk-cli/src/factory_cmds.rs`
- Create or modify: `crates/rk-cli/tests/workflow_run_approval.rs`

**Interfaces:**
- Adds CLI commands:
  - `rk --json factory propose-workflow WORKFLOW --repo REPO [--param KEY=VALUE] [--coordinator ID]`.
  - `rk --json factory approve PROPOSAL_ID DIGEST`.
  - `rk --json factory execute-workflow PROPOSAL_ID DIGEST --workflow WORKFLOW --repo REPO [--param KEY=VALUE] [--coordinator ID]`.
- Existing `rk workflow run` may keep current behavior for backwards compatibility in Phase 2, but Jcode-facing docs and MCP tools must use the typed `factory` path.
- Global JSON output follows the existing convention: `--json` is a top-level `rk` flag before the command, for example `rk --json factory approve ...`. Do not introduce subcommand-local `--json`.

- JSON output must include `schema`, `proposal`, `digest`, `risk`, and exact typed `action` on propose; `approval` on approve; `instance` on execute.

- [ ] **Step 1: Write failing CLI tests**

Implement these exact tests with the real temp-layout CLI/daemon pattern used by existing CLI integration tests. Use `tempfile::TempDir`, initialize a disposable repo, start/connect to a temp daemon, and invoke the `rk` binary with top-level `--json`; do not use a mocked protocol recorder unless the repo already has a reusable fixture with the same authority and socket semantics.

- `test_factory_propose_workflow_sends_typed_action_not_shell_command`: assert RPC method is `factory.propose_action` and params contain structured `action`, not an argv string.
- `test_factory_approve_sends_no_identity_param`: `rk --json factory approve PROPOSAL_ID DIGEST` sends only proposal id and digest; any attempt to pass an approval identity flag fails parsing.
- `test_factory_approve_requires_exact_digest`: assert missing or malformed digest fails client-side before RPC.
- `test_factory_execute_workflow_preserves_param_values_with_spaces`: assert `KEY=value with spaces` is one JSON string in `params`.
- `test_factory_execute_prints_instance_json_on_success`: temp daemon response with `instance` is preserved in JSON output.
- `test_factory_execute_rejects_param_without_equals`: invalid `--param broken` exits with code 2.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cargo test -p rk-cli --test workflow_run_approval -- --nocapture
```

Expected: failure because `factory` CLI commands do not exist.

- [ ] **Step 3: Implement minimal CLI bindings**

Reuse existing `Client::connect_or_spawn` behavior only where current mutating commands already use it. Keep parsing boring: repeatable `--param KEY=VALUE` becomes a JSON object with string values. Do not add shell display rendering as authority. Include digest display only from daemon response.

- [ ] **Step 4: Run tests and verify GREEN**

Run the CLI test command. Expected: all new CLI approval tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/rk-cli/src/main.rs crates/rk-cli/src/factory_cmds.rs crates/rk-cli/tests/workflow_run_approval.rs
git commit -m "feat: add typed factory approval CLI"
```

### Task 4: Coordinator Event Projection and Cursor Watch

**Files:**
- Modify: `crates/rk-daemon/src/coordinator.rs`
- Create: `crates/rk-daemon/src/factory_events.rs`
- Modify: `crates/rk-daemon/src/server.rs`
- Modify existing mutation sites as needed:
  - `crates/rk-daemon/src/supervisor.rs`
  - `crates/rk-daemon/src/workflow_exec.rs`
  - `crates/rk-daemon/src/tickets.rs`
  - `crates/rk-daemon/src/action_approval.rs`
- Create: `crates/rk-daemon/tests/factory_events.rs`

**Interfaces:**
- Produces RPC methods:
  - `factory.snapshot` with params `{repo?, coordinator?, include_archived?}`.
  - `factory.events.replay` with params `{after?, repo?, kinds?, limit?}`.
  - `factory.events.watch` with params `{after?, repo?, kinds?}` and NDJSON streaming behavior matching existing watch endpoints.
- Event schema: `{schema, cursor, occurred_at, kind, repo, caller, source, subject, summary, payload}`. `caller` comes from authenticated daemon request context or existing coordinator event metadata, not from dashboard or MCP params.
- Event kinds: `agent.changed`, `workflow.changed`, `ticket.changed`, `inbox.changed`, `budget.changed`, `approval.changed`, `repo.resync.changed`.
- Replay shape: `{schema, events, boundary, truncated}`.

- [ ] **Step 1: Write failing factory event tests**

Implement these exact tests:

- `test_replay_uses_sentinel_boundary_when_truncated`: mirror `coordinator::replay` behavior and assert boundary is the sentinel cursor after the returned page.
- `test_watch_skips_events_at_or_before_replay_boundary`: replay then watch from boundary and assert no duplicate event is delivered.
- `test_workflow_run_emits_workflow_event`: start an approved workflow and assert event kinds include `workflow.changed` with source and subject metadata.
- `test_approval_lifecycle_emits_approval_changed_events`: propose, approve, execute and assert proposal, approved, and consumed states appear in order.
- `test_factory_snapshot_contains_agents_workflows_tickets_inbox_budget_approvals_and_resync`: assert snapshot top-level keys exactly cover the Phase 2 dashboard inputs.
- `test_replay_filters_by_repo_and_kind`: mixed repos/kinds only return requested rows.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cargo test -p rk-daemon --test factory_events -- --nocapture
```

Expected: failure because factory event RPCs and projection do not exist.

- [ ] **Step 3: Implement projection over durable coordinator events**

Reuse or extract the existing `CoordinatorEvent`, `CoordinatorFilter`, and `replay` ideas from `crates/rk-daemon/src/coordinator.rs`: bounded replay, `boundary`, `truncated`, and live stream from strictly after the boundary. Do not create another durable event store. Project factory event envelopes from existing `rk-space` coordinator events and extend the existing durable event row only when a factory view needs one additional field such as source, subject, or approval status. Emit projection-source events only for the `workflow.run` approval lifecycle and workflow changes introduced in Phase 2; ticket, inbox, budget, and resync data may appear in snapshots without new mutation hooks unless existing coordinator events already expose them. Do not add background scanners or SDLC ingestion.

If a source already writes a tuple or durable record, project a compact event envelope that points at the source and subject rather than duplicating entire objects.

- [ ] **Step 4: Run tests and verify GREEN**

Run the factory event test command. Expected: all new event tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/rk-daemon/src/coordinator.rs crates/rk-daemon/src/factory_events.rs crates/rk-daemon/src/server.rs crates/rk-daemon/src/supervisor.rs crates/rk-daemon/src/workflow_exec.rs crates/rk-daemon/src/tickets.rs crates/rk-daemon/src/action_approval.rs crates/rk-daemon/tests/factory_events.rs
git commit -m "feat: project factory events"
```

### Task 5: Minimal Stdio `rk-mcp` Crate with Stable Typed Schemas

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/rk-mcp/Cargo.toml`
- Create: `crates/rk-mcp/src/main.rs`
- Create: `crates/rk-mcp/src/schema.rs`
- Create: `crates/rk-mcp/src/tools.rs`
- Create: `crates/rk-mcp/tests/stdio_contract.rs`
- Create: `crates/rk-mcp/tests/fixtures/*.json`

**Interfaces:**
- Binary: `rk-mcp`.
- Tools:
  - `factory_snapshot` executes a read and returns typed snapshot.
  - `factory_events_replay` returns a finite bounded replay page with `after`, `limit`, `boundary`, and `truncated`; live daemon watch remains a native UDS/CLI capability and is not modeled as a long-running MCP tool.
  - `propose_workflow_run` returns a proposal only.
  - `approve_action` records an approval grant.
  - `execute_approved_workflow_run` executes only with exact proposal id and digest.
- Schemas are generated or checked from Rust types with `schemars` and fixture snapshots.
- Tool contract version: `schema: 1` in all request/response payloads.

- [ ] **Step 1: Write failing MCP contract tests**

Implement these exact tests with a real temp daemon/socket harness or a subprocess-level stdio fixture that talks to the same `Client::call_raw`/`Client::call_typed` API used in production. Do not assert against a mocked daemon shape that can hide authentication, scope, or `RpcError.code` behavior.

- `test_initialize_lists_factory_tools_with_stable_names`: stdio initialize/list-tools response contains the five tool names above.
- `test_tool_schemas_match_fixtures`: generated JSON schemas match committed fixtures byte-for-byte after canonical formatting.
- `test_factory_snapshot_tool_calls_read_rpc_only`: temp daemon logs or observes only `factory.snapshot` for the read tool.
- `test_propose_workflow_run_tool_never_executes`: temp daemon observes `factory.propose_action` and never `workflow.run` or `factory.execute_action`.
- `test_execute_approved_workflow_run_requires_digest`: missing digest is rejected before daemon call.
- `test_mcp_errors_preserve_daemon_rejection_code`: daemon stale/tamper rejection is surfaced to MCP clients without converting to success text.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cargo test -p rk-mcp -- --nocapture
```

Expected: failure because the crate is not in the workspace.

- [ ] **Step 3: Implement smallest stdio MCP server**

Implement only the MCP JSON-RPC 2.0 messages needed for `initialize`, `tools/list`, and `tools/call`, including `jsonrpc`, request ids, capabilities, typed tool content, and protocol error shapes. Use the standard line-delimited stdio transport expected by MCP clients, with one complete JSON-RPC message per line. Bridge tool calls to existing daemon RPCs through the typed client. Keep descriptions precise about authority: read tools execute; mutation tools propose unless exact digest approval is supplied and verified by the daemon.

Do not add async task orchestration, OAuth, web server transport, or full dashboard hosting to `rk-mcp`.

- [ ] **Step 4: Run tests and verify GREEN**

Run the `rk-mcp` test command. Expected: all MCP contract tests pass.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/rk-mcp
git commit -m "feat: add typed factory MCP server"
```

### Task 6: Repository-Owned Jcode Dashboard Renderer

**Files:**
- Create: `.jcode/skills/factory-foreman/dashboard/render_factory_dashboard.py`
- Create: `.jcode/skills/factory-foreman/dashboard/templates/factory-dashboard.md`
- Create: `.jcode/skills/factory-foreman/tests/test_factory_dashboard.py`
- Modify: `.jcode/skills/factory-foreman/SKILL.md`
- Modify: `.jcode/skills/factory-foreman/REFERENCE.md`

**Interfaces:**
- Produces CLI:
  - `python3 .jcode/skills/factory-foreman/dashboard/render_factory_dashboard.py --snapshot PATH --events PATH --output PATH`.
- Input snapshot/events come from typed `factory.snapshot` and `factory.events.replay` or MCP tool output.
- Output sections: `Factory Dashboard`, `Connection State`, `Resync State`, `Approvals`, `Workflow Runs`, `Agents`, `Tickets`, `Inbox`, `Budget`, `Recent Events`, `Degraded Data`.
- Resync state fields shown when present: `{status, last_started_at, last_finished_at, source, error}`.

- [ ] **Step 1: Write failing dashboard renderer tests**

Implement these exact tests with fixture JSON:

- `test_dashboard_renders_snapshot_and_events_sections`: assert all required headings exist.
- `test_dashboard_exposes_replay_truncation_and_boundary`: event replay with `truncated:true` shows the boundary cursor and warning.
- `test_dashboard_marks_resyncing_state`: snapshot `repo_resync.status == "running"` renders `RESYNCING`.
- `test_dashboard_marks_stale_or_degraded_state`: stale snapshot or failed observation renders `DEGRADED` and the failing source.
- `test_dashboard_lists_pending_approvals_with_digest`: pending approval row includes proposal id, digest, requester, scope, and expiry.
- `test_dashboard_never_renders_execute_command_as_approved`: proposal rows are labelled `proposal` until the daemon snapshot says approved.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
python3 -m unittest discover -s .jcode/skills/factory-foreman/tests -v
```

Expected: dashboard tests fail because the renderer does not exist.

- [ ] **Step 3: Implement pure renderer**

Use Python standard library only. Read JSON files, render deterministic Markdown, and write the output path. Do not call `rk`, `rk-mcp`, or the daemon from the renderer. The dashboard is a repository-owned presentation layer over typed snapshot/events, not a second source of truth.

Update the skill docs so Jcode prefers typed MCP/CLI snapshot and event inputs when available, then invokes the renderer. Keep the strict approval language from Phase 1 and update it to say daemon-verifiable digest approval is required for execution.

- [ ] **Step 4: Run tests and verify GREEN**

Run the Python unittest command. Expected: all factory-foreman Python tests pass.

- [ ] **Step 5: Commit**

```bash
git add .jcode/skills/factory-foreman/dashboard .jcode/skills/factory-foreman/tests/test_factory_dashboard.py .jcode/skills/factory-foreman/SKILL.md .jcode/skills/factory-foreman/REFERENCE.md
git commit -m "feat: render factory dashboard state"
```

### Task 7: End-to-End Typed Factory Acceptance and Documentation

**Files:**
- Modify: `docs/factory-foreman.md`
- Create: `docs/superpowers/plans/2026-08-13-jcode-factory-phase-2-typed-integration-acceptance.md` only if implementation notes need a separate checklist
- Modify: `README.md` only if Phase 1 already linked factory foreman there and the Phase 2 typed path needs one sentence

**Interfaces:**
- Documents typed authority model, canonical digest schema, MCP tools, coordinator event projection/watch semantics, dashboard renderer workflow, and Phase 2 limitations.

- [ ] **Step 1: Run full Rust verification**

Run:

```bash
cargo test -p rk-core --test action_digest -- --nocapture
cargo test -p rk-daemon --test workflow_run_approval -- --nocapture
cargo test -p rk-daemon --test factory_events -- --nocapture
cargo test -p rk-cli --test workflow_run_approval -- --nocapture
cargo test -p rk-mcp -- --nocapture
MISE_TRUSTED_CONFIG_PATHS="$PWD" mise run verify
```

Expected: all tests pass. If `mise.toml` is dirty before this task, do not modify it; run with the current workspace config and preserve unrelated changes.

- [ ] **Step 2: Run live typed acceptance**

With an already-running daemon and a disposable test workflow/repo fixture, run the full proposal boundary:

```bash
rk --json factory propose-workflow approval-binding-test --repo rat-kingdom \
  --param reason=phase2-acceptance > "$JCODE_SCRATCH_DIR/factory-proposal.json"
python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); assert d["proposal"]["digest"] and d["proposal"]["risk"] == "mutation"' "$JCODE_SCRATCH_DIR/factory-proposal.json"
DIGEST=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["proposal"]["digest"])' "$JCODE_SCRATCH_DIR/factory-proposal.json")
PROPOSAL=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["proposal"]["id"])' "$JCODE_SCRATCH_DIR/factory-proposal.json")
rk --json factory approve "$PROPOSAL" "$DIGEST" > "$JCODE_SCRATCH_DIR/factory-approval.json"
rk --json factory execute-workflow "$PROPOSAL" "$DIGEST" \
  --workflow approval-binding-test --repo rat-kingdom \
  --param reason=phase2-acceptance > "$JCODE_SCRATCH_DIR/factory-execute.json"
```

Expected: proposal and approval succeed, execute returns an instance, and a second identical execute is rejected.

- [ ] **Step 3: Run tamper and stale acceptance**

Run:

```bash
rk --json factory execute-workflow "$PROPOSAL" "$DIGEST" \
  --workflow approval-binding-test --repo rat-kingdom \
  --param reason=tampered
```

Expected: daemon rejects with stale/tamper/scope mismatch and creates no second workflow instance.

- [ ] **Step 4: Run MCP and dashboard acceptance**

Run:

```bash
rk-mcp < crates/rk-mcp/tests/fixtures/list_tools_request.json > "$JCODE_SCRATCH_DIR/rk-mcp-tools.json"
rk --json factory snapshot --repo rat-kingdom > "$JCODE_SCRATCH_DIR/factory-snapshot.json"
rk --json factory events replay --repo rat-kingdom > "$JCODE_SCRATCH_DIR/factory-events.json"
python3 .jcode/skills/factory-foreman/dashboard/render_factory_dashboard.py \
  --snapshot "$JCODE_SCRATCH_DIR/factory-snapshot.json" \
  --events "$JCODE_SCRATCH_DIR/factory-events.json" \
  --output "$JCODE_SCRATCH_DIR/factory-dashboard.md"
```

Expected: MCP lists stable tools, snapshot/events JSON validate, and the rendered dashboard shows current connection/resync state plus the approval and workflow events from acceptance.

- [ ] **Step 5: Document Phase 2 limitations**

Clearly label limitations:

- only initial `workflow.run` has daemon-enforced typed approval/execution;
- legacy direct mutation RPCs may remain for existing CLI/operator flows, but Jcode/MCP paths must use typed factory proposals;
- factory event projection is a view over coordinator events, not an external CI/deployment/SDLC ingestion system or separate journal;
- dashboard is a renderer over snapshot/events, not a control plane;
- Python triage remains deterministic hints and is not the source of daemon authority;
- canonical digest proves exact typed action identity, not that the human made a good decision;
- MCP stdio transport is local and inherits daemon auth/caller semantics.

- [ ] **Step 6: Review the diff**

Check:

```bash
git diff --check
git status --short
git diff --stat origin/main..HEAD
```

An independent reviewer must verify authority boundaries, canonical digest coverage, daemon rejection paths, event cursor semantics, stable MCP schemas, dashboard resync/degraded states, and that no broad Python triage port or unrelated `mise.toml` edit slipped in.

- [ ] **Step 7: Commit documentation**

```bash
git add docs/factory-foreman.md README.md docs/superpowers/plans/2026-08-13-jcode-factory-phase-2-typed-integration-acceptance.md
git commit -m "docs: document typed factory integration"
```

# Product-to-Code Operator CLI Completion Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every typed Factory Foreman proposal executable through a safe public CLI path, including `ticket_graph.apply` and `product_to_code.dispatch`, and document a complete copy-paste product-to-code operator workflow.

**Architecture:** Proposal commands will emit a versioned execution envelope containing the daemon proposal id, exact digest, action kind, and the original pre-canonical action payload required by `factory.execute_action`. `rk factory approve` will accept either its existing positional id/digest pair or `--proposal-file`; a new `rk factory execute-action --proposal-file` command will replay the exact submitted action to the daemon. The daemon remains the sole authority and re-resolves repository identity, recomputes canonical action state, validates status/digest/CAS, and rejects altered or stale files.

**Tech Stack:** Rust, clap, serde/serde_json, existing `rk_daemon::Client`, Tokio integration tests, Markdown documentation.

## Global Constraints

- Test-first: every public behavior must fail in an integration test before implementation.
- Do not change the daemon execution or approval routes unless a failing test proves the existing contract is insufficient.
- The CLI must never treat a proposal file as authority. The daemon remains authoritative for proposal status, digest, scope, caller, CAS, and execution idempotency.
- Preserve existing `rk factory approve <proposal-id> <digest>` and `rk factory execute-workflow` compatibility.
- Proposal files must contain the original submitted action, not only the daemon-canonical action, because the execute RPC intentionally re-resolves canonical fields.
- Reject ambiguous files when top-level and nested proposal id, digest, or kind disagree.
- Do not touch `.git-issue/`.
- Serialize cargo with `CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1` and `-j1`.
- Do not run `cargo fmt`; format only scoped changes if required.

---

### Task 1: Executable Proposal Envelope and Generic Factory Commands

**Files:**
- Modify: `crates/rk-cli/src/factory_cmds.rs`
- Test: `crates/rk-cli/tests/workflow_run_approval.rs`

**Interfaces:**
- Produces proposal JSON fields: `proposal_id: string`, `digest: string`, `kind: string`, `execution_action: object`.
- Produces CLI: `rk factory approve --proposal-file PATH`.
- Produces CLI: `rk factory execute-action --proposal-file PATH`.
- Preserves CLI: `rk factory approve PROPOSAL_ID DIGEST` and `rk factory execute-workflow ...`.

- [ ] Add an integration test that saves `factory propose-workflow` JSON, approves it with `--proposal-file`, executes it with `execute-action --proposal-file`, and observes one started workflow.
- [ ] Run the focused test and verify clap rejects the missing commands or fields.
- [ ] Add proposal-envelope parsing with consistency checks across top-level and nested proposal fields.
- [ ] Add `execution_action` to `propose-workflow` output.
- [ ] Extend `approve` with mutually exclusive positional or `--proposal-file` input.
- [ ] Add `execute-action --proposal-file`, forwarding `{proposal_id,digest,kind,action}` to `factory.execute_action`.
- [ ] Run the focused test and existing workflow approval tests.
- [ ] Commit only the CLI implementation and its tests.

### Task 2: Product-to-Code End-to-End Approval and Execution

**Files:**
- Modify: `crates/rk-cli/src/product_to_code_cmds.rs`
- Modify: `crates/rk-cli/tests/product_to_code_e2e.rs`
- Modify: `crates/rk-cli/tests/product_to_code_ticket_graph.rs`
- Modify: `crates/rk-cli/tests/product_to_code_workflow.rs`

**Interfaces:**
- `graph propose-apply` emits the generic proposal execution envelope with the original `{repo,graph,initiative}` payload.
- `workflow propose` emits the generic proposal execution envelope with the original `{repo,initiative,graph_id,graph_apply_proposal_id,dispatches,blocked}` payload.
- Both files are consumable by Task 1's `approve --proposal-file` and `execute-action --proposal-file` commands.

- [ ] Add a ticket graph integration test that proposes through the CLI, saves the output, approves from the file, executes from the file, and verifies minted `TKT-...` ids.
- [ ] Add or upgrade the lifecycle e2e test to execute the graph apply, propose dispatch, approve dispatch, execute dispatch, and verify only unblocked tickets launch `implement-featureset`.
- [ ] Run both tests and verify they fail because product proposal JSON lacks `execution_action` or the generic commands are absent.
- [ ] Add `execution_action` to graph and workflow proposal output and update approval instructions to use saved proposal files.
- [ ] Keep `canonical_action` for review/display and prove the execution file carries the separate pre-canonical action.
- [ ] Run product-to-code ticket graph, workflow, evidence, verification, and e2e tests.
- [ ] Commit only scoped product-to-code CLI and tests.

### Task 3: Operator Documentation and Final Verification

**Files:**
- Modify: `docs/product-to-code.md`
- Modify: `docs/factory-foreman.md`
- Modify: `README.md`

**Interfaces:**
- Documents the proposal-file sequence exactly as exposed by clap.
- Points users at `crates/rk-cli/tests/fixtures/product_to_code/` for example artifacts.

- [ ] Correct every research, graph, and verification example to include required `--initiative` and `--evidence-dir` arguments.
- [ ] Add a complete shell walkthrough that saves proposal JSON, reviews it, approves it, executes it, and captures execution results for both graph apply and dispatch.
- [ ] State explicitly that editing a proposal file does not grant authority and will be rejected by the daemon's exact-action validation.
- [ ] Update Factory Foreman and README summaries so they no longer claim the public CLI cannot execute product-to-code actions.
- [ ] Run `target/debug/rk ... --help` checks for every documented command.
- [ ] Run focused CLI/product-to-code tests, `git diff --check`, then workspace build/tests/clippy with the known pre-existing `factory_scorecards.rs` lint handled exactly as previously agreed.
- [ ] Obtain one independent read-only review of safety, backward compatibility, and documentation accuracy.
- [ ] Commit only documentation and any final scoped corrections.

## Self-Review

- Spec coverage: public approval and execution exists for both missing action kinds; existing workflow commands remain compatible; docs provide a complete path.
- Placeholder scan: no deferred implementation placeholders remain.
- Type consistency: every proposal envelope uses the same `proposal_id`, `digest`, `kind`, and `execution_action` field names consumed by generic factory commands.

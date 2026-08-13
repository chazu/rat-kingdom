# Jcode Factory Foreman Phase 3 Product-to-Code Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add repo-owned offline contracts and workflows that let Jcode Factory Foreman turn product intent into validated code work through structured architecture research, ticket graph planning, feature implementation dispatch, and independent verification evidence, while preserving Rat Kingdom as the authority and keeping Jcode, browser automation, and GitNexus out of RK runtime dependencies.

**Architecture:** Rat Kingdom owns the offline contract schemas, serde types, validation, dry-run, and apply semantics. Jcode remains an operator and evidence producer outside RK. Product-to-code is modeled as deterministic artifacts: initiative, architecture research artifact, generic evidence, ticket graph, and verification report. Workflow dispatch and graph application reuse Phase 2 canonical action/proposal approval: RK renders a canonical proposal, the user approves the exact proposal, and only then may the action be applied. RK accepts generic impact evidence produced by tools such as Jcode or GitNexus, but depends only on the repo-owned evidence contract, not those producers.

**Tech Stack:** Rust, serde, serde_json, CUE schemas checked into the repo, existing `rk` CLI and workflow engine, completed Phase 2 generic daemon proposal approval and executor interfaces, browser evidence captured externally and stored as structured evidence when applicable.


## Hard Prerequisites and Contract Conventions

- Phase 3 starts only after Phase 2 generic daemon proposals are complete, merged, and verified. In particular, RK must already have daemon-side typed proposal persistence, authenticated operator approval, canonical digest/status/CAS checks, and executor dispatch for approved generic actions. Treat missing Phase 2 generic proposal machinery as a blocker, not as work to recreate locally in Phase 3.
- Phase 3 must not add a local proposal file mutation path, `--approved-id` mutation shortcut, or CLI-owned apply path. The CLI may validate local artifacts and request/propose actions, but every mutation is executed by the daemon after authenticated operator approval of the canonical action digest.
- CUE contracts live in the owning crate when crate-local, under `crates/<owner>/contracts/`, or in a top-level `contracts/` directory only for contracts intentionally shared across crates. This plan chooses `crates/rk-core/contracts/product_to_code/` as the owning location for product-to-code schemas, with `examples/workflows/*.cue` reserved for runnable workflow definitions.
- Workflow definitions use existing CUE workflow conventions from `examples/workflows/*.cue`. Reuse `examples/workflows/implement-featureset.cue` exactly by dispatching `rk workflow run implement-featureset --param taskId=TKT-... --param taskDescription=...` plus optional `maxWorkers`, `reviewMode`, `check`, `timeout`, and `budgetUsd`. Do not introduce YAML workflow files.
- Keep the CLI thin: parse arguments, call validators, print JSON, and submit typed daemon proposals. CLI tests cover command wiring and JSON output only. Mutation safety, approval, CAS/status/digest verification, and executor idempotency belong in `rk-daemon` integration tests.

## Global Constraints

- Do not add Jcode, browser automation, GitNexus, SaaS, network, or runtime service dependencies to Rat Kingdom.
- Rat Kingdom owns every contract in offline CUE and Rust serde form.
- Preserve the authority split: Jcode and external tools may propose, research, implement, and produce evidence; RK validates, records, and applies only explicitly approved artifacts.
- Add typed Phase 3 `FactoryAction` variants for `ticket_graph.apply` and `product_to_code.dispatch`; they are daemon-executed only, after Phase 2 generic proposal approval.
- Reuse Phase 2 canonical daemon action/proposal approval for ticket graph apply and product-to-code workflow dispatch. Do not invent a parallel approval path.
- Graph validation must reject missing dependencies and dependency cycles before dry-run or apply.
- Graph dry-run must be read-only and must describe exact creates, updates, dependency edges, workflow dispatches, and blocked actions.
- Graph apply must be mutation-capable only after canonical proposal approval.
- Dispatch gate must require generic impact evidence under RK's evidence contract. Evidence may be produced by Jcode, GitNexus, or another tool, but RK must not know or require that producer at runtime.
- Delivery gate must require browser acceptance evidence when the ticket or initiative declares browser acceptance is applicable.
- Independent verifier reports must map every acceptance criterion to concrete evidence or a stated gap.
- Research workflow must produce a validated CUE-described structured JSON architecture research artifact before tickets are applied.
- Follow test-driven development. Each behavior gets a failing test before implementation.
- Commit each independently testable task.
- Use `MISE_TRUSTED_CONFIG_PATHS="$PWD" mise run verify` for full verification.
- Use `env -u RK_AGENT` for authenticated operator approval tests that must prove approval is operator-initiated rather than agent-inherited.
- Keep generic offline evidence and dependency scans in every relevant acceptance pass.

---

### Task 1: Offline Product-to-Code Contracts

**Files:**
- Create: `crates/rk-core/contracts/product_to_code/initiative.cue`
- Create: `crates/rk-core/contracts/product_to_code/architecture_research.cue`
- Create: `crates/rk-core/contracts/product_to_code/evidence.cue`
- Create: `crates/rk-core/contracts/product_to_code/ticket_graph.cue`
- Create: `crates/rk-core/contracts/product_to_code/verification_report.cue`
- Create: `crates/rk-core/src/product_to_code/mod.rs`
- Create: `crates/rk-core/src/product_to_code/contracts.rs`
- Modify: `crates/rk-core/src/lib.rs`
- Create: `crates/rk-core/tests/product_to_code_contracts.rs`
- Create fixtures under: `crates/rk-core/tests/fixtures/product_to_code/`

**Interfaces:**
- Produces serde types: `InitiativeContract`, `ArchitectureResearchArtifact`, `GenericEvidence`, `TicketGraph`, `TicketGraphNode`, `TicketGraphEdge`, `VerificationReport`, `AcceptanceCriterionVerification`.
- Contract schemas: `initiative`, `architecture_research`, `evidence`, `ticket_graph`, `verification_report`.
- Evidence kinds: `impact`, `browser_acceptance`, `test_run`, `code_review`, `research_note`, `workflow_result`, `manual_observation`.
- Producer identity is generic: `{producer: {kind, name, version?, invocation?}}`. `kind` must not include a closed enum value requiring Jcode, browser, or GitNexus.

- [ ] **Step 1: Write failing contract round-trip tests**

Implement these exact test methods:

- `test_initiative_contract_deserializes_minimal_fixture`: assert id, title, acceptance criteria, scope, and declared browser applicability round-trip through `serde_json`.
- `test_architecture_research_artifact_requires_decisions_and_open_questions`: deserialize an invalid fixture without decisions or open questions and assert validation fails.
- `test_generic_evidence_accepts_tool_agnostic_impact_payload`: assert an impact evidence fixture with producer kind `external-tool` validates without any GitNexus-specific type.
- `test_browser_acceptance_evidence_is_generic_and_offline`: assert browser acceptance evidence stores URL, scenario, steps, observations, and artifact paths without depending on browser automation crates.
- `test_ticket_graph_fixture_preserves_nodes_edges_and_acceptance_links`: assert graph nodes, dependency edges, and acceptance criterion references survive serde round-trip.
- `test_verification_report_maps_each_acceptance_criterion_to_evidence`: assert report entries reference evidence IDs and criterion IDs exactly.
- `test_contract_modules_do_not_reference_jcode_browser_or_gitnexus_crates`: scan `Cargo.toml` files and contract Rust modules for forbidden runtime dependencies or imports.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cargo test -p rk-core --test product_to_code_contracts
```

Expected: failure because the contract modules and fixtures do not exist.

- [ ] **Step 3: Implement minimal serde contracts and CUE schemas**

Implement Rust serde structs with validation helpers. CUE schemas must be usable offline from the repository and must document required fields, stable IDs, acceptance criterion references, evidence references, and generic producer metadata.

Do not add runtime validation against CUE unless the repo already has an offline CUE validation path. The Rust validation layer is authoritative for tests in this task.

- [ ] **Step 4: Run tests and verify GREEN**

Run the cargo test command above. Expected: all product-to-code contract tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/rk-core/contracts/product_to_code crates/rk-core/src/product_to_code crates/rk-core/src/lib.rs crates/rk-core/tests/product_to_code_contracts.rs crates/rk-core/tests/fixtures/product_to_code
git commit -m "feat: add product-to-code contracts"
```

### Task 2: Architecture Research Artifact Workflow

**Files:**
- Modify: `crates/rk-core/src/product_to_code/mod.rs`
- Create: `crates/rk-core/src/product_to_code/research.rs`
- Modify: `crates/rk-cli/src/main.rs`
- Create: `crates/rk-cli/tests/product_to_code_research.rs`
- Add fixtures under: `crates/rk-cli/tests/fixtures/product_to_code/`
- Modify or create workflow definition: `examples/workflows/research.cue`

**Interfaces:**
- CLI commands:
  - `rk product-to-code research validate --artifact PATH --initiative PATH --json`
  - `rk product-to-code research render --artifact PATH --format json|markdown`
- Workflow output artifact: `ArchitectureResearchArtifact` with `initiative_id`, `researched_files`, `domain_terms`, `architecture_decisions`, `constraints`, `risks`, `open_questions`, `recommended_ticket_graph_path`, and `evidence_ids`.
- Validation requires the research artifact to reference the initiative and at least one concrete file, decision, risk or constraint, and open question or explicit `open_questions_exhausted: true`.

- [ ] **Step 1: Write failing research validation tests**

Implement these exact test methods:

- `test_research_validate_accepts_complete_structured_artifact`: assert valid fixture exits 0 and prints JSON with `valid:true`.
- `test_research_validate_rejects_artifact_for_wrong_initiative`: assert mismatched `initiative_id` exits non-zero and names both IDs.
- `test_research_validate_rejects_empty_researched_files`: assert validation requires at least one repo file path.
- `test_research_validate_rejects_no_decisions_constraints_or_risks`: assert artifact must contain architecture substance, not a prose-only note.
- `test_research_render_markdown_has_decisions_risks_and_open_questions`: assert Markdown contains `## Decisions`, `## Risks`, and `## Open Questions`.
- `test_architecture_research_workflow_declares_structured_artifact_output`: assert workflow CUE definition names the structured artifact path and validation command.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cargo test -p rk-cli product_to_code_research --test product_to_code_research
```

Expected: failure because the command and workflow contract do not exist.

- [ ] **Step 3: Implement research validation and rendering**

Add the CLI subcommands using existing CLI patterns. The command reads local JSON files only and validates them as CUE structured JSON artifacts against the repo-owned convention where tooling exists, then Rust validation remains authoritative for runtime tests. It validates with `ArchitectureResearchArtifact::validate_for_initiative(&initiative)`. Rendering is deterministic and does not mutate RK state.

Update `examples/workflows/research.cue` or add a product-specific CUE workflow beside it so the workflow instructs an agent to produce the structured JSON artifact and run the validation command before reporting completion.

- [ ] **Step 4: Run tests and verify GREEN**

Run the cargo test command above. Expected: all research tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/rk-core/src/product_to_code crates/rk-cli/src/main.rs crates/rk-cli/tests/product_to_code_research.rs crates/rk-cli/tests/fixtures/product_to_code examples/workflows/research.cue
git commit -m "feat: validate architecture research artifacts"
```

### Task 3: Ticket Graph Validate, Dry-Run, and Apply Proposal (Read-Only CLI)

**Files:**
- Create: `crates/rk-core/src/product_to_code/ticket_graph.rs`
- Modify: `crates/rk-core/src/product_to_code/mod.rs`
- Modify: `crates/rk-cli/src/main.rs`
- Create: `crates/rk-cli/tests/product_to_code_ticket_graph.rs`
- Add fixtures under: `crates/rk-cli/tests/fixtures/product_to_code/`

**Interfaces:**
- CLI commands:
  - `rk product-to-code graph validate --graph PATH --initiative PATH --json`
  - `rk product-to-code graph dry-run --graph PATH --initiative PATH --json`
  - `rk product-to-code graph propose-apply --graph PATH --initiative PATH --json`
- New typed Phase 3 `FactoryAction` kind: `ticket_graph.apply`. The CLI submits this typed action to the Phase 2 proposal API and never applies it locally.
- Daemon proposal and executor handlers are implemented in Task 5 after all read-only contracts, research, graph validation, dry-run, and evidence gates land.
- Reuses Phase 2 canonical proposal identity and exact approval semantics.
- Validation output: `{valid, graph_id, errors, warnings, topological_order}`.
- Dry-run output: `{graph_id, creates, updates, dependencies, dispatches, blocked}`.
- Apply proposal output includes `proposal_id`, canonical action payload, human display command, daemon approval instructions, and `authority_boundary`.

- [ ] **Step 1: Write failing graph validation tests**

Implement these exact test methods:

- `test_graph_validate_accepts_acyclic_graph_with_existing_acceptance_refs`: assert a valid fixture returns topological order.
- `test_graph_validate_rejects_missing_dependency_node`: graph edge to unknown node exits non-zero and names the missing ID.
- `test_graph_validate_rejects_dependency_cycle`: cyclic graph exits non-zero and returns the cycle path.
- `test_graph_validate_rejects_unknown_acceptance_criterion_ref`: graph node referencing absent criterion exits non-zero and names the criterion.
- `test_graph_dry_run_is_read_only_and_lists_exact_mutations`: use a fake repository store or temp state and assert no tickets are created.
- `test_graph_propose_apply_uses_phase2_daemon_canonical_proposal`: assert proposal ID equals SHA-256 of canonical action payload and output instructs operator approval through the daemon.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cargo test -p rk-cli product_to_code_ticket_graph --test product_to_code_ticket_graph
```

Expected: graph command tests fail because the commands and graph engine do not exist.

- [ ] **Step 3: Implement validation, dry-run, and proposal generation**

Implement deterministic graph validation with explicit missing-node, missing-criterion, and cycle errors. Use a topological sort that returns a stable order.

`dry-run` must call validation first and must not write tickets, edges, workflow runs, or tuple data.

Graph application semantics are specified here but implemented in Task 5 after read-only validation and evidence gates exist: create tickets in topological order, record a durable mapping from contract graph node IDs to minted `TKT-...` IDs, then create dependency edges using the minted ticket IDs. Distinguish graph node IDs from TKT IDs in every schema, log, and JSON response. Prefer a single atomic transaction; if the current store cannot provide one, persist an execution ID and idempotency ledger so retries resume without duplicate tickets or edges.

`propose-apply` must build one canonical Phase 2 daemon proposal describing the graph application. There is no local `graph apply` command and no `--approved-id` CLI mutation path.

- [ ] **Step 4: Run tests and verify GREEN**

Run the cargo test command above. Expected: all ticket graph tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/rk-core/src/product_to_code crates/rk-cli/src/main.rs crates/rk-cli/tests/product_to_code_ticket_graph.rs crates/rk-cli/tests/fixtures/product_to_code
git commit -m "feat: validate product ticket graphs"
```

### Task 4: Generic Evidence Gates for Dispatch and Delivery

**Files:**
- Create: `crates/rk-core/src/product_to_code/evidence.rs`
- Modify: `crates/rk-core/src/product_to_code/mod.rs`
- Modify: `crates/rk-cli/src/main.rs`
- Create: `crates/rk-cli/tests/product_to_code_evidence_gates.rs`
- Add fixtures under: `crates/rk-cli/tests/fixtures/product_to_code/`
- Modify existing CUE workflow dispatch gate files if present under: `examples/workflows/`

**Interfaces:**
- CLI commands:
  - `rk product-to-code evidence validate --evidence PATH --initiative PATH --json`
  - `rk product-to-code dispatch-gate --ticket PATH --evidence-dir PATH --json`
  - `rk product-to-code delivery-gate --ticket PATH --verification-report PATH --evidence-dir PATH --json`
- Dispatch gate requires at least one valid `impact` evidence item that covers the ticket or its feature set.
- Delivery gate requires browser acceptance evidence when `browser_acceptance_applicable:true` appears on the initiative, ticket, or acceptance criterion.
- Evidence producer names are informational and must not affect acceptance except for schema and declared coverage.

- [ ] **Step 1: Write failing evidence gate tests**

Implement these exact test methods:

- `test_dispatch_gate_accepts_generic_impact_evidence_from_jcode`: fixture producer name `Jcode` passes through the generic evidence contract.
- `test_dispatch_gate_accepts_generic_impact_evidence_from_gitnexus_without_dependency`: fixture producer name `GitNexus` passes while Cargo metadata contains no GitNexus crate.
- `test_dispatch_gate_rejects_missing_impact_evidence`: ticket with no covering impact evidence exits non-zero and names the dispatch gate.
- `test_dispatch_gate_rejects_stale_or_wrong_ticket_coverage`: impact evidence for another ticket or older artifact hash is rejected.
- `test_delivery_gate_requires_browser_acceptance_when_applicable`: applicable browser criterion without browser evidence exits non-zero.
- `test_delivery_gate_does_not_require_browser_when_not_applicable`: non-browser ticket can pass with test and review evidence only.
- `test_delivery_gate_rejects_browser_evidence_without_observations`: browser evidence must include scenario, steps, observations, and artifact paths.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cargo test -p rk-cli product_to_code_evidence_gates --test product_to_code_evidence_gates
```

Expected: evidence gate tests fail because the commands and validators do not exist.

- [ ] **Step 3: Implement generic evidence validation and gates**

Implement validators over offline JSON files. For impact evidence, validate `covers.ticket_ids`, `covers.files_or_symbols`, `artifact_hash`, timestamp, and producer metadata. Do not inspect or call Jcode, browser tools, GitNexus, or network services.

Delivery gate must inspect the initiative/ticket/criterion applicability flags and require at least one `browser_acceptance` evidence item that maps to each applicable criterion.

- [ ] **Step 4: Run tests and verify GREEN**

Run the cargo test command above. Expected: all evidence gate tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/rk-core/src/product_to_code crates/rk-cli/src/main.rs crates/rk-cli/tests/product_to_code_evidence_gates.rs crates/rk-cli/tests/fixtures/product_to_code examples/workflows
git commit -m "feat: gate dispatch and delivery on evidence"
```

### Task 5: Daemon-Approved Ticket Graph Apply

**Files:**
- Modify: `crates/rk-daemon/src/` proposal/action modules that own Phase 2 generic daemon proposals
- Modify: `crates/rk-core/src/product_to_code/ticket_graph.rs`
- Modify: `crates/rk-core/src/product_to_code/mod.rs`
- Create: `crates/rk-daemon/tests/product_to_code_ticket_graph_apply.rs`
- Add fixtures under: `crates/rk-daemon/tests/fixtures/product_to_code/`

**Interfaces:**
- Typed Phase 3 `FactoryAction` variant: `ticket_graph.apply`.
- Daemon proposal handler validates canonical payload, status, digest, CAS preconditions, graph validity, graph-node-id uniqueness, and absence of graph-node-id/TKT ID confusion.
- Daemon executor handler requires authenticated operator approval, then rechecks proposal status, digest, and CAS before mutation.
- Execution output includes `execution_id`, `graph_id`, `graph_node_to_ticket_id`, `created_ticket_ids`, `created_dependency_edges`, `idempotent_replay`, and `status`.

- [ ] **Step 1: Write failing daemon mutation safety tests**

Implement these exact daemon integration test methods:

- `test_daemon_ticket_graph_apply_requires_authenticated_operator_approval`: run approval-sensitive path with `env -u RK_AGENT` and assert agent-inherited context cannot apply.
- `test_daemon_ticket_graph_apply_rejects_unapproved_status`: proposed but unapproved action creates no tickets.
- `test_daemon_ticket_graph_apply_rejects_digest_mismatch`: changed canonical payload creates no tickets and reports expected vs actual digest.
- `test_daemon_ticket_graph_apply_rejects_cas_mismatch`: stale repository or ticket-store CAS creates no tickets.
- `test_daemon_ticket_graph_apply_creates_tickets_topologically_then_edges`: valid approved action mints `TKT-...` IDs in topological order, records graph-node-id to TKT ID mapping, then creates dependency edges between minted IDs.
- `test_daemon_ticket_graph_apply_idempotent_replay_does_not_duplicate_tickets_or_edges`: repeated executor call with the same `execution_id` returns the persisted result.
- `test_daemon_ticket_graph_apply_distinguishes_graph_node_ids_from_tkt_ids`: fixture with graph node ID shaped like `TKT-...` is rejected or normalized according to the contract.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cargo test -p rk-daemon product_to_code_ticket_graph_apply --test product_to_code_ticket_graph_apply
```

Expected: daemon integration tests fail because the typed action variant and executor do not exist.

- [ ] **Step 3: Implement daemon proposal and executor handlers**

Add the `ticket_graph.apply` `FactoryAction` variant and wire it into the completed Phase 2 generic daemon proposal registry. Do not add a CLI apply path. The executor must create tickets topologically, persist the graph-node-id to minted `TKT-...` ID mapping, create dependency edges after ticket creation, and use either an atomic store transaction or a durable execution idempotency ledger.

- [ ] **Step 4: Run tests and verify GREEN**

Run the daemon cargo test above. Expected: all graph apply daemon tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/rk-daemon crates/rk-core/src/product_to_code crates/rk-daemon/tests/product_to_code_ticket_graph_apply.rs crates/rk-daemon/tests/fixtures/product_to_code
git commit -m "feat: apply ticket graphs through daemon approval"
```

### Task 6: Product-to-Code Workflow Composition

**Files:**
- Create or modify: `examples/workflows/product-to-code.cue`
- Reuse existing: `examples/workflows/implement-featureset.cue`
- Modify: `crates/rk-cli/src/main.rs`
- Modify: `crates/rk-daemon/src/` proposal/action modules that own Phase 2 generic daemon proposals
- Create: `crates/rk-cli/tests/product_to_code_workflow.rs`
- Create: `crates/rk-daemon/tests/product_to_code_dispatch.rs`
- Add fixtures under: `crates/rk-cli/tests/fixtures/product_to_code/`
- Add fixtures under: `crates/rk-daemon/tests/fixtures/product_to_code/`

**Interfaces:**
- CLI commands:
  - `rk product-to-code workflow propose --initiative PATH --research PATH --graph PATH --evidence-dir PATH --json`
- New typed Phase 3 `FactoryAction` kind: `product_to_code.dispatch`. The CLI proposes this typed action only; it does not dispatch workflows locally.
- New daemon proposal handler: validates the canonical dispatch payload, stores status/digest/CAS preconditions, and requires authenticated operator approval.
- New daemon executor handler: rechecks approval status, digest, and CAS before dispatching workflows.
- Workflow composition order:
  1. validate initiative contract;
  2. run or validate architecture research artifact;
  3. validate ticket graph;
  4. propose approved graph apply;
  5. after daemon-approved graph apply has minted `TKT-...` IDs, dispatch `implement-featureset` only for graph nodes whose dispatch gate passes;
  6. block delivery until independent verification report passes.
- Reuses Phase 2 canonical daemon action/proposal approval for workflow dispatch. Dispatch uses `rk workflow run implement-featureset --param taskId=TKT-... --param taskDescription="..."` semantics from `examples/workflows/implement-featureset.cue`.

- [ ] **Step 1: Write failing workflow composition tests**

Implement these exact test methods:

- `test_workflow_propose_validates_research_before_graph_apply`: invalid research prevents graph proposal and names the research validation error.
- `test_workflow_propose_rejects_graph_with_cycle_before_dispatch`: cyclic graph prevents any workflow dispatch proposal.
- `test_workflow_propose_blocks_nodes_without_impact_evidence`: graph node without generic impact evidence is listed in `blocked`.
- `test_workflow_propose_includes_implement_featureset_dispatches_for_unblocked_nodes`: approved graph nodes produce canonical dispatch actions for `implement-featureset`.
- `test_daemon_workflow_dispatch_requires_exact_phase2_operator_approval`: wrong approval status, digest, or CAS exits non-zero and dispatches nothing in daemon integration tests.
- `test_daemon_workflow_dispatch_uses_existing_phase2_proposal_validator`: assert the daemon code path calls or wraps the Phase 2 proposal validator rather than duplicating digest logic.
- `test_product_to_code_workflow_definition_lists_research_graph_apply_implement_and_verify_steps`: assert workflow CUE contains the required composition steps.
- `test_cli_workflow_propose_is_thin_wiring_only`: assert CLI only emits/submits the typed proposal and has no workflow mutation path.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cargo test -p rk-cli product_to_code_workflow --test product_to_code_workflow
cargo test -p rk-daemon product_to_code_dispatch --test product_to_code_dispatch
```

Expected: CLI workflow proposal tests and daemon dispatch integration tests fail because the command, typed action, daemon handler, and workflow definition do not exist.

- [ ] **Step 3: Implement workflow proposal and approved dispatch**

Build a product-to-code workflow proposal from already validated local artifacts. The proposal must reference the prior approved graph apply execution and its graph-node-id to minted TKT ID mapping. It must include `implement-featureset` dispatch actions in canonical order, show blocked graph node IDs separately from minted TKT IDs, and report missing evidence without mutating state.

There is no local `workflow dispatch --approved-id` mutation path. The daemon `product_to_code.dispatch` executor must reuse the Phase 2 canonical proposal validator and execute only validated actions after authenticated operator approval, status, digest, and CAS checks.

- [ ] **Step 4: Run tests and verify GREEN**

Run the cargo test commands above. Expected: all CLI workflow and daemon dispatch tests pass.

- [ ] **Step 5: Commit**

```bash
git add examples/workflows/product-to-code.cue crates/rk-cli/src/main.rs crates/rk-cli/tests/product_to_code_workflow.rs crates/rk-cli/tests/fixtures/product_to_code crates/rk-daemon crates/rk-daemon/tests/product_to_code_dispatch.rs crates/rk-daemon/tests/fixtures/product_to_code
git commit -m "feat: compose product-to-code workflow"
```

### Task 7: Independent Verification Report

**Files:**
- Create: `crates/rk-core/src/product_to_code/verification.rs`
- Modify: `crates/rk-core/src/product_to_code/mod.rs`
- Modify: `crates/rk-cli/src/main.rs`
- Create: `crates/rk-cli/tests/product_to_code_verification.rs`
- Add fixtures under: `crates/rk-cli/tests/fixtures/product_to_code/`
- Create or modify workflow definition: `examples/workflows/independent-verifier.cue`

**Interfaces:**
- CLI commands:
  - `rk product-to-code verify-report validate --report PATH --initiative PATH --evidence-dir PATH --json`
  - `rk product-to-code verify-report render --report PATH --format json|markdown`
- Report fields: `report_id`, `initiative_id`, `verifier`, `scope`, `criteria`, `evidence`, `gaps`, `recommendation`.
- Each acceptance criterion maps to one of: `satisfied`, `partially_satisfied`, `not_satisfied`, `not_applicable`.
- Each non-`not_applicable` criterion must reference at least one evidence ID or one explicit gap.

- [ ] **Step 1: Write failing verification report tests**

Implement these exact test methods:

- `test_verify_report_accepts_complete_mapping_from_criteria_to_evidence`: every criterion has status and evidence or gap.
- `test_verify_report_rejects_missing_acceptance_criterion`: initiative criterion absent from report exits non-zero and names the missing ID.
- `test_verify_report_rejects_unknown_evidence_id`: report evidence ID not found in evidence dir exits non-zero.
- `test_verify_report_requires_gap_for_unsatisfied_without_evidence`: unsatisfied criterion with neither evidence nor gap is rejected.
- `test_verify_report_requires_browser_evidence_for_browser_applicable_criterion`: applicable browser criterion cannot be satisfied by test-run evidence alone.
- `test_verify_report_render_markdown_groups_satisfied_gaps_and_recommendation`: Markdown contains `## Satisfied`, `## Gaps`, and `## Recommendation`.
- `test_independent_verifier_workflow_declares_no_implementation_authority`: workflow definition instructs verifier to report evidence and gaps, not modify code.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cargo test -p rk-cli product_to_code_verification --test product_to_code_verification
```

Expected: verification report tests fail because the command and validator do not exist.

- [ ] **Step 3: Implement verification report validation and rendering**

Validate complete coverage of initiative acceptance criteria, evidence ID existence, browser evidence requirements, and explicit gaps. Render JSON and Markdown deterministically. Keep the independent verifier workflow read-only with no code mutation authority.

- [ ] **Step 4: Run tests and verify GREEN**

Run the cargo test command above. Expected: all verification report tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/rk-core/src/product_to_code crates/rk-cli/src/main.rs crates/rk-cli/tests/product_to_code_verification.rs crates/rk-cli/tests/fixtures/product_to_code examples/workflows/independent-verifier.cue
git commit -m "feat: validate independent verification reports"
```

### Task 8: Documentation and End-to-End Acceptance

**Files:**
- Create: `docs/product-to-code.md`
- Modify: `docs/factory-foreman.md`
- Modify: `README.md`
- Create: `crates/rk-cli/tests/product_to_code_e2e.rs`
- Add fixtures under: `crates/rk-cli/tests/fixtures/product_to_code/e2e/`

**Interfaces:**
- Documents initiative contract, research artifact, evidence contract, graph validation and apply approval, workflow dispatch approval, delivery gate, and independent verifier report.
- End-to-end fixture covers research validation, graph validation, graph dry-run, apply proposal generation, dispatch proposal generation, evidence gates, and verification report validation.

- [ ] **Step 1: Write failing end-to-end acceptance tests**

Implement these exact test methods:

- `test_e2e_product_to_code_happy_path_produces_apply_and_dispatch_proposals`: valid offline fixtures produce graph apply and workflow dispatch proposals without applying them.
- `test_e2e_rejects_cycle_before_any_proposal`: cyclic graph fixture produces no apply or dispatch proposal.
- `test_e2e_rejects_missing_dependency_before_any_proposal`: missing dependency fixture produces no apply or dispatch proposal.
- `test_e2e_blocks_dispatch_without_impact_evidence`: graph validates but dispatch proposal lists blocked nodes and no implement action for them.
- `test_e2e_delivery_gate_requires_browser_acceptance_when_applicable`: fixture without browser evidence fails delivery gate.
- `test_e2e_independent_report_maps_all_acceptance_criteria`: valid report fixture maps every criterion to evidence or gap.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cargo test -p rk-cli product_to_code_e2e --test product_to_code_e2e
```

Expected: end-to-end tests fail until Tasks 1-7 are integrated.

- [ ] **Step 3: Add documentation**

Document the product-to-code lifecycle:

1. define initiative;
2. produce validated architecture research artifact;
3. validate ticket graph;
4. dry-run graph;
5. propose daemon graph apply and wait for authenticated operator approval;
6. daemon applies graph, persists execution idempotency, and records graph-node-id to minted `TKT-...` ID mapping;
7. require generic impact evidence before implementation dispatch;
8. propose `product_to_code.dispatch` and wait for authenticated operator approval;
9. daemon dispatches `rk workflow run implement-featureset --param taskId=TKT-... --param taskDescription="..."` for unblocked minted tickets;
10. collect test, review, workflow, and browser acceptance evidence when applicable;
11. require independent verifier report mapping acceptance criteria to evidence;
12. deliver only when gates pass or documented gaps are accepted by the user.

Clearly label limitations:

- RK has no runtime dependency on Jcode, browser automation, or GitNexus;
- impact evidence is accepted through a generic offline evidence contract;
- browser acceptance evidence is required only when declared applicable and is stored as offline evidence;
- proposal approval remains the Phase 2 canonical exact approval boundary, executed by the daemon with authenticated operator approval, status, digest, and CAS checks;
- CUE schemas are repo-owned contract documentation and may be validated offline where CUE tooling is available;
- independent verifier reports establish evidence mapping, not absolute proof of correctness.

- [ ] **Step 4: Run full verification**

```bash
cargo test -p rk-core --test product_to_code_contracts
cargo test -p rk-cli product_to_code_research --test product_to_code_research
cargo test -p rk-cli product_to_code_ticket_graph --test product_to_code_ticket_graph
cargo test -p rk-cli product_to_code_evidence_gates --test product_to_code_evidence_gates
cargo test -p rk-daemon product_to_code_ticket_graph_apply --test product_to_code_ticket_graph_apply
cargo test -p rk-cli product_to_code_workflow --test product_to_code_workflow
cargo test -p rk-daemon product_to_code_dispatch --test product_to_code_dispatch
cargo test -p rk-cli product_to_code_verification --test product_to_code_verification
cargo test -p rk-cli product_to_code_e2e --test product_to_code_e2e
MISE_TRUSTED_CONFIG_PATHS="$PWD" mise run verify
```

- [ ] **Step 5: Review the diff**

Check:

```bash
git diff --check
git status --short
git diff --stat origin/main..HEAD
```

An independent reviewer must verify authority boundaries, absence of Jcode/browser/GitNexus runtime dependencies, graph cycle and missing-dependency rejection, exact Phase 2 daemon approval reuse, authenticated operator approval, CAS/status/digest verification, graph-node-id to TKT ID mapping, browser evidence applicability, and acceptance-criteria-to-evidence traceability.

- [ ] **Step 6: Commit documentation and acceptance tests**

```bash
git add docs/product-to-code.md docs/factory-foreman.md README.md crates/rk-cli/tests/product_to_code_e2e.rs crates/rk-cli/tests/fixtures/product_to_code/e2e
git commit -m "docs: describe product-to-code workflow"
```

# Product-to-Code

The product-to-code lifecycle turns a product initiative into implemented,
independently verified code through a sequence of **offline, contract-validated
artifacts** and **daemon-executed, operator-approved actions**. Every mutating
step crosses the Phase 2 canonical approval boundary: the CLI prepares and
forwards exact typed envelopes, while the daemon alone applies changes after an
authenticated operator approval with status, digest, and compare-and-swap (CAS)
checks.

## Lifecycle

1. **Define the initiative.** Author an initiative artifact describing the
   product goal, acceptance criteria, and scope. It is validated offline against
   the initiative contract.
2. **Produce a validated architecture research artifact.** Capture the design
   research (components, interfaces, risks) and validate it against the research
   contract before any graph work.
3. **Validate the ticket graph.** The ticket graph decomposes the initiative
   into nodes with explicit dependencies. Validation rejects cycles and missing
   dependencies before any proposal is generated.
4. **Dry-run the graph.** Preview the mint plan (which nodes become `TKT-...`
   tickets, in what order) without mutating anything.
5. **Propose the daemon graph apply.** The CLI submits a canonical
   `ticket_graph.apply` proposal and emits a saveable execution envelope. No
   local mutation occurs.
6. **Daemon applies the graph.** On approval, the daemon mints tickets,
   persists execution idempotency, and records the graph-node-id to minted
   `TKT-...` ID mapping.
7. **Require impact evidence before dispatch.** Each node must carry generic,
   offline impact evidence before its implementation can be dispatched. Nodes
   without impact evidence are **blocked**, not dispatched.
8. **Propose `product_to_code.dispatch`.** The CLI submits a canonical dispatch
   proposal referencing the approved graph-apply execution and emits another
   saveable execution envelope.
9. **Daemon dispatches implementation.** On approval, the daemon runs
   `rk workflow run implement-featureset --param taskId=TKT-... --param
   taskDescription="..."` for **unblocked** minted tickets only.
10. **Collect acceptance evidence.** Gather test, review, workflow, and — when
    declared applicable — browser acceptance evidence, all stored as offline
    evidence.
11. **Require an independent verifier report.** An independent verifier maps
    every acceptance criterion to evidence or to an explicit documented gap. The
    verifier declares no implementation authority.
12. **Deliver only when gates pass** or when documented gaps are explicitly
    accepted by the user.

## Commands

```bash
# Validate the offline research artifact against the research contract.
rk product-to-code research validate \
  --artifact research.json \
  --initiative initiative.json

# Validate the ticket graph (rejects cycles and missing dependencies).
rk product-to-code graph validate \
  --graph graph.json \
  --initiative initiative.json

# Dry-run the graph to preview the mint plan.
rk product-to-code graph dry-run \
  --graph graph.json \
  --initiative initiative.json \
  --repo <registered-name-or-path>

# Propose the daemon graph apply and save its exact execution envelope.
rk --json product-to-code graph propose-apply \
  --graph graph.json \
  --initiative initiative.json \
  --repo <registered-name-or-path> \
  > graph-proposal.json

# Inspect graph-proposal.json, then approve and execute the saved envelope.
rk --json factory approve --proposal-file graph-proposal.json
rk --json factory execute-action --proposal-file graph-proposal.json \
  > graph-result.json

# Propose implementation dispatch for unblocked minted tickets and save it.
rk --json product-to-code workflow propose \
  --initiative initiative.json \
  --research research.json \
  --graph graph.json \
  --evidence-dir evidence/ \
  --repo <registered-name-or-path> \
  > dispatch-proposal.json

# Inspect dispatch-proposal.json, then approve and execute the saved envelope.
rk --json factory approve --proposal-file dispatch-proposal.json
rk --json factory execute-action --proposal-file dispatch-proposal.json \
  > dispatch-result.json

# Validate an independent verifier report (criterion -> evidence or gap).
rk product-to-code verify-report validate \
  --report report.json \
  --initiative initiative.json \
  --evidence-dir evidence/
rk product-to-code verify-report render --report report.json
```

The proposal files contain the daemon proposal metadata, canonical action,
digest, and the original typed `execution_action`. The operator should inspect
the file before approving it. Editing the file does not create authority: the
daemon reloads its persisted proposal, resolves repository scope and
preconditions again, recomputes the canonical digest, and rejects mismatched,
expired, consumed, or caller-incompatible approval and execution requests.

Example initiative, research, graph, evidence, ticket, and verification-report
artifacts live under `crates/rk-cli/tests/fixtures/product_to_code/`.

## Contracts

CUE schemas under `crates/rk-core/contracts/product_to_code/` are **repo-owned
contract documentation**. They may be validated offline where CUE tooling is
available, but RK does not require CUE at runtime. The Rust contract types in
`crates/rk-core/src/product_to_code/` are the authoritative validators.

## Limitations and safety boundaries

- **No runtime dependency on Jcode, browser automation, or GitNexus.** RK
  performs no network or SaaS calls as part of this lifecycle.
- **Impact evidence is generic and offline.** It is accepted through a generic
  offline evidence contract, never by inspecting a live system.
- **Browser acceptance evidence is conditional.** It is required only when the
  initiative declares it applicable, and it is stored as offline evidence.
- **Proposal approval is the Phase 2 canonical boundary.** Every mutating step
  is executed by the daemon under an authenticated operator approval with
  status, digest, and CAS verification. The CLI only saves and forwards the
  exact typed proposal envelope.
- **CUE schemas are documentation.** They describe contracts and may be
  validated offline; they are not a runtime dependency.
- **Independent verifier reports establish evidence mapping, not proof.** A
  passing report maps acceptance criteria to evidence. It does not assert
  absolute correctness, and the verifier holds no implementation authority.

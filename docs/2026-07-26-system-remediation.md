# Rat Kingdom system remediation

Date: 2026-07-26

This document records the whole-system review of the Rust swarm orchestrator and
the remediation plan. It is intentionally implementation-oriented: each item
names the invariant that should hold, the current failure mode, and the change
that proves the invariant in code or tests.

## System model

`rk` is a long-lived daemon exposing an NDJSON protocol over a Unix socket. The
daemon owns a SQLite-backed tuplespace, agent/worktree lifecycle, CUE workflow
execution, ticket state, scheduler/reactor loops, and optional signed git-notes
replication between castles. Harnesses run in Git worktrees and report events
back through the tuplespace.

The remediation assumes the normal unattended case: a workflow or agent may be
buggy or compromised, local processes may be less trusted than the operator, and
sync peers may be authenticated but not universally authorized. Operator actions
remain available, but destructive actions must have an explicit authority path.

## Findings and implementation plan

### R1 — IPC and tuple authorization

**Finding.** The socket inherited its mode from the process umask and the RPC
server accepted every method without authentication. `space.out` accepted an
arbitrary tuple instance, lifecycle, category, and payload. A signed sync record
authenticated its originating castle but did not authorize the tuple operation.

**Remediation.** Create a per-layout 256-bit token with mode `0600`, require it
on every request, identify the caller as `operator` or `RK_AGENT`, deny agent
access to operator/destructive control methods, and restrict agent tuple writes
to the caller's instance and agent-safe lifecycle/categories. Keep sync
provenance separate from local authorization and add explicit peer/category
checks before importing remote tuples.

### R2 — Harness trust defaults

**Finding.** Spawn defaulted to Claude `bypassPermissions` and Codex
`danger-full-access`. A Git worktree is branch isolation, not an operating-system
sandbox.

**Remediation.** Default profiles to `workspace-write`; accept only a documented
set of permission modes; make full host access explicit. Preserve a narrow
completion path for agents whose sandbox cannot reach the daemon.

### R3 — Workflow capability and approval enforcement

**Finding.** The CUE schema documented that `land`/`open_pr` should only follow an
approval, but the executor did not enforce that convention. Raw commands were
also allowed unless an opt-in policy was enabled. Definitions could be selected
from repository-local or arbitrary paths.

**Remediation.** Validate workflow control flow before execution: destructive
steps require an approval gate or a trusted operator-started workflow, targets
must be allowlisted, and raw commands are rejected by default unless a named
check policy is explicitly disabled. Record the definition path and digest in
the instance so a resumed workflow cannot silently change meaning.

### R4 — Sync durability and single-flight

**Finding.** The sync cursor advanced before the notes append completed. The
periodic sync loop and `sync.now` RPC could run the same cycle concurrently.
Cursor/presence files were also written non-atomically.

**Remediation.** Append the durable outbox first, atomically persist the cursor
after success, persist presence atomically, and serialize all sync cycles. Add a
regression test that injects an append failure and verifies the tuple is retried.

### R5 — Reactor delivery semantics

**Finding.** The reactor advanced its cursor even when a trigger could not find a
repo or workflow dispatch failed. This made transient failures permanently
invisible.

**Remediation.** Distinguish successful delivery, intentional permanent skip,
and retryable failure. Persist retry state with backoff and only acknowledge a
tuple/trigger pair after the workflow instance is durably created.

### R6 — Tuplespace concurrency and resource bounds

**Finding.** `out_if_new` checked existence and inserted in separate critical
sections. Timed-out blocking readers remained until a future matching write.
NDJSON reads and broad scans had no useful size bounds.

**Remediation.** Make replication insertion atomic, remove timed-out waiters by
identity, cap request/response frames, and add bounded/paginated scan behavior
where an operator or peer can request large data.

### R7 — Restart and side-effect recovery

**Finding.** Workflow instance JSON was written directly and unreadable files
were skipped. Agent spawn created a worktree and launched a process before its
registry record was durable, leaving untracked resources after a crash.

**Remediation.** Use atomic versioned instance writes and preserve corrupt files
as explicit recovery failures. Journal agent allocation before side effects and
reconcile `Spawning` records/worktrees on startup.

### R8 — Blocking work on async request paths

**Finding.** Agent spawn, dismiss, and land performed synchronous Git/filesystem
work inside Tokio request tasks.

**Remediation.** Move blocking Git and filesystem operations behind a dedicated
blocking boundary, keeping merge-queue serialization while the operation runs.

### R9 — Git target safety

**Finding.** If a live target checkout was dirty and `git merge --ff-only`
failed, the code fell back to moving the branch ref directly, leaving the
checkout stale. Workflow land targets were not validated.

**Remediation.** Refuse ref-only advancement when the target is checked out;
surface a blocked merge instead. Validate branch names and configured target
allowlists before any merge or push.

### R10 — Multi-castle ticket identity and state transitions

**Finding.** Ticket IDs were allocated by local maximum (`TKT-1`, `TKT-2`), so
partitioned castles could create the same identity. Ticket updates validated
status names but not legal transitions.

**Remediation.** Use globally unique ticket identifiers while retaining a human
display number, resolve replicated conflicts deterministically, and enforce the
ticket state machine at the daemon boundary.

### R11 — Numeric and schema hardening

**Finding.** TTL/duration conversions used unchecked casts or multiplication.
SQLite migration errors were swallowed, and Clippy was not clean.

**Remediation.** Use checked conversions with bounded inputs, make migrations
versioned and fail loudly, and make `cargo clippy --workspace --all-targets
-- -D warnings` part of the verification gate.

## Verification requirements

The remediation is complete only when:

1. `cargo fmt --all -- --check` passes.
2. `cargo clippy --workspace --all-targets -- -D warnings` passes.
3. `cargo test --workspace` passes, including regression tests for each repaired
   concurrency, authorization, recovery, and workflow-control invariant.
4. The daemon/client integration tests prove authenticated operator and agent
   paths separately.
5. The final commits are pushed to `origin/main`, with this document describing
   the shipped behavior rather than an aspirational design.

## Newly discovered baseline issues

The initial baseline also exposed a stale `quiet_night` fake harness: after the
completion contract changed, the fake emits a successful result without calling
`rk_done`. This is tracked as a test migration in the first implementation
slice. The initial strict Clippy run also found two `rk-space` lints; both are
included in R11.

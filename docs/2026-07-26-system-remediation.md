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

## Shipped implementation status

The review findings are implemented on `main` as of 2026-07-26. The work was
split into regular, pushed slices:

- `de689fe` authenticates daemon clients, creates private root/agent tokens,
  enforces agent-scoped tuple writes, and bounds NDJSON frames.
- `7a52f6a` makes landing approval and named-check policy fail closed, and
  changes Claude/Codex harness defaults to workspace-scoped permissions.
- `e5cc703` serializes sync, makes cursor/presence writes durable and atomic,
  retries reactor delivery failures, and fixes tuplespace waiter/replication
  races.
- `bec8252` makes workflow snapshots atomic, turns corrupt snapshots into
  durable obstacles, and journals agent spawn before worktree/process side
  effects.
- The final remediation slice moves Git/filesystem lifecycle work behind
  `spawn_blocking`, validates Git refs, hardens ticket identity/transitions,
  protects checked-out targets from ref-only advancement, adds definition
  digests for restart recovery, forces the socket to `0600`, bounds TTL and
  scheduler inputs, and makes SQLite migrations fail loudly.
- The audit-closure slice bounds ranked and inbox reads in SQLite, makes tuple
  payload matching literal and row decoding fail closed, confines workflow
  checks to their worktree with capped child output, isolates inbox Git checks
  from Tokio workers, and narrows agent-authored event capabilities.

The implementation also added regression coverage for authenticated agent
access, ungated landing, missing-repository reactor retry, atomic replication,
timed-out waiter cleanup, workflow persistence corruption, dirty-target Git
merges, globally unique ticket IDs, and closed-ticket recovery.

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
identity, cap request/response frames, and bound RPC scans to 10,000 tuples
with an explicit `truncated` result flag.

### R7 — Restart and side-effect recovery

**Finding.** Workflow instance JSON was written directly and unreadable files
were skipped. Agent spawn created a worktree and launched a process before its
registry record was durable, leaving untracked resources after a crash.

**Remediation.** Use atomic versioned instance writes and preserve corrupt files
as explicit recovery failures. Journal agent allocation before side effects and
reconcile `Spawning` records/worktrees on startup. Persist a SHA-256 definition
digest and refuse to resume an instance against changed workflow bytes.

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
fail loudly, and make `cargo clippy --workspace --all-targets -- -D warnings`
part of the verification gate. RPC trail TTLs are capped at one year and
scheduler catch-up at seven days.

## Verification requirements

The remediation is complete only when:

1. `git diff --check` passes; the repository's existing unrelated workspace
   formatting drift is recorded below rather than rewritten wholesale.
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
included in R11. The initial workspace formatting check still reports
pre-existing unrelated drift (for example in `crates/rk-cli/src/observe.rs`);
the remediation deliberately avoids a workspace-wide formatting rewrite.

## Net-new usability and hardening findings

The implementation pass exposed a few issues not visible in the initial
read-only survey:

1. A socket token alone did not guarantee a private socket file, so bind now
   explicitly sets Unix mode `0600`.
2. Ticket IDs could not remain locally sequential and globally collision-free;
   generated IDs are now `TKT-<ULID>`, and CLI/README examples use returned IDs
   instead of implying `TKT-1` will exist.
3. Ordinary ticket updates could skip backward or terminal transitions; the
   state machine now requires the explicit reopen path used by `rk revert`.
4. Restarted workflows could reload edited definitions; snapshots now carry a
   content digest and fail closed on a mismatch.
5. Async RPC handlers still had synchronous CUE, Git, registry, and lifecycle
   work; the blocking paths are now isolated from Tokio request workers.
6. Daemon-backed CLI integration tests used a one-second startup window that
   was too short under workspace-wide parallel load; the tests now allow thirty
   seconds and report the same socket contract.
7. A frame cap alone still allowed `space.scan` to materialize an unbounded
   SQLite result before serialization; RPC scans now cap materialization and
   disclose truncation.
8. `rk prime` and the dependency examples still taught numeric ticket ids after
   ticket creation switched to ULIDs; operator guidance now uses the returned
   `TKT-<id>` everywhere.
9. Ranked scans passed a limit only to Rust result truncation, so a broad
   `--hot` query still materialized the whole table; ranking now happens in
   SQLite before the bounded result is decoded.
10. SQLite `LIKE` is ASCII case-insensitive while `Pattern::matches` is not, so
    a payload search could route a workflow on a tuple the waiter predicate
    would reject; storage now uses a literal, case-sensitive substring query.
11. Agent callers could write `task` tuples through `space.out` or mutate
    arbitrary ticket status through the RPC; task creation remains available,
    while task tuples and ticket mutation/dependency methods are operator-only.
12. `inbox.list` still performed local Git branch checks on a Tokio request
    worker; the branch-resolution portion now runs behind `spawn_blocking`, and
    newest-first source histories plus a response cap prevent old event buildup
    from starving the RPC frame.
13. Workflow `run` steps accepted absolute or symlinked `cwd` values outside an
    agent worktree and buffered unlimited command output; cwd resolution now
    canonicalizes and confines the directory, while each output stream is
    capped and overflow/timeout kills the child.
14. Malformed SQLite tuple rows silently became default ids, `Event` tuples,
    `null` payloads, or current timestamps; row decoding now fails closed so
    persisted corruption cannot silently alter coordination semantics.
15. Agent-authenticated callers could emit arbitrary `Event` identities and
    forge workflow lifecycle signals in payloads; agents now have an explicit
    `task_done` event capability tied to their authenticated identity, while
    daemon lifecycle events remain daemon-owned.

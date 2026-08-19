# TKT-01M0BVWJ4JJE8EQATANMWZNTC2: hot_scan ENOENT under shared-target-dir contention

## Symptom

A full `mise run verify` (env stripped per convention) failed at
`crates/rk-daemon/tests/hot_scan.rs`:

```
error: test failed... Caused by: could not execute process
.../hot_scan-<hash> (never executed) Caused by: No such file or
directory (os error 2)
```

The same test passes cleanly in isolation
(`cargo test -p rk-daemon --test hot_scan`), and the failure was not caused
by any code change on the branch that hit it — that branch only touched
`crates/rk-daemon/tests/agent_archive.rs`.

## Root cause

Every rat-spawned agent gets `CARGO_TARGET_DIR` pointed at one shared
`<RK_HOME>/cargo-target-cache/<repo>` directory rather than its own
worktree's `target/` (`[disk] shared_cargo_target`, default `true`,
landed in 52f1c5840 / TKT-01M04D1QDBNCF0T0D0EHRVNJV5). That change was a
deliberate trade: per-worktree target dirs were multiplying disk usage
(3-7 GB × 60+ concurrent worktrees) until `cargo test --workspace` failed
outright on `ENOSPC`. Sharing one cache trades that multiplication for
relying on cargo's own target-dir lock to serialize concurrent builds.

That lock only covers the *build* phase of a single `cargo test`
invocation, not the gap between "a build finished and resolved this test
binary's path" and "this process execs that path." When two different
rats' worktrees run `cargo test --workspace`/`cargo build` concurrently
against the *same* shared target dir (confirmed via `ps aux` at the time of
this failure — several other rats' cargo processes were live), one
process's build can recompile and garbage-collect a stale-fingerprint copy
of a test binary that another process already resolved and is about to
exec. The second process then gets `ENOENT` on a path that existed moments
earlier. This is cross-process contention, not an in-process race, and it
reproduces at the merge-base too (unrelated to any code change).

CI is unaffected: `.github/workflows/ci.yml` runs on ephemeral
GitHub-hosted runners, each with its own private `target/`, so this class
of failure only exists on the shared local castle machine.

## Not the same mechanism as the sibling flake tickets

Other open "flaky under full `cargo test --workspace`" tickets
(TKT-01M0BJBBQYZ6RPA7E218JPBN21 `agent_archive`, TKT-01M0BWWY15SH2KCQ99WKPGN9N7
`workflow_run` approval-gate, TKT-01M0BXVVTVM646XG3RY13NFGYS
`landing::tests::restart_mid_gate_run_resumes_and_lands`) fail with daemon
startup timeouts or gate-evaluation panics — symptoms of *intra-process*
contention between test binaries racing each other for ports/worktree
paths inside one `cargo test --workspace` invocation. This ticket's
`ENOENT (never executed)` signature is specific to a *missing build
artifact*, which only happens via the shared `CARGO_TARGET_DIR` +
concurrent-process path above. Worth keeping the two classes distinct so a
fix for one doesn't get credited (or blamed) for the other.

## Fix options for a follow-up decision

No safe, narrowly-scoped code fix exists within a single rat's worktree
diff — every real option changes shared, fleet-wide behavior and needs an
explicit trade-off call:

1. **Serialize the test-*execution* phase, not the build, across
   concurrently-spawned agents on the same repo** — e.g. an advisory lock
   the daemon holds around a spawned agent's `mise run verify`/`test` step
   when `shared_cargo_target` is on. Addresses the root cause directly;
   requires a `rk-daemon` supervisor change and slows verify under load.
2. **Scoped retry at the orchestration layer** (not inside `mise.toml`,
   to keep the CI-mirrored `verify` task and its 1:1 parity with
   `ci.yml` untouched) — whatever gates/re-runs a rat's verify step
   retries once on the specific `could not execute process ... (never
   executed) ... No such file or directory` signature. Lowest blast
   radius, doesn't touch the shared entrypoint every rat and CI depend on.
3. **Bounded per-worktree target dirs for a small number of concurrently
   live worktrees**, overflowing to the shared cache beyond that — partial
   reintroduction of the disk cost 52f1c5840 was fixing; needs capacity
   planning.

(2) is the safest immediate mitigation; (1) is the real fix but needs
design + operator sign-off given it changes daemon spawn behavior
fleet-wide.

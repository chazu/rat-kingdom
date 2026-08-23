# TKT-01M0HNESEECWWFQF8X6VH1XSJ6: bounded per-repo verification admission queue — partial delivery

## What this ticket asked for

One daemon-owned per-repository verification admission path used by both
landing gates and agent/reviewer completion checks, instead of every rat
launching an independent full workspace suite: resolve the repo-owned named
check, strip spawn identity, acquire a fair bounded lease, stream bounded
progress, return the exact child exit status, release the lease on
completion/timeout/agent death/daemon restart/cancellation, and prove FIFO
fairness, restart recovery, exact exit provenance, independent-repo
concurrency, and proof reuse for an exact key.

## What landed (commit 21f2f0e, this branch)

- **Config**: `[policy] verification_admission_limit` (u32, default `0` =
  disabled) and `verification_admission_limit_by_repo` — the fleet-wide
  default plus per-repo override, matching this codebase's existing
  `0 = disabled` convention (`min_free_gb`, `max_load_per_cpu`) and the
  existing `Reap::artifact_paths_by_repo` fallback shape.
- **`VerificationAdmission`** (`supervisor.rs`): a per-repo
  `tokio::sync::Semaphore`-backed bounded queue, deliberately mirroring the
  already-shipped `TestExecLock` (same lazy-per-repo-key creation, same
  fail-closed-on-its-own-timeout behaviour) but with a *configurable* WIP
  limit that may be raised above 1 — the ticket's explicit "repository policy
  limit greater than one" requirement. `tokio::sync::Semaphore` grants
  permits in acquire order, giving FIFO fairness by construction. Entirely
  in-memory: a daemon restart drops the whole struct along with every
  outstanding permit, so there is no durable lease state that could ever be
  left stranded — restart recovery is a structural property of the design,
  not a recovery procedure that has to run.
- **`run_check_in` wiring** (`workflow_exec.rs`): acquires this lease right
  after the existing `TestExecLock` acquisition, gated on the same
  `sharedCargoTarget` opt-in flag a check already declares (so only the
  CPU/wall-clock-heavy checks — `verify`, not the fast diff-scope checks —
  ever enter the queue) and on a nonzero configured limit (so the default
  configuration is byte-for-byte unchanged from before this queue existed).
  Held for the whole retry loop, released via RAII on every return path.
  Writes a durable `(Event, <repo>, "verification_admission")` `Furniture`
  tuple with `queue_wait_ms`/`duration_ms`/`exit`/`verdict` whenever
  admission was actually engaged — the durable timing the ticket asks for.
  Since landing gates and workflow `run` steps already funnel through
  `run_check_in`, this one insertion point covers both for free.
- **`WorkflowEngine::verify_repo_check`**: the same execution — env-stripped,
  admission-controlled, exact-exit-provenance — outside any workflow
  instance, for a rat's own completion check. Resolves the named check from
  `<dir>/.rk/checks.cue`, builds a `ResolvedRun` with `expect_exit: None`
  (same reasoning as `LandingPipeline`'s own construction: read
  `verdict`/`exit` off a clean `Ok` result rather than a propagated `Err`),
  and calls `run_check_in` directly.
- **Proof reuse**: an exact-match dedup key —
  `canonical_digest({repo, candidate_sha, command, toolchain,
  environment_policy})` — recorded as `(Event, <repo>, "verification_proof")`
  on a passing verdict, consulted before running. A dirty worktree (any
  uncommitted change) is detected via `git -C <dir> status --porcelain`
  scoped to the exact worktree under test (not `rk_git::Repo::discover`,
  whose `rev_parse`/`is_dirty` resolve to the *common* repo root, which is
  the wrong directory for a linked worktree) and NEVER reads or writes the
  cache — the only way to guarantee a "prepared merge" is never confused
  with a different candidate. Also recognizes an unrelated landing gate's
  existing `landing_gate_pass` event (commit `3d47d08`, "perf: reuse landing
  gate proof during review") for the same exact candidate sha as a free
  secondary hit, so a rat whose branch a landing gate already tested gets an
  instant result.
- **`verify.run` RPC** (`server.rs`): `{repo, check: Option<String>}` →
  resolves the caller's own worktree (an agent caller's `repo_name` must
  match `params.repo`, and it runs in ITS OWN worktree, uncommitted changes
  included) or the repo's registered root checkout (operator/empty caller),
  then calls `verify_repo_check`. No new authorization-list entry was
  needed: it is not in `authorize_reasoned`'s operator-only method list, so
  any authenticated agent can already call it for its own repo.

Verified: `cargo build --workspace` clean, `cargo check --workspace` clean
(zero warnings), and the full existing `workflow_exec::tests` module (52
tests, including every `run_check_in_*` test) green with no regressions —
run with `RK_*`/`RK_REVIEW_*`/`RK_AUTH_TOKEN` stripped per the fleet's
`strip_rk_spawn` test convention.

## What did NOT land — filed as TKT-01M0P3R59CFJP73ZGSEVTH75ED

1. **`rk verify` CLI subcommand.** No client-side surface exists yet to
   actually call `verify.run` from a rat's own bash session. Without it the
   RPC is unreachable to the audience the ticket names ("agent/reviewer
   completion checks").
2. **`prime.rs` completion guidance.** Still tells a rat to self-invoke its
   full suite directly; needs to recommend `rk verify` and note (at the
   documentation level — TKT-01M0CK4Z019SMBN9CTCZBYCTKX already found the
   daemon has no channel to observe a truly external invocation) that a
   self-invoked full suite bypasses admission control.
3. **The ticket's explicit proof tests**: FIFO/documented fairness under
   contention, restart recovery, exact exit provenance, independent-repo
   concurrency, landing-gate/`verify.run` shared-bound, and proof-reuse vs
   dirty-worktree. None of these were written — only the pre-existing
   `run_check_in` suite was re-run as a smoke check that nothing regressed.
4. **An open correctness question, not yet resolved**: what string
   `run_check_in`'s `repo` parameter actually holds differs by caller —
   `LandingPipeline` passes `entry.repo_name` (a bare name, confirmed by
   reading `landing.rs`), while a `workflow.run` RPC dispatched via `rk
   workflow run` canonicalizes `--repo` to an absolute PATH client-side
   before sending it (`crates/rk-cli/src/main.rs`). `verify.run`
   (this branch) deliberately keys on the repo NAME, matching
   `LandingPipeline`. If a workflow `run`-step check is ever keyed by PATH
   instead, it would land in a *different* `Semaphore` instance than a
   landing gate or `verify.run` for the exact same repo — undermining "one
   bound" for that specific combination, though the primary pairing this
   ticket names (landing gates + agent/reviewer completion checks) is
   unaffected since both are name-keyed. `TestExecLock` already has this
   exact same ambiguity; this design deliberately mirrors it rather than
   fixing it here, since resolving it (normalizing `repo` to a canonical
   name at every call site) is a separable, pre-existing-adjacent concern
   better scoped on its own.

## Why this stopped here

Budget constraints during this dispatch. What landed is a real, compiling,
non-regressing admission-control mechanism wired into the load-bearing
`run_check_in` path used by every existing landing gate and workflow `run`
step, plus a new RPC entry point for the self-driven case — but without the
CLI surface, prompt-guidance update, or the ticket's own required test
properties, it is not yet something a rat can actually use, nor is it
verified against the specific claims (fairness, restart safety, exact
provenance, cross-repo concurrency, dedup) the ticket asks to be proven.
TKT-01M0P3R59CFJP73ZGSEVTH75ED carries the remainder forward.

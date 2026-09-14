# TKT-vilug-hujok-bolis (P3.1): aggregate host-wide managed-verification cap

## What this ticket asked for

One optional static aggregate concurrency cap for daemon-managed named
checks across every repository this daemon serves, in addition to (never
instead of) each repository's existing per-repo cap — no coalescing,
weights, new recipes, release inventory, or automatic promotion.

## What landed (this branch)

- **Config**: `[policy] verification_admission_aggregate_limit` (`u32`,
  default `0` = disabled — zero behaviour change from before this existed).
  Read once at `Daemon::new` from config, the same startup-only convention
  as every other admission limit in `rk-core/src/config.rs`
  (`verification_admission_limit`, `implementation_admission_limit`, ...):
  changing it requires a daemon restart, it is never live-reloaded. Invalid
  values (negative, non-numeric) are rejected by ordinary TOML/serde
  parsing — `u32` cannot represent them, so no separate validation pass was
  needed.
- **Enforcement boundary**: this cap bounds only checks routed through THIS
  daemon's managed runner (`ManagedVerification::run` — landing gates,
  workflow `run` steps, and the `verify.run` RPC a rat's own completion
  check calls into). Another daemon on the same host, or unmanaged shell
  work an operator runs directly, is outside it entirely — there is no
  host-wide process-table enforcement, only admission at this one common
  entry point.
- **`HostVerificationAdmission`** (`managed_verification.rs`): one
  process-global `tokio::sync::Semaphore` sized to the configured limit,
  in-memory only — exactly like the existing per-repo
  `VerificationAdmission`, restart drops it and the next daemon starts
  fresh, full of permits, with no durable lease to strand. Reports
  `limit`/`executing`/`waiting` for native status.
- **Acquisition order in `ManagedVerification::run`**: the shared
  `CARGO_TARGET_DIR` lock (`TestExecLock`, when applicable), then the
  per-repo `VerificationAdmission` permit (when that repo has a nonzero
  limit), then — LAST — the aggregate `HostVerificationAdmission` permit
  (when the aggregate limit is nonzero), all within one overall admission
  timeout. Acquiring the host permit last means a request still queued
  behind its own saturated repo (or the shared-target lock) never enters
  the host semaphore's wait queue at all, so it can never occupy, or queue
  ahead of, a host slot an eligible request from a DIFFERENT repo could use
  immediately — this is what prevents cross-repo head-of-line blocking,
  as a pure ordering property of two independent semaphores, with no
  check-sharing or coalescing involved.
- **`uses_capacity_admission` fix**: this predicate (read by `landing.rs`'s
  combined-candidate batching, `process_batch`, to decide whether a repo is
  safe to batch multiple tickets' checks into one run) now also returns
  `true` when the aggregate limit is nonzero, not only when the repo has its
  own override or the shared-cargo-target flag is set. Before this fix, a
  repo with no per-repo override but a positive aggregate cap would have
  read as "unbounded, safe to batch" even though its checks are, in fact,
  now under host-wide admission.
- **Native status**: `status` RPC gains `verification_host: {limit,
  executing, waiting}`, additive alongside (never replacing) the existing
  per-repo `capacity[repo].verification` lane. `waiting` counts ONLY
  requests already past their own repo's admission and blocked
  specifically on the aggregate semaphore — a request still queued behind
  its own per-repo lane is not double-counted here (proven directly by
  `repo_specific_cap_still_holds_under_a_higher_aggregate_cap`). `rk daemon
  status` prints `capacity host verification: <executing>/<limit>[, N
  waiting]` only when the limit is nonzero.
- **Cancellation and restart**: reuses the existing
  `ManagedVerificationRuns`/RPC-disconnect cancellation path
  (`server.rs::dispatch_watching_disconnect` /
  `cancel_managed_verification_request`) and the existing
  `ManagedChildMarker`/`reap_stale_managed_children` restart-cleanup sweep
  unchanged — the aggregate permit is released the same way the per-repo
  permit already was (RAII guard held for `run`'s whole scope), so no new
  cancellation or restart-recovery code was needed, only new coverage of
  the existing mechanisms under the new cap.

## Tests

- `crates/rk-daemon/tests/host_verification_aggregate_cap.rs` (new): the
  real native RPC journey with two independently registered fixture repos
  and barrier-controlled checks (pid-file start proof + explicit release
  file, never a sleep as the success criterion; every concurrency assertion
  polls the native `status` RPC to a bounded deadline instead) —
  `aggregate_cap_1_serializes_two_repos`,
  `aggregate_cap_2_allows_two_concurrent_checks`,
  `repo_specific_cap_still_holds_under_a_higher_aggregate_cap` (also proves
  the `waiting`-count scoping above),
  `aggregate_cap_disabled_preserves_prior_behavior`, and
  `cancelling_queued_and_executing_aggregate_requests_reaps_children_and_lets_a_peer_progress`
  (a genuinely queued cancellation that never spawned a process, a genuinely
  executing cancellation whose real child pid is proven dead via `kill -0`,
  a still-waiting peer that then progresses, and a durable
  `verification_cancelled` event).
- `crates/rk-daemon/tests/managed_verification_cancel_e2e.rs` (extended):
  `daemon_restart_cleans_owned_verification_work_before_admitting_replacements_under_the_aggregate_cap`
  mirrors the existing
  `daemon_restart_never_blocks_progress_on_a_run_that_was_in_flight_when_it_died`
  real-crash/real-restart test, with the aggregate cap (not just the
  per-repo one) configured on both daemon generations, proving daemon B's
  own aggregate semaphore starts genuinely empty and immediately admits a
  fresh replacement run under the same cap.

## Deployment

Default `0` is harmless — no behaviour change until an operator sets a
positive value in `config.toml` and restarts through the normal paired
install/rollover procedure. Disabling again is the same restart with the
value set back to `0`. No new capability dependency: the existing managed
runner supplies all execution routes and owned-process cleanup this cap
reuses.

## Deferred (per ticket scope)

Weighted classes, stronger aging/fairness, and further named recipe routes
remain required follow-ups in P3/P4. This does not close the whole P3 track
or claim full host scheduling, and does not import any P0 check-sharing
code.

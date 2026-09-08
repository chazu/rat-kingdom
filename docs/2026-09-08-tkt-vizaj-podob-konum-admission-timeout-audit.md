# TKT-vizaj-podob-konum: admission-queue-timeout audit (2026-09-08)

*This ticket's job was to audit why cheap, content-clean, docs-only landing
attempts were silently dropped under WIP-limit-1 admission congestion
(observed twice on TKT-togin-zinus-nizip: Gouda-14 commit cbfbed2, Cinder-14
commit a8f5bce — both branches lost with the worktree, each redispatch
re-deriving the same finding from scratch) and recommend whether the
admission queue should retry-on-timeout or the WIP limit should rise, rather
than to implement a fix directly. That scoping holds: the actual code sites
this implicates are either live operator config outside any rat's worktree,
or a deliberately-reasoned daemon fail-closed policy that a maintainer should
sign off on changing, and `crates/rk-daemon/src/landing.rs` is claimed
in-progress right now by Thistle-14 for a touching-but-distinct concern
(TKT-susog-bovot-huzit).*

## Root cause, traced to source

1. **The WIP limit is fleet-wide, not landing-specific.**
   `~/.rat-kingdom/config.toml` sets `verification_admission_limit = 1` with
   no `rat-kingdom` override in `verification_admission_limit_by_repo`. Every
   managed check for this repo — a 30-second `steward-protected-paths` grep
   and a 60-minute `verify-full` — shares one queue slot
   (`crates/rk-daemon/src/managed_verification.rs`
   `VerificationAdmission::acquire`, `VerificationResources`). Under
   dogfooding load this repo is by far the most active in the fleet, so the
   single slot is close to always occupied.

2. **The admission-queue wait shares its time budget with the check's own
   execution timeout — deliberately.** In `ManagedVerification::run`
   (`managed_verification.rs:556-591`), `tokio::time::timeout(timeout, ...)`
   bounds the wait for a permit using the SAME `timeout` the check declared
   for its own execution in `checks.cue` (`steward-protected-paths` declares
   `2m`). The comment immediately above the sibling `test_exec_lock` guard
   (line 509-512) states the reasoning explicitly: *"Bounded by this check's
   own timeout: if the queue is deep enough that a check cannot even START
   within its own declared budget, that is as good as it failing outright —
   fail closed rather than let the wait grow unbounded."* This is a
   considered tradeoff, not an oversight — but it means a 2-minute-timeout
   check that would itself finish in seconds can be starved out entirely by
   an unrelated 60-minute check already holding the one WIP-1 slot.

3. **The resulting failure is knowingly, deliberately NOT retried.** The
   timed-out acquire returns `Err(rk_core::Error::other(stderr))`
   (`managed_verification.rs:568-590`, `LOCK_TIMEOUT_EXIT = -2`). In
   `LandingPipeline::run_gates_at` (`landing.rs`, the `Err(e) => { ... }` arm
   around line 6678), the comment reads: *"Any other run_check_in Err (a `sh`
   that could not even spawn) is an infra fault, not a verdict on the branch,
   but is treated the same way here: fail-closed, hold rather than land."*
   The author already recognized admission-queue congestion says nothing
   about the branch's correctness — and chose to hold anyway, same as a real
   `fail`.

4. **Contrast with the retry path that already exists for exactly this
   class of problem.** `landing.rs` has a well-tested, one-shot automatic
   "infra death" retry (`gate_infra_retry_used` /
   `GATE_INFRA_RETRY_IDENTITY`) for a check whose child process never
   reported its own exit code (killed by signal, OOM, runner loss) — see
   `infra_death_then_pass_retries_once_and_lands` and
   `infra_death_exhausted_after_one_retry_holds_with_precise_evidence`. That
   mechanism is deliberately withheld from a genuine check-execution timeout
   (`timeout_holds_without_infra_retry`: *"a genuine timeout must hold the
   branch"*) — the check ran and legitimately failed to finish, which IS
   informative. Admission-queue congestion is neither: the check never even
   started, so, per the author's own `Err(e)` comment, it is closer in kind
   to an infra death than to a genuine timeout or a real fail — it is just
   routed through the generic `Err` arm instead of the `verdict == "infra"`
   arm that already has retry logic.

## Recommendation

Two independent levers, either sufficient alone for the observed pattern —
not mutually exclusive:

1. **Config lever (operator-owned, zero code risk).** Add a `rat-kingdom`
   entry to `verification_admission_limit_by_repo` in
   `~/.rat-kingdom/config.toml` (e.g. `2`), matching the precedent already
   set there for `glossolalia`'s `implementation_admission_limit_by_repo`
   graduation from a WIP-1 pilot. This is live daemon config outside every
   rat's worktree — no agent caller can change it; an operator must.

2. **Code lever (in-repo, needs its own dispatch).** Reclassify the
   admission-queue-timeout signature in `managed_verification.rs`
   (`LOCK_TIMEOUT_EXIT`, `managed_verification.rs:568-590`) so it surfaces to
   `landing.rs` as `verdict == "infra"` (an `Ok(...)` carrying that verdict)
   instead of a bare `Err`, so it flows through the existing, already-tested
   one-shot infra-retry instead of the blanket fail-closed `Err(e)` arm.
   This targets exactly the gap the `Err(e)` comment itself names, without
   touching the deliberately-tested `timeout_holds_without_infra_retry`
   behavior for a genuine check-execution timeout, which should keep holding
   unretried. Needs a new regression test alongside the existing infra-death
   tests, and sequencing after Thistle-14's `TKT-susog-bovot-huzit` work
   lands (both touch `landing.rs`).

Filed as `TKT-harid-nakam-rozuh` (see ticket body for the same recommendation
in dispatchable form) rather than implemented here, per this ticket's own
audit scoping and to avoid colliding with `landing.rs`'s active claim.

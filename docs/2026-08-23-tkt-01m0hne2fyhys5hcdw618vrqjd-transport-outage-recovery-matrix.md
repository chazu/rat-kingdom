# Transport-outage recovery: policy knobs, commands, and coverage matrix

Closure notes for TKT-01M0HDFZNVPHE0JV382VSBCQD0 ("Bound harness transport
outages and recover work without zombie capacity"). The three sub-tickets
below built the typed transport-outage lifecycle; this document is the single
place that names the operator-facing knobs and commands it produced, and
tracks which cells of the outage x transition matrix are proven by an
end-to-end test today versus still open.

- TKT-01M0HND8M25GYN1ZTRET3S5769 — pre-work typed outage + castle-wide breaker
- TKT-01M0HNDJ7AS9F1A3W22FRCC63N — post-commit recovery record + at-most-once continuation
- TKT-01M0HNDT8CWBAYABC2GT5NXYJY — reviewer-ceiling fencing + late evidence

## Operator policy knobs

All live on `rk_core::config::SupervisorConfig` (`crates/rk-core/src/config.rs`)
and are per-castle, not per-provider — the breaker and retry schedule are
keyed by provider name at runtime, but the thresholds themselves are shared.

| Field | Default | Governs |
|---|---|---|
| `transport_retry_max_attempts` | `5` | How many times the pre-work retry sweep relaunches one agent whose launch failed before `Started`. Durable — survives a daemon restart without resetting. `0` disables the sweep. |
| `transport_retry_backoff_secs` | `30` | Base backoff between retries of the same outage episode, `base * 2^(attempts-1)`. |
| `transport_retry_jitter_secs` | `15` | Deterministic per-attempt jitter added to the backoff so many agents of one provider don't retry in lockstep. `0` disables jitter. |
| `transport_breaker_trip_threshold` | `3` | Consecutive castle-wide pre-work failures for one provider (any agent) that trip the circuit breaker. `0` disables the breaker (failures still tracked per-agent, but no provider is ever refused). |
| `transport_breaker_cooldown_secs` | `120` | How long a tripped provider breaker stays open with no further failures before it auto-closes. `0` disables the breaker. |
| `respawn_enabled` | `true` | Whether the sweep auto-`respawn`s agents that crashed out of their run (Orphaned by daemon restart, or Failed). |
| `respawn_max_attempts` | `3` | Crash-loop bound per agent before the sweep gives up and escalates a `need`. `0` disables auto-respawn even when `respawn_enabled` is true. |
| `respawn_backoff_secs` | `300` | Base backoff between auto-respawns of the same agent, exponential per attempt. |
| `respawn_rate_cap_per_hour` | `10` | Castle-wide rolling cap on auto-respawns (any agent, any repo) in a trailing hour, enforced by `RecoveryAnnouncer`. `0` disables the cap. |

Reviewer-ceiling fencing (distinct machinery, `rk_workflow::LandingPolicy` in
`crates/rk-workflow/src/lib.rs`):

| Field | Default | Governs |
|---|---|---|
| `review_timeout` (workflow ceiling) | `STEWARD_DEFAULT_REVIEW_TIMEOUT_SECS` = `900s` (15m) | Wall-clock ceiling a reviewer workflow instance may run before it is fenced (terminated/parked) regardless of transport health. |
| `max_review_death_attempts` | `1` | How many fresh review attempts a review-death retry (including a transport-fenced one) may spend before it stops and escalates, via `landing_review_retry::ReviewDeathBackoffPolicy`. |

`SupervisorConfig::stuck_after_secs` (default `600s`) is asserted at a unit
level (`default_stuck_after_secs_stays_below_shipped_review_timeout`) to stay
below the review ceiling, so ordinary stuck-agent detection never races
reviewer-ceiling fencing.

The post-commit `RecoveryAnnouncer` rate cap (distinct from
`respawn_rate_cap_per_hour`) is currently hardcoded at `RateCap::per_hour(20)`
in `supervisor.rs` rather than a config field — flagged here as a gap, not a
knob, should it need to become operator-tunable.

## Recovery and inbox commands

| Command | RPC | Use |
|---|---|---|
| `rk inbox` | `inbox.list` | Lists all actionable rows, including `transport-outage` and `recovery-action` rows. |
| `rk inbox ack <id>` | (ack) | Acknowledges an inbox row without taking its action. |
| `rk respawn <name>` | `agent.respawn` | Manual/operator respawn. For a pre-work transport episode, an attempt that reaches `Started` clears the typed episode on the agent and closes the provider's circuit breaker. |
| `rk continue-recovery <name> [--harness <kind>] [--action-id <id>]` | `agent.continue_recovery` | Resumes a parked post-commit recovery — same harness by default, or a configured alternate via `--harness`. Idempotent by `action_id`: a replayed id returns the same outcome; a *different* id after the first is acknowledged is refused. |
| `rk abandon-recovery <name> [--action-id <id>]` | `agent.abandon_recovery` | Releases WIP on a parked post-commit recovery without continuing it. Same at-most-once `action_id` semantics as `continue-recovery`. |
| `rk cancel-review <branch> --repo --target --task` | `repo.land.cancel_review` | Explicitly settles a reviewer-ceiling-fenced attempt (crash-safe: retrying after a durable settlement marker is refused rather than double-settling). |
| `rk reenqueue-review <branch> --repo --target --task --attempt` | (dedicated RPC) | Creates exactly one fresh review attempt after a fenced/cancelled one; idempotent — repeat calls return the same `new_attempt`. |

Types backing at-most-once acknowledgement and generation-fencing, both in
`crates/rk-daemon/src/agents.rs`: `RecoveryRecord` (carries `spawn: SpawnId`,
`budget_remaining_usd`, `ack: Option<RecoveryAck>`), `RecoveryRecord::stale`
(refuses continuation against a generation that has already moved on),
`RecoveryAck { action_id, outcome, acknowledged_at }` (persisted, so the
at-most-once guarantee survives a daemon restart), `RecoveryOutcome::{
ResumedSameProvider, ContinuedAlternateProvider, Abandoned }`.

## Coverage matrix

Rows are the four outage points named in the closure ticket; columns are the
properties the closure ticket asks the matrix to exercise.

| Outage point | Claude | Codex | Restart at durable transition | Breaker open/cooldown | Same/alternate harness | Ceiling/late evidence | Budgets+generation preserved |
|---|---|---|---|---|---|---|---|
| Before work | `transport_outage.rs::outage_retry_survives_restart_without_duplicate_launch_or_ledger_reset` | `transport_outage.rs::..._codex` (this ticket) | yes (daemon killed/replaced mid-episode) | yes (trip + refusal; auto-close proven via operator `respawn`→`Started`) | same-harness only (operator respawn) | ceiling exhaustion + no-duplicate-row proven | yes (`spawn`, `created_at`, `cost_usd`, `usage` asserted equal across restart) |
| After commit / during local verify | `post_commit_recovery_rpc.rs` (generic `FakeHarness`, not real claude/codex adapters) | same fixture, provider-agnostic | not yet — daemon-restart-mid-recovery is unit-level only (`supervisor.rs`), not proven over RPC | n/a (breaker is a pre-work concept only) | same-harness continuation unit-tested only; **alternate-harness continuation and WIP release not proven over RPC** | n/a | not asserted at the RPC level (only at-most-once ack is) |
| During reviewer wait | `review_ceiling_crash_barrier.rs` | same harness fixture is provider-agnostic (`fake` kind); not exercised against real claude/codex adapters | yes (`fault.rs` barriers pre/post durable marker + SIGKILL) | n/a | n/a (single reviewer identity per attempt) | yes — exactly-once convergence, late APPROVE retained as evidence without landing, idempotent reenqueue | not directly asserted (scope is settlement identity, not ledger) |

**Open gaps, not attempted in this closure pass** (filed as follow-up
tickets rather than left as an uncommitted attempt in this budget):

1. Post-commit recovery: prove same-harness resume, alternate-harness
   continuation, daemon-restart-mid-recovery, and WIP release over RPC/CLI
   (today only unit-tested in `supervisor.rs`), and against real claude/codex
   adapter fixtures rather than the generic `FakeHarness`. Filed as
   TKT-01M0RX7X896HF6WWYVX3BNFKEG.
2. Reviewer-wait: distinguish a *transport-classified* outage during review
   (adapter reports certificate/auth/unavailable) from the plain hung/timeout
   case `review_ceiling_crash_barrier.rs` already covers, and prove it against
   real claude/codex fixtures. Filed as TKT-01M0RX7X8Y7J6Y56QTXGKFCSHX.
3. `RecoveryAnnouncer`'s hardcoded post-commit rate cap: decide whether it
   should become an operator-tunable `SupervisorConfig` field for parity with
   `respawn_rate_cap_per_hour`. Not filed as a separate ticket — noted here
   for whoever picks up gap 1.

The parent ticket's closure gate ("close the parent only when this matrix
passes under full workspace verification") is **not** met by this pass alone
— items 1 and 2 above are load-bearing cells the parent's acceptance criteria
name explicitly (alternate-harness continuation, WIP release, at-most-once
continuation for the post-commit path). The parent is left open with these
gaps tracked as sub-tickets.

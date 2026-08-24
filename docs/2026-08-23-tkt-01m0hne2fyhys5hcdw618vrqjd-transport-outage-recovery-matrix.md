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
ResumedSameProvider, ContinuedAlternateProvider, Abandoned }`. A CONTINUED
recovery's `RecoveryRecord` (and its `ack`) is deleted by
`Supervisor::handle_event`'s `Started` arm the moment the resumed generation
proves liveness again (`AgentRecord::recovery` back to `None`, so the name is
eligible for `detect_post_commit_outage`/`respawn_sweep` on its next,
unrelated crash). The `ack` itself survives that deletion in
`AgentRecord::recovery_receipt: Option<RecoveryReceipt>` — a durable,
generation-scoped (`RecoveryReceipt::spawn`) tombstone written from the
outgoing `ack` in the same `Started` handler right before `recovery` is
cleared. `continue_recovery`/`abandon_recovery` consult it as a fallback
whenever `recovery` is `None`, so the SAME `action_id` still replays and a
DIFFERENT one is still refused after the resumed harness has already spoken
— across a daemon restart too, since the tombstone is a plain field on the
persisted `AgentRecord`. A later, unrelated post-commit outage on the same
generation parks a fresh `RecoveryRecord` (`ack: None`), which always takes
priority over the tombstone, so it can be freshly acknowledged without the
old tombstone interfering (TKT-01M0S28V7XQ17F0C3SDNGC4PQA).

## Coverage matrix

Rows are the four outage points named in the closure ticket; columns are the
properties the closure ticket asks the matrix to exercise.

| Outage point | Claude | Codex | Restart at durable transition | Breaker open/cooldown | Same/alternate harness | Ceiling/late evidence | Budgets+generation preserved |
|---|---|---|---|---|---|---|---|
| Before work | `transport_outage.rs::outage_retry_survives_restart_without_duplicate_launch_or_ledger_reset` | `transport_outage.rs::..._codex` (this ticket) | yes (daemon killed/replaced mid-episode) | yes (trip + refusal; auto-close proven via operator `respawn`→`Started`) | same-harness only (operator respawn) | ceiling exhaustion + no-duplicate-row proven | yes (`spawn`, `created_at`, `cost_usd`, `usage` asserted equal across restart) |
| After commit / during local verify | `post_commit_recovery_rpc.rs` (generic `FakeHarness`, wire ack path) + `post_commit_recovery_continuation_rpc.rs::continue_recovery_resumes_the_same_provider_across_a_restart_with_budget_preserved` (real Claude adapter, TKT-01M0RZWDJQ6B49WCMSK3DC54T6) | `post_commit_recovery_continuation_rpc.rs::continue_recovery_routes_to_a_real_alternate_harness_across_a_daemon_restart` (real Claude→Codex, TKT-01M0RX7X896HF6WWYVX3BNFKEG) | yes — both continuation tests and `abandoned_recovery_stays_terminal_across_a_restart_under_a_live_respawn_sweep` kill/replace the daemon between detection and the operator action, over a real socket | n/a (breaker is a pre-work concept only) | both proven over RPC/CLI with real adapters: same-provider resume via the real `rk continue-recovery` CLI (no `--harness`), alternate-provider via `agent.continue_recovery {"harness":"codex"}`; abandonment/WIP release proven under a LIVE restarted respawn-sweep loop | n/a | yes, byte-for-byte: same-provider test asserts `cost_usd`/`recovery.budget_remaining_usd` equal across detection, restart, continuation, and replay under a nonzero agent budget; alternate-provider test asserts `spawn` identity preserved across restart+continuation+replay |
| During reviewer wait | `review_ceiling_crash_barrier.rs` (plain hang) + reviewer-death fixture (real claude/codex, non-retryable auth failure, TKT-01M0RX7X8Y7J6Y56QTXGKFCSHX) | same fixture, parameterized over both providers | yes (`fault.rs` barriers pre/post durable marker + SIGKILL) | n/a | n/a (single reviewer identity per attempt) | yes — exactly-once convergence, late APPROVE retained as evidence without landing, idempotent reenqueue; typed transport-outage reviewer death now surfaces its own inbox row distinct from a plain hang | not directly asserted (scope is settlement identity, not ledger) |

**Resolved in this closure pass** (formerly the three open gaps below):

1. Post-commit recovery: same-harness resume, alternate-harness continuation,
   daemon-restart-mid-recovery, and WIP release are now all proven over
   RPC/CLI against real claude/codex adapter fixtures rather than the generic
   `FakeHarness`. Alternate-harness continuation, restart-mid-recovery, and
   abandon-under-live-sweep landed under TKT-01M0RX7X896HF6WWYVX3BNFKEG
   (Deyna-12); the same-provider resume plus exact `budget_remaining_usd`
   preservation across restart landed under
   TKT-01M0RZWDJQ6B49WCMSK3DC54T6 (Linguini-12), driven through the real `rk
   continue-recovery` CLI rather than raw RPC. Linguini-12 flagged a real, if
   narrow, edge this pass surfaced: `Supervisor::handle_event`'s `Started`
   arm cleared the whole parked `recovery` record — ack included — the
   instant a continued generation proved liveness again, so a caller
   replaying `action_id` after the resumed harness had already spoken saw
   "no pending recovery" instead of the recorded outcome. Closed under
   TKT-01M0S28V7XQ17F0C3SDNGC4PQA (Pip-13): `AgentRecord::recovery_receipt`
   is now a durable, generation-scoped tombstone written from the ack right
   before `Started` clears `recovery`, and `continue_recovery`/
   `abandon_recovery` fall back to it whenever `recovery` is `None`. Proven
   in `supervisor.rs`:
   `continue_recovery_replays_ack_after_started_clears_the_record_same_provider`,
   `..._alternate_provider`,
   `recovery_receipt_survives_started_clear_across_a_daemon_restart`, and
   `later_recovery_on_same_generation_supersedes_the_receipt_and_can_be_freshly_acknowledged`
   (the last proves a later, unrelated post-commit outage on the same
   generation is unaffected — its fresh, unacknowledged `RecoveryRecord`
   always takes priority over the tombstone). The documented "the SAME key
   after acknowledgement replays the same recorded outcome" contract now
   holds with no caveat.
2. Reviewer-wait: a *transport-classified* outage during review is now
   distinguished from the plain hung/timeout case, proven against real
   claude/codex fixtures, under TKT-01M0RX7X8Y7J6Y56QTXGKFCSHX (Emile-12).
3. `RecoveryAnnouncer`'s hardcoded post-commit rate cap: still open — decide
   whether it should become an operator-tunable `SupervisorConfig` field for
   parity with `respawn_rate_cap_per_hour`. Not filed as a separate ticket.

All three sub-tickets under TKT-01M0HNE2FYHYS5HCDW618VRQJD are now closed or
complete; the parent's closure gate ("close the parent only when this matrix
passes under full workspace verification") depends only on that full
workspace run, not on any further test authoring.

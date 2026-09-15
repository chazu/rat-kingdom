# TKT-nasif-danob-sirok (P3.2): weighted classes and fair progress for host admission

## What this ticket asked for

Extend P3.1's optional static aggregate host-wide verification cap
(`docs/2026-09-14-tkt-vilug-hujok-bolis-p3-1-aggregate-verification-cap.md`)
with a ticket-weighted/fair mode: a named check may cost more than one
aggregate unit, and a cheap mandatory guard check must be able to keep
making progress alongside a long admitted check — bounded, never an
unlimited or self-declared bypass — within the existing aggregate/repo
admission contract. Scope is limited to existing named checks; P4 build
routing/experiments are separate, and P0's in-flight check-sharing code is
not imported.

## Motivating evidence

Native inner-edge gate event `01M2GXHR9ZSDKQF7198G0YPW5S` (2026-09-14):
a phase that ran only the two lightweight mandatory guards
(`landing-protected-paths`, `landing-diff-scope`, `full_check_required:
false`) took 591692ms wall-clock, because `landing-protected-paths` alone
waited 590590ms for admission behind an undifferentiated aggregate check
lane. P3.2 gives an operator a way to reserve a small, bounded slice of the
aggregate specifically for checks like these, so a guard is never forced to
queue behind an unrelated long-running `verify` simply because both share
one undifferentiated pool.

## What landed (this branch)

- **Config** (`rk-core/src/config.rs`, `PolicyConfig`), all additive and
  empty by default — zero behaviour change for a daemon that never
  populates them:
  - `verification_admission_check_weight: BTreeMap<String, u32>` — per
    check-name aggregate cost, default 1.
  - `verification_admission_check_class: BTreeMap<String, String>` — per
    check-name fast-lane class membership.
  - `verification_admission_class_reserve: BTreeMap<String, u32>` — per
    class, aggregate units reserved for it, carved OUT OF (never additive
    to) the aggregate limit.
  Class/weight membership is entirely `config.toml`-owned. Nothing in the
  admission path reads a repository's own (possibly untrusted)
  `checks.cue` to decide class or weight, so no workflow- or repo-supplied
  check definition can self-declare its way into extra capacity or a
  reserved lane.
- **Validation at daemon startup** (`Daemon::new` → `Supervisor::set_verification_admission_class_policy`
  → `HostVerificationAdmission::set_class_policy`), fail-closed, same
  convention as `crate::authority::AuthorityPolicy::from_config`: refuses to
  start rather than silently clamp when
  - any configured weight is `0`,
  - any configured weight exceeds the aggregate limit (that check could
    never be admitted — would hang, not fail, at runtime), or
  - the sum of every class's reserve exceeds the aggregate limit.
  A rejected policy never partially applies — the general pool from the
  preceding `set_verification_admission_aggregate_limit` call is left
  exactly as it was. `set_verification_admission_class_policy` must be
  called AFTER the aggregate-limit call (`Daemon::new` does so in that
  order); a bare aggregate-limit change alone resets any previously
  configured class policy to empty, so a policy validated against an old,
  larger limit can never silently outlive a shrink.
- **`HostVerificationAdmission`** (`managed_verification.rs`): the general
  pool is now sized to `limit - sum(class_reserve)`; each class with a
  positive reserve gets its own dedicated `tokio::sync::Semaphore`. A
  check's aggregate cost (its configured weight, default 1) is acquired
  ATOMICALLY via `Semaphore::acquire_many_owned`/`try_acquire_many_owned` —
  never partially, so a heavy check can never hold only some of its cost
  while waiting on the rest.
- **Acquire order** (`HostVerificationAdmission::acquire`, called from
  `ManagedVerification::run` in the same position P3.1 already used — last,
  after the shared-target lock and per-repo admission): if the check's name
  is a member of a class, FIRST try a non-blocking `try_acquire_many_owned`
  against that class's own reserve. That only succeeds when the reserve
  currently has enough free capacity AND the check's weight does not exceed
  the class's own total reserve (a request that could never fit its own
  lane is never even tried against it). Either way, falling through lands
  in the same bounded general-pool queue every unclassified request already
  uses — a saturated fast lane degrades to ordinary shared admission, it
  never blocks indefinitely on its own reserve, and it never draws more
  than its configured share.
- **`CheckExecution` gains `check_name: &str`** (empty string for a raw
  inline `run` step with no named-check identity), threaded through from
  every construction site in `managed_verification.rs`, `landing.rs`, and
  `workflow_exec.rs` (15 call sites total) purely to look up this check's
  configured weight/class — an absent or empty name costs the default
  weight 1 and joins no fast lane, identical to pre-P3.2 behaviour.
- **Native status**: `verification_host` gains an additive `classes: {name:
  {limit, executing}}` map (empty unless a class policy is configured);
  `limit`/`executing`/`waiting` keep their P3.1 meaning exactly — with no
  class policy configured, `executing` is exactly the general-pool figure,
  unchanged from P3.1. With a class policy configured, `executing` is the
  TRUE total (general + every reserved lane), so the aggregate ceiling
  `limit` and `executing` stay comparable at a glance.

## Evidence

- Unit tests (`managed_verification.rs::tests`, 8 new): weight-0 rejected,
  weight-exceeds-limit rejected, reserve-total-exceeds-limit rejected (and
  proven non-partial), a no-op empty policy leaves `executing`/`class_summary`
  unchanged, and a bare `set_limit` resets a previously configured class
  policy.
- Real native CLI/daemon e2e (`crates/rk-daemon/tests/host_verification_weighted_fair.rs`,
  3 new tests, barrier-controlled fixture checks across two registered
  repos, polled via the native `status` RPC to a bounded deadline — same
  technique as `host_verification_aggregate_cap.rs`):
  - `weight_two_consumes_two_of_an_aggregate_two_cap` — atomic weight,
    shared host-wide across repos.
  - `guard_class_progresses_while_general_pool_is_saturated` — the
    motivating scenario: a guard-class check runs concurrently with a long
    check that has saturated the general pool, while an ORDINARY second
    unclassified request still queues normally (the reserve is bounded, not
    a blanket bypass).
  - `reserved_lane_saturated_falls_through_to_the_general_pool` — a
    class's own reserve exhausting itself degrades to the shared queue
    rather than blocking indefinitely.
- Regression: the full pre-existing P3.1 suite
  (`host_verification_aggregate_cap.rs`, 5 tests) and every `managed_verification::`
  (17), `workflow_exec::` (67), and `landing::` (136) unit test in
  `rk-daemon` still pass unchanged.

## Scope explicitly NOT covered here

- P0's in-flight check-sharing/coalescing is not imported.
- P4 build routing and experiments are a separate ticket.
- No live-reload: like every other admission limit in this file, changing
  weight/class/reserve config requires a daemon restart.
- No CLI surface beyond `config.toml` and the existing native `status` RPC
  was added — an operator inspects the new `classes` field the same way
  `verification_host` is already inspected today.

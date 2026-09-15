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

## REWORK correction (2026-09-15)

Native review `01M2HWE7TBKTGZZGEKK19WM16J` against the first candidate
(`9e7bde7`) found two confirmed correctness bugs, both reproduced by an
isolated native probe (BBS finding `01M2HWAQ60BB3Q26Q7E50S33VV` / evidence
`01M2HWAPF2741F06KP7DMNJJ50`) before ever reaching production (per
operator-verified message `01M2HWB936G96DXXJY28KTT900`, weighted mode was
never enabled, so no live system was affected):

1. **Full-reservation bypass**: `validate_host_admission_policy` accepted a
   class reserve total EQUAL TO the aggregate limit, leaving
   `general_limit = 0` and `self.general = None`. `acquire()`'s first
   statement cloned the general-pool `Option` and used `?` to return early
   — the documented "cap disabled" convention — even though the aggregate
   cap was very much enabled. Net effect: a config that reserved 100% of
   the aggregate into classes silently disabled ALL host admission
   fleet-wide (classified and unclassified checks alike ran fully
   unbounded). Native repro: `aggregate=1, class_reserve.cheap=1` admitted
   two simultaneously live checks while `status` reported `executing=0`.
2. **Weight validated against the wrong pool**: a configured weight was
   checked only against the raw aggregate limit, never against the smaller
   pool it would actually draw from once class reserves are carved out.
   `aggregate=2, class_reserve.guard=1` (general pool 1), unclassified
   `weight=2` passed the old check (`2 <= 2`) but could never be admitted
   through a general pool sized 1 — it would hang forever.

Both are fixed in this correction:

- `validate_host_admission_policy` now requires a class-reserve total be
  STRICTLY LESS than the aggregate limit whenever any class has a positive
  reserve, guaranteeing `general_limit >= 1` whenever the aggregate is
  enabled — so any check name not explicitly classified (the common case)
  always has a nonzero pool to fall back to.
- It also validates each weight against the MAX of the pools it could
  actually draw from: the general pool, and — for a classified check — its
  own class's reserve. A weight that fits neither is refused at startup.
- `acquire()` now checks `self.limit() == 0` directly as its disabled
  sentinel (never `general.is_none()`), and checks the reserved class lane
  BEFORE ever touching the general pool. A classified request whose weight
  fits its own reserve but exceeds the general pool now gets a genuine
  BLOCKING fallback onto its OWN reserved lane (still FIFO, still bounded)
  instead of falling through to a general pool that could never satisfy
  it — proving the "one reserve try and fallback FIFO" design actually
  holds under the exact shape that broke it, rather than assuming it.
- `class_summary()` gains a per-class `waiting` count (incremented only
  around that new blocking-reserved-lane fallback, never the fast
  non-blocking try or the general pool) — the per-class waiting visibility
  called for alongside the fix.

New regression coverage: 4 unit tests (reserve-total-equal-to-limit
rejected, unclassified-weight-exceeds-shrunken-general-pool rejected,
classified-weight-fitting-only-its-own-reserve accepted, plus the
`class_summary` shape update) and 1 new native e2e test
(`classified_weight_exceeding_the_general_pool_blocks_on_its_own_reserved_lane`)
that actually drives the blocking-fallback path end-to-end with real
barrier processes and polls the new per-class `waiting` field — the
previously-broken shape would have hung this test forever rather than
failed it quickly, so its passing is direct proof the hang is gone.

Weighted production mode remains off (`verification_admission_aggregate_limit`
default `0`); this correction changes no default behavior.

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
  - a class-reserve total is greater than OR EQUAL TO the aggregate limit
    (strictly less-than is required, so the general pool always keeps at
    least 1 unit for any check not explicitly classified — see the REWORK
    correction above), or
  - a configured weight exceeds every pool it could actually draw from
    (the general pool, and — for a classified check — its own class's
    reserve).
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
  after the shared-target lock and per-repo admission): disabled sentinel
  is `self.limit() == 0`, checked directly (not inferred from the general
  pool's own `Option`). If the check's name is a member of a class, FIRST
  try a non-blocking `try_acquire_many_owned` against that class's own
  reserve. That only succeeds when the reserve currently has enough free
  capacity AND the check's weight does not exceed the class's own total
  reserve (a request that could never fit its own lane is never even tried
  against it). If that fast try fails AND this weight exceeds the general
  pool's own capacity, it blocks on its OWN reserved lane instead (FIFO,
  bounded, counted in `class_summary`'s per-class `waiting`) rather than
  falling through to a general pool that could never satisfy it. Otherwise,
  falling through lands in the same bounded general-pool queue every
  unclassified request already uses — a saturated fast lane degrades to
  ordinary shared admission whenever the general pool actually can satisfy
  it, and it never draws more than its configured share.
- **`CheckExecution` gains `check_name: &str`** (empty string for a raw
  inline `run` step with no named-check identity), threaded through from
  every construction site in `managed_verification.rs`, `landing.rs`, and
  `workflow_exec.rs` (15 call sites total) purely to look up this check's
  configured weight/class — an absent or empty name costs the default
  weight 1 and joins no fast lane, identical to pre-P3.2 behaviour.
- **Native status**: `verification_host` gains an additive `classes: {name:
  {limit, executing, waiting}}` map (empty unless a class policy is
  configured); `limit`/`executing`/`waiting` at the top level keep their
  P3.1 meaning exactly — with no class policy configured, `executing` is
  exactly the general-pool figure, unchanged from P3.1. With a class policy
  configured, `executing` is the TRUE total (general + every reserved
  lane), so the aggregate ceiling `limit` and `executing` stay comparable
  at a glance. Per-class `waiting` counts ONLY the new blocking-reserved-
  lane fallback (see the REWORK correction above) — never the fast
  non-blocking try or the general pool's own `waiting`.

## Evidence

- Unit tests (`managed_verification.rs::tests`, 12 total: 8 original + 4
  from the REWORK correction): weight-0 rejected, reserve-total STRICTLY
  exceeding or EQUAL TO the limit both rejected (and proven non-partial),
  an unclassified weight exceeding the shrunken general pool rejected, a
  classified weight that fits only its own reserve accepted, a no-op empty
  policy leaves `executing`/`class_summary` unchanged, and a bare
  `set_limit` resets a previously configured class policy.
- Real native CLI/daemon e2e (`crates/rk-daemon/tests/host_verification_weighted_fair.rs`,
  4 tests total: 3 original + 1 from the REWORK correction, barrier-controlled
  fixture checks across registered repos, polled via the native `status`
  RPC to a bounded deadline — same technique as `host_verification_aggregate_cap.rs`):
  - `weight_two_consumes_two_of_an_aggregate_two_cap` — atomic weight,
    shared host-wide across repos.
  - `guard_class_progresses_while_general_pool_is_saturated` — the
    motivating scenario: a guard-class check runs concurrently with a long
    check that has saturated the general pool, while an ORDINARY second
    unclassified request still queues normally (the reserve is bounded, not
    a blanket bypass).
  - `reserved_lane_saturated_falls_through_to_the_general_pool` — a
    class's own reserve exhausting itself, while still fitting the general
    pool, degrades to the shared queue rather than blocking indefinitely.
  - `classified_weight_exceeding_the_general_pool_blocks_on_its_own_reserved_lane`
    — the REWORK regression: a classified check whose weight fits ONLY its
    own reserve, never the general pool, is admitted via the new blocking
    fallback rather than hanging forever; proves the per-class `waiting`
    field.
- Regression: the full pre-existing P3.1 suite
  (`host_verification_aggregate_cap.rs`, 5 tests) and every `managed_verification::`
  (20), `workflow_exec::` (67), and `landing::` (136) unit test in
  `rk-daemon` still pass unchanged.

## Scope explicitly NOT covered here

- P0's in-flight check-sharing/coalescing is not imported.
- P4 build routing and experiments are a separate ticket.
- No live-reload: like every other admission limit in this file, changing
  weight/class/reserve config requires a daemon restart.
- No CLI surface beyond `config.toml` and the existing native `status` RPC
  was added — an operator inspects the new `classes` field the same way
  `verification_host` is already inspected today.

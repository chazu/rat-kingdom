# TKT-nibuv-gokun-sibin (P4.1): admit the paired release build under configurable host capacity

## What this ticket asked for

Route the existing native `release.prepare` recipe `paired-rk-mcp` through
P3.1's already-delivered `HostVerificationAdmission` aggregate host-wide cap
(`[policy] verification_admission_aggregate_limit`), the same one every
managed named check already shares — behind an explicit, operator-controlled
config switch that preserves existing defaults exactly. This is the
independently-shippable P4 first slice (`docs/2026-09-13-continuous-validation-promotion.md`,
track table row P4): "Route one named artifact build through host admission
with bounded children and visible status." Full P4 experimentation/general
recipe migration remains on the parent ticket.

## Base state at implementation time

This branch forked from `main` at `830574d`, which carries P3.1 only.
`HostVerificationAdmission::acquire()` on `main` today is the plain
zero-argument P3.1 signature (`crates/rk-daemon/src/managed_verification.rs`)
— a single aggregate semaphore, no per-check weight or class concept. P3.2
(weighted classes and fair progress, `TKT-nasif-danob-sirok`, finding
`01M2HTEKQEY1DEQJR2QGCH5NK9`) is accepted-if-passed and, at the time this
slice landed, was running its final main gate on a separate candidate — not
yet merged. This slice is built against the delivered P3.1 interface only,
per the ticket's own explicit instruction to implement the first usable
route against delivered P3.1 now rather than waiting for or importing the
unaccepted P3.2 commit.

## What landed (this branch)

- **Config** (`rk-core/src/config.rs`, `PolicyConfig`): one new field,
  `release_build_admission_enabled: bool`, default `false` — zero behaviour
  change for a daemon that never sets it. Read once at daemon startup
  (`Daemon::new`, `server.rs`), same restart-required-to-change convention
  as every other admission field in this file; no live-reload. Disable by
  reverting to `false` and restarting — this never removes or rewrites an
  already-`Prepared` release.
- **Wiring** (`server.rs::handle_release_prepare`): when the switch is on,
  passes `Some(&supervisor.verification_resources().host_admission)` — the
  exact same `HostVerificationAdmission` instance every managed named check
  already acquires from — into `release::prepare`/`run_recipe`. When off,
  passes `None`, preserving the unmanaged legacy path exactly (build spawns
  unconditionally, as it always has).
- **`release.rs::run_recipe`**: acquires one host permit, bounded by a new
  30-minute `ADMISSION_WAIT_TIMEOUT` (distinct from the existing 20-minute
  `BUILD_TIMEOUT`, which bounds the build execution itself once admitted —
  "bounded wait/execution deadlines" as two separate bounds). The permit is
  acquired immediately before spawning the build subprocess and dropped
  (plain RAII, no explicit release call — same convention
  `ManagedVerification::run` uses for its own `_host_guard`) the instant the
  subprocess's own output collection finishes, so the slower binary-copy and
  smoke-check steps that follow never hold the aggregate slot. A timed-out
  wait fails the whole `prepare` call with a clear error, which the existing
  `prepare()` match arm already durably records as `ReleaseStatus::Failed` —
  no new failure-state plumbing needed. Nothing about the immutable selected
  source (`resolved_commit`/`tree_sha`, already frozen in `PrepareParams`
  before `prepare` is ever called) changes while waiting.
- **Legacy static reservation**: a documented, hardcoded weight of exactly
  1 aggregate unit (`RELEASE_ADMISSION_WEIGHT`), under a fixed,
  non-repo-supplied identity string `"release-build:paired-rk-mcp"`
  (`RELEASE_ADMISSION_IDENTITY`) — never derived from a repository's own
  `.rk/checks.cue`, so no workflow- or repo-supplied definition can relabel
  this build as cheap. A heavier cost is deliberately NOT approximated by
  acquiring more than one permit per build: `HostVerificationAdmission::acquire()`
  grants exactly one permit per call, and calling it more than once per
  build would require holding a partial reservation while awaiting the
  rest — the same recursive/partial-hold pattern that can deadlock two
  heavy builds each holding one unit and waiting on another under a tight
  aggregate cap. See the "P3.2 compatibility" section below for how this
  becomes a real configurable weight once that interface lands.
- **Telemetry**: `ReleaseManifest.recipe_bounds` gains a new
  `host_admission: Option<HostAdmissionBounds>` field — `None` when the
  switch was off for this build (the pre-P4.1 shape, unchanged). When on:
  `recipe_identity`, `weight`, `admission_wait_ms`, and `build_run_ms`,
  durably inspectable via `rk release show` alongside every other
  provenance field already there (source, recipe, config_provenance).
  `recipe_bounds.enforcement_note` is updated to say so when admission was
  enabled for that build, and left exactly as it always read when it
  wasn't. No new status RPC surface: a release build's wait/execution is
  already visible for free in the existing `status` RPC's
  `verification_host.{limit,executing,waiting}` (P3.1), since it now
  genuinely competes for the identical semaphore instance — an operator
  watching that field already sees release-build occupancy alongside every
  named check's, with no change to that RPC's own shape.
- **Ownership/cancellation**: unchanged. `release.prepare`'s existing
  process-wide `release_prepare_lock` single-flight, persistent staging
  worktree reuse, and durable `Preparing → Prepared/Failed` state machine
  are untouched — this adds one bounded `await` before the build subprocess
  spawns, nothing else. `release.prepare` had no caller-disconnect
  cancellation before this change (a dropped RPC connection does not
  interrupt an in-flight build; confirmed by inspection — no
  `ManagedVerificationRuns`-style registration exists in `release.rs`) and
  still doesn't; this ticket preserves that pre-existing behavior rather
  than introducing new cancellation semantics, per its own instruction to
  "coordinate lifecycle interfaces instead of creating another shutdown
  controller." No detached live builder outlives its own permit: the
  permit is a plain `OwnedSemaphorePermit` dropped via ordinary Rust scope
  rules on every exit path (success, build failure, admission timeout, or
  the enclosing future itself being dropped).

## Evidence

- New tests: `crates/rk-daemon/tests/release_prepare.rs::host_admission`
  (3 tests, real daemon + real tiny two-package Cargo fixture, reusing that
  file's existing dependency-free fixture rather than rebuilding a whole
  project):
  - `disabled_by_default_ignores_a_saturated_aggregate_cap` — a release
    build completes while a barrier-controlled named check on a SEPARATE
    repo holds the aggregate cap's one and only permit open for the whole
    test, proving the default switch position genuinely ignores admission
    (not merely "usually fast enough").
  - `enabled_shares_the_aggregate_cap_with_a_concurrent_named_check` — the
    motivating scenario: with the switch on, a release build genuinely
    queues (`verification_host.waiting == 1`, proven via the real `status`
    RPC, never inferred from elapsed time) behind a named check on a
    different repo holding the same semaphore's one permit, then proceeds
    once released; asserts the manifest's recorded `admission_wait_ms > 0`
    and the aggregate capacity fully drains afterward (no permit leak).
  - `enabled_with_aggregate_cap_disabled_never_waits` — the switch being on
    has no effect while the aggregate cap itself stays at its `0` default,
    matching every named check's own documented behavior.
- Regression: the full pre-existing `release_prepare.rs` suite (10 tests)
  and the full P3.1 `host_verification_aggregate_cap.rs` suite (5 tests)
  pass unchanged. `cargo test -p rk-core -p rk-daemon --lib` (1058 unit
  tests combined) passes unchanged. `cargo clippy -p rk-daemon -p rk-core
  --all-targets -- -D warnings` is clean. `cargo fmt --check` is clean on
  every file this ticket touched.

## P3.2 compatibility point (published live on BBS, artifact `01M2HVPCK3SR7BQHCQM4J94227`, finding `01M2HVPKPM9YNW6F5Y1F84S0HR`)

P3.2 changes `HostVerificationAdmission::acquire()` to
`acquire(check_name: &str)`, doing an internal weight/class lookup keyed on
that name (an empty name costs the default weight 1 and joins no class —
byte-identical to pre-P3.2 behavior). Once P3.2 actually lands on `main`,
adapting this slice is a ONE-LINE change at `release.rs`'s single call
site: `host_admission.acquire()` becomes
`host_admission.acquire(RELEASE_ADMISSION_IDENTITY)`, turning the already-
fixed, already-non-repo-supplied identity string this slice records for
telemetry today into a real `config.toml`-keyable weight/class lookup. No
other call site, struct field, or test shape changes — `HostAdmissionBounds`
already carries a `weight` field (currently always the static constant `1`)
that would simply start reflecting a configured value instead. This slice
does not import the unaccepted P3.2 commit and does not claim weighted
routing works today.

## Scope explicitly NOT covered here

- General experiment/recipe migration beyond the one `paired-rk-mcp` route
  (parent P4 track).
- Weighted-class admission for the release build (blocked on P3.2 landing
  on `main`; see compatibility point above).
- New caller-disconnect cancellation for `release.prepare` (pre-existing
  gap, out of this ticket's scope — coordinating with Munch-16's separate
  daemon-shutdown-during-active-review work, `TKT-karut-jaraf-hivur`,
  rather than building a second shutdown controller).
- A dedicated fixture proving the 30-minute `ADMISSION_WAIT_TIMEOUT` itself
  fires — the underlying `tokio::time::timeout` wrapping a
  `Semaphore::acquire_owned` future is the same primitive P3.1's own
  per-check admission-wait timeout already relies on and already has
  dedicated coverage for (`repo_specific_cap_still_holds_under_a_higher_aggregate_cap`
  in `host_verification_aggregate_cap.rs`); re-proving the primitive here
  against a real 30-minute wait was judged not worth an impractically long
  or artificially-shortened-for-the-test-only constant.

# TKT-ratik-rivam-jadud (P5.1): integration/release roles and a `release.select` candidate journey

## What this ticket asked for

The full parent ticket (`TKT-jibot-polur-majad`, P5: "Separate ongoing
integration from frozen release validation") lists five acceptance points:
policy roles + native status, focused-check routing, immutable coalesced
selection, aggregate admission with oldest-admissible-prefix overflow
handling, and a native two-branch barrier fixture proving all of the above
together. That is the full P5 track, not one bounded worker attempt — the
design doc's own decomposition table
(`docs/2026-09-13-continuous-validation-promotion.md`, section 11, row P5)
names the FIRST independently useful delivery narrowly: "Expose
integration/release roles and select one frozen release candidate while
later work integrates through existing gates." This ticket delivers exactly
that first slice, per its own explicit instruction to surface a concrete
independently-useful split rather than spend the bounded cap on the whole
track. See "Scope explicitly NOT covered here" below for what is retained on
a follow-up.

## Base state at implementation time

This branch forked from `main` at `64fb844`, which carries P3.1 (aggregate
host admission) and P4.1 (release build routed through that admission,
`release_build_admission_enabled`) only. P3.2 (weighted admission classes,
`TKT-nasif-danob-sirok`) is confirmed NOT on `main` at this commit —
`HostVerificationAdmission::acquire()` is still the plain zero-argument P3.1
signature. This slice does not touch admission weighting at all and has no
dependency on P3.2 landing.

Before this ticket, `release.rs`'s `ReleaseManifest`/`ReleaseIndexEntry` were
already content-addressed by `(repo, resolved_commit, recipe, recipe_revision)`
— the same exact commit always yields the same release id, and a manifest is
digest-verified on every read. There was no policy-level distinction between
an "integration" branch and a "release" branch anywhere: `release.prepare`
always required an operator-supplied `--candidate` (branch/tag/sha), and
`RepositoryPolicy` had no concept of a release role at all.

## What landed (this branch)

- **Policy** (`rk-workflow/src/lib.rs`, `RepositoryPolicy`): a new
  `release: ReleasePolicy` field with `integration_branch`/`release_target`
  (CUE `integrationBranch`/`releaseTarget`), both plain `String`, both
  defaulting to `""`. Both empty (the default) disables the role split
  entirely — the same "empty string disables" convention
  `LandingPolicy::shadow_review_model` already uses, so an unconfigured or
  already-registered repo's behavior is byte-for-byte unchanged. Validated
  in `validate_repository_policy`:
  - setting exactly one of the two fields fails activation (fails closed,
    never silently "looks active" with one field);
  - the two branches must differ;
  - both must be valid git branch names (`git check-ref-format`);
  - `release_target` must also appear in `landing.protected_targets` — an
    immutable release snapshot always targets a genuinely protected edge,
    never an inner one. Naming a branch here grants it no new authority: the
    edge's own protected-path/review gates are completely unchanged.
  - CUE schema (`repository-policy-schema.cue`) updated to match; the schema
    struct is closed, so an operator's `.rk/repo.cue` declaring a `release:`
    block would otherwise be rejected outright.
  - **Activation semantics**: unlike `rk-core::config::PolicyConfig` fields
    (daemon-wide TOML, read once at startup, restart-required),
    `RepositoryPolicy` is per-repo CUE activated through the existing
    digest-fenced `rk repo onboard start/propose/approve/apply/activate`
    flow (or picked up directly at `repo.add` time if `.rk/repo.cue` is
    already present, as every other field on this policy already works) —
    no daemon restart needed. **Disable/recovery path**: clear both fields
    (or remove the `release:` block) in `.rk/repo.cue` and re-run the same
    onboarding activation flow; this never touches an already-selected
    release, which stays exactly as immutable as it always was.
- **`release.select` RPC + `rk release select --repo R [--recipe NAME]`**
  (`server.rs`, `rk-cli/src/release_cmds.rs`): requires an activated repo
  policy with both `release.integrationBranch` and `release.releaseTarget`
  set; resolves the integration branch's CURRENT head commit and delegates
  to the exact same code path `release.prepare` already uses. `server.rs`'s
  `handle_release_prepare` body was factored into a shared
  `run_release_prepare(repo, candidate, recipe)` helper so `release.select`
  reuses 100% of `release.prepare`'s tested machinery — content-addressed
  identity, the process-wide `release_prepare_lock` single-flight, P4.1
  admission wiring, and caller-disconnect cancellation via
  `dispatch_watching_disconnect`/`ManagedVerificationRuns` (added to the
  same watched-method list as `verify.run`/`release.prepare`). No new
  immutability, admission, or cancellation logic was written — only new
  candidate resolution. Operator-only, same as `release.prepare` (absent
  from `capabilities.rs::method_policy`, which fails closed for every
  non-operator caller by default), matching the ticket's "King owns policy
  activation, release preparation and installation" direction.
- **Immutability guarantee**: comes entirely from `prepare()`'s pre-existing
  content addressing, unmodified. Calling `select` again against an
  unchanged integration branch head reuses the existing `Prepared` entry
  (`already_prepared: true`, same release id). Calling it again after new
  commits land on the integration branch resolves a DIFFERENT commit and
  therefore produces a SEPARATE, independently immutable release id — the
  prior selected candidate's manifest, digest, and binaries are never
  touched. This is exactly "later integration must not mutate the selected
  candidate," proved directly in the new tests below rather than assumed.
- **At-most-one-running**: the existing process-wide `release_prepare_lock`
  already serializes every `release.prepare`/`release.select` call
  daemon-wide (not just per-repo/stream), so two concurrent selections
  cannot run their builds simultaneously today. This is a real, tested
  safety property this slice relies on rather than reimplements — but it is
  coarser than the full track's "one running + one coalesced pending with
  supersession evidence" (a concurrent second call queues on the mutex FIFO
  rather than coalescing to a single pending marker). See "Scope explicitly
  NOT covered here."
- **`release.status` RPC + `rk release status --repo R`** (`server.rs`,
  `release.rs::id_for`, `rk-cli`): a genuinely new, read-only surface tying
  the activated role together — `integration_branch`/`integration_head`
  (resolved live, same code path `select` uses), `release_target`/
  `release_target_head` (resolved live; `null` rather than an error if it
  does not resolve), the selected `Prepared` release for the CURRENT
  integration head if one exists (via the new `release::id_for`, a
  read-only deterministic id computation added specifically so this never
  has to call `prepare`/`select` to check), and
  `integration_head_prepared: bool`. **Precise meaning, named and documented
  exactly per a verified operator correction**: this reports only
  `ReleaseStatus::Prepared` — an immutable artifact inventory entry exists
  for this exact commit. It does NOT mean accepted, deployed, or enabled;
  those are distinct states other subsystems own (`rk feature show/set`, a
  future release-activation slice) and this field must never be read as
  implying any of them. This makes `release_target` actually READ at
  runtime, not only validated once at policy-activation time — closing the
  gap where it was previously write-only. It still never advances, lands,
  or authorizes anything on `release_target`; that branch's own
  protected-path/review gates are completely unaffected, proved directly
  in `status_reports_integration_and_release_target_heads`
  (`release_target_head` is asserted unchanged after a `select` call).
- **What the admission-sharing evidence actually proves, precisely scoped**
  (corrected after two rounds of verified operator review — the first found
  the original evidence only proved sequential, not concurrent, behavior;
  the second found the replacement concurrency claim itself overstated).
  `select_shares_the_aggregate_admission_permit_with_a_concurrent_named_check`
  configures the daemon exactly as this ticket's own stated production base
  state (`release_build_admission_enabled: true`,
  `verification_admission_aggregate_limit: 1`) and proves: `release.select`'s
  build genuinely queues behind, and is admitted alongside, an ordinary
  named check (`verify.run`) on a SEPARATE repo through the SAME shared
  aggregate semaphore — the exact property
  `release_prepare.rs::host_admission::enabled_shares_the_aggregate_cap_with_a_concurrent_named_check`
  already proves for `release.prepare`, now confirmed preserved unchanged
  through `select`'s new integration-branch candidate resolution. **This is
  NOT** a proof that integration continues unblocked while a release
  validates — under this real, currently-deployed configuration, a release
  build DOES consume shared capacity a concurrent named check would need,
  and vice versa; they contend, they are not isolated. An earlier version of
  this evidence, built only against the admission-DISABLED default with a
  real (and, under host contention, unreliably slow) `cargo build` barrier,
  claimed the opposite ("never blocked behind a held release build") — that
  claim was true only for the untested-in-production default configuration
  and has been removed. Whether integration should be isolated from release
  build admission under production's actual settings is exactly the kind of
  question retained on the follow-up, not resolved here.
- **Integration continues through existing gates, unchanged**: this slice
  does not touch `landing.rs`, `LandingPolicy`, or focused-check routing at
  all. Ordinary deliveries onto the configured integration branch use
  whatever landing policy that repo already has — the ticket's "later work
  integrates through existing gates" is satisfied by NOT changing that
  path, not by adding a new one. This is a narrower claim than "integration
  is never slowed by a release build" (see above): the landing PATH is
  unchanged; whether it contends for shared admission capacity with an
  in-flight release build depends on the aggregate admission configuration,
  proved precisely above, not assumed.

## Evidence

- `crates/rk-workflow/src/lib.rs` unit tests (6 new, `cargo test -p
  rk-workflow`): defaults preserve existing behavior including the new
  fields; a fully-configured release role round-trips through real CUE
  export; half-configured, identical-branch, and release-target-outside-
  protected-targets are each rejected with an actionable message. Full
  existing `rk-workflow` suite (69+ tests) passes unchanged.
- `crates/rk-daemon/tests/release_select.rs` (6 new, real daemon + the same
  tiny dependency-free paired Cargo fixture `release_prepare.rs` already
  uses):
  - `select_without_any_activated_policy_reports_the_existing_inactive_repo_error`
    — an unregistered-policy repo gets the SAME pre-existing
    "no activated .rk/repo.cue policy" error `agent.spawn` already reports,
    not a bespoke message.
  - `select_reports_release_role_not_configured_when_policy_is_active_but_unset`
    — an activated policy with the release role left at its empty default
    (a valid, activatable policy) fails closed with an actionable message
    distinct from "not activated at all."
  - `select_resolves_the_integration_branch_head_and_is_idempotent` — the
    integration branch is deliberately diverged from `main` before the
    first call; asserts the resolved commit is the integration branch's
    head, never `main`'s, and a repeat call against an unchanged branch is
    idempotent (`already_prepared: true`, identical release id).
  - `later_integration_never_mutates_an_already_selected_candidate` — after
    a first selection, a further commit lands on the integration branch; a
    second `select` produces a distinct release id for the new commit, and
    the FIRST release is re-fetched via `release.show` and asserted to
    still report `prepared`/`content_verified: true` with its ORIGINAL
    frozen commit — the actual immutability property under real content
    drift risk, not just "the code path is unchanged."
  - `select_shares_the_aggregate_admission_permit_with_a_concurrent_named_check`
    — the precisely-scoped admission evidence described above, configured
    exactly as production's actual base state (admission enabled, aggregate
    cap 1): a barrier-held `verify.run` on a separate repo, `release.select`
    proven to genuinely queue behind it on the shared permit
    (`host_executing == 1 && host_waiting == 1`, via the real `status` RPC),
    then admitted and completing with nonzero recorded
    `admission_wait_ms`. Uses the same cheap shell-command barrier
    `release_prepare.rs::host_admission` already relies on, not a real
    `cargo build` — faster and not contention-prone under host load.
  - `status_reports_integration_and_release_target_heads` — `release.status`
    before and after a `select` call: `integration_head_prepared` flips
    from `false` to `true`, `release_target_head` is asserted unchanged
    across the call.
  - Regression: the full pre-existing `release_prepare.rs` suite (15 tests,
    including the P4.1 `host_admission` module) and `repository_policy.rs`
    (3 tests) pass unchanged after the `handle_release_prepare` refactor.

## Scope explicitly NOT covered here (retained on the parent P5 track)

- **Single coalesced pending candidate with supersession evidence.** Today
  a second concurrent `select` call queues FIFO on the daemon-wide
  `release_prepare_lock` rather than being recorded as one coalesced
  "pending" marker that overwrites an older pending record — correct
  (nothing runs concurrently, nothing is silently dropped) but not the
  throughput optimization the full track describes. Needs a small persisted
  selection-state record distinct from `ReleaseIndexEntry`.
- **Aggregate file/line admission with oldest-admissible-prefix selection
  and visible overflow disposition** (acceptance point 4 on the parent).
  This slice's `select` always resolves the CURRENT integration head
  unconditionally; it does not evaluate `landing-diff-scope`-style aggregate
  budgets across the constituent commits since the last release, retain
  overflow for a later snapshot, or give an individually-oversized
  constituent a visible disposition. Needs its own bounded slice against the
  existing diff-scope check machinery.
- **Whether integration should be isolated from release-build admission
  contention.** Proved precisely above: under production's actual
  configuration (admission enabled, aggregate cap 1), a `release.select`
  build and an ordinary named check genuinely contend for the same shared
  permit — a held release build can delay a concurrent check, and vice
  versa. Whether that is acceptable, or whether `release.select` should
  reserve separate capacity or a distinct admission class from ordinary
  landing checks, is an open design question this slice deliberately does
  not resolve; it only makes the actual current behavior observable and
  correctly documented instead of assumed.
- **Protected-path authority reuse for the release edge** (the parent's
  "prior integration approval alone cannot authorize the release edge; use
  existing authority checks and explicitly surface a missing/stale
  approval"). `release_target` is now READ and reported live
  (`release.status`), but `release.prepare`/`release.select` still do not
  LAND anything onto it themselves — they only build an immutable artifact
  inventory entry, exactly as `release.prepare` always has — so this slice
  has no release-edge landing action to authorize yet. Applies once a later
  slice adds an actual advancement of `release_target` to the selected
  candidate; that advancement is release ACTIVATION, a distinct capability
  from artifact preparation in the design doc's own track table (P7:
  "Explicitly activate and roll back a compatible bundle"), not something
  `release.prepare` has ever done.
- **The full native two-branch barrier fixture** (parent acceptance point
  5): a full release check held at a deterministic barrier while a second
  candidate integrates through its focused check, proving frozen
  SHA/membership, one-running/one-pending coalescing, restart
  reconstruction of pending state, stale-policy/candidate rejection, and two
  admissible changes exceeding a 3000-line aggregate progressing as separate
  prefixes. This slice's tests prove the narrower, already-delivered
  immutability/idempotency/policy-validation properties directly against a
  real daemon and real builds; the fuller barrier scenario depends on the
  coalescing and aggregate-admission slices above existing first.

Follow-up ticket: `TKT-gogas-ponoh-jivog` (filed against the P5 parent,
`TKT-jibot-polur-majad`), carrying exactly the four bullets above.

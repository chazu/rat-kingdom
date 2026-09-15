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
- **Observation**: no new status RPC was added. `release.select`'s own
  response, plus the existing unchanged `release.list`/`release.show` (`rk
  release list`/`rk release show`), are the observation surface for a
  selected candidate — the ticket's "one real operator journey selecting and
  observing" is satisfied by composing existing, unmodified surfaces rather
  than building a new one. `repo.get`/`rk repo get` also surfaces the
  activated `release.integrationBranch`/`releaseTarget` roles for free,
  since `RepoRecord`'s full `activated_policy` already serializes on that
  RPC and required no handler change.
- **Integration continues through existing gates, unchanged**: this slice
  does not touch `landing.rs`, `LandingPolicy`, or focused-check routing at
  all. Ordinary deliveries onto the configured integration branch use
  whatever landing policy that repo already has — the ticket's "later work
  integrates through existing gates" is satisfied by NOT changing that path,
  not by adding a new one.

## Evidence

- `crates/rk-workflow/src/lib.rs` unit tests (6 new, `cargo test -p
  rk-workflow`): defaults preserve existing behavior including the new
  fields; a fully-configured release role round-trips through real CUE
  export; half-configured, identical-branch, and release-target-outside-
  protected-targets are each rejected with an actionable message. Full
  existing `rk-workflow` suite (69+ tests) passes unchanged.
- `crates/rk-daemon/tests/release_select.rs` (4 new, real daemon + the same
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
- **Protected-path authority reuse for the release edge** (the parent's
  "prior integration approval alone cannot authorize the release edge; use
  existing authority checks and explicitly surface a missing/stale
  approval"). `release.prepare`/`release.select` do not land anything onto
  `release_target` themselves — they only build an immutable artifact
  inventory entry, exactly as `release.prepare` always has — so this
  slice has no release-edge landing action to authorize yet. Applies once a
  later slice adds an actual advancement of `release_target` to the
  selected candidate.
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

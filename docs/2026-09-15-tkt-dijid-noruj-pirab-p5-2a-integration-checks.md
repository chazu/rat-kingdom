# TKT-dijid-noruj-pirab (P5.2a): configured integration landings run the right checks

## What this ticket asked for

The parent P5.2 (`TKT-gogas-ponoh-jivog`) retains, from the original P5
clarification, that "activated policy must actually route worker bases and
delivery targets consistently" and that "integrated work runs
repository-owned focused checks bound to candidate/target/base, with
conservative broad fallback for unknown/workspace inputs." P5.1
(`TKT-ratik-rivam-jadud`) delivered the policy role and `release.select`
candidate resolution but explicitly left both of those gaps open — its own
doc says "this slice does not touch `landing.rs`, `LandingPolicy`, or
focused-check routing at all," and `Supervisor::spawn` never read
`release.integrationBranch`. This slice (P5.2a) closes exactly those two
gaps: routing and checks, not shared-resource concurrency or coalesced
release selection (both remain on the P5.2 parent).

## Base state at implementation time

This branch forked from `main` at `bc2d46d`, which carries P5.1
(`release.select`/`release.status`, `a38d26a`) and the activated-policy
onboarding commit (`55cc022`). Before this ticket:

- `Supervisor::spawn`'s base resolution (`delivery_target(&repo.current_branch()?)`)
  never consulted `release.integrationBranch` — an unbased worker on a
  release-activated repo forked from whatever the discovered repo's working
  copy happened to have checked out, not the configured integration branch.
- `LandingPipeline::gate_plan` only distinguished `ProtectedFinal` (a
  `landing.protectedTargets` entry — full check, always) from `Inner`
  (everything else — runs whatever `focusedChecks` selects, or nothing at
  all if no rule matches). The activated integration branch landed through
  the generic `Inner` path: a `focusedChecks` rule that matched some but not
  all of a changeset's paths still ran only the matched checks, silently
  leaving the rest of the diff unchecked.
- `select_focused_checks` selects checks the moment ANY rule matches ANY
  changed path — it never asked whether EVERY changed path was covered.
- `scripts/verify-changed.sh` read `RK_VERIFY_BASE` for its own diff base but
  the landing pipeline never set it (only `RK_CHECK_TARGET`, which that
  script does not read), so a `verify-changed` named check selected on a
  non-`main` edge silently defaulted to `git merge-base HEAD origin/main` —
  the wrong diff for any edge whose target isn't `main`.
- `scripts/verify-changed.sh`'s own `cargo clippy`/`cargo nextest` invocations
  had no `--jobs`/`--build-jobs`/`--test-threads` bound at all, unlike
  `verify-full.sh`'s documented fixed `verify_jobs=4`.

## What landed (this branch)

- **`RepositoryPolicy::spawn_base(role, current_branch)`** (`rk-workflow/src/lib.rs`):
  new method alongside `delivery_target`. When `release.integrationBranch` is
  non-empty and `role != "reviewer"`, returns the integration branch; else
  defers to the pre-existing `delivery_target`. `Supervisor::spawn`'s base
  resolution (`supervisor.rs`) now calls this instead of `delivery_target`
  directly — the only change to that call site. A native reviewer is always
  spawned with an explicit `branch:` (the candidate under review — see
  `landing.rs`'s `write_review_workflow` fixture, `branch: _input.branch`),
  so excluding `role == "reviewer"` from this routing is defense in depth,
  not a currently-exercised path; an explicit `--base`/`branch:` still always
  wins regardless of role, unchanged.
- **Reject incompatible activation before mutation**
  (`validate_repository_policy`, `rk-workflow/src/lib.rs`): activating
  `release` now also requires `delivery.target == "agent-base"`. A fixed
  (non-`"agent-base"`) delivery target ignores the branch a worker actually
  forked from — combined with the routing above, it would silently ship a
  worker's delivery somewhere other than the integration branch it was just
  forked from. This is checked at policy-activation time (same function that
  already validates `integrationBranch`/`releaseTarget` pairing), so an
  incompatible combination is rejected before it is ever activated, not
  discovered later at spawn time.
- **`LandingEdgeClass::Integration`** (`landing.rs`): a third edge class,
  selected when `target == release.integrationBranch` (populated onto
  `GateConfig` from the same activated policy `gate_config()` already reads).
  Distinct from `Inner`; the pre-existing generic `Inner` path (any target
  that is neither protected-final nor the release-integration branch) is
  completely untouched — same selection function, same "no rule matched ->
  run nothing beyond the two policy gates" behavior it always had.
- **`select_integration_checks`** (`landing.rs`): the integration edge's own
  selector. Requires every changed path be covered by at least one matching
  rule that ALSO names at least one check (a rule that matches but names zero
  checks does not count as coverage — the "invalid matching rule" case).
  Returns `Covered` (full coverage — may be an empty check list if there
  were no changed paths, or every covering rule named zero checks) or
  `Fallback(reason)` for: no `focusedChecks` rules configured at all, or any
  changed path left uncovered. `gate_plan` runs the `Fallback` case's full
  `check_name` check (same one `ProtectedFinal` runs) instead of the
  `Inner` edge's silent "run nothing" — this edge never lands unchecked.
  "Missing checks" (a selected or fallback check name absent from
  `checks.cue`) was already an explicit `Err` via the pre-existing `find()`
  helper and needed no change; it now applies to this edge the same way it
  always applied to `Inner`/`ProtectedFinal`.
- **`RK_VERIFY_BASE` bound alongside `RK_CHECK_TARGET`** (`gate_plan`,
  `landing.rs`): every check `gate_plan` schedules beyond the two policy
  gates (the protected-final full check, an integration edge's selected or
  fallback checks, and a generic inner edge's selected checks) now also
  receives `RK_VERIFY_BASE=<target>`. Purely additive — a check command that
  never reads the variable is unaffected — but it means `verify-changed.sh`'s
  own diff-base selection is now bound to the exact target/candidate this
  edge is gating, never `origin/main` by coincidence.
- **`scripts/verify-changed.sh` bounded parallelism**: `verify_jobs=2` (a
  fixed constant, not an env override — same untrusted/mistaken-caller-
  landing-in-a-gate rationale `verify-full.sh` documents for its own
  `verify_jobs=4`), applied to `cargo clippy --jobs` and
  `cargo nextest run --build-jobs --test-threads`. This is the documented
  supported production configuration for this script.

## Evidence

- `crates/rk-workflow/src/lib.rs` (6 new unit tests): `spawn_base` routes to
  the integration branch when activated, defers to `delivery_target`
  otherwise (including on an unconfigured repository, byte-identical to the
  old call site's behavior), and never reroutes `role == "reviewer"`.
  Activation with a fixed `delivery.target` alongside an activated release
  role is rejected with an actionable message; the default (`"agent-base"`)
  activates cleanly.
- `crates/rk-daemon/src/supervisor.rs` (4 new `respawn_tests` — a real
  `Supervisor::spawn_async` against a real git repo, not just the policy
  method in isolation): an unbased spawn on an activated release-role repo
  forks from `integration`, not whatever `main` had checked out; an explicit
  `--base` still wins; a `role: "reviewer"` spawn with no explicit base still
  forks from the checked-out branch; an unregistered/unconfigured repo is
  byte-for-byte unaffected.
- `crates/rk-daemon/src/landing.rs` (4 new tests, real git repos + real
  `checks.cue`, the same `nested_child_to_parent_to_main_runs_focused_then_full_check`
  pattern this file already used for `Inner`/`ProtectedFinal`):
  - `integration_edge_with_full_path_coverage_runs_only_focused_checks` —
    every changed path covered: only the focused check runs, `edge_class:
    "integration"`, `full_check_required: false`.
  - `integration_edge_with_no_focused_checks_falls_back_to_the_full_check` —
    no rules configured at all: the full check runs, never "nothing."
  - `integration_edge_with_partially_covered_changed_paths_falls_back_to_the_full_check` —
    one of two changed files matched, the other not: falls back to the full
    check rather than running the partial focused check alone (proves "one
    known path does not establish coverage of all changed inputs" is
    actually closed, not just documented).
  - `selected_checks_receive_rk_verify_base_bound_to_the_candidate_target` —
    a check command reads `$RK_VERIFY_BASE` back out to a marker file;
    asserted equal to the candidate's actual target.
- `crates/rk-core/tests/mise_verify_env.rs`'s existing
  `changed_test_runner_strips_the_full_strip_rk_spawn_environment` passes
  unchanged against the rewritten `verify-changed.sh` (env stripping moved
  into an array variable, same canonical flag set).
- Full existing `rk-workflow` (79 tests) and the `landing`/`supervisor`
  modules of `rk-daemon` pass unchanged alongside the new tests (targeted
  runs; a full-workspace sweep is deferred to the automatic landing gate per
  this ticket's own delivery instructions).

## Activation example (RK-owned; not applied to this repo's own `.rk/` by this branch)

Root/King owns activating this repo's own `.rk/repo.cue`/`.rk/checks.cue` —
this branch only prepares the exact reviewable diff below as a companion
patch, per the ticket's "does not widen authority or force it through worker
landing" instruction. Given the P5.1 activation (`integration` -> `main`)
already documented in
`docs/2026-09-15-tkt-ratik-rivam-jadud-p5-1-release-roles.md`, the additional
`.rk/repo.cue` needed for THIS ticket's routing+checks behavior is just a
`focusedChecks` rule naming a real check for the paths that matter:

```cue
// .rk/repo.cue
repo: {
	landing: {
		protectedTargets: ["main"]
		focusedChecks: [
			{
				class:  "rust"
				paths:  ["\\.rs$", "^Cargo\\.(toml|lock)$", "^crates/"]
				checks: ["verify-changed"]
			},
		]
	}
	release: {
		integrationBranch: "integration"
		releaseTarget:     "main"
	}
	// delivery.target defaults to "agent-base" already — required as-is by
	// `validate_repository_policy` once `release` is activated; do not set
	// delivery.target to a fixed branch alongside this block.
}
```

`.rk/checks.cue`'s existing `verify-changed` entry (already shipped in this
repo, wrapping `scripts/verify-changed.sh` via `mise run verify`) needs no
change to be nameable here — it is exactly the named check this rule
selects. Once activated: an ordinary implementation worker spawned with no
`--base` on this repo forks from `integration` instead of whatever was
checked out; a completion targeting `integration` whose changed paths are
all `.rs`/`Cargo.*`/`crates/*` runs only `verify-changed` (bound to
`RK_VERIFY_BASE=integration`); a completion touching anything else (docs,
`.github/*`, an unmatched extension) falls back to the full `verify` check
rather than landing unchecked; `main` keeps running the full `verify` check
on every promotion, exactly as before this ticket.

**Disable/recovery**: remove the `release:` block (or blank both fields) and
the `focusedChecks` rule above, then re-run the same onboarding activation
flow `docs/2026-09-15-tkt-ratik-rivam-jadud-p5-1-release-roles.md` describes.
This reverts `Supervisor::spawn` to forking from whatever branch is checked
out and `LandingPipeline::gate_plan` to the pre-existing two-class
(`ProtectedFinal`/`Inner`) behavior; no already-landed candidate, already-
prepared release, or already-merged commit is touched by either direction.

## Scope explicitly NOT covered here (retained on the P5.2 parent)

- **Shared-resource concurrency during a held release check.** This ticket
  does not claim (and does not test) that an integration landing makes
  progress while a release validation holds shared admission capacity — that
  is P5.1's own documented open question, unchanged here.
- **Coalesced pending release selection / aggregate admission overflow.**
  Both remain exactly as scoped on `TKT-gogas-ponoh-jivog`.
- **The full native two-branch barrier fixture.** This ticket's tests prove
  the narrower routing/check-selection properties directly (real git repos,
  real `checks.cue`, a real `Supervisor::spawn_async`); the fuller barrier
  scenario (hold one release check, integrate a second real change through
  its focused check while the barrier holds, observe the next candidate)
  depends on the P5.2 parent's concurrency/coalescing slices landing first.

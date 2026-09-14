#!/usr/bin/env bash
set -euo pipefail

# Full workspace build, test, and lint — the maintained recipe for
# protected-final landing (mise.toml [tasks.verify-full], .rk/checks.cue's
# `verify` check). Extracted out of mise.toml's `run` string
# (TKT-dagom-lajub-hijug) so this task, a human, and
# crates/rk-core/tests/verify_full_recipe_regression.rs all execute it
# identically instead of hand-kept copies that can drift.
#
# NOT yet wired into .github/workflows/ci.yml, and .rk/checks.cue's `verify`
# toolchain declaration is not yet updated to mention cargo-nextest either:
# both `.github` and `.rk` are protected paths, so adopting this recipe in CI
# and updating the check's toolchain label need the explicit protected-change
# authorization route rather than landing alongside this local recipe.
# Tracked separately as TKT-sojuz-bogij-bapip, which retains the prepared CI
# hunk. Until that lands, CI and protected-final landing run different
# recipes, and the named check's toolchain label undercounts its actual
# tools.
#
# The test phase runs cargo-nextest (pinned in mise.toml's [tools], already
# used by verify-changed.sh) instead of `cargo test --workspace`. This is a
# scheduling opportunity, not a measured causal proof: verify-changed's
# nextest phase ran 145.053s versus the full gate's 1331.540s on the same
# tracked tree a2865b3 (native evidence, candidate
# 34be02b2cfed52ed087b4c9ae80bda6b953f6928), but those numbers cover unequal
# scopes (a package-scoped nextest run versus fmt+build+test+clippy over the
# whole workspace) with no disjoint per-phase timing of the OLD recipe to
# compare against — nextest running test binaries concurrently while
# `cargo test` runs them one at a time is the hypothesis motivating this
# change, not something these two numbers alone establish as the cause; no
# speedup ratio is asserted. Nextest does not run doctests
# (https://nexte.st/docs/running/), so the explicit
# `cargo test --workspace --doc` step below preserves that category. It is a
# no-op on this workspace today (verified via `cargo metadata
# --format-version 1 --no-deps`: none of the workspace's 10 library targets
# has `doctest: false` — non-library targets such as bins and integration
# tests always report `doctest: false` regardless, since only libraries
# support doctests, so that is not evidence of anything here — and the only
# ``` fence outside a doctest is rk-git/src/lib.rs's ```text, which rustdoc
# never executes) but load-bearing the moment a runnable doctest is added.
# The same metadata query confirms no target has kind "example" or "bench"
# and no library target carries `doctest: false`. It does NOT confirm the absence of
# `harness = false` test targets — metadata's `test` field only says whether
# a target participates in `cargo test` at all, not which harness it uses, so
# a harness=false target still reports `test: true` and metadata alone cannot
# tell nextest and `cargo test` apart there. That was checked separately, by
# grepping every crate manifest for a `harness` key
# (`grep -rn harness --include=Cargo.toml crates/`): none sets it to false.
# Together these two checks confirm nextest's coverage of `--workspace` is
# equivalent to `cargo test --workspace`'s for every category actually
# present as of 2026-09-14 — a point-in-time fact about this workspace, not
# something re-verified on every run. If a bench, example, or custom-harness
# target is added later, redo both checks and re-derive this comment.
# crates/rk-core/tests/verify_full_recipe_regression.rs separately proves,
# every run, that each phase's actual command still rejects a broken
# fixture and that an earlier phase's failure stops the pipeline before a
# later one runs — it does not re-verify workspace target inventory.
#
# verify_jobs bounds build parallelism (`--jobs`/`--build-jobs`) and test
# process concurrency (`--test-threads`) explicitly on every expensive phase,
# rather than relying on this repo's `.cargo/config.toml` ([build] jobs = 4)
# to apply implicitly — that file only takes effect when cargo's config
# search walks up from the current directory into this repo, which is not
# true when this same script runs against an isolated tiny fixture directory
# elsewhere on disk (exactly what the regression test does). 4 is half this
# host's 8 logical cores — a conservative single-host bound, not every
# available core — matching the existing `.cargo/config.toml` convention
# (same single-operator-castle CPU/disk-sharing rationale) rather than
# inventing a second number. It is a fixed constant, not an environment
# override: an override here would let an untrusted or mistaken caller
# request every core (or a non-numeric value cargo would reject with a less
# obvious error) for a script that lands in CI and protected-final landing.
# This does not bound child-thread/test-internal fan-out inside one test
# process, or any unrelated build sharing this host; it only bounds what
# this script itself launches.
verify_jobs=4

strip_rk_spawn_env=(
	-u RK_AGENT -u RK_TASK -u RK_REPO -u RK_ROLE -u RK_HOME -u RK_BRANCH -u RK_WORKTREE
	-u RK_AUTH_TOKEN -u RK_REVIEW_BRANCH -u RK_REVIEW_HEAD -u RK_REVIEW_TARGET -u RK_REVIEW_TASK -u RK_REVIEW_ATTEMPT
)

cargo fmt --all --check
cargo build --workspace --jobs "$verify_jobs"
env "${strip_rk_spawn_env[@]}" cargo nextest run --workspace --no-fail-fast --build-jobs "$verify_jobs" --test-threads "$verify_jobs"
env "${strip_rk_spawn_env[@]}" cargo test --workspace --doc --jobs "$verify_jobs" -- --test-threads "$verify_jobs"
cargo clippy --workspace --all-targets --jobs "$verify_jobs" -- -D warnings

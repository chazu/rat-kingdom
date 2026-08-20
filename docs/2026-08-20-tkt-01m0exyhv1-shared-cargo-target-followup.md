# TKT-01M0EXYHV1GR9Z75QSS42HXBVK: shared-cargo-target-cache — root cause confirmed, fixed

## What the ticket reported

Emile-9 observed two failures in one self-driven `mise run verify` session
(worktree Emile-9), both clearing on a forced rebuild of just the affected
target and touching no file that branch's diff changed:

1. `crates/rk-cli/tests/version_handshake.rs` — all 3 build-mismatch tests
   failed because `rk_core::version::BUILD_VERSION` in the freshly-linked
   test binary disagreed with the stamp baked into the separately-cached `rk`
   binary (`c4fca09fd48b`, a stale build) it spawns via
   `env!("CARGO_BIN_EXE_rk")`.
2. `crates/rk-workflow/tests/examples.rs` — all 19 tests failed reading
   paths under a *different rat's* worktree
   (`.../Django-9/crates/rk-workflow/../../examples/...`).

## Finding 1: case 2 was already fixed on `main`

`crates/rk-workflow/tests/examples.rs` no longer resolves its fixture
directory from a compile-time-baked path. Commit `3be5f1d` ("test: resolve
workflow fixtures at runtime", landed 2026-08-20 05:30, ahead of this
ticket) replaced the `env!("CARGO_MANIFEST_DIR")`-style lookup with a
runtime `workspace_root_from(std::env::current_dir())` walk. Verified:
`cargo test -p rk-workflow --test examples` is 21/21 green on this branch.
No further action needed for this half.

## Finding 2: this is not a race — sharing corrupts sequential builds too

The `TestExecLock` line of work (`TKT-01M0CFA1RX36SJ7DV4YWGHQ9BT`,
`TKT-01M0C3FPN50PCSSGY5QNWRZZRW`, `docs/2026-08-19-tkt-hot-scan-target-dir-contention.md`)
framed the shared-`CARGO_TARGET_DIR` hazard as **concurrency contention**:
cargo's own target-dir lock covers the *build* phase, but a second
concurrent build can recompile-and-GC a binary a first process already
resolved and is about to exec, producing `ENOENT (never executed)`. That is
real, but it is not the whole story, and case 1 above doesn't fit it — there
was no evidence of a concurrent peer build at the moment Emile-9 hit it.

Reproduced directly: two real `git worktree` checkouts of this repo at
different commits (`c4fb747`, `bbbca0f`), built **sequentially, no time
overlap**, sharing one `CARGO_TARGET_DIR`:

```
$ git worktree add --detach /tmp/wt-a c4fb747
$ git worktree add --detach /tmp/wt-b bbbca0f
$ CARGO_TARGET_DIR=/tmp/target cargo build -p rk-cli   # in /tmp/wt-a — succeeds
$ CARGO_TARGET_DIR=/tmp/target cargo build -p rk-cli   # in /tmp/wt-b, AFTER wt-a finished
error[E0560]: struct `PrimeContext` has no field named `review`
   --> crates/rk-cli/src/main.rs:895:9
```

`wt-b` (`bbbca0f`) genuinely has that field; a private target dir builds it
clean (confirmed). With the shared dir, cargo linked `rk-cli` against a
stale, already-compiled `rk-core` from `wt-a`'s older checkout instead of
recompiling — cargo did not fully key that workspace-member unit's
fingerprint by the checkout's absolute path, so two worktrees of the same
repo collided onto the same cached artifact. No concurrency was involved;
a lock cannot fix this because there is no race to serialize against, the
*wrong answer is cached*. This also explains case 1 without needing a
concurrent peer: the shared `rk` binary was simply stale relative to
Emile-9's own new commit, for the same reason.

Confirmed with a minimal, fast (<0.3s) fixture too — a two-crate
`[workspace]` (not a single crate; a bare non-workspace fixture did *not*
reproduce it, only a workspace layout does) where one checkout's lib lacks
a symbol the other's bin calls: sharing the target dir makes the second
build silently link the first's stale lib and produce the *first*
checkout's output; private target dirs don't. See
`crates/rk-core/tests/shared_cargo_target_worktree_isolation.rs`.

## Fix implemented

`[disk] shared_cargo_target` (`crates/rk-core/src/config.rs`,
`DiskConfig::default`) now defaults to **`false`**. Each spawned agent goes
back to cargo's own per-worktree `target/`, which cannot collide with
another worktree's by construction. The flag itself, `TestExecLock`, and
the `run_check_in` contention-retry all remain in place — an operator who
wants cross-worktree build sharing back despite the correctness risk can
still opt in — but nothing depends on the default being `true`.

The original ENOSPC problem this traded against (60+ concurrently-live
worktrees x 3-7GB) is now covered independently: `WorktreeSweepConfig`
(enabled by default, hourly) reaps every terminal worktree's own `target/`,
and `[disk] min_free_gb` (10GB default) refuses new spawns before a live
batch can run a repo out of room. This confirms and resolves the "is
`shared_cargo_target` even still needed given the worktree-sweep work?"
question Finding 1 of `docs/2026-08-19-tkt-self-driven-verify-lock-gap-analysis.md`
left open — it needs to stay off given the now-proven correctness cost, not
merely because it's moot.

Regression coverage added:
- `crates/rk-core/src/config.rs::config::tests::shared_cargo_target_defaults_off`
- `crates/rk-core/tests/shared_cargo_target_worktree_isolation.rs` — the
  fast two-checkout fixture above, both pinning the hazard (so a future
  cargo upgrade that changes this behavior is noticed) and proving isolated
  target dirs avoid it.

`TKT-01M0CJY73NHNXNE3PTAY86033B` (self-driven verify-lock gap, still open)
no longer needs a harness-level wrapper/lock design to close the gap this
ticket surfaced: with sharing off by default, there is no shared target dir
for a self-driven `mise run verify` to contend over in the first place. Left
open in case an operator re-enables `shared_cargo_target` and wants that
gap covered for that mode.

## Isolated verification and landing recovery

The final repository verification was rerun with the deployed daemon's
shared-target environment explicitly overridden:

```sh
CARGO_TARGET_DIR="$PWD/target" MISE_TRUSTED_CONFIG_PATHS="$PWD" \
  env -u RK_AGENT -u RK_TASK -u RK_REPO -u RK_ROLE -u RK_HOME \
      -u RK_BRANCH -u RK_WORKTREE mise run verify
```

It completed formatting, the workspace build and test suite, and clippy in
the Skitter-10 worktree-local target directory. The first daemon landing
attempt did not report a repository failure: artifact
`01M0FX57KQNFM24DE1Y8Y8DAE8` recorded `exit = -1`, no timeout, no failing
test, and output ending during normal compilation while several independent
full builds were active. That infrastructure-death classification and its
bounded automatic retry are tracked separately by
`TKT-01M0FXGQMA10JYCV9QCGEAK4TT`; this commit gives the otherwise unchanged,
verified fix a fresh head for another normal gated submission.

# Proposal 0008 — Verify through the project's own runner, with your spawn env stripped

**Author:** Asiago-2 (task: refine-prompts)
**Target prompt:** `crates/rk-core/src/prime.rs` → `FRAGMENT_COMPLETION`, step 2
**Companion convention:** `verify-through-the-project-runner`
**Reconstructs:** Parmesan-2's lost 0008 (see 0010 for why it was lost), re-derived
independently from the live event feed and re-checked against the tree
**Status:** proposed (do NOT apply live — an operator/steward lands this; see the
completion protocol)

## The recurring pain

Step 2 of the completion protocol hands every rat three literal commands:

```
2. Verify with the project's own build, tests, and linters — for a Rust crate
   that means `cargo build`, `cargo test`, and `cargo clippy` all pass.
```

**All three are the wrong invocation inside a rat in this workspace**, for two
independent reasons. Both are already fixed in `README.md` and `mise.toml`. The
prompt — which is what a rat reads *first*, before it ever opens the README — was
never updated.

### Defect A — a bare `cargo` is the wrong toolchain

`mise.toml` pins `rust = "1.95.0"`; `Cargo.toml` declares an MSRV floor. A bare
`cargo` resolves against whatever is on `PATH`, and on these boxes that has
repeatedly been 1.85.1 — below the floor, so it fails *before it compiles
anything*. Rats keep re-deriving this one at a time, and one left a note in its
completion summary for whoever came next:

```
"(Note for whoever picks this up: bare `cargo` on PATH is rustc 1.85 and fails
 the MSRV check; use `mise exec -- cargo`.)"
"The workspace needs `cargo +1.95.0` (or mise); the default 1.85.1 toolchain
 can't build the crates."
"My environment pinned RUSTUP_TOOLCHAIN=1.85.1, below the workspace's 1.88
 floor; I built and verified against `stable` instead."
"Verification — `mise exec -- cargo build/test/clippy --workspace`: …"
"`mise run verify` exits 0 from inside this rat."
```

Five distinct rats, five independent rediscoveries of one fact, and at least one
(the third) silently verified against a *different* toolchain than the one the
project pins.

### Defect B — the rat's own spawn env poisons the suite

`rk_daemon::Client::connect` sends `$RK_AGENT` as the RPC caller. The daemon
refuses operator-only methods (`workflow.run`, `agent.spawn`, …) from an agent
caller. Every integration test that opens an RPC connection therefore fails
`forbidden` **for a rat and only for a rat**. One rat censused it at a single
commit rather than guessing:

```
| runner                 | failures        |
| bare `cargo test`      | 79              |
| `env -u RK_AGENT`      | 0 (418 passed)  |

"All 79 panic on Protocol(\"forbidden\") and nothing else. So the command the
 completion protocol tells every rat to verify with cannot go green inside a
 rat — and a rat has no way to tell those failures from a real regression."
```

Two more hit the same wall separately:

```
"cargo test --workspace initially failed on reviewer_drives_rework_loops_then_merges
 with `forbidden: Noodle-2 is not authorized for workflow.run` … re-running with
 `env -u RK_AGENT` passes it, so it's an identity leak in the test harness."
"convention_quorum.rs fails 0/2 for any rat that runs it — at my branch and at
 the merge-base — and it is not a real failure. … It reads exactly like
 pre-existing breakage."
```

`mise.toml`'s `[tasks.test]` already carries the fix and says why:

```toml
# `env -u RK_AGENT` is load-bearing, not hygiene. … the suite passes for a human
# and fails for every rat running the very command its completion protocol
# demands. Tracked: TKT-182.
run = "env -u RK_AGENT cargo test --workspace"
```

A peer has an open ballot on exactly this: **`sug-1w6fswmzet`** (Peanut-2,
TKT-168), endorsed by this rat on entry. It needs 3 distinct endorsers.

## Why this is worse than a wasted command

The prompt's *own step 3* routes an unexplained failure to a ticket:

> 3. … If you hit a pre-existing failure that is unrelated to your change, do
>    NOT fix it inline … file a ticket and post a `fact` tuple describing it.

So a rat that runs the command the prompt gave it sees 79 red tests it did not
break, correctly applies step 3, and **manufactures a phantom ticket and a false
`fact` tuple** — durable, `Furniture`-weight noise that the next rat reads as
evidence. The two rules compose into a defect generator. One rat drew the
conclusion explicitly:

```
"the RK_AGENT breakage means any past rat report of 'pre-existing test failures
 unrelated to my change' against this suite is unreliable evidence either way —
 it may have been this, or it may have masked something real."
```

The same bare `cargo` is the default `run` gate in
`examples/workflows/steward.cue` and `checked-merge.cue`. Those gates fail
closed, so a toolchain mismatch there produces a `workflow_failed` naming a red
suite that never compiled.

## Root cause in the prompt

The step names *tools* (`cargo`) where it should name the project's
*verification entrypoint*. `mise run verify` is literally step 2 expressed as one
command —

```toml
[tasks.verify]
description = "Build, test, and lint — the checks a rat must pass before `rk done`"
run = """
cargo build --workspace
env -u RK_AGENT cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
"""
```

— and a rat can only find it by accident.

## Proposed diff

```diff
--- a/crates/rk-core/src/prime.rs
+++ b/crates/rk-core/src/prime.rs
@@ const FRAGMENT_COMPLETION: &str = "\
-2. Verify with the project's own build, tests, and linters — for a Rust crate
-   that means `cargo build`, `cargo test`, and `cargo clippy` all pass. A
-   partial check (e.g. `cue vet`) is NOT verification: the code must actually
-   compile and the suite must actually run green.
+2. Verify with the project's own build, tests, and linters — and find the
+   project's verification entrypoint before you invent one. Look for a
+   `mise.toml` task, a `Makefile`/`justfile` target, or the README's
+   development section; a repo that has written down how it is verified has
+   usually written down the two traps below as well. In this workspace the
+   whole of this step is one command: `mise run verify`.
+   Two things a raw `cargo` invocation gets wrong here, both of which look
+   like someone else's breakage:
+   - WRONG TOOLCHAIN. If the repo pins a toolchain (`mise.toml`,
+     `rust-toolchain.toml`), run through it — `mise exec -- cargo ...` — not a
+     bare `cargo` off your `PATH`. An older `PATH` toolchain fails the MSRV
+     check before it compiles a line, and that failure is not your change.
+   - YOUR OWN SPAWN ENV. Run the suite with the RK_* spawn vars stripped
+     (`env -u RK_AGENT cargo test --workspace`). The test client authenticates
+     as `$RK_AGENT`, so tests that open an RPC connection are refused
+     `forbidden` for a rat and pass for a human. Inside a rat the raw command
+     produces a wall of red that no change of yours caused.
+   A partial check (e.g. `cue vet`) is NOT verification: the code must actually
+   compile and the suite must actually run green.
 3. Never `rk done` on a build you broke. If you hit a pre-existing failure that
```

## Safety against the `prime.rs` tests

One test reads this step:
`completion_protocol_puts_the_commit_ahead_of_verification` (`prime.rs:448`).

| assertion | after this diff |
|---|---|
| `text.find("Commit BEFORE you verify")` | untouched (step 1) |
| `text.find("Verify with the project's own build")` | **preserved verbatim** as the opening clause — the diff appends after `linters`, never rewrites the prefix |
| `commit_at < verify_at` | unchanged ordering |
| ``text.contains("`rk done` is NOT a\n   commit")`` | step 4 untouched, including its literal line wrap |
| `git status --porcelain`, `git log <base>..HEAD` | step 4 untouched |

`rat_role_includes_all_fragments_once` counts `"Completion protocol"` once — the
diff adds no second heading. No other test reads step 2. **No test change
required.**

## Scope note: this fragment is shared across repos

`FRAGMENT_COMPLETION` renders for every repo the fleet knows (`rat-kingdom`,
`grmpl`, `capsule`). The diff is written so the *rule* is repo-agnostic ("find
the project's entrypoint"; "if the repo pins a toolchain") and only the worked
example is concrete. That is strictly better than the status quo, which hardcodes
a bare `cargo` for all three.

## Companion convention proposal

```json
{
  "rule": "verify-through-the-project-runner: Verify with the project's own documented entrypoint (a mise/make/just task or the README's development section), not a bare toolchain binary. Run through the repo's pinned toolchain, and run the suite with your RK_* spawn env stripped (env -u RK_AGENT). A red suite you got from the wrong toolchain or from your own identity is not a pre-existing failure and must not become a ticket.",
  "why": "Five rats independently rediscovered that a PATH cargo is below the MSRV floor; a measured census showed 79 tests fail `forbidden` inside a rat and 0 outside it. Because completion-protocol step 3 routes unexplained failures to a ticket, the bad command manufactures phantom tickets and false fact tuples that the next rat reads as evidence."
}
```

Endorse the existing ballot **`sug-1w6fswmzet`** for the `env -u RK_AGENT` half
rather than minting a near-duplicate.

## Companion ticket

Land this diff into `FRAGMENT_COMPLETION`. Separately, `TKT-182` (an
explicit-caller `Client` constructor) is the durable fix for defect B; this
proposal is the prompt-side mitigation that stops rats mis-reading the symptom
until it lands.

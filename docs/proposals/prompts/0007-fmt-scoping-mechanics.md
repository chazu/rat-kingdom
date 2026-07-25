# Proposal 0007 — Give the no-reformat rule its mechanics: `cargo fmt` cannot be scoped, and the baseline is already dirty

**Author:** Burrow-2 (task: refine-prompts)
**Target prompt:** `crates/rk-core/src/prime.rs` → `FRAGMENT_GIT_SAFETY`
**Refines:** proposal 0002 (landed as commit `3690a85`)
**Companion convention:** extends `no-workspace-wide-reformatting` (does not
replace it)
**Status:** proposed (do NOT apply live — an operator/steward lands this; see the
completion protocol)

## The recurring pain

Proposal 0002 landed the *rule* — "Format only files you changed" — and it is
working: rats now catch and revert their sweeps instead of committing them. What
0002 did not land is the *mechanics*, so every rat still pays the same two
rediscoveries.

**Rediscovery 1: `cargo fmt` has no useful path scoping.** A rat that follows the
rule literally runs `cargo fmt` on its files and gets a workspace sweep anyway,
then has to work out why:

```
"cargo fmt sweeps the workspace regardless of path args, so I used rustfmt on my
 three files and left the pre-existing toolchain churn in untouched regions alone."
"rustfmt recursed from main.rs into sibling modules; I reverted all churn in files
 this task didn't touch, per the no-workspace-wide-reformatting convention."
"Caught and reverted a rustfmt-follows-mod reflow that had reformatted the entire
 grmpl-proc crate with a newer toolchain — re-applied only my semantic edits by hand."
```

(`rustfmt` follows `mod` declarations, so even the direct invocation needs the
files named explicitly rather than a crate root.)

**Rediscovery 2: the committed baseline is not fmt-clean under the current
toolchain**, so `cargo fmt --check` fails on files nobody touched. Rats keep
re-deriving that this is expected and must not be "fixed":

```
"main is NOT fmt-clean under the newly-bumped 1.95.0 toolchain (a pre-existing
 condition unrelated to my task) — cargo fmt --all --check reformats many
 committed files."
"the repo carries pervasive pre-existing rustfmt drift — cargo fmt reformats
 dozens of untouched files."
"The pre-existing rustfmt diffs in the file (74 on HEAD) were left untouched."
"I did not run rustfmt: local 1.9.0 disagrees with the committed baseline in 11
 places in untouched code."
"No fmt sweep: the workspace is already fmt-dirty on main under the current
 toolchain; this branch correctly left it alone."
```

Ten distinct instances across the drain corpus (289 `harness_result` tuples).
Each is a rat spending tokens reasoning from first principles about a fact that
is fixed, knowable, and identical for every rat. The last quote is the expensive
one: a rat that concludes "I did not run rustfmt at all" has been pushed off
formatting entirely by an unexplained dirty baseline.

## Root cause

The bullet 0002 landed states the goal but names the wrong tool and says nothing
about the baseline:

```
- Keep your diff to the files your task touches. NEVER commit a workspace-wide
  reformat (e.g. a bare `cargo fmt` that reflows untouched files — a newer
  toolchain will do this). Format only files you changed, and before you
  commit, revert any fmt/toolchain churn in files your task did not touch:
  `git checkout -- <untouched files>`. …
```

"Format only files you changed" is unactionable with `cargo fmt` — that is the
one thing `cargo fmt` cannot do. And a rat that then runs `cargo fmt --check`
sees a red workspace and has no way to know that is the expected steady state
rather than something it broke.

## Proposed change (unified diff)

```diff
--- a/crates/rk-core/src/prime.rs
+++ b/crates/rk-core/src/prime.rs
@@ const FRAGMENT_GIT_SAFETY: &str = "\
 - Keep your diff to the files your task touches. NEVER commit a workspace-wide
   reformat (e.g. a bare `cargo fmt` that reflows untouched files — a newer
-  toolchain will do this). Format only files you changed, and before you
-  commit, revert any fmt/toolchain churn in files your task did not touch:
-  `git checkout -- <untouched files>`. A reformat sweep races peers editing
-  those same files and buries your real change in review.
+  toolchain will do this). `cargo fmt` cannot be scoped — it reflows every
+  target in the workspace whatever paths you pass it. Format by naming your
+  files to the formatter directly (`rustfmt <files you changed>`), not through
+  `cargo fmt`, and before you commit revert any fmt/toolchain churn elsewhere:
+  `git checkout -- <untouched files>`. Expect the committed baseline to be
+  fmt-dirty under a newer toolchain than the one it was written with: a failing
+  `cargo fmt --check` over files you did not touch is the pre-existing state,
+  not a gate you must satisfy and not yours to fix. A reformat sweep races peers
+  editing those same files and buries your real change in review.
```

## Why this is safe

- Purely a refinement of an already-landed bullet in the shared
  `FRAGMENT_GIT_SAFETY`; both roles include it, so no per-role copy drifts.
- `rat_role_includes_all_fragments_once` asserts on the substring `"Git safety"`,
  unchanged, so it stays green.
- It does not weaken formatting: it tells rats *how* to format their own files
  correctly, which is strictly more formatting than the current status quo where
  at least one rat opted out entirely.
- The "not yours to fix" clause is the same shape as the already-accepted
  `preexisting-failure-is-a-ticket-not-an-inline-fix` rule, so it reinforces
  rather than competes.

## Companion convention proposal

Filed as a `convention-proposal` artifact this run, as an **extension** of the
existing `no-workspace-wide-reformatting` rule rather than a competing norm:

> **`no-workspace-wide-reformatting` (mechanics addendum)** — `cargo fmt` cannot
> be scoped to paths; format your changed files with `rustfmt <files>` directly.
> A fmt-dirty committed baseline under a newer toolchain is the pre-existing
> state: `cargo fmt --check` over untouched files is not a gate and not yours to
> fix.

## Related

- Proposal 0002 / commit `3690a85` — the rule this supplies mechanics for.
- fact `preexisting-clippy-warnings`, convention
  `preexisting-failure-is-a-ticket-not-an-inline-fix` — same "leave the baseline
  alone" spirit.
- `mise.toml` pins `rust = "1.95.0"` while much of the committed tree was
  formatted under 1.85 — the concrete source of the dirty baseline. Re-basing
  the whole workspace onto 1.95 rustfmt in one dedicated commit is a separate
  ticket, not a rat's inline work.

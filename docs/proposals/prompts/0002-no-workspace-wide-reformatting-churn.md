# Proposal 0002 — Forbid workspace-wide reformatting churn on shared files

**Author:** rat-114 (task: refine-prompts)
**Target prompt:** `crates/rk-core/src/prime.rs` → `FRAGMENT_GIT_SAFETY`
**Companion convention:** `no-workspace-wide-reformatting`
**Status:** proposed (do NOT apply live — an operator/steward lands this; see the
completion protocol)

## The recurring pain

The single most repeated line in the fleet's `harness_result` transcripts is a
rat reporting that it had to *undo* a `cargo fmt` sweep it (or its toolchain)
applied to files unrelated to its task. Five distinct instances in one night's
event feed:

```
reverted the workspace-wide `cargo fmt` drift that pre-existed in files unrelated to this ticket
Reverted `cargo fmt` churn it introduced in ~6 unrelated ...
reverting a stray whole-crate `cargo fmt` sweep that would have churn[ed] ...
reverted a `cargo fmt` ...
reverted an accidental toolchain-driven `cargo fmt` churn across 35 unrelated ...
```

The last is the telling one: **the churn is often not something the rat asked
for** — a newer `rustfmt` (the environment's toolchain has moved from 1.85 →
1.95 during the program) reformats untouched files the moment a rat runs
`cargo fmt` with no path argument, or an editor/format-on-save reflows a whole
crate. Every rat then independently:

1. notices the diff is 10–35 files bigger than its task,
2. spends tokens reasoning about whether the churn is safe,
3. manually reverts it before committing.

## Why this is a real cost, not a nit

- **It races peers on shared files.** A 35-file reformat sweep touches files
  other in-flight branches are editing. That is exactly the concurrent-merge
  hazard the fleet already had to paper over with the serialized merge queue
  (TKT-51): every extra shared-file hunk is another chance for a silent
  `merged:false`. Keeping diffs to the files the task actually touched is the
  cheap upstream fix.
- **It is paid N times.** There is no shared rule, so each rat rediscovers the
  hazard and re-derives the "revert the sweep" remedy from scratch. A one-line
  convention converts N reasoning episodes into zero.
- **It inflates review.** A reviewer (or the steward's diff-size budget gate)
  cannot tell task work from reformat noise in a 35-file diff.

## Root cause

Nothing in the shared prompt tells a rat to *scope its formatting to the files
it touched*. `FRAGMENT_GIT_SAFETY` currently reads:

```
## Git safety

- Work ONLY in your worktree (RK_WORKTREE) on your branch (RK_BRANCH).
- NEVER commit to main/master/develop; never switch branches; never force-push.
- Commit your work with clear messages as you go; your branch is merged by the
  orchestrator on dismissal.
```

The completion protocol (strengthened by TKT-41) tells rats to *run* the
linters, which is correct — but "run clippy/fmt" with no scope discipline is
precisely what produces the whole-workspace sweep. The two need to be paired.

## Proposed change (unified diff)

```diff
--- a/crates/rk-core/src/prime.rs
+++ b/crates/rk-core/src/prime.rs
@@ const FRAGMENT_GIT_SAFETY: &str = "\
 ## Git safety
 
 - Work ONLY in your worktree (RK_WORKTREE) on your branch (RK_BRANCH).
 - NEVER commit to main/master/develop; never switch branches; never force-push.
+- Keep your diff to the files your task touches. NEVER commit a workspace-wide
+  reformat (e.g. a bare `cargo fmt` that reflows untouched files — a newer
+  toolchain will do this). Format only files you changed, and before you
+  commit, revert any fmt/toolchain churn in files your task did not touch:
+  `git checkout -- <untouched files>`. A reformat sweep races peers editing
+  those same files and buries your real change in review.
 - Commit your work with clear messages as you go; your branch is merged by the
   orchestrator on dismissal.
 ";
```

## Why this is safe

- Purely additive guidance in the shared `FRAGMENT_GIT_SAFETY`; both `rat` and
  `reviewer` roles include it, so no per-role copy drifts.
- The `rat_role_includes_all_fragments_once` test asserts on the substring
  `"Git safety"`, which is unchanged, so it stays green.
- Reinforces the single-task banner ("Do not … continue any other work") — a
  reformat of unrelated files IS other work — and the merge-queue design, which
  wants smaller, non-overlapping diffs.
- It does not tell rats to skip formatting; it tells them to *scope* it. The
  completion protocol's "cargo clippy passes" bar is untouched.

## Companion convention proposal

Filed as a `convention-proposal` artifact this run:

> **`no-workspace-wide-reformatting`** — Never commit a workspace-wide reformat.
> Run `cargo fmt` scoped to the files you changed and revert any fmt/toolchain
> churn in files your task did not touch before committing. A whole-workspace
> reformat races peers on shared files and buries the real change.

## Related

- TKT-41 / proposal 0001 (strengthened verification — the other half: run the
  tools, but scope the formatting).
- TKT-51 (serialized merge queue — this reduces the shared-file overlap it
  guards against).
- convention `preexisting-failure-is-a-ticket-not-an-inline-fix` (same spirit:
  don't drag unrelated churn into your branch).

# Proposal 0012 — Make parallel-drain file ownership a stop condition

**Author:** Rummage-3 (task: refine-prompts)
**Target prompt:** `crates/rk-core/src/prime.rs` → `FRAGMENT_SPACE` and
`FRAGMENT_GIT_SAFETY`
**Companion convention:** `parallel-drain-file-ownership`
**Ticket:** TKT-01KZ3ZA4NM3DGKPEG28VMZANPD
**Status:** proposed (do NOT apply live — this rat only writes proposals)

## The recurring pain

The overnight `nightly-self-improve` drain produced four clean worker results,
then lost the batch at the merge boundary. Workflow `wf-fp0gwx21zw` (started
2026-08-03) failed in `dismiss_all` because three branch merges would overwrite
local changes in overlapping paths:

```text
Peppercorn-3: crates/rk-daemon/src/supervisor.rs
Ratatosk-3:   crates/rk-cli/src/agent_cmds.rs, crates/rk-daemon/src/supervisor.rs
Nezumi-3:     README.md, crates/rk-cli/src/agent_cmds.rs
```

The workers had already reported `declared_done: true`, `is_error: false`, and
successful verification. The useful work was not rejected by its tests; the
parallel hand-off failed because the branches edited the same files. The
failure therefore sits after role completion but before the orchestrator can
land the work.

The other recent failures are deliberately not folded into this proposal:

- `wf-qjbxfe68ef` dispatched three workers with an unavailable
  `gpt-5.6-luna` model. That is workflow/model configuration, not a role-prompt
  instruction.
- `wf-njpsbmr2jf` stopped on a failing `continuous_drain` test. That is a
  repository gate result and must follow the pre-existing-failure ticket path,
  not be repaired by every rat.
- The live obstacle and need scans were empty. The merge collision is the
  concrete recurring signal from the drain itself.

## Current recurrence

The same boundary recurred in nightly instance `wf-8twfp9c411` on
2026-08-15. `Scamper-5` completed its ticket, but the `dismiss_all` hand-off
failed while merging `rat/scamper-5/tkt-01m00ss4wefy0bbt9tzh3mf9gj` because
`crates/rk-cli/tests/workflow_run_approval.rs` conflicted with another drain
branch (`agent_dismissed` event `01M01QDR0D380ZKQTYHDQZ90S6`). This is the same
parallel-drain ownership failure, with a newly observed shared test path; it
does not justify a second proposal or convention ballot. The existing ticket
`TKT-01KZ3ZA4NM3DGKPEG28VMZANPD` remains the hand-off for the prompt change.

## Root cause in the prompt

The current coordination fragment says to scan peer claims and artifacts,
steer clear of peers' files, and mark an area with `rk claim`. That is useful
advice but not an ownership protocol:

1. It does not tell a rat what to do when a live claim overlaps a file it needs.
2. It does not require the claim to cover exact paths before editing.
3. It does not require a final diff-name check before commit, so a formatter,
   generated file, or incidental edit can silently widen the merge surface.
4. It does not say that isolated worktrees do not make same-file changes
   mergeable. The collision is merely deferred to `dismiss_all`, where the
   worker has already exited and cannot resolve ownership.

The prompt already teaches claims, but a parallel drain needs the negative
instruction as well: overlapping ownership is a stop-and-escalate condition,
not permission to make the edit and hope the merge queue resolves it.

## Proposed diff

```diff
--- a/crates/rk-core/src/prime.rs
+++ b/crates/rk-core/src/prime.rs
@@ const FRAGMENT_SPACE: &str = "\
 - Before editing an area, `rk scan claim <repo>` and `rk scan artifact <repo>`
   to see what peers are touching, and steer clear of their files. On entry,
   mark your area with `rk claim <area>` (a path or glob) so peers avoid it.
   Claims evaporate on a TTL, so re-run it if you are still working there.
+  Claim the exact paths or globs you will edit before opening them. If an
+  unexpired peer claim overlaps a file you need, do not edit that file: use
+  `rk need`, `rk obstacle`, or a ticket and let the orchestrator serialize or
+  reroute the work. Do not treat separate worktrees as permission for two
+  rats to edit the same path.
@@ const FRAGMENT_GIT_SAFETY: &str = "\
 - Keep your diff to the files your task touches. NEVER commit a workspace-wide
   reformat. Use the repository's documented formatter, scoped to files you
   changed whenever that formatter supports it, and before you commit revert any
   formatting churn elsewhere: `git checkout -- <untouched files>`. A formatting
   failure over files you did not touch may be pre-existing; do not absorb it
   into your task without checking the repository's own instructions. A reformat
   sweep races peers editing those same files and buries your real change in
   review.
+- Before committing, run `git diff --name-only` and compare every path with
+  your task and your claim. If an unclaimed, peer-claimed, or incidental path
+  appears, stop and report it; do not silently take ownership of another rat's
+  file or leave the merge boundary to discover the collision.
```

The exact wording can be adjusted by the landing ticket, but the invariant is
that overlap is surfaced before a rat commits, while the orchestrator still
has an active owner and can serialize or re-route the work.

## Why this is safe

This is additive guidance. It does not change tuple authorization, branch
creation, merge policy, or the existing claim trail. It gives a rat an explicit
safe outcome when the existing `rk scan claim` advice finds a collision.

The proposal should preserve the existing `prime.rs` regression checks:

- `templates_teach_area_claim_trails_not_work_claiming` must still find
  `rk claim <area>` and `rk scan claim` and must continue to distinguish area
  ownership from claiming extra tasks.
- `git_safety_keeps_formatting_guidance_project_agnostic` must still find the
  repository-owned formatter and scoped-formatting wording.
- No role receives a new task or permission. Reviewers inherit the same
  ownership guard through the shared fragments.

A focused test should be added when the prompt change lands, asserting that
both `rat` and `reviewer` render the overlap stop condition and the final
`git diff --name-only` check. The test should not assert a particular claim TTL
or orchestrator implementation; those remain daemon/workflow concerns.

## Durable convention proposal

```json
{
  "rule": "parallel-drain-file-ownership: In parallel work, claim the exact paths or globs before editing. If an active peer claim overlaps a required path, stop editing that path and hand off with a need, obstacle, or ticket so the orchestrator can serialize or reroute it. Before commit, verify git diff --name-only contains only task-owned and claimed paths. Separate worktrees do not make same-file edits safe to merge.",
  "why": "nightly-self-improve workflow wf-fp0gwx21zw produced four clean worker results but dismiss_all failed because three branches overlapped on crates/rk-daemon/src/supervisor.rs, crates/rk-cli/src/agent_cmds.rs, and README.md. The current prompt teaches scanning claims but does not define overlap as a stop condition or require a pre-commit ownership check, so the conflict is discovered only after workers exit at the merge boundary."
}
```

The convention complements the existing no-workspace-wide-reformatting and
pre-existing-failure conventions. It does not authorize a rat to fix the
merge queue or the failing repository gate inline; those remain separate
workflow tickets.

# Role-prompt proposals

Proposed edits to the shared role instructions in `crates/rk-core/src/prime.rs`,
each traced to a recurring failure in the fleet's own event feed.

**These are proposals.** A `refine-prompts` rat writes them; it does **not** edit
the live prompt. An operator or a ticketed rat lands them, one commit per
proposal, so a prompt change is reviewable on its own evidence.

Each file carries: the recurring pain (quoted from `rk scan event` /
`workflow_failed` / `harness_result`), the root cause in the current prompt text,
a unified diff, a safety argument against the `prime.rs` tests, and any companion
`convention-proposal`.

| # | Proposal | Target fragment | Status |
|---|----------|-----------------|--------|
| 0001 | Strengthen completion verification | `FRAGMENT_COMPLETION` | landed (TKT-41) |
| 0002 | Forbid workspace-wide reformatting churn | `FRAGMENT_GIT_SAFETY` | landed (`3690a85`) |
| 0003 | Reviewer verdict criteria and cost | reviewer role | landed |
| 0004 | Commit before you verify; `rk done` is not a commit | `FRAGMENT_COMPLETION` | landed (TKT-163) |
| 0005 | Reviewer: an empty review branch has two causes | reviewer role | landed (TKT-164) |
| 0006 | Read and endorse open suggestions on entry | `FRAGMENT_SPACE` | landed (TKT-165) |
| 0007 | `cargo fmt` mechanics for the no-reformat rule | `FRAGMENT_GIT_SAFETY` | landed (TKT-166) |
| 0008 | Verify through the project's runner, spawn env stripped | `FRAGMENT_COMPLETION` step 2 | proposed |
| 0009 | `<base>` is unresolvable, and rat/reviewer need opposite answers | `FRAGMENT_COMPLETION` step 4 + reviewer role | proposed |
| 0010 | Prove you can land before you spend a lifetime | `FRAGMENT_COMPLETION` (new step 1) | proposed |
| 0011 | The prompt orders a `fact` write the daemon forbids | `FRAGMENT_SINGLE_TASK` + `FRAGMENT_COMPLETION` step 3 | proposed |

Proposals 0004 and 0005 are the two halves of one failure (empty branches at the
implementer/reviewer seam) and were landed together.

0008–0011 all edit `FRAGMENT_COMPLETION` but touch disjoint prose (step 2, step 4,
a new step 1, and step 3 respectively) and compose in any order. Only **0009**
warrants a test change; 0008, 0010 and 0011 pass the existing `prime.rs` suite
unmodified — each proposal argues this assertion by assertion.

**0011 is the one to land first.** It is the only proposal here reporting a
prompt instruction that does not merely mislead but *fails*: `rk out fact` has
returned `forbidden` for every rat since `de689fe` (2026-07-26), and the prompt
orders it in two places, one of them the mandatory completion protocol.

0008 and 0009 were first written by **Parmesan-2**, whose sandbox denied `rk` and
`git commit`; its worktree was reaped on dismissal and both files were lost
uncommitted. The versions here are independent re-derivations checked against the
live event feed (which Parmesan-2 could not read) and against
`supervisor.rs`/`workflow_exec.rs`/`steward.cue`. **0010 is that loss written up
as its own proposal.**

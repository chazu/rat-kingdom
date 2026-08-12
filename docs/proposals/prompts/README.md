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
| 0008 | Verify through the project's runner, spawn env stripped | `FRAGMENT_COMPLETION` step 3 | landed |
| 0009 | `<base>` is unresolvable, and rat/reviewer need opposite answers | `FRAGMENT_COMPLETION` step 5 + reviewer role | landed |
| 0010 | Prove you can land before you spend a lifetime | `FRAGMENT_COMPLETION` (new step 1) | landed |
| 0011 | The prompt orders a `fact` write the daemon forbids | `FRAGMENT_SINGLE_TASK` + `FRAGMENT_COMPLETION` step 4 | implemented (daemon authorization) |
| 0012 | Make parallel-drain file ownership a stop condition | `FRAGMENT_SPACE` + `FRAGMENT_GIT_SAFETY` | proposed |
| 0013 | Classify failure boundaries before proposing prompt edits | `prompt-refine` task descriptions | proposed |
| 0014 | Define the stale-ticket grooming handoff | `groom-backlog` workflow descriptions | proposed |
| 0015 | Classify empty undeclared harness results before prompt edits | `prompt-refine` task descriptions | proposed |

Proposals 0004 and 0005 are the two halves of one failure (empty branches at the
implementer/reviewer seam) and were landed together.

0008–0011 all touched `FRAGMENT_COMPLETION` at different seams. Their current
step numbers reflect the landed 0010 preflight. Proposal 0011 was resolved by
authorizing rat-authored fact tuples in the daemon instead of deleting the
prompt instruction.

0008 and 0009 were first written by **Parmesan-2**, whose sandbox denied `rk` and
`git commit`; its worktree was reaped on dismissal and both files were lost
uncommitted. The versions here are independent re-derivations checked against the
live event feed (which Parmesan-2 could not read) and against
`supervisor.rs`/`workflow_exec.rs`/`steward.cue`. **0010 is that loss written up
as its own proposal.**

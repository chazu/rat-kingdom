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
| 0004 | Commit before you verify; `rk done` is not a commit | `FRAGMENT_COMPLETION` | proposed |
| 0005 | Reviewer: an empty review branch has two causes | reviewer role | proposed |
| 0006 | Read and endorse open suggestions on entry | `FRAGMENT_SPACE` | proposed |
| 0007 | `cargo fmt` mechanics for the no-reformat rule | `FRAGMENT_GIT_SAFETY` | proposed |

Proposals 0004 and 0005 are the two halves of one failure (empty branches at the
implementer/reviewer seam) and are best landed together.

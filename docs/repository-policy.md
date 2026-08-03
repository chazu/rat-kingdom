# Repository work and delivery policy

Each repository can version its Rat Kingdom lifecycle behavior in
`.rk/repo.cue`. This is where a repository chooses agent branch and worktree
names, the branch work returns to, whether delivery is local or remote, and
whether a successful delivery removes the source branch.

The versioned file is a request, not live authority. The daemon executes the
exact policy digest stored in its machine-local repository registry:

- `rk repo add <path>` validates and activates the current file for a new or
  re-registered checkout.
- An onboarding proposal can stage and validate a changed policy in its
  isolated worktree. `rk repo onboard activate` lands that exact commit and
  activates that exact digest.
- Editing `.rk/repo.cue` directly does not change running behavior. `rk repo
  show <name>` reports the activated digest and whether the versioned file has
  drifted from it.

This split keeps reviewable intent in Git while paths and execution authority
remain operator-owned and machine-local.

## Schema

```cue
repo: {
	work: {
		branch:   "rat/{{agent}}/{{task}}"
		worktree: "{{repo}}/{{agent}}"
	}

	delivery: {
		target:       "agent-base"
		mode:         "merge"
		remote:       "origin"
		remoteBranch: "{{branch}}"
		deleteSource: true
	}
}
```

`work.branch` supports `{{agent}}`, `{{task}}`, `{{repo}}`, and `{{role}}`.
`work.worktree` supports the same placeholders and is relative to Rat Kingdom's
worktree root. Both templates must include `{{agent}}`; absolute paths and
parent traversal are rejected.

`delivery.target` is either `agent-base` or a fixed branch. `agent-base` means
the branch the agent was actually cut from, so work spawned from an integration
branch returns to that integration branch instead of being redirected to
`main`. A fixed value such as `main` or `develop` deliberately pins every
delivery to that branch.

`delivery.remoteBranch` supports `{{branch}}`, `{{target}}`, and `{{repo}}`.
It must retain `{{branch}}` in `push-branch` and `pr` modes so concurrent
workers cannot publish over one another.

## Delivery modes

| Mode | Result |
|---|---|
| `merge` | Merge the source branch into the target in the registered checkout. |
| `merge-push` | Merge locally, then push the updated target to `delivery.remote`. A failed push makes delivery fail even though the local merge remains. |
| `push-branch` | Leave the target untouched and push the source as the rendered `remoteBranch`. |
| `pr` | Push the rendered `remoteBranch` and request a PR/MR against the target. GitLab creates an MR using push options; GitHub and unknown hosts expose the pushed branch/compare URL for manual PR creation. |

Every mode returns a common `delivered` result. Workflows should gate that
field, not infer success from `merged` or `pr_opened`. `deleteSource` applies
after successful `merge`, `merge-push`, or `push-branch`; PR branches are kept
for forge review.

## Steward and trust

The shipped global `steward` workflow is a daemon-managed landing algorithm. A
reviewer approval flows directly into delivery without a separate human gate,
and the completion event carries the daemon-authored target branch into the
workflow. The activated repository policy decides what delivery means for that
repository.

Only the installed global workflow receives the configured automated-landing
exception. A repository-local workflow with the same name cannot inherit that
authority. `agent-base` authorizes the daemon-authored base for a managed
steward run; a fixed target authorizes only that exact target. Manual workflows
and explicit `open_pr` steps retain the normal approval and target-allowlist
checks.

## Legacy registrations

Repositories without `.rk/repo.cue` keep the previous registry behavior:
`--merge-mode direct|pr`, `--remote`, and the daemon's
`default_merge_mode`. Those flags are rejected when `.rk/repo.cue` exists so
there cannot be two competing per-repository policies. Add the file through
onboarding to migrate a legacy registration.

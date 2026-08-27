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

	landing: {
		protectedPaths: "(^|/)(\\.github|\\.rk|migrations)/"
		maxDiffFiles:   50
		maxDiffLines:   2000
		gateTimeout:    "60m"
		reviewTimeout:  "15m"
		reviewMaxWait:  "45m"
	}
}
```

`landing` is the daemon-native landing pipeline's per-repo gate policy (see
[Steward and trust](#steward-and-trust) below) — versioned and digest-activated
exactly like `delivery`, not a separate config surface. The values shown are
the built-in defaults (same names, same defaults the pre-cutover
`steward.cue` workflow hardcoded as params), so an activated policy that
omits `landing` entirely behaves identically to these defaults; only set the
fields you want to change. `protectedPaths` is an ERE matched against
`git diff --name-only <target>...HEAD`; a hit holds the branch for a human.
`maxDiffFiles`/`maxDiffLines` bound the diff a branch may auto-merge with
(`0` disables that budget). `gateTimeout` bounds the repo's real `verify`
check; `reviewTimeout` bounds the wait for a reviewer's verdict before
treating it as a STOP-equivalent hold; `reviewMaxWait` is the hard ceiling the
wait extends to while the reviewer is confirmed still alive past
`reviewTimeout` (a merely slow reviewer is not abandoned at `reviewTimeout`
alone).

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

Every completed rat's branch may be triaged by the daemon-native landing
pipeline when the repository's activated CUE triggers include an
`action: "land"` match. That activated trigger is the unattended-landing
authorization. The same activated policy supplies protected paths, diff
budgets, timeouts, delivery mode, target, and the repository's named `verify`
check. These checks are evaluated mechanically; they do not require an agent.
A protected-path hit, an over-budget diff, or a failed/timed-out check holds
the branch and surfaces attention instead of weakening the policy.

Workflow `land` and `open_pr` steps are a separate path. When
`policy.require_approval_for_landing` is true they require a prior approved
human gate, regardless of workflow name, and their target must match the
activated repository policy. There is no workflow-name exception.

## Activation is mandatory

A registered repository without an activated `.rk/repo.cue` remains visible
to the operator but cannot dispatch or deliver work. Run `rk repo onboard
start <path-or-name>` (or `rk repo add <path>` once the file is committed) to
validate and activate the repository policy. Rat Kingdom does not translate
legacy registry flags into live policy and does not fall back to a fleet-wide
merge mode.

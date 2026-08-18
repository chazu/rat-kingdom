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

Every completed rat's branch is triaged by a daemon-managed landing algorithm,
the **steward**: a reviewer verdict (or, for a doc-only/trivial diff or a
cache hit, no review at all) flows directly into delivery without a separate
human gate, gated by the `landing` policy above plus the repo's real `verify`
check. As of the steward remediation's Phase 3/4 cutover
(`docs/reactor.md`, "Shipped reaction: the steward and the landing pipeline"),
this triage runs two ways, and they are authorized differently:

- **Daemon-native landing pipeline (`action: "land"` trigger, live).**
  `LandingPipeline` (`crates/rk-daemon/src/landing.rs`) calls
  `Supervisor::land` directly — never through the workflow engine — so the
  automated-landing exception described below (`automated_landing_workflows`,
  `require_approval_for_landing`) **does not apply to it at all**. For a repo
  whose installed triggers include an `action: "land"` entry, that trigger's
  own existence and match predicate is the sole unattended-landing
  authorization; the activated `landing` policy (protected paths, diff
  budget, timeouts) is the only per-repo tuning available. This is the
  intended end state, but narrowing/removing the two config fields below
  (steward remediation Phase 4, item 4) is not yet done
  (TKT-01M048ASY8MDB5DVV5VG3WRM47) — until then the fields keep working, but
  only for the workflow-driven path.
- **Workflow-driven mega-workflow (`run: "steward"` trigger, pre-cutover
  reference).** The `land` workflow step this trigger's spawned instance
  reaches is subject to the exception below, unchanged. This is the original
  design; nothing installs it by default anymore, but it remains valid to run
  instead of the daemon-native path (never both at once — see
  `docs/reactor.md`).

### The `automated_landing_workflows` / `require_approval_for_landing` exception (workflow-driven path only)

Only the installed global workflow receives the configured automated-landing
exception (`policy.automated_landing_workflows`, default `["steward"]`). A
repository-local workflow with the same name cannot inherit that authority. A
`land` step also checks `policy.require_approval_for_landing`, unless the
workflow is in the exception list. `agent-base` authorizes the daemon-authored
base for a managed steward run; a fixed target authorizes only that exact
target. Manual workflows and explicit `open_pr` steps retain the normal
approval and target-allowlist checks. Both fields are read only in
`crates/rk-daemon/src/workflow_exec.rs`, so they have no effect on the
daemon-native landing pipeline (see above).

## Legacy registrations

Repositories without `.rk/repo.cue` keep the previous registry behavior:
`--merge-mode direct|pr`, `--remote`, and the daemon's
`default_merge_mode`. Those flags are rejected when `.rk/repo.cue` exists so
there cannot be two competing per-repository policies. Add the file through
onboarding to migrate a legacy registration.

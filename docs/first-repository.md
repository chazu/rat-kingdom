# Your first repository

This walkthrough takes a small repository from registration to one delivered
ticket. It makes policy decisions explicit before workers can land changes.
The [CLI acceptance test](../crates/rk-cli/tests/first_repository_journey.rs)
replays this path with a fake coding provider and disposable Git repositories;
approval, activation, gates, delivery and cleanup use production code.

For an existing application, use its real build/test command in `.rk/checks.cue`
and review its delivery policy with [repository onboarding](repo-onboarding.md).
The README-only checks below are a teaching example.

## Prepare and inspect

Install RK, Git, CUE and one supported, authenticated coding harness. From the RK
source checkout, create a disposable repository and an explicit local remote:

```bash
RK_SOURCE="$PWD"
DEMO="$(mktemp -d)"
DEMO_REMOTE="$(mktemp -d)"
git init --bare "$DEMO_REMOTE"
git init -b main "$DEMO"
cd "$DEMO"
git config user.name 'First Repository'
git config user.email 'first@example.com'
git remote add origin "$DEMO_REMOTE"
printf '# First repository\n' > README.md
mkdir .rk
cp "$RK_SOURCE/examples/first-repo/checks.cue" .rk/checks.cue
git add README.md .rk/checks.cue
git commit -m 'Initialize first repository and named checks'
rk ping
rk repo add "$DEMO" --name first-repo
rk repo onboard inspect first-repo
```

Use an unused registration name. Inspection is read-only and initially reports
the missing policy as a warning. Its readiness result describes assessment
prerequisites, not permission to deliver. Resolve error findings before
proceeding; the report distinguishes observed entrypoints from inferred
recommendations. This example's remote is local, so it requires no forge account.

## Choose, approve and activate policy

Review [the example policy](../examples/first-repo/repo.cue). Its choices are:

| Decision | This walkthrough |
| --- | --- |
| Work isolation | A named agent branch and an RK-owned worktree |
| Target | `main` |
| Publication | Local merge; `origin` is configured but delivery does not push |
| Gates | Named verification, protected-path and README-only scope checks |
| Change budget | One file, at most 20 changed lines |
| Cleanup | Delete the delivered source branch |
| Automation | Dispatch this one ticket manually |

Start an onboarding session and note the `onb-...` session ID:

```bash
rk repo onboard start first-repo
SESSION=onb-...
rk repo onboard status "$SESSION"
```

The onboarder assesses in its isolated worktree. You can discuss alternatives
with it before approving anything. For these exact example choices, prepare a
reviewable patch outside the registered checkout:

```bash
POLICY_STAGE="$(mktemp -d)"
mkdir "$POLICY_STAGE/.rk"
cp "$RK_SOURCE/examples/first-repo/repo.cue" "$POLICY_STAGE/.rk/repo.cue"
(cd "$POLICY_STAGE" && git diff --no-index -- /dev/null .rk/repo.cue) > "$POLICY_STAGE/policy.patch"
cat "$POLICY_STAGE/policy.patch"
rk repo onboard propose "$SESSION" \
  --kind repo_file --title 'First repository: local gated delivery' \
  --evidence 'Reviewed main target, local merge, isolated worktrees and source cleanup' \
  --target .rk/repo.cue --action write_repo_file \
  --diff "$(cat "$POLICY_STAGE/policy.patch")" --risk high \
  --verification 'Validate repository policy and exact activation'
```

`git diff --no-index` returns 1 when it produces a diff; that is expected here.
Record the printed proposal ID and digest, then review status before approving:

```bash
PROPOSAL=onb-prop-...
DIGEST=...
rk repo onboard status "$SESSION"
rk repo onboard approve "$SESSION" "$PROPOSAL" --digest "$DIGEST"
rk repo onboard apply "$SESSION" "$PROPOSAL" --digest "$DIGEST"
rk repo onboard report "$SESSION"
rk repo onboard activate "$SESSION" "$PROPOSAL" --digest "$DIGEST"
rk repo onboard inspect first-repo
rk verify --repo first-repo
```

`apply` validates an isolated commit. `activate` lands that exact approved commit
and records its policy digest. After activation, inspection must report ready.
Changing the versioned policy later requires a new reviewed activation.

## Deliver one bounded ticket

```bash
rk ticket new 'Add the first README sentence' --repo first-repo \
  --body 'Append Verified by Rat Kingdom. to README.md, change no other file, commit, run rk done.'
TICKET=TKT-...
rk work first-repo
rk spawn --ticket "$TICKET"
```

Record the printed agent name and branch. Inspect its progress until it reports
completed. Review the resulting change, then deliver through RK's gates:

```bash
AGENT=...
BRANCH=rat/...
rk status "$AGENT"
rk log "$AGENT"
git diff main..."$BRANCH"
rk dismiss "$AGENT"
rk land "$BRANCH" --repo first-repo --target main --task "$TICKET"
rk ticket show "$TICKET"
rk work first-repo
```

Dismissal preserves the branch and removes the agent worktree. Landing prepares
a candidate, runs the named gates and advances the target to the tested commit.
A failed gate remains a hold: inspect the evidence and repair the branch before
retrying. Completion alone does not prove delivery; the ticket's delivery record
and the target's contents do.

## Clean up and retain evidence

After the onboarder has completed, remove its clean worktree and keep its report:

```bash
rk repo onboard cleanup "$SESSION"
rk repo onboard report "$SESSION"
git status --short
git branch --list "$BRANCH"
```

The checkout should be clean, the delivered source branch absent, and the ticket
delivered. The onboarding branch/report and agent history remain available.
The temporary paths printed by `printf '%s\n' "$DEMO" "$DEMO_REMOTE" "$POLICY_STAGE"`
identify this example's repositories and patch staging directory for later removal.

For daily operation use `rk work`; for deeper diagnosis see the
[operator reference](operator-reference.md), [repository policy](repository-policy.md),
[current architecture](architecture.md), and [external observation](2026-09-02-observation-runs.md).

# Repository onboarding

Repository onboarding is a guided, human-controlled walkthrough that prepares
a repository for reliable Rat Kingdom work. It discovers the repository's
actual tools and conventions, proposes concrete configuration changes, verifies
approved changes in isolation, and only then offers to activate them.

The onboarder is an advisor and implementation assistant. It does not edit the
human checkout, approve its own proposals, or enable automation without an
explicit human decision.

## Quick start

Prime the main operator agent for the walkthrough:

```bash
rk onboard
```

`rk onboard` is exact sugar for `rk prime --role onboarding`. It prints
gate-first onboarding instructions into the current operator session; it does
not launch an agent, create durable state, or change a repository.

Register the repository if needed, then run a read-only inspection:

```bash
rk repo add ~/dev/my-repo --name my-repo
rk repo onboard inspect my-repo
```

The existing durable spawned-assessor workflow remains available for
compatibility:

```bash
rk repo onboard start my-repo --attach
```

Use `--attach` for a live herdr pane. Omit it for a headless session. The
session gets a stable `onb-...` id, an isolated onboarding branch, and a
Rat-Kingdom-owned worktree. The human checkout is not used for edits.

## The workflow

1. **Inspect.** Onboarding reports Git state, remotes and base branch,
   repository instructions, toolchain entrypoints, named checks, repository
   work/delivery policy, workflows, triggers, schedules, and `rk` readiness.
   It compares `.rk/repo.cue` with the exact activated digest. Inspection is
   read-only and does not start an agent or change repository state.

2. **Discuss and propose.** The onboarder explains findings and submits a
   proposal for each meaningful change. A proposal contains the evidence,
   exact diff, target and action, risk, and verification plan. The daemon
   rejects it before journaling unless `git apply --check` succeeds and the
   patch changes exactly the declared target.

3. **Approve or decline.** The human reviews the proposal and its digest:

   ```bash
   rk repo onboard status onb-...
   rk repo onboard approve onb-... onb-prop-... --digest <sha256>
   # or: rk repo onboard decline onb-... onb-prop-... --digest <sha256>
   ```

   The digest binds the decision to the exact repository tree, proposal, and
   requested action. Edited or stale proposals are rejected.

4. **Apply and verify.** Apply an approved proposal in the onboarding
   worktree:

   ```bash
   rk repo onboard apply onb-... onb-prop-... --digest <sha256>
   ```

   Rat Kingdom applies only the approved patch, commits it on the onboarding
   branch, validates the relevant configuration, and runs the named check when
   one was specified. Verification records the command, toolchain, environment
   policy, exit status, timing, and bounded output.

   Generic repository files such as `AGENTS.md` and `mise.toml` are verified by
   the exact-target preflight and content-bound application commit. Multiple
   proposals approved from one assessment may be applied in order; only prior
   application commits journaled in that same session are accepted as the
   chain advances. CUE policy and automation continue through their schema
   validators, and named checks still execute their approved contract.

5. **Activate.** Applying stages and verifies the change; activation is the
   separate decision that lands it in the registered base checkout. Repository
   policy and automation proposals for workflows, triggers, and schedules
   always require this step:

   ```bash
   rk repo onboard activate onb-... onb-prop-... --digest <sha256>
   ```

   Activation fails closed if the base checkout, onboarding branch, target file,
   or approved digest has drifted. Use `decline-activation` to refuse a
   validated change while retaining its branch and report.

### Repository policy decisions

Onboarding should make `.rk/repo.cue` explicit before enabling autonomous
delivery. Review these as repository-owned choices:

- agent branch and worktree naming templates;
- whether completed work targets its actual agent base or a fixed branch;
- whether delivery locally merges, merges and pushes, only pushes a branch, or
  requests a PR/MR;
- remote/remote-branch mapping and source-branch cleanup.

Applying the proposal validates only the staged file. Activation lands the
approved commit and copies that exact file digest into the operator-owned repo
registry. Later direct edits are reported as drift and remain inert. The full
schema is in [Repository work and delivery policy](repository-policy.md).

## Recovery and cleanup

Session state is durable across disconnects and daemon restarts:

```bash
rk repo onboard report onb-...
rk repo onboard resume onb-...
rk repo onboard cleanup onb-...
```

`resume` recovers an orphaned or failed session and reuses its branch and
worktree. `cleanup` removes a clean terminal worktree but retains the branch
and report; it will not remove a running session or one with staged or
unresolved proposals.

## Safety boundaries

- The onboarder role is enforced by the daemon and is read-only/plan-mode at
  the harness boundary.
- The onboarder may inspect the repository, report progress, and submit
  proposals, but cannot approve proposals, mutate tickets or repo registry
  state, spawn ordinary rats, or activate automation.
- Repository-file changes happen only in the onboarding worktree until a human
  activates them.
- Onboarding does not automatically install hooks, change remotes, enable
  schedules or triggers, turn on fleet drain, or change global policy.
- Re-running onboarding reuses the durable session when appropriate and
  reports drift or missing configuration instead of silently duplicating it.

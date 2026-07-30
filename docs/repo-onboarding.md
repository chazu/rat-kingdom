# Repository onboarding

Repository onboarding is a guided, human-controlled walkthrough that prepares
a repository for reliable Rat Kingdom work. It discovers the repository's
actual tools and conventions, proposes concrete configuration changes, verifies
approved changes in isolation, and only then offers to activate them.

The onboarder is an advisor and implementation assistant. It does not edit the
human checkout, approve its own proposals, or enable automation without an
explicit human decision.

## Quick start

Register the repository if needed, then run a read-only inspection:

```bash
rk repo add ~/dev/my-repo --name my-repo
rk repo onboard inspect my-repo
```

Start a durable session for a conversational walkthrough:

```bash
rk repo onboard start my-repo --attach
```

Use `--attach` for a live herdr pane. Omit it for a headless session. The
session gets a stable `onb-...` id, an isolated onboarding branch, and a
Rat-Kingdom-owned worktree. The human checkout is not used for edits.

## The workflow

1. **Inspect.** Onboarding reports Git state, remotes and base branch,
   repository instructions, toolchain entrypoints, named checks, workflows,
   triggers, schedules, and `rk` readiness. Inspection is read-only and does
   not start an agent or change repository state.

2. **Discuss and propose.** The onboarder explains findings and submits a
   proposal for each meaningful change. A proposal contains the evidence,
   exact diff, target and action, risk, and verification plan.

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

5. **Activate.** Applying stages and verifies the change; activation is the
   separate decision that lands it in the registered base checkout. Automation
   proposals for workflows, triggers, and schedules always require this step:

   ```bash
   rk repo onboard activate onb-... onb-prop-... --digest <sha256>
   ```

   Activation fails closed if the base checkout, onboarding branch, target file,
   or approved digest has drifted. Use `decline-activation` to refuse a
   validated change while retaining its branch and report.

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

# Guided repository onboarding workflow

Status: implementation design

## Intent

Give a human a guided onboarding session for a repository so Rat Kingdom can
be configured around the repository's actual tools, checks, branch model, and
trust boundaries. The onboarding agent is an advisor and an implementation
assistant, not an unattended policy setter.

The desired operator experience is:

```text
rk repo onboard <repo> --attach

discover -> explain findings -> propose one change -> human approves/declines
        -> apply approved changes on an onboarding branch -> verify -> review
```

The session must be resumable. A disconnected terminal, a declined proposal,
or a failed check must leave a durable record rather than forcing the human to
repeat discovery.

## Goals

- Make a repository's existing development workflow legible to its human owner
  and to future rats.
- Establish trustworthy, repo-owned verification gates before unattended work.
- Capture the repository's git and branch discipline explicitly.
- Test that the chosen harness and `rk` coordination path work from an agent
  worktree.
- Let the human approve each material repository or castle configuration
  change, with evidence and a reviewable diff.
- Make a completed onboarding session useful to later spawns through existing
  priming, named checks, conventions, and workflow configuration.
- Be safe to run repeatedly. A second run should report drift and missing
  pieces, not duplicate files, checks, workflows, or conventions.

## Non-goals

- Automatically enabling fleet-wide drain, schedules, triggers, or merge policy.
- Replacing the repository's CI system or inventing a new project build tool.
- Treating an agent's observation as permission to change the human's checkout.
- Installing git hooks, changing remotes, changing authentication, or changing
  global Rat Kingdom policy without an explicit human decision.
- Using a free-form shell command from a workflow definition as a trust bypass.

## Existing seams and constraints

The implementation should compose existing surfaces rather than introduce a
second agent lifecycle:

- `rk repo add` persists a machine-local `RepoRecord` containing the path,
  remote, host, and merge mode. Onboarding may idempotently register a path,
  but registration must be shown as a separate decision from repository edits.
- `.rk/checks.cue` is already the repository-owned named-check registry. Its
  commands are the allowlisted inputs to workflow `run` steps when
  `require_named_checks` is enabled.
- Supervisor priming already loads `.rk/checks.cue` and renders its checks into
  worker guidance. A successful onboarding therefore improves later rats
  without adding a second prompt-injection path.
- Repo-local workflows and triggers are separate from the named-check registry.
  They require validation and explicit installation/activation; discovering a
  file is not permission to enable it.
- `--attach` launches an agent in a Rat Kingdom-owned worktree and gives a human
  a live harness session. The onboarding command should use this path for the
  conversational mode and retain headless JSON/report mode for automation.
- Workflow approval is the existing durable human gate. The onboarding state
  must not rely only on chat text or an in-memory process.
- Agent worktrees are the write boundary. The human's checkout is never edited
  directly by the onboarding agent.

The adversarial review found one prerequisite outside onboarding itself:
supervised processes must not be able to drop `RK_AGENT` and `RK_AUTH_TOKEN`
and fall back to operator authority through the shared `RK_HOME`. Onboarding
cannot safely apply or approve anything until that capability boundary is
fixed. This is tracked as `TKT-01KYQR6X2XEDAWEJTSXCSQJE5M` and blocks the
implementation batch.

## Domain model

These terms are canonical for this feature:

- **Onboarding session** — one durable guided assessment for one registered
  repository. It has a stable id and can be resumed.
- **Finding** — read-only evidence discovered during assessment. A finding is
  not an instruction and does not change state.
- **Proposal** — a specific recommended change derived from one or more
  findings. It includes risk, affected files/settings, and the verification
  that will prove it.
- **Human checkpoint** — the explicit approval or decline of one proposal (or a
  clearly named batch of low-risk proposals).
- **Onboarding branch** — the isolated branch where approved repository-file
  changes are applied.
- **Repository configuration** — repo-owned files such as `.rk/checks.cue`,
  workflow definitions, triggers, and the selected instruction file.
- **Castle configuration** — machine-local registration, merge mode, remote,
  harness, and global policy. It must be kept distinct from repository files.
- **Proposal digest** — a canonical hash over the proposal payload, target
  repository identity, onboarding branch/tree revision, and requested action.
  Approval and application are valid only for the exact digest the human
  reviewed.
- **Onboarding report** — the durable final record of findings, decisions,
  applied changes, verification results, and unresolved follow-ups.

## User experience

### Start

`rk repo onboard <path-or-name> [--attach] [--harness <kind>]`

- Resolve and canonicalize the path.
- If unregistered, show the proposed name/path/remote and register only after
  the human confirms. Re-adding the same path is idempotent.
- Create an onboarding session and an isolated branch/worktree.
- Start the onboarding rat with a dedicated onboarding role. The role must be
  distinct from `rat` and `reviewer`, and the daemon must enforce its
  capability set. An unknown role must not silently receive ordinary rat
  permissions. This prevents a prompt-only `onboarder` label from becoming a
  security boundary.
- In attached mode, print the session id and attach target. In headless mode,
  emit machine-readable session state and continue producing report events.

The initial prompt instructs the agent to begin read-only, claim no broad code
area, and stop for a human checkpoint before every mutation. It also requires
the agent to identify the repository's base branch rather than guessing
`main`, and to test `rk ping`/a harmless authenticated read from the actual
worktree before proposing agent execution.

### Guided stages

1. **Identity and access**

   Confirm repository path, registered name, remote/host, current branch,
   default/base branch, worktree cleanliness, and the selected merge mode.
   Report missing remotes or ambiguous base branches; do not repair them
   automatically.

2. **Project workflow discovery**

   Inspect repository-owned instructions and declared tooling: README files,
   AGENTS/CLAUDE/Codex instructions, `mise.toml`, `Makefile`, `justfile`,
   package manifests, CI definitions, and existing `.rk` files. Prefer a
   documented project entrypoint over inferred commands.

3. **Verification gate proposal**

   Identify the smallest trustworthy checks for formatting, type/build
   validation, unit/integration tests, and linting. Show exact commands,
   working directories, expected exit codes, timeout risks, required
   environment, and whether each command mutates state. Propose additions or
   repairs to `.rk/checks.cue`; validate CUE before applying them.

4. **Git discipline proposal**

   Check clean-worktree expectations, commit-before-verify order, branch/base
   resolution, branch naming, remote/merge mode, and whether repository
   instructions explain how a rat proves delivery. Prefer a focused update to
   the repository's existing instruction file; do not create a competing
   instruction hierarchy without approval.

5. **Workflow and policy proposal**

   Inspect available workflow definitions, triggers, schedules, approval gates,
   named-check references, and the castle's `require_named_checks` policy.
   Present these as separate proposals. Repo-local workflow, trigger, and
   schedule files are discovered and reloaded automatically by the daemon, so
   landing an approved file into the registered checkout is the activation
   boundary. Validation or staging in the onboarding branch is not activation.

6. **Agent readiness proof**

   From the onboarding worktree, prove the selected harness can invoke `rk`,
   read the repo scope, and complete a harmless test handshake. Probe the
   toolchain through the repository's pinned runner. Record failures with the
   exact command and environment instead of suggesting that the human loosen
   permissions blindly.

7. **Final review**

   Show the complete diff, accepted/declined proposals, check results, and
   remaining risks. The agent may commit the onboarding branch only after the
   human approves the final set. Landing/merging remains an explicit operator
   action.

## Proposal and checkpoint contract

Every proposal has:

```json
{
  "id": "onb-prop-...",
  "session": "onb-...",
  "kind": "repo_file | castle_config | registration | workflow_activation",
  "title": "Add the repository verification registry",
  "evidence": ["..."],
  "changes": [".rk/checks.cue"],
  "risk": "low | medium | high",
  "verification": ["check:verify", "git diff --check"],
  "status": "proposed"
}
```

The lifecycle is:

```text
proposed -> approved -> applied -> verified
         \-> declined
         \-> failed
```

Approval must be durable and attributable to the human/operator. A proposal
must be bound to its proposal digest and use compare-and-swap semantics: if the
proposal, repository identity, onboarding branch revision, target path, or
action changes, the old approval is stale and cannot be applied. A proposal
that changes repository files is applied only in the onboarding branch. A
castle-level proposal is applied through the existing operator RPC/config
surface, never by writing a guessed config file from the agent worktree.

The application key is `(session, proposal id, digest)`. Replaying an approval,
retrying after a daemon restart, or resuming a disconnected session must be
idempotent. The daemon must persist the decision before applying the change and
persist the application result before reporting success. Recovery re-reads the
proposal and working tree, then resumes or marks the proposal failed; it never
blindly repeats a side effect.

The attached conversation can be the friendly interface, but each decision
also needs a CLI/API representation so it survives disconnects:

- `rk repo onboard status <session>`
- `rk repo onboard approve <session> <proposal>`
- `rk repo onboard decline <session> <proposal>`
- `rk repo onboard resume <session> [--attach]`
- `rk repo onboard report <session> [--json]`

The exact subcommand spelling can follow the existing CLI's command tree, but
the underlying RPC names and state transitions must be stable and idempotent.

## Safety boundaries

- Discovery is read-only and may run repeatedly.
- Proposal generation is not approval.
- Applying a repository proposal is not landing the onboarding branch.
- Validating or staging a workflow is not activation. Because repo-local
  workflow, trigger, and schedule files are auto-discovered, landing them in
  the registered checkout is activation and requires its own final approval.
- A named check is repo-owner-controlled command data; it is not trusted merely
  because an onboarding rat suggested it. Human approval and CUE validation are
  required before it becomes part of the registry.
- Checks run with the environment contract recorded by the onboarding report.
  If the repository's tests must run without `RK_AGENT`, that requirement is
  explicit in the check or runner rather than silently hidden by the rat.
- A malformed or ambiguous finding fails closed for application and remains a
  visible follow-up; it never becomes a guessed default.
- Onboarding must not enable continuous drain, triggers, schedules, or broad
  permission modes as a side effect.
- The onboarding agent cannot approve its own proposals, invoke operator-only
  RPCs, or gain operator authority by removing ambient identity variables.

## Implementation shape

### Slice 0 — prerequisite identity isolation

Land `TKT-01KYQR6X2XEDAWEJTSXCSQJE5M` first. Supervised processes must retain a
non-operator caller identity even when ambient `RK_*` variables are absent, and
operator-only RPCs must remain unavailable to them. Add a regression proving
the exact fallback attack fails. No onboarding ticket is ready to run until
this slice is landed and deployed for the test harness.

### Slice A — read-only assessment

Add a reusable repository assessment service and `rk repo onboard inspect`.
It returns stable JSON plus a concise human rendering for identity, tooling,
instructions, checks, workflows/triggers, git state, and agent readiness. The
assessment is deterministic, bounded, and does not write repository files.

The inspect command is independently demoable: it performs no onboarding-agent
launch and no writes, and a fixture can compare its report against expected
findings.

### Slice B — guided session and enforced role

Add the onboarding session record, dedicated role priming, attached/headless
launch, resume/status/report RPCs, and the onboarding branch lifecycle. A
session can complete its assessment without applying any proposal. The daemon
must reject an unknown or downgraded onboarding role and persist orphaned/live
session recovery state rather than relying on in-memory attach tracking.

### Slice C — content-bound checkpoints

Add proposal persistence and operator approval/decline commands. Canonicalize
and digest the proposal, bind it to the repo identity and onboarding tree
revision, and reject stale or replayed decisions. Show the diff before approval
and implement one complete named-check proposal path for `.rk/checks.cue`.
Keep castle-level changes on their existing operator-owned surfaces.

### Slice D — verification and explicit activation

Validate and execute approved named checks, report environment/toolchain
details, verify workflow references and trigger/schedule definitions, and make
landing an approved workflow/trigger/schedule file the explicit activation
step. Produce the final onboarding report and a machine-readable readiness
result. Add restart/replay tests for every persisted transition.

### Slice E — operational fixtures and documentation

Add fake repositories covering missing tools, ambiguous branches, malformed
checks, dirty worktrees, existing instruction files, approval disconnects, and
reruns. Document the operator flow and the non-destructive boundaries.

Each slice must include an end-to-end command/API path and focused regression
coverage. The slices are ordered by dependency but each is demoable on its own:
read-only inspection, no-op guided session, one content-bound named-check
proposal, and finally activation/recovery. No slice should require a human to
manually edit JSON state or a worktree outside the Rat Kingdom lifecycle.

## Acceptance bar

The feature is ready when a fresh repository can be onboarded with this proof:

1. The human sees an evidence-backed assessment before any repository mutation.
2. The human approves a named-check proposal; the check lands only on the
   onboarding branch and validates through CUE.
3. The human declines a workflow activation; the definition remains available
   but no trigger/schedule is enabled.
4. The onboarding session survives disconnect and resumes at the pending
   checkpoint.
5. The final report distinguishes accepted, declined, failed, and unresolved
   proposals and records exact verification results.
6. A later `rk spawn --ticket` receives the repo-owned checks in its priming
   and can use the documented verification path.
7. Running onboarding a second time produces a drift report rather than
   duplicate files or duplicated durable conventions.

## Adversarial review disposition

Reviewer rat Cheesethief-2 completed an independent review on 2026-07-29.
`mise run verify` passed completely in the review worktree. The review approved
the document as a reviewable design but found that it was not implementation
ready without the changes above:

- supervised-agent/operator credential isolation is a blocker;
- current workflow approval is instance-wide, caller-supplied, and not bound to
  proposal content or revision;
- repo-local workflow, trigger, and schedule discovery is automatic, making
  landing the activation boundary;
- onboarding needs an enforced capability/profile, durable replay semantics,
  exact named-check digests, stable repository identity, and more vertical
  slices.

The detailed review is artifact `01KYQR881BZWS180Z0Q531G6GT`. The reviewer filed
the identity prerequisite as `TKT-01KYQR6X2XEDAWEJTSXCSQJE5M` and the design
hardening follow-up as `TKT-01KYQR6X34KCN7NDKXP0KNYZTV`.

## Open decisions for adversarial review

- Whether the onboarding session should use a new `onboarder` role or a
  workflow-owned role/profile layered over the existing role renderer.
- Whether proposal state belongs in workflow-instance persistence, a dedicated
  onboarding store, or both (workflow for lifecycle, report/artifacts for
  repository evidence).
- Whether the first implementation should support castle-level policy changes
  or report them as operator follow-ups only.
- Whether approval is per proposal only, or permits explicit low-risk batches.
- Whether the initial command should require `--attach` for guided mode or
  default to attached when a human terminal is present.

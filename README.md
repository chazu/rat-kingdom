# rat-kingdom

Rat Kingdom runs bounded coding tasks in isolated Git worktrees, checks changes
before delivery, and retains the evidence needed to operate and recover the
work. A Rust daemon owns agent generations, repository policy, tickets,
workflows and landing; the CLI presents the current work and its next actions.

## Install

You need Git, Rust, the [CUE CLI](https://cuelang.org), and an authenticated coding
harness: Claude Code, Codex CLI, Jcode, or Maki (headless-only in v1, ordinary
mutable roles only). Herdr is optional for interactive attach.
The repository's `mise.toml` records its development tools and verification tasks.

```bash
cargo build --release
export PATH="$PWD/target/release:$PATH"
rk ping
```

RK stores its daemon, worktrees, logs and durable records under
`~/.rat-kingdom/`; `RK_HOME` selects a separate instance.

## First repository

Follow [Your first repository](docs/first-repository.md) to create a disposable
README-only project, inspect readiness, review and activate explicit CUE policy,
deliver one ticket through gates, and clean up its worktrees. The guide has an
[automated CLI acceptance test](crates/rk-cli/tests/first_repository_journey.rs).

The journey is:

1. Register the repository and inspect its actual tools, checks and readiness.
2. Choose the branch/worktree layout, delivery target, publication mode and gates.
3. Review an exact onboarding proposal, approve it, apply it in isolation, then
   activate the verified policy.
4. Dispatch one bounded ticket, inspect its result and land it through RK.
5. Confirm the durable delivery and clean up while retaining the report.

For an existing application, use its real verification command and follow
[repository onboarding](docs/repo-onboarding.md). The
[policy reference](docs/repository-policy.md) explains activation, drift and
protected-path decisions. Registration alone does not enable delivery.

## Daily operation

```bash
rk work my-repo
rk spawn --ticket TKT-...
rk status <agent>
rk log <agent>
rk dismiss <agent>
rk land <branch> --repo /path/to/my-repo --target <approved-target> --task TKT-...
rk work my-repo
```

`rk work` separates live agents, ready tickets, actions, decisions and stalls.
Resolve a displayed action or decision, then read it again. Agent completion,
green checks and durable delivery are separate facts. A failed gate retains its
source branch and evidence for repair.

For a bad delivery, `rk revert <agent>` records a durable undo operation. If a
write fails, retry its printed `--operation` command; it resumes the original
revert across restart without duplicating the Git change or completion fact.

## Explore

| Need | Reference |
| --- | --- |
| Commands, harnesses, configuration, workflows and King administration | [Operator reference](docs/operator-reference.md) |
| Module ownership and current release acceptance | [Architecture and acceptance map](docs/architecture.md) |
| Native dashboard, saved evidence, scorecards and typed proposals | [Factory Foreman](docs/factory-foreman.md) |
| Product initiatives through implementation and independent verification | [Product-to-code](docs/product-to-code.md) |
| Repository event automation | [Reactor](docs/reactor.md) |
| Protected branch and forge delivery | [PR/MR mode](docs/pr-merge-mode.md) |
| Bounded external sampling and reproducible pilot reports | [Observation runs](docs/2026-09-02-observation-runs.md) |
| Release scope and remaining qualification work | [Roadmap](docs/ROADMAP.md) |

The current release target is a trusted foreign repository using direct merge.
The saved foreign pilot failed its registered thresholds and requires repair
and repeat. Source tests and individual successful deliveries do not establish
unattended-operation qualification.

## Develop

```bash
mise run verify
mise run verify-full
```

`verify-full` is the complete source acceptance gate. Building or testing does
not update an installed CLI or replace a running daemon; deployment is a separate
operation. See the [current improvement checklist](docs/2026-09-05-usefulness-improvements.md)
for this refactoring's requirements and validation evidence.

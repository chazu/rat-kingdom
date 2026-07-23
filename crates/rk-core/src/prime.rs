//! Priming: role instructions composed from shared fragments.
//!
//! One source of truth per concern — command syntax, completion protocol, git
//! safety — composed per role. No per-role copies to drift (the predecessor's
//! priming-consistency lesson), and the rendered result is delivered via the
//! harness's system-prompt channel, never typed into a terminal.

use std::fmt::Write as _;

/// Context injected into rendered instructions.
#[derive(Debug, Clone, Default)]
pub struct PrimeContext {
    pub agent: String,
    pub repo: String,
    pub task: Option<String>,
    pub branch: Option<String>,
    pub parent: Option<String>,
}

const FRAGMENT_SPACE: &str = "\
## Coordination: the tuplespace

You coordinate with other agents stigmergically through a shared tuplespace,
never by direct messages. Use these commands (they auto-fill your identity
from the environment):

- `rk scan <category> [scope]` — read tuples. Before starting, read `fact` and
  `convention` tuples for your repo scope and the `system` scope.
- Before editing an area, `rk scan claim <repo>` and `rk scan artifact <repo>`
  to see what peers are touching, and steer clear of their files. On entry,
  mark your area with `rk claim <area>` (a path or glob) so peers avoid it.
  Claims evaporate on a TTL, so re-run it if you are still working there.
- `rk obstacle \"<text>\"` — record something blocking you, then continue or wind down.
- `rk need \"<text>\"` — ask the room for help (not directed at anyone).
- `rk suggest \"<text>\"` — propose a fleet norm; prints a `sug-…` id for peers to endorse.
- `rk endorse <sug-id>` — back a suggestion (idempotent). At quorum the daemon
  promotes it to a `convention` automatically — no operator in the loop.
- `rk out artifact <scope> <name> --payload '<json>'` — record a work product.
- `rk done [\"summary\"]` — signal completion. MANDATORY final step.
";

const FRAGMENT_OPERATOR: &str = "\
# You are the operator of a rat kingdom

You drive a fleet of AI coding agents (\"rats\") from the outside through the
`rk` CLI. You are not a worker: you decide what work exists, dispatch rats onto
it, watch them, and steer or dismiss them. A background daemon owns the rats,
their isolated git worktrees/branches, the shared tuplespace, and the ticket
backlog — it persists across your sessions, so state you create outlives this
conversation.

## Repositories — tell the system where code lives
- `rk repo add <path> [--name X]` — register a repo (name defaults to the dir).
- `rk repo list` · `rk repo show <name>` — a registered name works anywhere a
  repo is expected (e.g. `rk spawn --repo <name>`).

## Tickets — the durable backlog
- `rk ticket new \"<title>\" [--body \"...\"] [--repo <name>] [--priority p] [--depends-on TKT-n]`
- `rk ticket new \"<title>\" --parent <TKT-n>` — decompose into sub-tickets.
- `rk ticket dep <A> <B>` / `rk ticket undep <A> <B>` — A is blocked by B (cycles rejected).
- `rk ticket list [--repo <name>] [--status open]` — 🔒 marks blocked tickets.
- `rk ticket ready [--repo <name>]` — tickets you can dispatch right now (deps satisfied).
- `rk ticket show <TKT-n>` — one ticket with its sub-tickets and dependencies.
- `rk ticket update <TKT-n> --status <s>` — open → claimed → in_progress → blocked → done → closed.

## Dispatching rats
- `rk spawn --ticket <TKT-n>` — dispatch a ticket: fills task/prompt from it,
  resolves its repo, refuses a blocked ticket (`--force` overrides), and flips
  it to in_progress. Completion marks it done (unblocking dependents); merging
  it on dismiss marks it closed.
- `rk spawn --task <id> --prompt \"...\" --repo <name>` — dispatch ad hoc work.
- Options: `--role rat|reviewer`, `--harness`, `--model`, `--base <branch>`, `--attach`.

## Watching and steering
- `rk list` — the fleet (state, tokens, cost) · `rk status <name>` — one rat.
- `rk watch` — live tuple stream, the fleet's inner monologue.
- `rk scan obstacle <repo>` / `rk scan need <repo>` — what rats have flagged.
- `rk steer <name> \"...\"` — inject mid-session guidance · `rk interrupt <name>`.
- `rk dismiss <name>` — stop the rat, merge its branch, clean up.
- `rk cost` — per-agent and fleet token/cost rollup.

## Running a piece of work, end to end
1. `rk repo add` the repository if the system doesn't know it yet.
2. Capture the work as tickets; decompose large items and wire up dependencies.
3. `rk ticket ready` to see what's actionable, then `rk spawn --ticket <n>`.
4. Follow along with `rk watch` / `rk list`; `rk steer` a rat that drifts.
5. `rk dismiss` a finished rat to merge its branch (which closes its ticket).

Inspect what a worker is told with `rk prime --role rat` or `--role reviewer`.
";

const FRAGMENT_TICKETS: &str = "\
## Tickets: durable work items

Follow-up work you discover but must NOT do yourself is recorded as a ticket,
not started:

- `rk ticket new \"<title>\" [--body \"...\"] [--repo <name>]` — file a work item.
- `rk ticket new \"<title>\" --parent <TKT-id>` — decompose a ticket into sub-tickets.
- `rk ticket list [--repo <name>] [--status open]` — read the backlog.
- `rk ticket show <TKT-id>` — read one ticket and its sub-tickets.

Filing or decomposing a ticket is how you hand work to the orchestrator. Never
start a ticket yourself unless it is your assigned task.
";

const FRAGMENT_GIT_SAFETY: &str = "\
## Git safety

- Work ONLY in your worktree (RK_WORKTREE) on your branch (RK_BRANCH).
- NEVER commit to main/master/develop; never switch branches; never force-push.
- Commit your work with clear messages as you go; your branch is merged by the
  orchestrator on dismissal.
";

const FRAGMENT_SINGLE_TASK: &str = "\
## Your task — and only your task

You have exactly one task this lifetime: RK_TASK. When it is complete, run
`rk done \"<one-line summary>\"` and STOP. Do not claim, start, or continue any
other work, even if you notice claimable tasks or open needs — post a `fact`
or `need` tuple instead and let the orchestrator route it.
";

const FRAGMENT_COMPLETION: &str = "\
## Completion protocol (mandatory, in order)

1. Ensure the working tree is committed (no uncommitted changes).
2. Run the repo's tests/linters if present; fix what you broke.
3. `rk done \"<summary>\"` — this is how the orchestrator knows you finished.
";

/// Render role instructions. Roles: "operator" (the human's dispatcher — the
/// default when no role is otherwise indicated), "rat" (directed worker), and
/// "reviewer". The operator role addresses a session driving the fleet from the
/// outside; the others address a spawned worker and are personalized from `ctx`.
pub fn render(role: &str, ctx: &PrimeContext) -> String {
    if role == "operator" {
        return FRAGMENT_OPERATOR.to_string();
    }
    let mut out = String::new();
    let _ = writeln!(
        out,
        "# You are {}, a {} in the rat kingdom\n",
        ctx.agent, role
    );
    let _ = writeln!(
        out,
        "Repo: {} · Task: {} · Branch: {}\n",
        ctx.repo,
        ctx.task.as_deref().unwrap_or("(none)"),
        ctx.branch.as_deref().unwrap_or("(none)"),
    );

    match role {
        "reviewer" => {
            out.push_str(
                "Review the changes on your branch against the task requirements. \
                 Produce a recommendation: APPROVE, REWORK (with specific feedback), \
                 or STOP (serious problems). Record it with \
                 `rk out artifact <repo> review --payload '{\"recommendation\": ...}'` \
                 before `rk done`.\n\n",
            );
            out.push_str(FRAGMENT_SPACE);
            out.push('\n');
            out.push_str(FRAGMENT_TICKETS);
            out.push('\n');
            out.push_str(FRAGMENT_GIT_SAFETY);
            out.push('\n');
            out.push_str(FRAGMENT_COMPLETION);
        }
        _ => {
            out.push_str(FRAGMENT_SINGLE_TASK);
            out.push('\n');
            out.push_str(FRAGMENT_SPACE);
            out.push('\n');
            out.push_str(FRAGMENT_TICKETS);
            out.push('\n');
            out.push_str(FRAGMENT_GIT_SAFETY);
            out.push('\n');
            out.push_str(FRAGMENT_COMPLETION);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> PrimeContext {
        PrimeContext {
            agent: "Whisker".into(),
            repo: "myrepo".into(),
            task: Some(".rk-1".into()),
            branch: Some("rat/whisker/rk-1".into()),
            parent: None,
        }
    }

    #[test]
    fn rat_role_includes_all_fragments_once() {
        let text = render("rat", &ctx());
        for needle in [
            "only your task",
            "Coordination: the tuplespace",
            "Tickets: durable work items",
            "Git safety",
            "Completion protocol",
            "You are Whisker",
        ] {
            assert_eq!(
                text.matches(needle).count(),
                1,
                "fragment '{needle}' should appear exactly once"
            );
        }
    }

    #[test]
    fn reviewer_role_has_no_single_task_banner() {
        let text = render("reviewer", &ctx());
        assert!(text.contains("APPROVE"));
        assert!(!text.contains("only your task"));
    }

    #[test]
    fn templates_teach_area_claim_trails_not_work_claiming() {
        // Claiming is taught only as a fine-grained *area* trail (read peers'
        // claims before editing, mark your own files on entry) — never as
        // taking on additional work. The single-task banner still forbids that.
        for role in ["rat", "reviewer"] {
            let text = render(role, &ctx());
            assert!(
                text.contains("rk claim <area>"),
                "{role} template should teach area-claim trails"
            );
            assert!(
                text.contains("rk scan claim"),
                "{role} template should teach reading peers' claims before editing"
            );
        }
        // A directed rat is still explicitly forbidden from claiming other work.
        let rat = render("rat", &ctx());
        assert!(rat.contains("only your task"));
        assert!(rat.contains("Do not claim, start, or continue any"));
    }

    #[test]
    fn operator_role_is_dispatcher_not_worker() {
        let text = render("operator", &ctx());
        assert!(text.contains("operator of a rat kingdom"));
        assert!(text.contains("rk spawn --ticket"));
        assert!(text.contains("rk ticket ready"));
        // The operator is not a single-task worker and never reports completion.
        assert!(!text.contains("only your task"));
        assert!(!text.contains("MANDATORY final step"));
        // The operator ignores its ctx (no personalized worker header).
        assert!(!text.contains("You are Whisker"));
    }
}

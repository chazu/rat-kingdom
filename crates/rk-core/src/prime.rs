//! Priming: role instructions composed from shared fragments.
//!
//! One source of truth per concern — command syntax, completion protocol, git
//! safety — composed per role. No per-role copies to drift (imp's
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
- `rk obstacle \"<text>\"` — record something blocking you, then continue or wind down.
- `rk need \"<text>\"` — ask the room for help (not directed at anyone).
- `rk out artifact <scope> <name> --payload '<json>'` — record a work product.
- `rk done [\"summary\"]` — signal completion. MANDATORY final step.
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

/// Render role instructions. Roles: "rat" (directed worker), "reviewer".
pub fn render(role: &str, ctx: &PrimeContext) -> String {
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
            out.push_str(FRAGMENT_GIT_SAFETY);
            out.push('\n');
            out.push_str(FRAGMENT_COMPLETION);
        }
        _ => {
            out.push_str(FRAGMENT_SINGLE_TASK);
            out.push('\n');
            out.push_str(FRAGMENT_SPACE);
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
    fn reviewer_role_has_no_claim_loop_and_no_single_task_banner() {
        let text = render("reviewer", &ctx());
        assert!(text.contains("APPROVE"));
        assert!(!text.contains("only your task"));
        // No template may ever teach a directed agent to claim work.
        assert!(!text.contains("rk claim"));
    }

    #[test]
    fn no_template_teaches_claiming() {
        for role in ["rat", "reviewer"] {
            assert!(
                !render(role, &ctx()).contains("rk claim"),
                "{role} template must not teach claiming"
            );
        }
    }
}

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
    /// Recent facts pre-scanned by the caller from the tuplespace for this
    /// rat's repo scope + system. The renderer caps injected facts at
    /// MAX_INJECTED_FACTS so durable history cannot grow a prompt without
    /// bound. Empty means the section is omitted.
    pub facts: Vec<String>,
    /// Active fleet conventions (promoted norms), pre-scanned by the caller from
    /// the tuplespace for this rat's repo scope + `system`. Composed verbatim
    /// into a "Standing conventions" section so a promoted norm changes an
    /// already-spawned rat's behaviour (stigmergy P6) instead of relying on the
    /// rat choosing to `rk scan convention`. Empty ⇒ the section is omitted.
    pub conventions: Vec<String>,
}

/// Maximum number of fact entries injected into one worker prompt.
pub const MAX_INJECTED_FACTS: usize = 10;

const FRAGMENT_SPACE: &str = "\
## Coordination: the tuplespace

You coordinate with other agents stigmergically through a shared tuplespace.
Daemon-routed directed messages are reserved for structural parent completion
notices; use tuples for all other coordination. Use these commands (they
auto-fill your identity from the environment):

- `rk scan <category> [scope]` — read tuples. Before starting, read `fact` and
  `convention` tuples for your repo scope and the `system` scope.
- On entry, also `rk scan suggestion system` and endorse every open proposal you
  agree with: `rk endorse <sug-id>`. A suggestion needs 3 DISTINCT endorsers to
  become binding. A ballot stays open until it reaches that quorum — it does \
  not expire on a clock — but it also never promotes on its own, so a proposal only
  ever becomes a rule if passing rats spend the one command on it. This is not
  extra work: it is a single cheap call, and it is the only way the fleet turns a
  lesson into a rule without a human. Endorse the existing suggestion rather than
  minting a near-duplicate.
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
- `rk ticket new \"<title>\" [--body \"...\"] [--repo <name>] [--priority p] [--depends-on <TKT-id>]`
- `rk ticket new \"<title>\" --parent <TKT-id>` — decompose into sub-tickets.
- `rk ticket dep <TKT-id> <TKT-id>` / `rk ticket undep <TKT-id> <TKT-id>` — the first is blocked by the second (cycles rejected).
- `rk ticket list [--repo <name>] [--status open]` — 🔒 marks blocked tickets.
- `rk ticket ready [--repo <name>]` — tickets you can dispatch right now (deps satisfied).
- `rk ticket show <TKT-id>` — one ticket with its sub-tickets and dependencies.
- `rk ticket update <TKT-id> --status <s>` — open → claimed → in_progress → blocked → done → closed.

## Dispatching rats
- `rk spawn --ticket <TKT-id>` — dispatch a ticket: fills task/prompt from it,
  resolves its repo, refuses a blocked ticket (`--force` overrides), and flips
  it to in_progress. Completion marks it done (unblocking dependents); merging
  it on dismiss marks it closed.
- `rk spawn --task <id> --prompt \"...\" --repo <name>` — dispatch ad hoc work.
- Options: `--role rat|reviewer`, `--harness`, `--model`, `--base <branch>`, `--attach`.

## Watching and steering
- `rk list` — the fleet (state, tokens, cost) · `rk status <name>` — one rat.
- `rk log <name>` — a rat's transcript (prose, tool calls, retries); `--follow` to stream.
- `rk watch` — live tuple stream, the fleet's inner monologue.
- `rk workflow watch <wf-id>` — replay the current workflow snapshot, follow
  durable state transitions, refresh after a lag, and exit when the workflow
  completes or fails. The plain output prints the coordinator cursor; use
  `--json` when another agent must save cursors from snapshot/event records.
  Resume after a disconnect with `--after <cursor>`. If a `lagged` or `resync`
  record appears, treat the refreshed snapshot as authoritative and continue
  from its cursor. `rk top` and raw `rk watch` are dashboards, not a reliable
  replacement for this workflow watch/replay path.
- `rk monitor --coordinator <session-id> --once` — read bounded attention and
  middle-rat rollups for all workflows owned by that coordinator session.
  Add `--follow` for a live NDJSON stream, or `--subtree <middle-rat>` to
  drill into one reporting boundary. Run the one-shot read before meaningful
  decisions. Rat Kingdom cannot inject into an arbitrary Codex, Claude Code, or
  other host session; a host wrapper may call this command at its turn boundary
  if the host exposes such a hook. Monitoring is advisory: it never steers,
  dismisses, merges, retries, or approves work.
- `rk scan obstacle <repo>` / `rk scan need <repo>` — what rats have flagged.
- `rk steer <name> \"...\"` — inject mid-session guidance · `rk interrupt <name>`.
- `rk dismiss <name>` — stop the rat, merge its branch, clean up.
- `rk cost` — per-agent and fleet token/cost rollup.
- `rk prune` — archive settled dead records (completed/failed/dismissed) out of
  `rk list`/`rk top` once they pile up, AND settled workflow instances out of
  `rk workflow list`/`rk inbox`. Nothing is lost: cost/usage/lineage survive,
  `rk list --archived` / `rk workflow list --archived` show them, and
  `rk unarchive <name>` / `rk workflow unarchive <id>` restore one. Live and
  orphaned rats, and running instances, are never archived. `--dry-run` to
  preview.
- `rk workflow prune <id>` — clear ONE settled instance (the resolving action on
  an `rk inbox` `workflow-failed` row). Refuses a running or unknown id.

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
- Keep your diff to the files your task touches. NEVER commit a workspace-wide
  reformat (e.g. a bare `cargo fmt` that reflows untouched files — a newer
  toolchain will do this). `cargo fmt` cannot be scoped — it reflows every
  target in the workspace whatever paths you pass it. Format by naming your
  files to the formatter directly (`rustfmt <files you changed>`), not through
  `cargo fmt`, and before you commit revert any fmt/toolchain churn elsewhere:
  `git checkout -- <untouched files>`. Expect the committed baseline to be
  fmt-dirty under a newer toolchain than the one it was written with: a failing
  `cargo fmt --check` over files you did not touch is the pre-existing state,
  not a gate you must satisfy and not yours to fix. A reformat sweep races peers
  editing those same files and buries your real change in review.
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

const FRAGMENT_FOREMAN: &str = "\
## Foreman role — coordinate, do not implement

You are a foreman: a middle-rat responsible for turning one feature set into
integrated work. Do not edit source code yourself. Your branch (`RK_BRANCH`) is
the shared integration branch for your workers, and the workflow will merge it
when you finish.

Build a dispatch table from the parent ticket and its children. Keep at most
the configured number of workers active. For every worker, use:

`rk spawn --ticket <ticket> --parent \"$RK_AGENT\" --base \"$RK_BRANCH\"`

The daemon authenticates the parent and forces those lineage fields, but keep
them explicit in the command so the integration intent is visible. A worker's
completion is delivered as a directed message:

`rk rd message \"$RK_REPO\" \"$RK_AGENT\" --timeout 2m`

On completion, inspect the worker's branch and result, then run `rk dismiss
<worker>` to merge that worker into your integration branch. Do not dismiss a
worker whose work is missing or failed; respawn, steer, or file an obstacle as
appropriate. Run the configured check after each accepted merge when practical.

Workers must commit, run their own verification, and finish with `rk done`; do
not ask them to dismiss themselves. If a worker is blocked, record the issue
and decide whether to steer, respawn, or re-dispatch it. Continue independent
work while one item is blocked, but never claim the feature set is complete
until every required item is integrated or explicitly escalated.

Publish semantic checkpoints at meaningful milestones (not every tool call):

`rk progress --summary \"4/7 child tickets complete\" --next \"reviewing the remaining three\"`

If a child is blocked or needs coordinator input, report that in `--status
blocked` and also use `rk obstacle` or `rk need` when durable detail is useful.

Before finishing, run the final integration check on `RK_BRANCH`, summarize the
completed and unresolved items, and run `rk done \"<summary>\"`. STOP after that.
";

const FRAGMENT_COMPLETION: &str = "\
## Completion protocol (mandatory, in order)

1. Commit BEFORE you verify, not after. Your branch is read by other agents
   while you are still working — a reviewer chains off it the moment your
   task is reported done, and an empty branch reads as a lost delivery. Never
   start a long verification run, and never end a turn, with the work sitting
   uncommitted in your worktree. Amend or add commits as verification forces
   changes.
2. Verify with the project's own build, tests, and linters — for a Rust crate
   that means `cargo build`, `cargo test`, and `cargo clippy` all pass. A
   partial check (e.g. `cue vet`) is NOT verification: the code must actually
   compile and the suite must actually run green.
3. Never `rk done` on a build you broke. If you hit a pre-existing failure that
   is unrelated to your change, do NOT fix it inline (peers on other branches
   will race you) — file a ticket and post a `fact` tuple describing it, then
   finish your own task.
4. Prove the branch carries the work before you signal. `rk done` is NOT a
   commit: run `git status --porcelain` (must be empty) and
   `git log <base>..HEAD` (must be non-empty). If a verification command is
   still running, wait for it — do not report while it is in flight.
5. Before you finish, review the injected facts that were relevant to your task.
   If a fact materially helped and appears correct, run `rk fact vote <fact-id> up`;
   if it is incorrect or harmful, run `rk fact vote <fact-id> down`. Use `clear`
   to retract an earlier vote. Vote only where you have a grounded view; this is
   optional and never replaces filing a ticket for a problem. Then run
   `rk done \"<summary>\"` — this is how the orchestrator knows you finished.
";

/// Compose the active fleet conventions into a binding "Standing conventions"
/// section, or `None` when there are none. Kept separate so `render` stays a
/// straight-line composition and the section can be tested in isolation.
fn render_conventions(conventions: &[String]) -> Option<String> {
    // Skip blanks (a convention whose source suggestion decayed can carry no
    // text) and de-duplicate while preserving first-seen order — the same
    // convention may surface under both the repo and system scopes.
    let mut seen = std::collections::HashSet::new();
    let mut section = String::from(
        "## Standing conventions\n\n\
         The fleet has promoted these norms to binding conventions. Follow them \
         as you work — they override your default approach where they conflict:\n\n",
    );
    let mut any = false;
    for text in conventions {
        let text = text.trim();
        if text.is_empty() || !seen.insert(text) {
            continue;
        }
        let _ = writeln!(section, "- {text}");
        any = true;
    }
    any.then_some(section)
}

/// Compose recent fact context into a bounded Known facts section, or None
/// when there are no usable facts. Facts are observations, not binding
/// conventions; the prompt says so explicitly to keep the two kinds distinct.
fn render_facts(facts: &[String]) -> Option<String> {
    let mut section = String::from(
        "## Known facts\n\n\
         These are observations from the fleet, not binding conventions. Use \
         them as context and verify them when they matter to your task:\n\n",
    );
    let mut any = false;
    for fact in facts.iter().take(MAX_INJECTED_FACTS) {
        let fact = fact.trim();
        if fact.is_empty() {
            continue;
        }
        let _ = writeln!(section, "- {fact}");
        any = true;
    }
    any.then_some(section)
}

/// Render role instructions. Roles: "operator" (the human's dispatcher — the
/// default when no role is otherwise indicated), "rat" (directed worker),
/// "reviewer", and "foreman". The operator role addresses a session driving
/// the fleet from the outside; the others address a spawned worker and are
/// personalized from `ctx`.
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

    // Standing conventions ride high in the prompt (right under the identity
    // header) so a promoted norm is binding context, not something the rat must
    // remember to go read. Omitted entirely when there are none.
    if let Some(section) = render_conventions(&ctx.conventions) {
        out.push_str(&section);
        out.push('\n');
    }
    if let Some(section) = render_facts(&ctx.facts) {
        out.push_str(&section);
        out.push('\n');
    }

    match role {
        "foreman" => {
            out.push_str(FRAGMENT_FOREMAN);
            out.push('\n');
            out.push_str(FRAGMENT_SPACE);
            out.push('\n');
            out.push_str(FRAGMENT_TICKETS);
            out.push('\n');
            out.push_str(FRAGMENT_GIT_SAFETY);
            out.push('\n');
            out.push_str(FRAGMENT_COMPLETION);
        }
        "reviewer" => {
            out.push_str(
                "Review the changes on your branch against the task requirements. \
                 FIRST establish there are changes: run `git log <base>..HEAD`. An \
                 EMPTY branch is not a verdict — it has two causes needing OPPOSITE \
                 verdicts, so disambiguate before you judge. Find the implementer's \
                 commit (`rk scan artifact <repo>` records the sha) and run \
                 `git merge-base --is-ancestor <sha> main`:\n\
                 - NOT an ancestor ⇒ the work was never committed (check the \
                 implementer's branch and worktree — it may still be live with the \
                 work staged). APPROVE would merge a no-op and lose the work: \
                 REWORK, naming exactly what is missing.\n\
                 - IS an ancestor ⇒ the work already landed and you are a duplicate \
                 reviewer. REWORK here manufactures a rework loop for finished work: \
                 verify the LANDED code against the task, then APPROVE.\n\
                 Never APPROVE an empty branch you have not disambiguated.\n\
                 Produce exactly one recommendation, choosing by what should happen next:\n\
                 - APPROVE — clean and safe to auto-merge as-is.\n\
                 - REWORK — fixable issues remain. Give specific, actionable feedback; \
                 this is auto-handed-off as a follow-up ticket, no human needed. Prefer \
                 REWORK for anything a rat could fix.\n\
                 - STOP — reserve for genuine dead-ends: fundamentally wrong, unsafe, or \
                 needing a human judgment call. STOP parks the work for a human and pages \
                 the operator, so do NOT use it for anything REWORK can carry.\n\
                 Record it with \
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
            facts: Vec::new(),
            conventions: Vec::new(),
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
    fn foreman_role_is_a_delegator_with_parent_merge_contract() {
        let text = render("foreman", &ctx());
        for needle in [
            "You are a foreman",
            "Do not edit source code yourself",
            "--parent \"$RK_AGENT\" --base \"$RK_BRANCH\"",
            "rk rd message",
            "rk dismiss",
            "run `rk done",
        ] {
            assert!(text.contains(needle), "foreman prompt missing {needle:?}");
        }
        assert!(!text.contains("You have exactly one task"));
    }

    #[test]
    fn reviewer_disambiguates_an_empty_branch_before_reaching_a_verdict() {
        // An empty review branch has two causes needing OPPOSITE verdicts
        // (fact `empty-review-branch-has-two-causes`, TKT-127): work never
        // committed ⇒ REWORK, work already merged ⇒ APPROVE. Getting it wrong
        // is expensive in both directions — a wrong REWORK manufactured
        // TKT-127/128/129, a wrong APPROVE would have silently lost TKT-113's
        // 283 lines — so the mechanical check is pinned here rather than left
        // to each reviewer to re-derive from a repo-scoped fact.
        let text = render("reviewer", &ctx());
        assert!(text.contains("git merge-base --is-ancestor <sha> main"));
        assert!(
            text.contains("NOT an ancestor ⇒ the work was never committed"),
            "reviewer should be told the uncommitted case is a REWORK"
        );
        assert!(
            text.contains("IS an ancestor ⇒ the work already landed"),
            "reviewer should be told the already-merged case is an APPROVE"
        );
        assert!(text.contains("Never APPROVE an empty branch you have not disambiguated."));
        // The check is a precondition on *reading* the branch, so it has to
        // land ahead of the verdict menu it gates.
        let check_at = text
            .find("EMPTY branch is not a verdict")
            .expect("empty-branch check");
        let verdicts_at = text
            .find("Produce exactly one recommendation")
            .expect("verdict menu");
        assert!(
            check_at < verdicts_at,
            "the empty-branch check should precede the verdict criteria it gates"
        );
        // Confined to the reviewer arm — a directed rat renders no verdicts.
        assert!(!render("rat", &ctx()).contains("git merge-base --is-ancestor"));
    }

    #[test]
    fn templates_send_rats_to_the_ballot_on_entry() {
        // The fleet promoted zero conventions in its whole life because
        // `suggestion` was never in the read-on-entry list: proposing was
        // taught, endorsing was framed as an optional favour, and the quorum
        // arithmetic was invisible. Both halves have to be stated or a
        // proposal can never reach quorum (TKT-165).
        for role in ["rat", "reviewer"] {
            let text = render(role, &ctx());
            assert!(
                text.contains("rk scan suggestion system"),
                "{role} template should put suggestions in the on-entry read list"
            );
            assert!(
                text.contains("rk endorse <sug-id>"),
                "{role} template should teach endorsing by id"
            );
            assert!(
                text.contains("3 DISTINCT endorsers"),
                "{role} template should make the quorum visible"
            );
            // What replaced the deadline. Ballots are durable since TKT-168, so
            // the urgency is no longer "vote before the clock runs out" — it is
            // "nothing promotes this but you". The template has to say the
            // second thing now that the first is false.
            assert!(
                text.contains("does not expire on a clock"),
                "{role} template should say a ballot no longer decays (TKT-168)"
            );
            assert!(
                text.contains("never promotes on its own"),
                "{role} template should keep the reason to vote now that the \
                 deadline is gone"
            );
            // Regression guard, not decoration: this exact sentence outlived the
            // behaviour it described by nine days and had to be swept out by
            // hand (TKT-186). Re-adding it fails here rather than in the fleet.
            assert!(
                !text.contains("24h voting window"),
                "{role} template still promises a voting window that TKT-168 removed"
            );
        }
    }

    #[test]
    fn git_safety_gives_the_no_reformat_rule_its_mechanics() {
        // "Format only files you changed" is unactionable with `cargo fmt` —
        // scoping is the one thing it cannot do — and a rat that then runs
        // `cargo fmt --check` sees a red workspace it did not break. Ten rats
        // in the drain corpus re-derived both facts; one concluded it should
        // not format at all (TKT-166).
        for role in ["rat", "reviewer"] {
            let text = render(role, &ctx());
            assert!(
                text.contains("`cargo fmt` cannot be scoped"),
                "{role} template should say why cargo fmt is the wrong tool here"
            );
            assert!(
                text.contains("rustfmt <files you changed>"),
                "{role} template should name the formatter that can be scoped"
            );
            assert!(
                text.contains("not a gate you must satisfy and not yours to fix"),
                "{role} template should mark the fmt-dirty baseline as pre-existing"
            );
            // The rule 0002 landed is refined, not replaced.
            assert!(text.contains("NEVER commit a workspace-wide"));
            assert!(text.contains("git checkout -- <untouched files>"));
        }
    }

    #[test]
    fn completion_protocol_puts_the_commit_ahead_of_verification() {
        // A rat that verifies first and commits after leaves its branch
        // byte-identical to main for the length of the suite, and a reviewer
        // chained off it reads the empty diff as a lost delivery (TKT-90,
        // TKT-113). The order is load-bearing, so pin it for both roles along
        // with the proof step that makes `rk done` more than a claim.
        for role in ["rat", "reviewer"] {
            let text = render(role, &ctx());
            let commit_at = text
                .find("Commit BEFORE you verify")
                .expect("commit-first step");
            let verify_at = text
                .find("Verify with the project's own build")
                .expect("verification step");
            assert!(
                commit_at < verify_at,
                "{role} template should tell the rat to commit before verifying"
            );
            assert!(
                text.contains("`rk done` is NOT a\n   commit"),
                "{role} template should teach the branch-carries-the-work proof"
            );
            assert!(text.contains("git status --porcelain"));
            assert!(text.contains("git log <base>..HEAD"));
        }
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
    fn no_conventions_means_no_standing_section() {
        // The section is omitted entirely when the fleet has promoted nothing,
        // so an empty convention set costs the prompt nothing.
        for role in ["rat", "reviewer"] {
            let text = render(role, &ctx());
            assert!(!text.contains("Standing conventions"));
        }
    }

    #[test]
    fn conventions_are_composed_into_a_binding_section() {
        let mut c = ctx();
        c.conventions = vec![
            "Prefer small, reviewable commits.".into(),
            "Never touch protected paths without a gate.".into(),
        ];
        for role in ["rat", "reviewer"] {
            let text = render(role, &c);
            assert_eq!(text.matches("## Standing conventions").count(), 1);
            assert!(text.contains("- Prefer small, reviewable commits."));
            assert!(text.contains("- Never touch protected paths without a gate."));
            // Rides above the coordination fragment so it reads as binding
            // context, not an afterthought.
            let conv_at = text.find("Standing conventions").unwrap();
            let space_at = text.find("Coordination: the tuplespace").unwrap();
            assert!(
                conv_at < space_at,
                "{role}: conventions should precede coordination"
            );
        }
    }

    #[test]
    fn conventions_are_deduped_and_blanks_dropped() {
        let mut c = ctx();
        // Same norm surfaced under both repo and system scope, plus a decayed
        // suggestion that carries no text.
        c.conventions = vec![
            "Prefer small commits.".into(),
            "   ".into(),
            "Prefer small commits.".into(),
        ];
        let text = render("rat", &c);
        assert_eq!(text.matches("- Prefer small commits.").count(), 1);
        // The blank never produces an empty bullet.
        assert!(!text.contains("- \n"));
    }

    #[test]
    fn facts_are_injected_as_bounded_non_binding_context() {
        let mut c = ctx();
        c.facts = (0..12).map(|n| format!("fact-{n}")).collect();
        let text = render("rat", &c);
        assert_eq!(text.matches("- fact-").count(), MAX_INJECTED_FACTS);
        assert!(text.contains("These are observations from the fleet, not binding conventions."));
        assert!(text.contains("- fact-0"));
        assert!(text.contains("- fact-9"));
        assert!(!text.contains("- fact-10"));
        assert!(text.contains("rk fact vote <fact-id> up"));
        assert!(text.contains("rk fact vote <fact-id> down"));
    }

    #[test]
    fn all_blank_conventions_omit_the_section() {
        let mut c = ctx();
        c.conventions = vec!["".into(), "  ".into()];
        assert!(!render("rat", &c).contains("Standing conventions"));
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

# Proposal: PR / MR merge mode for rat-kingdom

**Status:** discovery / research (no code changed)
**Author:** rat-47 (task `discover-pr-mode`)
**Question:** For projects that warrant it, how can we configure RK so a finished
rat's branch is turned into a **pull/merge request** for human or CI review,
instead of being merged directly into the base?

---

## 1. Current state — how merges happen today

Every merge in RK funnels through **one** function, and it is entirely local: it
merges refs inside the on-disk repo and never touches a remote.

### 1.1 The single merge primitive

`Repo::merge_branch` — `crates/rk-git/src/lib.rs:118`.

- Adds a **detached** temp worktree on `target` (`.git/rk-merge-<pid>`), runs
  `git merge --no-ff -m "merge <branch> into <target> [rk]"` there
  (`lib.rs:133`), then CAS-advances the target ref only if it did not move
  concurrently (`advance_target`, `lib.rs:183`).
- A conflict or a moved target is a clean `MergeOutcome { merged: false }`
  (`lib.rs:147`, `lib.rs:161`) — never a hard error — so callers can gate on it.
- **There is no push, fetch, or remote code anywhere in `rk-git`.** (`grep -n
  'push\|fetch\|remote' crates/rk-git/src/lib.rs` returns nothing.) The only
  remote handling in the whole daemon is `crates/rk-daemon/src/sync.rs`, and that
  is for the **tuplespace** state repo (git-notes sync), not for project repos.

### 1.2 The two callers of `merge_branch`

**`Supervisor::dismiss`** — `crates/rk-daemon/src/supervisor.rs:1163`.
Kills the session, removes the worktree (`supervisor.rs:1186`), and — unless
`no_merge` — calls `repo.merge_branch(branch, target_branch)` (`supervisor.rs:1191`),
deletes the branch on success (`1195`), closes the rat's ticket if merged
(`1207-1214`), and emits an `agent_dismissed` event (`1216`). This is the
"merge a rat's branch into **its own base**" path.

**`Supervisor::land`** — `crates/rk-daemon/src/supervisor.rs:1235`.
The explicit `{branch, target}` counterpart. Calls `repo.merge_branch(branch,
target)` (`supervisor.rs:1243`), deletes the source branch unless `keep_branch`,
emits a `branch_landed` event (`1262`). This is what lets an APPROVE verdict put
a reviewed branch straight onto `main`.

### 1.3 How a merge is reached / gated

- **CLI:** `rk dismiss [--no-merge]` → `crates/rk-cli/src/agent_cmds.rs:373` →
  RPC `agent.dismiss` → `crates/rk-daemon/src/server.rs:683` → `Supervisor::dismiss`.
- **Workflow steps** (`crates/rk-workflow/src/lib.rs`):
  - `Step::Dismiss` (`lib.rs:130`, `DismissStep` `lib.rs:254`, `noMerge` field) —
    single active agent.
  - `Step::DismissAll` (`lib.rs:146`, `DismissAllStep` `lib.rs:265`) — fan-out;
    executes in `dismiss_fanout` (`crates/rk-daemon/src/workflow_exec.rs:677`).
  - `Step::Land` (`lib.rs:151`, `LandStep` `lib.rs:451`: `branch`, `target`,
    `keepBranch`) — executes at `crates/rk-daemon/src/workflow_exec.rs:513`.
- **Gating workflows** (`examples/workflows/`):
  - `gated-merge.cue` — human `rk approve`/`rk reject` gate → `dismiss` (merge)
    vs `dismiss noMerge` (hold). The approval-gate primitive is TKT-2/TKT-5.
  - `land-on-approve.cue` — rat implements → cheap reviewer records a verdict →
    **approval gate** → APPROVE `land`s the branch onto `main`; REJECT holds it.
  - `steward.cue` — reactor-fired on every rat completion. Cheap reviewer +
    **policy gate** (protected-path ERE, `steward.cue` step 2) + **run gate**
    (repo's real test suite) + verdict artifact → `APPROVE` lands, `REWORK` files
    a ticket + holds, `STOP` emits a `need` + holds. Both gates fail closed.

**Key observation:** the gates decide *whether* to merge; the merge action
itself (`dismiss`/`land`) is always a **local** `merge_branch`. The `LandStep`
doc comment already flags the gap: *"A hard policy restriction (and merge-queue
serialization) is deferred to the policy engine"* (`lib.rs:448`). A PR mode is
the natural home for exactly that deferred policy.

### 1.4 Repo registry — what RK knows about a repo

`RepoRecord` (`crates/rk-daemon/src/repos.rs:14`) has only `{name, path,
created_at}`. It is a machine-local JSON file (`~/.rat-kingdom/repos.json`),
deliberately **not** replicated through the tuplespace because paths are
machine-local (`repos.rs:1-6`). Registration: `rk repo add <path> [--name]`
(`crates/rk-cli/src/repo_cmds.rs:8`) → RPC `repo.add` → `handle_repo_add`
(`crates/rk-daemon/src/server.rs:821`), `RepoAddParams` (`server.rs:1185`).

There is a **second** per-repo config surface in `config.toml`:
`DrainConfig.repos: HashMap<String, RepoDrainConfig>` (`crates/rk-core/src/config.rs:258`,
`RepoDrainConfig` at `271`) — precedent for a `[<section>.repos.<name>]` keyed
table. And `PolicyConfig` (`config.rs:331`, currently just `require_named_checks`)
is the existing home for fail-closed merge policy.

### 1.5 Branch / worktree model

- Branches are `rat/<agent>/<task>` (`agent_branch`, `crates/rk-git/src/lib.rs:220`).
- Worktrees are forked off a **local** base with `git worktree add -b`
  (`create_worktree`, `lib.rs:72`) — no upstream/tracking branch, never pushed.

**What must be true for a PR to be openable (none of it exists today):**
1. The branch must be **pushed to a remote** (`git push -u origin <branch>`).
2. The repo must have a **remote configured** (`origin`) pointing at a
   GitHub/GitLab host.
3. **Auth** must be present for the push and for the PR API — a `gh`/`glab` CLI
   already authenticated, or a token in the daemon's environment.
4. RK must know the **host/kind** (github vs gitlab) to pick `gh pr create` vs
   `glab mr create` (or the REST API).

---

## 2. Options for a PR / MR mode

All options share the same insight: **the branch is already committed and
mergeable; a PR is just "push + open PR" substituted for "local merge".** The
seam is `Supervisor::dismiss`/`land` → `repo.merge_branch`. We add a sibling
`repo.open_pull_request` and route to it by policy.

### Option A — Per-repo `merge_mode` policy (recommended core)

Add a merge policy to the repo record / config: `direct` (today's behavior) or
`pr`. When a merge is *requested* and the mode is `pr`, RK pushes the branch and
opens a PR/MR instead of calling `merge_branch`, then **holds** the branch
(equivalent to today's `noMerge` outcome) and reports the PR URL.

- **Where the policy lives:** two candidates.
  - **(A1) In `RepoRecord`** (`repos.rs:14`): add `merge_mode`, `remote`,
    `host`. Natural because it is genuinely machine-local (depends on the local
    checkout's `origin` and local `gh` auth). Set via `rk repo add --merge-mode
    pr --host github` or a new `rk repo set-policy`.
  - **(A2) In `config.toml`** under a new `[merge.repos.<name>]` table mirroring
    `DrainConfig.repos` (`config.rs:258`), with a `PolicyConfig` default. Natural
    because it sits next to the other fail-closed policy and is operator-edited.
- **Trade-off:** A1 keeps all repo-scoped facts in one record and is
  discoverable via `rk repo show`; A2 keeps policy in the operator's version of
  truth (`config.toml`) alongside `require_named_checks`. **Recommend A1 for the
  data (remote/host/mode travel with the checkout) with a `config.toml` default
  fallback** — the same layering the daemon already uses everywhere.
- **Pro:** one switch flips a repo from auto-merge to PR without touching any
  workflow; every existing merge path (`dismiss`, `land`, `dismiss_all`,
  steward, land-on-approve) inherits it for free.
- **Con:** "merge requested but held as PR" changes the meaning of the
  `merged: true/false` result; callers/gates that assert `merged: true` (e.g.
  steward's `evaluate {expect: {merged: true}}`) must learn a third outcome
  (`pr_opened`).

### Option B — A `--pr` flag on `dismiss` / a `pr: true` field on `land`

Make it explicit per-invocation instead of per-repo: `rk dismiss --pr`, and a
`LandStep { pr: true }`.

- **Pro:** minimal surface; no repo-policy plumbing; lets one workflow choose PR
  while another auto-merges the same repo.
- **Con:** every workflow and every operator has to *remember* to pass it — the
  opposite of "for projects that warrant it, always." Easy to forget → an
  unreviewed auto-merge slips through. Better as a *complement* to A (an explicit
  override), not the primary mechanism.

### Option C — A new workflow `land` variant / `open-pr` step

Add a distinct step `open_pr { branch, target }` and ship a `pr-on-approve.cue`
workflow (a fork of `land-on-approve.cue` that opens a PR instead of landing).

- **Pro:** zero change to `dismiss` semantics; the PR path is opt-in per
  workflow and reads explicitly in the `.cue`; composes with the existing
  approval/steward gates (gate → `open_pr` instead of gate → `land`).
- **Con:** doesn't cover the plain `rk dismiss` CLI path or the reactor-fired
  steward unless those workflows are also forked; policy lives in N workflow
  files instead of one repo record.

### Option D — Post-merge push mirror (explicitly rejected)

Keep local merge, then `git push origin main`. This is *not* a review gate — it
merges first and asks never. Mentioned only to rule out: it does not answer the
question (review *before* merge). Useful only as an orthogonal "publish merged
main" feature.

---

## 3. Recommended design

**A per-repo `merge_mode` policy (Option A1) that swaps the local merge for
push-and-open-PR at the single `dismiss`/`land` seam, plus a small `open_pr`
step (Option C) for workflows that want it explicitly.** Concretely:

1. **New git capability.** Add to `rk-git` (`crates/rk-git/src/lib.rs`):
   `Repo::push_branch(branch, remote)` and `Repo::open_pull_request(PrRequest {
   branch, target, title, body, remote, host })` that shells out to `gh pr
   create` / `glab mr create` (mirroring how `merge_branch` shells to `git` —
   "the same binary humans use"). Returns a `PrOutcome { url, opened }`, with a
   clean `opened: false` on auth/remote failure (never a panic), symmetric to
   `MergeOutcome`.

2. **Repo policy.** Extend `RepoRecord` (`repos.rs:14`) with
   `merge_mode: MergeMode` (`Direct`|`Pr`, default `Direct` — fully
   backward-compatible), `remote: Option<String>` (default `origin`),
   `host: Option<Host>` (`Github`|`Gitlab`, else inferred from the `origin`
   URL). Surface via `rk repo add --merge-mode pr [--host github]` and `rk repo
   show`. Fall back to a `[merge]` default in `PolicyConfig` (`config.rs:331`)
   when the record is silent.

3. **Route by policy.** In `Supervisor::dismiss` (`supervisor.rs:1189-1200`) and
   `Supervisor::land` (`supervisor.rs:1241-1254`), branch on the resolved
   `merge_mode`: `Direct` → today's `merge_branch`; `Pr` → `push_branch` +
   `open_pull_request`, then **do not delete the branch** and return a result
   with a new shape `{merged: false, pr_opened: true, pr_url, detail}`. Emit a
   new `pull_request_opened` event alongside the existing `agent_dismissed` /
   `branch_landed` events.

4. **Teach the gates the third outcome.** Where steward/land-on-approve today do
   `evaluate {expect: {merged: true}}` after a `land`, PR mode should instead
   assert `pr_opened: true`. Add an `open_pr` workflow step (thin wrapper over
   the new supervisor method) and a `pr-on-approve.cue` example so a workflow can
   choose PR explicitly regardless of repo policy.

5. **Surface in the operator queue.** Route the PR URL into `rk inbox` (TKT-24,
   `crates/rk-daemon/src/inbox.rs`) as an "awaiting review" row so an open PR is
   visible attention, and its branch is never silently forgotten.

**Why this shape:** it reuses the one existing merge seam (no new merge paths to
keep in sync), keeps today's behavior as the default (`Direct`), makes the
review-vs-merge choice a single per-repo fact "for projects that warrant it,"
and layers cleanly onto the approval/steward gates that already decide *whether*
to proceed — PR mode only changes *what "proceed" does*.

---

## 4. Ticket decomposition (proposed — NOT filed)

Small/obvious enough that I would file them; larger design ones I leave for the
operator to confirm scope first.

- **T1 — `rk-git` push + PR primitive.** `push_branch` and
  `open_pull_request` shelling to `gh`/`glab`, with a `PrOutcome` and
  clean-failure semantics mirroring `MergeOutcome`. Host inference from the
  `origin` URL. Unit-testable command construction; PR creation itself gated
  behind an integration flag (needs a real remote). *(depends on nothing)*
- **T2 — Repo merge policy.** Add `merge_mode`/`remote`/`host` to `RepoRecord`
  (+ migration: absent field ⇒ `Direct`); `rk repo add --merge-mode` and `rk
  repo show` rendering; `[merge]` default in `PolicyConfig`. *(depends on nothing)*
- **T3 — Route `dismiss`/`land` by policy.** Branch on `merge_mode` in
  `Supervisor::dismiss`/`land`; new `{pr_opened, pr_url}` result shape;
  `pull_request_opened` event; do-not-delete-branch in PR mode. *(depends on T1, T2)*
- **T4 — `open_pr` workflow step + `pr-on-approve.cue`.** New `Step::OpenPr` in
  `rk-workflow` + exec in `workflow_exec.rs`; example workflow forking
  `land-on-approve.cue`. *(depends on T1, T3)*
- **T5 — Gate outcome + inbox.** Make steward / land-on-approve assert
  `pr_opened` when the repo is PR-mode; add an "awaiting-review" inbox source for
  open PRs. *(depends on T3, T4)*
- **T6 — Docs.** Operator guide: auth prerequisites (`gh auth login` /
  `glab auth login` for the daemon's user), configuring a repo for PR mode, and
  the end-to-end flow. *(depends on T2, T3)*

Natural sequencing: **T1 + T2 in parallel → T3 → T4 → T5 → T6.**

---

## 5. Open questions for the operator

1. **Auth model.** Push + PR creation need credentials in the **daemon's**
   environment, not a rat's worktree. Do we rely on a pre-authenticated `gh`/`glab`
   CLI for the daemon's user (simplest, matches "same binary humans use"), or a
   token env var + REST API (more portable, more secrets handling)?
2. **Which host(s) first.** GitHub (`gh`) only, or GitHub + GitLab (`glab`) from
   the start? Host can be inferred from the `origin` URL, but the CLI/API differ.
3. **CI gating relationship.** Once a PR is open, is RK "done" (hand off entirely
   to the PR's own CI + human reviewer), or should the reactor watch PR
   status/checks and re-fire a rat on failing CI (a much larger feature — a PR
   status trigger)? Recommend v1 = open-and-hand-off; watch-CI is a follow-up.
4. **Interaction with the steward's own review.** In PR mode, does the steward's
   cheap-reviewer + run-gate still run *before* opening the PR (belt and
   suspenders — a green local suite in the PR body), or do we skip it and let the
   PR's CI be the sole gate? Recommend keeping the run-gate: its output makes a
   useful PR description and fails closed before we bother a human.
5. **Branch retention & cleanup.** In direct mode RK deletes the merged branch.
   In PR mode the branch must survive (the PR owns it). Who deletes it after the
   PR merges — the host's auto-delete-on-merge, or an RK reactor on a
   `pull_request_merged` webhook (out of scope for v1)?
6. **Remote push safety.** Pushing `rat/<agent>/<task>` branches to a shared
   remote is outward-facing. Any naming/namespace constraints, protected-branch
   rules, or a dry-run/confirm step the operator wants before the first real push?

---

*Grounding note:* every file:line reference above was read directly from the
tree at this branch's base. The central fact — that all merging is local and
`rk-git` has no remote/push code — was verified by grep returning empty for
`push|fetch|remote` in `crates/rk-git/src/lib.rs`.

# TKT-171 — a `land` that does not merge must not be able to disappear

**Status**: fixed. Detection in `crates/rk-daemon/src/inbox.rs` +
`server.rs`; reporting in `crates/rk-git/src/lib.rs`. Regression tests
`crates/rk-daemon/tests/dropped_land.rs`, `inbox.rs` unit tests,
`rk-git` `merge_conflict_reports_not_merged` /
`failure_reason_prefers_stderr_and_caps_a_noisy_conflict`.

## What was asked

TKT-147 was implemented, reviewed, given an APPROVE artifact — and never merged.
It sat off main for two days until it was re-landed by cherry-pick, costing a
rebase over two intervening tickets plus re-verification. The ticket asked:
*does the steward's land/merge step fail closed and report, or can an approve be
recorded while the merge silently no-ops?*

## What actually happened

The steward instance is still on disk. `wf-dn6das9gb9`:

```json
{ "workflow": "steward", "status": "completed",
  "current_step": 9, "total_steps": 9, "error": null,
  "params": { "taskId": "TKT-147", "branch": "rat/nezumi-2/tkt-147" } }
```

with a context holding `vars: {"verdict": "APPROVE"}` and:

```json
"previous_result": {
  "branch": "rat/dusty-2/steward-review-tkt-147", "merged": false,
  "pr_opened": false, "branch_deleted": false,
  "detail": "merge conflict or failure: git merge --no-ff -m merge … failed: " }
```

So: **the land ran, reported `merged: false`, and the instance completed
cleanly.** No failed agent, no failed instance, no inbox row. Nothing anywhere
said the approved work had not landed.

That is not a bug in `land`. `land` reported its outcome accurately —
`{merged: false}` is a deliberate clean result rather than an error, so a
workflow can gate on it and retry. The gate is the caller's job, and the shipped
`steward.cue` has carried one since TKT-44:

```cue
{type: "land", branch: "{{ctx.activeBranch}}", target: _input.target},
{type: "evaluate", expect: {merged: true}, anyOf: [{pr_opened: true}]},
```

The instance that dropped TKT-147 ran **9 top-level steps**. The definition
carrying that gate had 11 by then (TKT-55 added the diff-scope gate on
2026-07-23); 9 is the pre-TKT-55 shape. The deployed copy at
`~/.rat-kingdom/workflows/steward.cue` was stale — it is a hand-copied file, one
per castle and one per repo variant (`steward-grmpl.cue`), and nothing ties its
version to the repo's.

**So the answer to the ticket's question is: both.** The land step fails closed
and reports honestly. Whether anyone *acts* on that report depended on a
`.cue` file being current — and the enforcement of a safety invariant should not
be a property of a file that can go stale, be forked per repo, or be hand-edited.

## The class is bigger than recorded

Every `land` emits a durable `Event/branch_landed` carrying its own outcome. The
evidence was in the tuplespace the entire time; nothing read it. Over the live
store — **108 lands, 5 dropped** (`merged: false` and `pr_opened: false`):

| branch | ticket | recovered |
| --- | --- | --- |
| `rat/filch/steward-review-tkt-18` | TKT-18 | by hand |
| `rat/rat-11/steward-review-tkt-28` | TKT-28 | by hand |
| `rat/rat-9/steward-review-tkt-30` | TKT-30 | by hand |
| `rat/rat-44/steward-review-tkt-46` | TKT-46 | by hand |
| `rat/dusty-2/steward-review-tkt-147` | TKT-147 | by cherry-pick, 2 days later |

TKT-171 described this as the second recorded instance. It is the fifth
occurrence, ~4.6% of all lands. TKT-28 and TKT-30 were never recorded as
instances of the class at all — every one was noticed by a human eventually, and
none by the system.

## The fix

Two halves, matching the two halves of the ticket's question.

### Detection — assert the invariant at read time, for every workflow

`rk inbox` gains an **`unlanded-branch`** source over `branch_landed` events
where the land neither merged nor opened a PR. It sits at the same urgency as a
failure: the work is finished and reviewed but absent from the target, and the
cost of leaving it there grows with every commit the target advances.

This is the ticket's suggested `git merge-base --is-ancestor` guard, moved from
the workflow definition into the engine. Three properties matter:

- **Definition-independent.** A workflow that forgot the post-land `evaluate` —
  the exact failure above — is covered anyway.
- **Self-clearing.** A row is suppressed when local git says the branch has
  reached its target or is gone (`Repo::branch_merged_or_gone`, already the
  awaiting-review clear from TKT-69). A hand-merge, a re-land, a cherry-pick
  followed by deleting the branch: all retire the row with nothing having to
  write a "resolved" record. Newest-event-wins per branch does the same for a
  successful retry.
- **No new storage.** Pure read-side aggregation over events that already exist,
  like the rest of `inbox::build`.

`cleared_pull_requests` became `cleared_branches`, since both branch-shaped
sources ask git the same question about the same `{branch, target}` payload
shape. That check is a subprocess per branch, on the read path behind `rk top`,
and `branch_landed` accumulates one event per land the fleet has ever performed
and never shrinks (108 already). So `handle_inbox` reduces the events to the
actual drop candidates (`inbox::dropped_lands`) *before* the git check, and the
check asks once per distinct branch rather than once per event: 5 git queries
today rather than ~108, and self-limiting, since resolving a drop removes it.

### Reporting — carry git's actual reason

The recorded detail ended `… failed: ` with nothing after the colon. `git_in`
built its error from **stderr only**, and a failing `git merge` writes its whole
diagnostic (`CONFLICT (content): Merge conflict in …`) to **stdout**, leaving
stderr empty. Every merge conflict in every recorded `branch_landed` event
therefore named neither the conflicting files nor even that it was a conflict.

`failure_reason` now prefers stderr and falls back to stdout, flattens to one
line, and caps at three lines (a wide conflict prints one line per path) with an
explicit `(+N more lines)` rather than a silent truncation.

## What this does not do

It does not make an unlanded branch impossible — a conflict still needs a human.
It makes one **impossible to lose**: the row appears the moment the land drops
the branch and stays until the branch actually reaches its target.

Adding the missing `evaporate`-style guard to each workflow definition is still
worth doing, and the shipped `steward.cue` already has it. The point is that the
invariant no longer *depends* on that.

## Related

Same shape as TKT-147 itself — a gate reporting success over work that never
happened — and as TKT-67/69/70, which built the awaiting-review row for the
adjacent case of a pushed branch nobody merged. See also
`docs/pr-merge-mode.md`.

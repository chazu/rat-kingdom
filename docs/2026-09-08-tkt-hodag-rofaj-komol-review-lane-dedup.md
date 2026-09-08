# TKT-hodag-rofaj-komol — dedup the Review lane without breaking shadow/retry

**Status**: fixed. `crates/rk-daemon/src/agents.rs` (`Registry::try_reserve_review_task` /
`release_review_task` / `release_task_for_lane`), `crates/rk-daemon/src/supervisor.rs`
(`Supervisor::spawn`). Regression tests in `supervisor::respawn_tests`:
`review_lane_dedup_refuses_duplicate_manual_reviewer_dispatch`,
`review_lane_dedup_refuses_duplicate_dispatch_of_the_identical_workflow_instance`,
`review_lane_dedup_admits_shadow_and_retry_reviewers_despite_a_live_primary`.

## What was asked

TKT-pumod-hubir-robik closed a TOCTOU window where two `agent.spawn` calls for
the identical `(repo, task)` could both admit, but scoped the fix to
`Lane::Implementation` — turning it on for `role == "reviewer"` broke 5
landing.rs tests, because the Review lane has two legitimate patterns where a
second live generation on the identical task is intentional: shadow review's
secondary reviewer, and a review-death replacement dispatched before the dead
primary's row is guaranteed to have settled out of `is_live()`. This ticket
was filed to close that gap without reopening those breakages: an accidental
duplicate reviewer dispatch (the same tier-router race, landing on
`role == "reviewer"` instead) still admits twice today.

## The fix

The landing pipeline already mints a distinct, deterministic `workflow_instance`
id per *logical reviewer generation* for a given task
(`landing::review_instance_id` for the primary, `review_retry_instance_id` for
each retry, that id plus `-shadow` for the shadow — see `ReviewContext::attempt`
and `SHADOW_INSTANCE_SUFFIX`). An accidental duplicate, by contrast, always
repeats the identical `workflow_instance` — either the same `Some(id)` (a
stale retry of a workflow step call), or `None` for both (a manual or
tier-routed reviewer dispatch outside any workflow at all, the same shape as
the original Glossolalia incident).

So the Review lane's dedup key is `(repo, task, workflow_instance)` instead of
the Implementation lane's plain `(repo, task)`:

- `Registry::live_review_task_owner` / `try_reserve_review_task` /
  `release_review_task` mirror the existing `live_task_owner` /
  `try_reserve_task` / `release_task` triad, but additionally require the
  candidate row's `workflow_instance` to match exactly.
- `Supervisor::spawn` now reserves against `try_reserve_review_task` for
  `Lane::Review` (previously: no reservation at all), passing
  `params.workflow_instance.as_deref()`.
- The three release call sites route through a new
  `Registry::release_task_for_lane` dispatcher instead of unconditionally
  calling the Implementation-only `release_task`.

This admits shadow and retry reviewers unchanged (their `workflow_instance`
always differs from the primary's and from each other's), while refusing a
second spawn that repeats the identical `workflow_instance` — including two
`None`s.

## What this does not cover

Two independently-triggered reviews of the *same task* with genuinely
different `workflow_instance` ids (e.g. a stale review superseded by a fresh
one after a new push, which changes `head_sha` and therefore
`review_instance_id`) are not deduped by this check, and are not meant to be —
that is the landing pipeline's own supersession concern, not a duplicate
dispatch.

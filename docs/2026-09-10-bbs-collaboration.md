# BBS collaboration

## Problem

Workers scan a large artifact history, requests rarely acquire explicit resolution
links, and the single-task contract leaves useful peer assistance ambiguous.
Implement the three approved improvements in order: discovery, answer acceptance,
then bounded assistance. Existing tickets, claims, needs, and artifacts remain the
shared state; the BBS does not own dispatch, delivery, or permissions.

## Proposed solution and implementation

1. Add `rk bbs brief`, with repository/task defaults from the worker environment,
   optional areas, and a durable SQLite sequence checkpoint. Inject the same
   briefing on spawn and resume. Rank current claims, questions and artifacts by
   task/dependency identity, area and task-title relevance, with a separate cap
   per category. Include record IDs, selection reasons and bounded summaries.
   `--since` highlights writes and reinforcements since a prior checkpoint; it is
   an advisory current-state summary, not a complete event stream or read receipt.
   Show omitted counts and preserve `rk bbs show`/raw scans for deeper inspection.
2. Add durable help requests, answers and requester acceptance using Need and
   Artifact tuples. Replies reference their request, and acceptance references
   both the answer and an optional resulting contribution. Only the requester
   (or operator) can accept; posting an answer alone never resolves a request.
   Preserve the complete thread and make retries idempotent. Keep the legacy
   artifact resolution path working for ordinary obstacles and needs.
3. Teach workers to refresh the briefing at checkpoints and explicitly allow
   short peer answers, evidence sharing and interface agreement in support of
   their assignment. Claims remain advisory. Task ownership, role restrictions,
   delivery gates and authorization for substantial additional work still apply.

Peer content is evidence, never an authenticated steer or a permission grant.
The daemon validates authorship and references; CLI payload fields cannot grant
acceptance authority. No hosted service, embedding index, new coordinator, or
second persistent database is needed.

## Validation

Exercise the real CLI against isolated daemons: relevant dependency artifacts
outrank unrelated history; briefings are bounded and retain source IDs; checkpoints
detect newer writes independently of tuple-ID order; questions survive restarts;
answers remain pending until authorized acceptance; forged/cross-thread acceptance
is refused; accepted answers retain provenance and leave the open-question view.
Verify startup/resume injection and the bounded-assistance prompt contract. Run
the repository's complete source gate after the three slices.

The full workspace gate passed: 1,864 tests, formatting, workspace build, and
Clippy with warnings denied. After final refinements, focused CLI authorization,
BBS lifecycle, prompt-contract, and actual startup/resume injection tests also
passed. These checks use isolated daemons and a fake worker harness; they do not
establish a measured improvement in live agent collaboration. The changes have
not been installed in the running daemon.

# Usefulness and reliability improvements

Implementation of the seven ranked recommendations from the 2026-09-05
assessment. Repository-owned CUE remains policy authority; existing persisted
evidence, exact generation identity, and unrelated worktree state are preserved.

## Acceptance checklist

- [x] **Current work:** resolved gate/rework incidents disappear from current
  stalled work without deleting history or hiding a newer recurrence. Operator
  dispositions are structured data, not parsed command prose. Delivery advice
  uses the gated RK path rather than manual Git merges.
- [x] **Observation:** selected tickets include bounded correction lineage and
  consistently attribute liveness, usage and staleness; unrelated work remains
  excluded. Sampling is incremental, restartable and bounded by deadlines, with
  explicit evidence for timeouts and gaps. Full reports remain replayable from
  the append-only evidence.
- [x] **Revert:** durable exact-operation intent and outcome survive failure or
  restart between Git and registry/ticket writes. Retrying settles one revert,
  repairs delivery state and preserves completion evidence. Required writes
  cannot silently fail while the operation reports complete.
- [x] **Managed verification:** one module owns identity, admission, process
  execution/cancellation/cleanup and outcome evidence for landing, workflow and
  `verify.run` callers. General admission works for non-Cargo checks; shared
  Cargo directory locking remains a separate constraint.
- [x] **Typed delivery:** internal target-advance and delivery outcomes remain
  typed until external serialization; missing JSON fields cannot imply success.
- [x] **First repository:** one tested walkthrough covers readiness inspection,
  explicit policy decisions and activation, bounded work, gated delivery and
  cleanup. README leads with this journey and links advanced reference material.
- [x] **Presentation:** audit legacy Python triage/rendering consumers, migrate
  supported consumers to native typed snapshots, prove necessary presentation
  parity and retire duplicate implementations. Preserve distinct diagnostics
  and historical design evidence; provide a current architecture/milestone map.

## Verification

Each correctness change receives a regression at its real caller seam. Module
refactors preserve existing integration coverage. The final acceptance gate is
`mise run verify-full`, plus relevant non-Rust skill/CLI checks. The live daemon
and historical pilot are not used as a mutation target by repository tests.
Runtime activation and a fresh foreign pilot are separate from source validation.

Final source acceptance: `mise run verify-full` passed with 1,805 tests, zero
failures and zero ignored tests. Formatting, workspace build, documentation tests
and workspace/all-target Clippy with `-D warnings` passed. The complete local log
is `/tmp/rk-seven-improvements-acceptance.log`. All seven source requirements
above are complete; this validation did not install or activate a release.

## Implementation evidence

### Typed delivery

`rk-daemon/delivery.rs` owns explicit landed, stale and blocked outcomes.
Successful delivery requires a nonempty exact commit; local, pushed and pending
push publication remain distinct. The supervisor and every single/batch/recovery
landing finalization path use these types. Only RPC replies and event payloads
serialize JSON. Missing JSON flags no longer imply success, and a missing commit
can no longer silently skip delivery recording.

Validation: `cargo check -p rk-daemon --tests` and 152 landing-related daemon
tests passed, including target movement, dirty-worktree refusal, exact non-main
targets, restart recovery and bounded rework chains. Contract tests cover missing
commit rejection and pending-push wire semantics. Full workspace acceptance
remains the final gate after all seven items.

### Native presentation and removal audit

`rk factory render` reads native snapshot/replay files and renders saved Markdown
without contacting or starting a daemon. It preserves cursors, replay boundary
and truncation, resync, degraded source visibility, approval/digest display,
limits, HTML/table escaping and explicit SAVED / NOT CONNECTED provenance.
It refuses obsolete flat helper schemas and input-file overwrite. JSON output
retains the original native responses with saved provenance.

The consumer audit found only bundled-skill installation and documented commands
for the two Python implementations; no daemon, scheduler, MCP or deployment
consumer depended on them. Migrated SKILL/REFERENCE, README and active factory
documentation. Removed the two implementations, their three test files, thirteen
private fixtures and unused template (19 tracked files). Historical plans remain.
The native installer still requires explicit `--force` to replace a different
or customized global package and removes old files during that explicit upgrade.
No global installed skill was changed by this implementation.

Validation: native factory unit/CLI checks passed; a dedicated 14-test acceptance
run covers saved rendering, live dashboard behavior, analytics and skill install,
including the retired-package upgrade. Active documentation links resolve and
no runtime/documented legacy helper command remains. The
[architecture and acceptance map](architecture.md) preserves distinct diagnostics
and separates source implementation from foreign-pilot qualification.

### Current work

Inbox producers now emit explicit actionable, decision-required or stalled
dispositions. The old `action` field is derived display compatibility; current
work consumes the structured disposition and never splits command prose.
Unknown/malformed dispositions stay visible as stalled. Dropped-branch advice
uses gated `rk land` with the recorded target and task, quotes scope arguments,
and asks for inspection when the target is unknown. Landing queue diagnostics
use the valid `rk --json daemon status` command.

Validation: the command-prose regression failed before the fix and passed after;
three current-work checks and 33 inbox-related tests passed. Git-backed current
work tests now prove exact incident resolution from durable delivery and target
ancestry, including a replacement source branch, retained historical Needs and
a visible later recurrence. Legacy Needs bind only to an unambiguous structured
landing marker; malformed, truncated or ambiguous evidence remains visible.

### Observation

The collector follows bounded daemon-authored landing correction lineage and
attributes correction usage and liveness to selected roots. Root throughput and
correction deliveries remain separate. It uses a single-writer incremental log,
validated replaceable checkpoint, restartable cadence and RPC/sample deadlines.
Interrupted bytes are retained as explicit failure evidence. Report replay needs
no daemon or checkpoint. Twenty-one focused observer tests pass, including real
socket deadlines, lineage exclusions, bounded recovery and throughput attribution.

### Durable revert

`revert.rs` persists exact generation/merge/target intent, then the parked Git
candidate, before target advancement. Recovery checks that exact candidate's
ancestry. Required registry, ticket and completion evidence writes propagate
failure; `--operation` resumes the original generation even after name reuse.
Ticket receipt and delivery clearing use an atomic revision replacement, and
registry persistence failure restores its in-memory pointer. Retries cannot
clear another delivery, reopen newer work or emit duplicate completion facts.

Validation: all five revert integration tests pass, including six deterministic
crash/restart boundaries and injected failures in registry, ticket, fact and
completion writes. Generation and ticket fencing tests pass. All 72 storage
tests pass, including rollback on failed replacement. CLI, daemon, Git and space
all-target Clippy passes with warnings denied. These are disposable local test
repositories; no live delivery was reverted.

### Managed verification

`managed_verification.rs` now owns execution requests, repository identity,
admission and Cargo-directory locks, cancellation registrations, child process
groups and restart markers, proof reuse and bounded outcome evidence. Workflow
routing and landing policy compose this module. Every managed command observes
configured admission, including non-Cargo checks; shared Cargo serialization
remains a separate constraint. Dropping a verification request also releases its
registration, process tree and permit.

Validation: the non-Cargo cross-caller regression failed before the change and
passes after. Sixty-six workflow checks, 15 verification-focused checks and the
managed cancellation/saturation integration suites pass, including real nested
process cancellation, unrelated-process survival, exact proof reuse, FIFO and
cross-repository independence. The WIP-4 saturation test now uses non-Cargo
commands, while a separate shared-Cargo test still proves single execution.

Clippy with `-D warnings` passed for all CLI and daemon targets after these
changes. Source checks did not update the installed CLI, global skill or running
daemon. The last read-only live observation showed build `0.1.0+097572789a44`.

### First repository

The README now leads with a short operating journey and links the preserved
advanced material in `docs/operator-reference.md`. `docs/first-repository.md`
uses committed CUE examples with real README-only gates, explicit delivery and
cleanup choices, and exact proposal approval/activation.

Validation: the actual CLI journey passes against a disposable daemon and local
Git remote. Only the coding provider is fake. It proves read-only inspection,
refused unapproved apply, isolated apply, exact activation, a bounded ticket,
all three gates on the delivered SHA, clean checkout/source cleanup and retained
onboarding evidence. A second test proves the actual example commands accept
20 changed lines, reject 21, and reject protected or out-of-scope files; it
failed before adding the configured line-budget check and passes afterward.
The journey exposed and fixed shell-builtin readiness and
registered-name versus directory-basename inconsistencies. Landing and review
commands now resolve registered names as spawning already did.

The full workspace run exposed a remaining workflow-read consumer of the old
basename scope. Workflow reads/events now share the agents' registered scope,
and registry lookup resolves symlink spellings of the canonical checkout.
The reviewer/rework regression reproduced the scope timeout before the fix;
both its repeated-review and STOP paths now pass in seconds.
The analytics fixture now derives its fact scope from the producing agent,
matching that registered identity, and all new CLI probes explicitly select
the operator caller rather than inheriting identity from the environment.

The final recovery audit also added a regression proving startup candidate GC
preserves unfinished revert operations, even before `git gc --prune=now`.
Unreadable queue/revert evidence cannot authorize candidate reclamation.

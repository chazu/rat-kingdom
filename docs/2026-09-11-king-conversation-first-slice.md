# King conversation: first slice

The King is the human operator's primary conversation. Routine fleet changes
previously generated wakes from a broad snapshot digest, so a busy fleet could
repeatedly take the next conversation turn. The old review/landing component
also retained a name that suggested a standing operations agent.

## Resulting behavior

The daemon owns delegated dispatch without a King session. A positive
`[drain].max_wip` enables background operations within the existing `repo` or
`repos` scope. With `enabled = false`, only current `ready-for-agent` tickets
are eligible. Setting `enabled = true` retains whole-backlog drain. Setting
`max_wip = 0` pauses this background loop. Default configuration remains inert.
The existing admission, budget, machine, repository policy and tier paths are
shared. Delegated claims recheck the label, ticket revision, freeze and delivered
dependencies under the ticket mutation lock. Worker RPCs cannot grant that label.

The same background loop runs proven delivery repairs and explicitly allowlisted
conflict correction through `attention.decide`, preserving its journal, lease,
rate limit and bounded conflict chain. Held conflicts also recheck current parent
and correction tickets, then atomically claim the correction. Closed or delivered
work cannot be restarted by an old hold. Failed execution is handed to the King;
the timer does not repeatedly execute a recorded failure. Stale ticket ownership
remains a decision: reopening finished or salvageable work could otherwise
create an unbounded redispatch cycle. Existing lifecycle sweeps remain owners
of their prescribed recovery actions.

King wakes contain explicit decisions: current reconciliation exceptions outside
background authority, failed background repairs, deferred human gates, workflow
approvals, current landing incidents, exhausted transport recovery and budget
breaches. General inbox history, ready counts, live-agent timestamps and peer
BBS questions remain available as context and do not generate wakes.

Notification receipts are durable per incident and semantic revision. Settling a
wake acknowledges delivery, not resolution of its source. Clearing another item
does not re-notify an acknowledged incident. A claimed batch is immutable; new
arrivals remain pending for the next batch. Unclaimed delivery remains retryable.

Herdr must report the registered generation quiescent **and unfocused** before
ordinary wake delivery. Focus is a conservative first-slice signal, not a full
human-presence model. There is no urgent bypass in this slice. Automatic
compaction/replacement is disabled unless
`[king].automatic_context_lifecycle = true`; explicit restart remains available.

## Naming and compatibility

The review workflow is `candidate-review`. Gates use `landing-protected-paths`
and `landing-diff-scope`; escalation needs use identity `landing` and notification
class `landing-escalation`. The native completion trigger is
`landing-on-completion`. Source, examples, tests, docs and file names use these
terms. Old persisted need identities, check names, notification filters and
installed review definitions remain readable through a small compatibility
module. New producers emit canonical names. Durable history is not rewritten.

`scripts/migrate-landing-names.py` previews installed definition/config renames;
`--apply` performs that reviewed transformation with backups and collision checks.
It preserves local customization. Reload the daemon after applying it.

## Validation and trial

Regression coverage exercises quiet routine activity, focused terminal delivery,
individual receipt persistence across restart, arrivals during a claimed batch,
authority/dependency revocation at claim, proven delivery repair without a King,
and WIP-bounded delegated dispatch while unapproved/blocked tickets stay open.
The review and landing suite checks canonical names and legacy read compatibility.

For the live trial, keep whole-backlog drain disabled, set a bounded positive cap
and repository scope, and label the selected tickets `ready-for-agent`. Talk with
the King while those tickets run. Expect normal completions and peer help to stay
quiet, and a real unresolved decision to appear once after leaving the King pane.
Inspect `rk work`, `rk inbox`, `rk king status` and the decision journal for evidence.

A separate reasoning agent for operational exceptions, an explicit conversation
engagement state, urgent interruption policy and scheduled digests are deferred
until the ticket trial demonstrates a concrete need.

Validation on 2026-09-11: workspace build and 1,893 tests passed. After the final
receipt/transport and legacy-definition checks, the focused King, compatibility
and current-need regressions passed; workspace Clippy with warnings denied and
format checking passed. Migration was exercised with customized definitions,
backups, repeated application and a conflicting destination. The human ticket
trial remains the next acceptance step.

Activation exposed a stale conflict hold for an already delivered ticket. The
duplicate correction agent was dismissed without landing, and the daemon was
stopped while the current-ticket guard was added. Regression coverage now checks
closed and delivered parent/correction tickets; existing correction dispatch,
lease fencing and replay tests retain their normal open-ticket path.
The guard passed 22 focused conflict tests and 26 lease/attention integration
tests, followed by workspace Clippy and format checking.

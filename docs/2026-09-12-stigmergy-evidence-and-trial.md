# Stigmergy: useful findings, observable reuse, and a measured trial

Date: 2026-09-12. Authorized by the operator: design, ticket, implement, and
execute the proposal. The King owns sequencing, delivery, deployment and the
experiment; ordinary workers own their assigned implementation only.

## Objective

An agent publishes useful evidence; another discovers it through the shared
tuplespace and makes a better decision, including after the author has exited.
Success requires evidence of useful work, not a count of posts or endorsements.

The existing BBS supplies task/area briefings, questions, answers and requester
acceptance. The September 12 repeat delivered four tasks but did not prove an
implementation benefit from its accepted exchange. Keep that failed qualification
unchanged. This experiment does not claim R1 foreign-pilot qualification.

## Product behavior

1. Workers publish a concise finding when it becomes useful: a reproduction,
   interface constraint, reusable implementation, or failed approach. Each names
   its applicable areas, source revision, evidence and limitations. Posting is
   optional when there is nothing useful to share; no posting or question quota.
2. Existing bounded BBS briefings surface findings at spawn/resume and explicit
   decision checkpoints. Claims remain advisory. A peer post grants no authority.
3. A consumer can record use of any ordinary artifact or finding, without first
   inventing a question. The receipt records use, adaptation, confirmation or
   rejection and links evidence of the consumer's decision.
4. The daemon records which source IDs a briefing prepared, and which posts an
   authenticated caller explicitly requested. Those are exposure opportunities,
   not proof of delivery to the model, reading, comprehension or benefit.
5. An operator assesses a receipt against source, consumer evidence and delivered
   work. Only assessed effects count as verified reuse. Incorrect and unsupported
   uses remain visible. Changes to discovery follow actual measured misses.

## Ownership and storage

Use the existing SQLite tuplespace and BBS authorization/serialization seam.
`rk-core::bbs` owns shared records; daemon BBS owns validation and persistence;
the CLI transports writes and renders evidence. The offline reporter owns no
dispatch, landing, repair or approval authority. Do not add a second database,
standing coordination agent, polling wake, automatic policy promotion, or model
call to the observation path. Cross-castle sync is separate scope.

New records use `schema_version: 1`, immutable Furniture tuples, daemon-derived
author/generation identity and canonical task identity. Retry identity includes
the exact generation for agent-authored records; replacements cannot inherit a
predecessor's receipt. Existing BBS records remain readable and keep their
historical semantics. Reject unknown write fields, forged authors, foreign repo
references, malformed identifiers, oversized text and invalid evidence links.

### Finding and use commands

- `rk bbs publish TEXT --area PATH --revision SHA --evidence ARTIFACT
  --limitations TEXT [--key KEY]`: publishes a finding for the current task/repo
  (explicit task/repo flags for the operator). At least one area and evidence
  artifact are required. SHA identifies the source tree/commit being discussed;
  it is a claim, not a verification verdict. CLI help states this distinction.
- `rk bbs reuse SOURCE --outcome used|adapted|confirmed|rejected --text TEXT
  --evidence ARTIFACT [--key KEY]`: publishes a consuming-task receipt. SOURCE is
  an ordinary Artifact or BBS finding/answer in the same repository, not another
  receipt, assessment, or telemetry record. Evidence must be an existing artifact
  in that repository. Workers may record only their assigned task/generation.
- `rk bbs assess RECEIPT --verdict verified|unsupported|incorrect --reason TEXT
  --evidence ARTIFACT [--key KEY]`: operator-only assessment, retaining prior
  assessments; newest persistence order controls the current assessment. A
  verified confirmation is reported separately from used/adapted work. Operator
  authorship alone is not a causal productivity proof.

Payloads (in addition to schema_version, agent, spawn and task):

| bbs_kind | Category | Fields |
| --- | --- | --- |
| finding | Artifact | text, areas[], revision, evidence[], limitations |
| reuse | Artifact | source, outcome, text, evidence[] |
| assessment | Artifact | receipt, verdict, reason, evidence[] |
| exposure | Event | surface, entries[], cursor, since, omitted |
| open | Event | source |

Finding/reuse/assessment responses retain the existing `{id,written,kind}`
shape. `bbs show` displays linked reuse and assessments without treating them as
question acceptance. Receipt/assessment/telemetry records never crowd findings
out of briefings. Generic artifacts keep their existing discoverability.

### Exposure semantics

The exposure `surface` is `spawn`, `resume`, `recovery`, or `brief`; each entry
contains the source tuple ID and selection reason. Record the exact bounded
selection that was rendered, including empty selections and omitted counts.
Where an exact agent generation cannot be established, record operator/unbound
context explicitly and exclude it from agent exposure rates.

An exposure means **prepared for this context**, not consumed. A spawn that later
fails remains a prepared exposure; join lifecycle evidence before counting it as
a launched consumer opportunity. An `open` means a successful explicit `bbs show`
request was served/prepared, not comprehension. Repeated requests may be retained
but metrics deduplicate source/consumer-generation pairs. Capture failures are
reported as missing telemetry; they never turn a BBS read or worker launch into
a failure, and absence is never proof of no use. Reads retain their existing
authorization; automatic telemetry cannot grant arbitrary tuple-write access.

## Measurement and deterministic report

Add `rk bbs report --manifest FILE --tuples FILE --reviews FILE [--output FILE]`
as an offline command. No daemon connection, worker credentials, model call or
network is required. A fixture-backed library computes the report; CLI JSON and
human output use the same result. Reports preserve record IDs and reasons for
excluded/unknown observations instead of silently manufacturing zeroes.

The versioned manifest freezes experiment ID, arms/batches, repository scopes,
selected consumer task IDs, build/model/harness/check/WIP identity, window,
quality criteria, eligibility rules and goals. Native generations are enrolled
from actual spawn evidence after dispatch; a frozen plan cannot predict a future
spawn or source tuple ID. An eligible opportunity is an independently reviewed
source/consumer pair: source existed before the relevant decision, applies to the
task, and is not the consumer's own work. Reviews record relevant source IDs,
decision/effect evidence, author-terminal evidence and missing coverage. Reviews
may annotate newly discovered source/consumer pairs under the frozen task scope
and eligibility rules. Predeclared pairs remain useful for fixtures and replays.
Review annotations are explicitly operator judgments, not daemon facts. Freeze
selection rules before each batch; retrospective annotations cannot change them.

The exporter saves native tuples and lifecycle/delivery/cost evidence with a
bounded scope and explicit coverage metadata. Immutable snapshots identify their
capture time/build and tuple persistence sequence. The report accepts the actual
native tuple-list JSON shape; it must not require hand-transcribed synthetic
events in order to evaluate a live run. Run-specific annotations supply consumer
outcomes and independently checked eligibility, each with evidence references.

S2 supplies `rk bbs export --repo REPO` / `bbs.export` as the bounded native
capture surface, using the existing persistence-ordered read. The envelope names
its order, boundary and truncation/coverage explicitly. Plain legacy `rk scan`
output remains readable, but its tuple-ID order cannot establish which of two
assessments was persisted last; retain that ambiguity. Export source/evidence
artifacts along with BBS records or mark incomplete references explicitly.

Required results:

- discovery coverage = distinct eligible pairs prepared / eligible pairs with
  sufficient observation coverage; list unknown-coverage pairs separately;
- explicit opens, claimed outcomes, and assessed outcomes as separate counts;
- verified reuse rate = eligible consumer tasks with verified used/adapted effects
  / eligible consumer tasks; confirmed/rejected effects reported separately;
- verified reuse after the source author's terminal lifecycle event;
- repeated investigations and rework, with reviewed evidence (do not turn an
  agent's estimate of time saved into measured savings);
- reported agent cost estimates and active-work duration per accepted delivery,
  including failed attempts; keep verification/admission queue duration separate
  and identify missing cost or active-work coverage explicitly;
- incorrect reuse, regressions, operator interventions and observation coverage;
- mechanism goal: three verified used/adapted effects across at least two batches,
  at least one after its author exited, and no operator-relayed source pointers
  for effects counted toward the goal. Missing evidence cannot pass this goal.

Use persistence order for revisioned assessments. Reject wrong-repo and wrong-
generation joins, repeated self-use, future-source evidence and unsupported
claims of terminal-author reuse. Legacy records without sufficient identity stay
unattributed. Stable ordering and explicit schema/evaluator version make replay
deterministic. All read/export limits must reveal truncation.

Freeze the root tickets and a native lineage rule before dispatch. Cost scope
includes every descendant ticket through `parent`, regardless of outcome, and
reviewer tasks whose actual `AgentRecord.review.task` names an included ticket
in the same repo. Record those native bindings and generations; do not infer a
review relationship from a task-name prefix. Materialize future task IDs under
this prior rule in `consumer_tasks`, retaining the frozen plan and snapshots.
Implementation descendants remain eligible reuse consumers; reviewer support
rows are accounting only. Count accepted root deliveries as the productivity
denominator and all implementation, rework and review attempts in its cost
numerator. With no accepted roots the ratio is undefined. Missing lineage or
cost evidence remains unknown: a complete task-local cost row never proves
complete batch coverage. Apply this identical rule to all comparison arms.
Rework lineage has two authoritative sources, not one: a native rework ticket
may carry `parent: null` (this trial's own Bavus ticket does), and the daemon's
`landing_rework_dispatch` record then supplies the binding, linking task ->
rework_ticket with its own repo/source/target. Closure follows BOTH link kinds,
recursively, retaining failures, and never infers lineage from titles or names.

Task completion, the final provider cost report, and physical process exit are
separate observations. Native observations bind both the agent generation and
the particular process launch; a manual respawn can retain the generation while
starting a new launch. Cost aggregation also retains the provider session so
repeated cumulative results within one query are not summed as separate charges.
Provider cost fields are reported estimates, not billed charges. A process may
remain paused while awaiting verification or the operator: launch-to-exit is
process lifetime and must not be labelled measured active work. Preserve unknown
coverage when the available observations cannot establish either final cost or
active-work duration.

## Trial design

Before dispatch, freeze the selected tasks, build/configuration, eligibility and
scoring rules, UTC start, and a prospective stopping rule in an immutable run
plan. For the first real batch, the observation window ends at the earlier of
six hours after that start or the first recorded observation that all selected
work has settled. Settlement requires actual delivery or an explicit terminal
failure disposition for every selected task, terminal enrolled launches, and no
selected checks, reviews, queued landings or automatic continuations remaining.
Record the native evidence establishing settlement; missing exit or usage
observations remain unknown. A paused process or task-completion claim alone
does not establish settlement.

Materialize the evaluator manifest's end time mechanically from that frozen
rule and endpoint evidence. Retain the plan, derivation, capture timestamp and
pinned persistence boundary separately. Reaching the maximum leaves unfinished
work incomplete/censored in this report; authorized recovery continues outside
the window. It neither authorizes killing workers nor completes this program.
The six-hour maximum is an operational bound, not a delivery SLA. The separate
landing allowance is 120 minutes: the existing 60-minute gate and 45-minute
review bounds plus 15 minutes margin. Keep actual durations and every failure.

First deploy and verify the new product path. Use the same Claude/Sonnet worker
configuration, existing two-worker implementation lane, repository gates and
spend policy. Keep the host awake for measured windows and use a landing allowance
that includes the actual required full checks and bounded queueing. No shortened
deadline may manufacture a landing failure. Preserve failed attempts.

Use the two existing timeout tickets as the first real-work batch: the RPC
investigation may publish a reproduction and timings useful to the observer
classification repair. Publish evidence early. Their prompts describe assigned
work and normal BBS behavior, not a required exchange, named peer, or source ID.
Keep genuine delivery dependencies in the native tracker. Never ask a worker to
perform an operation owned by the King just to run this experiment.

A second batch uses fresh agents and genuinely related follow-up acceptance or
regression work after earlier authors have exited. Review posted findings and
task needs without relaying pointers to consumers. Do not invent needless code
changes or force a successful receipt to satisfy the target. If opportunities
are absent or missed, record why and make the smallest justified discovery change
before a fresh, separately identified repeat. Keep the original failed report.

After mechanism evidence, run matched, isolated replay batches on disposable
copies of representative real tasks, with fresh agent contexts and separate
tuplespace scopes. Compare current briefing behavior (baseline) with the improved
publication/reuse guidance (treatment), holding worker count, model, task inputs,
initial source evidence, checks and hardware conditions constant. Assign complete
batches to arms to avoid within-BBS contamination; counterbalance order. Neither
arm receives answer pointers from the King. Only selected production work lands;
replays remain disposable. Report per-batch cost, time, accepted work, failures,
rework and interference. Small samples yield descriptive evidence, not a broad
statistical productivity claim. A null/negative result is an honest completed
experiment and must be retained, investigated and reported, not relabeled green.

For this comparison, both arms run the same installed candidate. An isolated,
operator-owned adapter (`bbs-arm-harness.py`) removes only the distinctly headed
`Reusable findings` role fragment for the baseline arm; treatment retains it.
It covers BOTH providers — `RK_CLAUDE_BIN` and `RK_CODEX_BIN` point at it —
because the reviewer role actually runs on Codex, and a Claude-only wrapper
would have left reviewer guidance unchanged in the baseline despite the
both-role comparison contract. Claude transforms only its
`--append-system-prompt` argument; Codex transforms only the role fragment
before the role/task separator in its final exec prompt argument. Authenticated
Codex control-resume payloads pass through unchanged. An initial role prompt
must carry exactly one fragment — missing or duplicate fragments refuse the
provider launch. Task text, matching task-local headings, argv, stdin/stdout
and stderr are preserved, prompt/fragment hashes are recorded, and wrapper
control variables are removed from the provider environment. Models,
permissions, budgets, admission, checks and caps are identical across arms, and
it is never installed as the production harness. Acceptance: ten executable
cases and four malformed refusals
(`wrapper-both-harness-acceptance-02/proof.json`) plus eight native routing
cases (`native-wrapper-routing-03/proof.json`), all passed; evidence under
`/Users/chazu/.codex/artifacts/rk-stigmergy-20260913T022353Z`. This measures the
incremental effect of early-publication/reuse guidance with identical product
capabilities, not the total effect of having any shared memory.

## Implementation tickets and ordering

1. **S1 — Findings and independently assessed artifact reuse.** Complete core,
   daemon, CLI, worker instructions and real-CLI authorization/lifecycle tests.
2. **S2 — Exact briefing/open telemetry.** Depends on S1. Cover explicit reads,
   spawn, resume and recovery, bounded selection and telemetry failure behavior.
3. **S3 — Offline evidence report and capture contract.** Can develop alongside
   S1 in its own report module/tests; its CLI wiring lands after S1 to avoid
   conflicting command ownership. Final tests consume actual S1/S2 records.
4. **S4 — Integration, validation, release and first real-work batch.** Depends
   on S1-S3. King owns deployment/capture/dispatch; existing timeout tickets own
   their fixes. Inspect native delivery and exact build identity.
5. **S5 — Author-exit reuse, measured discovery correction, matched comparison.**
   Depends on S4. King chooses fresh useful work, assesses evidence and writes
   the final report. Apply a ranking/prompt change only if evidence justifies it.

Implementation workers coordinate through BBS. Publish contracts/evidence early,
refresh before committing to an interface, and record an actual supporting
contribution where it exists. Claims are advisory. Do not send direct messages
merely to create a collaboration record. Source gate success, native delivery,
deployment and measured benefit are separately recorded in the execution ledger.

## Completion evidence

- This design and native bounded tickets exist with dependency/ownership links.
- S1-S3 pass meaningful unit and real-CLI tests, including forged authority,
  cross-repo/generation evidence, restart, replay, omissions and failed capture.
- The combined candidate passes `mise run verify-full`; normal protected landing
  gates remain enabled. Main, remote, installed rk/rk-mcp and daemon identities
  are recorded at activation, with rollback artifacts.
- Real batches retain manifests, native snapshots, reviewer annotations and
  deterministic reports; the author-exit experiment and matched comparison are
  executed and honestly classified, including null or negative results.
- Report-derived discovery changes, if needed, are independently validated and
  included in the final installed candidate. Every created ticket has an evidenced
  delivered or completed-experiment disposition; unresolved prerequisites remain
  open rather than being declared complete.

The execution ledger and ticket IDs are appended below as work proceeds.

## Native execution ledger

- PROGRAM: `TKT-kuzud-gatag-dipom` — Stigmergy: implement observable artifact reuse and execute measured collaboration trials.
- S1: `TKT-tofak-kozav-sadug` — BBS: publish findings and record independently assessed artifact reuse.
- S2: `TKT-tapip-puhot-sitih` — BBS: retain exact briefing selections and explicit-open evidence.
- S3: `TKT-tavik-kifos-lozuf` — BBS: deterministic offline stigmergy evidence report and live capture.
- S4: `TKT-rujak-divaj-danad` — Stigmergy operator: integrate, deploy and run the first real-work batch.
- S5: `TKT-movap-muvoh-faran` — Stigmergy operator: author-exit reuse, discovery refinement and matched comparison.

Initial source: `8e0cccf`; installed daemon: `72911dd1d382`. No live
workers and no landing queue entries at preflight. Artifact directory: `/Users/chazu/.codex/artifacts/rk-stigmergy-20260913T022353Z`.

- S1 test tracking: `TKT-lakir-sosit-novug` — closed against S1's completed real-CLI, restart and authority coverage.
- S3 correction: `TKT-nonub-pugar-pilid` — operator review found incomplete native identity/scope validation and experiment denominators. Required before S4; retains the existing experiment criteria.
- S2 continuation: `TKT-dorod-sival-fumid` — continues the preserved S2 branch with final-cost/physical-exit observations, export corrections and real-CLI acceptance. `TKT-zutap-zavor-zuloj` tracks the same remaining acceptance and is blocked on this assigned continuation.
- Claude usage accounting: `TKT-jomig-zimab-pugiv` — deduplicate repeated assistant-message usage within a Claude stream. Required before resuming implementation and before S4; budget and burn-rate policies remain unchanged.
- Discovered verification failure: `TKT-mukos-pogim-lopis` — daemon-startup timeout during the accounting repair's broad check; isolated retry passed. The root cause is unproven and the ticket remains open.

Execution checkpoint, 2026-09-13 04:31 UTC:

- S1 delivered to `main` as `f75670b`, including the real-CLI acceptance tests
  and a deterministic persistence-order fixture. Its protected full gate and
  semantic review passed; the worker was dismissed after delivery.
- S3 delivered to `main` as `85b4e70` after an offline-command correction and
  merge-conflict correction. Protected full checks and semantic review passed.
  The separate evaluator correction above is still required before a live trial.
- S2 core and initial tests are preserved at `1e35d20`. The original generation
  remains stopped at its ordinary budget cap. The continuation's `bf3bd63` adds
  a contract document only: its remaining producers, export corrections and
  integration tests are unimplemented. It is paused until the usage repair is
  active, with no task-completion claim. Superseded documentation-check requests
  were cancelled and are not passing evidence.
- The accounting repair is committed at `ee087f1` and its broad check is retrying
  after the recorded startup timeout. It has not landed or been activated.
- Installed binaries and daemon remain `72911dd1d382`. Rollback binaries and
  their hashes are retained in the artifact directory. No live stigmergy trial,
  author-exit trial or matched comparison has run yet.

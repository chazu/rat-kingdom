# R1 qualification: four next deliverables

*Status: proposed for later review. 2026-09-06. This document records the next
body of work identified by the implementation gap analysis. It does not approve
implementation, deployment, ticket transitions, or a new pilot.*

## Objective

Make trusted direct-merge foreign-repository qualification reproducible: complete
the external audit, make the pilot contract executable, release the existing
repairs with the remaining dispatch fixes, and run a fresh measured pilot.

[ROADMAP.md](ROADMAP.md) remains the authority for R1 scope and milestone exit
criteria. The [architecture map](architecture.md) describes current ownership;
the [seven-improvement checklist](2026-09-05-usefulness-improvements.md) records
the preceding implementation. Protected-branch delivery, untrusted tenants,
additional dashboards, and broad lifecycle refactoring are outside this proposal.

| Deliverable | Result | Completion evidence |
| --- | --- | --- |
| D1. Complete the external acceptance auditor | The observer detects lack of progress independently of the daemon's health classification. | An injected silent stall fails the audit while the daemon continues to report `running`; legitimate bounded waits remain distinguishable. |
| D2. Make pilot requirements executable | A frozen run contract determines qualification, including required workload, continuity exercises, and intervention limits. | Missing exercises, insufficient work, or disallowed rescue cannot produce a qualifying result; saved evidence replays identically. |
| D3. Close dispatch risks and release the repairs | Ticket identity is consistent across recovery paths, the pilot's duplicate-dispatch finding is explained, and the reviewed changes reach the runtime. | Focused regressions, full source verification, and a recorded source/install/runtime/remote comparison. |
| D4. Run a fresh fault-injected foreign pilot | The approved R1 candidate demonstrates the required behavior on a foreign repository. | A complete 48–72-hour run, required exercises, retained evidence, and an explicit proceed / repair and repeat / stop assessment. |

## Evidence at the design checkpoint

These are dated observations from the 2026-09-06 assessment, not promises about
the state when this document is picked up again:

- The working tree contains current-work resolution, bounded observation
  lineage and deadlines, durable revert, shared managed verification, typed
  delivery, a first-repository journey, and native saved presentation. The
  preceding full verification log records 1,805 passing tests. The assessment
  also rebuilt the CLI and passed all 21 existing observer tests.
- Committed HEAD, remote `main`, the installed CLI and the daemon were at
  `097572789a44`; the seven improvements were still uncommitted. Build identity
  uses the commit and cannot distinguish different uncommitted builds; see
  [build.rs](../crates/rk-core/build.rs).
- The saved Glossolalia repeat report for `01M1HS3MBHVNN87XKVKC71JZ5D` covers
  53.5 hours and 5,690 samples. It records `passed: false`, 5,254 build mismatch
  samples, a 1,956-second sampling gap, one duplicate dispatch, maximum landing
  age of 1,561 seconds against 600, two reconciliation violations, one stale
  ticket, and zero King replacements. Evidence lives under
  `~/.rat-kingdom-observations/glossolalia-pilot-repeat-2026-09-02/`.
- A short isolated RPC fixture exercised the production observer with a
  selected in-progress ticket and a `running` agent whose progress evidence
  remained an hour old. With a one-second stale bound, it reported zero stale
  tickets. The derived report still passed after an ad-hoc intervention was
  recorded, with zero deliveries, daemon restarts, or King replacements.
  This is an executable counterexample to qualification completeness, not a
  live-fleet incident or a long-duration pilot. D1/D2 must turn this scenario
  into repository-owned regressions rather than depend on temporary files.
- The reopened-ticket sweep still has raw task-ID comparisons. The normal
  CLI `--ticket` path canonicalizes IDs, but historical records and other
  entrypoints require an audit. This risk is not established as the cause of
  the recorded pilot duplicate dispatch.

## What the auditor is and where it is specified

“External acceptance auditor” denotes the proposed extension of `rk observe`.
The existing collector is compiled Rust in
[observation_cmds.rs](../crates/rk-cli/src/observation_cmds.rs), with incremental
log/checkpoint handling in
[observation_store.rs](../crates/rk-cli/src/observation_store.rs). An operator or
authorized agent starts it as a separate process. It is deterministic and has
no model calls, repair authority, or per-run generated program.

The existing [observation runbook](2026-09-02-observation-runs.md) defines the
commands and evidence files. CLI arguments currently freeze scope, duration,
cadence, thresholds and observer build into `manifest.json`. Broader acceptance
requirements live in the roadmap and the tenant's pilot charter. D2 bridges
the requirements that currently exist only in prose.

| Scope | Current behavior and proposed boundary |
| --- | --- |
| Observation run | One repository and one time window; independently started and resumable. It is not automatically attached to each ticket. |
| Ticket cohort | Optional explicit root tickets, with bounded daemon-authored correction lineage. Ticket-related progress, usage and throughput must use this cohort consistently. |
| Repository | Landing queue and reconciliation currently include repository-wide conditions. Their scope must remain explicit even in a ticket-selected run. |
| Shared infrastructure | Daemon availability, build identity and King continuity describe infrastructure supporting the run. They are not attributable solely to a selected ticket. |
| Product correctness | Repository checks and semantic review evaluate the delivered change. The observer does not interpret arbitrary ticket acceptance prose. |

The collector has its own clock, deadlines and evidence log, but currently
obtains most evidence through daemon RPC views. Its independence is execution
and evaluation independence. It is not a separate source of truth for every
Git, process or registry fact. The R1 threat model trusts the machine and
repositories; detecting forged, consistently false daemon evidence is outside
this proposal. Missing or stale evidence must still prevent a healthy result.

An LLM may configure an authorized run, investigate a failed check, perform
delegated recovery, or provide a separately recorded semantic judgment. Those
actions are inputs to the audit. Report evaluation remains deterministic, and
an LLM cannot waive a frozen requirement by writing a favorable summary.

## D1. Complete the external acceptance auditor

### Problem

`derive_metrics_with_ready_age` treats a ticket with a `spawning` or `running`
agent as live and excludes it from ownerless-ticket staleness. The collector
retains some liveness fields, but does not independently establish bounded
forward progress for a live generation. A failed supervisor sweep can therefore
leave the external audit green.

### Proposed design

Extend the existing observer with a deterministic progress evaluator:

1. Bind evidence to canonical ticket identity, exact agent generation, and the
   current execution/session attempt where available. A replacement or resume
   must not inherit a predecessor's proof of progress.
2. Track relevant changes across samples: structured progress revisions,
   changed output evidence, lifecycle transitions, verification outcomes and
   durable delivery. Define the meaning of each signal; a repeated message,
   generic registry timestamp refresh, or transport retry alone cannot prove
   forward progress.
3. Model bounded waits explicitly. A verification execution, queued admission,
   declared human gate or recovery backoff needs an owner, exact identity and a
   deadline or registered hold policy. A static `running` state cannot provide
   an unlimited exemption. A held parent may be making progress through a
   selected correction descendant.
4. Retain the start, resolution and recurrence of each observed stall episode.
   Repairing a stall later must not erase the run's transient violation.
5. Preserve elapsed silence and evidence gaps across observer restarts. The
   replaceable checkpoint caches state; append-only evidence remains sufficient
   for replay. Missing, ambiguous or truncated sources stay visible as
   incomplete evidence.

Reuse existing progress, generation and verification evidence. If a necessary
field is absent from a read surface, add the smallest typed read projection at
its existing owner. Do not create another supervisor or recovery loop.

### Acceptance

- An isolated real-CLI scenario freezes worker progress while daemon RPC stays
  responsive and the agent remains `running`; the audit detects the stall
  within the configured bound plus documented sampling/deadline tolerance.
- A bounded verifier or declared gate remains classified within its allowance;
  an expired or unsupported exemption cannot keep the run healthy.
- Correction lineage, generation replacement, observer restart and a later
  recurrence preserve the correct progress clocks and incident attribution.
- Detection works without a daemon-produced “stuck” alarm. Observer outage or
  insufficient evidence produces an explicit coverage failure.
- Live evaluation and offline replay agree on the same immutable samples.

### Decisions to resolve before implementation

Choose the minimal progress signals and phase-specific allowances; establish
which existing RPC fields provide them; define how missing initial history and
clock anomalies affect coverage. Favor a small auditable state model over a
generic expression language.

## D2. Make pilot requirements executable

### Problem

`derive_report` counts deliveries, daemon restarts, King replacements and
intervention classes. Its success predicate does not require useful workload,
the planned continuity exercises, or an acceptable ad-hoc intervention count.
General observation thresholds therefore cannot establish full M3/M4 acceptance.

### Proposed design

Introduce a versioned, typed acceptance section frozen into the run manifest.
An optional repository-owned input may provide it; the frozen copy and its
digest are the authority for that run. Repository CUE continues to authorize
execution and delivery. An observation contract grants no execution authority.

| Requirement | Evidence required for qualification |
| --- | --- |
| Scope and workload | Exact root selection or explicit repository scope, expected terminal outcomes, and a declared minimum useful workload. Root and correction deliveries remain distinct. |
| Duration and coverage | Planned elapsed window, cadence, RPC deadlines and explicit coverage tolerances. Idle elapsed time alone cannot satisfy workload requirements. |
| Liveness and holds | D1's progress bounds and declared hold rules, with retained incident maxima and incomplete-source failures. |
| Continuity exercises | Named required exercises and ordering conditions, exact before/after identities, a recorded action, and evidence of successful continuation without repeated side effects. |
| Interventions | Allowed structural classes and limits. R1 qualification requires zero ad-hoc human rescues; predeclared human gates retain their authorization and scope evidence. |
| Build and policy identity | Frozen build and relevant activated-policy identities; unexpected changes become visible qualification failures. Planned restart exercises use the approved artifact. |
| Resource and delivery limits | Spend, ready/landing age, duplicates, forced landings, reconciliation and other registered constraints. |

A PID change alone proves a daemon process changed. A changed King identity
alone proves a registration changed. Neither proves successful continuation;
the exercise must link those transitions to subsequent progress and preserved
delivery state. Likewise, an intervention's submitted class is a claim to
validate against recorded authorization and the declared exercise or gate.
The evaluator cannot infer undocumented human activity.

Separate ordinary observation results from qualification. A run without an
acceptance contract can retain its general metric report, but cannot claim an
M3/M4 qualification result. An in-progress run may have healthy samples while
required duration or exercises remain pending. Qualification requires every
mandatory requirement to be evidenced; violations remain failures after repair.

### Acceptance

- The frozen contract represents every mandatory requirement used for the next
  pilot decision, with validation before measured work starts.
- A run with no useful deliveries, an omitted required exercise, or excessive
  ad-hoc intervention cannot qualify even when general health metrics pass.
- Exercise evidence proves continuation at the required lifecycle boundary;
  an unrelated restart or unsupported narrative cannot satisfy the requirement.
- Contract changes require a new run. Historical evidence remains readable
  with explicit schema/evaluator provenance; absent historical data cannot be
  retroactively treated as proof that a new requirement passed.
- Replaying saved evidence requires neither a running daemon nor an LLM and
  produces the same decisions for the same contract and evaluator version.

### Decisions to resolve before implementation

Choose the contract input format and CLI integration, report compatibility and
result vocabulary, the minimum workload definition, and the smallest typed
exercise/authorization evidence model. Exact new flags and file names are
deliberately unspecified here. General benchmarks and incident observations
must not inherit pilot-only workload or restart requirements.

## D3. Close dispatch risks and release the existing repairs

### Scope

Audit ticket identity through all relevant entrypoints and persisted records,
explain the pilot duplicate-dispatch finding, and deliver the reviewed code
through the existing repository policy. Include the seven improvements already
present in the working tree; recheck their state before planning the release.

The known source seams are `ticket_reopen_sweep_at` in
[server.rs](../crates/rk-daemon/src/server.rs), `queued_entry_for` in
[landing.rs](../crates/rk-daemon/src/landing.rs), spawn admission in
[supervisor.rs](../crates/rk-daemon/src/supervisor.rs), and canonical resolution
in [tickets.rs](../crates/rk-daemon/src/tickets.rs).

### Proposed work

1. Reproduce legacy canonical-ID/alias mismatches at real recovery and queue
   lookup seams. Audit CLI, RPC, workflow and drain entrypoints. Normalize known
   tickets at the owning boundary and retain alias-aware reads for persisted
   records; arbitrary ad-hoc task names must keep working.
2. Ensure a live owner or queued delivery under either valid spelling prevents
   a stale-ticket reopen. Review cancellation must find the same exact queued
   candidate through either spelling without relaxing generation fencing.
3. Reconstruct the recorded pilot duplicate from exact generations, roles,
   execution windows and delivery records. Determine whether it was a repeated
   implementation dispatch, legitimate activity misclassified by the observer,
   or insufficient evidence. Link any correction to a reproducing regression;
   do not assume the alias risk explains this incident. Unresolved evidence
   cannot satisfy this deliverable's entry condition for the next pilot.
4. Review and land the intended source changes, preserving unrelated dirty and
   private artifacts and honoring activated delivery gates. Run the complete
   repository source gate on the release candidate.
5. Record commit and artifact identities, deployment/rollback steps, daemon
   rollover and policy checks. Verify remote, installed CLI and running daemon
   against the approved committed source; a matching commit label on different
   uncommitted binaries is insufficient.
6. Reconcile tracker state with evidence. Implementation completion, runtime
   activation and milestone acceptance need separate recorded outcomes.

### Acceptance

- Regression coverage includes historical alias records, live owner recovery,
  queued delivery, relevant queue lookup, and canonical new entrypoints.
- The pilot duplicate has an evidence-backed disposition and any required
  regression/fix. Observer changes preserve detection of actual duplicates.
- `mise run verify-full` passes for the intended release candidate. Verification
  failures receive a disposition before source acceptance is claimed.
- The released source and runtime comparison is recorded; rollback and startup
  preserve durable state. No new pilot begins on an unrecorded mixed build.
- Tracker entries reference the appropriate proof and do not treat closed
  implementation work as evidence that an operational week occurred.

## D4. Run a fresh fault-injected foreign pilot

### Entry and ownership

Begin only after D1–D3 have their evidence. A human confirms the tenant and any
new or changed policy, budget, destructive scope and human gates. Glossolalia
is the existing proving ground, but its next workload and current authorization
must be re-read; completed historical tickets are not fresh work.

Use the roadmap's WIP-1, 48–72-hour supervised M3 scope unless a separately
reviewed roadmap decision changes it. Pre-register exact work, thresholds,
exercise timing and stop conditions through D2. The existing holder-fenced King
or an authorized operator owns dispatch and interventions. The observer only
records and evaluates them.

### Execution outline

1. Verify policy and build identity, workload readiness, available budget,
   observer continuity and rollback evidence. Freeze the run contract and
   confirm initial collection before dispatch.
2. Exercise worker death, a named-check failure and a merge conflict through
   bounded, approved scenarios. Show that work is recovered or enters the
   declared actionable hold while exact source and gate evidence survive.
3. Exercise daemon rollover and King replacement at the declared delivery
   boundaries. Record exact before/after identities and subsequent continuation;
   verify that no dispatch, landing or ticket finalization repeats.
4. Prove the auditor's injected-silent-stall detection in a clearly separated
   preflight run before the measured zero-stall pilot. If a diagnostic run
   deliberately violates a hard threshold, preserve its expected failing
   result. It cannot double as the passing qualification run.
5. Maintain the full observation window and registered workload. Planned
   recoveries must stay within the contract; a planned restart does not exempt
   an observed outage from a frozen zero-outage threshold. A hard failure sends
   the program to repair and repeat or stop. Later convergence cannot erase it.
6. Produce the final deterministic report and an outcome memo linking every
   mandatory requirement to evidence. A human reviews the release-program
   decision against the roadmap.

### Acceptance and retained artifacts

Retain the frozen contract and manifest, append-only samples, interruption
evidence, typed interventions, exact exercise records, release identities,
delivered-or-held disposition for each selected root, and replayable report.
The outcome must explicitly say **proceed**, **repair and repeat**, or **stop**.

Proceed requires all M3 requirements, including the full duration, useful work,
continuity exercises and absence of prohibited incidents. It permits planning
M4; the seven-day unattended acceptance period and final R1 release decision
remain separate work. The historical failed report is retained unchanged.

## Sequencing and existing tracker coverage

D1 and the contract design in D2 can develop together; D2 consumes D1's progress
evidence. The identity investigation in D3 can proceed independently. Final
release verification in D3 includes the completed D1/D2 implementation. D4
starts only after that released candidate and its entry evidence are ready.

The following ticket state was observed during the assessment and must be
refreshed when execution resumes. This document changes no tracker records.

| Existing ticket | Relevance and follow-up |
| --- | --- |
| `TKT-kadov-nabod-nofov` | Observation correction-lineage attribution; open despite implementation in the working tree. Include its acceptance and delivery evidence in D3. |
| `TKT-bipin-rinos-gatad` | Resolved historical Needs in current work; open despite implementation in the working tree. Include in D3. |
| `TKT-gusab-hihus-tijof` (`TKT-01M11QE25ARCM49DHSG0K904DJ`) | Open alias-comparison audit; direct coverage for the identity portion of D3. |
| `TKT-01M0FQ94FSY0VB4ZP60DK4Q8PJ` | Open post-slimming pilot; D4's operational anchor. Its `pilot-running` label needs reconciliation with the stopped, failed run. |
| `TKT-01M0CTC52EBF8KSCK1CTFRCFYD` | Closed external-audit/unattended-drain ticket; its closure does not establish the missing D1 proof. Review whether to reopen it or link a successor after approving this design. |
| `TKT-01M0E8PP3B8YX3S0D5C2RKMGTF` | Closed unattended-week ticket; M4 still requires its own accepted evidence after a passing D4. Reconcile status and coverage before scheduling. |

D2's executable acceptance contract and the pilot duplicate investigation need
explicit bounded coverage when this design becomes an implementation plan.
Do not reopen broad feature work merely to clear unrelated historical backlog.

## First review when work resumes

Refresh the working tree, tracker and runtime checkpoint. Resolve D1's evidence
signals and D2's contract representation first, then split the accepted design
into independently verifiable implementation slices. Select the foreign
workload and its numeric thresholds before starting the new run. This document
preserves the direction and acceptance bar while those choices remain open.

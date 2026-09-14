# Continuous validation, promotion, and recovery

Status: draft for adversarial review; implementation is authorized after this
plan is stabilized. Date: 2026-09-13. Initial source audit: `3cfabbe90337b8d7214fe4d5791a16dd7ce44b03`.

## 1. Mandate and operating principle

The operator authorized a thorough plan, adversarial review, then execution of
continuous validation, progressive promotion, rollback, and resource scheduling
for Rat Kingdom. Integrate stigmergy where it helps agents discover and reuse
evidence. The first application is RK maintaining its own local installation;
the same contracts must support another project and external environments.

Evidence controls the next increase in exposure. A failure or uncertainty holds
the affected candidate, feature, or environment. Independent implementation,
integration, and delivery continue within the machine's resource budget.
Correctness checks still gate the changes they cover. Evidence of business or
collaboration benefit can accumulate during ordinary useful work.

Required outcomes:

- R1: Reduce idle validation capacity and repeated checks caused by the landing
  pipeline; preserve exact candidate, review, target, task, and policy bindings.
- R2: Separate integration, release verification, installation, and feature
  exposure without changing existing ticket delivery semantics silently.
- R3: Provide durable local release identity, activation, health observation,
  recovery, and rollback to a compatible known good release.
- R4: Support disabled, shadow, selected-cohort, and default-enabled behavior
  with stable assignment, a working disable path, and eventual flag retirement.
- R5: Evaluate predeclared objectives and constraints incrementally, retaining
  sample sizes, denominators, uncertainty, provenance, and causal limitations.
- R6: Budget expensive work across repositories on the same host. Preserve
  interactive/control-plane capacity and prevent background evaluation starvation.
- R7: Extend the existing external SDLC boundary with policy-controlled outgoing
  deployment and recovery actions, exercised against a real independent local
  service before requiring any production credentials.
- R8: Put useful contracts, findings, needs, and verified reuse on the BBS;
  provide agents with discovery rather than requiring the King to relay findings.
- R9: Ship and exercise the mechanisms in bounded increments. Completion requires
  installed behavior and retained evidence, not only green unit tests or a plan.

## 2. Existing mechanisms and their limits

Use these modules and contracts; do not start a second factory or analytics DB.

| Existing capability | Reuse | Gap to close |
| --- | --- | --- |
| Activated `.rk/repo.cue` | Repository scope, policy digest, inner/protected targets, delivery modes | Release environments and exposure policy |
| `.rk/checks.cue`, managed verification | Named commands, timeout, owned process groups, per-repo admission, exact proof reuse | Host-wide budgets, resource classes, release artifact checks |
| Native landing queue | Durable candidates, stale-target handling, recovery, review, final target CAS | Target lock currently spans gates and review |
| Docs/trivial landing batches | Combined-candidate preparation, bisection, per-ticket closure | Fresh batches are serialized when capacity admission is enabled; no general code train |
| `verify-changed.sh` | Changed package and reverse-dependency checks, conservative full fallback | Known target/base binding and reuse across staged integration |
| `factory.scorecards`, `factory.recommend` | Structured normalizers, deterministic metrics and advisory recommendations | Some native delivery/cost source seams remain unavailable; no live exposure controller |
| Task spans, `rk observe`, factory replay/watch | Durable timing, bounded incremental observation, gaps and interventions | Release/config/cohort correlation and incremental objective assessments |
| `rk ingest` and SDLC current facts | Source authentication, deduplication, CI/deployment/alert correlation | Outgoing typed actions and environment reconciliation |
| Typed factory proposals/approvals | Scope/digest fencing, idempotent execution receipts | Explicit release and exposure action types |
| `rk daemon rollover` | Pause dispatch, preserve work, recover parked generations | Release inventory, boot-health rollback, reliable launcher |
| BBS findings, needs, reuse receipts/assessments | Reproducible artifacts and task-aware discovery | Tie useful release/validation evidence into existing discovery and metrics |

The initial RK policy protects `main`, selects `verify` (full workspace) there,
has no focused inner-check rules, and uses a verification limit of one per repo.
That limit is not a host-wide cap. Machine load currently guards new spawns, not
every expensive command. Most configuration is read at startup; editing a file
does not imply live reload. Repository policy requires digest activation.

The current installer copies `rk` and `rk-mcp` into the install directory. The
legacy `mise deploy` stops/pings the daemon. Neither establishes a release
transaction or guarantees a healthy successor. Existing `merge-reverted` facts
describe Git changes, not environment rollback.

Factory analytics already distinguish unavailable evidence from zero. Its
read-only API must remain read-only. New action execution is a separate typed
surface using existing authority machinery. Do not change the Factory Foreman
approval contract or make arbitrary recommendation text executable.

## 3. Domain model and authority

An **integration** is a delivery of source into a declared shared target. A
**candidate** is immutable source plus the exact check plan and policy version.
A **release** adds immutable build artifacts and compatibility metadata. A
**deployment** binds a release and configuration to a particular environment.
An **exposure** assigns a feature variant to an explicit cohort. A **promotion**
advances one of those named states under policy; it is not a synonym for merge.

The existing ticket DeliveryRecord remains a source-delivery record. New release
and deployment records link tickets/commits and do not overwrite it. Displays
show integrated, release-verified, deployed, enabled, and rolled-back separately.
Dependency satisfaction defaults to existing source delivery. A dependency on
deployment or exposure must declare that condition explicitly.

Canonical records (version 1, bounded payloads, stable IDs):

- Release: repo identity, source and tree SHA, artifact hashes and roles, build
  recipe/toolchain, check receipts, compatibility range, creation provenance.
- Environment: repo/service/target identity, adapter registration digest,
  desired and last observed release/config, revision, current transition ID.
- Transition: action, expected environment revision/current release, destination,
  policy digest, requester/authority, idempotency key, status, observations.
- Feature assignment: feature ID, variant, scope/cohort definition, config
  revision, release, task/spawn/attempt where applicable, assignment time.
- Objective: versioned metric definitions, source/eligibility rules, constraints,
  observation budget, promotion/disable conditions, freshness, minimum samples.
- Assessment: objective/config/release/cohort versions, bounded source cursor
  interval, values with numerators/denominators, coverage, verdict and reason.

Use existing tuple Event/Fact/Artifact patterns for audit and current projections,
with the existing durable mutation mechanism for CAS/idempotency. A cache or
checkpoint may be replaceable; source records and transition receipts are not.
Do not put every raw performance sample or transcript in the tuplespace.

Repository-owned policy declares allowed destinations, named checks/actions,
exposure bounds, objective versions, and rollback targets. Machine-local registry
binds adapter executables and credentials. A worker can propose a policy change;
it cannot activate a wider policy, spoof a health observation, or approve itself.
Ordinary successful transitions and preauthorized recovery are mechanical.
Ambiguous remediation may create a bounded agent task. Irreversible state changes,
unavailable credentials, or genuinely new authority reach the King/human.

## 4. Landing scheduling and integration

### 4.1 Remove review serialization in two bounded steps

First run a candidate's independent semantic review and named checks concurrently
after cheap source/protected-path/diff-scope admission. Both must pass before
target advancement. A failed gate cancels/settles any now-unneeded review using
its exact attempt, preserving cost and late verdict evidence. Neither result
authorizes advancement by itself. Retain existing review reuse constraints.

Next allow a bounded second candidate to prepare/check while the first awaits
review. Default remains one in-flight candidate until explicit activation; trial
cap is two per target and one expensive check per repository. Release the broad
target lock around waiting work; use short fenced claims and final target CAS.
Do not remove source fences or allow two actors to own one candidate phase.

Initially candidates use a captured actual target base, not an unreviewed
predecessor. If target advancement makes a checked candidate stale, reprepare
and recheck the changed candidate; never reuse the old SHA proof for it. Bound
lookahead to one, record wasted checks, and disable lookahead if it loses more
compute than it saves. A later general merge train is not required for this
program; the release snapshot path below provides amortization without it.

Restart must reconstruct active/waiting phases from durable records. Read-only
queue/status views must continue to work while gates/reviews wait. A slow review
must not starve other targets or independent checks. Preserve current FIFO target
advancement initially; this slice overlaps preparation, not delivery ordering.

### 4.2 Integration and immutable release selection

Add explicit repository integration/release roles rather than infer them from
branch names. First adoption can retain protected `main` and add a rolling
integration target. Worker bases and delivery targets must agree with activated
policy. Configure actual focused checks before any unattended inner landing.
Unknown or workspace-wide inputs keep their conservative broad checks.

Select immutable release snapshots from integrated commits. While a snapshot is
validated, newer work may continue integrating. Maintain at most one running and
one coalesced pending release candidate per environment/stream. Never mutate the
running candidate or restart it on each new commit. Record superseded pending
candidates and included ticket membership. A failed snapshot holds its release;
revert/correct the implicated change and select a new snapshot. Independent
features may remain integrated with exposure disabled.

The first release policy keeps the full suite on the protected release edge,
amortized across the snapshot's included work. Later removal of a specific check
requires evidence and a reviewed policy change; this plan does not automatically
replace full correctness checks with production monitoring.

Proof reuse requires exact declared inputs: repo, immutable candidate/artifact,
check name/definition, toolchain/environment, target/base when relevant, and
policy/config context consumed by the check. Live-service health checks are
time/environment-bound and must not reuse a timeless compile/test proof.

## 5. Host resource scheduling

Extend managed verification into a shared execution budget for heavy named work:
checks, artifact builds, deployment smoke tests, and explicitly launched trials.
Retain per-repo fairness/limits and managed process ownership.

Declare resource class and conservative weight in the named recipe: cheap read,
compile/test, or optional experiment. A host ceiling limits aggregate heavy work
across repositories. Bound build job count and test parallelism in the recipe;
one admitted command must not silently create an unbounded child workload.
Reserve capacity for the daemon/observer/human interaction and prioritize useful
release work over optional experiments, with aging to prevent starvation.

Start with static capacity selected from current hardware and measured workload;
do not invent an adaptive optimizer. Report queue and execution time separately,
resource class, weight, cancellation and oversubscription. Admission deadline is
distinct from execution timeout. No nested permit acquisition deadlock: a parent
recipe must either retain one budget for its children or explicitly yield it.

Opaque worker shell commands cannot be reliably controlled by instruction alone.
Route repository-mandated broad verification/build paths through native admission,
document the remaining enforcement boundary, and detect known unmanaged runs
without process-name-based killing. Worktree cache isolation stays intact; do not
enable shared Cargo targets as an assumed optimization (known stale artifacts).

## 6. Local release and rollback

Build into immutable, content-verified release directories. Bind the paired RK
and MCP executables to the same release, preserving source-to-build provenance.
Install once; promotion changes an active release/config pointer rather than
rebuilding. Resolve the pointer once per launch to avoid mixed binaries.

Use states `prepared -> activating -> observing -> healthy`, with explicit
`failed`, `rollback_pending`, `rolled_back`, and `unknown` outcomes. Persist intent
before changing the active pointer and persist observation before claiming
success. Retries use the same transition ID; reconcile actual state after a crash.
Only one transition owns an environment revision. A stale request cannot restore
an old release over a newer deliberate activation.

Use the existing rollover lifecycle to preserve workers. A small separately
installed stable launcher starts the selected daemon, checks its release identity
and bounded read-only health probe, and can restore the prior compatible release
if the candidate cannot boot. It must work when the candidate daemon is absent.
The launcher has a filesystem lock, bounded retries, and durable receipts that
the recovered daemon ingests; it does not start a second controller agent.

Health includes process liveness, correct release identity, successful bounded
read, and ability to access current state. Read-only health checks cannot prove
all mutation paths; use isolated state for transactional smoke tests. Runtime
regression feedback may disable a feature or request a preauthorized rollback.
Do not run two writable daemons against the production tuplespace.

Rollback changes binaries/config/exposure, not the journal or ticket history.
Declare supported state schema read/write ranges. Prefer additive compatible
changes; refuse automatic binary rollback across incompatible state writes.
Record a human gate or a named forward-recovery action in that case. Retain the
current and previous healthy release while pruning only unreferenced artifacts.

Acceptance includes the CLI/MCP pair, existing session recovery, a boot-failing
candidate, a healthy candidate, interrupted activation, stale rollback, and a
rollback that preserves work created after activation.

## 7. Feature exposure and fitness

### 7.1 Feature lifecycle

Feature policy supports `off`, `shadow`, `cohort`, and `on`. Shadow implementations
must be side-effect-free; no duplicate tasks, provider calls, ticket mutations,
or remote writes. Optional shadow work has an explicit compute budget.

Resolve cohort membership deterministically from a policy seed and stable unit
(repository, task, or agent generation, declared per feature). Freeze a worker's
variant and config revision for its generation; a reconnect/resume of that
generation retains it. A separate emergency disable overrides new feature
operations even for an assigned generation; record both assignment and disable
event. Some features cannot safely change mid-operation: finish/abort under their
declared transaction boundary before switching.

Provide typed show/propose/activate/disable operations and validated atomic config
updates. Preserve startup-only behavior for unsupported settings and say which
changes require rollover. Bounds and activation authority live in repo policy;
do not allow an exposure change to silently change authority or named checks.

First feature: alternative BBS discovery ranking, with current ranking retained
as baseline. Shadow compares bounded candidate lists; cohort activation changes
only the briefing for selected tasks. Keep authoritative tuples/reuse schemas the
same. Select the actual ranking adjustment from observed eligible misses/noise,
not from a fabricated requirement to produce more posts. Publish limitations.
Record owner, introduction, dependency/interaction constraints, retirement
condition, and a removal ticket once one variant is accepted or abandoned.

### 7.2 Continuous assessment

Extend existing scorecards with missing authoritative native delivery, cost,
release, exposure and BBS sources. Keep existing v1 fields compatible and label
their exact meaning; add explicit integration/deployment/enablement counts.
Do not equate root ticket closure with user acceptance or a reviewer task with
a product deliverable. Costs include all linked implementation, correction,
review and experiment attempts; report partial provenance and denominator zero
as unknown/undefined. Never infer task class from a title or model name.

Evaluation consumes bounded replay pages and durable checkpoints, deduplicates
source occurrences, and publishes assessments on meaningful change or a bounded
cadence. Collection and evaluation require no LLM. Missing pages, source outages,
clock discontinuities, and unknown cohort assignment stay visible. Preserve raw
reference links and evaluator versions so a saved assessment is reproducible.
Do not copy the full historic tuplespace for each observation tick.

Separate operational guardrails from benefit claims. Severe known failures can
disable exposure immediately under declared policy. A benefit assessment needs
minimum eligible samples and comparable cohorts; repeated dashboard inspection
is not a valid significance test. Predeclare a fixed-sample or explicitly
sequential decision rule and report uncertainty. Low evidence holds expansion
while ordinary work continues. A new release cannot reset an ongoing feature's
evaluation clock or discard its failures; incompatible variants start a new
linked evaluation cohort with old evidence retained.

The controller evaluates an activated objective and action policy. Verdicts are
`pass`, `fail`, `inconclusive`, and `unavailable`; source outages cannot produce
`pass`. Action selection is separate from metric computation. Only allowlisted
transitions within exposure bounds execute mechanically. Use hysteresis/minimum
dwell and a recovery budget to prevent oscillation. An unresolved regression
creates one coalesced need/work item with reproduction and affected identity.

## 8. Predeclared program objectives

These are initial acceptance contracts for this program, not universal SLAs or
claims of improvement. Record the activated numeric profile before each exercise.

| Objective | Decision and evidence |
| --- | --- |
| Landing overlap | Barrier-based test proves review A waiting does not prevent an admitted check B, with cap 2 and heavy cap 1; no duplicate advancement or stale proof reuse |
| Scheduling improvement | Matched bounded fake-harness workload separates review waits and check execution; new critical path overlaps stages and reports any extra stale work |
| Integration progress | A later independent change integrates while a frozen release candidate is validating; it does not alter that candidate |
| Release recovery | Initial local profile: boot identity/read healthy within 30 s; failed boot recovers previous compatible release within 60 s; one rollback attempt, then explicit degraded state |
| Host budget | Two repo fixtures cannot exceed configured aggregate weight; queued cheap control reads remain available; cancellation frees ownership after exact child cleanup |
| Feedback latency | Native completed events reflected within two configured 30 s evaluator ticks while the daemon remains available; gap is unavailable rather than pass |
| BBS benefit | Verified useful effects per eligible source-consumer opportunity, plus miss rate and assessment coverage; production goal is direction of improvement with uncertainty reported |
| BBS guardrails | Briefing cost/latency and incorrect reuse/rework tracked by feature cohort; no mandatory exchange count or invented time saved |
| Factory throughput | Delivered and deployed root changes per host-hour, p50/p95 lead time, check minutes, rework/rollback rate, model cost, and operator interventions; no ticket splitting to improve score |

Use normal load for functionality and operational feedback. Reserve quiet-host
benchmarks for a specific timing claim that cannot be answered under normal load.
No 48-hour quiet run, four-arm replay matrix, or multi-hour observation wait is
a prerequisite for shipping this program. Long-lived observation is useful only
while useful work continues; absence of enough samples remains inconclusive.

## 9. Stigmergic coordination

Workers read the normal BBS brief before key decisions and publish concrete
contracts/findings with revision, affected areas, executable evidence and limits.
Cross-slice questions use Need/answer relationships. A consumer records verified
use or adaptation only when it changes actual work, referencing supporting
artifacts. Author completion does not delete findings. Reviewer corrections and
recovery incidents are discoverable evidence, with superseded claims linked.

The King controls scope and authority, not every information handoff. Task briefs
name interfaces and dependencies, not a required peer or conversation. A blocked
consumer can discover a provider's published schema/example from task/area links.
Unresolved needs have ownership/expiry and cannot endlessly wake the King.
Telemetry, exposure summaries and failed validations may generate bounded,
coalesced needs; ordinary metric ticks never spawn agents or King wakes.

## 10. External project delivery

Define a narrow adapter contract with named `prepare`, `activate`, `observe`, and
`rollback` operations. Inputs bind repo/service/environment, immutable artifact,
config, expected observed revision, action policy, and transition ID. Outputs
are bounded structured receipts with actual state and evidence references.
Executable registration is machine-owned; source credentials are kept outside
worker context. Never execute command text from an ingested alert or BBS post.

A mutation timeout means unknown outcome. Observe/reconcile with the same
idempotency key before retrying; do not claim exactly-once remote effects.
Concurrent/stale requests fail CAS. A preauthorized reversible rollback may run
automatically, while destructive data migration or new authority is an explicit
gate. Existing authenticated ingress observes the results and health transitions.

Ship an independent local HTTP/service fixture adapter with its own state,
versioned artifacts and health endpoint. Exercise stage -> selected activation
-> healthy promotion -> induced failure -> rollback, including lost responses
and restart. This proves the general adapter contract without claiming a remote
production deployment. Vendor-specific cloud integrations are later consumers,
not an excuse to leave the external action interface unimplemented here.

## 11. Dependency-ordered execution slices

All slices are AFK within activated policy except final operator activation and
any actual new credentials/irreversible decisions. File native RK tickets only
after review stabilization. Mark only currently authorized ready slices for
dispatch. Split a slice further if its reviewed diff exceeds repository limits.

| Slice | Deliverable and decisive acceptance | Dependencies |
| --- | --- | --- |
| P1 | Overlap one candidate's review and full gate; both must pass, stale/failed/cancelled cases settle correctly, phase metrics truthful | Plan |
| P2 | Bounded preparation/check progress behind a pending review; phase claims survive restart and final target CAS remains serial | P1 |
| P3 | Host-wide weighted admission for named checks across two repositories, managed cancellation and fairness | Plan |
| P4 | Named artifact-build/experiment routing through host budget, bounded child fan-out and visible unmanaged boundary | P3 |
| P5 | Explicit integration/release roles and one running/one pending immutable candidate; focused inner checks and separate status | P2, P4 |
| P6 | Versioned release inventory and paired RK/MCP artifact installation with source/check/config provenance | P4 |
| P7 | Stable launcher, transactional activation, health observation and compatible rollback preserving live state | P6 |
| P8 | Typed feature config and deterministic cohort assignment, emergency disable, restart/reconnect persistence | P6 |
| P9 | Native delivery/cost/exposure source adapters for existing scorecards and bounded incremental objective assessment | P8 |
| P10 | Policy-controlled exposure promotion/disable/recovery from assessments; inconclusive and stale evidence hold only that expansion | P7, P9 |
| P11 | BBS ranking shadow/cohort feature with useful discovered evidence, original behavior fallback and retirement path | P8, P9 |
| P12 | External action adapter and independent local-service staged deployment/rollback journey | P7, P10 |
| P13 | Activate RK integration/release policy and install release controller; demonstrate continued integration during validation and local recovery | P5, P7, P10 |
| P14 | Real work under BBS cohort, continuous scorecards, lifecycle/throughput report and honest benefit verdict; update operator docs | P11, P12, P13 |

P1/P2 share landing internals; P3/P4 share managed execution. At most two
implementers initially, assigning disjoint areas where possible. Publish
cross-area contracts on BBS before dependent integration. Preserve current
spending, review, rework and verification limits until their replacement policy
is explicitly installed. Parallel review must also respect reviewer admission.

## 12. Validation, review and adoption

Before execution, freeze the plan commit and compare against R1-R9 and current
repo contracts in two independent adversarial passes: requirements/completeness
and standards/operational failure modes. Require concrete failure sequences,
especially daemon self-recovery, stale target checks, authority, missing metrics,
resource deadlocks, and feature assignment contamination. Retain findings with
resolved/deferred dispositions. Revise and repeat review until no blocking
finding remains. A deferral may change sequencing; it cannot erase R1-R9.

For each implementation slice: reproduce the behavior with a finite meaningful
test, implement the complete CLI/RPC/persistence path it needs, run focused
managed checks, get independent native review, and land through activated gates.
Use barriers and fake providers for lifecycle/concurrency failures, not sleeps
and paid model loops. Maintain tested source/policy identity through delivery.

Introduce new policy options with old behavior as their compatibility default.
Activate new behavior deliberately per slice after its failure-path acceptance.
Do not wait for every slice before installing useful safe improvements. Until
native release control exists, retain the established operator build/install/
rollover procedure and exact rollback artifacts. After P7/P13 use the product
path being built, including its recovery evidence.

Finish existing Pipsqueak/Hazel deliveries and retain their original observation
window/results. Their failures and costs are not reclassified. This program
supersedes the earlier mandatory multi-arm stigmergy replay plan as a prerequisite
to feature promotion; preserve those historical plans and mark unrun arms as
superseded, not passed. Carry useful pending bug tickets forward without
redispatching duplicates or losing prior delivery provenance.

Completion audit maps every R1-R9 requirement and P1-P14 slice to source delivery,
check/review evidence, installed identity, and an actual product journey. Missing
evidence keeps the program incomplete. A null BBS benefit verdict is a valid
experimental result; absence of a working feature/exposure/reporting path is not.

## 13. Source references

- `crates/rk-daemon/src/landing.rs`: process_entry/process_batch, gate plan,
  drain_key, review and target advancement.
- `crates/rk-daemon/src/managed_verification.rs`: ownership/admission/proof cache.
- `crates/rk-workflow/src/lib.rs` and repository/check CUE schemas: policy.
- `crates/rk-daemon/src/factory_analytics.rs`, `crates/rk-core/src/factory/`:
  existing outcome normalization and advisory scorecards.
- `crates/rk-daemon/src/span.rs`, `phase_latency.rs`, `factory_events.rs`:
  timing and bounded event observation.
- `crates/rk-core/src/sdlc.rs`, daemon ingestion/reactor, `ingest_cmds.rs`:
  authenticated external signal semantics.
- `crates/rk-cli/src/main.rs` daemon_rollover and `scripts/install.sh`:
  current local activation boundary.
- `docs/factory-foreman.md`, `docs/repository-policy.md`,
  `docs/2026-09-12-stigmergy-evidence-and-trial.md`: existing authority and evidence.


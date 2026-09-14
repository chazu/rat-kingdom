# Continuous validation, promotion, and recovery

Status: execution authorized; amended 2026-09-14 to require independently
shippable vertical slices. The earlier two-round approval covers content
`66aa837e47f2a06bd4fa9143b74913655ad714ae`, not this amendment; see the review record.
Date: 2026-09-13. Initial source audit: `3cfabbe90337b8d7214fe4d5791a16dd7ce44b03`.

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

### Independently shippable vertical slices

**Deliver useful behavior in thin vertical slices, and ship each accepted slice
promptly.** This is a design pillar alongside continuous feedback, controlled
exposure, and bounded use of host resources. Each slice must work with the system
already deployed and remain useful indefinitely if the next slice never ships.
A vertical slice includes the interface, implementation, persistence, evidence,
and operational path its behavior actually needs. Completing a layer of a future
subsystem is not, by itself, a shipped feature.

Separate prerequisites for correctness from coordination and later optimization.
A hard dependency names a specific delivered capability and the invariant that
would fail without it. Sharing a module, wanting a cleaner interface, or planning
a faster implementation does not make an entire workstream a prerequisite.
Prefer an existing compatible path, a bounded manual operation, or a narrow
adapter until the improved mechanism is available. In particular, check
coalescing must not hold unrelated resource limits, release inventory, feature
configuration, or read-only metrics behind its completion.

Every implementation ticket must state:

- The immediately useful behavior and a real CLI/RPC/operator journey proving it.
- The currently deployed capabilities it uses, and each strictly necessary
  dependency with its reason; keep coordination needs and future enhancements
  separate from dispatch blockers.
- Bounded validation, including the relevant failure path, expected check time
  and compute cost, and the source/policy evidence required to ship this slice.
- How it is deployed or enabled, its safe default, and its disable, rollback, or
  explicit forward-recovery path. State which settings require restart.
- The contract and evidence it publishes on the BBS, and what remains deferred.

Choose scope that can reach a useful delivery within a bounded worker attempt;
estimate from observed implementation and check costs. If the scope grows,
extract independently acceptable behavior and retain the remaining obligations
as follow-ups. There is no arbitrary minute or line target that overrides needed
checks. Reducing scope and eliminating redundant waits are the ways to ship
sooner; weakening correctness, authority, or recovery guarantees is not.

Integration, installation, enablement, and demonstrated benefit remain distinct.
Once a slice passes its own acceptance and activated gates, proceed through its
authorized deployment path without waiting for the whole subsystem, a program
audit, or enough samples to claim long-term benefit. A default-off feature can
ship when its opt-in behavior and disable path are complete for its declared
scope. A flag is not sufficient isolation for changes outside the guarded path,
and incomplete functionality remains incomplete even if its source is integrated.

When recovering an oversized branch, extract a coherent fix onto the current
delivery base and validate that exact source independently. Do not declare the
whole branch accepted because one subset passed. Retain unresolved findings,
source and cost history, and aggregate scope checks for any later release.

### Required outcomes

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

### 4.1 Independent check reuse and review scheduling improvements

Deliver exact-context proof corrections independently, then extend reuse to
concurrent requests for the same exact reusable check identity. Today a reviewer
can miss the cache while a gate is running, wait for admission, then execute the
check again. A completed-cache lookup alone does not prevent this. Coalescing is
a separate optimization, not a prerequisite for every scheduling change.
Publish settlement/proof before releasing
execution ownership. Each requesting caller owns a subscription; cancelling one
subscriber must not kill work still required by another. Cancel and reap the
actual child when the last owner leaves. Dirty/unidentified inputs do not share.
Validate actual candidate bytes and complete execution context before joining.
The regression must prove a gate and reviewer execute one identical check once,
including concurrent cancellation and the proof-publication/admission race.

Separately, run a candidate's independent semantic review and named checks
concurrently after cheap source/protected-path/diff-scope admission. Both must
pass before target advancement. A failed gate cancels/settles any now-unneeded review using
its exact attempt, preserving cost and late verdict evidence. Neither result
authorizes advancement by itself. If review rejects first, cancel/settle the
now-unneeded check symmetrically without cancelling another subscribed owner.
Retain existing review reuse constraints.

This overlap slice requires correct candidate/workspace isolation, independent
result ownership, bounded cancellation and existing admission limits. It may use
the current unshared check runner: queued duplicate checks remain visible in its
cost/latency results and must fit the declared budget. It cannot claim eliminated
duplicate execution until the separate coalescing slice is accepted.

Next allow a bounded second candidate's source review and cheap preparation to
proceed while the first awaits review. Default remains one active candidate
until explicit activation; trial cap is two per target. Keep expensive merged
candidate checks at the current FIFO head after capturing its actual target.
Checking B against T while approved A is about to advance T predictably wastes a
full check; this design explicitly excludes that speculation. Release the broad
target lock around waiting work; use short fenced claims and final target CAS.
Do not remove source fences or allow two actors to own one candidate phase.

Source review remains bound to source SHA, task, target and declared review
context. Its reuse asserts only that scope; integration checks cover the final
assembled candidate. Any review claim dependent on a superseded target context
requires a fresh review. A later general merge train is not required for this
program. Independent integration/release selection does not depend on lookahead.
Measure total delivery latency and total check minutes; extra concurrency alone
does not prove improvement.

Workspace ownership is separate from execution admission. Existing gate trees
are keyed by repo/target and reset before admission. A new candidate must not
reset a checkout still read by an earlier check. Hold an exclusive workspace
lease spanning reset through confirmed child cleanup, or use candidate-specific
trees. A barrier test makes A read its source after B starts preparation and
checks that A's proof still describes those exact bytes. Retain bounded cleanup.

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
candidates and included ticket membership. Snapshot selection applies aggregate
scope admission before enqueue: choose the oldest admissible immutable prefix
of integration deliveries within the existing target's file/line budget and
retain overflow for the next snapshot. Do not repeatedly select an over-budget
latest head. If one constituent is inadmissible, retain it with an explicit
disposition; split/correct it or obtain a separately bounded activated release
policy. Other independently selectable work is not silently discarded.

Protected-path authority is edge-specific. A source integration approval is
evidence, not automatic approval of its later release edge. An activated release
policy may explicitly allow exact constituent changes with matching retained
authorization, path and digest bindings; otherwise the protected edge requests
its required decision. Never erase the path guard or infer approval from merge.
Tests include two individually admissible changes whose aggregate is over budget,
overflow progress, and missing/stale protected-path authorization.

A failed snapshot holds its release;
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

The first deployable slice is a static host-wide concurrency ceiling for existing
named checks across two repositories, with cancellation, status and bounded child
work. It uses the current runner and does not require coalescing. Add weighted
classes, stronger fairness and additional recipe types as independently accepted
extensions; basic limits remain useful without them.

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

First ship immutable paired-bundle preparation, inventory and inspection through
the existing bounded build/install workflow. This does not require the new host
scheduler or automatic promotion controller; record the recipe's actual resource
limits and its current enforcement boundary. Add explicit manual activation and
compatible rollback as the next usable slice. It still requires durable intent,
source/config binding, health evidence and interruption recovery for the state
it changes. Automatic post-boot supervision and objective-driven promotion can
follow without holding inventory or the completed manual journey.

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
Before stopping the current daemon, the transition durably records the exact
live/parked generation set, their launch identities and prior dispatch state.
Record each recovery outcome separately. On interruption, reconcile those exact
generations; never adopt unrelated pre-existing or deliberately parked orphans.
Current rollover's local vector is insufficient after its CLI crashes.

The launcher has a filesystem lock, bounded retries, and durable receipts that
the recovered daemon ingests; it does not start a second controller agent. It
also provides bounded post-boot supervision using an activated operational-health
contract and an authenticated local rollback command independent of the candidate
daemon. After boot-health success, use a low-cost external progress/read probe
with an explicit maximum observation interval and recovery budget. A responsive
RPC with broken dispatch/landing progress is not sufficient health. Missing
traffic is not failure: progress checks require known eligible work and account
for declared admission/approval waits. The launcher can stop and replace an
unhealthy candidate under its existing transition policy without its cooperation.

Health includes process liveness, correct release identity, successful bounded
read, and ability to access current state. Read-only health checks cannot prove
all mutation paths; use isolated state for transactional smoke tests. Runtime
regression feedback may disable a feature or request a preauthorized rollback.
Do not run two writable daemons against the production tuplespace.

Rollback changes binaries/config/exposure, not the journal or ticket history.
Declare supported state schema read/write ranges, persisted-record formats,
activated repository-policy versions, and CLI/MCP protocol ranges. Bind effective
configuration including environment overrides, not only config-file bytes.
Validate the rollback bundle against that effective configuration before opening
live state. Surviving clients must be compatible or explicitly reconnect; a
version mismatch warning alone cannot establish compatibility. Prefer additive
compatible changes; refuse automatic binary rollback across incompatible writes.
Record a human gate or a named forward-recovery action in that case. Retain the
current and previous healthy release while pruning only unreferenced artifacts.

Acceptance includes the CLI/MCP pair, existing session recovery, a boot-failing
candidate, a healthy candidate, interrupted activation/partial worker recovery,
stale rollback, incompatible override or surviving MCP client, a read-responsive
but operationally failed successor, and recovery preserving new work.

## 7. Feature exposure and fitness

### 7.1 Feature lifecycle

Start with one validated default-off feature setting, explicit enable/disable,
and the current release/config identity. Use the existing installation process;
new release inventory is not required. Declare restart requirements and test
actual isolation and disable behavior. Shadow execution, stable cohorts and a
general controller are later slices. Unsupported modes are rejected until their
complete behavior ships.

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

Start with bounded read-only reporting of existing native delivery, verification
and cost evidence. Report missing sources and denominators explicitly. This
slice does not wait for feature cohorts or a release controller; add release,
exposure and cohort attribution as those source contracts become available.
Automatic decisions remain separate from read-only observations.

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
| Check reuse | Concurrent exact eligible requests execute once; independent cancellation, immutable bytes and publication ownership hold; differing contexts cannot share proof |
| Landing overlap | Barrier-based test proves gate/review overlap and review B progresses while A waits, with cap 2 and heavy cap 1; no duplicate advancement or stale proof reuse; report any duplicate check cost separately |
| Scheduling improvement | Matched bounded fake-harness workload separates review waits and check execution; new critical path overlaps stages and reports any extra stale work |
| Integration progress | A later independent change integrates while a frozen release candidate is validating; it does not alter that candidate |
| Release recovery | Initial local profile: boot identity/read healthy within 30 s; failed boot recovers previous compatible release within 60 s; one rollback attempt, then explicit degraded state |
| Host budget | Two repo fixtures cannot exceed configured aggregate weight; queued cheap control reads remain available; cancellation frees ownership after exact child cleanup |
| Feedback latency | Native completed events reflected within two configured 30 s evaluator ticks while the daemon remains available; gap is unavailable rather than pass |
| BBS benefit | Verified useful effects per eligible source-consumer opportunity, plus miss rate and assessment coverage; production goal is direction of improvement with uncertainty reported |
| BBS guardrails | Briefing cost/latency and incorrect reuse/rework tracked by feature cohort; no mandatory exchange count or invented time saved |
| Factory throughput | Delivered and deployed root changes per host-hour, p50/p95 lead time, check minutes, rework/rollback rate, model cost, and operator interventions; no ticket splitting to improve score |
| Independent delivery | Track dispatch-to-first-useful-deployment, accepted-to-deployed delay, and blocked time by required capability; record slice-to-root lineage and deferred scope so splitting tickets cannot inflate feature throughput |

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

Publish each shipped slice's available interface, compatibility/default behavior,
source and installed identity where applicable, executable example, evidence,
and limits. Consumers can use that delivered contract immediately instead of
waiting for the provider's entire workstream. Distinguish a proposed contract
from implemented, delivered and installed behavior. Use a Need for a precise
missing capability, not a blanket dependency on every future provider feature.

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

## 11. Independently deployable execution slices

### Public journeys to implement

These are proposed interfaces, not commands available at the initial source:

- `rk release prepare --repo R --candidate SHA --recipe NAME` creates a verified
  immutable bundle through named managed execution; `rk release show ID` and
  `rk release status --repo R --environment E` expose provenance and desired/
  observed state, including pending/unknown transitions.
- `rk release activate ID --environment E --expect-revision N` and
  `rk release rollback --repo R --environment E --expect-revision N` use activated
  authority and exact transition receipts. The independent launcher exposes the
  same narrowly authenticated local rollback route when the daemon is absent.
- `rk feature show FEATURE --repo R`, `rk feature set FEATURE --repo R
  --mode MODE --expect-revision N`, and `rk feature disable FEATURE --repo R
  --expect-revision N` expose assigned/configured/disabled states distinctly.
  Cohort definition and objective references come from validated policy/input.
- Existing `rk factory scorecards`, `rk factory recommend`, native factory
  snapshot/replay/watch and MCP typed reads expose added evidence without
  acquiring mutation side effects. Add explicit assessment reads as needed.

JSON mode must provide versioned structured results and actual transition IDs.
CLI, RPC and MCP use the same daemon authority/resolution contracts. Reusable
activated policy grants are distinct from Foreman's existing exact one-action
human approval grants; retain both and reject an action without either the
required specific grant or applicable preauthorized policy.

### Tracks, first deliveries, and real prerequisites

Routine slice work proceeds within activated policy. Each deployment uses the
applicable existing activation authority; new credentials or irreversible
decisions retain their human gate. Reconcile affected ticket contracts with the
reviewed design before dispatch, and mark only currently authorized ready work.
Decompose scope before it exceeds an attempt or repository budget.

P0-P14 remain stable coverage tracks for the full R1-R9 outcomes; they are not
single indivisible worker tickets or a total shipping order. Decompose each into
vertical delivery tickets using section 1's contract. The entries below identify
first useful deliveries and the specific capabilities needed for later behavior.
The fuller contracts in sections 4-10 remain required for those later slices.

| Track | First independently useful delivery | Later extension and its actual prerequisite |
| --- | --- | --- |
| P0 | Extract an exact command/cwd proof correction with native regression coverage onto the current runner | Cross-route in-flight sharing requires complete effective identity, immutable execution bytes, subscriber ownership/deadlines, and proof publication before release |
| P1 | Overlap one candidate's review and gate under current admission; require both results and correct cancellation | Needs candidate/workspace isolation and truthful phase evidence, not completed P0; coalescing later reduces duplicate work |
| P2 | Permit bounded source review/cheap preparation of a second candidate while preserving FIFO advancement | Needs durable phase claims and workspace isolation; no speculative full checks or dependency on full P0/P1 optimization |
| P3 | Enforce one static host concurrency ceiling for existing named checks across two repositories | Add weights, fairness and resource classes using the delivered admission contract; no P0 prerequisite |
| P4 | Route one named artifact build through host admission with bounded children and visible status | Requires P3's admission interface for this route; migrate experiments and further recipes separately |
| P5 | Expose integration/release roles and select one frozen release candidate while later work integrates through existing gates | Requires immutable selection, aggregate scope/authority admission and usable checks; P1 overlap and P4 routing can follow |
| P6 | Prepare and inspect versioned paired RK/MCP bundles with source/check/config provenance using existing bounded recipes | Add native build routing when P4 is available; inventory does not depend on it |
| P7 | Explicitly activate and roll back a compatible bundle with durable intent, health checks and interrupted-transition recovery | Requires P6's verified bundle contract and applicable lifecycle repairs; independent automatic supervision is a later delivery |
| P8 | Enable/disable one default-off feature through validated config with tested isolation and stated restart behavior | Add shadow/cohort assignment and reconnect persistence; no new release inventory prerequisite |
| P9 | Report available native delivery/check/cost data through existing scorecards with honest coverage | Add release/exposure adapters when their records exist; bounded assessment then supplies P10, not a prerequisite for initial reads |
| P10 | Automatically promote or disable one scoped feature under a declared objective and action policy | Requires that feature's working P8 disable/exposure path and P9 assessments; binary rollback additionally requires P7 |
| P11 | Ship one optional BBS discovery adjustment with the original fallback and useful-reuse evidence | Needs the setting/disable path for this feature; add shadow/cohorts and comparative assessments as their capabilities ship |
| P12 | Exercise explicit prepare/activate/observe/rollback against an independent local service through a narrow adapter | Requires its own artifact, authority and recovery contracts; automatic promotion later uses P10, not RK's local launcher as a blanket dependency |
| P13 | Adopt each accepted integration, release or feature slice in RK through its available operational path | Native release control uses P5/P7 contracts; adopt automatic exposure when P10 is available without holding earlier installations |
| P14 | Maintain an incremental source/delivery/install/benefit audit during ordinary useful work | The final program audit requires every R1-R9 outcome; it never gates an otherwise accepted independent delivery |

Replace the earlier blanket track dependencies with specific capability
dependencies before dispatching affected tickets. A proposed dependency removal
must identify the existing usable path; retain any prerequisite whose absence
would violate correctness, authority, resource bounds, or recovery. Existing
partial P0 work and its unresolved findings remain preserved; this decomposition
does not accept it, reset spent budgets, or grant another corrective attempt.

P1/P2 share landing internals; P3/P4 share managed execution. Coordinate ownership
and publish compatible interfaces on BBS instead of serializing whole tracks
merely because they touch the same module. At most two implementers initially,
assigning disjoint changes where possible. Preserve current
spending, review, rework and verification limits until their replacement policy
is explicitly installed. Parallel review must also respect reviewer admission.

## 12. Validation, review and adoption

For initial plan review, freeze the plan commit and compare against R1-R9 and
current repo contracts in two independent adversarial passes:
requirements/completeness and standards/operational failure modes. Require concrete failure sequences,
especially daemon self-recovery, stale target checks, authority, missing metrics,
resource deadlocks, and feature assignment contamination. Retain findings with
resolved/deferred dispositions. Revise and repeat review until no blocking
finding remains. A deferral may change sequencing; it cannot erase R1-R9.
Later amendments retain their own review provenance; review the affected
contracts and dependencies without restarting acceptance of unrelated work.

For each implementation slice: reproduce the behavior with a finite meaningful
test, implement the complete CLI/RPC/persistence path it needs, run focused
managed checks, get independent native review, and land through activated gates.
Use barriers and fake providers for lifecycle/concurrency failures, not sleeps
and paid model loops. Maintain tested source/policy identity through delivery.
Select evidence for the behavior this slice exposes and the paths it changes.
Full checks required by activated policy still run on their declared edge; avoid
redundant broad runs on unchanged source and unrelated future-feature acceptance.
Measure queue time and validation cost so bottlenecks cause scope or scheduling
improvements, not arbitrary deadlines shorter than the required checks.

Introduce new policy options with old behavior as their compatibility default.
Activate new behavior deliberately per slice after its failure-path acceptance.
Do not wait for every slice before installing useful safe improvements. Until
native release control exists, retain the established operator build/install/
rollover procedure and exact rollback artifacts. Adopt each native release
operation as it becomes available, including its recovery evidence; do not wait
for the rest of P7/P13 or automatic promotion to install earlier useful slices.

Finish existing Pipsqueak/Hazel deliveries and retain their original observation
window/results. Their failures and costs are not reclassified. This program
supersedes the earlier mandatory multi-arm stigmergy replay plan as a prerequisite
to feature promotion; preserve those historical plans and mark unrun arms as
superseded, not passed. Carry useful pending bug tickets forward without
redispatching duplicates or losing prior delivery provenance.

Completion audit maps every R1-R9 requirement and P0-P14 track's delivered slices
to source delivery, check/review evidence, installed identity, and an actual product journey. Missing
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

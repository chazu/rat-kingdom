# mu/pudl integration — feasibility spike (rev 2, post-adversarial-review)

*2026-08-18. Companion to the Phase 2 epic. Rev 1 was subjected to an
adversarial fact-check against the actual mu and pudl code: of 19 factual
claims, 8 were false, overstated, or materially caveated. Rev 2 records the
corrections and **demotes the whole track from an integration plan to a
feasibility spike (M0) with explicit exit numbers**. Nothing in the Phase 2
epic waits on anything here; the epic's merge-queue economics are solved by
batching (epic E2), not by mu.*

## Corrections from the fact-check (the record)

| Rev-1 claim | Reality |
|---|---|
| "concurrent identical checks coalesce" | **False.** CAS dedupes *after the fact*; concurrent identical runs both execute. Coalescing would be new mu work. |
| "pass receipt = CAS manifest at that tree hash", third-party verifiable | **Overstated.** `--emit-manifest` outputs an unsigned build summary keyed to *outputs* — no input/tree hash, no commit; mu's signing layer is design-only. Receipt binding is new work. |
| "toolchain via mu scratch-build" for Rust | **Unproven.** Scratch does download→verify→extract of a *single archive*; Rust's dist is multi-component assembled by install.sh. Feasibility question, not a given. Also "scratch" = pinned prebuilt download, not built-from-source. |
| "hermetic actions retire the environment-drift class" | **Caveated.** Kernel sandboxing (Seatbelt/namespaces) applies only to toolchain actions; non-toolchain actions run bare; silent Copy (honor-system) fallback on platforms without isolation. |
| "until mu grows a declared non-hermetic action class" | **Wrong in mu's favor.** `Impure` and `Network:` action flags exist today; the go plugin already models declared-network fetch feeding a hermetic build — the exact pattern a Rust plugin needs for crates.io. |
| "a retest re-runs only what changed" → max_retest rises | **True at action level, void at rev-1's granularity.** Workspace-coarse actions invalidate wholesale on any change — every landing is a cache miss. The economics require crate-level actions. |
| pudl "existing mu bridge" | **Real but stale and infra-shaped.** Bridge + bitemporal store + drift are implemented and tested, but it exports `mu.json` while current mu loads only `mu.cue`, and the drift machinery is entirely infra-data-oriented. Delivery-facts capability would be built from scratch. |
| M4 "gate results computed anywhere are warm everywhere" | **Dangerous as stated.** An unauthenticated shared cache feeding gate verdicts makes the cache a trust boundary with no provenance story. Shared cache is for *artifacts*; verdicts stay tuple-authoritative, always. |

Verified true and still load-bearing: plugins are babashka emitting NDJSON
action subgraphs (a Go SDK also exists and is mu's newer direction);
action-level incremental caching is implemented; OCI CAS with implemented
remote push/pull; pudl's fact store is genuinely bitemporal.

## M0 — the feasibility spike (gates everything else)

Branch-level work in mu / a scratch repo; no rk integration. Four questions,
each with a measurable exit:

1. **Rust toolchain**: can scratch (or a scratch extension) assemble a
   working pinned rustc+cargo+std from Rust's multi-component dist?
   *Exit: `mu build` compiles a hello-workspace hermetically, or a written
   no-go describing the scratch changes needed.*
2. **Network fetch**: a `cargo fetch` action with `Network: true` producing
   a CARGO_HOME artifact consumed by hermetic build actions (clone the go
   plugin's pattern). *Exit: build succeeds under kernel network denial.*
3. **Granularity**: crate-level action emission for a real workspace
   (rat-kingdom's 11 crates). *Exit: measured warm-cache hit rate on 10
   realistic landing diffs from git history — the number that decides
   whether mu's gate economics beat epic-E2 batching.*
4. **Receipt binding**: extend the manifest (or wrap it) with the input
   tree hash. Unsigned is acceptable for a single-host castle **only
   because the tuple remains the sole verdict authority** — the manifest is
   evidence attached to the tuple, never a verdict source.

*M0 can start any time — it is mu-repo work. Everything below waits for
M0's numbers AND the epic's E2.*

## M1–M4 (contingent, summarized)

- **M1** productionize the Rust plugin (bb or Go SDK — pick in M0).
- **M2** rat-kingdom `mu.cue`. The canonical profile stays ONE list
  (`checks.canonical`); runner is a **per-check attribute** — never two
  half-profiles (review: rev 1's split recreated the four-definitions
  disease E2 cures). e2e tests use mu's existing `Impure`/`Network` action
  class if they fit, else remain `runner: command` checks in the same list.
- **M3** `checks.runner: mu` per check; gate invokes the target on the
  merged tree; **tuple remains the sole receipt authority everywhere**
  (decided here, not deferred). One-line rollback at all times.
- **M4** shared OCI cache with CI — artifacts only; never verdicts. Any
  future verdict-sharing requires the signing layer mu has not built.

## M5 — pudl evaluation (go/no-go memo, unchanged in spirit)

Three questions, answered with M0–M3 operating data, in a one-page memo
before any integration code:

1. One-way projection (pudl consumes rk events; tuplespace stays
   authoritative) is the default — anything more needs a reason that
   survives the TKT-171 test.
2. Is bitemporality load-bearing for a real decision? (Candidate:
   retroactive soak-window invalidation — relevant only when the epic's
   deferred `deployed` bar gains a tenant.)
3. What does pudl drift catch that the reconciler-class sweeps don't?

Plus the mechanical prerequisite the fact-check surfaced: the bridge's
`mu.json` output vs mu's cue-only loader must be reconciled before any
pudl→mu direction is exercised.

## Standing cautions (carried from rev 1, still true)

Layer boundaries here are hypotheses; M0/M3/M5 each force one explicit
decision with data. The slot/arbitration machinery stays in rk (epic E4).
Success for multi-project support is measured the same way: a new repo
onboards with a `mu.cue` and a policy block, zero rk code.

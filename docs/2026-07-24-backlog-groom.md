# Backlog groom — 2026-07-24

Groom pass by Colby-2 over `rk ticket list --status open`. No source code was
touched and no ticket was started; this is a record of what changed in the
backlog and why, so the next groom starts from decisions rather than re-deriving
them.

Counts: **11 decomposed** (3 umbrellas), **1 deduped**, **3 flagged**.

## Deduped

| Closed | Survivor | Basis |
| --- | --- | --- |
| TKT-137 `rework: TKT-113` | **TKT-133** (done) | Already verified landed |

TKT-137 was the fourth rework ticket filed against TKT-113 (after TKT-133 done,
TKT-134 closed, TKT-135 closed). A steward artifact filed under `task: TKT-137`
had already investigated it and recommended `APPROVE` with the explicit action
"TKT-137 should close as a duplicate of TKT-133" — but the ticket was never
closed, and its assignee `Ratatosk` (grmpl) is dismissed.

Both of the blockers TKT-137 was filed for are in `main` and were re-verified
individually by that steward: `rand_update` now draws `diff` from `-3..=3`
including 0 (`law_oracle.rs:327`), so `signum()` is no longer the identity; and
the vacuous `window.clone()` determinism assert is replaced by
`realloc_window()` (`law_oracle.rs:440`) with an `!Arc::ptr_eq` guard.
`DeltaInput`/`reify_delta` are present at `crates/grmpl-pattern/src/lib.rs:174,197`.

Reworking it would have manufactured a loop over work already landed and green.

## Decomposed

### TKT-86 — P14: Diff generalization (Size L) → 3 subs

All three stay gated with the parent.

- **TKT-149 P14a** — `TraceStore` generic over the diff type *or* a parallel
  engine layer. This fork decides whether P14 is affordable at all and shapes
  both siblings, so it goes first; settle it by prototyping one use site both
  ways, not by argument.
- **TKT-150 P14b** — positivity structure (`is_positive`). The semantic core:
  a general abelian group has no notion of "present", so without this
  `Distinct`/preconditions/DRed stay pinned to a count component and P14
  delivers much less than it appears to.
- **TKT-151 P14c** — record format bump under P0 policy, landed together with
  the two concrete group instances, because the acceptance test is what proves
  the format is right. The parent is emphatic that acceptance is *the two
  concrete instances*, not "any instance".

### TKT-87 — P15: Distribution (XL+) → 6 subs

The point of this decomposition: **the parent was not uniformly gated**, and
four separable items were stuck behind a gate that does not actually apply to
them.

Startable now:

- **TKT-152 P15a** — cross-domain read substrate. The parent's own "biggest
  item", but it is a local refactor: `Snapshot` holds exactly one
  `&dyn TraceStore` and *no* mechanism exists for a multi-domain read. Landing
  this alone drops the umbrella's honest size by a tier.
- **TKT-155 P15d** — iroh unpin. Two unrelated halves sharing a dependency: a
  cheap recurring retest of the ed25519-dalek 0.95 conflict (startable today,
  and the only P15 item that *decays* if ignored), plus gossip topic mapping,
  which is a design problem the unpin does not solve.
- **TKT-156 P15e** — content-addressed editions. Pays for itself via P10 cheap
  forks without any distribution existing.
- **TKT-157 P15f** — partition-injection test harness. A test harness, so it is
  best built *before* the thing it tests; the parent already calls it a
  multi-week deliverable in its own right.

Gated with the parent:

- **TKT-153 P15b** — frontier editions. Not additive: a rework of the delta
  calculus, since the `Edition` total order is load-bearing in `eval_delta`,
  `scan_updates`, `DeltaStream` and `commit_if`. Each is its own proof
  obligation.
- **TKT-154 P15c** — placement + CALM enforcement. Cheapest of the subs once
  triggered, because P8b (TKT-99) and P8c (TKT-100) both already landed — this
  is "make the existing checker binding on placement", not "build a checker".

Sequencing hazard recorded on both: TKT-153 and TKT-156 each change what an
`Edition` *is*. Wrong order means doing the second one twice.

### TKT-148 — duplicated agent-name generations → 2 subs

Split because the parent bundled data hygiene with a latent-defect hunt —
different risk, different verification, and the hunt should not wait on a
naming-policy decision.

- **TKT-158** — the hygiene half, with a measured scope correction (below).
- **TKT-159** — sweep for a fourth unbounded blocking read keyed on an agent
  name over a durable category. TKT-146 established this as a live bug class
  and fixed three sites; the parent flags that a fourth may exist.

Two findings measured during the groom rather than inherited:

- **Scope was understated ~6x.** TKT-148 says four names (Gouda, Brie, Scamper,
  Cheddar) have two generations. Against the live registry and archive
  (`rk list --all`) the real figure is **24 names**, two generations each, over
  267 records / 243 distinct names. That materially weakens "leave it, it's only
  four names" as the cheap option, since each is an ambiguous key for `rk log`,
  `rk status` and tuple payload searches.
- **The parent's open question is answered: `AgentLog` never became
  generation-aware.** `crates/rk-daemon/src/agent_log.rs:139` still keys the
  file on the name alone. TKT-138 was closed because TKT-146 removed the *cause*
  (name recycling), not because the keying was fixed — so the 24 pre-existing
  pairs still interleave two unrelated rats' transcripts.

## Flagged

**TKT-85 — do not close.** All its direct work now lives in TKT-101 (TKT-98 is
done), which normally reads as "close the umbrella". But TKT-86 and TKT-87's
gated subs declare `depends-on TKT-85`, so closing it would flip a deliberately
gated tier to ready and invite a start on work the roadmap says must wait for a
real trigger. It is load-bearing as a **gate**, not as a work item. A note to
that effect is now in the ticket body.

**TKT-125/139/144/145 — a cluster, deliberately not merged.** Four distinct call
sites that all bottom out in one missing primitive: `TraceStore` has no point
lookup and no index. They are *not* duplicates — each has its own independent
fix, and two of them (TKT-139 via arrangement reuse, TKT-144 via evicting
untouched anchor weights) have cheaper local fixes that do not need the
primitive at all. So building the index is not a prerequisite for closing them.
All three others already name TKT-125 in their bodies; TKT-125 now carries the
forward link, so whoever does build a point lookup checks the other three before
optimising them separately.

**TKT-141 — do not decompose.** The seed-mixer fix spans 12 files, which looks
splittable, but the ticket explicitly requires one sweep so the idiom stays
uniform. Left whole. Its fixed mixer is now referenced from the two new tickets
that will write fresh law oracles (TKT-151, TKT-157) so they do not reintroduce
the `seed ^ K | 1` collision at birth.

# rk-sync cross-castle audit: the name-collision question is moot today, for a worse reason

**Ticket:** TKT-01M08MWKW6FDSGCE26N2AYZB3S (sub-ticket of the C1/S3a generation-identity
program, TKT-01M08MWC1AM8SMSR4SQDXVKYRW)
**Author:** Widget-8, 2026-08-19
**Status:** audit complete, one follow-up bug ticket filed

---

## 1. The question

`docs/2026-08-17-tkt-c1-generation-identity.md` §5 flags, but explicitly does not
verify:

> Two castles independently minting "Basil-7" is a real and separate shape.
> `SpawnId` happens to fix it (ULID randomness makes independent mints disjoint),
> but no consumer in §3 is a replication path, so it is untested by this design.

The ask: audit `rk-sync` — which replicated records carry an agent name in
`Tuple.instance`, and can that value actually collide across two castles that
independently mint the same rat name.

## 2. What `Tuple.instance` carries for an agent-authored write

`crates/rk-daemon/src/server.rs` `handle_out` (introduced in `de689fe` "security:
authenticate and scope daemon clients", 2026-07-26):

```rust
params.instance.unwrap_or_else(|| {
    if is_agent {
        caller.clone()      // the RK_AGENT name, e.g. "Basil-7"
    } else {
        self.castle.clone() // the crypto castle id, e.g. "castle-878576ce..."
    }
})
```

Before that commit, every tuple defaulted `instance` to `self.castle` regardless
of caller. The CLI sugar layer (`crates/rk-cli/src/space_cmds.rs`) additionally
passes `instance: agent` explicitly for `claim`/`report` (obstacle/need)/`suggest`/
`endorse`, for the same reason (per-agent trail reinforcement, distinct-endorser
counting). So today, **any durable tuple an agent authors — `rk out artifact`,
`rk suggest`, an in-rat `rk endorse`, the `task_done` event — carries the agent's
name in `instance`**, not the castle id. This is exactly the shape the ticket
asked about: the field that IS the replication actor key, keyed on a value drawn
from the same finite, recyclable, cross-castle-uncoordinated name generator that
motivated the whole C1 program.

## 3. But it never reaches the replication log

`crates/rk-daemon/src/sync.rs` `Syncer::run_cycle`, step (1):

```rust
let ours: Vec<&Tuple> = delta
    .tuples
    .iter()
    .filter(|t| {
        t.lifecycle != Lifecycle::Ephemeral
            && t.instance == self.castle   // <-- self.castle is ALWAYS "castle-<hex>"
            && live_ids.contains(&t.id)
    })
    .collect();
```

`self.castle` is the daemon's crypto actor id — never an agent name (pinned by
`display_alias_tests::alias_is_the_display_but_never_the_wire_id`, which asserts
`daemon.castle.starts_with("castle-")`). So for any tuple whose `instance` is an
agent name, `t.instance == self.castle` is false, unconditionally. **Step (1)
never exports an agent-authored tuple.**

Confirmed empirically (scratch probe, not committed — reproducible by adding a
`handle_out` call with `caller: "Whisker"` writing a `Category::Artifact` tuple,
then calling `syncer.run_cycle`):

```
PROBE tuple.instance="Whisker" daemon.castle="castle-878576ce922d81b9" lifecycle=Session
PROBE exported=0 (want 1 if it replicates)
```

An agent-authored `Artifact` — Session lifecycle, not ephemeral, exactly the
shape step (1) is supposed to pick up — is silently dropped. The same applies to
`Suggestion`, an in-rat `Endorsement`, and `task_done` `Event`: every category an
agent can durably author.

## 4. Why the existing live-validation harness never caught this

`scripts/rk-sync-live-validation.sh` (TKT-71, the harness that closed P6's
"multi-machine validation" item) drives every write as `a out fact ...` /
`b out claim ...` with **no `RK_AGENT` set**. Unset `RK_AGENT` means
`req.caller` defaults to `"operator"` (`proto.rs::default_caller`), so
`is_agent` is false and `instance` takes the `self.castle` branch — the one
path that was already correct before `de689fe`. The harness's "5 scenarios / 3
stable runs" never exercised an agent-authored write at all, so it could not
have caught this regression.

Worth noting: `de689fe` added a doc comment on `Request.caller` in the same
commit that introduced the bug — "This is authorization context, not a tuple
payload and never a sync identity" — which states the invariant the `handle_out`
change in that same commit violates.

## 5. Answering the original question

**Today: no.** Agent-authored durable tuples never leave their home castle, so
`Tuple.instance` values drawn from the recyclable name generator never enter the
union at all. There is no live path by which two castles' "Basil-7" tuples can
collide in the replicated log, because neither one is ever replicated.

**This is not a fix, it's a bigger hole.** The entire point of `Suggestion`/
`Endorsement` going durable (TKT-168, "ballots are ledgers") was so
convention-quorum works across castles. It currently doesn't: an in-rat
`rk suggest`/`rk endorse` (the normal case — the fleet convention text tells
every rat to run these) is invisible to every peer castle. Artifacts handed off
via `rk out artifact <repo> <name> --payload ...` — the fleet's own mandated
"hand off durable findings" mechanism — are equally invisible cross-castle.
Filed as **TKT-01M0BPX3MRJG81YPGFP6CH9RY2** (see below); it is a correctness bug
independent of SpawnId and should probably be fixed before or independent of the
C1 migration, since C1 does nothing to address it (SpawnId lives in the payload,
not `instance`, and the export filter doesn't look at the payload).

**If/when that bug is fixed, the original collision question becomes live**, and
the C1 design's answer holds: do not repurpose `Tuple.instance` as a
collision-proof key (it must keep replicating a human-readable actor label for
display and for the existing `instance`-keyed writers above); instead lean on
`payload.spawn` (`SpawnId`, ULID-random, minted once per generation) for any
predicate that needs to be namesake-proof. Once producers stamp `spawn`
(parent ticket §4 step 2 — `task_done`, `harness_result`, `claim_completion`
already planned), `Pattern::for_spawn`-style reads are safe against a
cross-castle namesake for exactly the same structural reason they're already
safe against a same-castle namesake predecessor: the key space is independent
per mint, not per name. No `rk-sync`-specific change is needed beyond what C1
already plans, once the export-filter bug is fixed.

One residual gap even after both fixes land: `arbitrate_claims`
(`crates/rk-sync/src/lib.rs`) breaks ties on `(RecordId, instance)` — an agent
name — when two claims share `(scope, identity)` and, im­probably, the exact same
`RecordId` (ULIDs collide only on out-of-order clocks at nanosecond granularity
across two writers, i.e. never in practice). This is a theoretical, not
practical, residual: flagged for completeness, not worth its own ticket.

## 6. Seams touched by this audit (read-only)

- `crates/rk-sync/src/lib.rs` — `NotesSync`, `SyncRecord`, `arbitrate_claims`.
- `crates/rk-daemon/src/sync.rs` — `Syncer::run_cycle` export/import/compact.
- `crates/rk-daemon/src/server.rs` — `handle_out`'s `is_agent` branch (`de689fe`).
- `crates/rk-cli/src/space_cmds.rs` — `out`/`report`/`claim`/`suggest`/`endorse`
  instance stamping.
- `scripts/rk-sync-live-validation.sh` — confirmed it never sets `RK_AGENT`.

No code changed by this ticket. The export-filter bug is real production impact
and is handed off as TKT-01M0BPX3MRJG81YPGFP6CH9RY2 per
`preexisting-failure-is-a-ticket-not-an-inline-fix` — it predates this branch,
is unrelated to the C1 migration this sub-ticket audits, and touching
`handle_out`/`sync.rs` is out of this ticket's scope.

# TKT-01M0H4YVC8M3Q2YZ2734QTCGSP: cross-harness model leakage in agent profile resolution

## Symptom

Scheduled workflow `wf-rh5438sj2t` (nightly-self-improve) spawned Warbeak-10
as `harness=codex`, `model=opus`. Codex rejected every dispatch attempt:

```
HTTP 400: the opus model is not supported when using Codex with a ChatGPT account.
```

The workflow declares `agents: {default: {harness: "codex"}}` — no `model`.
The castle's global config carries `[agents.default] harness = "claude"
model = "opus"`. Bounded self-healing respawned the step three times, all
identically rejected, then the instance failed durably and Warbeak-10 was
dismissed — the retry/escalation path itself worked as designed; only the
resolved (harness, model) pair going into the spawn was wrong.

## Root cause

`rk_workflow::resolve::resolve_fields` (`crates/rk-workflow/src/resolve.rs`)
merges a stack of `AgentProfile` layers (global default → workflow default →
named profile → tier) plus the inline step override, most-specific wins,
**field-wise**: each layer independently overwrote `harness` if it set one,
and independently overwrote `model` if it set one, with no relationship
between the two decisions.

That is correct when a more-specific layer leaves harness alone and only
narrows the model (the common case — `workflow_default_beats_global_default_profile`
already covered it). It is wrong when a more-specific layer changes the
*harness* but says nothing about `model`: the field-wise merge kept
whatever model the *previous* harness had accumulated, because nothing
tied `model`'s validity to the harness it was chosen alongside. Global
`claude`+`opus` → workflow `codex` (model unset) resolved to `codex`+`opus`,
a model/harness pair that was never selected together and that the new
provider does not accept.

The same shape existed one layer up too: an inline step-level `harness:`
override with no inline `model:` would just as silently inherit whatever
model the layers underneath had resolved for a different harness.

## Fix

`crates/rk-workflow/src/resolve.rs`: layers (including the inline override)
now fold through a shared `apply_layer` helper. Whenever a layer's `harness`
differs from the harness accumulated so far — including the first layer
that sets one at all — any previously accumulated `model` is dropped before
that layer's own `model` (if any) is applied. A layer that leaves `harness`
unset cannot change harnesses, so it still merges `model`/`permission_mode`
independently, exactly as before. `permission_mode` is untouched by this
change; the failure evidence and the acceptance criteria both scope the
provider-safety invariant to `model` only.

Net effect: a harness change with no accompanying model resolves to
`model: None`, which downstream (`workflow_exec.rs`, `drain.rs`,
`server.rs` — all three callers pass `resolved.model` straight into
`SpawnParams::model`) means "let this harness pick its own default,"
not "inherit a stranger's."

## Compatibility

This is a behavior change for exactly one shape: a profile/inline
selection that changes `harness` while leaving `model` unset, sitting on
top of a lower layer that *did* name a model for the old harness. Before
this fix that config resolved to `(new_harness, old_model)`; after, it
resolves to `(new_harness, None)`. Every other combination — same-harness
merges, a layer naming harness+model together, inline overrides that name
both, or a layer naming model only — is bit-for-bit unchanged; see the
`same_harness_across_layers_still_merges_model_independently` and
`layer_naming_both_harness_and_model_together_is_unaffected` unit tests in
`resolve.rs`. No named-profile, tier, or inline precedence ordering moved.

Any operator config that happened to rely on the old leak (naming a model
only at a lower layer, expecting it to survive an unrelated harness switch
at a higher layer) should instead name that model explicitly at the layer
that also names the harness.

## Regressions added

- `crates/rk-workflow/src/resolve.rs` unit tests: leak reproduced and fixed
  at the named-profile layer and the inline-override layer, in both harness
  directions (not hardcoded to one provider pair), plus sanity checks that
  compatible cases (same harness, or harness+model named together) are
  unaffected.
- `crates/rk-workflow/tests/examples.rs`:
  `nightly_self_improve_workflow_default_cannot_resolve_to_codex_plus_opus`
  resolves every spawn step in the actual shipped
  `examples/workflows/nightly-self-improve.cue` against a synthetic
  `claude`+`opus` global default and asserts none of them can land on
  `opus` while running `codex`. Verified this test fails against the
  pre-fix resolver (reproduces the live incident) and passes against the
  fix.

## Scope note: workflow-wait wake path

The acceptance criteria for this ticket also asked whether an agent that
fails before declaring done reliably wakes the workflow's wait path. Per
live evidence surfaced mid-task: `wf-rh5438sj2t` DID fail durably at
03:25:31Z once the bounded three self-healing respawns were exhausted, and
Warbeak-10 was dismissed — the wait/escalation path produced a durable
failed outcome as designed. No distinct bug was found there, so no change
or child ticket was filed for it; this fix is scoped to resolution
semantics only.

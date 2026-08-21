//! Agent resolution: which harness/model/permissions run a spawn step.
//!
//! Field-wise, most specific wins:
//! 1. inline step overrides (`harness:`/`model:`/`permission_mode:`)
//! 2. the tier profile a routing rule selected from the ticket's labels/priority
//! 3. the step's named profile in the workflow's `agents:`
//! 4. the same-named profile in global config `[agents.<name>]`
//! 5. the workflow's `agents.default` profile
//! 6. global `[agents.default]`
//! 7. global `[harness] default` for the harness kind
//!
//! A step (or tier rule) naming a profile that exists nowhere is an error
//! (silent fallback would mask typos). The tier layer sits just below inline
//! overrides so cost-routing beats the static profile defaults, yet an explicit
//! `model:`/`harness:` on the step still wins.
//!
//! Provider-safe: a `model` is only ever provider-specific to the `harness` it
//! was selected alongside. When a more-specific layer changes the harness
//! without also naming a model, any model carried up from a less-specific
//! layer is dropped rather than inherited — it was chosen for a different
//! provider and is not guaranteed to mean anything to the new one. A model
//! named at the same layer that changes the harness, or at any layer that
//! does not change it, still applies normally.

use crate::{AgentProfile, SpawnStep, TierRouting};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedAgent {
    pub harness: String,
    pub model: Option<String>,
    pub permission_mode: Option<String>,
}

/// Resolve one `spawn` step, including cost-tier routing over the step's own
/// `priority`/`labels` predicate (`tiers.route`) — the same table `for_each`
/// fan-out consults over a fanned ticket's fields. Before this, `spawn` steps
/// (which is how every reviewer dispatches — `for_each` is worker-only) never
/// consulted `tiers` at all.
pub fn resolve(
    step: &SpawnStep,
    tiers: &TierRouting,
    workflow_agents: &HashMap<String, AgentProfile>,
    global_agents: &HashMap<String, AgentProfile>,
    global_default_harness: &str,
) -> rk_core::Result<ResolvedAgent> {
    let tier = tiers.route(&step.labels, step.priority.as_deref());
    resolve_fields(
        step.agent.as_deref(),
        tier,
        step.harness.as_deref(),
        step.model.as_deref(),
        step.permission_mode.as_deref(),
        workflow_agents,
        global_agents,
        global_default_harness,
    )
}

/// Resolution over the raw agent-selection fields, shared by [`resolve`] (for
/// spawn steps) and the fan-out step, which carries the same fields but is not
/// a [`SpawnStep`]. See the module docs for the layering rules.
#[allow(clippy::too_many_arguments)]
pub fn resolve_fields(
    agent: Option<&str>,
    tier: Option<&str>,
    step_harness: Option<&str>,
    step_model: Option<&str>,
    step_permission_mode: Option<&str>,
    workflow_agents: &HashMap<String, AgentProfile>,
    global_agents: &HashMap<String, AgentProfile>,
    global_default_harness: &str,
) -> rk_core::Result<ResolvedAgent> {
    // Layered profiles, least specific first.
    let mut layers: Vec<&AgentProfile> = Vec::new();
    if let Some(p) = global_agents.get("default") {
        layers.push(p);
    }
    if let Some(p) = workflow_agents.get("default") {
        layers.push(p);
    }
    // The named-profile layer, then the tier layer above it: a routing rule's
    // tier overrides the step's static profile, but inline overrides still win.
    for (kind, name) in [("agent profile", agent), ("tier profile", tier)] {
        let Some(name) = name else { continue };
        let global_named = global_agents.get(name);
        let workflow_named = workflow_agents.get(name);
        if global_named.is_none() && workflow_named.is_none() {
            return Err(rk_core::Error::other(format!(
                "unknown {kind} '{name}' (not in workflow agents nor global [agents])"
            )));
        }
        if let Some(p) = global_named {
            layers.push(p);
        }
        if let Some(p) = workflow_named {
            layers.push(p);
        }
    }

    // Seed the accumulator with the global default harness (module doc layer
    // 7, the least specific of all) rather than `None`. Otherwise the first
    // layer that names a harness always looks like a change relative to
    // "nothing decided yet" — even when it merely spells out the harness a
    // model-only lower layer was already implicitly running under — and a
    // compatible model gets dropped for no provider-safety reason.
    let mut harness: Option<String> = Some(global_default_harness.to_string());
    let mut model: Option<String> = None;
    let mut permission_mode: Option<String> = None;
    for layer in layers {
        apply_layer(
            &mut harness,
            &mut model,
            &mut permission_mode,
            layer.harness.as_deref(),
            layer.model.as_deref(),
            layer.permission_mode.as_deref(),
        );
    }
    // Inline step overrides beat everything — folded through the same
    // provider-safe merge so an inline harness change without an inline model
    // drops a model inherited from a layer, rather than leaking it.
    apply_layer(
        &mut harness,
        &mut model,
        &mut permission_mode,
        step_harness,
        step_model,
        step_permission_mode,
    );

    Ok(ResolvedAgent {
        harness: harness.unwrap_or_else(|| global_default_harness.to_string()),
        model,
        permission_mode,
    })
}

/// Fold one layer's fields into the accumulated resolution, least-specific
/// layer applied first (the caller seeds `harness` with the global default
/// harness before the first call, so "accumulated so far" always reflects
/// the harness that is actually in effect, not merely "some layer already
/// named one"). `model` is treated as scoped to the `harness` it travels
/// with: if this layer sets a harness that differs from the harness in
/// effect, any previously accumulated model is dropped — it belongs to the
/// harness being replaced — and only this layer's own `model` (if any)
/// survives. If this layer names the SAME harness already in effect
/// (whether that came from an earlier explicit layer or is still just the
/// seeded global default), that is not a boundary crossing, so a model
/// accumulated under it is preserved and merges independently, same as a
/// layer that leaves `harness` unset entirely.
fn apply_layer(
    harness: &mut Option<String>,
    model: &mut Option<String>,
    permission_mode: &mut Option<String>,
    layer_harness: Option<&str>,
    layer_model: Option<&str>,
    layer_permission_mode: Option<&str>,
) {
    if let Some(h) = layer_harness {
        // Reset unconditionally on a harness change (even to None, dropping
        // an incompatible inherited model); on the same harness, only
        // overwrite when this layer actually names a model.
        let changing_harness = harness.as_deref() != Some(h);
        if changing_harness || layer_model.is_some() {
            *model = layer_model.map(String::from);
        }
        *harness = Some(h.to_string());
    } else if layer_model.is_some() {
        *model = layer_model.map(String::from);
    }
    if layer_permission_mode.is_some() {
        *permission_mode = layer_permission_mode.map(String::from);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TaskDef;

    fn step(agent: Option<&str>) -> SpawnStep {
        SpawnStep {
            role: "rat".into(),
            coordination: None,
            agent: agent.map(String::from),
            harness: None,
            model: None,
            permission_mode: None,
            task: TaskDef {
                title: "t".into(),
                description: None,
            },
            branch: None,
            review: None,
            priority: None,
            labels: Vec::new(),
        }
    }

    fn profile(harness: Option<&str>, model: Option<&str>) -> AgentProfile {
        AgentProfile {
            harness: harness.map(String::from),
            model: model.map(String::from),
            permission_mode: None,
        }
    }

    #[test]
    fn falls_back_to_global_default_harness() {
        let resolved = step(None)
            .pipe_resolve(&HashMap::new(), &HashMap::new(), "claude")
            .unwrap();
        assert_eq!(resolved.harness, "claude");
        assert_eq!(resolved.model, None);
    }

    #[test]
    fn workflow_default_beats_global_default_profile() {
        let global = HashMap::from([("default".into(), profile(Some("codex"), Some("gpt-5.5")))]);
        let wf = HashMap::from([("default".into(), profile(None, Some("sonnet")))]);
        let resolved = step(None).pipe_resolve(&wf, &global, "fake").unwrap();
        // harness from global default profile, model overridden by workflow.
        assert_eq!(resolved.harness, "codex");
        assert_eq!(resolved.model.as_deref(), Some("sonnet"));
    }

    #[test]
    fn workflow_harness_only_default_does_not_inherit_global_default_model() {
        // The nightly-self-improve regression: global [agents.default] pins a
        // model to the claude harness, and the workflow's own `agents.default`
        // only overrides the harness to codex. The codex-bound spawn must NOT
        // inherit "opus" — that model was never selected for codex, it just
        // happened to be the model attached to the harness this replaced.
        let global = HashMap::from([("default".into(), profile(Some("claude"), Some("opus")))]);
        let wf = HashMap::from([("default".into(), profile(Some("codex"), None))]);
        let resolved = step(None).pipe_resolve(&wf, &global, "fake").unwrap();
        assert_eq!(resolved.harness, "codex");
        assert_eq!(
            resolved.model, None,
            "changing harness with no model specified must not leak the prior \
             harness's model — let codex choose its own default"
        );
    }

    #[test]
    fn workflow_harness_only_default_is_provider_neutral_the_other_direction() {
        // Same shape, harnesses swapped, to prove the fix isn't hardcoded to any
        // particular provider pair: switching claude -> codex or codex -> claude
        // must equally drop the inherited model.
        let global = HashMap::from([("default".into(), profile(Some("codex"), Some("gpt-5.5")))]);
        let wf = HashMap::from([("default".into(), profile(Some("claude"), None))]);
        let resolved = step(None).pipe_resolve(&wf, &global, "fake").unwrap();
        assert_eq!(resolved.harness, "claude");
        assert_eq!(resolved.model, None);
    }

    #[test]
    fn layer_naming_both_harness_and_model_together_is_unaffected() {
        // A layer that changes harness AND explicitly names a compatible model
        // still resolves that model — only an *unspecified* model is dropped.
        let global = HashMap::from([("default".into(), profile(Some("claude"), Some("opus")))]);
        let wf = HashMap::from([(
            "default".into(),
            profile(Some("codex"), Some("gpt-5-codex")),
        )]);
        let resolved = step(None).pipe_resolve(&wf, &global, "fake").unwrap();
        assert_eq!(resolved.harness, "codex");
        assert_eq!(resolved.model.as_deref(), Some("gpt-5-codex"));
    }

    #[test]
    fn explicit_harness_matching_the_global_default_harness_preserves_a_model_only_layer() {
        // Not a provider boundary: global_default_harness is "claude", the
        // global `default` profile only names a model (no harness — it just
        // rides the fallback), and a more-specific workflow layer spells out
        // harness: "claude" explicitly — the SAME harness already in effect.
        // Restating the already-effective harness must not read as a change
        // and must not drop the model that was chosen for it.
        let global = HashMap::from([("default".into(), profile(None, Some("opus")))]);
        let wf = HashMap::from([("default".into(), profile(Some("claude"), None))]);
        let resolved = step(None).pipe_resolve(&wf, &global, "claude").unwrap();
        assert_eq!(resolved.harness, "claude");
        assert_eq!(
            resolved.model.as_deref(),
            Some("opus"),
            "restating the fallback harness explicitly must not clear a model \
             chosen for that same harness"
        );

        // Sanity check the other side of the same seam: if the more-specific
        // layer instead names a genuinely different harness, the model still
        // must not leak (this is the codex/opus case from the other tests,
        // just reached via a model-only global default instead of an
        // explicit-harness one).
        let wf_switch = HashMap::from([("default".into(), profile(Some("codex"), None))]);
        let resolved_switch = step(None)
            .pipe_resolve(&wf_switch, &global, "claude")
            .unwrap();
        assert_eq!(resolved_switch.harness, "codex");
        assert_eq!(resolved_switch.model, None);
    }

    #[test]
    fn inline_harness_override_without_inline_model_drops_the_inherited_model() {
        // Same leak, but at the inline step-override layer instead of a named
        // profile: `harness:` alone on the step must not carry a model chosen
        // for a different harness by the layers underneath it.
        let global = HashMap::from([("default".into(), profile(Some("claude"), Some("opus")))]);
        let mut s = step(None);
        s.harness = Some("codex".into());
        let resolved = s.pipe_resolve(&HashMap::new(), &global, "fake").unwrap();
        assert_eq!(resolved.harness, "codex");
        assert_eq!(resolved.model, None);
    }

    #[test]
    fn inline_harness_and_model_together_still_win_over_layers() {
        let global = HashMap::from([("default".into(), profile(Some("claude"), Some("opus")))]);
        let mut s = step(None);
        s.harness = Some("codex".into());
        s.model = Some("gpt-5-codex".into());
        let resolved = s.pipe_resolve(&HashMap::new(), &global, "fake").unwrap();
        assert_eq!(resolved.harness, "codex");
        assert_eq!(resolved.model.as_deref(), Some("gpt-5-codex"));
    }

    #[test]
    fn same_harness_across_layers_still_merges_model_independently() {
        // Sanity check that the fix only fires on an actual harness *change* —
        // when consecutive layers agree on harness, field-wise independent
        // merging (the pre-existing, correct behavior) is untouched.
        let global = HashMap::from([("default".into(), profile(Some("claude"), Some("opus")))]);
        let wf = HashMap::from([("default".into(), profile(Some("claude"), Some("sonnet")))]);
        let resolved = step(None).pipe_resolve(&wf, &global, "fake").unwrap();
        assert_eq!(resolved.harness, "claude");
        assert_eq!(resolved.model.as_deref(), Some("sonnet"));
    }

    #[test]
    fn named_profile_merges_over_defaults_and_inline_wins() {
        let global = HashMap::from([
            ("default".into(), profile(Some("claude"), Some("sonnet"))),
            (
                "cheap".into(),
                profile(Some("codex"), Some("gpt-5.5-codex")),
            ),
        ]);
        let mut s = step(Some("cheap"));
        s.model = Some("o4-mini".into());
        let resolved = s.pipe_resolve(&HashMap::new(), &global, "fake").unwrap();
        assert_eq!(resolved.harness, "codex");
        assert_eq!(
            resolved.model.as_deref(),
            Some("o4-mini"),
            "inline beats profile"
        );
    }

    #[test]
    fn workflow_named_profile_beats_global_named_profile() {
        let global = HashMap::from([("fast".into(), profile(Some("codex"), Some("gpt-5.5")))]);
        let wf = HashMap::from([("fast".into(), profile(None, Some("haiku")))]);
        let resolved = step(Some("fast"))
            .pipe_resolve(&wf, &global, "claude")
            .unwrap();
        assert_eq!(resolved.harness, "codex", "harness from global layer");
        assert_eq!(
            resolved.model.as_deref(),
            Some("haiku"),
            "model from workflow layer"
        );
    }

    #[test]
    fn jcode_settings_follow_profile_and_inline_precedence_per_field() {
        let global = HashMap::from([
            (
                "default".into(),
                AgentProfile {
                    harness: Some("jcode".into()),
                    model: Some("gpt-global".into()),
                    permission_mode: Some("danger-full-access".into()),
                },
            ),
            (
                "nightly".into(),
                AgentProfile {
                    harness: None,
                    model: Some("gpt-profile".into()),
                    permission_mode: Some("bypassPermissions".into()),
                },
            ),
        ]);
        let workflow = HashMap::from([(
            "nightly".into(),
            AgentProfile {
                harness: None,
                model: Some("gpt-workflow".into()),
                permission_mode: None,
            },
        )]);

        let inherited = step(None)
            .pipe_resolve(&HashMap::new(), &global, "claude")
            .unwrap();
        assert_eq!(inherited.harness, "jcode");
        assert_eq!(inherited.model.as_deref(), Some("gpt-global"));
        assert_eq!(
            inherited.permission_mode.as_deref(),
            Some("danger-full-access")
        );

        let mut overridden = step(Some("nightly"));
        overridden.model = Some("gpt-inline".into());
        overridden.permission_mode = Some("danger-full-access".into());
        let resolved = overridden
            .pipe_resolve(&workflow, &global, "claude")
            .unwrap();
        assert_eq!(resolved.harness, "jcode", "global default supplies harness");
        assert_eq!(resolved.model.as_deref(), Some("gpt-inline"));
        assert_eq!(
            resolved.permission_mode.as_deref(),
            Some("danger-full-access")
        );
    }

    #[test]
    fn tier_layer_beats_named_profile_but_loses_to_inline() {
        let global = HashMap::from([
            ("default".into(), profile(Some("claude"), Some("opus"))),
            ("cheap".into(), profile(Some("codex"), Some("haiku"))),
            ("premium".into(), profile(Some("claude"), Some("opus"))),
        ]);
        // Step names the `premium` profile, but a routing rule selected `cheap`;
        // the tier wins over the named profile.
        let resolved = resolve_fields(
            Some("premium"),
            Some("cheap"),
            None,
            None,
            None,
            &HashMap::new(),
            &global,
            "fake",
        )
        .unwrap();
        assert_eq!(
            resolved.harness, "codex",
            "tier harness beats named profile"
        );
        assert_eq!(resolved.model.as_deref(), Some("haiku"));

        // An inline model override still beats the tier.
        let resolved = resolve_fields(
            None,
            Some("cheap"),
            None,
            Some("sonnet"),
            None,
            &HashMap::new(),
            &global,
            "fake",
        )
        .unwrap();
        assert_eq!(resolved.harness, "codex", "tier harness applies");
        assert_eq!(
            resolved.model.as_deref(),
            Some("sonnet"),
            "inline model wins"
        );
    }

    #[test]
    fn unknown_tier_is_an_error() {
        let err = resolve_fields(
            None,
            Some("nope"),
            None,
            None,
            None,
            &HashMap::new(),
            &HashMap::new(),
            "claude",
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown tier profile 'nope'"));
    }

    #[test]
    fn unknown_profile_is_an_error() {
        let err = step(Some("nope"))
            .pipe_resolve(&HashMap::new(), &HashMap::new(), "claude")
            .unwrap_err();
        assert!(err.to_string().contains("unknown agent profile 'nope'"));
    }

    impl SpawnStep {
        fn pipe_resolve(
            &self,
            wf: &HashMap<String, AgentProfile>,
            global: &HashMap<String, AgentProfile>,
            default_harness: &str,
        ) -> rk_core::Result<ResolvedAgent> {
            resolve(self, &TierRouting::default(), wf, global, default_harness)
        }
    }

    #[test]
    fn spawn_step_tier_routing_beats_named_profile_but_loses_to_inline() {
        let global = HashMap::from([
            ("default".into(), profile(Some("claude"), Some("opus"))),
            ("cheap".into(), profile(Some("codex"), Some("haiku"))),
            ("premium".into(), profile(Some("claude"), Some("opus"))),
        ]);
        let tiers = TierRouting {
            rules: vec![crate::TierRule {
                priority: Some("low".into()),
                label: None,
                tier: "cheap".into(),
            }],
        };
        let mut low_priority = step(Some("premium"));
        low_priority.priority = Some("low".into());
        let resolved = resolve(&low_priority, &tiers, &HashMap::new(), &global, "fake").unwrap();
        assert_eq!(
            resolved.harness, "codex",
            "the routing rule's tier beats the step's named profile"
        );
        assert_eq!(resolved.model.as_deref(), Some("haiku"));

        // A priority the table has no rule for falls through untouched.
        let mut other_priority = step(Some("premium"));
        other_priority.priority = Some("high".into());
        let resolved = resolve(&other_priority, &tiers, &HashMap::new(), &global, "fake").unwrap();
        assert_eq!(resolved.harness, "claude", "named profile applies as-is");
        assert_eq!(resolved.model.as_deref(), Some("opus"));
    }
}

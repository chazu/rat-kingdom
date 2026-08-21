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

    let mut harness: Option<String> = None;
    let mut model: Option<String> = None;
    let mut permission_mode: Option<String> = None;
    for layer in layers {
        if layer.harness.is_some() {
            harness = layer.harness.clone();
        }
        if layer.model.is_some() {
            model = layer.model.clone();
        }
        if layer.permission_mode.is_some() {
            permission_mode = layer.permission_mode.clone();
        }
    }
    // Inline step overrides beat everything.
    if step_harness.is_some() {
        harness = step_harness.map(String::from);
    }
    if step_model.is_some() {
        model = step_model.map(String::from);
    }
    if step_permission_mode.is_some() {
        permission_mode = step_permission_mode.map(String::from);
    }

    Ok(ResolvedAgent {
        harness: harness.unwrap_or_else(|| global_default_harness.to_string()),
        model,
        permission_mode,
    })
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

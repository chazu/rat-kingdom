//! Agent resolution: which harness/model/permissions run a spawn step.
//!
//! Field-wise, most specific wins:
//! 1. inline step overrides (`harness:`/`model:`/`permission_mode:`)
//! 2. the step's named profile in the workflow's `agents:`
//! 3. the same-named profile in global config `[agents.<name>]`
//! 4. the workflow's `agents.default` profile
//! 5. global `[agents.default]`
//! 6. global `[harness] default` for the harness kind
//!
//! A step naming a profile that exists nowhere is an error (silent fallback
//! would mask typos).

use crate::{AgentProfile, SpawnStep};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedAgent {
    pub harness: String,
    pub model: Option<String>,
    pub permission_mode: Option<String>,
}

pub fn resolve(
    step: &SpawnStep,
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
    if let Some(name) = &step.agent {
        let global_named = global_agents.get(name);
        let workflow_named = workflow_agents.get(name);
        if global_named.is_none() && workflow_named.is_none() {
            return Err(rk_core::Error::other(format!(
                "unknown agent profile '{name}' (not in workflow agents nor global [agents])"
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
    if step.harness.is_some() {
        harness = step.harness.clone();
    }
    if step.model.is_some() {
        model = step.model.clone();
    }
    if step.permission_mode.is_some() {
        permission_mode = step.permission_mode.clone();
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
            agent: agent.map(String::from),
            harness: None,
            model: None,
            permission_mode: None,
            task: TaskDef {
                title: "t".into(),
                description: None,
            },
            branch: None,
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
            resolve(self, wf, global, default_harness)
        }
    }
}

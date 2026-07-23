//! The daemon tuple-reactor: registered `#Trigger` reactions that fire
//! workflows when matching tuples land in the space. Zero-token, zero-model
//! dispatch — the keystone the stigmergy proposals (quorum promotion, obstacle
//! coalescence, convention injection) all ride on.
//!
//! # Why dispatch is scan-driven, not feed-driven
//!
//! The live feed ([`Space::subscribe`]) is a lossy broadcast: it drops events
//! for laggy consumers. A trigger must never miss an event, so the feed is used
//! only as a *wake signal*. The source of truth is a durable cursor over the
//! store: each cycle scans tuples with `id` greater than the last processed id
//! (ULIDs sort by creation time), fires matching triggers, then advances the
//! cursor. This is the same cursor discipline the multiplayer sync loop uses.
//!
//! # Idempotency and re-entrancy
//!
//! Dispatch is at-least-once (a crash mid-cycle re-runs from the saved cursor),
//! made idempotent by a durable marker written per fired `(trigger, tuple)`:
//! `already_fired` short-circuits a repeat. Re-entrancy — a workflow whose
//! output re-fires its own trigger — is broken three ways: the reactor tags its
//! own output with the reserved `reactor` instance (never reacted to), triggers
//! and config can exclude specific authors, and every trigger has a per-window
//! fire cap (`maxFires`, <=100) mirroring the `repeat` discipline.

use crate::repos::RepoRegistry;
use crate::workflow_exec::WorkflowEngine;
use rk_core::config::ReactorConfig;
use rk_core::id::RecordId;
use rk_core::paths::Layout;
use rk_core::tuple::{Category, Lifecycle, Pattern, Tuple, SYSTEM_SCOPE};
use rk_space::Space;
use rk_workflow::Trigger;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// The reserved author of every tuple the reactor writes (markers, obstacles).
/// Triggers never react to it, so a reaction can never fire on its own output.
pub const REACTOR_INSTANCE: &str = "reactor";
/// Identity of the durable idempotency marker tuples (system scope).
const MARKER_IDENTITY: &str = "reactor_fired";

/// A loaded trigger plus where it came from (a repo-local file defaults its
/// target repo to that repo; a global-dir trigger has no default repo).
struct Loaded {
    trigger: Trigger,
    source_repo: Option<String>,
}

pub struct Reactor {
    space: Space,
    engine: Arc<WorkflowEngine>,
    layout: Layout,
    config: ReactorConfig,
    cursor_file: PathBuf,
    /// Per-trigger fire timestamps for the rolling rate cap. In-memory: a storm
    /// is a live-daemon phenomenon, and a restart legitimately resets the window.
    fires: Mutex<HashMap<String, Vec<Instant>>>,
}

impl Reactor {
    pub fn new(
        space: Space,
        engine: Arc<WorkflowEngine>,
        layout: Layout,
        config: ReactorConfig,
    ) -> Self {
        let cursor_file = layout.home().join("reactor-cursor");
        Self {
            space,
            engine,
            layout,
            config,
            cursor_file,
            fires: Mutex::new(HashMap::new()),
        }
    }

    /// Baseline the cursor to the newest existing tuple so a fresh daemon does
    /// not react to the entire pre-existing backlog on first boot. A no-op once
    /// a cursor file exists (restarts resume where they left off).
    pub fn initialize_cursor(&self) -> rk_core::Result<()> {
        if self.cursor_file.exists() {
            return Ok(());
        }
        let all = self.space.scan(&Pattern::default())?;
        if let Some(max) = all.iter().map(|t| t.id).max() {
            self.save_cursor(max)?;
        }
        Ok(())
    }

    /// Process every tuple newer than the cursor: match it against all loaded
    /// triggers and fire the workflows. Returns how many workflows were fired.
    pub fn run_cycle(&self) -> rk_core::Result<usize> {
        let cursor = self.load_cursor();
        // Snapshot the tuples, then load triggers (a slow `cue` shell-out) and —
        // last, so it is the freshest thing relative to the scanned tuples — the
        // repo registry. Loading the registry after the scan closes the window
        // where a repo registered just before a tuple landed would be missed and
        // the tuple dropped as the cursor advances past it.
        let all = self.space.scan(&Pattern::default())?;
        let registry = RepoRegistry::load(&self.layout.home().join("repos.json"))?;
        let triggers = self.load_all_triggers(&registry);

        let mut fired = 0usize;
        let mut max_id = cursor;
        for tuple in &all {
            if let Some(c) = cursor {
                if tuple.id <= c {
                    continue;
                }
            }
            // Advance the cursor past every scanned tuple, including the
            // reactor's own markers, so they are seen once and never re-scanned.
            max_id = Some(match max_id {
                Some(m) => m.max(tuple.id),
                None => tuple.id,
            });
            if tuple.instance == REACTOR_INSTANCE {
                continue;
            }
            for loaded in &triggers {
                match self.try_fire(loaded, tuple, &registry) {
                    Ok(true) => fired += 1,
                    Ok(false) => {}
                    Err(e) => {
                        warn!(trigger = %loaded.trigger.name, error = %e, "reactor dispatch failed")
                    }
                }
            }
        }
        if let Some(m) = max_id {
            if cursor.map(|c| m > c).unwrap_or(true) {
                self.save_cursor(m)?;
            }
        }
        Ok(fired)
    }

    /// Decide and, if warranted, dispatch a single trigger against one tuple.
    /// Returns whether a workflow was fired.
    fn try_fire(
        &self,
        loaded: &Loaded,
        tuple: &Tuple,
        registry: &RepoRegistry,
    ) -> rk_core::Result<bool> {
        let trigger = &loaded.trigger;
        if self.excluded(trigger, tuple) {
            return Ok(false);
        }
        if !self.pattern(trigger)?.matches(tuple) {
            return Ok(false);
        }
        // Idempotency: one dispatch per (trigger, tuple) for the marker's life.
        let key = format!("{}@{}", trigger.name, tuple.id);
        if self.already_fired(&key)? {
            return Ok(false);
        }
        if self.rate_limited(trigger) {
            warn!(trigger = %trigger.name, "reactor rate cap reached; skipping fire");
            let _ = self.space.out(Tuple::new(
                Category::Obstacle,
                SYSTEM_SCOPE,
                "reactor_rate_capped",
                REACTOR_INSTANCE,
                json!({"trigger": trigger.name, "window_secs": self.config.window_secs}),
            ));
            return Ok(false);
        }
        // Target repo: explicit override > the trigger file's own repo > the
        // matched tuple's scope. It must resolve to a registered repo path.
        let repo_name = trigger
            .repo
            .clone()
            .or_else(|| loaded.source_repo.clone())
            .unwrap_or_else(|| tuple.scope.clone());
        let Some(record) = registry.get(&repo_name) else {
            warn!(trigger = %trigger.name, repo = %repo_name, "reactor: no such registered repo; skipping");
            return Ok(false);
        };
        let repo_path = record.path.to_string_lossy().to_string();
        let params = template_params(&trigger.params, tuple);

        // The workflow runs in the background; run() returns the instance now.
        let instance = self.engine.run(&trigger.run, &repo_path, params)?;
        info!(
            trigger = %trigger.name,
            workflow = %trigger.run,
            instance = %instance.id,
            tuple = %tuple.id,
            "reactor fired workflow"
        );
        self.mark_fired(&key, trigger, tuple, &instance.id)?;
        self.record_fire(&trigger.name);
        Ok(true)
    }

    /// Discover triggers from the global dir and each registered repo's
    /// `.rk/triggers.cue`. A malformed file is logged and skipped, never fatal.
    fn load_all_triggers(&self, registry: &RepoRegistry) -> Vec<Loaded> {
        let mut out = Vec::new();
        for file in rk_workflow::definitions(&self.layout.triggers_dir()) {
            match rk_workflow::load_triggers(&file) {
                Ok(ts) => out.extend(ts.into_iter().map(|trigger| Loaded {
                    trigger,
                    source_repo: None,
                })),
                Err(e) => {
                    warn!(file = %file.display(), error = %e, "reactor: bad global trigger file")
                }
            }
        }
        for repo in registry.list() {
            let file = repo.path.join(".rk").join("triggers.cue");
            if !file.exists() {
                continue;
            }
            match rk_workflow::load_triggers(&file) {
                Ok(ts) => out.extend(ts.into_iter().map(|trigger| Loaded {
                    trigger,
                    source_repo: Some(repo.name.clone()),
                })),
                Err(e) => warn!(repo = %repo.name, error = %e, "reactor: bad repo trigger file"),
            }
        }
        out
    }

    fn excluded(&self, trigger: &Trigger, tuple: &Tuple) -> bool {
        tuple.instance == REACTOR_INSTANCE
            || self
                .config
                .exclude_instances
                .iter()
                .any(|e| e == &tuple.instance)
            || trigger.exclude.iter().any(|e| e == &tuple.instance)
    }

    /// Build the one authoritative [`Pattern`] for a trigger's match predicate,
    /// so the reactor uses the same match logic as every reader in the system.
    fn pattern(&self, trigger: &Trigger) -> rk_core::Result<Pattern> {
        let m = &trigger.matcher;
        let mut p = Pattern::default();
        if let Some(c) = &m.category {
            p.category = Some(Category::from_str(c)?);
        }
        p.scope = m.scope.clone();
        p.identity = m.identity.clone();
        p.instance = m.instance.clone();
        p.payload_search = m.search.clone();
        Ok(p)
    }

    fn already_fired(&self, key: &str) -> rk_core::Result<bool> {
        let mut p = Pattern::category(Category::Event)
            .scope(SYSTEM_SCOPE)
            .identity(MARKER_IDENTITY);
        p.payload_search = Some(format!("\"key\":\"{key}\""));
        Ok(!self.space.scan(&p)?.is_empty())
    }

    fn mark_fired(
        &self,
        key: &str,
        trigger: &Trigger,
        tuple: &Tuple,
        instance: &str,
    ) -> rk_core::Result<()> {
        let mut marker = Tuple::new(
            Category::Event,
            SYSTEM_SCOPE,
            MARKER_IDENTITY,
            REACTOR_INSTANCE,
            json!({
                "key": key,
                "trigger": trigger.name,
                "tuple": tuple.id.to_string(),
                "workflow": trigger.run,
                "instance": instance,
            }),
        );
        // Ephemeral with a TTL: the marker only needs to outlive any redelivery,
        // then self-collects. (Ephemeral tuples do not replicate — dedup is
        // per-castle, which is correct: each castle runs its own reactor.)
        marker.lifecycle = Lifecycle::Ephemeral;
        if self.config.marker_ttl_secs > 0 {
            marker.expires_at = Some(
                chrono::Utc::now() + chrono::Duration::seconds(self.config.marker_ttl_secs as i64),
            );
        }
        self.space.out(marker)
    }

    fn rate_limited(&self, trigger: &Trigger) -> bool {
        let cap = trigger.max_fires.unwrap_or(self.config.max_fires).min(100);
        if cap == 0 {
            return true;
        }
        let window = Duration::from_secs(self.config.window_secs.max(1));
        let now = Instant::now();
        let mut map = self.fires.lock().unwrap_or_else(|p| p.into_inner());
        let entry = map.entry(trigger.name.clone()).or_default();
        entry.retain(|t| now.duration_since(*t) < window);
        entry.len() as u32 >= cap
    }

    fn record_fire(&self, name: &str) {
        let mut map = self.fires.lock().unwrap_or_else(|p| p.into_inner());
        map.entry(name.to_string())
            .or_default()
            .push(Instant::now());
    }

    /// Test-only: how many workflow instances the fired workflows created.
    #[doc(hidden)]
    pub fn engine_instance_count(&self) -> usize {
        self.engine.list().len()
    }

    fn load_cursor(&self) -> Option<RecordId> {
        std::fs::read_to_string(&self.cursor_file)
            .ok()?
            .trim()
            .parse()
            .ok()
    }

    fn save_cursor(&self, id: RecordId) -> rk_core::Result<()> {
        std::fs::write(&self.cursor_file, id.to_string())?;
        Ok(())
    }
}

/// Template each workflow param from the matched tuple.
fn template_params(params: &HashMap<String, String>, tuple: &Tuple) -> HashMap<String, Value> {
    params
        .iter()
        .map(|(k, v)| (k.clone(), template_param(v, tuple)))
        .collect()
}

/// Render one param value. A lone whole-value `{{tuple.payload.<key>}}`
/// placeholder passes the raw JSON value through (preserving its type for the
/// workflow's typed params); anything else is string-interpolated.
fn template_param(raw: &str, tuple: &Tuple) -> Value {
    if let Some(rest) = raw.strip_prefix("{{tuple.payload.") {
        if let Some(key) = rest.strip_suffix("}}") {
            if !key.contains("{{") && !key.contains("}}") {
                return tuple.payload.get(key).cloned().unwrap_or(Value::Null);
            }
        }
    }
    Value::String(interpolate_tuple(raw, tuple))
}

fn interpolate_tuple(text: &str, tuple: &Tuple) -> String {
    let mut out = text
        .replace("{{tuple.category}}", tuple.category.as_str())
        .replace("{{tuple.scope}}", &tuple.scope)
        .replace("{{tuple.identity}}", &tuple.identity)
        .replace("{{tuple.instance}}", &tuple.instance)
        .replace("{{tuple.id}}", &tuple.id.to_string());
    if let Value::Object(map) = &tuple.payload {
        for (k, v) in map {
            out = out.replace(&format!("{{{{tuple.payload.{k}}}}}"), &scalar_str(v));
        }
    }
    out
}

fn scalar_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rk_workflow::TriggerMatch;

    fn tuple() -> Tuple {
        Tuple::new(
            Category::Endorsement,
            "system",
            "endorse",
            "Whisker",
            json!({"suggestion": "sug-1", "count": 3}),
        )
    }

    fn trigger(params: &[(&str, &str)]) -> Trigger {
        Trigger {
            name: "t".into(),
            matcher: TriggerMatch::default(),
            run: "w".into(),
            repo: None,
            params: params
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            exclude: Vec::new(),
            max_fires: None,
        }
    }

    #[test]
    fn whole_value_payload_placeholder_preserves_type() {
        let t = tuple();
        let params = template_params(&trigger(&[("n", "{{tuple.payload.count}}")]).params, &t);
        // Passed through as the raw JSON number, not a string.
        assert_eq!(params["n"], json!(3));
    }

    #[test]
    fn string_interpolation_mixes_fields_and_payload() {
        let t = tuple();
        let params = template_params(
            &trigger(&[("label", "{{tuple.identity}}:{{tuple.payload.suggestion}}")]).params,
            &t,
        );
        assert_eq!(params["label"], json!("endorse:sug-1"));
    }

    #[test]
    fn missing_payload_key_is_null() {
        let t = tuple();
        let params = template_params(&trigger(&[("x", "{{tuple.payload.absent}}")]).params, &t);
        assert_eq!(params["x"], Value::Null);
    }

    #[test]
    fn tuple_structural_fields_interpolate() {
        let t = tuple();
        assert_eq!(
            interpolate_tuple("{{tuple.category}}/{{tuple.scope}}", &t),
            "endorsement/system"
        );
    }
}

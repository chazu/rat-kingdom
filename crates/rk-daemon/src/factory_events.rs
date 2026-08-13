use rk_core::tuple::{Category, Tuple};
use rk_space::CoordinatorEvent;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeSet;

pub const SCHEMA: u64 = 1;
pub const DEFAULT_LIMIT: usize = 256;
pub const MAX_LIMIT: usize = 256;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct FactoryEventFilter {
    pub after: Option<u64>,
    pub repo: Option<String>,
    pub kinds: Vec<String>,
    pub limit: Option<usize>,
    pub coordinator: Option<String>,
    pub include_archived: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct FactoryReplay {
    pub schema: u64,
    pub events: Vec<FactoryEvent>,
    pub boundary: Option<u64>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct FactoryEvent {
    pub schema: u64,
    pub cursor: u64,
    pub occurred_at: chrono::DateTime<chrono::Utc>,
    pub kind: String,
    pub repo: String,
    pub caller: String,
    pub source: String,
    pub subject: Value,
    pub summary: String,
    pub payload: Value,
}

impl FactoryEventFilter {
    pub fn limit(&self) -> usize {
        self.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
    }

    pub fn matches(&self, event: &FactoryEvent) -> bool {
        self.repo.as_deref().is_none_or(|repo| event.repo == repo)
            && (self.kinds.is_empty() || self.kinds.iter().any(|kind| kind == &event.kind))
    }
}

pub fn replay(scanned: Vec<CoordinatorEvent>, filter: &FactoryEventFilter) -> FactoryReplay {
    let limit = filter.limit();
    let truncated = scanned.len() > limit;
    let boundary = scanned
        .get(if truncated {
            limit
        } else {
            scanned.len().saturating_sub(1)
        })
        .map(|event| event.cursor)
        .or(filter.after);
    let events = scanned
        .into_iter()
        .take(limit)
        .filter_map(project)
        .filter(|event| filter.matches(event))
        .collect();
    FactoryReplay {
        schema: SCHEMA,
        events,
        boundary,
        truncated,
    }
}

pub fn project(event: CoordinatorEvent) -> Option<FactoryEvent> {
    let tuple = event.event;
    if tuple.category != Category::Event || tuple.identity != "factory_event" {
        return None;
    }
    let kind = tuple.payload.get("kind")?.as_str()?.to_string();
    let source = tuple
        .payload
        .get("source")
        .and_then(Value::as_str)
        .unwrap_or("factory")
        .to_string();
    let caller = tuple
        .payload
        .get("caller")
        .and_then(Value::as_str)
        .unwrap_or(&tuple.instance)
        .to_string();
    let subject = tuple.payload.get("subject").cloned().unwrap_or(Value::Null);
    let summary = tuple
        .payload
        .get("summary")
        .and_then(Value::as_str)
        .unwrap_or(&kind)
        .to_string();
    let payload = tuple.payload.get("payload").cloned().unwrap_or(Value::Null);
    Some(FactoryEvent {
        schema: SCHEMA,
        cursor: event.cursor,
        occurred_at: tuple.created_at,
        kind,
        repo: tuple.scope,
        caller,
        source,
        subject,
        summary,
        payload,
    })
}

pub fn event_tuple(
    repo: &str,
    caller: &str,
    kind: &str,
    source: &str,
    subject: Value,
    summary: impl Into<String>,
    payload: Value,
) -> Tuple {
    Tuple::new(
        Category::Event,
        repo,
        "factory_event",
        caller,
        json!({
            "schema": SCHEMA,
            "kind": kind,
            "caller": caller,
            "source": source,
            "subject": subject,
            "summary": summary.into(),
            "payload": payload,
        }),
    )
}

pub fn snapshot_value(
    agents: Value,
    workflows: Value,
    tickets: Value,
    inbox: Value,
    budget: Value,
    approvals: Value,
    repo_resync: Value,
) -> Value {
    json!({
        "agents": agents,
        "approvals": approvals,
        "budget": budget,
        "inbox": inbox,
        "repo_resync": repo_resync,
        "tickets": tickets,
        "workflows": workflows,
    })
}

pub fn event_kinds(events: &[FactoryEvent]) -> BTreeSet<String> {
    events.iter().map(|event| event.kind.clone()).collect()
}

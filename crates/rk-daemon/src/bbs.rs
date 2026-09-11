//! Task-scoped discovery over the existing tuplespace.

use crate::tickets::Tickets;
use rk_core::bbs::{bounded_text, Briefing, BriefingEntry};
use rk_core::tuple::{Category, Pattern, Tuple};
use rk_space::Space;
use serde::Deserialize;
use std::collections::HashSet;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BriefParams {
    pub repo: String,
    pub task: String,
    #[serde(default)]
    pub areas: Vec<String>,
    pub since: Option<u64>,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    5
}

impl BriefParams {
    pub fn for_task(repo: &str, task: &str) -> Self {
        Self {
            repo: repo.into(),
            task: task.into(),
            areas: vec![],
            since: None,
            limit: default_limit(),
        }
    }
}

pub fn brief(space: &Space, tickets: &Tickets, params: &BriefParams) -> rk_core::Result<Briefing> {
    if params.repo.trim().is_empty()
        || params.task.trim().is_empty()
        || !(1..=20).contains(&params.limit)
        || params.areas.len() > 20
        || params.areas.iter().any(|a| {
            a.trim().is_empty() || a.len() > 512 || a.trim().trim_end_matches('*').is_empty()
        })
    {
        return Err(rk_core::Error::other(
            "repo/task required; limit must be 1..20 and at most 20 areas are allowed",
        ));
    }
    // Capture before reading: concurrent writes may appear twice, never advance
    // the checkpoint past a write that was invisible to this read.
    let cursor = space.latest_persistence_sequence()?;
    if params.since.is_some_and(|since| since > cursor) {
        return Err(rk_core::Error::other(
            "BBS checkpoint is ahead of this store; omit --since to refresh",
        ));
    }
    let changed: HashSet<_> = match params.since {
        Some(since) => space
            .persistence_delta(Some(since))?
            .tuples
            .into_iter()
            .map(|t| t.id)
            .collect(),
        None => HashSet::new(),
    };
    let task = tickets.resolve(&params.task)?;
    if task.as_ref().is_some_and(|t| t.scope != params.repo) {
        return Err(rk_core::Error::other(
            "task belongs to a different repository",
        ));
    }
    let canonical = task
        .as_ref()
        .map_or(params.task.as_str(), |t| t.identity.as_str());
    let mut related = HashSet::new();
    let mut pending = vec![canonical.to_string()];
    let mut visited = HashSet::new();
    while let Some(id) = pending.pop() {
        if !visited.insert(id.clone()) || visited.len() > 64 {
            continue;
        }
        for spelling in tickets.id_spellings(&id)? {
            related.insert(spelling.to_lowercase());
        }
        if let Some(ticket) = tickets.resolve(&id)? {
            if ticket.scope != params.repo {
                continue;
            }
            if let Some(parent) = ticket.payload["parent"].as_str() {
                pending.push(parent.into());
            }
            if let Some(deps) = ticket.payload["depends_on"].as_array() {
                pending.extend(deps.iter().filter_map(|v| v.as_str().map(str::to_string)));
            }
        }
    }
    related.insert(canonical.to_lowercase());
    let words: HashSet<_> = task
        .as_ref()
        .and_then(|t| t.payload["title"].as_str())
        .unwrap_or("")
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| w.len() >= 5)
        .map(str::to_lowercase)
        .filter(|w| {
            ![
                "implement",
                "ticket",
                "follow",
                "existing",
                "support",
                "through",
                "remaining",
            ]
            .contains(&w.as_str())
        })
        .take(24)
        .collect();
    let mut tuples = Vec::new();
    for category in [Category::Claim, Category::Need, Category::Artifact] {
        tuples.extend(space.scan(&Pattern::category(category).scope(&params.repo))?);
    }
    let now = chrono::Utc::now();
    tuples.retain(|t| {
        t.expires_at.is_none_or(|expiry| expiry > now)
            && t.strength.is_none_or(|strength| strength > 0.0)
    });
    let accepted: HashSet<_> = tuples
        .iter()
        .filter(|t| rk_core::bbs::is_acceptance(t))
        .filter_map(|t| t.payload["question"].as_str())
        .collect();
    let mut areas: Vec<String> = params
        .areas
        .iter()
        .filter(|a| !a.trim().is_empty())
        .map(|a| a.trim().trim_end_matches('*').to_lowercase())
        .collect();
    for t in &tuples {
        if t.category == Category::Claim && refers_to(t, &related) {
            areas.push(t.identity.trim_end_matches('*').to_lowercase());
        }
    }
    areas.retain(|area| !area.is_empty());
    let mut entries = Vec::new();
    let mut omitted = 0;
    for category in [Category::Claim, Category::Need, Category::Artifact] {
        let mut selected = Vec::new();
        for tuple in tuples.iter().filter(|t| t.category == category) {
            if rk_core::bbs::is_question(tuple) && accepted.contains(tuple.id.to_string().as_str())
            {
                continue;
            }
            let body = format!("{} {}", tuple.identity, tuple.payload).to_lowercase();
            if !params.areas.is_empty()
                && !params
                    .areas
                    .iter()
                    .any(|area| body.contains(&area.trim().trim_end_matches('*').to_lowercase()))
            {
                continue;
            }
            let (score, reason) = if refers_to(tuple, &related) {
                (100, "task or dependency")
            } else if areas.iter().any(|a| body.contains(a)) {
                (80, "shared area")
            } else if words.iter().any(|w| {
                body.split(|c: char| !c.is_alphanumeric())
                    .any(|part| part == w)
            }) {
                (40, "task topic")
            } else {
                continue;
            };
            selected.push((score, tuple, reason));
        }
        selected.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then_with(|| changed.contains(&b.1.id).cmp(&changed.contains(&a.1.id)))
                .then_with(|| b.1.id.cmp(&a.1.id))
        });
        omitted += selected.len().saturating_sub(params.limit);
        for (_, t, reason) in selected.into_iter().take(params.limit) {
            let summary = ["summary", "text", "notes", "answer"]
                .iter()
                .find_map(|key| t.payload[key].as_str())
                .map(str::to_string)
                .unwrap_or_else(|| t.payload.to_string());
            entries.push(BriefingEntry {
                id: t.id.to_string(),
                category: category.as_str().into(),
                kind: (t.lifecycle == rk_core::tuple::Lifecycle::Furniture)
                    .then(|| t.payload["bbs_kind"].as_str().map(str::to_string))
                    .flatten(),
                author: bounded_text(&t.instance, 80),
                reason: reason.into(),
                summary: bounded_text(&summary, 500),
                branch: t.payload["branch"].as_str().map(|s| bounded_text(s, 180)),
                commit: t.payload["commit"]
                    .as_str()
                    .or_else(|| t.payload["head_sha"].as_str())
                    .map(|s| bounded_text(s, 80)),
                question: t.payload["question"].as_str().map(str::to_string),
                changed: changed.contains(&t.id),
            });
        }
    }
    Ok(Briefing {
        repo: params.repo.clone(),
        task: canonical.into(),
        cursor,
        since: params.since,
        entries,
        omitted,
    })
}

fn refers_to(tuple: &Tuple, ids: &HashSet<String>) -> bool {
    // Match complete ticket tokens, including legacy aliases embedded in a
    // branch, rather than allowing TKT-1 to match TKT-10.
    format!("{} {}", tuple.identity, tuple.payload)
        .to_lowercase()
        .split(|c: char| !c.is_alphanumeric() && c != '-')
        .any(|token| ids.contains(token))
}

pub fn show(space: &Space, id: &str) -> rk_core::Result<serde_json::Value> {
    let id = id
        .parse::<rk_core::id::RecordId>()
        .map_err(|e| rk_core::Error::other(e.to_string()))?;
    let tuple = space
        .get(id)?
        .ok_or_else(|| rk_core::Error::other("BBS post not found"))?;
    let question_id = if rk_core::bbs::is_question(&tuple) {
        Some(tuple.id.to_string())
    } else if tuple.lifecycle == rk_core::tuple::Lifecycle::Furniture
        && matches!(
            tuple.payload["bbs_kind"].as_str(),
            Some("answer" | "acceptance")
        )
    {
        tuple.payload["question"].as_str().map(str::to_string)
    } else {
        None
    };
    if let Some(question_id) = question_id {
        let question = get_post(space, &question_id)?;
        let replies: Vec<_> = space
            .scan(&Pattern::category(Category::Artifact).scope(&tuple.scope))?
            .into_iter()
            .filter(|t| {
                t.lifecycle == rk_core::tuple::Lifecycle::Furniture
                    && t.payload["question"] == question_id
            })
            .collect();
        let acceptance = replies.iter().find(|t| rk_core::bbs::is_acceptance(t));
        return Ok(
            serde_json::json!({"tuple":tuple,"question":question,"status":if acceptance.is_some(){"accepted"}else{"open"},"acceptance":acceptance,"replies":replies}),
        );
    }
    Ok(serde_json::json!({"tuple":tuple}))
}

fn get_post(space: &Space, id: &str) -> rk_core::Result<Tuple> {
    let id = id
        .parse::<rk_core::id::RecordId>()
        .map_err(|e| rk_core::Error::other(e.to_string()))?;
    space
        .get(id)?
        .ok_or_else(|| rk_core::Error::other("BBS post not found"))
}

pub struct WriteError {
    pub code: &'static str,
    pub message: String,
}
impl From<rk_core::Error> for WriteError {
    fn from(error: rk_core::Error) -> Self {
        Self {
            code: crate::proto::codes::BAD_PARAMS,
            message: error.to_string(),
        }
    }
}
fn invalid(message: &str) -> WriteError {
    WriteError {
        code: crate::proto::codes::BAD_PARAMS,
        message: message.into(),
    }
}
fn forbidden(message: &str) -> WriteError {
    WriteError {
        code: crate::proto::codes::FORBIDDEN,
        message: message.into(),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AskParams {
    repo: String,
    task: String,
    text: String,
    #[serde(default)]
    areas: Vec<String>,
    key: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AnswerParams {
    question: String,
    text: String,
    artifact: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AcceptParams {
    question: String,
    answer: String,
    text: String,
    contribution: Option<String>,
}

fn parse<T: serde::de::DeserializeOwned>(value: &serde_json::Value) -> Result<T, WriteError> {
    serde_json::from_value(value.clone()).map_err(|e| invalid(&e.to_string()))
}
fn check_text(text: &str) -> Result<(), WriteError> {
    if text.trim().is_empty() || text.len() > 8192 {
        return Err(invalid("text must contain 1..8192 bytes"));
    }
    Ok(())
}
fn check_repo(record: Option<&crate::agents::AgentRecord>, repo: &str) -> Result<(), WriteError> {
    if record.is_some_and(|r| r.repo_name != repo) {
        return Err(forbidden(
            "BBS writes must stay in the agent's assigned repository",
        ));
    }
    Ok(())
}
fn check_artifact(space: &Space, id: Option<&str>, scope: &str) -> Result<(), WriteError> {
    if let Some(id) = id {
        let post = get_post(space, id)?;
        if post.scope != scope || post.category != Category::Artifact {
            return Err(invalid("evidence must be an artifact in this repository"));
        }
    }
    Ok(())
}

/// Called under the daemon's BBS write lock. Each operation commits exactly one
/// immutable tuple; its logical identity makes retries safe across restarts.
pub fn write(
    space: &Space,
    tickets: &Tickets,
    caller: &str,
    record: Option<&crate::agents::AgentRecord>,
    method: &str,
    params: &serde_json::Value,
) -> Result<serde_json::Value, WriteError> {
    use rk_core::tuple::Lifecycle;
    use serde_json::json;
    let caller = if caller.is_empty() {
        "operator"
    } else {
        caller
    };
    let spawn = record.map(|r| r.spawn_id().to_string());
    let (category, scope, identity, payload) = match method {
        "bbs.ask" => {
            let p: AskParams = parse(params)?;
            check_text(&p.text)?;
            if p.repo.trim().is_empty()
                || p.task.trim().is_empty()
                || p.areas.len() > 20
                || p.areas.iter().any(|a| a.is_empty() || a.len() > 512)
                || p.key
                    .as_ref()
                    .is_some_and(|k| k.is_empty() || k.len() > 256)
            {
                return Err(invalid("repo/task required; at most 20 nonempty areas (512 bytes each), key 1..256 bytes"));
            }
            check_repo(record, &p.repo)?;
            let task = tickets.resolve(&p.task)?;
            if task.as_ref().is_some_and(|t| t.scope != p.repo) {
                return Err(invalid("task belongs to a different repository"));
            }
            let task = task.map_or(p.task, |t| t.identity);
            if let Some(record) = record {
                let owned = record.task.as_deref().unwrap_or("");
                if !tickets.id_spellings(owned)?.contains(&task) {
                    return Err(forbidden("ask must reference your assigned task"));
                }
            }
            let key = rk_core::action::canonical_digest(&json!([
                caller,
                p.repo,
                task,
                p.key.as_deref().unwrap_or(&p.text)
            ]))?;
            (
                Category::Need,
                p.repo,
                format!("bbs-question-{key}"),
                json!({"bbs_kind":"question","agent":caller,"spawn":spawn,"task":task,"text":p.text,"areas":p.areas}),
            )
        }
        "bbs.answer" => {
            let p: AnswerParams = parse(params)?;
            check_text(&p.text)?;
            let question = get_post(space, &p.question)?;
            if !rk_core::bbs::is_question(&question) {
                return Err(invalid("answer requires a BBS question"));
            }
            check_repo(record, &question.scope)?;
            check_artifact(space, p.artifact.as_deref(), &question.scope)?;
            let key = rk_core::action::canonical_digest(&json!([
                caller,
                question.id,
                p.text,
                p.artifact
            ]))?;
            (
                Category::Artifact,
                question.scope,
                format!("bbs-answer-{key}"),
                json!({"bbs_kind":"answer","agent":caller,"spawn":spawn,"task":question.payload["task"],"areas":question.payload["areas"],"question":question.id,"text":p.text,"source_artifact":p.artifact}),
            )
        }
        "bbs.accept" => {
            let p: AcceptParams = parse(params)?;
            check_text(&p.text)?;
            let question = get_post(space, &p.question)?;
            if !rk_core::bbs::is_question(&question) {
                return Err(invalid("accept requires a BBS question"));
            }
            check_repo(record, &question.scope)?;
            if caller != "operator" && caller != question.instance {
                return Err(forbidden(
                    "only the requester or operator may accept an answer",
                ));
            }
            let answer = get_post(space, &p.answer)?;
            if answer.category != Category::Artifact
                || answer.lifecycle != Lifecycle::Furniture
                || answer.payload["bbs_kind"] != "answer"
                || answer.scope != question.scope
                || answer.payload["question"] != question.id.to_string()
            {
                return Err(invalid("answer must belong to this question"));
            }
            check_artifact(space, p.contribution.as_deref(), &question.scope)?;
            (
                Category::Artifact,
                question.scope,
                format!("bbs-accept-{}", question.id),
                json!({"bbs_kind":"acceptance","agent":caller,"spawn":spawn,"task":question.payload["task"],"areas":question.payload["areas"],"question":question.id,"answer":answer.id,"resolves":question.id,"text":p.text,"contribution":p.contribution}),
            )
        }
        _ => return Err(invalid("unknown BBS write")),
    };
    if let Some(existing) = space
        .scan(
            &Pattern::category(category)
                .scope(&scope)
                .identity(&identity),
        )?
        .into_iter()
        .next()
    {
        // Exact semantic retry; generation attribution is the original write's.
        let mut before = existing.payload.clone();
        let mut after = payload.clone();
        before.as_object_mut().map(|p| p.remove("spawn"));
        after.as_object_mut().map(|p| p.remove("spawn"));
        if before != after {
            return Err(invalid(
                "BBS operation already recorded with different content; use a new question key",
            ));
        }
        return Ok(json!({"id":existing.id,"written":false,"kind":payload["bbs_kind"]}));
    }
    if method == "bbs.answer" {
        let question = get_post(
            space,
            payload["question"]
                .as_str()
                .ok_or_else(|| invalid("question missing"))?,
        )?;
        if question_accepted(space, &question)? {
            return Err(invalid(
                "question already accepted; open a new question for a changed decision",
            ));
        }
    }
    let tuple =
        Tuple::new(category, scope, identity, caller, payload).with_lifecycle(Lifecycle::Furniture);
    space.out(tuple.clone()).map_err(|e| WriteError {
        code: crate::proto::codes::INTERNAL,
        message: e.to_string(),
    })?;
    Ok(json!({"id":tuple.id,"written":true,"kind":tuple.payload["bbs_kind"]}))
}

pub fn question_accepted(space: &Space, question: &Tuple) -> rk_core::Result<bool> {
    Ok(space
        .scan(
            &Pattern::category(Category::Artifact)
                .scope(&question.scope)
                .identity(format!("bbs-accept-{}", question.id)),
        )?
        .iter()
        .any(rk_core::bbs::is_acceptance))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn briefing_handles_aliases_late_ids_expiry_and_category_bounds() {
        let space = Space::open_in_memory().unwrap();
        let tickets = Tickets::new(space.clone(), "castle".into());
        let task = Tuple::new(
            Category::Task,
            "repo",
            format!("TKT-{}", rk_core::id::RecordId::new()),
            "operator",
            json!({"title":"Parser grammar","status":"open"}),
        );
        space.out(task.clone()).unwrap();
        let alias = tickets
            .id_spellings(&task.identity)
            .unwrap()
            .into_iter()
            .find(|s| s != &task.identity)
            .unwrap();
        let late = Tuple::new(
            Category::Artifact,
            "repo",
            "late",
            "peer",
            json!({"task":alias,"summary":"An older ID persisted later"}),
        );
        // Advance SQLite's sequence while the earlier-minted artifact is held.
        space
            .out(Tuple::new(
                Category::Event,
                "repo",
                "checkpoint",
                "operator",
                json!({}),
            ))
            .unwrap();
        let checkpoint = space.latest_persistence_sequence().unwrap();
        space.out(late.clone()).unwrap();
        let mut params = BriefParams::for_task("repo", &task.identity);
        params.since = Some(checkpoint);
        let briefing = brief(&space, &tickets, &params).unwrap();
        assert!(briefing
            .entries
            .iter()
            .any(|e| e.id == late.id.to_string() && e.changed));
        let mut expired = Tuple::new(
            Category::Claim,
            "repo",
            "src/parser.rs",
            "peer",
            json!({"task":task.identity}),
        );
        expired.expires_at = Some(chrono::Utc::now() - chrono::Duration::seconds(1));
        space.out(expired).unwrap();
        for category in [Category::Claim, Category::Need, Category::Artifact] {
            for n in 0..8 {
                space
                    .out(Tuple::new(
                        category,
                        "repo",
                        format!("entry-{n}"),
                        "peer",
                        json!({"task":task.identity,"text":"x".repeat(2000)}),
                    ))
                    .unwrap();
            }
        }
        params.limit = 2;
        let briefing = brief(&space, &tickets, &params).unwrap();
        assert_eq!(briefing.entries.len(), 6);
        assert_eq!(briefing.omitted, 19);
        assert!(briefing
            .entries
            .iter()
            .all(|e| e.summary.chars().count() <= 501));
        params.repo = "foreign".into();
        assert!(brief(&space, &tickets, &params).is_err());
    }

    #[test]
    fn durable_questions_do_not_decay() {
        let space = Space::open_in_memory().unwrap();
        let tickets = Tickets::new(space.clone(), "castle".into());
        let question = write(
            &space,
            &tickets,
            "alice",
            None,
            "bbs.ask",
            &json!({"repo":"repo","task":"parser","text":"Which grammar?"}),
        )
        .unwrap_or_else(|e| panic!("{}", e.message));
        space.gc_expired(2.0).unwrap();
        assert!(get_post(&space, question["id"].as_str().unwrap()).is_ok());
    }
}

//! Task-scoped discovery over the existing tuplespace.

use crate::tickets::Tickets;
use rk_core::bbs::{bounded_text, Briefing, BriefingEntry};
use rk_core::tuple::{Category, Pattern, Tuple};
use rk_space::Space;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

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
            if rk_core::bbs::is_excluded_from_discovery(tuple) {
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
    let mut result = if let Some(question_id) = question_id {
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
        serde_json::json!({"tuple":tuple,"question":question,"status":if acceptance.is_some(){"accepted"}else{"open"},"acceptance":acceptance,"replies":replies})
    } else {
        serde_json::json!({"tuple":tuple})
    };
    // Any potential reuse SOURCE — an ordinary artifact regardless of
    // lifecycle, or a finding/answer — threads its receipts and their current
    // assessment into the same `show`, whether or not it also rendered as a
    // question reply above (an answer is both). A receipt itself threads its
    // own assessments. This mirrors exactly what `bbs.reuse` accepts as SOURCE.
    let is_reuse_source = tuple.category == Category::Artifact
        && !matches!(tuple.payload["bbs_kind"].as_str(), Some(k) if !matches!(k, "finding" | "answer"));
    let receipts = if is_reuse_source {
        space
            .scan(&Pattern::category(Category::Artifact).scope(&tuple.scope))?
            .into_iter()
            .filter(|t| rk_core::bbs::is_reuse(t) && t.payload["source"] == tuple.id.to_string())
            .collect()
    } else if rk_core::bbs::is_reuse(&tuple) {
        vec![tuple.clone()]
    } else {
        Vec::new()
    };
    if !receipts.is_empty() {
        let artifacts = space.scan(&Pattern::category(Category::Artifact).scope(&tuple.scope))?;
        let assessment_ids: HashSet<_> = artifacts
            .iter()
            .filter(|t| rk_core::bbs::is_assessment(t))
            .map(|t| t.id)
            .collect();
        // `scan`/`Pattern` reads are ULID-ordered, which is only usually the
        // same as commit order — a delayed writer can mint an earlier ULID
        // but persist later. "Newest by persistence order" must use the
        // immutable persistence journal's actual commit sequence, not id.
        let rank = persistence_rank(space, &assessment_ids)?;
        let mut reuse = Vec::new();
        for receipt in receipts {
            let mut assessments: Vec<Tuple> = artifacts
                .iter()
                .filter(|t| {
                    rk_core::bbs::is_assessment(t)
                        && t.payload["receipt"] == receipt.id.to_string()
                })
                .cloned()
                .collect();
            assessments.sort_by_key(|t| rank.get(&t.id).copied().unwrap_or(0));
            let current = assessments.last().cloned();
            reuse.push(
                serde_json::json!({"receipt":receipt,"assessments":assessments,"current_assessment":current}),
            );
        }
        result["reuse"] = serde_json::json!(reuse);
    }
    Ok(result)
}

/// Rank a small, known id set by actual SQLite commit order, not by
/// `RecordId`/ULID mint order (see caller) — a bounded, indexed lookup keyed
/// on the `tuples` table's primary key, independent of total store size.
fn persistence_rank(
    space: &Space,
    ids: &HashSet<rk_core::id::RecordId>,
) -> rk_core::Result<HashMap<rk_core::id::RecordId, u64>> {
    let ids: Vec<_> = ids.iter().copied().collect();
    space.commit_sequences(&ids)
}

fn get_post(space: &Space, id: &str) -> rk_core::Result<Tuple> {
    let id = id
        .parse::<rk_core::id::RecordId>()
        .map_err(|e| rk_core::Error::other(e.to_string()))?;
    space
        .get(id)?
        .ok_or_else(|| rk_core::Error::other("BBS post not found"))
}

#[derive(Debug)]
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
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishParams {
    repo: String,
    task: String,
    text: String,
    #[serde(default)]
    areas: Vec<String>,
    revision: String,
    #[serde(default)]
    evidence: Vec<String>,
    limitations: String,
    key: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReuseParams {
    source: String,
    task: String,
    outcome: String,
    text: String,
    #[serde(default)]
    evidence: Vec<String>,
    key: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AssessParams {
    receipt: String,
    verdict: String,
    reason: String,
    #[serde(default)]
    evidence: Vec<String>,
    key: Option<String>,
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
fn check_areas(areas: &[String]) -> Result<(), WriteError> {
    if areas.is_empty()
        || areas.len() > 20
        || areas
            .iter()
            .any(|a| a.trim().is_empty() || a.len() > 512)
    {
        return Err(invalid(
            "at least one and at most 20 nonempty areas (512 bytes each) are required",
        ));
    }
    Ok(())
}
/// Evidence must name existing artifacts in the same repository. Required and
/// bounded so a hostile caller cannot point at a foreign scope or manufacture
/// an unbounded payload.
fn check_evidence(space: &Space, ids: &[String], scope: &str) -> Result<(), WriteError> {
    if ids.is_empty() || ids.len() > 20 {
        return Err(invalid("1..20 evidence artifact ids are required"));
    }
    for id in ids {
        check_artifact(space, Some(id.as_str()), scope)?;
    }
    Ok(())
}
/// A finding's `revision` names a source tree/commit by shape (lowercase hex,
/// short or full SHA length) only. This is a claim the publisher makes about
/// what they looked at, not a verified match against the repository — nothing
/// here resolves or checks it against real git history.
fn check_revision(revision: &str) -> Result<(), WriteError> {
    let shape_ok = (7..=64).contains(&revision.len())
        && revision
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
    if !shape_ok {
        return Err(invalid(
            "revision must be a lowercase hex commit id, 7..64 characters (a claim about the source tree, not a verified match)",
        ));
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
        // A finding's identity binds the exact generation that published it —
        // unlike ask/answer/accept above, a respawned agent's byte-identical
        // republish must not silently inherit a predecessor's tuple, since
        // author-generation is load-bearing evidence for the reuse trial.
        "bbs.publish" => {
            let p: PublishParams = parse(params)?;
            check_text(&p.text)?;
            check_text(&p.limitations)?;
            check_areas(&p.areas)?;
            check_revision(&p.revision)?;
            if p.repo.trim().is_empty()
                || p.task.trim().is_empty()
                || p.key
                    .as_ref()
                    .is_some_and(|k| k.is_empty() || k.len() > 256)
            {
                return Err(invalid(
                    "repo/task required; key must be 1..256 bytes",
                ));
            }
            check_repo(record, &p.repo)?;
            check_evidence(space, &p.evidence, &p.repo)?;
            let task = tickets.resolve(&p.task)?;
            if task.as_ref().is_some_and(|t| t.scope != p.repo) {
                return Err(invalid("task belongs to a different repository"));
            }
            let task = task.map_or(p.task, |t| t.identity);
            if let Some(record) = record {
                let owned = record.task.as_deref().unwrap_or("");
                if !tickets.id_spellings(owned)?.contains(&task) {
                    return Err(forbidden("publish must reference your assigned task"));
                }
            }
            let key = rk_core::action::canonical_digest(&json!([
                caller,
                spawn,
                "finding",
                p.repo,
                task,
                p.key.as_deref().unwrap_or(&p.text),
                p.revision,
                p.areas,
                p.evidence,
                p.limitations
            ]))?;
            (
                Category::Artifact,
                p.repo,
                format!("bbs-finding-{key}"),
                json!({"schema_version":1,"bbs_kind":"finding","agent":caller,"spawn":spawn,"task":task,"text":p.text,"areas":p.areas,"revision":p.revision,"evidence":p.evidence,"limitations":p.limitations}),
            )
        }
        "bbs.reuse" => {
            let p: ReuseParams = parse(params)?;
            check_text(&p.text)?;
            if !matches!(p.outcome.as_str(), "used" | "adapted" | "confirmed" | "rejected") {
                return Err(invalid(
                    "outcome must be used, adapted, confirmed, or rejected",
                ));
            }
            if p.key
                .as_ref()
                .is_some_and(|k| k.is_empty() || k.len() > 256)
            {
                return Err(invalid("key must be 1..256 bytes"));
            }
            if p.task.trim().is_empty() {
                return Err(invalid("task is required"));
            }
            let source = get_post(space, &p.source)?;
            // An ordinary artifact (no `bbs_kind` at all) is reusable
            // regardless of lifecycle — a daemon/operator-authored Furniture
            // artifact (e.g. a gate result) can carry useful reproduction
            // evidence just as much as a Session-lifecycle one. Only a BBS
            // record that is itself a receipt/assessment/telemetry kind (or
            // a question, rejected already by the category check) is barred;
            // `finding`/`answer` are explicitly allowed BBS kinds.
            if source.category != Category::Artifact
                || matches!(source.payload["bbs_kind"].as_str(), Some(k) if !matches!(k, "finding" | "answer"))
            {
                return Err(invalid(
                    "reuse source must be an ordinary artifact or a finding/answer, not a receipt, assessment, or telemetry record",
                ));
            }
            check_repo(record, &source.scope)?;
            check_evidence(space, &p.evidence, &source.scope)?;
            let task = tickets.resolve(&p.task)?;
            let task = task.map_or(p.task.clone(), |t| t.identity);
            if let Some(record) = record {
                let owned = record.task.as_deref().unwrap_or("");
                if !tickets.id_spellings(owned)?.contains(&task) {
                    return Err(forbidden("reuse must reference your own assigned task"));
                }
            }
            let key = rk_core::action::canonical_digest(&json!([
                caller,
                spawn,
                "reuse",
                source.id,
                p.outcome,
                p.key.as_deref().unwrap_or(&p.text),
                p.evidence
            ]))?;
            (
                Category::Artifact,
                source.scope.clone(),
                format!("bbs-reuse-{key}"),
                json!({"schema_version":1,"bbs_kind":"reuse","agent":caller,"spawn":spawn,"task":task,"source":source.id,"outcome":p.outcome,"text":p.text,"evidence":p.evidence}),
            )
        }
        "bbs.assess" => {
            // Defense in depth alongside capabilities.rs: `bbs.assess` is
            // absent from the non-operator method grant, so an agent caller is
            // already refused at the wire before reaching this handler; this
            // repeats the check in-handler so the invariant does not depend on
            // capabilities.rs staying correct.
            if caller != "operator" {
                return Err(forbidden("only the operator may assess a reuse receipt"));
            }
            let p: AssessParams = parse(params)?;
            check_text(&p.reason)?;
            if !matches!(p.verdict.as_str(), "verified" | "unsupported" | "incorrect") {
                return Err(invalid(
                    "verdict must be verified, unsupported, or incorrect",
                ));
            }
            if p.key
                .as_ref()
                .is_some_and(|k| k.is_empty() || k.len() > 256)
            {
                return Err(invalid("key must be 1..256 bytes"));
            }
            let receipt = get_post(space, &p.receipt)?;
            if !rk_core::bbs::is_reuse(&receipt) {
                return Err(invalid("assess requires an existing reuse receipt"));
            }
            check_evidence(space, &p.evidence, &receipt.scope)?;
            let key = rk_core::action::canonical_digest(&json!([
                caller,
                spawn,
                "assessment",
                receipt.id,
                p.verdict,
                p.key.as_deref().unwrap_or(&p.reason),
                p.evidence
            ]))?;
            (
                Category::Artifact,
                receipt.scope.clone(),
                format!("bbs-assessment-{key}"),
                json!({"schema_version":1,"bbs_kind":"assessment","agent":caller,"spawn":spawn,"task":receipt.payload["task"],"receipt":receipt.id,"verdict":p.verdict,"reason":p.reason,"evidence":p.evidence}),
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
                "BBS operation already recorded with different content; use a distinguishing --key",
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

    fn agent_record(name: &str, task: &str) -> crate::agents::AgentRecord {
        crate::agents::AgentRecord {
            name: name.into(),
            spawn: Some(rk_core::id::SpawnId::new()),
            role: "rat".into(),
            coordination: None,
            harness: "fake".into(),
            permission_mode: None,
            model: None,
            repo_root: "/tmp/repo".into(),
            repo_name: "repo".into(),
            task: Some(task.into()),
            branch: Some(format!("rat/{name}/{task}")),
            fork_point: None,
            worktree: Some(format!("/tmp/wt/{name}").into()),
            target_branch: "main".into(),
            parent: None,
            workflow_instance: None,
            review: None,
            coordinator: None,
            session_id: None,
            attach_target: None,
            pid: Some(1234),
            merge_commit: None,
            state: crate::agents::AgentState::Running,
            crashed: false,
            stderr_tail: None,
            result: None,
            progress: None,
            usage: rk_harness::TokenUsage::default(),
            cost_usd: 0.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            archived_at: None,
            liveness: Default::default(),
            transport_outage: None,
            recovery: None,
            recovery_receipt: None,
        }
    }

    fn evidence_artifact(space: &Space, scope: &str, identity: &str) -> String {
        let t = Tuple::new(Category::Artifact, scope, identity, "peer", json!({}));
        space.out(t.clone()).unwrap();
        t.id.to_string()
    }

    fn publish_params(evidence: &str) -> serde_json::Value {
        json!({
            "repo":"repo","task":"parser","text":"Delimiters must be escaped",
            "areas":["src/parser.rs"],"revision":"abc1234","evidence":[evidence],
            "limitations":"only checked ASCII input",
        })
    }

    #[test]
    fn publish_validates_areas_evidence_and_revision_shape() {
        let space = Space::open_in_memory().unwrap();
        let tickets = Tickets::new(space.clone(), "castle".into());
        let ev = evidence_artifact(&space, "repo", "ev1");
        let ok = write(&space, &tickets, "alice", None, "bbs.publish", &publish_params(&ev))
            .unwrap_or_else(|e| panic!("{}", e.message));
        assert_eq!(ok["kind"], "finding");
        assert_eq!(ok["written"], true);
        // Exact retry is idempotent: same content, same tuple.
        let retry = write(&space, &tickets, "alice", None, "bbs.publish", &publish_params(&ev)).unwrap();
        assert_eq!(retry["id"], ok["id"]);
        assert_eq!(retry["written"], false);
        // At least one area is required.
        let mut no_areas = publish_params(&ev);
        no_areas["areas"] = json!([]);
        assert!(write(&space, &tickets, "alice", None, "bbs.publish", &no_areas).is_err());
        // Evidence must exist.
        let mut bad_evidence = publish_params(&ev);
        bad_evidence["evidence"] = json!([rk_core::id::RecordId::new().to_string()]);
        assert!(write(&space, &tickets, "alice", None, "bbs.publish", &bad_evidence).is_err());
        // Evidence must be in the same repository.
        let foreign = evidence_artifact(&space, "other", "ev-foreign");
        let mut cross_repo = publish_params(&ev);
        cross_repo["evidence"] = json!([foreign]);
        assert!(write(&space, &tickets, "alice", None, "bbs.publish", &cross_repo).is_err());
        // Revision must look like a lowercase hex commit id, not any string.
        for bad in ["", "not-hex!", "AB12CD3", "12"] {
            let mut params = publish_params(&ev);
            params["revision"] = json!(bad);
            let err = write(&space, &tickets, "alice", None, "bbs.publish", &params)
                .expect_err(&format!("{bad:?} should be rejected"));
            assert!(err.message.contains("revision"), "{}", err.message);
        }
        // A valid full-length SHA is also accepted (shape check, not just short).
        let mut full_sha = publish_params(&ev);
        full_sha["revision"] = json!("a".repeat(40));
        assert!(write(&space, &tickets, "alice", None, "bbs.publish", &full_sha).is_ok());
    }

    #[test]
    fn reuse_source_accepts_ordinary_and_finding_answer_rejects_receipts_and_assessments() {
        let space = Space::open_in_memory().unwrap();
        let tickets = Tickets::new(space.clone(), "castle".into());
        let ev = evidence_artifact(&space, "repo", "ev1");
        // A Session-lifecycle ordinary artifact (e.g. `rk out artifact`).
        let ordinary = evidence_artifact(&space, "repo", "ordinary");
        // A Furniture-lifecycle ordinary artifact with no bbs_kind (e.g. a
        // daemon/operator-authored gate result) must be reusable too —
        // lifecycle alone must never gate this, only `bbs_kind`.
        let furniture_ordinary = Tuple::new(
            Category::Artifact,
            "repo",
            "gate-result",
            "operator",
            json!({"outcome":"pass"}),
        )
        .with_lifecycle(rk_core::tuple::Lifecycle::Furniture);
        space.out(furniture_ordinary.clone()).unwrap();
        let finding = write(&space, &tickets, "alice", None, "bbs.publish", &publish_params(&ev)).unwrap();
        let question = write(
            &space, &tickets, "alice", None, "bbs.ask",
            &json!({"repo":"repo","task":"parser","text":"Which delimiter?"}),
        ).unwrap();
        let answer = write(
            &space, &tickets, "bob", None, "bbs.answer",
            &json!({"question":question["id"],"text":"Use a newline","artifact":null}),
        ).unwrap();
        let reuse_of_ordinary = |source: &str| {
            write(&space, &tickets, "carol", None, "bbs.reuse",
                &json!({"source":source,"task":"parser","outcome":"used","text":"Applied it","evidence":[ev]}))
        };
        assert!(reuse_of_ordinary(&ordinary).is_ok(), "ordinary Session artifact must be reusable");
        assert!(
            reuse_of_ordinary(&furniture_ordinary.id.to_string()).is_ok(),
            "ordinary Furniture artifact (no bbs_kind) must be reusable regardless of lifecycle"
        );
        assert!(reuse_of_ordinary(finding["id"].as_str().unwrap()).is_ok(), "a finding must be reusable");
        assert!(reuse_of_ordinary(answer["id"].as_str().unwrap()).is_ok(), "an answer must be reusable");
        let receipt = reuse_of_ordinary(&ordinary).unwrap();
        assert!(
            reuse_of_ordinary(receipt["id"].as_str().unwrap()).is_err(),
            "a reuse receipt must not itself be a valid reuse source"
        );
        let assessment = write(
            &space, &tickets, "operator", None, "bbs.assess",
            &json!({"receipt":receipt["id"],"verdict":"verified","reason":"Confirmed independently","evidence":[ev]}),
        ).unwrap();
        assert!(
            reuse_of_ordinary(assessment["id"].as_str().unwrap()).is_err(),
            "an assessment must not itself be a valid reuse source"
        );
        assert!(
            reuse_of_ordinary(question["id"].as_str().unwrap()).is_err(),
            "a question (Category::Need) is not an artifact source"
        );
    }

    #[test]
    fn assess_is_operator_only_and_retains_prior_assessments_by_persistence_order() {
        let space = Space::open_in_memory().unwrap();
        let tickets = Tickets::new(space.clone(), "castle".into());
        let ev = evidence_artifact(&space, "repo", "ev1");
        let ordinary = evidence_artifact(&space, "repo", "ordinary");
        let receipt = write(
            &space, &tickets, "carol", None, "bbs.reuse",
            &json!({"source":ordinary,"task":"parser","outcome":"used","text":"Applied it","evidence":[ev]}),
        ).unwrap();
        let assess = |caller: &str, verdict: &str, reason: &str| {
            write(&space, &tickets, caller, None, "bbs.assess",
                &json!({"receipt":receipt["id"],"verdict":verdict,"reason":reason,"evidence":[ev]}))
        };
        let denied = assess("bob", "verified", "trying to self-grant authority").unwrap_err();
        assert_eq!(denied.code, crate::proto::codes::FORBIDDEN);
        assert!(denied.message.contains("operator"));
        assert!(
            assess("", "unsupported", "the operator can call with an empty caller too").is_ok(),
            "empty caller normalizes to operator, matching the rest of BBS"
        );
        let first = assess("operator", "unsupported", "not enough evidence yet").unwrap();
        assert_eq!(first["written"], true);
        // Retaining prior assessments: a genuinely new verdict/reason is a
        // NEW record, not an overwrite or a rejected conflict.
        let second = assess("operator", "verified", "confirmed after more evidence").unwrap();
        assert_eq!(second["written"], true);
        assert_ne!(second["id"], first["id"]);
        // An exact retry of the second call is idempotent.
        let retry = assess("operator", "verified", "confirmed after more evidence").unwrap();
        assert_eq!(retry["id"], second["id"]);
        assert_eq!(retry["written"], false);
        let shown = show(&space, receipt["id"].as_str().unwrap()).unwrap();
        let assessments = shown["reuse"][0]["assessments"].as_array().unwrap();
        assert_eq!(
            assessments.len(),
            3,
            "all three distinct assessments (empty-caller, first, second) are retained"
        );
        assert_eq!(
            shown["reuse"][0]["current_assessment"]["id"], second["id"],
            "the most recently persisted assessment is current"
        );
    }

    #[test]
    fn assess_requires_an_existing_reuse_receipt() {
        let space = Space::open_in_memory().unwrap();
        let tickets = Tickets::new(space.clone(), "castle".into());
        let ev = evidence_artifact(&space, "repo", "ev1");
        let finding = write(&space, &tickets, "alice", None, "bbs.publish", &publish_params(&ev)).unwrap();
        let err = write(
            &space, &tickets, "operator", None, "bbs.assess",
            &json!({"receipt":finding["id"],"verdict":"verified","reason":"not a receipt","evidence":[ev]}),
        )
        .unwrap_err();
        assert!(err.message.contains("reuse receipt"));
    }

    #[test]
    fn publish_and_reuse_identity_is_generation_bound_but_ask_semantics_are_unchanged() {
        let space = Space::open_in_memory().unwrap();
        let tickets = Tickets::new(space.clone(), "castle".into());
        let ev = evidence_artifact(&space, "repo", "ev1");
        let gen1 = agent_record("alice", "parser");
        let gen2 = agent_record("alice", "parser");
        assert_ne!(gen1.spawn, gen2.spawn, "test fixture must use distinct generations");

        let first = write(&space, &tickets, "alice", Some(&gen1), "bbs.publish", &publish_params(&ev)).unwrap();
        let retry_same_gen = write(&space, &tickets, "alice", Some(&gen1), "bbs.publish", &publish_params(&ev)).unwrap();
        assert_eq!(retry_same_gen["id"], first["id"], "a retry from the same generation is idempotent");
        assert_eq!(retry_same_gen["written"], false);
        let other_gen = write(&space, &tickets, "alice", Some(&gen2), "bbs.publish", &publish_params(&ev)).unwrap();
        assert_ne!(
            other_gen["id"], first["id"],
            "a byte-identical publish from a DIFFERENT generation must not inherit the predecessor's tuple"
        );
        assert_eq!(other_gen["written"], true);

        // Historical `ask` semantics must not change: a content-identical
        // retry from a different generation still collapses into the same
        // durable question (this is what lets a respawned agent's retry
        // survive a restart without creating a duplicate question).
        let ask_params = json!({"repo":"repo","task":"parser","text":"Which grammar?"});
        let q1 = write(&space, &tickets, "alice", Some(&gen1), "bbs.ask", &ask_params).unwrap();
        let q2 = write(&space, &tickets, "alice", Some(&gen2), "bbs.ask", &ask_params).unwrap();
        assert_eq!(q1["id"], q2["id"], "ask must still collapse across generations");
        assert_eq!(q2["written"], false);
    }

    #[test]
    fn reuse_requires_own_task_when_caller_is_a_supervised_agent() {
        let space = Space::open_in_memory().unwrap();
        let tickets = Tickets::new(space.clone(), "castle".into());
        let ev = evidence_artifact(&space, "repo", "ev1");
        let ordinary = evidence_artifact(&space, "repo", "ordinary");
        let alice = agent_record("alice", "parser");
        let honest = write(&space, &tickets, "alice", Some(&alice), "bbs.reuse",
            &json!({"source":ordinary,"task":"parser","outcome":"used","text":"Applied it","evidence":[ev]}));
        assert!(honest.is_ok());
        let dishonest = write(&space, &tickets, "alice", Some(&alice), "bbs.reuse",
            &json!({"source":ordinary,"task":"someone-elses-task","outcome":"used","text":"Applied it","evidence":[ev]}));
        let err = dishonest.unwrap_err();
        assert_eq!(err.code, crate::proto::codes::FORBIDDEN);
    }

    #[test]
    fn show_threads_reuse_onto_ordinary_artifact_and_onto_an_answer() {
        let space = Space::open_in_memory().unwrap();
        let tickets = Tickets::new(space.clone(), "castle".into());
        let ev = evidence_artifact(&space, "repo", "ev1");
        let ordinary = evidence_artifact(&space, "repo", "ordinary");
        let receipt_on_artifact = write(&space, &tickets, "carol", None, "bbs.reuse",
            &json!({"source":ordinary,"task":"parser","outcome":"used","text":"Applied it","evidence":[ev]})).unwrap();
        write(&space, &tickets, "operator", None, "bbs.assess",
            &json!({"receipt":receipt_on_artifact["id"],"verdict":"verified","reason":"Checked","evidence":[ev]})).unwrap();
        let shown_artifact = show(&space, &ordinary).unwrap();
        assert!(shown_artifact.get("question").is_none());
        assert_eq!(shown_artifact["reuse"].as_array().unwrap().len(), 1);
        assert_eq!(
            shown_artifact["reuse"][0]["current_assessment"]["payload"]["verdict"],
            "verified"
        );

        let question = write(&space, &tickets, "alice", None, "bbs.ask",
            &json!({"repo":"repo","task":"parser","text":"Which delimiter?"})).unwrap();
        let answer = write(&space, &tickets, "bob", None, "bbs.answer",
            &json!({"question":question["id"],"text":"Use a newline","artifact":null})).unwrap();
        write(&space, &tickets, "carol", None, "bbs.reuse",
            &json!({"source":answer["id"],"task":"parser","outcome":"confirmed","text":"Matched my case","evidence":[ev]})).unwrap();
        let shown_answer = show(&space, answer["id"].as_str().unwrap()).unwrap();
        assert_eq!(shown_answer["status"], "open", "reuse threading must not fabricate acceptance");
        assert_eq!(shown_answer["reuse"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn current_assessment_follows_persistence_order_not_recordid_order() {
        let space = Space::open_in_memory().unwrap();
        let tickets = Tickets::new(space.clone(), "castle".into());
        let ev = evidence_artifact(&space, "repo", "ev1");
        let ordinary = evidence_artifact(&space, "repo", "ordinary");
        let receipt = write(&space, &tickets, "carol", None, "bbs.reuse",
            &json!({"source":ordinary,"task":"parser","outcome":"used","text":"Applied it","evidence":[ev]})).unwrap();
        let receipt_id = receipt["id"].as_str().unwrap().to_string();
        // `minted_first` is created (and so gets a lower/earlier RecordId)
        // before `minted_second`, but is PERSISTED after it — the exact
        // inversion a delayed concurrent writer can produce. `current` must
        // follow real commit order, not RecordId order.
        let minted_first = Tuple::new(
            Category::Artifact, "repo", "bbs-assessment-a", "operator",
            json!({"schema_version":1,"bbs_kind":"assessment","agent":"operator","spawn":null,
                   "task":"parser","receipt":receipt_id,"verdict":"unsupported",
                   "reason":"minted first, persisted second","evidence":[ev]}),
        ).with_lifecycle(rk_core::tuple::Lifecycle::Furniture);
        let minted_second = Tuple::new(
            Category::Artifact, "repo", "bbs-assessment-b", "operator",
            json!({"schema_version":1,"bbs_kind":"assessment","agent":"operator","spawn":null,
                   "task":"parser","receipt":receipt_id,"verdict":"verified",
                   "reason":"minted second, persisted first","evidence":[ev]}),
        ).with_lifecycle(rk_core::tuple::Lifecycle::Furniture);
        assert!(minted_first.id < minted_second.id);
        space.out(minted_second.clone()).unwrap();
        space.out(minted_first.clone()).unwrap();
        let shown = show(&space, &receipt_id).unwrap();
        assert_eq!(
            shown["reuse"][0]["current_assessment"]["id"],
            minted_first.id.to_string(),
            "the record persisted LAST is current, even though it has the SMALLER RecordId"
        );
    }
}

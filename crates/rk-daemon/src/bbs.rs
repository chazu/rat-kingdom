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
        // `brief` computes the selection; capture is the caller's decision,
        // because only the caller knows which surface and which generation
        // this selection was prepared for.
        telemetry: None,
        exposure: None,
    })
}

/// Who a prepared selection or explicit read was prepared FOR.
///
/// The daemon derives this from its own authenticated view of the caller — an
/// agent process never supplies it. When an exact agent generation cannot be
/// established the binding is recorded explicitly as operator/unbound rather
/// than guessed, so a report can exclude it from agent exposure rates instead
/// of silently attributing it to someone.
#[derive(Debug, Clone, Default)]
pub struct ConsumerBinding {
    pub agent: Option<String>,
    pub spawn: Option<String>,
    pub task: Option<String>,
}

impl ConsumerBinding {
    pub fn operator() -> Self {
        Self::default()
    }

    pub fn agent(name: &str, spawn: &str, task: Option<&str>) -> Self {
        Self {
            agent: Some(name.to_string()),
            spawn: Some(spawn.to_string()),
            task: task.map(str::to_string),
        }
    }

    /// `agent` only when BOTH a name and an exact generation are known. A name
    /// without a `SpawnId` cannot distinguish a namesake predecessor from this
    /// generation, so it is deliberately not agent-bound.
    fn bound(&self) -> &'static str {
        match (&self.agent, &self.spawn) {
            (Some(_), Some(_)) => "agent",
            (Some(_), None) => "unbound",
            (None, _) => "operator",
        }
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "agent": self.agent,
            "spawn": self.spawn,
            "bound": self.bound(),
        })
    }
}

/// The outcome of a telemetry capture. A failure NEVER propagates into the
/// read or launch it describes; it is reported so the gap is known-missing
/// rather than mistaken for a known-negative.
#[derive(Debug, Clone)]
pub struct Capture {
    pub status: rk_core::bbs::TelemetryStatus,
    pub record: Option<String>,
}

impl Capture {
    fn recorded(id: rk_core::id::RecordId) -> Self {
        Self {
            status: rk_core::bbs::TelemetryStatus::Recorded,
            record: Some(id.to_string()),
        }
    }

    pub fn failed() -> Self {
        Self {
            status: rk_core::bbs::TelemetryStatus::Failed,
            record: None,
        }
    }

    pub fn is_failed(&self) -> bool {
        self.status == rk_core::bbs::TelemetryStatus::Failed
    }
}

/// Record that this exact bounded selection was PREPARED for `binding` at
/// `surface`. Returns a capture outcome; it never returns an error, because no
/// telemetry failure may turn a successful briefing or launch into a failure.
///
/// An empty selection is recorded as an exposure with zero entries, which is
/// what makes "nothing relevant was available" distinguishable from "no record
/// was ever written" — the first is an empty `entries` array, the second is
/// the absence of any exposure tuple (or, when capture itself failed, a
/// `telemetry_gap`).
pub fn record_exposure(
    space: &Space,
    castle: &str,
    surface: rk_core::bbs::ExposureSurface,
    binding: &ConsumerBinding,
    briefing: &Briefing,
) -> Capture {
    let entries: Vec<_> = briefing
        .entries
        .iter()
        .map(|entry| {
            serde_json::json!({
                "source": entry.id,
                "reason": entry.reason,
                "kind": entry.kind,
                "category": entry.category,
            })
        })
        .collect();
    let payload = serde_json::json!({
        "schema_version": 1,
        "bbs_kind": "exposure",
        "surface": surface.as_str(),
        "repo": briefing.repo,
        "task": briefing.task,
        "agent": binding.agent,
        "spawn": binding.spawn,
        "bound": binding.bound(),
        "entries": entries,
        "prepared": briefing.entries.len(),
        "omitted": briefing.omitted,
        "cursor": briefing.cursor,
        "since": briefing.since,
        // Stated in the record itself so no consumer has to rediscover it:
        // preparing a selection is not delivering it to a model.
        "semantics": "prepared",
    });
    write_telemetry(
        space,
        castle,
        &briefing.repo,
        &format!("bbs-exposure-{}", surface.as_str()),
        payload,
        serde_json::json!({
            "surface": surface.as_str(),
            "repo": briefing.repo,
            "task": briefing.task,
            "binding": binding.json(),
        }),
    )
}

/// Record that an authenticated caller's explicit `bbs show` request for
/// `source` was served. Requested/prepared, never comprehended.
///
/// `dedup_key` pairs the source with the consumer GENERATION, which is exactly
/// the pair the report deduplicates on: a generation that opens the same source
/// five times leaves five retained records carrying one dedup key.
pub fn record_open(
    space: &Space,
    castle: &str,
    binding: &ConsumerBinding,
    source: &Tuple,
) -> Capture {
    let dedup_key = format!(
        "{}:{}",
        source.id,
        binding.spawn.clone().unwrap_or_else(|| format!(
            "unbound:{}",
            binding.agent.as_deref().unwrap_or("operator")
        ))
    );
    let payload = serde_json::json!({
        "schema_version": 1,
        "bbs_kind": "open",
        "source": source.id.to_string(),
        "source_kind": source.payload["bbs_kind"],
        "repo": source.scope,
        "agent": binding.agent,
        "spawn": binding.spawn,
        "task": binding.task,
        "bound": binding.bound(),
        "dedup_key": dedup_key,
        "semantics": "requested",
    });
    write_telemetry(
        space,
        castle,
        &source.scope,
        "bbs-open",
        payload,
        serde_json::json!({
            "surface": "show",
            "repo": source.scope,
            "source": source.id.to_string(),
            "binding": binding.json(),
        }),
    )
}

/// Commit one daemon-authored telemetry record, falling back to a durable
/// `telemetry_gap` when the record itself cannot be written.
///
/// Records are immutable `Furniture` Events authored by the castle, never by
/// the agent whose context they describe, and each write mints a fresh tuple:
/// repeats are RETAINED (the report deduplicates on source/generation) rather
/// than collapsed into one, so a re-read is visible as a re-read.
fn write_telemetry(
    space: &Space,
    castle: &str,
    scope: &str,
    identity: &str,
    payload: serde_json::Value,
    gap_context: serde_json::Value,
) -> Capture {
    let tuple = Tuple::new(
        rk_core::tuple::Category::Event,
        scope.to_string(),
        identity.to_string(),
        castle.to_string(),
        payload,
    )
    .with_lifecycle(rk_core::tuple::Lifecycle::Furniture);
    let id = tuple.id;
    match space.out(tuple) {
        Ok(()) => Capture::recorded(id),
        Err(error) => {
            tracing::warn!(%error, identity, "BBS telemetry capture failed; work is unaffected");
            let gap = Tuple::new(
                rk_core::tuple::Category::Event,
                scope.to_string(),
                "bbs-telemetry-gap",
                castle.to_string(),
                serde_json::json!({
                    "schema_version": 1,
                    "bbs_kind": "telemetry_gap",
                    "missing": identity,
                    "error": error.to_string(),
                    "context": gap_context,
                }),
            )
            .with_lifecycle(rk_core::tuple::Lifecycle::Furniture);
            // The store that just refused the record will usually refuse this
            // too; the in-process return value is the reliable signal and the
            // gap tuple is the durable one when it survives.
            if let Err(error) = space.out(gap) {
                tracing::warn!(%error, "BBS telemetry gap record could not be persisted either");
            }
            Capture::failed()
        }
    }
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
                    rk_core::bbs::is_assessment(t) && t.payload["receipt"] == receipt.id.to_string()
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportParams {
    pub repo: String,
    /// Resume point: the last `commit_sequence` a prior page returned.
    #[serde(default)]
    pub after: Option<u64>,
    #[serde(default = "default_export_limit")]
    pub limit: usize,
}

fn default_export_limit() -> usize {
    500
}

const MAX_EXPORT_LIMIT: usize = 2000;
/// References are resolved with bounded, indexed `get`s. The cap keeps a page
/// of densely cross-linked records from turning into an unbounded fan-out.
const MAX_EXPORT_REFERENCES: usize = 4000;

/// Payload keys that name another tuple in the same repository. Every one of
/// these must either appear in the exported page, be resolved and appended as
/// a reference, or be reported under `missing_references` — a reference is
/// never silently dropped.
const REFERENCE_KEYS: &[&str] = &[
    "source",
    "receipt",
    "question",
    "answer",
    "contribution",
    "source_artifact",
];

/// A bounded, read-only capture of one repository's records in ACTUAL
/// persistence order, for the offline evidence report.
///
/// Why this exists rather than `rk scan`: a scan returns `{"tuples": [...]}`
/// in `RecordId`/ULID order, which a delayed writer can invert, so it cannot
/// establish which of two assessments was persisted last. This reads the
/// immutable journal's `commit_sequence` and says so in the envelope. The
/// historical `tuples` array is retained as the record list so an existing
/// consumer keeps working; `order` is what makes the ordering claim
/// authoritative, and a consumer must not infer persistence order from a plain
/// scan that lacks it.
///
/// Bounded by construction: scope, cursor and limit are pushed into SQL before
/// any payload is deserialized (see [`Space::persistence_page`]), so a 371k
/// event journal costs a page, not a journal. Truncation is always reported.
pub fn export(space: &Space, params: &ExportParams) -> rk_core::Result<serde_json::Value> {
    if params.repo.trim().is_empty() || !(1..=MAX_EXPORT_LIMIT).contains(&params.limit) {
        return Err(rk_core::Error::other(format!(
            "repo is required and limit must be 1..{MAX_EXPORT_LIMIT}"
        )));
    }
    let page = space.persistence_page(&params.repo, params.after, params.limit)?;
    let present: HashSet<_> = page.entries.iter().map(|(_, t)| t.id).collect();
    let mut wanted: Vec<rk_core::id::RecordId> = Vec::new();
    let mut seen = HashSet::new();
    let mut unresolvable: Vec<String> = Vec::new();
    let mut reference_budget_exhausted = false;
    for (_, tuple) in &page.entries {
        if !is_bbs_record(tuple) {
            continue;
        }
        let mut raw: Vec<String> = REFERENCE_KEYS
            .iter()
            .filter_map(|key| tuple.payload[*key].as_str().map(str::to_string))
            .collect();
        if let Some(evidence) = tuple.payload["evidence"].as_array() {
            raw.extend(
                evidence
                    .iter()
                    .filter_map(|v| v.as_str().map(str::to_string)),
            );
        }
        for id in raw {
            match id.parse::<rk_core::id::RecordId>() {
                Ok(parsed) if present.contains(&parsed) => {}
                Ok(parsed) => {
                    if !seen.insert(parsed) {
                        continue;
                    }
                    if wanted.len() >= MAX_EXPORT_REFERENCES {
                        reference_budget_exhausted = true;
                        unresolvable.push(id);
                        continue;
                    }
                    wanted.push(parsed);
                }
                // A malformed reference cannot be resolved; report it rather
                // than dropping it or failing the whole export.
                Err(_) => unresolvable.push(id),
            }
        }
    }
    let mut references = Vec::new();
    let mut missing = unresolvable;
    for id in wanted {
        match space.get(id)? {
            // A reference into another repository is reported, never exported:
            // a bounded per-repo capture must not leak a foreign scope.
            Some(tuple) if tuple.scope == params.repo => references.push(tuple),
            Some(_) | None => missing.push(id.to_string()),
        }
    }
    let sequences = space.commit_sequences(&references.iter().map(|t| t.id).collect::<Vec<_>>())?;
    let records: Vec<serde_json::Value> = page
        .entries
        .iter()
        .map(|(sequence, tuple)| {
            let mut value = serde_json::to_value(tuple).unwrap_or(serde_json::Value::Null);
            value["commit_sequence"] = serde_json::json!(sequence);
            value
        })
        .collect();
    let reference_records: Vec<serde_json::Value> = references
        .iter()
        .map(|tuple| {
            let mut value = serde_json::to_value(tuple).unwrap_or(serde_json::Value::Null);
            // A live row a reference resolved to always has a commit sequence;
            // `null` means the row exists but its order is unknown, which a
            // consumer must treat as unordered rather than as sequence zero.
            value["commit_sequence"] = serde_json::json!(sequences.get(&tuple.id));
            value
        })
        .collect();
    missing.sort();
    missing.dedup();
    Ok(serde_json::json!({
        "schema_version": 1,
        "kind": "bbs.export",
        "repo": params.repo,
        "build": rk_core::version::BUILD_VERSION,
        "captured_at": chrono::Utc::now().to_rfc3339(),
        // The ordering claim this surface exists to make. A consumer that does
        // not see exactly this string must not assume persistence order.
        "order": "tuple_persistence_events.commit_sequence ascending",
        "boundary": page.boundary,
        "after": params.after.unwrap_or(0),
        "next_cursor": page.next_cursor,
        "limit": params.limit,
        "truncated": page.more,
        // Historical `rk scan` shape: an object carrying a `tuples` array.
        "tuples": records,
        "references": reference_records,
        "coverage": {
            "tuples": records.len(),
            "references": reference_records.len(),
            "missing_references": missing,
            "complete": !page.more && missing.is_empty() && !reference_budget_exhausted,
            "reference_budget_exhausted": reference_budget_exhausted,
            "scope": params.repo,
        },
    }))
}

/// Whether a tuple is one of the BBS record kinds whose payload may name other
/// tuples that the export must carry or report.
fn is_bbs_record(tuple: &Tuple) -> bool {
    rk_core::bbs::is_question(tuple)
        || rk_core::bbs::is_answer(tuple)
        || rk_core::bbs::is_acceptance(tuple)
        || rk_core::bbs::is_finding(tuple)
        || rk_core::bbs::is_reuse(tuple)
        || rk_core::bbs::is_assessment(tuple)
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
        || areas.iter().any(|a| a.trim().is_empty() || a.len() > 512)
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
        // unlike ask/answer/accept above, a byte-identical republish from a
        // REPLACEMENT generation (a new `SpawnId`, e.g. a fresh `agent.spawn`
        // after the original was abandoned) must not silently inherit a
        // predecessor's tuple, since author-generation is load-bearing
        // evidence for the reuse trial. This is NOT triggered by `rk
        // respawn`/`agent.respawn`, which deliberately continues the SAME
        // `SpawnId` (`Supervisor::respawn_mode` reads `record.spawn_id()`
        // unchanged) — that case is and must remain an idempotent retry.
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
                return Err(invalid("repo/task required; key must be 1..256 bytes"));
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
            if !matches!(
                p.outcome.as_str(),
                "used" | "adapted" | "confirmed" | "rejected"
            ) {
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
            // An ordinary artifact is reusable regardless of lifecycle only
            // when `bbs_kind` is genuinely ABSENT — a daemon/operator-authored
            // Furniture artifact (e.g. a gate result) carries useful
            // reproduction evidence just as much as a Session-lifecycle one.
            // A record that CARRIES a `bbs_kind` must satisfy the real
            // `is_finding`/`is_answer` predicates (category + Furniture
            // lifecycle + exact string), not merely have that string
            // somewhere in its payload — otherwise a forged Session-lifecycle
            // artifact claiming `"bbs_kind":"finding"`, or a non-string
            // `bbs_kind`, would pass as a legitimate typed record it is not.
            let bbs_kind_present = source.payload.get("bbs_kind").is_some();
            let ok = source.category == Category::Artifact
                && (!bbs_kind_present
                    || rk_core::bbs::is_finding(&source)
                    || rk_core::bbs::is_answer(&source));
            if !ok {
                return Err(invalid(
                    "reuse source must be an ordinary artifact or a finding/answer, not a receipt, assessment, or telemetry record",
                ));
            }
            check_repo(record, &source.scope)?;
            check_evidence(space, &p.evidence, &source.scope)?;
            let task_ticket = tickets.resolve(&p.task)?;
            if task_ticket
                .as_ref()
                .is_some_and(|t| t.scope != source.scope)
            {
                return Err(invalid("task belongs to a different repository"));
            }
            let task = task_ticket.map_or(p.task.clone(), |t| t.identity);
            if let Some(record) = record {
                let owned = record.task.as_deref().unwrap_or("");
                if !tickets.id_spellings(owned)?.contains(&task) {
                    return Err(forbidden("reuse must reference your own assigned task"));
                }
            }
            // The consuming TASK is part of the receipt's logical identity,
            // not just its payload: two different tasks genuinely reusing the
            // same source the same way (same outcome/text/evidence/key) must
            // get two distinct receipts, never collide into one retry.
            let key = rk_core::action::canonical_digest(&json!([
                caller,
                spawn,
                "reuse",
                source.id,
                task,
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
        let ok = write(
            &space,
            &tickets,
            "alice",
            None,
            "bbs.publish",
            &publish_params(&ev),
        )
        .unwrap_or_else(|e| panic!("{}", e.message));
        assert_eq!(ok["kind"], "finding");
        assert_eq!(ok["written"], true);
        // Exact retry is idempotent: same content, same tuple.
        let retry = write(
            &space,
            &tickets,
            "alice",
            None,
            "bbs.publish",
            &publish_params(&ev),
        )
        .unwrap();
        assert_eq!(retry["id"], ok["id"]);
        assert_eq!(retry["written"], false);
        // At least one area is required.
        let mut no_areas = publish_params(&ev);
        no_areas["areas"] = json!([]);
        assert!(write(&space, &tickets, "alice", None, "bbs.publish", &no_areas).is_err());
        // Evidence must exist.
        let mut bad_evidence = publish_params(&ev);
        bad_evidence["evidence"] = json!([rk_core::id::RecordId::new().to_string()]);
        assert!(write(
            &space,
            &tickets,
            "alice",
            None,
            "bbs.publish",
            &bad_evidence
        )
        .is_err());
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
        let finding = write(
            &space,
            &tickets,
            "alice",
            None,
            "bbs.publish",
            &publish_params(&ev),
        )
        .unwrap();
        let question = write(
            &space,
            &tickets,
            "alice",
            None,
            "bbs.ask",
            &json!({"repo":"repo","task":"parser","text":"Which delimiter?"}),
        )
        .unwrap();
        let answer = write(
            &space,
            &tickets,
            "bob",
            None,
            "bbs.answer",
            &json!({"question":question["id"],"text":"Use a newline","artifact":null}),
        )
        .unwrap();
        let reuse_of_ordinary = |source: &str| {
            write(
                &space,
                &tickets,
                "carol",
                None,
                "bbs.reuse",
                &json!({"source":source,"task":"parser","outcome":"used","text":"Applied it","evidence":[ev]}),
            )
        };
        assert!(
            reuse_of_ordinary(&ordinary).is_ok(),
            "ordinary Session artifact must be reusable"
        );
        assert!(
            reuse_of_ordinary(&furniture_ordinary.id.to_string()).is_ok(),
            "ordinary Furniture artifact (no bbs_kind) must be reusable regardless of lifecycle"
        );
        assert!(
            reuse_of_ordinary(finding["id"].as_str().unwrap()).is_ok(),
            "a finding must be reusable"
        );
        assert!(
            reuse_of_ordinary(answer["id"].as_str().unwrap()).is_ok(),
            "an answer must be reusable"
        );
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
    fn reuse_rejects_forged_typed_sources_and_binds_identity_to_task() {
        let space = Space::open_in_memory().unwrap();
        let tickets = Tickets::new(space.clone(), "castle".into());
        let ev = evidence_artifact(&space, "repo", "ev1");
        // A Session-lifecycle artifact merely CLAIMING `"bbs_kind":"finding"`
        // is not what `bbs.publish` ever produces (always Furniture) — it
        // must be rejected exactly like a non-string `bbs_kind`, not waved
        // through because the string happens to read "finding".
        let forged_finding = Tuple::new(
            Category::Artifact,
            "repo",
            "forged",
            "someone",
            json!({"bbs_kind":"finding","text":"not a real finding"}),
        );
        space.out(forged_finding.clone()).unwrap();
        let non_string_kind = Tuple::new(
            Category::Artifact,
            "repo",
            "non-string-kind",
            "someone",
            json!({"bbs_kind":123}),
        );
        space.out(non_string_kind.clone()).unwrap();
        let reuse = |source: &str, task: &str| {
            write(
                &space,
                &tickets,
                "operator",
                None,
                "bbs.reuse",
                &json!({"source":source,"task":task,"outcome":"used","text":"x","evidence":[ev]}),
            )
        };
        assert!(
            reuse(&forged_finding.id.to_string(), "task-a").is_err(),
            "a forged Session-lifecycle 'finding' must not pass as a typed source"
        );
        assert!(
            reuse(&non_string_kind.id.to_string(), "task-a").is_err(),
            "a non-string bbs_kind must not pass as an ordinary artifact"
        );

        // Cross-repo task: a task ticket that actually resolves, but to a
        // DIFFERENT repository than the source, must be rejected — not just
        // an unresolvable string (which harmlessly falls back to itself).
        let ordinary = evidence_artifact(&space, "repo", "ordinary");
        let foreign_task = Tuple::new(
            Category::Task,
            "otherrepo",
            "OTHER-1",
            "operator",
            json!({"title":"x"}),
        );
        space.out(foreign_task.clone()).unwrap();
        let cross_repo = reuse(&ordinary, "OTHER-1").unwrap_err();
        assert!(
            cross_repo.message.contains("different repository"),
            "{}",
            cross_repo.message
        );

        // The consuming TASK is part of the receipt's logical identity: two
        // different tasks reusing the same source the same way (identical
        // outcome/text/evidence/key) must get two DISTINCT receipts, and an
        // exact retry under the SAME task must still collapse.
        let for_task_a = reuse(&ordinary, "task-a").unwrap();
        let retry_task_a = reuse(&ordinary, "task-a").unwrap();
        assert_eq!(
            retry_task_a["id"], for_task_a["id"],
            "same task, same content: idempotent"
        );
        assert_eq!(retry_task_a["written"], false);
        let for_task_b = reuse(&ordinary, "task-b").unwrap();
        assert_ne!(
            for_task_b["id"], for_task_a["id"],
            "a different consuming task must not receive the first task's receipt"
        );
        assert_eq!(for_task_b["written"], true);
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
            write(
                &space,
                &tickets,
                caller,
                None,
                "bbs.assess",
                &json!({"receipt":receipt["id"],"verdict":verdict,"reason":reason,"evidence":[ev]}),
            )
        };
        let denied = assess("bob", "verified", "trying to self-grant authority").unwrap_err();
        assert_eq!(denied.code, crate::proto::codes::FORBIDDEN);
        assert!(denied.message.contains("operator"));
        assert!(
            assess(
                "",
                "unsupported",
                "the operator can call with an empty caller too"
            )
            .is_ok(),
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
        let finding = write(
            &space,
            &tickets,
            "alice",
            None,
            "bbs.publish",
            &publish_params(&ev),
        )
        .unwrap();
        let err = write(
            &space, &tickets, "operator", None, "bbs.assess",
            &json!({"receipt":finding["id"],"verdict":"verified","reason":"not a receipt","evidence":[ev]}),
        )
        .unwrap_err();
        assert!(err.message.contains("reuse receipt"));
    }

    #[test]
    fn publish_identity_is_bound_to_a_replacement_generation_but_ask_semantics_are_unchanged() {
        let space = Space::open_in_memory().unwrap();
        let tickets = Tickets::new(space.clone(), "castle".into());
        let ev = evidence_artifact(&space, "repo", "ev1");
        // Two distinct `SpawnId`s model a REPLACEMENT generation (a fresh
        // `agent.spawn` after the original was abandoned/lost) — not `rk
        // respawn`/`agent.respawn`, which deliberately continues the SAME
        // `SpawnId` (`Supervisor::respawn_mode` reads `record.spawn_id()`
        // unchanged) and is exactly the same-generation retry case asserted
        // via `retry_same_gen` below.
        let original_generation = agent_record("alice", "parser");
        let replacement_generation = agent_record("alice", "parser");
        assert_ne!(
            original_generation.spawn, replacement_generation.spawn,
            "test fixture must use distinct generations"
        );

        let first = write(
            &space,
            &tickets,
            "alice",
            Some(&original_generation),
            "bbs.publish",
            &publish_params(&ev),
        )
        .unwrap();
        let retry_same_gen = write(
            &space,
            &tickets,
            "alice",
            Some(&original_generation),
            "bbs.publish",
            &publish_params(&ev),
        )
        .unwrap();
        assert_eq!(
            retry_same_gen["id"], first["id"],
            "a retry from the same generation (incl. after `rk respawn`) is idempotent"
        );
        assert_eq!(retry_same_gen["written"], false);
        let other_gen = write(
            &space,
            &tickets,
            "alice",
            Some(&replacement_generation),
            "bbs.publish",
            &publish_params(&ev),
        )
        .unwrap();
        assert_ne!(
            other_gen["id"], first["id"],
            "a byte-identical publish from a REPLACEMENT generation must not inherit the predecessor's tuple"
        );
        assert_eq!(other_gen["written"], true);

        // Historical `ask` semantics must not change: a content-identical
        // retry from a different generation still collapses into the same
        // durable question (this is what lets a REPLACEMENT generation's
        // retry — a fresh `agent.spawn` after the original was lost, not a
        // same-`SpawnId` `rk respawn` — survive without creating a duplicate
        // question).
        let ask_params = json!({"repo":"repo","task":"parser","text":"Which grammar?"});
        let q1 = write(
            &space,
            &tickets,
            "alice",
            Some(&original_generation),
            "bbs.ask",
            &ask_params,
        )
        .unwrap();
        let q2 = write(
            &space,
            &tickets,
            "alice",
            Some(&replacement_generation),
            "bbs.ask",
            &ask_params,
        )
        .unwrap();
        assert_eq!(
            q1["id"], q2["id"],
            "ask must still collapse across generations"
        );
        assert_eq!(q2["written"], false);
    }

    #[test]
    fn reuse_requires_own_task_when_caller_is_a_supervised_agent() {
        let space = Space::open_in_memory().unwrap();
        let tickets = Tickets::new(space.clone(), "castle".into());
        let ev = evidence_artifact(&space, "repo", "ev1");
        let ordinary = evidence_artifact(&space, "repo", "ordinary");
        let alice = agent_record("alice", "parser");
        let honest = write(
            &space,
            &tickets,
            "alice",
            Some(&alice),
            "bbs.reuse",
            &json!({"source":ordinary,"task":"parser","outcome":"used","text":"Applied it","evidence":[ev]}),
        );
        assert!(honest.is_ok());
        let dishonest = write(
            &space,
            &tickets,
            "alice",
            Some(&alice),
            "bbs.reuse",
            &json!({"source":ordinary,"task":"someone-elses-task","outcome":"used","text":"Applied it","evidence":[ev]}),
        );
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

        let question = write(
            &space,
            &tickets,
            "alice",
            None,
            "bbs.ask",
            &json!({"repo":"repo","task":"parser","text":"Which delimiter?"}),
        )
        .unwrap();
        let answer = write(
            &space,
            &tickets,
            "bob",
            None,
            "bbs.answer",
            &json!({"question":question["id"],"text":"Use a newline","artifact":null}),
        )
        .unwrap();
        write(&space, &tickets, "carol", None, "bbs.reuse",
            &json!({"source":answer["id"],"task":"parser","outcome":"confirmed","text":"Matched my case","evidence":[ev]})).unwrap();
        let shown_answer = show(&space, answer["id"].as_str().unwrap()).unwrap();
        assert_eq!(
            shown_answer["status"], "open",
            "reuse threading must not fabricate acceptance"
        );
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
        // Fix the IDs so this inversion does not depend on the clock or
        // random ordering of ULIDs minted within the same millisecond.
        // The lower ID is persisted last, as a delayed writer can cause.
        let mut minted_first = Tuple::new(
            Category::Artifact,
            "repo",
            "bbs-assessment-a",
            "operator",
            json!({"schema_version":1,"bbs_kind":"assessment","agent":"operator","spawn":null,
                   "task":"parser","receipt":receipt_id,"verdict":"unsupported",
                   "reason":"minted first, persisted second","evidence":[ev]}),
        )
        .with_lifecycle(rk_core::tuple::Lifecycle::Furniture);
        let mut minted_second = Tuple::new(
            Category::Artifact,
            "repo",
            "bbs-assessment-b",
            "operator",
            json!({"schema_version":1,"bbs_kind":"assessment","agent":"operator","spawn":null,
                   "task":"parser","receipt":receipt_id,"verdict":"verified",
                   "reason":"minted second, persisted first","evidence":[ev]}),
        )
        .with_lifecycle(rk_core::tuple::Lifecycle::Furniture);
        minted_first.id = "01ARZ3NDEKTSV4RRFFQ69G5FA0".parse().unwrap();
        minted_second.id = "01ARZ3NDEKTSV4RRFFQ69G5FA1".parse().unwrap();
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

    use rk_core::bbs::{ExposureSurface, TelemetryStatus};

    fn exposures(space: &Space) -> Vec<Tuple> {
        space
            .scan(&Pattern::category(Category::Event).scope("repo"))
            .unwrap()
            .into_iter()
            .filter(rk_core::bbs::is_exposure)
            .collect()
    }

    #[test]
    fn exposure_records_the_exact_selection_and_distinguishes_empty_from_absent() {
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

        // Before anything relevant exists, the selection is genuinely EMPTY.
        let params = BriefParams::for_task("repo", &task.identity);
        let empty = brief(&space, &tickets, &params).unwrap();
        assert!(empty.entries.is_empty());
        let binding = ConsumerBinding::agent("Scurry-15", "spawn-1", Some(&task.identity));
        let capture = record_exposure(&space, "castle", ExposureSurface::Spawn, &binding, &empty);
        assert_eq!(capture.status, TelemetryStatus::Recorded);

        let recorded = exposures(&space);
        assert_eq!(recorded.len(), 1, "an empty selection is still an exposure");
        let payload = &recorded[0].payload;
        assert_eq!(payload["bbs_kind"], "exposure");
        assert_eq!(payload["surface"], "spawn");
        assert_eq!(payload["semantics"], "prepared");
        assert_eq!(payload["spawn"], "spawn-1");
        assert_eq!(payload["bound"], "agent");
        assert_eq!(payload["entries"].as_array().unwrap().len(), 0);
        assert_eq!(payload["prepared"], 0);
        assert_eq!(
            recorded[0].instance, "castle",
            "the castle authors the record, never the agent it describes"
        );

        // A real peer post makes the next selection non-empty, and the record
        // names the exact source id and the exact reason that selected it.
        let post = Tuple::new(
            Category::Artifact,
            "repo",
            "peer-note",
            "peer",
            json!({"task":task.identity,"summary":"a reproduction"}),
        );
        space.out(post.clone()).unwrap();
        let filled = brief(&space, &tickets, &params).unwrap();
        record_exposure(&space, "castle", ExposureSurface::Brief, &binding, &filled);
        let brief_record = exposures(&space)
            .into_iter()
            .find(|t| t.payload["surface"] == "brief")
            .expect("brief surface recorded");
        let entries = brief_record.payload["entries"].as_array().unwrap().clone();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["source"], post.id.to_string());
        assert_eq!(entries[0]["reason"], "task or dependency");

        // An exposure is measurement metadata: it must never come back as a
        // peer finding, however well it matches the task.
        let after = brief(&space, &tickets, &params).unwrap();
        assert!(
            after
                .entries
                .iter()
                .all(|e| e.id != brief_record.id.to_string()),
            "telemetry must not surface as a useful peer post"
        );
    }

    #[test]
    fn open_binds_the_caller_and_deduplicates_by_source_and_generation() {
        let space = Space::open_in_memory().unwrap();
        let source = Tuple::new(Category::Artifact, "repo", "note", "peer", json!({}));
        space.out(source.clone()).unwrap();

        let agent = ConsumerBinding::agent("Scurry-15", "spawn-1", Some("TKT-a"));
        let first = record_open(&space, "castle", &agent, &source);
        let second = record_open(&space, "castle", &agent, &source);
        assert_ne!(
            first.record, second.record,
            "repeat reads are retained as separate records"
        );

        let opens: Vec<_> = space
            .scan(&Pattern::category(Category::Event).scope("repo"))
            .unwrap()
            .into_iter()
            .filter(rk_core::bbs::is_open)
            .collect();
        assert_eq!(opens.len(), 2);
        let keys: HashSet<_> = opens
            .iter()
            .map(|t| t.payload["dedup_key"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            keys.len(),
            1,
            "both retained reads share one source/generation dedup key"
        );
        assert_eq!(opens[0].payload["semantics"], "requested");
        assert_eq!(opens[0].payload["source"], source.id.to_string());

        // A different generation of the SAME agent name is a different
        // consumer and must not collapse into the first one's key.
        let successor = ConsumerBinding::agent("Scurry-15", "spawn-2", Some("TKT-a"));
        record_open(&space, "castle", &successor, &source);
        // An operator read is recorded explicitly as unbound, so a report can
        // exclude it from agent exposure rates rather than misattribute it.
        record_open(&space, "castle", &ConsumerBinding::operator(), &source);
        let opens: Vec<_> = space
            .scan(&Pattern::category(Category::Event).scope("repo"))
            .unwrap()
            .into_iter()
            .filter(rk_core::bbs::is_open)
            .collect();
        let keys: HashSet<_> = opens
            .iter()
            .map(|t| t.payload["dedup_key"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(keys.len(), 3);
        assert!(opens
            .iter()
            .any(|t| t.payload["bound"] == "operator" && t.payload["agent"].is_null()));
    }

    #[test]
    fn export_states_its_order_boundary_truncation_and_reference_coverage() {
        let space = Space::open_in_memory().unwrap();
        let tickets = Tickets::new(space.clone(), "castle".into());
        let ev = evidence_artifact(&space, "repo", "evidence");
        let finding = write(
            &space,
            &tickets,
            "alice",
            None,
            "bbs.publish",
            &publish_params(&ev),
        )
        .unwrap();
        let finding_id = finding["id"].as_str().unwrap().to_string();
        // A record in ANOTHER repository must never appear in this capture.
        space
            .out(Tuple::new(
                Category::Artifact,
                "other-repo",
                "foreign",
                "peer",
                json!({}),
            ))
            .unwrap();

        let full = export(
            &space,
            &ExportParams {
                repo: "repo".into(),
                after: None,
                limit: 500,
            },
        )
        .unwrap();
        assert_eq!(
            full["order"], "tuple_persistence_events.commit_sequence ascending",
            "the envelope makes its ordering claim explicit"
        );
        assert_eq!(full["truncated"], false);
        assert_eq!(full["coverage"]["complete"], true);
        assert!(full["boundary"].as_u64().unwrap() > 0);
        let tuples = full["tuples"].as_array().unwrap();
        assert!(
            tuples.iter().all(|t| t["scope"] == "repo"),
            "a bounded per-repo capture never leaks a foreign scope"
        );
        assert!(tuples.iter().any(|t| t["id"] == finding_id.as_str()));
        // Persistence order is carried per record, ascending.
        let sequences: Vec<u64> = tuples
            .iter()
            .map(|t| t["commit_sequence"].as_u64().unwrap())
            .collect();
        assert!(sequences.windows(2).all(|w| w[0] < w[1]));

        // A page smaller than the scope reports truncation and a resume point
        // rather than letting a short page imply completeness.
        let page = export(
            &space,
            &ExportParams {
                repo: "repo".into(),
                after: None,
                limit: 1,
            },
        )
        .unwrap();
        assert_eq!(page["truncated"], true);
        assert_eq!(page["coverage"]["complete"], false);
        assert_eq!(page["tuples"].as_array().unwrap().len(), 1);
        let next = page["next_cursor"].as_u64().unwrap();
        let second = export(
            &space,
            &ExportParams {
                repo: "repo".into(),
                after: Some(next),
                limit: 1,
            },
        )
        .unwrap();
        assert_ne!(second["tuples"][0]["id"], page["tuples"][0]["id"]);

        // A finding whose evidence points outside the page must have that
        // reference resolved and carried, never silently dropped.
        let narrow = export(
            &space,
            &ExportParams {
                repo: "repo".into(),
                after: Some(next),
                limit: 500,
            },
        )
        .unwrap();
        let carried = narrow["tuples"]
            .as_array()
            .unwrap()
            .iter()
            .chain(narrow["references"].as_array().unwrap())
            .any(|t| t["id"] == ev.as_str());
        assert!(carried, "evidence is exported or reported, never dropped");

        // An unresolvable reference is reported explicitly.
        let dangling = Tuple::new(
            Category::Artifact,
            "repo",
            "bbs-reuse-dangling",
            "peer",
            json!({"schema_version":1,"bbs_kind":"reuse","agent":"peer","task":"t",
                   "source":finding_id,"outcome":"used","text":"x",
                   "evidence":["01ARZ3NDEKTSV4RRFFQ69G5FAV"]}),
        )
        .with_lifecycle(rk_core::tuple::Lifecycle::Furniture);
        space.out(dangling).unwrap();
        let with_gap = export(
            &space,
            &ExportParams {
                repo: "repo".into(),
                after: None,
                limit: 500,
            },
        )
        .unwrap();
        assert!(with_gap["coverage"]["missing_references"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "01ARZ3NDEKTSV4RRFFQ69G5FAV"));
        assert_eq!(with_gap["coverage"]["complete"], false);

        // Limits are bounded, and an out-of-range request is refused rather
        // than silently clamped into an unbounded read.
        assert!(export(
            &space,
            &ExportParams {
                repo: "repo".into(),
                after: None,
                limit: 0,
            },
        )
        .is_err());
    }
}

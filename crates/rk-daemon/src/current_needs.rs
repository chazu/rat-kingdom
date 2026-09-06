//! Resolve current landing incidents from durable ticket delivery, preserving
//! the original need as history. The caller verifies Git before suppressing a
//! candidate resolution; absence or ambiguity of evidence always keeps the row.

use crate::tickets::{delivery_of, Tickets};
use chrono::{DateTime, Utc};
use rk_core::{
    id::RecordId,
    tuple::{Category, Tuple},
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Immutable identity written at the escalation producer, including the exact
/// agent generation when present. A later incident is a new tuple, even when
/// its task or prose is identical.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct LandingIncident {
    pub branch: String,
    pub target: String,
    pub head_sha: String,
    #[serde(default)]
    pub source_spawn: Option<rk_core::id::SpawnId>,
}

impl LandingIncident {
    fn valid(&self) -> bool {
        [&self.branch, &self.target, &self.head_sha]
            .iter()
            .all(|s| !s.trim().is_empty())
    }
}

pub(crate) struct ResolutionCandidate {
    pub need_id: RecordId,
    pub repo: String,
    pub merge_commit: String,
    pub target: String,
}

/// A delivery of the same ticket onto the incident's intended target settles
/// older incidents, including work salvaged by a replacement generation on a
/// different source branch. Merely closing the ticket is insufficient.
pub(crate) fn resolution_candidates(
    needs: &[Tuple],
    processed: &[Tuple],
    tickets: &Tickets,
    history_truncated: bool,
) -> rk_core::Result<Vec<ResolutionCandidate>> {
    let mut candidates = Vec::new();
    for need in needs {
        if need.category != Category::Need
            || need.identity != "steward"
            || need.instance != "daemon"
        {
            continue;
        }
        let Some(task) = need.payload.get("task").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(ticket) = tickets.get(task)? else {
            continue;
        };
        if ticket.scope != need.scope {
            continue;
        }
        let Some(delivery) = delivery_of(&ticket) else {
            continue;
        };
        if delivery.merge_commit.trim().is_empty() {
            continue;
        }
        let Ok(landed_at) = DateTime::parse_from_rfc3339(&delivery.landed_at) else {
            continue;
        };
        let landed_at = landed_at.with_timezone(&Utc);
        if need.created_at >= landed_at {
            continue;
        }
        let spellings = tickets.id_spellings(task)?;
        let incident = match need.payload.get("landing_incident") {
            Some(value) => serde_json::from_value::<LandingIncident>(value.clone()).ok(),
            None if !history_truncated => {
                legacy_incident(need, needs, processed, &spellings, landed_at)
            }
            None => None,
        };
        let Some(incident) = incident.filter(LandingIncident::valid) else {
            continue;
        };
        if incident.target != delivery.target {
            continue;
        }
        candidates.push(ResolutionCandidate {
            need_id: need.id,
            repo: need.scope.clone(),
            merge_commit: delivery.merge_commit,
            target: delivery.target,
        });
    }
    Ok(candidates)
}

/// Old daemon needs carried only a task. Their compatibility binding must be
/// unambiguous in durable structured history: exactly one held candidate after
/// this need and before either the next need for that task or its delivery.
/// No text, branch naming convention, ticket title or default target is read.
/// Incomplete history or multiple possible candidate identities cannot retire
/// the row. New producers never depend on this compatibility inference.
fn legacy_incident(
    need: &Tuple,
    needs: &[Tuple],
    processed: &[Tuple],
    spellings: &[String],
    delivered_at: DateTime<Utc>,
) -> Option<LandingIncident> {
    let same_task = |row: &Tuple| {
        row.scope == need.scope
            && row
                .payload
                .get("task")
                .and_then(|v| v.as_str())
                .is_some_and(|task| spellings.iter().any(|id| id == task))
    };
    if needs
        .iter()
        .any(|row| same_task(row) && row.id != need.id && row.created_at == need.created_at)
    {
        return None;
    }
    let until = needs
        .iter()
        .filter(|row| same_task(row) && row.id != need.id && row.created_at > need.created_at)
        .map(|row| row.created_at)
        .min()
        .unwrap_or(delivered_at)
        .min(delivered_at);
    let candidates = processed
        .iter()
        .filter(|row| {
            row.category == Category::Event
                && row.identity == "landing_processed"
                && row.instance == "daemon"
                && same_task(row)
                && row.created_at >= need.created_at
                && row.created_at < until
                && matches!(
                    row.payload.get("outcome").and_then(|v| v.as_str()),
                    Some("gate-held" | "rework-filed" | "escalated" | "no-gate")
                )
        })
        .map(|row| {
            serde_json::from_value::<LandingIncident>(row.payload.clone())
                .ok()
                .filter(LandingIncident::valid)
        })
        .collect::<Option<HashSet<_>>>()?;
    if candidates.len() == 1 {
        candidates.into_iter().next()
    } else {
        None
    }
}

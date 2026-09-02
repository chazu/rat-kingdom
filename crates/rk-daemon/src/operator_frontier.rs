//! Bounded, cross-repository ready-work projection for the King.
//!
//! The native ticket store remains authoritative. This module owns the
//! information-preserving compression needed by the bounded King pull:
//! exact totals and a full identity digest, per-repository summaries, and a
//! small representative set.

use rk_core::tuple::Tuple;
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, VecDeque};

pub(crate) const MAX_READY_REPRESENTATIVES: usize = 20;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ReadyRepository {
    pub(crate) repo: String,
    pub(crate) count: usize,
    pub(crate) oldest_ticket: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ReadyFrontier {
    pub(crate) total: usize,
    pub(crate) digest: String,
    pub(crate) truncated: bool,
    pub(crate) repositories: Vec<ReadyRepository>,
    pub(crate) representatives: Vec<Tuple>,
}

pub(crate) fn build(ready: Vec<Tuple>, limit: usize) -> ReadyFrontier {
    let total = ready.len();
    let mut identities = ready
        .iter()
        .map(|ticket| (ticket.scope.clone(), ticket.identity.clone()))
        .collect::<Vec<_>>();
    identities.sort();
    let digest = hex::encode(Sha256::digest(
        serde_json::to_vec(&identities).expect("ready identity pairs serialize"),
    ));

    let mut by_repo: BTreeMap<String, VecDeque<Tuple>> = BTreeMap::new();
    for ticket in ready {
        by_repo
            .entry(ticket.scope.clone())
            .or_default()
            .push_back(ticket);
    }
    let repositories = by_repo
        .iter()
        .map(|(repo, tickets)| ReadyRepository {
            repo: repo.clone(),
            count: tickets.len(),
            oldest_ticket: tickets
                .front()
                .expect("ready repository has at least one ticket")
                .identity
                .clone(),
        })
        .collect::<Vec<_>>();

    // Round-robin across repositories while each repository's own queue stays
    // FIFO. A deep backlog can consume the remaining slots only after every
    // other ready repository has contributed one representative.
    let mut representatives = Vec::with_capacity(limit.min(total));
    while representatives.len() < limit {
        let mut advanced = false;
        for tickets in by_repo.values_mut() {
            if representatives.len() == limit {
                break;
            }
            if let Some(ticket) = tickets.pop_front() {
                representatives.push(ticket);
                advanced = true;
            }
        }
        if !advanced {
            break;
        }
    }
    ReadyFrontier {
        total,
        digest,
        truncated: total > representatives.len(),
        repositories,
        representatives,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rk_core::tuple::Category;
    use serde_json::json;

    fn ticket(repo: &str, identity: &str) -> Tuple {
        Tuple::new(
            Category::Task,
            repo,
            identity,
            "castle",
            json!({"status": "open", "title": identity}),
        )
    }

    #[test]
    fn deep_backlog_cannot_hide_another_repositories_ready_ticket() {
        let mut ready = (0..25)
            .map(|index| ticket("alpha", &format!("TKT-alpha-{index:02}")))
            .collect::<Vec<_>>();
        ready.push(ticket("glossolalia", "TKT-glossolalia-00"));

        let frontier = build(ready, MAX_READY_REPRESENTATIVES);

        assert_eq!(frontier.total, 26);
        assert!(frontier.truncated);
        assert_eq!(frontier.repositories[0].count, 25);
        assert_eq!(frontier.repositories[1].count, 1);
        assert!(frontier
            .representatives
            .iter()
            .any(|ticket| ticket.scope == "glossolalia"));
    }

    #[test]
    fn full_digest_changes_when_only_a_hidden_identity_changes() {
        let first = (0..25)
            .map(|index| ticket("alpha", &format!("TKT-alpha-{index:02}")))
            .collect::<Vec<_>>();
        let mut second = first.clone();
        second[24] = ticket("alpha", "TKT-alpha-replaced");

        let first = build(first, MAX_READY_REPRESENTATIVES);
        let second = build(second, MAX_READY_REPRESENTATIVES);

        assert_eq!(first.representatives, second.representatives);
        assert_ne!(first.digest, second.digest);
    }
}

//! The continuous-drain controller: a WIP-limited fleet autoscaler.
//!
//! Where a `backlog-drain` workflow fans out ONCE over the ready backlog, this
//! REFILLS continuously. It maintains a target live-agent concurrency `W`
//! ([`DrainConfig::max_wip`]): each cycle it counts the live rats, and while
//! fewer than `W` are running and the ready backlog is non-empty it claims the
//! highest-priority ready ticket and spawns a rat for it. That turns "keep the
//! fleet busy" from one operator spawn per ticket into a single config dial;
//! combined with the steward closing each merged item it is a closed loop — the
//! operator grooms and prioritises, the fleet executes.
//!
//! # Cadence
//!
//! The loop wakes on the tuple feed (a completion or dismissal frees a slot) and
//! on a fallback interval, exactly like the [reactor]. The feed is only a wake
//! signal — every cycle recomputes live count and readiness from scratch, so a
//! dropped feed event just delays a refill by at most one interval. A cycle that
//! finds the fleet full is a cheap no-op (two scans, no writes), so the loop
//! quiesces once `W` rats are live and re-arms the moment one finishes.
//!
//! [reactor]: crate::reactor
//!
//! # Safety (all inherited, none re-invented)
//!
//! - **Never double-grabs a ticket.** Each candidate is atomically claimed
//!   (`open` → `in_progress`) via [`Tickets::claim`] *before* its rat spawns —
//!   the same compare-and-set the workflow fan-out uses (TKT-6). A lost claim is
//!   simply skipped, so a drain racing another drain (or a fan-out) never
//!   dispatches one ticket twice.
//! - **Never drains the wallet.** A read-only [`Supervisor::would_exceed_budget`]
//!   preflight skips a ticket whose repo/fleet cap is already hit (so it is not
//!   claimed-then-orphaned), and `Supervisor::spawn` re-checks the hierarchical
//!   cap authoritatively (TKT-16) and refuses over-budget dispatch regardless.
//! - **Never spawns blindly into a hang.** The liveness sweep (TKT-15) reaps
//!   stuck/runaway rats, which frees their WIP slot for the next refill.
//!
//! # Priority and aging
//!
//! Ready tickets are ordered by an *effective* priority: the base level
//! (`high` = 2, `normal` = 1, `low` = 0) plus an aging bonus that grows with how
//! long the ticket has waited, so a low-priority ticket cannot starve forever
//! behind a steady stream of higher-priority work. [`DrainConfig::aging_secs`]
//! sets how much waiting buys one level of boost; zero disables aging (strict
//! priority, oldest ticket first). Ties break on ticket id (FIFO).

use crate::repos::RepoRegistry;
use crate::supervisor::{SpawnParams, Supervisor};
use crate::tickets::{Tickets, ID_PREFIX};
use chrono::{DateTime, Utc};
use rk_core::config::DrainConfig;
use rk_core::paths::Layout;
use rk_core::tuple::Tuple;
use serde_json::Value;
use std::sync::Arc;
use tracing::{info, warn};

pub struct Drain {
    supervisor: Arc<Supervisor>,
    tickets: Arc<Tickets>,
    layout: Layout,
    config: DrainConfig,
}

impl Drain {
    pub fn new(
        supervisor: Arc<Supervisor>,
        tickets: Arc<Tickets>,
        layout: Layout,
        config: DrainConfig,
    ) -> Self {
        Self {
            supervisor,
            tickets,
            layout,
            config,
        }
    }

    /// One refill pass. Returns how many rats it spawned this cycle.
    pub async fn run_cycle(&self) -> rk_core::Result<usize> {
        self.run_cycle_at(Utc::now()).await
    }

    /// [`run_cycle`](Self::run_cycle) with an injectable "now" for aging tests.
    pub async fn run_cycle_at(&self, now: DateTime<Utc>) -> rk_core::Result<usize> {
        let max_wip = self.config.max_wip;
        if max_wip == 0 {
            return Ok(0);
        }

        // Count the live rats fleet-wide. This is the whole registry, not just
        // drain-spawned rats: a WIP cap governs total concurrency, so an operator
        // spawn or a workflow fan-out counts against it too.
        let live = self
            .supervisor
            .list()
            .iter()
            .filter(|r| r.state.is_live())
            .count();
        if live >= max_wip {
            return Ok(0);
        }
        let mut slots = max_wip - live;

        // Reload the repo registry from disk each cycle so a repo registered
        // after the daemon booted is picked up (mirrors the scheduler).
        let registry = RepoRegistry::load(&self.layout.home().join("repos.json"))?;

        // Dependency-aware ready backlog, optionally pinned to one repo scope,
        // ranked by effective (aged) priority — strongest first.
        let mut ready = self.tickets.ready(self.config.repo.clone())?;
        ready.sort_by(|a, b| {
            let (sa, sb) = (self.score(a, now), self.score(b, now));
            // Highest score first; FIFO (oldest id) on a tie.
            sb.partial_cmp(&sa)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| id_num(&a.identity).cmp(&id_num(&b.identity)))
        });

        let mut spawned = 0usize;
        for ticket in &ready {
            if slots == 0 {
                break;
            }
            // A ticket's scope is its repo *name*; skip any that does not resolve
            // to a registered repo (e.g. a system-scope ticket has no worktree to
            // dispatch into).
            let Some(repo) = registry.get(&ticket.scope) else {
                continue;
            };
            // Preflight the budget so we do not claim a ticket we cannot spawn.
            // A per-repo cap only blocks that repo; the fleet cap blocks every
            // candidate — `continue` handles both without over-claiming.
            if self.supervisor.would_exceed_budget(&ticket.scope) {
                continue;
            }
            // Atomic claim before spawn: if a concurrent drain/fan-out already
            // took it we lose the race and skip, so no ticket is double-grabbed.
            if !self.tickets.claim(&ticket.identity).await? {
                continue;
            }

            let repo_path = repo.path.to_string_lossy().to_string();
            let params = SpawnParams {
                repo: repo_path,
                // Task IS the ticket id: the supervisor keys the ticket's status
                // lifecycle (→ done on clean finish, → closed on merge) and the
                // branch name on `task.starts_with("TKT-")`.
                task: ticket.identity.clone(),
                prompt: Some(ticket_prompt(ticket)),
                role: "rat".to_string(),
                harness: None,
                parent: None,
                base: None,
                model: None,
                permission_mode: None,
                attach: false,
                // Drain dispatches standalone tickets, never a workflow instance,
                // so there is no per-instance budget scope (TKT-32) to key on.
                workflow_instance: None,
                instance_max_usd: None,
            };
            match self.supervisor.spawn(params) {
                Ok(record) => {
                    info!(ticket = %ticket.identity, agent = %record.name, "drain dispatched ready ticket");
                    spawned += 1;
                    slots -= 1;
                }
                Err(e) => {
                    // The authoritative in-spawn budget guard (or a transient git
                    // failure) refused this dispatch after we claimed the ticket.
                    // Leave it `in_progress` (parity with fan-out's documented
                    // claimed-then-refused semantics) and stop this cycle: a hit
                    // cap will refuse every remaining candidate too.
                    warn!(ticket = %ticket.identity, error = %e, "drain spawn refused; stopping cycle");
                    break;
                }
            }
        }
        Ok(spawned)
    }

    /// Effective priority of a ticket: base level plus an aging bonus. `now` is
    /// injectable so aging is testable without sleeping.
    fn score(&self, ticket: &Tuple, now: DateTime<Utc>) -> f64 {
        let base = priority_rank(field(&ticket.payload, "priority"));
        if self.config.aging_secs == 0 {
            return base;
        }
        // The tuple's `created_at` is preserved across ticket edits/claims
        // (`with_payload` keeps it), so it is a stable wait-time origin.
        let age_secs = (now - ticket.created_at).num_seconds().max(0) as f64;
        base + age_secs / self.config.aging_secs as f64
    }
}

/// Base priority level: `high` = 2, `low` = 0, everything else (incl. the
/// default `normal` and any unknown value) = 1.
fn priority_rank(priority: &str) -> f64 {
    match priority {
        "high" => 2.0,
        "low" => 0.0,
        _ => 1.0,
    }
}

/// The dispatch prompt for a ticket: its title, then its body if any. The rat
/// also receives the ticket id as `RK_TASK`, so the prompt carries the substance
/// and the env carries the identity.
fn ticket_prompt(ticket: &Tuple) -> String {
    let title = field(&ticket.payload, "title");
    let body = field(&ticket.payload, "body");
    if body.is_empty() {
        title.to_string()
    } else {
        format!("{title}\n\n{body}")
    }
}

fn field<'a>(payload: &'a Value, key: &str) -> &'a str {
    payload.get(key).and_then(Value::as_str).unwrap_or_default()
}

/// Numeric part of a `TKT-<n>` id, for a FIFO tiebreak (0 if unparseable).
fn id_num(identity: &str) -> u64 {
    identity
        .strip_prefix(ID_PREFIX)
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rk_space::Space;
    use serde_json::json;

    fn drain(aging_secs: u64) -> Drain {
        let space = Space::open_in_memory().unwrap();
        let tickets = Arc::new(Tickets::new(space.clone(), "castle".into()));
        let layout = Layout::at(std::path::Path::new("/tmp/rk-drain-test"));
        let supervisor = Arc::new(
            Supervisor::new(
                layout.clone(),
                "castle".into(),
                "fake".into(),
                rk_ledger::Budget::default(),
                rk_ledger::FleetBudget::default(),
                space,
                tickets.clone(),
            )
            .unwrap(),
        );
        Drain::new(
            supervisor,
            tickets,
            layout,
            DrainConfig {
                enabled: true,
                max_wip: 2,
                interval_secs: 30,
                repo: None,
                aging_secs,
            },
        )
    }

    fn ticket(id: &str, priority: &str, created_at: DateTime<Utc>) -> Tuple {
        let mut t = Tuple::new(
            rk_core::tuple::Category::Task,
            "repo",
            id,
            "castle",
            json!({"title": id, "priority": priority, "status": "open"}),
        );
        t.created_at = created_at;
        t
    }

    #[test]
    fn strict_priority_orders_high_over_low() {
        let d = drain(0); // aging disabled
        let now = Utc::now();
        let high = ticket("TKT-2", "high", now);
        let low = ticket("TKT-1", "low", now);
        assert!(d.score(&high, now) > d.score(&low, now));
    }

    #[test]
    fn aging_lets_a_waiting_low_ticket_overtake_a_fresh_high() {
        let d = drain(3600); // one level per hour of waiting
        let now = Utc::now();
        // Low ticket has waited 3h (+3.0 aging) → 0 + 3 = 3.0.
        let stale_low = ticket("TKT-1", "low", now - chrono::Duration::hours(3));
        // Fresh high → 2 + ~0 = 2.0.
        let fresh_high = ticket("TKT-2", "high", now);
        assert!(
            d.score(&stale_low, now) > d.score(&fresh_high, now),
            "a low ticket that waited long enough must overtake a fresh high one"
        );
    }

    #[test]
    fn aging_disabled_keeps_strict_priority_regardless_of_age() {
        let d = drain(0);
        let now = Utc::now();
        let ancient_low = ticket("TKT-1", "low", now - chrono::Duration::days(30));
        let fresh_normal = ticket("TKT-2", "normal", now);
        assert!(
            d.score(&fresh_normal, now) > d.score(&ancient_low, now),
            "with aging off, age never boosts a lower priority"
        );
    }

    #[test]
    fn prompt_combines_title_and_body() {
        let mut t = ticket("TKT-1", "normal", Utc::now());
        t.payload["body"] = json!("cache the API layer");
        assert_eq!(ticket_prompt(&t), "TKT-1\n\ncache the API layer");
        // No body → title only.
        let bare = ticket("TKT-2", "normal", Utc::now());
        assert_eq!(ticket_prompt(&bare), "TKT-2");
    }
}

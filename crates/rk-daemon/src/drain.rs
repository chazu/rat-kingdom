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
use crate::tickets::Tickets;
use crate::workflow_exec::is_fleet_wip_refusal;
use chrono::{DateTime, Utc};
use rk_core::config::DrainConfig;
use rk_core::paths::Layout;
use rk_core::tuple::Tuple;
use rk_workflow::{
    resolve::{resolve_fields, ResolvedAgent},
    AgentProfile, TierRouting,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

pub struct Drain {
    supervisor: Arc<Supervisor>,
    tickets: Arc<Tickets>,
    layout: Layout,
    config: DrainConfig,
    /// Cost-tier routing (global `[tiers]`): a drained ticket's labels/priority
    /// pick a tier (an `[agents.<tier>]` profile), exactly as the workflow
    /// fan-out routes. Empty = no routing, every ticket drains on the default.
    tier_routing: TierRouting,
    /// Named agent profiles (`[agents.<name>]`) a resolved tier binds to.
    global_agents: HashMap<String, AgentProfile>,
    /// The harness a spawn falls back to when no profile/tier names one.
    default_harness: String,
}

impl Drain {
    pub fn new(
        supervisor: Arc<Supervisor>,
        tickets: Arc<Tickets>,
        layout: Layout,
        config: DrainConfig,
        tier_routing: TierRouting,
        global_agents: HashMap<String, AgentProfile>,
        default_harness: String,
    ) -> Self {
        Self {
            supervisor,
            tickets,
            layout,
            config,
            tier_routing,
            global_agents,
            default_harness,
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

        // Count the live rats fleet-wide AND per repo. This is the whole
        // registry, not just drain-spawned rats: a WIP cap governs total
        // concurrency, so an operator spawn or a workflow fan-out counts against
        // it too. The per-repo tally seeds the cross-repo partition (each repo's
        // slots are its cap minus what it already holds).
        //
        // This snapshot (and the `slots` countdown below) is a cheap estimate
        // to size this cycle's candidate loop, NOT the authoritative gate: a
        // workflow `spawn` step can admit against the same fleet cap between
        // this read and this cycle's `spawn_async` calls below, which is why
        // each of those calls re-checks atomically against the live registry
        // lock (`Registry::try_reserve_wip`) and a refusal there stops the
        // cycle regardless of what `slots` still says.
        let mut live = 0usize;
        let mut live_by_repo: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for record in self.supervisor.list().iter().filter(|r| r.state.is_live()) {
            live += 1;
            *live_by_repo.entry(record.repo_name.clone()).or_insert(0) += 1;
        }
        if live >= max_wip {
            return Ok(0);
        }
        let mut slots = max_wip - live;

        // Reload the repo registry from disk each cycle so a repo registered
        // after the daemon booted is picked up (mirrors the scheduler).
        let registry = RepoRegistry::load(&self.layout.home().join("repos.json"))?;

        // In partition mode the `repos` map is the allowlist, so scan every
        // repo's backlog and filter per-ticket; otherwise honour the single-repo
        // pin (or drain all) exactly as before.
        let ready_scope = if self.config.repos.is_empty() {
            self.config.repo.clone()
        } else {
            None
        };

        // Dependency-aware ready backlog, optionally pinned to one repo scope,
        // ranked by effective (aged) priority — strongest first.
        let mut ready = self.tickets.ready(ready_scope)?;
        ready.sort_by(|a, b| {
            let (sa, sb) = (self.score(a, now), self.score(b, now));
            // Highest score first; FIFO (oldest ticket) on a tie.
            sb.partial_cmp(&sa)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.created_at.cmp(&b.created_at))
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
            // Cross-repo partition: in allowlist mode a repo absent or disabled
            // from the `repos` map is not drained, and a repo already holding its
            // per-repo cap is skipped (but other repos' tickets still dispatch,
            // hence `continue` not `break`) so no single repo monopolizes the
            // fleet. `None` = this repo must be skipped this cycle.
            let Some(repo_cap) = self.repo_slot_policy(&ticket.scope) else {
                continue;
            };
            if let Some(cap) = repo_cap {
                let held = *live_by_repo.get(&ticket.scope).unwrap_or(&0);
                if held >= cap {
                    continue;
                }
            }
            // Preflight the budget so we do not claim a ticket we cannot spawn.
            // A per-repo cap only blocks that repo; the fleet cap blocks every
            // candidate — `continue` handles both without over-claiming.
            if self.supervisor.would_exceed_budget(&ticket.scope) {
                continue;
            }
            // Route this ticket to a cost tier from its labels/priority and
            // resolve the harness/model/permissions the same way the workflow
            // fan-out does. Done *before* the claim so a config error (unknown
            // tier) surfaces without orphaning a claimed ticket; an unknown tier
            // errors out the whole cycle exactly like an unknown profile.
            let resolved = self.resolve_tier(ticket)?;
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
                coordination: None,
                harness: Some(resolved.harness),
                parent: None,
                base: None,
                model: resolved.model,
                permission_mode: resolved.permission_mode,
                attach: false,
                // Drain dispatches standalone tickets, never a workflow instance,
                // so there is no per-instance budget scope (TKT-32) to key on.
                workflow_instance: None,
                coordinator: None,
                instance_max_usd: None,
            };
            // `max_wip` is passed through so the supervisor atomically admits
            // this spawn against the SAME fleet-wide ceiling a workflow
            // `spawn` step checks — the `live`/`slots` count above is only a
            // cheap cycle-start estimate; this is the authoritative check
            // (TOCTOU-safe: check-and-reserve happen under one registry lock,
            // see `Registry::try_reserve_wip`). A refusal here means a
            // concurrent admitter (a workflow spawn, or another drain cycle)
            // claimed the slot after our estimate, and is handled the same as
            // any other spawn refusal below: stop this cycle.
            match self.supervisor.spawn_async(params, max_wip).await {
                Ok(record) => {
                    info!(ticket = %ticket.identity, agent = %record.name, "drain dispatched ready ticket");
                    spawned += 1;
                    slots -= 1;
                    // Charge this repo's partition so a second ticket for the
                    // same repo respects its per-repo cap within this cycle.
                    *live_by_repo.entry(ticket.scope.clone()).or_insert(0) += 1;
                }
                Err(e) if is_fleet_wip_refusal(&e) => {
                    // Transient: the fleet-WIP ceiling had no free slot at the
                    // moment `Supervisor::spawn` atomically checked (our own
                    // `slots`/`held` count above is only a cycle-start
                    // estimate, raced by a concurrent admitter). The ticket is
                    // ready work, not broken work — reopening it (rather than
                    // leaving it stranded `in_progress` forever) lets the next
                    // cycle, or another admitter with a free slot, pick it back
                    // up instead of losing it silently.
                    if let Err(reopen_err) = self.tickets.reopen(&ticket.identity, "open").await {
                        warn!(ticket = %ticket.identity, error = %reopen_err, "drain: failed to reopen ticket after fleet-wip refusal; ticket left in_progress");
                    }
                    info!(ticket = %ticket.identity, "drain spawn refused by fleet-wip cap; ticket reopened, stopping cycle");
                    break;
                }
                Err(e) => {
                    // The authoritative in-spawn budget guard (or a non-transient
                    // failure like an unresolvable repo/harness) refused this
                    // dispatch after we claimed the ticket. Leave it `in_progress`
                    // — retrying it automatically could spin on a config error —
                    // and stop this cycle: a hit cap will refuse every remaining
                    // candidate too.
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

    /// Cross-repo partition policy for a repo scope, evaluated per candidate
    /// ticket:
    ///
    /// - `None` — the repo must be **skipped** this cycle: it is not in the
    ///   allowlist, or it is present but disabled.
    /// - `Some(None)` — drain the repo with **no** per-repo cap (bounded only by
    ///   the fleet-wide ceiling). This is always the case in the legacy,
    ///   unpartitioned mode (empty `repos` map).
    /// - `Some(Some(n))` — drain the repo but hold it to at most `n` live rats.
    fn repo_slot_policy(&self, scope: &str) -> Option<Option<usize>> {
        if self.config.repos.is_empty() {
            // Legacy mode: readiness/pin filtering already selected the scope;
            // every candidate is allowed against the shared fleet budget.
            return Some(None);
        }
        match self.config.repos.get(scope) {
            // A 0 cap means "unlimited within the fleet ceiling".
            Some(rc) if rc.enabled => Some((rc.max_wip != 0).then_some(rc.max_wip)),
            // Absent from the allowlist, or explicitly disabled.
            _ => None,
        }
    }

    /// Resolve a ready ticket's harness/model/permissions through the same
    /// tier-routing → profile-layering path the workflow fan-out uses: route the
    /// ticket's labels/priority to a tier (an `[agents.<tier>]` profile), then
    /// resolve that profile over the global agents. A bare drain has no
    /// workflow-scoped agents, so only global profiles apply. An unknown tier
    /// name errors, exactly like an unknown profile in a spawn step.
    fn resolve_tier(&self, ticket: &Tuple) -> rk_core::Result<ResolvedAgent> {
        let labels = string_array(&ticket.payload, "labels");
        let priority = field(&ticket.payload, "priority");
        let tier = self.tier_routing.route(&labels, Some(priority));
        if let Some(tier) = tier {
            info!(ticket = %ticket.identity, tier, "drain routed ticket to cost tier");
        }
        resolve_fields(
            None,
            tier,
            None,
            None,
            None,
            &HashMap::new(),
            &self.global_agents,
            &self.default_harness,
        )
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

/// The string array under `key` in a ticket payload (e.g. `labels`), empty when
/// absent or not an array — mirrors the fan-out's `labels` extraction so a
/// routing rule keys on the same values in the drain as in a workflow.
fn string_array(payload: &Value, key: &str) -> Vec<String> {
    payload
        .get(key)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
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
                repos: std::collections::HashMap::new(),
                aging_secs,
            },
            TierRouting::default(),
            HashMap::new(),
            "fake".into(),
        )
    }

    /// A drain whose per-repo partition map is the given `(repo, enabled, cap)`
    /// entries. `max_wip` is a generous fleet ceiling so the per-repo caps, not
    /// the fleet cap, are what the partition tests exercise.
    fn partitioned_drain(entries: &[(&str, bool, usize)]) -> Drain {
        let d = drain(0);
        let mut config = d.config.clone();
        config.max_wip = 100;
        config.repos = entries
            .iter()
            .map(|(name, enabled, max_wip)| {
                (
                    (*name).to_string(),
                    rk_core::config::RepoDrainConfig {
                        enabled: *enabled,
                        max_wip: *max_wip,
                    },
                )
            })
            .collect();
        Drain::new(
            d.supervisor,
            d.tickets,
            d.layout,
            config,
            d.tier_routing,
            d.global_agents,
            d.default_harness,
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

    /// A drain whose tier table + global profiles route drained tickets, so the
    /// per-ticket `resolve_tier` path can be unit-tested without a live spawn.
    fn tiered_drain(
        rules: &[(Option<&str>, Option<&str>, &str)],
        profiles: &[(&str, Option<&str>, Option<&str>)],
    ) -> Drain {
        let d = drain(0);
        let tier_routing = TierRouting {
            rules: rules
                .iter()
                .map(|(priority, label, tier)| rk_workflow::TierRule {
                    priority: priority.map(String::from),
                    label: label.map(String::from),
                    tier: (*tier).to_string(),
                })
                .collect(),
        };
        let global_agents = profiles
            .iter()
            .map(|(name, harness, model)| {
                (
                    (*name).to_string(),
                    AgentProfile {
                        harness: harness.map(String::from),
                        model: model.map(String::from),
                        permission_mode: None,
                    },
                )
            })
            .collect();
        Drain::new(
            d.supervisor,
            d.tickets,
            d.layout,
            d.config,
            tier_routing,
            global_agents,
            "fake".into(),
        )
    }

    /// A ticket carrying labels, for tier rules that key on a `label`.
    fn labeled_ticket(id: &str, priority: &str, labels: &[&str]) -> Tuple {
        Tuple::new(
            rk_core::tuple::Category::Task,
            "repo",
            id,
            "castle",
            json!({"title": id, "priority": priority, "status": "open", "labels": labels}),
        )
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
    fn empty_partition_map_allows_every_repo_uncapped() {
        // Legacy mode: no `repos` map → every scope drains against the shared
        // fleet budget with no per-repo cap.
        let d = drain(0);
        assert_eq!(d.repo_slot_policy("anything"), Some(None));
    }

    #[test]
    fn partition_map_is_an_allowlist() {
        let d = partitioned_drain(&[("alpha", true, 2)]);
        // Listed + enabled → drained with its cap.
        assert_eq!(d.repo_slot_policy("alpha"), Some(Some(2)));
        // Absent from the allowlist → skipped.
        assert_eq!(d.repo_slot_policy("beta"), None);
    }

    #[test]
    fn disabled_repo_is_skipped_even_when_listed() {
        let d = partitioned_drain(&[("alpha", false, 2)]);
        assert_eq!(d.repo_slot_policy("alpha"), None);
    }

    #[test]
    fn zero_cap_means_unlimited_within_fleet_ceiling() {
        let d = partitioned_drain(&[("alpha", true, 0)]);
        assert_eq!(d.repo_slot_policy("alpha"), Some(None));
    }

    #[test]
    fn label_rule_routes_a_drained_ticket_onto_its_tier_profile() {
        // A `mechanical` label routes to the cheap tier; the drain resolves the
        // spawn onto that profile's harness/model, exactly like a fan-out.
        let d = tiered_drain(
            &[(None, Some("mechanical"), "cheap")],
            &[("cheap", Some("codex"), Some("gpt-5.5-codex"))],
        );
        let resolved = d
            .resolve_tier(&labeled_ticket("TKT-1", "normal", &["mechanical"]))
            .unwrap();
        assert_eq!(resolved.harness, "codex");
        assert_eq!(resolved.model.as_deref(), Some("gpt-5.5-codex"));
    }

    #[test]
    fn priority_rule_routes_a_high_ticket_onto_the_premium_tier() {
        let d = tiered_drain(
            &[(Some("high"), None, "premium")],
            &[("premium", Some("claude"), Some("opus"))],
        );
        let resolved = d
            .resolve_tier(&ticket("TKT-1", "high", Utc::now()))
            .unwrap();
        assert_eq!(resolved.harness, "claude");
        assert_eq!(resolved.model.as_deref(), Some("opus"));
    }

    #[test]
    fn no_matching_rule_falls_back_to_the_default_harness() {
        // A ticket that matches no rule resolves exactly as before the tier layer
        // existed: the global default harness, no model.
        let d = tiered_drain(
            &[(Some("high"), None, "premium")],
            &[("premium", Some("claude"), Some("opus"))],
        );
        let resolved = d
            .resolve_tier(&ticket("TKT-1", "low", Utc::now()))
            .unwrap();
        assert_eq!(resolved.harness, "fake", "falls back to default harness");
        assert_eq!(resolved.model, None);
    }

    #[test]
    fn unknown_tier_name_errors_like_an_unknown_profile() {
        // A rule points at a tier with no `[agents.<tier>]` profile: resolution
        // errors rather than silently falling back, so a config typo is loud.
        let d = tiered_drain(&[(None, None, "ghost")], &[]);
        let err = d
            .resolve_tier(&ticket("TKT-1", "normal", Utc::now()))
            .unwrap_err();
        assert!(
            err.to_string().contains("unknown tier profile 'ghost'"),
            "unexpected error: {err}"
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

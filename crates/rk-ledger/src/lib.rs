//! Cost ledger: tokens→USD pricing and budget policy.
//!
//! Harnesses that self-report USD (Claude Code) are authoritative; for the
//! rest (codex, axe) cost is computed from token deltas against a pricing
//! table. The vendored table is a curated subset of LiteLLM's
//! `model_prices_and_context_window.json` (the de facto standard ccusage and
//! OpenCode also use); `merge_pricing_json` layers a runtime refresh or user
//! overrides on top.

pub mod pricing;

use rk_harness::TokenUsage;
use serde::{Deserialize, Serialize};

/// Per-token USD prices for one model.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct ModelPrice {
    pub input_cost_per_token: f64,
    pub output_cost_per_token: f64,
    #[serde(default)]
    pub cache_read_input_token_cost: f64,
    #[serde(default)]
    pub cache_creation_input_token_cost: f64,
}

impl ModelPrice {
    /// ccusage/LiteLLM convention: cached reads bill at the cache-read rate,
    /// uncached input at the input rate, reasoning tokens count as output
    /// (already folded into `output` by the adapters).
    pub fn cost(&self, usage: &TokenUsage) -> f64 {
        usage.input as f64 * self.input_cost_per_token
            + usage.output as f64 * self.output_cost_per_token
            + usage.cache_read as f64 * self.cache_read_input_token_cost
            + usage.cache_creation as f64 * self.cache_creation_input_token_cost
    }
}

/// Budget thresholds for one agent/task. Zero = unlimited.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Budget {
    pub max_usd: f64,
    pub max_tokens: u64,
    /// Fraction of the cap at which a warning fires (default 0.8).
    pub warn_at: f64,
}

/// Graduated budget decision, checked after every usage update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetAction {
    Ok,
    /// Crossed the warn threshold: post an obstacle tuple / steer.
    Warn,
    /// Crossed the cap: interrupt/kill the agent.
    Stop,
}

impl Budget {
    pub fn check(&self, spent_usd: f64, spent_tokens: u64) -> BudgetAction {
        let warn_frac = if self.warn_at > 0.0 {
            self.warn_at
        } else {
            0.8
        };
        let over = |spent: f64, cap: f64| cap > 0.0 && spent >= cap;
        let warn = |spent: f64, cap: f64| cap > 0.0 && spent >= cap * warn_frac;

        if over(spent_usd, self.max_usd) || over(spent_tokens as f64, self.max_tokens as f64) {
            BudgetAction::Stop
        } else if warn(spent_usd, self.max_usd) || warn(spent_tokens as f64, self.max_tokens as f64)
        {
            BudgetAction::Warn
        } else {
            BudgetAction::Ok
        }
    }
}

/// Fleet/repo caps layered ABOVE the per-agent [`Budget`]. Where `Budget`
/// governs one agent's own spend (warn→steer→kill mid-run), these govern the
/// SUM of every agent's cost — fleet-wide and per-repo — and are enforced as a
/// *dispatch preflight*: once the running total reaches the cap, new spawns are
/// refused rather than a live agent killed. This is the wallet kill-switch that
/// lets a continuous autoscaler or nightly run go unattended without draining
/// the account. Zero on either cap = unlimited.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct FleetBudget {
    pub fleet_max_usd: f64,
    pub repo_max_usd: f64,
    /// Fraction of a cap at which dispatch is still allowed but flagged (0.8).
    pub warn_at: f64,
}

/// Which cap bound a dispatch decision, for reporting and obstacle tuples.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetScope {
    Fleet,
    Repo,
}

impl BudgetScope {
    pub fn as_str(self) -> &'static str {
        match self {
            BudgetScope::Fleet => "fleet",
            BudgetScope::Repo => "repo",
        }
    }
}

/// Outcome of a pre-dispatch fleet/repo budget check. `action == Stop` means
/// refuse the spawn; `Warn` means allow it but surface an obstacle.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DispatchCheck {
    pub action: BudgetAction,
    /// The binding cap when `action != Ok`.
    pub scope: Option<BudgetScope>,
    pub spent_usd: f64,
    pub cap_usd: f64,
}

impl DispatchCheck {
    fn ok() -> Self {
        Self {
            action: BudgetAction::Ok,
            scope: None,
            spent_usd: 0.0,
            cap_usd: 0.0,
        }
    }

    /// True when dispatch must be refused.
    pub fn denied(&self) -> bool {
        self.action == BudgetAction::Stop
    }
}

fn severity(a: BudgetAction) -> u8 {
    match a {
        BudgetAction::Ok => 0,
        BudgetAction::Warn => 1,
        BudgetAction::Stop => 2,
    }
}

impl FleetBudget {
    /// Decide whether another spawn is allowed given the current fleet-wide and
    /// per-repo cost rollups. The tightest binding constraint wins; the fleet
    /// cap is reported first on a severity tie since it is the broader
    /// guardrail. Zero caps are skipped (unlimited).
    pub fn check_dispatch(&self, fleet_spent_usd: f64, repo_spent_usd: f64) -> DispatchCheck {
        let warn_frac = if self.warn_at > 0.0 { self.warn_at } else { 0.8 };
        let mut worst = DispatchCheck::ok();
        for (scope, spent, cap) in [
            (BudgetScope::Fleet, fleet_spent_usd, self.fleet_max_usd),
            (BudgetScope::Repo, repo_spent_usd, self.repo_max_usd),
        ] {
            if cap <= 0.0 {
                continue;
            }
            let action = if spent >= cap {
                BudgetAction::Stop
            } else if spent >= cap * warn_frac {
                BudgetAction::Warn
            } else {
                BudgetAction::Ok
            };
            if severity(action) > severity(worst.action) {
                worst = DispatchCheck {
                    action,
                    scope: Some(scope),
                    spent_usd: spent,
                    cap_usd: cap,
                };
            }
        }
        worst
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usage(input: u64, output: u64, cache_read: u64) -> TokenUsage {
        TokenUsage {
            input,
            output,
            cache_read,
            cache_creation: 0,
        }
    }

    #[test]
    fn cost_formula_matches_convention() {
        let price = ModelPrice {
            input_cost_per_token: 3e-6,
            output_cost_per_token: 15e-6,
            cache_read_input_token_cost: 0.3e-6,
            cache_creation_input_token_cost: 3.75e-6,
        };
        let cost = price.cost(&usage(1000, 100, 10_000));
        // 1000*3e-6 + 100*15e-6 + 10000*0.3e-6 = 0.003 + 0.0015 + 0.003
        assert!((cost - 0.0075).abs() < 1e-9);
    }

    #[test]
    fn budget_graduates_ok_warn_stop() {
        let budget = Budget {
            max_usd: 1.0,
            max_tokens: 0,
            warn_at: 0.8,
        };
        assert_eq!(budget.check(0.5, 0), BudgetAction::Ok);
        assert_eq!(budget.check(0.85, 0), BudgetAction::Warn);
        assert_eq!(budget.check(1.0, 0), BudgetAction::Stop);
        assert_eq!(budget.check(2.0, 0), BudgetAction::Stop);
    }

    #[test]
    fn zero_caps_are_unlimited() {
        assert_eq!(Budget::default().check(1e9, u64::MAX), BudgetAction::Ok);
    }

    #[test]
    fn token_caps_work_independently() {
        let budget = Budget {
            max_usd: 0.0,
            max_tokens: 1000,
            warn_at: 0.8,
        };
        assert_eq!(budget.check(0.0, 799), BudgetAction::Ok);
        assert_eq!(budget.check(0.0, 800), BudgetAction::Warn);
        assert_eq!(budget.check(0.0, 1000), BudgetAction::Stop);
    }

    #[test]
    fn fleet_cap_graduates_and_denies_dispatch() {
        let fb = FleetBudget {
            fleet_max_usd: 10.0,
            repo_max_usd: 0.0,
            warn_at: 0.8,
        };
        assert_eq!(fb.check_dispatch(5.0, 0.0).action, BudgetAction::Ok);
        let warn = fb.check_dispatch(8.0, 0.0);
        assert_eq!(warn.action, BudgetAction::Warn);
        assert_eq!(warn.scope, Some(BudgetScope::Fleet));
        let deny = fb.check_dispatch(10.0, 0.0);
        assert!(deny.denied());
        assert_eq!(deny.scope, Some(BudgetScope::Fleet));
        assert_eq!(deny.cap_usd, 10.0);
    }

    #[test]
    fn repo_cap_binds_before_fleet() {
        let fb = FleetBudget {
            fleet_max_usd: 100.0,
            repo_max_usd: 5.0,
            warn_at: 0.8,
        };
        // Fleet well under cap, but this repo is over its own cap.
        let deny = fb.check_dispatch(20.0, 5.0);
        assert!(deny.denied());
        assert_eq!(deny.scope, Some(BudgetScope::Repo));
    }

    #[test]
    fn fleet_wins_severity_tie() {
        let fb = FleetBudget {
            fleet_max_usd: 10.0,
            repo_max_usd: 10.0,
            warn_at: 0.8,
        };
        // Both caps hit — fleet is reported as the binding scope.
        let deny = fb.check_dispatch(10.0, 10.0);
        assert!(deny.denied());
        assert_eq!(deny.scope, Some(BudgetScope::Fleet));
    }

    #[test]
    fn zero_fleet_caps_are_unlimited() {
        assert_eq!(
            FleetBudget::default().check_dispatch(1e9, 1e9).action,
            BudgetAction::Ok
        );
    }
}

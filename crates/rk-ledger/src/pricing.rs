//! Pricing table: vendored subset of LiteLLM's model prices (USD per token),
//! with longest-prefix alias resolution and runtime-refresh merging.
//!
//! Snapshot date: 2026-07. Refresh with `rk pricing refresh` (fetches the
//! LiteLLM JSON and layers it over these entries) or drop overrides into
//! `~/.rat-kingdom/pricing.json`.

use crate::ModelPrice;
use std::collections::HashMap;

/// (model-name-prefix, input, output, cache_read, cache_creation) — USD/token.
const VENDORED: &[(&str, f64, f64, f64, f64)] = &[
    // Anthropic (API names)
    ("claude-fable-5", 10e-6, 50e-6, 1e-6, 12.5e-6),
    ("claude-opus-4", 15e-6, 75e-6, 1.5e-6, 18.75e-6),
    ("claude-sonnet-5", 3e-6, 15e-6, 0.3e-6, 3.75e-6),
    ("claude-sonnet-4", 3e-6, 15e-6, 0.3e-6, 3.75e-6),
    ("claude-haiku-4-5", 1e-6, 5e-6, 0.1e-6, 1.25e-6),
    ("claude-3-5-haiku", 0.8e-6, 4e-6, 0.08e-6, 1e-6),
    // Claude Code aliases
    ("opus", 15e-6, 75e-6, 1.5e-6, 18.75e-6),
    ("sonnet", 3e-6, 15e-6, 0.3e-6, 3.75e-6),
    ("haiku", 1e-6, 5e-6, 0.1e-6, 1.25e-6),
    // OpenAI / Codex
    ("gpt-5.5-codex", 1.25e-6, 10e-6, 0.125e-6, 0.0),
    ("gpt-5-codex", 1.25e-6, 10e-6, 0.125e-6, 0.0),
    ("gpt-5.5", 1.25e-6, 10e-6, 0.125e-6, 0.0),
    ("gpt-5", 1.25e-6, 10e-6, 0.125e-6, 0.0),
    ("o4-mini", 1.1e-6, 4.4e-6, 0.275e-6, 0.0),
    ("codex-mini", 1.5e-6, 6e-6, 0.375e-6, 0.0),
];

#[derive(Debug, Clone, Default)]
pub struct PricingTable {
    prices: HashMap<String, ModelPrice>,
}

impl PricingTable {
    pub fn vendored() -> Self {
        let prices = VENDORED
            .iter()
            .map(|(name, input, output, cache_read, cache_creation)| {
                (
                    name.to_string(),
                    ModelPrice {
                        input_cost_per_token: *input,
                        output_cost_per_token: *output,
                        cache_read_input_token_cost: *cache_read,
                        cache_creation_input_token_cost: *cache_creation,
                    },
                )
            })
            .collect();
        Self { prices }
    }

    /// Layer entries from a LiteLLM-shaped JSON document over this table
    /// (runtime refresh / user overrides). Non-price entries are skipped.
    pub fn merge_pricing_json(&mut self, json: &str) -> Result<usize, serde_json::Error> {
        let doc: HashMap<String, serde_json::Value> = serde_json::from_str(json)?;
        let mut merged = 0;
        for (name, value) in doc {
            if name == "sample_spec" {
                continue;
            }
            if let Ok(price) = serde_json::from_value::<ModelPrice>(value) {
                if price.input_cost_per_token > 0.0 || price.output_cost_per_token > 0.0 {
                    self.prices.insert(name, price);
                    merged += 1;
                }
            }
        }
        Ok(merged)
    }

    /// Resolve a model name: exact match, else the longest table key that is a
    /// prefix of (or contained in) the queried name — handles dated variants
    /// like `claude-sonnet-5-20260115` and provider prefixes.
    pub fn lookup(&self, model: &str) -> Option<ModelPrice> {
        if let Some(price) = self.prices.get(model) {
            return Some(*price);
        }
        let lowered = model.to_lowercase();
        self.prices
            .iter()
            .filter(|(name, _)| lowered.contains(name.as_str()))
            .max_by_key(|(name, _)| name.len())
            .map(|(_, price)| *price)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_and_fuzzy_lookup() {
        let table = PricingTable::vendored();
        assert!(table.lookup("haiku").is_some());
        // Dated variant resolves via containment.
        let dated = table.lookup("claude-haiku-4-5-20251001").unwrap();
        assert_eq!(dated.input_cost_per_token, 1e-6);
        // Longest match wins: gpt-5.5-codex over gpt-5.
        let codex = table.lookup("openai/gpt-5.5-codex").unwrap();
        assert_eq!(codex.output_cost_per_token, 10e-6);
        assert!(table.lookup("unknown-model-xyz").is_none());
    }

    #[test]
    fn merge_overlays_and_skips_sample_spec() {
        let mut table = PricingTable::vendored();
        let merged = table
            .merge_pricing_json(
                r#"{
                    "sample_spec": {"anything": true},
                    "future-model": {"input_cost_per_token": 2e-6, "output_cost_per_token": 8e-6},
                    "haiku": {"input_cost_per_token": 9e-6, "output_cost_per_token": 9e-6}
                }"#,
            )
            .unwrap();
        assert_eq!(merged, 2);
        assert_eq!(
            table.lookup("future-model").unwrap().output_cost_per_token,
            8e-6
        );
        // Override replaced the vendored haiku entry.
        assert_eq!(table.lookup("haiku").unwrap().input_cost_per_token, 9e-6);
    }
}

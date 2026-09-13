//! Pricing table: vendored subset of LiteLLM's model prices (USD per token),
//! with longest-prefix alias resolution and runtime-refresh merging.
//!
//! Snapshot date: 2026-09, verified against
//! <https://platform.claude.com/docs/en/about-claude/pricing>. There is no
//! `rk pricing refresh` CLI (an earlier revision of this comment claimed
//! one) — the only refresh path is dropping a LiteLLM-shaped JSON override
//! into `~/.rat-kingdom/pricing.json`, merged over these entries at daemon
//! startup by [`PricingTable::merge_pricing_json`].
//!
//! Entries carry one base `input`/`output`/`cache_read` rate plus a
//! *fallback* cache-creation rate (`cache_creation`, historically the only
//! one). Anthropic's Claude-5-generation models additionally split cache
//! writes by TTL — a 5-minute bucket priced at 1.25x input and a 1-hour
//! bucket at 2x input — so those entries also carry `cache_creation_5m`/
//! `cache_creation_1h`; `ModelPrice::cost` uses those for tokens whose
//! bucket is known and falls back to the flat rate otherwise (legacy
//! records, or providers that never report the split). Models without an
//! explicit split (`None`) keep pricing every cache write at the one
//! fallback rate exactly as before this distinction existed.
use crate::ModelPrice;
use std::collections::HashMap;

/// One vendored model's per-token USD rates.
struct Vendored {
    prefix: &'static str,
    input: f64,
    output: f64,
    cache_read: f64,
    /// Fallback cache-creation rate (also the 5m rate for models that don't
    /// distinguish TTL buckets at all).
    cache_creation: f64,
    cache_creation_5m: Option<f64>,
    cache_creation_1h: Option<f64>,
}

const VENDORED: &[Vendored] = &[
    // Anthropic (API names)
    Vendored {
        prefix: "claude-fable-5",
        input: 10e-6,
        output: 50e-6,
        cache_read: 1e-6,
        cache_creation: 12.5e-6,
        cache_creation_5m: None,
        cache_creation_1h: None,
    },
    Vendored {
        prefix: "claude-opus-4",
        input: 15e-6,
        output: 75e-6,
        cache_read: 1.5e-6,
        cache_creation: 18.75e-6,
        cache_creation_5m: None,
        cache_creation_1h: None,
    },
    // Standard Opus 5: $5/$25 base+output, verified 2026-09-13 (the
    // vendored "opus" alias previously pointed at Opus 4's $15/$75 rate,
    // which live Claude Code 2.1.270 streams resolving to "claude-opus-5"
    // do NOT use — see TKT-hakir-zuraj-sovun).
    Vendored {
        prefix: "claude-opus-5",
        input: 5e-6,
        output: 25e-6,
        cache_read: 0.5e-6,
        cache_creation: 6.25e-6,
        cache_creation_5m: Some(6.25e-6),
        cache_creation_1h: Some(10e-6),
    },
    // Standard Sonnet 5: $2/$10 base+output remains standard — the
    // scheduled Sept 1 increase to Opus4/Sonnet4-era rates was cancelled.
    Vendored {
        prefix: "claude-sonnet-5",
        input: 2e-6,
        output: 10e-6,
        cache_read: 0.2e-6,
        cache_creation: 2.5e-6,
        cache_creation_5m: Some(2.5e-6),
        cache_creation_1h: Some(4e-6),
    },
    Vendored {
        prefix: "claude-sonnet-4",
        input: 3e-6,
        output: 15e-6,
        cache_read: 0.3e-6,
        cache_creation: 3.75e-6,
        cache_creation_5m: None,
        cache_creation_1h: None,
    },
    Vendored {
        prefix: "claude-haiku-4-5",
        input: 1e-6,
        output: 5e-6,
        cache_read: 0.1e-6,
        cache_creation: 1.25e-6,
        cache_creation_5m: None,
        cache_creation_1h: None,
    },
    Vendored {
        prefix: "claude-3-5-haiku",
        input: 0.8e-6,
        output: 4e-6,
        cache_read: 0.08e-6,
        cache_creation: 1e-6,
        cache_creation_5m: None,
        cache_creation_1h: None,
    },
    // Claude Code aliases — the CLI's `--model opus`/`sonnet`/`haiku`
    // shorthand resolves to whichever generation is currently standard, so
    // these track the explicit current-generation entries above, not the
    // older Opus4/Sonnet4 rates.
    Vendored {
        prefix: "opus",
        input: 5e-6,
        output: 25e-6,
        cache_read: 0.5e-6,
        cache_creation: 6.25e-6,
        cache_creation_5m: Some(6.25e-6),
        cache_creation_1h: Some(10e-6),
    },
    Vendored {
        prefix: "sonnet",
        input: 2e-6,
        output: 10e-6,
        cache_read: 0.2e-6,
        cache_creation: 2.5e-6,
        cache_creation_5m: Some(2.5e-6),
        cache_creation_1h: Some(4e-6),
    },
    Vendored {
        prefix: "haiku",
        input: 1e-6,
        output: 5e-6,
        cache_read: 0.1e-6,
        cache_creation: 1.25e-6,
        cache_creation_5m: None,
        cache_creation_1h: None,
    },
    // OpenAI / Codex
    Vendored {
        prefix: "gpt-5.5-codex",
        input: 1.25e-6,
        output: 10e-6,
        cache_read: 0.125e-6,
        cache_creation: 0.0,
        cache_creation_5m: None,
        cache_creation_1h: None,
    },
    Vendored {
        prefix: "gpt-5-codex",
        input: 1.25e-6,
        output: 10e-6,
        cache_read: 0.125e-6,
        cache_creation: 0.0,
        cache_creation_5m: None,
        cache_creation_1h: None,
    },
    Vendored {
        prefix: "gpt-5.5",
        input: 1.25e-6,
        output: 10e-6,
        cache_read: 0.125e-6,
        cache_creation: 0.0,
        cache_creation_5m: None,
        cache_creation_1h: None,
    },
    Vendored {
        prefix: "gpt-5",
        input: 1.25e-6,
        output: 10e-6,
        cache_read: 0.125e-6,
        cache_creation: 0.0,
        cache_creation_5m: None,
        cache_creation_1h: None,
    },
    Vendored {
        prefix: "o4-mini",
        input: 1.1e-6,
        output: 4.4e-6,
        cache_read: 0.275e-6,
        cache_creation: 0.0,
        cache_creation_5m: None,
        cache_creation_1h: None,
    },
    Vendored {
        prefix: "codex-mini",
        input: 1.5e-6,
        output: 6e-6,
        cache_read: 0.375e-6,
        cache_creation: 0.0,
        cache_creation_5m: None,
        cache_creation_1h: None,
    },
];

#[derive(Debug, Clone, Default)]
pub struct PricingTable {
    prices: HashMap<String, ModelPrice>,
}

impl PricingTable {
    pub fn vendored() -> Self {
        let prices = VENDORED
            .iter()
            .map(|v| {
                (
                    v.prefix.to_string(),
                    ModelPrice {
                        input_cost_per_token: v.input,
                        output_cost_per_token: v.output,
                        cache_read_input_token_cost: v.cache_read,
                        cache_creation_input_token_cost: v.cache_creation,
                        cache_creation_5m_input_token_cost: v.cache_creation_5m,
                        cache_creation_1h_input_token_cost: v.cache_creation_1h,
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

    /// TKT-hakir-zuraj-sovun: live Claude Code 2.1.270 streams resolve the
    /// current Opus generation to the model name `claude-opus-5`, and the
    /// `opus` CLI alias tracks the same current generation — both must price
    /// at the corrected standard $5/$25, not Opus 4's $15/$75.
    #[test]
    fn opus5_explicit_and_alias_use_corrected_standard_price() {
        let table = PricingTable::vendored();
        for name in ["claude-opus-5", "opus"] {
            let price = table.lookup(name).unwrap();
            assert_eq!(price.input_cost_per_token, 5e-6, "{name} input");
            assert_eq!(price.output_cost_per_token, 25e-6, "{name} output");
            assert_eq!(price.cache_read_input_token_cost, 0.5e-6, "{name} cache read");
        }
        // Opus 4 is untouched.
        let opus4 = table.lookup("claude-opus-4").unwrap();
        assert_eq!(opus4.input_cost_per_token, 15e-6);
        assert_eq!(opus4.output_cost_per_token, 75e-6);
    }

    /// Sonnet 5's scheduled Sept 1 price increase was cancelled — $2/$10
    /// remains standard for both the explicit model id and the `sonnet`
    /// alias; Sonnet 4 keeps its own unrelated $3/$15 rate.
    #[test]
    fn sonnet5_explicit_and_alias_use_corrected_standard_price() {
        let table = PricingTable::vendored();
        for name in ["claude-sonnet-5", "sonnet"] {
            let price = table.lookup(name).unwrap();
            assert_eq!(price.input_cost_per_token, 2e-6, "{name} input");
            assert_eq!(price.output_cost_per_token, 10e-6, "{name} output");
        }
        let sonnet4 = table.lookup("claude-sonnet-4").unwrap();
        assert_eq!(sonnet4.input_cost_per_token, 3e-6);
        assert_eq!(sonnet4.output_cost_per_token, 15e-6);
    }

    /// A dated Opus5 stream id must resolve to the explicit entry (longest
    /// match), carrying its TTL-specific cache-creation rates, not just the
    /// bare `opus` alias's fallback.
    #[test]
    fn dated_opus5_variant_resolves_to_explicit_ttl_aware_entry() {
        let table = PricingTable::vendored();
        let price = table.lookup("claude-opus-5-20260901").unwrap();
        assert_eq!(price.input_cost_per_token, 5e-6);
        assert_eq!(price.cache_creation_5m_input_token_cost, Some(6.25e-6));
        assert_eq!(price.cache_creation_1h_input_token_cost, Some(10e-6));
    }

    /// Mixed 5m+1h cache-creation usage against the vendored Opus5 entry:
    /// each bucket bills at its own official rate.
    #[test]
    fn opus5_mixed_5m_and_1h_cache_creation_prices_each_bucket() {
        let table = PricingTable::vendored();
        let price = table.lookup("claude-opus-5").unwrap();
        let usage = rk_harness::TokenUsage {
            cache_creation: 1500,
            cache_creation_5m: 1000,
            cache_creation_1h: 500,
            ..Default::default()
        };
        let expected = 1000.0 * 6.25e-6 + 500.0 * 10e-6;
        assert!((price.cost(&usage) - expected).abs() < 1e-12);
    }

    /// A legacy `TokenUsage` with no TTL split (as any record persisted
    /// before this fix would deserialize, via `#[serde(default)]`) must
    /// still price its full cache-creation total, at the fallback rate —
    /// never zero, never double-counted.
    #[test]
    fn opus5_legacy_usage_missing_ttl_split_bills_at_fallback_rate() {
        let table = PricingTable::vendored();
        let price = table.lookup("claude-opus-5").unwrap();
        let legacy_json = r#"{"input":0,"output":0,"cache_read":0,"cache_creation":1000}"#;
        let usage: rk_harness::TokenUsage = serde_json::from_str(legacy_json).unwrap();
        assert_eq!(usage.cache_creation_5m, 0);
        assert_eq!(usage.cache_creation_1h, 0);
        assert!((price.cost(&usage) - 1000.0 * 6.25e-6).abs() < 1e-12);
    }

    /// A `pricing.json` override written before the TTL fields existed
    /// (like the operator's conservative interim override, which sets only
    /// `cache_creation_input_token_cost`) must keep working exactly as
    /// before: with no TTL-specific rate given, both buckets fall back to
    /// that one flat rate.
    #[test]
    fn json_override_without_ttl_fields_still_works() {
        let mut table = PricingTable::vendored();
        table
            .merge_pricing_json(
                r#"{"claude-opus-5": {"input_cost_per_token": 5e-6, "output_cost_per_token": 25e-6, "cache_read_input_token_cost": 5e-7, "cache_creation_input_token_cost": 1e-5}}"#,
            )
            .unwrap();
        let price = table.lookup("claude-opus-5").unwrap();
        let usage = rk_harness::TokenUsage {
            cache_creation: 1000,
            cache_creation_5m: 400,
            cache_creation_1h: 600,
            ..Default::default()
        };
        // No TTL split in the override -> both buckets bill at the single
        // 1e-5 override rate, same as pre-TTL-aware behavior.
        assert!((price.cost(&usage) - 1000.0 * 1e-5).abs() < 1e-12);
    }
}

//! Read compatibility for names retired when review and landing were separated
//! from the human-facing King. Producers and documentation use canonical names.

pub fn is_landing_need(name: &str) -> bool {
    matches!(name, "landing" | "steward")
}

pub fn canonical(name: &str) -> &str {
    match name {
        "steward-review" => "candidate-review",
        "steward-escalation" => "landing-escalation",
        "steward-protected-paths" => "landing-protected-paths",
        "steward-diff-scope" => "landing-diff-scope",
        other => other,
    }
}

pub const LEGACY_REVIEW_WORKFLOW: &str = "steward-review";

#[cfg(test)]
mod tests {
    #[test]
    fn old_landing_records_and_configuration_remain_readable() {
        assert!(super::is_landing_need("steward"));
        assert_eq!(super::canonical("steward-review"), "candidate-review");
        assert_eq!(super::canonical("steward-escalation"), "landing-escalation");
        assert_eq!(
            super::canonical("steward-protected-paths"),
            "landing-protected-paths"
        );
    }
}

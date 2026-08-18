//! The freeze tag vocabulary: which ticket labels make a ticket undispatchable
//! by the *automated* generators (the continuous drain and the scheduler-driven
//! self-improve chain).
//!
//! # Why this exists
//!
//! The 2026-08-16 strategic review (R6) observed that a freeze list which does
//! not constrain the *generators* only treats the symptom: a nightly
//! self-improve schedule plus a drain autoscaler pointed at the backlog will
//! regrow frozen mass as fast as it is deleted, and dual-implementation drift
//! (the TKT-171 failure mode) scales with line count. So the freeze is enforced
//! where work is *dispatched*, not merely written in a doc.
//!
//! # The vocabulary
//!
//! - `frozen:<subsystem>` — this ticket touches a frozen subsystem. The
//!   recognised subsystems are [`FROZEN_SUBSYSTEMS`].
//! - `frozen` — bare form, frozen with the subsystem unstated.
//! - `frozen-except:<reason>` — an approved **carve-out**: the ticket touches a
//!   frozen subsystem but is nonetheless dispatchable, because the work is on
//!   the ratified exception list ([`CARVE_OUTS`]). Today the only carve-out is
//!   `extraction` — extracting the onboarding wizard's read-only tool
//!   enforcement machinery into the reusable diagnostician role.
//!
//! # Fail-closed, deliberately
//!
//! Both unknown-value cases resolve toward *less* dispatch, never more:
//!
//! - An unrecognised subsystem (`frozen:something-new`) still freezes. A typo
//!   in a freeze tag must not silently hand the ticket back to the drain.
//! - An unrecognised carve-out reason (`frozen-except:whatever`) does **not**
//!   unfreeze. Otherwise any misspelling would be a self-service exception, and
//!   the carve-out list would stop meaning anything.
//!
//! [`validate_label`] is the loud half of that pair: it rejects both unknown
//! values at the point a ticket is written, so a typo is a visible error at
//! filing time rather than a ticket that quietly never drains.

/// Marks a ticket as touching a frozen subsystem, as `frozen:<subsystem>`.
pub const FROZEN_PREFIX: &str = "frozen:";

/// The bare freeze label, for a ticket frozen without naming a subsystem.
pub const FROZEN_BARE: &str = "frozen";

/// Marks a ratified exception, as `frozen-except:<reason>`.
pub const CARVE_OUT_PREFIX: &str = "frozen-except:";

/// The frozen subsystems, per the operator-ratified freeze list of 2026-08-17.
pub const FROZEN_SUBSYSTEMS: &[&str] = &[
    "factory-foreman",
    "product-to-code",
    "onboarding-wizard",
    "jcode-adapter",
    "ballots",
];

/// The ratified carve-out reasons. A `frozen-except:<reason>` label unfreezes a
/// ticket only when `<reason>` appears here.
///
/// - `extraction` — extracting the onboarding wizard's read-only enforcement
///   machinery into the reusable diagnostician role. Frozen subsystems may
///   still be *mined* for machinery that outlives them; the freeze is on
///   growing them, not on harvesting them.
pub const CARVE_OUTS: &[&str] = &["extraction"];

/// Does this single label freeze (the bare label, or any `frozen:<subsystem>`)?
///
/// Note this is true for an *unrecognised* subsystem too — see the fail-closed
/// rule in the module docs.
pub fn is_freeze_label(label: &str) -> bool {
    label == FROZEN_BARE || label.starts_with(FROZEN_PREFIX)
}

/// The subsystem named by a freeze label, if it names one. `frozen` → `None`.
pub fn freeze_subsystem(label: &str) -> Option<&str> {
    label.strip_prefix(FROZEN_PREFIX)
}

/// The reason named by a carve-out label, whether or not it is recognised.
pub fn carve_out_reason(label: &str) -> Option<&str> {
    label.strip_prefix(CARVE_OUT_PREFIX)
}

/// Does this label grant a *recognised* carve-out?
pub fn is_active_carve_out(label: &str) -> bool {
    carve_out_reason(label).is_some_and(|r| CARVE_OUTS.contains(&r))
}

/// Whether a ticket carrying `labels` is barred from **automated** dispatch.
///
/// True iff it carries at least one freeze label and no recognised carve-out.
/// This is the single predicate both enforcement points call — the drain
/// ([`crate`]-external: `rk_daemon::drain`) and the scheduler-driven fan-out —
/// so the two can never drift apart on what "frozen" means.
///
/// This governs automation only. An operator spawning a rat by hand, or running
/// a workflow explicitly, is a deliberate act and is never blocked by a tag.
pub fn blocks_automated_dispatch<S: AsRef<str>>(labels: &[S]) -> bool {
    let mut frozen = false;
    for label in labels {
        let label = label.as_ref();
        if is_active_carve_out(label) {
            return false;
        }
        if is_freeze_label(label) {
            frozen = true;
        }
    }
    frozen
}

/// The freeze/carve-out labels in `labels`, for logging and `rk ticket show`.
pub fn freeze_labels<S: AsRef<str>>(labels: &[S]) -> Vec<&str> {
    labels
        .iter()
        .map(AsRef::as_ref)
        .filter(|l| is_freeze_label(l) || carve_out_reason(l).is_some())
        .collect()
}

/// Reject a freeze/carve-out label naming a value outside the vocabulary.
///
/// Any label that is not a freeze or carve-out label passes untouched — this
/// validates the freeze vocabulary, it is not a general label allowlist.
pub fn validate_label(label: &str) -> Result<(), String> {
    if let Some(subsystem) = freeze_subsystem(label) {
        if !FROZEN_SUBSYSTEMS.contains(&subsystem) {
            return Err(format!(
                "unknown frozen subsystem '{subsystem}' (known: {}). \
                 Note the ticket stays frozen regardless; fix the tag or add the subsystem.",
                FROZEN_SUBSYSTEMS.join(", ")
            ));
        }
    }
    if let Some(reason) = carve_out_reason(label) {
        if !CARVE_OUTS.contains(&reason) {
            return Err(format!(
                "unknown carve-out reason '{reason}' (ratified: {}). \
                 An unrecognised carve-out does NOT unfreeze the ticket.",
                CARVE_OUTS.join(", ")
            ));
        }
    }
    Ok(())
}

/// [`validate_label`] over a whole label set, reporting the first offender.
pub fn validate_labels<S: AsRef<str>>(labels: &[S]) -> Result<(), String> {
    labels.iter().try_for_each(|l| validate_label(l.as_ref()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plain_ticket_is_dispatchable() {
        assert!(!blocks_automated_dispatch(&[
            "strategic-review",
            "mechanical"
        ]));
        assert!(!blocks_automated_dispatch::<&str>(&[]));
    }

    #[test]
    fn every_ratified_subsystem_freezes() {
        for subsystem in FROZEN_SUBSYSTEMS {
            let label = format!("{FROZEN_PREFIX}{subsystem}");
            assert!(
                blocks_automated_dispatch(std::slice::from_ref(&label)),
                "{label} must block automated dispatch"
            );
            assert!(validate_label(&label).is_ok(), "{label} must validate");
        }
    }

    #[test]
    fn the_bare_label_freezes() {
        assert!(blocks_automated_dispatch(&[FROZEN_BARE]));
        assert!(validate_label(FROZEN_BARE).is_ok());
    }

    #[test]
    fn a_ratified_carve_out_unfreezes() {
        // The onboarding carve-out from the ticket: extraction work on a frozen
        // subsystem stays dispatchable.
        assert!(!blocks_automated_dispatch(&[
            "frozen:onboarding-wizard",
            "frozen-except:extraction",
        ]));
    }

    #[test]
    fn a_carve_out_beats_several_freeze_tags() {
        assert!(!blocks_automated_dispatch(&[
            "frozen:onboarding-wizard",
            "frozen:ballots",
            "frozen-except:extraction",
        ]));
    }

    #[test]
    fn an_unknown_subsystem_still_freezes() {
        // Fail closed: a typo must not hand the ticket back to the drain.
        assert!(blocks_automated_dispatch(&["frozen:typo-here"]));
        assert!(validate_label("frozen:typo-here").is_err());
    }

    #[test]
    fn an_unknown_carve_out_does_not_unfreeze() {
        // Fail closed the other way: an unrecognised exception is not an
        // exception, or every misspelling would be a self-service unfreeze.
        assert!(blocks_automated_dispatch(&[
            "frozen:ballots",
            "frozen-except:because-i-said-so",
        ]));
        assert!(validate_label("frozen-except:because-i-said-so").is_err());
    }

    #[test]
    fn a_carve_out_alone_is_not_a_freeze() {
        // Carve-out with nothing to carve out of: dispatchable, and it does not
        // accidentally read as a freeze label.
        assert!(!blocks_automated_dispatch(&["frozen-except:extraction"]));
        assert!(!is_freeze_label("frozen-except:extraction"));
    }

    #[test]
    fn freeze_prefix_does_not_swallow_unrelated_labels() {
        assert!(!is_freeze_label("frozenset"));
        assert!(!blocks_automated_dispatch(&["frozenset", "unfrozen"]));
    }

    #[test]
    fn non_freeze_labels_are_not_validated() {
        assert!(validate_labels(&["strategic-review", "high", "anything"]).is_ok());
    }

    #[test]
    fn freeze_labels_are_extracted_for_display() {
        let labels = [
            "strategic-review",
            "frozen:ballots",
            "frozen-except:extraction",
        ];
        assert_eq!(
            freeze_labels(&labels),
            vec!["frozen:ballots", "frozen-except:extraction"]
        );
    }
}

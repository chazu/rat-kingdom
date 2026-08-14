//! Factory analytics domain types.

pub mod outcome_events;
pub mod outcome_facts;
pub mod recommendations;
pub mod scorecards;

pub use outcome_facts::{
    OutcomeEvidenceKind, OutcomeFactGroupKey, OutcomeStatus, SourceAvailability, SourceCounts,
};

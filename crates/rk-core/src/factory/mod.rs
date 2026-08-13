//! Factory analytics domain types.

pub mod outcome_events;
pub mod outcome_facts;

pub use outcome_facts::{
    OutcomeEvidenceKind, OutcomeFactGroupKey, OutcomeStatus, SourceAvailability, SourceCounts,
};

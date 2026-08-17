//! Core types for rat-kingdom: tuple model, ids, errors, config, and path layout.

pub mod action;
pub mod config;
pub mod error;
pub mod factory;
pub mod freeze;
pub mod id;
pub mod identity;
pub mod names;
pub mod paths;
pub mod prime;
pub mod product_to_code;
pub mod sdlc;
pub mod tuple;
pub mod version;

pub use error::{Error, Result};

/// Jcode tools allowed for assessment-only repository onboarding.
pub const JCODE_READ_ONLY_TOOLS: &str = "read,ls,agentgrep";

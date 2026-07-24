//! Core types for rat-kingdom: tuple model, ids, errors, config, and path layout.

pub mod config;
pub mod error;
pub mod id;
pub mod identity;
pub mod names;
pub mod paths;
pub mod prime;
pub mod tuple;

pub use error::{Error, Result};

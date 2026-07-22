//! Core types for rat-kingdom: tuple model, ids, errors, config, and path layout.

pub mod config;
pub mod error;
pub mod id;
pub mod paths;
pub mod tuple;

pub use error::{Error, Result};

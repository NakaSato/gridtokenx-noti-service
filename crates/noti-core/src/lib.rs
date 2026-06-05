//! # noti-core
//!
//! Domain types, trait contracts, and error definitions for the
//! `GridTokenX` Notification Service. This crate has **no I/O** —
//! it defines the vocabulary that all other crates depend on.

pub mod config;
pub mod domain;
pub mod error;
pub mod traits;

pub use error::{NotiError, Result};
pub use traits::*;

//! Typed error definitions for the notification service.
//!
//! Uses `thiserror` for structured, matchable errors across the domain
//! layer. Infrastructure crates convert their native errors into these
//! variants at adapter boundaries.

/// Crate-level result alias.
pub type Result<T> = std::result::Result<T, NotiError>;

/// Domain-level error type for the notification service.
#[derive(Debug, thiserror::Error)]
pub enum NotiError {
    /// Database operation failed.
    #[error("database error: {0}")]
    Database(String),

    /// Template rendering failed.
    #[error("template error: {0}")]
    Template(String),

    /// External provider delivery failed (potentially retryable).
    #[error("provider error: {0}")]
    Provider(String),

    /// Input validation failed.
    #[error("validation error: {0}")]
    Validation(String),

    /// Requested entity was not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// Request is a duplicate (idempotency key already exists).
    #[error("duplicate request: idempotency key '{0}' already processed")]
    Idempotent(String),

    /// Catch-all for unexpected internal errors.
    #[error("internal error: {0}")]
    Internal(String),
}

impl NotiError {
    /// Build a [`NotiError::Database`] from any displayable source error.
    ///
    /// Adapter crates use this at the persistence boundary to map their
    /// native errors (e.g. `sqlx::Error`) without `noti-core` depending on
    /// the infrastructure crate: `.map_err(NotiError::database)?`.
    pub fn database(e: impl std::fmt::Display) -> Self {
        Self::Database(e.to_string())
    }
}

// ---------------------------------------------------------------------------
// Conversion helpers so adapter crates can use `?` ergonomically.
// ---------------------------------------------------------------------------

impl From<serde_json::Error> for NotiError {
    fn from(e: serde_json::Error) -> Self {
        Self::Internal(e.to_string())
    }
}

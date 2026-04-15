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

// ---------------------------------------------------------------------------
// Conversion helpers so adapter crates can use `?` ergonomically.
// ---------------------------------------------------------------------------

impl From<sqlx::Error> for NotiError {
    fn from(e: sqlx::Error) -> Self {
        Self::Database(e.to_string())
    }
}

impl From<serde_json::Error> for NotiError {
    fn from(e: serde_json::Error) -> Self {
        Self::Internal(e.to_string())
    }
}

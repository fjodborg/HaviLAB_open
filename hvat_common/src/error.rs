//! Structured errors used by the frontend and application logic.

use serde::{Deserialize, Serialize};

use crate::protocol::{ErrorCode, Severity};

/// Structured error information for display and retry decisions.
#[derive(Debug, Clone)]
pub struct ProtocolError {
    /// Error code from the hierarchy
    pub code: ErrorCode,

    /// Severity level
    pub severity: Severity,

    /// Whether this error can be retried
    pub retryable: bool,

    /// Suggested retry delay in milliseconds
    pub retry_after_ms: u32,

    /// Additional structured context
    pub context: ErrorContext,

    /// Human-readable error message
    pub message: String,
}

impl ProtocolError {
    /// Create a non-retryable error.
    pub fn fatal(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            severity: Severity::Fatal,
            retryable: false,
            retry_after_ms: 0,
            context: ErrorContext::None,
            message: message.into(),
        }
    }

    /// Create a non-fatal error.
    pub fn error(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            severity: Severity::Error,
            retryable: false,
            retry_after_ms: 0,
            context: ErrorContext::None,
            message: message.into(),
        }
    }

    /// Create a retryable error.
    pub fn retryable(code: ErrorCode, message: impl Into<String>, retry_after_ms: u32) -> Self {
        Self {
            code,
            severity: Severity::Error,
            retryable: true,
            retry_after_ms,
            context: ErrorContext::None,
            message: message.into(),
        }
    }

    /// Create a warning.
    pub fn warning(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            severity: Severity::Warning,
            retryable: false,
            retry_after_ms: 0,
            context: ErrorContext::None,
            message: message.into(),
        }
    }

    /// Add context to the error.
    pub fn with_context(mut self, context: ErrorContext) -> Self {
        self.context = context;
        self
    }
}

/// Structured error context for additional information.
///
/// Context is retained for diagnostics and user-facing error details.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ErrorContext {
    /// No additional context
    None,

    /// Image-related context
    Image {
        image_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        level: Option<u8>,
    },

    /// Pyramid build progress context
    Pyramid {
        image_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        progress: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        eta_seconds: Option<u32>,
    },

    /// SAM operation context
    Sam {
        image_id: String,
        operation: String, // "embed" or "segment"
        #[serde(skip_serializing_if = "Option::is_none")]
        required_vram_mb: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        available_vram_mb: Option<u32>,
    },

    /// Contract version mismatch context
    Protocol {
        expected_version: u8,
        received_version: u8,
    },

    /// Connection error context
    Connection { server_url: String, attempt: u32 },
}

#[cfg(test)]
#[path = "error.test.rs"]
mod tests;

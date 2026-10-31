//! REST capability schemas and shared model/error types.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Current protocol version.
///
/// Version 2: Model-agnostic contract with self-describing capabilities.
pub const PROTOCOL_VERSION: u8 = 2;

/// Error codes organized by category.
///
/// Error codes follow a hierarchical scheme:
/// - 1xxx: Image-related errors
/// - 2xxx: Pyramid-related errors
/// - 3xxx: Inference/model-related errors
/// - 4xxx: Connection-related errors
/// - 5xxx: Server-related errors
/// - 6xxx: Client-related errors
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    // Image errors (1xxx)
    ImageNotFound = 1000,
    InvalidLevel = 1001,
    ImageCorrupted = 1002,
    UnsupportedFormat = 1003,
    ImageTooLarge = 1004,
    NoActiveImage = 1010,

    // Pyramid errors (2xxx)
    PyramidNotReady = 2000,
    PyramidBuildFailed = 2001,
    PyramidCorrupted = 2002,
    PyramidStorageFull = 2003,
    PyramidTimeout = 2004,

    // Inference/model errors (3xxx)
    ModelNotFound = 3010,
    InvalidInput = 3011,
    MissingInput = 3012,
    EmbeddingRequired = 3013,
    EmbeddingExpired = 3014,
    ModelBusy = 3015,
    ModelEncodeFailed = 3016,
    ModelDecodeFailed = 3017,
    ModelOutOfMemory = 3018,

    // Connection errors (4xxx)
    ConnectionTimeout = 4000,
    ConnectionRefused = 4001,
    ConnectionDropped = 4002,
    ProtocolMismatch = 4003,
    AuthRequired = 4004,
    AuthFailed = 4005,
    RateLimited = 4006,
    SessionExpired = 4007,

    // Server errors (5xxx)
    InternalError = 5000,
    ServiceUnavailable = 5001,
    StorageError = 5002,
    OutOfMemory = 5003,
    ConfigurationError = 5004,

    // Client errors (6xxx)
    InvalidRequest = 6000,
    InvalidAction = 6001,
    MissingField = 6002,
    InvalidValue = 6003,
}

impl ErrorCode {
    /// Get the error category.
    pub fn category(&self) -> ErrorCategory {
        match *self as u16 {
            1000..=1999 => ErrorCategory::Image,
            2000..=2999 => ErrorCategory::Pyramid,
            3000..=3999 => ErrorCategory::Model,
            4000..=4999 => ErrorCategory::Connection,
            5000..=5999 => ErrorCategory::Server,
            6000..=6999 => ErrorCategory::Client,
            _ => ErrorCategory::Unknown,
        }
    }

    /// Is this error retryable by default?
    pub fn default_retryable(&self) -> bool {
        matches!(
            self,
            Self::PyramidNotReady
                | Self::ModelBusy
                | Self::ConnectionTimeout
                | Self::ConnectionDropped
                | Self::RateLimited
                | Self::SessionExpired
                | Self::ServiceUnavailable
        )
    }

    /// Default retry delay in milliseconds.
    pub fn default_retry_ms(&self) -> u32 {
        match self {
            Self::PyramidNotReady => 5000,
            Self::ModelBusy => 2000,
            Self::RateLimited => 10000,
            Self::ServiceUnavailable => 30000,
            _ => 1000,
        }
    }
}

/// Error category for grouping related errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    Image,
    Pyramid,
    Model,
    Connection,
    Server,
    Client,
    Unknown,
}

/// Error severity level.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    /// Informational message, operation continues
    Info = 0,
    /// Warning, operation continues with degraded functionality
    Warning = 1,
    /// Error, operation failed but connection remains open
    Error = 2,
    /// Fatal error, operation cannot continue
    Fatal = 3,
}

// ============================================================================
// Capabilities and Model Definitions
// ============================================================================

/// Server information returned by REST capability discovery.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerInfo {
    pub name: String,
    pub version: String,
}

/// Server resource limits.
///
/// These limits can be configured on the server side based on available resources.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerLimits {
    /// Maximum image file size in bytes (not pixel dimensions).
    ///
    /// This limits the raw file size to prevent memory exhaustion, but does not
    /// restrict image dimensions.
    ///
    /// Default: 4GB (4,294,967,296 bytes)
    pub max_image_size: u64,

    /// Maximum number of pyramid levels for progressive loading.
    ///
    /// Default: 8
    pub max_pyramid_levels: u8,

    /// Maximum concurrent inference operations.
    ///
    /// Default: 2
    pub max_concurrent_inferences: u32,
}

impl Default for ServerLimits {
    fn default() -> Self {
        Self {
            max_image_size: 4 * 1024 * 1024 * 1024, // 4GB
            max_pyramid_levels: 8,
            max_concurrent_inferences: 2,
        }
    }
}

/// Server capabilities returned by `GET /api/capabilities`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ServerCapabilities {
    /// Protocol version supported by server
    pub protocol_version: u8,

    /// Server identification
    pub server: ServerInfo,

    /// Resource limits
    pub limits: ServerLimits,

    /// Supported backend feature set.
    ///
    /// This allows frontend clients to enable/disable UI surfaces without
    /// guessing from endpoint behavior.
    #[serde(default)]
    pub features: ServerFeatures,

    /// Image-download behavior contract for this backend.
    ///
    /// Backends that don't provide this field (older versions) deserialize to
    /// `DownloadMode::Unknown`, and clients should use compatibility probing.
    #[serde(default)]
    pub download_mode: DownloadMode,

    /// Available models
    pub models: Vec<ModelCapability>,
}

impl Default for ServerCapabilities {
    fn default() -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            server: ServerInfo {
                name: "hvat-axum".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            limits: ServerLimits::default(),
            features: ServerFeatures::default(),
            download_mode: DownloadMode::Unknown,
            models: vec![],
        }
    }
}

impl ServerCapabilities {
    /// Find a model by type (convenience method).
    pub fn find_model_by_type(&self, model_type: ModelType) -> Option<&ModelCapability> {
        self.models.iter().find(|m| m.model_type == model_type)
    }

    /// Check if a specific model is available.
    pub fn has_model(&self, model_id: &str) -> bool {
        self.models.iter().any(|m| m.id == model_id)
    }

    /// Find a SAM-compatible segmentation model.
    ///
    /// A model is considered SAM-compatible when it supports universal
    /// `seg.point_to_mask` semantics.
    pub fn find_sam_prompt_model(&self) -> Option<&ModelCapability> {
        self.models
            .iter()
            .find(|model| model.supports_point_to_mask())
    }

    /// Whether inference is supported.
    ///
    /// For backwards compatibility with older servers that don't emit
    /// `features`, a non-empty model list is also treated as inference support.
    pub fn supports_inference(&self) -> bool {
        self.features.inference || !self.models.is_empty()
    }

    /// Whether SAM-style interactive segmentation is supported.
    ///
    /// For backwards compatibility with older servers that don't emit
    /// `features`, this also checks for a SAM-compatible model in `models`.
    pub fn supports_sam(&self) -> bool {
        self.features.sam || self.find_sam_prompt_model().is_some()
    }

    /// Whether the backend declares support for chunked HTTP image downloads.
    pub fn supports_chunked_downloads(&self) -> bool {
        matches!(self.download_mode, DownloadMode::Chunked)
    }

    /// Whether the backend supports HTTP thumbnail previews.
    ///
    /// Backwards compatibility:
    /// - Prefer explicit `features.thumbnails` when present.
    /// - Otherwise treat download-capable servers as thumbnail-capable.
    pub fn supports_thumbnails(&self) -> bool {
        self.features.thumbnails || self.features.downloads || self.supports_chunked_downloads()
    }

    /// Check if client version is compatible with server.
    pub fn is_compatible(&self, client_version: u8) -> bool {
        // For now, require exact match. In the future, we might allow
        // minor version differences.
        self.protocol_version == client_version
    }
}

/// Optional feature flags announced by the backend.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ServerFeatures {
    /// Supports HTTP project state persistence endpoints.
    #[serde(default)]
    pub project_state: bool,
    /// Supports HTTP image download endpoints.
    #[serde(default)]
    pub downloads: bool,
    /// Supports HTTP thumbnail preview endpoints.
    #[serde(default)]
    pub thumbnails: bool,
    /// Supports model inference over REST.
    #[serde(default)]
    pub inference: bool,
    /// Supports SAM-style interactive segmentation.
    #[serde(default)]
    pub sam: bool,
    /// Supports no-clobber KEEP BOTH semantics for managed image uploads.
    #[serde(default)]
    pub managed_upload_keep_both: bool,
}

/// Download transport mode announced by the backend.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum DownloadMode {
    /// Backend did not explicitly declare a mode (legacy compatibility path).
    #[default]
    Unknown,
    /// Backend supports only single-archive ZIP downloads.
    SingleZip,
    /// Backend supports chunked archive planning and part downloads.
    Chunked,
}

/// Model type category for UI hints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelType {
    Segmentation,
    Detection,
    Classification,
    Feature,
    Custom,
}

/// Input schema for a model.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InputSchema {
    pub name: String,
    #[serde(rename = "type")]
    pub input_type: String,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub description: String,
}

/// Output schema for a model.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutputSchema {
    pub name: String,
    #[serde(rename = "type")]
    pub output_type: String,
    #[serde(default)]
    pub description: String,
}

/// Option schema for configurable model parameters.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct OptionSchema {
    #[serde(rename = "type")]
    pub option_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
    #[serde(default)]
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
}

/// Model capability definition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelCapability {
    pub id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub model_type: ModelType,
    #[serde(default)]
    pub description: String,
    pub inputs: Vec<InputSchema>,
    pub outputs: Vec<OutputSchema>,
    #[serde(default)]
    pub options: HashMap<String, OptionSchema>,
    #[serde(default)]
    pub requires_embedding: bool,
    #[serde(default)]
    pub embedding_time_ms: u32,
}

impl ModelCapability {
    fn has_input_type(&self, input_type: &str) -> bool {
        self.inputs
            .iter()
            .any(|input| input.input_type == input_type)
    }

    fn has_any_input_type(&self, input_types: &[&str]) -> bool {
        input_types
            .iter()
            .any(|input_type| self.has_input_type(input_type))
    }

    fn has_output_type(&self, output_type: &str) -> bool {
        self.outputs
            .iter()
            .any(|output| output.output_type == output_type)
    }

    fn supports_mask_output(&self) -> bool {
        self.has_output_type("polygon_list") || self.has_output_type("mask")
    }

    /// Whether this model supports universal `seg.point_to_mask` semantics.
    pub fn supports_point_to_mask(&self) -> bool {
        self.model_type == ModelType::Segmentation
            && self.has_input_type("point_list")
            && self.supports_mask_output()
    }

    /// Whether this model supports universal `seg.bbox_to_mask` semantics.
    pub fn supports_bbox_to_mask(&self) -> bool {
        self.model_type == ModelType::Segmentation
            && self.has_input_type("bbox")
            && self.supports_mask_output()
    }

    /// Whether this model supports universal `seg.point_bbox_to_mask` semantics.
    pub fn supports_point_bbox_to_mask(&self) -> bool {
        self.model_type == ModelType::Segmentation
            && self.has_input_type("point_list")
            && self.has_input_type("bbox")
            && self.supports_mask_output()
    }

    /// Whether this model supports universal `seg.text_to_mask` semantics.
    pub fn supports_text_to_mask(&self) -> bool {
        self.model_type == ModelType::Segmentation
            && self.has_input_type("text")
            && self.supports_mask_output()
    }

    /// Whether this model supports universal `seg.example_similarity_to_mask` semantics.
    ///
    /// Example prompting can be represented as `region`, `mask`, `embedding`,
    /// or `image_region` (legacy synonym).
    pub fn supports_example_similarity_to_mask(&self) -> bool {
        self.model_type == ModelType::Segmentation
            && self.has_any_input_type(&["region", "mask", "embedding", "image_region"])
            && self.supports_mask_output()
    }

    /// Whether this model is SAM-compatible (accepts point prompts, returns masks).
    ///
    /// This is a compatibility alias for `supports_point_to_mask`.
    pub fn is_sam_compatible(&self) -> bool {
        self.supports_point_to_mask()
    }
}

#[cfg(test)]
#[path = "protocol.test.rs"]
mod tests;

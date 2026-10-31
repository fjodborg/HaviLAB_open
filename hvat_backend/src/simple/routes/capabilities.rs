//! REST capabilities endpoint for the simple backend profile.

use std::sync::Arc;

use axum::{Json, extract::State};
use hvat_common::{
    DownloadMode, PROTOCOL_VERSION, ServerCapabilities, ServerFeatures, ServerInfo, ServerLimits,
};

use crate::simple::state::AppState;

/// Build the capability document from routes available in the simple profile.
#[must_use]
pub fn build_server_capabilities(state: &AppState) -> ServerCapabilities {
    ServerCapabilities {
        protocol_version: PROTOCOL_VERSION,
        server: ServerInfo {
            name: "hvat-axum".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        },
        limits: ServerLimits {
            max_image_size: state.config.max_cache_memory,
            max_pyramid_levels: hvat_common::MAX_PYRAMID_LEVEL,
            max_concurrent_inferences: 0,
        },
        features: ServerFeatures {
            project_state: true,
            downloads: true,
            thumbnails: true,
            inference: false,
            sam: false,
            managed_upload_keep_both: true,
        },
        download_mode: DownloadMode::SingleZip,
        models: vec![],
    }
}

/// `GET /api/capabilities`
pub async fn get_capabilities(State(state): State<Arc<AppState>>) -> Json<ServerCapabilities> {
    Json(build_server_capabilities(&state))
}

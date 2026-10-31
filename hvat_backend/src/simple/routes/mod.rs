//! API routes for the HVAT simple backend.

mod capabilities;
mod images;
mod projects;

use std::sync::Arc;

use crate::simple::state::AppState;
use axum::{Router, routing::get};

/// Create the main API router.
pub fn api_router() -> Router<Arc<AppState>> {
    Router::new()
        .merge(projects::router())
        .route("/capabilities", get(capabilities::get_capabilities))
        .nest("/images", images::router())
}

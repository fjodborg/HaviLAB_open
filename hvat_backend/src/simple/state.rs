//! Application state shared across handlers.

use crate::common::config::ServerConfig;
use crate::common::loaders::ImageLoaderRegistry;

/// Shared application state.
pub struct AppState {
    pub config: ServerConfig,
    pub loaders: ImageLoaderRegistry,
}

impl AppState {
    pub fn new(config: ServerConfig) -> Self {
        Self {
            loaders: ImageLoaderRegistry::with_defaults(),
            config,
        }
    }
}

impl crate::helper::http::projects::ProjectRoutesState for AppState {
    fn project_name(&self) -> &str {
        &self.config.project_name
    }

    fn data_dir(&self) -> &std::path::Path {
        &self.config.data_dir
    }

    fn loaders(&self) -> &ImageLoaderRegistry {
        &self.loaders
    }
}

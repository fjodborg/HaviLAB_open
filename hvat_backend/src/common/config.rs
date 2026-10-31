use std::ops::Deref;
use std::path::PathBuf;

use clap::{Args, Parser};

/// Shared base CLI arguments used by backend implementations.
#[derive(Args, Debug, Clone)]
pub struct BaseCliArgs {
    /// Port to listen on.
    #[arg(short, long, default_value = "3000", env = "HVAT_PORT")]
    pub port: u16,

    /// Root directory containing images to serve.
    #[arg(short, long, default_value = "./data", env = "HVAT_DATA_DIR")]
    pub data_dir: PathBuf,

    /// Directory for pyramid cache.
    #[arg(long, default_value = "./.cache/pyramids", env = "HVAT_CACHE_DIR")]
    pub cache_dir: PathBuf,

    /// Maximum total memory for pyramid cache in MB.
    #[arg(long, default_value = "2048", env = "HVAT_MAX_CACHE_MB")]
    pub max_cache_mb: u64,

    /// Project name (display name for this server/data directory).
    /// Defaults to the data directory name.
    #[arg(short = 'n', long, env = "HVAT_PROJECT_NAME")]
    pub name: Option<String>,
}

/// HVAT simple server - lightweight HTTP backend.
#[derive(Parser, Debug, Clone)]
#[command(name = "hvat-server")]
#[command(about = "HVAT HTTP API server for hyperspectral images")]
#[command(version)]
pub struct CliArgs {
    #[command(flatten)]
    pub base: BaseCliArgs,
}

impl Deref for CliArgs {
    type Target = BaseCliArgs;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

/// Parse CLI arguments.
#[must_use]
pub fn parse_cli_args() -> CliArgs {
    CliArgs::parse()
}

/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Port to listen on.
    pub port: u16,
    /// Root directory for images.
    pub data_dir: PathBuf,
    /// Directory for pyramid cache.
    pub cache_dir: PathBuf,
    /// Maximum total memory for pyramid cache in bytes.
    pub max_cache_memory: u64,
    /// Project name (display name).
    pub project_name: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 3000,
            data_dir: PathBuf::from("./data"),
            cache_dir: PathBuf::from("./.cache/pyramids"),
            max_cache_memory: 2 * 1024 * 1024 * 1024,
            project_name: "data".to_string(),
        }
    }
}

impl ServerConfig {
    /// Create configuration from CLI arguments.
    #[must_use]
    pub fn from_cli(args: CliArgs) -> Self {
        Self::from_base_cli(args.base)
    }

    /// Create configuration from shared base CLI arguments.
    #[must_use]
    pub fn from_base_cli(args: BaseCliArgs) -> Self {
        let project_name = args.name.unwrap_or_else(|| {
            args.data_dir
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("project")
                .to_string()
        });

        Self {
            port: args.port,
            data_dir: args.data_dir,
            cache_dir: args.cache_dir,
            max_cache_memory: args.max_cache_mb * 1024 * 1024,
            project_name,
        }
    }
}

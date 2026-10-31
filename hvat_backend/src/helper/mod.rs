//! Internal helper surface for building custom HVAT backends inside this crate.
//!
//! This module is intentionally crate-local in spirit: it keeps reusable backend
//! primitives close to `hvat_backend` while exposing a cleaner, narrower API
//! than importing from many `common::*` modules directly.

pub mod http;

pub use crate::common::archive;
pub use crate::common::catalog;
pub use crate::common::config;
pub use crate::common::error;
pub use crate::common::loaders;
pub use crate::common::packer;
pub use crate::common::project_state;
pub use crate::common::upload;

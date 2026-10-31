use std::{
    future::Future,
    io,
    path::{Path, PathBuf},
};

use hvat_common::annotation_io::{ExportBundle, ImportBundle};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

pub const MAX_PROJECT_STATE_BYTES: usize = 64 * 1024 * 1024;

const PROJECT_STATE_DIR: &str = ".hvat";
const PROJECT_STATE_FILE: &str = "project_annotations.hvatbundle.json";

struct ProjectStateTempGuard {
    path: PathBuf,
    persisted: bool,
}

impl ProjectStateTempGuard {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            persisted: false,
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn persist(mut self) {
        self.persisted = true;
    }
}

impl Drop for ProjectStateTempGuard {
    fn drop(&mut self) {
        if !self.persisted {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[must_use]
pub fn project_state_path(data_dir: &Path) -> PathBuf {
    data_dir.join(PROJECT_STATE_DIR).join(PROJECT_STATE_FILE)
}

#[must_use]
pub fn validate_project_state_payload(payload: &[u8]) -> bool {
    let import_valid = serde_json::from_slice::<ImportBundle>(payload)
        .map(|bundle| !bundle.files.is_empty())
        .unwrap_or(false);
    if import_valid {
        return true;
    }

    serde_json::from_slice::<ExportBundle>(payload)
        .map(|bundle| !bundle.files.is_empty())
        .unwrap_or(false)
}

pub async fn read_project_state(data_dir: &Path) -> std::io::Result<Vec<u8>> {
    let path = project_state_path(data_dir);
    tokio::fs::read(path).await
}

pub async fn write_project_state(data_dir: &Path, payload: &[u8]) -> std::io::Result<()> {
    let path = project_state_path(data_dir);
    let temp_path = project_state_temp_path(&path);
    write_project_state_with_temp_path_and_rename(
        data_dir,
        payload,
        temp_path,
        |from, to| async move { tokio::fs::rename(from, to).await },
    )
    .await
}

async fn write_project_state_with_temp_path_and_rename<Rename, RenameFuture>(
    data_dir: &Path,
    payload: &[u8],
    temp_path: PathBuf,
    rename: Rename,
) -> io::Result<()>
where
    Rename: FnOnce(PathBuf, PathBuf) -> RenameFuture,
    RenameFuture: Future<Output = io::Result<()>>,
{
    let path = project_state_path(data_dir);
    let parent = path.parent().expect("project state path has a parent");
    tokio::fs::create_dir_all(parent).await?;

    let mut temp_file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .await?;
    let temp_guard = ProjectStateTempGuard::new(temp_path);
    temp_file.write_all(payload).await?;
    temp_file.flush().await?;
    drop(temp_file);

    rename(temp_guard.path().to_path_buf(), path).await?;
    temp_guard.persist();
    Ok(())
}

fn project_state_temp_path(path: &Path) -> PathBuf {
    path.with_file_name(format!(".{PROJECT_STATE_FILE}.{}.tmp", Uuid::new_v4()))
}

#[cfg(test)]
pub(crate) async fn write_project_state_with_test_hooks<Rename, RenameFuture>(
    data_dir: &Path,
    payload: &[u8],
    temp_path: PathBuf,
    rename: Rename,
) -> io::Result<()>
where
    Rename: FnOnce(PathBuf, PathBuf) -> RenameFuture,
    RenameFuture: Future<Output = io::Result<()>>,
{
    write_project_state_with_temp_path_and_rename(data_dir, payload, temp_path, rename).await
}

#[cfg(test)]
#[path = "project_state.test.rs"]
mod tests;

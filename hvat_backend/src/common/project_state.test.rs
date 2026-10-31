use super::*;

#[test]
fn project_state_temp_paths_are_unique_and_same_directory() {
    let destination = Path::new("/data/.hvat/project_annotations.hvatbundle.json");
    let first = project_state_temp_path(destination);
    let second = project_state_temp_path(destination);

    assert_ne!(first, second);
    assert_eq!(first.parent(), destination.parent());
    assert_eq!(second.parent(), destination.parent());
}

#[test]
fn uncommitted_project_state_temp_guard_removes_file_on_drop() {
    let temp_dir = tempfile::tempdir().expect("temporary state directory");
    let path = temp_dir.path().join("pending.tmp");
    std::fs::write(&path, b"partial state").expect("write temporary state");

    drop(ProjectStateTempGuard::new(path.clone()));

    assert!(!path.exists());
}

#[tokio::test]
async fn project_state_temp_collision_preserves_candidate_and_destination() {
    let temp_dir = tempfile::tempdir().expect("temporary project directory");
    let data_dir = temp_dir.path();
    let destination = project_state_path(data_dir);
    let state_dir = destination.parent().expect("project state directory");
    tokio::fs::create_dir_all(state_dir)
        .await
        .expect("create project state directory");
    let candidate = state_dir.join(".preexisting-project-state-candidate.tmp");
    let destination_contents = b"existing destination bytes";
    let candidate_contents = b"unowned candidate bytes";
    tokio::fs::write(&destination, destination_contents)
        .await
        .expect("seed existing regular destination");
    tokio::fs::write(&candidate, candidate_contents)
        .await
        .expect("seed colliding temp candidate");

    let error = write_project_state_with_test_hooks(
        data_dir,
        b"replacement payload",
        candidate.clone(),
        |_from, _to| async { Ok(()) },
    )
    .await
    .expect_err("exclusive temp creation must reject the existing candidate");

    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(
        tokio::fs::read(&destination)
            .await
            .expect("read existing destination"),
        destination_contents
    );
    assert_eq!(
        tokio::fs::read(&candidate)
            .await
            .expect("read unowned candidate"),
        candidate_contents
    );
    assert!(candidate.exists(), "collision must not remove another file");
}

#[tokio::test]
async fn concurrent_saves_and_reads_observe_complete_payloads() {
    let temp_dir = tempfile::tempdir().expect("temporary data directory");
    let data_dir = temp_dir.path().to_path_buf();
    let initial = vec![b'i'; 1024 * 1024];
    let first = vec![b'a'; 4 * 1024 * 1024];
    let second = vec![b'b'; 4 * 1024 * 1024];
    write_project_state(&data_dir, &initial)
        .await
        .expect("write initial state");

    let reader = async {
        for _ in 0..64 {
            let current = read_project_state(&data_dir)
                .await
                .expect("read state during writes");
            assert!(
                current == initial || current == first || current == second,
                "reader observed a partial or interleaved payload"
            );
            tokio::task::yield_now().await;
        }
    };

    let (first_result, second_result, ()) = tokio::join!(
        write_project_state(&data_dir, &first),
        write_project_state(&data_dir, &second),
        reader,
    );
    first_result.expect("first concurrent save");
    second_result.expect("second concurrent save");

    let final_state = read_project_state(&data_dir)
        .await
        .expect("read final state");
    assert!(final_state == first || final_state == second);

    let state_path = project_state_path(&data_dir);
    let state_dir = state_path.parent().expect("project state directory");
    let entries: Vec<_> = std::fs::read_dir(state_dir)
        .expect("read project state directory")
        .map(|entry| entry.expect("directory entry").file_name())
        .collect();
    assert_eq!(entries, vec![std::ffi::OsString::from(PROJECT_STATE_FILE)]);
}

#[tokio::test]
async fn failed_replace_removes_request_temp_file() {
    let temp_dir = tempfile::tempdir().expect("temporary data directory");
    let data_dir = temp_dir.path();
    let destination = project_state_path(data_dir);
    let state_dir = destination.parent().expect("project state directory");
    tokio::fs::create_dir_all(&destination)
        .await
        .expect("create conflicting destination directory");

    let error = write_project_state(data_dir, b"bundle")
        .await
        .expect_err("renaming over a directory must fail");
    assert!(matches!(
        error.kind(),
        std::io::ErrorKind::IsADirectory | std::io::ErrorKind::PermissionDenied
    ));

    let entries: Vec<_> = std::fs::read_dir(state_dir)
        .expect("read project state directory")
        .map(|entry| entry.expect("directory entry").file_name())
        .collect();
    assert_eq!(entries, vec![std::ffi::OsString::from(PROJECT_STATE_FILE)]);
}

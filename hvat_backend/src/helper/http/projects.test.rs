use super::*;

use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use futures::stream;
use hvat_common::annotation_io::{BundleFile, ExportBundle};
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot};
use tower::ServiceExt;

use crate::helper::project_state::{project_state_path, write_project_state_with_test_hooks};

struct TestProjectState {
    data_dir: TempDir,
    loaders: ImageLoaderRegistry,
}

impl TestProjectState {
    fn new() -> Self {
        Self {
            data_dir: TempDir::new().expect("create test project directory"),
            loaders: ImageLoaderRegistry::new(),
        }
    }
}

impl ProjectRoutesState for TestProjectState {
    fn project_name(&self) -> &str {
        "project-state-test"
    }

    fn data_dir(&self) -> &Path {
        self.data_dir.path()
    }

    fn loaders(&self) -> &ImageLoaderRegistry {
        &self.loaders
    }
}

fn valid_payload(label: &str) -> Bytes {
    Bytes::from(
        serde_json::to_vec(&ExportBundle {
            files: vec![BundleFile {
                path: format!("annotations/{label}.json"),
                bytes: format!("{{\"label\":\"{label}\"}}").into_bytes(),
                mime_type: Some("application/json".to_string()),
            }],
        })
        .expect("serialize valid project bundle"),
    )
}

fn production_writer() -> ProjectStateWriter {
    Arc::new(|data_dir, payload| {
        Box::pin(async move { write_project_state(&data_dir, &payload).await })
    })
}

fn hooks_with_channels() -> (
    ProjectStateSaveTestHooks,
    mpsc::UnboundedReceiver<Bytes>,
    mpsc::UnboundedReceiver<Bytes>,
) {
    let (body_tx, body_rx) = mpsc::unbounded_channel();
    let (waiting_tx, waiting_rx) = mpsc::unbounded_channel();
    (
        ProjectStateSaveTestHooks {
            body_extracted: Some(Arc::new(move |body| {
                let _ = body_tx.send(body.clone());
            })),
            waiting_for_turn: Some(Arc::new(move |body| {
                let _ = waiting_tx.send(body.clone());
            })),
        },
        body_rx,
        waiting_rx,
    )
}

fn test_app(
    state: Arc<TestProjectState>,
    writer: ProjectStateWriter,
    hooks: ProjectStateSaveTestHooks,
) -> Router {
    router_with_test_writer::<TestProjectState>(writer, hooks).with_state(state)
}

fn put_request(body: Body) -> Request<Body> {
    Request::builder()
        .method(Method::PUT)
        .uri("/project-state")
        .body(body)
        .expect("build project-state PUT")
}

fn get_request() -> Request<Body> {
    Request::builder()
        .method(Method::GET)
        .uri("/project-state")
        .body(Body::empty())
        .expect("build project-state GET")
}

async fn read_route_state(app: Router) -> Bytes {
    let response = app
        .oneshot(get_request())
        .await
        .expect("route GET response");
    assert_eq!(response.status(), StatusCode::OK);
    to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read project-state response")
}

fn delayed_body(started: oneshot::Sender<()>, release: oneshot::Receiver<Bytes>) -> Body {
    Body::from_stream(stream::once(async move {
        let _ = started.send(());
        release
            .await
            .map_err(|_| io::Error::other("controlled request body was dropped"))
    }))
}

struct PausedCommitControls {
    writer: ProjectStateWriter,
    writer_events: mpsc::UnboundedReceiver<Bytes>,
    a_rename_started: oneshot::Receiver<()>,
    a_rename_release: oneshot::Sender<()>,
    b_write_release: oneshot::Sender<()>,
}

fn paused_commit_writer(a: Bytes, b: Bytes) -> PausedCommitControls {
    let (writer_events_tx, writer_events) = mpsc::unbounded_channel();
    let (a_rename_started_tx, a_rename_started) = oneshot::channel();
    let a_rename_started_tx = Arc::new(Mutex::new(Some(a_rename_started_tx)));
    let (a_rename_release, a_rename_release_rx) = oneshot::channel();
    let a_rename_release_rx = Arc::new(Mutex::new(Some(a_rename_release_rx)));
    let (b_write_release, b_write_release_rx) = oneshot::channel();
    let b_write_release_rx = Arc::new(Mutex::new(Some(b_write_release_rx)));
    let writer: ProjectStateWriter = Arc::new(move |data_dir, payload| {
        writer_events_tx
            .send(payload.clone())
            .expect("record persistence operation start");
        if payload == a {
            let candidate =
                project_state_path(&data_dir).with_file_name(".project-state-paused-commit-a.tmp");
            let started = a_rename_started_tx
                .lock()
                .expect("lock rename start sender")
                .take()
                .expect("one A operation");
            let release = a_rename_release_rx
                .lock()
                .expect("lock rename release receiver")
                .take()
                .expect("one A operation");
            Box::pin(async move {
                write_project_state_with_test_hooks(
                    &data_dir,
                    &payload,
                    candidate,
                    move |from, to| async move {
                        let _ = started.send(());
                        release
                            .await
                            .map_err(|_| io::Error::other("A rename release dropped"))?;
                        tokio::fs::rename(from, to).await
                    },
                )
                .await
            }) as ProjectStateWriteFuture
        } else if payload == b {
            let release = b_write_release_rx
                .lock()
                .expect("lock B write release receiver")
                .take()
                .expect("one B operation");
            Box::pin(async move {
                release
                    .await
                    .map_err(|_| io::Error::other("B write release dropped"))?;
                write_project_state(&data_dir, &payload).await
            })
        } else {
            Box::pin(async move { write_project_state(&data_dir, &payload).await })
        }
    });

    PausedCommitControls {
        writer,
        writer_events,
        a_rename_started,
        a_rename_release,
        b_write_release,
    }
}

#[tokio::test]
async fn project_state_request_arrival_order_uses_outer_put_call_before_body_completion() {
    let state = Arc::new(TestProjectState::new());
    let a = valid_payload("arrival-a");
    let b = valid_payload("arrival-b");
    let (writer_tx, mut writer_rx) = mpsc::unbounded_channel();
    let writer: ProjectStateWriter = Arc::new(move |data_dir, payload| {
        let writer_tx = writer_tx.clone();
        Box::pin(async move {
            writer_tx
                .send(payload.clone())
                .expect("record commit start");
            write_project_state(&data_dir, &payload).await
        })
    });
    let (hooks, mut body_rx, mut waiting_rx) = hooks_with_channels();
    let app = test_app(state, writer, hooks);

    let (a_started_tx, a_started_rx) = oneshot::channel();
    let (a_release_tx, a_release_rx) = oneshot::channel();
    let a_body = delayed_body(a_started_tx, a_release_rx);
    let a_app = app.clone();
    let a_request = tokio::spawn(async move { a_app.oneshot(put_request(a_body)).await });
    a_started_rx.await.expect("A body extraction started");

    let b_app = app.clone();
    let b_body = b.clone();
    let b_request =
        tokio::spawn(async move { b_app.oneshot(put_request(Body::from(b_body))).await });
    assert_eq!(
        body_rx.recv().await.expect("B body was extracted"),
        b,
        "B must finish body extraction while A remains held"
    );

    tokio::select! {
        waiting = waiting_rx.recv() => {
            assert_eq!(waiting, Some(b.clone()), "B waits behind earlier A ticket");
        }
        event = writer_rx.recv() => {
            panic!("a save started before the earlier request body was released: {event:?}");
        }
    }

    a_release_tx.send(a.clone()).expect("release A body");
    let a_response = a_request
        .await
        .expect("A request task")
        .expect("A route response");
    let b_response = b_request
        .await
        .expect("B request task")
        .expect("B route response");
    assert_eq!(a_response.status(), StatusCode::NO_CONTENT);
    assert_eq!(b_response.status(), StatusCode::NO_CONTENT);
    assert_eq!(writer_rx.recv().await, Some(a.clone()));
    assert_eq!(writer_rx.recv().await, Some(b.clone()));
    assert_eq!(read_route_state(app).await, b);
}

#[tokio::test]
async fn project_state_request_arrival_order_skips_rejected_and_precommit_cancelled_requests() {
    let state = Arc::new(TestProjectState::new());
    let old = b"previous saved state";
    write_project_state(state.data_dir(), old)
        .await
        .expect("seed existing project state");
    let app = test_app(
        state.clone(),
        production_writer(),
        ProjectStateSaveTestHooks::default(),
    );

    let empty = app
        .clone()
        .oneshot(put_request(Body::empty()))
        .await
        .expect("empty-body response");
    assert_eq!(empty.status(), StatusCode::BAD_REQUEST);

    let malformed = app
        .clone()
        .oneshot(put_request(Body::from("not a bundle")))
        .await
        .expect("invalid-bundle response");
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);

    let extractor_error = Body::from_stream(stream::once(async {
        Err::<Bytes, _>(io::Error::other("injected body read failure"))
    }));
    let rejected = app
        .clone()
        .oneshot(put_request(extractor_error))
        .await
        .expect("body-extractor rejection response");
    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);

    let oversized = Body::from(vec![0_u8; 2 * 1024 * 1024 + 1]);
    let rejected = app
        .clone()
        .oneshot(put_request(oversized))
        .await
        .expect("oversized-body rejection response");
    assert_eq!(rejected.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        read_project_state(state.data_dir())
            .await
            .expect("old state remains"),
        old,
        "pre-commit rejections must not touch the saved state"
    );

    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let cancel_app = app.clone();
    let cancelled = tokio::spawn(async move {
        cancel_app
            .oneshot(put_request(delayed_body(started_tx, release_rx)))
            .await
    });
    started_rx.await.expect("cancelled body extraction started");
    cancelled.abort();
    assert!(
        cancelled
            .await
            .expect_err("request task was aborted")
            .is_cancelled()
    );
    drop(release_tx);
    assert_eq!(
        read_project_state(state.data_dir())
            .await
            .expect("old state remains after pre-commit cancellation"),
        old,
        "pre-commit cancellation must not mutate the saved state"
    );

    let next = valid_payload("after-cancel");
    let response = app
        .clone()
        .oneshot(put_request(Body::from(next.clone())))
        .await
        .expect("following save response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(read_route_state(app).await, next);
}

#[tokio::test]
async fn project_state_request_arrival_order_releases_after_failed_atomic_replace() {
    let state = Arc::new(TestProjectState::new());
    let old = b"existing regular destination";
    write_project_state(state.data_dir(), old)
        .await
        .expect("seed existing project state");
    let failing_payload = valid_payload("injected-rename-failure");
    let failing_candidate = project_state_path(state.data_dir())
        .with_file_name(".project-state-injected-rename-failure.tmp");
    let writer: ProjectStateWriter = {
        let failing_payload = failing_payload.clone();
        let failing_candidate = failing_candidate.clone();
        Arc::new(move |data_dir, payload| {
            if payload == failing_payload {
                let candidate = failing_candidate.clone();
                Box::pin(async move {
                    write_project_state_with_test_hooks(
                        &data_dir,
                        &payload,
                        candidate,
                        |_from, _to| async {
                            Err(io::Error::other("injected atomic rename failure"))
                        },
                    )
                    .await
                }) as ProjectStateWriteFuture
            } else {
                Box::pin(async move { write_project_state(&data_dir, &payload).await })
            }
        })
    };
    let app = test_app(state.clone(), writer, ProjectStateSaveTestHooks::default());

    let failed = app
        .clone()
        .oneshot(put_request(Body::from(failing_payload)))
        .await
        .expect("injected failing save response");
    assert_eq!(failed.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        read_project_state(state.data_dir())
            .await
            .expect("destination preserved"),
        old,
        "a failed rename must preserve the existing regular destination"
    );
    assert!(
        !failing_candidate.exists(),
        "the failed request must remove its own temporary file"
    );

    let next = valid_payload("after-failed-rename");
    let response = app
        .clone()
        .oneshot(put_request(Body::from(next.clone())))
        .await
        .expect("save after failed rename response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(read_route_state(app).await, next);
}

#[tokio::test]
async fn project_state_request_arrival_order_retains_turn_after_response_cancellation() {
    let state = Arc::new(TestProjectState::new());
    let a = valid_payload("cancelled-response-a");
    let b = valid_payload("after-cancelled-response-b");
    let PausedCommitControls {
        writer,
        mut writer_events,
        a_rename_started,
        a_rename_release,
        b_write_release,
    } = paused_commit_writer(a.clone(), b.clone());
    let (hooks, mut body_rx, mut waiting_rx) = hooks_with_channels();
    let app = test_app(state.clone(), writer, hooks);

    let a_app = app.clone();
    let a_body = a.clone();
    let a_request =
        tokio::spawn(async move { a_app.oneshot(put_request(Body::from(a_body))).await });
    assert_eq!(
        writer_events
            .recv()
            .await
            .expect("A persistence operation started"),
        a
    );
    a_rename_started
        .await
        .expect("A reached the paused atomic rename");
    assert_eq!(
        body_rx.recv().await.expect("A body was extracted"),
        a,
        "A must be extracted before its commit begins"
    );
    a_request.abort();
    assert!(
        a_request
            .await
            .expect_err("response task was cancelled")
            .is_cancelled()
    );

    let b_app = app.clone();
    let b_body = b.clone();
    let b_request =
        tokio::spawn(async move { b_app.oneshot(put_request(Body::from(b_body))).await });
    assert_eq!(
        body_rx.recv().await.expect("B body was extracted"),
        b,
        "B must reach the route while A's rename is paused"
    );
    tokio::select! {
        waiting = waiting_rx.recv() => {
            assert_eq!(waiting, Some(b.clone()), "B remains behind A's commit turn");
        }
        event = writer_events.recv() => {
            panic!("B overtook the still-running A commit: {event:?}");
        }
    }

    a_rename_release.send(()).expect("settle A atomic rename");
    assert_eq!(
        writer_events
            .recv()
            .await
            .expect("B starts after A rename settles"),
        b
    );
    assert_eq!(
        read_route_state(app.clone()).await,
        a,
        "A's commit settles before B is allowed to persist"
    );

    b_write_release.send(()).expect("release B write");
    let b_response = b_request
        .await
        .expect("B request task")
        .expect("B route response");
    assert_eq!(b_response.status(), StatusCode::NO_CONTENT);
    assert_eq!(read_route_state(app).await, b);
}

//! Streaming multipart-overhead acceptance tests against the production router.

use std::{path::Path, sync::Arc};

use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    http::{Request, StatusCode, header},
    response::Response,
};
use futures::stream;
use hvat_backend::{
    common::upload::MAX_MULTIPART_OVERHEAD_BYTES,
    simple::{AppState, ServerConfig, routes},
};
use tempfile::TempDir;
use tower::ServiceExt;

fn upload_app(data_dir: &Path) -> Router {
    let state = Arc::new(AppState::new(ServerConfig {
        port: 0,
        data_dir: data_dir.to_path_buf(),
        cache_dir: data_dir.join(".cache"),
        max_cache_memory: 1024 * 1024,
        project_name: "multipart-overhead-test".to_string(),
    }));

    Router::new()
        .nest("/api", routes::api_router())
        .with_state(state)
}

async fn post_streamed_upload(app: Router, boundary: &str, frames: Vec<Bytes>) -> Response {
    let body = Body::from_stream(stream::iter(
        frames.into_iter().map(Ok::<Bytes, std::io::Error>),
    ));
    let request = Request::builder()
        .method("POST")
        .uri("/api/images/upload")
        .header(
            header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(body)
        .expect("build streamed upload request");
    assert!(
        request.headers().get(header::CONTENT_LENGTH).is_none(),
        "streamed fixtures intentionally omit Content-Length"
    );

    app.oneshot(request)
        .await
        .expect("call production upload router")
}

async fn response_text(response: Response) -> String {
    String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read upload response body")
            .to_vec(),
    )
    .expect("upload response is UTF-8")
}

fn single_file_body(boundary: &str, filename: &str, payload: &[u8]) -> (Vec<u8>, usize) {
    let mut body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"{filename}\"\r\nContent-Type: image/png\r\n\r\n"
    )
    .into_bytes();
    body.extend_from_slice(payload);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    let overhead = body.len() - payload.len();
    (body, overhead)
}

fn assert_no_upload_temporary_files(data_dir: &Path) {
    let entries = std::fs::read_dir(data_dir)
        .expect("read upload data directory")
        .map(|entry| entry.expect("read upload directory entry").path())
        .collect::<Vec<_>>();
    assert!(
        entries.iter().all(|path| {
            !path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().contains(".uploading-"))
        }),
        "failed upload must not leave .uploading-* temporary files: {entries:?}"
    );
}

async fn assert_successful_upload(data_dir: &Path, boundary: &str, frames: Vec<Bytes>) {
    let response = post_streamed_upload(upload_app(data_dir), boundary, frames).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = response_text(response).await;
    let value: serde_json::Value = serde_json::from_str(&body).expect("decode upload response");
    assert_eq!(value["uploaded"], 1);
    assert_eq!(
        std::fs::read(data_dir.join("budget.png")).expect("read completed upload"),
        b"file-payload"
    );
    assert_no_upload_temporary_files(data_dir);
}

async fn assert_overhead_rejected_after_completed_file(
    data_dir: &Path,
    boundary: &str,
    frames: Vec<Bytes>,
) {
    let response = post_streamed_upload(upload_app(data_dir), boundary, frames).await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let error = response_text(response).await;
    assert!(
        error.starts_with("PARTIAL_UPLOAD:"),
        "unexpected partial-upload error: {error}"
    );
    assert!(
        error.contains("1 file(s) were kept"),
        "unexpected error: {error}"
    );
    assert_eq!(
        std::fs::read(data_dir.join("budget.png")).expect("completed upload is retained"),
        b"file-payload"
    );
    assert!(!data_dir.join("never.png").exists());
    assert_no_upload_temporary_files(data_dir);
}

#[tokio::test]
async fn rest_upload_rejects_same_ready_chunk_overhead_after_completed_file() {
    let temp_dir = TempDir::new().expect("create route-test data directory");
    let boundary = "hvat-route-overhead-header";
    let payload = b"file-payload";

    let mut body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"budget.png\"\r\nContent-Type: image/png\r\n\r\n"
    )
    .into_bytes();
    body.extend_from_slice(payload);
    body.extend_from_slice(format!("\r\n--{boundary}\r\n").as_bytes());

    let later_header_prefix = b"Content-Disposition: form-data; name=\"files\"; filename=\"never.png\"\r\nContent-Type: image/png\r\nX-Padding: ";
    let overhead_before_padding = body.len() - payload.len() + later_header_prefix.len() + 4;
    let padding_len = (MAX_MULTIPART_OVERHEAD_BYTES + 1)
        .checked_sub(overhead_before_padding)
        .expect("fixture prefix fits within the overhead budget");
    body.extend_from_slice(later_header_prefix);
    body.extend(std::iter::repeat_n(b'h', padding_len));
    body.extend_from_slice(b"\r\n\r\n");
    assert_eq!(body.len() - payload.len(), MAX_MULTIPART_OVERHEAD_BYTES + 1);

    // A single upstream data frame ensures Multer cannot read the later header
    // until the adapter has allowed the first completed file to be finalized.
    assert_overhead_rejected_after_completed_file(
        temp_dir.path(),
        boundary,
        vec![Bytes::from(body)],
    )
    .await;
}

#[tokio::test]
async fn rest_upload_counts_late_epilogue_at_budget_edge() {
    let boundary = "hvat-route-overhead-epilogue";
    let (terminal_body, framing_overhead) =
        single_file_body(boundary, "budget.png", b"file-payload");
    assert!(framing_overhead < MAX_MULTIPART_OVERHEAD_BYTES);

    // Same-frame epilogue bytes are accepted while the aggregate remains under
    // budget, even though Multer has already observed its terminal delimiter.
    let same_frame_ok_dir = TempDir::new().expect("create same-frame success directory");
    let mut same_frame_ok = terminal_body.clone();
    same_frame_ok.extend_from_slice(b"accepted epilogue");
    assert_successful_upload(
        same_frame_ok_dir.path(),
        boundary,
        vec![Bytes::from(same_frame_ok)],
    )
    .await;

    // One ready frame contains the complete valid file, terminal boundary and
    // the epilogue byte that takes aggregate overhead to 1,048,577.
    let same_frame_over_dir = TempDir::new().expect("create same-frame overflow directory");
    let same_frame_over_len = MAX_MULTIPART_OVERHEAD_BYTES + 1 - framing_overhead;
    let mut same_frame_over = terminal_body.clone();
    same_frame_over.extend(std::iter::repeat_n(b'e', same_frame_over_len));
    assert_overhead_rejected_after_completed_file(
        same_frame_over_dir.path(),
        boundary,
        vec![Bytes::from(same_frame_over)],
    )
    .await;

    // Later body frames remain owned by the raw adapter after Multer reports
    // EOF; the exact aggregate edge is accepted and the next byte is rejected.
    let later_frame_exact_dir = TempDir::new().expect("create later-frame exact directory");
    let later_frame_exact_len = MAX_MULTIPART_OVERHEAD_BYTES - framing_overhead;
    assert_successful_upload(
        later_frame_exact_dir.path(),
        boundary,
        vec![
            Bytes::from(terminal_body.clone()),
            Bytes::from(vec![b'e'; later_frame_exact_len]),
        ],
    )
    .await;

    let later_frame_over_dir = TempDir::new().expect("create later-frame overflow directory");
    let later_frame_over_len = MAX_MULTIPART_OVERHEAD_BYTES + 1 - framing_overhead;
    assert_overhead_rejected_after_completed_file(
        later_frame_over_dir.path(),
        boundary,
        vec![
            Bytes::from(terminal_body),
            Bytes::from(vec![b'e'; later_frame_over_len]),
        ],
    )
    .await;
}

use super::*;
use axum::body::{Body, Bytes};
use axum::extract::FromRequest;
use axum::http::{Request, header};
use futures::{StreamExt, stream};

fn multipart_body(boundary: &str, body: &[u8], complete: bool) -> Vec<u8> {
    let mut multipart = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"test.png\"\r\nContent-Type: image/png\r\n\r\n"
    )
    .into_bytes();
    multipart.extend_from_slice(body);
    if complete {
        multipart.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    }
    multipart
}

fn multipart_upload_body(
    boundary: &str,
    target_root: Option<&str>,
    files: &[(&str, &[u8])],
) -> Vec<u8> {
    let mut body = Vec::new();
    if let Some(target_root) = target_root {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"target_root\"\r\n\r\n{target_root}\r\n"
            )
            .as_bytes(),
        );
    }
    for (filename, payload) in files {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"{filename}\"\r\nContent-Type: image/png\r\n\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(payload);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    body
}

fn target_root_body(boundary: &str, value: &[u8]) -> Vec<u8> {
    let mut multipart =
        format!("--{boundary}\r\nContent-Disposition: form-data; name=\"target_root\"\r\n\r\n")
            .into_bytes();
    multipart.extend_from_slice(value);
    multipart.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    multipart
}

fn two_file_body(boundary: &str) -> Vec<u8> {
    let mut multipart = multipart_body(boundary, b"first-png", false);
    multipart.extend_from_slice(
        format!(
            "\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"unsupported.bin\"\r\nContent-Type: application/octet-stream\r\n\r\nunsupported"
        )
        .as_bytes(),
    );
    multipart.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    multipart
}

fn two_file_io_failure_body(boundary: &str) -> Vec<u8> {
    let mut multipart = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"first.png\"\r\nContent-Type: image/png\r\n\r\n"
    )
    .into_bytes();
    multipart.extend_from_slice(b"first-completed-bytes");
    multipart.extend_from_slice(
        format!(
            "\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"failed.png\"\r\nContent-Type: image/png\r\n\r\n"
        )
        .as_bytes(),
    );
    multipart.extend_from_slice(b"failed-part-bytes");
    multipart.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    multipart
}

async fn parse_multipart(boundary: &str, body: Vec<u8>) -> Multipart {
    let request = Request::builder()
        .header(
            header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(Body::from(body))
        .expect("build multipart request");
    Multipart::from_request(request, &())
        .await
        .expect("parse multipart request")
}

fn multipart_stream_request(boundary: &str, frames: Vec<Bytes>) -> Request<Body> {
    let body_stream = stream::iter(frames.into_iter().map(Ok::<Bytes, io::Error>));
    Request::builder()
        .header(
            header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(Body::from_stream(body_stream))
        .expect("build streamed multipart request")
}

async fn parse_metered_multipart(
    boundary: &str,
    frames: Vec<Bytes>,
) -> (Multipart, MultipartOverheadMeter) {
    let request = multipart_stream_request(boundary, frames);
    assert!(
        request.headers().get(header::CONTENT_LENGTH).is_none(),
        "metered fixtures intentionally omit Content-Length"
    );
    let (request, meter) = meter_multipart_request(request);
    let multipart = Multipart::from_request(request, &())
        .await
        .expect("parse metered multipart request");
    (
        multipart,
        meter.expect("valid multipart Content-Type is metered"),
    )
}

fn one_file_body(boundary: &str, payload: &[u8], epilogue: &[u8]) -> (Vec<u8>, usize) {
    let mut body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"budget.png\"\r\nContent-Type: image/png\r\n\r\n"
    )
    .into_bytes();
    body.extend_from_slice(payload);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    body.extend_from_slice(epilogue);
    let overhead_bytes = body.len() - payload.len() - epilogue.len();
    (body, overhead_bytes)
}

async fn run_metered_upload(
    data_dir: &Path,
    boundary: &str,
    frames: Vec<Bytes>,
) -> (
    std::result::Result<UploadedFiles, (StatusCode, String)>,
    Arc<MultipartOverheadStats>,
) {
    let (multipart, meter) = parse_metered_multipart(boundary, frames).await;
    let stats = meter.stats.clone();
    let result = save_uploaded_images_with_overhead(data_dir, multipart, |_| true, meter).await;
    (result, stats)
}

fn directory_entries(path: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(path)
        .expect("read upload directory")
        .map(|entry| entry.expect("directory entry").path())
        .collect()
}

#[tokio::test]
async fn successful_upload_persists_file_and_removes_temp_file() {
    let temp_dir = tempfile::tempdir().expect("temporary upload directory");
    let boundary = "hvat-success-boundary";
    let multipart = parse_multipart(boundary, multipart_body(boundary, b"png-bytes", true)).await;

    let uploaded = save_uploaded_images(temp_dir.path(), multipart, |_| true)
        .await
        .expect("save uploaded image");

    assert_eq!(uploaded.files, ["test.png"]);
    assert_eq!(
        std::fs::read(temp_dir.path().join("test.png")).expect("read uploaded image"),
        b"png-bytes"
    );
    assert_eq!(
        directory_entries(temp_dir.path()),
        [temp_dir.path().join("test.png")]
    );
}

#[tokio::test]
async fn truncated_multipart_removes_partial_upload_temp_file() {
    let temp_dir = tempfile::tempdir().expect("temporary upload directory");
    let boundary = "hvat-truncated-boundary";
    let (multipart, meter) = parse_metered_multipart(
        boundary,
        vec![Bytes::from(multipart_body(boundary, b"partial-png", false))],
    )
    .await;

    let result =
        save_uploaded_images_with_overhead(temp_dir.path(), multipart, |_| true, meter).await;
    assert!(result.is_err(), "truncated multipart payload should fail");
    assert!(
        directory_entries(temp_dir.path()).is_empty(),
        "failed upload must not leave temporary files"
    );
}

#[tokio::test]
async fn cancelled_upload_removes_partial_temp_file() {
    let temp_dir = tempfile::tempdir().expect("temporary upload directory");
    let data_dir = temp_dir.path().to_path_buf();
    let destination = temp_dir.path().join("test.png");
    std::fs::write(&destination, b"previous-image-bytes").expect("seed existing destination");
    let boundary = "hvat-cancel-boundary";
    let first_chunk = Bytes::from(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"test.png\"\r\nContent-Type: image/png\r\n\r\npartial-png"
        )
        .into_bytes(),
    );
    let request_stream = futures::stream::once(async move { Ok::<_, std::io::Error>(first_chunk) })
        .chain(futures::stream::pending());

    let task = tokio::spawn(async move {
        let request = Request::builder()
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from_stream(request_stream))
            .expect("build streaming multipart request");
        let (request, meter) = meter_multipart_request(request);
        let multipart = Multipart::from_request(request, &())
            .await
            .expect("parse streaming multipart request");
        save_uploaded_images_with_overhead(
            &data_dir,
            multipart,
            |_| true,
            meter.expect("valid multipart request is metered"),
        )
        .await
    });

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut nonempty_partial_file_exists = false;
    while std::time::Instant::now() < deadline {
        nonempty_partial_file_exists = directory_entries(temp_dir.path()).iter().any(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().contains(".uploading-"))
                && std::fs::metadata(path).is_ok_and(|metadata| metadata.len() > 0)
        });
        if nonempty_partial_file_exists {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        nonempty_partial_file_exists,
        "upload should write nonempty contents to its temporary file"
    );

    task.abort();
    let _ = task.await;
    assert_eq!(
        std::fs::read(&destination).expect("previous destination remains readable"),
        b"previous-image-bytes",
        "cancelled upload must preserve the existing destination"
    );
    let entries = directory_entries(temp_dir.path());
    assert_eq!(entries, [destination]);
    assert!(entries.iter().all(|path| {
        !path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().contains(".uploading-"))
    }));
}

#[tokio::test]
async fn upload_stage_acquisition_cancellation_cleanup() {
    let temp_dir = tempfile::tempdir().expect("temporary upload directory");
    let completed_boundary = "hvat-acquisition-completed-boundary";
    let completed = save_uploaded_images(
        temp_dir.path(),
        parse_multipart(
            completed_boundary,
            multipart_upload_body(
                completed_boundary,
                None,
                &[("earlier.png", b"completed-file-bytes")],
            ),
        )
        .await,
        |_| true,
    )
    .await
    .expect("persist earlier completed file");
    assert_eq!(completed.files, ["earlier.png"]);

    let destination = temp_dir.path().join("existing.png");
    std::fs::write(&destination, b"existing-destination-bytes").expect("seed existing destination");
    let completed_path = temp_dir.path().join("earlier.png");
    let (acquired_tx, acquired_rx) = oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (cleanup_tx, cleanup_rx) = oneshot::channel();
    let request_destination = destination.clone();
    let task = tokio::spawn(async move {
        create_upload_temp_output_with_test_hook(
            &request_destination,
            "existing.png",
            UploadTempAcquisitionTestHook {
                acquired: acquired_tx,
                release: release_rx,
                cleanup_complete: cleanup_tx,
            },
        )
        .await
    });

    let stage_path = acquired_rx
        .await
        .expect("exclusive stage acquired before the blocking closure is released");
    assert!(stage_path.exists(), "the owned stage exists at the barrier");
    task.abort();
    let cancellation = match task.await {
        Err(error) => error,
        Ok(_) => panic!("request future was cancelled"),
    };
    assert!(cancellation.is_cancelled());

    release_tx
        .send(())
        .expect("release outstanding exclusive acquisition");
    cleanup_rx
        .await
        .expect("detached acquisition output closed and cleaned its owned stage");

    assert!(
        !stage_path.exists(),
        "cancelled acquisition leaves no stage"
    );
    assert_eq!(
        std::fs::read(&destination).expect("existing destination remains readable"),
        b"existing-destination-bytes"
    );
    assert_eq!(
        std::fs::read(&completed_path).expect("earlier completed upload remains readable"),
        b"completed-file-bytes"
    );
    let entries = directory_entries(temp_dir.path());
    assert_eq!(entries.len(), 2, "only completed and existing files remain");
    assert!(entries.contains(&destination));
    assert!(entries.contains(&completed_path));
    assert!(entries.iter().all(|path| {
        !path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().contains(".uploading-"))
    }));
}

#[tokio::test]
async fn target_root_is_bounded_while_streaming() {
    let boundary = "hvat-target-root-boundary";
    let at_limit = vec![b'a'; MAX_UPLOAD_TARGET_ROOT_BYTES];
    let mut multipart = parse_multipart(boundary, target_root_body(boundary, &at_limit)).await;
    let field = multipart
        .next_field()
        .await
        .expect("read target_root part")
        .expect("target_root field exists");
    assert_eq!(
        parse_target_root_field(field, false)
            .await
            .expect("target_root exactly at limit is accepted")
            .len(),
        MAX_UPLOAD_TARGET_ROOT_BYTES
    );

    let over_limit = vec![b'a'; MAX_UPLOAD_TARGET_ROOT_BYTES + 1];
    let mut multipart = parse_multipart(boundary, target_root_body(boundary, &over_limit)).await;
    let field = multipart
        .next_field()
        .await
        .expect("read target_root part")
        .expect("target_root field exists");
    let error = parse_target_root_field(field, false)
        .await
        .expect_err("oversized target_root must fail before normalization");
    assert_eq!(error.0, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn duplicate_target_root_fields_are_rejected() {
    let temp_dir = tempfile::tempdir().expect("temporary upload directory");
    let boundary = "hvat-duplicate-target-root";
    let body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"target_root\"\r\n\r\nfirst\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"target_root\"\r\n\r\nsecond\r\n--{boundary}--\r\n"
    )
    .into_bytes();
    let multipart = parse_multipart(boundary, body).await;

    let error = save_uploaded_images(temp_dir.path(), multipart, |_| true)
        .await
        .expect_err("duplicate target_root must fail");

    assert_eq!(error.0, StatusCode::BAD_REQUEST);
    assert!(error.1.contains("Only one 'target_root'"));
    assert!(directory_entries(temp_dir.path()).is_empty());
}

#[tokio::test]
async fn completed_files_are_kept_when_a_later_file_fails() {
    let temp_dir = tempfile::tempdir().expect("temporary upload directory");
    let boundary = "hvat-partial-upload-boundary";
    let multipart = parse_multipart(boundary, two_file_body(boundary)).await;

    let error = save_uploaded_images(temp_dir.path(), multipart, |path| {
        path.extension().is_some_and(|extension| extension == "png")
    })
    .await
    .expect_err("unsupported later file should fail the request");

    assert_eq!(error.0, StatusCode::BAD_REQUEST);
    assert!(error.1.starts_with(PARTIAL_UPLOAD_ERROR_PREFIX));
    assert!(error.1.contains("1 file(s) were kept"));
    assert_eq!(
        std::fs::read(temp_dir.path().join("test.png")).expect("first file remains committed"),
        b"first-png"
    );
    assert!(!temp_dir.path().join("unsupported.bin").exists());
    assert_eq!(directory_entries(temp_dir.path()).len(), 1);
}

#[tokio::test]
async fn unnamed_multipart_field_read_error_preserves_completed_file_partial_status() {
    let temp_dir = tempfile::tempdir().expect("temporary upload directory");
    let boundary = "hvat-unnamed-field-failure-boundary";
    let mut body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"first.png\"\r\nContent-Type: image/png\r\n\r\n"
    )
    .into_bytes();
    body.extend_from_slice(b"first-completed-bytes");
    body.extend_from_slice(
        format!("\r\n--{boundary}\r\nContent-Disposition: form-data\r\n\r\nunnamed-payload")
            .as_bytes(),
    );
    let body_stream = stream::iter([
        Ok::<Bytes, io::Error>(Bytes::from(body)),
        Err(io::Error::other("injected multipart body read failure")),
    ]);
    let request = Request::builder()
        .header(
            header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(Body::from_stream(body_stream))
        .expect("build failing multipart request");
    let (request, meter) = meter_multipart_request(request);
    let multipart = Multipart::from_request(request, &())
        .await
        .expect("extract streamed multipart request");
    let error = save_uploaded_images_with_overhead(
        temp_dir.path(),
        multipart,
        |_| true,
        meter.expect("valid multipart request is metered"),
    )
    .await
    .expect_err("unnamed-field body read failure must fail the upload");

    assert_eq!(error.0, StatusCode::BAD_REQUEST);
    assert!(error.1.starts_with(PARTIAL_UPLOAD_ERROR_PREFIX));
    assert!(error.1.contains("1 file(s) were kept"));
    assert!(error.1.contains("Failed to read unnamed multipart field"));
    assert_eq!(
        std::fs::read(temp_dir.path().join("first.png")).expect("completed file is retained"),
        b"first-completed-bytes"
    );
    assert_eq!(
        directory_entries(temp_dir.path()),
        [temp_dir.path().join("first.png")]
    );
}

#[tokio::test]
async fn multipart_overhead_budget_accepts_exact_limit_and_rejects_first_excess_byte() {
    let boundary = "hvat-overhead-edge-boundary";
    let (_, framing_overhead) = one_file_body(boundary, b"png-bytes", b"");
    // Keep the CRLF after the terminal `--` outside Multer with the raw
    // epilogue tail; it is still charged to the aggregate overhead meter.
    let parser_visible_overhead = framing_overhead - b"\r\n".len();

    let exact_epilogue = vec![b'e'; MAX_MULTIPART_OVERHEAD_BYTES - framing_overhead];
    let (exact_body, exact_framing) = one_file_body(boundary, b"png-bytes", &exact_epilogue);
    assert_eq!(
        exact_framing + exact_epilogue.len(),
        MAX_MULTIPART_OVERHEAD_BYTES
    );
    let exact_dir = tempfile::tempdir().expect("exact-edge upload directory");
    let (exact_result, exact_stats) =
        run_metered_upload(exact_dir.path(), boundary, vec![Bytes::from(exact_body)]).await;
    assert_eq!(
        exact_result
            .expect("exact overhead limit is accepted")
            .files,
        ["budget.png"]
    );
    assert_eq!(
        exact_stats.overhead_bytes.load(Ordering::Acquire),
        MAX_MULTIPART_OVERHEAD_BYTES
    );
    assert_eq!(
        exact_stats.parser_overhead_bytes.load(Ordering::Acquire),
        parser_visible_overhead
    );

    let over_epilogue = vec![b'e'; MAX_MULTIPART_OVERHEAD_BYTES + 1 - framing_overhead];
    let (over_body, over_framing) = one_file_body(boundary, b"png-bytes", &over_epilogue);
    assert_eq!(
        over_framing + over_epilogue.len(),
        MAX_MULTIPART_OVERHEAD_BYTES + 1
    );
    let over_dir = tempfile::tempdir().expect("over-limit upload directory");
    let (over_result, over_stats) =
        run_metered_upload(over_dir.path(), boundary, vec![Bytes::from(over_body)]).await;
    let (status, message) = over_result.expect_err("first byte over limit is rejected");
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(message.starts_with(PARTIAL_UPLOAD_ERROR_PREFIX));
    assert!(message.contains("1 file(s) were kept"));
    assert_eq!(
        std::fs::read(over_dir.path().join("budget.png")).expect("completed file is retained"),
        b"png-bytes"
    );
    assert_eq!(
        over_stats.overhead_bytes.load(Ordering::Acquire),
        MAX_MULTIPART_OVERHEAD_BYTES
    );
    assert_eq!(
        over_stats.parser_overhead_bytes.load(Ordering::Acquire),
        over_framing - b"\r\n".len()
    );
    assert!(directory_entries(over_dir.path()).iter().all(|path| {
        !path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().contains(".uploading-"))
    }));
}

#[tokio::test]
async fn multipart_overhead_budget_is_chunk_split_invariant_for_delimiters_and_headers() {
    let temp_dir = tempfile::tempdir().expect("split multipart upload directory");
    let boundary = "hvat-overhead-split-boundary";
    let (body, expected_overhead) = one_file_body(boundary, b"payload", b"epilogue");
    let frames = body
        .iter()
        .map(|byte| Bytes::copy_from_slice(std::slice::from_ref(byte)))
        .collect();

    let (result, stats) = run_metered_upload(temp_dir.path(), boundary, frames).await;
    assert_eq!(
        result
            .expect("single-byte body frames remain parser-compatible")
            .files,
        ["budget.png"]
    );
    assert_eq!(
        std::fs::read(temp_dir.path().join("budget.png")).expect("read split upload"),
        b"payload"
    );
    assert_eq!(
        stats.overhead_bytes.load(Ordering::Acquire),
        expected_overhead + b"epilogue".len()
    );
}

#[tokio::test]
async fn multipart_overhead_budget_excludes_large_file_and_maximum_target_root_payloads() {
    let temp_dir = tempfile::tempdir().expect("large multipart upload directory");
    let boundary = "hvat-overhead-payload-boundary";
    let target_root = vec![b' '; MAX_UPLOAD_TARGET_ROOT_BYTES];
    let file_payload = vec![b'p'; 1_100_000];
    let mut body =
        format!("--{boundary}\r\nContent-Disposition: form-data; name=\"target_root\"\r\n\r\n")
            .into_bytes();
    body.extend_from_slice(&target_root);
    body.extend_from_slice(
        format!(
            "\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"large.png\"\r\nContent-Type: image/png\r\n\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(&file_payload);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    let expected_overhead = body.len() - target_root.len() - file_payload.len();

    let (result, stats) =
        run_metered_upload(temp_dir.path(), boundary, vec![Bytes::from(body)]).await;
    assert_eq!(
        result
            .expect("payload bytes use their separate limits")
            .files,
        ["large.png"]
    );
    assert_eq!(
        std::fs::metadata(temp_dir.path().join("large.png"))
            .expect("large uploaded file exists")
            .len(),
        file_payload.len() as u64
    );
    assert_eq!(
        stats.overhead_bytes.load(Ordering::Acquire),
        expected_overhead
    );
    assert!(file_payload.len() > MAX_MULTIPART_OVERHEAD_BYTES);
    assert_eq!(target_root.len(), MAX_UPLOAD_TARGET_ROOT_BYTES);
}

#[tokio::test]
async fn multipart_overhead_budget_rejects_oversized_incomplete_headers_before_excess_byte_reaches_parser()
 {
    let boundary = "hvat-overhead-header-boundary";
    let prefix = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"never.png\"\r\nX-Pad: "
    )
    .into_bytes();

    let exact_dir = tempfile::tempdir().expect("exact incomplete-header directory");
    let mut exact_header = prefix.clone();
    exact_header.resize(MAX_MULTIPART_OVERHEAD_BYTES, b'h');
    let (exact_result, exact_stats) =
        run_metered_upload(exact_dir.path(), boundary, vec![Bytes::from(exact_header)]).await;
    let (exact_status, _) = exact_result.expect_err("unterminated in-budget header is malformed");
    assert_eq!(exact_status, StatusCode::BAD_REQUEST);
    assert!(!exact_stats.exceeded.load(Ordering::Acquire));
    assert_eq!(
        exact_stats.parser_overhead_bytes.load(Ordering::Acquire),
        MAX_MULTIPART_OVERHEAD_BYTES
    );
    assert!(directory_entries(exact_dir.path()).is_empty());

    let over_dir = tempfile::tempdir().expect("over-limit incomplete-header directory");
    let mut over_header = prefix;
    over_header.resize(MAX_MULTIPART_OVERHEAD_BYTES + 1, b'h');
    let (over_result, over_stats) =
        run_metered_upload(over_dir.path(), boundary, vec![Bytes::from(over_header)]).await;
    let (over_status, message) = over_result.expect_err("header overflow is rejected");
    assert_eq!(over_status, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(message.contains("Multipart body overhead"));
    assert!(over_stats.exceeded.load(Ordering::Acquire));
    assert_eq!(
        over_stats.overhead_bytes.load(Ordering::Acquire),
        MAX_MULTIPART_OVERHEAD_BYTES
    );
    assert_eq!(
        over_stats.parser_overhead_bytes.load(Ordering::Acquire),
        MAX_MULTIPART_OVERHEAD_BYTES,
        "the first over-budget header byte must not reach Multer"
    );
    assert!(directory_entries(over_dir.path()).is_empty());
}

#[tokio::test]
async fn managed_keep_both_allocation_preserves_parent_extension_and_skips_occupied_suffixes() {
    let temp_dir = tempfile::tempdir().expect("temporary upload directory");
    let nested = temp_dir.path().join("folder");
    std::fs::create_dir_all(&nested).expect("create nested upload directory");
    std::fs::write(nested.join("sample.png"), b"original-image").expect("seed original image");
    std::fs::write(nested.join("sample_v2.png"), b"occupied-v2").expect("seed occupied v2 path");
    std::fs::write(nested.join("sample_v3.png"), b"occupied-v3").expect("seed occupied v3 path");

    let boundary = "hvat-keep-both-path-boundary";
    let body = multipart_upload_body(boundary, Some("folder"), &[("sample.png", b"new-image")]);
    let multipart = parse_multipart(boundary, body).await;
    let uploaded = save_uploaded_images(temp_dir.path(), multipart, |_| true)
        .await
        .expect("upload keeps both images");

    assert_eq!(uploaded.files, ["folder/sample_v4.png"]);
    assert_eq!(
        std::fs::read(nested.join("sample.png")).expect("read original image"),
        b"original-image"
    );
    assert_eq!(
        std::fs::read(nested.join("sample_v2.png")).expect("read occupied v2 path"),
        b"occupied-v2"
    );
    assert_eq!(
        std::fs::read(nested.join("sample_v3.png")).expect("read occupied v3 path"),
        b"occupied-v3"
    );
    assert_eq!(
        std::fs::read(nested.join("sample_v4.png")).expect("read newly uploaded version"),
        b"new-image"
    );
    assert!(directory_entries(&nested).iter().all(|path| {
        !path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().contains(".uploading-"))
    }));
}

#[tokio::test]
async fn managed_keep_both_allocation_atomic_no_clobber_race_and_unsupported_publication() {
    let temp_dir = tempfile::tempdir().expect("concurrent upload directory");
    let boundary_a = "hvat-keep-both-race-a";
    let boundary_b = "hvat-keep-both-race-b";
    let multipart_a = parse_multipart(
        boundary_a,
        multipart_upload_body(boundary_a, None, &[("same.png", b"first-version")]),
    )
    .await;
    let multipart_b = parse_multipart(
        boundary_b,
        multipart_upload_body(boundary_b, None, &[("same.png", b"second-version")]),
    )
    .await;

    let (first, second) = tokio::join!(
        save_uploaded_images(temp_dir.path(), multipart_a, |_| true),
        save_uploaded_images(temp_dir.path(), multipart_b, |_| true),
    );
    let first = first.expect("first concurrent upload succeeds");
    let second = second.expect("second concurrent upload succeeds");
    let mut paths = vec![first.files[0].clone(), second.files[0].clone()];
    paths.sort();
    assert_eq!(paths, ["same.png", "same_v2.png"]);

    let mut contents = paths
        .iter()
        .map(|path| std::fs::read(temp_dir.path().join(path)).expect("read published version"))
        .collect::<Vec<_>>();
    contents.sort();
    assert_eq!(
        contents,
        [b"first-version".to_vec(), b"second-version".to_vec()]
    );

    let unsupported_dir = tempfile::tempdir().expect("unsupported publication directory");
    let original_path = unsupported_dir.path().join("failed.png");
    std::fs::write(&original_path, b"previous-image").expect("seed original destination");
    let boundary = "hvat-keep-both-unsupported";
    let multipart = parse_multipart(
        boundary,
        multipart_upload_body(boundary, None, &[("failed.png", b"replacement-image")]),
    )
    .await;
    let io = FailingUploadIo::failing_at(UploadIoOperation::PublishNoClobber, "failed.png");
    let error = save_uploaded_images_with_io(unsupported_dir.path(), multipart, |_| true, &io)
        .await
        .expect_err("unsupported no-clobber publication fails closed");

    assert!(io.failure_was_injected());
    assert_eq!(error.0, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(error.1.contains("no-clobber publication"));
    assert!(
        error
            .1
            .contains("no replacing-rename fallback was attempted")
    );
    assert_eq!(
        std::fs::read(&original_path).expect("read preserved original"),
        b"previous-image"
    );
    assert_eq!(directory_entries(unsupported_dir.path()), [original_path]);
}

async fn assert_upload_io_failure_cleanup_preserves_previous_destination_and_completed_files(
    operation: UploadIoOperation,
) {
    let temp_dir = tempfile::tempdir().expect("temporary upload directory");
    let previous_destination = b"previous-image-bytes";
    let failed_destination = temp_dir.path().join("failed.png");
    std::fs::write(&failed_destination, previous_destination).expect("seed failed destination");

    let boundary = "hvat-upload-io-failure-boundary";
    let multipart = parse_multipart(boundary, two_file_io_failure_body(boundary)).await;
    let io = FailingUploadIo::failing_at(operation, "failed.png");
    let error = save_uploaded_images_with_io(temp_dir.path(), multipart, |_| true, &io)
        .await
        .expect_err("injected I/O failure should fail the later part");

    assert!(
        io.failure_was_injected(),
        "the configured {operation:?} failure must be reached on failed.png"
    );
    if operation == UploadIoOperation::Write {
        assert!(
            io.injected_write_bytes() > 0,
            "the write failure must follow a nonempty partial write"
        );
    }
    assert_eq!(error.0, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(error.1.starts_with(PARTIAL_UPLOAD_ERROR_PREFIX));
    assert!(error.1.contains("1 file(s) were kept"));
    assert_eq!(
        std::fs::read(temp_dir.path().join("first.png")).expect("first file remains committed"),
        b"first-completed-bytes"
    );
    assert_eq!(
        std::fs::read(&failed_destination).expect("previous destination remains readable"),
        previous_destination
    );
    let entries = directory_entries(temp_dir.path());
    assert!(entries.iter().all(|path| {
        !path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().contains(".uploading-"))
    }));
}

#[tokio::test]
async fn upload_io_failure_cleanup_write_preserves_previous_destination_and_completed_files() {
    assert_upload_io_failure_cleanup_preserves_previous_destination_and_completed_files(
        UploadIoOperation::Write,
    )
    .await;
}

#[tokio::test]
async fn upload_io_failure_cleanup_flush_preserves_previous_destination_and_completed_files() {
    assert_upload_io_failure_cleanup_preserves_previous_destination_and_completed_files(
        UploadIoOperation::Flush,
    )
    .await;
}

#[tokio::test]
async fn upload_io_failure_cleanup_publish_no_clobber_preserves_previous_destination_and_completed_files()
 {
    assert_upload_io_failure_cleanup_preserves_previous_destination_and_completed_files(
        UploadIoOperation::PublishNoClobber,
    )
    .await;
}

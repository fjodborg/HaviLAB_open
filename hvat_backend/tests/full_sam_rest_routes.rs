//! Integration tests for the REST-first full_sam backend routes.

use std::io::Cursor;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use image::{ImageBuffer, Rgb};
use ndarray::Array3;
use ndarray_npy::WriteNpyExt;
use tempfile::TempDir;
use tokio::net::TcpListener;

use hvat_backend::common::upload::MAX_UPLOAD_TARGET_ROOT_BYTES;
use hvat_backend::sam::{ExecutionProvider, SamVariant};
use hvat_backend::{AppState, ServerConfig, routes};

struct TestContext {
    temp_dir: TempDir,
    #[expect(dead_code)]
    cache_dir: TempDir,
    addr: SocketAddr,
}

fn create_test_png() -> Vec<u8> {
    let img: ImageBuffer<Rgb<u8>, Vec<u8>> =
        ImageBuffer::from_fn(12, 8, |_x, _y| Rgb([255u8, 32u8, 16u8]));
    let mut bytes = Vec::new();
    img.write_to(&mut Cursor::new(&mut bytes), image::ImageFormat::Png)
        .expect("encode png");
    bytes
}

fn create_large_test_png() -> Vec<u8> {
    let img: ImageBuffer<Rgb<u8>, Vec<u8>> = ImageBuffer::from_fn(512, 512, |x, y| {
        let r = (x % 256) as u8;
        let g = (y % 256) as u8;
        let b = ((x + y) % 256) as u8;
        Rgb([r, g, b])
    });
    let mut bytes = Vec::new();
    img.write_to(&mut Cursor::new(&mut bytes), image::ImageFormat::Png)
        .expect("encode large png");
    bytes
}

fn create_test_multiband_npy() -> Vec<u8> {
    let arr = Array3::from_shape_fn((7, 6, 5), |(band, y, x)| {
        ((band * 100 + y * 10 + x) as f32) / 1000.0
    });
    let mut bytes = Vec::new();
    arr.write_npy(&mut Cursor::new(&mut bytes))
        .expect("encode npy");
    bytes
}

async fn bind_test_listener() -> Option<(TcpListener, SocketAddr)> {
    match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => match listener.local_addr() {
            Ok(addr) => Some((listener, addr)),
            Err(e) => panic!("failed to get listener address: {e}"),
        },
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("Skipping test: cannot bind listener in this environment ({e})");
            None
        }
        Err(e) => panic!("failed to bind listener: {e}"),
    }
}

async fn setup_test_server() -> Option<TestContext> {
    let temp_dir = TempDir::new().expect("temp data dir");
    let cache_dir = TempDir::new().expect("temp cache dir");

    std::fs::write(temp_dir.path().join("test_image.png"), create_test_png())
        .expect("write root image");
    std::fs::write(
        temp_dir.path().join("large_image.png"),
        create_large_test_png(),
    )
    .expect("write large root image");
    std::fs::write(
        temp_dir.path().join("hyper_7band.npy"),
        create_test_multiband_npy(),
    )
    .expect("write multiband npy");
    let nested_dir = temp_dir.path().join("nested");
    std::fs::create_dir_all(&nested_dir).expect("create nested dir");
    std::fs::write(nested_dir.join("nested_image.png"), create_test_png())
        .expect("write nested image");

    let config = ServerConfig {
        base: hvat_backend::common::config::ServerConfig {
            port: 0,
            data_dir: temp_dir.path().to_path_buf(),
            cache_dir: cache_dir.path().to_path_buf(),
            max_cache_memory: 100 * 1024 * 1024,
            project_name: "rest_routes_test".to_string(),
        },
        max_user_memory: 50 * 1024 * 1024,
        pyramid_concurrency: 2,
        sam_enabled: false,
        sam_model_dir: PathBuf::from("./.cache/models"),
        sam_variant: SamVariant::Tiny,
        sam_provider: ExecutionProvider::Cpu,
        sam_cache_size: 8,
        band_cache_size: 4,
    };

    let state = Arc::new(AppState::new(config).expect("init app state"));
    let app = axum::Router::new()
        .nest("/api", routes::api_router())
        .layer(tower_http::compression::CompressionLayer::new())
        .layer(
            tower_http::cors::CorsLayer::new()
                .allow_origin(tower_http::cors::Any)
                .allow_methods(tower_http::cors::Any)
                .allow_headers(tower_http::cors::Any)
                .expose_headers(tower_http::cors::Any),
        )
        .with_state(state);

    let (listener, addr) = bind_test_listener().await?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    Some(TestContext {
        temp_dir,
        cache_dir,
        addr,
    })
}

async fn fetch_first_image_id(client: &reqwest::Client, addr: SocketAddr) -> String {
    let resp = client
        .get(format!("http://{addr}/api/images"))
        .send()
        .await
        .expect("fetch images");
    assert!(resp.status().is_success());
    let images: serde_json::Value = resp.json().await.expect("decode images");
    images[0]["id"].as_str().expect("image id").to_string()
}

async fn fetch_image_id_by_name(client: &reqwest::Client, addr: SocketAddr, name: &str) -> String {
    let resp = client
        .get(format!("http://{addr}/api/images"))
        .send()
        .await
        .expect("fetch images");
    assert!(resp.status().is_success());
    let images: serde_json::Value = resp.json().await.expect("decode images");
    let Some(found) = images.as_array().and_then(|items| {
        items
            .iter()
            .find(|item| item["name"].as_str() == Some(name))
    }) else {
        panic!("expected image named '{name}' in listing: {images}");
    };
    found["id"].as_str().expect("image id").to_string()
}

async fn fetch_metrics(client: &reqwest::Client, addr: SocketAddr) -> serde_json::Value {
    client
        .get(format!("http://{addr}/api/metrics"))
        .send()
        .await
        .expect("fetch metrics")
        .json()
        .await
        .expect("decode metrics json")
}

fn append_multipart_file(
    body: &mut Vec<u8>,
    boundary: &str,
    filename: &str,
    content_type: &str,
    content: &[u8],
) {
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"{filename}\"\r\nContent-Type: {content_type}\r\n\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(content);
    body.extend_from_slice(b"\r\n");
}

#[tokio::test]
async fn rest_info_images_and_capabilities_work() {
    let Some(ctx) = setup_test_server().await else {
        return;
    };
    let client = reqwest::Client::new();

    let info = client
        .get(format!("http://{}/api/info", ctx.addr))
        .send()
        .await
        .expect("fetch info");
    assert!(info.status().is_success());

    let images = client
        .get(format!("http://{}/api/images", ctx.addr))
        .send()
        .await
        .expect("fetch images");
    assert!(images.status().is_success());
    let images_json: serde_json::Value = images.json().await.expect("decode images");
    assert_eq!(images_json.as_array().map(Vec::len), Some(4));

    let caps = client
        .get(format!("http://{}/api/capabilities", ctx.addr))
        .send()
        .await
        .expect("fetch capabilities");
    assert!(caps.status().is_success());
    let caps_json: serde_json::Value = caps.json().await.expect("decode caps");
    assert_eq!(caps_json["features"]["project_state"].as_bool(), Some(true));
    assert_eq!(caps_json["features"]["downloads"].as_bool(), Some(true));
    assert_eq!(caps_json["features"]["thumbnails"].as_bool(), Some(true));
    assert_eq!(caps_json["features"]["inference"].as_bool(), Some(false));
    assert_eq!(caps_json["features"]["sam"].as_bool(), Some(false));
    assert_eq!(
        caps_json["features"]["managed_upload_keep_both"].as_bool(),
        Some(true)
    );
    assert_eq!(caps_json["models"].as_array().map(Vec::len), Some(0));
    assert!(caps_json["features"].get("streaming").is_none());
    assert!(caps_json["features"].get("progressive_streaming").is_none());
    assert!(caps_json["limits"].get("max_concurrent_streams").is_none());
}

#[tokio::test]
async fn legacy_websocket_route_is_not_mounted() {
    let Some(ctx) = setup_test_server().await else {
        return;
    };

    let response = reqwest::Client::new()
        .get(format!("http://{}/api/ws", ctx.addr))
        .send()
        .await
        .expect("request absent legacy route");

    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn rest_image_bands_payload_contract_is_valid() {
    let Some(ctx) = setup_test_server().await else {
        return;
    };
    let client = reqwest::Client::new();
    let image_id = fetch_image_id_by_name(&client, ctx.addr, "test_image.png").await;

    let resp = client
        .get(format!("http://{}/api/images/{}/bands", ctx.addr, image_id))
        .send()
        .await
        .expect("fetch bands");
    assert!(resp.status().is_success());

    let headers = resp.headers().clone();
    assert_eq!(
        headers
            .get("X-Hvat-Payload")
            .and_then(|h| h.to_str().ok())
            .unwrap_or_default(),
        "rgba_layers_u8"
    );
    assert_eq!(
        headers
            .get("X-Hvat-Layout")
            .and_then(|h| h.to_str().ok())
            .unwrap_or_default(),
        "layer-major"
    );
    assert_eq!(
        headers
            .get("X-Hvat-Num-Bands")
            .and_then(|h| h.to_str().ok())
            .unwrap_or_default(),
        "3"
    );
    assert_eq!(
        headers
            .get("X-Hvat-Payload-Version")
            .and_then(|h| h.to_str().ok())
            .unwrap_or_default(),
        "1"
    );

    let width = headers
        .get("X-Hvat-Width")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<usize>().ok())
        .expect("width header");
    let height = headers
        .get("X-Hvat-Height")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<usize>().ok())
        .expect("height header");
    let num_layers = headers
        .get("X-Hvat-Num-Layers")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<usize>().ok())
        .expect("num layers header");

    let body = resp.bytes().await.expect("read bytes");
    let expected_len = width * height * 4 * num_layers;
    assert_eq!(body.len(), expected_len);
}

#[tokio::test]
async fn rest_image_bands_reports_total_source_band_count_for_multiband_inputs() {
    let Some(ctx) = setup_test_server().await else {
        return;
    };
    let client = reqwest::Client::new();
    let image_id = fetch_image_id_by_name(&client, ctx.addr, "hyper_7band.npy").await;

    let resp = client
        .get(format!("http://{}/api/images/{}/bands", ctx.addr, image_id))
        .header(reqwest::header::ORIGIN, "https://example.test")
        .send()
        .await
        .expect("fetch bands");
    assert!(resp.status().is_success());

    let headers = resp.headers().clone();
    assert_eq!(
        headers
            .get(reqwest::header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|value| value.to_str().ok()),
        Some("*")
    );
    assert_eq!(
        headers
            .get(reqwest::header::ACCESS_CONTROL_EXPOSE_HEADERS)
            .and_then(|value| value.to_str().ok()),
        Some("*")
    );
    assert_eq!(
        headers
            .get("X-Hvat-Width")
            .and_then(|value| value.to_str().ok()),
        Some("5")
    );
    assert_eq!(
        headers
            .get("X-Hvat-Height")
            .and_then(|value| value.to_str().ok()),
        Some("6")
    );
    assert_eq!(
        headers
            .get("X-Hvat-Num-Bands")
            .and_then(|h| h.to_str().ok())
            .unwrap_or_default(),
        "7"
    );
    assert_eq!(
        headers
            .get("X-Hvat-Num-Layers")
            .and_then(|h| h.to_str().ok())
            .unwrap_or_default(),
        "2"
    );

    let body = resp.bytes().await.expect("read seven-band payload");
    assert_eq!(body.len(), 240);
}

#[tokio::test]
async fn rest_core_contract_characterization_malformed_npy_bands_returns_internal_server_error() {
    let Some(ctx) = setup_test_server().await else {
        return;
    };
    let mut malformed_npy = create_test_multiband_npy();
    malformed_npy.pop();
    std::fs::write(ctx.temp_dir.path().join("malformed.npy"), malformed_npy)
        .expect("write truncated but otherwise supported NPY file");

    let client = reqwest::Client::new();
    let image_id = fetch_image_id_by_name(&client, ctx.addr, "malformed.npy").await;
    let response = client
        .get(format!("http://{}/api/images/{}/bands", ctx.addr, image_id))
        .send()
        .await
        .expect("request malformed NPY bands");

    assert_eq!(
        response.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[tokio::test]
async fn rest_image_bands_supports_compression_negotiation() {
    let Some(ctx) = setup_test_server().await else {
        return;
    };
    let client = reqwest::Client::builder()
        .no_brotli()
        .no_deflate()
        .no_gzip()
        .build()
        .expect("build reqwest client");
    let image_id = fetch_image_id_by_name(&client, ctx.addr, "large_image.png").await;

    let resp = client
        .get(format!("http://{}/api/images/{}/bands", ctx.addr, image_id))
        .header(reqwest::header::ACCEPT_ENCODING, "gzip")
        .send()
        .await
        .expect("fetch bands");
    assert!(resp.status().is_success());

    let encoding = resp
        .headers()
        .get(reqwest::header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();
    assert_eq!(
        encoding, "gzip",
        "expected gzip content-encoding when requested"
    );
}

#[tokio::test]
async fn rest_sam_infer_without_model_returns_503() {
    let Some(ctx) = setup_test_server().await else {
        return;
    };
    let client = reqwest::Client::new();
    let image_id = fetch_first_image_id(&client, ctx.addr).await;

    let resp = client
        .post(format!("http://{}/api/sam/infer", ctx.addr))
        .json(&serde_json::json!({
            "model_id": "sam3",
            "image_id": image_id,
            "bands": { "r": 0, "g": 1, "b": 2 },
            "inputs": { "points": [[10.0, 12.0]], "labels": [1] }
        }))
        .send()
        .await
        .expect("post sam infer");

    assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let err: serde_json::Value = resp.json().await.expect("decode error");
    assert_eq!(err["code"], "model_unavailable");
}

#[tokio::test]
async fn rest_metrics_endpoint_tracks_requests_and_errors() {
    let Some(ctx) = setup_test_server().await else {
        return;
    };
    let client = reqwest::Client::new();
    let image_id = fetch_first_image_id(&client, ctx.addr).await;

    let before = fetch_metrics(&client, ctx.addr).await;

    let _ = client
        .get(format!("http://{}/api/images/{}/bands", ctx.addr, image_id))
        .send()
        .await
        .expect("image bands request");

    let _ = client
        .post(format!("http://{}/api/sam/infer", ctx.addr))
        .json(&serde_json::json!({
            "model_id": "sam3",
            "image_id": image_id,
            "inputs": { "points": [[1.0, 2.0]], "labels": [1] }
        }))
        .send()
        .await
        .expect("sam infer request");

    let after = fetch_metrics(&client, ctx.addr).await;

    let image_before = before["image_bands"]["requests"]
        .as_u64()
        .expect("image_bands.requests before");
    let image_after = after["image_bands"]["requests"]
        .as_u64()
        .expect("image_bands.requests after");
    assert!(image_after > image_before);

    let infer_before = before["sam_infer"]["requests"]
        .as_u64()
        .expect("sam_infer.requests before");
    let infer_after = after["sam_infer"]["requests"]
        .as_u64()
        .expect("sam_infer.requests after");
    assert!(infer_after > infer_before);

    let infer_errors_before = before["sam_infer"]["errors"]
        .as_u64()
        .expect("sam_infer.errors before");
    let infer_errors_after = after["sam_infer"]["errors"]
        .as_u64()
        .expect("sam_infer.errors after");
    assert!(infer_errors_after > infer_errors_before);
}

#[tokio::test]
async fn rest_upload_rejects_oversize_target_root() {
    let Some(ctx) = setup_test_server().await else {
        return;
    };
    let client = reqwest::Client::new();
    let boundary = "hvat-oversize-target-root";
    let mut body =
        format!("--{boundary}\r\nContent-Disposition: form-data; name=\"target_root\"\r\n\r\n")
            .into_bytes();
    body.extend(std::iter::repeat_n(b'a', MAX_UPLOAD_TARGET_ROOT_BYTES + 1));
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

    let response = client
        .post(format!("http://{}/api/images/upload", ctx.addr))
        .header(
            reqwest::header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(body)
        .send()
        .await
        .expect("post oversize upload metadata");

    assert_eq!(response.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
    assert!(
        response
            .text()
            .await
            .expect("read oversize response")
            .contains("target_root")
    );
    assert!(!ctx.temp_dir.path().join("a").exists());
}

#[tokio::test]
async fn rest_upload_reports_and_keeps_files_completed_before_failure() {
    let Some(ctx) = setup_test_server().await else {
        return;
    };
    let client = reqwest::Client::new();
    let boundary = "hvat-partial-upload";
    let mut body = Vec::new();
    append_multipart_file(
        &mut body,
        boundary,
        "uploaded.png",
        "image/png",
        &create_test_png(),
    );
    append_multipart_file(
        &mut body,
        boundary,
        "unsupported.txt",
        "text/plain",
        b"not an image",
    );
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

    let response = client
        .post(format!("http://{}/api/images/upload", ctx.addr))
        .header(
            reqwest::header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(body)
        .send()
        .await
        .expect("post partially valid upload");

    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    let error = response.text().await.expect("read partial upload response");
    assert!(
        error.starts_with("PARTIAL_UPLOAD:"),
        "unexpected error: {error}"
    );
    assert!(error.contains("1 file(s) were kept"));

    let uploaded = ctx.temp_dir.path().join("uploaded.png");
    assert_eq!(
        std::fs::read(&uploaded).expect("completed file is kept"),
        create_test_png()
    );
    assert!(!ctx.temp_dir.path().join("unsupported.txt").exists());

    let response = client
        .get(format!("http://{}/api/images", ctx.addr))
        .send()
        .await
        .expect("refresh image list");
    let images: serde_json::Value = response.json().await.expect("decode image list");
    assert!(
        images
            .as_array()
            .is_some_and(|images| images.iter().any(|image| image["name"] == "uploaded.png"))
    );
}

#[tokio::test]
async fn managed_keep_both_upload_preserves_original_and_returns_actual_path() {
    let Some(ctx) = setup_test_server().await else {
        return;
    };
    let client = reqwest::Client::new();
    let boundary = "hvat-managed-keep-both-original";
    let uploaded_bytes = create_large_test_png();
    let mut body = Vec::new();
    append_multipart_file(
        &mut body,
        boundary,
        "test_image.png",
        "image/png",
        &uploaded_bytes,
    );
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

    let response = client
        .post(format!("http://{}/api/images/upload", ctx.addr))
        .header(
            reqwest::header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(body)
        .send()
        .await
        .expect("post no-action duplicate upload");
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let uploaded: serde_json::Value = response.json().await.expect("decode upload response");
    assert_eq!(uploaded["uploaded"].as_u64(), Some(1));
    assert_eq!(uploaded["files"], serde_json::json!(["test_image_v2.png"]));
    assert_eq!(
        std::fs::read(ctx.temp_dir.path().join("test_image.png")).expect("read original image"),
        create_test_png()
    );
    assert_eq!(
        std::fs::read(ctx.temp_dir.path().join("test_image_v2.png"))
            .expect("read uploaded version"),
        uploaded_bytes
    );
}

#[tokio::test]
async fn managed_keep_both_upload_same_request_duplicates_get_distinct_paths_and_ids() {
    let Some(ctx) = setup_test_server().await else {
        return;
    };
    let client = reqwest::Client::new();
    let boundary = "hvat-managed-keep-both-batch";
    let first_bytes = create_test_png();
    let second_bytes = create_large_test_png();
    let mut body = Vec::new();
    append_multipart_file(
        &mut body,
        boundary,
        "repeated.png",
        "image/png",
        &first_bytes,
    );
    append_multipart_file(
        &mut body,
        boundary,
        "repeated.png",
        "image/png",
        &second_bytes,
    );
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

    let response = client
        .post(format!("http://{}/api/images/upload", ctx.addr))
        .header(
            reqwest::header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(body)
        .send()
        .await
        .expect("post same-request duplicate uploads");
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let uploaded: serde_json::Value = response.json().await.expect("decode upload response");
    assert_eq!(uploaded["uploaded"].as_u64(), Some(2));
    assert_eq!(
        uploaded["files"],
        serde_json::json!(["repeated.png", "repeated_v2.png"])
    );
    assert_eq!(
        std::fs::read(ctx.temp_dir.path().join("repeated.png")).expect("read first version"),
        first_bytes
    );
    assert_eq!(
        std::fs::read(ctx.temp_dir.path().join("repeated_v2.png")).expect("read second version"),
        second_bytes
    );

    let images: serde_json::Value = client
        .get(format!("http://{}/api/images", ctx.addr))
        .send()
        .await
        .expect("refresh image catalog")
        .json()
        .await
        .expect("decode image catalog");
    let find_by_path = |path: &str| {
        images
            .as_array()
            .and_then(|images| images.iter().find(|image| image["path"] == path))
            .unwrap_or_else(|| panic!("expected path {path} in image catalog: {images}"))
    };
    let original = find_by_path("repeated.png");
    let version = find_by_path("repeated_v2.png");
    assert_ne!(original["id"], version["id"]);
}

#[tokio::test]
async fn managed_keep_both_upload_later_part_failure_keeps_completed_version() {
    let Some(ctx) = setup_test_server().await else {
        return;
    };
    let client = reqwest::Client::new();
    let boundary = "hvat-managed-keep-both-partial";
    let uploaded_bytes = create_large_test_png();
    let mut body = Vec::new();
    append_multipart_file(
        &mut body,
        boundary,
        "test_image.png",
        "image/png",
        &uploaded_bytes,
    );
    append_multipart_file(
        &mut body,
        boundary,
        "unsupported.txt",
        "text/plain",
        b"not an image",
    );
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

    let response = client
        .post(format!("http://{}/api/images/upload", ctx.addr))
        .header(
            reqwest::header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(body)
        .send()
        .await
        .expect("post partially valid duplicate upload");
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    let error = response.text().await.expect("read partial upload response");
    assert!(
        error.starts_with("PARTIAL_UPLOAD:"),
        "unexpected error: {error}"
    );
    assert!(error.contains("1 file(s) were kept"));
    assert_eq!(
        std::fs::read(ctx.temp_dir.path().join("test_image.png")).expect("read original image"),
        create_test_png()
    );
    assert_eq!(
        std::fs::read(ctx.temp_dir.path().join("test_image_v2.png"))
            .expect("read completed version"),
        uploaded_bytes
    );
    assert!(!ctx.temp_dir.path().join("unsupported.txt").exists());
}

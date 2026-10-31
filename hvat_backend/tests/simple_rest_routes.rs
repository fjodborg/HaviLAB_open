//! Integration tests for the HVAT simple REST backend.

use image::{ImageBuffer, Rgb};
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;

use hvat_backend::simple::{AppState, ServerConfig, routes};
use hvat_common::annotation_io::{BundleFile, ExportBundle};
use zip::ZipArchive;

struct TestContext {
    temp_dir: TempDir,
    addr: SocketAddr,
}

fn create_test_png() -> Vec<u8> {
    let img: ImageBuffer<Rgb<u8>, Vec<u8>> =
        ImageBuffer::from_fn(10, 10, |_x, _y| Rgb([255u8, 0u8, 0u8]));

    let mut bytes: Vec<u8> = Vec::new();
    img.write_to(&mut Cursor::new(&mut bytes), image::ImageFormat::Png)
        .expect("Failed to encode PNG");
    bytes
}

fn append_multipart_file(body: &mut Vec<u8>, boundary: &str, filename: &str, content: &[u8]) {
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"{filename}\"\r\nContent-Type: image/png\r\n\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(content);
    body.extend_from_slice(b"\r\n");
}

async fn bind_test_listener() -> Option<(TcpListener, SocketAddr)> {
    match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => match listener.local_addr() {
            Ok(addr) => Some((listener, addr)),
            Err(e) => panic!("Failed to get listener address: {e}"),
        },
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("Skipping test: unable to bind local TCP listener in this environment ({e})");
            None
        }
        Err(e) => panic!("Failed to bind local TCP listener: {e}"),
    }
}

async fn setup_test_server() -> Option<TestContext> {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let test_image_path = temp_dir.path().join("test_image.png");
    std::fs::write(&test_image_path, create_test_png()).expect("Failed to write test image");

    let subfolder = temp_dir.path().join("subfolder");
    std::fs::create_dir(&subfolder).expect("Failed to create subfolder");
    std::fs::write(subfolder.join("nested_image.png"), create_test_png())
        .expect("Failed to write nested image");

    let config = ServerConfig {
        port: 0,
        data_dir: temp_dir.path().to_path_buf(),
        cache_dir: temp_dir.path().join(".cache"),
        max_cache_memory: 100 * 1024 * 1024,
        project_name: "test_project".to_string(),
    };

    let state = Arc::new(AppState::new(config));

    let cors = tower_http::cors::CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods(tower_http::cors::Any)
        .allow_headers(tower_http::cors::Any);

    let app = axum::Router::new()
        .nest("/api", routes::api_router())
        .layer(cors)
        .with_state(state);

    let (listener, addr) = bind_test_listener().await?;

    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            eprintln!("Test server exited with error: {err}");
        }
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    Some(TestContext { temp_dir, addr })
}

#[tokio::test]
async fn simple_rest_api_info() {
    let Some(ctx) = setup_test_server().await else {
        return;
    };

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{}/api/info", ctx.addr))
        .send()
        .await
        .expect("Failed to fetch info");

    assert!(resp.status().is_success());

    let info: serde_json::Value = resp.json().await.expect("Failed to parse JSON");
    assert!(info.get("name").is_some());
    assert_eq!(info["image_count"].as_u64().unwrap(), 2);
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
async fn simple_rest_api_images() {
    let Some(ctx) = setup_test_server().await else {
        return;
    };

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{}/api/images", ctx.addr))
        .send()
        .await
        .expect("Failed to fetch images");

    assert!(resp.status().is_success());

    let images: serde_json::Value = resp.json().await.expect("Failed to parse JSON");
    let images_arr = images.as_array().unwrap();
    assert_eq!(images_arr.len(), 2);

    for img in images_arr {
        assert!(img.get("id").is_some());
        assert!(img.get("name").is_some());
        assert!(img.get("path").is_some());
        assert!(img.get("format").is_some());
    }
}

#[tokio::test]
async fn simple_rest_managed_keep_both_capability_and_shared_upload_behavior() {
    let Some(ctx) = setup_test_server().await else {
        return;
    };
    let client = reqwest::Client::new();
    let capabilities = client
        .get(format!("http://{}/api/capabilities", ctx.addr))
        .send()
        .await
        .expect("fetch simple-profile capabilities");
    assert_eq!(capabilities.status(), reqwest::StatusCode::OK);
    let capabilities: serde_json::Value = capabilities
        .json()
        .await
        .expect("decode simple-profile capabilities");
    assert_eq!(
        capabilities["features"]["managed_upload_keep_both"].as_bool(),
        Some(true)
    );
    assert_eq!(
        capabilities["features"]["project_state"].as_bool(),
        Some(true)
    );
    assert_eq!(capabilities["features"]["downloads"].as_bool(), Some(true));
    assert_eq!(capabilities["features"]["thumbnails"].as_bool(), Some(true));
    assert_eq!(capabilities["features"]["inference"].as_bool(), Some(false));
    assert_eq!(capabilities["features"]["sam"].as_bool(), Some(false));
    assert_eq!(capabilities["download_mode"].as_str(), Some("single_zip"));
    assert_eq!(capabilities["models"].as_array().map(Vec::len), Some(0));

    let original_path = ctx.temp_dir.path().join("test_image.png");
    let original_bytes = std::fs::read(&original_path).expect("read original image");
    let uploaded_image: ImageBuffer<Rgb<u8>, Vec<u8>> =
        ImageBuffer::from_fn(7, 5, |_x, _y| Rgb([12_u8, 34_u8, 56_u8]));
    let mut uploaded_bytes = Vec::new();
    uploaded_image
        .write_to(
            &mut Cursor::new(&mut uploaded_bytes),
            image::ImageFormat::Png,
        )
        .expect("encode uploaded PNG");
    let boundary = "hvat-simple-managed-keep-both";
    let mut body = Vec::new();
    append_multipart_file(&mut body, boundary, "test_image.png", &uploaded_bytes);
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
        .expect("post legacy-shaped simple-profile upload");
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let uploaded: serde_json::Value = response.json().await.expect("decode upload response");
    assert_eq!(uploaded["uploaded"].as_u64(), Some(1));
    assert_eq!(uploaded["files"], serde_json::json!(["test_image_v2.png"]));
    assert_eq!(
        std::fs::read(original_path).expect("read preserved original image"),
        original_bytes
    );
    assert_eq!(
        std::fs::read(ctx.temp_dir.path().join("test_image_v2.png"))
            .expect("read newly published version"),
        uploaded_bytes
    );
}

#[tokio::test]
async fn simple_rest_api_images_download_archive() {
    let Some(ctx) = setup_test_server().await else {
        return;
    };

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{}/api/images/download", ctx.addr))
        .send()
        .await
        .expect("Failed to fetch image archive");

    assert!(resp.status().is_success());
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    assert!(content_type.contains("application/zip"));

    let bytes = resp.bytes().await.expect("Failed to read archive response");
    let mut archive =
        ZipArchive::new(Cursor::new(bytes.to_vec())).expect("Failed to parse zip archive");

    let mut names = Vec::new();
    for i in 0..archive.len() {
        let entry = archive.by_index(i).expect("Failed to read zip entry");
        names.push(entry.name().to_string());
    }
    names.sort();

    assert_eq!(names.len(), 2);
    assert!(names.iter().any(|name| name == "test_image.png"));
    assert!(
        names
            .iter()
            .any(|name| name == "subfolder/nested_image.png")
    );
}

#[tokio::test]
async fn simple_rest_project_state_round_trip() {
    let Some(ctx) = setup_test_server().await else {
        return;
    };

    let client = reqwest::Client::new();
    let url = format!("http://{}/api/project-state", ctx.addr);

    let missing = client.get(&url).send().await.expect("GET failed");
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);

    let bundle = ExportBundle {
        files: vec![BundleFile {
            path: "annotations/default.json".to_string(),
            bytes: br#"{"items":[]}"#.to_vec(),
            mime_type: Some("application/json".to_string()),
        }],
    };
    let payload = serde_json::to_vec(&bundle).expect("serialize bundle");

    let save = client
        .put(&url)
        .header("content-type", "application/json")
        .body(payload.clone())
        .send()
        .await
        .expect("PUT failed");
    assert_eq!(save.status(), reqwest::StatusCode::NO_CONTENT);

    let loaded = client.get(&url).send().await.expect("GET failed");
    assert_eq!(loaded.status(), reqwest::StatusCode::OK);
    let loaded_bytes = loaded.bytes().await.expect("read body");
    assert_eq!(loaded_bytes.as_ref(), payload.as_slice());

    let decoded: ExportBundle = serde_json::from_slice(&loaded_bytes).expect("decode bundle");
    assert_eq!(decoded.files.len(), 1);
    assert_eq!(decoded.files[0].path, "annotations/default.json");
}

#[tokio::test]
async fn simple_rest_project_state_rejects_invalid_payload() {
    let Some(ctx) = setup_test_server().await else {
        return;
    };

    let client = reqwest::Client::new();
    let url = format!("http://{}/api/project-state", ctx.addr);

    let resp = client
        .put(&url)
        .header("content-type", "application/json")
        .body(r#"{"not":"a valid bundle"}"#)
        .send()
        .await
        .expect("PUT failed");

    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
}

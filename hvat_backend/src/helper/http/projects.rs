//! Shared server info, image listing, upload, and project-state endpoints.

use std::{
    collections::BTreeSet,
    future::Future,
    io::{self, ErrorKind},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, FromRequest, Multipart, Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Serialize;
use tokio::sync::watch;
use tower::{Layer, Service};

use crate::helper::catalog::{ImageInfo, count_supported_images, list_supported_images};
use crate::helper::error::Result;
use crate::helper::loaders::ImageLoaderRegistry;
use crate::helper::project_state::{
    MAX_PROJECT_STATE_BYTES, read_project_state, validate_project_state_payload,
    write_project_state,
};
use crate::helper::upload::{meter_multipart_request, save_uploaded_images_with_overhead};

/// Minimal state contract needed by shared project routes.
pub trait ProjectRoutesState: Send + Sync + 'static {
    fn project_name(&self) -> &str;
    fn data_dir(&self) -> &Path;
    fn loaders(&self) -> &ImageLoaderRegistry;
}

type ProjectStateWriteFuture = Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'static>>;
type ProjectStateWriter =
    Arc<dyn Fn(PathBuf, Bytes) -> ProjectStateWriteFuture + Send + Sync + 'static>;
type ProjectStateWaitingHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
type ProjectStateBodyHook = Arc<dyn Fn(&Bytes) + Send + Sync>;

#[derive(Clone)]
struct ProjectStateSaveContext {
    order: ProjectStateSaveOrder,
    writer: ProjectStateWriter,
    #[cfg(test)]
    test_hooks: Option<Arc<ProjectStateSaveTestHooks>>,
}

impl ProjectStateSaveContext {
    fn production() -> Arc<Self> {
        Arc::new(Self {
            order: ProjectStateSaveOrder::new(),
            writer: Arc::new(|data_dir, payload| {
                Box::pin(async move { write_project_state(&data_dir, &payload).await })
            }),
            #[cfg(test)]
            test_hooks: None,
        })
    }
}

#[derive(Clone)]
struct ProjectStateSaveOrder {
    inner: Arc<ProjectStateSaveOrderInner>,
}

struct ProjectStateSaveOrderInner {
    state: Mutex<ProjectStateSaveOrderState>,
    serving: watch::Sender<u64>,
}

struct ProjectStateSaveOrderState {
    next_ticket: u64,
    serving_ticket: u64,
    skipped: BTreeSet<u64>,
}

impl ProjectStateSaveOrder {
    fn new() -> Self {
        let (serving, _receiver) = watch::channel(0);
        Self {
            inner: Arc::new(ProjectStateSaveOrderInner {
                state: Mutex::new(ProjectStateSaveOrderState {
                    next_ticket: 0,
                    serving_ticket: 0,
                    skipped: BTreeSet::new(),
                }),
                serving,
            }),
        }
    }

    fn register(&self) -> ProjectStateSaveTicket {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let ticket = state.next_ticket;
        state.next_ticket = state
            .next_ticket
            .checked_add(1)
            .expect("project-state save ticket counter exhausted");
        ProjectStateSaveTicket(Arc::new(ProjectStateSaveTicketInner {
            order: self.clone(),
            ticket,
            resolved: AtomicBool::new(false),
        }))
    }

    fn is_serving(&self, ticket: u64) -> bool {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .serving_ticket
            == ticket
    }

    fn resolve(&self, ticket: u64) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if ticket < state.serving_ticket {
            return;
        }
        if ticket == state.serving_ticket {
            state.serving_ticket += 1;
            while {
                let serving_ticket = state.serving_ticket;
                state.skipped.remove(&serving_ticket)
            } {
                state.serving_ticket += 1;
            }
        } else {
            state.skipped.insert(ticket);
        }
        self.inner.serving.send_replace(state.serving_ticket);
    }
}

#[derive(Clone)]
struct ProjectStateSaveTicket(Arc<ProjectStateSaveTicketInner>);

struct ProjectStateSaveTicketInner {
    order: ProjectStateSaveOrder,
    ticket: u64,
    resolved: AtomicBool,
}

impl ProjectStateSaveTicket {
    async fn wait_for_turn(&self, on_waiting: Option<&ProjectStateWaitingHook>) {
        let mut serving = self.0.order.inner.serving.subscribe();
        loop {
            if self.0.order.is_serving(self.0.ticket) {
                return;
            }
            if let Some(on_waiting) = on_waiting {
                on_waiting();
            }
            serving
                .changed()
                .await
                .expect("project-state save order sender remains alive");
        }
    }

    fn resolve(&self) {
        if !self.0.resolved.swap(true, Ordering::AcqRel) {
            self.0.order.resolve(self.0.ticket);
        }
    }
}

impl Drop for ProjectStateSaveTicketInner {
    fn drop(&mut self) {
        if !self.resolved.swap(true, Ordering::AcqRel) {
            self.order.resolve(self.ticket);
        }
    }
}

#[derive(Clone)]
struct ProjectStateSaveRequest {
    ticket: ProjectStateSaveTicket,
    writer: ProjectStateWriter,
    #[cfg(test)]
    test_hooks: Option<Arc<ProjectStateSaveTestHooks>>,
}

#[cfg(test)]
#[derive(Clone, Default)]
struct ProjectStateSaveTestHooks {
    body_extracted: Option<ProjectStateBodyHook>,
    waiting_for_turn: Option<ProjectStateBodyHook>,
}

#[derive(Clone)]
struct ProjectStateSaveLayer {
    context: Arc<ProjectStateSaveContext>,
}

impl<S> Layer<S> for ProjectStateSaveLayer {
    type Service = ProjectStateSaveService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ProjectStateSaveService {
            inner,
            context: self.context.clone(),
        }
    }
}

#[derive(Clone)]
struct ProjectStateSaveService<S> {
    inner: S,
    context: Arc<ProjectStateSaveContext>,
}

impl<S> Service<axum::extract::Request> for ProjectStateSaveService<S>
where
    S: Service<axum::extract::Request>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, mut request: axum::extract::Request) -> Self::Future {
        if request.method() == Method::PUT {
            request.extensions_mut().insert(ProjectStateSaveRequest {
                ticket: self.context.order.register(),
                writer: self.context.writer.clone(),
                #[cfg(test)]
                test_hooks: self.context.test_hooks.clone(),
            });
        }
        self.inner.call(request)
    }
}

/// Server/project information.
#[derive(Debug, Serialize)]
pub struct ServerInfo {
    pub name: String,
    pub image_count: usize,
    pub version: String,
}

#[derive(Debug, Serialize)]
struct UploadImagesResponse {
    uploaded: usize,
    files: Vec<String>,
}

/// Build shared `/api` routes for project metadata/state operations.
pub fn router<S>() -> Router<Arc<S>>
where
    S: ProjectRoutesState,
{
    router_with_save_context::<S>(ProjectStateSaveContext::production())
}

fn router_with_save_context<S>(save_context: Arc<ProjectStateSaveContext>) -> Router<Arc<S>>
where
    S: ProjectRoutesState,
{
    Router::new()
        .route("/info", get(get_info::<S>))
        .route("/images", get(list_images::<S>))
        .route(
            "/images/upload",
            post(upload_images::<S>).layer(DefaultBodyLimit::disable()),
        )
        .route(
            "/project-state",
            get(get_project_state::<S>)
                .put(put_project_state::<S>)
                .route_layer(ProjectStateSaveLayer {
                    context: save_context,
                }),
        )
}

#[cfg(test)]
fn router_with_test_writer<S>(
    writer: ProjectStateWriter,
    test_hooks: ProjectStateSaveTestHooks,
) -> Router<Arc<S>>
where
    S: ProjectRoutesState,
{
    router_with_save_context::<S>(Arc::new(ProjectStateSaveContext {
        order: ProjectStateSaveOrder::new(),
        writer,
        test_hooks: Some(Arc::new(test_hooks)),
    }))
}

async fn get_info<S>(State(state): State<Arc<S>>) -> Result<Json<ServerInfo>>
where
    S: ProjectRoutesState,
{
    let image_count =
        count_supported_images(state.data_dir(), |path| state.loaders().supports(path));

    Ok(Json(ServerInfo {
        name: state.project_name().to_string(),
        image_count,
        version: env!("CARGO_PKG_VERSION").to_string(),
    }))
}

async fn list_images<S>(State(state): State<Arc<S>>) -> Result<Json<Vec<ImageInfo>>>
where
    S: ProjectRoutesState,
{
    let images = list_supported_images(state.data_dir(), |path| state.loaders().supports(path));
    Ok(Json(images))
}

async fn upload_images<S>(State(state): State<Arc<S>>, request: Request) -> Response
where
    S: ProjectRoutesState,
{
    let (request, meter) = meter_multipart_request(request);
    let multipart = match Multipart::from_request(request, &state).await {
        Ok(multipart) => multipart,
        Err(rejection) => return rejection.into_response(),
    };
    let Some(meter) = meter else {
        return (
            StatusCode::BAD_REQUEST,
            "Invalid multipart overhead meter state".to_string(),
        )
            .into_response();
    };

    match save_uploaded_images_with_overhead(
        state.data_dir(),
        multipart,
        |path| state.loaders().supports(path),
        meter,
    )
    .await
    {
        Ok(uploaded) => (
            StatusCode::CREATED,
            Json(UploadImagesResponse {
                uploaded: uploaded.uploaded_count(),
                files: uploaded.files,
            }),
        )
            .into_response(),
        Err(error) => error.into_response(),
    }
}

async fn get_project_state<S>(
    State(state): State<Arc<S>>,
) -> std::result::Result<impl IntoResponse, (StatusCode, String)>
where
    S: ProjectRoutesState,
{
    let bytes = match read_project_state(state.data_dir()).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == ErrorKind::NotFound => {
            return Err((StatusCode::NOT_FOUND, "No saved project state".to_string()));
        }
        Err(err) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to read saved project state: {err}"),
            ));
        }
    };

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    Ok((StatusCode::OK, headers, bytes))
}

async fn put_project_state<S>(
    State(state): State<Arc<S>>,
    Extension(save_request): Extension<ProjectStateSaveRequest>,
    body: Bytes,
) -> std::result::Result<impl IntoResponse, (StatusCode, String)>
where
    S: ProjectRoutesState,
{
    if body.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "Request body is empty".to_string()));
    }
    if body.len() > MAX_PROJECT_STATE_BYTES {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "Project state payload exceeds limit of {} bytes",
                MAX_PROJECT_STATE_BYTES
            ),
        ));
    }

    if !validate_project_state_payload(&body) {
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid native project bundle payload".to_string(),
        ));
    }

    #[cfg(test)]
    if let Some(on_body_extracted) = save_request
        .test_hooks
        .as_ref()
        .and_then(|hooks| hooks.body_extracted.as_ref())
    {
        on_body_extracted(&body);
    }

    #[cfg(test)]
    let waiting_hook = save_request
        .test_hooks
        .as_ref()
        .and_then(|hooks| hooks.waiting_for_turn.as_ref())
        .map(|hook| {
            let hook = hook.clone();
            let body = body.clone();
            Arc::new(move || hook(&body)) as ProjectStateWaitingHook
        });
    #[cfg(not(test))]
    let waiting_hook: Option<ProjectStateWaitingHook> = None;

    save_request
        .ticket
        .wait_for_turn(waiting_hook.as_ref())
        .await;

    let ticket = save_request.ticket.clone();
    let writer = save_request.writer.clone();
    let data_dir = state.data_dir().to_path_buf();
    let commit = tokio::spawn(async move {
        let result = writer(data_dir, body).await;
        ticket.resolve();
        result
    });
    let write_result = match commit.await {
        Ok(result) => result,
        Err(err) => Err(io::Error::other(format!(
            "project-state save task failed: {err}"
        ))),
    };

    write_result.map_err(|err| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to save project state: {err}"),
        )
    })?;

    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
#[path = "projects.test.rs"]
mod tests;
